"""Tests for the metrics_snapshot mechanism.

Covers the in-process ``_metrics.snapshot()`` shape and the broadcast
daemon's periodic ``metrics_snapshot`` structured-log emission. The
emission is the channel the stress and soak harnesses use to scrape
per-tick metric state from a subprocess daemon without an HTTP scrape
endpoint.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
import pytest_asyncio

from waitbus import _metrics, broadcast

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)


@pytest.fixture(autouse=True)
def _reset_metrics() -> Generator[None, None, None]:
    _metrics.reset()
    yield
    _metrics.reset()


# --- _metrics.snapshot() shape ------------------------------------------------


def test_snapshot_returns_json_serialisable_dict() -> None:
    """``_metrics.snapshot()`` output round-trips through ``json.dumps``.

    A future consumer (the stress or soak harness) will pipe this dict
    into ``_log.structured(... families=...)``, which serialises it as
    JSON. Type drift here is caught loudly at the test-suite gate.
    """
    _metrics.incr("waitbus_subscriber_evicted_total", reason="lag_limit_exceeded")
    _metrics.incr("waitbus_subscriber_evicted_total", reason="subscribe_ack_send_failed")
    out = _metrics.snapshot()
    body = json.dumps(out)
    decoded = json.loads(body)
    assert decoded == out


def test_snapshot_captures_counter_label_combinations() -> None:
    """Each (family, label-tuple) increment shows up in the snapshot output."""
    _metrics.incr("waitbus_subscriber_evicted_total", reason="lag_limit_exceeded")
    _metrics.incr("waitbus_subscriber_evicted_total", reason="lag_limit_exceeded")
    _metrics.incr("waitbus_subscriber_evicted_total", reason="subscribe_ack_send_failed")

    out = _metrics.snapshot()

    family = out["waitbus_subscriber_evicted"]
    by_reason = {tuple(sample["labels"].items()): sample["value"] for sample in family}
    assert by_reason[(("reason", "lag_limit_exceeded"),)] == 2.0
    assert by_reason[(("reason", "subscribe_ack_send_failed"),)] == 1.0


def test_snapshot_includes_unlabelled_gauges() -> None:
    """Unlabelled gauges appear in the snapshot with an empty labels dict."""
    _metrics.BROADCAST_EMISSION_LATENCY_SECONDS.set(0.0123)

    out = _metrics.snapshot()

    family = out["waitbus_broadcast_emission_latency_seconds"]
    assert any(sample["labels"] == {} and sample["value"] == pytest.approx(0.0123) for sample in family)


# --- broadcast daemon periodic emission --------------------------------------


@pytest_asyncio.fixture
async def _short_period_daemon(
    broadcast_paths: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[broadcast.Broadcast, None]:
    """Spin a daemon with a sub-second metrics-snapshot period."""
    # Override the cached config so the snapshot period is short enough for the
    # test to land at least one emission inside its budget. The override goes
    # through pydantic-settings' env-var path.
    monkeypatch.setenv("WAITBUS_METRICS_SNAPSHOT_PERIOD_SEC", "0.2")
    monkeypatch.setenv("WAITBUS_HEARTBEAT_SEC", "30.0")
    # _config caches the constructed CiStatusConfig; drop the cache so the env
    # override is picked up.
    from waitbus import _config

    _config.get_config.cache_clear()

    daemon = broadcast.Broadcast(db_path=str(broadcast_paths["db"]))
    task = asyncio.create_task(daemon.run())
    # Wait for the listener socket to appear before yielding.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if broadcast_paths["broadcast"].exists():
            break
        await asyncio.sleep(0.02)
    else:
        task.cancel()
        raise RuntimeError("daemon failed to bind broadcast socket")
    try:
        yield daemon
    finally:
        await daemon.stop()
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except (TimeoutError, asyncio.CancelledError):
            task.cancel()
        _config.get_config.cache_clear()


@pytest.mark.asyncio
async def test_daemon_emits_periodic_metrics_snapshot(
    _short_period_daemon: broadcast.Broadcast,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The daemon emits at least one ``metrics_snapshot`` log line per period.

    The structured-log helper writes a single JSON line per call; the
    ``event`` key is the canonical filter the stress harness will use to
    isolate snapshots from other daemon log traffic.
    """
    caplog.set_level(logging.INFO, logger="waitbus.broadcast")
    # Period is 0.2 s; sleeping 0.6 s gives at least two emissions worth of
    # budget with margin for asyncio task scheduling jitter.
    await asyncio.sleep(0.6)

    snapshots = [
        json.loads(record.message)
        for record in caplog.records
        if record.message.startswith("{") and "metrics_snapshot" in record.message
    ]
    assert snapshots, "expected at least one metrics_snapshot log line during the test window"

    head = snapshots[0]
    assert head.get("event") == "metrics_snapshot"
    families = head.get("families")
    assert isinstance(families, dict) and families, "metrics_snapshot must carry a non-empty families dict"
    # waitbus_subscriber_count is registered at module import, so it always
    # appears in a fresh snapshot.
    assert "waitbus_subscriber_count" in families
