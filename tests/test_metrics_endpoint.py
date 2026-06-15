"""Tests for the broadcast daemon's opt-in loopback ``/metrics`` endpoint.

The endpoint is OFF by default (no socket bound), enabled via
``WAITBUS_METRICS_PORT`` (the ``--metrics-port`` CLI flag is sugar that
sets the env var), and binds ``127.0.0.1`` only. Daemon tests run a real
in-process ``Broadcast`` with a real wire subscriber -- no bus mocks.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from prometheus_client.parser import text_string_to_metric_families
from typer.testing import CliRunner

from tests._daemon_helpers import await_subscribers, isolated_subprocess_env
from tests._wire_helpers import connect as _connect
from tests._wire_helpers import recv_until as _recv_until
from tests._wire_helpers import subscribe as _subscribe
from waitbus import _config, _db, _metrics, broadcast
from waitbus._metrics_http import MetricsServer
from waitbus._types import EventInsert
from waitbus.cli.main import app

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def _clean_metrics_and_config(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Scrub the metrics-port env var and reset metrics + config cache."""
    monkeypatch.delenv("WAITBUS_METRICS_PORT", raising=False)
    _metrics.reset()
    _config._reset_for_test()
    yield
    _metrics.reset()
    _config._reset_for_test()


def _insert(db: Path, delivery_id: str) -> None:
    """Insert one event row (rings the patched doorbell via insert_event)."""
    event = EventInsert(
        delivery_id=delivery_id,
        source="github",
        event_type="workflow_run",
        owner="test-owner",
        repo="test-repo",
        received_at=time.time_ns(),
        payload_json="{}",
        ingest_method="webhook",
        run_id=1,
        workflow_name="Tests",
        head_branch="main",
        head_sha="abc",
        status="completed",
        conclusion="success",
    )
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(conn, event)


@pytest_asyncio.fixture
async def metrics_daemon(
    broadcast_paths: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[tuple[broadcast.Broadcast, dict[str, Path]], None]:
    """A running daemon with the metrics endpoint enabled on an ephemeral port."""
    monkeypatch.setenv("WAITBUS_METRICS_PORT", "0")
    _config._reset_for_test()
    daemon = broadcast.Broadcast(db_path=str(broadcast_paths["db"]))
    task = asyncio.create_task(daemon.run())
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if daemon._metrics_server is not None:
            break
        await asyncio.sleep(0.02)
    else:
        daemon.stop_event.set()
        raise RuntimeError("daemon did not start the metrics server")
    try:
        yield daemon, broadcast_paths
    finally:
        await daemon.stop()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(task, timeout=5.0)


def _scrape(daemon: broadcast.Broadcast) -> tuple[int, str, str]:
    """GET /metrics from the daemon's server; return (status, content_type, body)."""
    assert daemon._metrics_server is not None
    url = f"http://127.0.0.1:{daemon._metrics_server.port}/metrics"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return resp.status, resp.headers["Content-Type"], resp.read().decode("utf-8")


# --- config / disabled-by-default -------------------------------------------


def test_metrics_port_defaults_to_none(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """With env scrubbed and no config file, no metrics port is configured."""
    cfg_dir = tmp_path / "empty"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    _config._reset_for_test()
    assert _config.get_config().metrics_port is None


def test_metrics_port_env_var_is_picked_up(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """WAITBUS_METRICS_PORT reaches the config field as an int."""
    cfg_dir = tmp_path / "empty"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("WAITBUS_METRICS_PORT", "9464")
    _config._reset_for_test()
    assert _config.get_config().metrics_port == 9464


@pytest.mark.asyncio
async def test_daemon_without_port_opens_no_http_socket(
    broadcast_paths: dict[str, Path],
) -> None:
    """Default config: the running daemon never constructs a metrics server."""
    daemon = broadcast.Broadcast(db_path=str(broadcast_paths["db"]))
    task = asyncio.create_task(daemon.run())
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and not broadcast_paths["broadcast"].exists():
        await asyncio.sleep(0.02)
    try:
        assert daemon.metrics_port is None
        await asyncio.sleep(0.1)
        assert daemon._metrics_server is None
    finally:
        await daemon.stop()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(task, timeout=5.0)


# --- enabled path ------------------------------------------------------------


@pytest.mark.asyncio
async def test_enabled_endpoint_serves_parseable_exposition(
    metrics_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """GET /metrics returns parseable Prometheus text with the expected families."""
    daemon, _paths = metrics_daemon
    status, content_type, body = _scrape(daemon)
    assert status == 200
    assert content_type.startswith("text/plain; version=0.0.4")
    families = {family.name for family in text_string_to_metric_families(body)}
    # The parser reports counter families without their _total suffix.
    assert families >= {
        "waitbus_subscriber_count",
        "waitbus_broadcast_send_seconds",
        "waitbus_subscriber_rejected",
        "waitbus_subscriber_evicted",
        "waitbus_db_error",
        "waitbus_subscriber_opened",
        "waitbus_subscriber_closed",
        "waitbus_broadcast_events_emitted",
        "waitbus_broadcast_events_delivered",
        "waitbus_subscriber_lag_max",
        "waitbus_subscriber_tx_buffer_bytes",
    }


@pytest.mark.asyncio
async def test_endpoint_binds_loopback_only(
    metrics_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """The daemon-wired server binds exactly 127.0.0.1."""
    daemon, _paths = metrics_daemon
    assert daemon._metrics_server is not None
    assert daemon._metrics_server.host == "127.0.0.1"


@pytest.mark.asyncio
async def test_unknown_path_returns_404(
    metrics_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    daemon, _paths = metrics_daemon
    assert daemon._metrics_server is not None
    url = f"http://127.0.0.1:{daemon._metrics_server.port}/nope"
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(url, timeout=5)
    assert excinfo.value.code == 404
    excinfo.value.close()


@pytest.mark.asyncio
async def test_lifecycle_counters_move_on_subscribe_emit_close(
    metrics_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """Subscriber connect, one emitted event, and disconnect each move counters."""
    daemon, paths = metrics_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["test-owner/test-repo"])
        await await_subscribers(daemon)
        assert _metrics.get("waitbus_subscriber_opened_total") == 1
        assert _metrics.SUBSCRIBER_COUNT.value() == 1.0

        _insert(paths["db"], "d-metrics-1")
        frame = await _recv_until(reader, "event")
        assert frame is not None
        assert _metrics.get("waitbus_broadcast_events_emitted_total") == 1
        assert _metrics.get("waitbus_broadcast_events_delivered_total") >= 1

        # The scraped exposition agrees with the in-process view.
        _status, _ct, body = _scrape(daemon)
        assert "waitbus_broadcast_events_emitted_total 1.0" in body
    finally:
        writer.close()
        await writer.wait_closed()

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if _metrics.get("waitbus_subscriber_closed_total") == 1:
            break
        await asyncio.sleep(0.02)
    assert _metrics.get("waitbus_subscriber_closed_total") == 1
    assert _metrics.SUBSCRIBER_COUNT.value() == 0.0


# --- MetricsServer units ------------------------------------------------------


def test_metrics_server_binds_loopback_and_serves() -> None:
    server = MetricsServer(0)
    server.start()
    try:
        assert server.host == "127.0.0.1"
        assert server.port > 0
        url = f"http://127.0.0.1:{server.port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Type"].startswith("text/plain; version=0.0.4")
            assert b"# HELP" in resp.read()
    finally:
        server.stop()


def test_metrics_server_stop_is_idempotent() -> None:
    server = MetricsServer(0)
    server.start()
    server.stop()
    server.stop()


def test_metrics_server_properties_raise_before_start() -> None:
    server = MetricsServer(0)
    with pytest.raises(RuntimeError):
        _ = server.host
    with pytest.raises(RuntimeError):
        _ = server.port


# --- CLI flag ------------------------------------------------------------------


def test_cli_flag_sets_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """--metrics-port sets WAITBUS_METRICS_PORT before the daemon main runs."""
    captured: dict[str, Any] = {}

    def _fake_main() -> int:
        captured["port"] = os.environ.get("WAITBUS_METRICS_PORT")
        return 0

    monkeypatch.setattr("waitbus.broadcast.main", _fake_main)
    try:
        result = runner.invoke(app, ["broadcast", "serve", "--metrics-port", "9464"])
    finally:
        os.environ.pop("WAITBUS_METRICS_PORT", None)
    assert result.exit_code == 0
    assert captured["port"] == "9464"


def test_cli_flag_omitted_leaves_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without --metrics-port the command does not set the env var."""
    captured: dict[str, Any] = {}

    def _fake_main() -> int:
        captured["port"] = os.environ.get("WAITBUS_METRICS_PORT")
        return 0

    monkeypatch.setattr("waitbus.broadcast.main", _fake_main)
    result = runner.invoke(app, ["broadcast", "serve"])
    assert result.exit_code == 0
    assert captured["port"] is None


# --- busy-port resilience ------------------------------------------------------


@pytest.mark.asyncio
async def test_busy_metrics_port_does_not_crash_daemon(
    broadcast_paths: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A metrics port already in use must not take the daemon down.

    Binds a throwaway socket on an ephemeral loopback port first, then
    starts the daemon configured to use that same port: the daemon must
    log the bind failure, run without a metrics server, and still stop
    cleanly through the graceful shutdown path (exit code 0).
    """
    import socket as _socket

    blocker = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    busy_port = blocker.getsockname()[1]
    monkeypatch.setenv("WAITBUS_METRICS_PORT", str(busy_port))
    _config._reset_for_test()

    daemon = broadcast.Broadcast(db_path=str(broadcast_paths["db"]))
    task = asyncio.create_task(daemon.run())
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not broadcast_paths["broadcast"].exists():
            await asyncio.sleep(0.02)
        assert broadcast_paths["broadcast"].exists(), "daemon never bound its listener"
        assert not task.done(), "daemon crashed instead of continuing without metrics"
        assert daemon._metrics_server is None
    finally:
        blocker.close()
        await daemon.stop()
    assert await asyncio.wait_for(task, timeout=5.0) == 0


# --- end-to-end CLI flag ---------------------------------------------------------


def test_cli_flag_serves_metrics_end_to_end(tmp_path: Path) -> None:
    """``--metrics-port`` on a real daemon subprocess actually serves /metrics.

    The in-process flag tests assert only the env mutation; this one proves
    the flag survives the whole config-load path: spawn ``broadcast serve
    --metrics-port <free port>`` as a subprocess and scrape the port.
    """
    import signal
    import socket as _socket
    import subprocess

    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    env, _dirs = isolated_subprocess_env(tmp_path)
    env.pop("WAITBUS_METRICS_PORT", None)

    with subprocess.Popen(
        [
            sys.executable,
            "-m",
            "waitbus.cli.main",
            "broadcast",
            "serve",
            "--metrics-port",
            str(port),
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    ) as proc:
        try:
            body = ""
            deadline = time.monotonic() + 15.0
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    break
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as resp:
                        assert resp.status == 200
                        body = resp.read().decode("utf-8")
                        break
                except (urllib.error.URLError, ConnectionError, TimeoutError):
                    time.sleep(0.1)
            assert proc.poll() is None, f"daemon exited early: {proc.stderr.read().decode() if proc.stderr else ''}"
            assert "waitbus_broadcast_events_emitted_total" in body
        finally:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
