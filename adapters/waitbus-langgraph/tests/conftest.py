"""Self-contained daemon harness for the adapter's offline e2e tests.

A minimal copy of the waitbus repository's test fixtures: the adapter is a
standalone package, so its suite carries its own harness instead of
importing the main repository's tests. The fixtures stand up the REAL
in-process broadcast daemon against per-test tmp paths; the helper
functions insert event rows (ringing the daemon's doorbell) and provide a
deterministic subscriber-registration barrier.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import sqlite3
import time
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from waitbus import _config, _db, broadcast
from waitbus._types import EventInsert


@pytest.fixture(autouse=True)
def _force_gc_after_test() -> Generator[None, None, None]:
    """Collect after each test so leak finalisers attribute to their owner."""
    yield
    gc.collect()


@pytest.fixture
def broadcast_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """Per-test DB + broadcast/doorbell sockets, with path factories redirected.

    Socket paths cannot be expressed as env vars (they are arbitrary
    tmp_path locations), so the path factory functions are patched
    directly. The doorbell factory is patched on the DB module so
    ``insert_event`` rings the daemon-under-test's socket.
    """
    db = tmp_path / "events.db"
    broadcast_sock = tmp_path / "broadcast.sock"
    doorbell_sock = tmp_path / "doorbell.sock"
    monkeypatch.setattr(broadcast, "broadcast_socket", lambda: broadcast_sock)
    monkeypatch.setattr(broadcast, "doorbell_socket", lambda: doorbell_sock)
    monkeypatch.setenv("WAITBUS_HEARTBEAT_SEC", "1")
    # Clear the config cache so the daemon picks up the env override above.
    _config._reset_for_test()
    monkeypatch.setattr(_db._doorbell, "doorbell_socket", lambda: doorbell_sock)
    return {"db": db, "broadcast": broadcast_sock, "doorbell": doorbell_sock}


@pytest_asyncio.fixture
async def running_daemon(
    broadcast_paths: dict[str, Path],
) -> AsyncGenerator[tuple[broadcast.Broadcast, dict[str, Path]], None]:
    """Spin up the broadcast daemon in-loop; yield (daemon, paths)."""
    daemon = broadcast.Broadcast(db_path=str(broadcast_paths["db"]))
    task = asyncio.create_task(daemon.run())
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if broadcast_paths["broadcast"].exists():
            break
        await asyncio.sleep(0.02)
    else:
        task.cancel()
        raise RuntimeError("daemon failed to bind broadcast socket")
    try:
        yield daemon, broadcast_paths
    finally:
        # Graceful stop via the public event, not task.cancel(): cancellation
        # races the daemon's cleanup and leaks sockets to GC-time warnings.
        await daemon.stop()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(task, timeout=5.0)


def insert_event_row(db: Path, delivery_id: str, **overrides: Any) -> None:
    """Insert one event row (rings the doorbell so the daemon fans it out).

    Defaults to a docker frame; override ``source`` / ``event_type`` / etc.
    to shape the triggering event.
    """
    defaults: dict[str, Any] = {
        "source": "docker",
        "event_type": "docker_container",
        "owner": "local",
        "repo": "docker",
        "received_at": time.time_ns(),
        "payload_json": "{}",
        "ingest_method": "watcher",
    }
    defaults.update(overrides)
    with contextlib.closing(sqlite3.connect(str(db))) as conn:
        _db.insert_event(conn, EventInsert(delivery_id=delivery_id, **defaults))


async def await_subscribers(daemon: broadcast.Broadcast, *, added: int = 1, timeout: float = 5.0) -> None:
    """Block until ``added`` net new subscribers register with the daemon.

    Deterministic registration barrier: snapshots the subscriber count at
    entry and polls until the daemon's map grows by ``added``, so an event
    inserted after this returns is guaranteed to fan out to the new
    subscriber rather than racing its registration.
    """
    baseline = len(daemon.subscribers)
    target = baseline + added
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(daemon.subscribers) >= target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"daemon did not add {added} subscriber(s) within {timeout}s "
        f"(baseline={baseline}, current={len(daemon.subscribers)})"
    )
