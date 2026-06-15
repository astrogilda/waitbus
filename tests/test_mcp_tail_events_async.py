"""Async-safety contract for the ``tail_events`` bounded long-poll.

``_tail_events_blocking`` can block up to ``max_wait_seconds`` (capped at
270s). The MCP ``_call_tool`` path runs it inside ``asyncio.to_thread``
so the server's single event loop is never frozen. These tests pin the
three load-bearing properties:

(a) the event loop stays responsive during a long tail_events wait
    (a concurrent coroutine still makes progress);
(b) the to_thread worker terminates BY the deadline (no leaked thread)
    -- await_predicate self-enforces its own deadline because it is
    built on the bounded select+deadline loop, not an unbounded recv;
(c) tail_events still returns the windowed rows.

Linux-only: the broadcast daemon's SO_PEERCRED check is Linux-only.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, cast

import pytest

from waitbus import broadcast, mcp
from waitbus._broadcast_sub import SubscriberHandle, open_subscriber

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)


def _insert(db: Path, delivery_id: str, **overrides: Any) -> None:
    import sqlite3

    from waitbus import _db
    from waitbus._types import EventInsert

    defaults: dict[str, Any] = {
        "source": "github",
        "event_type": "workflow_run",
        "owner": "test-owner",
        "repo": "test-repo",
        "received_at": time.time_ns(),
        "payload_json": "{}",
        "ingest_method": "webhook",
        "run_id": 1,
        "workflow_name": "Tests",
        "head_branch": "main",
        "head_sha": "abc",
        "status": "completed",
        "conclusion": "success",
    }
    defaults.update(overrides)
    with contextlib.closing(sqlite3.connect(str(db))) as conn:
        _db.insert_event(conn, EventInsert(delivery_id=delivery_id, **defaults))


@pytest.fixture
def _patched_socket(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Path]:
    """Point _tail_events_blocking's subscriber + db_path at the test daemon."""
    _daemon, paths = running_daemon
    monkeypatch.setattr("waitbus._paths.db_path", lambda: paths["db"])
    original_open = open_subscriber

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = str(paths["broadcast"])
        return original_open(**kwargs)

    monkeypatch.setattr(mcp, "open_subscriber", patched_open)
    return paths


@pytest.mark.asyncio
async def test_event_loop_responsive_during_long_tail_wait(
    _patched_socket: dict[str, Path],
) -> None:
    """A long (3s) tail_events wait dispatched via asyncio.to_thread must
    NOT freeze the event loop: a concurrent coroutine ticking every 50ms
    keeps making progress while the worker is blocked in await_predicate.

    The ``_patched_socket`` fixture wires the subscriber + db_path; this
    test does not need the returned paths directly.
    """
    ticks = 0

    async def heartbeat() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.05)
            ticks += 1

    hb = asyncio.create_task(heartbeat())
    try:
        # No matching rows -> blocks ~max_wait_seconds in the worker.
        worker = asyncio.create_task(
            asyncio.to_thread(
                mcp._tail_events_blocking,
                "test-owner/test-repo",
                None,
                100,
                3,
            )
        )
        # Give the worker time to actually enter the blocking wait.
        await asyncio.sleep(1.0)
        # If the loop were frozen, ticks would be ~0; it should be ~20.
        assert ticks >= 10, f"event loop was starved (ticks={ticks})"
        result = await asyncio.wait_for(worker, timeout=6.0)
        assert "events" in result
    finally:
        hb.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb


@pytest.mark.asyncio
async def test_worker_terminates_by_deadline_no_leak(
    _patched_socket: dict[str, Path],
) -> None:
    """With no matching frame the worker must terminate BY the deadline
    (await_predicate self-enforces it); assert wall-time bound and that
    no extra thread is left alive afterwards.
    """
    before = {t.ident for t in threading.enumerate()}
    start = time.monotonic()
    result = await asyncio.wait_for(
        asyncio.to_thread(
            mcp._tail_events_blocking,
            "test-owner/test-repo",
            None,
            100,
            1,  # 1s deadline
        ),
        # Generous outer bound: the inner deadline must fire first. If the
        # worker leaked we'd hit this and the inner self-enforcement claim
        # would be false.
        timeout=5.0,
    )
    elapsed = time.monotonic() - start
    assert elapsed < 3.0, f"worker overran its 1s deadline ({elapsed:.2f}s)"
    assert result["events"] == []

    # Let the to_thread executor reclaim the worker, then assert no net
    # thread growth attributable to a leaked await_predicate worker.
    await asyncio.sleep(0.2)
    after_alive = {t.ident for t in threading.enumerate() if t.is_alive()}
    leaked = after_alive - before - {threading.get_ident()}
    # asyncio's default thread-pool may keep idle workers; what must NOT
    # happen is an ever-growing set. One pooled idle thread is fine; a
    # thread still blocked in await_predicate would have failed the
    # wall-time assertion above.
    assert len(leaked) <= 1, f"unexpected lingering threads: {leaked}"


@pytest.mark.asyncio
async def test_tail_events_still_returns_windowed_rows(
    _patched_socket: dict[str, Path],
) -> None:
    """The wait only optimises latency: tail_events still returns the
    durable windowed rows. (a) immediate read when rows already exist;
    (b) a frame arriving mid-wait wakes the re-read.
    """
    paths = _patched_socket

    # (a) rows already present -> immediate return, no blocking.
    _insert(paths["db"], "d-pre-1")
    _insert(paths["db"], "d-pre-2")
    start = time.monotonic()
    result = await asyncio.to_thread(mcp._tail_events_blocking, "test-owner/test-repo", "", 100, 5)
    assert time.monotonic() - start < 1.0, "should not block when rows exist"
    assert len(result["events"]) == 2
    assert result["next_cursor"] is not None
    cursor = result["next_cursor"]

    # (b) empty window -> blocks, a new frame wakes it, re-read returns it.
    async def insert_after_delay() -> None:
        await asyncio.sleep(0.4)
        _insert(paths["db"], "d-late")

    inserter = asyncio.create_task(insert_after_delay())
    woke = await asyncio.to_thread(mcp._tail_events_blocking, "test-owner/test-repo", cursor, 100, 5)
    await inserter
    assert len(woke["events"]) == 1
    assert woke["events"][0]["delivery_id"] == "d-late"


@pytest.mark.asyncio
async def test_toctou_row_committed_in_read_subscribe_gap_is_not_lost(
    _patched_socket: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TOCTOU regression: a row committed in the window AFTER the
    first `_tail_events_read` returns empty but BEFORE `open_subscriber`
    + `await_predicate` subscribe must still be delivered.

    `_tail_events_blocking` does: one-shot read -> (empty) -> open_subscriber
    -> await_predicate -> durable re-read. If a producer commits a matching
    row in the read->subscribe gap, naive "wait only for a NEW live frame"
    designs lose it forever (the frame fired before the subscription
    existed). The fix is the daemon's `since=`-replay: on subscribe the
    daemon snapshots MAX(event_id) and replays rows in (since, snapshot],
    so a row already durable at subscribe time is replayed and the final
    re-read returns it.

    Determinism: rather than racing a sleep, this wraps `mcp.open_subscriber`
    so the gap row is committed on the exact call boundary -- immediately
    before the real subscribe/replay, guaranteed to be after the first
    empty read. No sleeps, no timing race.
    """
    paths = _patched_socket

    # Seed one row so there is a real `since_cursor` and the first
    # _tail_events_read above it returns an empty window.
    _insert(paths["db"], "d-seed")
    seed = await asyncio.to_thread(mcp._tail_events_blocking, "test-owner/test-repo", "", 100, 0)
    assert len(seed["events"]) == 1
    cursor = seed["next_cursor"]
    assert cursor is not None

    # Capture the fixture's already-patched subscriber (socket-redirected)
    # dynamically: a static `mcp.open_subscriber` reach is not an explicit
    # export, and we need the live patched callable, not the raw import.
    # Read via __dict__ (equivalent to getattr for a module) so neither
    # mypy's no-implicit-reexport nor ruff B009 is tripped.
    real_open: Any = mcp.__dict__["open_subscriber"]
    injected = {"done": False}

    def open_with_gap_commit(**kwargs: Any) -> socket.socket:
        # This runs strictly AFTER _tail_events_blocking's first
        # _tail_events_read returned empty and strictly BEFORE the daemon
        # registers the subscriber / runs since-replay. Commit the row
        # here to land it precisely in the TOCTOU window.
        if not injected["done"]:
            _insert(paths["db"], "d-gap")
            injected["done"] = True
        return cast("socket.socket", real_open(**kwargs))

    monkeypatch.setattr(mcp, "open_subscriber", open_with_gap_commit)

    result = await asyncio.to_thread(mcp._tail_events_blocking, "test-owner/test-repo", cursor, 100, 5)

    assert injected["done"], "open_subscriber wrapper did not run"
    # The gap row must survive the read->subscribe race via since-replay
    # waking the bounded wait, with the durable re-read returning it.
    assert len(result["events"]) == 1, (
        f"TOCTOU: row committed in the read->subscribe gap was lost (got {result['events']!r})"
    )
    assert result["events"][0]["delivery_id"] == "d-gap"
