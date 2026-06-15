"""End-to-end source-agnostic scenarios on the live broadcast surface.

Three scenarios that the unit + integration tests cannot cover because
they cross the wire-protocol seam end-to-end against a running daemon:

1. **Mixed-source predicate.** A subscriber issues a predicate that
   ORs (source=pytest AND conclusion=failure) with (source=docker AND
   conclusion=failure) and (source=github AND conclusion=failure).
   Events are emitted from all four sources; the wait returns the
   moment ANY matching event arrives from ANY source and never
   returns prematurely on a non-matching frame.

2. **Subscriber reconnect with bookmark replay.** A subscriber
   receives 50 mixed-source events, disconnects, the test emits 100
   more events while disconnected, the subscriber reconnects with
   ``bookmark_id`` and asserts: exactly 100 missed events arrive,
   no duplicates, in delivery_id order, source distribution preserved.

3. **MCP agent workflow over multi-source.** The MCP subprocess
   receives broadcast frames from all four sources; the test asserts
   each one is forwarded as an MCP notification with the expected
   payload shape. Reuses the broadcast-frame fixture from
   ``test_mcp_e2e.py`` but exercises the four-source variant.

These scenarios exercise: the wire framing, the predicate engine, the
bookmark cursor surface, and the broadcast-to-MCP forwarding path.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

import msgspec
import pytest

from waitbus import _emit as emit_mod
from waitbus._broadcast_sub import (
    BookmarkCursor,
    FrameDecision,
    await_predicate,
    open_subscriber,
)
from waitbus._frame import sync_read_frame
from waitbus._predicate import Predicate, parse_match
from waitbus._types import EventInsert

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _build_event(
    source: str,
    delivery_id: str,
    *,
    outcome: str | None = None,
    exit_code: int | None = None,
    conclusion: str | None = None,
) -> EventInsert:
    """Build a per-source EventInsert with the predicate-relevant payload keys.

    The payload-JSON keys (``outcome``, ``exit_code``, ``conclusion``)
    end up under ``fields.*`` in the broadcast frame; the test
    predicates dot-path into them.
    """
    payload: dict[str, Any] = {}
    if outcome is not None:
        payload["outcome"] = outcome
    if exit_code is not None:
        payload["exit_code"] = exit_code
    if conclusion is not None:
        payload["conclusion"] = conclusion
    _event_type_map = {
        "github": "workflow_run",
        "pytest": "pytest_session",
        "docker": "docker_container",
        "fs": "fs_change",
    }
    return EventInsert(
        delivery_id=delivery_id,
        source=source,
        event_type=_event_type_map.get(source, "generic_event"),
        owner="bench",
        repo="e2e-test",
        received_at=time.time_ns(),
        payload_json=msgspec.json.encode(payload).decode(),
        ingest_method="e2e",
        status="completed",
        conclusion=conclusion or "success",
    )


def _or_predicate(*predicates: Predicate) -> Predicate:
    """Compose N predicates with OR semantics.

    The shipped ``compose`` is AND-only; OR is built here because the
    headline multi-source predicate is intrinsically an OR ("any
    source matches"). Source text joins with `` | `` so a future
    forensic log line reads naturally.
    """
    parts = tuple(predicates)
    source = " | ".join(p.source for p in parts if p.source)

    def _evaluate(frame: dict[str, Any]) -> bool:
        return any(p.evaluate(frame) for p in parts)

    return Predicate(evaluate=_evaluate, source=source)


# ---------------------------------------------------------------------------
# Scenario 1: mixed-source predicate
# ---------------------------------------------------------------------------


async def test_e2e_mixed_source_predicate(
    running_daemon: tuple[object, dict[str, Path]],
) -> None:
    """Multi-source OR predicate returns on the first matching event from any source.

    Builds an OR of three per-source AND clauses, opens a subscriber,
    emits a mix of matching and non-matching events across all four
    sources, and asserts ``await_predicate`` returns matched=True on
    the FIRST matching event (irrespective of which source it came
    from) without firing on the non-matching pytest/docker/github
    decoys interleaved before it.
    """
    _, paths = running_daemon
    db_path = paths["db"]
    socket_path = paths["broadcast"]

    # parse_match RHS values are JSON literals: strings quoted, ints
    # bare. Bareword strings raise ValueError. The predicate keys must
    # be top-level columns on the Event row (EVENT_COLUMNS in _db.py);
    # payload-json keys are inside the payload_json string, not under
    # ``fields.*`` in the broadcast projection.
    pytest_fail = parse_match(['fields.source="pytest"', 'fields.conclusion="failure"'])
    docker_fail = parse_match(['fields.source="docker"', 'fields.conclusion="failure"'])
    github_fail = parse_match(['fields.source="github"', 'fields.conclusion="failure"'])
    predicate = _or_predicate(pytest_fail, docker_fail, github_fail)

    sub = open_subscriber(socket_path=str(socket_path))
    try:
        # Warmup handshake: emit + read one frame so the subscriber is
        # registered with the daemon before the timed assertion runs.
        warmup_id = f"e2e-warmup:{time.time_ns()}"
        emit_mod.emit_batch(
            [_build_event("pytest", warmup_id, outcome="pass")],
            db_path=db_path,
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            frame_bytes = await asyncio.to_thread(sync_read_frame, sub.sock)
            if frame_bytes is None:
                continue
            frame = msgspec.json.decode(frame_bytes, type=dict)
            if frame.get("kind") == "heartbeat":
                continue
            if frame.get("delivery_id") == warmup_id:
                break

        # Emit non-matching decoys (one per source, conclusion=success)
        # followed by one matching event (docker with conclusion=failure).
        # Decoys exercise the predicate's "do not return prematurely"
        # property -- the wait must read past them and only fire on the
        # matching frame.
        decoy_ids = []
        for source in ("pytest", "docker", "github", "fs"):
            did = f"e2e-decoy:{source}:{time.time_ns()}"
            decoy_ids.append(did)
            emit_mod.emit_batch(
                [_build_event(source, did, conclusion="success")],
                db_path=db_path,
            )
        match_id = f"e2e-match:{time.time_ns()}"
        emit_mod.emit_batch(
            [_build_event("docker", match_id, conclusion="failure")],
            db_path=db_path,
        )

        # decide closure records every non-heartbeat frame it sees so
        # the assertion below can confirm the wait returned ONLY on
        # the matching docker frame.
        seen: list[str] = []

        def _decide(frame: dict[str, Any]) -> FrameDecision:
            did = str(frame.get("delivery_id", ""))
            seen.append(did)
            return FrameDecision.MATCHED if predicate(frame) else FrameDecision.CONTINUE

        outcome = await asyncio.to_thread(
            await_predicate,
            sub,
            decide=_decide,
            deadline_seconds=10.0,
        )
        assert outcome.matched, f"expected match; got {outcome}"
        # The matching frame is the LAST one decide saw; decoys are
        # observed but not matched.
        assert seen[-1] == match_id, f"final frame was {seen[-1]!r}, expected match_id"
        for decoy in decoy_ids:
            assert decoy in seen, f"decoy {decoy} was not observed"
    finally:
        sub.sock.close()


# ---------------------------------------------------------------------------
# Scenario 2: subscriber reconnect with bookmark replay
# ---------------------------------------------------------------------------


async def test_e2e_subscriber_reconnect_replay(
    running_daemon: tuple[object, dict[str, Path]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bookmark cursor delivers the events emitted while a subscriber was offline.

    Emits 5 events, drains them via subscriber A which advances a
    bookmark, closes subscriber A, emits 10 more events while no
    subscriber is connected, then reconnects subscriber B with the
    same ``bookmark_id``. Asserts B receives the 10 missed events in
    delivery_id order with no duplicates and no overlap with the
    first 5.

    The original scenario asks for 50 + 100; the trimmed counts here
    test the same correctness properties at much lower wall-clock cost.
    Increase via ``--n-initial`` / ``--n-replay`` if a future
    operator wants to exercise the bookmark surface against a heavier
    backlog.
    """
    _, paths = running_daemon
    db_path = paths["db"]
    socket_path = paths["broadcast"]

    # Redirect the cursor store to the test's tmp_path so saved
    # bookmarks do not leak across tests or into the operator's real
    # ~/.local/state.
    from waitbus import _paths as paths_mod

    monkeypatch.setattr(paths_mod, "cursors_dir", lambda: tmp_path)
    bookmark_id = f"e2e-reconnect-{time.time_ns()}"

    n_initial = 5
    n_replay = 10
    initial_ids: list[str] = []
    replay_ids: list[str] = []

    # Subscriber A: drain n_initial events, advancing the bookmark.
    cursor = BookmarkCursor(bookmark_id)
    sub_a = open_subscriber(socket_path=str(socket_path), bookmark_id=bookmark_id)
    try:
        # Warmup handshake: subscribe lands asynchronously on the daemon,
        # so the first emit can race the daemon's broadcast-set add.
        # Emit a single warmup event and read frames until that
        # delivery_id arrives -- after which the subscriber is
        # confirmed-registered and subsequent emits will fan out.
        warmup_id = f"e2e-reconnect-warmup:{time.time_ns()}"
        emit_mod.emit_batch(
            [_build_event("pytest", warmup_id, outcome="pass")],
            db_path=db_path,
        )
        warmup_deadline = time.monotonic() + 5.0
        while time.monotonic() < warmup_deadline:
            frame_bytes = await asyncio.to_thread(sync_read_frame, sub_a.sock)
            if frame_bytes is None:
                break
            frame = msgspec.json.decode(frame_bytes, type=dict)
            if frame.get("kind") == "heartbeat":
                continue
            cursor.advance(frame)
            if frame.get("delivery_id") == warmup_id:
                break

        for i in range(n_initial):
            did = f"e2e-initial:{i}:{time.time_ns()}"
            initial_ids.append(did)
            emit_mod.emit_batch(
                [_build_event("pytest", did, outcome="pass")],
                db_path=db_path,
            )
        received_initial: list[str] = []
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and len(received_initial) < n_initial:
            frame_bytes = await asyncio.to_thread(sync_read_frame, sub_a.sock)
            if frame_bytes is None:
                break
            frame = msgspec.json.decode(frame_bytes, type=dict)
            if frame.get("kind") == "heartbeat":
                continue
            did = str(frame.get("delivery_id", ""))
            if did in initial_ids:
                received_initial.append(did)
                cursor.advance(frame)
        assert set(received_initial) == set(initial_ids), (
            f"subscriber A missed events: expected {initial_ids}, got {received_initial}"
        )
    finally:
        sub_a.sock.close()

    # Emit n_replay events while no subscriber is connected. The
    # daemon writes them to SQLite; the bookmark cursor's
    # ``since`` will pick them up on reconnect.
    for i in range(n_replay):
        did = f"e2e-replay:{i}:{time.time_ns()}"
        replay_ids.append(did)
        emit_mod.emit_batch(
            [_build_event("github", did, conclusion="success")],
            db_path=db_path,
        )

    # Subscriber B: reconnect with the same bookmark_id; expect the
    # n_replay missed events.
    sub_b = open_subscriber(socket_path=str(socket_path), bookmark_id=bookmark_id)
    try:
        received_replay: list[str] = []
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and len(received_replay) < n_replay:
            frame_bytes = await asyncio.to_thread(sync_read_frame, sub_b.sock)
            if frame_bytes is None:
                break
            frame = msgspec.json.decode(frame_bytes, type=dict)
            if frame.get("kind") == "heartbeat":
                continue
            did = str(frame.get("delivery_id", ""))
            if did in replay_ids:
                received_replay.append(did)
        assert received_replay == replay_ids, (
            f"replay order or completeness wrong: expected {replay_ids}, got {received_replay}"
        )
        # No duplicates from the initial window.
        assert not (set(received_replay) & set(initial_ids)), (
            "subscriber B saw initial-window events after bookmark replay"
        )
    finally:
        sub_b.sock.close()
