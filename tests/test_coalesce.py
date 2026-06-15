"""Contract tests for ``waitbus replay --coalesce`` / ``coalesce_replay``.

The coalesced delivery mode is a strictly client-side projection over
the existing broadcast wire: it consumes the same length-prefix frames
the daemon would emit, runs a latest-per-entity fold over the backlog
window, and re-emits the collapsed snapshot in event_id order followed
(optionally) by a faithful live tail.

These tests drive ``coalesce_replay`` over a deterministic
``socket.socketpair()`` rather than a real daemon: one socket end
writes framed JSON exactly as the daemon's ``_row_to_frame`` would, the
other is the subscriber end the consumer reads from. No timing races
beyond a short idle window (matches the operator-replay shape: send
the backlog, then go quiet -> consumer exits ``timed_out``).
"""

from __future__ import annotations

import contextlib
import json
import socket
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from _subscriber_harness import drive_sync_engine

from waitbus._broadcast_sub import (
    BookmarkCursor,
    SubscriberHandle,
    WaitOutcome,
)
from waitbus._frame import encode_frame
from waitbus.coalesce import coalesce_replay


def _frame_bytes(**fields: Any) -> bytes:
    """Encode one daemon-shape frame as length-prefixed JSON bytes."""
    return encode_frame(json.dumps(fields, separators=(",", ":")).encode("utf-8"))


def _github_run(
    event_id: str,
    run_id: int,
    *,
    conclusion: str | None,
    owner: str = "acme",
    repo: str = "widgets",
) -> bytes:
    """A GitHub workflow_run wire frame (the daemon's _row_to_frame shape)."""
    return _frame_bytes(
        event_id=event_id,
        kind="event",
        event_type="workflow_run",
        owner=owner,
        repo=repo,
        received_at=0,
        delivery_id=f"gh:{event_id}",
        summary="",
        fields={
            "source": "github",
            "event_type": "workflow_run",
            "run_id": run_id,
            "conclusion": conclusion,
            "status": "completed" if conclusion else "in_progress",
        },
    )


def _pytest_frame(event_id: str, nodeid: str) -> bytes:
    """A non-CI source frame -- expected pass-through (never collapsed)."""
    return _frame_bytes(
        event_id=event_id,
        kind="event",
        event_type="fs_change",
        owner="local",
        repo="lab",
        received_at=0,
        delivery_id=f"py:{nodeid}",
        summary="",
        fields={"source": "pytest", "event_type": "fs_change"},
    )


def _drive(
    bytes_to_send: list[bytes],
    *,
    cursor: BookmarkCursor | None = None,
    idle_seconds: float = 0.4,
) -> tuple[WaitOutcome, list[dict[str, Any]]]:
    """Run ``coalesce_replay`` against a socketpair pre-loaded with frames."""
    # `drive_sync_engine` lives in `_subscriber_harness`, a bare-module
    # test helper that the strict wide-scope mypy treats as Any-returning
    # (see the pyproject ``[[tool.mypy.overrides]]`` entry).  The
    # value-level annotation pins the declared return shape on the
    # assignment so the wide-scope pass does not raise no-any-return
    # without confusing project-default mypy (which infers the precise
    # return type from the helper's real signature and would flag a
    # cast as redundant).
    result: tuple[WaitOutcome, list[dict[str, Any]]] = drive_sync_engine(
        bytes_to_send,
        engine=lambda sub, emit: coalesce_replay(
            sub,
            emit=emit,
            idle_seconds=idle_seconds,
            cursor=cursor,
            live_tail=False,
        ),
        idle_seconds_extra=5.0,
        deadline_seconds=idle_seconds,
    )
    return result


# --- collapse + version-guard -----------------------------------------------


def test_coalesce_collapses_run_to_latest_event_id() -> None:
    """Four workflow_run frames for one run_id collapse to the last."""
    outcome, emitted = _drive(
        [
            _github_run("01HZ0000000000000000000001", run_id=42, conclusion=None),
            _github_run("01HZ0000000000000000000002", run_id=42, conclusion=None),
            _github_run("01HZ0000000000000000000003", run_id=42, conclusion=None),
            _github_run("01HZ0000000000000000000004", run_id=42, conclusion="success"),
        ],
    )
    assert outcome.timed_out is True
    assert len(emitted) == 1
    assert emitted[0]["event_id"] == "01HZ0000000000000000000004"
    assert emitted[0]["fields"]["conclusion"] == "success"


def test_coalesce_keeps_failure_after_rerun_regardless_of_arrival_order() -> None:
    """success at eid_lo, then re-run failure at eid_hi -> keep failure
    (the documented success -> re-run -> failure regression guard)."""
    outcome, emitted = _drive(
        [
            _github_run("01HZ0000000000000000000010", run_id=7, conclusion="success"),
            _github_run("01HZ0000000000000000000011", run_id=7, conclusion="failure"),
        ],
    )
    assert outcome.timed_out is True
    assert len(emitted) == 1
    assert emitted[0]["fields"]["conclusion"] == "failure"
    assert emitted[0]["event_id"] == "01HZ0000000000000000000011"


def test_coalesce_out_of_order_arrival_still_keeps_max_event_id() -> None:
    """The regression guard is strictly event_id-keyed, not arrival-keyed:
    a stale 'success' arriving AFTER a higher-event_id 'failure' is
    discarded (not allowed to overwrite the newer state)."""
    outcome, emitted = _drive(
        [
            _github_run("01HZ0000000000000000000021", run_id=11, conclusion="failure"),
            _github_run("01HZ0000000000000000000020", run_id=11, conclusion="success"),
        ],
    )
    assert outcome.timed_out is True
    assert len(emitted) == 1
    assert emitted[0]["fields"]["conclusion"] == "failure"


# --- pass-through (non-CI sources) ----------------------------------------


def test_coalesce_passes_non_ci_sources_through_verbatim() -> None:
    """A mixed backlog: github frames collapse, pytest frames stay
    verbatim, all emitted in monotonic event_id order."""
    outcome, emitted = _drive(
        [
            _github_run("01HZ0000000000000000000030", run_id=99, conclusion=None),
            _pytest_frame("01HZ0000000000000000000031", nodeid="t_a"),
            _github_run("01HZ0000000000000000000032", run_id=99, conclusion="success"),
            _pytest_frame("01HZ0000000000000000000033", nodeid="t_b"),
        ],
    )
    assert outcome.timed_out is True
    # 1 collapsed run + 2 pytest pass-through = 3
    assert len(emitted) == 3
    assert [f["event_id"] for f in emitted] == [
        "01HZ0000000000000000000031",
        "01HZ0000000000000000000032",
        "01HZ0000000000000000000033",
    ], "emitted order must be monotonic in event_id"


def test_coalesce_alertmanager_collapses_on_fingerprint() -> None:
    """Three alerts on one fingerprint (firing, firing, resolved) ->
    one emitted frame: the resolved one (the terminal state at the
    highest event_id)."""
    fp = "fp-svc-down"

    def alert(eid: str, status: str) -> bytes:
        return _frame_bytes(
            event_id=eid,
            kind="event",
            event_type="prometheus_alert",
            owner="prom",
            repo="prod",
            received_at=0,
            delivery_id=f"al:{eid}",
            summary="",
            fields={
                "source": "alertmanager",
                "event_type": "prometheus_alert",
                "alert_fingerprint": fp,
                "status": status,
            },
        )

    outcome, emitted = _drive(
        [
            alert("01HZ0000000000000000000040", "firing"),
            alert("01HZ0000000000000000000041", "firing"),
            alert("01HZ0000000000000000000042", "resolved"),
        ],
    )
    assert outcome.timed_out is True
    assert len(emitted) == 1
    assert emitted[0]["fields"]["status"] == "resolved"


# --- empty backlog + EOF + bookmark advancement ---------------------------


def test_coalesce_empty_backlog_emits_nothing_and_times_out() -> None:
    """An empty backlog: phase 1 goes idle immediately, nothing flushes."""
    outcome, emitted = _drive([])
    assert outcome.timed_out is True
    assert emitted == []


def test_coalesce_eof_mid_backlog_still_flushes_what_was_seen() -> None:
    """A peer that closes mid-backlog: phase 1 returns peer_closed, the
    accumulated snapshot is still flushed (no silent data loss)."""
    outcome, emitted = drive_sync_engine(
        [
            _github_run("01HZ0000000000000000000050", run_id=5, conclusion=None),
            _github_run("01HZ0000000000000000000051", run_id=5, conclusion="success"),
        ],
        engine=lambda sub, emit: coalesce_replay(
            sub,
            emit=emit,
            idle_seconds=1.0,
            cursor=None,
            live_tail=False,
        ),
        close_server_before_engine=True,
    )
    assert outcome.peer_closed is True
    # The two frames collapse to one (latest = success).
    assert len(emitted) == 1
    assert emitted[0]["fields"]["conclusion"] == "success"


def test_coalesce_bookmark_advances_only_at_flush(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The bookmark must record the LAST FLUSHED event_id, NOT any
    intermediate (superseded) event_id seen during accumulation. This
    is the resume-safety contract: a crash mid-accumulation re-pulls
    the entire window from the daemon on next resume."""
    # Redirect BookmarkCursor's storage to a temp dir.
    monkeypatch.setattr("waitbus._broadcast_sub.cursors_dir", lambda: tmp_path)
    cursor = BookmarkCursor("c14-test")

    outcome, emitted = _drive(
        [
            _github_run("01HZ0000000000000000000060", run_id=3, conclusion=None),
            _github_run("01HZ0000000000000000000061", run_id=3, conclusion=None),
            _github_run("01HZ0000000000000000000062", run_id=3, conclusion="success"),
        ],
        cursor=cursor,
    )
    assert outcome.timed_out is True
    assert len(emitted) == 1
    # Bookmark advanced to the FLUSHED event_id (the latest), not any
    # intermediate accumulator value.
    assert cursor.current == "01HZ0000000000000000000062"


def test_coalesce_default_replay_path_is_unchanged_by_this_module() -> None:
    """Belt-and-suspenders: ``coalesce_replay`` does NOT touch the
    socket lifecycle (mirrors await_predicate). A caller's faithful
    path is unaffected by this module's mere existence."""

    # No frames; immediate close -> peer_closed.
    # The socket-not-closed assertion runs inside the engine so it executes
    # before the harness's finally block closes the client socket.
    def _engine(sub: SubscriberHandle, emit: Any) -> WaitOutcome:
        outcome = coalesce_replay(
            sub,
            emit=emit,
            idle_seconds=0.2,
            cursor=None,
            live_tail=False,
        )
        # Socket NOT closed by coalesce_replay -- caller still owns it.
        assert sub.sock.fileno() != -1
        return outcome

    outcome, emitted = drive_sync_engine(
        [],
        engine=_engine,
        close_server_before_engine=True,
    )
    assert outcome.peer_closed is True
    assert emitted == []


# --- _event_id hard-rejects malformed frames ------------------------------


def test_event_id_raises_on_missing_id() -> None:
    """A frame missing the ``event_id`` field surfaces a wire-corruption bug."""
    from waitbus.coalesce import _event_id

    with pytest.raises(ValueError, match="missing or non-string 'event_id'"):
        _event_id({"event_type": "workflow_run"})


def test_event_id_raises_on_empty_id() -> None:
    """Empty-string event_id is treated as malformed (same as missing)."""
    from waitbus.coalesce import _event_id

    with pytest.raises(ValueError, match="missing or non-string 'event_id'"):
        _event_id({"event_id": ""})


def test_event_id_raises_on_non_string_id() -> None:
    """Non-string event_id (e.g. int 42) surfaces as a wire-corruption bug."""
    from waitbus.coalesce import _event_id

    with pytest.raises(ValueError, match="missing or non-string 'event_id'"):
        _event_id({"event_id": 42})


def test_event_id_accepts_valid_ulid() -> None:
    """Sanity: a well-formed ULID string round-trips unchanged."""
    from waitbus.coalesce import _event_id

    assert _event_id({"event_id": "01HZ0000000000000000000001"}) == "01HZ0000000000000000000001"


# --- live_tail=True coverage ----------------------------------------------


def test_coalesce_live_tail_true_drains_snapshot_then_continues() -> None:
    """live_tail=True: phase 1 collapses the backlog, phase 2 flushes,
    phase 3 await_predicate-as-tail continues until a subsequently-
    injected live frame is received."""
    server, client = socket.socketpair()
    try:
        # Pre-load 2 backlog frames for run_id=1 (collapse to the second)
        server.sendall(_github_run("01HZ0000000000000000000001", run_id=1, conclusion=None))
        server.sendall(_github_run("01HZ0000000000000000000002", run_id=1, conclusion="success"))

        emitted: list[dict[str, Any]] = []

        def _run() -> WaitOutcome:
            return coalesce_replay(
                SubscriberHandle(sock=client),
                emit=emitted.append,
                idle_seconds=0.4,
                cursor=None,
                live_tail=True,
            )

        outcome_holder: list[WaitOutcome] = []
        t = threading.Thread(
            target=lambda: outcome_holder.append(_run()),
            daemon=True,
        )
        t.start()

        # Wait for phase-1/2 to complete (idle window + a small margin).
        time.sleep(1.0)
        # Snapshot must have flushed: 1 collapsed run frame.
        assert len(emitted) == 1
        assert emitted[0]["event_id"] == "01HZ0000000000000000000002"

        # Inject a live tail frame (run_id=2). Phase 3 must emit it
        # verbatim (no coalescing in the live tail).
        server.sendall(_github_run("01HZ0000000000000000000010", run_id=2, conclusion=None))
        # Then close the server side so phase 3's await_predicate exits
        # via peer_closed (since deadline_seconds=None, this is the only
        # terminus).
        time.sleep(0.5)
        server.close()
        t.join(timeout=5.0)
        assert not t.is_alive()

        # Live frame was received in phase 3.
        assert len(emitted) >= 2
        assert any(f["event_id"] == "01HZ0000000000000000000010" for f in emitted)
        assert outcome_holder[0].peer_closed is True
    finally:
        client.close()
        with contextlib.suppress(OSError):
            server.close()


def test_coalesce_live_tail_true_sigint_during_phase3_returns_cancelled() -> None:
    """live_tail=True: a SIGINT during phase 3 returns outcome.cancelled.

    Simulated via a decide-callback that raises KeyboardInterrupt on
    the first live frame after the snapshot flushes — not a real signal
    (the test runner shouldn't be interrupted) but it tests the same
    code path in ``await_predicate`` that real SIGINT would hit. Per
    the documented contract, ``await_predicate`` translates a SIGINT
    or KeyboardInterrupt in the emit callback to ``outcome.cancelled
    = True`` rather than letting it bubble up.
    """
    server, client = socket.socketpair()
    try:
        # Pre-load one snapshot frame.
        server.sendall(_github_run("01HZ0000000000000000000050", run_id=5, conclusion="success"))

        emitted: list[dict[str, Any]] = []
        injected = threading.Event()

        def _emit(frame: dict[str, Any]) -> None:
            emitted.append(frame)
            if len(emitted) >= 2:
                # Phase-3 live frame arrived; simulate Ctrl-C.
                injected.set()
                raise KeyboardInterrupt

        outcome_holder: list[WaitOutcome] = []

        def _run() -> None:
            outcome_holder.append(
                coalesce_replay(
                    SubscriberHandle(sock=client),
                    emit=_emit,
                    idle_seconds=0.4,
                    cursor=None,
                    live_tail=True,
                )
            )

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        # Wait for phase 1+2 to flush (1 frame emitted).
        time.sleep(1.0)
        assert len(emitted) == 1
        # Inject a phase-3 live frame to trigger the emit-raises path.
        server.sendall(_github_run("01HZ0000000000000000000051", run_id=6, conclusion=None))
        t.join(timeout=5.0)
        assert not t.is_alive()
        assert injected.is_set()
        # await_predicate translates the KeyboardInterrupt to
        # outcome.cancelled = True; it does NOT let it bubble up.
        assert outcome_holder[0].cancelled is True
        assert outcome_holder[0].peer_closed is False
    finally:
        client.close()
        with contextlib.suppress(OSError):
            server.close()


# --- Peer-closed partial-flush hazard -------------------------------------


def test_coalesce_peer_closed_mid_backlog_flushes_partial_snapshot() -> None:
    """When phase 1 returns peer_closed mid-backlog (the daemon FINs
    before idle), the partial snapshot still flushes and the cursor
    advances past entities whose latest frame the daemon had not yet
    sent.

    Forward-only version-guard makes this acceptable (a future resume
    can only get the same-or-newer state, never a regression) — this
    test pins the documented degradation.
    """
    cursor = BookmarkCursor(name="test-peer-closed-partial")
    server, client = socket.socketpair()
    try:
        # Pre-load partial state for TWO entities: run_id=1 gets a
        # complete pair (queued + success); run_id=2 gets only one frame
        # (queued, no terminal state) before the daemon FINs.
        server.sendall(_github_run("01HZ0000000000000000000060", run_id=1, conclusion=None))
        server.sendall(_github_run("01HZ0000000000000000000061", run_id=1, conclusion="success"))
        server.sendall(_github_run("01HZ0000000000000000000062", run_id=2, conclusion=None))
        # Close immediately — daemon FIN before idle window expires.
        server.close()

        emitted: list[dict[str, Any]] = []
        outcome = coalesce_replay(
            SubscriberHandle(sock=client),
            emit=emitted.append,
            idle_seconds=0.4,
            cursor=cursor,
            live_tail=False,
        )

        # phase 1 returned peer_closed (the FIN); phase 2 still flushed
        # the partial snapshot of BOTH entities (run_id=1's terminal
        # success and run_id=2's queued-only frame).
        assert outcome.peer_closed is True
        assert len(emitted) == 2
        ids = sorted(f["event_id"] for f in emitted)
        assert ids == [
            "01HZ0000000000000000000061",  # run_id=1 latest (success)
            "01HZ0000000000000000000062",  # run_id=2 only-seen (queued)
        ]
        # Cursor advanced PAST run_id=2's queued frame, even though the
        # daemon hadn't sent its terminal frame. Acceptable per the
        # forward-only version-guard: a future resume can only see a
        # newer state for run_id=2.
        assert cursor.current == "01HZ0000000000000000000062"
    finally:
        client.close()
