"""Characterization gate for the three divergent subscriber loops.

Pins the EXACT current observable contract of:

- ``replay._stream_until_idle``  (replay.py)
- ``broadcast_tap._tap``         (broadcast_tap.py)
- ``read_events.watch``          (read_events.py)

before they are converged onto the shared ``await_predicate`` engine.

Post-convergence these assertions MUST still hold *unchanged* EXCEPT for
the explicitly documented ``watch`` behaviour deltas (see the module
docstring of ``waitbus._broadcast_sub`` / the test markers below):

  D1. ``--watch`` now flows the standard ``open_subscriber`` token path
      (a configured broadcast token is sent on the subscribe frame).
  D2. ``--watch`` resumes via the unified ``BookmarkCursor`` model; the
      bespoke ``(owner,repo)``-keyed ``_save_cursor`` / ``_load_cursor`` /
      ``_cursor_path`` model is DELETED.

Tests covering D1/D2 are marked ``watch_delta`` so the post-refactor
diff is auditable: a ``watch_delta`` test is *expected* to be rewritten
when convergence lands; every other test in this file must stay green
verbatim.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from waitbus import _db, broadcast, broadcast_tap, read_events, replay
from waitbus._broadcast_sub import SubscriberHandle, open_subscriber
from waitbus._types import EventInsert

_DaemonPaths = tuple[broadcast.Broadcast, dict[str, Path]]

# Every loop here connects to the AF_UNIX SOCK_STREAM broadcast daemon
# (SO_PEERCRED + systemd socket activation = Linux-only).
pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)


def _event_stub(delivery_id: str, **overrides: Any) -> EventInsert:
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
    return EventInsert(delivery_id=delivery_id, **defaults)


def _insert(db: Path, delivery_id: str, **overrides: Any) -> None:
    with contextlib.closing(sqlite3.connect(str(db))) as conn:
        _db.insert_event(conn, _event_stub(delivery_id, **overrides))


def _patched_open(paths: dict[str, Path]) -> Any:
    original = open_subscriber

    def _open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = str(paths["broadcast"])
        return original(**kwargs)

    return _open


# ---------------------------------------------------------------------------
# replay._stream_until_idle — observable contract
# ---------------------------------------------------------------------------


def test_replay_invalid_ulid_exits_2() -> None:
    """Malformed ULID -> startup failure exit 2 (no socket touched)."""
    assert replay.main(["not-a-ulid"]) == 2


def test_replay_no_cursor_exits_2(capsys: pytest.CaptureFixture[str]) -> None:
    """No SINCE_ULID and no --bookmark -> exit 2 with operator hint."""
    rc = replay.main([])
    assert rc == 2
    err = capsys.readouterr().err
    assert "a cursor is required" in err


def test_replay_no_daemon_exits_2(tmp_path: Path) -> None:
    """Daemon socket absent -> startup failure exit 2."""
    absent = str(tmp_path / "no.sock")
    original = open_subscriber

    def _open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = absent
        return original(**kwargs)

    with patch.object(replay, "open_subscriber", _open):
        assert replay.main(["01JZ0ABC1230EF456GHJ789KMN"]) == 2


@pytest.mark.asyncio
async def test_replay_idle_timeout_exits_0_with_summary(
    running_daemon: _DaemonPaths,
) -> None:
    """No frames within --timeout -> typer.Exit(0) + 'caught up' stderr."""
    _daemon, paths = running_daemon
    rc_holder: list[int] = []
    err_buf = io.StringIO()

    def run() -> None:
        with (
            patch.object(replay, "open_subscriber", _patched_open(paths)),
            patch("sys.stderr", err_buf),
        ):
            # Future ULID: nothing matches; short timeout keeps it fast.
            rc_holder.append(replay.main(["7ZZZZZZZZZZZZZZZZZZZZZZZZZ", "--timeout", "0.5"]))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive(), "replay did not exit on idle timeout"
    assert rc_holder == [0]
    assert "caught up" in err_buf.getvalue()


@pytest.mark.asyncio
async def test_replay_delivers_historical_then_idle_exits_0(
    running_daemon: _DaemonPaths,
) -> None:
    """Historical replay frame emitted, then idle -> exit 0, frame on stdout."""
    _daemon, paths = running_daemon
    _insert(paths["db"], "d-replay-hist")
    await asyncio.sleep(0.05)
    rc_holder: list[int] = []
    out_buf = io.StringIO()

    def run() -> None:
        with (
            patch.object(replay, "open_subscriber", _patched_open(paths)),
            patch("sys.stdout", out_buf),
        ):
            rc_holder.append(replay.main(["00000000000000000000000000", "--timeout", "1.0"]))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive()
    assert rc_holder == [0]
    assert "d-replay-hist" in out_buf.getvalue()


@pytest.mark.asyncio
async def test_replay_peer_close_exits_0(running_daemon: _DaemonPaths) -> None:
    """Daemon closing the connection -> clean exit 0 (EOF == caught up)."""
    daemon, paths = running_daemon
    rc_holder: list[int] = []

    def run() -> None:
        with patch.object(replay, "open_subscriber", _patched_open(paths)):
            rc_holder.append(replay.main(["00000000000000000000000000", "--timeout", "30"]))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await asyncio.sleep(0.3)  # let subscribe register
    for fd in list(daemon.subscribers):
        daemon._close_subscriber(fd, reason="characterization_eof")
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive(), "replay did not exit on peer close"
    assert rc_holder == [0]


# ---------------------------------------------------------------------------
# broadcast_tap._tap — observable contract
# ---------------------------------------------------------------------------


def test_tap_no_daemon_exits_2(tmp_path: Path) -> None:
    """Daemon socket absent -> startup failure exit 2."""
    absent = str(tmp_path / "no.sock")
    original = open_subscriber

    def _open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = absent
        return original(**kwargs)

    with patch.object(broadcast_tap, "open_subscriber", _open):
        assert broadcast_tap.main([]) == 2


@pytest.mark.asyncio
async def test_tap_count_exits_0_after_n_frames(
    running_daemon: _DaemonPaths,
) -> None:
    """--count N exits 0 after exactly N non-... frames + summary stderr."""
    _daemon, paths = running_daemon
    rc_holder: list[int] = []
    err_buf = io.StringIO()

    def run() -> None:
        with (
            patch.object(broadcast_tap, "open_subscriber", _patched_open(paths)),
            patch("sys.stderr", err_buf),
        ):
            rc_holder.append(broadcast_tap.main(["--count", "1", "--json"]))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await asyncio.sleep(0.15)
    _insert(paths["db"], "d-tap-count")
    deadline = time.monotonic() + 3.0
    while t.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    assert not t.is_alive(), "tap did not exit after --count 1"
    assert rc_holder == [0]
    assert "received 1 frame(s); exiting" in err_buf.getvalue()


@pytest.mark.asyncio
async def test_tap_peer_close_exits_0(running_daemon: _DaemonPaths) -> None:
    """Daemon closing the connection -> clean exit 0."""
    daemon, paths = running_daemon
    rc_holder: list[int] = []

    def run() -> None:
        with patch.object(broadcast_tap, "open_subscriber", _patched_open(paths)):
            rc_holder.append(broadcast_tap.main([]))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await asyncio.sleep(0.3)
    for fd in list(daemon.subscribers):
        daemon._close_subscriber(fd, reason="characterization_eof")
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive(), "tap did not exit on peer close"
    assert rc_holder == [0]


# ---------------------------------------------------------------------------
# read_events.watch — observable contract (int returns + summary stdout)
# ---------------------------------------------------------------------------


def test_watch_returns_2_when_socket_missing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Socket absent -> int return 2 + 'broadcast socket not bound' stderr."""
    rc = read_events.watch(
        filters=["*"],
        event_types=None,
        since=None,
        cursor=None,
        socket_path=tmp_path / "no-such-socket",
    )
    assert rc == 2
    assert "broadcast socket" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_watch_clean_eof_returns_0(running_daemon: _DaemonPaths) -> None:
    """Daemon closing the connection -> int return 0 (Monitor re-arm)."""
    daemon, paths = running_daemon
    rc_holder: list[int] = []

    def run() -> None:
        rc_holder.append(
            read_events.watch(
                filters=["*"],
                event_types=None,
                since=None,
                cursor=None,
                socket_path=paths["broadcast"],
            )
        )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await asyncio.sleep(0.3)
    for fd in list(daemon.subscribers):
        daemon._close_subscriber(fd, reason="characterization_eof")
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive(), "watch did not return on clean EOF"
    assert rc_holder == [0]


@pytest.mark.asyncio
async def test_watch_prints_one_summary_line_per_event(
    running_daemon: _DaemonPaths,
) -> None:
    """Each matching non-heartbeat frame -> exactly one stdout summary line."""
    daemon, paths = running_daemon
    out_buf = io.StringIO()

    def run() -> None:
        with patch("sys.stdout", out_buf):
            read_events.watch(
                filters=["test-owner/test-repo"],
                event_types=None,
                since=None,
                cursor=None,
                socket_path=paths["broadcast"],
            )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await asyncio.sleep(0.15)
    _insert(paths["db"], "d-w1")
    _insert(paths["db"], "d-w2", owner="other", repo="other")  # filtered out
    _insert(paths["db"], "d-w3")
    await asyncio.sleep(0.4)
    for fd in list(daemon.subscribers):
        daemon._close_subscriber(fd, reason="characterization_eof")
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive()
    lines = [ln for ln in out_buf.getvalue().splitlines() if ln.strip()]
    # Two matching events -> two summary lines; the other/other row filtered.
    assert len(lines) == 2, lines
    assert all("main" in ln for ln in lines), lines


@pytest.mark.watch_delta
@pytest.mark.asyncio
async def test_watch_cursor_persistence_delta(
    running_daemon: _DaemonPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DELTA D2 (post-refactor): ``--watch`` resume is now the unified
    ``BookmarkCursor`` under the ``watch-<owner>-<repo>`` name. The
    bespoke ``(owner,repo)``-keyed cursor-file model is DELETED.

    Pins the *new* contract so the documented behaviour delta is
    asserted, not silent.
    """
    from waitbus._broadcast_sub import BookmarkCursor

    daemon, paths = running_daemon
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state"))
    bookmark = read_events.watch_bookmark_name("o", "r")
    cursor = BookmarkCursor(bookmark)

    def run() -> None:
        read_events.watch(
            filters=["o/r"],
            event_types=None,
            since=None,
            cursor=BookmarkCursor(bookmark),
            socket_path=paths["broadcast"],
        )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await asyncio.sleep(0.15)
    _insert(paths["db"], "d-cur", owner="o", repo="r")
    await asyncio.sleep(0.3)
    for fd in list(daemon.subscribers):
        daemon._close_subscriber(fd, reason="characterization_eof")
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive()
    # Unified BookmarkCursor persisted a 26-char ULID resume token.
    persisted = cursor.load()
    assert persisted is not None and len(persisted) == 26


@pytest.mark.watch_delta
def test_watch_bespoke_cursor_api_deleted_delta() -> None:
    """DELTA D2 (post-refactor): the bespoke
    ``_save_cursor`` / ``_load_cursor`` / ``_cursor_path`` API is
    DELETED. ``read_events`` exposes only ``watch_bookmark_name`` for
    the unified ``BookmarkCursor`` model.

    Asserts the deletion explicitly so the API removal is an auditable
    documented delta, not an accidental regression.
    """
    assert not hasattr(read_events, "_save_cursor")
    assert not hasattr(read_events, "_load_cursor")
    assert not hasattr(read_events, "_cursor_path")
    # The replacement is the unified bookmark-name derivation.
    assert read_events.watch_bookmark_name("acme", "widgets") == "watch-acme-widgets"
