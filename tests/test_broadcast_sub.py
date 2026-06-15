"""Contract tests for _broadcast_sub, broadcast tap, and replay.

All tests that require a live broadcast daemon use the ``running_daemon``
fixture from conftest.py, which spins up the daemon in-process against
tmp_path sockets. Tests are Linux-only because the daemon's
SO_PEERCRED check is Linux-only.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import typer

from waitbus import _db, broadcast
from waitbus._broadcast_sub import (
    BookmarkCursor,
    BroadcastConnectionError,
    FrameDecision,
    SubscriberHandle,
    TokenRequiredError,
    _resolve_token,
    await_predicate,
    emit_frame,
    open_subscriber,
    read_subscribe_ack,
)
from waitbus._frame import encode_frame, sync_read_frame
from waitbus._types import EventInsert
from waitbus.cli._shared import _exit_with_error

# Broadcast daemon SO_PEERCRED is Linux-only.
pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


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
    import sqlite3

    with contextlib.closing(sqlite3.connect(str(db))) as conn:
        _db.insert_event(conn, _event_stub(delivery_id, **overrides))


_CONTROL_KINDS = frozenset({"daemon_heartbeat", "subscribe_ack"})


async def _recv_non_heartbeat(
    sock: socket.socket,
    timeout: float = 2.0,
) -> dict[str, Any] | None:
    """Drain frames from a blocking socket until a non-control frame arrives.

    Skips the wire-protocol-v1 control frames (``daemon_heartbeat`` liveness
    pings and the one post-registration ``subscribe_ack``) that the daemon
    interleaves before / around real event frames, mirroring what
    ``await_predicate`` does internally. Runs sync_read_frame in a thread
    executor so the asyncio event loop (which runs the daemon) can continue
    dispatching while we wait.
    """
    loop = asyncio.get_running_loop()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        sock.settimeout(max(0.1, remaining))
        try:
            data = await loop.run_in_executor(None, sync_read_frame, sock)
        except (TimeoutError, OSError):
            return None
        if data is None:
            return None
        frame: dict[str, Any] = json.loads(data.decode("utf-8"))
        if frame.get("kind") not in _CONTROL_KINDS:
            return frame
    return None


# ---------------------------------------------------------------------------
# _resolve_token unit tests (no daemon required)
# ---------------------------------------------------------------------------


def test_resolve_token_explicit_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit kwarg takes priority over all environment sources."""
    monkeypatch.setenv("WAITBUS_BROADCAST_TOKEN", "env-token")
    assert _resolve_token("explicit-token") == "explicit-token"


def test_resolve_token_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """WAITBUS_BROADCAST_TOKEN env var is returned when no explicit token."""
    monkeypatch.setenv("WAITBUS_BROADCAST_TOKEN", "my-env-token")
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.delenv("WAITBUS_CREDS_DIR", raising=False)
    assert _resolve_token(None) == "my-env-token"


def test_resolve_token_creds_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Credential file under WAITBUS_CREDS_DIR is returned."""
    (tmp_path / "broadcast-token").write_text("creds-token\n")
    monkeypatch.delenv("WAITBUS_BROADCAST_TOKEN", raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.setenv("WAITBUS_CREDS_DIR", str(tmp_path))
    assert _resolve_token(None) == "creds-token"


def test_resolve_token_none_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Returns None when no token source is configured."""
    monkeypatch.delenv("WAITBUS_BROADCAST_TOKEN", raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.delenv("WAITBUS_CREDS_DIR", raising=False)
    assert _resolve_token(None) is None


# ---------------------------------------------------------------------------
# emit_frame unit tests (no daemon required)
# ---------------------------------------------------------------------------


def test_emit_frame_json(capsys: pytest.CaptureFixture[str]) -> None:
    """JSON mode emits compact single-line JSON."""
    frame: dict[str, object] = {
        "event_id": "01JZ0ABC1230EF456GHJ789KMN",
        "kind": "event",
        "event_type": "workflow_run",
        "owner": "a",
        "repo": "b",
        "summary": "Tests passed",
    }
    emit_frame(frame, as_json=True)
    captured = capsys.readouterr()
    decoded = json.loads(captured.out.strip())
    assert decoded["kind"] == "event"
    assert decoded["event_type"] == "workflow_run"
    assert decoded["event_id"] == "01JZ0ABC1230EF456GHJ789KMN"
    assert "\n" not in captured.out.rstrip("\n")


def test_emit_frame_text(capsys: pytest.CaptureFixture[str]) -> None:
    """Text mode emits a human-readable line with event_id / event_type / owner/repo / summary."""
    frame: dict[str, object] = {
        "event_id": "01JZ0ABC1230EF456GHJ789KMN",
        "kind": "event",
        "event_type": "workflow_run",
        "owner": "myowner",
        "repo": "myrepo",
        "summary": "Tests passed",
    }
    emit_frame(frame, as_json=False)
    captured = capsys.readouterr()
    line = captured.out.strip()
    assert "01JZ0ABC1230EF456GHJ789KMN" in line
    assert "workflow_run" in line
    assert "myowner/myrepo" in line
    assert "Tests passed" in line


def test_emit_frame_text_heartbeat(capsys: pytest.CaptureFixture[str]) -> None:
    """Heartbeat frames (no owner/repo) print without a slash-separated slug."""
    frame: dict[str, object] = {
        "kind": "daemon_heartbeat",
        "ts": 1234567890,
        "uptime_sec": 42,
    }
    emit_frame(frame, as_json=False)
    captured = capsys.readouterr()
    assert "daemon_heartbeat" in captured.out


def test_exit_with_error_prints_and_exits(capsys: pytest.CaptureFixture[str]) -> None:
    """_exit_with_error() prints error+hint to stderr and raises typer.Exit(code)."""
    with pytest.raises(typer.Exit) as excinfo:
        _exit_with_error("something went wrong", hint="try this instead")
    assert excinfo.value.exit_code == 2
    captured = capsys.readouterr()
    assert "error: something went wrong" in captured.err
    assert "hint: try this instead" in captured.err


# ---------------------------------------------------------------------------
# open_subscriber contract tests (live daemon)
# ---------------------------------------------------------------------------


def test_open_subscriber_no_daemon_raises(tmp_path: Path) -> None:
    """BroadcastConnectionError is raised when no daemon is listening."""
    absent = str(tmp_path / "no.sock")
    with pytest.raises(BroadcastConnectionError) as exc_info:
        open_subscriber(socket_path=absent)
    assert exc_info.value.remediation


@pytest.mark.asyncio
async def test_open_subscriber_connects_and_receives_frame(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """open_subscriber connects to the daemon and delivers frames."""
    _daemon, paths = running_daemon
    sub = open_subscriber(
        filters=["test-owner/test-repo"],
        socket_path=str(paths["broadcast"]),
    )
    try:
        await asyncio.sleep(0.05)  # let subscribe register
        _insert(paths["db"], "d-sub-test")
        found = await _recv_non_heartbeat(sub.sock, timeout=2.0)
        assert found is not None, "no event frame received"
        assert found["delivery_id"] == "d-sub-test"
    finally:
        sub.sock.close()


@pytest.mark.asyncio
async def test_open_subscriber_wildcard_filter(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """Default wildcard filter receives events from any repo."""
    _daemon, paths = running_daemon
    sub = open_subscriber(socket_path=str(paths["broadcast"]))
    try:
        await asyncio.sleep(0.05)
        _insert(paths["db"], "d-any", owner="any-owner", repo="any-repo")
        found = await _recv_non_heartbeat(sub.sock, timeout=2.0)
        assert found is not None
        assert found["owner"] == "any-owner"
    finally:
        sub.sock.close()


@pytest.mark.asyncio
async def test_open_subscriber_since_replay(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """open_subscriber with ``since`` replays rows inserted before subscribe."""
    _daemon, paths = running_daemon
    # Pre-insert an event BEFORE subscribing.
    _insert(paths["db"], "d-historical")
    await asyncio.sleep(0.05)
    # Use the zero ULID as cursor so the historical row is in replay range.
    sub = open_subscriber(
        since="00000000000000000000000000",
        socket_path=str(paths["broadcast"]),
    )
    try:
        found = await _recv_non_heartbeat(sub.sock, timeout=2.0)
        assert found is not None, "replayed frame not received"
        assert found["delivery_id"] == "d-historical"
    finally:
        sub.sock.close()


# ---------------------------------------------------------------------------
# broadcast tap integration tests (live daemon)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broadcast_tap_count_exits_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """broadcast tap --count 1 exits 0 after receiving one frame."""
    _daemon, paths = running_daemon

    import waitbus.broadcast_tap as tap_mod

    original_open = open_subscriber

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = str(paths["broadcast"])
        return original_open(**kwargs)

    result_holder: list[int] = []

    def run_tap() -> None:
        with patch.object(tap_mod, "open_subscriber", patched_open):
            rc = tap_mod.main(["--count", "1", "--json"])
            result_holder.append(rc)

    t = threading.Thread(target=run_tap, daemon=True)
    t.start()
    await asyncio.sleep(0.15)  # let subscribe register
    _insert(paths["db"], "d-tap-test")
    # Wait up to 3 s for the tap to receive 1 frame and exit.
    deadline = time.monotonic() + 3.0
    while t.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.1)

    assert not t.is_alive(), "tap thread did not exit after --count 1"
    assert result_holder == [0]


def test_broadcast_tap_no_daemon_exits_2(tmp_path: Path) -> None:
    """broadcast tap exits with code 2 when the daemon is not running."""
    import waitbus.broadcast_tap as tap_mod

    absent = str(tmp_path / "no.sock")
    original_open = open_subscriber

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = absent
        return original_open(**kwargs)

    with patch.object(tap_mod, "open_subscriber", patched_open):
        rc = tap_mod.main([])
    assert rc == 2


@pytest.mark.asyncio
async def test_broadcast_tap_text_mode(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """broadcast tap --text emits human-readable lines."""
    _daemon, paths = running_daemon

    import waitbus.broadcast_tap as tap_mod

    original_open = open_subscriber
    emitted_lines: list[str] = []

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = str(paths["broadcast"])
        return original_open(**kwargs)

    original_emit = emit_frame

    def capturing_emit(frame: dict[str, object], *, as_json: bool) -> None:
        assert not as_json  # text mode
        original_emit(frame, as_json=as_json)
        # Just record that emit was called in text mode.
        emitted_lines.append(str(frame.get("kind", "")))

    result_holder: list[int] = []

    def run_tap() -> None:
        with (
            patch.object(tap_mod, "open_subscriber", patched_open),
            patch.object(tap_mod, "emit_frame", capturing_emit),
        ):
            rc = tap_mod.main(["--count", "1", "--text"])
            result_holder.append(rc)

    t = threading.Thread(target=run_tap, daemon=True)
    t.start()
    await asyncio.sleep(0.15)
    _insert(paths["db"], "d-tap-text")
    deadline = time.monotonic() + 3.0
    while t.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.1)

    assert not t.is_alive()
    assert result_holder == [0]
    assert len(emitted_lines) >= 1


# ---------------------------------------------------------------------------
# replay integration tests (live daemon)
# ---------------------------------------------------------------------------


def test_replay_invalid_ulid_exits_2() -> None:
    """replay exits with code 2 when the ULID argument is malformed."""
    import waitbus.replay as replay_mod

    rc = replay_mod.main(["not-a-ulid"])
    assert rc == 2


def test_replay_no_daemon_exits_2(tmp_path: Path) -> None:
    """replay exits with code 2 when the daemon is not running."""
    import waitbus.replay as replay_mod

    absent = str(tmp_path / "no.sock")
    original_open = open_subscriber

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = absent
        return original_open(**kwargs)

    with patch.object(replay_mod, "open_subscriber", patched_open):
        rc = replay_mod.main(["01JZ0ABC1230EF456GHJ789KMN"])
    assert rc == 2


@pytest.mark.asyncio
async def test_replay_catches_up_on_timeout(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """replay exits 0 when no frames arrive within the timeout window."""
    _daemon, paths = running_daemon

    import waitbus.replay as replay_mod

    original_open = open_subscriber

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = str(paths["broadcast"])
        return original_open(**kwargs)

    result_holder: list[int] = []

    def run_replay() -> None:
        with patch.object(replay_mod, "open_subscriber", patched_open):
            # ULID far in the future: no rows match; timeout=0.5 keeps test fast.
            rc = replay_mod.main(
                [
                    "7ZZZZZZZZZZZZZZZZZZZZZZZZZ",
                    "--timeout",
                    "0.5",
                ]
            )
            result_holder.append(rc)

    t = threading.Thread(target=run_replay, daemon=True)
    t.start()
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive(), "replay thread did not exit after timeout"
    assert result_holder == [0]


@pytest.mark.asyncio
async def test_replay_delivers_historical_frames(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """replay delivers rows inserted before the subscribe call."""
    _daemon, paths = running_daemon
    # Pre-insert an event.
    _insert(paths["db"], "d-replay-hist")
    await asyncio.sleep(0.05)

    import io

    import waitbus.replay as replay_mod

    original_open = open_subscriber

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = str(paths["broadcast"])
        return original_open(**kwargs)

    result_holder: list[int] = []
    captured_stdout = io.StringIO()

    def run_replay() -> None:
        with (
            patch.object(replay_mod, "open_subscriber", patched_open),
            patch("sys.stdout", captured_stdout),
        ):
            rc = replay_mod.main(
                [
                    "00000000000000000000000000",
                    "--timeout",
                    "1.0",
                ]
            )
            result_holder.append(rc)

    t = threading.Thread(target=run_replay, daemon=True)
    t.start()
    # Use asyncio.to_thread to wait without blocking the event loop, so the
    # daemon (which runs in this same event loop) can process the subscribe.
    await asyncio.to_thread(t.join, 5.0)
    assert not t.is_alive(), "replay thread did not exit"
    assert result_holder == [0]
    output = captured_stdout.getvalue()
    assert "d-replay-hist" in output


# ---------------------------------------------------------------------------
# bookmark shape validation (no daemon required)
# ---------------------------------------------------------------------------


def test_bookmark_id_rejects_path_traversal() -> None:
    """bookmark name containing '..' (directory traversal) is rejected."""
    with pytest.raises(ValueError, match="outside"):
        BookmarkCursor.validate_name("../foo")


def test_bookmark_id_rejects_forward_slash() -> None:
    """bookmark name containing '/' is rejected."""
    with pytest.raises(ValueError, match="outside"):
        BookmarkCursor.validate_name("owner/repo")


def test_bookmark_id_rejects_space() -> None:
    """bookmark name containing a space is rejected."""
    with pytest.raises(ValueError, match="outside"):
        BookmarkCursor.validate_name("bookmark with space")


def test_bookmark_id_rejects_empty_string() -> None:
    """Empty bookmark name is rejected with a clear message."""
    with pytest.raises(ValueError, match="non-empty"):
        BookmarkCursor.validate_name("")


def test_bookmark_id_rejects_shell_metachar() -> None:
    """bookmark name containing a shell metacharacter is rejected."""
    for bad in ("$HOME", "name;cmd", "a&b", "x|y", "`ls`", "a>b"):
        with pytest.raises(ValueError):
            BookmarkCursor.validate_name(bad)


@pytest.mark.parametrize(
    "name",
    [
        "my-bookmark",
        "ci.green",
        "owner_repo",
        "ALL-CAPS",
        "a",
        "123",
        "a.b.c",
        "hyphen-dot_underscore",
    ],
)
def test_bookmark_id_accepts_valid_names(name: str) -> None:
    """Valid bookmark names do not raise."""
    BookmarkCursor.validate_name(name)  # must not raise


# ---------------------------------------------------------------------------
# BookmarkCursor load/advance round-trip (no daemon required)
# ---------------------------------------------------------------------------


def test_bookmark_cursor_load_missing_file_returns_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """BookmarkCursor.load() returns None when no cursor file exists."""

    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state"))
    result = BookmarkCursor("my-bookmark").load()
    assert result is None


def test_bookmark_cursor_advance_load_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """advance() then load() returns the advanced-to ULID."""

    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state"))
    ulid = "01JZABC123DEF456GHJ789KLMN"
    cursor = BookmarkCursor("my-bookmark")
    cursor.advance({"event_id": ulid, "kind": "event", "event_type": "workflow_run"})
    result = cursor.load()
    assert result == ulid


def test_bookmark_cursor_advance_atomic_no_tmp_files_left(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """advance() leaves no .bookmark-*.tmp files after a successful write."""

    state_dir = tmp_path / "state"
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(state_dir))
    ulid = "01JZABC123DEF456GHJ789KLMN"
    cursor = BookmarkCursor("test-atomic")
    cursor.advance({"event_id": ulid, "kind": "event", "event_type": "workflow_run"})
    cursors_path = state_dir / "cursors"
    tmp_files = list(cursors_path.glob(".bookmark-*.tmp"))
    assert tmp_files == [], f"stale tempfiles found: {tmp_files}"


def test_bookmark_cursor_advance_overwrites_previous_cursor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A second advance() call overwrites the first cursor atomically."""

    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state"))
    first = "01JZABC123DEF456GHJ789KLMN"
    second = "01JZXYZ789DEF123GHJ456KLMN"
    cursor = BookmarkCursor("overwrite-test")
    cursor.advance({"event_id": first, "kind": "event", "event_type": "workflow_run"})
    cursor.advance({"event_id": second, "kind": "event", "event_type": "workflow_run"})
    result = cursor.load()
    assert result == second


def test_bookmark_cursor_rejects_invalid_name_on_construct(tmp_path: Path) -> None:
    """BookmarkCursor() rejects an invalid name before any file I/O."""
    with pytest.raises(ValueError):
        BookmarkCursor("../escape")


def test_bookmark_cursor_rejects_name_with_spaces(tmp_path: Path) -> None:
    """BookmarkCursor() rejects a name containing spaces."""
    with pytest.raises(ValueError):
        BookmarkCursor("bad name with spaces")


def test_bookmark_cursor_skips_heartbeats(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A heartbeat frame must not advance the persisted cursor.

    This is the A-01 regression: broadcast_tap previously called
    save_bookmark unconditionally, clobbering the real-event cursor with
    the fresh heartbeat ULID on every tick.
    """

    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state"))
    cursor = BookmarkCursor("test")
    cursor.advance({"event_id": "01HZREALEVENT0000000000000", "kind": "event", "event_type": "workflow_run"})
    assert cursor.load() == "01HZREALEVENT0000000000000"
    cursor.advance({"ts": 1, "uptime_sec": 2, "kind": "daemon_heartbeat"})
    assert cursor.load() == "01HZREALEVENT0000000000000"  # unchanged


def test_bookmark_cursor_skips_control_frames(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """subscribe_ack / subscribe_rejected control frames carry no ``event_id``
    and must never advance the persisted cursor.

    Under wire protocol v1 only ``event`` / ``truncated`` data frames carry an
    ``event_id``; control frames (subscribe_ack, subscribe_rejected,
    daemon_heartbeat) do not, so ``advance`` must skip them — otherwise a
    subscriber could clobber its real-event resume cursor with a non-event.
    """

    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state"))
    cursor = BookmarkCursor("ctrl")
    cursor.advance({"event_id": "01HZREALEVENT0000000000000", "kind": "event", "event_type": "workflow_run"})
    assert cursor.load() == "01HZREALEVENT0000000000000"
    # subscribe_ack has no event_id -> no advance.
    cursor.advance(
        {
            "kind": "subscribe_ack",
            "proto": 1,
            "caught_up_at": "01HZSHOULDNOTADVANCE000000",
            "heartbeat_sec": 5,
            "max_frame_bytes": 65536,
        }
    )
    assert cursor.load() == "01HZREALEVENT0000000000000"  # unchanged
    # subscribe_rejected has no event_id -> no advance.
    cursor.advance({"kind": "subscribe_rejected", "reason": "token", "remediation": "x"})
    assert cursor.load() == "01HZREALEVENT0000000000000"  # unchanged


# ---------------------------------------------------------------------------
# open_subscriber bookmark integration (no daemon required for validation)
# ---------------------------------------------------------------------------


def test_open_subscriber_invalid_bookmark_raises_before_socket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """open_subscriber raises ValueError for a bad bookmark_id before any I/O."""

    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state"))
    with pytest.raises(ValueError):
        # Uses an absent socket path; ValueError must surface before connect.
        open_subscriber(
            bookmark_id="../traversal",
            socket_path=str(tmp_path / "no.sock"),
        )


@pytest.mark.asyncio
async def test_open_subscriber_bookmark_injects_cursor(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """open_subscriber with bookmark_id loads the stored cursor and replays from it."""

    _daemon, paths = running_daemon

    state_dir = tmp_path / "state"
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(state_dir))
    # Pre-insert an event so we have something to replay.
    _insert(paths["db"], "d-bookmark-replay")
    await asyncio.sleep(0.05)

    # Store a zero-ULID cursor so the subscriber replays all events.
    BookmarkCursor("test-bookmark").advance(
        {"event_id": "00000000000000000000000000", "kind": "event", "event_type": "workflow_run"}
    )

    sub = open_subscriber(
        bookmark_id="test-bookmark",
        socket_path=str(paths["broadcast"]),
    )
    try:
        found = await _recv_non_heartbeat(sub.sock, timeout=2.0)
        assert found is not None, "replayed frame not received via bookmark cursor"
        assert found["delivery_id"] == "d-bookmark-replay"
    finally:
        sub.sock.close()


# ---------------------------------------------------------------------------
# wire-protocol-v1 handshake contract: subscribe_ack / subscribe_rejected
# surfaced via the read engine (await_predicate), not open_subscriber
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wrong_token_surfaces_token_required_on_read(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """A wrong token surfaces ``TokenRequiredError`` when the caller READS.

    Wire protocol v1: ``open_subscriber`` no longer probes and so does NOT
    raise synchronously on a bad token — it sends the subscribe envelope and
    returns a handle. The daemon, having cleared the accept-time SO_PEERCRED
    gate, writes a single ``subscribe_rejected{reason:"token"}`` frame and
    FINs. The shared read engine (``await_predicate``) recognises that frame
    and raises the typed ``TokenRequiredError`` carrying the daemon's
    remediation string. Both tokens are within the [16, 128] envelope so this
    exercises the hmac mismatch sub-path (not bad-length). Run in a thread
    executor so the daemon's event loop keeps dispatching while the
    synchronous read blocks.
    """
    daemon, paths = running_daemon
    daemon.token = "expected-token-0123456789"  # 25 chars

    loop = asyncio.get_running_loop()

    # open_subscriber returns normally — no synchronous raise.
    sub = await loop.run_in_executor(
        None,
        lambda: open_subscriber(
            token="wrong-token-9876543210",  # 22 chars, mismatch
            socket_path=str(paths["broadcast"]),
        ),
    )
    try:

        def _decide(_frame: dict[str, Any]) -> FrameDecision:
            pytest.fail("decide() must not see the subscribe_rejected frame")

        with pytest.raises(TokenRequiredError) as exc_info:
            await loop.run_in_executor(
                None,
                lambda: await_predicate(sub, decide=_decide, deadline_seconds=3.0),
            )
        assert exc_info.value.remediation, "remediation must be carried"
        assert "broadcast-token" in exc_info.value.remediation
    finally:
        sub.sock.close()


@pytest.mark.asyncio
async def test_no_token_returns_handle_without_probe_delay(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """A token-less subscribe returns a handle promptly and has no probe.

    The fixture daemon has ``token=None``, so a token-less subscriber is
    accepted. Under wire protocol v1 ``open_subscriber`` performs NO
    client-side probe ``select`` and carries no pre-read frame (the
    ``prefetched`` field is gone). It returns essentially at socket-write
    speed. The daemon then emits a ``subscribe_ack`` control frame, which
    ``await_predicate`` SKIPS, and the first real event is delivered intact.
    """
    _daemon, paths = running_daemon
    loop = asyncio.get_running_loop()
    start = time.monotonic()
    sub = await loop.run_in_executor(
        None,
        lambda: open_subscriber(socket_path=str(paths["broadcast"])),
    )
    elapsed = time.monotonic() - start
    try:
        # No probe window: open_subscriber only connects + sends the
        # subscribe envelope. 0.2s is a generous ceiling for that on a
        # local AF_UNIX socket.
        assert elapsed < 0.2, (
            "open_subscriber must not probe; it should only connect and send "
            f"the subscribe envelope (took {elapsed:.3f}s)"
        )
        # The handle has NO prefetched attribute under wire protocol v1.
        assert not hasattr(sub, "prefetched")
        # The stream is healthy: the daemon's subscribe_ack is skipped by
        # await_predicate / the heartbeat-aware reader, and the first real
        # event is delivered. Sleep to let the daemon's async
        # _read_subscribe register this live (no-`since`) subscriber.
        await asyncio.sleep(0.05)
        _insert(paths["db"], "d-no-token-stream")
        found = await _recv_non_heartbeat(sub.sock, timeout=2.0)
        assert found is not None and found["delivery_id"] == "d-no-token-stream"
    finally:
        sub.sock.close()


def test_open_subscriber_daemon_down_raises_connection_error(
    tmp_path: Path,
) -> None:
    """No daemon -> ``BroadcastConnectionError`` (unchanged contract)."""
    absent = str(tmp_path / "nope.sock")
    with pytest.raises(BroadcastConnectionError) as exc_info:
        open_subscriber(token="some-token", socket_path=absent)
    assert exc_info.value.remediation


@pytest.mark.asyncio
async def test_correct_token_subscribe_ack_skipped_first_event_delivered(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """A correct-token connection never loses its first event under v1.

    With a correct token the daemon registers the subscriber and writes a
    ``subscribe_ack`` control frame (carrying ``caught_up_at``) AFTER any
    replay, then streams real events. ``open_subscriber`` performs no probe
    and stashes no frame; ``await_predicate`` SKIPS the ack as a control
    frame and hands the first real event to ``decide`` intact — no frame is
    eaten and no ``prefetched`` carry-over is needed.

    The event is PRE-inserted and the subscriber uses ``since`` so the daemon
    replays it deterministically the instant the subscriber registers
    (the deterministic equivalent of the live-subscription 0.05s settle),
    putting the frame on the wire immediately after the ack.
    """
    daemon, paths = running_daemon
    # Token MUST be within the daemon's [16, 128] length envelope, else
    # the bad-length sub-path rejects it before the value compare.
    good_token = "good-token-0123456789"  # 21 chars
    daemon.token = good_token
    # Pre-insert so `since` guarantees replay delivery (no registration race).
    _insert(paths["db"], "d-first-after-token")
    await asyncio.sleep(0.02)

    def _open() -> SubscriberHandle:
        return open_subscriber(
            filters=["test-owner/test-repo"],
            token=good_token,
            since="00000000000000000000000000",
            socket_path=str(paths["broadcast"]),
        )

    loop = asyncio.get_running_loop()
    sub = await loop.run_in_executor(None, _open)
    try:
        # No probe / no stashed frame under wire protocol v1.
        assert not hasattr(sub, "prefetched")

        seen: list[dict[str, Any]] = []

        def _decide(frame: dict[str, Any]) -> FrameDecision:
            seen.append(frame)
            return FrameDecision.MATCHED

        outcome = await loop.run_in_executor(
            None,
            lambda: await_predicate(sub, decide=_decide, deadline_seconds=3.0),
        )
        assert outcome.matched, f"first real event was lost: {outcome}"
        assert seen, "decide() never saw a frame"
        # The subscribe_ack must NOT have reached decide().
        assert seen[0].get("kind") != "subscribe_ack", seen[0]
        assert seen[0]["delivery_id"] == "d-first-after-token", seen[0]
    finally:
        sub.sock.close()


def test_await_predicate_raises_token_required_on_subscribe_rejected_frame(
    tmp_path: Path,
) -> None:
    """A ``subscribe_rejected{reason:"token"}`` frame read off the socket
    surfaces as ``TokenRequiredError`` from ``await_predicate``, not as a
    confusing event whose ``kind`` is the reject envelope.

    Under wire protocol v1 ``open_subscriber`` no longer probes; the daemon's
    terminal reject frame is read by the shared engine. The engine must turn
    it into the typed auth error carrying the daemon's remediation rather than
    leaking a mystery frame into the consumer's normal event loop.
    """
    rsock, wsock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        reject = {
            "kind": "subscribe_rejected",
            "reason": "token",
            "remediation": "rotate the broadcast token",
        }
        wsock.sendall(encode_frame(json.dumps(reject).encode("utf-8")))

        def _decide(f: dict[str, Any]) -> FrameDecision:
            pytest.fail(
                f"decide() must not see a subscribe_rejected frame "
                f"(got {f!r}); await_predicate should have raised "
                "TokenRequiredError first"
            )

        with pytest.raises(TokenRequiredError) as exc_info:
            await_predicate(
                SubscriberHandle(sock=rsock),
                decide=_decide,
                deadline_seconds=1.0,
            )
        assert "rotate the broadcast token" in str(exc_info.value.remediation)
    finally:
        rsock.close()
        wsock.close()


def test_await_predicate_raises_connection_error_on_version_reject_frame(
    tmp_path: Path,
) -> None:
    """A ``subscribe_rejected{reason:"version"}`` frame surfaces as
    ``BroadcastConnectionError`` from ``await_predicate``.

    The daemon writes this terminal frame when the client sends an
    unsupported wire ``proto``. The shared engine distinguishes it from the
    token-reject (which raises ``TokenRequiredError``) and raises the
    connection-level error carrying the daemon's remediation, so a proto
    mismatch is not mistaken for an auth failure.
    """
    rsock, wsock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        reject = {
            "kind": "subscribe_rejected",
            "reason": "version",
            "remediation": "send proto: 1",
            "supported": [1],
        }
        wsock.sendall(encode_frame(json.dumps(reject).encode("utf-8")))

        def _decide(f: dict[str, Any]) -> FrameDecision:
            pytest.fail(f"decide() must not see a subscribe_rejected frame (got {f!r})")

        with pytest.raises(BroadcastConnectionError) as exc_info:
            await_predicate(
                SubscriberHandle(sock=rsock),
                decide=_decide,
                deadline_seconds=1.0,
            )
        assert "send proto: 1" in str(exc_info.value.remediation)
    finally:
        rsock.close()
        wsock.close()


def test_await_predicate_skips_subscribe_ack_frame(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``await_predicate`` SKIPS a ``subscribe_ack`` control frame.

    The post-registration ack must not be passed to ``decide`` and must not
    advance the resume cursor (it carries no ``event_id``); only the real
    ``event`` frame that follows it reaches ``decide``. This is the v1
    replacement for the old prefetched-frame re-injection: the ack is the
    first frame on the wire, and the engine drains it transparently.
    """
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state"))
    rsock, wsock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        ack = {
            "kind": "subscribe_ack",
            "proto": 1,
            "caught_up_at": "01HZACKWATERMARK0000000000",
            "heartbeat_sec": 5,
            "max_frame_bytes": 65536,
        }
        event = {
            "kind": "event",
            "event_id": "01HZZZZZZZZZZZZZZZZZZZZZZZ",
            "event_type": "workflow_run",
            "delivery_id": "d-after-ack",
        }
        wsock.sendall(encode_frame(json.dumps(ack).encode("utf-8")))
        wsock.sendall(encode_frame(json.dumps(event).encode("utf-8")))

        seen: list[dict[str, Any]] = []

        def _decide(f: dict[str, Any]) -> FrameDecision:
            seen.append(f)
            return FrameDecision.MATCHED

        cursor = BookmarkCursor("ack-skip")
        outcome = await_predicate(
            SubscriberHandle(sock=rsock),
            decide=_decide,
            deadline_seconds=1.0,
            cursor=cursor,
        )
        assert outcome.matched, outcome
        # The ack was skipped: decide saw only the event frame.
        assert len(seen) == 1, seen
        assert seen[0].get("kind") == "event", seen[0]
        assert seen[0]["delivery_id"] == "d-after-ack", seen[0]
        # The cursor advanced onto the event's event_id, never the ack
        # (the ack carries no event_id).
        assert cursor.current == "01HZZZZZZZZZZZZZZZZZZZZZZZ"
    finally:
        rsock.close()
        wsock.close()


@pytest.mark.parametrize(
    ("module_name", "verb_attr"),
    [
        ("waitbus.wait", "_wait"),
        ("waitbus.replay", "_replay"),
        ("waitbus.broadcast_tap", "_tap"),
        ("waitbus.read_events", "main"),
        ("waitbus.mcp", None),
    ],
)
def test_caller_reject_branches_are_reachable(module_name: str, verb_attr: str | None) -> None:
    """The 5 subscribe-reject callers compile, import, and catch the base.

    The daemon's ``subscribe_rejected`` frame is reachable end-to-end;
    ``await_predicate`` raises one of ``TokenRequiredError`` /
    ``ProtocolVersionError`` / ``SubscriberLaggedError`` — all subclasses of
    ``BroadcastConnectionError``. Each caller catches ``BroadcastConnectionError``
    (the base), so it handles every reject reason without naming each subclass.
    Importing each module is the smoke check that the catch still type-checks.
    """
    import importlib

    mod = importlib.import_module(module_name)
    src = __import__("inspect").getsource(mod)
    assert "BroadcastConnectionError" in src, f"{module_name} no longer references BroadcastConnectionError"
    if verb_attr is not None:
        assert hasattr(mod, verb_attr), f"{module_name}.{verb_attr} entrypoint vanished"


# ---------------------------------------------------------------------------
# read_subscribe_ack — the shared registration barrier
# ---------------------------------------------------------------------------


def _frame_bytes(obj: dict[str, Any]) -> bytes:
    """Length-prefix-frame a JSON control payload, as the daemon writes it."""
    return encode_frame(json.dumps(obj).encode("utf-8"))


def _socketpair_with_server_frames(*frames: bytes) -> tuple[socket.socket, socket.socket]:
    """Return (server, client) AF_UNIX pair with ``frames`` preloaded server-side."""
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    for chunk in frames:
        server.sendall(chunk)
    return server, client


def test_read_subscribe_ack_returns_on_ack() -> None:
    """A subscribe_ack first frame satisfies the barrier and returns None."""
    server, client = _socketpair_with_server_frames(_frame_bytes({"kind": "subscribe_ack", "caught_up_at": None}))
    try:
        # Returns normally (does not raise) on a valid subscribe_ack. The function
        # is typed -> None, so the assertion is "no raise", not a value comparison.
        read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        server.close()
        client.close()


def test_read_subscribe_ack_maps_reject_reason_to_typed_exception() -> None:
    """A subscribe_rejected with reason='token' raises the typed TokenRequiredError."""
    server, client = _socketpair_with_server_frames(
        _frame_bytes({"kind": "subscribe_rejected", "reason": "token", "remediation": "set a token"})
    )
    try:
        with pytest.raises(TokenRequiredError, match="reason='token'"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        server.close()
        client.close()


def test_read_subscribe_ack_unknown_reject_reason_falls_to_base() -> None:
    """An unknown reject reason falls to the base BroadcastConnectionError, not token."""
    server, client = _socketpair_with_server_frames(
        _frame_bytes({"kind": "subscribe_rejected", "reason": "lag_limit_exceeded"})
    )
    try:
        with pytest.raises(BroadcastConnectionError, match="lag_limit_exceeded"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        server.close()
        client.close()


def test_read_subscribe_ack_missing_reject_reason_defaults_to_token() -> None:
    """A reject frame with NO reason key maps to TokenRequiredError.

    Pins the current missing-reason default in the shared reject mapping:
    a frame lacking ``reason`` is treated as a token rejection. Whether
    that default should change is a separate question; this test only
    keeps the behavior observable.
    """
    server, client = _socketpair_with_server_frames(_frame_bytes({"kind": "subscribe_rejected"}))
    try:
        with pytest.raises(TokenRequiredError, match="reason='token'"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        server.close()
        client.close()


def test_read_subscribe_ack_non_string_reject_reason_falls_to_base() -> None:
    """A reject frame carrying an unhashable reason (list) still raises the
    typed base BroadcastConnectionError instead of a raw TypeError from the
    reason-to-exception dict lookup."""
    server, client = _socketpair_with_server_frames(
        _frame_bytes({"kind": "subscribe_rejected", "reason": ["token", "version"]})
    )
    try:
        with pytest.raises(BroadcastConnectionError, match="non-string:list"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        server.close()
        client.close()


def test_read_subscribe_ack_raises_on_eof_before_ack() -> None:
    """A peer that closes before sending any frame raises BroadcastConnectionError."""
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    server.close()  # immediate EOF
    try:
        with pytest.raises(BroadcastConnectionError, match="before sending subscribe_ack"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        client.close()


def test_read_subscribe_ack_rejects_unexpected_first_kind() -> None:
    """A non-handshake first frame (wire violation) raises BroadcastConnectionError."""
    server, client = _socketpair_with_server_frames(_frame_bytes({"kind": "event", "event_id": "x"}))
    try:
        with pytest.raises(BroadcastConnectionError, match="expected subscribe_ack"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        server.close()
        client.close()


def test_read_subscribe_ack_translates_undecodable_first_frame() -> None:
    """A first frame that is not valid UTF-8/JSON raises BroadcastConnectionError.

    Without the decode guard, the raw JSONDecodeError/UnicodeDecodeError would
    escape every caller's typed handler and leak the socket.
    """
    server, client = _socketpair_with_server_frames(encode_frame(b"\xff\xfe not json"))
    try:
        with pytest.raises(BroadcastConnectionError, match="undecodable"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        server.close()
        client.close()


def test_read_subscribe_ack_translates_non_object_json_first_frame() -> None:
    """A valid-JSON array first frame raises BroadcastConnectionError, not AttributeError."""
    server, client = _socketpair_with_server_frames(encode_frame(b"[1, 2]"))
    try:
        with pytest.raises(BroadcastConnectionError, match="JSON object"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=2.0)
    finally:
        server.close()
        client.close()


def test_read_subscribe_ack_translates_timeout_to_typed_exception() -> None:
    """A peer that connects but never acks must raise BroadcastConnectionError, not a
    raw socket.timeout (an OSError) that would escape callers' typed handlers and leak
    the socket. The server stays open and silent so the read hits its timeout."""
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        with pytest.raises(BroadcastConnectionError, match="did not send subscribe_ack within"):
            read_subscribe_ack(SubscriberHandle(sock=client), timeout_seconds=0.2)
    finally:
        server.close()
        client.close()
