"""Unit tests for the demo decoder helpers in ``waitbus.cli.demo``.

Covers the two pure-ish helpers ``_emit_frame_if_event`` and
``_read_frames_until_done`` that the existing ``test_cli_demo.py``
smoke test cannot reach from its end-to-end happy path:

* bad-JSON input → False, no stdout
* daemon_heartbeat frame → False (control-frame skip)
* subscribe_ack frame → False (control-frame skip)
* plain event frame → True + formatted ``[event] …`` line on stdout
* ConnectionError mid-read → loop breaks cleanly
* EOF (recv returns b"") → loop breaks cleanly

All tests are pure-function / mock-socket; no daemon is started.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
import unittest.mock

import pytest

from waitbus.cli.demo import _emit_frame_if_event, _read_frames_until_done

# ---------------------------------------------------------------------------
# _emit_frame_if_event
# ---------------------------------------------------------------------------


def test_emit_frame_if_event_returns_false_on_bad_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Non-decodable bytes must return False and produce no stdout."""
    result = _emit_frame_if_event(b"\xff\xfenot json")
    assert result is False
    captured = capsys.readouterr()
    assert captured.out == ""


def test_emit_frame_if_event_returns_false_on_heartbeat(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A daemon_heartbeat control frame must be skipped (returns False)."""
    frame = {"kind": "daemon_heartbeat", "ts": 1, "uptime_sec": 0}
    result = _emit_frame_if_event(json.dumps(frame).encode())
    assert result is False
    captured = capsys.readouterr()
    assert captured.out == ""


def test_emit_frame_if_event_returns_false_on_subscribe_ack(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A subscribe_ack control frame must be skipped (returns False)."""
    frame = {
        "kind": "subscribe_ack",
        "proto": 1,
        "caught_up_at": None,
        "heartbeat_sec": 1,
        "max_frame_bytes": 65536,
    }
    result = _emit_frame_if_event(json.dumps(frame).encode())
    assert result is False
    captured = capsys.readouterr()
    assert captured.out == ""


def test_emit_frame_if_event_returns_true_on_event(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A valid event frame must return True and print a ``[event] …`` line."""
    # 26-character ULID-shaped event_id (all uppercase alphanumeric)
    event_id = "01HZZZZZZZZZZZZZZZZZZZZZZ"
    frame = {
        "kind": "event",
        "event_type": "workflow_run",
        "event_id": event_id,
        "owner": "o",
        "repo": "r",
        "received_at": time.time_ns(),
        "delivery_id": "d-1",
        "summary": "",
        "fields": {"status": "completed", "conclusion": "success"},
    }
    result = _emit_frame_if_event(json.dumps(frame).encode())
    assert result is True
    captured = capsys.readouterr()
    assert captured.out.startswith("[event] ")
    assert captured.out.rstrip("\n") != ""


# ---------------------------------------------------------------------------
# _read_frames_until_done
# ---------------------------------------------------------------------------


def test_read_frames_until_done_breaks_on_connection_error() -> None:
    """A ConnectionError from the underlying socket must break the loop cleanly."""
    mock_sock = unittest.mock.Mock(spec=socket.socket)
    mock_sock.recv.side_effect = ConnectionError("peer reset")

    done = asyncio.Event()

    async def _run() -> int:
        return await _read_frames_until_done(sock=mock_sock, expected=1, done=done)

    result = asyncio.run(_run())
    # Must return without hanging; seen count is 0 (no frames delivered)
    assert result == 0
    assert done.is_set()


def test_read_frames_until_done_breaks_on_eof() -> None:
    """An empty recv (EOF / daemon closed) must break the loop cleanly."""
    mock_sock = unittest.mock.Mock(spec=socket.socket)
    mock_sock.recv.return_value = b""

    done = asyncio.Event()

    async def _run() -> int:
        return await _read_frames_until_done(sock=mock_sock, expected=1, done=done)

    result = asyncio.run(_run())
    assert result == 0
    assert done.is_set()
