"""Minimal waitbus broadcast subscriber (Python, stdlib only).

Connects to the broadcast daemon's AF_UNIX SOCK_STREAM socket, sends a
"subscribe everything" envelope, and prints each event frame's
delivery_id + source + event_type as it arrives.

Wire protocol
-------------
- AF_UNIX SOCK_STREAM. Default socket path is
  ``$XDG_RUNTIME_DIR/waitbus/broadcast.sock`` (Linux) or the macOS-side
  equivalent. The WAITBUS_BROADCAST_SOCKET env var overrides.
- Each frame is a 4-byte big-endian length prefix followed by that many
  bytes of UTF-8 JSON. Max payload 65536 bytes. The subscribe envelope
  uses the same framing.
- Subscribe envelope is a JSON object; must include ``"proto": 1``.
  ``{"proto": 1}`` means "all repos, all event types, from now". Add
  ``"filters": ["owner/*"]`` or ``"event_types": ["workflow_run"]`` to
  narrow.

This snippet is the authoritative Python implementation; the .rs / .go /
.ts / .sh peers in this directory implement the same wire contract.
``tests/test_subscriber_snippet.py`` exercises this file end-to-end
against a running daemon to catch protocol drift at the same commit
that ships the change.

Usage
-----
::

    python docs/snippets/minimal_subscriber.py
    # or with a custom socket path:
    WAITBUS_BROADCAST_SOCKET=/tmp/my-waitbus.sock \\
        python docs/snippets/minimal_subscriber.py
"""

from __future__ import annotations

import json
import os
import socket
import struct
import sys
from pathlib import Path

_LENGTH_STRUCT = struct.Struct(">I")
_MAX_FRAME_BYTES = 65_536


def _default_socket_path() -> Path:
    """Return the default broadcast-socket path.

    Honours ``WAITBUS_BROADCAST_SOCKET`` for explicit overrides, then
    falls back to ``$XDG_RUNTIME_DIR/waitbus/broadcast.sock`` (Linux) or
    ``$HOME/Library/Application Support/waitbus/broadcast.sock`` (macOS,
    matching waitbus's runtime_dir convention for the same reason waitbus
    avoids ``user_runtime_dir`` -- the platformdirs runtime path is an
    evictable cache, wrong for AF_UNIX sockets).
    """
    override = os.environ.get("WAITBUS_BROADCAST_SOCKET")
    if override:
        return Path(override)
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "waitbus" / "broadcast.sock"
    runtime = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    return Path(runtime) / "waitbus" / "broadcast.sock"


def _recv_exactly(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly ``n`` bytes from ``sock``; return ``None`` on clean EOF.

    Treats any short read past the first byte as an error (the wire
    contract is "all-or-nothing per frame"); a zero-length read with
    no bytes already received is a clean shutdown.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            if not buf:
                return None
            raise ConnectionError(f"short read: expected {n} bytes, got {len(buf)}")
        buf.extend(chunk)
    return bytes(buf)


def read_frame(sock: socket.socket) -> bytes | None:
    """Read one length-prefixed frame; return ``None`` on EOF."""
    prefix = _recv_exactly(sock, 4)
    if prefix is None:
        return None
    (length,) = _LENGTH_STRUCT.unpack(prefix)
    if length == 0 or length > _MAX_FRAME_BYTES:
        raise ConnectionError(f"frame length {length} out of bounds")
    payload = _recv_exactly(sock, length)
    if payload is None:
        raise ConnectionError("EOF inside frame payload")
    return payload


def write_frame(sock: socket.socket, payload: bytes) -> None:
    """Write one length-prefixed frame."""
    if len(payload) > _MAX_FRAME_BYTES:
        raise ValueError(f"payload {len(payload)} bytes exceeds {_MAX_FRAME_BYTES}")
    sock.sendall(_LENGTH_STRUCT.pack(len(payload)) + payload)


def main() -> int:
    socket_path = _default_socket_path()
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.setblocking(True)
    try:
        sock.connect(str(socket_path))
    except (FileNotFoundError, ConnectionRefusedError) as exc:
        print(
            f"error: broadcast socket {socket_path} unavailable ({type(exc).__name__}). "
            "Start the daemon via `systemctl --user start waitbus-broadcast.service`.",
            file=sys.stderr,
        )
        return 2

    # Subscribe envelope: proto=1 is mandatory. Empty filters means "all
    # repos, all event types, from now". Add "filters" or "event_types"
    # keys to narrow.
    write_frame(sock, json.dumps({"proto": 1}).encode("utf-8"))

    try:
        while True:
            frame = read_frame(sock)
            if frame is None:
                return 0
            event = json.loads(frame.decode("utf-8"))
            if event.get("kind") == "subscribe_rejected":
                reason = event.get("reason", "unknown")
                remediation = event.get("remediation", "")
                sys.stderr.write(f"error: subscribe_rejected: {reason}\n")
                if remediation:
                    sys.stderr.write(f"remediation: {remediation}\n")
                return 2
            # A "truncated" frame is a DATA frame (it carries an event_id and
            # advances the resume cursor), not a control frame: the event's
            # payload exceeded the wire cap, so only its identity rides the
            # socket. Surface it -- silently dropping it makes a large event
            # invisible -- and re-fetch the full row out of band.
            if event.get("kind") == "truncated":
                print(
                    f"{event.get('event_id', '?')}\t[truncated; re-fetch full payload via `waitbus read-events`]",
                    flush=True,
                )
                continue
            # Control frames (daemon_heartbeat, subscribe_ack) carry no event
            # identity; skip them.
            if event.get("kind") != "event":
                continue
            print(
                f"{event.get('delivery_id', '?')}\t"
                f"source={event.get('fields', {}).get('source', '?')}\t"
                f"type={event.get('event_type', '?')}",
                flush=True,
            )
    except KeyboardInterrupt:
        return 0
    finally:
        sock.close()


if __name__ == "__main__":
    sys.exit(main())
