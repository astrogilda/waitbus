"""Shared client-side wire helpers for broadcast wire tests."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, cast

from waitbus._frame import encode_frame, read_frame


def read_nonblocking(fd: int) -> str:
    """Read whatever is available from a non-blocking fd; '' if nothing yet or at EOF.

    ``os.read`` on an ``O_NONBLOCK`` fd returns the available bytes (``b''`` on
    EOF) or raises ``BlockingIOError`` (EAGAIN) when nothing is buffered yet --
    the correct non-blocking read. By contrast ``TextIOWrapper.read()`` on a
    non-blocking stream returns ``None`` from its raw layer and the decoder then
    raises ``TypeError: can't concat NoneType to bytes``. Subprocess e2e tests
    must read via this helper, never via ``proc.stdout.read()``, at every
    non-blocking poll site so a transient empty read is never mistaken for a
    crash. Decodes with ``errors="replace"`` because a poll can land mid-UTF-8
    sequence; the caller reassembles lines across reads.
    """
    try:
        return os.read(fd, 65536).decode("utf-8", "replace")
    except BlockingIOError:
        return ""


async def connect(path: Path) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Open a SOCK_STREAM connection to the daemon at *path*."""
    return await asyncio.open_unix_connection(str(path))


async def subscribe(writer: asyncio.StreamWriter, **payload: Any) -> None:
    """Send one length-prefix-framed subscribe frame."""
    writer.write(encode_frame(json.dumps(payload).encode("utf-8")))
    await writer.drain()


async def recv(reader: asyncio.StreamReader, timeout: float = 2.0) -> dict[str, Any] | None:
    """Read one length-prefix-framed frame and return the decoded dict.

    Returns None on EOF.
    """
    data = await asyncio.wait_for(read_frame(reader), timeout=timeout)
    if data is None:
        return None
    return cast(dict[str, Any], json.loads(data.decode("utf-8")))


# --- fake wire socket --------------------------------------------------------


class FakeWireSocket:
    """Stand-in socket for ``Subscriber`` / ``_close_subscriber`` unit tests.

    Captures every ``send``/``sendall`` payload into ``sent`` so tests can
    inspect the wire frames produced. ``fileno`` returns ``fd`` -- the integer
    key the daemon uses to register the subscriber (must be unique per live
    subscriber); ``close`` records that the daemon closed it. Setting ``exc``
    makes every ``send``/``sendall`` raise that exception -- used to simulate
    ``BlockingIOError`` (EAGAIN) for the lag-count path. ``send`` (the
    data-plane primitive) mimics a non-blocking socket that accepts up to
    ``send_limit`` bytes per call and returns the accepted byte count;
    ``send_limit=None`` accepts the whole frame, while a small limit produces
    short counts (0 < n < len) that exercise the partial-write buffering path.
    ``sendall`` always takes the whole blob; it is retained for the terminal
    reject-frame close path. All attributes are public and mutable so a test
    can flip the failure mode mid-scenario.
    """

    def __init__(
        self,
        exc: BaseException | None = None,
        *,
        fileno: int = -1,
        send_limit: int | None = None,
    ) -> None:
        self.exc = exc
        self.fd = fileno
        self.send_limit = send_limit
        self.sent: list[bytes] = []
        self.closed = False

    def send(self, blob: bytes) -> int:
        if self.exc is not None:
            raise self.exc
        data = bytes(blob)
        n = len(data) if self.send_limit is None else min(self.send_limit, len(data))
        self.sent.append(data[:n])
        return n

    def sendall(self, blob: bytes) -> None:
        if self.exc is not None:
            raise self.exc
        self.sent.append(bytes(blob))

    def fileno(self) -> int:
        return self.fd

    def close(self) -> None:
        self.closed = True


async def recv_until(
    reader: asyncio.StreamReader,
    kind: str,
    timeout: float = 3.0,
) -> dict[str, Any]:
    """Read frames in a loop, returning the first whose ``kind`` matches.

    Raises AssertionError on EOF or timeout. Each inner read is bounded by the
    REMAINING budget to the deadline (not the full ``timeout``), so the
    worst-case wall time is ``timeout``, not ``timeout`` per non-matching frame.
    """
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"no {kind!r} frame within {timeout}s")
        frame = await recv(reader, timeout=max(0.05, remaining))
        if frame is None:
            raise AssertionError(f"daemon closed before a {kind!r} frame arrived")
        if frame.get("kind") == kind:
            return frame
