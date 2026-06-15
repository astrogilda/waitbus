"""Wake the broadcast daemon after an event INSERT.

The doorbell is a fire-and-forget signal: a writer rings it on every
successful event INSERT, the daemon reads any pending wake state and
sweeps ``SELECT WHERE event_id > :last`` for the new rows. Coalescing
multiple rings into one sweep is desirable, not a bug — the daemon's
response to any wake is the same SELECT regardless of how many rings
preceded it.

Two platform-specific primitives, identical semantics from the daemon's
perspective:

- **Linux (``os.eventfd``)** — a single 64-bit counter in kernel space.
  ``write()`` adds to the counter; ``read()`` returns the counter value
  and resets it to 0. Multiple concurrent writes all increment the same
  counter; the kernel cannot drop a ring because there is no queue, just
  an incrementing integer. This replaces the previous ``SOCK_DGRAM``
  mechanism whose per-socket recv buffer COULD drop under burst.

- **macOS (``AF_UNIX SOCK_STREAM``)** — a connected stream socket where
  the daemon listens and writers connect-and-write a single byte. The
  stream's per-socket buffer can in principle fill, but the daemon reads
  in a loop with the stream level-triggered ready on kqueue; any unread
  byte keeps the readable flag set, so as long as one byte is in the
  buffer the daemon wakes. Coalescing is fine for the same reason as
  Linux.

Writers call ``ring()``; the daemon owns one ``Doorbell`` instance via
``Doorbell.open(path)``, calls ``Doorbell.fd`` to register with the
event loop, and calls ``Doorbell.drain()`` after each wake to consume
the pending state. The daemon never inspects the counter value — the
wake is a level signal, not a count.
"""

from __future__ import annotations

import contextlib
import os
import socket
import sys
from pathlib import Path
from typing import Final

from ._paths import doorbell_socket

_IS_LINUX = sys.platform == "linux"

_DOORBELL_LISTEN_BACKLOG: Final[int] = 64
"""Kernel listen-backlog depth for the AF_UNIX doorbell listener.

Each ring is a connect-and-write; with coalescing semantics the daemon
accepts and drains connections faster than writers produce them under
normal load. 64 gives comfortable headroom for burst ringings without
exhausting kernel queue slots.
"""

_DOORBELL_RECV_TIMEOUT_SEC: Final[float] = 0.1
"""Socket receive timeout in seconds applied to each accepted doorbell connection.

Guards against a same-uid buggy peer that connects but never sends or
closes; the timeout bounds the daemon's accept-loop iteration without
stalling the event loop.
"""

_DOORBELL_RECV_BUFFER_BYTES: Final[int] = 64
"""Maximum bytes to read from a single accepted doorbell connection.

A valid ring sends exactly one byte; the buffer is intentionally larger
to drain any padding a buggy writer might include without making multiple
``recv`` calls.
"""


# ---------------------------------------------------------------------------
# Writer side — ring()
# ---------------------------------------------------------------------------


def ring(path: Path | None = None) -> None:
    """Send one wake signal to the daemon's doorbell. Best-effort.

    Opens a connected AF_UNIX SOCK_STREAM socket and writes a single
    byte. OSError subclasses (FileNotFoundError, ConnectionRefusedError,
    BrokenPipeError) are silently swallowed because a missed ring is
    observably no worse than a delayed one — the daemon's start-time
    ``MAX(event_id)`` sweep catches everything up on the next boot.

    ``path`` defaults to :func:`doorbell_socket` (the env / XDG-resolved
    location). An explicit path lets an in-process caller (e.g. a self-contained
    demo) ring a daemon bound to a non-default runtime dir without mutating the
    process-global ``WAITBUS_RUNTIME_DIR`` env -- the doorbell-side counterpart
    of the explicit ``db_path`` injection :func:`waitbus.emit` accepts.
    """
    target = path if path is not None else doorbell_socket()
    if _IS_LINUX:
        _ring_linux(target)
    else:
        _ring_macos(target)


def _ring_linux(path: Path) -> None:
    # Writers connect to the daemon's AF_UNIX SOCK_STREAM listener and
    # write one byte. The daemon's accept-thread pulls the byte off the
    # connection and feeds it into its internal eventfd; the eventfd is
    # then the actual wake primitive the asyncio loop registers with
    # add_reader. The unix socket is the cross-process surface; the
    # eventfd is the daemon-internal coalescing layer.
    with contextlib.suppress(OSError), socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(str(path))
        s.sendall(b".")


def _ring_macos(path: Path) -> None:
    # Identical wire surface on macOS — the daemon doesn't have an
    # eventfd equivalent so its event loop accepts and reads directly
    # off the listener.
    with contextlib.suppress(OSError), socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(str(path))
        s.sendall(b".")


# Both platforms share the same writer surface — keep _ring_linux and
# _ring_macos as separate functions even though they are currently
# identical, so a future macOS-specific optimisation can land without
# churning the public ring() API.


# ---------------------------------------------------------------------------
# Daemon side — Doorbell
# ---------------------------------------------------------------------------


class Doorbell:
    """Daemon-side wake primitive. Owns one platform-specific resource.

    Lifecycle::

        d = Doorbell.open(path)        # bind listener (and Linux eventfd)
        # ... register d.fd with the event loop (eventfd on Linux,
        #     listener fd on macOS) ...
        # ... on readable: ...
        d.drain()                      # consume the wake state
        # ... run the SELECT sweep ...
        d.close()                      # release the resource

    On Linux an accept-thread (daemon-managed, not owned here) calls
    ``accept_one()`` in a loop to pull bytes off the listener and feed
    them into the eventfd; ``fd`` returns the eventfd descriptor so the
    asyncio loop wakes when the counter goes non-zero. On macOS there is
    no eventfd; ``fd`` returns the listener fd directly and the asyncio
    callback calls ``accept_one()`` inline.
    """

    def __init__(
        self,
        *,
        listener: socket.socket,
        eventfd: int | None,
        path: Path,
    ) -> None:
        self._listener = listener
        self._eventfd = eventfd  # None on macOS
        self._path = path

    @classmethod
    def open(cls, path: Path) -> Doorbell:
        """Bind the AF_UNIX SOCK_STREAM listener; on Linux also create the eventfd."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        listener.listen(_DOORBELL_LISTEN_BACKLOG)
        listener.setblocking(False)

        eventfd: int | None = None
        if sys.platform == "linux":
            eventfd = os.eventfd(0, os.EFD_NONBLOCK | os.EFD_CLOEXEC)
        return cls(listener=listener, eventfd=eventfd, path=path)

    @property
    def fd(self) -> int:
        """Return the fd the event loop should monitor for readability.

        On Linux the eventfd is the readable fd; the daemon's accept-thread
        feeds rings into the eventfd. On macOS we use the listener fd itself
        because there is no eventfd — the daemon accepts directly off the
        listener.
        """
        if self._eventfd is not None:
            return self._eventfd
        return self._listener.fileno()

    @property
    def listener_fd(self) -> int:
        """The listener fd — used by the daemon's accept-loop thread on Linux
        (separate from ``.fd`` which is the eventfd) and the same as ``.fd``
        on macOS."""
        return self._listener.fileno()

    def accept_one(self) -> bool:
        """Accept and drain one writer connection. Returns True iff a byte was read.

        On Linux, called from the daemon's accept-loop thread; the byte is
        forwarded to the eventfd so the asyncio loop wakes. On macOS, called
        directly from the asyncio readable callback on the listener fd.
        """
        try:
            conn, _addr = self._listener.accept()
        except BlockingIOError:
            return False
        # Guard against a same-uid buggy peer that connects but never sends or closes.
        conn.settimeout(_DOORBELL_RECV_TIMEOUT_SEC)
        data = b""
        try:
            with contextlib.suppress(OSError):
                data = conn.recv(_DOORBELL_RECV_BUFFER_BYTES)
        finally:
            conn.close()
        if not data:
            return False
        if sys.platform == "linux" and self._eventfd is not None:
            with contextlib.suppress(OSError):
                os.eventfd_write(self._eventfd, 1)
        return True

    def drain(self) -> int:
        """Consume pending wake state.

        On Linux, reads and resets the eventfd counter; returns the counter
        value (number of rings since the last drain). On macOS, returns 1
        because bytes are already consumed by ``accept_one`` calls in the
        event-loop callback — the return value is used only to confirm a
        wake occurred.
        """
        if sys.platform == "linux" and self._eventfd is not None:
            try:
                return os.eventfd_read(self._eventfd)
            except BlockingIOError:
                return 0
        return 1

    def close(self) -> None:
        """Release all resources and remove the socket path."""
        with contextlib.suppress(OSError):
            self._listener.close()
        if self._eventfd is not None:
            with contextlib.suppress(OSError):
                os.close(self._eventfd)
        with contextlib.suppress(FileNotFoundError):
            self._path.unlink()
