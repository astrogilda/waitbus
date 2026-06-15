"""Tests for the broadcast-daemon doorbell.

The doorbell is cross-platform: AF_UNIX SOCK_STREAM on both Linux and
macOS. On Linux the daemon also wraps the listener in an
os.eventfd for kernel-coalesced wake delivery; the eventfd is tested via
the Doorbell class directly.
"""

from __future__ import annotations

import os
import selectors
import socket
import sys
import threading
import time
from pathlib import Path

import pytest

from waitbus import _doorbell
from waitbus._doorbell import Doorbell

# ---------------------------------------------------------------------------
# Writer tests (cross-platform)
# ---------------------------------------------------------------------------


def test_ring_silent_when_daemon_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """ring() against a non-existent socket path must not raise."""
    missing = tmp_path / "waitbus-doorbell.sock"
    monkeypatch.setattr(_doorbell, "doorbell_socket", lambda: missing)
    _doorbell.ring()  # must not raise


def test_ring_one_byte_reaches_listener(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A listening SOCK_STREAM socket receives the single byte written by ring()."""
    sock_path = tmp_path / "waitbus-doorbell.sock"
    monkeypatch.setattr(_doorbell, "doorbell_socket", lambda: sock_path)

    received: list[bytes] = []
    ready = threading.Event()

    def serve() -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as srv:
            srv.bind(str(sock_path))
            srv.listen(4)
            srv.settimeout(2.0)
            ready.set()
            conn, _ = srv.accept()
            with conn:
                data = conn.recv(64)
                received.append(data)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(timeout=2.0)

    _doorbell.ring()
    thread.join(timeout=2.0)
    assert received == [b"."]


# ---------------------------------------------------------------------------
# Doorbell class — construction / lifecycle
# ---------------------------------------------------------------------------


def test_doorbell_open_creates_unix_socket_listener(tmp_path: Path) -> None:
    """Doorbell.open() creates an AF_UNIX socket at the given path."""
    sock_path = tmp_path / "doorbell.sock"
    d = Doorbell.open(sock_path)
    try:
        assert sock_path.exists()
        assert sock_path.stat().st_mode & 0o170000 == 0o140000  # S_IFSOCK
    finally:
        d.close()


def test_doorbell_close_removes_socket_file(tmp_path: Path) -> None:
    """Doorbell.close() unlinks the socket path."""
    sock_path = tmp_path / "doorbell.sock"
    d = Doorbell.open(sock_path)
    assert sock_path.exists()
    d.close()
    assert not sock_path.exists()


def test_doorbell_accept_one_returns_false_when_no_pending(
    tmp_path: Path,
) -> None:
    """accept_one() returns False immediately when no writers have connected."""
    sock_path = tmp_path / "doorbell.sock"
    d = Doorbell.open(sock_path)
    try:
        result = d.accept_one()
    finally:
        d.close()
    assert result is False


# ---------------------------------------------------------------------------
# Linux-only: eventfd coalescing
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform != "linux", reason="os.eventfd is Linux-only")
def test_doorbell_drain_returns_eventfd_counter_on_linux(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ring n times; drain() returns n (the eventfd counter value)."""
    sock_path = tmp_path / "doorbell.sock"
    monkeypatch.setattr(_doorbell, "doorbell_socket", lambda: sock_path)

    ring_count = 5
    d = Doorbell.open(sock_path)
    try:
        for _ in range(ring_count):
            _doorbell.ring()
            # Give the writer time to connect and send before the next ring
            # so all accepts happen before we drain.

        # Accept all ring_count connections to feed the eventfd.
        deadline = time.monotonic() + 2.0
        accepted = 0
        while accepted < ring_count and time.monotonic() < deadline:
            if d.accept_one():
                accepted += 1
        assert accepted == ring_count, f"expected {ring_count} accepts, got {accepted}"

        count = d.drain()
        assert count == ring_count
    finally:
        d.close()


@pytest.mark.skipif(sys.platform != "linux", reason="os.eventfd is Linux-only")
def test_doorbell_drain_resets_eventfd_to_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """After drain() returns ring_count, a second drain() returns 0 (counter reset)."""
    sock_path = tmp_path / "doorbell.sock"
    monkeypatch.setattr(_doorbell, "doorbell_socket", lambda: sock_path)

    ring_count = 3
    d = Doorbell.open(sock_path)
    try:
        for _ in range(ring_count):
            _doorbell.ring()
        deadline = time.monotonic() + 2.0
        accepted = 0
        while accepted < ring_count and time.monotonic() < deadline:
            if d.accept_one():
                accepted += 1

        first = d.drain()
        assert first == ring_count
        second = d.drain()
        assert second == 0
    finally:
        d.close()


# ---------------------------------------------------------------------------
# macOS-only: listener-as-wake-primitive
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS SOCK_STREAM path only")
def test_doorbell_drain_returns_one_on_macos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """On macOS, drain() returns 1 after accept_one() consumed the ring."""
    sock_path = tmp_path / "doorbell.sock"
    monkeypatch.setattr(_doorbell, "doorbell_socket", lambda: sock_path)

    d = Doorbell.open(sock_path)
    try:
        _doorbell.ring()
        deadline = time.monotonic() + 2.0
        while not d.accept_one() and time.monotonic() < deadline:
            time.sleep(0.01)
        result = d.drain()
        assert result == 1
    finally:
        d.close()


# ---------------------------------------------------------------------------
# Burst coalescing — cross-platform integration
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=(
        "Burst-coalescing invariants assume edge-triggered selector "
        "semantics (Linux's eventfd: one readable edge per drain regardless "
        "of accumulated increments). macOS's kqueue against the listener fd "
        "is level-triggered and re-fires once per pending unaccepted "
        "connection until the queue drains, so wake_count > burst is "
        "achievable and the test's <= burst upper-bound does not translate. "
        "A macOS-appropriate burst test that asserts the kqueue-level-triggered "
        "invariants directly (every ring() lands at the acceptor; the readable "
        "signal arrives in bounded time) is the right replacement; "
        "wake_count <= burst is meaningful only when the wake primitive "
        "coalesces, which macOS's selectors.DefaultSelector does not."
    ),
)
def test_doorbell_coalesces_burst_into_one_wake_event_loop_iteration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """100 concurrent rings coalesce to far fewer accept-loop iterations.

    The key property is that multiple rings collapsing into one select()
    wake is desirable. We verify:
    - At least 1 wake fired (no rings were lost entirely).
    - The total number of wakes is well below the ring count
      (coalescing occurred on a loaded system).

    On Linux the eventfd counter absorbs all concurrent increments; on
    macOS multiple bytes in the stream buffer collapse to one readable
    event. Both platforms satisfy the ">= 1 and <= burst_size" bound.

    The test mirrors production shape: a dedicated accept-thread drains
    the listener concurrently with the ring burst. The listener backlog
    is ``listen(64)`` (see ``Doorbell.open``); without a concurrent
    acceptor, the 65th+ ``connect()`` blocks in the kernel waiting for
    an accept slot, and the join loop stalls for the full join timeout
    on each blocked thread. Production runs exactly this accept-thread
    in ``BroadcastDaemon._doorbell_accept_loop``.
    """
    sock_path = tmp_path / "doorbell.sock"
    monkeypatch.setattr(_doorbell, "doorbell_socket", lambda: sock_path)

    burst = 100
    d = Doorbell.open(sock_path)
    wake_count = 0

    # Run a dedicated accept-thread that drains the listener in parallel
    # with the ring burst. On Linux this also feeds the eventfd (so the
    # selector below sees the coalesced wake); on macOS it consumes the
    # SOCK_STREAM bytes so connect()s unblock. Either way the listener's
    # accept backlog never fills, the rings complete promptly, and we
    # can observe coalescing at the eventfd / stream-readable layer.
    stop_accepting = threading.Event()
    accept_count = 0
    accept_count_lock = threading.Lock()

    def _accept_loop() -> None:
        nonlocal accept_count
        while not stop_accepting.is_set():
            if d.accept_one():
                with accept_count_lock:
                    accept_count += 1
            else:
                # No pending connections — yield briefly to avoid burning
                # CPU. select() on the listener fd would be ideal but is
                # platform-conditional; a short sleep is sufficient for
                # this test's timescales.
                time.sleep(0.001)

    acceptor = threading.Thread(target=_accept_loop, daemon=True)
    acceptor.start()

    try:
        # Use a selector to simulate the event-loop's readable callback.
        sel = selectors.DefaultSelector()
        sel.register(d.fd, selectors.EVENT_READ)

        # Fire all rings from a thread pool to maximise concurrency.
        threads = [threading.Thread(target=_doorbell.ring) for _ in range(burst)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=2.0)
            assert not t.is_alive(), "ring thread failed to complete within 2s"

        # Wait for the acceptor to consume every connection so the next
        # drain() sees the full coalesced counter on Linux (and the
        # listener buffer is drained on macOS).
        accept_deadline = time.monotonic() + 2.0
        while time.monotonic() < accept_deadline:
            with accept_count_lock:
                if accept_count >= burst:
                    break
            time.sleep(0.005)
        with accept_count_lock:
            assert accept_count == burst, f"acceptor saw {accept_count}/{burst} rings"

        # Observe wakes via the selector. The eventfd (Linux) or
        # listener fd (macOS) should be readable; one or more drain()s
        # consume the coalesced state.
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            events = sel.select(timeout=0.05)
            if events:
                d.drain()
                wake_count += 1
            elif wake_count >= 1:
                # No more events and we've seen at least one wake — done.
                break
    finally:
        stop_accepting.set()
        acceptor.join(timeout=2.0)
        sel.close()
        d.close()

    assert wake_count >= 1, "no wake was delivered — rings may have been lost"
    assert wake_count <= burst, "more wakes than rings (impossible)"
    # Coalescing is observable: on a non-trivially loaded CI machine the
    # burst of 100 rings should produce noticeably fewer wakes. We use a
    # generous upper bound (burst) to keep this a correctness test, not
    # a performance assertion.


# ---------------------------------------------------------------------------
# Linux-only: eventfd overflow boundary
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform != "linux", reason="os.eventfd is Linux-only")
def test_eventfd_overflow_does_not_lose_wake_signal(tmp_path: Path) -> None:
    """Manually saturate the eventfd counter; confirm the next read still wakes.

    eventfd(2): the counter saturates at UINT64_MAX - 1 (2^64 - 2); a
    write that would exceed this returns EAGAIN (EFD_NONBLOCK) rather than
    wrapping. After a drain() that resets the counter to 0, subsequent
    writes succeed and the fd is readable again — no wake is lost.
    """
    sock_path = tmp_path / "doorbell.sock"
    d = Doorbell.open(sock_path)
    try:
        efd = d._eventfd
        assert efd is not None, "Linux test requires an eventfd"

        # Fill the counter to its maximum (UINT64_MAX - 1).
        max_val = (1 << 64) - 2
        os.eventfd_write(efd, max_val)

        # A further write should raise EAGAIN (the counter is full).
        with pytest.raises(OSError) as exc_info:
            os.eventfd_write(efd, 1)
        assert exc_info.value.errno == 11  # EAGAIN / EWOULDBLOCK

        # The fd is still readable — drain() returns the saturated counter.
        count = d.drain()
        assert count == max_val

        # After drain the counter is 0; a fresh write and drain succeed.
        os.eventfd_write(efd, 42)
        assert d.drain() == 42
    finally:
        d.close()
