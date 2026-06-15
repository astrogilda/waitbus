"""Unit tests for the peer-credential UID primitive.

The Linux branch exercises SO_PEERCRED on a real AF_UNIX socketpair; the
macOS branch exercises getpeereid() via ctypes on a real socketpair. The
"unsupported platform" path is exercised by importing the module under a
fabricated ``sys.platform`` so the falling-off-the-end stub returns the
expected OSError without needing actual non-Linux / non-macOS runtime.
"""

from __future__ import annotations

import importlib
import os
import socket
import struct
import sys
from typing import Any

import pytest

from waitbus import _peercred


@pytest.mark.skipif(sys.platform != "linux", reason="SO_PEERCRED is Linux-only")
def test_peer_uid_returns_current_uid_on_linux() -> None:
    """SO_PEERCRED-derived UID equals os.getuid() on a same-process pair.

    A SOCK_STREAM AF_UNIX socketpair has both endpoints owned by the
    test process, so SO_PEERCRED on either side reports the test
    process's own UID.
    """
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        assert _peercred.peer_uid(a) == os.getuid()
        assert _peercred.peer_uid(b) == os.getuid()
    finally:
        a.close()
        b.close()


@pytest.mark.skipif(sys.platform != "linux", reason="SO_PEERCRED struct layout is Linux-only")
def test_peer_uid_high_uid_not_misread_as_negative() -> None:
    """A peer UID >= 2**31 must decode as the same positive integer.

    Regression: ``struct ucred``'s ``uid_t`` / ``gid_t`` are UNSIGNED. Unpacking
    them as signed (``"3i"``) misreads any UID >= 2**31 -- rootless Podman, high
    subuid/subgid maps -- as negative, which then never equals the daemon's own
    ``os.getuid()`` and fail-closes the same-UID gate against its own peers. We
    craft the raw native kernel bytes (unsigned uid/gid) and confirm
    ``peer_uid`` decodes them positive, regardless of the running user's UID.
    """
    high_uid = 2**31 + 1234  # 2_147_484_882 -- outside the signed 32-bit range
    raw = struct.pack("=iII", 4321, high_uid, high_uid)  # pid (signed), uid, gid (unsigned)

    class _FakeSock:
        def getsockopt(self, level: int, optname: int, buflen: int) -> bytes:
            return raw

    fake: Any = _FakeSock()
    assert _peercred.peer_uid(fake) == high_uid


@pytest.mark.skipif(sys.platform != "darwin", reason="getpeereid() is macOS-only")
def test_peer_uid_returns_current_uid_on_macos() -> None:
    """getpeereid()-derived UID equals os.getuid() on a same-process pair.

    The ctypes binding resolves to libc on macOS; both endpoints of the
    socketpair are owned by the test process, so the EUID returned is
    the test process's own UID.
    """
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        assert _peercred.peer_uid(a) == os.getuid()
        assert _peercred.peer_uid(b) == os.getuid()
    finally:
        a.close()
        b.close()


@pytest.mark.skipif(sys.platform != "darwin", reason="ctypes binding is macOS-only")
def test_getpeereid_ctypes_signature() -> None:
    """Verify the ctypes binding for getpeereid() matches the man-page signature.

    getpeereid(int s, uid_t *euid, gid_t *egid) -> int. uid_t and gid_t
    are 32-bit unsigned on macOS (matching the POSIX standard). A drift
    in the ctypes argtypes would surface either as a TypeError at call
    time or, worse, as silent corruption of the EUID/EGID return values.
    """
    import ctypes

    binding = _peercred._getpeereid  # type: ignore[attr-defined]
    assert binding.restype is ctypes.c_int
    assert len(binding.argtypes) == 3
    assert binding.argtypes[0] is ctypes.c_int
    # POINTER(c_uint32) instances compare equal by ._type_, not by identity;
    # check the target type explicitly.
    assert binding.argtypes[1]._type_ is ctypes.c_uint32
    assert binding.argtypes[2]._type_ is ctypes.c_uint32


def test_peer_uid_raises_on_unsupported_platform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-imported with sys.platform set to a non-supported value, the
    module's else-branch installs a stub that raises OSError on call.

    importlib.reload() under a patched sys.platform exercises the
    module's top-level if/elif/else dispatch. The stub must raise
    OSError with a message mentioning the unsupported platform so
    operators get a clear error rather than a cryptic AttributeError.
    """
    original_platform = sys.platform
    monkeypatch.setattr(sys, "platform", "freebsd")
    reloaded = importlib.reload(_peercred)
    try:
        a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            with pytest.raises(OSError, match="freebsd"):
                reloaded.peer_uid(a)
        finally:
            a.close()
            b.close()
    finally:
        # Restore the canonical module state so subsequent tests see the
        # real platform branch.
        monkeypatch.setattr(sys, "platform", original_platform)
        importlib.reload(_peercred)
