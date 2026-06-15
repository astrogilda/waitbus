"""Peer-credential UID check for AF_UNIX SOCK_STREAM connections.

Used by the broadcast daemon to verify that a connecting client is
running as the same UID as the daemon itself — a single-user-workstation
trust boundary. Platform-dispatched at the leaf function:

- **Linux**: ``SO_PEERCRED`` returns ``struct ucred {pid, uid, gid}``.
  Standard since 2.6; documented in unix(7).
- **macOS**: ``getpeereid()`` returns ``(uid, gid)`` from the connected
  AF_UNIX peer. Documented in getpeereid(3). This matches dbus-on-Darwin's
  documented posture.

We deliberately do NOT use ``LOCAL_PEERPID`` on macOS — it is exploitable
per CVE-2017-7004 (Ian Beer), CVE-2020-14977 (F-Secure / Reguła / Alkemade), and the
AMFI tightening trail. UID-only auth is sufficient for the
single-user-workstation threat model.
"""

from __future__ import annotations

import ctypes
import socket
import struct
import sys
from typing import Final

if sys.platform == "linux":
    # SO_PEERCRED is documented in unix(7). The optval is struct ucred:
    #   struct ucred { pid_t pid; uid_t uid; gid_t gid; };
    # pid_t is a SIGNED 32-bit int; uid_t and gid_t are UNSIGNED 32-bit ints
    # (glibc/musl, x86_64/arm64) -- hence "iII", native size/alignment to match
    # the kernel ABI. Unpacking uid/gid as signed ("3i") misreads any value
    # >= 2**31 -- rootless Podman, high subuid/subgid maps -- as negative, so it
    # never equals the daemon's own os.getuid() and fail-closes the same-UID
    # gate against its own peers. The SO_PEERCRED option number is also
    # arch-dependent (17 on x86/arm, e.g. 18 on mips), so resolve it from the
    # stdlib constant where present rather than hardcoding the x86 value.
    _SO_PEERCRED: Final[int] = getattr(socket, "SO_PEERCRED", 17)
    _UCRED_STRUCT: Final[struct.Struct] = struct.Struct("iII")

    def peer_uid(sock: socket.socket) -> int:
        """Return the connected peer's UID via SO_PEERCRED."""
        cred_bytes = sock.getsockopt(socket.SOL_SOCKET, _SO_PEERCRED, _UCRED_STRUCT.size)
        _pid, uid, _gid = _UCRED_STRUCT.unpack(cred_bytes)
        return int(uid)

elif sys.platform == "darwin":
    # getpeereid(int s, uid_t *euid, gid_t *egid) -> 0 on success.
    # libc on macOS is /usr/lib/libc.dylib; CDLL with None resolves it.
    _libc: Final[ctypes.CDLL] = ctypes.CDLL(None, use_errno=True)
    _getpeereid: Final = _libc.getpeereid
    _getpeereid.restype = ctypes.c_int
    _getpeereid.argtypes = [
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
    ]

    def peer_uid(sock: socket.socket) -> int:
        """Return the connected peer's UID via getpeereid()."""
        euid = ctypes.c_uint32(0)
        egid = ctypes.c_uint32(0)
        rc = _getpeereid(sock.fileno(), ctypes.byref(euid), ctypes.byref(egid))
        if rc != 0:
            err = ctypes.get_errno()
            raise OSError(err, f"getpeereid failed: errno {err}")
        return int(euid.value)

else:
    # Unsupported platform — the daemon stack does not run here.
    def peer_uid(sock: socket.socket) -> int:  # pragma: no cover
        raise OSError(f"peer_uid not supported on {sys.platform!r}; waitbus's daemon stack is Linux + macOS only")
