"""Stdlib-only sd_notify helper shared by all waitbus daemons.

Writes to the ``$NOTIFY_SOCKET`` socket when running under systemd.
No-ops cleanly when the variable is unset (macOS launchd, manual run,
container without systemd). Uses only Python stdlib; no ``python3-systemd``
dependency required.
"""

from __future__ import annotations

import logging
import os
import socket

from ._log import structured

logger = logging.getLogger("waitbus.sdnotify")


def sd_notify(payload: bytes) -> None:
    """Send a state update to systemd via ``$NOTIFY_SOCKET``. No-op when unset.

    Args:
        payload: Newline-terminated key=value pairs per sd_notify(3), e.g.
            ``b"READY=1\\n"`` or ``b"STOPPING=1\\n"``.
    """
    path = os.environ.get("NOTIFY_SOCKET")
    if not path:
        return
    if path[0] == "@":
        path = "\x00" + path[1:]  # abstract namespace
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.connect(path)
            s.sendall(payload)
    except OSError as exc:
        structured(logger, logging.WARNING, "sd_notify_failed", error=str(exc))
