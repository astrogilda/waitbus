"""Shared stdlib HTTP-server base for waitbus's loopback HTTP surfaces.

One subclass, two consumers — the webhook listener and the broadcast
daemon's opt-in ``/metrics`` endpoint — so the restart/exit semantics of
every waitbus HTTP socket are defined in exactly one place. Deliberately
import-light (stdlib only): the metrics module must not pull the
listener's import graph into the daemon.
"""

from __future__ import annotations

from http.server import ThreadingHTTPServer


class ReusableThreadingServer(ThreadingHTTPServer):
    """ThreadingHTTPServer with immediate rebind and non-blocking exit.

    ``allow_reuse_address`` (SO_REUSEADDR): on Linux, TIME_WAIT for a
    closed TCP socket can hold the bind port for ~60 s after a daemon
    restart; forcing reuse lets the next invocation rebind immediately,
    avoiding spurious systemd restart failures during sub-second
    ``systemctl --user restart`` cycles.

    ``daemon_threads``: request workers must not block process exit when
    SIGTERM arrives mid-request; the OS reaps them with the parent
    (``socketserver.ThreadingMixIn`` defaults to False otherwise).
    """

    allow_reuse_address = True
    daemon_threads = True
