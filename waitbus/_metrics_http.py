"""Opt-in loopback HTTP server for the broadcast daemon's ``/metrics``.

OFF by default: no socket is opened unless ``CiStatusConfig.metrics_port``
is set (env ``WAITBUS_METRICS_PORT`` or the ``--metrics-port`` flag on
``waitbus broadcast serve``). The bind host is hardcoded to ``127.0.0.1``
-- there is no configuration path to a public interface. The server runs
``serve_forever`` on a daemon thread, so the daemon's asyncio event loop
is untouched; the handler only ever calls :func:`waitbus._metrics.render`,
whose prometheus_client instruments are internally locked, and never
touches daemon state. Scrapes are not registry-atomic: counters updated
mid-render may be mutually torn relative to each other, which is benign
for monotonic counters.
"""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler
from typing import Any

from ._http import ReusableThreadingServer

logger = logging.getLogger(__name__)

_METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


class _MetricsHandler(BaseHTTPRequestHandler):
    """Serve ``GET /metrics`` from the process-wide metrics registry."""

    def do_GET(self) -> None:
        """Return the Prometheus text exposition for ``/metrics``, else 404."""
        if self.path != "/metrics":
            body = b"not found\n"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        from . import _metrics

        payload = _metrics.render()
        self.send_response(200)
        self.send_header("Content-Type", _METRICS_CONTENT_TYPE)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        """Route per-request lines through the module logger at DEBUG.

        The BaseHTTPRequestHandler default writes every request to
        stderr, which would interleave scrape noise with the daemon's
        structured log stream.
        """
        logger.debug("metrics_http %s", format % args)


class MetricsServer:
    """Lifecycle wrapper around the loopback metrics HTTP server.

    ``start()`` binds ``127.0.0.1:<port>`` (port 0 binds an OS-assigned
    ephemeral port, resolved via :attr:`port`) and spawns the serve
    thread; ``stop()`` is idempotent and joins the thread.
    """

    def __init__(self, port: int) -> None:
        self._requested_port = port
        self._server: ReusableThreadingServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Bind the loopback socket and start serving on a daemon thread."""
        self._server = ReusableThreadingServer(("127.0.0.1", self._requested_port), _MetricsHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="waitbus-metrics",
            daemon=True,
        )
        self._thread.start()

    @property
    def host(self) -> str:
        """Bound host -- always ``127.0.0.1``; raises if not started."""
        if self._server is None:
            raise RuntimeError("MetricsServer not started")
        return str(self._server.server_address[0])

    @property
    def port(self) -> int:
        """Bound port (resolves port 0 to the OS-assigned ephemeral port)."""
        if self._server is None:
            raise RuntimeError("MetricsServer not started")
        return int(self._server.server_address[1])

    def stop(self) -> None:
        """Shut down the server and join the serve thread. Idempotent."""
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
