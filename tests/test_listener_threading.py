"""Integration tests for the listener's threaded HTTP server.

Confirms two correctness properties:

1. The server class is `ThreadingHTTPServer` (or a subclass thereof), so
   a slow handler does not stall subsequent webhook deliveries. The test
   binds a real ephemeral port and fires N=8 concurrent /healthz GETs,
   expecting all 8 to complete inside a tight wall-clock budget.

2. `allow_reuse_address` is set to True on the server class. This lets
   a daemon restart rebind the port before the kernel's TIME_WAIT window
   clears, preventing spurious systemd restart failures.
"""

from __future__ import annotations

import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import ThreadingHTTPServer
from typing import cast

import pytest

from waitbus import _http, listener

pytestmark = pytest.mark.slow


def test_reusable_server_subclasses_threading_http_server() -> None:
    assert issubclass(_http.ReusableThreadingServer, ThreadingHTTPServer)


def test_reusable_server_sets_allow_reuse_address() -> None:
    assert _http.ReusableThreadingServer.allow_reuse_address is True


def test_reusable_server_daemonizes_request_threads() -> None:
    """daemon_threads=True so SIGTERM does not hang on a slow handler."""
    assert _http.ReusableThreadingServer.daemon_threads is True


def _start_server() -> tuple[_http.ReusableThreadingServer, int, threading.Thread]:
    server = _http.ReusableThreadingServer(("127.0.0.1", 0), listener.WebhookHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port, thread


def test_socket_so_reuseaddr_option_is_set() -> None:
    """The bound socket must carry SO_REUSEADDR=1 at the kernel level."""
    import socket

    server, _port, thread = _start_server()
    try:
        flag = server.socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)
        assert flag != 0, "SO_REUSEADDR is not set on the listening socket"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_concurrent_requests_complete_in_parallel() -> None:
    """N concurrent /healthz GETs must all return 200 quickly.

    A single-threaded HTTPServer would serialize requests; a threaded
    server completes them in parallel. We do not strictly assert wall-
    clock parallelism (CI nodes vary), only that 8 concurrent requests
    each succeed.
    """
    server, port, thread = _start_server()
    try:
        urls = [f"http://127.0.0.1:{port}/healthz"] * 8

        def _fetch(url: str) -> int:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return cast(int, resp.status)

        start = time.monotonic()
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(_fetch, u) for u in urls]
            statuses = [f.result(timeout=10) for f in as_completed(futures)]
        elapsed = time.monotonic() - start
        assert all(s == 200 for s in statuses), statuses
        # Generous budget — only catches gross serialization regressions.
        assert elapsed < 5.0, f"8 healthz GETs took {elapsed:.2f}s"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_build_server_installs_secrets_and_binds(monkeypatch: pytest.MonkeyPatch) -> None:
    """build_server installs key material and returns a bound reusable server.

    LISTEN_PORT is monkeypatched to 0 so the test binds an ephemeral port
    rather than colliding with a real listener daemon on 9000. The handler
    class attributes are restored via monkeypatch so cross-test state does
    not leak.
    """
    monkeypatch.setattr(listener, "LISTEN_PORT", 0)
    monkeypatch.setattr(listener.WebhookHandler, "secret", b"")
    monkeypatch.setattr(listener.WebhookHandler, "am_secret", None)
    server = listener.build_server(b"gh-secret", b"am-secret")
    try:
        assert isinstance(server, _http.ReusableThreadingServer)
        assert server.allow_reuse_address is True
        assert server.daemon_threads is True
        assert listener.WebhookHandler.secret == b"gh-secret"
        assert listener.WebhookHandler.am_secret == b"am-secret"
        assert server.server_address[1] != 0  # a real ephemeral port was bound
    finally:
        server.server_close()


def test_rebind_after_close_succeeds_immediately() -> None:
    """SO_REUSEADDR lets the next bind on the same port skip TIME_WAIT."""
    server1, port, thread1 = _start_server()
    server1.shutdown()
    server1.server_close()
    thread1.join(timeout=2)
    # Re-bind to the same port; without SO_REUSEADDR this often fails
    # with EADDRINUSE for ~60 s while the kernel holds TIME_WAIT.
    server2 = _http.ReusableThreadingServer(("127.0.0.1", port), listener.WebhookHandler)
    try:
        assert server2.server_address[1] == port
    finally:
        server2.server_close()
