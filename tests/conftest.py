"""Shared pytest fixtures for the waitbus test suite.

The fixtures rely on the `waitbus` package being importable via the
editable install declared in `pyproject.toml`.

Fixtures:
- `gh_secret`, `am_secret`: stable HMAC keys for listener tests.
- `hmac_sig`: signer factory for arbitrary (secret, body) pairs.
- `tmp_db_path`: per-test SQLite path with the canonical schema applied.
- `server_fixture`: live `HTTPServer` on 127.0.0.1:0 dispatching to the
  listener's `WebhookHandler`, with path factory functions rebound via
  `monkeypatch` so the operator's real DB and the WebhookHandler class
  attributes are restored at test teardown.
- `broadcast_paths`: tmp_path-based DB + sockets for the broadcast daemon,
  with path factory functions rebound via monkeypatch.
- `running_daemon`: spins up `waitbus.broadcast.Broadcast` against the
  tmp paths inside the pytest event loop; tears down on yield-return.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import hashlib
import hmac
import sys
import tempfile
import threading
import time
from collections.abc import AsyncGenerator, Callable, Generator
from dataclasses import dataclass
from http.server import HTTPServer
from pathlib import Path

import pytest
import pytest_asyncio

from waitbus import _db, broadcast, listener


def pytest_configure(config: pytest.Config) -> None:
    """Shorten pytest's basetemp on macOS so AF_UNIX paths fit in 104 chars.

    macOS limits ``sockaddr_un.sun_path`` to roughly 104 characters. The
    default pytest basetemp on macOS runners resolves under
    ``/var/folders/.../T/pytest-of-runner/pytest-N/test_<name>_0/`` which
    eats most of that budget before tests even append a socket filename.
    AF_UNIX bind() then fails with ``OSError: AF_UNIX path too long``.

    On Linux the limit is 108 and the default basetemp is short enough
    (``/tmp/pytest-of-<user>/...``) that this is a non-issue, so the
    override fires only on darwin and only when the user has not set
    ``--basetemp`` explicitly. The fresh ``/tmp/p_<hex>`` directory keeps
    test paths short enough that every AF_UNIX bind succeeds.
    """
    if sys.platform == "darwin" and not config.option.basetemp:
        # tempfile.mkdtemp uses O_EXCL semantics — atomic, no token-collision race.
        config.option.basetemp = tempfile.mkdtemp(prefix="p_", dir="/tmp")


@pytest.fixture
def serve_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """Isolated state/runtime/config dirs exported via the WAITBUS_*_DIR env vars.

    The shared in-process isolation triple (subprocess-shaped callers build
    an env dict via tests/_daemon_helpers.isolated_subprocess_env instead).
    """
    dirs = {
        "state": tmp_path / "state",
        "runtime": tmp_path / "runtime",
        "config": tmp_path / "config",
    }
    for d in dirs.values():
        d.mkdir(exist_ok=True)
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(dirs["state"]))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(dirs["runtime"]))
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(dirs["config"]))
    monkeypatch.delenv("WAITBUS_FS_WATCH_PATH", raising=False)
    # Per-test state dir has no secrets.json, so the default is secret-free
    # (the broadcast/wait path needs no secrets). Clear the secret read cache
    # so a prior test's secrets.json under a different state dir is not seen.
    from waitbus import _secrets

    _secrets._reset_cache_for_test()
    return dirs


@pytest.fixture
def gh_secret() -> bytes:
    return b"github-secret-fixture"


@pytest.fixture(autouse=True)
def _force_gc_after_test() -> Generator[None, None, None]:
    """Force a full ``gc.collect()`` at the end of every test.

    Pins resource-leak ``__del__`` finalisers to the test that owns the
    resource so the ``PytestUnraisableExceptionWarning`` gate is stable.
    """
    yield
    gc.collect()


@pytest.fixture
def am_secret() -> bytes:
    return b"alertmanager-secret-fixture"


@pytest.fixture
def hmac_sig() -> Callable[[bytes, bytes], str]:
    """Factory: sign an arbitrary (secret, body) pair into an X-Hub-Signature-256 value."""

    def _sign(secret: bytes, body: bytes) -> str:
        return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()

    return _sign


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Per-test SQLite path with the canonical schema applied."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    return db


@dataclass
class ServerHandle:
    db_path: Path
    port: int

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"


@pytest.fixture
def broadcast_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """Per-test DB + broadcast/doorbell sockets, with path factories redirected.

    The DB is intentionally NOT pre-created. The broadcast daemon now
    invokes `_db.ensure_schema` itself at startup; pre-seeding the schema
    here would mask the cold-start regression covered by
    `test_starts_on_empty_db`.

    Socket paths cannot be expressed as env vars (they are arbitrary tmp_path
    locations, not XDG-rooted), so we patch the _paths factory functions
    directly. The doorbell factory is patched on _db._doorbell so
    insert_event rings the daemon-under-test's socket, not the operator's.
    """
    db = tmp_path / "events.db"
    broadcast_sock = tmp_path / "broadcast.sock"
    doorbell_sock = tmp_path / "doorbell.sock"
    # broadcast.py imports broadcast_socket and doorbell_socket directly from
    # _paths, so patching must redirect the local references in that module.
    monkeypatch.setattr(broadcast, "broadcast_socket", lambda: broadcast_sock)
    monkeypatch.setattr(broadcast, "doorbell_socket", lambda: doorbell_sock)
    # The wire's SubscribeAckFrame.heartbeat_sec is int (NATS INFO precedent;
    # multilingual JSON-int decoders); 0.2 here was truncating to 0 on the
    # wire and forced the conformance suite to drop the positive-value
    # assertion. Use an integer cadence that's still fast enough for the
    # heartbeat-presence test to observe at least one tick within its own
    # deadline (the 1 s cadence gives 1-2 heartbeats in the 1.5 s window
    # the relevant test waits, with comfortable margin).
    monkeypatch.setenv("WAITBUS_HEARTBEAT_SEC", "1")
    # Clear the config cache so Broadcast.__init__ picks up the env override above.
    # Without this, a cached config (loaded at listener.py import time) would carry
    # heartbeat_sec=60.0 into every daemon, breaking the heartbeat timing test.
    from waitbus import _config

    _config._reset_for_test()
    # Doorbell inside insert_event must land at the daemon under test, not
    # the operator's real socket.
    monkeypatch.setattr(_db._doorbell, "doorbell_socket", lambda: doorbell_sock)
    return {"db": db, "broadcast": broadcast_sock, "doorbell": doorbell_sock}


@pytest_asyncio.fixture
async def running_daemon(
    broadcast_paths: dict[str, Path],
) -> AsyncGenerator[tuple[broadcast.Broadcast, dict[str, Path]], None]:
    """Spin up the broadcast daemon in-loop; yield (daemon, paths)."""
    daemon = broadcast.Broadcast(db_path=str(broadcast_paths["db"]))
    task = asyncio.create_task(daemon.run())
    # Wait for the listener socket to appear before yielding.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if broadcast_paths["broadcast"].exists():
            break
        await asyncio.sleep(0.02)
    else:
        task.cancel()
        raise RuntimeError("daemon failed to bind broadcast socket")
    try:
        yield daemon, broadcast_paths
    finally:
        # Graceful stop via the public event, NOT task.cancel(). Cancellation
        # injects CancelledError into ``run()``'s ``await stop_event.wait()``,
        # which races the ``finally`` block: the in-flight ``_handle_accept``
        # tasks get cancelled but their done-callbacks may not fire before
        # pytest-asyncio reclaims the per-test event loop, leaving sockets
        # to be reaped at GC time by ``__del__`` — surfacing as
        # ``ResourceWarning`` / ``PytestUnraisableExceptionWarning`` in a
        # later test. ``daemon.stop()`` sets the event without cancellation
        # so the finally block runs to completion before the awaiter returns.
        await daemon.stop()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(task, timeout=5.0)


@pytest.fixture
def server_fixture(
    tmp_db_path: Path,
    gh_secret: bytes,
    am_secret: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[ServerHandle, None, None]:
    """Live HTTPServer on an ephemeral port pointed at the tmp DB.

    monkeypatch rebinds the module-level DB path and the WebhookHandler
    class attributes; both are reverted automatically at teardown,
    eliminating cross-test state leakage.
    """
    monkeypatch.setattr(listener, "db_path", lambda: tmp_db_path)
    monkeypatch.setattr(listener.WebhookHandler, "secret", gh_secret)
    monkeypatch.setattr(listener.WebhookHandler, "am_secret", am_secret)

    server = HTTPServer(("127.0.0.1", 0), listener.WebhookHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield ServerHandle(db_path=tmp_db_path, port=port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


# ---------------------------------------------------------------------------
# Plugin-registry + XDG-config isolation for CLI tests
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_waitbus_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Redirect XDG_CONFIG_HOME + reset the plugin registry around a CLI test.

    Shared seam for CLI verb tests that touch either the publisher
    allowlist (``$XDG_CONFIG_HOME/waitbus/plugins.allowlist.toml``) or
    the in-process plugin registry (``_PLUGIN_SOURCES`` /
    ``_PLUGIN_PUBLISHERS``). Yields the redirected config root so
    tests can assert against the on-disk file layout without
    duplicating the redirection boilerplate.

    Cleans plugin-source state via ``_clear_for_test_isolation``
    both before and after the test so registrations from one test
    cannot leak into the next. Removes any env-var overrides that
    could bleed from the outer environment.
    """
    from waitbus.sources._registry import _clear_for_test_isolation

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.delenv("WAITBUS_DISABLE_SOURCE_AUTOLOAD", raising=False)
    monkeypatch.delenv("WAITBUS_PLUGINS", raising=False)
    _clear_for_test_isolation()
    try:
        yield tmp_path
    finally:
        _clear_for_test_isolation()
