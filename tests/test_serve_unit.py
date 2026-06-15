"""Unit tests for the ``waitbus serve`` supervisor-hardening fixes.

Covers the failure paths the happy-path suites cannot reach: a daemon
startup failure surfacing as one clean stderr line and exit 1 (instead
of a raw traceback), the daemon-crash-during-shutdown exit-code mapping
(a teardown that consumes the daemon's exception must not exit 0), the
bounded await on a wedged poll tick, and the docker watcher's
cross-thread stop seam. Runs in-process (like ``tests/test_cli_serve.py``)
so the CI coverage run traces ``waitbus/cli/serve.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
import typer

from waitbus.cli import serve
from waitbus.sources import docker_watch

if TYPE_CHECKING:
    from socketserver import BaseServer

    from waitbus.broadcast import Broadcast

# ---------------------------------------------------------------------------
# Fixtures (mirrors tests/test_cli_serve.py so both files stay standalone)
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_config() -> Generator[None, None, None]:
    """Clear the config cache before and after a test that mutates env vars."""
    from waitbus import _config

    _config._reset_for_test()
    yield
    _config._reset_for_test()


class _StubDaemon:
    """Bare-minimum Broadcast stand-in: ``_teardown`` only awaits ``stop()``."""

    def __init__(self, on_stop: asyncio.Event | None = None) -> None:
        self._on_stop = on_stop

    async def stop(self) -> None:
        if self._on_stop is not None:
            self._on_stop.set()


# ---------------------------------------------------------------------------
# Startup failure exits 1 via one clean stderr line, no traceback
# ---------------------------------------------------------------------------


def test_serve_cmd_daemon_startup_failure_exits_1_with_clean_stderr(
    serve_dirs: dict[str, Path],
    fresh_config: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A daemon that cannot bind exits 1 through one serve: line, no traceback."""
    # A directory squatting on the socket path makes the daemon's manual
    # bind path (unlink-then-bind) raise during startup, while the
    # supervisor's connect-probe still reports "nothing serving here".
    (serve_dirs["runtime"] / "broadcast.sock").mkdir()
    with pytest.raises(typer.Exit) as excinfo:
        serve.serve_cmd(
            components="broadcast",
            all_components=False,
            no_listener=False,
            poll=False,
            docker_socket="/nonexistent/docker.sock",
        )
    assert excinfo.value.exit_code == 1
    err = capsys.readouterr().err
    assert "serve: broadcast daemon failed during startup" in err
    assert "Traceback" not in err


# ---------------------------------------------------------------------------
# A daemon crash consumed during teardown must not exit 0
# ---------------------------------------------------------------------------


async def test_teardown_returns_exception_of_already_dead_daemon() -> None:
    """The exception retrieved from a pre-dead daemon task is returned, not dropped."""

    async def _boom() -> int:
        raise RuntimeError("daemon crashed")

    daemon_task = asyncio.create_task(_boom())
    await asyncio.wait({daemon_task})
    outcome = await serve._teardown(cast("Broadcast", _StubDaemon()), daemon_task, None, None, None, [])
    assert isinstance(outcome, RuntimeError)
    assert str(outcome) == "daemon crashed"


async def test_teardown_returns_exception_raised_during_bounded_join() -> None:
    """A daemon that dies between stop() and the bounded join is surfaced too."""
    release = asyncio.Event()

    async def _crash_after_stop() -> int:
        await release.wait()
        raise OSError("flush failed")

    daemon_task = asyncio.create_task(_crash_after_stop())
    await asyncio.sleep(0)  # let the task start awaiting
    outcome = await serve._teardown(cast("Broadcast", _StubDaemon(on_stop=release)), daemon_task, None, None, None, [])
    assert isinstance(outcome, OSError)
    assert str(outcome) == "flush failed"


async def test_teardown_returns_none_on_clean_stop(capsys: pytest.CaptureFixture[str]) -> None:
    """The clean path keeps its no-exception outcome (exit 0 upstream)."""
    release = asyncio.Event()

    async def _clean_exit() -> int:
        await release.wait()
        return 0

    daemon_task = asyncio.create_task(_clean_exit())
    await asyncio.sleep(0)
    outcome = await serve._teardown(cast("Broadcast", _StubDaemon(on_stop=release)), daemon_task, None, None, None, [])
    assert outcome is None
    assert "serve: stopped" in capsys.readouterr().out


async def test_run_serve_maps_shutdown_crash_to_exit_1(
    serve_dirs: dict[str, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fresh_config: None,
) -> None:
    """A non-None teardown outcome turns an otherwise-clean run into exit 1."""
    real_teardown = serve._teardown

    async def _crashing_teardown(
        daemon: Broadcast,
        daemon_task: asyncio.Task[int],
        listener_handle: tuple[BaseServer, threading.Thread] | None,
        fs_handle: tuple[threading.Event, threading.Thread] | None,
        docker_handle: tuple[docker_watch.WatchStopper, threading.Thread] | None,
        poll_tasks: list[asyncio.Task[None]],
    ) -> BaseException | None:
        # Run the real teardown so the daemon actually stops (no leak into
        # later tests), then report a synthetic daemon crash.
        await real_teardown(daemon, daemon_task, listener_handle, fs_handle, docker_handle, poll_tasks)
        return RuntimeError("synthetic shutdown crash")

    monkeypatch.setattr(serve, "_teardown", _crashing_teardown)
    stop = asyncio.Event()
    run_task = asyncio.create_task(
        serve._run_serve(
            frozenset({"broadcast"}),
            poll=False,
            docker_socket=str(tmp_path / "no-docker.sock"),
            stop_event=stop,
        )
    )
    sock_path = serve_dirs["runtime"] / "broadcast.sock"
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline and not sock_path.exists():
        assert not run_task.done(), f"supervisor exited early: {run_task!r}"
        await asyncio.sleep(0.02)
    assert sock_path.exists(), "broadcast socket never appeared"
    stop.set()
    rc = await asyncio.wait_for(run_task, timeout=15.0)
    assert rc == 1
    err = capsys.readouterr().err
    assert "serve: broadcast daemon failed during shutdown: RuntimeError: synthetic shutdown crash" in err


# ---------------------------------------------------------------------------
# A wedged poll tick must not pin the loop (or teardown) forever
# ---------------------------------------------------------------------------


async def test_hung_poll_tick_is_bounded_and_loop_survives(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A tick past the budget logs poll_tick_timeout; the loop and cancel stay live."""
    release = threading.Event()
    calls = {"n": 0}

    def _tick() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            # Wedge only the first tick; the bound is released in the
            # finally block so the abandoned thread exits with the test.
            release.wait(10.0)
        return 0

    monkeypatch.setattr(serve, "_POLL_TICK_TIMEOUT_S", 0.05)
    caplog.set_level(logging.WARNING, logger="waitbus.serve")
    task = asyncio.create_task(serve._poll_loop(0.01, _tick, "etag_poll"))
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not any(
            "poll_tick_timeout" in record.getMessage() for record in caplog.records
        ):
            await asyncio.sleep(0.02)
        assert any("poll_tick_timeout" in record.getMessage() for record in caplog.records), (
            "the hung tick never produced a poll_tick_timeout warning"
        )
        assert not task.done(), "the poll loop died on a hung tick"
        # The loop moved on to a second tick despite the wedged first one.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and calls["n"] < 2:
            await asyncio.sleep(0.02)
        assert calls["n"] >= 2, "the poll loop never ticked again after the timeout"
        # Cancellation (the teardown path) is not held hostage by the
        # still-running abandoned tick thread.
        started = time.monotonic()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert time.monotonic() - started < 1.0
    finally:
        release.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ---------------------------------------------------------------------------
# The docker watcher stop seam wakes the blocking /events read
# ---------------------------------------------------------------------------


def _start_fake_events_server(
    sock_path: Path,
    responded: threading.Event,
    held: list[socket.socket],
) -> tuple[socket.socket, threading.Thread]:
    """AF_UNIX server speaking just enough HTTP to wedge a /events read.

    Accepts connections in a loop: a probe (connect then close, no
    bytes) is dropped; a GET /events request is answered with
    chunked-response headers and then held open with no chunk ever
    sent — the client blocks reading the first chunk, which is exactly
    the wedge the stop seam must be able to wake.
    """
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(4)

    def _serve() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return  # server closed: test over
            conn.settimeout(10.0)
            try:
                data = conn.recv(65536)
            except OSError:
                data = b""
            if b"GET /events" in data:
                with contextlib.suppress(OSError):
                    conn.sendall(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                held.append(conn)
                responded.set()
            else:
                conn.close()

    thread = threading.Thread(target=_serve, name="fake-docker-events", daemon=True)
    thread.start()
    return server, thread


def test_watch_stopper_wakes_blocked_events_read(tmp_path: Path) -> None:
    """stop() wakes the blocking chunked read and watch() returns 0 promptly."""
    sock_path = tmp_path / "docker.sock"
    responded = threading.Event()
    held: list[socket.socket] = []
    server, server_thread = _start_fake_events_server(sock_path, responded, held)
    stopper = docker_watch.WatchStopper()
    result: list[int] = []

    def _run_watch() -> None:
        result.append(
            docker_watch.watch(
                socket_path=str(sock_path),
                db_path=tmp_path / "events.db",
                stopper=stopper,
            )
        )

    watcher = threading.Thread(target=_run_watch, name="watch-under-test", daemon=True)
    watcher.start()
    try:
        assert responded.wait(5.0), "watch never issued its /events GET"
        stopper.stop()
        watcher.join(5.0)
        assert not watcher.is_alive(), "stop() did not wake the blocking read"
        assert result == [0]
    finally:
        stopper.stop()
        server.close()
        server_thread.join(5.0)
        for conn in held:
            conn.close()


def test_start_docker_watch_handle_stops_and_joins_promptly(tmp_path: Path) -> None:
    """The supervisor's (stopper, thread) handle joins within the bounded budget."""
    sock_path = tmp_path / "docker.sock"
    responded = threading.Event()
    held: list[socket.socket] = []
    server, server_thread = _start_fake_events_server(sock_path, responded, held)
    statuses: list[serve.ComponentStatus] = []
    handle = serve._start_docker_watch(str(sock_path), statuses)
    try:
        assert handle is not None
        stopper, thread = handle
        assert statuses[0].started is True
        assert responded.wait(5.0), "the supervised watcher never issued its /events GET"
        stopper.stop()
        thread.join(serve._TEARDOWN_TIMEOUT_S)
        assert not thread.is_alive(), "the docker watcher outlived the teardown budget"
    finally:
        if handle is not None:
            handle[0].stop()
        server.close()
        server_thread.join(5.0)
        for conn in held:
            conn.close()
