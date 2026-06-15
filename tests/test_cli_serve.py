"""Tests for the ``waitbus serve`` foreground supervisor.

Covers the component-set parser, the broadcast-socket connect probe,
the per-component start/skip helpers, the poll timers' fault isolation,
and an in-process end-to-end run of the supervisor against tmp state
dirs (manifest, subscriber wake, graceful stop, already-running
refusal). The subprocess variant lives in ``tests/test_serve_e2e.py``;
the in-process tests here are the coverage source for
``waitbus/cli/serve.py`` because the CI coverage run does not trace
subprocesses.
"""

from __future__ import annotations

import asyncio
import socket
import time
import urllib.request
from collections.abc import Generator
from pathlib import Path
from typing import cast

import msgspec
import pytest
import typer

from waitbus import _emit as emit_mod
from waitbus import _paths, listener
from waitbus._broadcast_sub import open_subscriber, read_subscribe_ack
from waitbus._frame import sync_read_frame
from waitbus._types import EventInsert
from waitbus.cli import serve
from waitbus.sources import docker_watch

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_config() -> Generator[None, None, None]:
    """Clear the config cache before and after a test that mutates env vars."""
    from waitbus import _config

    _config._reset_for_test()
    yield
    _config._reset_for_test()


def _build_event(delivery_id: str) -> EventInsert:
    """One pytest-shaped event for emit/wake assertions."""
    return EventInsert(
        delivery_id=delivery_id,
        source="pytest",
        event_type="pytest_session",
        owner="serve-test",
        repo="in-process",
        received_at=time.time_ns(),
        payload_json=msgspec.json.encode({"outcome": "pass"}).decode(),
        ingest_method="e2e",
        status="completed",
        conclusion="success",
    )


# ---------------------------------------------------------------------------
# _parse_components
# ---------------------------------------------------------------------------


def test_parse_all_yields_every_component() -> None:
    plan = serve._parse_components(None, all_components=True, no_listener=False)
    assert plan == frozenset(serve._COMPONENTS)


def test_parse_all_no_listener_removes_listener() -> None:
    plan = serve._parse_components(None, all_components=True, no_listener=True)
    assert plan == frozenset(serve._COMPONENTS) - {"listener"}


def test_parse_explicit_subset_is_exact() -> None:
    plan = serve._parse_components("broadcast,fs,docker", all_components=False, no_listener=False)
    assert plan == frozenset({"broadcast", "fs", "docker"})


def test_parse_subset_always_includes_broadcast() -> None:
    plan = serve._parse_components("fs", all_components=False, no_listener=False)
    assert plan == frozenset({"broadcast", "fs"})


def test_parse_unknown_component_rejected() -> None:
    with pytest.raises(typer.BadParameter, match="unknown component"):
        serve._parse_components("fs,teapot", all_components=False, no_listener=False)


def test_parse_empty_subset_rejected() -> None:
    with pytest.raises(typer.BadParameter, match="empty"):
        serve._parse_components(" , ", all_components=False, no_listener=False)


def test_parse_neither_all_nor_subset_rejected() -> None:
    with pytest.raises(typer.BadParameter, match="pass --all"):
        serve._parse_components(None, all_components=False, no_listener=False)


def test_parse_both_all_and_subset_rejected() -> None:
    with pytest.raises(typer.BadParameter, match="not both"):
        serve._parse_components("fs", all_components=True, no_listener=False)


# ---------------------------------------------------------------------------
# _probe_broadcast_bound
# ---------------------------------------------------------------------------


def test_probe_false_when_path_absent(tmp_path: Path) -> None:
    assert serve._probe_broadcast_bound(tmp_path / "missing.sock") is False


def test_probe_true_against_live_listener_and_false_after_close(tmp_path: Path) -> None:
    """A bound, listening socket probes True; a stale path file probes False."""
    sock_path = tmp_path / "live.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    try:
        assert serve._probe_broadcast_bound(sock_path) is True
    finally:
        server.close()
    # The path file survives the close; a connect now refuses.
    assert sock_path.exists()
    assert serve._probe_broadcast_bound(sock_path) is False


# ---------------------------------------------------------------------------
# _format_status
# ---------------------------------------------------------------------------


def test_format_status_started_line() -> None:
    status = serve.ComponentStatus("broadcast", True, "socket /run/x/broadcast.sock")
    assert serve._format_status(status) == "serve: broadcast: started (socket /run/x/broadcast.sock)"


def test_format_status_listener_skip_line_is_exact() -> None:
    status = serve.ComponentStatus("listener", False, "no github-webhook-secret")
    assert serve._format_status(status) == "serve: listener: skipped (no github-webhook-secret)"


# ---------------------------------------------------------------------------
# _start_listener
# ---------------------------------------------------------------------------


def test_start_listener_skips_without_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.delenv("WAITBUS_CREDS_DIR", raising=False)
    statuses: list[serve.ComponentStatus] = []
    assert serve._start_listener(statuses) is None
    assert statuses == [serve.ComponentStatus("listener", False, "no github-webhook-secret")]


def test_start_listener_serves_healthz_with_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With a staged credential the listener thread starts and answers /healthz."""
    creds = tmp_path / "creds"
    creds.mkdir()
    (creds / "github-webhook-secret").write_text("test-secret")
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.setenv("WAITBUS_CREDS_DIR", str(creds))
    monkeypatch.setattr(listener, "LISTEN_PORT", 0)  # ephemeral port, no 9000 collision
    monkeypatch.setattr(listener.WebhookHandler, "secret", b"")
    monkeypatch.setattr(listener.WebhookHandler, "am_secret", None)
    statuses: list[serve.ComponentStatus] = []
    handle = serve._start_listener(statuses)
    assert handle is not None
    server, thread = handle
    try:
        assert statuses[0].started is True
        port = cast("tuple[str, int]", server.server_address)[1]
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5) as resp:
            assert resp.status == 200
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)
    assert not thread.is_alive()


# ---------------------------------------------------------------------------
# _start_fs_watch
# ---------------------------------------------------------------------------


def test_start_fs_watch_skips_when_unconfigured(serve_dirs: dict[str, Path], fresh_config: None) -> None:
    from waitbus import _config

    statuses: list[serve.ComponentStatus] = []
    assert serve._start_fs_watch(_config.get_config(), statuses) is None
    assert statuses == [serve.ComponentStatus("fs", False, "no fs_watch_path configured")]


def test_start_fs_watch_skips_when_path_missing(
    serve_dirs: dict[str, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fresh_config: None,
) -> None:
    from waitbus import _config

    monkeypatch.setenv("WAITBUS_FS_WATCH_PATH", str(tmp_path / "nope"))
    _config._reset_for_test()
    statuses: list[serve.ComponentStatus] = []
    assert serve._start_fs_watch(_config.get_config(), statuses) is None
    assert statuses[0].started is False
    assert "does not exist" in statuses[0].detail


def test_start_fs_watch_starts_and_stops_on_event(
    serve_dirs: dict[str, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fresh_config: None,
) -> None:
    pytest.importorskip("watchdog")
    from waitbus import _config

    watch_dir = tmp_path / "watched"
    watch_dir.mkdir()
    monkeypatch.setenv("WAITBUS_FS_WATCH_PATH", str(watch_dir))
    _config._reset_for_test()
    statuses: list[serve.ComponentStatus] = []
    handle = serve._start_fs_watch(_config.get_config(), statuses)
    assert handle is not None
    stop, thread = handle
    assert statuses[0].started is True
    assert str(watch_dir) in statuses[0].detail
    stop.set()
    thread.join(timeout=10.0)
    assert not thread.is_alive()


# ---------------------------------------------------------------------------
# _start_docker_watch
# ---------------------------------------------------------------------------


def test_start_docker_watch_skips_when_socket_absent(tmp_path: Path) -> None:
    statuses: list[serve.ComponentStatus] = []
    assert serve._start_docker_watch(str(tmp_path / "no-docker.sock"), statuses) is None
    assert statuses[0].started is False
    assert "does not exist" in statuses[0].detail


def test_start_docker_watch_starts_against_connectable_socket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A connectable AF_UNIX socket passes the probe and the thread starts.

    The real ``docker_watch.watch`` would block on the fake socket
    forever; it is monkeypatched to a no-op so the thread exits and the
    test asserts only the probe + start seam.
    """
    sock_path = tmp_path / "docker.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    monkeypatch.setattr(docker_watch, "watch", lambda **kwargs: 0)
    try:
        statuses: list[serve.ComponentStatus] = []
        handle = serve._start_docker_watch(str(sock_path), statuses)
        assert handle is not None
        _stopper, thread = handle
        thread.join(timeout=5.0)
        assert statuses[0].started is True
        assert str(sock_path) in statuses[0].detail
    finally:
        server.close()


# ---------------------------------------------------------------------------
# _start_poll_timers
# ---------------------------------------------------------------------------


async def test_poll_timers_skipped_when_poll_unset() -> None:
    statuses: list[serve.ComponentStatus] = []
    assert serve._start_poll_timers(False, statuses) == []
    assert statuses == [serve.ComponentStatus("poll", False, "--poll not set")]


async def test_poll_timers_tick_and_survive_a_raising_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    """The timers fire repeatedly and a raising tick does not kill the loop."""
    import waitbus.etag_poll as etag_poll_mod
    import waitbus.watchdog_check as watchdog_mod

    counts = {"etag": 0, "watchdog": 0}

    def _raising_etag_tick() -> int:
        counts["etag"] += 1
        raise RuntimeError("synthetic poll failure")

    def _watchdog_tick(argv: list[str] | None = None) -> int:
        counts["watchdog"] += 1
        return 0

    monkeypatch.setattr(etag_poll_mod, "main", _raising_etag_tick)
    monkeypatch.setattr(watchdog_mod, "main", _watchdog_tick)
    monkeypatch.setattr(serve, "_ETAG_POLL_PERIOD_S", 0.01)
    monkeypatch.setattr(serve, "_WATCHDOG_PERIOD_S", 0.01)
    statuses: list[serve.ComponentStatus] = []
    tasks = serve._start_poll_timers(True, statuses)
    assert statuses[0].started is True
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and (counts["etag"] < 2 or counts["watchdog"] < 2):
            await asyncio.sleep(0.02)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert counts["etag"] >= 2, "a raising tick must not stop the poll loop"
    assert counts["watchdog"] >= 2


# ---------------------------------------------------------------------------
# _run_serve in-process end-to-end
# ---------------------------------------------------------------------------


async def test_run_serve_manifest_wake_and_graceful_stop(
    serve_dirs: dict[str, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fresh_config: None,
) -> None:
    """Full in-process supervisor run: manifest, subscriber wake, clean stop."""
    pytest.importorskip("watchdog")
    from waitbus import _config

    watch_dir = tmp_path / "watched"
    watch_dir.mkdir()
    monkeypatch.setenv("WAITBUS_FS_WATCH_PATH", str(watch_dir))
    _config._reset_for_test()

    stop = asyncio.Event()
    run_task = asyncio.create_task(
        serve._run_serve(
            frozenset(serve._COMPONENTS),
            poll=False,
            docker_socket=str(tmp_path / "no-docker.sock"),
            stop_event=stop,
        )
    )
    sock_path = serve_dirs["runtime"] / "broadcast.sock"
    deadline = time.monotonic() + 10.0
    # Barrier on the daemon ACCEPTING (connect-probe), not on the socket
    # file merely existing: the daemon creates the file a beat before it
    # listens, so an exists() barrier can release while the supervisor's own
    # _await_socket probe is still failing. Setting stop in that window makes
    # the supervisor fall straight to teardown without ever printing the
    # manifest (the order-dependent "out == 'serve: stopped\n'" flake).
    while time.monotonic() < deadline and not serve._probe_broadcast_bound(sock_path, timeout=0.05):
        assert not run_task.done(), f"supervisor exited early: {run_task!r}"
        await asyncio.sleep(0.02)
    assert serve._probe_broadcast_bound(sock_path, timeout=0.05), "broadcast socket never accepted"

    # Subscriber wake: emit one event against the real-resolved store and
    # read it back on an open subscriber socket.
    sub = open_subscriber(socket_path=str(sock_path))
    try:
        # Registration barrier: emitting before the daemon has confirmed
        # the subscription would race the fan-out and drop the event.
        await asyncio.to_thread(read_subscribe_ack, sub)
        delivery_id = f"serve-test:{time.time_ns()}"
        woke = False
        emit_deadline = time.monotonic() + 10.0
        emit_mod.emit_batch([_build_event(delivery_id)], db_path=_paths.db_path())
        while time.monotonic() < emit_deadline:
            frame_bytes = await asyncio.to_thread(sync_read_frame, sub.sock)
            if frame_bytes is None:
                continue
            frame = msgspec.json.decode(frame_bytes, type=dict)
            if frame.get("delivery_id") == delivery_id:
                woke = True
                break
        assert woke, "subscriber did not receive the emitted event"
    finally:
        sub.sock.close()

    stop.set()
    rc = await asyncio.wait_for(run_task, timeout=15.0)
    assert rc == 0
    assert not sock_path.exists(), "broadcast socket was not unlinked on shutdown"

    out = capsys.readouterr().out
    assert "serve: broadcast: started (socket " in out
    assert "serve: listener: skipped (no github-webhook-secret)" in out
    assert f"serve: fs: started (watching {watch_dir})" in out
    assert "serve: docker: skipped (" in out
    assert "serve: poll: skipped (--poll not set)" in out
    assert "serve: ready" in out
    assert "serve: stopped" in out


async def test_run_serve_reports_unrequested_components(
    serve_dirs: dict[str, Path],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    fresh_config: None,
) -> None:
    """A subset run reports every unrequested component as skipped."""
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
    # Barrier on accept, not file-existence: see the manifest-flake note in
    # test_run_serve_manifest_wake_and_graceful_stop. Releasing on exists()
    # and setting stop before the daemon accepts skips the manifest print.
    while time.monotonic() < deadline and not serve._probe_broadcast_bound(sock_path, timeout=0.05):
        assert not run_task.done(), f"supervisor exited early: {run_task!r}"
        await asyncio.sleep(0.02)
    stop.set()
    assert await asyncio.wait_for(run_task, timeout=15.0) == 0
    out = capsys.readouterr().out
    assert "serve: listener: skipped (not requested)" in out
    assert "serve: fs: skipped (not requested)" in out
    assert "serve: docker: skipped (not requested)" in out


async def test_run_serve_refuses_when_daemon_already_bound(
    serve_dirs: dict[str, Path],
    capsys: pytest.CaptureFixture[str],
    fresh_config: None,
) -> None:
    """A live socket at the broadcast path is a hard refusal with exit 2."""
    sock_path = serve_dirs["runtime"] / "broadcast.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    try:
        rc = await serve._run_serve(
            frozenset({"broadcast"}),
            poll=False,
            docker_socket="/nonexistent/docker.sock",
        )
    finally:
        server.close()
    assert rc == 2
    err = capsys.readouterr().err
    assert "refusing to start" in err
    assert str(sock_path) in err
    assert sock_path.exists(), "the refusal path must not unlink the live daemon's socket"


# ---------------------------------------------------------------------------
# CLI registration
# ---------------------------------------------------------------------------


def test_serve_registered_on_root_app() -> None:
    from waitbus.cli.main import app

    names = {cmd.name for cmd in app.registered_commands}
    assert "serve" in names


def test_serve_cmd_usage_error_without_selection() -> None:
    """Invoking serve with neither --all nor a subset is a usage error."""
    from typer.testing import CliRunner

    from waitbus.cli.main import app

    result = CliRunner().invoke(app, ["serve"])
    assert result.exit_code == 2
    # rich/click style "--all" in the error panel with ANSI color codes under
    # CI's renderer, so strip the codes before the substring check.
    import re

    plain = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
    assert "pass --all" in plain


def test_serve_cmd_refusal_exit_code_propagates(
    serve_dirs: dict[str, Path],
    fresh_config: None,
) -> None:
    """The already-running refusal surfaces as exit code 2 through the CLI."""
    from typer.testing import CliRunner

    from waitbus.cli.main import app

    sock_path = serve_dirs["runtime"] / "broadcast.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    try:
        result = CliRunner().invoke(app, ["serve", "broadcast"])
    finally:
        server.close()
    assert result.exit_code == 2
