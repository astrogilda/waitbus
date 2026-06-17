"""Tests for ``waitbus on`` -- block until a predicate matches, then run a command.

Covers the startup guards, the helper units (event-context projection, return-code
normalisation, the no-shell-injection guarantee, the match-decision closure, and
the restart process-group termination + SIGKILL escalation), and the once /
timeout / loop+restart paths end to end against an in-process daemon. The daemon
runs in the test's event loop (the blessed ``running_daemon`` fixture); the
blocking ``waitbus on`` CLI and the ``emit`` call run in a thread executor while the
daemon serves. Linux-only: the broadcast daemon's SO_PEERCRED check is Linux-only.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests._daemon_helpers import await_subscribers
from waitbus import _broadcast_sub, broadcast
from waitbus import on as on_mod
from waitbus._broadcast_sub import BroadcastConnectionError, FrameDecision, WaitOutcome
from waitbus._predicate import parse_match
from waitbus._types import EventInsert
from waitbus.cli.main import app
from waitbus.sources._protocol import SourceSpec
from waitbus.sources._registry import _clear_for_test_isolation, is_known_source, register_source

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def _isolate_registry() -> Iterator[None]:
    """Clear the process-singleton source registry around each test so the
    demo-scoped ``agent`` source never leaks between tests."""
    _clear_for_test_isolation()
    yield
    _clear_for_test_isolation()


def _register_agent() -> None:
    """Register the ``agent`` source in-process so agent events validate + emit."""
    if not is_known_source("agent"):
        register_source(SourceSpec(name="agent", event_types=("agent_claim", "agent_task_failed")))


def _emit_agent_event(db_path: Path, event_type: str, **fields: object) -> None:
    """Emit one synthesized agent event into ``db_path`` (rings the daemon doorbell).

    Registration is the caller's responsibility: each test registers the ``agent``
    source once in its body (main thread, before launching the CLI), so this helper
    does not re-register on every emit.
    """
    from waitbus._emit import emit

    emit(
        EventInsert(
            delivery_id=f"on-test:{event_type}:{time.time_ns()}",
            source="agent",
            event_type=event_type,
            owner="local",
            repo="swarm",
            received_at=time.time_ns(),
            payload_json=json.dumps(fields),
            ingest_method="test",
        ),
        db_path=db_path,
    )


# ---------------------------------------------------------------------------
# Startup guards (no daemon needed)
# ---------------------------------------------------------------------------


def test_on_requires_a_command() -> None:
    """`waitbus on` with no command after `--` is a startup error (exit 2)."""
    result = runner.invoke(app, ["on", "--source", "pytest", "--match", 'fields.event_type="pytest_session"'])
    assert result.exit_code == 2
    assert "requires a command" in result.stdout + str(result.stderr or "")


def test_on_restart_without_loop_is_error() -> None:
    """--restart without --loop is rejected at startup (exit 2)."""
    result = runner.invoke(
        app, ["on", "--source", "pytest", "--match", 'fields.event_type="pytest_session"', "--restart", "--", "true"]
    )
    assert result.exit_code == 2
    assert "--restart only applies with --loop" in result.stdout + str(result.stderr or "")


def test_on_bad_timeout_is_error() -> None:
    """A malformed --timeout is a startup error (exit 2)."""
    result = runner.invoke(
        app,
        [
            "on",
            "--source",
            "pytest",
            "--match",
            'fields.event_type="pytest_session"',
            "--timeout",
            "nope",
            "--",
            "true",
        ],
    )
    assert result.exit_code == 2
    assert "invalid --timeout" in result.stdout + str(result.stderr or "")


# ---------------------------------------------------------------------------
# Helper units (no daemon)
# ---------------------------------------------------------------------------


def test_event_env_projects_present_fields_only() -> None:
    """_event_env exports WAITBUS_* for present fields and omits absent ones."""
    frame = {
        "event_id": "01ABC",
        "event_type": "agent_task_failed",
        "owner": "local",
        "repo": "swarm",
        "fields": {"source": "agent", "conclusion": None},
    }
    env = on_mod._event_env(frame)
    assert env["WAITBUS_EVENT_ID"] == "01ABC"
    assert env["WAITBUS_EVENT_TYPE"] == "agent_task_failed"
    assert env["WAITBUS_SOURCE"] == "agent"
    assert "WAITBUS_CONCLUSION" not in env  # None is omitted, not exported empty
    assert "WAITBUS_HEAD_SHA" not in env


def test_normalise_returncode_maps_signal_death() -> None:
    """A negative (signal-killed) return code maps to 128 + signum."""
    assert on_mod._normalise_returncode(0) == 0
    assert on_mod._normalise_returncode(3) == 3
    assert on_mod._normalise_returncode(-15) == 143  # SIGTERM


def test_make_decide_matches_and_captures() -> None:
    """_make_decide returns MATCHED and captures the frame only on a predicate match."""
    captured: list[dict[str, object]] = []
    decide = on_mod._make_decide(parse_match(['fields.event_type="agent_task_failed"']), captured)
    assert decide({"fields": {"event_type": "agent_claim"}}) is FrameDecision.CONTINUE
    assert captured == []
    frame = {"event_type": "agent_task_failed", "fields": {"event_type": "agent_task_failed", "source": "agent"}}
    assert decide(frame) is FrameDecision.MATCHED
    assert captured == [frame]


def test_make_decide_github_nonterminal_keeps_waiting() -> None:
    """A matching GitHub frame with a non-terminal conclusion keeps waiting (CONTINUE)."""
    captured: list[dict[str, object]] = []
    decide = on_mod._make_decide(parse_match(['fields.source="github"']), captured)
    assert decide({"fields": {"source": "github", "conclusion": None}}) is FrameDecision.CONTINUE
    assert captured == []
    terminal = {"fields": {"source": "github", "conclusion": "success"}}
    assert decide(terminal) is FrameDecision.MATCHED


def test_run_blocking_passes_event_context_and_returns_child_code(tmp_path: Path) -> None:
    """_run_blocking runs the command with $WAITBUS_EVENT_FILE + WAITBUS_* and returns its code."""
    out_file = tmp_path / "seen.json"
    script = tmp_path / "capture.py"
    script.write_text(
        "import json, os, sys\n"
        "ev = json.load(open(os.environ['WAITBUS_EVENT_FILE']))\n"
        "json.dump({'event': ev, 'source_env': os.environ.get('WAITBUS_SOURCE'),\n"
        "           'etype_env': os.environ.get('WAITBUS_EVENT_TYPE')}, open(sys.argv[1], 'w'))\n"
        "sys.exit(7)\n"
    )
    frame = {
        "event_id": "01XYZ",
        "event_type": "agent_task_failed",
        "owner": "local",
        "repo": "swarm",
        "fields": {"source": "agent", "event_type": "agent_task_failed"},
    }
    rc = on_mod._run_blocking([sys.executable, str(script), str(out_file)], frame)
    assert rc == 7  # the child's own exit code propagates
    captured = json.loads(out_file.read_text())
    assert captured["event"] == frame
    assert captured["source_env"] == "agent"
    assert captured["etype_env"] == "agent_task_failed"


def test_run_blocking_does_not_shell_interpolate_event_fields(tmp_path: Path) -> None:
    """Event field values are never interpreted by a shell (no command injection).

    A frame field carries a shell payload that would delete a sentinel if it were
    ever interpolated into the command line. The operator command does not
    reference the field; the sentinel must survive.
    """
    sentinel = tmp_path / "DO_NOT_DELETE"
    sentinel.write_text("alive")
    frame = {
        "event_id": "01EVIL",
        "event_type": "agent_claim",
        "owner": "local",
        "repo": "swarm",
        "fields": {"source": "agent", "file": f"$(rm -rf {sentinel}); echo pwned"},
    }
    rc = on_mod._run_blocking(["printf", "%s", "ok"], frame)
    assert rc == 0
    assert sentinel.exists(), "event field was shell-interpreted -- command injection!"


@pytest.mark.slow
def test_terminate_group_kills_running_child() -> None:
    """_terminate_group SIGTERMs a running child's process group and reaps it."""
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    try:
        on_mod._terminate_group(proc, stop_timeout=3.0)
        assert proc.poll() is not None, "child was not terminated"
    finally:
        if proc.poll() is None:  # pragma: no cover -- safety net if the assert above failed
            proc.kill()


@pytest.mark.slow
def test_terminate_group_escalates_to_sigkill(tmp_path: Path) -> None:
    """A child ignoring SIGTERM is force-killed after the grace period.

    The child writes a readiness marker only AFTER installing the SIGTERM-ignore
    handler; the test waits for that marker before terminating, so the SIGTERM
    cannot win a race against an un-installed handler and the SIGKILL-escalation
    path is exercised deterministically.
    """
    ready = tmp_path / "ready"
    code = (
        "import signal, time, pathlib\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"pathlib.Path({str(ready)!r}).write_text('x')\n"
        "time.sleep(30)\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    try:
        for _ in range(200):
            if ready.exists():
                break
            time.sleep(0.02)
        assert ready.exists(), "child never signalled readiness"
        start = time.monotonic()
        on_mod._terminate_group(proc, stop_timeout=1.0)
        elapsed = time.monotonic() - start
        assert proc.poll() is not None, "SIGTERM-ignoring child was not SIGKILLed"
        assert elapsed >= 1.0, "should have waited the grace period before SIGKILL"
        assert elapsed < 6.0, f"escalation took too long: {elapsed:.1f}s"
    finally:
        if proc.poll() is None:  # pragma: no cover -- safety net
            proc.kill()


def test_terminate_group_idempotent_on_dead_child() -> None:
    """_terminate_group does not raise when the child has already exited.

    The unconditional final SIGKILL hits an already-dead group and the resulting
    ProcessLookupError is suppressed (the M3 grandchild-reaping change must remain
    safe on the common case of a child that already exited cleanly).
    """
    proc = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
    proc.wait(timeout=5.0)
    on_mod._terminate_group(proc, stop_timeout=1.0)  # must not raise
    assert proc.poll() is not None


def test_exec_error_code_maps_exec_failures() -> None:
    """Spawn-time OSErrors map to the bash/timeout exec-failure exit codes (M1)."""
    from waitbus.wait import EXIT_STARTUP

    assert on_mod._exec_error_code(FileNotFoundError()) == 127
    assert on_mod._exec_error_code(PermissionError()) == 126
    assert on_mod._exec_error_code(OSError()) == EXIT_STARTUP


def test_run_blocking_raises_exec_error_127_on_missing_command() -> None:
    """A command not on PATH raises _ExecError(127) (uniform exec-failure signal)."""
    frame = {"event_id": "01X", "event_type": "agent_claim", "fields": {"source": "agent"}}
    with pytest.raises(on_mod._ExecError) as exc:
        on_mod._run_blocking(["waitbus-no-such-command-xyz"], frame)
    assert exc.value.code == 127


def test_run_blocking_raises_exec_error_126_on_non_executable(tmp_path: Path) -> None:
    """A command file without the execute bit raises _ExecError(126)."""
    script = tmp_path / "not-exec.sh"
    script.write_text("#!/bin/sh\necho hi\n")
    script.chmod(0o644)  # readable, NOT executable
    frame = {"event_id": "01X", "event_type": "agent_claim", "fields": {"source": "agent"}}
    with pytest.raises(on_mod._ExecError) as exc:
        on_mod._run_blocking([str(script)], frame)
    assert exc.value.code == 126


def test_running_child_spawn_unlinks_event_file_on_exec_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed Popen raises _ExecError and unlinks the just-written event file (M2)."""
    created: list[str] = []
    real_write = on_mod._write_event_file

    def _spy(frame: dict[str, object]) -> str:
        path = real_write(frame)
        created.append(path)
        return path

    monkeypatch.setattr(on_mod, "_write_event_file", _spy)
    frame = {"event_id": "01X", "event_type": "agent_claim", "fields": {"source": "agent"}}
    with pytest.raises(on_mod._ExecError) as exc:
        on_mod._RunningChild.spawn(["waitbus-no-such-command-xyz"], frame)
    assert exc.value.code == 127
    assert created, "event file should have been written before the failed spawn"
    assert not Path(created[0]).exists(), "event file leaked after spawn failure"


@pytest.mark.slow
def test_terminate_group_kills_grandchild_left_by_graceful_child(tmp_path: Path) -> None:
    """Negative-PGID SIGKILL reaps a grandchild even when the direct child exits cleanly (M3).

    The watchexec/containerd #4594 leak class: the direct child spawns a grandchild
    in the same group, records its pid, then exits 0 immediately. Terminating only
    on a wait-timeout would orphan the grandchild; the unconditional group SIGKILL
    reaps it.
    """
    marker = tmp_path / "grandchild.pid"
    code = (
        "import subprocess, pathlib, sys\n"
        "gc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(gc.pid))\n"
        # direct child exits here; the grandchild keeps running in the same group
    )
    proc = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    proc.wait(timeout=5.0)  # direct child exits ~immediately
    for _ in range(250):
        if marker.exists():
            break
        time.sleep(0.02)
    assert marker.exists(), "grandchild pid marker never written"
    gc_pid = int(marker.read_text())
    try:
        on_mod._terminate_group(proc, stop_timeout=1.0)
        for _ in range(250):
            try:
                os.kill(gc_pid, 0)
                time.sleep(0.02)
            except ProcessLookupError:
                break
        with pytest.raises(ProcessLookupError):
            os.kill(gc_pid, 0)  # grandchild is gone
    finally:
        with contextlib.suppress(ProcessLookupError):  # pragma: no cover -- safety net
            os.kill(gc_pid, signal.SIGKILL)


@pytest.mark.slow
def test_terminate_group_second_ctrl_c_shortcuts_to_sigkill(monkeypatch: pytest.MonkeyPatch) -> None:
    """A KeyboardInterrupt during the grace wait shortcuts to SIGKILL, no traceback."""
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    state: list[int] = []
    real_wait = proc.wait

    def _wait(timeout: float | None = None) -> int:
        if not state:  # the grace wait: simulate an impatient 2nd Ctrl-C
            state.append(1)
            raise KeyboardInterrupt
        return real_wait(timeout=timeout)

    monkeypatch.setattr(proc, "wait", _wait)
    try:
        on_mod._terminate_group(proc, stop_timeout=5.0)  # must NOT raise KeyboardInterrupt
    finally:
        if proc.poll() is None:  # pragma: no cover -- safety net
            proc.kill()
    assert proc.poll() is not None, "child not SIGKILLed after the 2nd-Ctrl-C shortcut"


def test_signal_forwarder_raises_shutdown_and_restores() -> None:
    """Restart-loop forwarders raise _SignalShutdown and restore prior handlers on exit."""
    before_term = signal.getsignal(signal.SIGTERM)
    before_hup = signal.getsignal(signal.SIGHUP)
    with contextlib.ExitStack() as stack:
        on_mod._install_signal_forwarders(stack)
        handler = signal.getsignal(signal.SIGTERM)
        assert handler is not before_term, "SIGTERM handler was not installed"
        assert callable(handler)  # narrow Handlers | Callable | None -> Callable
        with pytest.raises(on_mod._SignalShutdown) as exc:
            handler(signal.SIGTERM, None)
        assert exc.value.signum == signal.SIGTERM
    assert signal.getsignal(signal.SIGTERM) is before_term, "SIGTERM handler not restored"
    assert signal.getsignal(signal.SIGHUP) is before_hup, "SIGHUP handler not restored"


def _stub_acked_subscriber(monkeypatch: pytest.MonkeyPatch) -> socket.socket:
    """Stub open_subscriber + read_subscribe_ack with a live socketpair; return the server end."""
    from waitbus._broadcast_sub import SubscriberHandle

    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    monkeypatch.setattr(on_mod, "open_subscriber", lambda **_kw: SubscriberHandle(sock=client))
    monkeypatch.setattr(on_mod, "read_subscribe_ack", lambda _sub: None)
    return server


def test_on_maps_keyboardinterrupt_to_130(monkeypatch: pytest.MonkeyPatch) -> None:
    """A KeyboardInterrupt escaping the run region maps to exit 130, not a traceback (M4)."""
    server = _stub_acked_subscriber(monkeypatch)

    def _boom(*_a: object, **_k: object) -> int:
        raise KeyboardInterrupt

    monkeypatch.setattr(on_mod, "_run_once", _boom)
    _register_agent()
    try:
        result = runner.invoke(
            app, ["on", "--source", "agent", "--match", 'fields.event_type="agent_claim"', "--", "true"]
        )
        assert result.exit_code == 130
    finally:
        server.close()


def test_on_maps_exec_error_to_its_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """An _ExecError from any run path maps to its exit code (uniform exec handling)."""
    server = _stub_acked_subscriber(monkeypatch)

    def _boom(*_a: object, **_k: object) -> int:
        raise on_mod._ExecError(127)

    monkeypatch.setattr(on_mod, "_run_once", _boom)
    _register_agent()
    try:
        result = runner.invoke(
            app, ["on", "--source", "agent", "--match", 'fields.event_type="agent_claim"', "--", "nope"]
        )
        assert result.exit_code == 127
    finally:
        server.close()


def test_on_maps_signal_shutdown_to_128_plus_signum(monkeypatch: pytest.MonkeyPatch) -> None:
    """A forwarded SIGTERM (_SignalShutdown) maps to 143 = 128 + 15, not a traceback."""
    server = _stub_acked_subscriber(monkeypatch)

    def _boom(*_a: object, **_k: object) -> int:
        raise on_mod._SignalShutdown(signal.SIGTERM)

    monkeypatch.setattr(on_mod, "_run_once", _boom)
    _register_agent()
    try:
        result = runner.invoke(
            app, ["on", "--source", "agent", "--match", 'fields.event_type="agent_claim"', "--", "true"]
        )
        assert result.exit_code == 128 + int(signal.SIGTERM)
    finally:
        server.close()


def test_on_stop_timeout_without_restart_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    """--stop-timeout without --restart prints the ignored-note (L6) but still runs (exit 0)."""
    server = _stub_acked_subscriber(monkeypatch)
    monkeypatch.setattr(on_mod, "_run_once", lambda *_a, **_k: 0)
    _register_agent()
    try:
        result = runner.invoke(
            app,
            [
                "on",
                "--source",
                "agent",
                "--match",
                'fields.event_type="agent_claim"',
                "--stop-timeout",
                "30s",
                "--",
                "true",
            ],
        )
        assert result.exit_code == 0
        assert "--stop-timeout has no effect without --restart" in result.output
    finally:
        server.close()


def test_outcome_exit_maps_each_terminal_outcome() -> None:
    """_outcome_exit maps each WaitOutcome shape to its exit code (or None on match)."""
    cancelled = WaitOutcome(matched=False, timed_out=False, cancelled=True, peer_closed=False, framing_error=False)
    timed_out = WaitOutcome(matched=False, timed_out=True, cancelled=False, peer_closed=False, framing_error=False)
    matched = WaitOutcome(matched=True, timed_out=False, cancelled=False, peer_closed=False, framing_error=False)
    peer = WaitOutcome(matched=False, timed_out=False, cancelled=False, peer_closed=True, framing_error=False)
    assert on_mod._outcome_exit(cancelled) == 130
    assert on_mod._outcome_exit(timed_out) == 124
    assert on_mod._outcome_exit(matched) is None
    assert on_mod._outcome_exit(peer) == 2


def test_on_exits_2_when_daemon_absent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`waitbus on` with no broadcast daemon reachable is a startup error (exit 2)."""
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: tmp_path / "nonexistent.sock")
    _register_agent()
    result = runner.invoke(
        app,
        ["on", "--source", "agent", "--match", 'fields.event_type="agent_task_failed"', "--", "true"],
    )
    assert result.exit_code == 2
    assert "broadcast" in (result.stdout + str(result.stderr or "")).lower()


def test_on_exits_2_when_open_subscriber_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A BroadcastConnectionError from open_subscriber is a startup error (exit 2)."""

    def _raise(**_kw: object) -> None:
        raise BroadcastConnectionError("daemon down", remediation="start the broadcast daemon")

    monkeypatch.setattr(on_mod, "open_subscriber", _raise)
    _register_agent()
    result = runner.invoke(
        app,
        ["on", "--source", "agent", "--match", 'fields.event_type="agent_task_failed"', "--", "true"],
    )
    assert result.exit_code == 2


def test_on_exits_2_when_subscribe_ack_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A subscribe rejection at the ack barrier is surfaced as a startup error (exit 2).

    open_subscriber succeeds (a real socketpair), but the ack read raises -- the
    daemon's reject path -- which _on maps to exit 2 after closing the socket.
    """
    import socket

    from waitbus._broadcast_sub import SubscriberHandle

    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    monkeypatch.setattr(on_mod, "open_subscriber", lambda **_kw: SubscriberHandle(sock=client))

    def _reject(_sub: object) -> None:
        raise BroadcastConnectionError("subscribe rejected", remediation="send proto: 1")

    monkeypatch.setattr(on_mod, "read_subscribe_ack", _reject)
    _register_agent()
    try:
        result = runner.invoke(
            app,
            ["on", "--source", "agent", "--match", 'fields.event_type="agent_task_failed"', "--", "true"],
        )
        assert result.exit_code == 2
    finally:
        server.close()


# ---------------------------------------------------------------------------
# End to end against the in-process daemon (running_daemon fixture)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.asyncio
async def test_on_once_runs_command_on_match_and_propagates_exit(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """once mode: a matching event fires the command (exit 0) and writes a sentinel.

    The daemon serves in the test loop; the blocking ``waitbus on`` and the emit run
    in a thread executor. The CLI's default subscriber socket is redirected to the
    daemon-under-test's socket.
    """
    daemon, paths = running_daemon
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: paths["broadcast"])
    _register_agent()

    sentinel = tmp_path / "fired"
    loop = asyncio.get_running_loop()
    invoke = loop.run_in_executor(
        None,
        lambda: runner.invoke(
            app,
            [
                "on",
                "--source",
                "agent",
                "--match",
                'fields.event_type="agent_task_failed"',
                "--timeout",
                "10s",
                "--",
                "touch",
                str(sentinel),
            ],
        ),
    )
    # Wait for on's subscriber to register deterministically (the ack barrier lives
    # inside the executor thread, invisible here) rather than a fixed wall-clock sleep.
    await await_subscribers(daemon, added=1)
    await loop.run_in_executor(None, lambda: _emit_agent_event(paths["db"], "agent_task_failed", agent="a1", error="x"))
    result = await asyncio.wait_for(invoke, timeout=10.0)

    assert result.exit_code == 0, f"expected 0 (touch ok), got {result.exit_code}\n{result.stdout}"
    assert sentinel.exists(), "the command did not run on match"


@pytest.mark.slow
@pytest.mark.asyncio
async def test_on_once_times_out_with_no_match(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """once mode: no matching event before --timeout exits 124."""
    _daemon, paths = running_daemon
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: paths["broadcast"])
    _register_agent()
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        lambda: runner.invoke(
            app,
            [
                "on",
                "--source",
                "agent",
                "--match",
                'fields.event_type="agent_task_failed"',
                "--timeout",
                "1s",
                "--",
                "true",
            ],
        ),
    )
    assert result.exit_code == 124


@pytest.mark.slow
@pytest.mark.asyncio
async def test_on_loop_restart_terminates_running_command(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """loop+restart: two matches launch two commands (the first is terminated), then idle-timeout (124)."""
    daemon, paths = running_daemon
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: paths["broadcast"])
    _register_agent()

    marker_dir = tmp_path / "markers"
    marker_dir.mkdir()
    script = tmp_path / "server.py"
    script.write_text(
        "import os, time, pathlib\n"
        f"pathlib.Path({str(marker_dir)!r}, str(os.getpid())).write_text('up')\n"
        "time.sleep(30)\n"
    )
    loop = asyncio.get_running_loop()
    invoke = loop.run_in_executor(
        None,
        lambda: runner.invoke(
            app,
            [
                "on",
                "--source",
                "agent",
                "--match",
                'fields.event_type="agent_claim"',
                "--loop",
                "--restart",
                "--stop-timeout",
                "2s",
                # The idle window must comfortably exceed the emit->deliver->
                # spawn chain under a loaded serial-coverage run (observed
                # >2s); 6s keeps the test deterministic without slowing the
                # happy path (the loop exits as soon as the window lapses).
                "--timeout",
                "6s",
                "--",
                sys.executable,
                str(script),
            ],
        ),
    )
    await await_subscribers(daemon, added=1)  # deterministic register-before-emit
    await loop.run_in_executor(
        None, lambda: _emit_agent_event(paths["db"], "agent_claim", agent="a1", file="parser.py")
    )
    # Deterministic first-launch wait (replaces a fixed pacing sleep that
    # raced loaded runs): the first command must have spawned and written
    # its marker before the second match restarts it, so both launches are
    # observable.
    deadline = asyncio.get_running_loop().time() + 10.0
    while not list(marker_dir.iterdir()):
        assert asyncio.get_running_loop().time() < deadline, "first command never wrote its marker"
        await asyncio.sleep(0.05)
    await loop.run_in_executor(None, lambda: _emit_agent_event(paths["db"], "agent_claim", agent="a2", file="lexer.py"))
    result = await asyncio.wait_for(invoke, timeout=30.0)

    assert result.exit_code == 124, f"expected idle-timeout 124, got {result.exit_code}\n{result.stdout}"
    assert len(list(marker_dir.iterdir())) >= 2, "restart did not launch a second command"


@pytest.mark.slow
@pytest.mark.asyncio
async def test_on_loop_sequential_runs_command_then_idle_times_out(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """loop WITHOUT --restart: a match runs the command to completion (sequential),
    then with no further match the loop idle-times-out (exit 124)."""
    daemon, paths = running_daemon
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: paths["broadcast"])
    _register_agent()

    sentinel = tmp_path / "ran"
    loop = asyncio.get_running_loop()
    invoke = loop.run_in_executor(
        None,
        lambda: runner.invoke(
            app,
            [
                "on",
                "--source",
                "agent",
                "--match",
                'fields.event_type="agent_claim"',
                "--loop",
                # Wide enough that the emit->deliver->spawn chain under a
                # loaded serial-coverage run (observed >2s) cannot eat the
                # whole idle window before the command runs.
                "--timeout",
                "6s",
                "--",
                "touch",
                str(sentinel),
            ],
        ),
    )
    await await_subscribers(daemon, added=1)  # deterministic register-before-emit
    await loop.run_in_executor(None, lambda: _emit_agent_event(paths["db"], "agent_claim", agent="a1", file="p.py"))
    result = await asyncio.wait_for(invoke, timeout=25.0)

    assert result.exit_code == 124, f"expected idle-timeout 124, got {result.exit_code}\n{result.stdout}"
    assert sentinel.exists(), "the command did not run on the match (sequential loop)"
