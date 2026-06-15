"""The broadcast daemon self-terminates when its bench/soak spawner dies.

Regression for the orphaned-daemon leak: ``spawn_waitbus_daemon``
detaches the daemon into its own session and relies on a ``finally`` to reap
it. A SIGKILL'd or crashed harness bypasses that ``finally`` and would orphan
the session-detached daemon forever. The harness hands the daemon the read end
of an inherited pipe and keeps the write end open for the daemon's lifetime;
the spawner's death by any means (including SIGKILL) closes the write end, so
the daemon's read end hits EOF and the daemon shuts down gracefully, unlinking
its socket. These tests prove that mechanism with a controlled stand-in
spawner, and unit-test the env-gated fd-opening helper directly.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from waitbus.broadcast import Broadcast

_WAITBUS = Path(sys.executable).parent / "waitbus"

# A stand-in for the bench/soak harness. It mirrors ``spawn_waitbus_daemon``:
# create a pipe, mark the read end inheritable, spawn the real daemon with the
# read fd passed down via ``pass_fds`` and named in ``WAITBUS_DEATH_FD``, keep the
# write end open, then block forever. Killing this process closes the write end
# and trips the daemon's EOF watch. The daemon is detached into its own session
# so this stand-in's death does not take it down via the process tree -- only
# the pipe EOF can, which is exactly what we are testing.
_SPAWNER_PROGRAM = """
import os, subprocess, sys, time
waitbus, state, runtime = sys.argv[1], sys.argv[2], sys.argv[3]
death_r, death_w = os.pipe()
os.set_inheritable(death_r, True)
env = {
    **os.environ,
    "WAITBUS_STATE_DIR": state,
    "WAITBUS_RUNTIME_DIR": runtime,
    "WAITBUS_DEATH_FD": str(death_r),
    "WAITBUS_HEARTBEAT_SEC": "3600",
}
proc = subprocess.Popen(
    [waitbus, "broadcast", "serve"],
    env=env,
    start_new_session=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    pass_fds=(death_r,),
)
os.close(death_r)
# Report the daemon pid on stdout so the parent test can reap it, then block
# until killed (holding the write end is the liveness signal).
sys.stdout.write(str(proc.pid) + "\\n")
sys.stdout.flush()
time.sleep(120)
"""


# ---------------------------------------------------------------------------
# Fast unit tests: the env-gated fd helper. These run on every platform and
# do not spawn a daemon, so they cover the helper's branches cheaply.
# ---------------------------------------------------------------------------


def test_open_death_watch_returns_none_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """No ``WAITBUS_DEATH_FD`` -> no watch (production / non-bench path)."""
    monkeypatch.delenv("WAITBUS_DEATH_FD", raising=False)
    assert Broadcast._open_death_watch() is None


def test_open_death_watch_returns_fd_and_clears_inheritable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the env var set, the helper returns the fd and marks it
    non-inheritable so the daemon's own future children cannot keep the
    watch alive by holding an open copy of the read end."""
    r, w = os.pipe()
    try:
        os.set_inheritable(r, True)
        assert os.get_inheritable(r) is True
        monkeypatch.setenv("WAITBUS_DEATH_FD", str(r))
        got = Broadcast._open_death_watch()
        assert got == r
        assert os.get_inheritable(r) is False
    finally:
        os.close(r)
        os.close(w)


# ---------------------------------------------------------------------------
# In-process: run the real ``Broadcast.run`` event loop with a death fd and
# prove closing the write end drives a graceful shutdown. Exercises the reader
# registration and the ``finally``-block fd teardown in-process (the subprocess
# integration tests below prove the same end-to-end but run in a child the
# coverage tool cannot see).
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform != "linux", reason="broadcast daemon is Linux-only")
async def test_death_fd_close_stops_in_process_daemon(
    broadcast_paths: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r, w = os.pipe()
    os.set_inheritable(r, True)
    monkeypatch.setenv("WAITBUS_DEATH_FD", str(r))
    daemon = Broadcast(db_path=str(broadcast_paths["db"]))
    task = asyncio.create_task(daemon.run())
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not broadcast_paths["broadcast"].exists():
            await asyncio.sleep(0.02)
        assert broadcast_paths["broadcast"].exists(), "daemon did not bind"
        assert not task.done(), "daemon exited before the spawner died"
        # The spawner dies: closing the write end makes the daemon's read end
        # readable at EOF, which sets the stop event and runs the finally block
        # (which removes the reader and closes the read fd).
        os.close(w)
        w = -1
        await asyncio.wait_for(task, timeout=5.0)
        assert task.result() == 0
        assert not broadcast_paths["broadcast"].exists(), "socket not unlinked on shutdown"
        # run() closed the read fd in its finally; closing it again must fail.
        with pytest.raises(OSError):
            os.close(r)
    finally:
        if not task.done():
            await daemon.stop()
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(task, timeout=5.0)
        if w != -1:
            with contextlib.suppress(OSError):
                os.close(w)


# ---------------------------------------------------------------------------
# Integration tests: a real daemon under a controlled stand-in spawner. These
# require the console-script and the Linux SO_PEERCRED path, so they are gated.
# ---------------------------------------------------------------------------

_integration = pytest.mark.skipif(
    sys.platform != "linux" or not _WAITBUS.exists(),
    reason="needs Linux SO_PEERCRED and the installed waitbus console-script",
)


def _reap(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    with contextlib.suppress(ProcessLookupError):
        proc.kill()
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=5)


def _reap_pid(pid: int) -> None:
    # The daemon is a session-detached grandchild, not our child, so we cannot
    # waitpid it (init reaps it); a best-effort group SIGKILL is the cleanup.
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(os.getpgid(pid), signal.SIGKILL)


def _start_spawner_and_daemon(tmp_path: Path) -> tuple[subprocess.Popen[bytes], int, Path]:
    """Launch the stand-in spawner; return (spawner, daemon_pid, socket_path).

    Blocks until the daemon has bound its listener socket so the kill in the
    caller cannot race a half-booted daemon.
    """
    state = tmp_path / "state"
    runtime = tmp_path / "runtime"
    state.mkdir()
    runtime.mkdir()
    spawner = subprocess.Popen(
        [sys.executable, "-c", _SPAWNER_PROGRAM, str(_WAITBUS), str(state), str(runtime)],
        stdout=subprocess.PIPE,
    )
    assert spawner.stdout is not None
    daemon_pid = int(spawner.stdout.readline().decode().strip())
    spawner.stdout.close()
    sock = runtime / "broadcast.sock"
    boot_deadline = time.monotonic() + 10.0
    while time.monotonic() < boot_deadline and not sock.exists():
        time.sleep(0.05)
    assert sock.exists(), "daemon did not bind its listener socket"
    return spawner, daemon_pid, sock


def _assert_daemon_tore_down(daemon_pid: int, sock: Path) -> None:
    """The daemon must exit AND unlink its socket (graceful path), not orphan."""
    deadline = time.monotonic() + 12.0
    while time.monotonic() < deadline:
        try:
            os.kill(daemon_pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    with contextlib.suppress(ProcessLookupError):
        os.kill(daemon_pid, 0)
        raise AssertionError("daemon orphaned after spawner died (EOF watch did not fire)")
    # Graceful shutdown unlinks the owned listener socket; a SIGKILL'd daemon
    # would leave it behind. Its absence proves the EOF routed through the
    # stop_event / unlink path rather than an abrupt death.
    assert not sock.exists(), "daemon exited but left its socket (non-graceful shutdown)"


@_integration
def test_daemon_dies_when_spawner_sigterms(tmp_path: Path) -> None:
    spawner, daemon_pid, sock = _start_spawner_and_daemon(tmp_path)
    try:
        spawner.send_signal(signal.SIGTERM)
        spawner.wait(timeout=5)
        _assert_daemon_tore_down(daemon_pid, sock)
    finally:
        _reap(spawner)
        _reap_pid(daemon_pid)


@_integration
def test_daemon_dies_when_spawner_sigkilled(tmp_path: Path) -> None:
    """SIGKILL bypasses any cleanup the spawner could run; the kernel still
    closes the write end, so the EOF watch fires regardless."""
    spawner, daemon_pid, sock = _start_spawner_and_daemon(tmp_path)
    try:
        spawner.kill()
        spawner.wait(timeout=5)
        _assert_daemon_tore_down(daemon_pid, sock)
    finally:
        _reap(spawner)
        _reap_pid(daemon_pid)


@_integration
def test_daemon_stays_up_without_death_fd(tmp_path: Path) -> None:
    """No ``WAITBUS_DEATH_FD`` -> the watch is inert and the daemon keeps
    running even after an unrelated process exits (production parity)."""
    state = tmp_path / "state"
    runtime = tmp_path / "runtime"
    state.mkdir()
    runtime.mkdir()
    env = {
        **os.environ,
        "WAITBUS_STATE_DIR": str(state),
        "WAITBUS_RUNTIME_DIR": str(runtime),
        "WAITBUS_HEARTBEAT_SEC": "3600",
    }
    daemon = subprocess.Popen(
        [str(_WAITBUS), "broadcast", "serve"],
        env=env,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        sock = runtime / "broadcast.sock"
        boot_deadline = time.monotonic() + 10.0
        while time.monotonic() < boot_deadline and not sock.exists():
            time.sleep(0.05)
        assert sock.exists(), "daemon did not bind its listener socket"
        # An unrelated short-lived process exits; with no death fd the daemon
        # must be unaffected.
        subprocess.Popen([sys.executable, "-c", "pass"]).wait(timeout=5)
        time.sleep(1.0)
        assert daemon.poll() is None, "daemon exited without a death fd (watch not inert)"
    finally:
        daemon.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            daemon.wait(timeout=5)
        _reap(daemon)
