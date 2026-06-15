"""Subprocess end-to-end scenarios for ``waitbus serve``.

Spawns the real console script against isolated state dirs and asserts
the operator-visible contract: the startup manifest, a subscriber
waking on an emitted event, a real file save flowing through the fs
watcher, SIGINT producing a clean exit with the socket unlinked, the
already-running refusal, and the deterministic docker skip report.

These tests complement ``tests/test_cli_serve.py``: the in-process
suite there is the coverage source for ``waitbus/cli/serve.py`` (the
CI coverage run does not trace subprocesses); this module proves the
same behaviour through the packaged entry point and real signals.
"""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec
import pytest

from tests._daemon_helpers import isolated_subprocess_env
from waitbus import _emit as emit_mod
from waitbus._broadcast_sub import SubscriberHandle, open_subscriber, read_subscribe_ack
from waitbus._frame import sync_read_frame
from waitbus._types import EventInsert

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform != "linux",
        reason="AF_UNIX daemon + POSIX signal semantics are exercised on Linux",
    ),
]

_WAITBUS = str(Path(sys.executable).parent / "waitbus")
_READY_DEADLINE_S = 20.0
_EXIT_DEADLINE_S = 15.0


def _serve_env(tmp_path: Path, **extra: str) -> tuple[dict[str, str], dict[str, Path]]:
    """Isolated WAITBUS_*_DIR env for one serve subprocess.

    Strips any credential env vars so the listener-skip manifest line is
    deterministic, and stretches the daemon's periodic log cadences so
    stderr stays quiet during the test window.
    """
    env, dirs = isolated_subprocess_env(
        tmp_path,
        WAITBUS_HEARTBEAT_SEC="30",
        WAITBUS_METRICS_SNAPSHOT_PERIOD_SEC="30",
    )
    env.update(extra)
    return env, dirs


def _spawn_serve(args: list[str], env: dict[str, str], tmp_path: Path) -> tuple[subprocess.Popen[bytes], Path, Path]:
    """Start one serve subprocess with stdout/stderr captured to files."""
    out_path = tmp_path / "serve.out"
    err_path = tmp_path / "serve.err"
    with out_path.open("wb") as out, err_path.open("wb") as err:
        proc = subprocess.Popen([_WAITBUS, "serve", *args], env=env, stdout=out, stderr=err)
    return proc, out_path, err_path


def _wait_for_ready(proc: subprocess.Popen[bytes], out_path: Path) -> str:
    """Block until the manifest's ready line lands in the stdout file."""
    deadline = time.monotonic() + _READY_DEADLINE_S
    while time.monotonic() < deadline:
        text = out_path.read_text(encoding="utf-8", errors="replace")
        if "serve: ready" in text:
            return text
        assert proc.poll() is None, f"serve exited early (rc={proc.returncode}):\n{text}"
        time.sleep(0.05)
    raise AssertionError(f"serve never printed the ready line:\n{out_path.read_text(errors='replace')}")


def _interrupt_and_wait(proc: subprocess.Popen[bytes]) -> int:
    """SIGINT the supervisor and wait for it, escalating only on a hang."""
    proc.send_signal(signal.SIGINT)
    try:
        return proc.wait(timeout=_EXIT_DEADLINE_S)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)
        raise


def _build_event(delivery_id: str) -> EventInsert:
    return EventInsert(
        delivery_id=delivery_id,
        source="pytest",
        event_type="pytest_session",
        owner="serve-e2e",
        repo="subprocess",
        received_at=time.time_ns(),
        payload_json=msgspec.json.encode({"outcome": "pass"}).decode(),
        ingest_method="e2e",
        status="completed",
        conclusion="success",
    )


def _read_until(
    sub: SubscriberHandle,
    predicate: Callable[[dict[str, Any]], bool],
    deadline_s: float,
) -> dict[str, Any] | None:
    """Read frames off a subscriber socket until ``predicate(frame)`` holds."""
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        frame_bytes = sync_read_frame(sub.sock)
        if frame_bytes is None:
            continue
        frame: dict[str, Any] = msgspec.json.decode(frame_bytes, type=dict)
        if predicate(frame):
            return frame
    return None


def test_serve_all_manifest_wake_fs_event_and_clean_sigint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path: manifest lines, subscriber wake, fs event, clean SIGINT exit."""
    pytest.importorskip("watchdog")
    watch_dir = tmp_path / "watched"
    watch_dir.mkdir()
    env, dirs = _serve_env(tmp_path, WAITBUS_FS_WATCH_PATH=str(watch_dir))
    proc, out_path, _err_path = _spawn_serve(["--all"], env, tmp_path)
    try:
        manifest = _wait_for_ready(proc, out_path)
        assert "serve: broadcast: started (socket " in manifest
        assert "serve: listener: skipped (no github-webhook-secret)" in manifest
        assert f"serve: fs: started (watching {watch_dir})" in manifest
        assert "serve: docker: " in manifest  # started or skipped, host-dependent
        assert "serve: poll: skipped (--poll not set)" in manifest

        # The test process must resolve the same paths as the subprocess.
        for var in ("WAITBUS_STATE_DIR", "WAITBUS_RUNTIME_DIR", "WAITBUS_CONFIG_DIR"):
            monkeypatch.setenv(var, env[var])
        sock_path = dirs["runtime"] / "broadcast.sock"
        sub = open_subscriber(socket_path=str(sock_path))
        try:
            read_subscribe_ack(sub)
            # Subscriber wake on an emitted event.
            delivery_id = f"serve-e2e:{time.time_ns()}"
            emit_mod.emit_batch([_build_event(delivery_id)], db_path=dirs["state"] / "github.db")
            frame = _read_until(sub, lambda f: f.get("delivery_id") == delivery_id, 10.0)
            assert frame is not None, "subscriber did not wake on the emitted event"

            # A real completed save flows through the supervised fs watcher.
            saved = watch_dir / "artifact.txt"
            saved.write_text("done")
            fs_frame = _read_until(
                sub,
                lambda f: f.get("kind") != "heartbeat" and str(f.get("delivery_id", "")).startswith("fs:"),
                10.0,
            )
            assert fs_frame is not None, "fs watcher event never reached the subscriber"
        finally:
            sub.sock.close()

        rc = _interrupt_and_wait(proc)
        assert rc == 0
        final = out_path.read_text(encoding="utf-8", errors="replace")
        assert "serve: stopped" in final
        assert not sock_path.exists(), "broadcast socket was not unlinked on shutdown"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)


def test_serve_refuses_when_daemon_already_serving(tmp_path: Path) -> None:
    """A second serve against a live broadcast socket exits 2 with a clear message."""
    env, dirs = _serve_env(tmp_path)
    first, first_out, _ = _spawn_serve(["broadcast"], env, tmp_path)
    try:
        _wait_for_ready(first, first_out)
        second_dir = tmp_path / "second"
        second_dir.mkdir()
        second = subprocess.run(
            [_WAITBUS, "serve", "--all"],
            env=env,
            capture_output=True,
            timeout=30,
        )
        assert second.returncode == 2, second.stderr.decode(errors="replace")
        assert b"refusing to start" in second.stderr
        # The refusal must not have disturbed the live daemon's socket.
        assert (dirs["runtime"] / "broadcast.sock").exists()
        rc = _interrupt_and_wait(first)
        assert rc == 0
    finally:
        if first.poll() is None:
            first.kill()
            first.wait(timeout=5.0)


def test_serve_docker_skip_report_is_deterministic(tmp_path: Path) -> None:
    """Pointing --docker-socket at a missing path yields a loud, specific skip."""
    env, _dirs = _serve_env(tmp_path)
    proc, out_path, _ = _spawn_serve(["broadcast,docker", "--docker-socket", "/nonexistent/docker.sock"], env, tmp_path)
    try:
        manifest = _wait_for_ready(proc, out_path)
        assert "serve: docker: skipped (" in manifest
        assert "does not exist" in manifest
        assert _interrupt_and_wait(proc) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)


def test_serve_daemon_startup_failure_exits_1_with_clean_stderr(tmp_path: Path) -> None:
    """A daemon bind failure exits 1 with one serve: stderr line, no traceback."""
    env, dirs = _serve_env(tmp_path)
    # A directory squatting on the socket path breaks the daemon's
    # unlink-then-bind during startup, while the supervisor's pre-probe
    # still sees "nothing serving here" and proceeds to boot.
    (dirs["runtime"] / "broadcast.sock").mkdir()
    proc = subprocess.run(
        [_WAITBUS, "serve", "broadcast"],
        env=env,
        capture_output=True,
        timeout=30,
    )
    err = proc.stderr.decode(errors="replace")
    assert proc.returncode == 1, err
    assert "serve: broadcast daemon failed during startup" in err
    assert "Traceback" not in err


def test_serve_subset_reports_unrequested_components(tmp_path: Path) -> None:
    """The bare-broadcast subset reports every other component as not requested."""
    env, _dirs = _serve_env(tmp_path)
    proc, out_path, _ = _spawn_serve(["broadcast"], env, tmp_path)
    try:
        manifest = _wait_for_ready(proc, out_path)
        assert "serve: listener: skipped (not requested)" in manifest
        assert "serve: fs: skipped (not requested)" in manifest
        assert "serve: docker: skipped (not requested)" in manifest
        assert _interrupt_and_wait(proc) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)
