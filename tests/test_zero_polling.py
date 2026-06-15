"""Zero-polling structural assertion.

waitbus's launch position-line claims "zero polling": a correctly
implemented blocking subscriber, when it has no work to do, sits in
exactly one of ``read`` / ``recv`` / ``epoll_wait`` / ``poll`` /
``select`` and makes zero syscalls per unit time while waiting. Any
non-zero syscall rate during a known-idle window is evidence of a
busy loop, a misconfigured timeout, or a polling fallback path -- a
regression that this test class catches loudly.

The test runs ``perf stat -e raw_syscalls:sys_enter -p <pid> sleep N``
against a subscriber subprocess parked on a never-matching predicate.
``perf`` reads the kernel tracepoint with very low overhead (cf.
``strace``'s ptrace-based mechanism which Brendan Gregg measured at
~60x slowdown), so the cost of observing is decoupled from the cost
of the daemon being observed -- a structural prerequisite for the
zero-syscall claim to hold under measurement.

The corroborating signal comes from ``/proc/<pid>/status`` voluntary
+ nonvoluntary context-switch counts (delta over the same idle
window). A correctly-blocked task on a kernel waitqueue is not
runnable -- it cannot be preempted -- so ``nonvoluntary_ctxt_switches``
must not increment. ``voluntary_ctxt_switches`` increments at most
once (the at-most-one transition into the blocking syscall if it
occurred during the window).

@pytest.mark.requires_perf gates execution on the perf binary; CI
runners without perf installed skip the test rather than fail
falsely.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts.stress._scrape import read_ctxt_switches

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="AF_UNIX SO_PEERCRED daemon + perf are Linux-only",
)


# The idle window length is long enough that one missed
# context-switch counter increment is statistically harmless yet short
# enough to keep CI runtime tight. 5 s matches the canonical Brendan
# Gregg perf-stat examples.
_IDLE_WINDOW_SEC = 5

# perf paranoid lower than 1 (kernel tracepoint readable without
# capabilities) is the prerequisite for the syscall-tracepoint
# assertion to be measurable without sudo. We skip rather than fail
# when the runner has the perf binary but the paranoid level forbids
# the read -- the failure mode is environmental, not a regression.
_PERF_PARANOID_PATH = Path("/proc/sys/kernel/perf_event_paranoid")
_SYSCALL_COUNT_RE = re.compile(r"^\s*([\d,]+)\s+raw_syscalls:sys_enter", re.MULTILINE)


def _perf_available() -> bool:
    if shutil.which("perf") is None:
        return False
    if not _PERF_PARANOID_PATH.exists():
        return False
    try:
        level = int(_PERF_PARANOID_PATH.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return False
    # paranoid <= 1 lets perf attach to tracepoints without CAP_SYS_ADMIN.
    return level <= 1


def _parse_perf_syscall_count(stderr: str) -> int:
    """Parse the ``raw_syscalls:sys_enter`` row out of a perf-stat stderr block.

    perf writes its summary to stderr (not stdout) by default. The
    ``-x ,`` machine-readable form is not used here so the test
    matches the canonical Gregg invocation; the regex captures the
    leading count and strips thousands separators.
    """
    match = _SYSCALL_COUNT_RE.search(stderr)
    if not match:
        raise AssertionError(f"perf stat output did not carry the raw_syscalls:sys_enter row:\n{stderr}")
    return int(match.group(1).replace(",", ""))


@pytest.mark.skipif(not _perf_available(), reason="perf not available or paranoid level too restrictive")
def test_blocked_subscriber_issues_zero_syscalls_during_idle_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A subscriber parked on a never-matching predicate must idle silently.

    Spawn the broadcast daemon + a ``waitbus wait`` subprocess pinned
    on an unmatchable predicate; once the subscriber has registered
    we ``perf stat`` it for the idle window. The kernel tracepoint
    must record zero syscall entries; ``/proc/<pid>/status``
    nonvoluntary_ctxt_switches must not increment. Any deviation is
    structural evidence that some code path is polling rather than
    blocking -- the exact regression this structural test is designed
    to catch.
    """
    state_dir = tmp_path / "state"
    runtime_dir = tmp_path / "runtime"
    state_dir.mkdir()
    runtime_dir.mkdir()
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(state_dir))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(runtime_dir))
    # Tight metrics-snapshot period would dump JSON lines from the daemon
    # mid-window; the structural assertion is about the SUBSCRIBER's
    # syscall count, not the daemon's, but stretching the snapshot
    # period out keeps the daemon-side log volume down and makes the
    # test's interleaving easier to reason about post-mortem.
    monkeypatch.setenv("WAITBUS_METRICS_SNAPSHOT_PERIOD_SEC", "30.0")
    monkeypatch.setenv("WAITBUS_HEARTBEAT_SEC", "30.0")

    env = dict(os.environ)
    waitbus = str(Path(sys.executable).parent / "waitbus")

    # Spawn the broadcast daemon. The daemon writes structured-log
    # lines to stderr; we redirect them to a file so the perf-stat
    # output stays uncluttered.
    daemon_stderr_path = tmp_path / "daemon.err"
    with daemon_stderr_path.open("w", encoding="utf-8") as daemon_stderr:
        daemon = subprocess.Popen(
            [waitbus, "broadcast", "serve"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=daemon_stderr,
        )
        try:
            broadcast_socket = runtime_dir / "broadcast.sock"
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                if broadcast_socket.exists():
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError(f"broadcast daemon did not bind {broadcast_socket} in time")

            # Spawn a subscriber parked on an unmatchable predicate. The
            # subscriber will block in the canonical select-with-deadline
            # path; this is what the assertion is about.
            subscriber = subprocess.Popen(
                [
                    waitbus,
                    "wait",
                    "--source",
                    "nonexistent-source",
                    "--timeout",
                    f"{_IDLE_WINDOW_SEC + 30}s",
                ],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                # Give the subscriber time to connect, send subscribe,
                # receive subscribe_ack, and enter the blocking select.
                time.sleep(1.0)
                assert subscriber.poll() is None, "subscriber exited before the measurement window started"

                # Snapshot ctxt switches before the perf measurement.
                pre = read_ctxt_switches(subscriber.pid)

                perf = subprocess.run(
                    [
                        "perf",
                        "stat",
                        "-e",
                        "raw_syscalls:sys_enter",
                        "-p",
                        str(subscriber.pid),
                        "sleep",
                        str(_IDLE_WINDOW_SEC),
                    ],
                    capture_output=True,
                    check=False,
                    text=True,
                )

                post = read_ctxt_switches(subscriber.pid)

                if perf.returncode != 0:
                    pytest.skip(
                        "perf stat failed -- likely tracepoint access denied:\n" + perf.stderr,
                    )

                syscall_count = _parse_perf_syscall_count(perf.stderr)

                # Primary assertion: zero syscall entries during the idle window.
                assert syscall_count == 0, (
                    f"blocked subscriber issued {syscall_count} syscalls over "
                    f"{_IDLE_WINDOW_SEC}s idle window; expected 0:\n{perf.stderr}"
                )

                # Corroborating signals from /proc/<pid>/status.
                assert post.nonvoluntary - pre.nonvoluntary == 0, (
                    "subscriber was preempted while it should have been blocked; "
                    f"nonvoluntary_ctxt_switches delta = {post.nonvoluntary - pre.nonvoluntary}"
                )
                assert post.voluntary - pre.voluntary <= 1, (
                    "subscriber voluntary_ctxt_switches incremented more than once during the "
                    f"idle window: delta = {post.voluntary - pre.voluntary}"
                )
            finally:
                subscriber.terminate()
                try:
                    subscriber.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    subscriber.kill()
                    subscriber.wait(timeout=2.0)
        finally:
            daemon.terminate()
            try:
                daemon.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait(timeout=2.0)


def test_parse_perf_syscall_count_strips_thousands_separator() -> None:
    """The perf-stat output parser handles the ``1,234`` thousands form."""
    sample = (
        "\n Performance counter stats for process id 'X':\n\n"
        "         1,234,567      raw_syscalls:sys_enter\n\n"
        "       5.001234567 seconds time elapsed\n"
    )
    assert _parse_perf_syscall_count(sample) == 1_234_567


def test_parse_perf_syscall_count_handles_zero() -> None:
    """Zero is the expected canonical case for a correctly-blocked subscriber."""
    sample = (
        "\n Performance counter stats for process id 'X':\n\n"
        "                 0      raw_syscalls:sys_enter\n\n"
        "       5.001234567 seconds time elapsed\n"
    )
    assert _parse_perf_syscall_count(sample) == 0


def test_parse_perf_syscall_count_raises_on_missing_row() -> None:
    """A perf-stat output without the syscall row is a setup-side bug, not a pass."""
    with pytest.raises(AssertionError):
        _parse_perf_syscall_count("Performance counter stats unavailable")
