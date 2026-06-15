"""Pin the daemon-group teardown contract against never-started daemons.

The soak harness runs ``terminate_daemon_group(proc)`` in a ``finally``
block. If the daemon never reached READY -- or already exited -- the
teardown must still be a no-op rather than raising and masking the
original startup error. These tests pin that contract: teardown never
raises for an already-exited process, kills a live process group, and is
safe to call repeatedly. Both spawned processes lack the spawner-death
pipe attribute, covering the getattr-default path as well.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from benchmarks._harness import terminate_daemon_group

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="process-group teardown uses POSIX process groups",
)


def test_terminate_daemon_group_tolerates_already_exited_process() -> None:
    """Teardown of a process that already exited must not raise, twice over."""
    proc = subprocess.Popen([sys.executable, "-c", ""], start_new_session=True)
    proc.wait()
    terminate_daemon_group(proc, term_timeout=1.0, kill_timeout=1.0)
    # Repeat: a double teardown (e.g. finally after an explicit teardown)
    # must be equally safe.
    terminate_daemon_group(proc, term_timeout=1.0, kill_timeout=1.0)


def test_terminate_daemon_group_kills_live_group_and_is_repeatable() -> None:
    """Teardown of a live process group terminates it; a second call is a no-op."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
    )
    try:
        terminate_daemon_group(proc, term_timeout=2.0, kill_timeout=2.0)
        assert proc.returncode is not None
        terminate_daemon_group(proc, term_timeout=1.0, kill_timeout=1.0)
    finally:
        # Never leak the sleeper if an assertion fired before teardown ran.
        if proc.returncode is None:
            proc.kill()
            proc.wait()
