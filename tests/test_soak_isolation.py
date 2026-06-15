"""Regression test for the ``_isolated_waitbus_dirs`` context manager.

Verifies that environment variable restoration runs even when the body
of the ``with`` block raises an exception.  This guards against a class
of bug where a daemon spawn failure inside the isolated context leaks
the soak's temporary ``WAITBUS_STATE_DIR`` / ``WAITBUS_RUNTIME_DIR``
values into the calling process's environment for the rest of its
lifetime.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts.soak._suspend import _isolated_waitbus_dirs


def test_env_vars_restored_after_raise(tmp_path: Path) -> None:
    """Env vars must not leak into the process when the body raises."""
    state_dir = tmp_path / "state"
    runtime_dir = tmp_path / "runtime"
    state_dir.mkdir()
    runtime_dir.mkdir()

    # Capture the pre-context values (may be absent).
    before_state = os.environ.get("WAITBUS_STATE_DIR", "__ABSENT__")
    before_runtime = os.environ.get("WAITBUS_RUNTIME_DIR", "__ABSENT__")

    with (
        pytest.raises(OSError, match="daemon spawn failed"),
        _isolated_waitbus_dirs(state_dir=state_dir, runtime_dir=runtime_dir),
    ):
        raise OSError("daemon spawn failed")

    # After the with-block, env must be back to its pre-context state.
    after_state = os.environ.get("WAITBUS_STATE_DIR", "__ABSENT__")
    after_runtime = os.environ.get("WAITBUS_RUNTIME_DIR", "__ABSENT__")

    assert after_state == before_state, (
        f"WAITBUS_STATE_DIR leaked after _isolated_waitbus_dirs raise: was {before_state!r}, now {after_state!r}"
    )
    assert after_runtime == before_runtime, (
        f"WAITBUS_RUNTIME_DIR leaked after _isolated_waitbus_dirs raise: was {before_runtime!r}, now {after_runtime!r}"
    )
    # Explicitly confirm the soak's temp values are gone.
    assert os.environ.get("WAITBUS_STATE_DIR") != str(state_dir)
    assert os.environ.get("WAITBUS_RUNTIME_DIR") != str(runtime_dir)


def test_env_vars_set_inside_context(tmp_path: Path) -> None:
    """Inside the context, the env vars must point at the soak's isolated dirs."""
    state_dir = tmp_path / "state"
    runtime_dir = tmp_path / "runtime"
    state_dir.mkdir()
    runtime_dir.mkdir()

    seen_state: list[str] = []
    seen_runtime: list[str] = []

    with _isolated_waitbus_dirs(state_dir=state_dir, runtime_dir=runtime_dir):
        seen_state.append(os.environ.get("WAITBUS_STATE_DIR", ""))
        seen_runtime.append(os.environ.get("WAITBUS_RUNTIME_DIR", ""))

    assert seen_state == [str(state_dir)]
    assert seen_runtime == [str(runtime_dir)]
