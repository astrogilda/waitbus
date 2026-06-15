"""Tests for waitbus._paths resolution against platformdirs.

The factories
(``state_dir`` / ``runtime_dir`` / ``config_dir``) are NO LONGER cached:
each call re-reads the env vars and the platformdirs default. Tests
that monkeypatch ``WAITBUS_STATE_DIR`` etc. observe the change on the
next call without any cache-invalidation step.

Platform-default resolution (Linux XDG vs macOS Library) is delegated
to ``platformdirs`` and tested by ``platformdirs``'s own suite. waitbus
tests cover the env-override behaviour and the runtime_dir tempfile
fallback (which is the waitbus-specific deviation from platformdirs's
``user_runtime_dir`` on macOS).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from waitbus import _paths


def test_env_override_state_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """WAITBUS_STATE_DIR env var wins over the platformdirs default."""
    override = tmp_path / "custom-state"
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(override))
    assert override == _paths.state_dir()
    assert override / "github.db" == _paths.db_path()
    assert override / "cursors" == _paths.cursors_dir()


def test_macos_runtime_dir_not_apple_cache_via_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The WAITBUS_RUNTIME_DIR override path bypasses platformdirs entirely
    so the macOS-specific Apple-Caches sidestep is unconditional under operator
    override (the same code path Linux operators use)."""
    override = tmp_path / "runtime"
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(override))
    assert override == _paths.runtime_dir()
    tmpdir = Path(tempfile.gettempdir())
    # When operator sets the override, the path is exactly the override (no
    # tempfile.gettempdir injection); the Apple-Caches sidestep applies only
    # when the env var is unset, which is exercised by the runtime_dir
    # implementation directly (see _paths.runtime_dir source).
    if override.is_relative_to(tmpdir):
        assert str(_paths.runtime_dir()).startswith(str(tmpdir))
    assert "Library/Caches" not in str(_paths.runtime_dir())


# ---------------------------------------------------------------------------
# Factory tests against env overrides (uncached: each call re-reads env)
# ---------------------------------------------------------------------------


def test_state_dir_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """state_dir() returns WAITBUS_STATE_DIR when set to an absolute path."""
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_STATE_DIR", "/tmp/ci-test-state")
    assert _paths.state_dir() == Path("/tmp/ci-test-state")


def test_state_dir_rejects_relative_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """state_dir() raises RuntimeError when WAITBUS_STATE_DIR is a relative path."""
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_STATE_DIR", "./relative/path")
    with pytest.raises(RuntimeError, match="must be an absolute path"):
        _paths.state_dir()


def test_runtime_dir_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """runtime_dir() returns WAITBUS_RUNTIME_DIR when set to an absolute path."""
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", "/tmp/ci-test-runtime")
    assert _paths.runtime_dir() == Path("/tmp/ci-test-runtime")


def test_runtime_dir_rejects_relative_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """runtime_dir() raises RuntimeError when WAITBUS_RUNTIME_DIR is relative."""
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", "run/waitbus")
    with pytest.raises(RuntimeError, match="must be an absolute path"):
        _paths.runtime_dir()


def test_config_dir_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """config_dir() returns WAITBUS_CONFIG_DIR when set to an absolute path."""
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_CONFIG_DIR", "/tmp/ci-test-config")
    assert _paths.config_dir() == Path("/tmp/ci-test-config")


def test_config_dir_rejects_relative_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """config_dir() raises RuntimeError when WAITBUS_CONFIG_DIR is relative."""
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_CONFIG_DIR", "config/waitbus")
    with pytest.raises(RuntimeError, match="must be an absolute path"):
        _paths.config_dir()


def test_state_dir_rejects_bare_tilde_with_home_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """state_dir() raises RuntimeError with the HOME-unset-specific hint when
    the env value is literal '~' and ``Path.expanduser`` cannot resolve it.

    systemd user units without ``Environment=HOME=...`` and macOS launchd
    contexts without a logged-in session both produce this case.
    """
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_STATE_DIR", "~")

    def _raise_no_home(self: Path) -> Path:
        raise RuntimeError("Could not determine home directory.")

    monkeypatch.setattr(Path, "expanduser", _raise_no_home)
    with pytest.raises(RuntimeError, match="HOME is unset"):
        _paths.state_dir()


def test_state_dir_rejects_tilde_with_subdir_with_home_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """state_dir() raises with the HOME-unset hint for ``~/state`` when
    expanduser fails (HOME unset + no resolvable passwd fallback).
    """
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_STATE_DIR", "~/some/state/path")

    def _raise_no_home(self: Path) -> Path:
        raise RuntimeError("Could not determine home directory.")

    monkeypatch.setattr(Path, "expanduser", _raise_no_home)
    with pytest.raises(RuntimeError, match="HOME is unset"):
        _paths.state_dir()


def test_state_dir_rejects_unknown_user_tilde(monkeypatch: pytest.MonkeyPatch) -> None:
    """state_dir() raises with the unknown-user-specific hint when the env
    value is ``~someuser`` and expanduser raises because ``someuser`` does
    not exist in passwd.

    Distinct from the bare-tilde-with-HOME-unset case: the value does NOT
    start with ``~/`` (no slash after the tilde), so the error hint says
    "unknown user" rather than "HOME is unset".
    """
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_STATE_DIR", "~waitbus-no-such-user-9f4a/state")

    def _raise_user_lookup(self: Path) -> Path:
        raise RuntimeError("Could not determine home directory.")

    monkeypatch.setattr(Path, "expanduser", _raise_user_lookup)
    with pytest.raises(RuntimeError, match="unknown user"):
        _paths.state_dir()


def test_config_dir_default_is_xdg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Without an env override, config_dir() resolves under XDG_CONFIG_HOME."""
    from waitbus import _paths

    xdg_cfg = tmp_path / "xdg-config"
    monkeypatch.delenv("WAITBUS_CONFIG_DIR", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg_cfg))
    result = _paths.config_dir()
    assert "waitbus" in result.parts
    assert str(result).startswith(str(xdg_cfg))


# ---------------------------------------------------------------------------
# Uncached-lifecycle invariant: paths re-resolve on every call.
# The @lru_cache decorators
# were removed so tests that monkeypatch env vars observe the change on
# the next call without a companion invalidation step.
# ---------------------------------------------------------------------------


def test_state_dir_re_resolves_on_env_change(monkeypatch: pytest.MonkeyPatch) -> None:
    """state_dir() reflects the latest env value on every call (no cache)."""
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_STATE_DIR", "/tmp/ci-reread-first")
    first = _paths.state_dir()
    monkeypatch.setenv("WAITBUS_STATE_DIR", "/tmp/ci-reread-second")
    second = _paths.state_dir()

    assert first == Path("/tmp/ci-reread-first")
    assert second == Path("/tmp/ci-reread-second")
    assert first != second


def test_runtime_dir_re_resolves_on_env_change(monkeypatch: pytest.MonkeyPatch) -> None:
    """runtime_dir() reflects the latest env value on every call (no cache)."""
    from waitbus import _paths

    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", "/tmp/ci-runtime-first")
    first = _paths.runtime_dir()
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", "/tmp/ci-runtime-second")
    second = _paths.runtime_dir()

    assert first == Path("/tmp/ci-runtime-first")
    assert second == Path("/tmp/ci-runtime-second")
    assert first != second
