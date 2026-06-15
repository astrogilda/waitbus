"""Tests for waitbus._config resolution chain.

The resolution order is, first hit wins:

  1. Environment variable (prefix WAITBUS_).
  2. ~/.config/waitbus/config.toml (XDG-honored; WAITBUS_CONFIG_DIR override).
  3. Built-in field default.

Loud-fail config handling is tested in both its original form (malformed
TOML raises RuntimeError with a remediation hint) and its new form (a bad env
var value also raises RuntimeError, not silently falls back to defaults).

Tests isolate via monkeypatch.setenv / WAITBUS_CONFIG_DIR + _reset_for_test();
they do NOT reimport the module.  WAITBUS_CONFIG_DIR is the config directory
itself (the directory that contains config.toml), so tests point it at the
directory they create.
"""

from __future__ import annotations

from pathlib import Path

import hypothesis.strategies as st
import pytest
from hypothesis import HealthCheck, example, given, settings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_prometheus_toml(cfg_dir: Path, owner: str | None, repo: str | None) -> None:
    """Write a [prometheus] TOML block with only the supplied (non-None) keys."""
    cfg_dir.mkdir(parents=True, exist_ok=True)
    lines = ["[prometheus]\n"]
    if owner is not None:
        lines.append(f"owner = {_toml_str(owner)}\n")
    if repo is not None:
        lines.append(f"repo = {_toml_str(repo)}\n")
    (cfg_dir / "config.toml").write_text("".join(lines))


def _toml_str(value: str) -> str:
    """Encode a Python string as a TOML double-quoted string literal."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _get_fresh(monkeypatch: pytest.MonkeyPatch) -> waitbus._config.WaitbusConfig:  # type: ignore[name-defined]  # noqa: F821
    """Reset the lru_cache and return a freshly loaded config."""
    from waitbus import _config

    _config._reset_for_test()
    return _config.get_config()


# ---------------------------------------------------------------------------
# Basic unit tests
# ---------------------------------------------------------------------------


def test_config_defaults_match_field_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Construct WaitbusConfig with no env, no TOML; assert every field default."""
    for var in (
        "WAITBUS_PROM_OWNER",
        "WAITBUS_PROM_REPO",
        "WAITBUS_LOG_LEVEL",
        "WAITBUS_STALL_THRESHOLD_MIN",
        "WAITBUS_HEARTBEAT_SEC",
    ):
        monkeypatch.delenv(var, raising=False)
    # Point config dir at an empty directory (no config.toml present).
    cfg_dir = tmp_path / "empty-config"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    cfg = _get_fresh(monkeypatch)
    assert cfg.prom_owner == "prometheus"
    assert cfg.prom_repo == "alerts"
    assert cfg.log_level == "INFO"
    assert cfg.stall_threshold_min == 60
    assert cfg.heartbeat_sec == 60.0


def test_env_overrides_toml_value(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Env var takes precedence over matching TOML key."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.toml").write_text("stall_threshold_min = 999\n")
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("WAITBUS_STALL_THRESHOLD_MIN", "42")
    cfg = _get_fresh(monkeypatch)
    assert cfg.stall_threshold_min == 42


def test_toml_overrides_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """TOML value is used when the corresponding env var is absent."""
    monkeypatch.delenv("WAITBUS_PROM_OWNER", raising=False)
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.toml").write_text('[prometheus]\nowner = "my-org"\n')
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    cfg = _get_fresh(monkeypatch)
    assert cfg.prom_owner == "my-org"


def test_fs_watch_path_defaults_to_none(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """fs_watch_path is unset by default (the fs watcher is skipped)."""
    monkeypatch.delenv("WAITBUS_FS_WATCH_PATH", raising=False)
    cfg_dir = tmp_path / "empty-config"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    cfg = _get_fresh(monkeypatch)
    assert cfg.fs_watch_path is None


def test_fs_watch_path_env_override_round_trips(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """WAITBUS_FS_WATCH_PATH resolves to a Path field value."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    watch_dir = tmp_path / "watched"
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("WAITBUS_FS_WATCH_PATH", str(watch_dir))
    cfg = _get_fresh(monkeypatch)
    assert cfg.fs_watch_path == watch_dir


def test_fs_watch_path_toml_overrides_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A top-level fs_watch_path key in config.toml is honoured."""
    monkeypatch.delenv("WAITBUS_FS_WATCH_PATH", raising=False)
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    watch_dir = tmp_path / "watched"
    (cfg_dir / "config.toml").write_text(f"fs_watch_path = {_toml_str(str(watch_dir))}\n")
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    cfg = _get_fresh(monkeypatch)
    assert cfg.fs_watch_path == watch_dir


def test_malformed_toml_raises_runtime_error_with_hint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A broken config.toml must loud-fail with a remediation hint in the message.

    Silent fallback is forbidden.
    """
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.toml").write_text("not = valid toml :::")
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    with pytest.raises(RuntimeError, match="malformed TOML"):
        _get_fresh(monkeypatch)


def test_invalid_stall_threshold_raises_runtime_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """An out-of-range stall_threshold_min (ge=1 violated) raises RuntimeError."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("WAITBUS_STALL_THRESHOLD_MIN", "0")
    with pytest.raises(RuntimeError):
        _get_fresh(monkeypatch)


def test_invalid_log_level_raises_runtime_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """An unrecognised log level raises RuntimeError, not silent fallback."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("WAITBUS_LOG_LEVEL", "DEBOG")
    with pytest.raises(RuntimeError, match="log_level"):
        _get_fresh(monkeypatch)


def test_get_config_caches_within_process(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Two consecutive get_config() calls return the same object."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    from waitbus import _config

    _config._reset_for_test()
    first = _config.get_config()
    second = _config.get_config()
    assert first is second


def test_reset_for_test_clears_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """_reset_for_test() lets a subsequent get_config() pick up env changes."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("WAITBUS_STALL_THRESHOLD_MIN", "30")
    from waitbus import _config

    _config._reset_for_test()
    first = _config.get_config()
    assert first.stall_threshold_min == 30

    monkeypatch.setenv("WAITBUS_STALL_THRESHOLD_MIN", "45")
    _config._reset_for_test()
    second = _config.get_config()
    assert second.stall_threshold_min == 45
    assert first is not second


def test_log_level_case_insensitive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """log_level is accepted in any capitalisation and normalised to upper-case."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("WAITBUS_LOG_LEVEL", "debug")
    cfg = _get_fresh(monkeypatch)
    assert cfg.log_level == "DEBUG"


def test_toml_flat_keys_loaded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Flat TOML keys (log_level, stall_threshold_min, heartbeat_sec) are loaded."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.toml").write_text('log_level = "WARNING"\nstall_threshold_min = 15\nheartbeat_sec = 30.0\n')
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    for var in ("WAITBUS_LOG_LEVEL", "WAITBUS_STALL_THRESHOLD_MIN", "WAITBUS_HEARTBEAT_SEC"):
        monkeypatch.delenv(var, raising=False)
    cfg = _get_fresh(monkeypatch)
    assert cfg.log_level == "WARNING"
    assert cfg.stall_threshold_min == 15
    assert cfg.heartbeat_sec == 30.0


def test_permission_error_raises_runtime_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """An unreadable config.toml raises RuntimeError with a permissions hint."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    config_file = cfg_dir / "config.toml"
    config_file.write_text('[prometheus]\nowner = "x"\n')
    config_file.chmod(0o000)
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    try:
        with pytest.raises(RuntimeError, match="unreadable"):
            _get_fresh(monkeypatch)
    finally:
        config_file.chmod(0o644)


# ---------------------------------------------------------------------------
# Backwards-compat: existing behaviour preserved with new API
# ---------------------------------------------------------------------------


def test_defaults_when_no_file_and_no_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for var in ("WAITBUS_PROM_OWNER", "WAITBUS_PROM_REPO"):
        monkeypatch.delenv(var, raising=False)
    # Point at a directory that has no config.toml.
    cfg_dir = tmp_path / "empty"
    cfg_dir.mkdir()
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    cfg = _get_fresh(monkeypatch)
    assert cfg.prom_owner == "prometheus"
    assert cfg.prom_repo == "alerts"


def test_toml_file_overrides_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for var in ("WAITBUS_PROM_OWNER", "WAITBUS_PROM_REPO"):
        monkeypatch.delenv(var, raising=False)
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.toml").write_text('[prometheus]\nowner = "acme"\nrepo = "platform-alerts"\n')
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    cfg = _get_fresh(monkeypatch)
    assert cfg.prom_owner == "acme"
    assert cfg.prom_repo == "platform-alerts"


def test_env_var_overrides_toml(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.toml").write_text('[prometheus]\nowner = "file-owner"\nrepo = "file-repo"\n')
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setenv("WAITBUS_PROM_OWNER", "env-owner")
    monkeypatch.setenv("WAITBUS_PROM_REPO", "env-repo")
    cfg = _get_fresh(monkeypatch)
    assert cfg.prom_owner == "env-owner"
    assert cfg.prom_repo == "env-repo"


def test_malformed_toml_raises_loudly_at_load(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A broken config MUST loud-fail at load time. Silent-fallback would let an
    operator typo silently route events to the wrong label and contaminate workflow
    data. The error message must point at the offending file AND suggest a remediation.
    """
    for var in ("WAITBUS_PROM_OWNER", "WAITBUS_PROM_REPO"):
        monkeypatch.delenv(var, raising=False)
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.toml").write_text("this is = not [ valid toml")
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    with pytest.raises(RuntimeError, match="malformed TOML"):
        _get_fresh(monkeypatch)


def test_partial_toml_only_overrides_present_keys(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for var in ("WAITBUS_PROM_OWNER", "WAITBUS_PROM_REPO"):
        monkeypatch.delenv(var, raising=False)
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.toml").write_text('[prometheus]\nowner = "only-owner-set"\n')
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
    cfg = _get_fresh(monkeypatch)
    assert cfg.prom_owner == "only-owner-set"
    assert cfg.prom_repo == "alerts"  # default kept


# ---------------------------------------------------------------------------
# Property-based tests (hypothesis)
# ---------------------------------------------------------------------------

# Shared alphabet: printable ASCII characters safe for use inside TOML
# double-quoted strings (no backslash, no double-quote, no control chars).
_PRINTABLE = st.characters(
    whitelist_categories=("Ll", "Lu", "Nd"),
    whitelist_characters=" !#$%&'()*+,-./:;<=>?@[]^_`{|}~",
)
_NONEMPTY_STR = st.text(min_size=1, max_size=50, alphabet=_PRINTABLE)

_ENV_VARS = ("WAITBUS_PROM_OWNER", "WAITBUS_PROM_REPO")


# ---------------------------------------------------------------------------
# Property 1: env var always wins when set to a non-empty value
# ---------------------------------------------------------------------------


@given(
    env_owner=_NONEMPTY_STR,
    env_repo=_NONEMPTY_STR,
    toml_owner=st.one_of(st.none(), _NONEMPTY_STR),
    toml_repo=st.one_of(st.none(), _NONEMPTY_STR),
)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@example(env_owner="e-owner", env_repo="e-repo", toml_owner="t-owner", toml_repo="t-repo")
@example(env_owner="x", env_repo="y", toml_owner=None, toml_repo=None)
@example(env_owner="only-env", env_repo="only-env-repo", toml_owner="", toml_repo="")
def test_env_var_always_wins_when_set_to_non_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    env_owner: str,
    env_repo: str,
    toml_owner: str | None,
    toml_repo: str | None,
) -> None:
    tmp = tmp_path_factory.mktemp("prop1")
    mp = pytest.MonkeyPatch()
    try:
        mp.setenv("WAITBUS_PROM_OWNER", env_owner)
        mp.setenv("WAITBUS_PROM_REPO", env_repo)
        cfg_dir = tmp / "waitbus"
        _write_prometheus_toml(cfg_dir, toml_owner, toml_repo)
        mp.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
        from waitbus import _config

        _config._reset_for_test()
        cfg = _config.get_config()
        assert env_owner == cfg.prom_owner
        assert env_repo == cfg.prom_repo
    finally:
        mp.undo()
        from waitbus import _config as _c

        _c._reset_for_test()


# ---------------------------------------------------------------------------
# Property 2: TOML wins when env is absent or empty
# ---------------------------------------------------------------------------


@given(
    env_sentinel=st.sampled_from([None, ""]),
    toml_owner=_NONEMPTY_STR,
    toml_repo=_NONEMPTY_STR,
)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@example(env_sentinel=None, toml_owner="acme", toml_repo="alerts")
@example(env_sentinel="", toml_owner="acme", toml_repo="alerts")
@example(env_sentinel=None, toml_owner="a", toml_repo="b")
def test_toml_wins_when_env_absent_or_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    env_sentinel: str | None,
    toml_owner: str,
    toml_repo: str,
) -> None:
    tmp = tmp_path_factory.mktemp("prop2")
    mp = pytest.MonkeyPatch()
    try:
        for var in _ENV_VARS:
            if env_sentinel is None:
                mp.delenv(var, raising=False)
            else:
                mp.setenv(var, env_sentinel)
        cfg_dir = tmp / "waitbus"
        _write_prometheus_toml(cfg_dir, toml_owner, toml_repo)
        mp.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
        from waitbus import _config

        _config._reset_for_test()
        # Empty string env vars: pydantic-settings may see them as "set" and
        # our min_length=1 will reject them (loud-fail).  Both outcomes are
        # acceptable; the forbidden outcome is silent fallback to defaults.
        if env_sentinel == "":
            try:
                cfg = _config.get_config()
                # Didn't raise — pydantic-settings ignored the empty string
                # and used TOML.  Either outcome is acceptable.
                assert toml_owner == cfg.prom_owner
                assert toml_repo == cfg.prom_repo
            except RuntimeError:
                pass  # loud-fail on empty string is also acceptable
        else:
            cfg = _config.get_config()
            assert toml_owner == cfg.prom_owner
            assert toml_repo == cfg.prom_repo
    finally:
        mp.undo()
        from waitbus import _config as _c

        _c._reset_for_test()


# ---------------------------------------------------------------------------
# Property 3: defaults when neither env nor TOML is set
# ---------------------------------------------------------------------------


@given(
    env_sentinel=st.sampled_from([None, ""]),
    include_toml=st.booleans(),
)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@example(env_sentinel=None, include_toml=False)
@example(env_sentinel="", include_toml=False)
@example(env_sentinel=None, include_toml=True)
def test_default_when_neither_set(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    env_sentinel: str | None,
    include_toml: bool,
) -> None:
    tmp = tmp_path_factory.mktemp("prop3")
    mp = pytest.MonkeyPatch()
    try:
        for var in _ENV_VARS:
            if env_sentinel is None:
                mp.delenv(var, raising=False)
            else:
                mp.setenv(var, env_sentinel)
        cfg_dir = tmp / "waitbus"
        if include_toml:
            # [prometheus] section present but none of the keys → defaults still apply
            cfg_dir.mkdir(parents=True, exist_ok=True)
            (cfg_dir / "config.toml").write_text("[prometheus]\n")
        else:
            cfg_dir.mkdir(parents=True, exist_ok=True)
        mp.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
        from waitbus import _config

        _config._reset_for_test()
        if env_sentinel == "":
            # Empty string may loud-fail on min_length=1; both outcomes acceptable.
            try:
                cfg = _config.get_config()
                assert cfg.prom_owner == "prometheus"
                assert cfg.prom_repo == "alerts"
            except RuntimeError:
                pass
        else:
            cfg = _config.get_config()
            assert cfg.prom_owner == "prometheus"
            assert cfg.prom_repo == "alerts"
    finally:
        mp.undo()
        from waitbus import _config as _c

        _c._reset_for_test()


# ---------------------------------------------------------------------------
# Property 4: partial TOML overrides only present keys
# ---------------------------------------------------------------------------


@given(
    set_owner=st.booleans(),
    set_repo=st.booleans(),
    toml_owner=_NONEMPTY_STR,
    toml_repo=_NONEMPTY_STR,
)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@example(set_owner=True, set_repo=False, toml_owner="my-org", toml_repo="x")
@example(set_owner=False, set_repo=True, toml_owner="x", toml_repo="my-repo")
@example(set_owner=False, set_repo=False, toml_owner="x", toml_repo="x")
def test_partial_toml_only_overrides_present_keys_property(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    set_owner: bool,
    set_repo: bool,
    toml_owner: str,
    toml_repo: str,
) -> None:
    tmp = tmp_path_factory.mktemp("prop4")
    mp = pytest.MonkeyPatch()
    try:
        for var in _ENV_VARS:
            mp.delenv(var, raising=False)
        cfg_dir = tmp / "waitbus"
        _write_prometheus_toml(
            cfg_dir,
            toml_owner if set_owner else None,
            toml_repo if set_repo else None,
        )
        mp.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
        from waitbus import _config

        _config._reset_for_test()
        cfg = _config.get_config()

        expected_owner = toml_owner if set_owner else "prometheus"
        expected_repo = toml_repo if set_repo else "alerts"

        assert expected_owner == cfg.prom_owner
        assert expected_repo == cfg.prom_repo
    finally:
        mp.undo()
        from waitbus import _config as _c

        _c._reset_for_test()


# ---------------------------------------------------------------------------
# Property 5: malformed TOML never silently falls back
# ---------------------------------------------------------------------------


@given(
    raw_bytes=st.text(max_size=200).map(lambda s: s.encode("utf-8")),
)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@example(raw_bytes=b"this is = not [ valid toml")
@example(raw_bytes=b"[prometheus\nowner = 'x'")
@example(raw_bytes=b"")
def test_arbitrary_bytes_either_parse_or_raise_loud(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    raw_bytes: bytes,
) -> None:
    tmp = tmp_path_factory.mktemp("prop5")
    mp = pytest.MonkeyPatch()
    try:
        for var in _ENV_VARS:
            mp.delenv(var, raising=False)
        cfg_dir = tmp / "waitbus"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        (cfg_dir / "config.toml").write_bytes(raw_bytes)
        mp.setenv("WAITBUS_CONFIG_DIR", str(cfg_dir))
        from waitbus import _config

        _config._reset_for_test()
        # Two acceptable outcomes; one forbidden outcome (silent fallback).
        try:
            cfg = _config.get_config()
        except RuntimeError as exc:
            # Acceptable: loud-fail. Error message must reference the file.
            assert "malformed TOML" in str(exc) or "unreadable" in str(exc)
            return
        # The config loaded — assert it resolved sensible values.
        assert isinstance(cfg.prom_owner, str) and cfg.prom_owner
        assert isinstance(cfg.prom_repo, str) and cfg.prom_repo
    finally:
        mp.undo()
        from waitbus import _config as _c

        _c._reset_for_test()
