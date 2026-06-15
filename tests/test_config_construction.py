"""Construction-time smoke test for ``WaitbusConfig``.

Exists to lock the pydantic-settings ``settings_customise_sources``
callback contract: the framework calls the method with keyword arguments
matching the parameter names listed in the canonical signature.  A
future contributor who renames any of the four source-factory parameters
(e.g. to underscore-prefixed forms to quiet a vulture dead-arg warning)
would raise ``TypeError: got an unexpected keyword argument 'dotenv_settings'``
on the very first ``WaitbusConfig()`` construction.  This test catches
that class of regression at test-time before it can ride a release.

The test runs against a cleared environment so it does not depend on the
operator's real ``$WAITBUS_*`` env vars; it tolerates a missing
``config.toml`` (the default-defaults path) so it does not depend on the
operator's real config file either.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from waitbus import _config


def test_ci_status_config_constructs_against_cleared_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Constructing ``WaitbusConfig()`` against a cleared environment
    succeeds and produces an instance with the documented field defaults.

    The construction path exercises the full ``settings_customise_sources``
    callback chain that pydantic-settings calls with keyword arguments;
    a rename of any of the four source-factory parameter names would
    raise ``TypeError`` here.  An empty ``WAITBUS_CONFIG_DIR`` pointing
    at a directory that does not contain a ``config.toml`` exercises the
    file-missing branch of ``_TomlSettingsSource``.
    """
    for var in (
        "WAITBUS_LOG_LEVEL",
        "WAITBUS_STALL_THRESHOLD_MIN",
        "WAITBUS_HEARTBEAT_SEC",
        "WAITBUS_PROM_OWNER",
        "WAITBUS_PROM_REPO",
        "WAITBUS_MCP_FILTER",
        "WAITBUS_MCP_EVENT_TYPES",
        "WAITBUS_MCP_SINCE",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(tmp_path))
    _config.get_config.cache_clear()

    cfg = _config.WaitbusConfig.from_environment_and_toml()

    assert cfg.log_level == "INFO"
    assert cfg.stall_threshold_min == 60
    assert cfg.heartbeat_sec == 60.0
    assert cfg.prom_owner == "prometheus"
    assert cfg.prom_repo == "alerts"
    assert cfg.mcp_filter == ["*"]
    assert cfg.mcp_event_types is None
    assert cfg.mcp_since is None
