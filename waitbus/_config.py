"""Single-source-of-truth config for waitbus.

Merges operator-visible settings from two sources (highest priority first):

  1. Environment variables (prefix ``WAITBUS_``, e.g. ``WAITBUS_LOG_LEVEL``).
  2. ``~/.config/waitbus/config.toml`` (XDG convention; honors
     ``$WAITBUS_CONFIG_DIR`` and ``$XDG_CONFIG_HOME`` overrides).
  3. Field-level defaults encoded in :class:`CiStatusConfig`.

The config file is optional; the defaults are generic so a
fresh install works without operator intervention.

Loud-fail semantics: a malformed config.toml
or a pydantic ``ValidationError`` raises ``RuntimeError`` at load time with a
clear remediation hint.  Silent fallback to defaults on invalid input is
explicitly forbidden — an operator typo in ``WAITBUS_LOG_LEVEL`` or the
config file must surface immediately, not later as silent misbehaviour.

Path-override env vars (``WAITBUS_STATE_DIR``, ``WAITBUS_RUNTIME_DIR``,
``WAITBUS_CONFIG_DIR``) are handled by ``waitbus._paths`` via
``platformdirs``; they are excluded from this model because their
values are filesystem paths rather than operator-tunable daemon settings.

Example config.toml snippet::

    [prometheus]
    owner = "my-org"
    repo  = "infra-alerts"

    log_level = "DEBUG"
    stall_threshold_min = 30
"""

from __future__ import annotations

import logging
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from . import _paths

logger = logging.getLogger("waitbus.config")

_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def _read_toml_file(toml_path: Path) -> dict[str, Any]:
    """Read and parse a TOML config file, raising RuntimeError on any failure.

    Missing file returns an empty dict (no config file is normal; defaults
    apply). Malformed TOML and permission errors are loud-fail.
    """
    if not toml_path.exists():
        return {}
    try:
        with toml_path.open("rb") as fp:
            return tomllib.load(fp)
    except tomllib.TOMLDecodeError as exc:
        raise RuntimeError(
            f"waitbus: malformed TOML at {toml_path}: {exc}. "
            "Fix the syntax or delete the file to fall back to defaults. "
            "Run `waitbus doctor` to validate the file before restarting "
            "affected services."
        ) from exc
    except PermissionError as exc:
        raise RuntimeError(
            f"waitbus: config file at {toml_path} is unreadable ({exc}). "
            "Fix file permissions (it should be operator-readable) "
            "or delete the file to fall back to defaults."
        ) from exc


def _flatten_toml(toml_data: dict[str, Any]) -> dict[str, Any]:
    """Extract flat field values from the TOML dict.

    Maps two nested subsections to flat field names used in
    :class:`CiStatusConfig`:

    - ``[prometheus]`` -> ``prom_owner`` / ``prom_repo``
    - ``[mcp]`` -> ``mcp_filter`` / ``mcp_event_types`` / ``mcp_since``
      (replaces the retired ``filters.json`` file: the JSON-only filter
      surface was folded into the canonical TOML config tree during the
      pydantic-settings convergence)

    Top-level flat keys (``log_level``, ``stall_threshold_min``,
    ``heartbeat_sec``, ``fs_watch_path``) pass through directly.
    """
    flat: dict[str, Any] = {}
    prom = toml_data.get("prometheus", {})
    if isinstance(prom, dict):
        if "owner" in prom:
            flat["prom_owner"] = prom["owner"]
        if "repo" in prom:
            flat["prom_repo"] = prom["repo"]
    mcp = toml_data.get("mcp", {})
    if isinstance(mcp, dict):
        if "filter" in mcp:
            flat["mcp_filter"] = mcp["filter"]
        if "event_types" in mcp:
            flat["mcp_event_types"] = mcp["event_types"]
        if "since" in mcp:
            flat["mcp_since"] = mcp["since"]
    for key in (
        "log_level",
        "stall_threshold_min",
        "heartbeat_sec",
        "metrics_snapshot_period_sec",
        "metrics_port",
        "fs_watch_path",
    ):
        if key in toml_data:
            flat[key] = toml_data[key]
    return flat


class _TomlSettingsSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source that reads the waitbus config.toml.

    Placed at lower priority than the env-var source so that
    ``WAITBUS_LOG_LEVEL`` in the environment always wins over
    ``log_level = ...`` in the TOML file.  The TOML data is read once
    at construction time and cached for the lifetime of this source
    object (which is discarded after CiStatusConfig is built).
    """

    def __init__(self, settings_cls: type[BaseSettings], toml_path: Path) -> None:
        super().__init__(settings_cls)
        # Loud-fail happens here so the RuntimeError propagates cleanly
        # through CiStatusConfig.settings_customise_sources → __init__.
        self._flat = _flatten_toml(_read_toml_file(toml_path))

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        value = self._flat.get(field_name)
        return value, field_name, value is not None

    def __call__(self) -> dict[str, Any]:
        return {k: v for k, v in self._flat.items() if v is not None}


class CiStatusConfig(BaseSettings):
    """All operator-visible env-var-honoring daemon settings in one place.

    Source precedence (highest to lowest): environment variables (prefix
    ``WAITBUS_``) > config.toml > field defaults.

    Pydantic validates types and ranges on construction; a bad value raises
    ``ValidationError`` loudly at load time — never silently ignored.

    Path overrides (``WAITBUS_STATE_DIR`` etc.) are managed by
    ``waitbus._paths`` and are absent here.  Setting
    ``extra="ignore"`` is therefore required so that those env vars do not
    trigger a validation error when they are present in the environment.
    """

    model_config = SettingsConfigDict(
        env_prefix="WAITBUS_",
        env_file=None,
        extra="ignore",  # WAITBUS_STATE_DIR / _RUNTIME_DIR / _CONFIG_DIR live in _paths
        frozen=True,
        validate_default=True,
    )

    # === Prometheus synthetic event labels =================================
    prom_owner: str = Field(
        default="prometheus",
        min_length=1,
        description="Synthetic owner label for prometheus_alert/watchdog rows.",
    )
    prom_repo: str = Field(
        default="alerts",
        min_length=1,
        description="Synthetic repo label for prometheus_alert/watchdog rows.",
    )

    # === Logging ===========================================================
    log_level: str = Field(
        default="INFO",
        description="Stdlib logging level used by all daemon entry points.",
    )

    # === etag-poll =========================================================
    stall_threshold_min: int = Field(
        default=60,
        ge=1,
        description=(
            "Minutes of in-progress job silence before a synthetic stall event is emitted by the etag-poll pass."
        ),
    )

    # === broadcast daemon ==================================================
    heartbeat_sec: float = Field(
        default=60.0,
        gt=0,
        description="Seconds between daemon_heartbeat frames sent to subscribers.",
    )
    metrics_snapshot_period_sec: float = Field(
        default=5.0,
        gt=0,
        description=(
            "Seconds between metrics_snapshot structured-log lines emitted by "
            "the broadcast daemon. Each line carries the current values of "
            "every prometheus_client family in JSON form and is the channel "
            "the stress and soak harnesses use to scrape per-tick metric "
            "state without depending on the optional HTTP /metrics endpoint."
        ),
    )
    metrics_port: int | None = Field(
        default=None,
        ge=0,
        le=65535,
        description=(
            "TCP port for the broadcast daemon's optional Prometheus /metrics "
            "scrape endpoint. Unset (the default) opens no socket. The "
            "endpoint binds 127.0.0.1 only; 0 binds an OS-assigned ephemeral "
            "port (test use)."
        ),
    )

    # === fs watcher ========================================================
    fs_watch_path: Path | None = Field(
        default=None,
        description=(
            "Directory tree the fs watcher supervises when running under "
            "`waitbus serve`; unset means the fs watcher is skipped."
        ),
    )

    # === MCP server filter (was filters.json before v0.4.0) ================
    mcp_filter: list[str] = Field(
        default_factory=lambda: ["*"],
        description=(
            "Repository scopes the MCP server's broadcast subscription will "
            "request. Each entry is either '*' (all events), 'owner/*' (any "
            "repo under the owner), or 'owner/repo' (exact match). The MCP "
            "server reads this list at startup; operators set it in "
            "config.toml under [mcp] filter = [...]."
        ),
    )
    mcp_event_types: list[str] | None = Field(
        default=None,
        description=(
            "Optional event-type filter for the MCP server's broadcast "
            "subscription. When set, the server only receives the listed "
            "event types ('workflow_run', 'workflow_job', "
            "'prometheus_alert', 'prometheus_watchdog'). Unset means all."
        ),
    )
    mcp_since: str | None = Field(
        default=None,
        description=(
            "Optional ULID cursor for the MCP server's broadcast subscription. "
            "When set, the server requests replay of events with id > since. "
            "Operators rarely set this; intended for ops debugging."
        ),
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalise_log_level(cls, v: object) -> str:
        """Upper-case and validate the log level string.

        Accepts any capitalisation of the five stdlib levels; rejects
        anything else with a clear error so the operator knows immediately
        that ``WAITBUS_LOG_LEVEL=debog`` is a typo.
        """
        if not isinstance(v, str):
            raise ValueError(f"log_level must be a string, got {type(v).__name__!r}")
        upper = v.upper()
        if upper not in _LOG_LEVELS:
            raise ValueError(f"log_level {v!r} is not a valid logging level; expected one of {sorted(_LOG_LEVELS)}")
        return upper

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Place env vars above TOML file above field defaults.

        pydantic-settings evaluates sources left-to-right; leftmost wins.
        Returning ``(init_settings, env_settings, toml_source)`` ensures:
        explicit kwargs > ``WAITBUS_*`` env vars > config.toml > field defaults.

        The TOML path is resolved here (at settings-source construction time)
        so that it honours ``WAITBUS_CONFIG_DIR`` even when that env var is
        set after module import.

        ``dotenv_settings`` and ``file_secret_settings`` are the remaining
        two source factories pydantic-settings passes to this callback by
        keyword; waitbus uses neither (no .env file loader; secrets resolve
        through ``waitbus/_secrets.py`` with the operator's
        per-deployment backend choice).  The stdlib-idiomatic
        unused ``_ = ...`` assignment marks the non-use
        without the imperative-``del`` ceremony, and the vulture
        allowlist in ``pyproject.toml`` keeps a future dead-arg lint
        from rename-pressuring the framework-required keyword names.
        """
        _ = dotenv_settings, file_secret_settings
        toml_path = _paths.config_dir() / "config.toml"
        toml_source = _TomlSettingsSource(settings_cls, toml_path)
        return (init_settings, env_settings, toml_source)

    @classmethod
    def from_environment_and_toml(cls) -> CiStatusConfig:
        """Build a config object, merging env > TOML > defaults.

        Raises ``RuntimeError`` with a remediation hint on:

        - malformed TOML (``tomllib.TOMLDecodeError``)
        - unreadable config file (``PermissionError``)
        - any pydantic ``ValidationError`` (wrapped into a clear message)

        These errors originate inside the ``_TomlSettingsSource`` or
        pydantic validation and are caught here to wrap them in a
        uniform ``RuntimeError`` with a user-actionable message.
        """
        try:
            return cls()
        except RuntimeError:
            raise  # already formatted by _read_toml_file
        except Exception as exc:
            toml_path = _paths.config_dir() / "config.toml"
            raise RuntimeError(f"waitbus: invalid configuration (toml={toml_path}, env=WAITBUS_*): {exc}") from exc


@lru_cache(maxsize=1)
def get_config() -> CiStatusConfig:
    """Cached entry point — first call loads, subsequent calls reuse.

    The cache is process-scoped: daemon settings are fixed
    at startup.  Test code must call :func:`_reset_for_test` between test
    cases that mutate env vars or write different config files.
    """
    return CiStatusConfig.from_environment_and_toml()


def _reset_for_test() -> None:
    """Clear the lru_cache on ``get_config`` so env/file changes take effect.

    NOT public API.  Production code must never call this — the cache
    invariant assumes environment variables are fixed at process startup.

    ``_paths`` factories are no longer cached, so they re-resolve every
    call; no companion invalidation needed.
    """
    get_config.cache_clear()
