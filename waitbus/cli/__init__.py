"""waitbus CLI package.

Re-exports the root Typer ``app`` and console-script ``main`` from
``cli.main`` so the pyproject ``[project.scripts]`` entry point
(``waitbus = "waitbus.cli:main"``) keeps resolving after
the cli.py -> cli/ package split.

Also re-exports a few module-level names that the test suite reaches
into directly (sub-apps for command-name introspection, ``subprocess``
/ ``shutil`` for ``unittest.mock.patch`` targets, the legacy-migration
constants for fixture setup). These re-exports preserve the
``waitbus.cli.<symbol>`` attribute surface the test suite
depends on; over time the patch targets should move to the leaf
modules (cli.install.systemd, cli.install.credentials, ...).
"""

# stdlib modules the test suite patches via ``waitbus.cli.<name>``
import shutil
import subprocess
import sys

# leaf-module helpers + constants the tests reach into
from .. import _paths
from ._shared import (
    _MIGRATION_DAEMON_UNITS,
    _check_config_validation,
    _check_credentials,
    _check_metrics_endpoint,
    _migrate_legacy_state_if_needed,
    _resolve_launchd_placeholders,
)
from .allowlist import allowlist_app
from .daemons import (
    broadcast_app,
    etag_poll_app,
    listener_app,
    mcp_app,
    pr_monitor_app,
    read_events_app,
    watchdog_check_app,
)
from .main import app, main
from .query import events_app

__all__ = [
    "_MIGRATION_DAEMON_UNITS",
    "_check_config_validation",
    "_check_credentials",
    "_check_metrics_endpoint",
    "_migrate_legacy_state_if_needed",
    "_paths",
    "_resolve_launchd_placeholders",
    "allowlist_app",
    "app",
    "broadcast_app",
    "etag_poll_app",
    "events_app",
    "listener_app",
    "main",
    "mcp_app",
    "pr_monitor_app",
    "read_events_app",
    "shutil",
    "subprocess",
    "sys",
    "watchdog_check_app",
]
