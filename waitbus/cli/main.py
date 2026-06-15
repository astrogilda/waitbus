"""waitbus root Typer app and console-script entry point.

Wires the top-level commands (init, doctor, status, verify-plugin,
replay, install-{systemd,launchd,credentials}, migrate) and the
daemon / query sub-apps onto a single ``app`` Typer. The console-script
entry point declared in pyproject.toml resolves to ``main`` re-exported
from ``waitbus.cli.__init__``.

Splitting cli.py into this package is mechanical decomposition; the
nested ``install <target>`` / ``db migrate`` surface and the waitbus
rename land in later cycles.
"""

from __future__ import annotations

import sys

import typer

from ..config_validate import config_app
from ._shared import _version_callback
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
from .db import migrate, prune
from .demo import demo
from .doctor import doctor
from .emit import emit
from .init import init
from .install import install_credentials, install_launchd, install_systemd
from .on import on_cmd
from .query import events_app
from .replay import replay_cmd
from .serve import serve_cmd
from .sources import sources_app
from .stats import stats
from .status import status
from .stress import stress_cmd
from .swarm_demo import swarm_demo
from .top import top_cmd
from .verify_plugin import verify_plugin
from .wait import wait_cmd

# ---------------------------------------------------------------------------
# root app
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="waitbus",
    help="waitbus admin CLI — bootstrap, systemd unit install, credential install.",
    no_args_is_help=True,
    add_completion=True,
)


@app.callback()
def _root(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """waitbus admin CLI."""


# ---------------------------------------------------------------------------
# top-level command registration
# ---------------------------------------------------------------------------

# Order MUST match cli.py pre-split:
#   replay, wait, init, demo, install-systemd, install-launchd,
#   install-credentials, doctor, status, stats, emit, verify-plugin,
#   migrate, db-prune
# Typer prints commands in registration order in --help.

app.command(
    name="replay",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)(replay_cmd)

app.command(
    name="wait",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)(wait_cmd)

app.command(
    name="on",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)(on_cmd)

app.command()(init)
app.command()(demo)
app.command(name="swarm-demo")(swarm_demo)
app.command(name="install-systemd")(install_systemd)
app.command(name="install-launchd")(install_launchd)
app.command(name="install-credentials")(install_credentials)
app.command()(doctor)
app.command()(status)
app.command()(stats)
app.command(
    name="top",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)(top_cmd)
app.command()(emit)
app.command(name="verify-plugin")(verify_plugin)
app.command(name="migrate")(migrate)
app.command(name="db-prune")(prune)
app.command(
    name="stress",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)(stress_cmd)
app.command(name="serve")(serve_cmd)

# ---------------------------------------------------------------------------
# sub-app wiring
# ---------------------------------------------------------------------------

app.add_typer(listener_app)
app.add_typer(broadcast_app)
app.add_typer(etag_poll_app)
app.add_typer(mcp_app)
app.add_typer(read_events_app)
app.add_typer(events_app)
app.add_typer(pr_monitor_app)
app.add_typer(watchdog_check_app)
app.add_typer(sources_app)
app.add_typer(allowlist_app)
app.add_typer(config_app)


def main() -> int:
    """Entry-point for the `waitbus` console-script (declared in
    pyproject.toml `[project.scripts]`)."""
    app()
    return 0


if __name__ == "__main__":
    sys.exit(main())
