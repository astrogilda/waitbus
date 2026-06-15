"""`pr-monitor` daemon sub-app."""

from __future__ import annotations

import typer

from .._shared import _sub_version_callback

pr_monitor_app = typer.Typer(
    name="pr-monitor",
    help="Push-driven PR CI state monitor.",
    no_args_is_help=True,
    add_completion=False,
)


@pr_monitor_app.callback()
def _pr_monitor_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """PR monitor sub-commands."""


@pr_monitor_app.command(
    name="tick",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def pr_monitor_tick(ctx: typer.Context) -> None:
    """Watch a PR's CI state via the broadcast bus."""
    import waitbus.pr_monitor as mod

    raise typer.Exit(mod.main(list(ctx.args)))
