"""`watchdog-check` daemon sub-app."""

from __future__ import annotations

import typer

from .._shared import _sub_version_callback

watchdog_check_app = typer.Typer(
    name="watchdog-check",
    help="Ingestion-silence detector.",
    no_args_is_help=True,
    add_completion=False,
)


@watchdog_check_app.callback()
def _watchdog_check_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """Watchdog check sub-commands."""


@watchdog_check_app.command(
    name="run",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def watchdog_check_run(ctx: typer.Context) -> None:
    """Single-shot ingestion-silence detector."""
    import waitbus.watchdog_check as mod

    raise typer.Exit(mod.main(list(ctx.args)))
