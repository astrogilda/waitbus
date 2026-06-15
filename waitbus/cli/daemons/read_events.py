"""`read-events` sub-app."""

from __future__ import annotations

import typer

from .._shared import _sub_version_callback

read_events_app = typer.Typer(
    name="read-events",
    help="Query and tail the event store.",
    no_args_is_help=True,
    add_completion=False,
)


@read_events_app.callback()
def _read_events_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """Event-store query sub-commands."""


@read_events_app.command(
    name="watch",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def read_events_watch(ctx: typer.Context) -> None:
    """Subscribe to the broadcast bus and stream matching events live."""
    import waitbus.read_events as mod

    raise typer.Exit(mod.main(["--watch", *list(ctx.args)]))


@read_events_app.command(
    name="list",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def read_events_list(ctx: typer.Context) -> None:
    """Print recent events from the local cache."""
    import waitbus.read_events as mod

    raise typer.Exit(mod.main(list(ctx.args)))
