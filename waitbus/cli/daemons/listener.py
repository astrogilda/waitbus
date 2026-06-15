"""`listener` daemon sub-app."""

from __future__ import annotations

import typer

from .._shared import _sub_version_callback

listener_app = typer.Typer(
    name="listener",
    help="Webhook listener daemon.",
    no_args_is_help=True,
    add_completion=False,
)


@listener_app.callback()
def _listener_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """Webhook listener daemon sub-commands."""


@listener_app.command(name="serve")
def listener_serve() -> None:
    """Run the webhook listener daemon (loopback :9000/webhook).

    Takes no CLI arguments; configuration is environment-driven via
    CiStatusConfig. Typer rejects any unexpected args at parse time.
    """
    import waitbus.listener as mod

    raise typer.Exit(mod.main())
