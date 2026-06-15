"""`etag-poll` daemon sub-app."""

from __future__ import annotations

import typer

from .._shared import _sub_version_callback

etag_poll_app = typer.Typer(
    name="etag-poll",
    help="ETag-aware GitHub API poll worker.",
    no_args_is_help=True,
    add_completion=False,
)


@etag_poll_app.callback()
def _etag_poll_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """ETag-poll sub-commands."""


@etag_poll_app.command(name="run")
def etag_poll_run() -> None:
    """Run one ETag-poll pass (designed for timer invocation).

    Takes no CLI arguments; configuration is environment-driven via
    WaitbusConfig.
    """
    import waitbus.etag_poll as mod

    raise typer.Exit(mod.main())
