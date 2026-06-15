"""`mcp` daemon sub-app."""

from __future__ import annotations

import typer

from .._shared import _sub_version_callback

mcp_app = typer.Typer(
    name="mcp",
    help="MCP stdio server.",
    no_args_is_help=True,
    add_completion=False,
)


@mcp_app.callback()
def _mcp_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """MCP server sub-commands."""


@mcp_app.command(
    name="serve",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def mcp_serve(ctx: typer.Context) -> None:
    """Run the MCP stdio server."""
    import waitbus.mcp as mod

    mod.main(argv=list(ctx.args))
    raise typer.Exit(0)


@mcp_app.command(name="info")
def mcp_info() -> None:
    """Print server identity and supported MCP protocol version range as JSON."""
    import json

    import waitbus.mcp as mod

    typer.echo(json.dumps(mod.info(), indent=2))
    raise typer.Exit(0)
