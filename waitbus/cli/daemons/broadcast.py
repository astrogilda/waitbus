"""`broadcast` daemon sub-app."""

from __future__ import annotations

import typer

from .._shared import _sub_version_callback

broadcast_app = typer.Typer(
    name="broadcast",
    help="Broadcast hub daemon.",
    no_args_is_help=True,
    add_completion=False,
)


@broadcast_app.callback()
def _broadcast_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """Broadcast hub daemon sub-commands."""


@broadcast_app.command(
    name="tap",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def broadcast_tap(ctx: typer.Context) -> None:
    """Subscribe to the broadcast daemon and stream every received frame.

    Connects to the local broadcast daemon, subscribes with the supplied
    filters (default ``*``), and prints each frame to stdout. Runs until
    interrupted with Ctrl-C, the daemon closes the connection, or
    ``--count N`` frames have been received.

    Example: waitbus broadcast tap --filters owner/repo --count 5
    """
    import waitbus.broadcast_tap as mod

    raise typer.Exit(mod.main(list(ctx.args)))


@broadcast_app.command(name="serve")
def broadcast_serve(
    metrics_port: int | None = typer.Option(
        None,
        "--metrics-port",
        min=0,
        max=65535,
        help=("Serve Prometheus metrics on 127.0.0.1:<port> (off when omitted; sets WAITBUS_METRICS_PORT)."),
    ),
) -> None:
    """Run the broadcast hub daemon (AF_UNIX SOCK_STREAM).

    Configuration is environment-driven via CiStatusConfig.
    ``--metrics-port`` is documented sugar over the canonical
    ``WAITBUS_METRICS_PORT`` env var:
    it sets the env var before the daemon's cached config first loads.
    """
    if metrics_port is not None:
        import os

        os.environ["WAITBUS_METRICS_PORT"] = str(metrics_port)

    import waitbus.broadcast as mod

    raise typer.Exit(mod.main())
