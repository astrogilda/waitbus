"""`wait` top-level command — block until a commit's CI is terminal."""

from __future__ import annotations

import typer


def wait_cmd(ctx: typer.Context) -> None:
    """Block until a commit's CI reaches a terminal conclusion.

    Subscribes to the broadcast daemon scoped to ``--repo`` (defaulting
    to the current git checkout's origin), streams frames, and exits as
    soon as a frame matching ``--sha`` carries a terminal GitHub
    ``conclusion``. The terminal conclusion drives the exit code:

        waitbus wait --sha 1a2b3c4 --repo owner/repo --timeout 5m

    Exit codes: 0 success, 1 failure/cancelled/timed_out, 124 timeout,
    130 SIGINT, 2 startup failure. ``--no-exit-status`` always exits 0
    on a terminal match.
    """
    import waitbus.wait as mod

    raise typer.Exit(mod.main(list(ctx.args)))
