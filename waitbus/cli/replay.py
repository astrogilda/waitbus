"""`replay` top-level command — replay broadcast events from a ULID cursor."""

from __future__ import annotations

import typer


def replay_cmd(ctx: typer.Context) -> None:
    """Replay broadcast events from a ULID cursor.

    Connects to the broadcast daemon with ``since=<SINCE_ULID>``, streams
    replayed frames to stdout, and exits when no frame arrives within the
    idle timeout (the daemon has finished the replay batch).

    Useful for operator-triggered backfill after a subscriber missed events:

        waitbus replay 01JZABC123DEF456GHJ789KLMN
    """
    import waitbus.replay as mod

    raise typer.Exit(mod.main(list(ctx.args)))
