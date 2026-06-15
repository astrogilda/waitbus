"""`top` top-level command — live, collapsed, full-screen view of the event bus."""

from __future__ import annotations

import typer


def top_cmd(ctx: typer.Context) -> None:
    """Render a live, collapsed view of the event bus, one updating row per entity.

    Like ``read-events watch`` but folded: a GitHub run/job, an Alertmanager
    alert, or each source's latest event occupies a single row that mutates as
    events arrive. On a TTY it draws full-screen with in-place redraw; piped, it
    degrades to an append-only line stream.

        waitbus top
        waitbus top --timeout 30s          # end after 30s idle
        waitbus top --max-frames 100       # end after 100 frames (bounded)

    Exit codes: 0 clean end / SIGINT / max-frames, 1 wire-framing error,
    2 daemon unreachable.
    """
    import waitbus.top as mod

    raise typer.Exit(mod.main(list(ctx.args)))
