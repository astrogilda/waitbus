"""`on` top-level command — block until a predicate matches, then run a command."""

from __future__ import annotations

import typer


def on_cmd(ctx: typer.Context) -> None:
    """Block until an event matches a predicate, then run a command.

    The action counterpart to ``waitbus wait``: subscribes to the broadcast
    daemon, blocks on the same source-agnostic predicate surface, and runs an
    operator-supplied command (after ``--``) when a match arrives, with the
    matched event exposed via ``$WAITBUS_EVENT_FILE`` and ``WAITBUS_*`` variables.

        waitbus on --source pytest --match 'fields.event_type="pytest_session"' -- ./deploy.sh

    Default (once) mode runs the command once and exits with its code; ``--loop``
    keeps reacting, and ``--loop --restart`` terminates the still-running command
    before each new match. Exit codes: the command's own code (once), 124 idle
    timeout, 130 SIGINT, 2 startup failure.
    """
    import waitbus.on as mod

    raise typer.Exit(mod.main(list(ctx.args)))
