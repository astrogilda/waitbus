"""broadcast tap -- debug subscriber for the waitbus broadcast daemon.

Connects to the broadcast daemon, subscribes with the supplied filters
(defaulting to ``*``), and streams every received frame to stdout. Intended
for operator smoke-testing: "is my broadcast bus sending events at all?"

Exit codes:
  0  Clean shutdown (SIGINT, ``--count`` reached, connection closed by daemon).
  2  Startup failure (daemon not running, token required but not configured).
"""

from __future__ import annotations

import sys
from typing import Annotated

import typer

from ._broadcast_sub import (
    BookmarkCursor,
    BroadcastConnectionError,
    _emit_predicate,
    await_predicate,
    emit_frame,
    open_subscriber,
)
from ._secrets import SecretNotConfigured
from .cli._shared import _exit_with_error, run_typer_app

_app = typer.Typer(
    name="tap",
    help="Subscribe to the broadcast daemon and stream every received frame.",
    no_args_is_help=False,
    add_completion=False,
)


@_app.command()
def _tap(
    filters: Annotated[
        list[str] | None,
        typer.Option("--filters", help="Repo filters: owner/repo, owner/*, or *."),
    ] = None,
    event_types: Annotated[
        list[str] | None,
        typer.Option("--event-types", help="Event types to subscribe to."),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option("--since", help="26-char ULID: replay events after this cursor."),
    ] = None,
    bookmark: Annotated[
        str | None,
        typer.Option(
            "--bookmark",
            help=(
                "Persistent bookmark name (^[A-Za-z0-9_.-]+$). "
                "On subscribe, load the stored cursor and send it as since=. "
                "Each received non-heartbeat frame updates the stored cursor. "
                "Ignored when --since is also supplied."
            ),
        ),
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option("--json/--text", help="Emit frames as JSON (default) or human-readable text."),
    ] = True,
    count: Annotated[
        int | None,
        typer.Option("--count", help="Exit after receiving N frames."),
    ] = None,
) -> None:
    """Subscribe to the broadcast daemon and stream every received frame.

    Connects to the local broadcast daemon, sends a subscribe frame with the
    supplied filters, and prints each incoming frame to stdout. Runs until
    interrupted with Ctrl-C, until the daemon closes the connection, or until
    ``--count N`` frames have been received.

    Pass ``--bookmark NAME`` to resume automatically from the last-consumed
    event on reconnect. The cursor is updated after every non-heartbeat frame.

    Useful for verifying that the broadcast bus is delivering events:

        waitbus broadcast tap --filters owner/repo --count 5
    """
    resolved_filters: list[str] = filters if filters else ["*"]
    cursor: BookmarkCursor | None = BookmarkCursor(bookmark) if bookmark else None

    try:
        sub = open_subscriber(
            filters=resolved_filters,
            event_types=event_types if event_types else None,
            since=since,
            bookmark_id=bookmark,
        )
    except BroadcastConnectionError as exc:
        _exit_with_error(str(exc), hint=exc.remediation)
    except SecretNotConfigured as exc:
        _exit_with_error(str(exc))

    # tap is an unbounded smoke-test stream: no deadline (None), exit
    # only on --count, EOF, or SIGINT. A thin adapter over the shared
    # await_predicate engine -- the predicate emits each frame and
    # signals MATCHED once --count frames have been seen.
    try:
        outcome = await_predicate(
            sub,
            decide=_emit_predicate(lambda frame: emit_frame(frame, as_json=as_json), count=count),
            deadline_seconds=None,
            cursor=cursor,
        )
    finally:
        sub.sock.close()

    if outcome.matched:
        # MATCHED fires the first time the emitted count reaches --count,
        # so exactly `count` frames were streamed.
        print(
            f"received {count} frame(s); exiting",
            file=sys.stderr,
            flush=True,
        )
    elif outcome.peer_closed:
        print("broadcast connection closed", file=sys.stderr)
    raise typer.Exit(0)


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``waitbus broadcast tap``."""
    return run_typer_app(_app, argv)
