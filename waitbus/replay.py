"""replay -- admin command for replaying broadcast events since a ULID cursor.

Connects to the broadcast daemon with ``since=<ULID>``, receives the replayed
frames, and exits when no frame arrives within the ``--timeout`` window (the
daemon has finished the replay batch) or on SIGINT.

Exit codes:
  0    Replay caught up (timeout), or daemon closed the connection cleanly.
  2    Startup failure (daemon not running, token required but not configured,
       malformed ULID) OR daemon broke wire framing mid-stream.
  130  SIGINT (Ctrl-C); 128 + SIGINT(2) per POSIX shell convention.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Callable
from typing import Annotated

import typer

from ._broadcast_sub import (
    BookmarkCursor,
    BroadcastConnectionError,
    SubscriberHandle,
    WaitOutcome,
    _emit_predicate,
    await_predicate,
    emit_frame,
    open_subscriber,
)
from ._secrets import SecretNotConfigured
from .cli._shared import _exit_with_error, run_typer_app
from .coalesce import coalesce_replay

DEFAULT_TIMEOUT_SECONDS = 30.0

_ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")

_app = typer.Typer(
    name="replay",
    help="Replay broadcast events from a ULID cursor.",
    no_args_is_help=False,
    add_completion=False,
)


def _resolve_cursor(
    since_ulid: str | None,
    bookmark: str | None,
) -> tuple[str, BookmarkCursor | None]:
    """Resolve the effective ``since`` cursor and optional bookmark handle.

    Precedence: an explicit positional ULID wins; otherwise the named
    bookmark's stored cursor is loaded. Raises ``typer.Exit`` (with the
    formatted operator error already printed) when no cursor can be
    resolved, when the bookmark name is malformed, or when the resolved
    cursor is not a well-formed 26-char ULID.

    Returns ``(effective_since, cursor)`` where ``cursor`` is the
    ``BookmarkCursor`` handle to advance after each delivered frame, or
    ``None`` when the cursor came from the positional argument.
    """
    try:
        effective_since, cursor = BookmarkCursor.resolve_since(since_ulid, bookmark)
    except ValueError as exc:
        _exit_with_error(
            str(exc),
            hint="Bookmark names must match ^[A-Za-z0-9_.-]+$",
        )

    if effective_since is None:
        _exit_with_error(
            "a cursor is required: supply a SINCE_ULID argument or --bookmark NAME",
            hint=(
                "Example: waitbus replay 01JZABC123DEF456GHJ789KLMN\n         waitbus replay --bookmark my-subscriber"
            ),
        )

    if not _ULID_RE.match(effective_since):
        _exit_with_error(
            f"since_ulid {effective_since!r} is not a valid 26-char ULID",
            hint=(
                "ULIDs use Crockford base-32 (digits + uppercase, excluding I, L, O, U) and are exactly 26 characters."
            ),
        )

    return effective_since, cursor


def _drain_and_print(
    sub: SubscriberHandle,
    *,
    timeout: float,
    engine: Callable[[], WaitOutcome],
) -> None:
    """Drive ``engine`` to completion, map its ``WaitOutcome`` to an exit code.

    Single locus for the operator-facing outcome→exit matrix shared by
    the faithful (``_stream_until_idle``) and coalesced
    (``_stream_coalesced``) wrappers. ``engine`` is a zero-arg closure
    pre-bound to its specific engine + emit signature (``await_predicate``
    uses ``deadline_seconds``; ``coalesce_replay`` uses ``idle_seconds``
    — the closure absorbs the difference). Socket lifecycle: ``sub.sock``
    is closed in ``finally`` exactly once on the way out.

    Outcome → exit code:

    * ``timed_out`` → 0 with ``"replay caught up (no frames in Ns)"`` on
      stderr. The expected terminus when the daemon's replay batch has
      stopped arriving.
    * ``peer_closed`` → 0 with ``"broadcast connection closed"`` on
      stderr. A clean daemon FIN (graceful shutdown / restart).
    * ``cancelled`` (SIGINT) → **130** with ``"replay interrupted"`` on
      stderr. Mirrors ``waitbus wait``'s SIGINT contract (POSIX convention
      that ``$?`` for a signal-killed child is 128 + signum). Silent
      ``Exit(0)`` on Ctrl-C was a pre-existing bug.
    * ``framing_error`` → **2** with ``"broadcast framing error"`` on
      stderr. The daemon violated wire framing mid-stream; collapsing
      this into a clean ``peer_closed`` would hide a wire-protocol
      regression behind ``Exit(0)``. Distinct exit code so the operator
      knows the daemon, not the network, broke the contract.
    """
    try:
        outcome = engine()
    finally:
        sub.sock.close()

    if outcome.cancelled:
        print("replay interrupted", file=sys.stderr, flush=True)
        raise typer.Exit(130)
    if outcome.framing_error:
        print("broadcast framing error", file=sys.stderr, flush=True)
        raise typer.Exit(2)
    if outcome.timed_out:
        print(
            f"replay caught up (no frames in {timeout:.0f}s)",
            file=sys.stderr,
            flush=True,
        )
    elif outcome.peer_closed:
        print("broadcast connection closed", file=sys.stderr)
    raise typer.Exit(0)


def _stream_until_idle(
    sub: SubscriberHandle,
    *,
    timeout: float,
    as_json: bool,
    cursor: BookmarkCursor | None,
) -> None:
    """Stream the replay batch to stdout until it goes idle.

    Two-line dispatcher over :func:`_drain_and_print` using
    :func:`await_predicate` in ``idle_reset=True`` mode: each
    non-heartbeat frame is emitted and resets the idle deadline.
    """
    _drain_and_print(
        sub,
        timeout=timeout,
        engine=lambda: await_predicate(
            sub,
            decide=_emit_predicate(lambda frame: emit_frame(frame, as_json=as_json)),
            deadline_seconds=timeout,
            cursor=cursor,
            idle_reset=True,
        ),
    )


def _stream_coalesced(
    sub: SubscriberHandle,
    *,
    timeout: float,
    as_json: bool,
    cursor: BookmarkCursor | None,
) -> None:
    """Drain the replay backlog as a latest-per-entity snapshot, then exit.

    Two-line dispatcher over :func:`_drain_and_print` using
    :func:`coalesce.coalesce_replay` in ``live_tail=False`` mode
    (operator-replay semantics: backfill then exit).
    """
    _drain_and_print(
        sub,
        timeout=timeout,
        engine=lambda: coalesce_replay(
            sub,
            emit=lambda frame: emit_frame(frame, as_json=as_json),
            idle_seconds=timeout,
            cursor=cursor,
            live_tail=False,
        ),
    )


@_app.command()
def _replay(
    since_ulid: Annotated[
        str | None,
        typer.Argument(
            help=(
                "26-char ULID: replay events with event_id > this value. "
                "Omit when --bookmark is supplied (the stored cursor is used)."
            ),
        ),
    ] = None,
    filters: Annotated[
        list[str] | None,
        typer.Option("--filters", help="Repo filters: owner/repo, owner/*, or *."),
    ] = None,
    event_types: Annotated[
        list[str] | None,
        typer.Option("--event-types", help="Event types to replay."),
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option("--json/--text", help="Emit frames as JSON (default) or human-readable text."),
    ] = True,
    timeout: Annotated[
        float,
        typer.Option("--timeout", help="Exit cleanly after this many idle seconds (replay caught up)."),
    ] = DEFAULT_TIMEOUT_SECONDS,
    bookmark: Annotated[
        str | None,
        typer.Option(
            "--bookmark",
            help=(
                "Persistent bookmark name (^[A-Za-z0-9_.-]+$). "
                "Load the stored cursor and send it as since=. "
                "Each received non-heartbeat frame updates the stored cursor. "
                "Explicit SINCE_ULID argument takes precedence when both are supplied."
            ),
        ),
    ] = None,
    coalesce: Annotated[
        bool,
        typer.Option(
            "--coalesce/--faithful",
            help=(
                "Coalesced delivery: collapse the replay backlog to the latest "
                "event per entity (run / job / alert) and emit one row per "
                "entity in event_id order, then switch to a faithful live "
                "tail. Lossy by design (intermediate states are dropped). "
                "Faithful (default) emits every frame. See CONSUMER_API.md §6."
            ),
        ),
    ] = False,
) -> None:
    """Replay broadcast events from a ULID cursor.

    Connects to the broadcast daemon with ``since=<SINCE_ULID>``, streams the
    replayed frames to stdout, and exits cleanly when no frame arrives within
    ``--timeout`` seconds (the daemon has finished the replay batch).

    The cursor can be supplied either as a positional ULID argument or loaded
    from a named bookmark (``--bookmark NAME``). When ``--bookmark`` is given
    without a positional ULID, the stored cursor is used; the cursor file is
    updated after each received non-heartbeat frame.

    Useful for operator-triggered backfill after a subscriber missed events:

        waitbus replay 01JZABC123DEF456GHJ789KLMN

    Or using a saved bookmark:

        waitbus replay --bookmark my-subscriber
    """
    # Resolve the effective cursor: positional ULID > bookmark file > error.
    effective_since, cursor = _resolve_cursor(since_ulid, bookmark)

    resolved_filters: list[str] = filters if filters else ["*"]

    try:
        sub = open_subscriber(
            filters=resolved_filters,
            event_types=event_types if event_types else None,
            since=effective_since,
        )
    except BroadcastConnectionError as exc:
        _exit_with_error(str(exc), hint=exc.remediation)
    except SecretNotConfigured as exc:
        _exit_with_error(str(exc))

    if coalesce:
        _stream_coalesced(sub, timeout=timeout, as_json=as_json, cursor=cursor)
    else:
        _stream_until_idle(sub, timeout=timeout, as_json=as_json, cursor=cursor)


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``waitbus replay``."""
    return run_typer_app(_app, argv)
