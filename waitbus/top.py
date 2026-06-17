"""``waitbus top`` -- a live, collapsed, full-screen view of the event bus.

Where ``waitbus read-events watch`` streams one append-only line per frame, ``top``
renders a ``top(1)``-style dashboard: one line per *entity*, updated in place, so
a long-running CI matrix, a pytest loop, a container's lifecycle, and any agent
coordination chatter each occupy a single row that mutates as new events arrive.
It is another subscriber on the same bus -- no new daemon, no new dependency, no
new ingress.

Collapse key. Frames with a stable upstream identity collapse via
:func:`waitbus._terminal.entity_key` (a GitHub run/job, an Alertmanager
alert); everything else collapses by ``(source, event_type, repo)`` so each
source folds into one updating row showing its latest event. The newest frame
for a key replaces the row.

Rendering. On a TTY, ``top`` uses the alternate screen buffer and stdlib ANSI
in-place redraw (no curses, no TUI framework -- consistent with the lean-deps
posture). Off a TTY (a pipe, a captured test stdout), it degrades to the same
append-only line stream ``read-events watch`` produces, so it is safe to pipe and
deterministic to test. ``NO_COLOR`` is honoured; terminal width is re-queried each
redraw, so a resize is reflected on the next frame without a signal handler.

It runs until ``--timeout`` elapses with no new frame, ``--max-frames`` frames
have been rendered (bounded mode, for scripts and tests), the daemon closes, or
SIGINT. Exit codes mirror the watch contract: 0 on clean end / SIGINT / reaching
``--max-frames``, 1 on a wire-framing error, 2 if the daemon is unreachable.
"""

from __future__ import annotations

import shutil
import sys
from typing import Annotated, Any, Final

import typer

from ._broadcast_sub import (
    BroadcastConnectionError,
    WaitOutcome,
    _emit_predicate,
    await_predicate,
    open_subscriber,
    read_subscribe_ack,
)
from ._duration import parse_duration
from ._terminal import entity_key
from .cli._shared import _exit_with_error, run_typer_app, use_colour

EXIT_SUCCESS = 0
EXIT_FRAMING = 1
EXIT_STARTUP = 2

_DEFAULT_TERMINAL_ROWS: Final[int] = 24
"""Fallback terminal row count used when ``shutil.get_terminal_size`` cannot
determine the actual height (e.g. the process has no controlling TTY).

24 rows is the POSIX-standard 80x24 VT100 default; real terminals are
almost always taller, so this is a safe conservative floor.
"""

_DEFAULT_TERMINAL_COLS: Final[int] = 100
"""Fallback terminal column count used when ``shutil.get_terminal_size`` cannot
determine the actual width.

100 columns is slightly wider than the POSIX 80-column default; it matches
the ``shutil.get_terminal_size((100, 24))`` fallback already present in
``_render``, making the constant the single source of that value.
"""

# Alternate-screen-buffer enter/leave: render the dashboard on a scratch screen
# and restore the operator's scrollback on exit (TTY only).
_ALT_SCREEN_ENTER = "\x1b[?1049h"
_ALT_SCREEN_LEAVE = "\x1b[?1049l"
_CLEAR_HOME = "\x1b[H\x1b[J"  # cursor home + clear to end of screen

_app = typer.Typer(
    name="top",
    help="Live, collapsed, full-screen view of the event bus.",
    no_args_is_help=False,
    add_completion=False,
)


def _display_key(frame: dict[str, Any]) -> tuple[str, ...]:
    """Return the row-collapse key for a frame.

    Prefers the stable upstream :func:`entity_key` (GitHub run/job, Alertmanager
    alert); for pass-through sources (pytest / docker / fs / agent / watchdog)
    folds by ``(source, event_type, repo)`` so each collapses to one updating row.
    """
    key = entity_key(frame)
    if key is not None:
        return key
    fields = frame.get("fields")
    source = fields.get("source") if isinstance(fields, dict) else None
    return (
        "by-type",
        str(source or "?"),
        str(frame.get("event_type") or "?"),
        str(frame.get("repo") or "*"),
    )


def _status_token(frame: dict[str, Any]) -> str:
    """A short status token for the row: the conclusion, else the status, else '-'."""
    fields = frame.get("fields")
    if not isinstance(fields, dict):
        return "-"
    conclusion = fields.get("conclusion")
    if conclusion:
        return str(conclusion)
    status = fields.get("status")
    if status:
        return str(status)
    return "-"


def _format_row(frame: dict[str, Any]) -> str:
    """One-line, fixed-shape summary of a frame's current state for the dashboard."""
    fields = frame.get("fields")
    source = fields.get("source") if isinstance(fields, dict) else None
    event_type = frame.get("event_type") or "?"
    repo = frame.get("repo") or "*"
    status = _status_token(frame)
    summary = frame.get("summary")
    label = f"{source or '?'}/{event_type}"
    tail = f"  {summary}" if isinstance(summary, str) and summary else ""
    return f"{label:<28} {repo:<24} {status:<12}{tail}"


class _TopModel:
    """Insertion-ordered collapse of the latest frame per display key."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, ...], dict[str, Any]] = {}

    def __len__(self) -> int:
        """Number of distinct entities (collapsed rows) currently tracked."""
        return len(self._rows)

    def update(self, frame: dict[str, Any]) -> None:
        """Record ``frame`` as the latest state for its display key."""
        self._rows[_display_key(frame)] = frame

    def lines(self, *, width: int | None = None) -> list[str]:
        """Rendered rows in insertion order, each truncated to ``width`` if given."""
        out: list[str] = []
        for frame in self._rows.values():
            line = _format_row(frame)
            if width is not None and len(line) > width:
                line = line[: max(0, width - 1)] + "…"
            out.append(line)
        return out


def _render(model: _TopModel, *, count: int) -> None:
    """Redraw the dashboard in place on a TTY (cursor-home + clear, then rows)."""
    width = shutil.get_terminal_size((_DEFAULT_TERMINAL_COLS, _DEFAULT_TERMINAL_ROWS)).columns
    entities = len(model)
    header = f"waitbus top  -  {entities} entit{'y' if entities == 1 else 'ies'}  -  {count} events"
    body = "\n".join(model.lines(width=width))
    if use_colour():
        header = typer.style(header, fg=typer.colors.CYAN, bold=True)
    sys.stdout.write(_CLEAR_HOME + header + "\n" + body + "\n")
    sys.stdout.flush()


def _run(deadline_seconds: float | None, max_frames: int | None) -> int:
    """Subscribe to every source and render the collapsed dashboard until done.

    Returns the watch-style exit code. On a TTY the alternate screen buffer is
    entered and restored; off a TTY each frame's row is appended once (pipe-safe,
    deterministic), so a captured stdout carries one line per delivered frame.
    """
    try:
        sub = open_subscriber()
    except BroadcastConnectionError as exc:
        _exit_with_error(str(exc), hint=exc.remediation, code=EXIT_STARTUP)

    try:
        read_subscribe_ack(sub)
    except BroadcastConnectionError as exc:
        sub.sock.close()
        _exit_with_error(str(exc), hint=exc.remediation, code=EXIT_STARTUP)

    interactive = sys.stdout.isatty()
    model = _TopModel()
    seen = 0

    def _on_frame(frame: dict[str, Any]) -> None:
        nonlocal seen
        seen += 1
        if interactive:
            model.update(frame)
            _render(model, count=seen)
        else:
            # Non-TTY: append-only line stream (pipe-safe, test-deterministic).
            print(_format_row(frame), flush=True)

    if interactive:
        sys.stdout.write(_ALT_SCREEN_ENTER)
        sys.stdout.flush()
    try:
        outcome: WaitOutcome = await_predicate(
            sub,
            decide=_emit_predicate(_on_frame, count=max_frames),
            deadline_seconds=deadline_seconds,
            idle_reset=True,
        )
    finally:
        if interactive:
            sys.stdout.write(_ALT_SCREEN_LEAVE)
            sys.stdout.flush()
        sub.sock.close()

    if outcome.framing_error:
        print("waitbus top: framing error (daemon broke the wire protocol)", file=sys.stderr, flush=True)
        return EXIT_FRAMING
    return EXIT_SUCCESS


@_app.command()
def _top(
    timeout: Annotated[
        str,
        typer.Option(
            "--timeout",
            help="Idle timeout: end after this long with no new frame. Number = seconds; s/m/h/d; 'none' = forever.",
        ),
    ] = "none",
    max_frames: Annotated[
        int | None,
        typer.Option("--max-frames", help="End after rendering this many frames (bounded mode for scripts/tests)."),
    ] = None,
) -> None:
    """Render a live, collapsed, full-screen view of the event bus."""
    deadline_seconds: float | None
    if timeout.strip().lower() == "none":
        deadline_seconds = None
    else:
        try:
            deadline_seconds = parse_duration(timeout)
        except ValueError as exc:
            _exit_with_error(f"invalid --timeout: {exc}", code=EXIT_STARTUP)
    if max_frames is not None and max_frames <= 0:
        _exit_with_error("--max-frames must be a positive integer", code=EXIT_STARTUP)

    raise typer.Exit(_run(deadline_seconds, max_frames))


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``waitbus top``."""
    return run_typer_app(_app, argv)
