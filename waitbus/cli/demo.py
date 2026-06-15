"""``demo`` top-level command — sixty-second first-impression flow.

Spins up an in-process broadcast daemon against a temporary state
directory and runs two phases:

1. Block, don't poll. An agent blocks on the event bus via the
   same ``await_predicate`` engine ``waitbus wait`` uses -- no polling, no
   API calls, idle CPU. After a visible idle pause the github event is
   emitted and the wait returns the instant it lands. The demo then
   states the avoided polling cost: ``gh run watch``'s default poll
   interval is 3s (~20 API requests per minute of CI runtime, every
   run); waitbus holds one connection and polls zero times.

2. Breadth. The same primitive delivers every source: pytest,
   docker, and filesystem events burst to a live subscriber under one
   header, paced so each arrival is visible.

Every event is **synthesized** in-process — the demo runs no real HTTP
listener, GitHub webhook, pytest session, Docker daemon, or filesystem
watcher. Banners make this explicit, consistent with the convention
waitbus uses elsewhere (e.g., the SYNTHESIZED tag in ``waitbus stats``).

Run via ``uvx waitbus demo`` (one command, no install) or ``waitbus demo``
once installed. Total wall-clock budget is well under sixty seconds on
a workstation; the cleanup is exception-safe and removes the temporary
state directory so the operator's real waitbus install is untouched.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import sys
import tempfile
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import typer

from .._broadcast_sub import SubscriberHandle
from ._shared import use_colour

# The demo wires several module-internal seams (broadcast socket
# resolution, doorbell socket resolution) at process scope, then
# tears them down on exit. Importing the modules at function-body top
# keeps ``waitbus --help`` startup fast — typer does not need any of
# the broadcast / emit machinery just to print help.


# Value-proposition banners -- block on the event, never poll.
_BANNER_WAIT_INTRO = (
    "[demo] An agent needs to know the instant CI passes. The old way polls "
    "(`gh run watch` hits the API every ~3s). waitbus blocks on the event instead."
)
# The displayed command is the canonical github-CI wait from `waitbus wait
# --help` (--sha is sugar for `--source github` plus a git-style head_sha
# prefix match). It is copy-paste-real: every flag exists on the real wait
# command, and the 7-char `abc123d` prefix-matches the demo event's
# head_sha `abc123def456`.
_BANNER_WAIT_CMD = "[demo] agent runs: waitbus wait --sha abc123d --repo astrogilda/waitbus-demo --timeout 5m"
_BANNER_WAIT_BLOCKED = "[demo] blocked on the event bus -- 0 polls, 0 API calls, idle CPU. waiting for CI to finish..."
_BANNER_WAIT_RETURNED = (
    "[demo] waitbus wait returned the instant the event landed -- no polling, no wasted round-trips."
)
# The pain made concrete: gh run watch's default poll interval is 3s
# (verified: `gh run watch --help`, `-i, --interval int ... (default 3)`),
# i.e. ~20 API requests per minute of CI runtime, every run. The viewer
# scales it to their own build length. waitbus holds one connection and
# polls zero times.
_BANNER_POLL_CONTRAST = (
    "[demo] for comparison: gh run watch polls the GitHub API every 3s -- ~20 requests per minute of CI "
    "runtime, every run. waitbus: one connection, zero polls."
)

# Breadth banner -- the same primitive delivers every source waitbus speaks.
# The three breadth events burst under this one header (no per-source
# banner) so breadth reads as a quick "and all of these too", not a
# drawn-out list. The synthesized-in-process disclaimer rides here.
_BANNER_BREADTH = (
    "[demo] the same wait matches on any source -- here are pytest, docker, and fs "
    "(all synthesized in-process), live to one subscriber:"
)
_BANNER_CLOSING = (
    "[demo] That is the waitbus difference: block on the event, never poll.\n"
    "[demo] Try it in one command, no install:  uvx waitbus demo\n"
    "[demo] Or install the CLI:  uv tool install waitbus   (pip install waitbus also works)\n"
    "[demo] Documentation: https://github.com/astrogilda/waitbus"
)


def _echo(line: str) -> None:
    """Print one demo line, colour-coded by role when the terminal supports it.

    Three tiers keep the transcript scannable: ``[event]`` lines (the
    actual deliveries -- the payoff) render green+bold; the polling-cost
    comparison renders yellow (the avoided waste); all other ``[demo]``
    narration renders cyan. When colour is off the line prints verbatim.
    """
    if use_colour():
        if line.startswith("[event]"):
            line = typer.style(line, fg=typer.colors.GREEN, bold=True)
        elif line.startswith("[demo] for comparison:"):
            line = typer.style(line, fg=typer.colors.YELLOW)
        elif line.startswith("[demo]"):
            line = typer.style(line, fg=typer.colors.CYAN)
    print(line, flush=True)


# Hard wall-clock guard. The flow is heavily bounded by sleeps; this
# keeps a wedged subscriber from spinning past the user's sixty-second
# expectation.
_DEMO_TIMEOUT_SECONDS = 60.0
_DRAIN_SECONDS = 1.5
_BIND_TIMEOUT_SECONDS = 5.0

# Pace between the breadth-phase source emits so a viewer sees each
# [event] line land in turn rather than as a single instantaneous
# burst. Three sources at this pace add ~2.4 s -- well inside the 60 s
# guard. The burst-all-at-once shape defeats the "watch it fan out"
# point of the breadth phase.
_SOURCE_PACE_SECONDS = 0.8

# How long the phase-1 wait sits visibly blocked before the event is
# emitted. Long enough that a viewer registers "it is parked, not
# polling"; short enough to keep the flow well under the 60s guard.
_WAIT_PARK_SECONDS = 1.2

# Beat between consecutive narration lines so the transcript reveals one
# line at a time rather than bursting several at once. A steady reveal
# is both easier to read live and -- because the screen changes on a
# regular cadence -- yields a recording VHS captures faithfully (it
# compresses idle frames, not active ones), so the rendered gif/mp4 is
# a readable length without slow-motion playback.
_REVEAL_PACE_SECONDS = 0.7


def _synthetic_github_payload() -> str:
    """Minimal workflow_run payload identical in shape to what the
    listener stores on a real github delivery. The values are
    arbitrary; the demo's measurement budget is not latency."""
    payload: dict[str, object] = {
        "action": "completed",
        "delivery": str(uuid.uuid4()),
        "repository": {
            "name": "waitbus-demo",
            "owner": {"login": "astrogilda"},
        },
        "workflow_run": {
            "id": 1,
            "name": "ci",
            "head_branch": "main",
            "head_sha": "abc123def456",
            "status": "completed",
            "conclusion": "success",
        },
    }
    return json.dumps(payload)


def _synthetic_docker_payload() -> str:
    """Return a synthetic docker_container EventInsert payload for the demo."""
    payload: dict[str, object] = {
        "Action": "die",
        "Actor": {
            "ID": "abcd1234ef567890" * 2,
            "Attributes": {
                "name": "demo-worker",
                "image": "alpine:3.20",
                "exitCode": "0",
            },
        },
        "time": int(time.time()),
    }
    return json.dumps(payload)


def _synthetic_fs_payload(path: str) -> str:
    """Return a synthetic fs_change EventInsert payload for the demo."""
    payload: dict[str, object] = {
        "path": path,
        "event_type": "modified",
        "is_directory": False,
    }
    return json.dumps(payload)


def _synthetic_pytest_payload() -> str:
    """Return a synthetic pytest_session EventInsert payload for the demo."""
    payload: dict[str, object] = {
        "session_id": str(uuid.uuid4()),
        "passed": 42,
        "failed": 0,
        "skipped": 1,
        "duration_sec": 4.2,
        "exitstatus": 0,
    }
    return json.dumps(payload)


async def _wait_for_socket(path: Path, deadline: float) -> None:
    """Poll until the broadcast socket file appears at ``path`` or the deadline passes."""
    while time.monotonic() < deadline:
        if path.exists():
            return
        await asyncio.sleep(0.02)
    raise RuntimeError(  # pragma: no cover  -- defensive: socket-never-binds
        f"broadcast socket did not appear at {path}"
    )


async def _await_subscribe_ack(handle: SubscriberHandle) -> None:
    """Block until the daemon's ``subscribe_ack`` arrives (registration barrier).

    Thin async wrapper over the shared synchronous
    :func:`waitbus._broadcast_sub.read_subscribe_ack`, run off the event
    loop via :func:`asyncio.to_thread`. Confirming registration before the demo
    emits guarantees the emit cannot race subscriber registration. The shared
    helper raises a typed :class:`BroadcastConnectionError` on reject / EOF;
    the demo daemon has no token and always acks, so those paths are unreachable
    here.
    """
    from .._broadcast_sub import read_subscribe_ack

    await asyncio.to_thread(read_subscribe_ack, handle)


async def _read_frames_until_done(
    sock: socket.socket,
    expected: int,
    done: asyncio.Event,
) -> int:
    """Read up to ``expected`` event frames, printing each one formatted
    as a single line. Sets ``done`` when ``expected`` frames have arrived.
    Returns the count actually delivered before timeout.

    Uses the project's canonical ``_frame.sync_read_frame`` via
    ``asyncio.to_thread`` (the same pattern ``tests/test_e2e_scenarios.py``
    uses). Control frames (daemon_heartbeat / subscribe_ack) are skipped by
    ``_emit_frame_if_event``; the subscribe_ack was already consumed by
    ``_await_subscribe_ack`` before this reader starts.
    """
    from waitbus._frame import sync_read_frame

    seen = 0

    while seen < expected:
        try:
            frame_bytes = await asyncio.to_thread(sync_read_frame, sock)
        except ConnectionError:
            break
        if frame_bytes is None:
            break
        if _emit_frame_if_event(frame_bytes):
            seen += 1

    done.set()
    return seen


def _emit_frame_if_event(frame_bytes: bytes) -> bool:
    """Decode a wire-frame JSON body and print its summary unless it
    is a heartbeat. Returns True iff a non-heartbeat frame was emitted.
    """
    try:
        frame = json.loads(frame_bytes)
    except json.JSONDecodeError:
        return False
    # Only plain event frames are rendered. Control frames
    # (daemon_heartbeat / subscribe_ack / subscribe_rejected) and truncated
    # stubs all carry kind != "event" and are skipped.
    if frame.get("kind") != "event":
        return False
    _echo(_render_demo_line(frame))
    return True


def _render_workflow_run(fields: dict[str, object], owner: str, repo: str) -> str:
    """Render a one-line summary from ``conclusion``, ``head_branch``, and ``head_sha`` fields."""
    conclusion = fields.get("conclusion") or "pending"
    branch = fields.get("head_branch") or "?"
    sha = str(fields.get("head_sha") or "?")
    return f"[event] github workflow_run {conclusion} on {branch} (sha={sha[:7]}, repo={owner}/{repo})"


# status/conclusion are SQL columns on the EventInsert (not payload_json keys);
# broadcast._row_to_frame surfaces them via the fields projection.
def _render_pytest_session(fields: dict[str, object], owner: str, repo: str) -> str:
    """Render a one-line summary from ``status`` and ``conclusion`` fields."""
    status = fields.get("status") or "?"
    conclusion = fields.get("conclusion") or "?"
    return f"[event] pytest_session {status}/{conclusion} (repo={owner}/{repo})"


def _render_docker_container(fields: dict[str, object], owner: str, repo: str) -> str:
    """Render a one-line summary from ``conclusion`` and ``workflow_name`` fields."""
    conclusion = fields.get("conclusion") or "?"
    workflow_name = fields.get("workflow_name") or "?"
    return f"[event] docker_container {conclusion} container={workflow_name} (repo={owner}/{repo})"


def _render_fs_change(fields: dict[str, object], owner: str, repo: str) -> str:
    """Render a one-line summary from the ``workflow_name`` field."""
    workflow_name = fields.get("workflow_name") or "?"
    return f"[event] fs_change modified {workflow_name} (repo={owner}/{repo})"


_RENDERERS: dict[str, Callable[[dict[str, object], str, str], str]] = {
    "workflow_run": _render_workflow_run,
    "pytest_session": _render_pytest_session,
    "docker_container": _render_docker_container,
    "fs_change": _render_fs_change,
}


def _frame_context(frame: dict[str, object]) -> tuple[dict[str, object], str, str, str]:
    """Extract the common ``(fields, event_type, owner, repo)`` tuple
    used by every per-event renderer."""
    fields = frame.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    event_type = str(frame.get("event_type") or frame.get("kind") or "?")
    owner = str(frame.get("owner") or "?")
    repo = str(frame.get("repo") or "?")
    return fields, event_type, owner, repo


def _render_demo_line(frame: dict[str, object]) -> str:
    """One-line operator-facing summary keyed off the broadcast wire
    frame. Reads from ``frame["fields"]`` (which carries the SQL
    columns minus ``delivery_id`` / ``received_at`` / ``payload_json``
    / ``event_id``) rather than the row directly, matching
    ``broadcast._row_to_frame``'s output shape.

    Per-event-type renderers live in ``_RENDERERS``; unmapped event
    types fall back to the broadcast-supplied ``summary`` (which
    covers github-shaped events via ``read_events.format_text``'s rich
    branch), then to a generic ``source event_type`` line.
    """
    fields, event_type, owner, repo = _frame_context(frame)
    renderer = _RENDERERS.get(event_type)
    if renderer is not None:
        return renderer(fields, owner, repo)
    summary = frame.get("summary")
    if isinstance(summary, str) and summary:
        return f"[event] {summary}"
    source = str(fields.get("source") or "?")
    return f"[event] {source} {event_type} (repo={owner}/{repo})"


async def _demo_block_on_wait(socket_path: Path, db_path: Path, doorbell_path: Path) -> None:
    """Block on the event, never poll.

    Opens a subscriber and parks it on ``await_predicate`` -- the exact
    egress engine ``waitbus wait`` uses -- matching a github
    ``workflow_run`` success. After a visible idle pause (the wait is
    truly blocked: no polling, no CPU) the github event is emitted and
    the wait returns the instant it lands. The demo then states the
    avoided polling cost (gh run watch's documented 3s interval) to make
    the "0 polls" claim concrete.
    """
    from waitbus._emit import emit
    from waitbus._types import EventInsert

    from .._broadcast_sub import FrameDecision, await_predicate, open_subscriber

    _echo(_BANNER_WAIT_INTRO)
    await asyncio.sleep(_REVEAL_PACE_SECONDS)
    _echo(_BANNER_WAIT_CMD)
    await asyncio.sleep(_REVEAL_PACE_SECONDS)

    handle = await asyncio.to_thread(open_subscriber, socket_path=str(socket_path))
    # Confirm registration before the emit so the event cannot race
    # subscribe completion (the same ack-first guarantee the tap uses).
    await _await_subscribe_ack(handle)

    matched: list[str] = []

    def _decide(frame: dict[str, Any]) -> FrameDecision:
        if frame.get("kind") == "event" and frame.get("event_type") == "workflow_run":
            matched.append(_render_demo_line(frame))
            return FrameDecision.MATCHED
        return FrameDecision.CONTINUE

    waiter = asyncio.create_task(asyncio.to_thread(await_predicate, handle, decide=_decide, deadline_seconds=10.0))
    _echo(_BANNER_WAIT_BLOCKED)
    # Hold so the block is visibly parked before the event arrives.
    await asyncio.sleep(_WAIT_PARK_SECONDS)

    await asyncio.to_thread(
        emit,
        EventInsert(
            delivery_id=f"demo-github-{uuid.uuid4()}",
            source="github",
            event_type="workflow_run",
            owner="astrogilda",
            repo="waitbus-demo",
            received_at=time.time_ns(),
            payload_json=_synthetic_github_payload(),
            ingest_method="demo_synthesized",
            run_id=1,
            workflow_name="ci",
            head_branch="main",
            head_sha="abc123def456",
            status="completed",
            conclusion="success",
        ),
        db_path=db_path,
        doorbell_path=doorbell_path,
    )
    outcome = await waiter

    if matched and outcome.matched:
        _echo(matched[0])
        await asyncio.sleep(_REVEAL_PACE_SECONDS)
        _echo(_BANNER_WAIT_RETURNED)
        await asyncio.sleep(_REVEAL_PACE_SECONDS)
        _echo(_BANNER_POLL_CONTRAST)
        await asyncio.sleep(_REVEAL_PACE_SECONDS)
    else:  # pragma: no cover -- the demo always matches within the 10s window
        _echo("[demo] (the wait did not match within the demo window)")
    with contextlib.suppress(OSError):
        handle.sock.close()


async def _demo_fan_out_breadth(socket_path: Path, db_path: Path, state_dir: Path, doorbell_path: Path) -> None:
    """Breadth -- the same primitive delivers every source waitbus speaks.

    Opens a fresh tap and emits the remaining three synthesized sources
    (pytest, docker, fs), paced so each fan-out is visible as it lands.
    """
    from waitbus._emit import emit
    from waitbus._types import EventInsert

    from .._broadcast_sub import open_subscriber

    _echo(_BANNER_BREADTH)
    handle = await asyncio.to_thread(open_subscriber, socket_path=str(socket_path))
    await _await_subscribe_ack(handle)
    done = asyncio.Event()
    reader_task = asyncio.create_task(_read_frames_until_done(sock=handle.sock, expected=3, done=done))

    # Burst the three remaining sources under the single breadth header.
    # A short pace between emits keeps each [event] line individually
    # visible without a per-source banner (which made breadth read as a
    # drawn-out list rather than a quick "and all of these too").
    breadth_events = [
        EventInsert(
            delivery_id=f"demo-pytest-{uuid.uuid4()}",
            source="pytest",
            event_type="pytest_session",
            owner="astrogilda",
            repo="waitbus-demo",
            received_at=time.time_ns(),
            payload_json=_synthetic_pytest_payload(),
            ingest_method="demo_synthesized",
            status="completed",
            conclusion="success",
        ),
        EventInsert(
            delivery_id=f"demo-docker-{uuid.uuid4()}",
            source="docker",
            event_type="docker_container",
            owner="astrogilda",
            repo="waitbus-demo",
            received_at=time.time_ns(),
            payload_json=_synthetic_docker_payload(),
            ingest_method="demo_synthesized",
            status="completed",
            conclusion="success",
            workflow_name="demo-worker",
        ),
        EventInsert(
            delivery_id=f"demo-fs-{uuid.uuid4()}",
            source="fs",
            event_type="fs_change",
            owner="astrogilda",
            repo="waitbus-demo",
            received_at=time.time_ns(),
            payload_json=_synthetic_fs_payload(str(state_dir / "demo.txt")),
            ingest_method="demo_synthesized",
            status="completed",
            conclusion="success",
            workflow_name="demo.txt",
        ),
    ]
    for event in breadth_events:
        await asyncio.to_thread(emit, event, db_path=db_path, doorbell_path=doorbell_path)
        await asyncio.sleep(_SOURCE_PACE_SECONDS)

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(done.wait(), timeout=_DRAIN_SECONDS)
    with contextlib.suppress(OSError):
        handle.sock.close()
    with contextlib.suppress(asyncio.CancelledError):
        reader_task.cancel()
        await reader_task


async def _run_demo(state_dir: Path, runtime_dir: Path) -> int:
    """Execute the demo flow end-to-end. Returns 0 on success, non-zero
    on internal failure. The tmp directories are owned by the caller and
    cleaned up after this returns regardless of exit code.

    Every daemon path is injected explicitly -- the temporary events DB, the
    broadcast listener socket, and the doorbell socket are passed straight to
    ``Broadcast(...)`` and ``emit(...)``. The demo therefore runs fully
    self-contained with NO process-global env mutation, so it is safe to call
    from within an in-process test runner with nothing to save or restore.
    """
    from waitbus import broadcast

    db_path = state_dir / "events.db"
    broadcast_sock = runtime_dir / "broadcast.sock"
    doorbell_sock = runtime_dir / "doorbell.sock"

    # Print before the daemon spins up so the terminal is not silent
    # during the ~3 s startup (socket bind + subscriber handshake). A
    # viewer (and the rendered demo recording) sees activity from the
    # first second rather than a blank prompt until the daemon is up.
    _echo("[demo] starting broadcast daemon...")
    daemon = broadcast.Broadcast(
        db_path=str(db_path), socket_path=str(broadcast_sock), doorbell_path=str(doorbell_sock)
    )
    daemon_task = asyncio.create_task(daemon.run())
    # Broadcast.run replaces asyncio's SIGINT handler at startup; safe here because demo() is
    # the asyncio.run entry point. Do not call _run_demo from a parent event loop.

    try:
        await _wait_for_socket(broadcast_sock, time.monotonic() + _BIND_TIMEOUT_SECONDS)
        _echo("[demo] waitbus broadcast daemon up.")
        print(file=sys.stderr)

        # Block on the event via the same await_predicate engine waitbus
        # wait uses, and unblock the instant CI finishes.
        await _demo_block_on_wait(broadcast_sock, db_path, doorbell_sock)
        print(file=sys.stderr)

        # Breadth: the same primitive delivers every source.
        await _demo_fan_out_breadth(broadcast_sock, db_path, state_dir, doorbell_sock)

        print(file=sys.stderr)
        _echo(_BANNER_CLOSING)
        return 0

    finally:
        await daemon.stop()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(daemon_task, timeout=5.0)


def demo() -> None:
    """Run the sixty-second waitbus first-impression flow against a tmp dir.

    Boots an in-process broadcast daemon, then shows the blocking wait
    (an agent blocks on ``waitbus wait`` and unblocks the instant a github
    event lands, with the avoided polling cost stated) followed by
    breadth (pytest / docker / fs events fan out to one subscriber).
    Output is colour-coded by role on a terminal. Total wall-clock
    budget is well under sixty seconds.

    Every event is synthesized in-process — no real HTTP listener, no
    real pytest run, no real Docker daemon, no real watchdog — and the
    banners say so. The flow is entirely local and idempotent; the
    temporary state directory is removed on exit.
    """
    with tempfile.TemporaryDirectory(prefix="waitbus-demo-") as tmp:
        tmp_root = Path(tmp)
        state_dir = tmp_root / "state"
        runtime_dir = tmp_root / "runtime"
        state_dir.mkdir()
        runtime_dir.mkdir()

        try:
            rc = asyncio.run(
                asyncio.wait_for(
                    _run_demo(state_dir, runtime_dir),
                    timeout=_DEMO_TIMEOUT_SECONDS,
                ),
            )
        except TimeoutError:
            print(
                f"[demo] timed out after {_DEMO_TIMEOUT_SECONDS}s; aborting.",
                file=sys.stderr,
            )
            raise typer.Exit(code=1) from None  # pragma: no cover  -- defensive: cancellation

    if rc != 0:
        raise typer.Exit(code=rc)  # pragma: no cover  -- defensive: daemon-wedged
