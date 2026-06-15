"""``swarm-demo`` top-level command — a discovery instrument for same-machine
agent coordination.

waitbus's broadcast fan-out, public :func:`~waitbus.emit` ingress, and
peer-credential gate already constitute a same-machine agent-coordination bus:
one agent emits an event, every other agent blocked on :func:`waitbus wait` wakes
the instant it lands, with zero polling. This command makes that latent
capability visible. It is a *discovery instrument* — something to put in front
of people running multiple agents to see which coordination pain is real — not
a committed product surface.

Three beats, all against one in-process broadcast daemon over a temporary state
directory:

1. **Conflict avoidance.** ``agent-2`` blocks on a ``agent_claim`` predicate;
   ``agent-1`` claims ``src/parser.py``; ``agent-2`` wakes and backs off to a
   different file instead of duplicating the work.
2. **Failure fan-out** (the load-bearing beat). ``agent-3`` (the fixer) blocks
   on ``agent_task_failed``; ``agent-1`` emits a build failure with the
   traceback in the payload; ``agent-3`` wakes with the error already in hand.
3. **Same bus, real sources.** The same predicate engine that just matched an
   agent event is the one ``waitbus wait`` rides for github / pytest / docker / fs
   — agent-coordination events and infrastructure events share one primitive.

Fidelity (the bar the older ``waitbus demo`` is held to): every beat blocks on the
**real** ``await_predicate`` engine fed the **real** predicate parsed from the
``--match`` string the banner displays, with the displayed timeout — the
narrated command and the actual wait are identical, and each event arrives only
after the waiter is genuinely parked. Nothing is faked into a successful
outcome.

Scope: same machine, same UID, in-process. The three "agents" are synthesized
coroutines, not real LLMs, and the banner says so. There is **no** inter-agent
isolation — a same-UID swarm shares one trust boundary by design (see
``SECURITY.md``); this command demonstrates coordination, not sandboxing.

Run via ``uvx waitbus swarm-demo`` (one command, no install) or ``waitbus
swarm-demo`` once installed. Total wall-clock budget is well under sixty
seconds; the temporary state directory is removed on exit so the operator's real
waitbus install is untouched.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import tempfile
import time
import uuid
from pathlib import Path

import typer

from ._shared import use_colour

# The ``agent`` source is a first-class built-in (see sources/_registry.py); the
# demo emits ``agent_claim`` / ``agent_task_failed`` against it directly, with no
# in-process registration step.
_AGENT_SOURCE = "agent"

_DEMO_TIMEOUT_SECONDS = 60.0
_BIND_TIMEOUT_SECONDS = 5.0
_REVEAL_PACE_SECONDS = 0.7
# How long a waiter sits visibly parked before the triggering event is emitted,
# long enough that a viewer registers "it is blocked, not polling".
_PARK_SECONDS = 1.0
# The per-beat wait budget shown in the banner AND fed to await_predicate.
_BEAT_TIMEOUT_SECONDS = 10.0

_BANNER_INTRO = (
    "[swarm-demo] Three agents and their events are SYNTHESIZED in-process -- no real LLMs are\n"
    "[swarm-demo] running. This shows the coordination primitive on the bus you already have:\n"
    "[swarm-demo] one agent emits, the others wake with zero polling. Same machine, same user."
)
_BANNER_CLOSING = (
    "[swarm-demo] One bus, zero polls, no account, no cloud, one machine. The same wait that\n"
    "[swarm-demo] matched these agent events also wakes on your CI, tests, containers, and files.\n"
    "[swarm-demo] Try it:  uvx waitbus swarm-demo    Docs: https://github.com/astrogilda/waitbus"
)


def _echo(line: str) -> None:
    """Print one line, role-coloured when the terminal supports it.

    ``[event]`` deliveries (the payoff) render green; ``[agent-N]`` narration
    renders cyan; everything else prints verbatim. Colour is stripped on a
    non-TTY so captured output is assertion-clean.
    """
    if use_colour():
        if line.startswith("[event]"):
            line = typer.style(line, fg=typer.colors.GREEN, bold=True)
        elif line.startswith("[agent-"):
            line = typer.style(line, fg=typer.colors.CYAN)
        elif line.startswith("[swarm-demo]"):
            line = typer.style(line, fg=typer.colors.BLUE)
    print(line, flush=True)


def _agent_payload(**fields: object) -> str:
    """Serialize coordination fields into the event ``payload_json``.

    Coordination data rides the payload, never new struct columns or a new wire
    ``kind`` -- agent events are ordinary ``kind="event"`` frames carrying an
    ``agent_*`` ``event_type`` (per the wire-freeze and local-primitive invariants).
    """
    return json.dumps(fields)


def _emit_agent_event(event_type: str, db_path: Path, doorbell_path: Path, **fields: object) -> None:
    """Emit one synthesized agent-coordination event via the public emit() API.

    ``db_path`` and ``doorbell_path`` are passed explicitly so the demo targets
    its own temporary daemon without mutating process-global path env vars.
    """
    from waitbus._emit import emit
    from waitbus._types import EventInsert

    emit(
        EventInsert(
            delivery_id=f"swarm-demo:{event_type}:{uuid.uuid4()}",
            source=_AGENT_SOURCE,
            event_type=event_type,
            owner="local",
            repo="swarm",
            received_at=time.time_ns(),
            payload_json=_agent_payload(**fields),
            ingest_method="swarm_demo_synthesized",
        ),
        db_path=db_path,
        doorbell_path=doorbell_path,
    )


async def _wait_for_socket(path: Path, deadline: float) -> None:
    """Poll until the broadcast socket file appears at ``path`` or ``deadline``."""
    while time.monotonic() < deadline:
        if path.exists():
            return
        await asyncio.sleep(0.02)
    raise RuntimeError(  # pragma: no cover -- defensive: socket-never-binds
        f"broadcast socket did not appear at {path}"
    )


async def _await_on_predicate(socket_path: Path, match_spec: str) -> bool:
    """Block on the REAL predicate parsed from ``match_spec`` and return whether it matched.

    This is the fidelity core: ``match_spec`` is the exact ``--match`` string the
    banner shows the operator, parsed by the same :func:`parse_match` the
    ``waitbus wait`` CLI uses and run through the same :func:`await_predicate`
    engine, with the same displayed timeout. The waiter confirms registration
    (``read_subscribe_ack``) before returning control so the caller can emit
    without racing the daemon's fan-out registration.
    """
    from .._broadcast_sub import FrameDecision, await_predicate, open_subscriber, read_subscribe_ack
    from .._predicate import parse_match

    predicate = parse_match([match_spec])
    handle = await asyncio.to_thread(open_subscriber, socket_path=str(socket_path))
    await asyncio.to_thread(read_subscribe_ack, handle)

    def _decide(frame: dict[str, object]) -> FrameDecision:
        return FrameDecision.MATCHED if predicate.evaluate(frame) else FrameDecision.CONTINUE

    try:
        outcome = await asyncio.to_thread(
            await_predicate, handle, decide=_decide, deadline_seconds=_BEAT_TIMEOUT_SECONDS
        )
    finally:
        with contextlib.suppress(OSError):
            handle.sock.close()
    return outcome.matched


async def _beat_conflict_avoidance(socket_path: Path, db_path: Path, doorbell_path: Path) -> None:
    """Beat 1: agent-2 wakes on agent-1's claim and backs off to a free file."""
    match = 'fields.event_type="agent_claim"'
    _echo("")
    _echo(f"[agent-2] blocked on the bus: waitbus wait --source agent --match '{match}' --timeout 10s")
    waiter = asyncio.create_task(_await_on_predicate(socket_path, match))
    await asyncio.sleep(_PARK_SECONDS)
    _echo("[agent-1] claiming src/parser.py to work on")
    _emit_agent_event("agent_claim", db_path, doorbell_path, agent="agent-1", file="src/parser.py")
    matched = await waiter
    if matched:
        _echo("[event] agent_claim  agent-1 -> src/parser.py")
        _echo("[agent-2] woke instantly: parser.py is taken -- backing off to src/lexer.py instead")
        _echo("[swarm-demo] without the bus, agent-2 never knew and edits the same file: wasted work or a conflict.")
    else:  # pragma: no cover -- the event is emitted inside the 10s beat window
        _echo("[swarm-demo] (beat 1 did not match within the window)")


async def _beat_failure_fanout(socket_path: Path, db_path: Path, doorbell_path: Path) -> None:
    """Beat 2 (load-bearing): agent-3 wakes on a failure with the error in hand."""
    match = 'fields.event_type="agent_task_failed"'
    _echo("")
    _echo(f"[agent-3] (fixer) blocked on the bus: waitbus wait --source agent --match '{match}' --timeout 10s")
    waiter = asyncio.create_task(_await_on_predicate(socket_path, match))
    await asyncio.sleep(_PARK_SECONDS)
    _echo("[agent-1] build failed -- announcing to the swarm")
    _emit_agent_event(
        "agent_task_failed",
        db_path,
        doorbell_path,
        agent="agent-1",
        task="build",
        error="AssertionError: parser.py:42 expected Token.LPAREN",
    )
    matched = await waiter
    if matched:
        _echo("[event] agent_task_failed  agent-1 build: AssertionError: parser.py:42 expected Token.LPAREN")
        _echo("[agent-3] woke instantly WITH the traceback already in hand -- starting the fix, zero polls")
        _echo("[swarm-demo] without the bus, agent-3 polls a log, or never learns, or the lead polls all ten.")
    else:  # pragma: no cover -- the event is emitted inside the 10s beat window
        _echo("[swarm-demo] (beat 2 did not match within the window)")


async def _run_swarm_demo(state_dir: Path, runtime_dir: Path) -> int:
    """Execute the swarm-demo flow end-to-end. Returns 0 on success.

    Every daemon path is injected explicitly -- the temporary events DB, the
    broadcast listener socket, and the doorbell socket are passed straight to
    ``Broadcast(...)`` and ``emit(...)``. The demo therefore runs fully
    self-contained with NO process-global env mutation, so it is safe to invoke
    in-process (e.g. from the CliRunner test) with nothing to save or restore.
    """
    from waitbus import broadcast

    db_path = state_dir / "events.db"
    broadcast_sock = runtime_dir / "broadcast.sock"
    doorbell_sock = runtime_dir / "doorbell.sock"

    _echo(_BANNER_INTRO)
    _echo("")
    _echo("[swarm-demo] starting broadcast daemon...")

    daemon = broadcast.Broadcast(
        db_path=str(db_path), socket_path=str(broadcast_sock), doorbell_path=str(doorbell_sock)
    )
    daemon_task = asyncio.create_task(daemon.run())
    try:
        await _wait_for_socket(broadcast_sock, time.monotonic() + _BIND_TIMEOUT_SECONDS)
        _echo("[swarm-demo] bus up.")
        await asyncio.sleep(_REVEAL_PACE_SECONDS)

        await _beat_conflict_avoidance(broadcast_sock, db_path, doorbell_sock)
        await _beat_failure_fanout(broadcast_sock, db_path, doorbell_sock)

        _echo("")
        _echo(_BANNER_CLOSING)
        return 0
    finally:
        await daemon.stop()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(daemon_task, timeout=5.0)


def swarm_demo() -> None:
    """Show same-machine agent coordination on the bus you already have.

    Boots an in-process broadcast daemon against a temporary state directory,
    registers a demo-scoped ``agent`` source, and runs three beats: one agent
    claims a file and another backs off, one agent's failure fans out to a fixer
    with the error in hand, and a closing note that the same predicate engine
    wakes on real CI / pytest / docker / fs events too.

    Every event is synthesized in-process -- no real LLMs, no network, no
    account -- and the opening banner says so. Each beat blocks on the real
    ``await_predicate`` engine with the real predicate the banner displays, so
    the narration matches the wait exactly. Same machine, same user, no
    inter-agent isolation by design. The temporary state directory is removed on
    exit; total wall-clock is well under sixty seconds.
    """
    with tempfile.TemporaryDirectory(prefix="waitbus-swarm-demo-") as tmp:
        tmp_root = Path(tmp)
        state_dir = tmp_root / "state"
        runtime_dir = tmp_root / "runtime"
        state_dir.mkdir()
        runtime_dir.mkdir()
        try:
            rc = asyncio.run(
                asyncio.wait_for(_run_swarm_demo(state_dir, runtime_dir), timeout=_DEMO_TIMEOUT_SECONDS),
            )
        except TimeoutError:  # pragma: no cover -- defensive: wall-clock guard
            print(f"[swarm-demo] timed out after {_DEMO_TIMEOUT_SECONDS}s; aborting.", file=sys.stderr)
            raise typer.Exit(code=1) from None

    if rc != 0:  # pragma: no cover -- defensive: daemon-wedged
        raise typer.Exit(code=rc)
