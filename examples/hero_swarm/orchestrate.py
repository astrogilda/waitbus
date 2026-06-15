"""HERO cross-harness demo: two different real agent frameworks on ONE local bus.

This is the load-bearing proof that waitbus is a *cross-harness* coordination bus,
not a single-process toy: TWO genuinely different agent frameworks --
**Pydantic AI** and **LangGraph** -- run as **separate OS processes**, each
subscribed to one local broadcast daemon. A third process (a Pydantic AI
"worker") FAILS and announces it on the bus; a PEER process built on the *other*
framework (LangGraph) wakes the instant the failure lands, and a live
``waitbus top`` view reacts at the same time. Nothing polls; the peer is parked
in the waitbus SDK's blocking ``wait_for`` until the failure event arrives.

Why separate OS processes (not coroutines). ``waitbus swarm-demo`` proves the
coordination primitive but runs every "agent" as an in-process coroutine against
one in-process daemon -- it cannot prove *cross-harness* failure broadcast,
because there is only one process and one framework. This orchestrator launches
real ``waitbus broadcast serve``, ``waitbus top``, and the agent frameworks as
distinct ``subprocess`` children connecting over an AF_UNIX socket, so the proof
is genuinely "framework A in process X wakes framework B in process Y".

HONESTY (read the banner the orchestrator prints):

* The agents are SYNTHESIZED with FAKE models -- Pydantic AI's ``TestModel`` and
  LangGraph's ``FakeListChatModel``. No real LLM, no network, no account, no
  cloud. The waitbus integration (the ``wait_for`` subscribe and the ``emit``
  failure broadcast) is REAL; only the model is faked, exactly like the
  committed ``examples/agent_pydantic_ai`` and ``examples/agent_langgraph``
  canaries.
* The failure event is INJECTED: the failing worker does not crash a real build;
  it deterministically emits one ``agent_task_failed`` event so the demo is
  reproducible. Each agent process's PID is named in the banner.

The ``agent`` source is a first-class built-in waitbus source, owning the
``agent_message`` / ``agent_claim`` / ``agent_task_failed`` event types in the
built-in taxonomy. The failing worker simply ``emit()``s against it -- no
in-process registration step is needed, because the daemon's
``event_types_supported()`` already knows ``agent_task_failed`` (a taxonomy entry
is not a daemon-resident watcher, so the daemon footprint is unchanged).

Process supervision. Every child runs in its OWN process group
(``process_group=0``) and is torn down on exit with the ``waitbus on`` supervision
model: ``SIGTERM`` to the whole
group, a bounded grace window, then an unconditional group ``SIGKILL`` so no
child -- and no grandchild -- is orphaned. The temporary state directory is
removed on exit, leaving the operator's real waitbus install untouched.

Run it::

    python -m examples.hero_swarm.orchestrate

The same module file is also the subprocess entry point for the three agent
roles (``peer-pydantic`` / ``peer-langgraph`` / ``failing-worker``); the
orchestrator dispatches to them by argv so the whole demo is one file.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict


class _LangGraphState(TypedDict, total=False):
    """LangGraph peer's graph state (module-level so the StateGraph overload
    binds the TypedDict node-input TypeVar -- a node-local class does not)."""

    reacted: bool
    summary: str | None


# The built-in `agent` source + the single failure event type this demo emits.
# `agent` is a first-class built-in (see the module docstring); the worker just
# emits against it, with no in-process registration step.
_AGENT_SOURCE = "agent"
_AGENT_FAILED_EVENT = "agent_task_failed"
_FAILURE_MATCH = f'fields.event_type="{_AGENT_FAILED_EVENT}"'

# Marker line a peer prints on its stdout when it OBSERVABLY wakes on the bus.
# The orchestrator and the e2e test both scan for it -- it is the cross-harness
# proof signal (framework B woke on framework A's failure).
_WOKE_MARKER = "HERO_PEER_WOKE"

# Wall-clock budgets. Generous so a loaded CI box does not flake, but bounded so
# a wedged child cannot hang the demo forever.
_SOCKET_BIND_TIMEOUT = 10.0
_SUBSCRIBER_SETTLE_SECONDS = 1.5
_PEER_WAIT_TIMEOUT = 20.0
_PEER_JOIN_TIMEOUT = 25.0
_STOP_GRACE_SECONDS = 5.0


# ---------------------------------------------------------------------------
# Agent role entry points (run as separate OS processes)
# ---------------------------------------------------------------------------
#
# Each role is invoked as `python -m examples.hero_swarm.orchestrate <role>
# --socket ... [--db ... --doorbell ...]`. They import their framework lazily
# so a missing optional dependency only fails the role that needs it.


def _daemon() -> int:
    """Run the unmodified broadcast daemon.

    A thin bootstrap around the real ``waitbus broadcast serve`` (``broadcast.main``).
    The ``agent`` source is a first-class built-in, so the daemon's
    ``event_types_supported()`` already knows ``agent_task_failed`` and fans it out
    to the parked peers -- no demo-scoped registration step is needed.
    """
    from waitbus import broadcast

    return broadcast.main()


def _peer_pydantic(socket_path: str, timeout: float) -> int:
    """Pydantic AI peer: a real Agent (offline TestModel) parked on the bus.

    Builds a genuine ``pydantic_ai.Agent`` whose only LLM is the deterministic
    offline ``TestModel`` (no network), exposing one tool that calls the REAL
    waitbus SDK ``wait_for`` on the failure predicate. The fake model fires the
    tool once; the tool blocks until the failing worker's ``agent_task_failed``
    event arrives, then the peer prints the wake marker and exits 0. This is the
    same integration shape as ``examples/agent_pydantic_ai`` -- only the
    predicate (an agent failure, not a docker event) differs.
    """
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    from waitbus import wait_for

    woke = {"value": False}
    agent: Agent[None, str] = Agent(
        model=TestModel(),
        system_prompt="React to a peer agent's failure on the waitbus bus.",
    )

    @agent.tool_plain
    def wait_for_peer_failure() -> str:
        """Block on the waitbus bus for one peer ``agent_task_failed`` event."""
        frame = wait_for(_FAILURE_MATCH, source=_AGENT_SOURCE, timeout=timeout, socket_path=socket_path)
        if frame is None:
            return "timed out waiting for a peer failure"
        woke["value"] = True
        print(f"{_WOKE_MARKER} framework=pydantic-ai event={frame.event_type} delivery={frame.delivery_id}", flush=True)
        return f"woke on peer failure: {frame.event_type}"

    agent.run_sync("Wait for a peer agent to fail, then react.")
    return 0 if woke["value"] else 1


def _peer_langgraph(socket_path: str, timeout: float) -> int:
    """LangGraph peer: a real StateGraph (offline fake model) parked on the bus.

    Builds a genuine ``langgraph.graph.StateGraph`` whose first node blocks in
    the REAL waitbus SDK ``wait_for`` on the failure predicate and whose second
    node "summarises" the failure with a deterministic offline
    ``FakeListChatModel`` (no network). Same integration shape as
    ``examples/agent_langgraph`` -- only the predicate differs. Prints the wake
    marker and exits 0 once the failure arrives.
    """
    from langchain_core.language_models.fake_chat_models import FakeListChatModel
    from langgraph.graph import END, START, StateGraph

    from waitbus import wait_for

    def wait_on_failure(_state: _LangGraphState) -> dict[str, Any]:
        frame = wait_for(_FAILURE_MATCH, source=_AGENT_SOURCE, timeout=timeout, socket_path=socket_path)
        if frame is None:
            return {"reacted": False}
        print(
            f"{_WOKE_MARKER} framework=langgraph event={frame.event_type} delivery={frame.delivery_id}",
            flush=True,
        )
        return {"reacted": True}

    def summarize(state: _LangGraphState) -> dict[str, Any]:
        if not state.get("reacted"):
            return {"summary": None}
        chat = FakeListChatModel(responses=["A peer agent failed; taking over its task."])
        reply = chat.invoke("Summarise the peer failure.")
        content = reply.content
        return {"summary": content if isinstance(content, str) else str(content)}

    # LangGraph's `add_node` overloads do not bind cleanly to a TypedDict node
    # under mypy --strict (the committed examples are outside the mypy `files`
    # scope, so they never hit this); the graph builder is dynamically typed in
    # practice, so build it through an Any-typed handle rather than scattering
    # per-call ignores. The runtime graph is unchanged.
    builder: Any = StateGraph(_LangGraphState)
    builder.add_node("wait_on_failure", wait_on_failure)
    builder.add_node("summarize", summarize)
    builder.add_edge(START, "wait_on_failure")
    builder.add_edge("wait_on_failure", "summarize")
    builder.add_edge("summarize", END)
    graph = builder.compile()
    result: _LangGraphState = graph.invoke({"reacted": False, "summary": None})
    return 0 if result.get("reacted") else 1


def _failing_worker(socket_path: str, db_path: str, doorbell_path: str, delivery_id: str) -> int:
    """The worker that FAILS and broadcasts it (a real Pydantic AI agent).

    Runs a real ``pydantic_ai.Agent`` with an offline ``TestModel`` whose tool
    deterministically "fails its build" and emits ONE ``agent_task_failed`` event
    via the public ``emit()`` API against the demo's temporary daemon. ``agent``
    is a first-class built-in source, so the emit needs no registration step --
    it targets the built-in ``agent_task_failed`` event type directly. The failure
    is injected, not a real crash, so the demo is reproducible -- the banner says
    so.
    """
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    from waitbus._emit import emit
    from waitbus._types import EventInsert

    agent: Agent[None, str] = Agent(
        model=TestModel(),
        system_prompt="Run a build task; announce failure to the swarm if it breaks.",
    )

    @agent.tool_plain
    def run_build_then_announce_failure() -> str:
        """Simulate a failing build and broadcast one ``agent_task_failed`` event."""
        emit(
            EventInsert(
                delivery_id=delivery_id,
                source=_AGENT_SOURCE,
                event_type=_AGENT_FAILED_EVENT,
                owner="local",
                repo="swarm",
                received_at=time.time_ns(),
                payload_json='{"agent": "worker-pydantic", "task": "build", '
                '"error": "AssertionError: parser.py:42 expected Token.LPAREN"}',
                ingest_method="hero_swarm_synthesized",
            ),
            db_path=Path(db_path),
            doorbell_path=Path(doorbell_path),
        )
        return "announced build failure to the swarm"

    agent.run_sync("Run the build task.")
    return 0


# ---------------------------------------------------------------------------
# Process supervision (the `waitbus on` model: own group + SIGTERM->grace->SIGKILL)
# ---------------------------------------------------------------------------


@dataclass
class _Child:
    """A supervised child subprocess in its own process group.

    Spawned with ``process_group=0`` (``setpgid(0,0)`` => PGID == child PID) so
    :meth:`terminate` can signal the whole group via :func:`os.killpg` without
    touching the orchestrator. Teardown follows:
    ``SIGTERM`` to the group, a bounded grace window, then an UNCONDITIONAL group
    ``SIGKILL`` (a no-op if already dead) so a gracefully-exiting child that left
    grandchildren cannot orphan them.
    """

    name: str
    proc: subprocess.Popen[str]

    def terminate(self, grace: float = _STOP_GRACE_SECONDS) -> None:
        """Tear the child's whole process group down: SIGTERM, grace, then SIGKILL.

        Also closes any open ``Popen`` std streams after the process is reaped, so
        a captured child whose stdout pipe was never consumed (the failing worker,
        which is supervised but not ``communicate()``d) cannot leak an open file
        descriptor -- the strict ``ResourceWarning`` gate would otherwise fail.
        """
        if self.proc.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.proc.pid, signal.SIGTERM)
            with contextlib.suppress(subprocess.TimeoutExpired):
                self.proc.wait(timeout=grace)
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.proc.pid, signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            self.proc.wait(timeout=2.0)
        for stream in (self.proc.stdout, self.proc.stderr, self.proc.stdin):
            if stream is not None:
                with contextlib.suppress(OSError):
                    stream.close()


def _spawn(name: str, argv: list[str], env: dict[str, str], *, capture: bool) -> _Child:
    """Launch ``argv`` in a fresh process group as a supervised :class:`_Child`.

    ``capture`` routes stdout/stderr to pipes (peers, whose wake marker the
    orchestrator scans); otherwise the child inherits the orchestrator's stdio
    (the daemon and ``waitbus top``, whose live output is the show).
    """
    proc = subprocess.Popen(
        argv,
        env=env,
        process_group=0,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )
    return _Child(name=name, proc=proc)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _wait_for_socket(path: Path, deadline: float) -> bool:
    """Poll until the broadcast socket file appears at ``path`` or ``deadline``."""
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.05)
    return False


def _role_argv(
    role: str,
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    delivery_id: str | None = None,
) -> list[str]:
    """Build the ``python -m examples.hero_swarm <role> ...`` argv for a child."""
    # `-m examples.hero_swarm` (the package, via __main__.py) rather than
    # `-m ...orchestrate`: running a module that is ALSO imported (by the
    # package, the test, and sibling children) triggers a runpy double-import
    # RuntimeWarning. The package __main__ is a distinct module, so no warning.
    argv = [sys.executable, "-m", "examples.hero_swarm", role, "--socket", str(socket_path)]
    if role == "failing-worker":
        argv += ["--db", str(db_path), "--doorbell", str(doorbell_path), "--delivery-id", str(delivery_id)]
    return argv


@dataclass
class HeroResult:
    """Outcome of one hero-demo run, for the orchestrator + the e2e test.

    ``peer_woke`` maps each peer's framework name to whether it OBSERVABLY woke
    on the cross-harness failure (the proof). ``peer_output`` carries each peer's
    captured stdout (the wake marker line) for assertion / display.
    """

    peer_woke: dict[str, bool]
    peer_output: dict[str, str]
    delivery_id: str

    @property
    def cross_harness_proven(self) -> bool:
        """True iff at least two DIFFERENT frameworks woke on one failure event."""
        return sum(1 for woke in self.peer_woke.values() if woke) >= 2


def _banner(daemon_pid: int, peer_pids: dict[str, int], worker_pid: int) -> str:
    """The honesty banner: name each PID, state the fakes, state the injection."""
    return (
        "\n[hero-swarm] CROSS-HARNESS demo: two DIFFERENT real agent frameworks on ONE local bus.\n"
        f"[hero-swarm]   broadcast daemon   pid={daemon_pid}  (real `waitbus broadcast serve`, temp socket)\n"
        f"[hero-swarm]   peer: Pydantic AI  pid={peer_pids.get('pydantic-ai', 0)}  (parked in wait_for on the bus)\n"
        f"[hero-swarm]   peer: LangGraph    pid={peer_pids.get('langgraph', 0)}  (parked in wait_for on the bus)\n"
        f"[hero-swarm]   failing worker     pid={worker_pid}  (Pydantic AI; will emit agent_task_failed)\n"
        "[hero-swarm] Agents are SYNTHESIZED with FAKE models (TestModel / FakeListChatModel) -- no real\n"
        "[hero-swarm] LLMs, no network, no account. The waitbus subscribe/emit is REAL; the failure event\n"
        "[hero-swarm] is INJECTED (a deterministic emit, not a real crash). `agent` is a first-class\n"
        "[hero-swarm] built-in waitbus source; the worker emits `agent_task_failed` against it directly."
    )


def run_hero_demo(state_dir: Path, runtime_dir: Path) -> HeroResult:
    """Run the cross-harness demo end-to-end and return the proof result.

    Spawns the real broadcast daemon, two peer agents on two different
    frameworks (both parked in ``wait_for`` on the bus), and a ``waitbus top``
    view; then spawns the failing worker, whose single ``agent_task_failed``
    emit wakes BOTH peers across the process boundary. Every child is supervised
    in its own process group and torn down deterministically on exit.
    """
    socket_path = runtime_dir / "broadcast.sock"
    db_path = state_dir / "github.db"
    doorbell_path = runtime_dir / "doorbell.sock"

    env = dict(os.environ)
    env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
    env["WAITBUS_STATE_DIR"] = str(state_dir)
    # Keep the children from auto-loading the operator's entry-point plugins:
    # the demo emits against the built-in `agent` source only, so no third-party
    # plugin discovery is wanted here.
    env["WAITBUS_DISABLE_SOURCE_AUTOLOAD"] = "1"

    children: list[_Child] = []
    peers: dict[str, _Child] = {}
    try:
        daemon = _spawn(
            "daemon",
            [sys.executable, "-m", "examples.hero_swarm", "daemon"],
            env,
            capture=False,
        )
        children.append(daemon)
        if not _wait_for_socket(socket_path, time.monotonic() + _SOCKET_BIND_TIMEOUT):
            raise RuntimeError(f"broadcast daemon did not bind {socket_path} in time")

        for role, fw in (("peer-pydantic", "pydantic-ai"), ("peer-langgraph", "langgraph")):
            child = _spawn(fw, _role_argv(role, socket_path, db_path, doorbell_path), env, capture=True)
            children.append(child)
            peers[fw] = child

        top = _spawn(
            "top",
            # The `waitbus` console-script (next to this interpreter), not
            # `-m waitbus.cli.main` -- the latter runs an importable
            # module as __main__ and emits a runpy double-import warning that
            # would surface in the recorded demo.
            [str(Path(sys.executable).parent / "waitbus"), "top", "--max-frames", "1", "--timeout", "20s"],
            env,
            capture=False,
        )
        children.append(top)

        # Let the parked peers + top finish their daemon-side subscribe handshake
        # before the failure is emitted, so the live fan-out reaches them.
        time.sleep(_SUBSCRIBER_SETTLE_SECONDS)

        # The orchestrator MINTS the failure event's delivery_id and threads it
        # to the worker, so the proof can assert each peer observed THIS specific
        # event (not merely "a" wake): the peers echo frame.delivery_id back.
        delivery_id = f"hero-swarm:{_AGENT_FAILED_EVENT}:{uuid.uuid4()}"
        worker = _spawn(
            "failing-worker",
            _role_argv("failing-worker", socket_path, db_path, doorbell_path, delivery_id),
            env,
            capture=True,
        )
        children.append(worker)

        print(
            _banner(
                daemon.proc.pid,
                {fw: child.proc.pid for fw, child in peers.items()},
                worker.proc.pid,
            ),
            flush=True,
        )

        return _collect_peer_results(peers, delivery_id)
    finally:
        # Reverse order: children spawned last (worker) torn down first.
        for child in reversed(children):
            child.terminate()


def _woke_on_delivery(text: str, framework: str, expected_delivery_id: str) -> bool:
    """True iff a full wake-marker line for ``framework`` carries the exact id.

    A structured, full-line, event-identity match -- the peer observed THIS
    delivery, not merely a substring somewhere in a merged stdout+stderr stream.
    """
    prefix = f"{_WOKE_MARKER} framework={framework} "
    want = f"delivery={expected_delivery_id}"
    return any(line.startswith(prefix) and want in line.split() for line in text.splitlines())


def _collect_peer_results(peers: dict[str, _Child], expected_delivery_id: str) -> HeroResult:
    """Join each peer and record whether it OBSERVED the specific failure event.

    A peer "woke" only if its wake-marker line carries the exact delivery_id the
    worker emitted -- decoupled from the peer's exit code, so the proof asserts
    the cross-harness broadcast delivered THIS event to THIS framework.
    """
    peer_woke: dict[str, bool] = {}
    peer_output: dict[str, str] = {}
    for fw, child in peers.items():
        try:
            out, _ = child.proc.communicate(timeout=_PEER_JOIN_TIMEOUT)
        except subprocess.TimeoutExpired:
            child.terminate()
            out, _ = child.proc.communicate(timeout=_PEER_JOIN_TIMEOUT)
        text = out or ""
        peer_output[fw] = text
        peer_woke[fw] = _woke_on_delivery(text, fw, expected_delivery_id)
    return HeroResult(peer_woke=peer_woke, peer_output=peer_output, delivery_id=expected_delivery_id)


def _orchestrate() -> int:
    """Top-level orchestrator: run the demo in a throwaway state dir, report the proof."""
    with tempfile.TemporaryDirectory(prefix="waitbus-hero-swarm-") as tmp:
        root = Path(tmp)
        state_dir = root / "state"
        runtime_dir = root / "runtime"
        state_dir.mkdir(mode=0o700)
        runtime_dir.mkdir(mode=0o700)
        result = run_hero_demo(state_dir, runtime_dir)

    print("\n[hero-swarm] --- result ---", flush=True)
    for fw, woke in result.peer_woke.items():
        line = (result.peer_output.get(fw) or "").strip().splitlines()
        marker = next((s for s in line if _WOKE_MARKER in s), "(no wake marker)")
        print(f"[hero-swarm]   {fw:<12} woke={woke}  {marker}", flush=True)
    if result.cross_harness_proven:
        print(
            "[hero-swarm] PROVEN: two DIFFERENT frameworks woke on ONE peer's failure -- "
            "cross-harness failure broadcast on a single local bus.",
            flush=True,
        )
        return 0
    print("[hero-swarm] NOT proven: fewer than two frameworks woke (see output above).", file=sys.stderr, flush=True)
    return 1


def main(argv: list[str] | None = None) -> int:
    """Entry point: dispatch to an agent role (subprocess) or run the orchestrator.

    With no role argument this runs the full cross-harness demo. The role
    subcommands (``peer-pydantic`` / ``peer-langgraph`` / ``failing-worker``) are
    the subprocess entry points the orchestrator spawns; they are not meant to be
    invoked by hand (they need ``--socket`` and, for the worker, ``--db`` /
    ``--doorbell`` pointing at the demo's temporary daemon).
    """
    parser = argparse.ArgumentParser(description="waitbus HERO cross-harness swarm demo.")
    sub = parser.add_subparsers(dest="role")
    sub.add_parser("daemon")  # demo daemon bootstrap (no args; env-driven paths)
    for role in ("peer-pydantic", "peer-langgraph", "failing-worker"):
        rp = sub.add_parser(role)
        rp.add_argument("--socket", required=True)
        rp.add_argument("--timeout", type=float, default=_PEER_WAIT_TIMEOUT)
        if role == "failing-worker":
            # The worker EMITS, so it must target the demo's daemon explicitly --
            # never default XDG paths (a hand-run worker would hit the real DB).
            rp.add_argument("--db", required=True)
            rp.add_argument("--doorbell", required=True)
            rp.add_argument("--delivery-id", required=True)
    args = parser.parse_args(argv)

    if args.role == "daemon":
        return _daemon()
    if args.role == "peer-pydantic":
        return _peer_pydantic(args.socket, args.timeout)
    if args.role == "peer-langgraph":
        return _peer_langgraph(args.socket, args.timeout)
    if args.role == "failing-worker":
        return _failing_worker(args.socket, args.db, args.doorbell, args.delivery_id)
    return _orchestrate()


if __name__ == "__main__":
    sys.exit(main())
