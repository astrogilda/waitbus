"""Canary: a real Pydantic AI agent subscribes to waitbus and wakes on an event.

It builds a
``pydantic_ai.Agent`` (its LLM replaced by the deterministic, offline
``TestModel``) whose tool calls the real waitbus SDK ``wait_for``, runs it on a
worker thread, emits one ``docker_container`` event through the broadcast
daemon, and asserts the agent OBSERVABLY reacted.

CANARY scope: the assertions live at the waitbus boundary — the event was
DELIVERED to the agent and the agent WOKE and captured it — never on Pydantic
AI's internal state machine.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from pathlib import Path
from threading import Thread

import pytest

pytest.importorskip("pydantic_ai")

from examples.agent_pydantic_ai import EventCapture
from tests._agent_harness import AgentRun, run_agent_and_emit
from waitbus import broadcast

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform != "linux",
        reason="broadcast daemon SO_PEERCRED is Linux-only",
    ),
]


def _closing_thread_loop(target: Callable[[], None]) -> Callable[[], None]:
    """Run ``target`` on a worker thread, then close the event loop it left behind.

    ``Agent.run_sync`` outside a running loop creates a fresh event loop for the
    calling thread, installs it with ``asyncio.set_event_loop`` and never closes
    it. When that loop is garbage-collected it raises ``ResourceWarning:
    unclosed event loop`` (and one for each of its self-pipe sockets), which the
    strict unraisable-exception gate turns into a teardown error. The thread
    owns the loop, so the thread closes it.
    """

    def _run() -> None:
        try:
            target()
        finally:
            loop: asyncio.AbstractEventLoop | None
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # No loop was ever installed on this thread (target failed first).
                loop = None
            if loop is not None and not loop.is_closed():
                loop.close()
            asyncio.set_event_loop(None)

    return _run


def _start_agent(socket_path: str) -> AgentRun:
    """Launch the Pydantic AI agent on a worker thread.

    The agent blocks inside its ``wait_for_waitbus_event`` tool until the harness
    emits the event, then ``run_sync`` completes and the thread exits — closing
    the SDK socket deterministically for the strict ResourceWarning gate.
    """
    capture: EventCapture = EventCapture()

    def _run() -> None:
        # Reuse the example's wiring but with our own capture so the test can
        # observe the delivered event after the thread joins.
        from examples.agent_pydantic_ai.agent import WaitbusDeps, build_agent

        agent = build_agent()
        deps = WaitbusDeps(socket_path=socket_path, timeout=5.0, capture=capture)
        agent.run_sync("Wait for the next waitbus event.", deps=deps)

    thread = Thread(target=_closing_thread_loop(_run), name="pydantic-ai-agent", daemon=True)
    thread.start()
    return AgentRun(
        thread=thread,
        reacted=lambda: capture.reacted,
        detail={"capture": capture},
    )


@pytest.mark.asyncio
async def test_pydantic_ai_agent_wakes_on_waitbus_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """A real Pydantic AI agent (offline TestModel) wakes on a waitbus event."""
    run_result = await run_agent_and_emit(
        running_daemon,
        start_agent=_start_agent,
        delivery_id="pyai-1",
        event={"source": "docker", "event_type": "docker_container"},
    )

    assert run_result.reacted(), "agent did not react to the emitted waitbus event"
    capture: EventCapture = run_result.detail["capture"]
    assert capture.events, "no event captured by the agent tool"
    assert capture.events[0]["event_type"] == "docker_container"
    assert capture.events[0]["delivery_id"] == "pyai-1"


def _start_run_wrapper(socket_path: str) -> AgentRun:
    """Launch the example's ``run()`` wrapper (the literal copy-paste entrypoint)."""
    holder: dict[str, EventCapture] = {}

    def _run() -> None:
        from examples.agent_pydantic_ai import run

        holder["capture"] = run(socket_path, timeout=5.0)

    thread = Thread(target=_closing_thread_loop(_run), name="pydantic-ai-run", daemon=True)
    thread.start()
    return AgentRun(
        thread=thread,
        reacted=lambda: bool(holder.get("capture") and holder["capture"].reacted),
        detail=holder,
    )


@pytest.mark.asyncio
async def test_pydantic_ai_run_wrapper_wakes_on_waitbus_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """The example's ``run()`` wrapper wakes on a waitbus event -- exercises the
    literal copy-paste entrypoint, not just ``build_agent``."""
    run_result = await run_agent_and_emit(
        running_daemon,
        start_agent=_start_run_wrapper,
        delivery_id="pyai-run-1",
        event={"source": "docker", "event_type": "docker_container"},
    )
    assert run_result.reacted(), "run() wrapper did not react to the emitted waitbus event"
    capture: EventCapture = run_result.detail["capture"]
    assert capture.events and capture.events[0]["event_type"] == "docker_container"
