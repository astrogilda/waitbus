"""Canary: a REAL LangGraph agent subscribes to waitbus and reacts to an event.

It stands up
the broadcast daemon, runs the real LangGraph graph from
``examples/agent_langgraph`` on a worker thread (the ``wait_on_waitbus`` node
blocks in the waitbus SDK), emits one event, and asserts the graph WOKE and
captured it.

CANARY scope (defined in ``_agent_harness``):
assert waitbus DELIVERED the event and the graph node woke — NOT LangGraph's
internal channel/state-machine details.

Skips cleanly when the agent-recipes group (LangGraph) is not installed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from threading import Thread

import pytest

pytest.importorskip("langgraph")

from examples.agent_langgraph import build_graph, run
from tests._agent_harness import AgentRun, run_agent_and_emit
from waitbus import broadcast

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform != "linux",
        reason="broadcast daemon SO_PEERCRED is Linux-only",
    ),
]


@pytest.mark.asyncio
async def test_langgraph_agent_reacts_to_waitbus_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """A compiled LangGraph graph wakes on a waitbus docker_container event."""

    def start_agent(socket_path: str) -> AgentRun:
        # The graph blocks in wait_on_waitbus, so run graph.invoke on a worker
        # thread and capture its final state. The thread returns once the event
        # arrives and the graph completes — deterministic teardown for the
        # strict ResourceWarning gate (the SDK socket closes when invoke ends).
        graph = build_graph(socket_path, timeout=5.0)
        captured: dict[str, object] = {}

        def _invoke() -> None:
            captured.update(graph.invoke({"event_type": None, "summary": None, "reacted": False}))

        thread = Thread(target=_invoke, name="langgraph-agent", daemon=True)
        thread.start()
        return AgentRun(
            thread=thread,
            reacted=lambda: bool(captured.get("reacted")),
            detail=captured,
        )

    run = await run_agent_and_emit(
        running_daemon,
        start_agent=start_agent,
        delivery_id="lg-1",
        event={"source": "docker", "event_type": "docker_container"},
    )

    assert run.reacted(), f"graph did not wake on the waitbus event: {run.detail!r}"
    assert run.detail["event_type"] == "docker_container"


@pytest.mark.asyncio
async def test_langgraph_run_wrapper_reacts_to_waitbus_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """The example's ``run()`` wrapper (compile + invoke) wakes on a waitbus event --
    exercises the literal copy-paste entrypoint, not just ``build_graph``."""

    def start_agent(socket_path: str) -> AgentRun:
        captured: dict[str, object] = {}

        def _invoke() -> None:
            captured.update(run(socket_path, timeout=5.0))

        thread = Thread(target=_invoke, name="langgraph-run", daemon=True)
        thread.start()
        return AgentRun(thread=thread, reacted=lambda: bool(captured.get("reacted")), detail=captured)

    out = await run_agent_and_emit(
        running_daemon,
        start_agent=start_agent,
        delivery_id="lg-run-1",
        event={"source": "docker", "event_type": "docker_container"},
    )
    assert out.reacted(), f"run() did not wake on the waitbus event: {out.detail!r}"
    assert out.detail["event_type"] == "docker_container"
