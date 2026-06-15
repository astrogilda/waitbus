"""Offline e2e: real Pydantic AI agents + the real broadcast daemon.

The model is a deterministic :class:`pydantic_ai.models.test.TestModel`
(calls each registered tool exactly once, no network, no LLM); the bus is
the REAL in-process broadcast daemon from the harness fixtures. Assertions
live at the bus boundary: the event was delivered, the agent woke.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
from conftest import await_subscribers, insert_event_row
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel
from waitbus_pydantic_ai import emit_tool, wait_tool

from waitbus import EventFrame, broadcast, wait_for

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED is Linux-only",
)


async def test_wait_tool_delivers_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """An agent blocked in wait_tool wakes on the emitted bus event."""
    daemon, paths = running_daemon
    captured: list[EventFrame] = []
    agent: Agent[None, str] = Agent(
        model=TestModel(),
        tools=[
            wait_tool(
                'fields.event_type="docker_container"',
                source="docker",
                timeout=5.0,
                socket_path=str(paths["broadcast"]),
                on_event=captured.append,
            )
        ],
    )
    # run_sync blocks inside the tool's wait_for, so it runs on a worker
    # thread while this coroutine drives the daemon and emits the event.
    run = asyncio.create_task(asyncio.to_thread(agent.run_sync, "Wait for the next bus event."))
    try:
        await await_subscribers(daemon)
        insert_event_row(paths["db"], "adapter-wait-1")
        result = await asyncio.wait_for(run, timeout=10.0)
    finally:
        run.cancel()
    assert result.output, "agent run produced no output"
    assert len(captured) == 1, "wait_tool did not capture the delivered event"
    assert captured[0].event_type == "docker_container"
    assert captured[0].delivery_id == "adapter-wait-1"


async def test_emit_tool_round_trips_through_the_bus(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """An agent's emit_tool message reaches a bus subscriber."""
    daemon, paths = running_daemon
    received: list[EventFrame | None] = []

    def _consume() -> None:
        received.append(wait_for(source="agent", timeout=5.0, socket_path=str(paths["broadcast"])))

    consumer = asyncio.create_task(asyncio.to_thread(_consume))
    try:
        await await_subscribers(daemon)
        agent: Agent[None, str] = Agent(
            model=TestModel(),
            tools=[
                emit_tool(
                    agent_name="adapter-demo",
                    db_path=paths["db"],
                    doorbell_path=paths["doorbell"],
                )
            ],
        )
        await asyncio.wait_for(asyncio.to_thread(agent.run_sync, "Announce yourself on the bus."), timeout=10.0)
        await asyncio.wait_for(consumer, timeout=10.0)
    finally:
        consumer.cancel()
    frame = received[0]
    assert frame is not None, "subscriber did not receive the emitted agent event"
    assert frame.event_type == "agent_message"
    assert frame.fields.get("msg_from") == "adapter-demo"
    assert frame.delivery_id.startswith("adapter-demo-")


async def test_wait_tool_times_out_without_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """With no event on the bus, the tool returns the timed-out string."""
    _, paths = running_daemon
    tool = wait_tool(source="docker", timeout=0.2, socket_path=str(paths["broadcast"]))
    # Tool.function is typed as a union over context-taking and plain
    # callables; the factory's closure takes no arguments, so narrow it.
    fn = cast(Callable[[], str | dict[str, Any]], tool.function)
    result = await asyncio.to_thread(fn)
    assert result == "timed out waiting for a bus event"


def test_wait_tool_requires_a_predicate() -> None:
    """The SDK's no-predicate ValueError propagates when the tool runs."""
    tool = wait_tool(timeout=0.1)
    fn = cast(Callable[[], str | dict[str, Any]], tool.function)
    with pytest.raises(ValueError, match="match spec"):
        fn()
