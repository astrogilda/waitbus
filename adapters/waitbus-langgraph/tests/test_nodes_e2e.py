"""Offline e2e: a real compiled StateGraph + the real broadcast daemon.

The chat model is a deterministic
:class:`langchain_core.language_models.fake_chat_models.FakeListChatModel`
(no network, no LLM); the bus is the REAL in-process broadcast daemon from
the harness fixtures. Assertions live at the bus boundary: the event was
delivered (or the wait timed out) and the graph routed accordingly.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, TypedDict

import pytest
from conftest import await_subscribers, insert_event_row
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langgraph.graph import END, START, StateGraph
from waitbus_langgraph import event_router, wait_node

from waitbus import broadcast

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED is Linux-only",
)

_FAKE_SUMMARY = "A bus event arrived; acting on it."


class BusState(TypedDict, total=False):
    """Channels the test graph reads/writes."""

    event: dict[str, Any] | None
    summary: str | None
    reacted: bool


def _build_graph(socket_path: str, *, timeout: float) -> Any:
    """Compile the wait -> route -> summarize test graph."""

    def summarize(state: BusState) -> dict[str, Any]:
        chat = FakeListChatModel(responses=[_FAKE_SUMMARY])
        event = state.get("event") or {}
        reply = chat.invoke(f"Summarise this bus event: {event.get('event_type')}")
        content = reply.content
        return {"summary": content if isinstance(content, str) else str(content)}

    builder: StateGraph[BusState] = StateGraph(BusState)
    builder.add_node(
        "wait_on_bus",
        wait_node(
            'fields.event_type="docker_container"',
            source="docker",
            timeout=timeout,
            socket_path=socket_path,
        ),
    )
    builder.add_node("summarize", summarize)
    builder.add_edge(START, "wait_on_bus")
    builder.add_conditional_edges("wait_on_bus", event_router(on_event="summarize", on_timeout=END))
    builder.add_edge("summarize", END)
    return builder.compile()


async def test_graph_reacts_to_delivered_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """The graph wakes on the emitted event and routes to the summarize node."""
    daemon, paths = running_daemon
    graph = _build_graph(str(paths["broadcast"]), timeout=5.0)
    initial: BusState = {"event": None, "summary": None, "reacted": False}
    # graph.invoke blocks inside wait_node's wait_for, so it runs on a worker
    # thread while this coroutine drives the daemon and emits the event.
    run = asyncio.create_task(asyncio.to_thread(graph.invoke, initial))
    try:
        await await_subscribers(daemon)
        insert_event_row(paths["db"], "adapter-lg-1")
        final = await asyncio.wait_for(run, timeout=10.0)
    finally:
        run.cancel()
    assert final["reacted"] is True, f"graph did not wake on the bus event: {final!r}"
    assert final["event"] is not None
    assert final["event"]["event_type"] == "docker_container"
    assert final["event"]["delivery_id"] == "adapter-lg-1"
    assert final["summary"] == _FAKE_SUMMARY


async def test_graph_routes_to_end_on_timeout(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """With no event on the bus, the router takes the timeout branch."""
    _, paths = running_daemon
    graph = _build_graph(str(paths["broadcast"]), timeout=0.2)
    initial: BusState = {"event": None, "summary": None, "reacted": False}
    final = await asyncio.wait_for(asyncio.to_thread(graph.invoke, initial), timeout=10.0)
    assert final["reacted"] is False
    assert final["event"] is None
    assert final["summary"] is None, "summarize must not run on the timeout branch"


def test_event_router_branches() -> None:
    """The router picks the event target iff the reacted channel is truthy."""
    route = event_router(on_event="summarize", on_timeout="give_up")
    assert route({"reacted": True}) == "summarize"
    assert route({"reacted": False}) == "give_up"
    assert route({}) == "give_up"


def test_event_router_honours_a_custom_key() -> None:
    """A custom reacted_key pairs the router with a re-keyed wait_node."""
    route = event_router(on_event="a", on_timeout="b", reacted_key="woke")
    assert route({"woke": True, "reacted": False}) == "a"
    assert route({"reacted": True}) == "b"
