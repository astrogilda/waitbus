"""A real LangGraph agent that reacts to a waitbus event — fully offline.

The graph is a genuine :class:`langgraph.graph.StateGraph` with two nodes:

1. ``wait_on_waitbus`` — calls the waitbus SDK's blocking :func:`waitbus.wait_for`
   against the broadcast daemon and writes the received event into graph state.
   This node is the real waitbus integration: it subscribes to the bus and blocks
   until an event arrives.
2. ``summarize`` — feeds the event into a LangChain chat model and stores the
   reply. In tests (and in this offline example) that model is a
   :class:`~langchain_core.language_models.fake_chat_models.FakeListChatModel`,
   so the graph runs deterministically with no network or LLM calls. Swap in a
   real ``ChatAnthropic``/``ChatOpenAI`` for a live agent — the waitbus wiring is
   unchanged.

``build_graph`` compiles the graph and ``run`` invokes it.
"""

from __future__ import annotations

from typing import Any, TypedDict

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langgraph.graph import END, START, StateGraph

from waitbus import wait_for


class AgentState(TypedDict, total=False):
    """Channels the graph reads/writes.

    ``event_type`` and ``summary`` start unset and are populated by the two
    nodes; ``reacted`` flips ``True`` once ``wait_on_waitbus`` captures an event
    (vs. timing out), giving callers a single boolean to assert on.
    """

    event_type: str | None
    summary: str | None
    reacted: bool


def _default_model(event_type: str) -> BaseChatModel:
    """Deterministic offline stand-in for a real chat model.

    Returns a fake model preloaded with exactly the reply we want, so the
    ``summarize`` node is reproducible and never touches the network. Replace
    this with a real provider client for a live agent.
    """
    return FakeListChatModel(responses=[f"A waitbus '{event_type}' event arrived; acting on it."])


def build_graph(
    socket_path: str,
    *,
    timeout: float = 5.0,
    model: BaseChatModel | None = None,
) -> Any:
    """Build and compile the LangGraph agent.

    Args:
        socket_path: path to the waitbus broadcast daemon's listener socket.
        timeout: seconds the ``wait_on_waitbus`` node blocks before giving up.
        model: optional chat model for the ``summarize`` node; defaults to a
            deterministic offline fake model so the graph runs without an LLM.

    Returns the compiled graph (call ``.invoke(initial_state)``).
    """

    def wait_on_waitbus(state: AgentState) -> dict[str, Any]:
        """Block on the waitbus SDK and record the event we woke on.

        Takes ``state`` positionally (the LangGraph node contract) but reads none
        of it -- it waits on the closed-over ``socket_path`` / ``timeout``. The
        parameter is named ``state`` (not ``_state``) deliberately: LangGraph's
        ``add_node`` typed overload matches the node callable on that name, so a
        rename to ``_state`` breaks ``mypy --strict`` (overload resolution).
        """
        frame = wait_for(
            'fields.event_type="docker_container"',
            source="docker",
            timeout=timeout,
            socket_path=socket_path,
        )
        if frame is None:
            return {"event_type": None, "reacted": False}
        return {"event_type": frame.event_type, "reacted": True}

    def summarize(state: AgentState) -> dict[str, Any]:
        """Summarise the received event with the (fake, offline) chat model."""
        if not state.get("reacted"):
            return {"summary": None}
        event_type = state.get("event_type") or "unknown"
        chat = model if model is not None else _default_model(event_type)
        reply = chat.invoke(f"Summarise this CI event: {event_type}")
        content = reply.content
        text = content if isinstance(content, str) else str(content)
        return {"summary": text}

    builder: StateGraph[AgentState] = StateGraph(AgentState)
    builder.add_node("wait_on_waitbus", wait_on_waitbus)
    builder.add_node("summarize", summarize)
    builder.add_edge(START, "wait_on_waitbus")
    builder.add_edge("wait_on_waitbus", "summarize")
    builder.add_edge("summarize", END)
    return builder.compile()


def run(socket_path: str, *, timeout: float = 5.0) -> AgentState:
    """Compile + invoke the graph against ``socket_path`` and return final state."""
    graph = build_graph(socket_path, timeout=timeout)
    initial: AgentState = {"event_type": None, "summary": None, "reacted": False}
    result: AgentState = graph.invoke(initial)
    return result
