"""Runnable LangGraph + waitbus integration example.

A real :class:`langgraph.graph.StateGraph` whose first node blocks on the
waitbus SDK (``wait_for``) and whose second node "summarises" the received event
with a deterministic offline fake model. See :mod:`agent` for the graph.
"""

from __future__ import annotations

from examples.agent_langgraph.agent import AgentState, build_graph, run

__all__ = ["AgentState", "build_graph", "run"]
