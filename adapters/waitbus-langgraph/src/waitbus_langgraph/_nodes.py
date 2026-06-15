"""Node and router factories wiring a LangGraph graph to the waitbus bus.

Two factories:

- :func:`wait_node` -- a graph node that blocks on the public
  :func:`waitbus.wait_for` until an event matches the configured predicate
  (or the timeout elapses) and writes the outcome into graph state.
- :func:`event_router` -- a conditional-edge router that branches on
  whether :func:`wait_node` captured an event or timed out.

The integration pattern mirrors the canonical example shipped in the
waitbus repository: a plain blocking node that subscribes via the SDK,
plus a routing edge so the graph reacts differently to delivery vs
timeout.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

import msgspec

from waitbus import wait_for


class StateNode(Protocol):
    """A graph-node callable as LangGraph's typed ``add_node`` expects it.

    The parameter is named ``state`` because LangGraph's node protocol
    matches the callable on that name; it is typed ``Any`` (not a concrete
    mapping) so the factory's node binds against ANY caller-defined state
    schema -- LangGraph bounds its node-input type variable to
    TypedDict-like / dataclass-like / pydantic schemas the adapter cannot
    name for the caller.
    """

    def __call__(self, state: Any) -> dict[str, Any]: ...


class StateRouter(Protocol):
    """A conditional-edge router callable, same ``state``-name contract."""

    def __call__(self, state: Any) -> str: ...


def wait_node(
    match: str | Sequence[str] | None = None,
    *,
    source: str | None = None,
    to: str | None = None,
    timeout: float | None = None,
    socket_path: str | None = None,
    event_key: str = "event",
    reacted_key: str = "reacted",
) -> StateNode:
    """Build a graph node that blocks until one bus event matches.

    The returned callable is ready for ``builder.add_node(...)``. When the
    graph reaches it, execution blocks in the public :func:`waitbus.wait_for`
    until an event matches ``match`` / ``source`` / ``to`` or ``timeout``
    elapses. The node then writes two state channels: ``event_key`` carries
    the delivered frame as a JSON-able dict (``None`` on timeout) and
    ``reacted_key`` flips ``True`` on delivery (``False`` on timeout) --
    the boolean :func:`event_router` branches on.

    Args:
        match: a waitbus match spec (``'fields.conclusion="failure"'``) or a
            sequence of specs AND-composed.
        source: restrict to one source (``"github"`` / ``"docker"`` / ...).
        to: addressed-messaging inbox filter (``fields.msg_to`` equals this).
        timeout: seconds to block; ``None`` blocks until a match.
        socket_path: broadcast socket override (test / multi-daemon seam).
        event_key: state channel for the delivered frame dict.
        reacted_key: state channel for the delivered-vs-timeout boolean.
    """

    def _wait(state: Mapping[str, Any]) -> dict[str, Any]:
        # Takes ``state`` positionally (the LangGraph node contract) but reads
        # none of it -- the wait is configured by the closed-over factory
        # arguments. The parameter is named ``state`` (not ``_state``)
        # deliberately: LangGraph's ``add_node`` typed overload matches the
        # node callable on that name, so a rename breaks strict type checking.
        frame = wait_for(match, source=source, to=to, timeout=timeout, socket_path=socket_path)
        if frame is None:
            return {event_key: None, reacted_key: False}
        return {event_key: msgspec.to_builtins(frame), reacted_key: True}

    return _wait


def event_router(
    *,
    on_event: str,
    on_timeout: str,
    reacted_key: str = "reacted",
) -> StateRouter:
    """Build a conditional-edge router branching on the wait outcome.

    For use with ``builder.add_conditional_edges(...)`` after a
    :func:`wait_node`: returns ``on_event`` (a node name) when the wait
    captured an event, ``on_timeout`` (a node name or ``END``) when it
    timed out.

    Args:
        on_event: target when ``reacted_key`` is truthy in state.
        on_timeout: target when it is falsy or absent.
        reacted_key: state channel written by the paired :func:`wait_node`.
    """

    def _route(state: Mapping[str, Any]) -> str:
        return on_event if state.get(reacted_key) else on_timeout

    return _route
