"""Tool factories wiring a Pydantic AI agent to the waitbus event bus.

Two factories, both returning :class:`pydantic_ai.Tool` objects ready to
pass into ``Agent(tools=[...])``:

- :func:`wait_tool` -- the consumer side: a tool that blocks on the public
  :func:`waitbus.wait_for` until an event matches the configured predicate,
  then returns the delivered frame to the model as a plain dict.
- :func:`emit_tool` -- the producer side: a tool the model calls with a
  message string; it emits an addressed agent event onto the bus via the
  public :func:`waitbus.emit`.

The integration pattern mirrors the canonical example shipped in the
waitbus repository: register a tool that calls the blocking SDK, run the
agent with any model (a deterministic ``TestModel`` in the offline tests).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import msgspec
from pydantic_ai import Tool

from waitbus import EventFrame, emit, wait_for

# EventInsert is the write-shape struct the public emit() accepts; it is not
# re-exported at the waitbus package root yet, so this is the one private
# import in the adapter. The waitbus version band (>=0.1.0,<0.2) bounds drift.
from waitbus._types import EventInsert

_WAIT_DESCRIPTION = "Block until one matching event arrives on the waitbus event bus and return it."
_EMIT_DESCRIPTION = "Publish a message onto the waitbus event bus as an agent event."

_TIMED_OUT = "timed out waiting for a bus event"


def wait_tool(
    match: str | Sequence[str] | None = None,
    *,
    source: str | None = None,
    to: str | None = None,
    timeout: float | None = None,
    socket_path: str | None = None,
    on_event: Callable[[EventFrame], None] | None = None,
    name: str = "wait_for_bus_event",
    description: str | None = None,
) -> Tool[None]:
    """Build a tool that blocks on the bus until one event matches.

    The tool wraps the public :func:`waitbus.wait_for`: when the model calls
    it, the agent blocks until an event matches ``match`` / ``source`` / ``to``
    (or ``timeout`` elapses). On delivery the frame is handed to ``on_event``
    (an observability seam for callers that want the typed
    :class:`~waitbus.EventFrame`) and returned to the model as a JSON-able
    dict; on timeout the model sees a short timed-out string.

    Args:
        match: a waitbus match spec (``'fields.conclusion="failure"'``) or a
            sequence of specs AND-composed.
        source: restrict to one source (``"github"`` / ``"docker"`` / ...).
        to: addressed-messaging inbox filter (``fields.msg_to`` equals this).
        timeout: seconds to block; ``None`` blocks until a match.
        socket_path: broadcast socket override (test / multi-daemon seam).
        on_event: called with the delivered :class:`~waitbus.EventFrame`
            before the tool returns; never called on timeout.
        name: the tool name the model sees.
        description: the tool description the model sees.

    At least one of ``match`` / ``source`` / ``to`` is required; the wrapped
    SDK raises ``ValueError`` when the tool runs with none configured.
    """

    def _wait() -> str | dict[str, Any]:
        frame = wait_for(match, source=source, to=to, timeout=timeout, socket_path=socket_path)
        if frame is None:
            return _TIMED_OUT
        if on_event is not None:
            on_event(frame)
        built = msgspec.to_builtins(frame)
        assert isinstance(built, dict)  # EventFrame is a struct; its builtins form is a dict
        return built

    return Tool(_wait, name=name, description=description or _WAIT_DESCRIPTION)


def emit_tool(
    *,
    agent_name: str,
    event_type: str = "agent_message",
    to: str | None = None,
    db_path: Path | None = None,
    doorbell_path: Path | None = None,
    name: str = "emit_bus_event",
    description: str | None = None,
) -> Tool[None]:
    """Build a tool that publishes the model's message onto the bus.

    The tool wraps the public :func:`waitbus.emit` with the agent-event
    envelope the bus already carries for agent-originated rows: source
    ``"agent"``, synthetic ``owner`` / ``repo`` labels (those columns are
    required by the store but agent events are not repository-bound), the
    message body on ``msg_from`` / ``msg_body`` so it survives onto the lean
    wire frame, and a unique ``delivery_id`` prefixed with ``agent_name``.

    Args:
        agent_name: self-asserted sender name (an address, not a credential);
            lands on the frame's ``msg_from`` and prefixes the delivery id.
        event_type: event class for the row; defaults to ``"agent_message"``,
            the canonical addressed-message vocabulary of the ``agent`` source.
        to: optional recipient name (``msg_to``) for addressed delivery.
        db_path: events DB path override (defaults to the resolved location).
        doorbell_path: doorbell socket override (test / multi-daemon seam).
        name: the tool name the model sees.
        description: the tool description the model sees.
    """

    def _emit(message: str) -> str:
        result = emit(
            EventInsert(
                delivery_id=f"{agent_name}-{uuid.uuid4().hex}",
                source="agent",
                event_type=event_type,
                owner="local",
                repo="agents",
                received_at=time.time_ns(),
                payload_json="{}",
                ingest_method="api",
                msg_from=agent_name,
                msg_to=to,
                msg_body=message,
            ),
            db_path=db_path,
            doorbell_path=doorbell_path,
        )
        return f"emitted bus event {result.event.delivery_id}"

    return Tool(_emit, name=name, description=description or _EMIT_DESCRIPTION)
