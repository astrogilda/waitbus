"""A Pydantic AI agent that subscribes to waitbus and reacts to one event.

Uses a genuine :class:`pydantic_ai.Agent` whose LLM is replaced by
:class:`pydantic_ai.models.test.TestModel` so the whole thing runs offline
with no network calls. The model is a deterministic stand-in; the waitbus
integration is real — the agent's ``wait_for_waitbus_event`` tool calls the
public waitbus SDK (:func:`waitbus.wait_for`) to block on the
workstation-local event bus and records the delivered
:class:`~waitbus._frame.EventFrame` where a caller can observe it.

The same pattern applies to any framework: build an agent, register a tool
that calls ``wait_for(...)`` (the waitbus subscribe primitive), and run it
with the framework's deterministic fake model.

``TestModel`` drives the agent graph and, by default, calls each registered
tool exactly once, making the example deterministic and LLM-free.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.test import TestModel

from waitbus import wait_for


@dataclass
class EventCapture:
    """Observable sink for the event the agent's tool received from waitbus.

    The agent tool appends the delivered event's salient fields here so a
    caller (e.g. the canary test) can assert, at the waitbus boundary, that the
    event was DELIVERED and the agent WOKE — without reaching into Pydantic
    AI's internal state.
    """

    events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def reacted(self) -> bool:
        """True once the agent's tool has captured at least one waitbus event."""
        return bool(self.events)


@dataclass
class WaitbusDeps:
    """Run-scoped dependencies passed into the agent's tool.

    Carries the broadcast socket path to subscribe against, the wait timeout,
    and the :class:`EventCapture` sink the tool writes the delivered event to.
    """

    socket_path: str
    timeout: float
    capture: EventCapture


def build_agent() -> Agent[WaitbusDeps, str]:
    """Build the Pydantic AI agent wired to waitbus via a real subscribe tool.

    The agent uses :class:`TestModel` (no LLM, no network) and exposes a single
    tool, ``wait_for_waitbus_event``, that blocks on :func:`waitbus.wait_for`
    and records the delivered event into the run's :class:`EventCapture`.
    """
    agent: Agent[WaitbusDeps, str] = Agent(
        model=TestModel(),
        deps_type=WaitbusDeps,
        system_prompt=(
            "You react to CI / Docker / filesystem events from the waitbus "
            "event bus. Call wait_for_waitbus_event to block until one arrives."
        ),
    )

    @agent.tool
    def wait_for_waitbus_event(ctx: RunContext[WaitbusDeps]) -> str:
        """Block on the waitbus bus for one docker_container event and record it.

        Calls the real waitbus SDK ``wait_for`` against the run's broadcast
        socket. On delivery, the event's salient fields are appended to the
        run's :class:`EventCapture`; the returned string is what the (fake)
        model sees as the tool result.
        """
        deps = ctx.deps
        frame = wait_for(
            'fields.event_type="docker_container"',
            source="docker",
            timeout=deps.timeout,
            socket_path=deps.socket_path,
        )
        if frame is None:
            return "timed out waiting for waitbus event"
        deps.capture.events.append(
            {
                "event_id": frame.event_id,
                "event_type": frame.event_type,
                "owner": frame.owner,
                "repo": frame.repo,
                "delivery_id": frame.delivery_id,
            }
        )
        return f"received waitbus event: {frame.event_type}"

    return agent


def run(socket_path: str, *, timeout: float = 5.0) -> EventCapture:
    """Run the agent once against ``socket_path`` and return its capture.

    Standalone entry point: build the agent, run it synchronously (the fake
    model fires the waitbus-wait tool once), and return the :class:`EventCapture`
    so the caller can inspect what waitbus delivered. Requires the daemon to be
    running and an event to be emitted while the agent blocks.
    """
    capture = EventCapture()
    agent = build_agent()
    deps = WaitbusDeps(socket_path=socket_path, timeout=timeout, capture=capture)
    agent.run_sync("Wait for the next waitbus event.", deps=deps)
    return capture
