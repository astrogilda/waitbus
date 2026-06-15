"""Framework-neutral harness for the agent-integration canary tests.

The "any agent" proof needs the same dance for every framework: stand up the
broadcast daemon, start a REAL framework agent subscribed to waitbus (via the
public SDK) with a deterministic FAKE model so it runs offline, emit one event,
and assert the agent OBSERVABLY reacted. This module centralises that dance so
each framework test collapses to "define ``start_agent`` (framework + fake
model wiring) and assert ``run.reacted()``".

CANARY scope: assertions live at the waitbus boundary — the event was DELIVERED
to the agent and the agent WOKE and handled it — never on the framework's
internal state machine. That keeps the tests robust to upstream framework API
churn: a red test means waitbus failed to deliver, or the framework changed its
public subscribe/interrupt entry point, not that some internal field was renamed.

The agent runs the SYNCHRONOUS SDK (``wait_for``/``subscribe``) so ``start_agent``
launches it on a worker thread and returns immediately; this harness owns the
daemon-registration barrier, the emit, and the thread join (deterministic
teardown for the strict ResourceWarning gate).
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from threading import Thread
from typing import Any

from tests._daemon_helpers import await_subscribers
from waitbus import broadcast


def insert_event_row(db: Path, delivery_id: str, **overrides: Any) -> None:
    """Insert one event row (rings the doorbell so the daemon fans it out).

    Defaults to a docker frame; override ``source`` / ``event_type`` / etc. to
    shape the triggering event. Shared by every agent-integration test.
    """
    from waitbus import _db
    from waitbus._types import EventInsert

    defaults: dict[str, Any] = {
        "source": "docker",
        "event_type": "docker_container",
        "owner": "local",
        "repo": "docker",
        "received_at": time.time_ns(),
        "payload_json": "{}",
        "ingest_method": "watcher",
    }
    defaults.update(overrides)
    with contextlib.closing(sqlite3.connect(str(db))) as conn:
        _db.insert_event(conn, EventInsert(delivery_id=delivery_id, **defaults))


@dataclass
class AgentRun:
    """A started, waitbus-subscribed framework agent + a view of its reaction.

    ``start_agent`` returns this: ``thread`` is the running agent (it ends when
    the agent has handled the event), ``reacted`` reports whether the agent
    observably processed the waitbus event, and ``detail`` carries any
    framework-specific captured state for the test to inspect at the waitbus
    boundary (e.g. the received event_type).
    """

    thread: Thread
    reacted: Callable[[], bool]
    detail: dict[str, Any] = field(default_factory=dict)


async def run_agent_and_emit(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    *,
    start_agent: Callable[[str], AgentRun],
    delivery_id: str = "agent-evt-1",
    event: dict[str, Any] | None = None,
    timeout: float = 5.0,
) -> AgentRun:
    """Start the agent, emit one event, and wait for it to react.

    Args:
        running_daemon: the ``running_daemon`` fixture's ``(daemon, paths)``.
        start_agent: given the broadcast socket path, start the framework agent
            (subscribed via the SDK, threaded) and return its :class:`AgentRun`.
        delivery_id: unique id for the emitted event row.
        event: ``insert_event_row`` overrides shaping the triggering event
            (defaults to a docker frame).
        timeout: seconds to wait for the agent thread to finish reacting.

    Returns the :class:`AgentRun` (the caller asserts ``run.reacted()`` and
    inspects ``run.detail``). Always joins the agent thread so its SDK socket is
    closed deterministically (strict ResourceWarning gate).
    """
    daemon, paths = running_daemon
    run = start_agent(str(paths["broadcast"]))
    try:
        await await_subscribers(daemon)
        insert_event_row(paths["db"], delivery_id, **(event or {}))
        deadline = time.monotonic() + timeout
        while run.thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
    finally:
        # The agent thread should have returned once it handled the event; join
        # so a hung agent surfaces as a test failure rather than a leaked socket.
        run.thread.join(timeout=2.0)
    return run
