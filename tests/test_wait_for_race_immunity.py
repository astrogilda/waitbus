"""``wait_for(since=anchor)`` race-immunity contract.

The waitbus SDK's ``wait_for`` accepts a ``since`` cursor (a ULID
``event_id``) so a subscriber that registers AFTER an event of
interest was emitted still receives it via the daemon's seq-replay
window. This test asserts that contract directly against a real
broadcast daemon (no mocks) under a parametrised registration
delay: events emitted between the anchor and the subscribe call
must be delivered.

The test exists so a regression on the waitbus SDK side of the
race-immune subscribe path -- e.g. a refactor that drops the
``since`` plumbing or changes its semantics -- surfaces in CI
cheaply, before it lands as a phantom subscribe race in production.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path

import pytest

from waitbus import broadcast, wait_for
from waitbus._emit import emit
from waitbus._types import EventInsert


def _emit_event(*, owner: str, db_path: Path, doorbell_path: Path) -> str:
    """Emit one ``agent_message`` event with the given owner; return its event_id."""
    result = emit(
        EventInsert(
            delivery_id=f"race-immunity-{uuid.uuid4()}",
            source="agent",
            event_type="agent_message",
            owner=owner,
            repo="race-immunity-test",
            received_at=time.time_ns(),
            payload_json='{"kind": "race_immunity_event"}',
            ingest_method="race-immunity-test",
        ),
        db_path=db_path,
        doorbell_path=doorbell_path,
    )
    return result.event.event_id


@pytest.mark.asyncio
@pytest.mark.parametrize("delay_s", [0.1, 0.5, 1.0])
async def test_wait_for_since_replays_event_emitted_during_registration_delay(
    delay_s: float,
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """An event emitted AFTER the anchor but BEFORE subscribe registers is replayed.

    Sequence:
    1. Mint an anchor event; capture its event_id.
    2. Emit one "interesting" event with a unique owner (no live
       subscriber sees it via fan-out).
    3. Sleep ``delay_s`` to simulate the spawn-and-import latency a
       real driver subprocess pays before it reaches ``wait_for``.
    4. Call ``wait_for(match=owner-equal, since=anchor_event_id)`` on
       a worker thread (the SDK is synchronous).
    5. Assert the matched ``EventFrame`` is the interesting event.

    A regression that dropped ``since`` from the wire envelope or
    truncated the replay window would surface as a ``None`` return
    (subscribe-from-live, the interesting event already past).
    """
    _daemon, paths = running_daemon
    owner = f"race-immune-{uuid.uuid4().hex[:12]}"

    # Mint the anchor, then the event the wait_for must replay.
    anchor_event_id = _emit_event(owner=f"anchor:{owner}", db_path=paths["db"], doorbell_path=paths["doorbell"])
    interesting_event_id = _emit_event(owner=owner, db_path=paths["db"], doorbell_path=paths["doorbell"])

    # Simulate subprocess spawn + cold-import delay.
    await asyncio.sleep(delay_s)

    # Run the synchronous wait_for on a worker thread so the daemon's
    # asyncio loop keeps servicing socket I/O.
    def _run_wait_for() -> object:
        return wait_for(
            [f'fields.owner="{owner}"'],
            source=None,
            timeout=5.0,
            socket_path=str(paths["broadcast"]),
            since=anchor_event_id,
        )

    frame = await asyncio.get_running_loop().run_in_executor(None, _run_wait_for)
    from waitbus._frame import EventFrame

    assert isinstance(frame, EventFrame), f"wait_for(since={anchor_event_id!r}) returned None after a {delay_s}s delay"
    # The matched frame is the interesting event the daemon replayed
    # over the seq-cursor window.
    assert frame.event_id == interesting_event_id
    assert frame.owner == owner


@pytest.mark.asyncio
async def test_wait_for_without_since_misses_event_emitted_before_subscribe(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """Without ``since``, an event emitted before subscribe is NOT delivered.

    The complementary contract to the race-immunity test: a subscribe
    without a replay cursor reads from the live watermark only. This
    pins the WHY: the bench/stress orchestrators mint an anchor and
    thread it through driver subprocesses precisely because the live
    path would miss a seed the driver was not yet subscribed for.
    """
    _daemon, paths = running_daemon
    owner = f"no-since-{uuid.uuid4().hex[:12]}"

    # Emit the event BEFORE the subscribe registers.
    _emit_event(owner=owner, db_path=paths["db"], doorbell_path=paths["doorbell"])
    await asyncio.sleep(0.1)

    # Subscribe with NO since cursor; the event lands before the
    # subscribe socket connects, so the live fan-out path skips it.
    def _run_wait_for() -> object:
        return wait_for(
            [f'fields.owner="{owner}"'],
            source=None,
            timeout=1.0,  # Short timeout -- the event will never arrive.
            socket_path=str(paths["broadcast"]),
        )

    frame = await asyncio.get_running_loop().run_in_executor(None, _run_wait_for)

    # Timeout-with-None confirms the live path missed the event the
    # since-cursor would have replayed.
    assert frame is None
