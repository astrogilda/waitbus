"""Replay-anchor emit shared by the measurement benches and the stress controller.

A measurement bench (or the ``waitbus stress`` controller) emits a sentinel
"anchor" event immediately BEFORE spawning its agent-driver subscribers, then
hands each driver a ``since=<anchor.event_id>`` replay cursor. A driver whose
``wait_for`` subscribe registers after the producer's seed lands (cold-import
jitter, scheduler contention) still receives the seed, because the daemon's
seq-replay walks every row with ``seq > anchor`` matching the driver's filters.

The anchor owner is namespaced ``anchor:<seed_scope_id>`` so the driver's
owner-equal predicate (``fields.owner == "<seed_scope_id>"``) does NOT match the
anchor frame itself -- only the real seed (which uses the bare scope as
``owner``) wakes the drivers.

This module lives in ``benchmarks/`` because the sanctioned dependency direction
is ``scripts -> benchmarks`` (the soak/stress harnesses already consume
``benchmarks._bench_source_mix`` and ``benchmarks._harness``). Keeping the anchor
here lets each caller stamp its own ``repo`` / ``ingest_method`` /
``delivery_id_prefix`` provenance instead of inheriting one caller's hardcoded
identity.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

# The seed and every driver reaction ride the same registered
# ``(source, event_type)`` pair: the built-in ``agent`` source's
# ``agent_message`` event type (registered in
# ``waitbus.sources._registry``). Per-run isolation comes from the
# ``owner`` field -- the orchestrator mints a per-window scope id and threads it
# through every driver as the predicate's owner clause. ``agent_message``
# (rather than a fresh per-run event_type) is structurally required: the
# daemon's ``_fan_out`` skips any frame whose event_type is not in
# ``event_types_supported()``, so an unregistered event_type would never reach
# the bus.
SEED_SOURCE = "agent"
SEED_EVENT_TYPE = "agent_message"


def emit_anchor_event(
    *,
    seed_scope_id: str,
    db_path: Path,
    doorbell_path: Path,
    repo: str,
    ingest_method: str,
    delivery_id_prefix: str,
    source: str = SEED_SOURCE,
    event_type: str = SEED_EVENT_TYPE,
) -> str:
    """Emit one sentinel anchor event; return its daemon-assigned ``event_id``.

    The caller forwards the returned ``event_id`` to its spawn factory as the
    driver-side ``since`` cursor. ``repo`` / ``ingest_method`` /
    ``delivery_id_prefix`` stamp the caller's provenance onto the anchor row so a
    bench's anchor is not mis-attributed to the stress controller (or vice
    versa).
    """
    from waitbus._emit import emit
    from waitbus._types import EventInsert

    delivery_id = f"{delivery_id_prefix}:{uuid.uuid4()}"
    result = emit(
        EventInsert(
            delivery_id=delivery_id,
            source=source,
            event_type=event_type,
            owner=f"anchor:{seed_scope_id}",
            repo=repo,
            received_at=time.time_ns(),
            payload_json='{"kind": "since_anchor"}',
            ingest_method=ingest_method,
        ),
        db_path=db_path,
        doorbell_path=doorbell_path,
    )
    return result.event.event_id


__all__ = ["SEED_EVENT_TYPE", "SEED_SOURCE", "emit_anchor_event"]
