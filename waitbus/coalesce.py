"""Coalesced backlog replay -- the opt-in client-side delivery mode.

Reconnect-time projection that collapses the replay backlog to the
latest event per entity (a "snapshot then live tail" in the Materialize
``SUBSCRIBE ... WITH SNAPSHOT`` shape), then switches to a faithful
live tail. Strictly client-side: composes over the existing
``await_predicate`` engine and the unchanged ``open_subscriber`` /
broadcast wire, with **zero server, schema or protocol change**.

Per-entity collapse is keyed on a stable upstream identity
(:func:`waitbus._terminal.entity_key`) and version-guarded by the
monotonic ULID :attr:`event_id` (lexicographic over the ULID alphabet).
A stale frame (an earlier ``event_id`` for the same entity) is
discarded; a later frame replaces the snapshot entry. This is the
``success → re-run → failure`` regression guard: re-run produces a
*newer* ``event_id``, so the newer state always wins regardless of
arrival order.

Sources without a stable entity key (the local watcher sources
``pytest`` / ``docker`` / ``fs``, the Alertmanager watchdog liveness
signal, and any GitHub / Alertmanager row missing its identity column)
are pass-through: kept in arrival (event_id) order, never collapsed.

Opt-in / not-default. Faithful ordered replay remains the STABLE
contract of ``open_subscriber`` and the existing ``waitbus replay``;
this module is reached only by ``waitbus replay --coalesce``, a
separate, explicitly-named, lossy-by-design delivery mode.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ._broadcast_sub import (
    BookmarkCursor,
    FrameDecision,
    SubscriberHandle,
    WaitOutcome,
    _emit_predicate,
    await_predicate,
)
from ._terminal import EntityKey, entity_key


def _event_id(frame: dict[str, Any]) -> str:
    """Return the frame's monotonic ULID cursor (lexicographic order).

    Raises ``ValueError`` on a missing or non-string ``event_id`` field.
    The wire contract guarantees every event-bearing frame the daemon emits
    carries a string ULID ``event_id`` (``broadcast._row_to_frame`` copies
    it from the events row); an empty or non-string id is the symptom of
    wire corruption or test scaffolding bypassing the daemon, NOT something
    to tolerate silently. The previous best-effort empty-string fallback
    silently sorted malformed frames to the front of the merged stream and
    silently lost version-guard comparisons; hard rejection surfaces the
    bug class at the call site.
    """
    eid = frame.get("event_id")
    if not isinstance(eid, str) or not eid:
        raise ValueError(f"coalesce: frame missing or non-string 'event_id' field: got {type(eid).__name__}={eid!r}")
    return eid


def coalesce_replay(
    sub: SubscriberHandle,
    *,
    emit: Callable[[dict[str, Any]], None],
    idle_seconds: float,
    cursor: BookmarkCursor | None = None,
    live_tail: bool = False,
) -> WaitOutcome:
    """Drain a faithful replay into a latest-per-entity snapshot; flush; tail.

    **The caller owns the socket lifecycle** (mirrors
    :func:`await_predicate`). This function does not close
    ``sub.sock`` — the caller is responsible for ``sub.sock.close()``
    after the function returns (typically in a ``finally`` block, see
    ``replay.py``'s ``_stream_coalesced`` for the canonical CLI
    shape).

    ``sub`` must be a :class:`SubscriberHandle` already returned by
    :func:`open_subscriber` (typically with ``since=<cursor>`` set so
    the daemon's replay strictly-after the bookmark is the backlog
    window being collapsed).

    Algorithm:

    1. **Accumulate.** Run :func:`await_predicate` in
       ``idle_reset=True`` with a predicate that **never matches**
       (always returns :attr:`FrameDecision.CONTINUE`), so the engine
       drains until the daemon's replay batch has been idle for
       ``idle_seconds``. Each frame either lands in ``snapshot``
       (entity-keyed, last-version-wins by event_id) or in
       ``passthrough`` (event_id-ordered list for sources without an
       entity key).
    2. **Flush.** Emit the merged result in ``event_id`` order: the
       collapsed snapshot entries (each at the event_id of its
       surviving frame) interleaved with the pass-through frames, so
       the emitted stream is monotonic in the cursor end-to-end. The
       bookmark advances **only** at flush (per emitted frame) -- not
       per accumulated frame -- so a crash mid-accumulation leaves the
       bookmark at the last *flushed* event_id and the next resume
       re-pulls the un-flushed remainder from the daemon.
    3. **Live tail (optional).** With ``live_tail=True``, switch to a
       faithful live tail by running :func:`await_predicate` again with
       ``deadline_seconds=None`` and the shared emit-then-CONTINUE
       predicate; live frames are NOT coalesced (coalescing applies
       only to the offline backlog window). The bookmark advances per
       live frame, exactly as ``await_predicate`` already drives it.
       Default is ``False`` (the only production caller —
       ``replay.py::_stream_coalesced`` — terminates after flush phase);
       a future long-lived blocking consumer opts in explicitly.

    ``WaitOutcome`` semantics mirror :func:`await_predicate`: a
    ``timed_out`` replay phase is the expected "replay caught up" terminus
    and is the outcome returned when ``live_tail=False``;
    ``peer_closed`` mid-backlog still flushes whatever was accumulated;
    ``cancelled`` (SIGINT during replay phase) discards the snapshot and
    propagates -- the bookmark is not advanced. With ``live_tail=True``
    the returned outcome is tail phase's (EOF / SIGINT / framing).
    """
    snapshot: dict[EntityKey, dict[str, Any]] = {}
    passthrough: list[dict[str, Any]] = []

    def _accumulate(frame: dict[str, Any]) -> FrameDecision:
        key = entity_key(frame)
        if key is None:
            passthrough.append(frame)
            return FrameDecision.CONTINUE
        prev = snapshot.get(key)
        if prev is None or _event_id(frame) > _event_id(prev):
            snapshot[key] = frame
        # else: stale (older event_id for the same entity) -- discard.
        return FrameDecision.CONTINUE

    replay_phase = await_predicate(
        sub,
        decide=_accumulate,
        deadline_seconds=idle_seconds,
        cursor=None,  # advance the bookmark only at flush, not per drained frame
        idle_reset=True,
    )

    if replay_phase.cancelled:
        # SIGINT during replay phase: flush nothing, propagate. The bookmark
        # is unchanged so the next resume re-pulls the entire window.
        return replay_phase

    # Merge snapshot + pass-through, globally sorted by event_id ASC so
    # the emitted stream is monotonic in the cursor.
    merged = sorted(
        list(snapshot.values()) + passthrough,
        key=_event_id,
    )
    # Phase-2 emit-then-advance is per-frame, not batch-atomic. The
    # invariant from CONSUMER_API §6: "the cursor advances only on
    # emitted frames." If emit() raises mid-merged-list, prior
    # successfully-emitted frames keep their advance and the failing
    # frame's predecessor is the resume point; on retry the daemon
    # replays strictly after the advanced cursor, re-collapsing the
    # remaining entities. There is NO consumer-side delivery_id dedup
    # wire on the emit path (emit_frame is plain stdout); the
    # idempotency guarantee is daemon-side via `INSERT OR IGNORE` on
    # the wire input, not on the consumer's output stream.
    #
    # Peer-closed partial-flush edge case: when replay phase returns
    # ``peer_closed`` mid-backlog (the branch below), the partial
    # snapshot flushes AND the cursor advances past entities whose
    # latest frame the daemon hadn't sent yet. The forward-only
    # version-guard makes this acceptable (a future resume can only
    # get the same-or-newer state, never a regression) but it is a
    # documented degradation, NOT a bug.
    #
    # Design note: this client-side coalesce is the documented STABLE
    # shape (CONSUMER_API.md §6). Pulsar TableView is a precedent that
    # ships the same separation in production
    # (client-library projection over an unchanged broker wire).
    for frame in merged:
        emit(frame)
        if cursor is not None:
            cursor.advance(frame)

    if replay_phase.peer_closed or not live_tail:
        return replay_phase

    # tail phase: the same handle continues into the live stream. The
    # replay phase already drained the backlog snapshot; the tail
    # streams live frames from where it left off.
    return await_predicate(
        sub,
        decide=_emit_predicate(emit),
        deadline_seconds=None,
        cursor=cursor,
        idle_reset=False,
    )
