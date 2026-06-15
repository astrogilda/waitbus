"""Soak event-emit helpers: synthetic + corpus-replay paths.

Imports ``_context`` for the accumulator + state structs.  Does not
import any other soak sibling; ``_verdict`` and ``_suspend`` depend on
the constants defined here but read them through their own imports
of this module.
"""

from __future__ import annotations

import bisect
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

from benchmarks._source_taxonomy import SOAK_SOURCE_REGISTRY
from scripts.soak._context import _SoakAccumulators, _SoakState
from waitbus import _emit as emit_mod
from waitbus._types import NS_PER_SECOND, EventInsert

#: Canonical source names in soak-emission order, derived from the shared
#: taxonomy so this file and benchmarks/gen_corpus.py stay in sync.
_SOURCES: tuple[str, ...] = tuple(s.name for s in SOAK_SOURCE_REGISTRY)

#: Primary event type each source emits, derived from the shared taxonomy.
_EVENT_TYPES: dict[str, str] = {s.name: s.event_type for s in SOAK_SOURCE_REGISTRY}

#: Default mixed-source share targets (sum to 1.0). The 40/15/15/10/20
#: split reflects the per-source emit weights documented in the soak
#: design decision and is the comparison target for
#: :func:`per_source_share_threshold`.
_DEFAULT_SOURCE_MIX: dict[str, float] = {s.name: s.default_mix_share for s in SOAK_SOURCE_REGISTRY}

#: Source picker is the cumulative-distribution form of ``_DEFAULT_SOURCE_MIX``
#: so a deterministic seed-driven walk through ``[0, 1)`` lands on each
#: source at exactly the 40/15/15/10/20 target frequencies. Round-robin
#: would land at uniform shares, which trips ``per_source_share_threshold``
#: on every synthetic-emit soak (a per-source-share imbalance bug surfaced
#: during a fault-injection baseline run). The Hawkes generator used
#: by ``--corpus`` already produces the right distribution; this CDF
#: brings the no-corpus path into agreement.
#
# Pre-sorted by cumulative fraction so bisect_right gives O(log n) lookup.
_SOURCE_CDF_VALUES: tuple[float, ...] = tuple(
    sum(_DEFAULT_SOURCE_MIX[s] for s in _SOURCES[: idx + 1]) for idx in range(len(_SOURCES))
)


def _pick_weighted_source(i: int) -> str:
    """Return the source whose CDF bucket contains the i-th deterministic step.

    Uses ``i / golden_ratio`` mod 1 (additive recurrence with the irrational
    golden ratio) to land each step on a different sub-interval of [0, 1),
    giving exact long-run convergence to the target mix without RNG state.
    The first 100 picks already match the target shape within 1 percentage
    point per class -- well inside the ``per_source_share_threshold``
    tolerance.
    """
    # Golden-ratio additive recurrence: Knuth TAOCP Vol 3 section 6.4
    # (deterministic, low-discrepancy, no RNG state needed).
    u = (i * 0.6180339887498949) % 1.0
    idx = bisect.bisect_right(_SOURCE_CDF_VALUES, u)
    return _SOURCES[min(idx, len(_SOURCES) - 1)]


def _build_event_insert(
    source: str,
    *,
    owner: str = "soak",
    repo: str = "waitbus",
    delivery_id: str,
    ingest_method: str,
    payload_json: str = "{}",
) -> EventInsert:
    """Build an ``EventInsert`` from the shared fields common to all soak emit paths.

    Centralises the ``EventInsert`` construction so ``_emit_one`` and
    ``_emit_corpus_event`` share the same field shape.  Each caller still
    owns its own ``delivery_id`` namespace and ``ingest_method`` tag --
    the divergence between the two paths is intentional and documented
    below.
    """
    return EventInsert(
        delivery_id=delivery_id,
        source=source,
        event_type=_EVENT_TYPES.get(source, "unknown"),
        owner=owner,
        repo=repo,
        received_at=time.time_ns(),
        payload_json=payload_json,
        ingest_method=ingest_method,
        status="completed",
        conclusion="success",
    )


# Divergence between _emit_one (synthetic) and _emit_corpus_event (corpus replay)
# is intentional: delivery_id namespace, ingest_method tag, payload_json source differ.


def _emit_one(db_path: Path, i: int) -> str:
    """Emit one synthetic event weighted to match _DEFAULT_SOURCE_MIX. Returns the source."""
    source = _pick_weighted_source(i)
    emit_mod.emit_batch(
        [_build_event_insert(source, delivery_id=f"soak:{source}:{i}-{time.time_ns()}", ingest_method="soak")],
        db_path=db_path,
    )
    return source


def _emit_corpus_event(
    db_path: Path,
    event: dict[str, Any] | None,
    i: int,
    *,
    state: _SoakState,
    accums: _SoakAccumulators,
) -> tuple[int, str]:
    """Emit one corpus event. Returns the corpus event's inter_arrival_ns.

    ``event`` is a pre-parsed dict from ``replay_corpus`` (which yields
    ``dict | None``).  A ``None`` value signals a parse failure in the
    corpus reader; the function falls back to a synthetic emit so a
    malformed line does not abort a 24-hour soak run.

    Fall-through to synthetic emit on parse failure is logged once per
    run via ``state.json_decode_warned``; the total count is surfaced in
    ``accums.corpus_decode_fallthroughs`` so the verdict-doc includes it.
    """
    if event is None:
        if not state.json_decode_warned:
            sys.stderr.write(
                "[soak] corpus line failed to parse as JSON; falling back to synthetic "
                "emit for this and any subsequent malformed lines. "
                "(Warning emitted once per soak run.)\n"
            )
            state.json_decode_warned = True
        accums.corpus_decode_fallthroughs += 1
        source = _emit_one(db_path, i)
        return 0, source
    # Validate the source at the corpus boundary rather than at the
    # downstream accumulator.  A plugin-registered source name (which
    # would pass EventInsert.__post_init__'s registry validator) still
    # falls outside the soak's hardcoded ``_SOURCES`` 4-tuple and would
    # corrupt the per-source-share verdict if accumulated.  Fall back to
    # the same weighted picker ``_emit_one`` uses (``_pick_weighted_source``)
    # so an unknown corpus source becomes a synthetic event that respects
    # the per-source-share target distribution -- a round-robin fallback
    # would skew toward 25/25/25/25 and trip ``per_source_share_threshold``
    # on any soak whose corpus carries unknown source names.
    # Rationale: validate untrusted values at the boundary (loud-fail on a
    # malformed config; reject an unrecognised source rather than coercing it).
    raw_source = event.get("source", _pick_weighted_source(i))
    source = raw_source if raw_source in _SOURCES else _pick_weighted_source(i)
    payload = event.get("payload", {})
    raw_inter = event.get("inter_arrival_ns", 0)
    # Boundary coercion: a malformed corpus value (str, None, bool,
    # float-inf, float-nan, negative) must NOT abort a 24-hour run.
    # Catch all four non-int classes: TypeError (None/list/dict),
    # ValueError ("abc"), OverflowError (inf), explicit non-finite
    # guard for nan.  Fail-soft to 0 and clamp negatives to 0
    # (monotonic timestamps).  Surfaced by tests/test_corpus_property.py.
    try:
        if isinstance(raw_inter, float) and not math.isfinite(raw_inter):
            inter_arrival_ns = 0
        else:
            inter_arrival_ns = max(int(raw_inter), 0)
    except (TypeError, ValueError, OverflowError):
        inter_arrival_ns = 0
    # Boundary fallback: ``.get(key, default)`` returns the present-but-falsy
    # value (e.g. ``""``) when the key exists with that value, NOT the
    # default. Use ``or`` so an empty/missing delivery_id collapses to a
    # unique synthetic id -- without this, two corpus lines with empty
    # ``delivery_id`` collide under INSERT OR IGNORE and the second
    # event silently drops. Surfaced by tests/test_corpus_property.py.
    emit_mod.emit_batch(
        [
            _build_event_insert(
                source,
                delivery_id=event.get("delivery_id") or f"soak-corpus:{i}-{time.time_ns()}",
                owner=event.get("owner") or "soak",
                repo=event.get("repo") or "waitbus",
                ingest_method=event.get("ingest_method") or "corpus_replay",
                payload_json=json.dumps(payload),
            )
        ],
        db_path=db_path,
    )
    return inter_arrival_ns, source


# ``NS_PER_SECOND`` is re-exported for the ``_main`` orchestrator's
# ``--preserve-timing`` arithmetic; importing it here keeps the canonical
# constant accessible through the same module that owns the corpus
# inter-arrival logic.
__all__ = [
    "NS_PER_SECOND",
    "_DEFAULT_SOURCE_MIX",
    "_EVENT_TYPES",
    "_SOURCES",
    "_SOURCE_CDF_VALUES",
    "_build_event_insert",
    "_emit_corpus_event",
    "_emit_one",
    "_pick_weighted_source",
]
