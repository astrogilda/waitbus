"""Deterministic weighted source-mix picker for v2 bench seeds.

The v2 measurement benches historically emitted every per-iteration seed
event on the single ``(agent, agent_message)`` pair, structurally
exercising only one of the five built-in source taxonomies registered
in ``waitbus.sources._registry`` (github 0.40 / pytest 0.15 /
docker 0.15 / fs 0.10 / agent 0.20 by share). The daemon's fan-out
path was therefore never reached by the benches' representative load
on the github / pytest / docker / fs taxonomies, biasing every v2
verdict's latency / CPU / wakeup-distribution figures toward the
narrow `agent_message` codepath.

This module supplies a deterministic blake2b-based picker keyed on the
iteration id so a re-run reproduces the same source per iteration
(stable across hosts and across Python invocations; PYTHONHASHSEED is
not load-bearing here). The bench's per-iteration loop calls
:func:`pick_source_for_iter` once and threads the result through the
seed-emit callsite, so the daemon's fan-out is exercised with the
full registered taxonomy at the benches' representative load.

Reads the cumulative shares directly from
``benchmarks._source_taxonomy.SOAK_SOURCE_REGISTRY`` so the bench's
mix stays in sync with the soak generator's mix without a parallel
constants table to drift against.

Order-independence contract:
:func:`_cumulative_thresholds` walks ``SOAK_SOURCE_REGISTRY`` sorted by
``spec.name``, so the cumulative-bucket assignment is invariant under
any reordering of the registry tuple. A future commit reordering
``SOAK_SOURCE_REGISTRY`` for any reason (alphabetical sort,
deterministic-id refactor, plugin-priority shuffle) cannot silently
remap a historical ``iter_id`` to a different source. The same
``iter_id`` always picks the same source from the order-independence
commit onward regardless of how the registry happens to be declared.
"""

from __future__ import annotations

import hashlib
from typing import Final

from benchmarks._source_taxonomy import SOAK_SOURCE_REGISTRY

# Resolution used for the cumulative-distribution rounding. 1_000_000
# is enough headroom for the 5 registered shares (largest fractional
# step is 0.40 = 400_000); a larger denominator would invite floating-
# point drift without buying coverage.
_PICKER_RESOLUTION: Final[int] = 1_000_000


def _cumulative_thresholds() -> tuple[tuple[int, str, str], ...]:
    """Return ``((threshold, source_name, event_type), ...)`` for the picker.

    Each tuple's ``threshold`` is the upper bound (exclusive) for a
    blake2b reduction modulo ``_PICKER_RESOLUTION``; the picker walks
    the tuples in order and returns the first entry whose threshold
    exceeds the reduction. The final entry's threshold equals
    ``_PICKER_RESOLUTION`` so every reduction lands on a real entry.

    The walk is over ``SOAK_SOURCE_REGISTRY`` **sorted by name** so the
    cumulative-bucket assignment is order-independent: reordering the
    registry tuple in ``_source_taxonomy`` (alphabetical sort,
    deterministic-id refactor, plugin-priority shuffle) does NOT remap
    any historical ``iter_id`` to a different source. The same iter_id
    always picks the same source regardless of declaration order.
    """
    sorted_registry = sorted(SOAK_SOURCE_REGISTRY, key=lambda spec: spec.name)
    out: list[tuple[int, str, str]] = []
    running = 0.0
    for spec in sorted_registry:
        running += spec.default_mix_share
        threshold = round(running * _PICKER_RESOLUTION)
        out.append((threshold, spec.name, spec.event_type))
    # The cumulative shares sum to 1.0 by the
    # ``_source_taxonomy._validate_share_sum`` invariant, so the final
    # threshold equals ``_PICKER_RESOLUTION`` modulo rounding. Pin it
    # explicitly so a rounding-induced gap at the upper boundary cannot
    # leak past the last entry.
    if out:
        last_threshold, last_name, last_event_type = out[-1]
        if last_threshold < _PICKER_RESOLUTION:
            out[-1] = (_PICKER_RESOLUTION, last_name, last_event_type)
    return tuple(out)


_THRESHOLDS: Final[tuple[tuple[int, str, str], ...]] = _cumulative_thresholds()


def pick_source_for_iter(iter_id: int) -> tuple[str, str]:
    """Return ``(source_name, event_type)`` for the given iteration id.

    Deterministic: the same ``iter_id`` always yields the same pair,
    across hosts and across Python invocations. The blake2b digest is
    stable on every platform; the modulo-``_PICKER_RESOLUTION``
    reduction maps it into the registry's cumulative-share buckets.
    """
    digest = hashlib.blake2b(str(int(iter_id)).encode("ascii"), digest_size=8).digest()
    value = int.from_bytes(digest, "big") % _PICKER_RESOLUTION
    for threshold, source_name, event_type in _THRESHOLDS:
        if value < threshold:
            return source_name, event_type
    # Defensive fallback: the final threshold equals _PICKER_RESOLUTION
    # so the loop above always returns; this raise is unreachable
    # unless the registry is empty (which the soak-taxonomy invariant
    # rejects at import time).
    raise RuntimeError("pick_source_for_iter: no entry matched; SOAK_SOURCE_REGISTRY is empty")


__all__ = ["pick_source_for_iter"]
