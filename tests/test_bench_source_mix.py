"""Tests for ``benchmarks._bench_source_mix.pick_source_for_iter``.

The picker is the deterministic seed-source selector the v2 measurement
benches use to thread the per-iteration ``(source, event_type)`` pair
through their ``_emit_seed_event`` callsites, exercising the full
soak-taxonomy fan-out on the daemon instead of the narrow
``(agent, agent_message)`` path the benches had historically used.

Assertions on:

- Determinism: the same ``iter_id`` always yields the same pair.
- Registry-membership: every returned pair is in the
  ``SOAK_SOURCE_REGISTRY``.
- Distribution sanity: at N=10_000 the empirical shares track the
  registered ``default_mix_share`` values within a generous tolerance
  (the picker is a stable hash, not a real RNG; the tolerance protects
  against rounding-induced bucket drift, not statistical variance).
- Bench-iteration coverage: at N=50 (the v2 bench production posture)
  every registered source name lands at least once, so the daemon's
  per-source codepath is structurally reached.
"""

from __future__ import annotations

import importlib

import pytest

from benchmarks._bench_source_mix import pick_source_for_iter
from benchmarks._source_taxonomy import SOAK_SOURCE_REGISTRY


def test_pick_source_for_iter_is_deterministic() -> None:
    """Same ``iter_id`` always yields the same ``(source, event_type)`` pair."""
    for iter_id in (0, 1, 7, 42, 100, 1_000):
        first = pick_source_for_iter(iter_id)
        second = pick_source_for_iter(iter_id)
        assert first == second, f"iter_id={iter_id} non-deterministic: {first!r} vs {second!r}"


def test_pick_source_for_iter_returns_registered_pair() -> None:
    """Every returned pair is in the soak source registry."""
    registered_pairs = {(spec.name, spec.event_type) for spec in SOAK_SOURCE_REGISTRY}
    for iter_id in range(200):
        pair = pick_source_for_iter(iter_id)
        assert pair in registered_pairs, f"iter_id={iter_id} returned unregistered pair: {pair!r}"


def test_pick_source_for_iter_distribution_tracks_registered_shares() -> None:
    """At N=10_000 the empirical shares track the registered shares within 5%.

    The picker is a stable hash; statistical variance is zero across
    re-runs. The tolerance protects against rounding-induced drift in
    the cumulative-threshold table, not Monte-Carlo noise.
    """
    n = 10_000
    counts: dict[str, int] = {}
    for iter_id in range(n):
        name, _event_type = pick_source_for_iter(iter_id)
        counts[name] = counts.get(name, 0) + 1
    for spec in SOAK_SOURCE_REGISTRY:
        observed_share = counts.get(spec.name, 0) / n
        delta = abs(observed_share - spec.default_mix_share)
        assert delta < 0.05, (
            f"source={spec.name}: registered_share={spec.default_mix_share:.4f}, "
            f"observed_share={observed_share:.4f}, delta={delta:.4f} exceeds 0.05"
        )


def test_pick_source_for_iter_covers_full_taxonomy_at_n50() -> None:
    """At N=50 (the v2 bench production posture) every registered source name lands."""
    names_seen = {pick_source_for_iter(iter_id)[0] for iter_id in range(50)}
    registered_names = {spec.name for spec in SOAK_SOURCE_REGISTRY}
    assert names_seen == registered_names, (
        f"missing sources at N=50: {registered_names - names_seen}; extras: {names_seen - registered_names}"
    )


def test_pick_source_for_iter_is_invariant_under_registry_reorder(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reordering ``SOAK_SOURCE_REGISTRY`` in-place must not change any pick.

    The picker walks the registry sorted by ``spec.name``; the
    cumulative-bucket assignment is therefore invariant under any
    order the source-taxonomy module happens to declare. This test
    captures the picker output for a fixed iter_id set under the
    current order, monkeypatches the registry into reverse order,
    reloads ``_bench_source_mix`` to recompute the threshold table from
    the reordered tuple, and asserts every captured pair survives.
    """
    import benchmarks._bench_source_mix as v2_source_mix
    import benchmarks._source_taxonomy as taxonomy

    iter_ids = list(range(200))
    baseline = [v2_source_mix.pick_source_for_iter(i) for i in iter_ids]

    reversed_registry = tuple(reversed(taxonomy.SOAK_SOURCE_REGISTRY))
    monkeypatch.setattr(taxonomy, "SOAK_SOURCE_REGISTRY", reversed_registry)
    reloaded = importlib.reload(v2_source_mix)
    try:
        reordered = [reloaded.pick_source_for_iter(i) for i in iter_ids]
        assert reordered == baseline, (
            "pick_source_for_iter must be invariant under registry reorder; "
            f"first divergence at iter_id="
            f"{next(i for i, (a, b) in enumerate(zip(baseline, reordered, strict=True)) if a != b)}"
        )
    finally:
        # Restore module-level _THRESHOLDS table by reloading against
        # the original (un-monkeypatched) registry.
        importlib.reload(v2_source_mix)
