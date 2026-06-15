"""Shared source taxonomy for the soak harness and corpus generator.

Defines the four built-in sources that the soak and corpus tools know
about, together with the event type each source emits and the default
traffic share for mixed-source load tests.

This module is harness-internal: it lives in ``benchmarks/`` because the
``default_mix_share`` field is a load-test concept, not part of the
production plugin API.  Adding it to the production ``SourceSpec`` (in
``waitbus.sources._protocol``) would force a v2 entry-point group
— so the soak-side struct lives here instead.
"""

from __future__ import annotations

from typing import Final

import msgspec


class SoakSourceSpec(msgspec.Struct, kw_only=True, frozen=True):
    """Immutable descriptor for one source in the soak / corpus harness.

    ``name`` matches the canonical source name in
    ``waitbus.sources._registry``.  ``event_type`` is the primary
    event type the corpus generator assigns to events from this source.
    ``default_mix_share`` is the fraction of synthetic traffic routed to
    this source when no explicit ``--source-mix`` is given; values across
    all registry entries must sum to 1.0.
    """

    name: str
    event_type: str
    default_mix_share: float


SOAK_SOURCE_REGISTRY: Final[tuple[SoakSourceSpec, ...]] = (
    SoakSourceSpec(name="github", event_type="workflow_run", default_mix_share=0.40),
    SoakSourceSpec(name="pytest", event_type="pytest_session", default_mix_share=0.15),
    SoakSourceSpec(name="docker", event_type="docker_container", default_mix_share=0.15),
    SoakSourceSpec(name="fs", event_type="fs_change", default_mix_share=0.10),
    SoakSourceSpec(name="agent", event_type="agent_message", default_mix_share=0.20),
)


def _validate_share_sum() -> None:
    """Enforce the sum-to-1.0 invariant at module import time.

    The ``default_mix_share`` values are the prior on waitbus's launch-claim
    workload. They must sum to 1.0 because every emit path (the
    cumulative-distribution picker in ``scripts/soak/_emit.py`` and the
    Hawkes-process generator in ``benchmarks/gen_corpus.py``) assumes a
    proper probability distribution. A silent drift here would skew every
    synthetic-load measurement and trip ``per_source_share_threshold`` on
    every soak run with a non-obvious cause.
    """
    total = sum(spec.default_mix_share for spec in SOAK_SOURCE_REGISTRY)
    if abs(total - 1.0) > 1e-9:
        names_shares = ", ".join(f"{spec.name}={spec.default_mix_share}" for spec in SOAK_SOURCE_REGISTRY)
        raise AssertionError(f"SOAK_SOURCE_REGISTRY shares must sum to 1.0; got {total!r} from ({names_shares})")


_validate_share_sum()
