"""Consistency test: soak taxonomy names must match production source registry.

Guards against drift where a soak-side source name diverges from the
canonical built-in name in ``waitbus.sources._registry``.  A
mismatch would mean the soak harness emits events with source values the
production daemon considers unknown —
"""

from __future__ import annotations

from benchmarks._source_taxonomy import SOAK_SOURCE_REGISTRY
from waitbus.sources._registry import _BUILTIN_SOURCES_RAW


def test_soak_registry_names_are_known_builtin_sources() -> None:
    """Every name in SOAK_SOURCE_REGISTRY must appear in the production built-in registry.

    The soak and corpus tools work exclusively with built-in sources; a
    name in SOAK_SOURCE_REGISTRY that is absent from _BUILTIN_SOURCES_RAW
    means harness events would arrive with an unrecognised source value.
    """
    builtin_names = {name for name, _ in _BUILTIN_SOURCES_RAW}
    soak_names = {spec.name for spec in SOAK_SOURCE_REGISTRY}
    unknown = soak_names - builtin_names
    assert not unknown, (
        f"SOAK_SOURCE_REGISTRY contains source name(s) not in the production "
        f"built-in registry: {sorted(unknown)!r}. "
        f"Known built-ins: {sorted(builtin_names)!r}."
    )
