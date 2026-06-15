"""Shared duration-string parser for the CLI verbs.

A standalone, stdlib-only module so any verb (``wait``, ``on``, ``top``,
``db prune``) can parse ``--timeout`` / ``--max-age`` style durations without
importing another verb's heavier module graph. ``wait`` and ``db prune``
previously each carried their own copy for exactly that import-cost reason; a
dedicated light module removes the duplication without reintroducing the
heavy-import problem.
"""

from __future__ import annotations

_UNITS = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def parse_duration(raw: str) -> float:
    """Parse a duration with an optional unit suffix into seconds.

    Accepts a bare number (seconds) or one of ``s`` / ``m`` / ``h`` / ``d``.
    Plain ``int`` / ``float`` strings are treated as seconds so ``30`` and
    ``30s`` are equivalent.

    Raises:
        ValueError: the value is unparseable or non-positive.
    """
    text = raw.strip().lower()
    if not text:
        raise ValueError("duration must be non-empty")
    suffix = text[-1]
    value = float(text[:-1]) * _UNITS[suffix] if suffix in _UNITS else float(text)
    if value <= 0:
        raise ValueError(f"duration must be positive, got {raw!r}")
    return value
