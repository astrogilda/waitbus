"""Minimal pytest suite for the waitbus-demo repository.

Three test shapes the demo exercises:

- A parametrized pass test (proves the pytest_emit plugin reports
  per-parameter outcomes correctly).
- An always-passing smoke (control sample).
- An intentionally-failing test (proves waitbus observes pytest events
  with conclusion=failure).
"""

import pytest


@pytest.mark.parametrize("payload", ["alpha", "beta", "gamma"])
def test_parametrized_passes(payload: str) -> None:
    """Parametrized smoke; each case passes."""
    assert isinstance(payload, str)
    assert len(payload) >= 4


def test_smoke_always_passes() -> None:
    """Single passing test, separate from the parametrized cell."""
    assert 1 + 1 == 2


def test_intentional_fail() -> None:
    """Deterministic failure; demonstrates waitbus sees failed pytest events.

    Uses ``raise AssertionError(...)`` rather than ``assert False`` so the
    failure survives ``python -O`` (which strips ``assert`` statements).
    The whole point of the demo is that the failure is visible to waitbus's
    pytest-event emitter; an asserts-stripped path would make this test
    silently pass and break the demo.
    """
    raise AssertionError("intentional failure to demonstrate waitbus pytest event emission")
