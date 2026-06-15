"""Property-based tests for ULID invariants in waitbus._ulid.

Covers monotonicity, format, and timestamp encoding via Hypothesis.
"""

from __future__ import annotations

import itertools
import re
from collections.abc import Generator

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from waitbus import _ulid

_CROCKFORD_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


@pytest.fixture(autouse=True)
def _reset_ulid() -> Generator[None, None, None]:
    """Each test starts with a clean monotonic state."""
    _ulid._reset_state_for_test()
    yield
    _ulid._reset_state_for_test()


# --- format properties -------------------------------------------------------


@given(n=st.integers(min_value=1, max_value=200))
@settings(max_examples=40, deadline=2000)
def test_ulid_format_always_matches_crockford_re(n: int) -> None:
    """Every ULID generated in a batch of n must match the Crockford-base32 pattern."""
    _ulid._reset_state_for_test()
    batch = [_ulid.new() for _ in range(n)]
    assert all(len(u) == 26 for u in batch)
    assert all(_CROCKFORD_RE.match(u) for u in batch), "ULID contains non-Crockford character"


# --- monotonicity properties -------------------------------------------------


@given(n=st.integers(min_value=2, max_value=300))
@settings(max_examples=40, deadline=2000)
def test_successive_ulids_are_strictly_increasing(n: int) -> None:
    """Successive _ulid.new() calls within one process must be strictly increasing."""
    _ulid._reset_state_for_test()
    batch = [_ulid.new() for _ in range(n)]
    assert all(a < b for a, b in itertools.pairwise(batch)), "ULID sequence is not strictly increasing"


# --- integer encoding property -----------------------------------------------


@given(ms=st.integers(min_value=0, max_value=(1 << 48) - 1))
@settings(max_examples=100, deadline=500)
def test_ulid_encodes_non_negative_integer(ms: int) -> None:
    """The ULID timestamp portion encodes a non-negative 48-bit integer."""
    # Build an encoded timestamp string the same way _ulid.new() does.
    out: list[str] = []
    for shift in range(45, -5, -5):
        out.append(_ulid._ALPHABET[(ms >> shift) & 0x1F])
    encoded_ts = "".join(out)
    suffix = "0" * 16
    recovered = _ulid.decode_timestamp_ms(encoded_ts + suffix)
    assert recovered >= 0
    assert recovered == ms
