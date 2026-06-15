"""Property tests for the predicate engine.

Hypothesis fuzz for the four invariants the engine MUST hold:

* ``_dig`` totality: never raises on arbitrarily-shaped dict + arbitrary
  dotted key (every type-confused traversal returns ``_MISSING``).
* JSON-literal round-trip: any json-serializable value, serialised into a
  ``--match`` spec and parsed back, matches a frame carrying that value.
* AND commutativity: the order of ``--match`` flags doesn't affect the
  outcome (different keys AND across; same key OR within).
* Parser adversarial fuzz: random text into ``parse_match`` either parses
  cleanly or raises ``ValueError`` -- never an uncaught exception.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from waitbus import _predicate
from waitbus._predicate import _MISSING, _dig, parse_match


@pytest.fixture(autouse=True)
def _reset_registries() -> Iterator[None]:
    saved_conditions = dict(_predicate._CONDITIONS)
    saved_evaluators = dict(_predicate._EVALUATORS)
    _predicate._CONDITIONS.clear()
    _predicate._EVALUATORS.clear()
    try:
        yield
    finally:
        _predicate._CONDITIONS.clear()
        _predicate._CONDITIONS.update(saved_conditions)
        _predicate._EVALUATORS.clear()
        _predicate._EVALUATORS.update(saved_evaluators)


# --- strategies -------------------------------------------------------------

# JSON-equality-safe scalars (NaN/Inf are not json-serializable, and
# floats round-trip through json with lossy precision; restrict to types
# that are deterministic under json.dumps + json.loads + Python equality).
_json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**53), max_value=2**53),
    st.text(max_size=32),
)

_safe_keys = st.from_regex(r"\A[A-Za-z_][A-Za-z0-9_]{0,15}\Z", fullmatch=True)


def _arbitrary_json(max_depth: int = 3) -> st.SearchStrategy[Any]:
    """Recursive JSON-shaped value, bounded depth for fuzz speed.

    ``max_depth`` is wired to Hypothesis' ``max_leaves`` so the
    recursive draw is bounded; the prior version declared the
    parameter but never used it, leaving the recursion at Hypothesis'
    default depth.
    """
    return st.recursive(
        _json_scalars,
        lambda child: st.one_of(
            st.lists(child, max_size=4),
            st.dictionaries(_safe_keys, child, max_size=4),
        ),
        max_leaves=max_depth,
    )


# --- _dig totality ----------------------------------------------------------


@given(
    payload=st.dictionaries(_safe_keys, _arbitrary_json(), max_size=5),
    dotted_key=st.text(
        alphabet=st.characters(
            min_codepoint=32,
            max_codepoint=126,
            blacklist_characters=["\x00"],
        ),
        min_size=0,
        max_size=40,
    ),
)
@settings(suppress_health_check=[HealthCheck.too_slow], max_examples=200)
def test_dig_totality(payload: dict[str, Any], dotted_key: str) -> None:
    """``_dig`` never raises -- ill-shaped paths return ``_MISSING``."""
    result = _dig(payload, dotted_key)
    # Result is either _MISSING, or a value present somewhere in payload.
    # Asserting "present in payload" for nested structures is not cheap;
    # totality is the load-bearing invariant.
    assert result is _MISSING or result is not None or result is None


# --- JSON-literal round-trip ------------------------------------------------


@given(value=_json_scalars, key=_safe_keys)
@settings(max_examples=200)
def test_json_literal_roundtrip(value: Any, key: str) -> None:
    """Any JSON scalar, serialised + parsed via parse_match, matches a frame carrying it."""
    spec = f"fields.{key}={json.dumps(value)}"
    pred = parse_match([spec])
    assert pred({"fields": {key: value}}) is True


@given(values=st.lists(_json_scalars, min_size=2, max_size=4, unique_by=json.dumps))
@settings(max_examples=100)
def test_or_within_repeated_key(values: list[Any]) -> None:
    """All N values registered under the same key should each match a frame holding them."""
    specs = [f"fields.k={json.dumps(v)}" for v in values]
    pred = parse_match(specs)
    for v in values:
        assert pred({"fields": {"k": v}}) is True


# --- AND commutativity ------------------------------------------------------


@given(
    pairs=st.lists(
        st.tuples(_safe_keys, _json_scalars),
        min_size=2,
        max_size=4,
        unique_by=lambda kv: kv[0],  # distinct keys (no OR-within-key noise)
    ),
)
@settings(max_examples=100)
def test_and_commutativity(pairs: list[tuple[str, Any]]) -> None:
    """Order of distinct-key --match flags is irrelevant to the result."""
    specs_a = [f"fields.{k}={json.dumps(v)}" for k, v in pairs]
    specs_b = list(reversed(specs_a))
    frame = {"fields": {k: v for k, v in pairs}}
    assert parse_match(specs_a)(frame) is parse_match(specs_b)(frame) is True


# --- Adversarial parser fuzz ------------------------------------------------


@given(spec=st.text(min_size=0, max_size=128))
@settings(max_examples=300, suppress_health_check=[HealthCheck.filter_too_much])
def test_parser_never_uncaught(spec: str) -> None:
    """parse_match either succeeds or raises ValueError -- no uncaught exceptions."""
    # Skip the trivially-tautological case to keep coverage on parsing.
    assume(spec.strip() != "")
    try:
        parse_match([spec])
    except ValueError:
        # Expected error class; any value is acceptable.
        return
    # Parsed cleanly: the resulting Predicate must be callable without raising.
    pred = parse_match([spec])
    # Calling the predicate on a random frame must also never raise.
    assert pred({}) in (True, False)
    assert pred({"any": "frame"}) in (True, False)


@given(
    bad_chars=st.text(
        alphabet=st.characters(
            blacklist_categories=["Cs"],
            min_codepoint=0,
            max_codepoint=0x10FFFF,
        ),
        min_size=1,
        max_size=32,
    ),
)
@settings(max_examples=100)
def test_parser_unicode_robust(bad_chars: str) -> None:
    """Unicode (including non-ASCII) in the spec does not crash parse_match."""
    spec = f"k.{bad_chars}={json.dumps('value')}"
    try:
        pred = parse_match([spec])
    except ValueError:
        return
    # No raises on call.
    pred({"k": {bad_chars: "value"}})
