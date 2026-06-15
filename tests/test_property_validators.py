"""Property-based tests for subscribe-frame validators in broadcast.py.

Covers _validate_subscribe_filters, _validate_subscribe_event_types,
_validate_since_cursor, and _validate_subscribe_token using Hypothesis.
"""

from __future__ import annotations

import string

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from waitbus import broadcast
from waitbus.sources._registry import event_types_supported

# Crockford base32: digits + uppercase minus I, L, O, U
_CROCKFORD_CHARS = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

_valid_name = st.text(alphabet=string.ascii_letters + string.digits + "_.-", min_size=1, max_size=40)
_valid_filter = st.one_of(
    st.just("*"),
    st.builds(lambda o, r: f"{o}/{r}", _valid_name, _valid_name),
    st.builds(lambda o: f"{o}/*", _valid_name),
)


# --- _validate_subscribe_filters -------------------------------------------


@given(filters=st.lists(_valid_filter, min_size=1, max_size=broadcast.MAX_FILTERS_PER_SUBSCRIBER))
@settings(max_examples=80, deadline=1000)
def test_filters_valid_list_passes(filters: list[str]) -> None:
    """Any list of FILTER_RE-matching strings up to the cap must pass."""
    result = broadcast._validate_subscribe_filters(filters)
    assert isinstance(result, list)
    assert all(isinstance(f, str) for f in result)


_OVER_CAP_MIN = broadcast.MAX_FILTERS_PER_SUBSCRIBER + 1
_OVER_CAP_MAX = broadcast.MAX_FILTERS_PER_SUBSCRIBER + 50


@given(filters=st.lists(_valid_filter, min_size=_OVER_CAP_MIN, max_size=_OVER_CAP_MAX))
@settings(max_examples=30, deadline=1000)
def test_filters_over_cap_raises(filters: list[str]) -> None:
    """A list exceeding MAX_FILTERS_PER_SUBSCRIBER must raise ValueError."""
    with pytest.raises(ValueError, match="too many filters"):
        broadcast._validate_subscribe_filters(filters)


_SHELL_METACHAR = st.sampled_from(list(";|&$`!{}[]()<>~#\\\"'"))


@given(
    owner=_valid_name,
    repo=_valid_name,
    meta=_SHELL_METACHAR,
)
@settings(max_examples=60, deadline=1000)
def test_filters_shell_metachar_raises(owner: str, repo: str, meta: str) -> None:
    """A filter element containing a shell metacharacter must raise ValueError."""
    bad = f"{owner}{meta}/{repo}"
    with pytest.raises(ValueError):
        broadcast._validate_subscribe_filters([bad])


@given(filters=st.lists(_valid_filter, min_size=1, max_size=20))
@settings(max_examples=50, deadline=1000)
def test_filters_idempotent(filters: list[str]) -> None:
    """Re-validating the output of a successful call must also pass."""
    first = broadcast._validate_subscribe_filters(filters)
    second = broadcast._validate_subscribe_filters(first)
    assert first == second


# --- _validate_subscribe_event_types ---------------------------------------


@given(subset=st.frozensets(st.sampled_from(sorted(event_types_supported())), min_size=1))
@settings(max_examples=50, deadline=500)
def test_event_types_valid_subset_passes(subset: frozenset[str]) -> None:
    """Any non-empty subset of event_types_supported() must pass."""
    result = broadcast._validate_subscribe_event_types(list(subset))
    assert result <= event_types_supported()
    assert result == subset


@given(
    subset=st.frozensets(st.sampled_from(sorted(event_types_supported())), min_size=1),
    extra=st.text(min_size=1, max_size=30).filter(lambda s: s not in event_types_supported()),
)
@settings(max_examples=50, deadline=500)
def test_event_types_extra_unknown_filtered_not_raised(subset: frozenset[str], extra: str) -> None:
    """Unknown event types are silently filtered; result is still the valid subset."""
    result = broadcast._validate_subscribe_event_types([*list(subset), extra])
    assert result == subset


@given(
    types=st.lists(
        st.text(min_size=1, max_size=30).filter(lambda s: s not in event_types_supported()),
        min_size=1,
        max_size=10,
    )
)
@settings(max_examples=40, deadline=500)
def test_event_types_all_unknown_raises(types: list[str]) -> None:
    """A list with zero recognized types must raise ValueError."""
    with pytest.raises(ValueError, match="zero recognized"):
        broadcast._validate_subscribe_event_types(types)


# --- _validate_since_cursor ------------------------------------------------


@given(ulid=st.text(alphabet=_CROCKFORD_CHARS, min_size=26, max_size=26))
@settings(max_examples=80, deadline=500)
def test_since_cursor_valid_ulid_passes(ulid: str) -> None:
    """Any 26-char Crockford-base32 string must pass."""
    result = broadcast._validate_since_cursor(ulid)
    assert result == ulid


@given(ulid=st.text(alphabet=_CROCKFORD_CHARS, min_size=0, max_size=50).filter(lambda s: len(s) != 26))
@settings(max_examples=60, deadline=500)
def test_since_cursor_wrong_length_raises(ulid: str) -> None:
    """A string not exactly 26 chars must raise ValueError."""
    with pytest.raises(ValueError):
        broadcast._validate_since_cursor(ulid)


@given(
    prefix=st.text(alphabet=_CROCKFORD_CHARS, min_size=0, max_size=25),
    bad_char=st.sampled_from(list("ILOUilou")),
)
@settings(max_examples=60, deadline=500)
def test_since_cursor_invalid_char_raises(prefix: str, bad_char: str) -> None:
    """A 26-char string containing I, L, O, or U (excluded from Crockford) must raise."""
    candidate = (prefix + bad_char + "0" * 26)[:26]
    if len(candidate) == 26:
        with pytest.raises(ValueError):
            broadcast._validate_since_cursor(candidate)


# --- _validate_subscribe_token --------------------------------------------


@given(
    token=st.text(
        alphabet=st.characters(
            whitelist_categories=("Ll", "Lu", "Nd"),
            whitelist_characters=string.punctuation,
        ),
        min_size=broadcast.TOKEN_MIN_LEN,
        max_size=broadcast.TOKEN_MAX_LEN,
    ).filter(lambda s: s.isascii())
)
@settings(max_examples=80, deadline=500)
def test_token_valid_length_passes(token: str) -> None:
    """Any printable-ASCII token in [TOKEN_MIN_LEN, TOKEN_MAX_LEN] must pass."""
    result = broadcast._validate_subscribe_token(token)
    assert result == token


@given(token=st.text(min_size=0, max_size=broadcast.TOKEN_MIN_LEN - 1))
@settings(max_examples=50, deadline=500)
def test_token_too_short_raises(token: str) -> None:
    """Tokens shorter than TOKEN_MIN_LEN must raise ValueError."""
    with pytest.raises(ValueError):
        broadcast._validate_subscribe_token(token)


@given(token=st.text(min_size=broadcast.TOKEN_MAX_LEN + 1, max_size=broadcast.TOKEN_MAX_LEN + 50))
@settings(max_examples=50, deadline=500)
def test_token_too_long_raises(token: str) -> None:
    """Tokens longer than TOKEN_MAX_LEN must raise ValueError."""
    with pytest.raises(ValueError):
        broadcast._validate_subscribe_token(token)
