"""Tests for the ULID generator."""

from __future__ import annotations

import datetime
import itertools
import re
import time
from collections.abc import Generator
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from hypothesis import given, settings
from hypothesis import strategies as st

from waitbus import _ulid

_CROCKFORD_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


@pytest.fixture(autouse=True)
def _reset_ulid_state() -> Generator[None, None, None]:
    """Each test starts with a clean monotonic state."""
    _ulid._reset_state_for_test()
    yield
    _ulid._reset_state_for_test()


# --- shape ------------------------------------------------------------------


def test_length_is_26() -> None:
    assert len(_ulid.new()) == _ulid.ULID_LEN == 26


def test_crockford_alphabet_only() -> None:
    """100 samples must all match the Crockford base32 character set."""
    for _ in range(100):
        assert _CROCKFORD_RE.match(_ulid.new())


def test_decode_timestamp_recovers_current_ms() -> None:
    """The embedded timestamp must round-trip within a 50-ms tolerance.

    Bracket the comparison against the same expression the generator uses
    internally (``time.monotonic_ns()//1M + _BOOT_WALL_MS``) rather than
    against ``time.time()``. The generator switched to monotonic-clock
    timestamps with a boot-time wall offset for NTP-step-back immunity;
    a wall-clock bracket can drift away from the recovered value if the
    OS runs an NTP correction between the test's ``time.time()`` calls
    and the generator's ``monotonic_ns()`` read in between. Bracketing
    on the generator's own clock keeps the bound diagnostic without any
    cross-clock flake risk.
    """
    boot_offset = _ulid._BOOT_WALL_MS
    before = time.monotonic_ns() // 1_000_000 + boot_offset
    ulid = _ulid.new()
    after = time.monotonic_ns() // 1_000_000 + boot_offset
    recovered = _ulid.decode_timestamp_ms(ulid)
    assert before - 50 <= recovered <= after + 50


def test_decode_rejects_wrong_length() -> None:
    with pytest.raises(ValueError, match="exactly 26 chars"):
        _ulid.decode_timestamp_ms("01HZ0")


def test_decode_rejects_invalid_char() -> None:
    # 'I' is excluded from Crockford base32; placed in the timestamp
    # portion (first 10 chars) since decode_timestamp_ms only inspects
    # those.
    with pytest.raises(ValueError, match="non-Crockford-base32 character"):
        _ulid.decode_timestamp_ms("I" + "0" * 25)


# --- monotonicity -----------------------------------------------------------


def test_strict_lexicographic_monotonicity_in_batch() -> None:
    """A back-to-back batch of 1000 ULIDs must be strictly increasing."""
    batch = [_ulid.new() for _ in range(1000)]
    assert batch == sorted(batch), "ULID batch is not lexicographically sorted"
    # Strict: no duplicates.
    assert len(set(batch)) == len(batch)


def test_overflow_within_a_ms_raises() -> None:
    """If the 80-bit random pool is exhausted within one millisecond, raise."""
    # Force the state to a fixed millisecond regardless of how long the test
    # takes. new() now calls time.monotonic_ns(), so we patch that and zero out
    # _BOOT_WALL_MS so the effective ms stays constant throughout the test.
    fixed_ms = 1_715_000_000_000
    with (
        patch.object(_ulid.time, "monotonic_ns", return_value=fixed_ms * 1_000_000),
        patch.object(_ulid, "_BOOT_WALL_MS", 0),
    ):
        # Prime the state: first call assigns a random suffix; force it
        # to MAX_RAND so the next call within the same ms must overflow.
        _ulid.new()
        with _ulid._LOCK:
            _ulid._LAST_MS = fixed_ms
            _ulid._LAST_RAND = _ulid._MAX_RAND
        with pytest.raises(RuntimeError, match="monotonic overflow"):
            _ulid.new()


# --- property-based ---------------------------------------------------------


@given(n=st.integers(min_value=2, max_value=500))
@settings(max_examples=25, deadline=2000)
def test_batch_of_any_size_is_strictly_increasing(n: int) -> None:
    """For any N in [2, 500], `new()` called N times produces strictly
    increasing 26-char ULIDs.
    """
    _ulid._reset_state_for_test()
    batch = [_ulid.new() for _ in range(n)]
    assert all(len(u) == 26 for u in batch)
    assert all(_CROCKFORD_RE.match(u) for u in batch)
    assert all(a < b for a, b in itertools.pairwise(batch)), "batch not strictly increasing"


@given(ms=st.integers(min_value=0, max_value=(1 << 48) - 1))
@settings(max_examples=100, deadline=500)
def test_decode_round_trips_for_any_valid_timestamp(ms: int) -> None:
    """Encode any 48-bit ms timestamp through the same path the generator
    uses, then decode; result must equal the input.
    """
    out: list[str] = []
    for shift in range(45, -5, -5):
        out.append(_ulid._ALPHABET[(ms >> shift) & 0x1F])
    # Pad with a valid 16-char random suffix so the string is well-formed.
    encoded_ts = "".join(out)
    suffix = "0" * 16
    assert _ulid.decode_timestamp_ms(encoded_ts + suffix) == ms


# --- NTP step-back regression -----------------------------------------------


def test_monotonic_under_time_step_back() -> None:
    """ULIDs remain lexicographically increasing when the wall clock steps back.

    freezegun patches time.time() but, when the waitbus module is listed in
    the ignore list, freezegun's _should_use_real_time() lets calls from within
    waitbus._ulid fall through to the real time.monotonic_ns(). This models
    an NTP step-back: the wall clock (and anything calling time.time() from
    test-level code) rewinds, but the waitbus generator continues advancing
    on the real monotonic clock.

    Also sanity-checks that decode_timestamp_ms() returns a value within a
    few seconds of the real wall-clock time at the point of generation,
    confirming the _BOOT_WALL_MS offset is in the right ballpark.
    """
    ulid_before = _ulid.new()

    # Capture wall time before freezing so we have a reference point.
    wall_ms_before = int(time.time() * 1000)

    # Simulate an NTP correction that rewinds the wall clock by 5 seconds.
    # ignore=['waitbus'] tells freezegun to let calls from the waitbus
    # package fall through to the real time functions, so time.monotonic_ns()
    # inside _ulid.new() sees real monotonic time even though time.time()
    # returns the frozen (rewound) value.
    frozen_wall = datetime.datetime.fromtimestamp(time.time() - 5, tz=datetime.UTC)
    with freeze_time(frozen_wall, ignore=["waitbus"]):
        ulid_after = _ulid.new()

        # Sanity check: decoded timestamp should be close to the real
        # (non-frozen) wall-clock time, not the frozen wall time.
        recovered_ms = _ulid.decode_timestamp_ms(ulid_after)
        # Allow a 10 s window to absorb scheduling jitter.
        assert abs(recovered_ms - wall_ms_before) < 10_000, (
            f"decoded timestamp {recovered_ms} too far from real time "
            f"{wall_ms_before} (diff={abs(recovered_ms - wall_ms_before)} ms)"
        )

    # The ULID generated under the frozen (stepped-back) wall clock must
    # sort after the one generated before the step-back.
    assert ulid_after > ulid_before, f"ULID ordering broken under NTP step-back: {ulid_before!r} >= {ulid_after!r}"
