"""Pure-stdlib ULID generator (Crockford base32, monotonic within a ms).

Layout per the ULID spec (https://github.com/ulid/spec):
  - 26 chars total
  - First 10 chars  = 48-bit millisecond timestamp (5 bits per char)
  - Last 16 chars   = 80-bit cryptographically random suffix

Monotonicity: when `new()` is called multiple times within the same
millisecond, the random suffix is incremented by 1 instead of being
redrawn so the lexicographic ordering of ULIDs matches their insertion
order. On the (vanishingly rare) overflow of the 80-bit suffix within
a single millisecond, the call raises RuntimeError rather than silently
wrapping.

Clock source: `time.monotonic_ns()` is used rather than `time.time()`.
`time.time()` is wall-clock based and can step backward during an NTP
correction, producing ULIDs that sort before their predecessors and
breaking any cursor-based query that relies on lexicographic monotonicity.
`time.monotonic_ns()` is NTP-immune. A boot-time offset (_BOOT_WALL_MS)
is added so that `decode_timestamp_ms()` still returns approximate
epoch-millis; the decoded value drifts by at most the monotonic clock's
rate skew over the process lifetime (typically <1 s/day).
"""

from __future__ import annotations

import secrets
import threading
import time as time

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"  # Crockford base32 (no I, L, O, U)
_LOCK = threading.Lock()
_LAST_MS: int = 0
_LAST_RAND: int = 0

# Captured once at module load. `time.monotonic_ns()` is NTP-immune; we
# add the boot-time wall-clock offset back so decode_timestamp_ms()
# still returns approximate epoch-millis (drift is bounded by the
# monotonic-clock rate skew over process lifetime, typically <1 s/day).
#
# Design decision: capturing the offset here (module load) rather than
# lazily inside new() is intentional. The test-isolation contract lives
# in _reset_state_for_test.__doc__; monkeypatch this constant to shift
# the apparent epoch in tests.
_BOOT_WALL_MS: int = int(time.time() * 1000) - time.monotonic_ns() // 1_000_000

ULID_LEN = 26
_MAX_RAND = (1 << 80) - 1


def new() -> str:
    """Return a fresh 26-char ULID. Thread-safe and monotonic within a ms."""
    global _LAST_MS, _LAST_RAND
    with _LOCK:
        ms = time.monotonic_ns() // 1_000_000 + _BOOT_WALL_MS
        if ms == _LAST_MS:
            rand = _LAST_RAND + 1
            if rand > _MAX_RAND:
                msg = "ULID monotonic overflow within a millisecond — system clock jump or pathological event rate"
                raise RuntimeError(msg)
        else:
            rand = int.from_bytes(secrets.token_bytes(10), "big")
        _LAST_MS = ms
        _LAST_RAND = rand

    out: list[str] = []
    # 48-bit timestamp -> 10 chars. Shifts walk down from bit 45 to bit 0
    # in 5-bit steps; the top 2 bits of the 50-bit encoding are always 0
    # because the timestamp is 48 bits.
    for shift in range(45, -5, -5):
        out.append(_ALPHABET[(ms >> shift) & 0x1F])
    # 80-bit randomness -> 16 chars.
    for shift in range(75, -5, -5):
        out.append(_ALPHABET[(rand >> shift) & 0x1F])
    return "".join(out)


def decode_timestamp_ms(ulid_str: str) -> int:
    """Recover the millisecond timestamp embedded in a ULID's first 10 chars."""
    if len(ulid_str) != ULID_LEN:
        msg = f"ULID must be exactly {ULID_LEN} chars; got {len(ulid_str)}"
        raise ValueError(msg)
    ms = 0
    for ch in ulid_str[:10]:
        idx = _ALPHABET.find(ch)
        if idx < 0:
            msg = f"ULID contains non-Crockford-base32 character: {ch!r}"
            raise ValueError(msg)
        ms = (ms << 5) | idx
    return ms


def _reset_state_for_test() -> None:
    """Internal: zero the monotonic state so tests can drive overflow paths.

    NOT public API. Tests call this between scenarios to avoid carrying
    state across; production code must never touch it.

    To simulate a different wall-clock epoch in tests, patch the module-level
    constant directly::

        monkeypatch.setattr("waitbus._ulid._BOOT_WALL_MS", desired_ms_offset)
    """
    global _LAST_MS, _LAST_RAND
    with _LOCK:
        _LAST_MS = 0
        _LAST_RAND = 0
