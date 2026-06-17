# waitbus Robustness Tests

This document tracks correctness and consistency issues in the broadcast
daemon that are covered by the robustness test suite. Each entry records
the defect, when it was fixed, and a reproduction command that verifies
the fix is still in place. The format follows the etcd robustness
track-record pattern: evidence that the test infrastructure can
reproduce a known failure class is as important as the fix itself.

All file:line references were verified against the source at the time
of writing; they are navigation aids, not part of any stability
contract.

---

## Robustness vs Soak

The project separates two distinct longevity layers.

**Robustness** is the correctness and consistency layer. The primary
fixtures live in `tests/test_broadcast_robustness.py` (a Hypothesis
`RuleBasedStateMachine` exercising the subscriber-lifecycle drain paths
under arbitrary rule sequences), `tests/test_broadcast.py`,
`tests/test_broadcast_exception_mapping.py`, and the other property
suites under `tests/test_*_property.py`. They construct a `Broadcast`
instance in-process (no subprocess, no network), drive concurrent
client traffic, and assert daemon-state invariants and wire-frame
contracts. Hypothesis shrinks any failure to a minimal reproducer.
This layer runs in CI on every push and completes in seconds.

**Soak** is the longevity layer. Scripts under `scripts/soak/` spin up
a real daemon on a Hetzner VPS, drive synthetic load for 24 hours, and
record a pass/fail verdict against eight resource-drift and
suspend-recovery thresholds. A clean soak run is a signal that the project is ready for a release; it is not a substitute for the correctness layer.
See `docs/SOAK_TEST.md` for the how-to.

The two layers are kept deliberately separate: the correctness layer
runs in CI on every push and proves invariants in seconds, while the
longevity layer runs occasionally on dedicated hardware and proves
resource stability over a continuous-uptime window. Neither substitutes
for the other.

---

## Robustness track record

| Correctness / Consistency issue | Fixed | Fix commit | Last reproduction commit | Reproduction script |
| --- | --- | --- | --- | --- |
| Subscribe-rejected exception class drift: a non-`version` reject reason defaulted to a wrong auth-specific client exception instead of the base `BroadcastConnectionError`, mislabeling lag drops (that auth exception was later removed with the broadcast token) | May 2026 | `bd78daf` | `bd78daf` from May 2026 | `uv run pytest tests/test_broadcast_exception_mapping.py -v` |
| Replay wrong-key-pop subscriber leak and `SUBSCRIBER_COUNT` double-decrement: lag eviction popped fd `-1` after socket close, leaking the real map entry; a subsequent `_fan_out` close double-decremented the gauge | May 2026 | `f148f29` | `f148f29` from May 2026 | `uv run pytest tests/test_broadcast.py::test_replay_lag_eviction_closes_subscriber_with_real_fd tests/test_broadcast_robustness.py -v` |
| Replay `sqlite3.Error` task crash and subscriber leak: a DB fault during `_replay` crashed the `_read_subscribe` task silently and left the subscriber registered with no wire close | May 2026 | `f148f29` | `f148f29` from May 2026 | `uv run pytest tests/test_broadcast.py -k test_close_subscriber tests/test_broadcast_robustness.py -v` |

---

## Maintaining bug reproducibility during non-trivial changes

When making large changes to the broadcast daemon, the subscription
path, or the test helpers, confirm that the test
suite can still detect the failure classes listed above. The track
record table documents known defects, and each row names a
reproduction command.

**Best practices:**

- **Establish baseline.** Before starting a large non-trivial change,
  run the reproduction commands for every row in the track record
  table and confirm they pass.
- **Verify reproducibility.** After completing the change, re-run each
  reproduction command. A test that silently stops covering its target
  defect is as harmful as a regression.
- **Update tracking.** Refresh the "Last reproduction commit" column
  with the commit hash and month to confirm the current framework
  version works.
- **Update commands.** If a change renames a test, moves a file, or
  changes the invocation shape, update the reproduction commands in
  the table before closing the change.
- **Gate completion.** Consider the change incomplete until all
  reproduction commands run green.

This ensures that improvements to the daemon or to the test
infrastructure do not inadvertently reduce the ability to detect known
failure modes.

---

## Execution model

Robustness tests drive the `Broadcast` class directly in-process. There
is no subprocess boundary and no operator-visible Unix socket listener.
The daemon's synchronous fan-out and close-subscriber methods are
exercised against `_FakeSock` stubs (`tests/test_broadcast_robustness.py`)
or against a real socket pair (`tests/test_broadcast.py`). The
sock-level seam is the only mechanism that can deterministically reach
the new branches added by the recent subscribe-reject and replay-error
work, because the alternative (running the daemon as a subprocess)
cannot be monkeypatched across the process boundary.

The typical pattern in `tests/test_broadcast_robustness.py`:

1. **Daemon construction.** A `Broadcast` instance is created with a
   temporary SQLite database under `tmp_path`.
2. **Rules.** Hypothesis generates sequences of rules: `subscribe_normal`,
   `subscribe_with_pre_ack_buffer`, `trigger_lag_via_fan_out`,
   `close_subscriber_*` (one rule per close reason),
   `trigger_pre_ack_frame_overflow`, `trigger_pre_ack_byte_overflow`,
   `double_close_is_idempotent`.
3. **Invariants.** After every rule the machine asserts daemon-state
   consistency: `SUBSCRIBER_COUNT` gauge equal to `len(daemon.subscribers)`;
   no `fd=-1` key in the subscriber map; pre-ack buffer within
   `PRE_ACK_BUFFER_FRAMES` and `PRE_ACK_BUFFER_BYTES`; every wire reject
   reason in the consumer-facing taxonomy from `_REJECT_REASON_EXCEPTIONS`;
   non-lag close reasons emit no reject frame.
4. **Shrinking.** On a counterexample Hypothesis shrinks the rule
   sequence to the smallest set that still triggers the failure.

State-machine tests carve out of the project's per-test 100 ms budget
by Hypothesis framework default (`deadline=None` on
`RuleBasedStateMachine.TestCase`). The typical run completes in roughly
ten to twenty seconds for fifty explored examples.

---

## Key concepts

### Exception taxonomy and `_REJECT_REASON_EXCEPTIONS`

The reference client (`waitbus/_broadcast_sub.py`) maps each
`subscribe_rejected` wire reason to a typed Python exception. The
mapping is `_REJECT_REASON_EXCEPTIONS`:

| Wire `reason` | Client exception |
|---|---|
| `version` | `ProtocolVersionError` |
| `lag_limit_exceeded` | `SubscriberLaggedError` |

Both are subclasses of `BroadcastConnectionError`. An unknown future
reason falls to the base class rather than defaulting to either specific
exception, which would mislabel (for example) a lag drop as a version
failure -- the first defect in the track record.

### `_TERMINAL_REJECT_FRAMES` and the wire-close contract

Not every internal close reason emits a frame. `_TERMINAL_REJECT_FRAMES`
in `waitbus/broadcast.py` maps internal reasons to pre-encoded
reject frames. Only three internal reasons appear in the map:
`lag_limit_exceeded`, `heartbeat_lag`, and `replay_lag_limit_exceeded`
-- all three encode the same consumer-facing
`{"reason":"lag_limit_exceeded"}` frame because the consumer's recovery
is identical in every case. Internal faults such as `replay_db_error`
close the socket silently (EOF, no frame) so the consumer sees a clean
reconnect trigger without being misled by a spurious reject reason.
The wire-side reject write is a single non-blocking `send` wrapped in
`contextlib.suppress(BlockingIOError, OSError)`, attempted only when
the subscriber's tx queue is empty (the wire sits at a frame
boundary); with queued unsent bytes the subscriber gets a clean EOF
instead, so the reject can never land mid-frame. The frame is
best-effort either way — by definition the path runs only when the
subscriber is already lag-saturated. This contract is documented in
`docs/CONSUMER_API.md` section 2a.

### `SUBSCRIBER_COUNT` gauge consistency

The `SUBSCRIBER_COUNT` Prometheus gauge is incremented exactly once in
`_read_subscribe` and decremented exactly once in `_close_subscriber`.
Every code path that removes a subscriber must route through
`_close_subscriber` so that the gauge stays balanced. The replay
eviction path that produced the double-decrement in the second
track-record row bypassed this routing.

### Pre-ack buffer and `PRE_ACK_BUFFER_BYTES`

Between a subscriber's registration and the emission of `subscribe_ack`,
live fan-out frames are held in a per-subscriber pre-ack buffer capped
at `PRE_ACK_BUFFER_BYTES`. The cap is
`LAG_LIMIT * (MAX_FRAME_BYTES + _LENGTH_PREFIX_BYTES)`: sized to match
the lag-eviction threshold so a subscriber cannot be held in the
pre-ack window longer than it would be tolerated on the live wire. A
subscriber that overflows the pre-ack buffer is evicted with
`reason="lag_limit_exceeded"` (live fan-out) or
`reason="replay_lag_limit_exceeded"` (replay drain); both map to the
consumer-facing `lag_limit_exceeded` wire frame.

### Six internal reasons collapsing onto one wire reason

The daemon uses six internal close-reason strings for structured
logging and the per-reason `waitbus_subscriber_evicted_total{reason}`
counter: `lag_limit_exceeded` (live fan-out), `heartbeat_lag`,
`replay_lag_limit_exceeded`, `replay_db_error`,
`subscribe_ack_send_failed`, and `shutdown`. The consumer-facing wire
vocabulary is minimal: only `lag_limit_exceeded` appears in a framed
reject, and only when the drop is due to a backpressure condition the
consumer can act on. The other reasons either close silently or, for
`shutdown`, drain cleanly. `subscribe_ack_send_failed` covers the
narrow case where the daemon cannot deliver the post-registration
`subscribe_ack` frame to the peer (write error or close-during-ack);
the peer never observes a wire reject because the channel has already
failed at that point. The precise internal trigger appears only in
the daemon's structured `subscriber_closed` log line and the
evicted-counter label.

See `docs/CONSUMER_API.md` section 3 for the full consumer-facing
wire contract.

---

## Running locally

```bash
uv run pytest tests/test_broadcast_robustness.py -v
uv run pytest tests/test_broadcast.py -v
uv run pytest tests/test_broadcast_exception_mapping.py -v
uv run pytest tests/test_subscribe_envelope_hygiene.py -v
```

To reproduce a specific track-record scenario in isolation:

```bash
# Exception class drift
uv run pytest tests/test_broadcast_exception_mapping.py -v

# Replay wrong-key-pop subscriber leak
uv run pytest tests/test_broadcast.py::test_replay_lag_eviction_closes_subscriber_with_real_fd tests/test_broadcast_robustness.py -v

# Replay sqlite3.Error task crash
uv run pytest tests/test_broadcast.py -k test_close_subscriber tests/test_broadcast_robustness.py -v

# Hygiene scan false positives on narrative docs
uv run pytest tests/test_subscribe_envelope_hygiene.py -v
```

---

## Related documents

- [`SOAK_TEST.md`](SOAK_TEST.md) -- how-to for the 24-hour longevity soak on Hetzner.
- [`CONSUMER_API.md`](CONSUMER_API.md) -- stable wire contracts;
  section 2a documents the frame catalogue, section 3 the
  subscribe-rejected contract including the
  five-reason-to-one-wire-reason collapsing rule.
- [`../tests/test_broadcast_robustness.py`](../tests/test_broadcast_robustness.py)
  -- Hypothesis state-machine exercising the subscriber-lifecycle drain
  paths under arbitrary rule sequences.
- [`../tests/test_broadcast.py`](../tests/test_broadcast.py) -- primary
  daemon correctness tests; covers subscriber lifecycle, lag eviction,
  replay paths, and gauge balance.
- [`../tests/test_broadcast_exception_mapping.py`](../tests/test_broadcast_exception_mapping.py)
  -- contract tests for `_REJECT_REASON_EXCEPTIONS`; asserts that every
  consumer-facing wire reason maps to the correct typed exception
  class.
- [`../tests/test_corpus_property.py`](../tests/test_corpus_property.py)
  -- property tests for the corpus-replay contract and Hawkes-median
  estimator stability.
- [`../tests/test_property_frame.py`](../tests/test_property_frame.py)
  -- property tests for wire-frame encoding and decoding.
- [`../tests/test_predicate_property.py`](../tests/test_predicate_property.py)
  -- property tests for the `await_predicate` engine.
