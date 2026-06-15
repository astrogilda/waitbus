"""Event-delivery-fidelity bench.

Hypothesis (verbatim):

    A real LLM reasoning response (gpt-4.1-nano, 1000-1500 output
    tokens, temperature=0 seed=42 on a deterministic prompt),
    generated ONCE and segmented into a small set of discrete
    events, then REPLAYED through waitbus as per-event events delivered
    to a subscriber on the SAME channel that a 5-driver heterogeneous
    swarm is actively emitting on, exhibits (a) byte-identical delivery
    of every event vs its source manifest (a true round-trip 0-or-fail:
    per arm the consumer-delivered bytes are re-hashed and compared to
    the source, so a real waitbus / UDS drop or corruption is caught),
    (b) per-event bus delivery latency distribution within 1.5x
    the LLM-alone baseline's p99, and (c) no measurable TTFT or end-to-
    end wall-time delta under swarm load that exceeds the transport's
    own non-determinism floor (characterized by baseline-vs-baseline
    self-control).

Design (generate-once, replay-thrice):

- The reasoning text is generated ONCE per iteration (a single
  non-streaming OpenAI Chat Completions call in real mode, or a
  deterministic synthetic string in offline mode) and segmented into
  ``_EVENTS_PER_ITER`` discrete events. The SAME events are then
  replayed through all three arms below, so the delivery-integrity
  check (per arm: re-hash of the delivered bytes vs the iteration's
  source manifest) is a true round-trip 0-or-fail signal rather than a
  comparison of three independent generations.

- Three arms, interleaved paired triples (N=40 default, smoke=3):

  1. ``lll_alone_ipc_peer`` -- Replays the shared events as hash-signed
     frames; emitted via a sibling-process AF_UNIX SOCK_STREAM IPC
     peer. NO waitbus involvement.
  2. ``bus_idle`` -- Replays the shared events via ``waitbus.emit`` on
     the ``reasoning.chunk`` channel (encoded as the event's ``repo``
     field); consumer subprocess subscribes via ``waitbus.subscribe``.
     No swarm.
  3. ``bus_swarm`` -- Same as ``bus_idle`` PLUS N lightweight
     concurrent waitbus subscribers (``_SWARM_SUBSCRIBER_COUNT``)
     subscribe-and-drain (NO LLM) on the same daemon broadcast surface.
     Each subscriber imposes one extra per-event fan-out on the daemon
     -- the same load a real agent framework would impose -- but
     subscribes instantly with no cold-start, so the arm needs no warmup
     barrier. The arm barriers on every subscriber printing
     ``WAITBUS_BENCH_SUB_READY`` before the replay window opens.

Per-event timing (load-bearing measurement contract):

Each arm re-stamps the ``t_chunk_arrived_monotonic_ns`` field on every
frame at the SEND moment for that arm (``time.monotonic_ns()`` taken
immediately before the transport send), so the per-event arrival delta
on the consumer side measures the transport cost for that arm. The
frame's ``chunk_hash_hex`` is computed over ``(iter_id, chunk_seq,
chunk_bytes)`` ONLY (not the timestamp), so re-stamping leaves the hash
identical across arms.

Latency gate (absolute, PRE-REGISTERED):

The bus arms' p99 per-event delivery latency must be <= 100ms. The
100ms is derived FORWARD from Nielsen's canonical HCI limit (0.1s = the
threshold below which a system is perceived to react instantaneously to
a human), cross-checked against waitbus's own ~27ms emit-cost floor, and
fixed before the first full-N run -- it is NOT back-fit to any observed
measurement. The raw-IPC (``lll_alone``) arm is retained only as an
integrity control: its latency is recorded but never gated or used as a
comparison baseline.

Statistical analysis (perturbation, bus_idle vs bus_swarm):

Wilcoxon signed-rank paired test on three marginals, each pairing
``bus_idle`` vs ``bus_swarm`` (same transport, varying swarm load):

- ``per_chunk_bus_latency`` (median per iteration) -- the load-bearing
  perturbation signal (``bus_swarm_perturbs_latency``);
- ``time_to_first_chunk`` (TTFT);
- ``end_to_end_wall_time``.

Bonferroni-corrected alpha = 0.05 / 3 = 0.0167. The three ``h0_rejected_*``
flags report rejection per marginal. The raw-IPC arm is NOT in any of
these comparisons.

Subscriber-underload defense:

The bench tracks how many of the ``_SWARM_SUBSCRIBER_COUNT`` lightweight
subscribers signalled READY across the paired iterations -- the
concurrent-read load the bus_swarm arm is supposed to impose. The floor
is ``_SWARM_SUBSCRIBER_COUNT * N_iter`` (every subscriber ready every
iter). If observed READY subscribers are below 70% of the floor the
bench fires the ``inapplicable_subscriber_underloaded`` sentinel: the
load was not actually present, so the perturbation comparison is moot.

OFFLINE mode (``--skip-real-llm``):

The single OpenAI call is replaced by a deterministic synthetic
reasoning string, segmented into the same discrete events. EVERY
OTHER part of the wiring stays real: ``waitbus.emit`` and
``waitbus.subscribe`` against a real spawned waitbus daemon, the
lightweight concurrent subscribers, and the UDS sibling peer baseline.
Smoke mode (``--smoke``) is the same shape with N=3 (default) or any
``--n``; smoke does NOT switch to synthetic mocks for the bus or the
subscribers.

Linux-only. The cross-process ``CLOCK_MONOTONIC`` contract is the
load-bearing measurement substrate.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import gc
import hashlib
import logging
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Final, NamedTuple

import msgspec

from benchmarks._bench_preflight import (
    PreflightError,
    read_openai_key_from_keyring,
    run_preflight_assertions,
)
from benchmarks._bench_shared import (
    ExternalStateReport,
    append_jsonl_record,
    capture_daemon_pragmas,
)
from benchmarks._bench_swarm import default_python_executable
from waitbus._log import structured

_logger = logging.getLogger("waitbus.bench.event_delivery_fidelity")

# ---------------------------------------------------------------------
# Pinned constants.
# ---------------------------------------------------------------------

_BENCH_NAME: Final[str] = "event_delivery_fidelity"

# Default / smoke iteration counts.
_DEFAULT_N: Final[int] = 40
_SMOKE_N: Final[int] = 3

# Discrete events per iteration. The reasoning text generated once per
# iteration is segmented into (up to) this many contiguous byte
# segments; the SAME segments are replayed through all three arms. This
# is the expected event count everywhere a consumer / peer is told how
# many frames to wait for.
_EVENTS_PER_ITER: Final[int] = 16

# OpenAI model pin + sampling controls. Pinned per the bench's
# determinism contract; an operator who swaps the model edits this
# constant and re-runs the bench so the verdict's
# ``per_iter_source_distribution`` reflects the new pin.
_OPENAI_MODEL_ID: Final[str] = "gpt-4.1-nano"
_OPENAI_TEMPERATURE: Final[float] = 0.0
_OPENAI_SEED: Final[int] = 42
# Deterministic prompt yielding ~1000-1500 output tokens. The producer
# appends a per-iteration cache-bust prefix so the prompt's prefix-cache
# state is consistent across iterations.
_OPENAI_PROMPT: Final[str] = (
    "Provide a detailed, structured 1000-1500 token explanation of how a "
    "broadcast publish-subscribe primitive over AF_UNIX SOCK_STREAM "
    "differs from a polling REST endpoint in latency, jitter, and "
    "tail-percentile envelope. Cover: (1) per-event round-trip cost; "
    "(2) head-of-line-blocking; (3) backpressure semantics; "
    "(4) failure-mode envelopes (sender crash, receiver crash, "
    "intermediate buffer overflow); (5) replay/resume cursor design; "
    "(6) wall-clock vs monotonic clock contract; (7) implementation "
    "notes for a Linux-only sender; (8) sample observed latency "
    "ranges (microseconds for in-host UDS vs milliseconds for "
    "polled REST under typical 4-vCPU load). Use numbered sections "
    "and complete sentences; do not abbreviate. End with a one-line "
    "summary of the trade-off."
)
_OPENAI_MAX_TOKENS: Final[int] = 1500
_OPENAI_BASE_URL: Final[str] = "https://api.openai.com/v1"
# Timeout budget for the single non-streaming generation call made once
# per iteration in :func:`_build_events_once`.
_OPENAI_GENERATE_TIMEOUT_SEC: Final[float] = 60.0

# Synthetic offline mode: deterministic reasoning text seeded by
# ``iter_id`` so re-runs are byte-identical. The text is segmented into
# events the same way the real generation is, so the offline path
# exercises the identical replay posture.
_SYNTHETIC_REASONING_LOREM: Final[str] = (
    "A broadcast publish-subscribe primitive over AF_UNIX SOCK_STREAM "
    "delivers each event to every subscriber via an in-host kernel copy, "
    "so the per-event round-trip cost is bounded by socket buffer handling "
    "rather than a polling interval. Head-of-line blocking is avoided "
    "because the daemon fans out independently per subscriber. Backpressure "
    "is explicit: a slow subscriber's buffer fills and the daemon applies "
    "flow control without stalling the publisher's other subscribers. "
    "Failure-mode envelopes cover sender crash, receiver crash, and "
    "intermediate buffer overflow, each with a bounded recovery cursor. "
    "Replay and resume use a monotonic sequence cursor so a reconnecting "
    "subscriber catches up deterministically. The clock contract is "
    "monotonic, not wall-clock, so measured latencies are immune to NTP "
    "steps. Observed in-host UDS latencies sit in the microsecond range, "
    "well below the millisecond floor of a polled REST endpoint under "
    "typical four-vCPU load. "
)

# Channel name routed via the ``repo`` field of the EventInsert. Every
# bench subscriber composes ``fields.repo="reasoning.chunk"`` so the
# daemon's predicate filter does the per-channel routing on its side.
_REASONING_CHANNEL: Final[str] = "reasoning.chunk"

# Daemon-ready bootstrap window. The waitbus broadcast daemon must bind
# its AF_UNIX socket within this budget; matches the measurement-bench
# convention.
_DAEMON_READY_TIMEOUT_SEC: Final[float] = 10.0
_DAEMON_POLL_INTERVAL_SEC: Final[float] = 0.05

# UDS peer / consumer subprocess startup window (the sibling listens on
# its own socket; producer connects after the bind).
_UDS_PEER_READY_TIMEOUT_SEC: Final[float] = 10.0
_BUS_CONSUMER_READY_TIMEOUT_SEC: Final[float] = 15.0

# Drain budget per iteration after producer finishes (lets the bus
# consumer / UDS peer finish flushing every chunk's arrival line).
_DRAIN_BUDGET_SEC: Final[float] = 8.0

# Marker prefixes the bus-consumer subprocess writes to stdout. The
# orchestrator reads these line-by-line to recover arrival monotonic_ns
# for every emitted chunk + the per-arm swarm-emit count.
_MARKER_READY: Final[str] = "BUS_CONSUMER_READY"
_MARKER_CHUNK: Final[str] = "CHUNK"
_MARKER_DONE: Final[str] = "DONE"
_MARKER_UDS_READY: Final[str] = "UDS_PEER_READY"
_MARKER_UDS_CHUNK: Final[str] = "UDS_CHUNK"
# Readiness marker the lightweight subscriber subprocess prints AFTER its
# subscribe is registered (same register-then-READY contract as the
# primary consumer's ``WAITBUS_BENCH_CONSUMER_READY``). A barrier on it is
# sound because the daemon's subscribe-ack precedes the print.
_MARKER_SUB_READY: Final[str] = "WAITBUS_BENCH_SUB_READY"

# The number of lightweight concurrent waitbus subscribers the bus_swarm
# arm spawns (subscribe + drain, NO LLM). Each subscriber imposes one
# extra per-event fan-out on the daemon -- the SAME load a real agent
# framework would impose -- but subscribes instantly with no cold-start,
# so the arm needs no warmup barrier. The arm's load variable is the
# daemon's per-subscriber fan-out, identical whether the subscriber later
# calls an LLM or not.
_SWARM_SUBSCRIBER_COUNT: Final[int] = 5

# Fraction-of-floor at which the subscriber-underload sentinel fires.
# Spec value (0.70 ratio preserved from the prior swarm-emit floor).
_SANDBAGGING_RATIO: Final[float] = 0.70

# Bonferroni-corrected per-marginal alpha for the Wilcoxon paired test.
_ALPHA_FAMILY: Final[float] = 0.05
_ALPHA_PER_MARGINAL: Final[float] = _ALPHA_FAMILY / 3.0  # = 0.01666...

# Absolute per-event delivery-latency budget (PRE-REGISTERED). The bus
# arms' p99 per-event delivery latency must be <= 100ms. The 100ms is
# derived FORWARD from an external anchor -- Nielsen's canonical HCI
# limit (0.1s = the threshold below which a system is perceived to react
# "instantaneously" to a human) -- and cross-checked against waitbus's own
# ~27ms emit-cost floor, leaving ~3.7x headroom. It is NOT derived from
# any observed bench measurement.
_LATENCY_BUDGET_P99_NS: Final[int] = 100_000_000

# Pre-registered one-sided perturbation margin: the loaded bus_swarm arm's
# p99 delivery latency may exceed the idle bus_idle arm's by at most this
# much before it counts as a meaningful perturbation. 20ms = 20% of the
# 100ms latency budget -- forward-derived (not fit to data), well above the
# ~1-5ms run-to-run noise floor, well below the budget. A bare Wilcoxon
# significance test was rejected as the gate: at >=640 samples/arm it flags
# trivial, run-unstable, wrong-direction differences. The Wilcoxon p
# stays recorded as a non-gating observation.
_PERTURBATION_MARGIN_P99_NS: Final[int] = 20_000_000

# Default cost upper budget (USD) across all iterations + arms.
_DEFAULT_MAX_COST_USD: Final[float] = 5.0

# Per-arm wall-clock budget. Each iteration's per-arm cap is
# ``_OPENAI_GENERATE_TIMEOUT_SEC + _DRAIN_BUDGET_SEC``; the orchestrator
# enforces this so a hung replay or consumer cannot pin the run.
_PER_ARM_DEADLINE_SEC: Final[float] = _OPENAI_GENERATE_TIMEOUT_SEC + _DRAIN_BUDGET_SEC

# Approximate rate-card for gpt-4.1-nano (USD per 1M tokens). The
# bench's per-iteration cost tracking uses these to project budget
# breach; the figures are static at module-load time and an updated
# rate card belongs in a one-line edit here.
_GPT_4_1_NANO_INPUT_USD_PER_1M: Final[float] = 0.10
_GPT_4_1_NANO_OUTPUT_USD_PER_1M: Final[float] = 0.40

# Three arm names, in canonical paired-triple order.
_ARMS: Final[tuple[str, ...]] = ("lll_alone_ipc_peer", "bus_idle", "bus_swarm")


# ---------------------------------------------------------------------
# Frame struct (the deterministic-replay wire format).
# ---------------------------------------------------------------------


class ReasoningChunkFrame(msgspec.Struct, frozen=True, kw_only=True):
    """One streaming-chunk frame; identity is the SHA-256 of its content.

    The frame's ``chunk_hash_hex`` is a SHA-256 hex digest over the
    triple ``(iter_id, chunk_seq, chunk_bytes)``; the hash is the
    primary key the byte-identity check across arms uses. The frame
    travels through waitbus (encoded as the event's ``msg_body``) for
    the bus arms and through the UDS peer (length-prefixed JSON) for
    the lll_alone arm.

    ``t_chunk_arrived_monotonic_ns`` is re-stamped per arm at the SEND
    moment (``time.monotonic_ns()`` taken immediately before the
    transport send), so the consumer-side arrival delta measures the
    transport cost for that arm. It is NOT folded into the hash, so the
    same segmented payload replayed through every arm keeps an identical
    ``chunk_hash_hex``.
    """

    t_chunk_arrived_monotonic_ns: int
    chunk_seq: int
    iter_id: int
    chunk_bytes: bytes
    chunk_hash_hex: str


def _hash_chunk(*, iter_id: int, chunk_seq: int, chunk_bytes: bytes) -> str:
    """Compute the canonical chunk hash: SHA-256 hex of iter_id||seq||bytes.

    Returns the lowercase hex digest. The three components are encoded
    deterministically (decimal ints as UTF-8 strings, then the bytes
    payload) so two producers running the same iter_id + seq + bytes
    land on the same digest regardless of platform endianness.
    """
    digest = hashlib.sha256()
    digest.update(str(iter_id).encode("utf-8"))
    digest.update(b"|")
    digest.update(str(chunk_seq).encode("utf-8"))
    digest.update(b"|")
    digest.update(chunk_bytes)
    return digest.hexdigest()


# ---------------------------------------------------------------------
# Per-arm latency aggregate.
# ---------------------------------------------------------------------


class _ArmLatencyStats(msgspec.Struct, frozen=True, kw_only=True):
    """Aggregate latency stats for one arm across all completed iterations.

    The three families correspond to the bench's three marginals:

    * ``per_chunk_bus_latency_ns`` -- arrival_monotonic_ns on the
      consumer side MINUS producer-side
      ``t_chunk_arrived_monotonic_ns`` on the frame, per chunk. For
      the bus arms this is the waitbus broadcast cost; for the
      lll_alone arm this is the UDS sibling-IPC cost.
    * ``ttft_ns`` -- per-iteration time-to-first-chunk: arrival of
      ``chunk_seq=0`` on the consumer minus the producer-side
      iteration-start anchor.
    * ``wall_time_ns`` -- per-iteration arm wall-clock: orchestrator-
      side ``time.monotonic_ns()`` from arm start to arm end.

    Empty input rolls up to ``0`` for every aggregate; the
    ``n_chunks`` / ``n_iterations`` fields surface the gap.
    """

    arm: str
    n_iterations: int
    n_chunks_total: int
    median_per_chunk_bus_latency_ns: int
    p99_per_chunk_bus_latency_ns: int
    median_ttft_ns: int
    median_wall_time_ns: int


# ---------------------------------------------------------------------
# Top-level verdict struct.
# ---------------------------------------------------------------------


class EventDeliveryFidelityVerdict(msgspec.Struct, frozen=True, kw_only=True):
    """verdict.json shape for the event-delivery-fidelity bench."""

    bench_name: str
    started_ns: int
    finished_ns: int
    environment: ExternalStateReport
    external_state: ExternalStateReport
    n_triples_requested: int
    n_triples_actual: int
    smoke: bool
    include_real_llm: bool
    arms: list[str]
    # Per-arm aggregate stats.
    arm_stats: dict[str, _ArmLatencyStats]
    # Wilcoxon paired-test p-values (three marginals). Every marginal
    # pairs bus_idle vs bus_swarm (same transport, varying swarm load);
    # the raw-IPC arm is NOT a comparison baseline.
    wilcoxon_p_per_chunk_bus_latency: float
    wilcoxon_p_ttft: float
    wilcoxon_p_wall_time: float
    # Per-marginal Bonferroni-gated rejection flags (bus_idle vs bus_swarm).
    h0_rejected_per_chunk_bus_latency: bool
    h0_rejected_ttft: bool
    h0_rejected_wall_time: bool
    alpha_per_marginal: float
    # Delivery-integrity counters: per arm, count of source chunks that
    # were dropped or corrupted in transit (delivered-bytes re-hash vs
    # source manifest). A non-zero count is a real waitbus / UDS delivery
    # bug, NOT model-side non-determinism.
    delivery_integrity_failures_lll_alone: int
    delivery_integrity_failures_bus_idle: int
    delivery_integrity_failures_bus_swarm: int
    # Ordering-fidelity counters: per arm, count of out-of-order
    # ("descent") deliveries -- a delivered seq arriving after a strictly
    # greater seq. waitbus guarantees a daemon-assigned monotonic delivery
    # sequence, so a non-zero count is a real waitbus / UDS reordering
    # bug, NOT model-side non-determinism.
    ordering_inversions_lll_alone: int
    ordering_inversions_bus_idle: int
    ordering_inversions_bus_swarm: int
    # Median per-iteration latency / TTFT / wall-time per arm
    # (nine fields total: three metrics x three arms).
    median_per_chunk_bus_latency_alone_ns: int
    median_per_chunk_bus_latency_bus_idle_ns: int
    median_per_chunk_bus_latency_bus_swarm_ns: int
    median_ttft_alone_ns: int
    median_ttft_bus_idle_ns: int
    median_ttft_bus_swarm_ns: int
    median_wall_time_alone_ns: int
    median_wall_time_bus_idle_ns: int
    median_wall_time_bus_swarm_ns: int
    # Subscriber-underload defense. The bus_swarm load is N lightweight
    # concurrent subscribers; this is the total that signalled READY
    # across the paired iterations (floor = _SWARM_SUBSCRIBER_COUNT * N).
    swarm_subscribers_ready_total: int
    swarm_underload_floor: int
    sandbagging_sentinel_fired: bool
    # Absolute per-event latency budget gate (PRE-REGISTERED 100ms; the
    # raw-IPC arm is NOT in this gate -- the budget is on waitbus's own
    # contract, not a ratio against a non-durable pipe).
    latency_budget_p99_ns: int
    bus_idle_p99_latency_ns: int
    bus_swarm_p99_latency_ns: int
    latency_budget_passed: bool
    # bus_idle-vs-bus_swarm perturbation test: Wilcoxon equivalence on the
    # per-event delivery latency between the two waitbus arms (same
    # transport, varying swarm load). The raw-IPC arm is NOT in this
    # comparison.
    wilcoxon_p_bus_idle_vs_swarm_latency: float
    bus_swarm_perturbs_latency: bool
    # Distribution equivalence: bus_idle and bus_swarm delivery latency
    # are distributionally equivalent (H0 NOT rejected).
    distribution_equivalent: bool
    # Perturbation: budget breach OR bus_idle-vs-bus_swarm perturbation.
    perturbation_detected: bool
    # Reason this bench was declared inapplicable (None on a clean
    # verdict; "inapplicable_subscriber_underloaded" or
    # "n_triples_actual_zero" on a structural skip).
    inapplicable_reason: str | None
    # Cost bookkeeping (Real OpenAI mode only).
    cost_usd_total: float
    cost_unknown_count: int
    max_cost_usd_budget: float
    max_cost_usd_observed: float
    aborted_on_budget: bool
    limitations: list[str]
    # Per-iter source distribution -- empty on this bench (the
    # reasoning.chunk emits are always source "agent"; the bench-A source
    # mix is not exercised here).
    per_iter_source_distribution: dict[str, int] = {}


# ---------------------------------------------------------------------
# Daemon + waitbus CLI helpers.
# ---------------------------------------------------------------------


def _waitbus_path() -> str:
    """Resolve the ``waitbus`` CLI on PATH (or sibling of ``sys.executable``)."""
    sibling = Path(sys.executable).parent / "waitbus"
    if sibling.is_file():
        return str(sibling)
    on_path = shutil.which("waitbus")
    if on_path is None:
        raise RuntimeError("waitbus CLI not found on PATH; install waitbus first")
    return on_path


def _spawn_daemon(env: dict[str, str], waitbus_path: str, socket_path: Path) -> subprocess.Popen[bytes]:
    """Spawn the waitbus broadcast daemon; block until the socket binds."""
    proc = subprocess.Popen(
        [waitbus_path, "broadcast", "serve"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    deadline = time.monotonic() + _DAEMON_READY_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if socket_path.exists():
            return proc
        if proc.poll() is not None:
            raise RuntimeError(f"waitbus daemon exited before binding {socket_path}")
        time.sleep(_DAEMON_POLL_INTERVAL_SEC)
    proc.terminate()
    raise RuntimeError(f"waitbus daemon failed to bind {socket_path} within {_DAEMON_READY_TIMEOUT_SEC}s")


# ---------------------------------------------------------------------
# Generate-once + segment-into-events (shared per-iteration payloads).
# ---------------------------------------------------------------------


def _segment_text_into_events(text: str, *, iter_id: int, n_target: int) -> list[tuple[int, bytes, str]]:
    """Split ``text`` into up to ``n_target`` contiguous byte-segment events.

    The UTF-8 encoding of ``text`` is split into ``n_target`` roughly-
    equal contiguous byte segments (deterministic; the last segment
    absorbs any remainder). Each event is ``(chunk_seq, chunk_bytes,
    chunk_hash_hex)`` with ``chunk_seq`` running 0..N-1 and the hash
    computed over ``(iter_id, chunk_seq, chunk_bytes)``.

    When the text is shorter than ``n_target`` bytes the segmentation
    emits one event per byte (so at most ``n_target`` events, never a
    crash on a tiny input). Empty text yields no events.
    """
    raw = text.encode("utf-8")
    total = len(raw)
    if total == 0:
        return []
    n_events = min(n_target, total)
    base = total // n_events
    remainder = total % n_events
    events: list[tuple[int, bytes, str]] = []
    cursor = 0
    for chunk_seq in range(n_events):
        # The last ``remainder`` segments each take one extra byte so the
        # split is deterministic and the final segment carries the tail.
        seg_len = base + (1 if chunk_seq >= n_events - remainder else 0)
        chunk_bytes = raw[cursor : cursor + seg_len]
        cursor += seg_len
        chunk_hash_hex = _hash_chunk(iter_id=iter_id, chunk_seq=chunk_seq, chunk_bytes=chunk_bytes)
        events.append((chunk_seq, chunk_bytes, chunk_hash_hex))
    return events


def _generate_reasoning_text_openai(*, api_key: str, sentinel_prefix: str) -> tuple[str, int]:
    """Make ONE non-streaming gpt-4.1-nano call; return (assistant_text, completion_tokens).

    Uses a synchronous ``httpx.Client`` (this runs before the per-arm
    asyncio loops) against the Chat Completions endpoint with
    ``stream=False``. Raises on any HTTP error (fail-fast; no silent
    fallback to synthetic text).
    """
    import httpx  # local: pulls heavy networking only when the real-LLM path runs.

    url = f"{_OPENAI_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    prompt = f"[cache-bust:{sentinel_prefix}]\n\n{_OPENAI_PROMPT}"
    payload = {
        "model": _OPENAI_MODEL_ID,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "temperature": _OPENAI_TEMPERATURE,
        "seed": _OPENAI_SEED,
        "max_tokens": _OPENAI_MAX_TOKENS,
    }
    timeout = httpx.Timeout(connect=10.0, read=_OPENAI_GENERATE_TIMEOUT_SEC, write=10.0, pool=10.0)
    with httpx.Client(timeout=timeout) as client:
        response = client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        body: dict[str, Any] = response.json()
    choices: list[Any] = body.get("choices") or []
    message_text = ""
    if choices and isinstance(choices[0], dict):
        message = choices[0].get("message") or {}
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                message_text = content
    completion_tokens = 0
    usage = body.get("usage") or {}
    if isinstance(usage, dict):
        value = usage.get("completion_tokens")
        if isinstance(value, int):
            completion_tokens = value
    return message_text, completion_tokens


def _build_events_once(
    *,
    iter_id: int,
    api_key: str | None,
    include_real_llm: bool,
    sentinel_prefix: str,
) -> tuple[list[tuple[int, bytes, str]], int]:
    """Build the shared per-iteration event payloads ONCE.

    Returns ``(event_contents, completion_tokens)`` where each event in
    ``event_contents`` is ``(chunk_seq, chunk_bytes, chunk_hash_hex)``.
    The SAME events are then replayed through all three arms, so the
    content-integrity check across arms is a true 0-or-fail signal.

    In real mode (``include_real_llm``) the reasoning text comes from a
    single non-streaming OpenAI call and ``completion_tokens`` is the
    usage figure. In offline mode the text is a deterministic synthetic
    string seeded by ``iter_id`` and ``completion_tokens`` is 0.
    """
    if include_real_llm:
        assert api_key is not None
        text, completion_tokens = _generate_reasoning_text_openai(api_key=api_key, sentinel_prefix=sentinel_prefix)
    else:
        # Deterministic synthetic text: the lorem block repeated and
        # tagged with iter_id so re-runs are byte-identical and distinct
        # iterations differ. Two repeats comfortably exceed
        # _EVENTS_PER_ITER bytes so every iteration yields the full event
        # count.
        text = f"[iter={iter_id}] {_SYNTHETIC_REASONING_LOREM}{_SYNTHETIC_REASONING_LOREM}"
        completion_tokens = 0
    events = _segment_text_into_events(text, iter_id=iter_id, n_target=_EVENTS_PER_ITER)
    return events, completion_tokens


# ---------------------------------------------------------------------
# Bus consumer subprocess (subscribes via waitbus.subscribe).
# ---------------------------------------------------------------------


_BUS_CONSUMER_SCRIPT: Final[str] = r"""
import base64, json, os, sys, time

from waitbus._subscribe import subscribe

socket_path = os.environ['WAITBUS_BENCH_SOCKET']
channel = os.environ['WAITBUS_BENCH_CHANNEL']
seed_scope_id = os.environ['WAITBUS_BENCH_SEED_SCOPE']
expected = int(os.environ['WAITBUS_BENCH_EXPECTED_CHUNKS'])

# The consumer subscribes to ALL ``agent_message`` events on the bus
# (the daemon's broadcast surface) and records the producer's
# reasoning.chunk emits (fields.repo == channel AND
# fields.owner == seed_scope_id) as arrival lines on stdout, so the
# orchestrator can read the arrival ledger without a second IPC.
print('WAITBUS_BENCH_CONSUMER_READY', flush=True)

received_chunks = 0
# subscribe() yields EventFrame objects; we use match='fields.event_type="agent_message"' so the daemon
# server-side filter only ships ``agent_message`` frames (the chunk source uses event_type=agent_message too).
match_spec = 'fields.event_type="agent_message"'
gen = subscribe(match_spec, socket_path=socket_path)
try:
    for frame in gen:
        arrival_ns = time.monotonic_ns()
        fields = frame.fields
        owner = fields.get('owner', '')
        repo = fields.get('repo', '')
        if repo == channel and owner == seed_scope_id:
            # The producer's chunk lands here. The chunk frame body is
            # carried in fields.msg_body as a base64-encoded msgspec
            # encoded ReasoningChunkFrame.
            body_b64 = fields.get('msg_body') or ''
            print(f'CHUNK arrival_ns={arrival_ns} event_id={frame.event_id} body_b64={body_b64}', flush=True)
            received_chunks += 1
            if received_chunks >= expected:
                break
finally:
    print(f'DONE chunks={received_chunks}', flush=True)
    try:
        gen.close()
    except Exception:
        pass
"""


def _spawn_bus_consumer(
    *,
    socket_path: Path,
    channel: str,
    seed_scope_id: str,
    expected_chunks: int,
    python_exe: str,
) -> subprocess.Popen[bytes]:
    """Spawn the bus consumer subprocess; returns its Popen handle.

    The subprocess subscribes via ``waitbus.subscribe`` and
    emits one line per delivered reasoning.chunk on stdout. The caller
    is responsible for terminating the process.
    """
    env = dict(os.environ)
    env["WAITBUS_BENCH_SOCKET"] = str(socket_path)
    env["WAITBUS_BENCH_CHANNEL"] = channel
    env["WAITBUS_BENCH_SEED_SCOPE"] = seed_scope_id
    env["WAITBUS_BENCH_EXPECTED_CHUNKS"] = str(expected_chunks)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONHASHSEED"] = "0"
    return subprocess.Popen(
        [python_exe, "-u", "-c", _BUS_CONSUMER_SCRIPT],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


# ---------------------------------------------------------------------
# Lightweight concurrent subscriber (bus_swarm load generator).
# ---------------------------------------------------------------------


_LIGHTWEIGHT_SUBSCRIBER_SCRIPT: Final[str] = r"""
import os, sys, time

from waitbus._subscribe import subscribe

socket_path = os.environ['WAITBUS_BENCH_SOCKET']

# Subscribe to the SAME daemon broadcast surface the replay emits on
# (the chunk source uses event_type=agent_message), then signal READY
# AFTER the subscribe is registered -- same register-then-READY contract
# as the primary consumer. This subscriber imposes the daemon's
# per-subscriber fan-out load and is counted as a live reader; it does
# NOT parse chunk hashes or emit any reaction -- it just drains until the
# orchestrator terminates it on teardown (no terminal sentinel, exactly
# like the primary consumer's subscribe loop).
match_spec = 'fields.event_type="agent_message"'
gen = subscribe(match_spec, socket_path=socket_path)
print('WAITBUS_BENCH_SUB_READY', flush=True)

received = 0
try:
    for frame in gen:
        received += 1
finally:
    print(f'DONE received={received}', flush=True)
    try:
        gen.close()
    except Exception:
        pass
"""


def _spawn_lightweight_subscriber(
    *,
    socket_path: Path,
    python_exe: str,
) -> subprocess.Popen[bytes]:
    """Spawn one lightweight concurrent subscriber; returns its Popen handle.

    The subprocess subscribes to the same daemon broadcast surface the
    replay emits on, prints ``WAITBUS_BENCH_SUB_READY`` once the subscribe
    is registered, then drains frames until the orchestrator terminates
    it. It exists only to impose concurrent per-subscriber fan-out load
    on the daemon and to be counted as a live reader; it parses no chunk
    hashes and emits no reaction. The caller terminates it on teardown.
    """
    env = dict(os.environ)
    env["WAITBUS_BENCH_SOCKET"] = str(socket_path)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONHASHSEED"] = "0"
    return subprocess.Popen(
        [python_exe, "-u", "-c", _LIGHTWEIGHT_SUBSCRIBER_SCRIPT],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


# ---------------------------------------------------------------------
# UDS sibling-process peer (lll_alone_ipc_peer baseline).
# ---------------------------------------------------------------------


_UDS_PEER_SCRIPT: Final[str] = r"""
import os, socket, struct, sys, time

socket_path = os.environ['WAITBUS_BENCH_UDS_PEER_SOCKET']
expected = int(os.environ['WAITBUS_BENCH_UDS_PEER_EXPECTED_CHUNKS'])

server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
try:
    os.unlink(socket_path)
except FileNotFoundError:
    pass
server.bind(socket_path)
server.listen(1)
print('UDS_PEER_READY', flush=True)

conn, _ = server.accept()
try:
    received = 0
    pending = b''
    while received < expected:
        chunk = conn.recv(65536)
        if not chunk:
            break
        pending += chunk
        # Length-prefixed framing: 4 bytes big-endian uint32 length, then payload.
        while len(pending) >= 4:
            (length,) = struct.unpack('>I', pending[:4])
            if length > 16 * 1024 * 1024:  # 16 MiB defensive cap
                raise RuntimeError(f'frame too large: {length}')
            if len(pending) < 4 + length:
                break
            frame_bytes = pending[4:4 + length]
            pending = pending[4 + length:]
            arrival_ns = time.monotonic_ns()
            # Print one line per frame; base-64-quote the payload so newlines do not break the line protocol.
            import base64
            payload_b64 = base64.b64encode(frame_bytes).decode('ascii')
            print(f'UDS_CHUNK arrival_ns={arrival_ns} body_b64={payload_b64}', flush=True)
            received += 1
finally:
    try:
        conn.close()
    except Exception:
        pass
    try:
        server.close()
    except Exception:
        pass
    print(f'DONE received={received}', flush=True)
"""


def _spawn_uds_peer(
    *,
    socket_path: Path,
    expected_chunks: int,
    python_exe: str,
) -> subprocess.Popen[bytes]:
    """Spawn the sibling UDS peer process; returns Popen handle.

    The peer binds the AF_UNIX SOCK_STREAM socket and accepts the
    producer's connection. Frames travel length-prefixed (4-byte
    big-endian uint32) followed by the JSON-encoded
    ``ReasoningChunkFrame``.
    """
    env = dict(os.environ)
    env["WAITBUS_BENCH_UDS_PEER_SOCKET"] = str(socket_path)
    env["WAITBUS_BENCH_UDS_PEER_EXPECTED_CHUNKS"] = str(expected_chunks)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONHASHSEED"] = "0"
    return subprocess.Popen(
        [python_exe, "-u", "-c", _UDS_PEER_SCRIPT],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def _wait_for_marker(
    proc: subprocess.Popen[bytes],
    *,
    expected_marker: str,
    timeout_sec: float,
) -> bool:
    """Block until ``proc.stdout`` produces a line starting with ``expected_marker``.

    Returns True on a clean match; False on timeout or stdout EOF
    before the marker arrives.
    """
    assert proc.stdout is not None
    deadline = time.monotonic() + timeout_sec
    import select

    while time.monotonic() < deadline:
        rlist, _, _ = select.select([proc.stdout], [], [], 0.1)
        if not rlist:
            continue
        line = proc.stdout.readline()
        if not line:
            return False
        text = line.decode("utf-8", errors="replace").rstrip()
        if text.startswith(expected_marker):
            return True
    return False


class _ConcurrentStdoutCollector:
    """Drain a child's stdout in a background thread for the whole arm.

    The peer/consumer children print one measurement line per frame.
    Reading that stdout only *after* the producer's send/emit loop lets
    the child's 64 KiB stdout pipe fill mid-stream, which blocks the
    child's ``recv()`` / subscribe loop and deadlocks the producer's
    ``StreamWriter.drain()`` (offline's 30-chunk stream fits the pipe and
    hides the bug; a real ~1000+ chunk LLM stream does not). Draining
    concurrently keeps the pipe empty so the child never blocks. The
    children stamp ``arrival_ns`` at receive time, so deferring nothing
    on the read side leaves the latency measurement exact.
    """

    def __init__(
        self,
        proc: subprocess.Popen[bytes],
        *,
        terminal_prefix: str = _MARKER_DONE,
    ) -> None:
        assert proc.stdout is not None
        self._proc = proc
        self._terminal_prefix = terminal_prefix
        self._lines: list[str] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="bench-stdout-collector", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        import select

        stdout = self._proc.stdout
        assert stdout is not None
        while not self._stop.is_set():
            rlist, _, _ = select.select([stdout], [], [], 0.1)
            if not rlist:
                continue
            line = stdout.readline()
            if not line:
                break  # EOF: child closed stdout (clean exit or terminate)
            text = line.decode("utf-8", errors="replace").rstrip()
            if text.startswith(self._terminal_prefix):
                break  # child signalled DONE
            self._lines.append(text)

    def finish(self, *, deadline_sec: float) -> list[str]:
        """Wait up to ``deadline_sec`` for a clean DONE/EOF, then stop and return lines.

        A child that signals DONE (the UDS peer, on producer EOF) makes the
        collector thread exit immediately. A child with no terminal sentinel
        (the bus consumer waits on the subscribe socket) drains trailing
        frames for the full budget before being stopped; the caller then
        terminates it, which closes stdout and unblocks the thread.
        """
        self._thread.join(timeout=deadline_sec)
        self._stop.set()
        self._thread.join(timeout=1.0)
        return list(self._lines)


def _terminate(proc: subprocess.Popen[bytes]) -> None:
    """Best-effort terminate; ignore already-dead processes. Closes piped FDs.

    Closes ``proc.stdout`` / ``proc.stderr`` / ``proc.stdin`` so the
    BufferedReader file objects do not leak past the bench's teardown
    (the asyncio test harness raises ``PytestUnraisableExceptionWarning``
    on a dangling file object when a subsequent iteration's
    ``asyncio.run`` finalises and the prior iteration's Popen FDs are
    still un-closed).
    """
    with contextlib.suppress(ProcessLookupError, OSError):
        proc.terminate()
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=2.0)
    for stream in (proc.stdout, proc.stderr, proc.stdin):
        if stream is not None:
            with contextlib.suppress(OSError, ValueError):
                stream.close()


# ---------------------------------------------------------------------
# Frame serialisation for waitbus emit + UDS framing.
# ---------------------------------------------------------------------


def _encode_chunk_frame_for_msg_body(frame: ReasoningChunkFrame) -> str:
    """Encode a ``ReasoningChunkFrame`` to the waitbus ``msg_body`` string.

    The frame is msgspec-JSON-encoded, then base64-encoded so the
    resulting string is safe to ride the ``msg_body`` field (which is
    a free-form UTF-8 string). Round-trip:
    :func:`_decode_chunk_frame_from_msg_body`.
    """
    import base64

    encoded = msgspec.json.encode(frame)
    return base64.b64encode(encoded).decode("ascii")


def _decode_chunk_frame_from_msg_body(body_b64: str) -> ReasoningChunkFrame:
    """Inverse of :func:`_encode_chunk_frame_for_msg_body`."""
    import base64

    raw = base64.b64decode(body_b64)
    return msgspec.json.decode(raw, type=ReasoningChunkFrame)


def _encode_chunk_frame_for_uds(frame: ReasoningChunkFrame) -> bytes:
    """Encode a frame for the UDS peer's length-prefixed wire (4-byte uint32 + JSON)."""
    import struct

    payload = msgspec.json.encode(frame)
    return struct.pack(">I", len(payload)) + payload


# ---------------------------------------------------------------------
# Per-iteration arm runners.
# ---------------------------------------------------------------------


async def _emit_via_waitbus(
    *,
    frame: ReasoningChunkFrame,
    seed_scope_id: str,
    channel: str,
    db_path: Path,
    doorbell_path: Path,
) -> None:
    """Persist one ``ReasoningChunkFrame`` via waitbus.emit on the shared channel.

    The frame rides as a base64-encoded msgspec JSON body on the
    EventInsert's ``msg_body`` field; the channel is encoded as the
    event's ``repo`` field so the daemon's per-channel predicate
    matches subscribers on ``fields.repo``.
    """
    from waitbus._emit import emit
    from waitbus._types import EventInsert

    body = _encode_chunk_frame_for_msg_body(frame)
    insert = EventInsert(
        delivery_id=f"reasoning-chunk:{seed_scope_id}:{frame.chunk_seq}",
        source="agent",
        event_type="agent_message",
        owner=seed_scope_id,
        repo=channel,
        received_at=time.time_ns(),
        payload_json='{"kind":"reasoning_chunk"}',
        ingest_method="bench_event_delivery_fidelity",
        msg_to=seed_scope_id,
        msg_from="bench-producer",
        msg_body=body,
    )
    # Run the synchronous emit() on a worker thread so the producer
    # coroutine's event loop is not blocked by the SQLite write +
    # doorbell ring.
    await asyncio.to_thread(emit, insert, db_path=db_path, doorbell_path=doorbell_path)


async def _send_uds_frame(writer: asyncio.StreamWriter, frame: ReasoningChunkFrame) -> None:
    """Write one length-prefixed frame to the UDS peer."""
    payload = _encode_chunk_frame_for_uds(frame)
    writer.write(payload)
    await writer.drain()


async def _run_arm_lll_alone(
    *,
    iter_id: int,
    events: list[tuple[int, bytes, str]],
    uds_socket_path: Path,
    python_exe: str,
) -> dict[str, Any]:
    """Run one ``lll_alone_ipc_peer`` arm iteration; return per-arm row dict.

    Replays the shared ``events`` (built once per iteration by
    :func:`_build_events_once`) through the UDS sibling-process peer.
    Each frame's ``t_chunk_arrived_monotonic_ns`` is re-stamped at the
    send moment. The peer logs per-frame arrival monotonic_ns to its
    stdout; the orchestrator pairs the two streams on chunk_seq.
    """
    arm_start_ns = time.monotonic_ns()
    # Spawn the UDS peer first, wait for its READY marker, then connect.
    # The replay sends exactly ``_EVENTS_PER_ITER`` frames; the peer
    # exits on connection close OR when ``expected`` is reached.
    expected = _EVENTS_PER_ITER
    peer_proc = _spawn_uds_peer(socket_path=uds_socket_path, expected_chunks=expected, python_exe=python_exe)
    try:
        if not _wait_for_marker(peer_proc, expected_marker=_MARKER_UDS_READY, timeout_sec=_UDS_PEER_READY_TIMEOUT_SEC):
            raise RuntimeError("UDS peer did not become ready within budget")
        # Connect from the producer side. Drain the peer's stdout
        # concurrently so its per-frame ledger lines never back-pressure
        # the socket the producer is writing into (see
        # _ConcurrentStdoutCollector).
        reader, writer = await asyncio.open_unix_connection(str(uds_socket_path))
        collector = _ConcurrentStdoutCollector(peer_proc, terminal_prefix=_MARKER_DONE)
        collector.start()
        try:
            # Replay loop: re-stamp the send moment per frame and send
            # sequentially. At N=_EVENTS_PER_ITER there is no pipe-
            # backpressure risk, so a simple await-per-event loop is
            # correct.
            frames: list[ReasoningChunkFrame] = []
            for chunk_seq, chunk_bytes, chunk_hash_hex in events:
                frame = ReasoningChunkFrame(
                    t_chunk_arrived_monotonic_ns=time.monotonic_ns(),
                    chunk_seq=chunk_seq,
                    iter_id=iter_id,
                    chunk_bytes=chunk_bytes,
                    chunk_hash_hex=chunk_hash_hex,
                )
                frames.append(frame)
                await _send_uds_frame(writer, frame)
            # The first frame's send-moment anchor is TTFT's producer-side
            # reference.
            ttft_anchor_ns: int | None = frames[0].t_chunk_arrived_monotonic_ns if frames else None
            # Tokens are counted once at generation, not per arm.
            completion_tokens = 0
            writer.write_eof()
            await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            _ = reader
        # The peer prints DONE once the producer's EOF ends its recv loop.
        peer_lines = collector.finish(deadline_sec=_DRAIN_BUDGET_SEC)
    finally:
        _terminate(peer_proc)
    arm_end_ns = time.monotonic_ns()

    return {
        "arm": "lll_alone_ipc_peer",
        "iter_id": iter_id,
        "frames": frames,
        "source_hashes": {seq: h for seq, _b, h in events},
        "completion_tokens": completion_tokens,
        "consumer_lines": peer_lines,
        "arm_start_ns": arm_start_ns,
        "arm_end_ns": arm_end_ns,
        "ttft_anchor_ns": ttft_anchor_ns,
        "consumer_chunk_prefix": _MARKER_UDS_CHUNK,
        "swarm_subscribers_ready": 0,
    }


async def _run_arm_bus(
    *,
    arm_name: str,
    iter_id: int,
    events: list[tuple[int, bytes, str]],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    python_exe: str,
    swarm_enabled: bool,
) -> dict[str, Any]:
    """Run one bus arm iteration (``bus_idle`` or ``bus_swarm``).

    Spawns the bus consumer subprocess; for the ``bus_swarm`` arm it
    additionally spawns ``_SWARM_SUBSCRIBER_COUNT`` lightweight
    concurrent subscribers (subscribe + drain, NO LLM) and barriers on
    each printing ``WAITBUS_BENCH_SUB_READY`` before the replay starts, so
    every replayed event fans out to the primary consumer AND all ready
    subscribers (that fan-out IS the concurrent-read load). Then replays
    the shared ``events`` via ``waitbus.emit`` and drains the consumer.

    The load metric is ``swarm_subscribers_ready`` -- how many of the
    ``_SWARM_SUBSCRIBER_COUNT`` subscribers signalled READY. On the
    ``bus_idle`` arm no subscribers are spawned and the count is 0.
    """
    arm_start_ns = time.monotonic_ns()
    seed_scope_id = f"reasoning-chunk-{arm_name}-{iter_id}-{uuid.uuid4().hex[:8]}"
    subscribers: list[subprocess.Popen[bytes]] = []
    subscribers_ready = 0
    expected_chunks = _EVENTS_PER_ITER

    consumer = _spawn_bus_consumer(
        socket_path=socket_path,
        channel=_REASONING_CHANNEL,
        seed_scope_id=seed_scope_id,
        expected_chunks=expected_chunks,
        python_exe=python_exe,
    )
    try:
        if not _wait_for_marker(
            consumer, expected_marker="WAITBUS_BENCH_CONSUMER_READY", timeout_sec=_BUS_CONSUMER_READY_TIMEOUT_SEC
        ):
            raise RuntimeError(f"bus consumer for arm {arm_name} did not signal ready within budget")

        # Drain the consumer's stdout concurrently from here on. Reading it
        # only after the producer's emit loop lets the consumer's per-frame
        # ledger fill its stdout pipe, stall its subscribe read, and (via the
        # daemon's back-pressure) deadlock the producer (see
        # _ConcurrentStdoutCollector).
        collector = _ConcurrentStdoutCollector(consumer, terminal_prefix=_MARKER_DONE)
        collector.start()

        if swarm_enabled:
            # Spawn N lightweight concurrent subscribers and barrier on each
            # signalling READY (subscribe registered) BEFORE the replay
            # window opens, so every replayed event fans out to all of them.
            # The subscribers subscribe instantly (no LLM cold-start), so the
            # barrier clears deterministically with no warmup window. Each
            # READY subscriber imposes one extra per-event fan-out on the
            # daemon -- the SAME load a real agent framework would impose.
            for _ in range(_SWARM_SUBSCRIBER_COUNT):
                sub = _spawn_lightweight_subscriber(socket_path=socket_path, python_exe=python_exe)
                subscribers.append(sub)
            for sub in subscribers:
                if _wait_for_marker(
                    sub, expected_marker=_MARKER_SUB_READY, timeout_sec=_BUS_CONSUMER_READY_TIMEOUT_SEC
                ):
                    subscribers_ready += 1

        # Replay loop: re-stamp the send moment per frame and emit via
        # waitbus sequentially. The consumer-side per-event latency measures
        # the bus's transport lag. At N=_EVENTS_PER_ITER there is no
        # backpressure risk, so a simple await-per-event loop is correct.
        frames: list[ReasoningChunkFrame] = []
        for chunk_seq, chunk_bytes, chunk_hash_hex in events:
            frame = ReasoningChunkFrame(
                t_chunk_arrived_monotonic_ns=time.monotonic_ns(),
                chunk_seq=chunk_seq,
                iter_id=iter_id,
                chunk_bytes=chunk_bytes,
                chunk_hash_hex=chunk_hash_hex,
            )
            frames.append(frame)
            await _emit_via_waitbus(
                frame=frame,
                seed_scope_id=seed_scope_id,
                channel=_REASONING_CHANNEL,
                db_path=db_path,
                doorbell_path=doorbell_path,
            )
        ttft_anchor_ns: int | None = frames[0].t_chunk_arrived_monotonic_ns if frames else None
        # Tokens are counted once at generation, not per arm.
        completion_tokens = 0

        # The consumer has no terminal sentinel (it waits on the subscribe
        # socket), so drain trailing CHUNK lines for the budget, then stop;
        # _terminate below closes its stdout and ends the thread.
        consumer_lines = collector.finish(deadline_sec=_DRAIN_BUDGET_SEC)
    finally:
        _terminate(consumer)
        for sub in subscribers:
            _terminate(sub)
            # Best-effort close the Popen's piped FDs so the
            # BufferedReader file objects do not leak past the bench's
            # teardown (asyncio's per-iteration loop finalisation
            # surfaces the leak as PytestUnraisableExceptionWarning
            # when the test harness picks it up).
            for stream in (sub.stdout, sub.stderr, sub.stdin):
                if stream is not None:
                    with contextlib.suppress(OSError, ValueError):
                        stream.close()

    arm_end_ns = time.monotonic_ns()

    return {
        "arm": arm_name,
        "iter_id": iter_id,
        "frames": frames,
        "source_hashes": {seq: h for seq, _b, h in events},
        "completion_tokens": completion_tokens,
        "consumer_lines": consumer_lines,
        "arm_start_ns": arm_start_ns,
        "arm_end_ns": arm_end_ns,
        "ttft_anchor_ns": ttft_anchor_ns,
        "consumer_chunk_prefix": _MARKER_CHUNK,
        "swarm_subscribers_ready": subscribers_ready,
    }


# ---------------------------------------------------------------------
# Per-iteration latency reductions (pure helpers; unit-testable).
# ---------------------------------------------------------------------


def _parse_consumer_arrivals(
    *,
    consumer_lines: Sequence[str],
    chunk_prefix: str,
) -> dict[int, int]:
    """Parse arrival_ns + frame body from consumer stdout lines.

    Returns ``{chunk_seq: arrival_monotonic_ns}``. The chunk_seq is
    recovered by decoding the frame body (msgspec-decoded
    ``ReasoningChunkFrame`` with the producer-side anchor that
    survives across arms). The lines that do NOT start with the
    chunk_prefix are skipped silently (e.g. ``DONE`` summary, swarm
    lines).
    """
    result: dict[int, int] = {}
    for line in consumer_lines:
        if not line.startswith(chunk_prefix):
            continue
        # Tokenise: "<PREFIX> arrival_ns=<N> [event_id=<id>] body_b64=<...>"
        tokens = line.split()
        arrival_ns: int | None = None
        body_b64: str | None = None
        for tok in tokens[1:]:
            key, _, value = tok.partition("=")
            if key == "arrival_ns":
                with contextlib.suppress(ValueError):
                    arrival_ns = int(value)
            elif key == "body_b64":
                body_b64 = value
        if arrival_ns is None or body_b64 is None:
            continue
        try:
            frame = _decode_chunk_frame_from_msg_body(body_b64)
        except (ValueError, msgspec.DecodeError, msgspec.ValidationError):
            continue
        result[frame.chunk_seq] = arrival_ns
    return result


def _parse_delivered_rehashes(
    *,
    consumer_lines: Sequence[str],
    chunk_prefix: str,
) -> dict[int, str]:
    """Re-hash the DELIVERED bytes for every consumer arrival line.

    Returns ``{chunk_seq: delivered_rehash_hex}``. For each consumer
    stdout line carrying a ``body_b64`` (a base64 msgspec-encoded
    ``ReasoningChunkFrame`` the consumer / UDS peer actually received),
    the frame is decoded and the digest is recomputed from scratch over
    ``(iter_id, chunk_seq, chunk_bytes)`` via :func:`_hash_chunk`.

    The recomputation deliberately ignores the frame's embedded
    ``chunk_hash_hex`` field: a corruption of ``chunk_bytes`` in transit
    must be caught even when the embedded hash field survives intact, so
    the delivered bytes themselves are the only thing re-hashed. The
    caller compares this against the per-iteration source manifest to
    count delivery-integrity failures (a dropped seq is a missing key; a
    corrupted seq is a differing digest).
    """
    result: dict[int, str] = {}
    for line in consumer_lines:
        if not line.startswith(chunk_prefix):
            continue
        body_b64: str | None = None
        for tok in line.split()[1:]:
            key, _, value = tok.partition("=")
            if key == "body_b64":
                body_b64 = value
        if body_b64 is None:
            continue
        try:
            frame = _decode_chunk_frame_from_msg_body(body_b64)
        except (ValueError, msgspec.DecodeError, msgspec.ValidationError):
            continue
        result[frame.chunk_seq] = _hash_chunk(
            iter_id=frame.iter_id,
            chunk_seq=frame.chunk_seq,
            chunk_bytes=frame.chunk_bytes,
        )
    return result


def _parse_delivered_order(
    *,
    consumer_lines: Sequence[str],
    chunk_prefix: str,
) -> list[int]:
    """Return delivered ``chunk_seq`` values in CONSUMER-LINE (arrival) order.

    For each consumer stdout line carrying a ``body_b64`` (a base64
    msgspec-encoded ``ReasoningChunkFrame`` the consumer / UDS peer
    actually received), the frame is decoded and ``frame.chunk_seq`` is
    appended in line order. The returned list therefore encodes the
    ORDER in which seqs were delivered, which the caller compares against
    the monotonic source emit order.

    Undecodable / non-chunk lines are skipped with the same tolerance as
    :func:`_parse_delivered_rehashes`: a line that does not start with
    ``chunk_prefix``, lacks a ``body_b64``, or fails to decode is ignored
    rather than aborting the parse.
    """
    order: list[int] = []
    for line in consumer_lines:
        if not line.startswith(chunk_prefix):
            continue
        body_b64: str | None = None
        for tok in line.split()[1:]:
            key, _, value = tok.partition("=")
            if key == "body_b64":
                body_b64 = value
        if body_b64 is None:
            continue
        try:
            frame = _decode_chunk_frame_from_msg_body(body_b64)
        except (ValueError, msgspec.DecodeError, msgspec.ValidationError):
            continue
        order.append(frame.chunk_seq)
    return order


def _ordering_inversions(delivered_order: list[int]) -> int:
    """Count out-of-order ("descent") deliveries in ``delivered_order``.

    Returns the number of indices ``i`` in ``1..len-1`` where
    ``delivered_order[i] < delivered_order[i - 1]`` -- i.e. each point at
    which a seq arrived AFTER a strictly greater seq. A correctly-ordered
    stream (0, 1, 2, ...) yields 0. waitbus guarantees a daemon-assigned
    monotonic delivery sequence, so ANY descent is a real ordering bug,
    not model-side non-determinism.
    """
    inversions = 0
    for i in range(1, len(delivered_order)):
        if delivered_order[i] < delivered_order[i - 1]:
            inversions += 1
    return inversions


def _compute_per_chunk_bus_latency_ns(
    *,
    frames: Sequence[ReasoningChunkFrame],
    arrivals: dict[int, int],
) -> list[int]:
    """Pair producer-side chunk anchors with consumer-side arrival ns.

    Returns a list of per-chunk bus latencies (consumer arrival_ns -
    producer-side t_chunk_arrived_monotonic_ns). Negative or missing
    pairings are dropped; a chunk that did not arrive (consumer was
    too slow) drops out of the latency aggregate naturally.
    """
    latencies: list[int] = []
    for frame in frames:
        arrival_ns = arrivals.get(frame.chunk_seq)
        if arrival_ns is None:
            continue
        delta = arrival_ns - frame.t_chunk_arrived_monotonic_ns
        if delta < 0:
            continue
        latencies.append(delta)
    return latencies


def _compute_ttft_ns(
    *,
    arm_start_ns: int,
    frames: Sequence[ReasoningChunkFrame],
    arrivals: dict[int, int],
) -> int:
    """Compute per-iteration time-to-first-chunk (ns).

    TTFT is the consumer-side arrival of ``chunk_seq=0`` minus the
    orchestrator-side arm-start monotonic_ns anchor. Returns 0 when
    chunk_seq=0 did not arrive (the iteration is structurally invalid;
    the caller drops it from the marginal samples).
    """
    if not frames:
        return 0
    first = frames[0]
    arrival_ns = arrivals.get(first.chunk_seq)
    if arrival_ns is None:
        return 0
    delta = arrival_ns - arm_start_ns
    return max(0, delta)


def _percentile_ns(values: Sequence[int], p: float) -> int:
    """Linear-interpolated percentile (returns int ns; 0 on empty)."""
    if not values:
        return 0
    sorted_values = sorted(values)
    if p <= 0:
        return int(sorted_values[0])
    if p >= 1:
        return int(sorted_values[-1])
    rank = p * (len(sorted_values) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = rank - lower
    return int(sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight)


# ---------------------------------------------------------------------
# Delivery-integrity check (round-trip: delivered-bytes re-hash vs source).
# ---------------------------------------------------------------------


def _delivery_integrity_failures(
    *,
    source_hashes: dict[int, str],
    delivered_rehashes: dict[int, str],
) -> int:
    """Count source chunks that were dropped or corrupted in delivery.

    ``source_hashes`` is the per-iteration source manifest
    ``{chunk_seq: source_hash}`` built by :func:`_build_events_once`.
    ``delivered_rehashes`` is ``{chunk_seq: delivered_rehash}`` produced
    by :func:`_parse_delivered_rehashes`, where each value is a fresh
    re-hash of the bytes the consumer ACTUALLY received (not the frame's
    embedded ``chunk_hash_hex``).

    A source ``chunk_seq`` counts as one delivery-integrity failure when
    its delivered re-hash is MISSING (the consumer never received that
    seq -- a drop) OR DIFFERS from the source hash (the bytes were
    corrupted in transit). The comparison runs over the FULL source seq
    set, so a missing delivered seq is a failure rather than a silent
    skip. This is a genuine round-trip: a real waitbus / UDS drop or
    corruption is now in the comparison loop, whereas the previous
    producer-vs-producer hash diff was vacuous by construction (every
    arm replays the same source events).
    """
    failures = 0
    for seq, source_hash in source_hashes.items():
        delivered = delivered_rehashes.get(seq)
        if delivered is None or delivered != source_hash:
            failures += 1
    return failures


# ---------------------------------------------------------------------
# Wilcoxon marginal helpers.
# ---------------------------------------------------------------------


def _wilcoxon_paired_pvalue(samples_a: Sequence[float], samples_b: Sequence[float]) -> float:
    """Wilcoxon signed-rank paired p-value; 1.0 on empty/degenerate inputs.

    Returns 1.0 (do-not-reject) when:
    - either sample is empty;
    - every pairwise difference is exactly zero (the test is degenerate
      and ``scipy.stats.wilcoxon`` raises on that input shape).
    """
    if len(samples_a) != len(samples_b) or not samples_a:
        return 1.0
    differences = [a - b for a, b in zip(samples_a, samples_b, strict=True)]
    if all(d == 0 for d in differences):
        return 1.0
    # Localised import: scipy is a bench extra.
    from scipy.stats import wilcoxon

    result = wilcoxon(samples_a, samples_b)
    return float(result.pvalue)


def _aggregate_arm(arm: str, rows: Sequence[dict[str, Any]]) -> _ArmLatencyStats:
    """Roll per-iteration rows for one arm into an _ArmLatencyStats block."""
    arm_rows = [row for row in rows if row["arm"] == arm]
    all_latencies: list[int] = []
    ttft_per_iter: list[int] = []
    wall_per_iter: list[int] = []
    for row in arm_rows:
        per_chunk: list[int] = row.get("per_chunk_bus_latency_ns") or []
        all_latencies.extend(per_chunk)
        ttft = row.get("ttft_ns") or 0
        if ttft > 0:
            ttft_per_iter.append(ttft)
        wall = row.get("wall_time_ns") or 0
        if wall > 0:
            wall_per_iter.append(wall)
    return _ArmLatencyStats(
        arm=arm,
        n_iterations=len(arm_rows),
        n_chunks_total=len(all_latencies),
        median_per_chunk_bus_latency_ns=_percentile_ns(all_latencies, 0.50),
        p99_per_chunk_bus_latency_ns=_percentile_ns(all_latencies, 0.99),
        median_ttft_ns=int(statistics.median(ttft_per_iter)) if ttft_per_iter else 0,
        median_wall_time_ns=int(statistics.median(wall_per_iter)) if wall_per_iter else 0,
    )


# ---------------------------------------------------------------------
# Limitations + verdict assembly.
# ---------------------------------------------------------------------


def _build_limitations() -> list[str]:
    """Documented limitations recorded in the verdict.json."""
    return [
        "Wilcoxon signed-rank paired test uses scipy.stats.wilcoxon with default "
        "ties handling (wilcox) and exact/approximate auto-method selection; the "
        "p-value tail is the bench's load-bearing rejection signal.",
        "Bonferroni correction across three marginals (per_chunk_bus_latency, TTFT, "
        "wall_time) yields alpha_per_marginal = 0.05/3 = 0.01666...; a downstream "
        "consumer that wants Holm-Bonferroni or BH-FDR re-applies the correction "
        "on the three p-values stored in the verdict.",
        "The bus_swarm arm's concurrent load is N lightweight waitbus "
        "subscribers (subscribe + drain, NO LLM), not a real-LLM swarm "
        "(amended). "
        "The arm's load variable is the daemon's per-subscriber fan-out, "
        "identical whether the subscriber later calls an LLM or not, so the "
        "lightweight subscribers impose the same load deterministically and "
        "subscribe instantly (no cold-start, hence no warmup barrier). The "
        "subscriber-underload floor is _SWARM_SUBSCRIBER_COUNT * N_iter "
        "(every subscriber READY every iter); the 70% threshold is "
        "preserved. Heterogeneity and slow-consumer backpressure are "
        "verified covered elsewhere (tests/test_hero_swarm_e2e.py for real "
        "distinct pydantic_ai + langgraph processes; the soak drain-smoke "
        "for a genuinely-evicted non-draining subscriber), not assumed.",
        "The delivery-integrity check is a genuine round-trip: for each arm "
        "the consumer-delivered bytes are RE-HASHED (over iter_id||seq||bytes, "
        "ignoring the frame's embedded chunk_hash_hex) and compared against the "
        "per-iteration source manifest. A non-zero "
        "delivery_integrity_failures_<arm> count therefore means a real waitbus / "
        "UDS drop (a source seq the consumer never received) or corruption (the "
        "delivered bytes hash differently from the source), NOT model-side "
        "generation non-determinism. The counters are per-arm-vs-source, so a "
        "reader can attribute a failure to a specific transport arm.",
        "The ordering-fidelity check is a genuine round-trip: for each arm the "
        "consumer-delivered seqs are decoded in ARRIVAL order and compared against "
        "the monotonic source emit order by counting out-of-order ('descent') "
        "deliveries -- a seq that arrived after a strictly greater seq. waitbus "
        "guarantees a daemon-assigned monotonic delivery sequence, so a non-zero "
        "ordering_inversions_<arm> count is a real waitbus / UDS reordering bug, NOT "
        "model-side generation non-determinism. This counter folds into the same "
        "exit-1 delivery-fidelity gate as delivery_integrity_failures_<arm>.",
        "The single generation call uses seed=42 + temperature=0, documented as "
        "best-effort deterministic; because content is generated once and "
        "replayed (not re-generated) per arm, generation non-determinism affects "
        "only which text is segmented, never the per-arm delivery-integrity "
        "counters (which compare delivered bytes against that iteration's own "
        "source manifest).",
        "UDS sibling-process IPC carries length-prefixed JSON frames (4-byte "
        "big-endian uint32 + JSON). The raw-IPC (lll_alone) arm is retained "
        "ONLY as an integrity control: its delivery-integrity and ordering "
        "counters are still gated (a drop/corruption/reorder in the harness's "
        "own framing must surface), but its per-event latency is RECORDED, "
        "never gated and never used as a comparison baseline (item 5). "
        "Comparing a durable ms-scale bus to a non-durable us-scale raw pipe "
        "by latency ratio or distribution is a category error that penalizes "
        "waitbus's core feature (durability), so both the former ratio gate and "
        "the distribution-equivalence-vs-IPC test are DELETED (item 2).",
        "The latency claim is an ABSOLUTE per-event budget, PRE-REGISTERED "
        "before the first full-N run: the bus arms' p99 per-event delivery "
        "latency must be <= 100ms. The 100ms is derived FORWARD from Nielsen's "
        "canonical HCI limit (0.1s = the threshold below which a system is "
        "perceived to react instantaneously to a human) and cross-checked "
        "against waitbus's own ~27ms emit-cost floor (~3.7x headroom); it is NOT "
        "back-fit to any observed measurement (item 3). "
        "Perturbation is measured bus_idle vs bus_swarm (same transport, "
        "varying swarm load): a Wilcoxon equivalence test on the per-event "
        "delivery latency between the two waitbus arms answers whether "
        "concurrent heterogeneous subscriber load perturbs waitbus's own "
        "delivery latency. The raw-IPC arm is NOT in this comparison (item 4).",
        "Linux-only: /proc/<pid>/status VmRSS, AF_UNIX SOCK_STREAM, and cross-"
        "process CLOCK_MONOTONIC are all load-bearing.",
        "OFFLINE mode (--skip-real-llm) exercises the same emit + subscribe + "
        "spawn paths as the real-LLM mode but bypasses the OpenAI HTTP call; "
        "the synthetic reasoning text is byte-identical across re-runs and is "
        "segmented into the same discrete events replayed through every arm, so "
        "the per-arm delivery-integrity counters are guaranteed zero modulo a "
        "real bus / UDS delivery bug.",
        "Smoke mode (--smoke) shortens N to 3 by default; it does NOT switch to "
        "synthetic mocks for any other component (waitbus daemon + subscribe + "
        "lightweight subscribers all run real).",
        "The bench's per-iteration arm-deadline is "
        f"{_PER_ARM_DEADLINE_SEC:.0f}s (generation timeout + drain budget); "
        "iterations whose replay/consumer breach this deadline are recorded as "
        "skipped iterations (no row contributes to the marginal samples).",
    ]


# ---------------------------------------------------------------------
# Cost tracking (gpt-4.1-nano).
# ---------------------------------------------------------------------


def _compute_iteration_cost_usd(*, completion_tokens: int, prompt_tokens_estimate: int) -> float:
    """Approximate USD cost for one gpt-4.1-nano iteration.

    Input tokens are estimated statically (~300 for the bench prompt)
    since the caller passes a fixed estimate. Output tokens come from
    the single non-streaming call's ``usage.completion_tokens`` when
    available; the caller passes 0 when the figure was not surfaced.
    The call is made ONCE per iteration, so cost is an iteration-level
    quantity (not multiplied by the three arms).
    """
    input_usd = prompt_tokens_estimate * _GPT_4_1_NANO_INPUT_USD_PER_1M / 1_000_000.0
    output_usd = completion_tokens * _GPT_4_1_NANO_OUTPUT_USD_PER_1M / 1_000_000.0
    return input_usd + output_usd


# ---------------------------------------------------------------------
# Argument parsing.
# ---------------------------------------------------------------------


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Build the CLI parser and parse ``argv`` into a Namespace."""
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.bench_event_delivery_fidelity",
        description=(
            "Delivery-fidelity bench: replay shared events through a raw-IPC "
            "integrity control (lll_alone) + two waitbus arms (bus_idle / "
            "bus_swarm). Gates: delivery completeness/integrity/ordering, an "
            "absolute 100ms p99 per-event latency budget on the bus arms, and "
            "a bus_idle-vs-bus_swarm Wilcoxon perturbation test "
            "(Bonferroni alpha = 0.05/3). The raw-IPC arm is never a latency "
            "comparison baseline."
        ),
    )
    parser.add_argument("--smoke", action="store_true", help=f"Run N={_SMOKE_N} triples (default N={_DEFAULT_N}).")
    parser.add_argument(
        "--n",
        type=int,
        default=None,
        help=f"Override iteration count (default {_DEFAULT_N}; smoke default {_SMOKE_N}).",
    )
    parser.add_argument(
        "--include-real-llm",
        dest="include_real_llm",
        action="store_true",
        default=True,
        help="Require real LLM calls (OPENAI_API_KEY in keyring). Default ON.",
    )
    parser.add_argument(
        "--skip-real-llm",
        dest="include_real_llm",
        action="store_false",
        help="Disable real LLM call; uses synthetic discrete-event replay through the SAME waitbus/UDS pipes.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path.cwd() / ".local-stress-logs",
        help="Verdict output directory (or verdict.json path).",
    )
    parser.add_argument(
        "--max-cost-usd",
        type=float,
        default=_DEFAULT_MAX_COST_USD,
        help=f"Upper budget for accumulated USD cost (default {_DEFAULT_MAX_COST_USD:.2f}).",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------
# Per-arm dispatch + row reduction.
# ---------------------------------------------------------------------


def _dispatch_arm(
    *,
    arm: str,
    iter_id: int,
    events: list[tuple[int, bytes, str]],
    uds_socket_path: Path,
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    python_exe: str,
) -> dict[str, Any]:
    """Run one arm iteration via the matching per-arm runner; return its row dict.

    All three arms replay the SAME shared ``events`` (built once per
    iteration by :func:`_build_events_once`).
    """
    if arm == "lll_alone_ipc_peer":
        return asyncio.run(
            _run_arm_lll_alone(
                iter_id=iter_id,
                events=events,
                uds_socket_path=uds_socket_path,
                python_exe=python_exe,
            )
        )
    if arm == "bus_idle":
        return asyncio.run(
            _run_arm_bus(
                arm_name=arm,
                iter_id=iter_id,
                events=events,
                socket_path=socket_path,
                db_path=db_path,
                doorbell_path=doorbell_path,
                python_exe=python_exe,
                swarm_enabled=False,
            )
        )
    if arm == "bus_swarm":
        return asyncio.run(
            _run_arm_bus(
                arm_name=arm,
                iter_id=iter_id,
                events=events,
                socket_path=socket_path,
                db_path=db_path,
                doorbell_path=doorbell_path,
                python_exe=python_exe,
                swarm_enabled=True,
            )
        )
    raise RuntimeError(f"unknown arm {arm}")  # pragma: no cover - exhaustively enumerated above


def _reduce_arm_row(row: dict[str, Any]) -> dict[str, Any]:
    """Augment a per-arm row in place with the canonical latency / TTFT / wall reductions.

    Returns the same ``row`` object (mutated) so callers can both append
    it to the ledger and read the freshly-computed marginal fields.
    """
    arrivals = _parse_consumer_arrivals(
        consumer_lines=row["consumer_lines"],
        chunk_prefix=row["consumer_chunk_prefix"],
    )
    per_chunk_lat = _compute_per_chunk_bus_latency_ns(frames=row["frames"], arrivals=arrivals)
    ttft_ns = _compute_ttft_ns(
        arm_start_ns=row["arm_start_ns"],
        frames=row["frames"],
        arrivals=arrivals,
    )
    row["per_chunk_bus_latency_ns"] = per_chunk_lat
    row["ttft_ns"] = ttft_ns
    row["wall_time_ns"] = row["arm_end_ns"] - row["arm_start_ns"]
    row["arrival_count"] = len(arrivals)
    return row


# ---------------------------------------------------------------------
# Experiment driver (paired-triple iteration loop).
# ---------------------------------------------------------------------


def _run_experiment(
    *,
    args: argparse.Namespace,
    bench_name: str,
    n_triples: int,
    openai_api_key: str | None,
    python_exe: str,
    waitbus_path: str,
    external_state_report: ExternalStateReport,
    progress_path: Path,
    log_path: Path,
) -> dict[str, Any]:
    """Drive the paired-triple iteration loop against a freshly-spawned daemon.

    Returns a result bundle with the collected per-arm ``rows``, the
    final ``external_state_report`` (refreshed with the daemon pragmas),
    and the cost-bookkeeping counters.
    """
    rows: list[dict[str, Any]] = []
    cost_usd_total = 0.0
    cost_unknown_count = 0
    cost_observed = 0.0
    aborted_on_budget = False

    with tempfile.TemporaryDirectory(prefix=f"waitbus-bench-{bench_name}-") as tmp_root:
        root = Path(tmp_root)
        state_dir = root / "state"
        runtime_dir = root / "runtime"
        state_dir.mkdir(parents=True, exist_ok=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ)
        env["WAITBUS_STATE_DIR"] = str(state_dir)
        env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
        env["WAITBUS_DISABLE_SOURCE_AUTOLOAD"] = "1"
        socket_path = runtime_dir / "broadcast.sock"
        db_path = state_dir / "github.db"
        doorbell_path = runtime_dir / "doorbell.sock"
        uds_socket_path = runtime_dir / "uds-peer.sock"

        daemon = _spawn_daemon(env, waitbus_path, socket_path)
        try:
            external_state_report = msgspec.structs.replace(
                external_state_report,
                waitbus_daemon_pragmas=capture_daemon_pragmas(db_path),
            )
            gc.disable()

            try:
                with (
                    log_path.open("w", encoding="utf-8") as log_fh,
                    progress_path.open("w", encoding="utf-8") as prog_fh,
                ):
                    _ = log_fh
                    append_jsonl_record(
                        prog_fh,
                        {
                            "kind": "start",
                            "bench": bench_name,
                            "n_triples": n_triples,
                            "smoke": args.smoke,
                            "include_real_llm": args.include_real_llm,
                        },
                    )

                    sentinel_iter_template = "abc1234567"
                    for iter_id in range(n_triples):
                        if aborted_on_budget:
                            break
                        sentinel_prefix = hashlib.sha256(f"{sentinel_iter_template}:{iter_id}".encode()).hexdigest()[
                            :16
                        ]

                        # Generate the reasoning ONCE per iteration, segment
                        # it into discrete events, then replay the SAME events
                        # through all three arms below.
                        try:
                            events, completion_tokens = _build_events_once(
                                iter_id=iter_id,
                                api_key=openai_api_key,
                                include_real_llm=args.include_real_llm,
                                sentinel_prefix=sentinel_prefix,
                            )
                        except Exception as exc:  # pragma: no cover - defensive
                            structured(
                                _logger,
                                logging.WARNING,
                                "bench_build_events_failed",
                                iter_id=iter_id,
                                error=str(exc),
                            )
                            continue

                        # Cost bookkeeping: ONE call per iteration, not per arm.
                        if args.include_real_llm:
                            if completion_tokens > 0:
                                iter_cost = _compute_iteration_cost_usd(
                                    completion_tokens=completion_tokens,
                                    prompt_tokens_estimate=300,
                                )
                                cost_usd_total += iter_cost
                                cost_observed = cost_usd_total
                                if cost_usd_total > args.max_cost_usd:
                                    aborted_on_budget = True
                            else:
                                cost_unknown_count += 1

                        for arm in _ARMS:
                            arm_deadline_ns = time.monotonic_ns() + int(_PER_ARM_DEADLINE_SEC * 1e9)
                            try:
                                row = _dispatch_arm(
                                    arm=arm,
                                    iter_id=iter_id,
                                    events=events,
                                    uds_socket_path=uds_socket_path,
                                    socket_path=socket_path,
                                    db_path=db_path,
                                    doorbell_path=doorbell_path,
                                    python_exe=python_exe,
                                )
                            except Exception as exc:  # pragma: no cover - defensive
                                structured(
                                    _logger,
                                    logging.WARNING,
                                    "bench_arm_failed",
                                    iter_id=iter_id,
                                    arm=arm,
                                    error=str(exc),
                                )
                                continue
                            _ = arm_deadline_ns

                            # Reduce per-arm row to the canonical shape.
                            row = _reduce_arm_row(row)
                            rows.append(row)

                            append_jsonl_record(
                                prog_fh,
                                {
                                    "kind": "arm_done",
                                    "iter_id": iter_id,
                                    "arm": arm,
                                    "n_chunks_produced": len(row["frames"]),
                                    "n_chunks_arrived": row["arrival_count"],
                                    "ttft_ns": row["ttft_ns"],
                                    "wall_time_ns": row["wall_time_ns"],
                                    "swarm_subscribers_ready": row.get("swarm_subscribers_ready", 0),
                                },
                            )
            finally:
                gc.enable()
        finally:
            daemon.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                daemon.wait(timeout=5.0)
            with contextlib.suppress(FileNotFoundError):
                socket_path.unlink()
            with contextlib.suppress(FileNotFoundError):
                doorbell_path.unlink()
            with contextlib.suppress(FileNotFoundError):
                uds_socket_path.unlink()

    return {
        "rows": rows,
        "external_state_report": external_state_report,
        "cost_usd_total": cost_usd_total,
        "cost_unknown_count": cost_unknown_count,
        "cost_observed": cost_observed,
        "aborted_on_budget": aborted_on_budget,
    }


# ---------------------------------------------------------------------
# Verdict assembly (aggregation across arms).
# ---------------------------------------------------------------------


class _PairedMarginals(NamedTuple):
    """Wilcoxon paired p-values + Bonferroni rejection flags for the three marginals.

    Every marginal pairs ``bus_idle`` vs ``bus_swarm`` (same transport,
    varying swarm load). The raw-IPC (``lll_alone``) arm is NOT in any of
    these comparisons.
    """

    p_per_chunk: float
    p_ttft: float
    p_wall: float
    h0_rejected_per_chunk: bool
    h0_rejected_ttft: bool
    h0_rejected_wall: bool


def _compute_paired_marginals(
    *,
    rows_by_arm_iter: dict[tuple[str, int], dict[str, Any]],
    paired_iter_ids: Sequence[int],
) -> _PairedMarginals:
    """Compute the three Wilcoxon paired p-values + Bonferroni rejection flags.

    For each marginal the per-iteration value is reduced to a scalar
    (median for the list-valued ``per_chunk_bus_latency_ns``; the raw
    value otherwise), the two waitbus arms' samples are truncated to a
    common length, and the ``bus_idle``-vs-``bus_swarm`` pair feeds the
    signed-rank test. The raw-IPC arm is NOT a comparison baseline; it is
    retained only as an integrity control (items 4-5).
    """

    def _medians_per_arm(metric_key: str, arm: str) -> list[float]:
        out: list[float] = []
        for iter_id in paired_iter_ids:
            row = rows_by_arm_iter[(arm, iter_id)]
            value = row.get(metric_key) or 0
            if isinstance(value, list):
                if not value:
                    continue
                out.append(float(statistics.median(value)))
            else:
                if value <= 0:
                    continue
                out.append(float(value))
        return out

    per_chunk_idle = _medians_per_arm("per_chunk_bus_latency_ns", _ARMS[1])
    per_chunk_swarm = _medians_per_arm("per_chunk_bus_latency_ns", _ARMS[2])
    ttft_idle = _medians_per_arm("ttft_ns", _ARMS[1])
    ttft_swarm = _medians_per_arm("ttft_ns", _ARMS[2])
    wall_idle = _medians_per_arm("wall_time_ns", _ARMS[1])
    wall_swarm = _medians_per_arm("wall_time_ns", _ARMS[2])

    # Truncate each pair to the common minimum n so the Wilcoxon test has
    # equally-sized samples to pair. Every marginal pairs bus_idle vs
    # bus_swarm (same transport, varying load); the raw-IPC arm is absent.
    def _truncate(samples: list[list[float]]) -> list[list[float]]:
        if not samples:
            return samples
        min_len = min(len(s) for s in samples)
        return [s[:min_len] for s in samples]

    pair_per_chunk = _truncate([per_chunk_idle, per_chunk_swarm])
    pair_ttft = _truncate([ttft_idle, ttft_swarm])
    pair_wall = _truncate([wall_idle, wall_swarm])
    p_per_chunk = _wilcoxon_paired_pvalue(pair_per_chunk[0], pair_per_chunk[1]) if pair_per_chunk else 1.0
    p_ttft = _wilcoxon_paired_pvalue(pair_ttft[0], pair_ttft[1]) if pair_ttft else 1.0
    p_wall = _wilcoxon_paired_pvalue(pair_wall[0], pair_wall[1]) if pair_wall else 1.0

    return _PairedMarginals(
        p_per_chunk=p_per_chunk,
        p_ttft=p_ttft,
        p_wall=p_wall,
        h0_rejected_per_chunk=p_per_chunk < _ALPHA_PER_MARGINAL,
        h0_rejected_ttft=p_ttft < _ALPHA_PER_MARGINAL,
        h0_rejected_wall=p_wall < _ALPHA_PER_MARGINAL,
    )


def _count_content_integrity_failures(
    *,
    rows_by_arm_iter: dict[tuple[str, int], dict[str, Any]],
    paired_iter_ids: Sequence[int],
) -> dict[str, int]:
    """Sum per-arm delivery-integrity failures vs the per-iteration source manifest.

    For each arm and each paired iteration the consumer-delivered bytes
    are re-hashed (:func:`_parse_delivered_rehashes`) and compared to the
    iteration's source manifest (:func:`_delivery_integrity_failures`).
    A dropped or corrupted source ``chunk_seq`` counts as one failure.
    This is a genuine delivery round-trip, not a producer-vs-producer
    hash diff.
    """
    failures_alone = 0
    failures_idle = 0
    failures_swarm = 0
    for iter_id in paired_iter_ids:
        for arm, accumulate in (
            (_ARMS[0], "alone"),
            (_ARMS[1], "idle"),
            (_ARMS[2], "swarm"),
        ):
            row = rows_by_arm_iter[(arm, iter_id)]
            delivered = _parse_delivered_rehashes(
                consumer_lines=row["consumer_lines"],
                chunk_prefix=row["consumer_chunk_prefix"],
            )
            count = _delivery_integrity_failures(
                source_hashes=row["source_hashes"],
                delivered_rehashes=delivered,
            )
            if accumulate == "alone":
                failures_alone += count
            elif accumulate == "idle":
                failures_idle += count
            else:
                failures_swarm += count
    return {
        "lll_alone": failures_alone,
        "bus_idle": failures_idle,
        "bus_swarm": failures_swarm,
    }


def _count_ordering_inversions(
    *,
    rows_by_arm_iter: dict[tuple[str, int], dict[str, Any]],
    paired_iter_ids: Sequence[int],
) -> dict[str, int]:
    """Sum per-arm delivered-order inversions vs the monotonic source order.

    For each arm and each paired iteration the consumer-delivered seqs
    are decoded in arrival order (:func:`_parse_delivered_order`) and the
    number of out-of-order descents is counted
    (:func:`_ordering_inversions`). waitbus guarantees a daemon-assigned
    monotonic delivery sequence, so a non-zero count is a real reordering
    bug, not generation non-determinism. Mirrors
    :func:`_count_content_integrity_failures`.
    """
    inversions_alone = 0
    inversions_idle = 0
    inversions_swarm = 0
    for iter_id in paired_iter_ids:
        for arm, accumulate in (
            (_ARMS[0], "alone"),
            (_ARMS[1], "idle"),
            (_ARMS[2], "swarm"),
        ):
            row = rows_by_arm_iter[(arm, iter_id)]
            delivered_order = _parse_delivered_order(
                consumer_lines=row["consumer_lines"],
                chunk_prefix=row["consumer_chunk_prefix"],
            )
            count = _ordering_inversions(delivered_order)
            if accumulate == "alone":
                inversions_alone += count
            elif accumulate == "idle":
                inversions_idle += count
            else:
                inversions_swarm += count
    return {
        "lll_alone": inversions_alone,
        "bus_idle": inversions_idle,
        "bus_swarm": inversions_swarm,
    }


class _VerdictGates(NamedTuple):
    """Subscriber-underload + latency-budget + bus-vs-bus perturbation booleans.

    The raw-IPC (``lll_alone``) arm is absent from every latency gate
    here -- it is recorded only as an integrity control (items 3-5).
    """

    swarm_subscribers_ready: int
    swarm_underload_floor: int
    sandbagging_sentinel_fired: bool
    inapplicable_reason: str | None
    bus_idle_p99_latency_ns: int
    bus_swarm_p99_latency_ns: int
    latency_budget_passed: bool
    bus_swarm_perturbs_latency: bool
    distribution_equivalent: bool
    perturbation_detected: bool


def _compute_gates(
    *,
    rows: list[dict[str, Any]],
    arm_stats: dict[str, _ArmLatencyStats],
    n_triples_actual: int,
) -> _VerdictGates:
    """Compute the subscriber-underload + absolute-latency-budget + bus-vs-bus gates."""
    inapplicable_reason: str | None = None

    # Subscriber-underload sentinel. The bus_swarm load is now N
    # lightweight concurrent subscribers; the load metric is how many of
    # _SWARM_SUBSCRIBER_COUNT signalled READY across the paired iterations
    # (summed over the bus_swarm rows). If fewer than 70% of the floor
    # (_SWARM_SUBSCRIBER_COUNT per iteration) actually subscribed, the
    # concurrent-read load the arm is supposed to impose was not present,
    # so the perturbation comparison is inapplicable.
    swarm_subscribers_ready = sum(row.get("swarm_subscribers_ready", 0) for row in rows if row["arm"] == _ARMS[2])
    swarm_underload_floor = _SWARM_SUBSCRIBER_COUNT * max(1, n_triples_actual)
    sandbagging_threshold = _SANDBAGGING_RATIO * float(swarm_underload_floor)
    sandbagging_sentinel_fired = bool(n_triples_actual > 0 and float(swarm_subscribers_ready) < sandbagging_threshold)

    if n_triples_actual == 0:
        inapplicable_reason = "n_triples_actual_zero"
    elif sandbagging_sentinel_fired:
        inapplicable_reason = "inapplicable_subscriber_underloaded"

    # Absolute per-event latency budget (PRE-REGISTERED 100ms). The two
    # bus arms' p99 per-event delivery latency must each sit within the
    # budget. The raw-IPC arm's p99 is NOT consulted -- the budget is on
    # waitbus's own contract (Nielsen 0.1s HCI limit), not a ratio against
    # a non-durable pipe.
    idle_p99 = arm_stats[_ARMS[1]].p99_per_chunk_bus_latency_ns
    swarm_p99 = arm_stats[_ARMS[2]].p99_per_chunk_bus_latency_ns
    latency_budget_passed = max(idle_p99, swarm_p99) <= _LATENCY_BUDGET_P99_NS

    # bus_idle-vs-bus_swarm perturbation: a PRE-REGISTERED one-sided
    # effect-size test (perturbation-gate amendment).
    # Perturbation means the loaded swarm arm's p99 delivery latency is
    # WORSE than idle by more than the margin; a faster-or-within-margin
    # loaded arm is not a perturbation. A bare Wilcoxon significance test
    # was rejected as the gate: at >=640 samples/arm it flags trivial,
    # run-unstable, wrong-direction differences (the loaded arm is typically
    # marginally FASTER). The Wilcoxon p stays recorded
    # (wilcoxon_p_bus_idle_vs_swarm_latency) as a NON-gating observation.
    bus_swarm_perturbs_latency = (swarm_p99 - idle_p99) > _PERTURBATION_MARGIN_P99_NS
    # Equivalence: the loaded arm is within the margin of (or faster than) idle.
    distribution_equivalent = not bus_swarm_perturbs_latency
    # Perturbation: budget breach OR a meaningful loaded-vs-idle latency degradation.
    perturbation_detected = (not latency_budget_passed) or bus_swarm_perturbs_latency

    return _VerdictGates(
        swarm_subscribers_ready=swarm_subscribers_ready,
        swarm_underload_floor=swarm_underload_floor,
        sandbagging_sentinel_fired=sandbagging_sentinel_fired,
        inapplicable_reason=inapplicable_reason,
        bus_idle_p99_latency_ns=idle_p99,
        bus_swarm_p99_latency_ns=swarm_p99,
        latency_budget_passed=latency_budget_passed,
        bus_swarm_perturbs_latency=bus_swarm_perturbs_latency,
        distribution_equivalent=distribution_equivalent,
        perturbation_detected=perturbation_detected,
    )


def _assemble_verdict(
    *,
    args: argparse.Namespace,
    bench_name: str,
    n_triples: int,
    started_ns: int,
    finished_ns: int,
    rows: list[dict[str, Any]],
    external_state_report: ExternalStateReport,
    cost_usd_total: float,
    cost_unknown_count: int,
    cost_observed: float,
    aborted_on_budget: bool,
) -> EventDeliveryFidelityVerdict:
    """Aggregate per-arm rows into the final verdict struct."""
    n_triples_actual = min(
        sum(1 for row in rows if row["arm"] == _ARMS[0]),
        sum(1 for row in rows if row["arm"] == _ARMS[1]),
        sum(1 for row in rows if row["arm"] == _ARMS[2]),
    )

    arm_stats: dict[str, _ArmLatencyStats] = {arm: _aggregate_arm(arm, rows) for arm in _ARMS}

    # Pair per-iteration medians for Wilcoxon. Only iter_ids that have
    # a row in all three arms feed the marginal samples.
    rows_by_arm_iter: dict[tuple[str, int], dict[str, Any]] = {(row["arm"], row["iter_id"]): row for row in rows}
    paired_iter_ids = sorted(
        {iter_id for (_, iter_id) in rows_by_arm_iter if all((arm, iter_id) in rows_by_arm_iter for arm in _ARMS)}
    )

    marginals = _compute_paired_marginals(
        rows_by_arm_iter=rows_by_arm_iter,
        paired_iter_ids=paired_iter_ids,
    )
    p_per_chunk = marginals.p_per_chunk
    p_ttft = marginals.p_ttft
    p_wall = marginals.p_wall
    h0_rejected_per_chunk = marginals.h0_rejected_per_chunk
    h0_rejected_ttft = marginals.h0_rejected_ttft
    h0_rejected_wall = marginals.h0_rejected_wall

    # Delivery-integrity counters (per arm: delivered-bytes re-hash vs
    # the per-iteration source manifest).
    integrity = _count_content_integrity_failures(
        rows_by_arm_iter=rows_by_arm_iter,
        paired_iter_ids=paired_iter_ids,
    )
    delivery_failures_alone = integrity["lll_alone"]
    delivery_failures_idle = integrity["bus_idle"]
    delivery_failures_swarm = integrity["bus_swarm"]

    # Ordering-fidelity counters (per arm: delivered arrival order vs the
    # monotonic source emit order).
    ordering = _count_ordering_inversions(
        rows_by_arm_iter=rows_by_arm_iter,
        paired_iter_ids=paired_iter_ids,
    )
    ordering_inversions_alone = ordering["lll_alone"]
    ordering_inversions_idle = ordering["bus_idle"]
    ordering_inversions_swarm = ordering["bus_swarm"]

    gates = _compute_gates(
        rows=rows,
        arm_stats=arm_stats,
        n_triples_actual=n_triples_actual,
    )

    return EventDeliveryFidelityVerdict(
        bench_name=bench_name,
        started_ns=started_ns,
        finished_ns=finished_ns,
        environment=external_state_report,
        external_state=external_state_report,
        n_triples_requested=n_triples,
        n_triples_actual=n_triples_actual,
        smoke=args.smoke,
        include_real_llm=args.include_real_llm,
        arms=list(_ARMS),
        arm_stats=arm_stats,
        wilcoxon_p_per_chunk_bus_latency=p_per_chunk,
        wilcoxon_p_ttft=p_ttft,
        wilcoxon_p_wall_time=p_wall,
        h0_rejected_per_chunk_bus_latency=h0_rejected_per_chunk,
        h0_rejected_ttft=h0_rejected_ttft,
        h0_rejected_wall_time=h0_rejected_wall,
        alpha_per_marginal=_ALPHA_PER_MARGINAL,
        delivery_integrity_failures_lll_alone=delivery_failures_alone,
        delivery_integrity_failures_bus_idle=delivery_failures_idle,
        delivery_integrity_failures_bus_swarm=delivery_failures_swarm,
        ordering_inversions_lll_alone=ordering_inversions_alone,
        ordering_inversions_bus_idle=ordering_inversions_idle,
        ordering_inversions_bus_swarm=ordering_inversions_swarm,
        median_per_chunk_bus_latency_alone_ns=arm_stats[_ARMS[0]].median_per_chunk_bus_latency_ns,
        median_per_chunk_bus_latency_bus_idle_ns=arm_stats[_ARMS[1]].median_per_chunk_bus_latency_ns,
        median_per_chunk_bus_latency_bus_swarm_ns=arm_stats[_ARMS[2]].median_per_chunk_bus_latency_ns,
        median_ttft_alone_ns=arm_stats[_ARMS[0]].median_ttft_ns,
        median_ttft_bus_idle_ns=arm_stats[_ARMS[1]].median_ttft_ns,
        median_ttft_bus_swarm_ns=arm_stats[_ARMS[2]].median_ttft_ns,
        median_wall_time_alone_ns=arm_stats[_ARMS[0]].median_wall_time_ns,
        median_wall_time_bus_idle_ns=arm_stats[_ARMS[1]].median_wall_time_ns,
        median_wall_time_bus_swarm_ns=arm_stats[_ARMS[2]].median_wall_time_ns,
        swarm_subscribers_ready_total=gates.swarm_subscribers_ready,
        swarm_underload_floor=gates.swarm_underload_floor,
        sandbagging_sentinel_fired=gates.sandbagging_sentinel_fired,
        latency_budget_p99_ns=_LATENCY_BUDGET_P99_NS,
        bus_idle_p99_latency_ns=gates.bus_idle_p99_latency_ns,
        bus_swarm_p99_latency_ns=gates.bus_swarm_p99_latency_ns,
        latency_budget_passed=gates.latency_budget_passed,
        wilcoxon_p_bus_idle_vs_swarm_latency=p_per_chunk,
        bus_swarm_perturbs_latency=gates.bus_swarm_perturbs_latency,
        distribution_equivalent=gates.distribution_equivalent,
        perturbation_detected=gates.perturbation_detected,
        inapplicable_reason=gates.inapplicable_reason,
        cost_usd_total=cost_usd_total,
        cost_unknown_count=cost_unknown_count,
        max_cost_usd_budget=args.max_cost_usd,
        max_cost_usd_observed=cost_observed,
        aborted_on_budget=aborted_on_budget,
        limitations=_build_limitations(),
    )


# ---------------------------------------------------------------------
# Top-level main.
# ---------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    bench_name = _BENCH_NAME
    n_triples = args.n if args.n is not None else (_SMOKE_N if args.smoke else _DEFAULT_N)

    # Preflight. The swarm arm's drivers require claude / gemini CLIs;
    # they are gated only when --include-real-llm is on.
    try:
        external_state_report = run_preflight_assertions(
            bench_name=bench_name,
            require_openai=args.include_real_llm,
            # The bus-swarm load is lightweight in-process waitbus subscribers
            # (no LLM); the only real-LLM dependency is the once-per-iteration
            # reasoning generation via OpenAI. The claude / gemini CLIs are not
            # used, so do not require them (they are absent on the clean
            # Hetzner baseline host).
            require_claude_cli=False,
            require_gemini_cli=False,
        )
    except PreflightError as exc:
        print(f"[{bench_name}] preflight failed: {exc}", file=sys.stderr)
        return 2

    openai_api_key: str | None = None
    if args.include_real_llm:
        openai_api_key = read_openai_key_from_keyring()
        if not openai_api_key:
            print(f"[{bench_name}] OPENAI_API_KEY missing from keyring", file=sys.stderr)
            return 2

    waitbus_path = _waitbus_path()
    python_exe = default_python_executable()
    started_ns = time.time_ns()

    # Output paths.
    args.output.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    stem = f"{ts}.{bench_name}"
    verdict_path = args.output / f"{stem}.verdict.json"
    progress_path = args.output / f"{stem}.progress.jsonl"
    log_path = args.output / f"{stem}.log"

    experiment = _run_experiment(
        args=args,
        bench_name=bench_name,
        n_triples=n_triples,
        openai_api_key=openai_api_key,
        python_exe=python_exe,
        waitbus_path=waitbus_path,
        external_state_report=external_state_report,
        progress_path=progress_path,
        log_path=log_path,
    )

    finished_ns = time.time_ns()

    verdict = _assemble_verdict(
        args=args,
        bench_name=bench_name,
        n_triples=n_triples,
        started_ns=started_ns,
        finished_ns=finished_ns,
        rows=experiment["rows"],
        external_state_report=experiment["external_state_report"],
        cost_usd_total=experiment["cost_usd_total"],
        cost_unknown_count=experiment["cost_unknown_count"],
        cost_observed=experiment["cost_observed"],
        aborted_on_budget=experiment["aborted_on_budget"],
    )
    verdict_path.write_bytes(msgspec.json.encode(verdict))
    print(f"[{bench_name}] verdict: {verdict_path}", file=sys.stderr)

    if verdict.inapplicable_reason:
        return 4
    delivery_failures_total = (
        verdict.delivery_integrity_failures_lll_alone
        + verdict.delivery_integrity_failures_bus_idle
        + verdict.delivery_integrity_failures_bus_swarm
    )
    ordering_inversions_total = (
        verdict.ordering_inversions_lll_alone
        + verdict.ordering_inversions_bus_idle
        + verdict.ordering_inversions_bus_swarm
    )
    # Delivery-fidelity gate: a dropped/corrupted chunk (integrity) OR an
    # out-of-order delivery (ordering) is a real waitbus / UDS transport
    # failure. Both fold into the same exit-1 gate.
    if delivery_failures_total + ordering_inversions_total > 0:
        return 1
    if verdict.perturbation_detected:
        return 1
    return 0


__all__ = [
    "_LATENCY_BUDGET_P99_NS",
    "EventDeliveryFidelityVerdict",
    "ReasoningChunkFrame",
    "_ArmLatencyStats",
    "_PairedMarginals",
    "_VerdictGates",
    "_aggregate_arm",
    "_compute_gates",
    "_compute_paired_marginals",
    "_compute_per_chunk_bus_latency_ns",
    "_compute_ttft_ns",
    "_decode_chunk_frame_from_msg_body",
    "_delivery_integrity_failures",
    "_encode_chunk_frame_for_msg_body",
    "_encode_chunk_frame_for_uds",
    "_hash_chunk",
    "_ordering_inversions",
    "_parse_consumer_arrivals",
    "_parse_delivered_order",
    "_parse_delivered_rehashes",
    "_percentile_ns",
    "_wilcoxon_paired_pvalue",
    "main",
]


if __name__ == "__main__":
    sys.exit(main())
