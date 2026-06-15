"""Shared helpers for the heterogeneous-swarm benchmark suite.

This module is bench-only: it lives under ``benchmarks/`` and is
excluded from the sdist. None of the consumer-facing daemon surface
imports from it. Every public helper is callable from the per-
experiment scripts under the same directory.

The struct shapes (``ExternalStateReport``,
``OpenAIEnvelope``) are recorded in the verdict and must remain
stable across bench releases — every field is documented inline.

Linux-only by design: ``read_daemon_cpu_ns`` and ``read_daemon_schedstat``
parse ``/proc``. Calling these on a non-Linux host raises rather than
silently degrades.

Operator's note on Anthropic/OpenAI prompt cache: the
``force_cold_cache_prefix`` helper returns a per-run-salted PREFIX
that both the per-prompt prefix cache and the suffix cache miss. A
suffix sentinel alone does not bust the prefix cache (the cache key is
computed over the first ~1024 tokens of the prompt). Because the cache
is scoped per-API-key and content-addressed (NOT per-process), the
helper mixes a run-scoped salt into the digest so the prefix is
deterministic within a run but distinct across runs -- a separate
benchmark process under the same key cannot HIT the prior run's
cached prefix within the ~5-min TTL.
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import hashlib
import importlib.metadata
import json
import logging
import os
import random as _random
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final, Protocol

import msgspec

from scripts.stress._context import DEFAULT_OPENAI_PROVIDER, openai_tokens_to_usd
from waitbus._log import structured

_logger = logging.getLogger("waitbus.bench.shared")

# Canonical model pins shared across the bench suite. Each pin is the
# single source of truth -- limitations text, tiktoken encoder choice,
# CLI argv for ``claude -p`` / ``gemini -p``, and the OpenAI Chat
# Completions request all read from here. Diverges from
# ``scripts.stress._real_drivers.REAL_OPENAI_MODEL_ID`` (= gpt-4.1-nano)
# because the bench's tiktoken encoder + OpenAI Chat path historically
# pins gpt-4o-mini for cost-rate-card stability; both pins coexist in
# the rate card so a model swap on either side is a one-line edit.
BENCH_CLAUDE_MODEL = "haiku"
BENCH_GEMINI_MODEL = "gemini-2.5-flash"
BENCH_OPENAI_MODEL = "gpt-4o-mini"

# Canonical bench-suite RNG seed. The bench's random.Random instances
# (workload arm ordering, predicate-subset selection, FS / pytest /
# github / docker baseline injection cadences) all default to this
# seed so re-running a bench with the same revision produces the same
# arm order, the same predicate set, and the same wall-clock arrival
# distribution. ``PYTHONHASHSEED=0`` is enforced on every bench process
# in tandem (the env-var path keeps dict iteration deterministic).
CANONICAL_RNG_SEED = 0xC1B5

# XOR mask layered on the canonical seed to derive a SECOND deterministic
# stream from the same root -- used by the polling-baseline benches to
# decouple their GC-trigger / synthetic-load injectors from the
# primary workload RNG without introducing a separate seed input.
RNG_GC_XOR_MASK = 0xDEAD

# Frame-generation seed for the predicate-evaluation latency bench's
# event-payload generator. Coexists with CANONICAL_RNG_SEED so the
# benches that consume both can derive two independent deterministic
# streams from one config surface.
FRAME_GEN_SEED = 0x1BCDB


def bench_rng() -> _random.Random:
    """Return a ``random.Random`` seeded with the canonical bench seed.

    The bench process must already have ``PYTHONHASHSEED=0`` set (the
    spawn factory writes this into every driver env) so dict iteration
    order is deterministic across re-runs; this helper asserts the
    invariant and returns a freshly-seeded RNG. Callers that need a
    second independent stream derive it from the canonical seed via
    ``RNG_GC_XOR_MASK`` rather than introducing a new seed input.
    """
    assert os.environ.get("PYTHONHASHSEED") == "0", (
        "bench_rng() requires PYTHONHASHSEED=0 for cross-run reproducibility; "
        "set it in the bench's spawn env before instantiating the RNG."
    )
    return _random.Random(CANONICAL_RNG_SEED)


# Inclusive upper bound on a valid ``PYTHONHASHSEED`` integer. CPython
# accepts ``0 .. 2**32-1`` (the value seeds SipHash); anything outside
# that band -- including a negative int -- is rejected by the interpreter
# at startup. ``_hashseed_or_default`` clamps to this range so a derived
# RNG seed is always a value the interpreter would itself have accepted.
_PYTHONHASHSEED_MAX = 2**32 - 1


def _hashseed_or_default() -> int:
    """Return ``PYTHONHASHSEED`` as a range-valid int, falling back to ``0``.

    ``PYTHONHASHSEED`` is an integer in the bench's spawn env, but the
    CPython sentinel ``"random"`` is also a legal value (it enables hash
    randomisation). A bare ``int(os.environ["PYTHONHASHSEED"])`` raises
    ``ValueError`` on that sentinel and on any non-numeric value. This
    helper maps ``"random"``, an unset variable, and any unparseable
    value to ``0`` so a seed read never aborts the bench.

    A numeric value is additionally clamped to the interpreter's documented
    ``0 .. 2**32-1`` range: CPython rejects a negative or oversized
    ``PYTHONHASHSEED`` at startup, so an out-of-range value reaching a
    downstream ``random.Random(...)`` seed would be a number the bench
    process could never actually have been launched with. Clamping (rather
    than passing the raw int through) keeps the derived RNG seed inside the
    same band the spawn env is constrained to.
    """
    raw = os.environ.get("PYTHONHASHSEED")
    if raw is None:
        return 0
    try:
        parsed = int(raw)
    except ValueError:
        return 0
    return max(0, min(parsed, _PYTHONHASHSEED_MAX))


# ---------------------------------------------------------------------------
# External-state probe timeouts and sentinel values.
# ---------------------------------------------------------------------------

_PROBE_TIMEOUT_SEC: Final[float] = 5.0
"""Maximum seconds passed to every external-state subprocess probe
(:func:`_safe_cli_version`, the ``timedatectl`` NTP probe, and the
``chronyc`` NTP probe).  Five seconds is sufficient for a healthy local
daemon IPC call or a CLI ``--version`` response while keeping preflight
startup predictable on a loaded host.  Probes that exceed this budget log a
structured warning and return ``None`` so the bench proceeds rather than
aborting on a transient system-daemon hiccup."""

_CHRONY_UNSYNC_STRATUM: Final[int] = 16
"""Chrony stratum value that signals ``chronyd`` is not synchronised.  By
the NTP specification, a stratum of 16 means "clock not synchronised / no
upstream source reachable".  :func:`detect_ntp_daemon` returns
``(False, "chronyc")`` when the probed stratum equals or exceeds this
sentinel, and ``(True, "chronyc")`` when it is strictly less than it."""


# Cold-cache prefix length. Anthropic's prompt cache only matches on the
# first ~1024 tokens of a prompt; padding to ~200 ASCII chars (~50 BPE
# tokens) is enough to defeat suffix-aware caches but not large enough
# to dominate the per-iteration prompt budget. The helper documents this
# in its docstring.
COLD_CACHE_PREFIX_LEN = 200


# ---------------------------------------------------------------------
# Capture structs.
# ---------------------------------------------------------------------


class OpenAIEnvelope(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
):
    """Per-driver OpenAI usage envelope, hydrated from a driver wake marker.

    Field names map directly to the OpenAI Chat Completions
    ``usage.prompt_tokens`` / ``usage.completion_tokens`` shape; the
    Responses API uses ``input_tokens`` / ``output_tokens`` but the
    bench uses Chat Completions for stable attribute naming.
    """

    model: str
    """Observed model id reported by the response envelope (not the
    requested alias). Pinned to the str returned by ``response.model``;
    the bench records the frozenset across iterations to detect a
    snapshot rotation mid-run."""
    input_tokens: int
    """Visible prompt tokens (``usage.prompt_tokens`` on Chat
    Completions). Does NOT include the cached subset; ``cached_tokens``
    is reported separately."""
    output_tokens: int
    """Completion tokens (``usage.completion_tokens``)."""
    cached_tokens: int
    """Prefix-cache reads from ``usage.prompt_tokens_details.cached_tokens``.
    Zero on cold-cache iterations; non-zero indicates the request hit
    the OpenAI prompt cache."""
    finish_reason: str | None
    """``choices[0].finish_reason`` — ``stop`` for clean completions;
    ``length`` / ``content_filter`` / ``tool_calls`` for moderated or
    truncated paths."""
    stop_reason: str | None = None
    """Normalised moderation reason mirroring ``ClaudeEnvelope.stop_reason``
    and ``GeminiEnvelope.stop_reason``: ``refusal`` / ``error_during_execution``
    / ``None`` for a clean completion. The Chat Completions SDK reports
    moderation via ``finish_reason``; this field is the cross-provider pivot
    a caller populates when synthesising a capture from a caught API
    exception or a moderated response. The construction default ``None`` is
    the clean-completion shape."""
    is_error: bool = False
    """Mirrors ``ClaudeEnvelope.is_error`` / ``GeminiEnvelope.is_error``:
    ``True`` iff the call surfaced an upstream error envelope (API exception,
    rate-limit, network failure). The construction default ``False`` is the
    clean-completion shape."""
    api_error_status: str | None = None
    """Upstream HTTP-style status string when ``is_error=True`` (e.g.
    ``429`` / ``5xx`` / ``timeout``). Mirrors the Claude/Gemini envelope
    convention so a single invariant gate consumes all three. ``None`` on a
    clean completion and on captures where the upstream status is not
    available."""
    terminal_reason: str | None = None
    """Free-text description of the terminal failure mode when
    ``is_error=True`` (typically ``f"{exc.__class__.__name__}: {exc}"``).
    Mirrors Claude/Gemini envelope convention. ``None`` on a clean
    completion."""
    cost_usd: float | None = None
    """USD cost of the call computed driver-side via the shared rate
    card (``scripts.stress._context.openai_tokens_to_usd``). Mirrors
    the ``cost_usd: float | None`` field on ``ClaudeEnvelope`` /
    ``GeminiEnvelope`` so the bench's verdict-level cost aggregator
    sees one symmetric envelope shape per row. ``None`` means the
    provider was unmapped in the rate card (a new model rolled out
    before the rate card was updated) -- the aggregator surfaces
    those rows in ``cost_unknown_count`` rather than silently
    coercing to ``0.0``."""


class ClaudeEnvelope(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
):
    """Per-iteration claude envelope (parser-output shape per the documented contract).

    Mirrors the ``parse_claude_envelope`` output fields the bench
    consumes from a driver's wake marker. Carries Anthropic billing
    semantics (visible + cache_creation + cache_read = billed_input)
    plus the moderation / error provenance fields a refusal envelope
    surfaces.
    """

    input_tokens_visible: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int
    output_tokens: int
    billed_input_tokens: int
    total_billed_tokens: int
    cost_usd: float | None
    model: str
    stop_reason: str | None
    is_error: bool
    api_error_status: str | None
    terminal_reason: str | None
    num_turns: int | None


class GeminiEnvelope(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
):
    """Per-iteration gemini envelope (parser-output shape per the documented contract).

    Mirrors the ``parse_gemini_envelope`` output fields. The gemini
    free-tier path reports no per-call cost so ``cost_usd`` is
    ``None``; the verdict's cost aggregator must skip ``None`` rather
    than coerce to zero.
    """

    prompt_tokens: int
    candidates_tokens: int
    thoughts_tokens: int
    cached_tokens: int
    tool_tokens: int
    total_tokens_reported: int
    total_tokens_recomputed: int
    cost_usd: float | None
    model: str
    stop_reason: str | None
    is_error: bool
    api_error_status: str | None
    terminal_reason: str | None
    num_turns: int | None


class IterationRow(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
):
    """One driver's row from one iteration of one arm.

    Used across the bench experiments: each row carries a single
    driver's envelope substruct (the other two stay ``None``) so the
    per-iteration moderation gate sees exactly which driver's
    moderation state is in play. Cross-driver joins happen at the
    aggregation layer.
    """

    iter_id: int
    arm: str
    driver: str
    sentinel: str
    # Per-driver end-to-end timing. A bench that measures driver latency
    # (polling-vs-subscribe) fills these with observed nanosecond values; a
    # bench that does not (multistream-proof measures daemon CPU, not driver
    # latency) sets them to the inapplicable sentinel 0 — a 0 here means "not
    # measured on this bench", not "observed zero latency". ``cache_state`` is
    # likewise ``"NA"`` when no cache information applies to the row.
    t_send_ns: int
    t_observe_ns: int
    latency_ns: int
    cache_state: str
    claude_env: ClaudeEnvelope | None
    gemini_env: GeminiEnvelope | None
    openai_env: OpenAIEnvelope | None
    invariant_failed: bool
    invariant_failure_field: str | None

    # --- cross-process monotonic timing markers (driver + orchestrator). ---
    # Captured at the seed-emit moment (orchestrator) and at the three
    # driver-side moments embedded in the ``WAKE_RECEIVED`` marker line.
    # All four anchors are on the Linux ``CLOCK_MONOTONIC`` that
    # ``benchmarks/_bench_preflight.run_preflight_assertions`` pins
    # cross-process so a subtraction across the boundary is sound.
    t_seed_emit_monotonic_ns: int = 0
    """Orchestrator-side monotonic moment the seed was emitted; the
    reference anchor for bus-latency math and delivery-mode
    classification. Replicated on every per-driver row of one iteration
    so a single-row consumer needs no join. The construction default
    ``0`` is the sentinel for "orchestrator did not record this
    iteration's seed-emit anchor"; a downstream consumer reading ``0``
    treats the row as missing the anchor rather than as a real
    measurement of monotonic epoch 0."""
    t_sub_monotonic_ns: int = 0
    """Driver-side monotonic moment captured just BEFORE the driver
    issued its ``wait_for`` call (the proxy for "subscribe registered"
    -- the actual register completes inside the SDK's socket-send a
    sub-millisecond later). The construction default ``0`` is the
    sentinel for "driver did not emit a ``WAKE_RECEIVED`` marker"
    (a driver-side crash before the wait_for return)."""
    t_import_done_monotonic_ns: int = 0
    """Driver-side monotonic moment the framework SDK imports completed.
    The delta against ``t_sub_monotonic_ns`` surfaces cold-import cost
    so an operator can attribute a slow subscribe to its real cause.
    The construction default ``0`` is the sentinel for "driver did
    not emit a ``WAKE_RECEIVED`` marker"."""
    wake_monotonic_ns: int = 0
    """Driver-side monotonic moment ``wait_for`` returned the matched
    frame. Bus ingest latency = ``wake_monotonic_ns -
    t_seed_emit_monotonic_ns`` (clean of LLM-call jitter because the
    driver emits the wake marker BEFORE its post-wake LLM exercise).
    The construction default ``0`` is the sentinel for "driver did
    not emit a ``WAKE_RECEIVED`` marker"."""
    delivery_mode: str = "unknown"
    """``"live"`` when the driver subscribed BEFORE the seed was
    emitted (the seed reached the driver via the daemon's live
    ``_fan_out`` path); ``"replay"`` when the driver subscribed at or
    after the seed-emit moment (the seed reached the driver via the
    daemon's seq-replay window the waitbus ``since=`` cursor opened);
    ``"unknown"`` when the driver did not emit a ``WAKE_RECEIVED``
    marker (a driver-side crash before the wait_for return). The bench's
    aggregation excludes ``"replay"`` rows from latency math so a
    median is a clean live-fan-out figure; the verdict surfaces the
    per-driver replay-contamination rate separately so a high-jitter
    iteration is operator-visible rather than silently mixing modes."""


# Bench-side fallback provider id sources the canonical constant from
# ``scripts.stress._context`` -- the rate-card-key and the bench
# fallback are now one constant, not two duplicated literals.
_DEFAULT_OPENAI_PROVIDER = DEFAULT_OPENAI_PROVIDER


def openai_envelope_to_usd(envelope: OpenAIEnvelope) -> float:
    """Compute the USD cost of an OpenAI call from its envelope.

    Delegates to the canonical ``openai_tokens_to_usd`` rate-card
    helper in ``scripts.stress._context`` so the bench and the
    driver-side cost computation share one source of truth. The cached
    prefix-read subset (``envelope.cached_tokens``) bills at the
    per-model cached rate (a 0.5x / 0.25x discount), disjoint from the
    visible ``input_tokens`` -- the figure is the true billed cost, not
    an upper bound.

    The provider id is read from ``envelope.model`` (driver path
    stamps the canonical ``openai-gpt-*`` provider identifier);
    falls back to the bench's default provider when ``model`` is the
    sentinel ``"unknown"`` (a legacy verdict file or a smoke row
    whose driver did not stamp the provider). The helper returns
    ``None`` for an unmapped provider; the bench coerces to ``0.0``
    here only on the model-was-stamped-but-rate-not-listed branch so
    a future model id rolls in at zero cost rather than crashing the
    aggregator -- the verdict's per-driver token totals remain
    truthful even if the rate card is one revision behind the model.
    """
    provider = envelope.model if envelope.model != "unknown" else _DEFAULT_OPENAI_PROVIDER
    cost = openai_tokens_to_usd(
        envelope.input_tokens,
        envelope.output_tokens,
        provider=provider,
        cached_tokens=envelope.cached_tokens,
    )
    return cost if cost is not None else 0.0


class DrainableChild(Protocol):
    """Structural type for a supervised subprocess the drainer can harvest.

    Both the swarm bench's ``_Child`` and the stress controller's
    ``_Child`` satisfy this shape: a ``proc`` Popen whose stdout the
    drainer reads to EOF, and a ``terminate()`` that SIGTERMs the child's
    process group when the deadline expires.
    """

    proc: subprocess.Popen[bytes]

    def terminate(self) -> None: ...


def drain_children_concurrently(
    children: Sequence[DrainableChild],
    *,
    deadline_monotonic: float,
    min_remaining_sec: float,
    term_grace_sec: float,
) -> dict[int, tuple[bytes, int]]:
    """Drain every child's stdout CONCURRENTLY; stamp each at its own EOF.

    Returns ``{index: (stdout_bytes, t_observe_ns)}`` keyed by the child's
    position in ``children``. One thread per child runs ``communicate``
    (which blocks until that child's EOF / process exit), so a slow or
    hung child does NOT delay the harvest of a faster sibling: each
    child's ``t_observe_ns`` (``CLOCK_MONOTONIC``) is stamped the instant
    THAT child's stdout is in hand, and the shared ``deadline_monotonic``
    bounds every thread independently rather than being consumed serially
    by whichever child happens to be first in iteration order.

    This is the single source of truth for the concurrent-drain mechanic.
    Inlining it serially (one ``communicate`` per child in a ``for`` loop)
    is the latency-artifact / deadline-starvation bug it exists to
    prevent: a serial drain lets one slow LLM driver eat the whole window
    before later drivers are ever read, terminating them pre-marker.

    Marker parsing is the caller's job and happens after this returns --
    each child emits only its own markers, so parse order is irrelevant
    to attribution. ``drained[index] = ...`` from distinct keys is safe
    under the GIL (each thread writes one unique key, no read-modify-write).
    On per-child timeout the child is terminated and given a
    ``term_grace_sec`` final drain; whatever bytes were already buffered
    are returned (an empty ``b""`` if nothing arrived).
    """
    drained: dict[int, tuple[bytes, int]] = {}

    def _drain_one(index: int, child: DrainableChild) -> None:
        remaining = max(min_remaining_sec, deadline_monotonic - time.monotonic())
        out: bytes = b""
        try:
            out, _ = child.proc.communicate(timeout=remaining)
        except subprocess.TimeoutExpired:
            child.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                out, _ = child.proc.communicate(timeout=term_grace_sec)
        drained[index] = (out, time.monotonic_ns())

    threads = [
        threading.Thread(target=_drain_one, args=(index, child), name=f"drain-{index}")
        for index, child in enumerate(children)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return drained


class CostBudgetTracker:
    """Per-iteration cost accumulator with a hard upper budget.

    The budget gate exists to cap genuinely-metered, pay-per-call spend.
    Only OpenAI is metered here: ``record_openai`` advances
    ``observed_usd``, and ``should_abort`` gates on that figure alone.

    The claude and gemini drivers run via their *subscription* CLIs
    (``claude -p`` / ``gemini -p`` -- the argv builders carry no API
    key), so their marginal dollar cost is zero. A ``claude -p`` envelope
    still *reports* a notional ``cost_usd`` (the API-equivalent price of
    the tokens it spent), but the operator on a Claude subscription never
    pays it. Folding that notional figure into the real-dollar budget
    would abort the run on money never spent -- so ``record_claude``
    routes a non-``None`` cost to ``notional_subscription_usd`` (surfaced
    for transparency, never gated) and a ``None`` cost to
    ``unknown_usd_call_count``. Gemini never surfaces a per-call cost, so
    ``record_gemini`` always advances the unknown counter.

    ``should_abort()`` returns True once the running MAX *metered* cost of
    any single completed iteration, projected one more iteration ahead,
    would breach ``max_usd``. The bench checks the gate BETWEEN
    iterations, not mid-iteration, so a single iteration's worst-case
    overshoot is at most one iteration's MAX observed cost. Projecting
    the max (rather than the mean) guarantees the bound holds even when
    one high-variance iteration costs far more than the running average.
    """

    def __init__(self, *, max_usd: float) -> None:
        self._max_usd = max_usd
        self._observed = 0.0
        self._notional = 0.0
        self._iters = 0
        self._unknown_count = 0
        # Cost of the running iteration captured at its ``begin_iteration``
        # boundary, so each iteration's delta is ``self._observed`` now
        # minus this snapshot. ``self._max_iter_cost`` tracks the largest
        # such delta seen across completed iterations; ``max_iter_cost`` folds
        # the still-open final window on read so ``should_abort``'s projection
        # stays correct after the loop's last iteration.
        self._iter_start_observed = 0.0
        self._max_iter_cost = 0.0

    def record_openai(self, envelope: OpenAIEnvelope) -> None:
        self._observed += openai_envelope_to_usd(envelope)

    def record_claude(self, cost: float | None) -> None:
        # claude runs via the subscription CLI: its reported cost is
        # notional (never billed to a Max/Pro subscriber), so it advances
        # the surfaced-but-ungated notional accumulator rather than the
        # metered budget. A None cost (tier surfaces no figure) is unknown.
        if cost is not None:
            self._notional += cost
        else:
            self._unknown_count += 1

    def record_gemini(self) -> None:
        # The gemini CLI surfaces no per-call cost figure (documented
        # contract), so every gemini call is an unknown-cost call.
        self._unknown_count += 1

    def record_unknown(self) -> None:
        """An LLM call whose cost could not be attributed (no parseable marker)."""
        self._unknown_count += 1

    def begin_iteration(self) -> None:
        # Close out the just-completed iteration: its cost delta is the
        # observed total now minus the snapshot taken when it began. Fold
        # that delta into the running max before opening the next window.
        # The first ``begin_iteration`` has a zero-width prior window
        # (start == observed == 0.0), contributing a no-op 0.0 delta.
        last_delta = self._observed - self._iter_start_observed
        if last_delta > self._max_iter_cost:
            self._max_iter_cost = last_delta
        self._iter_start_observed = self._observed
        self._iters += 1

    @property
    def max_iter_cost(self) -> float:
        """Largest single-iteration cost delta, folding the open window on read.

        ``begin_iteration`` folds each *completed* iteration's delta into
        ``self._max_iter_cost``, but the final iteration is never closed by a
        trailing ``begin_iteration`` -- the bench loops check the gate at the
        TOP of each iteration and simply end after the last one. Folding the
        open window (``self._observed - self._iter_start_observed``) on read
        keeps this value correct after the loop's last iteration without
        requiring a closing call. At every gate call site ``should_abort`` runs
        immediately after ``begin_iteration``, where the open window is
        zero-width, so the fold is a no-op there and the between-iterations
        gate semantics are unchanged.
        """
        return max(self._max_iter_cost, self._observed - self._iter_start_observed)

    def should_abort(self) -> bool:
        if self._iters <= 1:
            return False
        return self._observed + self.max_iter_cost >= self._max_usd

    @property
    def observed_usd(self) -> float:
        """Running total of genuinely-metered (OpenAI) spend -- what the gate caps."""
        return self._observed

    @property
    def notional_subscription_usd(self) -> float:
        """Running total of notional subscription-CLI cost (claude ``cost_usd``).

        Surfaced for transparency -- the API-equivalent price of the tokens
        the subscription drivers spent -- but never folded into the budget
        gate, since a subscriber's marginal dollar cost for these calls is
        zero.
        """
        return self._notional

    @property
    def unknown_usd_call_count(self) -> int:
        return self._unknown_count

    @property
    def max_usd(self) -> float:
        return self._max_usd


def claude_envelope_from_token_usage(tu: Any) -> ClaudeEnvelope:
    """Convert a driver-side ``TokenUsage`` into a typed ``ClaudeEnvelope``.

    ``tu`` is the ``scripts.stress._context.TokenUsage`` value the bench
    rehydrates from a wake-marker line; the conversion is a 1-to-1
    field copy. Kept here so the per-iteration row builder is a single
    construction call.
    """
    return ClaudeEnvelope(
        input_tokens_visible=tu.input_tokens,
        cache_creation_input_tokens=tu.cache_creation_input_tokens,
        cache_read_input_tokens=tu.cache_read_input_tokens,
        output_tokens=tu.output_tokens,
        billed_input_tokens=tu.billed_input_tokens,
        total_billed_tokens=tu.billed_input_tokens + tu.output_tokens,
        cost_usd=tu.cost_usd,
        model=tu.model,
        stop_reason=tu.stop_reason,
        is_error=tu.is_error,
        api_error_status=tu.api_error_status,
        terminal_reason=tu.terminal_reason,
        num_turns=tu.num_turns,
    )


def openai_envelope_from_token_usage(tu: Any) -> OpenAIEnvelope:
    """Convert a driver-side ``TokenUsage`` into a typed ``OpenAIEnvelope``.

    ``tu`` is the ``scripts.stress._context.TokenUsage`` value the bench
    rehydrates from a pydantic / langgraph driver's wake-marker line.
    The conversion is a 1-to-1 field copy mirroring the Claude / Gemini
    helpers; the resulting ``OpenAIEnvelope`` carries the same
    cross-provider moderation fields (``stop_reason`` / ``is_error`` /
    ``api_error_status`` / ``terminal_reason``) so the bench's
    invariant gate has one envelope shape per row regardless of the
    driver family that produced it.

    ``cached_tokens`` rides ``TokenUsage.cached_tokens`` (Gemini-side
    spelling); the OpenAI envelope's ``cached_tokens`` field carries
    the same semantic content. ``finish_reason`` is unavailable from
    the driver-side ``TokenUsage`` (the SDK does not surface it on the
    pydantic-ai / langchain paths the drivers run); ``None`` is the
    clean-completion shape the invariant gate consumes.
    """
    return OpenAIEnvelope(
        model=tu.model,
        input_tokens=tu.input_tokens,
        output_tokens=tu.output_tokens,
        cached_tokens=tu.cached_tokens,
        finish_reason=None,
        stop_reason=tu.stop_reason,
        is_error=tu.is_error,
        api_error_status=tu.api_error_status,
        terminal_reason=tu.terminal_reason,
        cost_usd=tu.cost_usd,
    )


def gemini_envelope_from_token_usage(tu: Any) -> GeminiEnvelope:
    """Convert a driver-side ``TokenUsage`` into a typed ``GeminiEnvelope``."""
    return GeminiEnvelope(
        prompt_tokens=tu.input_tokens,
        candidates_tokens=tu.output_tokens,
        thoughts_tokens=tu.thoughts_tokens,
        cached_tokens=tu.cached_tokens,
        tool_tokens=tu.tool_tokens,
        total_tokens_reported=tu.total_tokens_reported,
        total_tokens_recomputed=tu.total_tokens_recomputed,
        cost_usd=tu.cost_usd,
        model=tu.model,
        stop_reason=tu.stop_reason,
        is_error=tu.is_error,
        api_error_status=tu.api_error_status,
        terminal_reason=tu.terminal_reason,
        num_turns=tu.num_turns,
    )


def _classify_claude_cache_state(visible: int, cache_read: int, billed_input: int) -> str:
    """Map a parsed claude envelope to ``COLD`` / ``WARMING`` / ``WARM``.

    Mirrors the spec's classification: ``COLD`` when no cache reads,
    ``WARMING`` when reads cover less than half the billed input, else
    ``WARM``. Returns ``COLD`` when both billed_input and cache_read
    are zero (e.g. a refusal envelope) — the classifier does not
    silently degrade to ``NA`` on a malformed envelope.

    ``visible`` is the un-cached visible input the envelope reported
    (``usage.input_tokens``). It is accepted by the API for callsite
    symmetry with the Anthropic billing-semantics triple (visible +
    cache_creation + cache_read = billed_input) so the bench's row
    builder passes the full set of token counts without per-class
    re-derivation; the classifier itself only needs ``cache_read``
    and ``billed_input`` to decide the state. A future extension may
    use ``visible`` to gate a fourth state (e.g. ``VISIBLE_ONLY``).

    Shared by every bench that hydrates a claude envelope from a wake
    marker (``bench_polling_vs_subscribe_llm_agent`` and
    ``bench_multistream_proof``) so the cache-state mapping has a single
    source of truth.
    """
    _ = visible
    if cache_read <= 0:
        return "COLD"
    if billed_input <= 0:
        return "COLD"
    if cache_read < billed_input // 2:
        return "WARMING"
    return "WARM"


def count_cache_contaminated_rows(rows: Sequence[IterationRow]) -> int:
    """Count measured rows that read a prior run's cached prompt prefix.

    The bench claims cold-cache validity: every measured LLM call is
    supposed to start from a cold provider-side prompt cache so the
    cost and latency figures reflect a fresh prompt, not a warm replay
    of an earlier run's prefix. A row violates that premise when its
    OpenAI envelope reports ``cached_tokens > 0`` or its Claude envelope
    reports ``cache_read_input_tokens > 0`` -- the provider served part
    of the prompt from a cache the previous run populated (cross-run
    contamination).

    Returns the integer count across all rows. ``0`` means clean
    cold-cache isolation; any non-zero value means at least one measured
    call hit a warm cache and the cold-cache premise was violated for
    that call. This is the OBSERVABLE only -- the bench does not hard-fail
    on it here (the per-run salt that forces a cold cache lands
    separately); the count surfaces the contamination so an operator
    reading the verdict can see it rather than have it silently fold
    into the cost and latency aggregates.

    Gemini's ``cached_tokens`` is intentionally NOT counted: the bench's
    Gemini path carries no per-call cost and its cache semantics are
    implicit-context, not the explicit cross-run prefix cache this gate
    guards against.
    """
    count = 0
    for row in rows:
        if (row.openai_env is not None and row.openai_env.cached_tokens > 0) or (
            row.claude_env is not None and row.claude_env.cache_read_input_tokens > 0
        ):
            count += 1
    return count


class ExternalStateReport(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
):
    """Bench-wide external-state snapshot recorded in the verdict.

    Every field is either a probed value or ``None`` when the probe
    failed (in which case ``capture_external_state`` also logs a
    structured warning). The verdict consumer treats a field of ``None``
    as "probe failed; do not interpret"; aggregations that depend on
    a missing field are accompanied by a per-driver advisory string in
    the verdict's ``limitations`` list.
    """

    # Driver-side CLI versions.
    claude_cli_version: str | None
    gemini_cli_version: str | None
    # Python SDK versions discovered via ``importlib.metadata``.
    pydantic_ai_version: str | None
    langgraph_version: str | None
    langchain_core_version: str | None
    langchain_openai_version: str | None
    openai_sdk_version: str | None
    anthropic_sdk_version: str | None
    msgspec_version: str | None
    hdrhistogram_version: str | None
    tiktoken_version: str | None
    # Per-iteration model frozenset capture: each iteration appends the
    # observed model id; the bench-level aggregator turns the lists into
    # frozensets at verdict-time. The list shape lets the bench warn on a
    # mid-run snapshot rotation without throwing the partial data away.
    # The three sets are kept symmetric: every LLM-invoking driver
    # surface (claude / gemini / openai) lands its observed model here.
    anthropic_response_model_set: list[str]
    openai_response_model_set: list[str]
    gemini_response_model_set: list[str]
    # Per-iteration agent traversal shape (one entry per iteration).
    agent_tool_call_count_per_iter: list[int]
    agent_turn_count_per_iter: list[int]
    # Daemon-side observable configuration.
    waitbus_daemon_synchronous: str | None
    waitbus_daemon_journal_mode: str | None
    waitbus_daemon_page_size: int | None
    waitbus_daemon_broadcast_pool_size: int | None
    waitbus_daemon_doorbell_socket_buffer: int | None
    # Authoritative snapshot of the live daemon's PRAGMA settings.
    # Empty dict means the probe failed (the daemon's DB was unreachable
    # at probe time or the SQLite open failed); a populated dict
    # overrides the scalar fields above on any conflict.
    waitbus_daemon_pragmas: dict[str, str]
    # Environment knobs at orchestrator startup.
    waitbus_env_vars: dict[str, str]
    pythonhashseed: str | None
    pythonmalloc: str | None
    # Host-clock state.
    ntp_active: bool | None
    ntp_source: str | None
    boot_time_ns: int | None
    cpu_count_physical: int | None
    cpu_count_logical: int | None
    # Per-driver moderation event counts and stop-reason distribution.
    moderation_event_count: int
    stop_reason_distribution: dict[str, int]
    api_error_status_distribution: dict[str, int]
    # OPENAI_API_KEY presence (bool only; the key value is never
    # recorded in the verdict — see operator notes in module docstring).
    openai_key_present: bool


# ---------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------


def _safe_metadata_version(distribution: str) -> str | None:
    """Look up a distribution's installed version; return ``None`` on miss.

    ``importlib.metadata.version`` raises ``PackageNotFoundError`` when
    the distribution is not installed (or ships under a different name);
    that is a recoverable probe-failure for the bench's external-state
    capture, not a crash condition.
    """
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_missing",
            kind="python_distribution",
            distribution=distribution,
        )
        return None


def _safe_cli_version(binary: str, version_flag: str = "--version") -> str | None:
    """Probe ``<binary> <version_flag>``; return the stripped stdout.

    Returns ``None`` (and logs a structured warning) on any of:

    - the binary is not on PATH;
    - the probe exits non-zero;
    - the probe hangs longer than 5 seconds.

    The probe inherits the parent env so subscription-bound CLIs that
    read tokens from the user's home directory work transparently. We
    do NOT redirect stderr to stdout — diagnostic noise on
    stderr stays on the orchestrator's stderr and does not pollute the
    recorded version string.
    """
    binary_path = shutil.which(binary)
    if binary_path is None:
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_missing",
            kind="cli_binary",
            binary=binary,
        )
        return None
    try:
        result = subprocess.run(
            [binary_path, version_flag],
            capture_output=True,
            text=True,
            timeout=_PROBE_TIMEOUT_SEC,
            check=False,
        )
    except subprocess.TimeoutExpired:
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_timeout",
            kind="cli_binary",
            binary=binary,
        )
        return None
    if result.returncode != 0:
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_nonzero",
            kind="cli_binary",
            binary=binary,
            returncode=result.returncode,
        )
        return None
    return result.stdout.strip() or None


def _safe_int_proc_read(path: str, field_name: str) -> int | None:
    """Read a single int from a ``/proc`` file; return ``None`` on miss."""
    try:
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
    except OSError:
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_missing",
            kind="proc_file",
            path=path,
            field=field_name,
        )
        return None
    try:
        return int(text.strip())
    except ValueError:
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_parse_error",
            kind="proc_file",
            path=path,
            field=field_name,
        )
        return None


def _read_boot_time_ns() -> int | None:
    """Read system boot time in nanoseconds since unix epoch.

    Sourced from ``/proc/stat`` (the ``btime`` line). Returns ``None``
    on any read or parse failure (non-Linux hosts have no ``/proc``).
    """
    try:
        with open("/proc/stat", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("btime "):
                    return int(line.split()[1]) * 1_000_000_000
    except (OSError, ValueError, IndexError):
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_missing",
            kind="boot_time",
        )
    return None


def _parse_physical_cpu_count(cpuinfo_text: str) -> int | None:
    """Count distinct physical cores from ``/proc/cpuinfo`` text.

    Linux ``/proc/cpuinfo`` reports one block per LOGICAL processor; a
    physical core is identified by the ``(physical id, core id)`` pair
    (the socket and the core within that socket). Counting distinct
    pairs collapses SMT siblings (hyperthreads share a core id) onto
    their physical core, so the count is the true physical-core total
    rather than the logical count ``os.cpu_count()`` /
    ``os.sched_getaffinity(0)`` report.

    Returns ``None`` when no block carries BOTH keys -- a single block
    missing one key is skipped, and if no block is fully keyed the
    parser returns ``None`` rather than fabricating a count.
    Some kernels / architectures omit these keys entirely (containers
    with a masked cpuinfo, certain ARM kernels); ``None`` there is the
    correct "physical topology not observable" signal.
    """
    pairs: set[tuple[str, str]] = set()
    physical_id: str | None = None
    core_id: str | None = None
    for line in cpuinfo_text.splitlines():
        stripped = line.strip()
        if not stripped:
            # Blank line terminates a processor block. Commit the pair if
            # both keys were seen, then reset for the next block.
            if physical_id is not None and core_id is not None:
                pairs.add((physical_id, core_id))
            physical_id = None
            core_id = None
            continue
        if ":" not in stripped:
            continue
        key, _, value = stripped.partition(":")
        key = key.strip()
        value = value.strip()
        if key == "physical id":
            physical_id = value
        elif key == "core id":
            core_id = value
    # Commit the trailing block (the file may not end with a blank line).
    if physical_id is not None and core_id is not None:
        pairs.add((physical_id, core_id))
    return len(pairs) if pairs else None


def _read_physical_cpu_count() -> int | None:
    """Probe the host's physical-core count; ``None`` when unknown.

    Reads ``/proc/cpuinfo`` and delegates to
    :func:`_parse_physical_cpu_count`. Returns ``None`` on any read
    failure (non-Linux hosts have no ``/proc``) or when the topology
    keys are absent, so the recorded field is never a silently-wrong
    logical count masquerading as a physical one.
    """
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as fh:
            text = fh.read()
    except OSError:
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_missing",
            kind="cpu_count_physical",
        )
        return None
    count = _parse_physical_cpu_count(text)
    if count is None:
        structured(
            _logger,
            logging.WARNING,
            "bench_external_state_probe_missing",
            kind="cpu_count_physical",
        )
    return count


def detect_ntp_daemon() -> tuple[bool | None, str | None]:
    """Daemon-agnostic NTP-sync probe.

    Tries ``timedatectl show --property=NTPSynchronized`` first; falls
    back to ``chronyc tracking``; returns ``(None, None)`` on any failure
    path. Returns ``(True, daemon_name)`` when an active sync is
    detected; ``(False, daemon_name)`` when the probe ran but reported
    unsynchronized.

    The bench records this in ``ExternalStateReport.ntp_active`` and
    ``ntp_source`` so a downstream reader can correlate any wall-clock
    anomaly with a missing or inactive NTP daemon. Monotonic-clock
    measurements (the ones that actually drive the verdict's latency
    aggregates) are unaffected by NTP state.
    """
    # timedatectl path (systemd-on-host).
    timedatectl = shutil.which("timedatectl")
    if timedatectl is not None:
        try:
            result = subprocess.run(
                [timedatectl, "show", "--property=NTPSynchronized"],
                capture_output=True,
                text=True,
                timeout=_PROBE_TIMEOUT_SEC,
                check=False,
            )
            if result.returncode == 0 and "=" in result.stdout:
                value = result.stdout.strip().split("=", 1)[1].strip().lower()
                return (value == "yes", "timedatectl")
        except subprocess.TimeoutExpired:
            pass
    # chrony path (chronyd-on-host).
    chronyc = shutil.which("chronyc")
    if chronyc is not None:
        try:
            result = subprocess.run(
                [chronyc, "tracking"],
                capture_output=True,
                text=True,
                timeout=_PROBE_TIMEOUT_SEC,
                check=False,
            )
            if result.returncode == 0:
                # Stratum _CHRONY_UNSYNC_STRATUM (16) indicates unsynchronized on chrony's report.
                stratum_match = re.search(r"^Stratum\s*:\s*(\d+)", result.stdout, re.MULTILINE)
                if stratum_match is not None:
                    stratum = int(stratum_match.group(1))
                    return (stratum < _CHRONY_UNSYNC_STRATUM, "chronyc")
        except (subprocess.TimeoutExpired, ValueError):
            pass
    structured(
        _logger,
        logging.WARNING,
        "bench_external_state_probe_missing",
        kind="ntp_daemon",
    )
    return (None, None)


def capture_external_state(*, openai_api_key_present: bool) -> ExternalStateReport:
    """Snapshot the bench-relevant external state of the host.

    Every probe is wrapped so a missing dependency or unreachable CLI
    sets the corresponding field to ``None`` and logs a structured
    warning rather than crashing the bench. The per-iteration list
    fields are initialised empty; the bench's iteration loop appends to
    them and the verdict aggregator turns them into the report's
    summary distributions.

    ``openai_api_key_present`` is taken as a parameter (not probed
    inside this function) so the bench's orchestrator can keep the
    keyring lookup centralised at startup and the recorded bool reflects
    the same lookup the swarm uses.
    """
    ntp_active, ntp_source = detect_ntp_daemon()
    waitbus_env = {k: v for k, v in os.environ.items() if k.startswith("WAITBUS_") or k.startswith("WAITBUS_")}
    return ExternalStateReport(
        claude_cli_version=_safe_cli_version("claude"),
        gemini_cli_version=_safe_cli_version("gemini"),
        pydantic_ai_version=_safe_metadata_version("pydantic-ai-slim"),
        langgraph_version=_safe_metadata_version("langgraph"),
        langchain_core_version=_safe_metadata_version("langchain-core"),
        langchain_openai_version=_safe_metadata_version("langchain-openai"),
        openai_sdk_version=_safe_metadata_version("openai"),
        anthropic_sdk_version=_safe_metadata_version("anthropic"),
        msgspec_version=_safe_metadata_version("msgspec"),
        hdrhistogram_version=_safe_metadata_version("hdrhistogram"),
        tiktoken_version=_safe_metadata_version("tiktoken"),
        anthropic_response_model_set=[],
        openai_response_model_set=[],
        gemini_response_model_set=[],
        agent_tool_call_count_per_iter=[],
        agent_turn_count_per_iter=[],
        waitbus_daemon_synchronous=None,
        waitbus_daemon_journal_mode=None,
        waitbus_daemon_page_size=None,
        waitbus_daemon_broadcast_pool_size=None,
        waitbus_daemon_doorbell_socket_buffer=None,
        waitbus_daemon_pragmas={},
        waitbus_env_vars=waitbus_env,
        pythonhashseed=os.environ.get("PYTHONHASHSEED"),
        pythonmalloc=os.environ.get("PYTHONMALLOC"),
        ntp_active=ntp_active,
        ntp_source=ntp_source,
        boot_time_ns=_read_boot_time_ns(),
        cpu_count_physical=_read_physical_cpu_count(),
        cpu_count_logical=os.cpu_count(),
        moderation_event_count=0,
        stop_reason_distribution={},
        api_error_status_distribution={},
        openai_key_present=openai_api_key_present,
    )


def force_cold_cache_prefix(run_salt: str, iter_id: int) -> str:
    """Return a ~200-char prefix that busts the per-key prompt cache.

    Anthropic's and OpenAI's prompt caches are keyed on the PREFIX of
    the prompt (the first ~1024 tokens). A suffix sentinel alone does
    NOT bust the prefix cache because the cache lookup ignores any
    change past the first cached breakpoint. This helper returns a
    string that the caller PREPENDS to the agent's user content so the
    prefix changes every iteration.

    The cache is scoped per-organization/API-key and content-addressed
    by a prefix hash -- NOT per-process. A byte-identical prefix sent
    from a SEPARATE process / SEPARATE benchmark run, under the same
    key, within the ~5-min TTL, HITS the prior run's cached prefix.
    Deriving the prefix from ``iter_id`` alone is therefore byte-
    identical across runs (``iter_id=0`` -> same prefix every run), so
    the second run's "cold" prefix silently HITS the first run's cache
    and corrupts both the latency and the cost measurement.

    ``run_salt`` defeats that cross-run collision: it is minted ONCE
    per orchestrator process and mixed into the first-block digest, so
    the prefix is DETERMINISTIC WITHIN a run (same ``run_salt`` for all
    iterations -> re-running an iteration is byte-identical) but DIFFERS
    across runs (a fresh ``run_salt`` each process -> guaranteed cold
    prefix). This mirrors vLLM's shipped ``cache_salt`` request field
    (``vllm/entrypoints/openai/completion/protocol.py`` mixes a per-
    request salt into the first-block hash to force a cold prefix).
    The cold-cache assertion (``cache_contaminated_count``)
    is the in-band proof the salt worked: a non-zero count means a
    cached prefix leaked through and the run is contaminated.

    The prefix is the SHA-256 hex digest of ``f"{run_salt}:{iter_id}"``,
    repeated until it reaches the configured length (~200 ASCII chars,
    ~50 BPE tokens -- large enough to defeat the suffix-aware cache
    scanners but small enough that the prefix does not dominate the
    prompt budget).
    """
    base = hashlib.sha256(f"{run_salt}:{iter_id}".encode()).hexdigest()
    repetitions = (COLD_CACHE_PREFIX_LEN + len(base) - 1) // len(base)
    return (base * repetitions)[:COLD_CACHE_PREFIX_LEN]


def read_daemon_cpu_ns(pid: int) -> tuple[int, int]:
    """Read ``(utime_ns, stime_ns)`` for the given pid from ``/proc/<pid>/stat``.

    The ``/proc/<pid>/stat`` format is documented in ``man 5 proc``:
    ``pid (comm) state ppid pgrp ... utime stime ...`` where ``comm``
    is the executable basename in parentheses and may itself contain
    parentheses or whitespace (linux ``prctl(PR_SET_NAME)`` does not
    forbid them). A naive ``line.split()`` shifts every field after
    ``comm`` and silently returns garbage CPU numbers.

    This implementation uses ``rsplit(') ', 1)`` to anchor past the
    last ``) `` (the close of the comm field, which is always the last
    occurrence because the kernel writes ``comm`` field with surrounding
    parentheses and a single trailing space). Fields after that are
    space-delimited and reliable to index by position (closes N3).

    Returns nanoseconds, not jiffies — the caller does NOT need to
    multiply by ``CLK_TCK``. The conversion happens here so every
    consumer sees a uniform time-base.
    """
    with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
        raw = fh.read().rstrip("\n")
    # Anchor on the LAST ``) `` so ``comm`` may contain parens or spaces.
    try:
        _prefix, tail = raw.rsplit(") ", 1)
    except ValueError as exc:
        raise ValueError(f"unexpected /proc/{pid}/stat layout: no ') ' anchor") from exc
    fields = tail.split()
    # Per ``man 5 proc``, after the comm close the remaining fields
    # start at index 0 with ``state``; ``utime`` is field 14 of the full
    # line (1-indexed) which is index 11 of the post-anchor list (state,
    # ppid, pgrp, session, tty_nr, tpgid, flags, minflt, cminflt,
    # majflt, cmajflt, utime, stime, ...).
    try:
        utime_jiffies = int(fields[11])
        stime_jiffies = int(fields[12])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"/proc/{pid}/stat field decode failed: {exc}") from exc
    clk_tck = os.sysconf("SC_CLK_TCK")
    if clk_tck <= 0:
        raise ValueError(f"SC_CLK_TCK reported non-positive value {clk_tck}")
    ns_per_jiffy = 1_000_000_000 // clk_tck
    return utime_jiffies * ns_per_jiffy, stime_jiffies * ns_per_jiffy


class SchedstatSnapshot(msgspec.Struct, frozen=True, kw_only=True):
    """Per-process scheduler-statistics snapshot aggregated across all threads.

    Linux exposes per-task scheduler statistics at ``/proc/<pid>/schedstat``
    (gated by ``CONFIG_SCHEDSTATS``; on by default on Ubuntu / Debian /
    most enterprise distros). The file format is three space-separated
    integers per ``man 5 proc``::

        <run_time_ns> <wait_time_ns> <pcount>

    ``run_time_ns`` is cumulative nanoseconds the task was scheduled-IN
    on a CPU since task creation; ``wait_time_ns`` is cumulative
    nanoseconds runnable-but-not-running; ``pcount`` is the cumulative
    dispatch (wake-up) count.

    The ``/proc/<pid>/schedstat`` entry is **per-task (per-TID) only**,
    NOT process-aggregated -- this is the load-bearing difference from
    ``/proc/<pid>/stat`` (which IS process-aggregated for utime/stime).
    A pid with multiple threads exposes one schedstat file per TID under
    ``/proc/<pid>/task/<tid>/schedstat``; the parent ``/proc/<pid>/schedstat``
    is the group-leader thread's snapshot ALONE. The waitbus daemon runs a
    main thread (mostly ``epoll_wait``-blocked) plus a doorbell thread
    that handles every event-emit notification, so reading only the
    group-leader masks 24-96% of the daemon's actual CPU under load.

    This snapshot sums every TID's field-0/1/2 across the daemon's TGID
    so the substrate reflects the daemon's TOTAL scheduler footprint.
    The caller may also inspect ``tid_count`` to confirm the TID walk
    saw the expected number of threads.
    """

    run_time_ns: int
    wait_time_ns: int
    pcount: int
    tid_count: int


# The substrate-unavailable sentinel value used when ``CONFIG_SCHEDSTATS=n``
# or every per-TID read fails. ``tid_count=0`` is the load-bearing field a
# caller checks; the other fields are zero-cleared so an accidental sum of
# unavailable + available samples does not pollute the available value.
_SCHEDSTAT_UNAVAILABLE = SchedstatSnapshot(
    run_time_ns=0,
    wait_time_ns=0,
    pcount=0,
    tid_count=0,
)


def schedstat_substrate_available() -> bool:
    """Probe whether ``/proc/self/schedstat`` is exposed on this kernel.

    Disambiguates "kernel without ``CONFIG_SCHEDSTATS``" from "process's
    main thread happens to have run_time == 0 right now". The probe reads
    the current process's own per-task schedstat; a kernel without
    ``CONFIG_SCHEDSTATS=y`` returns ``ENOENT`` on the file.

    Returns ``True`` if the kernel exposes per-task scheduler statistics,
    ``False`` otherwise. Cheap (single open + close); the bench calls
    this once at startup and refuses the run when ``--include-real-llm``
    is set and the substrate is missing.
    """
    try:
        with open("/proc/self/schedstat", encoding="utf-8") as fh:
            raw = fh.read().rstrip("\n")
    except OSError:
        return False
    return bool(raw.split())


def read_daemon_schedstat(pid: int) -> SchedstatSnapshot:
    """Aggregate ``/proc/<pid>/task/*/schedstat`` across every TID.

    Walks ``/proc/<pid>/task`` to enumerate the daemon's threads, reads
    each TID's schedstat (``run_time_ns``, ``wait_time_ns``, ``pcount``),
    and returns the field-wise sum. The aggregated snapshot is what the
    bench needs to test "did the daemon do work" -- a daemon that
    delegates I/O to a sibling thread (waitbus's ``_doorbell_thread``,
    typical of any event-driven daemon) would be invisible to a
    group-leader-only read.

    Race handling: a thread can exit between ``listdir`` and ``open``,
    yielding ``ENOENT`` on a per-TID schedstat file. Per-TID failures
    are silently skipped (the missing thread's contribution is lost,
    but the rest of the sum stays valid). ``tid_count`` reports the
    number of TIDs that contributed; the caller compares it against
    the expected thread count to detect a partial read.

    Substrate-unavailable signal: when ``CONFIG_SCHEDSTATS=n`` (the
    kernel does not expose the per-task counters) OR ``/proc/<pid>/task``
    itself returns ``ENOENT`` (the daemon exited), the returned snapshot
    has ``tid_count == 0`` and zero-cleared sum fields. A caller treats
    ``tid_count == 0`` over consecutive reads as "substrate unavailable"
    rather than "no scheduler activity."

    The non-aggregated parent ``/proc/<pid>/schedstat`` exposes the
    group-leader thread alone; this helper never reads it -- if a
    consumer needs the per-thread breakdown for forensic analysis they
    can read each ``/proc/<pid>/task/<tid>/schedstat`` directly.
    """
    task_dir = f"/proc/{pid}/task"
    try:
        tids = os.listdir(task_dir)
    except OSError:
        return _SCHEDSTAT_UNAVAILABLE
    run_sum = 0
    wait_sum = 0
    pcount_sum = 0
    tid_count = 0
    for tid in tids:
        try:
            with open(f"{task_dir}/{tid}/schedstat", encoding="utf-8") as fh:
                raw = fh.read().rstrip("\n")
        except OSError:
            # The TID exited between listdir and open. Skip it; do not
            # poison the aggregate. The lost contribution is bounded by
            # the per-TID activity since the most recent read.
            continue
        parts = raw.split()
        if len(parts) < 3:
            # Malformed line (kernel-version mismatch or torn read).
            # Skip with no contribution; the bench-side caller surfaces
            # the substrate-unavailable signal via tid_count when ALL
            # reads fail this way.
            continue
        try:
            run_sum += int(parts[0])
            wait_sum += int(parts[1])
            pcount_sum += int(parts[2])
        except ValueError:
            continue
        tid_count += 1
    if tid_count == 0:
        return _SCHEDSTAT_UNAVAILABLE
    return SchedstatSnapshot(
        run_time_ns=run_sum,
        wait_time_ns=wait_sum,
        pcount=pcount_sum,
        tid_count=tid_count,
    )


def read_daemon_vmrss_kb(pid: int) -> int:
    """Read the daemon's resident-set size in kilobytes from ``/proc/<pid>/status``.

    ``/proc/<pid>/status`` exposes a human-readable key:value table; the
    ``VmRSS:`` line is the kilobyte-quantized resident-set size of the
    process (the bytes mapped into physical memory, excluding swapped-out
    pages). Format::

        VmRSS:      14328 kB

    The kernel-side resolution is one kilobyte (the units suffix is fixed
    at ``kB`` per ``man 5 proc``); the bench reports the raw integer and
    leaves rendering to the verdict consumer.

    The bench samples VmRSS at the start and end of every window. The
    end-of-window snapshot is the primary signal for cross-arm comparison;
    the start-snapshot is retained so a downstream reader can audit
    per-window deltas (which expose transient allocator-bound spikes
    that the end-snapshot would silently smooth).

    Returns ``0`` on read failure (the process exited between the
    sample window and the read, the proc entry was torn down, or the
    ``VmRSS`` line is absent — possible on kernel-thread targets and on
    a very few specialised configs). The caller treats consecutive zeros
    as a substrate-not-available signal and emits a structured warning
    rather than crashing the bench mid-run, matching the
    ``read_daemon_schedstat`` fallback contract.
    """
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as fh:
            raw = fh.read()
    except OSError:
        return 0
    for line in raw.splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            # Expected: ["VmRSS:", "<kb>", "kB"]
            if len(parts) >= 2:
                try:
                    return int(parts[1])
                except ValueError:
                    return 0
    return 0


def capture_daemon_pragmas(db_path: Any) -> dict[str, str]:
    """Snapshot the daemon's SQLite PRAGMA configuration read-only.

    Opens the daemon's events DB via the ``file:<path>?mode=ro`` URI
    so the probe cannot write to the DB. Reads the PRAGMAs the bench's
    preflight pins by name (``journal_mode``, ``synchronous``,
    ``cache_size``, ``mmap_size``). Every value is coerced to ``str``
    so the verdict's serialised form has uniform typing.

    Returns ``{}`` (with a structured warning) on any I/O or sqlite
    error; the verdict consumer treats an empty dict as "probe failed"
    rather than "PRAGMAs were defaults". The PRAGMA snapshot is
    provenance, not a gate — a probe miss never fails preflight.
    """
    import sqlite3
    from pathlib import Path

    pragmas: dict[str, str] = {}
    db_path_obj = db_path if isinstance(db_path, Path) else Path(db_path)
    if not db_path_obj.exists():
        structured(
            _logger,
            logging.WARNING,
            "bench_daemon_pragma_probe_missing_db",
            db_path=str(db_path_obj),
        )
        return pragmas
    uri = f"file:{db_path_obj}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    except sqlite3.Error as exc:
        structured(
            _logger,
            logging.WARNING,
            "bench_daemon_pragma_probe_open_failed",
            db_path=str(db_path_obj),
            error=str(exc),
        )
        return pragmas
    try:
        for pragma in ("journal_mode", "synchronous", "cache_size", "mmap_size"):
            try:
                row = conn.execute(f"PRAGMA {pragma}").fetchone()
            except sqlite3.Error as exc:
                structured(
                    _logger,
                    logging.WARNING,
                    "bench_daemon_pragma_probe_query_failed",
                    pragma=pragma,
                    error=str(exc),
                )
                continue
            if row is None:
                continue
            pragmas[pragma] = str(row[0])
    finally:
        conn.close()
    return pragmas


# ---------------------------------------------------------------------
# Aggregation helpers used by ExternalStateReport finalisation.
# ---------------------------------------------------------------------


def merge_observed_models(
    report_field: Iterable[str],
    observed: str | None,
) -> list[str]:
    """Append ``observed`` to the existing observed-models list, dedup-preserving.

    The bench-level aggregator drives ``ExternalStateReport``'s
    per-driver model list via successive calls to this helper. Order is
    preserved so the verdict's record reflects the actual order of
    first observation across iterations.
    """
    existing = list(report_field)
    if observed is None or observed in existing:
        return existing
    existing.append(observed)
    return existing


def merge_distribution(distribution: Mapping[str, int], key: str | None) -> dict[str, int]:
    """Increment ``distribution[key]`` by one; return a new dict.

    ``key=None`` is silently dropped (no implicit ``"unknown"`` bucket
    so the verdict's distribution does not over-claim a category that
    was never observed).
    """
    result = dict(distribution)
    if key is None:
        return result
    result[key] = result.get(key, 0) + 1
    return result


# ---------------------------------------------------------------------
# Bench run-artifact path resolution + progress JSONL append.
# ---------------------------------------------------------------------


def resolve_bench_log_paths(*, bench_name: str, output: Path | None) -> tuple[Path, Path, Path]:
    """Return ``(verdict_path, progress_path, log_path)`` for one bench run.

    ``output is None`` selects the default layout:
    ``.local-stress-logs/<ts>.<bench_name>.{verdict.json,progress.jsonl,log}``
    under the current working directory, with all three files sharing a
    timestamped stem so one run's artefacts sort together.

    ``output is not None`` treats ``output`` as the verdict path. The
    sibling progress/log files derive from the verdict's stem so all
    three sort together. Stem derivation strips exactly the trailing
    ``.verdict.json`` from the operator's output when present, otherwise
    drops only the single final suffix. This avoids the blind double-
    ``with_suffix("")`` strip that over-trims a multi-dotted name
    (``foo.tar.gz`` would lose ``.tar.gz`` and leave the three files on
    different stems); progress/log are always ``<stem>.progress.jsonl``
    / ``<stem>.log`` beside the verdict.
    """
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        name = output.name
        suffix = ".verdict.json"
        stem_name = name[: -len(suffix)] if name.endswith(suffix) else output.stem
        return (
            output,
            output.with_name(f"{stem_name}.progress.jsonl"),
            output.with_name(f"{stem_name}.log"),
        )
    log_root = Path.cwd() / ".local-stress-logs"
    log_root.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    base = log_root / f"{ts}.{bench_name}"
    return (
        base.with_suffix(".verdict.json"),
        base.with_suffix(".progress.jsonl"),
        base.with_suffix(".log"),
    )


# The value types the progress JSONL is DESIGNED to coerce via ``str``:
# ``Path`` (run-artifact paths) and ``datetime`` (timestamps). A value
# of any other type reaching the ``default`` hook is a record the bench
# did not anticipate -- still serialised (bounded-loss progress log), but
# worth one structured warning so the silent stringify is observable.
_JSONL_EXPECTED_DEFAULT_TYPES: Final = (Path, _dt.datetime)

# One-shot guard: the unexpected-type warning fires at most once per
# process so a misshaped record repeated every iteration does not flood
# the orchestrator's log.
_jsonl_unexpected_type_warned = False


def _jsonl_default(value: Any) -> str:
    """``json.dumps`` ``default`` hook: stringify, warning once on a surprise type.

    Invoked by ``json.dumps`` only for values it cannot natively encode, so
    the all-JSON-native common path never reaches here (zero added cost).
    ``Path`` / ``datetime`` are the documented-expected coercions and pass
    silently; the first value of any OTHER type emits a single structured
    warning so an unanticipated record shape is operator-visible rather than
    silently flattened to its ``str``.
    """
    global _jsonl_unexpected_type_warned
    if not isinstance(value, _JSONL_EXPECTED_DEFAULT_TYPES) and not _jsonl_unexpected_type_warned:
        _jsonl_unexpected_type_warned = True
        structured(
            _logger,
            logging.WARNING,
            "bench_progress_jsonl_unexpected_type",
            value_type=type(value).__name__,
        )
    return str(value)


def append_jsonl_record(fh: Any, record: dict[str, Any]) -> None:
    """Append one JSON line + flush; the progress file MUST be tail-able.

    The explicit ``flush()`` per record writes the kernel buffer
    immediately so a concurrent ``tail -F`` consumer sees each line the
    moment it is captured. This is a deliberately bounded-loss progress
    log: it does NOT ``fsync`` (the flush is enough for ``tail -F`` and
    a crash loses at most the unsynced kernel-buffer tail, which the
    verdict file -- written durably at run end -- supersedes; durability
    research). The ``_jsonl_default`` hook lets non-JSON-native
    values (``Path``, ``datetime``) serialise rather than raising, and
    surfaces a one-time structured warning if an UNEXPECTED type is
    coerced so the stringify is not silent.
    """
    fh.write(json.dumps(record, default=_jsonl_default) + "\n")
    fh.flush()
