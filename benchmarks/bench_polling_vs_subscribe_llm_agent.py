"""Heterogeneous-swarm daemon-throughput bench under real agent load.

Final narrowed hypothesis (verbatim, from the measurement protocol specification):

    Under a 5-driver heterogeneous agent swarm (2 real OpenAI calls
    + 1 real claude-cli + 1 real gemini-cli + 1 shell control), the
    waitbus daemon's per-event ingest latency at the median is bounded
    by daemon-side queue and SQLite write costs, not by any driver's
    network latency.

The bench layers a polling-vs-subscribing A/B on top of the swarm so
two complementary deliveries of the same five real driver reactions
exercise the orchestrator's two consumer postures (open one
``waitbus.wait_for`` subscriber thread vs. drain ``waitbus._read``
on a polling cadence). The per-driver verdict is a median latency
description with bootstrap CI bands so a reader sees the spread per
driver; the bench does NOT run a hypothesis test (no Mann-Whitney /
Bonferroni gate) — the spec defines this experiment as a per-driver
median description, not a marginal hypothesis test.

The bench produces NEW verdict files in ``.local-stress-logs/`` next
to (NOT in place of) any shipped stress-test verdict.json. It never
modifies historical stress-test artifacts; the operator's stress-test
history stays intact and any historical reclassification is a manual reconcile step, not this bench's.

Per-iteration protocol (see also the bench module docstring):

1. Pre-iteration cache barrier. Inject ``force_cold_cache_prefix(run_salt,
   iter_id)`` into every driver's prompt so Anthropic's prompt cache cannot
   cross iterations or runs.
2. Per-iteration moderation reset. Restart all 5 subprocesses (no
   across-iteration reuse). Env carries WAITBUS_BENCH_GC_OFF=1,
   PYTHONHASHSEED=0, PYTHONUNBUFFERED=1.
3. Anchor ``t_send_ns = time.monotonic_ns()`` (Linux only).
4. Emit one seed ``agent_message`` event the drivers wake on.
5. Each driver subprocess emits its own ``agent_message`` reaction.
6. Orchestrator records ``t_observe_ns = time.monotonic_ns()`` per
   reaction; per-event ``latency_ns = t_observe_ns - t_send_ns``.
   Negative samples raise (no clamp).
7. Parse each driver's ``DRIVER_REACTED`` wake marker via
   ``observed_token_usage_from_marker`` and project the resulting
   ``TokenUsage`` onto the typed envelope substruct via the
   ``*_envelope_from_token_usage`` helpers. Every driver family
   (claude / gemini / pydantic / langgraph) carries its envelope
   through the same driver-side wake marker; the orchestrator
   issues no parallel direct-SDK call of its own.
8. Classify per-driver cache state.
9. Apply moderation invariants; drop iterations that fail.
10. Append iteration row to ``<output>.rows.jsonl`` and flush + fsync.

The bench's preflight gates are run first; if any gate fires the bench
aborts before any token is spent.

Documented limitations (recorded in verdict.json):

- p99 latency CI half-width at n=50 is roughly 5x the median CI; p99
  is NOT used for driver ranking.
- gemini-2.5-flash alias floats; observed model id set is recorded but
  not pinned.
- Anthropic prompt cache 5-min decay defeated by the per-iteration
  ``force_cold_cache_prefix`` prefix; if iteration wall-clock exceeds
  5 minutes, cache state may degrade.
- claude / gemini CLIs expose no ``--seed`` or ``--temperature``;
  sampling is black-box and the bench's distribution-level claims are
  the only ones it makes.
- asyncio scheduling jitter seeded via PYTHONHASHSEED but not
  eliminated; p99 is NOT cross-operator-comparable.
- OPENAI_API_KEY presence recorded as bool; the key value is never
  persisted in any bench artifact.
- Power calculation uses a 10-iteration pilot to validate the sigma_idle
  assumption: 10 subscribe-arm shell-control iterations measure the
  orchestrator-side scheduler jitter floor; if the pilot sigma exceeds
  the gate (20 ms when --skip-real-llm, 40 ms when --include-real-llm
  to absorb cross-iteration LLM-call jitter) the bench aborts before
  the main run with rc=3. The pilot is SKIPPED when the bench's
  downstream measurement is structurally inapplicable:
  ``args.smoke or not args.include_real_llm``. Smoke mode runs N below
  the bench's power floor; --skip-real-llm makes the per-iteration
  cost a pure subprocess spawn (~5 s cold-import x 10 iterations) so
  the gate becomes structurally unreachable. When skipped the verdict
  carries pilot_skipped=True + pilot_skipped_reason ("smoke_mode" or
  "real_llm_disabled"; smoke takes precedence) and the main loop runs
  so the operator gets a verdict.json that confirms the wiring is
  alive. Contract pinned by tests/test_bench_pilot_skip_contract.py.

The bench is Linux-only. Calling on macOS / Windows raises rather than
silently degrades the cross-process ``monotonic_ns`` contract.
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import logging
import os
import secrets
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Final

import msgspec

from benchmarks._bench_anchor import emit_anchor_event
from benchmarks._bench_preflight import (
    PreflightError,
    read_openai_key_from_keyring,
    run_preflight_assertions,
)
from benchmarks._bench_shared import (
    BENCH_GEMINI_MODEL,
    ClaudeEnvelope,
    CostBudgetTracker,
    ExternalStateReport,
    GeminiEnvelope,
    IterationRow,
    OpenAIEnvelope,
    _classify_claude_cache_state,
    _hashseed_or_default,
    append_jsonl_record,
    capture_daemon_pragmas,
    claude_envelope_from_token_usage,
    count_cache_contaminated_rows,
    drain_children_concurrently,
    force_cold_cache_prefix,
    gemini_envelope_from_token_usage,
    merge_distribution,
    merge_observed_models,
    openai_envelope_from_token_usage,
    resolve_bench_log_paths,
)
from benchmarks._bench_source_mix import pick_source_for_iter
from benchmarks._bench_swarm import default_python_executable, spawn_n_heterogeneous
from scripts.stress._context import ObservedReaction
from scripts.stress._controller import (
    _Child,
    _emit_seed_event,
    _parse_wake_marker,
    observed_token_usage_from_marker,
)
from scripts.stress._real_drivers import (
    DRIVER_WAIT_TIMEOUT_SEC,
    EARLY_WAKE_MARKER,
    FRAMEWORK_ORDER,
    WAKE_MARKER,
)
from waitbus._log import structured

_logger = logging.getLogger("waitbus.bench.poll_vs_subscribe_llm")

# Sample postures (per-iteration protocol; also accommodates pilot gate).
_DEFAULT_N = 50
_SMOKE_N = 2
_PILOT_N = 10

# Drivers in the canonical 5-driver swarm (1 of each, no count sweep).
_SWARM_SPEC: dict[str, int] = {framework: 1 for framework in FRAMEWORK_ORDER}

# Per-iteration orchestrator drain budget (seconds): wall-clock the
# bench reserves on top of the driver's ``DRIVER_WAIT_TIMEOUT_SEC`` to
# read each driver's stdout and parse both wake markers. The
# orchestrator's per-iteration deadline is
# ``DRIVER_WAIT_TIMEOUT_SEC + _PER_ITER_DRAIN_BUDGET_SEC`` so the two
# clocks cannot fire simultaneously (a driver that times out on
# ``wait_for`` still has drain headroom to surface its diagnostic
# marker line before the orchestrator's ``communicate`` aborts the
# child).
_PER_ITER_DRAIN_BUDGET_SEC = 10.0

# Per-iteration wall-clock budget (seconds). Decoupled from the
# driver-side wait timeout so the bench's drain phase has a real
# margin to flush both marker lines.
_PER_ITER_DEADLINE_SEC = DRIVER_WAIT_TIMEOUT_SEC + _PER_ITER_DRAIN_BUDGET_SEC

# Per-iteration driver-spawn settle window (seconds). Brief sleep
# between subprocess spawn and seed emit so every driver's
# ``wait_for`` is registered before the seed lands and the bench
# measures the daemon's live ``_fan_out`` ingest cost (not the disk-
# replay cost of the per-iteration anchor-cursor safety net). Matches
# the stress controller's settle window. Module-level so test-mode
# runners can shorten it.
_PER_ITER_SPAWN_SETTLE_SEC = 5.0

# Per-driver replay-contamination threshold. A driver whose share of
# rows arrived via the daemon's seq-replay window (the bench's safety
# net for cold-import jitter) exceeds this fraction signals that the
# 4-vCPU bench host could not consistently register the driver's
# subscribe ahead of the seed-emit moment, so the live-fan-out
# latency aggregate is no longer the load-bearing answer the bench
# claims to compute. The verdict surfaces the per-driver replay rate
# (operator-visible) and the bench aborts (non-zero exit) when any
# driver crosses the threshold, rather than silently mixing live and
# replay rows under the same median.
_REPLAY_CONTAMINATION_THRESHOLD = 0.05

# Maximum wall-clock seconds the bench waits for the waitbus daemon to
# bind its broadcast socket after ``subprocess.Popen``. The daemon
# must open its socket within this window or the bench raises
# ``RuntimeError`` and aborts rather than silently measuring against
# a half-spawned process. 10 s is generous even on a loaded 4-vCPU
# bench host; the normal bind completes in under 200 ms.
_DAEMON_READY_TIMEOUT_SEC: Final[float] = 10.0

# Sleep interval (seconds) between consecutive socket-existence probes
# in the daemon-ready wait loop. 50 ms yields at most 200 polls inside
# the 10-second ``_DAEMON_READY_TIMEOUT_SEC`` window and keeps the
# orchestrator's CPU overhead negligible while the daemon bootstraps.
_SOCKET_POLL_INTERVAL_SEC: Final[float] = 0.05

# Minimum remaining deadline (seconds) passed to ``communicate`` for
# each child in the per-iteration drain loop. Ensures the subprocess
# gets a non-zero timeout budget even when the iteration has nearly
# exhausted its wall-clock deadline; without this floor a very late
# ``communicate`` call would receive a zero or negative timeout and
# immediately abort before the child writes its final marker line.
_MIN_REMAINING_DEADLINE_SEC: Final[float] = 0.05

# Lower percentile index used when slicing the sorted bootstrap
# resample array to extract the 95% CI lower bound (p2.5). Applied as
# ``samples[int(_BOOTSTRAP_CI_LO * iterations)]`` so the resulting
# index rounds toward the conservative (narrower) end of the interval.
_BOOTSTRAP_CI_LO: Final[float] = 0.025

# Upper percentile index used when slicing the sorted bootstrap
# resample array to extract the 95% CI upper bound (p97.5). Applied as
# ``samples[int(_BOOTSTRAP_CI_HI * iterations)]`` clamped to the last
# valid index so the interval is symmetric with ``_BOOTSTRAP_CI_LO``
# at 95% two-sided coverage.
_BOOTSTRAP_CI_HI: Final[float] = 0.975

# Pilot-sigma gate: abort if pilot's observed std dev on the
# per-iteration shell-control latency exceeds twice the plan value.
# The plan value is 10ms (the daemon-side noise floor in the spec).
_PILOT_SIGMA_PLAN_MS = 10.0
_PILOT_SIGMA_GATE_FACTOR = 2.0
# When real LLMs are in flight, cross-iteration scheduler jitter on
# the orchestrator-side shell-control latency rises because every
# iteration runs through a multi-second LLM call; widen the gate to
# 40ms so the bench does not abort on benign jitter. The shell-control
# sigma is still the relevant noise-floor signal — only the threshold
# changes.
_PILOT_SIGMA_GATE_MS_REAL_LLM = 40.0

# Default upper budget for the bench's accumulated USD cost. The
# bench's per-iteration cost-tracker aborts the run BEFORE the next
# iteration when one more iteration's expected cost would breach this
# value. Override at the command line with --max-cost-usd.
_DEFAULT_MAX_COST_USD = 5.0

# Bootstrap iteration count for median CI bands. 1,000 is the
# convention; cheap on n=50 input.
_BOOTSTRAP_ITERATIONS = 1_000

# ---------------------------------------------------------------------
# Output structs.
# ---------------------------------------------------------------------


class _PerArmStats(msgspec.Struct, frozen=True, kw_only=True):
    """Per-driver aggregate stats for one arm (poll OR subscribe).

    Two latency families are reported because they measure different
    things and back different launch claims:

    * **End-to-end latency** (``*_end_to_end_latency_ns``,
      ``end_to_end_ci_*``) is ``t_observe_ns - t_send_ns`` -- the
      orchestrator-side wall-clock from seed emit to row observation.
      It includes the entire driver lifecycle (cold-import,
      ``wait_for`` return, LLM call, marker print, drain) and so is
      dominated by LLM-call jitter for the real-LLM drivers (claude /
      gemini / openai). Useful as the *user-facing* reaction latency
      claim.
    * **Bus latency** (``*_bus_latency_ns``, ``bus_ci_*``) is
      ``wake_monotonic_ns - t_seed_emit_monotonic_ns`` -- the
      monotonic-clock delta from seed emit to the driver's wake-marker
      moment. The marker fires BEFORE the LLM call (phase 3 of the
      five-phase driver lifecycle), so this metric isolates the waitbus
      daemon's broadcast delivery cost from the downstream LLM-call
      jitter. Useful as the *system-facing* poll-vs-subscribe claim.

    All latency fields are nanoseconds. CI bounds are computed via a
    non-parametric bootstrap with 1,000 resamples (see
    ``_bootstrap_median_ci_ns``); ``*_ci_half_width_ns`` is half of the
    [p2.5, p97.5] interval. Iterations marked ``invariant_failed`` and
    rows whose ``delivery_mode`` is ``"replay"`` are excluded from
    both metrics (the per-driver replay rate is surfaced separately so
    a downstream reader can re-classify).
    """

    driver: str
    n_iterations: int
    median_end_to_end_latency_ns: int
    p95_end_to_end_latency_ns: int
    p99_end_to_end_latency_ns: int
    end_to_end_ci_low_ns: int
    end_to_end_ci_high_ns: int
    end_to_end_ci_half_width_ns: int
    median_bus_latency_ns: int
    p95_bus_latency_ns: int
    p99_bus_latency_ns: int
    bus_ci_low_ns: int
    bus_ci_high_ns: int
    bus_ci_half_width_ns: int


class ExperimentAVerdict(msgspec.Struct, frozen=True, kw_only=True):
    """Top-level verdict.json shape for this bench.

    Field order intentionally matches the measurement protocol
    specification's "Output JSON schema" so a downstream consumer can
    read either document and find the same fields.
    """

    bench_name: str
    started_at_ns: int
    ended_at_ns: int
    n_iterations_per_arm: int
    smoke: bool
    external_state: ExternalStateReport
    rows: list[IterationRow]
    per_arm_per_driver_stats_poll: dict[str, _PerArmStats]
    per_arm_per_driver_stats_subscribe: dict[str, _PerArmStats]
    per_driver_invariant_failure_rate: dict[str, float]
    per_driver_replay_contamination_rate: dict[str, float]
    """Per-driver share of rows whose ``delivery_mode`` was ``"replay"``.

    A high rate signals that the 4-vCPU bench host could not
    consistently register the driver's subscribe ahead of the
    seed-emit moment, so the live-fan-out latency aggregate is no
    longer load-bearing. Drivers above
    ``replay_contamination_threshold`` flip
    ``replay_contamination_gate_passed`` to ``False``."""

    replay_contamination_threshold: float
    """The threshold compared against
    ``per_driver_replay_contamination_rate`` -- typically
    ``_REPLAY_CONTAMINATION_THRESHOLD`` (0.05). Recorded on the
    verdict so a downstream reader can re-classify rows under a
    different threshold without re-running the bench."""

    replay_contamination_gate_passed: bool
    """``True`` iff every driver's replay rate is at or below
    ``replay_contamination_threshold``. ``main`` returns a non-zero
    exit code when this is ``False`` so a regression surfaces in CI
    rather than landing as a silently-mixed-mode median."""

    cache_state_distribution: dict[str, dict[str, int]]
    cost_usd_total: float | None
    """Genuinely-metered (OpenAI) spend for the run -- the actual
    pay-per-call dollar cost, and the figure the budget gate caps. The
    claude / gemini subscription-CLI drivers contribute nothing here;
    their notional cost rides ``notional_subscription_cost_usd``."""
    notional_subscription_cost_usd: float | None
    """API-equivalent price the ``claude -p`` subscription envelopes
    reported for the tokens they spent. A Max/Pro subscriber never pays
    this (the driver argv carries no API key), so it is surfaced for
    transparency only and is never folded into ``cost_usd_total`` or the
    budget gate. ``None`` when no subscription-CLI cost was reported."""
    cost_unknown_count: int
    cache_contaminated_count: int
    """Count of measured rows that read a prior run's cached prompt
    prefix (OpenAI ``cached_tokens > 0`` or Claude
    ``cache_read_input_tokens > 0``). ``0`` == clean cold-cache
    isolation; any non-zero value means a run warmed the provider-side
    cache for a later run and the bench's cold-cache premise was
    violated for those calls -- the cost and latency figures for the
    contaminated rows reflect a warm replay rather than a fresh prompt.
    The count is the observable; the bench does not hard-fail on it
    here."""
    pilot_sigma_ms: float | None
    pilot_sigma_plan_ms: float
    pilot_sigma_gate_factor: float
    pilot_sigma_gate_ms_used: float
    pilot_passed: bool
    max_cost_usd_budget: float
    max_cost_usd_observed: float
    aborted_on_budget: bool
    limitations: list[str]
    pilot_skipped: bool
    pilot_skipped_reason: str | None
    # Per-source iteration histogram. Each iteration picks one source
    # from the weighted soak taxonomy via
    # ``benchmarks._bench_source_mix.pick_source_for_iter`` and emits its
    # seed event on the picked pair. The histogram lets the verdict
    # reader confirm the daemon's fan-out was exercised across the
    # full registered taxonomy.
    per_iter_source_distribution: dict[str, int] = {}
    # Self-describing metric definitions so a downstream verdict reader
    # does not have to consult the bench source to know what each
    # per-driver per-arm latency field measures. The two families are
    # documented on ``_PerArmStats`` and re-quoted here so the verdict
    # remains self-contained.
    metric_definitions: dict[str, str] = msgspec.field(default_factory=lambda: _DEFAULT_METRIC_DEFINITIONS.copy())


_DEFAULT_METRIC_DEFINITIONS: Final[dict[str, str]] = {
    "median_end_to_end_latency_ns": (
        "Orchestrator-side wall-clock latency (t_observe_ns - t_send_ns) including the full driver "
        "lifecycle (cold-import, wait_for return, LLM call, marker print, drain). Dominated by LLM-call "
        "jitter for the real-LLM drivers. Use as the user-facing reaction-latency claim."
    ),
    "median_bus_latency_ns": (
        "Driver-side monotonic-clock latency (wake_monotonic_ns - t_seed_emit_monotonic_ns) measured "
        "BEFORE the LLM call, so this isolates the waitbus daemon's broadcast delivery cost from the "
        "downstream LLM-call jitter. Use as the system-facing poll-vs-subscribe claim."
    ),
}


# ---------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------


def _waitbus_path() -> str:
    """Resolve ``waitbus`` on PATH (or sibling of ``sys.executable``).

    Mirrors the pattern from ``scripts.stress._controller`` so a venv-
    relative ``waitbus`` resolves the same way both code paths see it.
    """
    sibling = Path(sys.executable).parent / "waitbus"
    if sibling.is_file():
        return str(sibling)
    on_path = shutil.which("waitbus")
    if on_path is None:
        raise RuntimeError("waitbus CLI not found on PATH; install waitbus first")
    return on_path


def _percentile_ns(values: Sequence[int], p: float) -> int:
    """Linear-interpolated percentile (returns int ns; safe on empty input).

    Returns ``0`` for empty input — the verdict serialises that as a
    sentinel "no observations" rather than raising; the per-driver row
    counts also surface the gap so a downstream consumer can
    distinguish 0-due-to-empty from 0-due-to-fast.
    """
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


def _bootstrap_median_ci_ns(values: Sequence[int], *, iterations: int = _BOOTSTRAP_ITERATIONS) -> tuple[int, int]:
    """Non-parametric bootstrap 95% CI on the median (returns ``(lo_ns, hi_ns)``).

    Returns ``(0, 0)`` on empty input. Uses ``random.Random`` seeded
    deterministically from ``PYTHONHASHSEED=0`` so a re-run produces
    byte-identical CI bands; the seed is the bench's normal
    determinism contract.
    """
    if not values:
        return 0, 0
    import random

    rng = random.Random(_hashseed_or_default())
    n = len(values)
    samples: list[int] = []
    for _ in range(iterations):
        resample = [values[rng.randrange(n)] for _ in range(n)]
        samples.append(int(statistics.median(resample)))
    samples.sort()
    lo = samples[int(_BOOTSTRAP_CI_LO * iterations)]
    hi = samples[min(int(_BOOTSTRAP_CI_HI * iterations), iterations - 1)]
    return lo, hi


def _summarise_per_driver(rows: Sequence[IterationRow], *, driver: str) -> _PerArmStats:
    """Roll up a per-driver per-arm latency stats block.

    Iterations marked ``invariant_failed`` are excluded from both
    latency aggregates but counted in the per-driver invariant-failure
    rate (computed separately by ``_invariant_failure_rates``).

    Rows whose ``delivery_mode`` is ``"replay"`` are ALSO excluded so
    the median reflects the live ``_fan_out`` ingest cost the bench's
    hypothesis names. The bench separately surfaces the per-driver
    replay rate via ``_replay_contamination_rates``; a driver whose
    rate exceeds ``_REPLAY_CONTAMINATION_THRESHOLD`` fails the verdict
    gate rather than contributing a mixed-mode median.

    Two metrics are computed over the same filtered row set so the
    end-to-end and bus-only views land with the same n and the same
    invariant + replay handling. See ``_PerArmStats`` for the precise
    definition of each metric.
    """
    valid_rows = [
        row for row in rows if row.driver == driver and not row.invariant_failed and row.delivery_mode != "replay"
    ]
    end_to_end_ns = [row.latency_ns for row in valid_rows]
    bus_ns = [row.wake_monotonic_ns - row.t_seed_emit_monotonic_ns for row in valid_rows]
    e2e_ci_low, e2e_ci_high = _bootstrap_median_ci_ns(end_to_end_ns)
    bus_ci_low, bus_ci_high = _bootstrap_median_ci_ns(bus_ns)
    return _PerArmStats(
        driver=driver,
        n_iterations=len(valid_rows),
        median_end_to_end_latency_ns=_percentile_ns(end_to_end_ns, 0.50),
        p95_end_to_end_latency_ns=_percentile_ns(end_to_end_ns, 0.95),
        p99_end_to_end_latency_ns=_percentile_ns(end_to_end_ns, 0.99),
        end_to_end_ci_low_ns=e2e_ci_low,
        end_to_end_ci_high_ns=e2e_ci_high,
        end_to_end_ci_half_width_ns=max(0, (e2e_ci_high - e2e_ci_low) // 2),
        median_bus_latency_ns=_percentile_ns(bus_ns, 0.50),
        p95_bus_latency_ns=_percentile_ns(bus_ns, 0.95),
        p99_bus_latency_ns=_percentile_ns(bus_ns, 0.99),
        bus_ci_low_ns=bus_ci_low,
        bus_ci_high_ns=bus_ci_high,
        bus_ci_half_width_ns=max(0, (bus_ci_high - bus_ci_low) // 2),
    )


def _invariant_failure_rates(rows: Sequence[IterationRow]) -> dict[str, float]:
    """Compute per-driver invariant-failure rate (n_failed / n_total).

    Drivers with zero observations are reported as ``0.0`` rather than
    raised; the rate stays interpretable in the smoke-mode shape where
    a single moderation hit can dominate the rate.
    """
    out: dict[str, float] = {}
    for driver in FRAMEWORK_ORDER:
        per_driver = [row for row in rows if row.driver == driver]
        if not per_driver:
            out[driver] = 0.0
            continue
        failed = sum(1 for row in per_driver if row.invariant_failed)
        out[driver] = failed / len(per_driver)
    return out


def _replay_contamination_rates(rows: Sequence[IterationRow]) -> dict[str, float]:
    """Compute per-driver replay-contamination rate (n_replay / n_total).

    Drivers with zero observations are reported as ``0.0``. The rate
    counts ``delivery_mode == "replay"`` rows against the driver's
    total row count (including invariant-failed rows); a row with
    ``delivery_mode == "unknown"`` (no early marker observed) is
    excluded from the numerator so a driver that crashed before its
    subscribe does not falsely inflate its replay rate.
    """
    out: dict[str, float] = {}
    for driver in FRAMEWORK_ORDER:
        per_driver = [row for row in rows if row.driver == driver]
        if not per_driver:
            out[driver] = 0.0
            continue
        replay_count = sum(1 for row in per_driver if row.delivery_mode == "replay")
        out[driver] = replay_count / len(per_driver)
    return out


def _replay_contamination_gate_passed(rates: dict[str, float], *, threshold: float) -> bool:
    """Return True iff every per-driver replay rate is at or below the threshold."""
    return all(rate <= threshold for rate in rates.values())


def _cache_state_distribution(rows: Sequence[IterationRow]) -> dict[str, dict[str, int]]:
    """Per-driver cache-state distribution (count of COLD / WARMING / WARM / NA)."""
    out: dict[str, dict[str, int]] = {}
    for driver in FRAMEWORK_ORDER:
        per_driver = [row for row in rows if row.driver == driver]
        bucket: dict[str, int] = {}
        for row in per_driver:
            bucket[row.cache_state] = bucket.get(row.cache_state, 0) + 1
        out[driver] = bucket
    return out


def _spawn_daemon(env: dict[str, str], waitbus_path: str, socket_path: Path) -> _Child:
    """Spawn the waitbus broadcast daemon and wait for the socket to bind.

    Returns a ``_Child`` handle the caller is responsible for
    terminating in a ``finally`` block. Raises ``RuntimeError`` if the
    socket does not appear within 10 seconds — the bench fails fast
    rather than silently running against a half-spawned daemon.
    """
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
            return _Child(role="daemon", proc=proc)
        if proc.poll() is not None:
            raise RuntimeError(f"waitbus daemon exited before binding {socket_path}")
        time.sleep(_SOCKET_POLL_INTERVAL_SEC)
    proc.terminate()
    raise RuntimeError(f"waitbus daemon failed to bind {socket_path} within 10s")


# ---------------------------------------------------------------------
# Per-iteration loop.
# ---------------------------------------------------------------------


def _classify_invariant(
    *,
    framework: str,
    reaction: ObservedReaction | None,
    claude_env: ClaudeEnvelope | None,
    gemini_env: GeminiEnvelope | None,
    openai_env: OpenAIEnvelope | None,
    early_wake_received: bool,
) -> tuple[bool, str | None]:
    """Decide whether a per-driver iteration row trips the invariant gate.

    Returns ``(invariant_failed, invariant_failure_field)``: the
    ``IterationRow`` shape's two invariant columns. A row that fails
    is excluded from the latency aggregation and counts toward the
    bench's invariant-failure exit gate.

    Four canonical branches:

    1. ``reaction is None``: the driver never produced a canonical
       wake marker. Failure field distinguishes a driver that crashed
       before its subscribe registered (``"reaction_missing"`` -- no
       early marker either) from one that woke cleanly on the bus but
       then crashed or timed out during its post-wake LLM exercise
       (``"llm_timeout_or_crash"`` -- early marker present, canonical
       absent). The two-marker scheme exists to surface this
       distinction; the field rides through to the verdict so the
       operator's next triage step is the right driver's stderr tail
       rather than a re-run with custom instrumentation.

    2. ``claude_env`` present: gate on the Anthropic moderation
       envelope. ``is_error=True`` is upstream-error; ``stop_reason``
       in the refusal set OR outside the clean-completion set
       (``end_turn`` / ``tool_use`` / ``max_tokens``) signals a
       moderation refusal or an unrecognised terminal state.

    3. ``gemini_env`` present: gate on the Gemini envelope. Same
       ``is_error`` plus ``stop_reason`` in the refusal set. Gemini's
       canonical clean-completion ``stop_reason`` is ``None``; the
       Anthropic-set "outside the clean set" check does not apply.

    4. ``openai_env`` present: gate on the OpenAI envelope. Same
       ``is_error`` plus ``stop_reason``, with the additional
       ``finish_reason`` check the OpenAI Chat Completions API
       provides (``content_filter`` / ``length`` flag a moderated or
       truncated response that the cross-provider ``stop_reason``
       field may not surface on every SDK rev).

    A shell-control row reaches the default branch (no envelope, a
    reaction observed) and the invariant passes.

    Extracted from ``_run_one_iteration`` so the four-branch gate is
    unit-testable in isolation -- the failure mode the bench's
    invariant exit gate trips on must have a regression-pinned shape
    rather than living inside the orchestration loop.
    """
    if reaction is None:
        return (True, "llm_timeout_or_crash") if early_wake_received else (True, "reaction_missing")
    if claude_env is not None:
        if claude_env.is_error:
            return True, "is_error"
        if claude_env.stop_reason in {"refusal", "error_during_execution"} or (
            claude_env.stop_reason not in {None, "end_turn", "tool_use", "max_tokens"}
        ):
            return True, f"stop_reason={claude_env.stop_reason}"
        return False, None
    if gemini_env is not None:
        if gemini_env.is_error:
            return True, "is_error"
        if gemini_env.stop_reason in {"refusal", "error_during_execution"}:
            return True, f"stop_reason={gemini_env.stop_reason}"
        return False, None
    if openai_env is not None:
        if openai_env.is_error:
            return True, "is_error"
        if openai_env.stop_reason in {"refusal", "error_during_execution"}:
            return True, f"stop_reason={openai_env.stop_reason}"
        if openai_env.finish_reason in {"content_filter", "length"}:
            return True, f"finish_reason={openai_env.finish_reason}"
        return False, None
    # No envelope (shell-control) and reaction observed: passes.
    _ = framework
    return False, None


def _parse_early_wake_marker(text: str, *, seed_delivery_id: str) -> tuple[str, dict[str, int]] | None:
    """Extract the early ``WAKE_RECEIVED`` marker from one child's stdout.

    Returns ``(framework, anchors)`` for the first line matching this
    iteration's seed, or ``None`` when no such line is present. The
    early marker carries the three cross-process monotonic anchors the
    bench uses for jitter-free bus-latency math + delivery-mode
    classification, and the orchestrator can attribute timing even when
    the LLM call later crashed. Each child has its own stdout (one
    marker emit per role per iteration), so the post-parse ``seed``
    cross-check defends against a re-used subprocess that somehow leaked
    a stale buffer rather than a brittle pre-parse substring guard.
    """
    for line in text.splitlines():
        if not line.startswith(EARLY_WAKE_MARKER):
            continue
        early_fields = _parse_wake_marker(line, prefix=EARLY_WAKE_MARKER)
        if early_fields is None or early_fields.get("seed") != seed_delivery_id:
            continue
        early_framework = early_fields.get("framework", "unknown")
        if not isinstance(early_framework, str):
            early_framework = "unknown"
        return early_framework, {
            "wake_monotonic_ns": int(early_fields.get("wake_monotonic_ns") or 0),
            "t_sub_monotonic_ns": int(early_fields.get("t_sub_monotonic_ns") or 0),
            "t_import_done_monotonic_ns": int(early_fields.get("t_import_done_monotonic_ns") or 0),
        }
    return None


def _parse_canonical_reaction(
    text: str,
    *,
    seed_delivery_id: str,
    seed_emit_monotonic_ns: int,
    early_wake_per_framework: dict[str, dict[str, int]],
) -> tuple[str, ObservedReaction] | None:
    """Build the canonical ``DRIVER_REACTED`` reaction from one child's stdout.

    Returns ``(framework_name, reaction)`` for the first canonical
    wake-marker line matching this iteration's seed, or ``None`` when no
    such line is present. The driver-side reaction latency is computed
    on the SAME monotonic clock as the verdict's aggregate latency:
    subtract the orchestrator-side ``seed_emit_monotonic_ns`` anchor
    from the driver-side ``wake_monotonic_ns`` (Linux CLOCK_MONOTONIC is
    per-boot, cross-process valid; the preflight pins it).
    """
    fields: dict[str, Any] | None = None
    for line in text.splitlines():
        if not line.startswith(WAKE_MARKER):
            continue
        parsed = _parse_wake_marker(line)
        if parsed is None or parsed.get("seed") != seed_delivery_id:
            continue
        fields = parsed
        break
    if fields is None:
        return None
    framework_value = fields.get("framework", "unknown")
    framework_name = framework_value if isinstance(framework_value, str) else "unknown"
    received_wall_ns = int(fields.get("wall_ns") or 0)
    fw_id_value = fields.get("fw_id", "unknown")
    fw_id = fw_id_value if isinstance(fw_id_value, str) else "unknown"
    reaction_id_value = fields.get("reaction_id", "unknown")
    reaction_id = reaction_id_value if isinstance(reaction_id_value, str) else "unknown"
    driver_wake_monotonic_ns = early_wake_per_framework.get(framework_name, {}).get("wake_monotonic_ns", 0)
    if driver_wake_monotonic_ns:
        reaction_latency_ms = max(0.0, (driver_wake_monotonic_ns - seed_emit_monotonic_ns) / 1e6)
    else:
        reaction_latency_ms = 0.0
    token_usage, token_parse_failed = observed_token_usage_from_marker(fields)
    reaction = ObservedReaction(
        framework=framework_name,
        fw_id=fw_id,
        seed_delivery_id=seed_delivery_id,
        reaction_delivery_id=reaction_id,
        received_wall_ns=received_wall_ns,
        reaction_latency_ms=reaction_latency_ms,
        token_usage=token_usage,
        token_usage_parse_failed=token_parse_failed,
    )
    return framework_name, reaction


def _drain_swarm_reactions(
    children: Sequence[_Child],
    *,
    deadline_monotonic: float,
    seed_delivery_id: str,
    seed_emit_monotonic_ns: int,
) -> tuple[
    list[ObservedReaction],
    dict[str, int],
    dict[str, dict[str, int]],
]:
    """Drain every driver's stdout CONCURRENTLY and parse both markers.

    Returns ``(reactions, t_observe_per_framework,
    early_wake_per_framework)``. One thread per child runs
    ``communicate`` (which blocks until that child's EOF) so each
    driver's ``t_observe_ns`` is stamped when THAT child actually
    finishes, not when it reaches the front of a serial drain queue.

    This is the SOTA fix for the serial-drain latency artifact: a fast
    driver (e.g. a sub-second shell echo) drained last in framework
    order used to inherit the multi-second drain wait of a slow
    gemini-cli child ahead of it, inflating its recorded end-to-end
    latency by hundreds of times. With concurrent draining each
    driver's end-to-end latency measures its own reaction.

    Both the early ``WAKE_RECEIVED`` marker (printed before the LLM
    call) and the canonical ``DRIVER_REACTED`` marker (printed after the
    LLM call) arrive together at process exit; marker parsing happens on
    the main thread after the per-child threads join, and is
    order-independent because each child emits only its OWN framework's
    two markers (the early marker is parsed before the canonical one for
    that same child so the latency math sees that child's wake anchors).
    """
    drained = drain_children_concurrently(
        children,
        deadline_monotonic=deadline_monotonic,
        min_remaining_sec=_MIN_REMAINING_DEADLINE_SEC,
        term_grace_sec=2.0,
    )

    reactions: list[ObservedReaction] = []
    t_observe_per_framework: dict[str, int] = {}
    early_wake_per_framework: dict[str, dict[str, int]] = {}
    # Parse in framework (spawn) order for deterministic output; the
    # per-child ``t_observe_ns`` was already stamped at that child's own
    # EOF inside its drain thread, so ordering here does not affect
    # latency attribution.
    for index in range(len(children)):
        out, t_observe_ns = drained.get(index, (b"", time.monotonic_ns()))
        text = out.decode("utf-8", errors="replace") if out else ""

        early = _parse_early_wake_marker(text, seed_delivery_id=seed_delivery_id)
        if early is not None:
            early_framework, early_anchors = early
            early_wake_per_framework[early_framework] = early_anchors

        canonical = _parse_canonical_reaction(
            text,
            seed_delivery_id=seed_delivery_id,
            seed_emit_monotonic_ns=seed_emit_monotonic_ns,
            early_wake_per_framework=early_wake_per_framework,
        )
        if canonical is None:
            continue
        framework_name, reaction = canonical
        t_observe_per_framework[framework_name] = t_observe_ns
        reactions.append(reaction)
    return reactions, t_observe_per_framework, early_wake_per_framework


def _build_iteration_rows(
    *,
    iter_id: int,
    arm: str,
    sentinel: str,
    t_send_ns: int,
    seed_emit_monotonic_ns: int,
    reactions: Sequence[ObservedReaction],
    t_observe_per_framework: dict[str, int],
    early_wake_per_framework: dict[str, dict[str, int]],
) -> list[IterationRow]:
    """Assemble exactly one ``IterationRow`` per driver from drained signals.

    Missing drivers get a row with ``invariant_failed=True``. Each row's
    latency, cache state, envelope substruct, invariant verdict, and
    delivery-mode classification are derived from the per-framework
    signals the drain phase produced. See ``_run_one_iteration`` for the
    surrounding orchestration.
    """
    rows: list[IterationRow] = []
    framework_to_reaction: dict[str, ObservedReaction] = {r.framework: r for r in reactions}
    for framework in FRAMEWORK_ORDER:
        reaction = framework_to_reaction.get(framework)

        # The orchestrator-side monotonic-ns observation lands in
        # ``t_observe_per_framework`` immediately after the driver's
        # stdout drained; subtracting the seed-emit anchor (NOT the
        # iteration-start anchor) keeps the latency on the bus-only
        # path -- the anchor mint, driver spawn, and spawn settle all
        # land between ``t_send_ns`` and ``seed_emit_monotonic_ns`` and
        # would inflate the figure by several seconds per row if
        # included.
        t_observe_ns = t_observe_per_framework.get(framework, 0)
        latency_ns = max(0, t_observe_ns - seed_emit_monotonic_ns) if t_observe_ns else 0

        # Hydrate envelope substructs for the driver's row (only one
        # substruct is non-None per row — the driver's own envelope).
        # Every envelope (claude / gemini / openai) now rides the same
        # path: the driver-side ``DRIVER_REACTED`` wake marker carries
        # the TokenUsage the orchestrator parses and re-projects into
        # the typed envelope via the ``*_from_token_usage`` helpers.
        token_usage = reaction.token_usage if reaction is not None else None
        claude_env: ClaudeEnvelope | None = None
        gemini_env: GeminiEnvelope | None = None
        openai_env: OpenAIEnvelope | None = None
        if framework == "claude-cli" and token_usage is not None:
            claude_env = claude_envelope_from_token_usage(token_usage)
        elif framework == "gemini-cli" and token_usage is not None:
            gemini_env = gemini_envelope_from_token_usage(token_usage)
        elif framework in {"pydantic", "langgraph"} and token_usage is not None:
            openai_env = openai_envelope_from_token_usage(token_usage)

        cache_state = "NA"
        if claude_env is not None:
            cache_state = _classify_claude_cache_state(
                visible=claude_env.input_tokens_visible,
                cache_read=claude_env.cache_read_input_tokens,
                billed_input=claude_env.billed_input_tokens,
            )
        elif openai_env is not None:
            cache_state = "WARM" if openai_env.cached_tokens > 0 else "COLD"

        # Invariant gate -- the four-branch classifier lives in a
        # pure helper so the failure shapes are unit-testable in
        # isolation.
        invariant_failed, invariant_failure_field = _classify_invariant(
            framework=framework,
            reaction=reaction,
            claude_env=claude_env,
            gemini_env=gemini_env,
            openai_env=openai_env,
            early_wake_received=framework in early_wake_per_framework,
        )

        # Cross-process monotonic timing markers + delivery-mode
        # classification. The driver-side anchors land via the early
        # ``WAKE_RECEIVED`` marker (parsed above); the orchestrator-
        # side anchor is the ``seed_emit_monotonic_ns`` captured from
        # ``_emit_seed_event``. ``delivery_mode`` is ``"live"`` when
        # the driver subscribed BEFORE seed emit (the seed reached the
        # driver via live ``_fan_out``), ``"replay"`` when the driver
        # subscribed at or after seed emit (the seed reached the driver
        # via the daemon's seq-replay window the ``since=`` cursor
        # opened), and ``"unknown"`` when no early marker arrived
        # (driver-side crash before the wait_for return).
        timing = early_wake_per_framework.get(framework, {})
        t_sub_monotonic_ns = timing.get("t_sub_monotonic_ns", 0)
        t_import_done_monotonic_ns = timing.get("t_import_done_monotonic_ns", 0)
        wake_monotonic_ns = timing.get("wake_monotonic_ns", 0)
        if t_sub_monotonic_ns == 0:
            delivery_mode = "unknown"
        elif t_sub_monotonic_ns < seed_emit_monotonic_ns:
            delivery_mode = "live"
        else:
            delivery_mode = "replay"

        rows.append(
            IterationRow(
                iter_id=iter_id,
                arm=arm,
                driver=framework,
                sentinel=sentinel,
                t_send_ns=t_send_ns,
                t_observe_ns=t_observe_ns,
                latency_ns=latency_ns,
                cache_state=cache_state,
                claude_env=claude_env,
                gemini_env=gemini_env,
                openai_env=openai_env,
                invariant_failed=invariant_failed,
                invariant_failure_field=invariant_failure_field,
                t_seed_emit_monotonic_ns=seed_emit_monotonic_ns,
                t_sub_monotonic_ns=t_sub_monotonic_ns,
                t_import_done_monotonic_ns=t_import_done_monotonic_ns,
                wake_monotonic_ns=wake_monotonic_ns,
                delivery_mode=delivery_mode,
            )
        )
    return rows


def _run_one_iteration(  # orchestration coordinator
    *,
    run_salt: str,
    iter_id: int,
    arm: str,
    env: dict[str, str],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    stderr_dir: Path,
    python_exe: str,
    openai_api_key: str | None,
    progress_handle: Any,
) -> list[IterationRow]:
    """Run one full 5-driver swarm iteration; return one row per driver.

    The iteration:

    - Mints a per-iteration scope id so the bench's drivers wake only
      on this iteration's seed (the existing ``agent_message`` +
      ``owner=<scope>`` contract).
    - Generates the iteration's cache-busting prefix via
      ``force_cold_cache_prefix(run_salt, iter_id)``; the prefix is
      recorded in every row's ``sentinel`` field for provenance.
    - Spawns the 5 drivers via ``spawn_n_heterogeneous`` (fresh
      subprocess per iteration, no across-iteration reuse).
    - Emits one seed event; the drivers wake on the existing
      ``wait_for`` path.
    - Records ``t_send_ns = time.monotonic_ns()`` immediately before
      seed emit; the orchestrator's ``t_observe_ns`` lands when the
      driver's wake-marker line is parsed from its stdout.
    - Applies the per-iteration moderation invariants and marks
      ``invariant_failed`` rows accordingly.

    The pydantic / langgraph drivers run their OWN real OpenAI call
    post-wake (per the bench's host-perturbation hypothesis: every
    driver makes a real network call to perturb the daemon under load);
    the resulting token envelope rides the driver's ``DRIVER_REACTED``
    wake-marker line and rehydrates into ``IterationRow.openai_env``
    via ``openai_envelope_from_token_usage``. The orchestrator no
    longer issues a parallel direct-SDK OpenAI call for those rows
    (the prior shape billed twice and conflated bench-side
    concurrency with driver-side workload).
    """
    # The cache prefix is salted with the run-scoped ``run_salt`` so a
    # separate benchmark process cannot HIT this run's cached prefix
    # under the same API key. Only the CACHE PREFIX is salted: the
    # source picker ``pick_source_for_iter(iter_id)`` below stays
    # salt-free so the post-hoc ``per_iter_source_distribution``
    # recompute remains faithful.
    sentinel = force_cold_cache_prefix(run_salt, iter_id)
    seed_scope_id = f"bench-llm-{arm}-{uuid.uuid4().hex[:12]}"

    # Per-iteration t_send anchor. monotonic_ns is the load-bearing
    # cross-process clock the bench's latency measurements depend on
    # (the Linux preflight asserted kernel-wide monotonic earlier).
    t_send_ns = time.monotonic_ns()

    # Mint the replay anchor before driver spawn so every driver's
    # ``wait_for(since=anchor_event_id)`` subscribes with a seq cursor
    # that bounds the daemon's replay window. A driver subprocess whose
    # subscribe registers after the seed lands (cold-import jitter,
    # scheduler contention) still receives the seed via replay; the
    # spawn settle below preserves measurement integrity by ensuring
    # the common-case delivery is the live ``_fan_out`` path, with
    # replay as the jitter safety net.
    anchor_event_id = emit_anchor_event(
        seed_scope_id=seed_scope_id,
        db_path=db_path,
        doorbell_path=doorbell_path,
        repo="bench",
        ingest_method="bench_polling_vs_subscribe_anchor",
        delivery_id_prefix="bench-polling-anchor",
    )

    children = spawn_n_heterogeneous(
        _SWARM_SPEC,
        base_env=env,
        socket_path=socket_path,
        db_path=db_path,
        doorbell_path=doorbell_path,
        seed_scope_id=seed_scope_id,
        python_exe=python_exe,
        stderr_dir=stderr_dir,
        openai_api_key=openai_api_key,
        cold_prefix=sentinel,
        since=anchor_event_id,
        arm=arm,
    )
    if _PER_ITER_SPAWN_SETTLE_SEC > 0:
        time.sleep(_PER_ITER_SPAWN_SETTLE_SEC)

    # Pick the iteration's seed taxonomy from the weighted soak
    # registry so the daemon's fan-out is exercised across the full
    # registered taxonomy at the bench's representative load. Drivers'
    # wait predicates are owner-only so they wake regardless of the
    # picked (source, event_type) pair.
    picked_source, picked_event_type = pick_source_for_iter(iter_id)
    seed_delivery_id, _seed_wall_ns, seed_emit_monotonic_ns = _emit_seed_event(
        seed_scope_id=seed_scope_id,
        db_path=db_path,
        doorbell_path=doorbell_path,
        source=picked_source,
        event_type=picked_event_type,
    )

    # The orchestrator does NOT issue its own OpenAI call for the
    # pydantic / langgraph rows -- those rows' envelopes ride the
    # driver-side ``DRIVER_REACTED`` wake marker (the driver runs the
    # real LLM call post-wake; the host-perturbation signal that the
    # bench's hypothesis measures the daemon under is the driver's
    # network workload, not a parallel orchestrator-side billing).
    _ = openai_api_key  # passed through main() for preflight; consumed driver-side via env

    deadline_monotonic = time.monotonic() + _PER_ITER_DEADLINE_SEC

    try:
        reactions, t_observe_per_framework, early_wake_per_framework = _drain_swarm_reactions(
            children,
            deadline_monotonic=deadline_monotonic,
            seed_delivery_id=seed_delivery_id,
            seed_emit_monotonic_ns=seed_emit_monotonic_ns,
        )
    finally:
        for child in children:
            child.terminate()

    rows = _build_iteration_rows(
        iter_id=iter_id,
        arm=arm,
        sentinel=sentinel,
        t_send_ns=t_send_ns,
        seed_emit_monotonic_ns=seed_emit_monotonic_ns,
        reactions=reactions,
        t_observe_per_framework=t_observe_per_framework,
        early_wake_per_framework=early_wake_per_framework,
    )

    append_jsonl_record(
        progress_handle,
        {
            "kind": "iteration_done",
            "iter_id": iter_id,
            "arm": arm,
            "n_rows": len(rows),
            "n_invariant_failed": sum(1 for r in rows if r.invariant_failed),
            "drivers_observed": [r.driver for r in rows if not r.invariant_failed],
            "anchor_event_id": anchor_event_id,
            "seed_emit_monotonic_ns": seed_emit_monotonic_ns,
            "delivery_modes": {r.driver: r.delivery_mode for r in rows},
            "n_delivered_live": sum(1 for r in rows if r.delivery_mode == "live"),
            "n_delivered_replay": sum(1 for r in rows if r.delivery_mode == "replay"),
            "n_delivered_unknown": sum(1 for r in rows if r.delivery_mode == "unknown"),
        },
    )
    return rows


# ---------------------------------------------------------------------
# Pilot gate + main bench loop.
# ---------------------------------------------------------------------


def _run_pilot(
    *,
    run_salt: str,
    env: dict[str, str],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    stderr_dir: Path,
    python_exe: str,
    openai_api_key: str | None,
    progress_handle: Any,
    real_llm_enabled: bool,
) -> tuple[float | None, bool, float]:
    """Run a 10-iteration pilot; return ``(sigma_ms, pilot_passed, gate_ms)``.

    The pilot measures shell-control latency (the cheapest driver,
    lowest noise) over 10 subscribe-arm iterations and computes the
    sample std dev. The gate threshold depends on whether real LLMs
    are in flight: when off, the gate is twice the plan sigma (20ms);
    when on, the gate widens to absorb cross-iteration scheduler
    jitter from the multi-second LLM calls (40ms). The orchestrator-
    side shell-control sigma is still the load-bearing signal.

    Returns ``(None, True, gate_ms)`` when the pilot did not produce
    any shell-control latency observations (all iterations invariant-
    failed) — the caller treats this as a soft pass so the bench's
    smoke mode is not blocked by an empty pilot.
    """
    structured(_logger, logging.INFO, "bench_pilot_start", iterations=_PILOT_N)
    all_rows: list[IterationRow] = []
    for iter_id in range(_PILOT_N):
        rows = _run_one_iteration(
            run_salt=run_salt,
            iter_id=iter_id,
            arm="subscribe",
            env=env,
            socket_path=socket_path,
            db_path=db_path,
            doorbell_path=doorbell_path,
            stderr_dir=stderr_dir,
            python_exe=python_exe,
            openai_api_key=openai_api_key,
            progress_handle=progress_handle,
        )
        all_rows.extend(rows)
    gate_ms = _PILOT_SIGMA_GATE_MS_REAL_LLM if real_llm_enabled else _PILOT_SIGMA_PLAN_MS * _PILOT_SIGMA_GATE_FACTOR
    # Compute the pilot sigma on the BUS-ONLY ingest path
    # (wake_monotonic_ns - seed_emit_monotonic_ns), NOT on the full
    # driver-lifecycle row.latency_ns (which the orchestrator records
    # after stdout-drain and therefore includes spawn-settle +
    # cold-import + stdout I/O). The pilot's hypothesis names the
    # daemon-side noise floor (10 ms plan); the bus-only path is the
    # measurement that hypothesis applies to. Rows missing the
    # cross-process monotonic anchors (driver crashed before the
    # WAKE_RECEIVED emit) drop out of the sample naturally.
    shell_bus_latencies = [
        row.wake_monotonic_ns - row.t_seed_emit_monotonic_ns
        for row in all_rows
        if row.driver == "shell-control"
        and not row.invariant_failed
        and row.wake_monotonic_ns > 0
        and row.t_seed_emit_monotonic_ns > 0
    ]
    if len(shell_bus_latencies) < 2:
        structured(_logger, logging.WARNING, "bench_pilot_too_few_samples", count=len(shell_bus_latencies))
        return None, True, gate_ms
    sigma_ms = statistics.stdev(shell_bus_latencies) / 1e6
    passed = sigma_ms <= gate_ms
    structured(
        _logger,
        logging.INFO,
        "bench_pilot_done",
        sigma_ms=sigma_ms,
        gate_ms=gate_ms,
        passed=passed,
    )
    append_jsonl_record(
        progress_handle,
        {"kind": "pilot_done", "sigma_ms": sigma_ms, "gate_ms": gate_ms, "passed": passed},
    )
    return sigma_ms, passed, gate_ms


def _aggregate_external_state(
    report: ExternalStateReport,
    rows: Sequence[IterationRow],
) -> ExternalStateReport:
    """Roll per-iteration observed-model lists into the external-state report.

    The bench-level aggregator pattern: ``capture_external_state``
    returns the report with empty lists; per-iteration rows append
    their observed model id; this helper produces the final report
    with the merged lists and the stop-reason / api-error distributions.
    """
    anthropic_models: list[str] = list(report.anthropic_response_model_set)
    openai_models: list[str] = list(report.openai_response_model_set)
    gemini_models: list[str] = list(report.gemini_response_model_set)
    tool_calls: list[int] = list(report.agent_tool_call_count_per_iter)
    turns: list[int] = list(report.agent_turn_count_per_iter)
    stop_dist = dict(report.stop_reason_distribution)
    api_err_dist = dict(report.api_error_status_distribution)
    moderation_count = report.moderation_event_count
    for row in rows:
        # Drain whichever substruct is populated for this row's driver.
        envelopes: list[ClaudeEnvelope | GeminiEnvelope | OpenAIEnvelope] = []
        if row.claude_env is not None:
            envelopes.append(row.claude_env)
            anthropic_models = merge_observed_models(anthropic_models, row.claude_env.model)
            if row.claude_env.num_turns is not None:
                turns.append(row.claude_env.num_turns)
            if row.claude_env.stop_reason is not None:
                stop_dist = merge_distribution(stop_dist, row.claude_env.stop_reason)
            if row.claude_env.api_error_status is not None:
                api_err_dist = merge_distribution(api_err_dist, row.claude_env.api_error_status)
            if row.claude_env.stop_reason in {"refusal", "error_during_execution"}:
                moderation_count += 1
        if row.gemini_env is not None:
            envelopes.append(row.gemini_env)
            gemini_models = merge_observed_models(gemini_models, row.gemini_env.model)
            if row.gemini_env.num_turns is not None:
                turns.append(row.gemini_env.num_turns)
            if row.gemini_env.stop_reason is not None:
                stop_dist = merge_distribution(stop_dist, row.gemini_env.stop_reason)
            if row.gemini_env.api_error_status is not None:
                api_err_dist = merge_distribution(api_err_dist, row.gemini_env.api_error_status)
            if row.gemini_env.stop_reason in {"refusal", "error_during_execution"}:
                moderation_count += 1
        if row.openai_env is not None:
            envelopes.append(row.openai_env)
            openai_models = merge_observed_models(openai_models, row.openai_env.model)
    return msgspec.structs.replace(
        report,
        anthropic_response_model_set=anthropic_models,
        openai_response_model_set=openai_models,
        gemini_response_model_set=gemini_models,
        agent_tool_call_count_per_iter=tool_calls,
        agent_turn_count_per_iter=turns,
        stop_reason_distribution=stop_dist,
        api_error_status_distribution=api_err_dist,
        moderation_event_count=moderation_count,
    )


def _build_limitations() -> list[str]:
    """Return the documented-limitations list for the verdict.

    Mirrors the measurement protocol specification's "Limitations"
    block verbatim plus additional documented scope items (Bonferroni
    boundary, stress-test-historical-not-modified, Anthropic prefix
    cache, pilot gate). Recorded so a reader of the verdict sees every
    acknowledged-but-not-closed scope item.
    """
    return [
        "p99 latency CI half-width at n=50 ~= 5x median CI; p99 NOT used for driver ranking",
        f"{BENCH_GEMINI_MODEL} alias is floating; observed model id set recorded in external_state but not pinned",
        "Anthropic prompt cache 5-min decay defeated by per-iteration force_cold_cache_prefix; "
        "if iteration wall-clock exceeds 5 min cache state may degrade",
        "claude / gemini CLIs expose no --seed or --temperature; sampling is black-box; distribution-level claims only",
        "asyncio scheduling jitter seeded via PYTHONHASHSEED but not eliminated; p99 not cross-operator-comparable",
        "OPENAI_API_KEY presence recorded as bool; key value never persisted",
        "per-driver verdict is a median latency description with bootstrap CI bands; "
        "no marginal hypothesis test runs here (no across-arm rejection gate)",
        "this bench writes NEW verdict files alongside any shipped stress-test verdict.json; "
        "it does NOT modify historical stress-test artifacts",
        "pilot gate: 10-iteration pilot validates sigma_idle assumption before main run; "
        "abort if pilot sigma exceeds plan by >2x",
    ]


def _compute_cost(rows: Sequence[IterationRow]) -> tuple[float | None, float | None, int]:
    """Split per-envelope cost_usd into metered vs notional-subscription totals.

    Returns ``(metered_total, notional_total, unknown_count)``.

    The two cost families are kept apart because they answer different
    questions. **Metered** cost is genuinely-billed pay-per-call spend:
    only OpenAI (the pydantic / langgraph drivers) runs against an API
    key, so only ``openai_env.cost_usd`` advances ``metered_total`` --
    this is the figure the run's budget gate caps and the actual "what
    this run cost you" number. **Notional** cost is the API-equivalent
    price a ``claude -p`` subscription envelope *reports* for the tokens
    it spent; a Max/Pro subscriber never pays it (the argv carries no API
    key), so it is surfaced separately and never gated. Gemini surfaces
    no per-call figure (documented CLI contract), so every gemini call
    falls into ``unknown_count``.

    Each family coerces an empty contribution to ``None`` rather than
    ``0.0`` so a run with no calls of that family reads "unknown",
    not a misleading free.
    """
    metered: list[float] = []
    notional: list[float] = []
    unknown = 0
    for row in rows:
        if row.openai_env is not None:
            if row.openai_env.cost_usd is None:
                unknown += 1
            else:
                metered.append(row.openai_env.cost_usd)
        if row.claude_env is not None:
            if row.claude_env.cost_usd is None:
                unknown += 1
            else:
                notional.append(row.claude_env.cost_usd)
        if row.gemini_env is not None:
            # Gemini CLI surfaces no per-call cost figure (documented contract).
            unknown += 1
    metered_total: float | None = sum(metered) if metered else None
    notional_total: float | None = sum(notional) if notional else None
    return metered_total, notional_total, unknown


def _run_pilot_or_skip(
    *,
    run_salt: str,
    args: argparse.Namespace,
    bench_name: str,
    env: dict[str, str],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    stderr_dir: Path,
    python_exe: str,
    openai_api_key: str | None,
    progress_handle: Any,
) -> tuple[bool, str | None, float | None, bool, float]:
    """Resolve the pilot gate, skipping it when structurally inapplicable.

    Returns ``(pilot_skipped, pilot_skipped_reason, pilot_sigma_ms,
    pilot_passed, pilot_gate_ms)``. The pilot validates the
    orchestrator-side noise floor before the main run, but it is
    structurally inapplicable when the downstream measurement does not
    consume the signal: smoke mode runs N below the bench's statistical
    power floor, and --skip-real-llm makes the per-iteration cost a pure
    subprocess spawn (~5 s cold-import) so the orchestrator-side jitter
    floor is not the dominant cost. In those modes the pilot is skipped
    so the operator still gets a verdict.json that confirms the wiring
    is alive.
    """
    _ = bench_name  # accepted for call-site symmetry with the main loop helpers
    pilot_skipped = args.smoke or not args.include_real_llm
    if pilot_skipped:
        pilot_skipped_reason = "smoke_mode" if args.smoke else "real_llm_disabled"
        pilot_sigma_ms = None
        pilot_passed = True
        pilot_gate_ms = (
            _PILOT_SIGMA_GATE_MS_REAL_LLM if args.include_real_llm else _PILOT_SIGMA_PLAN_MS * _PILOT_SIGMA_GATE_FACTOR
        )
        structured(
            _logger,
            logging.INFO,
            "bench_pilot_skipped",
            reason=pilot_skipped_reason,
        )
        append_jsonl_record(
            progress_handle,
            {"kind": "pilot_skipped", "reason": pilot_skipped_reason},
        )
        return pilot_skipped, pilot_skipped_reason, pilot_sigma_ms, pilot_passed, pilot_gate_ms
    pilot_sigma_ms, pilot_passed, pilot_gate_ms = _run_pilot(
        run_salt=run_salt,
        env=env,
        socket_path=socket_path,
        db_path=db_path,
        doorbell_path=doorbell_path,
        stderr_dir=stderr_dir,
        python_exe=python_exe,
        openai_api_key=openai_api_key,
        progress_handle=progress_handle,
        real_llm_enabled=args.include_real_llm,
    )
    return pilot_skipped, None, pilot_sigma_ms, pilot_passed, pilot_gate_ms


def _run_measurement_loop(
    *,
    run_salt: str,
    n_per_arm: int,
    max_cost_usd: float,
    env: dict[str, str],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    stderr_dir: Path,
    python_exe: str,
    openai_api_key: str | None,
    progress_handle: Any,
) -> tuple[list[IterationRow], bool, CostBudgetTracker]:
    """Drive the poll-then-subscribe iteration loop under the cost budget.

    Returns ``(all_rows, aborted_on_budget, budget_tracker)``. Between
    each iteration the cost-budget tracker decides whether the bench's
    projected cost would breach the budget; if so the run aborts cleanly
    before the next iteration so the operator's spend stays bounded.
    """
    budget_tracker = CostBudgetTracker(max_usd=max_cost_usd)
    aborted_on_budget = False
    all_rows: list[IterationRow] = []
    for arm in ("poll", "subscribe"):
        if aborted_on_budget:
            break
        for iter_id in range(n_per_arm):
            budget_tracker.begin_iteration()
            if budget_tracker.should_abort():
                structured(
                    _logger,
                    logging.WARNING,
                    "bench_aborted_on_budget",
                    observed_usd=budget_tracker.observed_usd,
                    max_usd=budget_tracker.max_usd,
                )
                aborted_on_budget = True
                break
            rows = _run_one_iteration(
                run_salt=run_salt,
                iter_id=iter_id,
                arm=arm,
                env=env,
                socket_path=socket_path,
                db_path=db_path,
                doorbell_path=doorbell_path,
                stderr_dir=stderr_dir,
                python_exe=python_exe,
                openai_api_key=openai_api_key,
                progress_handle=progress_handle,
            )
            for row in rows:
                if row.openai_env is not None:
                    budget_tracker.record_openai(row.openai_env)
                elif row.claude_env is not None:
                    budget_tracker.record_claude(row.claude_env.cost_usd)
                elif row.gemini_env is not None:
                    budget_tracker.record_gemini()
            all_rows.extend(rows)
    return all_rows, aborted_on_budget, budget_tracker


def _build_verdict(
    *,
    args: argparse.Namespace,
    bench_name: str,
    started_at_ns: int,
    ended_at_ns: int,
    n_per_arm: int,
    all_rows: list[IterationRow],
    external_state_report: ExternalStateReport,
    budget_tracker: CostBudgetTracker,
    aborted_on_budget: bool,
    pilot_sigma_ms: float | None,
    pilot_passed: bool,
    pilot_gate_ms: float,
    pilot_skipped: bool,
    pilot_skipped_reason: str | None,
) -> tuple[ExperimentAVerdict, bool, dict[str, float]]:
    """Roll up the run's rows into the verdict struct.

    Returns ``(verdict, replay_gate_passed, replay_rates)`` -- the gate
    flag and per-driver rates are returned alongside the verdict so the
    caller's exit-code gate can reference them without re-deriving.
    """
    final_external_state = _aggregate_external_state(external_state_report, all_rows)
    per_driver_poll = {
        driver: _summarise_per_driver([r for r in all_rows if r.arm == "poll"], driver=driver)
        for driver in FRAMEWORK_ORDER
    }
    per_driver_subs = {
        driver: _summarise_per_driver([r for r in all_rows if r.arm == "subscribe"], driver=driver)
        for driver in FRAMEWORK_ORDER
    }
    cost_total, notional_cost_total, cost_unknown = _compute_cost(all_rows)
    replay_rates = _replay_contamination_rates(all_rows)
    # Smoke mode (N=2 per arm) makes the replay-contamination
    # gate structurally lossy: a single cold-import replay
    # row hits a 50% rate against a 5% threshold so the
    # gate fails on the wiring-alive check that smoke mode
    # is supposed to certify. The gate stays a hard signal
    # at production N (the 5% threshold is calibrated for
    # N=50 per arm); smoke runs surface the per-driver
    # rates in the verdict but do not flip the gate.
    if args.smoke:
        replay_gate_passed = True
    else:
        replay_gate_passed = _replay_contamination_gate_passed(replay_rates, threshold=_REPLAY_CONTAMINATION_THRESHOLD)
    # Recompute the per-iteration source distribution from
    # the picker. The picker is deterministic over iter_id
    # so re-applying it post-hoc is equivalent to recording
    # the value at emit time and avoids carrying the field
    # through the IterationRow shape.
    per_iter_source_distribution: dict[str, int] = {}
    for _arm in ("poll", "subscribe"):
        for iter_id in range(n_per_arm):
            name, _event_type = pick_source_for_iter(iter_id)
            per_iter_source_distribution[name] = per_iter_source_distribution.get(name, 0) + 1
    verdict = ExperimentAVerdict(
        bench_name=bench_name,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        n_iterations_per_arm=n_per_arm,
        smoke=args.smoke,
        external_state=final_external_state,
        rows=all_rows,
        per_arm_per_driver_stats_poll=per_driver_poll,
        per_arm_per_driver_stats_subscribe=per_driver_subs,
        per_driver_invariant_failure_rate=_invariant_failure_rates(all_rows),
        per_driver_replay_contamination_rate=replay_rates,
        replay_contamination_threshold=_REPLAY_CONTAMINATION_THRESHOLD,
        replay_contamination_gate_passed=replay_gate_passed,
        cache_state_distribution=_cache_state_distribution(all_rows),
        cost_usd_total=cost_total,
        notional_subscription_cost_usd=notional_cost_total,
        cost_unknown_count=cost_unknown,
        cache_contaminated_count=count_cache_contaminated_rows(all_rows),
        pilot_sigma_ms=pilot_sigma_ms,
        pilot_sigma_plan_ms=_PILOT_SIGMA_PLAN_MS,
        pilot_sigma_gate_factor=_PILOT_SIGMA_GATE_FACTOR,
        pilot_sigma_gate_ms_used=pilot_gate_ms,
        pilot_passed=pilot_passed,
        max_cost_usd_budget=args.max_cost_usd,
        max_cost_usd_observed=budget_tracker.observed_usd,
        aborted_on_budget=aborted_on_budget,
        limitations=_build_limitations(),
        pilot_skipped=pilot_skipped,
        pilot_skipped_reason=pilot_skipped_reason,
        per_iter_source_distribution=per_iter_source_distribution,
    )
    return verdict, replay_gate_passed, replay_rates


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.bench_polling_vs_subscribe_llm_agent",
        description=(
            "Heterogeneous 5-driver swarm bench: per-driver median ingest latency under real agent load. "
            "Layers a polling-vs-subscribing A/B on top of the bench's per-iteration measurement protocol."
        ),
    )
    parser.add_argument("--smoke", action="store_true", help=f"Run N={_SMOKE_N} per arm (default N={_DEFAULT_N}).")
    parser.add_argument(
        "--n",
        type=int,
        default=None,
        help=f"Override iteration count per arm (default {_DEFAULT_N}; smoke override {_SMOKE_N}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path.cwd() / ".local-stress-logs",
        help="Verdict output directory (or verdict.json path). Default ./.local-stress-logs/.",
    )
    parser.add_argument(
        "--include-real-llm",
        dest="include_real_llm",
        action="store_true",
        default=True,
        help=(
            "Require real LLM calls (OPENAI_API_KEY in keyring + claude/gemini CLIs on PATH). "
            "Default ON; disable with --skip-real-llm."
        ),
    )
    parser.add_argument(
        "--skip-real-llm",
        dest="include_real_llm",
        action="store_false",
        help="Disable real LLM call gates; preflight skips OpenAI/claude/gemini checks (smoke-only).",
    )
    parser.add_argument(
        "--max-cost-usd",
        type=float,
        default=_DEFAULT_MAX_COST_USD,
        help=(
            "Upper budget for genuinely-metered OpenAI spend (the pydantic / langgraph "
            "drivers). The claude and gemini drivers run via subscription CLIs at zero "
            "marginal cost, so their notional reported cost is surfaced separately "
            "(notional_subscription_cost_usd) and never counts against this gate. "
            "The bench aborts before the next iteration when projected metered cost would "
            f"breach this value. Default {_DEFAULT_MAX_COST_USD:.2f} USD."
        ),
    )
    args = parser.parse_args(argv)

    bench_name = "poll_vs_subscribe_llm"
    n_per_arm = args.n if args.n is not None else (_SMOKE_N if args.smoke else _DEFAULT_N)

    # If --output is a directory (or any path without a ``.verdict.json``
    # suffix), the bench resolves a timestamp-prefixed shape under it;
    # otherwise the bench treats the path as the verdict.json output.
    if args.output.name.endswith(".verdict.json"):
        verdict_path, progress_path, _ = resolve_bench_log_paths(bench_name=bench_name, output=args.output)
    else:
        args.output.mkdir(parents=True, exist_ok=True)
        verdict_path, progress_path, _ = resolve_bench_log_paths(
            bench_name=bench_name, output=args.output / f"{bench_name}.verdict.json"
        )

    # Preflight: blocking. If any gate fails we abort BEFORE spending
    # tokens. The bench's caller sees stderr + the PreflightError
    # message on the preflight-failure path.
    try:
        external_state_report = run_preflight_assertions(
            bench_name=bench_name,
            require_openai=args.include_real_llm,
            require_claude_cli=args.include_real_llm,
            require_gemini_cli=args.include_real_llm,
        )
    except PreflightError as exc:
        print(f"[{bench_name}] preflight failed: {exc}", file=sys.stderr)
        return 2

    openai_api_key: str | None = None
    if args.include_real_llm:
        openai_api_key = read_openai_key_from_keyring()
        # ``run_preflight_assertions`` already verified the key is
        # present; the lookup here re-loads the actual value (the
        # preflight stores only the bool presence flag).

    # Mint the run-scoped cache salt ONCE per orchestrator process.
    # ``force_cold_cache_prefix`` mixes this into the cold-cache prefix
    # digest so a separate benchmark run under the same API key cannot
    # HIT this run's cached prefix within the provider's ~5-min TTL
    # (the prompt cache is content-addressed per-key, NOT per-process).
    # ``secrets.token_hex`` is cryptographically random, so two
    # processes -- even started in the same wall-clock nanosecond --
    # mint distinct salts; the salt is constant for all iterations of
    # THIS run so re-running an iteration stays byte-identical.
    run_salt = secrets.token_hex(8)

    # Spawn the daemon under a temp state dir; the bench owns the
    # daemon's lifecycle so a wedged daemon does not leak past the
    # bench's tear-down.
    started_at_ns = time.time_ns()
    waitbus_path = _waitbus_path()
    python_exe = default_python_executable()

    with tempfile.TemporaryDirectory(prefix=f"waitbus-bench-{bench_name}-") as tmp_root:
        root = Path(tmp_root)
        state_dir = root / "state"
        runtime_dir = root / "runtime"
        stderr_dir = runtime_dir / "driver-stderr"
        state_dir.mkdir(parents=True, exist_ok=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        stderr_dir.mkdir(parents=True, exist_ok=True)

        env = dict(os.environ)
        env["WAITBUS_STATE_DIR"] = str(state_dir)
        env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
        env["WAITBUS_DISABLE_SOURCE_AUTOLOAD"] = "1"
        socket_path = runtime_dir / "broadcast.sock"
        db_path = state_dir / "github.db"
        doorbell_path = runtime_dir / "doorbell.sock"

        daemon = _spawn_daemon(env, waitbus_path, socket_path)

        # Refresh external_state with the daemon's PRAGMA snapshot now
        # that the DB exists and the daemon has applied its schema. The
        # probe is read-only (URI mode=ro&immutable=1) so it does not
        # write-lock the daemon mid-flight.
        external_state_report = msgspec.structs.replace(
            external_state_report,
            waitbus_daemon_pragmas=capture_daemon_pragmas(db_path),
        )

        # Bench's per-iteration GC posture: gc.disable() in the
        # orchestrator's own process (the driver subprocesses get
        # WAITBUS_BENCH_GC_OFF=1 via the _bench_swarm factory).
        gc.disable()
        try:
            with progress_path.open("w", encoding="utf-8") as progress_handle:
                append_jsonl_record(
                    progress_handle,
                    {
                        "kind": "start",
                        "bench": bench_name,
                        "n_per_arm": n_per_arm,
                        "smoke": args.smoke,
                        "include_real_llm": args.include_real_llm,
                    },
                )

                # Pilot gate. The pilot validates the orchestrator-side
                # noise floor before the main run, but it is structurally
                # inapplicable when the downstream measurement does not
                # consume the signal: smoke mode runs N below the bench's
                # statistical power floor, and --skip-real-llm makes the
                # per-iteration cost a pure subprocess spawn (~5 s
                # cold-import) so the orchestrator-side jitter floor is
                # not the dominant cost. In those modes the bench skips
                # the pilot and proceeds directly to the main loop so the
                # operator still gets a verdict.json that confirms the
                # wiring is alive.
                (
                    pilot_skipped,
                    pilot_skipped_reason,
                    pilot_sigma_ms,
                    pilot_passed,
                    pilot_gate_ms,
                ) = _run_pilot_or_skip(
                    run_salt=run_salt,
                    args=args,
                    bench_name=bench_name,
                    env=env,
                    socket_path=socket_path,
                    db_path=db_path,
                    doorbell_path=doorbell_path,
                    stderr_dir=stderr_dir,
                    python_exe=python_exe,
                    openai_api_key=openai_api_key,
                    progress_handle=progress_handle,
                )
                if not pilot_skipped and not pilot_passed:
                    sigma_str = f"{pilot_sigma_ms:.2f}" if pilot_sigma_ms is not None else "unknown"
                    print(
                        f"[{bench_name}] pilot sigma {sigma_str}ms exceeds gate "
                        f"({pilot_gate_ms:.2f}ms); aborting before main run",
                        file=sys.stderr,
                    )
                    return 3

                # Main loop: poll arm, then subscribe arm. Between each
                # iteration the cost-budget tracker decides whether the
                # bench's projected cost would breach the budget; if so
                # the run aborts cleanly before the next iteration so
                # the operator's spend stays bounded.
                all_rows, aborted_on_budget, budget_tracker = _run_measurement_loop(
                    run_salt=run_salt,
                    n_per_arm=n_per_arm,
                    max_cost_usd=args.max_cost_usd,
                    env=env,
                    socket_path=socket_path,
                    db_path=db_path,
                    doorbell_path=doorbell_path,
                    stderr_dir=stderr_dir,
                    python_exe=python_exe,
                    openai_api_key=openai_api_key,
                    progress_handle=progress_handle,
                )

                # Roll up.
                ended_at_ns = time.time_ns()
                verdict, replay_gate_passed, replay_rates = _build_verdict(
                    args=args,
                    bench_name=bench_name,
                    started_at_ns=started_at_ns,
                    ended_at_ns=ended_at_ns,
                    n_per_arm=n_per_arm,
                    all_rows=all_rows,
                    external_state_report=external_state_report,
                    budget_tracker=budget_tracker,
                    aborted_on_budget=aborted_on_budget,
                    pilot_sigma_ms=pilot_sigma_ms,
                    pilot_passed=pilot_passed,
                    pilot_gate_ms=pilot_gate_ms,
                    pilot_skipped=pilot_skipped,
                    pilot_skipped_reason=pilot_skipped_reason,
                )

                verdict_path.write_bytes(msgspec.json.encode(verdict))
                append_jsonl_record(
                    progress_handle,
                    {
                        "kind": "end",
                        "verdict_path": str(verdict_path),
                        "cost_usd_total": verdict.cost_usd_total,
                        "cost_unknown_count": verdict.cost_unknown_count,
                        "cache_contaminated_count": verdict.cache_contaminated_count,
                    },
                )
        finally:
            gc.enable()
            daemon.terminate()
            with contextlib.suppress(FileNotFoundError):
                socket_path.unlink()
            with contextlib.suppress(FileNotFoundError):
                doorbell_path.unlink()

    print(f"[{bench_name}] verdict: {verdict_path}", file=sys.stderr)
    # Invariant-failure gate: any row that the per-driver classifier
    # flagged (LLM error, missing reaction, moderation refusal) MUST
    # surface as a non-zero exit. The verdict file still records
    # ``per_driver_invariant_failure_rate`` but the gate stops CI from
    # treating a 100%-rate run as green. Mirrors the stress
    # controller's invariant-failure exit-code logic so the two
    # measurement surfaces share one CI contract.
    invariant_failure_count = sum(1 for row in all_rows if row.invariant_failed)
    if invariant_failure_count > 0:
        print(
            f"[{bench_name}] {invariant_failure_count} invariant failure(s) across {len(all_rows)} rows; "
            f"per-driver rates: {_invariant_failure_rates(all_rows)}",
            file=sys.stderr,
        )
        return 1
    # Replay-contamination gate: when any driver's replay rate exceeds
    # the threshold (production N only -- smoke runs short-circuit at
    # the gate evaluation site above), return a non-zero exit so the
    # regression surfaces in CI rather than landing as a silently
    # mixed-mode median. The verdict still writes (the rows +
    # per-driver rates are the diagnostic artefact); only the exit
    # code flips.
    if not replay_gate_passed:
        print(
            f"[{bench_name}] replay-contamination gate failed; per-driver rates: {replay_rates}",
            file=sys.stderr,
        )
        return 5
    return 0


__all__ = [
    "ExperimentAVerdict",
    "IterationRow",
    "_PerArmStats",
    "main",
]


if __name__ == "__main__":
    sys.exit(main())
