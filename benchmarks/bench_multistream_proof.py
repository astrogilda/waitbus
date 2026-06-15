"""Daemon CPU-side broadcast-cost measurement under the production direction.

Locked hypothesis (rate-form):

    The waitbus daemon serves M parked LLM-agent subscribers under N
    heterogeneous CI-event producer streams. The bench measures the
    daemon's per-wall-second CPU cost in the loaded arm versus an idle
    baseline and reports the difference at high resolution: a
    one-sided Mann-Whitney U diff test per substrate, per-arm medians
    with BCa bootstrap CIs, the Hodges-Lehmann diff-of-medians, and a
    documented 1.5-sigma minimum-detectable-effect floor. Any
    push-based broadcast bus has to do non-zero work per delivered
    event, so the bench characterises that measured per-delivery cost
    descriptively rather than declaring the daemon's load footprint
    equivalent to idle within a fixed budget.

    The metric of comparison is CPU nanoseconds per wall-clock
    second of observation -- the unit that survives variable-length
    loaded-window stretch.

Workload direction (canonical pub/sub shape):

    Producers (N synthetic CI-event emitter threads, owner-scoped to
    the bench's seed_scope_id) fire taxonomy-weighted events into the
    bus at a fixed aggregate rate (``--producer-event-rate-hz``,
    coordinated-omission-safe via OpenLoopScheduler). M LLM-agent
    subscriber subprocesses (one per framework in
    ``--agent-frameworks``) park in ``waitbus.wait_for`` and
    react with their framework's LLM call on each wake. The bench
    measures the daemon's per-wall-second CPU rate during this loaded
    period vs an idle baseline (no producer firing, no agents
    parked).

Dual-emission contract. The verdict ships BOTH a per-wall-second
normalized U-statistic family (the primary signal, matching the
rate-form hypothesis) AND a raw-per-window family (for forensic
comparison when a reader wants to see absolute per-window CPU
totals). The raw family is NOT a replacement for the rate family --
its p-values shift with the loaded-arm wall stretch and should be
read alongside the wall-time-per-window distribution. The pcount
substrate is integer dispatch count and is NOT normalized; it
operates on raw per-window deltas (wake frequency under load IS
the substrate-specific perturbation framing).

Per-event granularity (LOCKED, verified by claude-cli empirical probe).
The bench measures per-event arrival into the daemon (one
``agent_message`` event per workload iteration per producer), NOT
per-token. claude's ``--output-format=stream-json`` emits per-event
frames (one delta block per LLM event); the spec pins this granularity
across the bench so the read shape is uniform across drivers and
matches the consumer-facing contract.

Workload-as-thread (per measurement protocol). The workload runs as a thread
INSIDE the orchestrator process so cross-process clock drift cannot
contaminate per-window CPU measurements (the daemon's ``/proc/<pid>/stat``
read is independent of GIL state; the workload's ``time.monotonic_ns``
samples share a single monotonic source with the daemon-sampling
thread). All five drivers (pydantic-ai, langgraph, claude-cli,
gemini-cli, shell-control) run as subprocesses; the orchestrator
captures each driver's token usage from its wake marker.

GIL-pressure proxy via moderation envelope capture. Each window records
``thread_time_ns()`` deltas on the orchestrator's measurement thread
alongside the wall-clock window length; the verdict surfaces the
``wall_ns - thread_time_ns`` gap so a future reader can spot
GIL-contended windows that attenuate the swarm's measurable load.

Outlier filter (Mann-Whitney + MAD-based outlier rejection). Outlier rejection uses a
pre-registered OUT-OF-BAND baseline threshold (median + 5 * MAD of the
preflight pilot's idle window), NOT a per-iteration 5x-median rule that
would re-shape the loaded distribution post-hoc. Any window whose
samples exceed the pilot's baseline threshold is recorded but flagged
``rejected=True`` so the Mann-Whitney U operates on a clean set.

The pilot is SKIPPED when the bench's downstream Mann-Whitney U test
is structurally inapplicable: ``args.smoke or not args.include_real_llm``.
Smoke mode runs n=5/arm, below the documented n=50/arm power floor;
--skip-real-llm collapses the loaded arm to shell-control only so the
loaded-vs-idle comparison is identity-vs-identity. In either case the
outlier threshold has no consumer worth its 10-second cost. When
skipped the verdict carries pilot_skipped=True + pilot_skipped_reason
("smoke_mode" or "real_llm_disabled"; smoke takes precedence),
outlier_threshold_ns=0, and the main loop's outlier-rejection branch
in ``_build_verdict`` guards on ``if not pilot_skipped:`` so the zero
sentinel cannot reject every window. Contract pinned by
tests/test_bench_pilot_skip_contract.py.

Limitations the bench acknowledges but does NOT close (see
``LIMITATIONS`` in the verdict):

- p99 latency at n=50 has wide bootstrap CI; the bench does NOT rank
  drivers by p99.
- gemini-2.5-flash alias floats; observed model id recorded but not
  pinned.
- Anthropic prompt cache 5-minute decay defeated by per-iteration
  prefix; iteration wall-clock over 5 min may still degrade.
- claude/gemini CLIs expose no ``--seed`` / ``--temperature``; sampling
  is black-box; distribution-level claims only.
- asyncio scheduling jitter seeded via ``PYTHONHASHSEED`` but NOT
  eliminated; sub-1.5 sigma perturbations not detectable.
- Mann-Whitney detection threshold at n=50/side is 1.5 sigma on the
  pooled per-arm SD of the rate metric; smaller per-second-rate
  perturbations are not detectable here.

Invocation:

    # Smoke run (~2 min). Skips long-clock probes.
    uv run python -m benchmarks.bench_multistream_proof --smoke

    # Production run (~15 min, real LLM calls; requires OPENAI key in
    # keyring + claude/gemini on PATH).
    taskset -c 2,3 uv run python -m benchmarks.bench_multistream_proof \\
        --n 50 --include-real-llm
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import random
import secrets
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, NamedTuple

import msgspec

from benchmarks._bench_ci_producer_swarm import CiProducerSwarm
from benchmarks._bench_llm_agent_pool import LlmAgentPool
from benchmarks._bench_preflight import (
    PreflightError,
    assert_cpu_isolation_for_baselines,
    compute_orchestrator_and_daemon_cores,
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
    force_cold_cache_prefix,
    gemini_envelope_from_token_usage,
    merge_observed_models,
    openai_envelope_from_token_usage,
    read_daemon_cpu_ns,
    read_daemon_schedstat,
    read_daemon_vmrss_kb,
    resolve_bench_log_paths,
    schedstat_substrate_available,
)
from benchmarks._bench_source_mix import pick_source_for_iter
from benchmarks._harness import EnvironmentReport, environment_report
from scripts.stress._controller import _parse_wake_marker, observed_token_usage_from_marker
from scripts.stress._real_drivers import FRAMEWORK_ORDER
from waitbus._log import structured

_logger = logging.getLogger("waitbus.bench.multistream_proof")

_BENCH_NAME = "bench_multistream_proof"

# Default workload-direction knobs. The producer swarm defaults match
# the audit-cycle's pre-registered launch claim (N=50 producers at
# ~200 events/sec aggregate); ``--agent-frameworks`` defaults to the
# three frameworks that work without OAuth subscription auth on a
# fresh remote VM (Hetzner). Operators with locally-authenticated
# claude / gemini CLIs pass the full set explicitly.
_DEFAULT_PRODUCER_COUNT = 50
_DEFAULT_PRODUCER_EVENT_RATE_HZ = 200.0
_DEFAULT_AGENT_FRAMEWORKS = "pydantic,langgraph,shell-control"
# Settle window between agent spawn and producer fire (matches the
# stress harness's anchor-event settle period).
_AGENT_SETTLE_SEC = 5.0
# Bound on the post-fire agent drain wait. LLM tails can run long;
# this caps the per-window deadline so a stuck agent does not pin
# the bench.
_AGENT_COLLECT_TIMEOUT_SEC = 90.0

# Per-window length. The 1.5 sigma minimum-detectable-effect derivation
# in the spec assumes a 1-second observation window per arm. Shorter
# windows shrink the CPU-time signal below the /proc/<pid>/stat jiffie
# granularity (10ms on most Linux kernels at CLK_TCK=100); longer
# windows blur the per-iteration swarm signal across multiple iterations.
_WINDOW_DURATION_SEC = 1.0

# Daemon-side /proc/<pid>/stat sample cadence. 10ms matches the jiffie
# granularity exactly so we never lose a tick within a window.
_DAEMON_SAMPLE_INTERVAL_MS = 10

# Default sample sizes. The 50/arm production figure derives from the
# Mann-Whitney 1.5 sigma power calc in the spec; 5/arm smoke exists for
# CI gate use only (n too small for a meaningful U statistic).
_DEFAULT_N_PER_ARM = 50
_SMOKE_N_PER_ARM = 5

# Outlier-filter pilot: number of preflight idle windows whose
# (median + 5*MAD) defines the out-of-band rejection threshold for the
# main run. 10 windows is enough to estimate MAD robustly while keeping
# preflight cheap (10 * 1s = 10s).
_PILOT_WINDOW_COUNT = 10
_OUTLIER_MAD_MULTIPLIER = 8.0

# Floor for the pilot outlier threshold. On a Linux host where
# ``/proc/<pid>/stat`` utime accounting runs at jiffie granularity
# (CLK_TCK = 100 -> 10 ms per jiffie on the canonical kernel), an
# idle daemon produces a long run of zero-jiffie samples; median and
# MAD both collapse to zero and ``median + multiplier * mad`` rounds
# to zero. The resulting ``> threshold`` gate rejects EVERY loaded
# window (the empty-loaded-arm pathology that leaves all-None
# top-level fields). Flooring the threshold at one
# jiffie keeps the gate strictly above the kernel's measurement
# resolution while preserving the MAD-based shape when the idle
# pilot actually produces above-floor samples.
_PILOT_THRESHOLD_FLOOR_NS = int(1_000_000_000 // os.sysconf("SC_CLK_TCK"))

# Mann-Whitney U significance gate. One-sided ``alternative='less'``
# (idle stochastically less than loaded), matching the asymmetric
# rate-form hypothesis. Forensic ``two-sided`` calls go through the
# same wrapper with an explicit ``alternative=`` argument.
#
# Family-wise Bonferroni correction. ``_ALPHA_CORRECTED_DIFF`` applies
# to the 7-substrate diff-test family (utime, stime, schedstat each in
# {per_sec, raw} plus pcount). Marginals that route to the substrate-
# unavailable sentinel do NOT shrink k; the family size was pre-
# registered regardless of which substrates the kernel exposes at run
# time.
_ALPHA = 0.05
_BONFERRONI_K_DIFF = 7
_ALPHA_CORRECTED_DIFF = _ALPHA / _BONFERRONI_K_DIFF


# Minimum-detectable-effect sigma multiplier; documented in the bench's
# limitations text. 1.5 sigma is the conventional power floor for a
# Mann-Whitney U at n=50/side.
_MIN_DETECTABLE_EFFECT_SIGMA = 1.5

_DAEMON_READY_TIMEOUT_SEC = 10.0

# Gemini model pin re-exported from the canonical bench-suite home so
# the limitations text references the same constant the agent pool
# spawns under (``benchmarks._bench_shared``).
_GEMINI_MODEL = BENCH_GEMINI_MODEL


class WindowRow(msgspec.Struct, frozen=True, kw_only=True):
    """One CPU-side perturbation sample window.

    Records both the daemon-side CPU deltas (jiffies-derived ns from
    /proc/<pid>/stat) and the orchestrator's measurement-thread
    GIL-pressure proxy. The bench's Mann-Whitney U test operates on
    ``daemon_utime_delta_ns`` across arms; the other fields surface
    in the verdict for downstream forensic use.
    """

    window_id: int
    arm: str  # "idle" | "loaded"
    t_window_start_ns: int
    t_window_end_ns: int
    daemon_utime_delta_ns: int
    daemon_stime_delta_ns: int
    # Per-window deltas of the daemon's per-task scheduler statistics,
    # aggregated across every TID under ``/proc/<pid>/task/``. The
    # group-leader-only read of ``/proc/<pid>/schedstat`` misses the
    # daemon's doorbell thread -- the CPU path that handles every
    # event-emit notification under load. ``pcount`` is the dispatch
    # (wake-up) count and gets its own Mann-Whitney marginal because
    # wake frequency is independent of how long each wake took.
    # ``wait_time_ns`` is forensic-only: keeping it out of the test
    # family avoids inflating the Bonferroni denominator with a
    # substrate strongly correlated with ``run_time_ns``. Consecutive
    # zeros across every field signal substrate-unavailable (kernel
    # without ``CONFIG_SCHEDSTATS=y``); the verdict's
    # ``mann_whitney_inapplicable_reason_schedstat`` surfaces this so
    # an empty path is not misread as "no perturbation."
    daemon_schedstat_run_delta_ns: int
    daemon_schedstat_wait_delta_ns: int = 0
    daemon_schedstat_pcount_delta: int = 0
    # Drop between adjacent windows signals a partial per-TID read.
    daemon_schedstat_tid_count_end: int = 0
    daemon_voluntary_ctxt_delta: int
    daemon_nonvoluntary_ctxt_delta: int
    # Daemon resident-set size in kilobytes at window start / end. The
    # end snapshot is the primary cross-arm signal; the start snapshot
    # lets a forensic reader audit per-window deltas (transient
    # allocator spikes the end snapshot would smooth out). Zero on
    # read failure (process exit, torn-down ``/proc`` entry, ``VmRSS``
    # absent for the target).
    daemon_vmrss_start_kb: int = 0
    daemon_vmrss_end_kb: int = 0
    # Orchestrator measurement-thread CPU time. The gap between
    # window wall-time and this value is the GIL-block proxy --
    # a large gap implies the measurement thread was descheduled
    # waiting for the GIL while in-proc workload threads ran.
    measurement_thread_time_delta_ns: int
    wall_minus_thread_time_ns: int
    # Per-driver capture for ``loaded`` arms; empty for ``idle``. Each
    # loaded iteration appends one IterationRow per driver, mirroring
    # Bench A's row shape, so a forensic reader can answer "which
    # driver caused window N's perturbation?" directly from the verdict.
    swarm_rows: list[IterationRow]
    rejected: bool
    rejection_reason: str | None
    # The picked seed-source name for this window's iteration (one of
    # github / pytest / docker / fs / agent), or empty string for idle
    # arms / unused windows. Threaded from the workload's
    # ``_WorkloadResult.picked_source`` so the verdict-builder can
    # aggregate the per-source distribution without re-deriving it
    # from iter_id.
    picked_source: str = ""


class ExperimentBVerdict(msgspec.Struct, frozen=True, kw_only=True):
    """Top-level verdict written to verdict.json.

    Field order is fixed: msgspec serialises in declaration order, so the
    JSON column order is stable across runs for diff-friendly review.
    """

    bench_name: str
    started_ns: int
    finished_ns: int
    environment: EnvironmentReport
    external_state: ExternalStateReport
    windows: list[WindowRow]
    # Per-arm sample counts. ``n_per_arm`` is the operator-configured
    # target (matches the CLI ``--n``); ``idle_window_count`` and
    # ``loaded_window_count`` are the counts that SURVIVED the outlier
    # filter (rejected windows are excluded from the U statistic). A
    # downstream reader compares the survived counts against
    # ``n_per_arm`` to spot an empty-arm pathology (every loaded
    # window tripping a zero-floored outlier threshold and leaving
    # zero loaded samples).
    n_per_arm: int
    idle_window_count: int
    loaded_window_count: int
    # N synthetic CI-event producer threads firing taxonomy-weighted
    # emits into the bus during the loaded arm; default 50. ``0`` is
    # the smoke / offline path (no producers spawned).
    producer_count: int
    # Aggregate target emit rate across all producer threads in events
    # per wall-clock second. ``0.0`` collapses the producer swarm to a
    # no-op.
    producer_event_rate_hz: float
    # M LLM-agent subscriber subprocesses parked in ``wait_for``
    # during the loaded arm. One driver per framework in
    # ``subscriber_framework_mix`` (e.g. pydantic-ai, langgraph,
    # shell-control). ``0`` is the smoke / offline path.
    subscriber_agent_count: int
    # Per-framework spawned-driver count (e.g. {"pydantic": 1,
    # "langgraph": 1, "shell": 1} for the default 3-framework set).
    # Empty dict in smoke / offline runs.
    subscriber_framework_mix: dict[str, int]
    # True when any producer thread crashed or did not settle
    # mid-run. The bench continues on attrition; a non-False value
    # invalidates the producer-fanout claim for that run.
    producer_attrition_detected: bool
    # True when any LLM-agent subscriber subprocess died or failed to
    # wake during any loaded window.
    subscriber_attrition_detected: bool
    # Total realized producer emits summed across every loaded window
    # (sum of CiProducerSwarm.emit_count for each window's swarm).
    producer_emit_count_total: int
    # Total count of OpenLoopScheduler-detected late dispatches across
    # all loaded windows.
    producer_late_count_total: int
    # Total count of emit-call failures across all loaded windows.
    producer_error_count_total: int
    # Family-wise Bonferroni alpha the bench applied to its diff tests.
    # The diff family carries 7 marginals. A downstream reader confirms
    # the bench's significance gate without re-deriving from the raw
    # p-values.
    alpha_corrected_diff: float
    bonferroni_k_diff: int
    # BCa bootstrap CIs on the three primary substrates: per-arm
    # medians plus Hodges-Lehmann diff-of-medians + CI. CIs let a
    # downstream reader distinguish a tight band ("0.71
    # [0.70, 0.72] vs 0.85 [0.84, 0.86] ms/s") from a noisy overlap
    # ("0.71 [0.6, 0.8] vs 0.85 [0.75, 0.95]"); the schedstat
    # boundary case is exactly where this matters.
    median_idle_utime_per_sec_ci_low_ns: int
    median_idle_utime_per_sec_ci_high_ns: int
    median_loaded_utime_per_sec_ci_low_ns: int
    median_loaded_utime_per_sec_ci_high_ns: int
    median_diff_utime_per_sec_ns: int
    median_diff_utime_per_sec_ci_low_ns: int
    median_diff_utime_per_sec_ci_high_ns: int
    median_idle_schedstat_per_sec_ci_low_ns: int
    median_idle_schedstat_per_sec_ci_high_ns: int
    median_loaded_schedstat_per_sec_ci_low_ns: int
    median_loaded_schedstat_per_sec_ci_high_ns: int
    median_diff_schedstat_per_sec_ns: int
    median_diff_schedstat_per_sec_ci_low_ns: int
    median_diff_schedstat_per_sec_ci_high_ns: int
    median_idle_pcount_ci_low: int
    median_idle_pcount_ci_high: int
    median_loaded_pcount_ci_low: int
    median_loaded_pcount_ci_high: int
    median_diff_pcount: int
    median_diff_pcount_ci_low: int
    median_diff_pcount_ci_high: int
    bootstrap_n_resamples: int
    bootstrap_method: str
    bootstrap_confidence_level: float
    # A priori MDE from a Monte-Carlo power simulation against the
    # accepted idle-arm samples. The smallest shift in the
    # pre-registered grid that achieves ``mde_apriori_target_power``
    # at the Bonferroni-corrected alpha; ``0`` when the bench had
    # too few idle samples to characterize variance.
    # ``mde_apriori_achieved_power`` is the realized power at the
    # selected shift -- a value below target means the grid was
    # exhausted and the bench is underpowered at the operator's N.
    mde_apriori_utime_per_sec_ns: int
    mde_apriori_target_power: float
    mde_apriori_achieved_power: float
    mde_apriori_n_per_arm: int
    mde_apriori_n_resamples: int
    # Per-arm outlier-filter counts. Loaded is always zero today
    # (filter is asymmetric idle-only until a warmup phase lets the
    # pilot characterize under-load idle noise); the count surfaces
    # the asymmetry to a downstream reader rather than hiding it.
    outlier_filtered_idle_count: int
    outlier_filtered_loaded_count: int
    # Mann-Whitney U statistics. The bench tracks FOUR substrates:
    # ``utime`` and ``stime`` are jiffie-quantized at the kernel CLK_TCK
    # granularity (10 ms on a CLK_TCK=100 kernel) via /proc/<pid>/stat
    # and AGGREGATED across all threads of the daemon; ``schedstat`` is
    # per-task scheduler run-time in nanoseconds via
    # /proc/<pid>/task/*/schedstat summed across every TID; ``pcount``
    # is the per-task dispatch (wake-up) count from the same schedstat
    # file's field-2 summed across every TID. ``schedstat`` does NOT
    # split user vs system time -- it is total CPU time scheduled on
    # the daemon's TGID -- but it has true nanosecond resolution where
    # utime/stime are floored at the jiffie. ``pcount`` is the most
    # direct operationalisation of "how often was the daemon woken to
    # do work" and is independent of how long each wake took. p-values
    # are two-sided.
    # Mann-Whitney U statistics, EACH SUBSTRATE EMITTED TWICE: once on
    # per-wall-second normalized samples (the primary signal matching
    # the rate-form hypothesis) and once on raw per-window deltas
    # (forensic comparison). pcount has no normalized variant because
    # wake-counts are integer dispatches, not durations.
    mann_whitney_u_utime_per_sec: float
    mann_whitney_p_utime_per_sec: float
    mann_whitney_u_utime_raw: float
    mann_whitney_p_utime_raw: float
    mann_whitney_u_stime_per_sec: float
    mann_whitney_p_stime_per_sec: float
    mann_whitney_u_stime_raw: float
    mann_whitney_p_stime_raw: float
    mann_whitney_u_schedstat_per_sec: float
    mann_whitney_p_schedstat_per_sec: float
    mann_whitney_u_schedstat_raw: float
    mann_whitney_p_schedstat_raw: float
    mann_whitney_u_pcount: float
    mann_whitney_p_pcount: float
    # Per-substrate inapplicable reasons (utime/stime/schedstat each
    # carry a per-sec AND a raw reason; pcount carries one). The reason
    # is set to a sentinel string when the outlier filter ate one arm
    # or the substrate is unavailable (kernel without CONFIG_SCHEDSTATS,
    # all per-TID reads raced).
    mann_whitney_inapplicable_reason_utime_per_sec: str | None
    mann_whitney_inapplicable_reason_utime_raw: str | None
    mann_whitney_inapplicable_reason_stime_per_sec: str | None
    mann_whitney_inapplicable_reason_stime_raw: str | None
    mann_whitney_inapplicable_reason_schedstat_per_sec: str | None
    mann_whitney_inapplicable_reason_schedstat_raw: str | None
    mann_whitney_inapplicable_reason_pcount: str | None
    # Per-marginal rejection flags (p < _ALPHA). One pair per substrate
    # for utime/stime/schedstat (per-sec + raw); a single flag for
    # pcount.
    h0_rejected_utime_per_sec: bool
    h0_rejected_utime_raw: bool
    h0_rejected_stime_per_sec: bool
    h0_rejected_stime_raw: bool
    h0_rejected_schedstat_per_sec: bool
    h0_rejected_schedstat_raw: bool
    h0_rejected_pcount: bool
    # Top-level classification derived from the Mann-Whitney diff
    # family. One of:
    # ``"perturbation_detected"`` - at least one diff test rejects H0.
    # ``"inconclusive"`` - no diff test rejected. Under-powered run or
    #   boundary case.
    # ``"inapplicable_empty_arm"`` - the outlier filter ate one arm
    #   on EVERY available substrate.
    # ``"inapplicable_pilot_skipped"`` - the bench ran in pilot-skip mode.
    verdict: str
    # ``CONFIG_SCHEDSTATS`` availability captured at bench start via the
    # ``/proc/self/schedstat`` probe. False on a kernel build without the
    # config option; True otherwise. Surfaced on the verdict so a
    # downstream reader who sees ``mann_whitney_inapplicable_reason_*``
    # can distinguish "kernel does not expose substrate" from "every
    # per-TID read raced and was skipped".
    schedstat_kernel_available: bool
    # Linux jiffie size in ns at run time. Surfaced so an operator
    # auditing the verdict can see the kernel-side measurement
    # resolution the outlier filter was floored against (the same
    # value as ``_PILOT_THRESHOLD_FLOOR_NS``).
    daemon_sample_jiffie_ns: int
    # Reported power-floor: spec-locked at 1.5 sigma for the verdict
    # narrative. The ms equivalent is derived from the pilot's MAD.
    min_detectable_effect_sigma: float
    min_detectable_effect_ms: float
    # Median CPU time per arm. Raw fields carry per-window absolute
    # CPU nanoseconds (mismatched units across arms when loaded windows
    # stretch wall-time); per-sec fields carry CPU ns per wall second
    # (the rate that survives the wall-time stretch). The README
    # renderer divides by 1e6 to display ms / ms-per-second. utime
    # medians are jiffie-quantized; schedstat medians have true
    # nanosecond resolution.
    median_idle_utime_ns: int
    median_loaded_utime_ns: int
    median_idle_utime_per_sec_ns: int
    median_loaded_utime_per_sec_ns: int
    median_idle_schedstat_ns: int
    median_loaded_schedstat_ns: int
    median_idle_schedstat_per_sec_ns: int
    median_loaded_schedstat_per_sec_ns: int
    # Median per-window dispatch count per arm. ``pcount`` is integer-
    # valued (count of wake-ups, not a duration); it is shipped as an
    # ``int`` so the verdict's serialised form does not silently coerce
    # to float. Pcount idle is typically 0-2 (the daemon's main thread
    # may wake on a timer); pcount loaded scales linearly with the
    # event-emit rate of the workload.
    median_idle_pcount: int
    median_loaded_pcount: int
    # Median end-of-window daemon resident-set size in kilobytes per
    # arm. Surfaces a class of regression the CPU substrates cannot
    # detect: a daemon that keeps CPU flat but grows RSS under swarm
    # load (subscriber-outbox buffers, SQLite WAL bloat, per-event
    # allocator pinning). ``vmrss_leak_slope_kb_per_window`` is a
    # simple OLS-regression slope of the loaded arm's end-of-window
    # VmRSS against ordinal window position — a non-zero positive slope
    # is a leak proxy. ``vmrss_substrate_inapplicable_reason`` records
    # the substrate-unavailable case (every sample zero) so a downstream
    # reader does not mistake the all-zero sentinel for "no leak".
    median_idle_vmrss_kb: int
    median_loaded_vmrss_kb: int
    vmrss_leak_slope_kb_per_window: float
    vmrss_leak_intercept_kb: float
    vmrss_substrate_inapplicable_reason: str | None
    # Out-of-band outlier thresholds (from preflight pilot). MAD-based
    # construction across two substrates: the utime threshold is floored
    # to one jiffie so it survives sub-resolution idle pilots; the
    # schedstat substrate is nanosecond-resolution so no floor is applied.
    outlier_threshold_ns: int
    outlier_threshold_schedstat_ns: int
    rejected_window_count: int
    # Final flag: True iff p-value < _ALPHA on ANY of the three
    # marginals (utime, stime, schedstat) AND that marginal's
    # ``mann_whitney_inapplicable_reason`` is None. Distinguished from
    # the three-way ``verdict`` classifier so the legacy boolean
    # consumer keeps working. The schedstat marginal is included
    # because it can carry a real signal when the jiffie-floored
    # utime/stime path falls below detection.
    perturbation_detected: bool
    # Per-window mean GIL gap.
    mean_gil_gap_ns: int
    # Per-event-granularity proof: one entry per loaded window with
    # the number of agent_message events the workload emitted that
    # window. The locked granularity is ONE event per producer per
    # iteration, NOT per token.
    events_per_loaded_window: list[int]
    # Total cost (OpenAI dollar cost is the only summable line item).
    cost_usd_total: float | None
    cost_unknown_count: int
    # Count of measured swarm rows (flattened across every loaded
    # window) that read a prior run's cached prompt prefix (OpenAI
    # ``cached_tokens > 0`` or Claude ``cache_read_input_tokens > 0``).
    # ``0`` == clean cold-cache isolation; any non-zero value means a
    # run warmed the provider-side cache for a later run and the
    # cold-cache premise was violated for those calls. The count is the
    # observable; the bench does not hard-fail on it here.
    cache_contaminated_count: int
    # Hard cost-budget circuit breaker. The bench aborts before the
    # next loaded window when projected cost would breach this value.
    max_cost_usd_budget: float
    max_cost_usd_observed: float
    aborted_on_budget: bool
    limitations: list[str]
    pilot_skipped: bool
    pilot_skipped_reason: str | None
    # Per-source iteration histogram. Each loaded iteration picks one
    # source from the weighted soak taxonomy via
    # ``benchmarks._bench_source_mix.pick_source_for_iter`` and emits its
    # five per-driver events on that pair. The histogram lets the
    # verdict reader confirm the daemon's fan-out was exercised across
    # the full registered taxonomy.
    per_iter_source_distribution: dict[str, int] = {}


@dataclass
class _WorkloadResult:
    """Returned by the workload thread for one ``loaded`` window.

    Aggregates everything the iteration's driver calls produced.
    The orchestrator weaves this back into the corresponding
    ``WindowRow``.
    """

    rows: list[IterationRow] = field(default_factory=list)
    openai_captures: list[OpenAIEnvelope] = field(default_factory=list)
    error_messages: list[str] = field(default_factory=list)
    observed_claude_models: list[str] = field(default_factory=list)
    observed_openai_models: list[str] = field(default_factory=list)
    stop_reason_distribution: dict[str, int] = field(default_factory=dict)
    api_error_status_distribution: dict[str, int] = field(default_factory=dict)
    # The iteration's picked seed-source name (one of github / pytest /
    # docker / fs / agent). Defaults to the empty string when the
    # workload short-circuits before pick (``idle`` arm); the per-arm
    # aggregator skips empty values.
    picked_source: str = ""
    # Per-window producer + subscriber aggregates threaded back to the
    # main loop so the verdict-builder can sum across loaded windows.
    # Zero / False / empty are the smoke / offline sentinels.
    producer_emit_count: int = 0
    producer_late_count: int = 0
    producer_error_count: int = 0
    producer_attrition: bool = False
    subscriber_attrition: bool = False
    subscriber_framework_mix: dict[str, int] = field(default_factory=dict)


def _read_voluntary_ctxt_switches(pid: int) -> tuple[int, int]:
    """Read (voluntary, nonvoluntary) ctxt switches from /proc/<pid>/status.

    Used per-window to detect daemon preemption by non-waitbus processes.
    Returns (0, 0) on read failure; the bench logs but does not abort
    so a transient /proc race does not crash the bench mid-run.
    """
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as fh:
            text = fh.read()
    except OSError:
        return (0, 0)
    voluntary = 0
    nonvoluntary = 0
    for line in text.splitlines():
        if line.startswith("voluntary_ctxt_switches:"):
            with contextlib.suppress(ValueError):
                voluntary = int(line.split(":", 1)[1].strip())
        elif line.startswith("nonvoluntary_ctxt_switches:"):
            with contextlib.suppress(ValueError):
                nonvoluntary = int(line.split(":", 1)[1].strip())
    return (voluntary, nonvoluntary)


def _measure_window(
    *,
    daemon_pid: int,
    arm: str,
    window_id: int,
    workload_runner: Any,
) -> tuple[WindowRow, _WorkloadResult]:
    """Capture one ``idle`` or ``loaded`` window.

    Daemon CPU sampling: read /proc/<pid>/stat at window start, run the
    arm-appropriate work for ``_WINDOW_DURATION_SEC``, read /proc/<pid>/stat
    again at window end. The delta is the daemon's CPU consumption
    during the window.

    GIL-pressure proxy: ``time.thread_time_ns()`` delta on THIS thread
    (the measurement thread) is subtracted from the wall-clock window
    duration; the gap is the time this thread was descheduled while
    other GIL holders ran.

    ``workload_runner`` is a zero-arg callable that returns a
    ``_WorkloadResult`` for ``loaded`` arms; for ``idle`` arms the bench
    passes a runner that sleeps for the window duration.

    The voluntary / nonvoluntary context-switch deltas are captured as
    forensic ``WindowRow`` metadata only; they do NOT gate window
    rejection. The daemon is core-pinned for every publishable baseline,
    so its context-switch counts track its own load-induced kernel
    activity rather than exogenous preemption -- an unreliable,
    one-arm-biased rejection signal.
    """
    t_window_start_ns = time.monotonic_ns()
    measurement_thread_t0 = time.thread_time_ns()
    utime_t0, stime_t0 = read_daemon_cpu_ns(daemon_pid)
    schedstat_t0 = read_daemon_schedstat(daemon_pid)
    vol_t0, nonvol_t0 = _read_voluntary_ctxt_switches(daemon_pid)
    vmrss_t0 = read_daemon_vmrss_kb(daemon_pid)
    workload_result = workload_runner()
    # If the workload returned before the window's nominal duration
    # elapsed, sleep out the rest so every window has a uniform
    # measurement length. This keeps Mann-Whitney's per-arm samples
    # comparable in unit (CPU-time-per-1s-window).
    elapsed_ns = time.monotonic_ns() - t_window_start_ns
    remaining_ns = int(_WINDOW_DURATION_SEC * 1_000_000_000) - elapsed_ns
    if remaining_ns > 0:
        time.sleep(remaining_ns / 1_000_000_000)
    utime_t1, stime_t1 = read_daemon_cpu_ns(daemon_pid)
    schedstat_t1 = read_daemon_schedstat(daemon_pid)
    vol_t1, nonvol_t1 = _read_voluntary_ctxt_switches(daemon_pid)
    vmrss_t1 = read_daemon_vmrss_kb(daemon_pid)
    measurement_thread_t1 = time.thread_time_ns()
    t_window_end_ns = time.monotonic_ns()

    utime_delta = max(0, utime_t1 - utime_t0)
    stime_delta = max(0, stime_t1 - stime_t0)
    schedstat_run_delta = max(0, schedstat_t1.run_time_ns - schedstat_t0.run_time_ns)
    schedstat_wait_delta = max(0, schedstat_t1.wait_time_ns - schedstat_t0.wait_time_ns)
    schedstat_pcount_delta = max(0, schedstat_t1.pcount - schedstat_t0.pcount)
    vol_delta = max(0, vol_t1 - vol_t0)
    nonvol_delta = max(0, nonvol_t1 - nonvol_t0)
    thread_delta = max(0, measurement_thread_t1 - measurement_thread_t0)
    wall_delta = t_window_end_ns - t_window_start_ns
    # The ctxt deltas above are forensic-only and never gate rejection.
    # Window rejection is owned by the loaded-arm attrition check below
    # and the utime idle-arm outlier filter in ``_build_verdict``. A
    # loaded window whose producer swarm or subscriber pool lost members
    # mid-window carries a biased CPU signal and is rejected here; the
    # idle arm has no attrition so it always falls through unrejected.
    rejected = False
    rejection_reason: str | None = None
    if workload_result.producer_attrition or workload_result.subscriber_attrition:
        rejected = True
        rejection_reason = "loaded_arm_attrition"

    return WindowRow(
        window_id=window_id,
        arm=arm,
        t_window_start_ns=t_window_start_ns,
        t_window_end_ns=t_window_end_ns,
        daemon_utime_delta_ns=utime_delta,
        daemon_stime_delta_ns=stime_delta,
        daemon_schedstat_run_delta_ns=schedstat_run_delta,
        daemon_schedstat_wait_delta_ns=schedstat_wait_delta,
        daemon_schedstat_pcount_delta=schedstat_pcount_delta,
        daemon_schedstat_tid_count_end=schedstat_t1.tid_count,
        daemon_voluntary_ctxt_delta=vol_delta,
        daemon_nonvoluntary_ctxt_delta=nonvol_delta,
        daemon_vmrss_start_kb=vmrss_t0,
        daemon_vmrss_end_kb=vmrss_t1,
        measurement_thread_time_delta_ns=thread_delta,
        wall_minus_thread_time_ns=max(0, wall_delta - thread_delta),
        swarm_rows=list(workload_result.rows),
        rejected=rejected,
        rejection_reason=rejection_reason,
        picked_source=workload_result.picked_source,
    ), workload_result


def _idle_runner() -> _WorkloadResult:
    """Sleep out the window length; return an empty WorkloadResult."""
    time.sleep(_WINDOW_DURATION_SEC)
    return _WorkloadResult()


# Frameworks that invoke an LLM (and therefore can incur cost). A
# ``shell-control`` driver issues no LLM call so it contributes neither
# observed cost nor an "unknown cost" to the budget tracker.
_LLM_FRAMEWORKS: frozenset[str] = frozenset({"pydantic", "langgraph", "claude-cli", "gemini-cli"})


def _record_row_cost(budget_tracker: CostBudgetTracker, swarm_row: IterationRow) -> None:
    """Attribute one loaded-arm swarm row's cost to the budget tracker.

    Exactly one of the four branches fires per row: an OpenAI / claude /
    gemini envelope routes to its typed recorder; a row with no envelope
    whose driver is an LLM framework routes to ``record_unknown`` so the
    spend gap is surfaced rather than silently dropped; a non-LLM
    (``shell-control``) row contributes nothing. Single-sourced so the
    run loop and the unit tests drive identical attribution logic.
    """
    if swarm_row.openai_env is not None:
        budget_tracker.record_openai(swarm_row.openai_env)
    elif swarm_row.claude_env is not None:
        budget_tracker.record_claude(swarm_row.claude_env.cost_usd)
    elif swarm_row.gemini_env is not None:
        budget_tracker.record_gemini()
    elif swarm_row.driver in _LLM_FRAMEWORKS:
        # An LLM driver ran but no envelope was hydrated (no parseable
        # wake marker — a crashed driver or a torn stdout). Record it as
        # unattributable cost so the verdict's ``cost_unknown_count``
        # reflects the gap rather than silently dropping the call. The
        # row's ``driver`` is the bare framework name (carried first-class
        # off the spawned child), so the membership test is exact.
        budget_tracker.record_unknown()


def _do_workload_iteration(
    *,
    iter_id: int,
    run_salt: str,
    include_real_llm: bool,
    producer_swarm: CiProducerSwarm | None,
    agent_pool: LlmAgentPool | None,
    daemon_env: dict[str, str],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    python_exe: str,
    stderr_dir: Path,
) -> _WorkloadResult:
    """Run one loaded-window iteration in the producer-then-subscriber shape.

    Workflow per iteration:

    1. Spawn M LLM-agent subscriber subprocesses (one per framework in
       ``agent_pool.frameworks``); each parks in ``waitbus.wait_for``
       against an owner-scoped predicate.
    2. Settle for ``_AGENT_SETTLE_SEC`` so every driver has subscribed
       before the producer burst lands.
    3. Fire the CI producer swarm (blocking; bounded by the swarm's
       ``run_duration_sec``). Each emit draws ``(source, event_type)``
       from the soak taxonomy via ``pick_source_for_iter``.
    4. Collect each agent's exit code + stdout (bounded by
       ``_AGENT_COLLECT_TIMEOUT_SEC``).
    5. Convert per-agent results into ``IterationRow`` entries and
       return the aggregated ``_WorkloadResult``.

    Smoke / offline (``include_real_llm=False`` OR either pool is
    ``None``): short-circuit to an empty ``_WorkloadResult`` so the
    bench's shape-check path still runs without spawning any
    subprocesses or producers.

    Per-agent envelope hydration: each child's stdout is scanned for the
    canonical ``DRIVER_REACTED`` wake marker, the marker's token-usage
    payload is parsed, and exactly one typed envelope is built keyed on
    the driver's framework (``claude_env`` / ``gemini_env`` /
    ``openai_env``; the other two stay ``None``). The bench's downstream
    cost tracker reads real spend off the OpenAI envelope; ``claude-cli``
    / ``gemini-cli`` spend is carried on its envelope but surfaced as
    ``cost_unknown_count`` because only OpenAI calls have a priced
    line-item. ``shell-control`` (and any child that crashed before
    emitting a marker) yields no envelope. A marker whose token-usage
    payload is present but malformed flips the row's invariant gate so a
    dropped envelope is observable rather than silently counting as a
    clean reaction.
    """
    result = _WorkloadResult()
    if not include_real_llm or producer_swarm is None or agent_pool is None:
        return result
    picked_source, _picked_event_type = pick_source_for_iter(iter_id)
    result.picked_source = picked_source
    # Anchor the daemon's seq cursor BEFORE the drivers spawn so a
    # heavyweight cold-import (langgraph + langchain-openai is the worst
    # offender under the bench's affinity-pinned 2-core orchestrator
    # split) cannot push a driver's wait_for subscription past the
    # producer's 1s emit window. With ``since=<anchor.event_id>`` the
    # daemon replays every matching row with ``seq > anchor`` on the
    # driver's subscribe, so the producer's emits land even when a
    # driver registers late. The anchor uses
    # ``owner=anchor:<seed_scope_id>`` so it does NOT itself match the
    # driver's ``fields.owner=<seed_scope_id>`` predicate; only the
    # producer's real seeds wake the drivers.
    from benchmarks._bench_anchor import emit_anchor_event

    anchor_event_id = emit_anchor_event(
        seed_scope_id=seed_scope_id,
        db_path=db_path,
        doorbell_path=doorbell_path,
        repo="bench",
        ingest_method="bench_multistream_proof_anchor",
        delivery_id_prefix="bench-multistream-anchor",
    )
    try:
        # Salt the cold-cache prefix with the run-scoped ``run_salt`` so a
        # separate benchmark process cannot hit this run's cached prompt
        # prefix under the same API key (the provider prompt caches are keyed
        # on a content-addressed prefix hash, not per-process). The prefix
        # also folds in ``iter_id`` so each iteration starts from a cold cache.
        cold_prefix = force_cold_cache_prefix(run_salt, iter_id)
        agent_pool.spawn(since_cursor=anchor_event_id, cold_prefix=cold_prefix)
        agent_pool.settle(timeout_sec=_AGENT_SETTLE_SEC)
        producer_swarm.fire()
        pool_results = agent_pool.collect(timeout_sec=_AGENT_COLLECT_TIMEOUT_SEC)
    finally:
        agent_pool.teardown()
    sentinel = f"waitbus-bench-iter-{iter_id}-{uuid.uuid4().hex[:8]}"
    for child_result in pool_results.per_child:
        # The framework identity rides ``child_result.framework`` as a
        # first-class field carried straight off the spawned ``_Child``;
        # the bench-level driver tag is the bare framework name so rows
        # group by framework without any string parsing.
        framework = child_result.framework
        invariant_failed = child_result.exit_code != 0
        invariant_failure_field = "agent_subprocess_failed" if invariant_failed else None
        # Per-child diagnostic surface: when the per-child invariant fails,
        # log the exit code plus the trailing stdout bytes (where the
        # WAKE_RECEIVED / DRIVER_REACTED markers would otherwise have
        # appeared) so an off-line reader can distinguish the CLI-driver
        # failure classes by exit code (see the EXIT_* taxonomy in
        # ``scripts.stress._real_drivers``): 1 (seed-wait timeout) /
        # 2 (CLI not on PATH) / 3 (LLM-call timeout) /
        # 4 (auth-or-invocation error) / 5 (refusal or non-zero envelope)
        # from "driver was SIGTERM'd by collect timeout (returncode < 0)".
        if invariant_failed:
            structured(
                _logger,
                logging.WARNING,
                "bench_invariant_child_failed",
                iter_id=iter_id,
                role=child_result.role,
                exit_code=child_result.exit_code,
                stdout_tail=child_result.stdout_bytes[-400:].decode(errors="replace"),
            )
        # Parse the child's stdout for the canonical ``DRIVER_REACTED``
        # wake marker and rehydrate the driver-side TokenUsage. Every LLM
        # driver emits exactly one such marker; ``shell-control`` (and any
        # child that crashed before emitting) yields ``token_usage=None``.
        # A marker whose ``token_usage`` payload is present but malformed
        # flips the row's invariant gate (distinct from a non-LLM driver's
        # legitimate ``None``) so a dropped envelope is observable rather
        # than counting as a clean reaction.
        token_usage = None
        for _line in child_result.stdout_bytes.decode(errors="replace").splitlines():
            _fields = _parse_wake_marker(_line)
            if _fields is not None:
                token_usage, _token_parse_failed = observed_token_usage_from_marker(_fields)
                if _token_parse_failed and not invariant_failed:
                    invariant_failed = True
                    invariant_failure_field = "token_usage_parse_failed"
                break
        # Hydrate exactly one typed envelope keyed on the driver's
        # framework (the other two stay ``None``) so the per-row cost
        # sink sees exactly which driver's spend is in play. Mirrors the
        # bench-A template in ``bench_polling_vs_subscribe_llm_agent``.
        claude_env: ClaudeEnvelope | None = None
        gemini_env: GeminiEnvelope | None = None
        openai_env: OpenAIEnvelope | None = None
        cache_state = "NA"
        if token_usage is not None:
            if framework == "claude-cli":
                claude_env = claude_envelope_from_token_usage(token_usage)
                cache_state = _classify_claude_cache_state(
                    visible=claude_env.input_tokens_visible,
                    cache_read=claude_env.cache_read_input_tokens,
                    billed_input=claude_env.billed_input_tokens,
                )
            elif framework == "gemini-cli":
                gemini_env = gemini_envelope_from_token_usage(token_usage)
            elif framework in {"pydantic", "langgraph"}:
                openai_env = openai_envelope_from_token_usage(token_usage)
                cache_state = "WARM" if openai_env.cached_tokens > 0 else "COLD"
        result.rows.append(
            IterationRow(
                iter_id=iter_id,
                arm="loaded",
                driver=framework,
                sentinel=sentinel,
                t_send_ns=0,
                t_observe_ns=0,
                latency_ns=0,
                cache_state=cache_state,
                claude_env=claude_env,
                gemini_env=gemini_env,
                openai_env=openai_env,
                invariant_failed=invariant_failed,
                invariant_failure_field=invariant_failure_field,
            )
        )
    return result


def _make_loaded_runner(
    *,
    iter_id_offset: int,
    run_salt: str,
    daemon_env: dict[str, str],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    include_real_llm: bool,
    producer_count: int,
    producer_event_rate_hz: float,
    agent_frameworks: tuple[str, ...],
    stderr_root: Path,
) -> Any:
    """Return a per-window runner constructing a fresh producer + agent pool.

    Each loaded window builds its own ``CiProducerSwarm`` +
    ``LlmAgentPool`` so the moderation / cold-prefix / wait_for state
    resets cleanly between windows (matches
    ``bench_polling_vs_subscribe_llm_agent``'s per-iteration
    lifecycle). The per-window index advances on each call via a
    ``nonlocal`` counter local to this factory; the runner is the sole
    owner of that state, so no caller-shared mutable cell is needed.
    """
    window_index = 0

    def _runner() -> _WorkloadResult:
        nonlocal window_index
        iter_id = iter_id_offset + window_index
        window_index += 1
        if not include_real_llm or producer_count == 0 or not agent_frameworks:
            return _do_workload_iteration(
                iter_id=iter_id,
                run_salt=run_salt,
                include_real_llm=False,
                producer_swarm=None,
                agent_pool=None,
                daemon_env=daemon_env,
                socket_path=socket_path,
                db_path=db_path,
                doorbell_path=doorbell_path,
                seed_scope_id=seed_scope_id,
                python_exe=sys.executable,
                stderr_dir=stderr_root,
            )
        window_stderr_dir = stderr_root / f"window-{iter_id:04d}"
        with (
            CiProducerSwarm(
                producer_count=producer_count,
                aggregate_rate_hz=producer_event_rate_hz,
                run_duration_sec=_WINDOW_DURATION_SEC,
                seed_scope_id=seed_scope_id,
                db_path=db_path,
                doorbell_path=doorbell_path,
                iter_id_base=iter_id * max(1, producer_count) * 100,
            ) as producer_swarm,
            LlmAgentPool(
                frameworks=agent_frameworks,
                env=daemon_env,
                socket_path=socket_path,
                db_path=db_path,
                doorbell_path=doorbell_path,
                seed_scope_id=seed_scope_id,
                python_exe=sys.executable,
                stderr_dir=window_stderr_dir,
            ) as agent_pool,
        ):
            result = _do_workload_iteration(
                iter_id=iter_id,
                run_salt=run_salt,
                include_real_llm=True,
                producer_swarm=producer_swarm,
                agent_pool=agent_pool,
                daemon_env=daemon_env,
                socket_path=socket_path,
                db_path=db_path,
                doorbell_path=doorbell_path,
                seed_scope_id=seed_scope_id,
                python_exe=sys.executable,
                stderr_dir=window_stderr_dir,
            )
            # Attach producer-swarm aggregates to the result so the
            # outer bench loop can aggregate them into the verdict.
            result.producer_emit_count = producer_swarm.emit_count
            result.producer_late_count = producer_swarm.late_count
            result.producer_error_count = producer_swarm.error_count
            result.producer_attrition = producer_swarm.attrition_detected
            result.subscriber_attrition = agent_pool.attrition_detected
            result.subscriber_framework_mix = dict(agent_pool.framework_mix)
            return result

    return _runner


def _wait_for_daemon_socket(socket_path: Path) -> None:
    """Block until the daemon's AF_UNIX socket appears, or raise."""
    deadline = time.monotonic() + _DAEMON_READY_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if socket_path.exists():
            return
        time.sleep(0.05)
    raise RuntimeError(f"daemon did not bind {socket_path} within {_DAEMON_READY_TIMEOUT_SEC}s")


def _wait_for_daemon_schema(db_path: Path) -> None:
    """Block until the daemon's events table exists in the DB, or raise.

    Socket-bound and schema-migrated are independent: the daemon can
    accept connections before it has finished creating the ``events``
    table. Any probe that reads the DB (``capture_daemon_pragmas``, the
    PRAGMA snapshot) must wait for the migration to land or it races a
    half-initialised file. Polling ``sqlite_master`` for the ``events``
    table is the schema marker -- once that row exists, the daemon's
    ``CREATE TABLE`` migration has run. The DB is opened read-only so the
    probe can never write to it.
    """
    import sqlite3

    deadline = time.monotonic() + _DAEMON_READY_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if db_path.exists():
            uri = f"file:{db_path}?mode=ro"
            try:
                with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=2.0)) as conn:
                    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'").fetchone()
                if row is not None:
                    return
            except sqlite3.Error:
                # DB file exists but is mid-migration (locked / no schema
                # yet); keep polling until the deadline.
                pass
        time.sleep(0.05)
    raise RuntimeError(f"daemon did not migrate schema in {db_path} within {_DAEMON_READY_TIMEOUT_SEC}s")


class _PilotThresholds(NamedTuple):
    """Per-substrate MAD-derived outlier thresholds from the idle pilot.

    Each threshold is applied to the corresponding column on each window
    row. Idle-pilot derivation means the threshold characterises the
    daemon's noise floor when no LLM workload is running.
    """

    utime_ns: int
    schedstat_ns: int


def _pilot_outlier_threshold(daemon_pid: int) -> _PilotThresholds:
    """Sample N idle windows and return per-substrate MAD-derived thresholds.

    Out-of-band (Mann-Whitney + MAD-based outlier rejection): the
    thresholds are fixed BEFORE the main run begins so the loaded arm
    cannot influence the rule.

    Two substrates are sampled from the same pilot windows so the
    substrate-comparison is apples-to-apples on noise floor:
      - utime: floored at one jiffie (``_PILOT_THRESHOLD_FLOOR_NS``) to
        survive the sub-jiffie idle pilot case where median + MAD
        collapses to zero.
      - schedstat: nanosecond-resolution; no floor needed.
    """
    utime_samples: list[int] = []
    schedstat_samples: list[int] = []
    for _ in range(_PILOT_WINDOW_COUNT):
        row, _ = _measure_window(
            daemon_pid=daemon_pid,
            arm="idle_pilot",
            window_id=-1,
            workload_runner=_idle_runner,
        )
        utime_samples.append(row.daemon_utime_delta_ns)
        schedstat_samples.append(row.daemon_schedstat_run_delta_ns)

    def _mad_threshold(samples: list[int]) -> int:
        """Outlier threshold = median + k*MAD over ``samples``.

        Unit-blind: the same estimator serves nanosecond CPU deltas and
        integer counter samples. Uses the midpoint element (not an
        interpolated median) so the result stays an integer in the
        input's own units.
        """
        samples_sorted = sorted(samples)
        median = samples_sorted[len(samples_sorted) // 2]
        abs_devs = sorted(abs(s - median) for s in samples_sorted)
        mad = abs_devs[len(abs_devs) // 2]
        return int(median + _OUTLIER_MAD_MULTIPLIER * mad)

    return _PilotThresholds(
        utime_ns=max(_PILOT_THRESHOLD_FLOOR_NS, _mad_threshold(utime_samples)),
        schedstat_ns=_mad_threshold(schedstat_samples),
    )


def _compute_mann_whitney(
    idle: list[int],
    loaded: list[int],
    *,
    alternative: str = "less",
) -> tuple[float, float, str | None]:
    """Compute Mann-Whitney U + p-value + inapplicable reason.

    Default ``alternative="less"`` tests the asymmetric hypothesis
    ``H1: idle stochastically less than loaded`` -- the right
    direction for the bench's "daemon consumes MORE under load" claim.
    A daemon that consumed LESS under load is not a perturbation in
    the hypothesis sense. Pass ``alternative="two-sided"`` (or
    ``"greater"``) for forensic counter-direction tests.

    Returns ``(0.0, 1.0, reason)`` when either arm is empty; the
    reason field distinguishes "no perturbation detected on real
    samples" from "test inapplicable because the outlier filter ate
    one arm." Both arms non-empty: returns ``(u, p, None)``.
    """
    if not idle and not loaded:
        return 0.0, 1.0, "empty_both_arms"
    if not idle:
        return 0.0, 1.0, "empty_idle"
    if idle and loaded and len(set(idle)) == 1 and len(set(loaded)) == 1 and idle[0] == loaded[0]:
        # Both arms degenerate (every sample identical AND equal across
        # arms). Mann-Whitney is statistically undefined; ``p == 1.0``
        # would be silently misread as "no perturbation" otherwise.
        return 0.0, 1.0, "both_arms_all_tied"
    if not loaded:
        return 0.0, 1.0, "empty_loaded_after_outlier_filter"
    from scipy.stats import mannwhitneyu

    u_stat, p_value = mannwhitneyu(idle, loaded, alternative=alternative)
    return float(u_stat), float(p_value), None


def _median_int(values: list[int]) -> int:
    """Median of a list of ints; 0 on empty input."""
    if not values:
        return 0
    sorted_values = sorted(values)
    n = len(sorted_values)
    if n % 2 == 1:
        return sorted_values[n // 2]
    return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) // 2


# Bootstrap parameters. 10k BCa resamples is the audit-recommended
# default; cost is ~50-100ms per CI at N=50/side -- negligible
# alongside the bench's wall-clock. The seed is deterministic per
# ``PYTHONHASHSEED`` so two runs on the same windows produce
# identical CI bands.
_BOOTSTRAP_N_RESAMPLES = 10_000
_BOOTSTRAP_METHOD = "BCa"
_BOOTSTRAP_CONFIDENCE = 0.95


def _bootstrap_seed() -> int:
    """Return the bench's bootstrap RNG seed (deterministic per PYTHONHASHSEED)."""
    return _hashseed_or_default()


def _median_with_ci(values: list[int]) -> tuple[int, int, int]:
    """Return ``(point, ci_low, ci_high)`` median + BCa bootstrap CI.

    Empty input returns ``(0, 0, 0)``; single-element input returns
    the value three times (BCa degenerates on n=1). Otherwise the
    point estimate is the sample median and the CI is the BCa 95%
    interval over 10k resamples.
    """
    if not values:
        return 0, 0, 0
    if len(values) < 2:
        # scipy.stats.bootstrap requires >= 2 samples; degenerate CI
        # collapses to the point estimate.
        return int(values[0]), int(values[0]), int(values[0])
    if len(set(values)) == 1:
        # All values identical: bootstrap is degenerate; BCa cannot
        # compute. Collapse to the point estimate to avoid NaN bands.
        return int(values[0]), int(values[0]), int(values[0])
    import warnings

    import numpy as np
    from scipy.stats import bootstrap

    arr = np.asarray(values, dtype=np.int64)
    point = int(np.median(arr))
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=Warning, message=".*[Dd]egenerate.*")
        try:
            result = bootstrap(
                (arr,),
                np.median,
                n_resamples=_BOOTSTRAP_N_RESAMPLES,
                method=_BOOTSTRAP_METHOD,
                confidence_level=_BOOTSTRAP_CONFIDENCE,
                rng=np.random.default_rng(_bootstrap_seed()),
            )
        except Warning:
            return point, point, point
    low_raw = float(result.confidence_interval.low)
    high_raw = float(result.confidence_interval.high)
    # Guard against NaN bands (BCa can return NaN on near-degenerate
    # data even without raising). NaN is the only float != itself.
    if low_raw != low_raw or high_raw != high_raw:
        return point, point, point
    return point, int(low_raw), int(high_raw)


def _hodges_lehmann_diff(idle: list[int], loaded: list[int]) -> tuple[int, int, int]:
    """Return ``(point, ci_low, ci_high)`` Hodges-Lehmann diff-of-medians.

    Hodges-Lehmann is the median of all pairwise (loaded - idle)
    differences; the location-shift estimator that matches the
    Mann-Whitney null. BCa bootstrap gives the CI. Empty arms
    degenerate to ``(0, 0, 0)``.
    """
    if not idle or not loaded:
        return 0, 0, 0
    import warnings

    import numpy as np
    from scipy.stats import bootstrap

    idle_arr = np.asarray(idle, dtype=np.int64)
    loaded_arr = np.asarray(loaded, dtype=np.int64)

    def _hl_stat(i_samples: object, l_samples: object) -> float:
        i_arr = np.asarray(i_samples)
        l_arr = np.asarray(l_samples)
        diffs = np.subtract.outer(l_arr, i_arr).reshape(-1)
        return float(np.median(diffs))

    point = int(_hl_stat(idle_arr, loaded_arr))
    if len(idle) < 2 or len(loaded) < 2:
        # scipy.stats.bootstrap requires >= 2 samples per arm.
        return point, point, point
    if len(set(idle)) == 1 and len(set(loaded)) == 1:
        # Both arms degenerate; BCa cannot compute.
        return point, point, point
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=Warning, message=".*[Dd]egenerate.*")
        try:
            result = bootstrap(
                (idle_arr, loaded_arr),
                _hl_stat,
                n_resamples=_BOOTSTRAP_N_RESAMPLES,
                method=_BOOTSTRAP_METHOD,
                confidence_level=_BOOTSTRAP_CONFIDENCE,
                paired=False,
                vectorized=False,
                rng=np.random.default_rng(_bootstrap_seed()),
            )
        except Warning:
            return point, point, point
    low_raw = float(result.confidence_interval.low)
    high_raw = float(result.confidence_interval.high)
    # NaN is the only float != itself; guards BCa NaN bands.
    if low_raw != low_raw or high_raw != high_raw:
        return point, point, point
    return point, int(low_raw), int(high_raw)


# Shift grid for the a priori MDE Monte-Carlo (utime / schedstat
# per-wall-second). Log-spaced from 0.5 ms/s to 50 ms/s; the
# smallest shift that achieves the operator-chosen power floor wins.
_MDE_SHIFT_GRID_NS_PER_SEC: list[int] = [
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    20_000_000,
    50_000_000,
]
_MDE_TARGET_POWER = 0.80
_MDE_N_RESAMPLES = 1_000


def _compute_mde_apriori(
    idle_samples: list[int],
    *,
    alpha: float,
    n_per_arm: int,
    target_power: float = _MDE_TARGET_POWER,
    shift_grid_ns_per_sec: list[int] | None = None,
) -> tuple[int, float]:
    """Smallest shift achieving ``target_power``; return ``(mde_ns_per_sec, achieved)``.

    Resamples ``idle_samples`` (with replacement) to synthesize both
    arms (loaded = idle + shift), runs a one-sided Mann-Whitney
    ``alternative='less'`` over ``_MDE_N_RESAMPLES`` trials per shift,
    and counts the rejection rate at the Bonferroni-corrected
    ``alpha``. Walks the shift grid from smallest; returns the first
    shift that achieves the target power AND its realized power.
    When no grid point achieves the target, returns the largest grid
    point plus the largest realized power (the bench underpowered at
    the operator's variance + N).

    Returns ``(0, 0.0)`` when ``len(idle_samples) < 10`` (too few
    samples to characterize variance) or when scipy.stats.power
    raises -- a downstream reader sees the zero sentinel and routes
    around the a priori claim.
    """
    grid = shift_grid_ns_per_sec or _MDE_SHIFT_GRID_NS_PER_SEC
    if len(idle_samples) < 10 or n_per_arm < 2:
        return 0, 0.0
    import numpy as np
    from scipy.stats import mannwhitneyu, power

    idle_arr = np.asarray(idle_samples, dtype=np.float64)
    rng = np.random.default_rng(_bootstrap_seed())

    def _mwu_test(x: object, y: object, axis: int = -1) -> object:
        return mannwhitneyu(x, y, alternative="less", axis=axis).pvalue

    last_power = 0.0
    last_shift = grid[-1] if grid else 0
    for shift in grid:

        def _rvs_idle(
            size: tuple[int, ...],
            _random_state: object = None,
            _rng: object = rng,
            _arr: object = idle_arr,
        ) -> object:
            return _rng.choice(_arr, size=size, replace=True)  # type: ignore[attr-defined]

        def _rvs_loaded(
            size: tuple[int, ...],
            _random_state: object = None,
            _rng: object = rng,
            _arr: object = idle_arr,
            _shift: int = shift,
        ) -> object:
            return _rng.choice(_arr, size=size, replace=True) + _shift  # type: ignore[attr-defined]

        try:
            res = power(
                _mwu_test,
                (_rvs_idle, _rvs_loaded),
                n_observations=(n_per_arm, n_per_arm),
                significance=alpha,
                n_resamples=_MDE_N_RESAMPLES,
                vectorized=False,
            )
        except (ValueError, RuntimeError):
            return 0, 0.0
        last_power = float(res.power)
        last_shift = shift
        if last_power >= target_power:
            return int(shift), last_power
    return int(last_shift), last_power


class _SubstrateSpec(NamedTuple):
    """Table-driven description of one Mann-Whitney substrate cell.

    Collapses the otherwise hand-expanded cartesian of
    {substrate} x {raw, per_sec} x {Mann-Whitney, median, CI}
    into a single uniform record. ``extractor`` pulls the per-window
    integer sample off a ``WindowRow``; the remaining flags encode the
    genuine per-substrate differences (pcount has no per-second form;
    schedstat/pcount route to an unavailable sentinel when the kernel
    lacks ``CONFIG_SCHEDSTATS``; stime carries no bootstrap CI or
    median fields on the verdict).
    """

    name: str
    extractor: Callable[[WindowRow], int]
    has_per_sec: bool
    schedstat_availability_override: bool
    has_bootstrap_ci: bool


# Frozen registry of the four uniform statistical substrates. The order
# is the historical field-emit order in ``ExperimentBVerdict`` and is
# load-bearing for the Bonferroni-family bookkeeping, so it must not be
# reordered. utime/stime/schedstat carry both raw and per-second forms;
# pcount is a dispatch count with no per-second variant.
_SUBSTRATE_SPECS: tuple[_SubstrateSpec, ...] = (
    _SubstrateSpec(
        name="utime",
        extractor=lambda r: r.daemon_utime_delta_ns,
        has_per_sec=True,
        schedstat_availability_override=False,
        has_bootstrap_ci=True,
    ),
    _SubstrateSpec(
        name="stime",
        extractor=lambda r: r.daemon_stime_delta_ns,
        has_per_sec=True,
        schedstat_availability_override=False,
        has_bootstrap_ci=False,
    ),
    _SubstrateSpec(
        name="schedstat",
        extractor=lambda r: r.daemon_schedstat_run_delta_ns,
        has_per_sec=True,
        schedstat_availability_override=True,
        has_bootstrap_ci=True,
    ),
    _SubstrateSpec(
        name="pcount",
        extractor=lambda r: r.daemon_schedstat_pcount_delta,
        has_per_sec=False,
        schedstat_availability_override=True,
        has_bootstrap_ci=True,
    ),
)


@dataclass(frozen=True)
class _SubstrateResult:
    """Every computed statistic for one substrate, keyed by ``spec.name``.

    Fields default to the empty-arm sentinels so the registry loop can
    populate exactly the cells a substrate carries (e.g. pcount leaves
    the ``*_per_sec`` cells at their sentinels) without the assembler
    needing per-substrate conditionals.
    """

    name: str
    # Sample lists (raw + per-second). pcount leaves per_sec empty.
    idle_raw: list[int]
    loaded_raw: list[int]
    idle_per_sec: list[int]
    loaded_per_sec: list[int]
    # Mann-Whitney diff test, raw + per_sec forms.
    u_raw: float
    p_raw: float
    inapp_raw: str | None
    u_per_sec: float
    p_per_sec: float
    inapp_per_sec: str | None
    h0_rejected_raw: bool
    h0_rejected_per_sec: bool
    # Medians (raw + per_sec; pcount leaves per_sec at 0).
    median_idle_raw: int
    median_loaded_raw: int
    median_idle_per_sec: int
    median_loaded_per_sec: int
    # Bootstrap CIs on the primary form (per_sec for rate substrates,
    # raw for pcount); HL diff CI. Zero sentinels when not computed.
    ci_idle_lo: int
    ci_idle_hi: int
    ci_loaded_lo: int
    ci_loaded_hi: int
    diff_pt: int
    diff_lo: int
    diff_hi: int


def _substrate_samples(
    spec: _SubstrateSpec,
    rows: list[WindowRow],
    per_wall_sec: Callable[[int, WindowRow], int],
) -> tuple[list[int], list[int], list[int], list[int]]:
    """Return ``(idle_raw, loaded_raw, idle_per_sec, loaded_per_sec)`` for one spec.

    The per-second lists are empty for a substrate that carries no
    per-second form (pcount).
    """
    extract = spec.extractor
    idle_raw = [extract(r) for r in rows if r.arm == "idle"]
    loaded_raw = [extract(r) for r in rows if r.arm == "loaded"]
    if spec.has_per_sec:
        idle_per_sec = [per_wall_sec(extract(r), r) for r in rows if r.arm == "idle"]
        loaded_per_sec = [per_wall_sec(extract(r), r) for r in rows if r.arm == "loaded"]
    else:
        idle_per_sec = []
        loaded_per_sec = []
    return idle_raw, loaded_raw, idle_per_sec, loaded_per_sec


def _maybe_schedstat_unavailable(
    reason: str | None,
    *,
    idle: list[int],
    loaded: list[int],
    schedstat_kernel_available: bool,
    override_existing: bool,
) -> str | None:
    """Route a Mann-Whitney reason to the unavailable sentinel when degenerate.

    A kernel without CONFIG_SCHEDSTATS=y returns all-zero samples; the
    test is statistically degenerate so a misleading p=1.0 "no
    perturbation" must be replaced with the unavailable sentinel. The
    diff-test path (``override_existing=True``) replaces even a
    pre-existing marginal reason (e.g. ``both_arms_all_tied``) on the
    all-zero condition, mirroring the prior unconditional assignment.
    The kernel-unavailable override always only fills an unset reason.
    """
    if (override_existing or reason is None) and not any(idle) and not any(loaded):
        return "schedstat_substrate_unavailable"
    if reason is None and not schedstat_kernel_available:
        return "schedstat_substrate_unavailable"
    return reason


def _compute_one_substrate(
    spec: _SubstrateSpec,
    rows: list[WindowRow],
    *,
    schedstat_kernel_available: bool,
    per_wall_sec: Callable[[int, WindowRow], int],
) -> _SubstrateResult:
    """Compute every statistic for one substrate spec into a ``_SubstrateResult``.

    Extracts the per-arm raw (+ per-second) samples, runs the
    Mann-Whitney diff test on each form (with the schedstat-availability
    sentinel override where the spec flags it), derives the H0-rejection
    flags, computes the medians, and (where the spec carries bootstrap
    CIs) the BCa per-arm CIs and Hodges-Lehmann diff CI.
    """
    idle_raw, loaded_raw, idle_per_sec, loaded_per_sec = _substrate_samples(spec, rows, per_wall_sec)

    u_raw, p_raw, inapp_raw = _compute_mann_whitney(idle_raw, loaded_raw)
    if spec.has_per_sec:
        u_per_sec, p_per_sec, inapp_per_sec = _compute_mann_whitney(idle_per_sec, loaded_per_sec)
    else:
        u_per_sec, p_per_sec, inapp_per_sec = 0.0, 1.0, None

    if spec.schedstat_availability_override:
        inapp_raw = _maybe_schedstat_unavailable(
            inapp_raw,
            idle=idle_raw,
            loaded=loaded_raw,
            schedstat_kernel_available=schedstat_kernel_available,
            override_existing=True,
        )
        if spec.has_per_sec:
            inapp_per_sec = _maybe_schedstat_unavailable(
                inapp_per_sec,
                idle=idle_per_sec,
                loaded=loaded_per_sec,
                schedstat_kernel_available=schedstat_kernel_available,
                override_existing=True,
            )

    h0_rejected_raw = inapp_raw is None and p_raw < _ALPHA_CORRECTED_DIFF
    h0_rejected_per_sec = inapp_per_sec is None and p_per_sec < _ALPHA_CORRECTED_DIFF

    # Medians: raw always; per_sec only for the rate substrates.
    median_idle_per_sec = _median_int(idle_per_sec) if spec.has_per_sec else 0
    median_loaded_per_sec = _median_int(loaded_per_sec) if spec.has_per_sec else 0

    # Bootstrap CIs on the primary form (per_sec for rate substrates, raw
    # for pcount). Substrates without CIs leave the (0,0,0) sentinels.
    if spec.has_bootstrap_ci:
        primary_idle = idle_per_sec if spec.has_per_sec else idle_raw
        primary_loaded = loaded_per_sec if spec.has_per_sec else loaded_raw
        _idle_pt, ci_idle_lo, ci_idle_hi = _median_with_ci(primary_idle)
        _loaded_pt, ci_loaded_lo, ci_loaded_hi = _median_with_ci(primary_loaded)
        diff_pt, diff_lo, diff_hi = _hodges_lehmann_diff(primary_idle, primary_loaded)
    else:
        ci_idle_lo = ci_idle_hi = ci_loaded_lo = ci_loaded_hi = 0
        diff_pt = diff_lo = diff_hi = 0

    return _SubstrateResult(
        name=spec.name,
        idle_raw=idle_raw,
        loaded_raw=loaded_raw,
        idle_per_sec=idle_per_sec,
        loaded_per_sec=loaded_per_sec,
        u_raw=u_raw,
        p_raw=p_raw,
        inapp_raw=inapp_raw,
        u_per_sec=u_per_sec,
        p_per_sec=p_per_sec,
        inapp_per_sec=inapp_per_sec,
        h0_rejected_raw=h0_rejected_raw,
        h0_rejected_per_sec=h0_rejected_per_sec,
        median_idle_raw=_median_int(idle_raw),
        median_loaded_raw=_median_int(loaded_raw),
        median_idle_per_sec=median_idle_per_sec,
        median_loaded_per_sec=median_loaded_per_sec,
        ci_idle_lo=ci_idle_lo,
        ci_idle_hi=ci_idle_hi,
        ci_loaded_lo=ci_loaded_lo,
        ci_loaded_hi=ci_loaded_hi,
        diff_pt=diff_pt,
        diff_lo=diff_lo,
        diff_hi=diff_hi,
    )


def _compute_substrate_results(
    *,
    accepted_utime: list[WindowRow],
    accepted_schedstat: list[WindowRow],
    schedstat_kernel_available: bool,
    per_wall_sec: Callable[[int, WindowRow], int],
) -> list[_SubstrateResult]:
    """Run the table-driven per-substrate statistics over the registry.

    Returns one ``_SubstrateResult`` per spec, in registry order. The CPU
    substrates (utime/stime) ride the utime-accepted rows; schedstat and
    pcount ride the schedstat-accepted rows (no per-substrate utime
    outlier filter applies to them).
    """
    results: list[_SubstrateResult] = []
    for spec in _SUBSTRATE_SPECS:
        rows = accepted_utime if spec.name in ("utime", "stime") else accepted_schedstat
        results.append(
            _compute_one_substrate(
                spec,
                rows,
                schedstat_kernel_available=schedstat_kernel_available,
                per_wall_sec=per_wall_sec,
            )
        )
    return results


class _VmrssResult(NamedTuple):
    """VmRSS aggregation: per-arm medians, leak slope/intercept, reason."""

    median_idle_kb: int
    median_loaded_kb: int
    leak_slope_kb_per_window: float
    leak_intercept_kb: float
    inapplicable_reason: str | None


def _compute_vmrss(windows: list[WindowRow]) -> _VmrssResult:
    """Aggregate the daemon VmRSS substrate (medians + loaded-arm leak OLS).

    The end-of-window snapshot is the primary cross-arm signal. The
    median is taken over EVERY non-rejected window in the arm (VmRSS is a
    memory-allocation footprint orthogonal to the CPU outlier filter -- a
    window rejected by the CPU-outlier filter still carries a valid
    VmRSS sample). The slope-intercept pair is a simple OLS over the
    loaded arm's end-of-window VmRSS against ordinal position; a positive
    slope is the leak proxy. Need at least 2 distinct samples for the
    slope to be defined; with fewer the slope is 0.0 and the intercept is
    the lone sample (or 0.0 when empty).
    """
    all_vmrss = [r for r in windows if not r.rejected]
    idle_vmrss = [r.daemon_vmrss_end_kb for r in all_vmrss if r.arm == "idle"]
    loaded_vmrss_end = [r.daemon_vmrss_end_kb for r in all_vmrss if r.arm == "loaded"]
    reason: str | None = None
    if not any(idle_vmrss) and not any(loaded_vmrss_end):
        reason = "vmrss_substrate_unavailable"
    if len(loaded_vmrss_end) >= 2 and any(loaded_vmrss_end):
        xs = list(range(len(loaded_vmrss_end)))
        ys = loaded_vmrss_end
        mx = sum(xs) / len(xs)
        my = sum(ys) / len(ys)
        denom = sum((x - mx) ** 2 for x in xs)
        if denom > 0:
            slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / denom
            intercept = my - slope * mx
        else:
            slope = 0.0
            intercept = my
    else:
        slope = 0.0
        intercept = float(loaded_vmrss_end[0]) if loaded_vmrss_end else 0.0
    return _VmrssResult(
        median_idle_kb=_median_int(idle_vmrss),
        median_loaded_kb=_median_int(loaded_vmrss_end),
        leak_slope_kb_per_window=slope,
        leak_intercept_kb=intercept,
        inapplicable_reason=reason,
    )


class _WindowPartition(NamedTuple):
    """Outcome of the per-window accept/reject pass.

    ``accepted_utime`` and ``accepted_schedstat`` are the per-substrate
    survivor lists fed to the Mann-Whitney pipeline; ``rejected_count`` is
    every window dropped by a shared filter (rejected flag /
    invariant_failed) OR the idle-arm utime outlier filter; the two
    ``outlier_filtered_*_count`` fields break out the idle/loaded split of
    the utime outlier rejections for the forensic reader.
    """

    accepted_utime: list[WindowRow]
    accepted_schedstat: list[WindowRow]
    rejected_count: int
    outlier_filtered_idle_count: int
    outlier_filtered_loaded_count: int


def _partition_windows(
    windows: list[WindowRow],
    *,
    outlier_threshold_ns: int,
    pilot_skipped: bool,
) -> _WindowPartition:
    """Split windows into per-substrate survivor lists + rejection tallies.

    utime path: the jiffie-floored outlier threshold applies ONLY to
    idle-arm rows. The threshold's intent is to reject windows where utime
    spiked due to OS-scheduler noise; on the idle arm an utime above the
    noise floor is a spike, and on the loaded arm an utime above the noise
    floor IS the signal we want to measure. Applying the threshold to the
    loaded arm (the prior design) systematically rejected the bench's own
    signal whenever loaded utime exceeded the jiffie floor -- the
    2026-06-03 N=50 run produced loaded_window_count of 2 (out of 50)
    because real loaded utime samples of 40-60 ms exceeded the 10 ms
    jiffie threshold. The asymmetric filter (idle-only) preserves the
    spike-rejection intent without collapsing the loaded arm.

    schedstat path: NO outlier filter. The schedstat substrate is
    nanosecond-resolution and the pilot phase runs BEFORE the swarm spawns
    -- pilot idle schedstat is genuinely zero, so a MAD-from-pilot
    threshold collapses to zero and rejects every post-spawn non-zero
    loaded sample. Mann-Whitney handles the distribution directly: it is
    rank-based and robust to tails, and removing the filter preserves the
    daemon's actual post-spawn background activity (the right baseline for
    the perturbation hypothesis). The schedstat threshold field on the
    verdict stays populated as the pilot's MAD-from-zero noise-floor
    characterization for forensic context only -- it is no longer applied
    as a filter.

    ``rejected_count`` counts any window that the utime filter rejected OR
    that hit a shared filter (rejected flag / invariant_failed).
    """
    accepted_utime: list[WindowRow] = []
    accepted_schedstat: list[WindowRow] = []
    rejected_count = 0
    outlier_filtered_idle_count = 0
    outlier_filtered_loaded_count = 0
    for row in windows:
        if row.rejected:
            rejected_count += 1
            continue
        if any(r.invariant_failed for r in row.swarm_rows):
            rejected_count += 1
            continue
        # utime: idle-arm-only outlier filter. This asymmetry is the
        # deliberate final design, not a stopgap. The idle arm is the
        # baseline we clean of host-noise spikes; the loaded arm's high
        # utime IS the measured signal and must be preserved. Filtering
        # the loaded arm against the pre-spawn pilot threshold would
        # over-reject legitimate loaded signal (loaded utime spikes
        # above one jiffie are the bench's measurement target, not
        # noise), so the filter applies to the idle arm only.
        if row.arm == "idle" and not pilot_skipped and row.daemon_utime_delta_ns > outlier_threshold_ns:
            rejected_count += 1
            outlier_filtered_idle_count += 1
            continue
        accepted_utime.append(row)
        # Schedstat path: only the shared filters above. No per-
        # substrate threshold for the reasons documented in the
        # docstring above.
        accepted_schedstat.append(row)
    return _WindowPartition(
        accepted_utime=accepted_utime,
        accepted_schedstat=accepted_schedstat,
        rejected_count=rejected_count,
        outlier_filtered_idle_count=outlier_filtered_idle_count,
        outlier_filtered_loaded_count=outlier_filtered_loaded_count,
    )


class _Aggregates(NamedTuple):
    """Cross-substrate roll-ups: detection flag, top-level label."""

    perturbation_detected: bool
    all_marginals_inapplicable: bool
    top_level_verdict: str


def _derive_aggregates(results: list[_SubstrateResult], *, pilot_skipped: bool) -> _Aggregates:
    """Roll the per-substrate results up into the top-level verdict label.

    ``perturbation_detected`` is the OR of every Mann-Whitney H0-rejection
    flag (both raw and per-second forms). ``all_marginals_inapplicable``
    is the AND of every Mann-Whitney inapplicable reason being set.
    """
    h0_flags: list[bool] = []
    inapp_flags: list[bool] = []
    for r in results:
        h0_flags.append(r.h0_rejected_raw)
        inapp_flags.append(r.inapp_raw is not None)
        if r.name != "pcount":
            h0_flags.append(r.h0_rejected_per_sec)
            inapp_flags.append(r.inapp_per_sec is not None)
    perturbation_detected = any(h0_flags)
    all_marginals_inapplicable = all(inapp_flags)

    if pilot_skipped:
        top_level_verdict = "inapplicable_pilot_skipped"
    elif all_marginals_inapplicable:
        top_level_verdict = "inapplicable_empty_arm"
    elif perturbation_detected:
        top_level_verdict = "perturbation_detected"
    else:
        top_level_verdict = "inconclusive"
    return _Aggregates(
        perturbation_detected=perturbation_detected,
        all_marginals_inapplicable=all_marginals_inapplicable,
        top_level_verdict=top_level_verdict,
    )


class SwarmAggregates(msgspec.Struct, frozen=True, kw_only=True):
    """Multi-producer / multi-subscriber swarm counters fed into the verdict.

    These nine fields travel together: they describe the producer/subscriber
    fan-out shape of one bench run (counts, the per-framework subscriber mix,
    attrition flags, and the producer emit/late/error totals). Grouping them
    keeps ``_build_verdict``'s signature free of the swarm-counter data clump
    that previously coupled every call site to all nine names. The defaults
    reproduce the prior keyword defaults so a call site that omits the group
    (the network-free helper tests) sees an empty/zero swarm.
    """

    producer_count: int = 0
    producer_event_rate_hz: float = 0.0
    subscriber_agent_count: int = 0
    subscriber_framework_mix: dict[str, int] | None = None
    producer_attrition_detected: bool = False
    subscriber_attrition_detected: bool = False
    producer_emit_count_total: int = 0
    producer_late_count_total: int = 0
    producer_error_count_total: int = 0


class CostSummary(msgspec.Struct, frozen=True, kw_only=True):
    """Per-run cost accounting fed into the verdict.

    The realized total (``None`` when no priced driver ran), the count of
    rows whose cost could not be priced, and the budget gate (configured
    ceiling, observed peak, and whether the run aborted on it). These five
    fields are a single cohesive cost-accounting clump; grouping them
    decouples each call site from the individual field names.
    """

    cost_usd_total: float | None
    cost_unknown_count: int
    max_cost_usd_budget: float
    max_cost_usd_observed: float
    aborted_on_budget: bool


# Shared empty-swarm default for ``_build_verdict``. A frozen struct is
# immutable, so a single module-level instance is safe to share as the
# parameter default (and keeps the call signature free of a default-arg
# construction call).
_EMPTY_SWARM = SwarmAggregates()


def _post_hoc_mde_ms(idle_utimes_per_sec: list[int], loaded_utimes_per_sec: list[int]) -> float:
    """Post-hoc approx MDE on the per-sec utime substrate, in ms/sec.

    Power floor of ``_MIN_DETECTABLE_EFFECT_SIGMA`` times the POOLED per-arm
    sample SD, converted from ns/sec to ms/sec. Single-armed SD (the prior
    implementation) under-estimated MDE on a daemon whose loaded-arm
    variance differs from idle by the wall-stretch factor. Pooled SD is the
    right input for a Mann-Whitney location-shift power calculation. The a
    priori MDE via Monte-Carlo lives downstream on a separate verdict field.
    """
    pooled = (idle_utimes_per_sec or []) + (loaded_utimes_per_sec or [])
    if pooled:
        pooled_mean = sum(pooled) / len(pooled)
        pooled_var = sum((x - pooled_mean) ** 2 for x in pooled) / max(1, len(pooled) - 1)
        sample_sd = float(pooled_var**0.5)
    else:
        sample_sd = 0.0
    return _MIN_DETECTABLE_EFFECT_SIGMA * sample_sd / 1_000_000.0


def _per_iter_source_distribution(windows: list[WindowRow]) -> dict[str, int]:
    """Count the picked seed-source per loaded window (idle windows ignored).

    Mirrors the workload's ``picked_source`` tally so the verdict carries
    the per-source iteration distribution without re-deriving it from the
    iter_id. Windows on the idle arm and loaded windows with no recorded
    source are skipped.
    """
    distribution: dict[str, int] = {}
    for window in windows:
        if window.arm != "loaded" or not window.picked_source:
            continue
        name = window.picked_source
        distribution[name] = distribution.get(name, 0) + 1
    return distribution


def _build_verdict(
    *,
    started_ns: int,
    finished_ns: int,
    env_report: EnvironmentReport,
    external_state: ExternalStateReport,
    windows: list[WindowRow],
    outlier_threshold_ns: int,
    outlier_threshold_schedstat_ns: int,
    n_per_arm: int,
    cost: CostSummary,
    swarm: SwarmAggregates = _EMPTY_SWARM,
    schedstat_kernel_available: bool = True,
    pilot_skipped: bool = False,
    pilot_skipped_reason: str | None = None,
) -> ExperimentBVerdict:
    """Aggregate per-window samples into the top-level verdict.

    Applies the outlier filter (windows above the pilot threshold are
    excluded from the U statistic); records both the median per arm and
    the rejected-window count so a downstream reader can audit the gate.

    When ``pilot_skipped`` is True the outlier-threshold check is
    bypassed: the sentinel threshold (0 ns) would otherwise reject every
    window with non-zero utime. The pilot-skip semantic is documented on
    the bench's module docstring.
    """
    # Per-window accept/reject pass. The idle-arm-only utime outlier
    # filter, the schedstat no-filter rationale, and the rejection-count
    # semantics all live on ``_partition_windows``.
    partition = _partition_windows(
        windows,
        outlier_threshold_ns=outlier_threshold_ns,
        pilot_skipped=pilot_skipped,
    )
    accepted_utime = partition.accepted_utime
    accepted_schedstat = partition.accepted_schedstat
    rejected_count = partition.rejected_count
    outlier_filtered_idle_count = partition.outlier_filtered_idle_count
    outlier_filtered_loaded_count = partition.outlier_filtered_loaded_count

    # Per-wall-second normalization. Loaded windows run the in-proc
    # LLM workload thread which can stretch wall-time well beyond the
    # idle arm's nominal ``_WINDOW_DURATION_SEC`` (idle ~1 s; loaded
    # observed up to ~30 s under real-LLM load). Comparing raw
    # ``daemon_utime_delta_ns`` would mix the per-sample unit -- a
    # loaded window's larger raw CPU could reflect MORE wall time
    # rather than higher per-second CPU rate. Normalising the input
    # to ``CPU ns per wall second`` keeps the Mann-Whitney U on a
    # single unit. The raw deltas survive on ``WindowRow`` for the
    # forensic reader; only the U-statistic input is normalised.
    def _per_wall_sec(value_ns: int, row: WindowRow) -> int:
        wall_ns = row.t_window_end_ns - row.t_window_start_ns
        if wall_ns <= 0:
            return 0
        return int(value_ns * 1_000_000_000 / wall_ns)

    # Per-substrate statistics via the table-driven registry. Each
    # ``_SubstrateResult`` carries the raw + per-second sample lists, the
    # Mann-Whitney diff test on both forms (with the schedstat-
    # availability sentinel override applied), the H0-rejection flags,
    # the medians, and the bootstrap CIs. See ``_SUBSTRATE_SPECS`` for
    # the per-substrate flag semantics (pcount has no per-second form;
    # stime carries no CI/median verdict fields).
    substrate_results = _compute_substrate_results(
        accepted_utime=accepted_utime,
        accepted_schedstat=accepted_schedstat,
        schedstat_kernel_available=schedstat_kernel_available,
        per_wall_sec=_per_wall_sec,
    )
    by_name = {r.name: r for r in substrate_results}
    res_utime = by_name["utime"]
    res_stime = by_name["stime"]
    res_sched = by_name["schedstat"]
    res_pcount = by_name["pcount"]

    # The downstream assembler indexes per-substrate fields off these
    # named bindings (kept identical to the prior hand-expanded names so
    # the struct constructor below is byte-for-byte unchanged).
    idle_utimes_per_sec = res_utime.idle_per_sec
    loaded_utimes_per_sec = res_utime.loaded_per_sec

    u_utime_ps, p_utime_ps, inapp_utime_ps = res_utime.u_per_sec, res_utime.p_per_sec, res_utime.inapp_per_sec
    u_utime_raw, p_utime_raw, inapp_utime_raw = res_utime.u_raw, res_utime.p_raw, res_utime.inapp_raw
    u_stime_ps, p_stime_ps, inapp_stime_ps = res_stime.u_per_sec, res_stime.p_per_sec, res_stime.inapp_per_sec
    u_stime_raw, p_stime_raw, inapp_stime_raw = res_stime.u_raw, res_stime.p_raw, res_stime.inapp_raw
    u_sched_ps, p_sched_ps, inapp_sched_ps = res_sched.u_per_sec, res_sched.p_per_sec, res_sched.inapp_per_sec
    u_sched_raw, p_sched_raw, inapp_sched_raw = res_sched.u_raw, res_sched.p_raw, res_sched.inapp_raw
    u_pcount, p_pcount, inapp_pcount = res_pcount.u_raw, res_pcount.p_raw, res_pcount.inapp_raw

    h0_rejected_utime_ps = res_utime.h0_rejected_per_sec
    h0_rejected_utime_raw = res_utime.h0_rejected_raw
    h0_rejected_stime_ps = res_stime.h0_rejected_per_sec
    h0_rejected_stime_raw = res_stime.h0_rejected_raw
    h0_rejected_sched_ps = res_sched.h0_rejected_per_sec
    h0_rejected_sched_raw = res_sched.h0_rejected_raw
    h0_rejected_pcount = res_pcount.h0_rejected_raw

    # Cross-substrate roll-ups (detection flag, top-level label) derived
    # from the per-substrate results.
    aggregates = _derive_aggregates(substrate_results, pilot_skipped=pilot_skipped)
    perturbation_detected = aggregates.perturbation_detected
    top_level_verdict = aggregates.top_level_verdict

    # Medians + BCa bootstrap CIs are carried on each ``_SubstrateResult``
    # (raw + per-sec medians; CIs on the primary form). Rebind to the
    # historical names so the struct constructor below is unchanged.
    median_idle_utime = res_utime.median_idle_raw
    median_loaded_utime = res_utime.median_loaded_raw
    median_idle_utime_per_sec = res_utime.median_idle_per_sec
    median_loaded_utime_per_sec = res_utime.median_loaded_per_sec
    median_idle_schedstat = res_sched.median_idle_raw
    median_loaded_schedstat = res_sched.median_loaded_raw
    median_idle_schedstat_per_sec = res_sched.median_idle_per_sec
    median_loaded_schedstat_per_sec = res_sched.median_loaded_per_sec
    median_idle_pcount = res_pcount.median_idle_raw
    median_loaded_pcount = res_pcount.median_loaded_raw

    ut_idle_ci_lo, ut_idle_ci_hi = res_utime.ci_idle_lo, res_utime.ci_idle_hi
    ut_loaded_ci_lo, ut_loaded_ci_hi = res_utime.ci_loaded_lo, res_utime.ci_loaded_hi
    ut_diff_pt, ut_diff_lo, ut_diff_hi = res_utime.diff_pt, res_utime.diff_lo, res_utime.diff_hi
    sc_idle_ci_lo, sc_idle_ci_hi = res_sched.ci_idle_lo, res_sched.ci_idle_hi
    sc_loaded_ci_lo, sc_loaded_ci_hi = res_sched.ci_loaded_lo, res_sched.ci_loaded_hi
    sc_diff_pt, sc_diff_lo, sc_diff_hi = res_sched.diff_pt, res_sched.diff_lo, res_sched.diff_hi
    pc_idle_ci_lo, pc_idle_ci_hi = res_pcount.ci_idle_lo, res_pcount.ci_idle_hi
    pc_loaded_ci_lo, pc_loaded_ci_hi = res_pcount.ci_loaded_lo, res_pcount.ci_loaded_hi
    pc_diff_pt, pc_diff_lo, pc_diff_hi = res_pcount.diff_pt, res_pcount.diff_lo, res_pcount.diff_hi

    # VmRSS aggregation (per-arm medians + loaded-arm leak-slope OLS).
    vmrss = _compute_vmrss(windows)
    median_idle_vmrss = vmrss.median_idle_kb
    median_loaded_vmrss = vmrss.median_loaded_kb
    vmrss_leak_slope_kb_per_window = vmrss.leak_slope_kb_per_window
    vmrss_leak_intercept_kb = vmrss.leak_intercept_kb
    vmrss_substrate_inapplicable_reason = vmrss.inapplicable_reason

    # Post-hoc approx MDE on the per-sec utime substrate (the primary
    # rate-form U-statistic input). See ``_post_hoc_mde_ms``.
    mde_ms = _post_hoc_mde_ms(idle_utimes_per_sec, loaded_utimes_per_sec)

    # A priori MDE via Monte-Carlo: resample idle utime per-sec and
    # search the shift grid for the smallest perturbation the bench
    # could detect at target power. Counterpart to the post-hoc
    # sigma-based MDE -- decouples claim from realized data.
    mde_apriori_ns, mde_apriori_achieved = _compute_mde_apriori(
        idle_utimes_per_sec,
        alpha=_ALPHA_CORRECTED_DIFF,
        n_per_arm=max(len(idle_utimes_per_sec), len(loaded_utimes_per_sec)),
    )

    gil_gaps = [r.wall_minus_thread_time_ns for r in windows]
    mean_gil_gap = int(sum(gil_gaps) / max(1, len(gil_gaps)))

    events_per_loaded = [len(r.swarm_rows) for r in windows if r.arm == "loaded"]

    per_iter_source_distribution = _per_iter_source_distribution(windows)

    limitations = [
        f"Mann-Whitney detection threshold at n={n_per_arm}/side is "
        f"{_MIN_DETECTABLE_EFFECT_SIGMA} sigma; smaller perturbations not detectable here",
        "Cross-process timing replaced with in-process workload thread; "
        "GIL contention may attenuate detectable perturbation; "
        "wall_minus_thread_time_ns reports per-window GIL gap",
        "asyncio scheduling jitter present; perturbation must exceed scheduler noise floor to register",
        f"{_GEMINI_MODEL} alias is floating; observed model id recorded but not pinned",
        "Anthropic prompt cache 5-min decay defeated by per-iteration prefix; if iteration "
        "wall-clock exceeds 5 min, cache state may degrade",
        "claude/gemini CLIs expose no --seed/--temperature; sampling is black-box; distribution-level claims only",
        "OPENAI_API_KEY presence recorded as bool; key value never persisted",
        "Per-event granularity locked at one event per producer per iteration (NOT per token); "
        "verified from claude CLI empirical probe",
        f"p99 latency CI half-width at n={n_per_arm} ~ 5x median CI; p99 NOT used for driver ranking",
    ]

    return ExperimentBVerdict(
        bench_name=_BENCH_NAME,
        started_ns=started_ns,
        finished_ns=finished_ns,
        environment=env_report,
        external_state=external_state,
        windows=windows,
        n_per_arm=n_per_arm,
        idle_window_count=len(idle_utimes_per_sec),
        loaded_window_count=len(loaded_utimes_per_sec),
        producer_count=swarm.producer_count,
        producer_event_rate_hz=swarm.producer_event_rate_hz,
        subscriber_agent_count=swarm.subscriber_agent_count,
        subscriber_framework_mix=dict(swarm.subscriber_framework_mix or {}),
        producer_attrition_detected=swarm.producer_attrition_detected,
        subscriber_attrition_detected=swarm.subscriber_attrition_detected,
        producer_emit_count_total=swarm.producer_emit_count_total,
        producer_late_count_total=swarm.producer_late_count_total,
        producer_error_count_total=swarm.producer_error_count_total,
        alpha_corrected_diff=_ALPHA_CORRECTED_DIFF,
        bonferroni_k_diff=_BONFERRONI_K_DIFF,
        median_idle_utime_per_sec_ci_low_ns=ut_idle_ci_lo,
        median_idle_utime_per_sec_ci_high_ns=ut_idle_ci_hi,
        median_loaded_utime_per_sec_ci_low_ns=ut_loaded_ci_lo,
        median_loaded_utime_per_sec_ci_high_ns=ut_loaded_ci_hi,
        median_diff_utime_per_sec_ns=ut_diff_pt,
        median_diff_utime_per_sec_ci_low_ns=ut_diff_lo,
        median_diff_utime_per_sec_ci_high_ns=ut_diff_hi,
        median_idle_schedstat_per_sec_ci_low_ns=sc_idle_ci_lo,
        median_idle_schedstat_per_sec_ci_high_ns=sc_idle_ci_hi,
        median_loaded_schedstat_per_sec_ci_low_ns=sc_loaded_ci_lo,
        median_loaded_schedstat_per_sec_ci_high_ns=sc_loaded_ci_hi,
        median_diff_schedstat_per_sec_ns=sc_diff_pt,
        median_diff_schedstat_per_sec_ci_low_ns=sc_diff_lo,
        median_diff_schedstat_per_sec_ci_high_ns=sc_diff_hi,
        median_idle_pcount_ci_low=pc_idle_ci_lo,
        median_idle_pcount_ci_high=pc_idle_ci_hi,
        median_loaded_pcount_ci_low=pc_loaded_ci_lo,
        median_loaded_pcount_ci_high=pc_loaded_ci_hi,
        median_diff_pcount=pc_diff_pt,
        median_diff_pcount_ci_low=pc_diff_lo,
        median_diff_pcount_ci_high=pc_diff_hi,
        bootstrap_n_resamples=_BOOTSTRAP_N_RESAMPLES,
        bootstrap_method=_BOOTSTRAP_METHOD,
        bootstrap_confidence_level=_BOOTSTRAP_CONFIDENCE,
        mde_apriori_utime_per_sec_ns=mde_apriori_ns,
        mde_apriori_target_power=_MDE_TARGET_POWER,
        mde_apriori_achieved_power=mde_apriori_achieved,
        mde_apriori_n_per_arm=max(len(idle_utimes_per_sec), len(loaded_utimes_per_sec)),
        mde_apriori_n_resamples=_MDE_N_RESAMPLES,
        outlier_filtered_idle_count=outlier_filtered_idle_count,
        outlier_filtered_loaded_count=outlier_filtered_loaded_count,
        mann_whitney_u_utime_per_sec=u_utime_ps,
        mann_whitney_p_utime_per_sec=p_utime_ps,
        mann_whitney_u_utime_raw=u_utime_raw,
        mann_whitney_p_utime_raw=p_utime_raw,
        mann_whitney_u_stime_per_sec=u_stime_ps,
        mann_whitney_p_stime_per_sec=p_stime_ps,
        mann_whitney_u_stime_raw=u_stime_raw,
        mann_whitney_p_stime_raw=p_stime_raw,
        mann_whitney_u_schedstat_per_sec=u_sched_ps,
        mann_whitney_p_schedstat_per_sec=p_sched_ps,
        mann_whitney_u_schedstat_raw=u_sched_raw,
        mann_whitney_p_schedstat_raw=p_sched_raw,
        mann_whitney_u_pcount=u_pcount,
        mann_whitney_p_pcount=p_pcount,
        mann_whitney_inapplicable_reason_utime_per_sec=inapp_utime_ps,
        mann_whitney_inapplicable_reason_utime_raw=inapp_utime_raw,
        mann_whitney_inapplicable_reason_stime_per_sec=inapp_stime_ps,
        mann_whitney_inapplicable_reason_stime_raw=inapp_stime_raw,
        mann_whitney_inapplicable_reason_schedstat_per_sec=inapp_sched_ps,
        mann_whitney_inapplicable_reason_schedstat_raw=inapp_sched_raw,
        mann_whitney_inapplicable_reason_pcount=inapp_pcount,
        h0_rejected_utime_per_sec=h0_rejected_utime_ps,
        h0_rejected_utime_raw=h0_rejected_utime_raw,
        h0_rejected_stime_per_sec=h0_rejected_stime_ps,
        h0_rejected_stime_raw=h0_rejected_stime_raw,
        h0_rejected_schedstat_per_sec=h0_rejected_sched_ps,
        h0_rejected_schedstat_raw=h0_rejected_sched_raw,
        h0_rejected_pcount=h0_rejected_pcount,
        verdict=top_level_verdict,
        schedstat_kernel_available=schedstat_kernel_available,
        daemon_sample_jiffie_ns=_PILOT_THRESHOLD_FLOOR_NS,
        min_detectable_effect_sigma=_MIN_DETECTABLE_EFFECT_SIGMA,
        min_detectable_effect_ms=mde_ms,
        median_idle_utime_ns=median_idle_utime,
        median_loaded_utime_ns=median_loaded_utime,
        median_idle_utime_per_sec_ns=median_idle_utime_per_sec,
        median_loaded_utime_per_sec_ns=median_loaded_utime_per_sec,
        median_idle_schedstat_ns=median_idle_schedstat,
        median_loaded_schedstat_ns=median_loaded_schedstat,
        median_idle_schedstat_per_sec_ns=median_idle_schedstat_per_sec,
        median_loaded_schedstat_per_sec_ns=median_loaded_schedstat_per_sec,
        median_idle_pcount=median_idle_pcount,
        median_loaded_pcount=median_loaded_pcount,
        median_idle_vmrss_kb=median_idle_vmrss,
        median_loaded_vmrss_kb=median_loaded_vmrss,
        vmrss_leak_slope_kb_per_window=vmrss_leak_slope_kb_per_window,
        vmrss_leak_intercept_kb=vmrss_leak_intercept_kb,
        vmrss_substrate_inapplicable_reason=vmrss_substrate_inapplicable_reason,
        outlier_threshold_ns=outlier_threshold_ns,
        outlier_threshold_schedstat_ns=outlier_threshold_schedstat_ns,
        rejected_window_count=rejected_count,
        perturbation_detected=perturbation_detected,
        mean_gil_gap_ns=mean_gil_gap,
        events_per_loaded_window=events_per_loaded,
        cost_usd_total=cost.cost_usd_total,
        cost_unknown_count=cost.cost_unknown_count,
        cache_contaminated_count=count_cache_contaminated_rows(
            [swarm_row for window in windows for swarm_row in window.swarm_rows]
        ),
        max_cost_usd_budget=cost.max_cost_usd_budget,
        max_cost_usd_observed=cost.max_cost_usd_observed,
        aborted_on_budget=cost.aborted_on_budget,
        limitations=limitations,
        pilot_skipped=pilot_skipped,
        pilot_skipped_reason=pilot_skipped_reason,
        per_iter_source_distribution=per_iter_source_distribution,
    )


class _DaemonHandle(NamedTuple):
    """The spawned-and-pinned daemon plus the paths the bench drives it through."""

    daemon_proc: subprocess.Popen[bytes]
    daemon_pid: int
    daemon_env: dict[str, str]
    socket_path: Path
    doorbell_path: Path
    db_path: Path
    env_report: EnvironmentReport


def _spawn_pinned_daemon(
    *,
    tmp_dir: Path,
    daemon_cores: set[int] | None,
    env_report: EnvironmentReport,
) -> _DaemonHandle:
    """Spawn ``waitbus broadcast serve`` in an isolated tmp tree, optionally core-pinned.

    Creates the state / runtime dirs under ``tmp_dir``, builds the daemon
    environment (isolated state + runtime dirs, heartbeat parked), and
    launches the daemon. When ``daemon_cores`` is set, the daemon is
    pinned via ``preexec_fn`` BEFORE its main thread starts so the first
    window's samples are not contaminated by a post-fork affinity race,
    and the pin is verified against what the daemon actually inherited.
    The returned ``env_report`` carries the verified ``daemon_taskset_mask``
    when a pin was requested; otherwise it is the caller's report unchanged.
    """
    state_dir = tmp_dir / "state"
    runtime_dir = tmp_dir / "runtime"
    state_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    # Spawn waitbus broadcast serve directly (the bench's harness wrapper
    # uses ``uv run`` which adds startup latency; the direct ``waitbus``
    # binary keeps the daemon-spawn deterministic in measurement scope).
    waitbus_path = shutil.which("waitbus") or "waitbus"
    daemon_env = os.environ.copy()
    daemon_env["WAITBUS_STATE_DIR"] = str(state_dir)
    daemon_env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
    daemon_env["WAITBUS_HEARTBEAT_SEC"] = "3600"
    socket_path = runtime_dir / "broadcast.sock"
    doorbell_path = runtime_dir / "doorbell.sock"
    db_path = state_dir / "github.db"
    # ``preexec_fn`` pins the daemon BEFORE its main thread starts;
    # post-fork ``sched_setaffinity(daemon.pid, ...)`` would race
    # against the daemon's startup work and contaminate the first
    # window's samples.
    daemon_preexec_fn: Callable[[], None] | None = None
    if daemon_cores is not None:
        pinned_cores = frozenset(daemon_cores)

        def _pin_daemon_to_cores() -> None:
            os.sched_setaffinity(0, pinned_cores)

        daemon_preexec_fn = _pin_daemon_to_cores
    daemon_proc = subprocess.Popen(
        [waitbus_path, "broadcast", "serve"],
        env=daemon_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        preexec_fn=daemon_preexec_fn,
    )
    daemon_pid = daemon_proc.pid

    # Verify the pin actually took -- a ``preexec_fn`` can silently
    # fail when the running cgroup does not permit the requested
    # core set. Record what the daemon ACTUALLY inherited (not what
    # the orchestrator intended) on the env report.
    if daemon_cores is not None:
        actual_affinity = os.sched_getaffinity(daemon_pid)
        if actual_affinity != daemon_cores:
            daemon_proc.terminate()
            raise PreflightError(
                f"preflight: daemon affinity pin failed; expected "
                f"{sorted(daemon_cores)} got {sorted(actual_affinity)}. "
                "Re-check cgroup permissions or pass --allow-unpinned-for-dev."
            )
        env_report = msgspec.structs.replace(
            env_report,
            daemon_taskset_mask=",".join(str(c) for c in sorted(actual_affinity)),
        )

    return _DaemonHandle(
        daemon_proc=daemon_proc,
        daemon_pid=daemon_pid,
        daemon_env=daemon_env,
        socket_path=socket_path,
        doorbell_path=doorbell_path,
        db_path=db_path,
        env_report=env_report,
    )


class _OutlierThresholds(NamedTuple):
    """Pilot outcome: whether the pilot ran and the resulting per-substrate thresholds."""

    pilot_skipped: bool
    pilot_skipped_reason: str | None
    outlier_threshold_ns: int
    outlier_threshold_schedstat_ns: int


def _compute_outlier_thresholds(
    *,
    daemon_pid: int,
    smoke: bool,
    include_real_llm: bool,
    progress_fh: Any,
) -> _OutlierThresholds:
    """Run (or skip) the idle pilot and return the out-of-band outlier thresholds.

    The pilot is skipped when the bench's downstream Mann-Whitney U test
    is structurally inapplicable: smoke mode runs n=5/arm (below the
    documented n=50/arm power floor), and --skip-real-llm collapses the
    loaded arm to shell-control only so the loaded-vs-idle comparison
    degrades to identity-vs-identity. In either case the outlier threshold
    has no consumer worth its 10-second cost and both thresholds are 0.
    """
    pilot_skipped = smoke or not include_real_llm
    if pilot_skipped:
        pilot_skipped_reason: str | None = "smoke_mode" if smoke else "real_llm_disabled"
        outlier_threshold_ns = 0
        outlier_threshold_schedstat_ns = 0
        structured(
            _logger,
            logging.INFO,
            "bench_pilot_skipped",
            reason=pilot_skipped_reason,
        )
        append_jsonl_record(
            progress_fh,
            {"kind": "pilot_skipped", "reason": pilot_skipped_reason},
        )
    else:
        pilot_skipped_reason = None
        pilot_thresholds = _pilot_outlier_threshold(daemon_pid)
        outlier_threshold_ns = pilot_thresholds.utime_ns
        outlier_threshold_schedstat_ns = pilot_thresholds.schedstat_ns
        append_jsonl_record(
            progress_fh,
            {
                "kind": "outlier_pilot",
                "threshold_ns": outlier_threshold_ns,
                "threshold_schedstat_ns": outlier_threshold_schedstat_ns,
            },
        )
    return _OutlierThresholds(
        pilot_skipped=pilot_skipped,
        pilot_skipped_reason=pilot_skipped_reason,
        outlier_threshold_ns=outlier_threshold_ns,
        outlier_threshold_schedstat_ns=outlier_threshold_schedstat_ns,
    )


class _WindowLoopResult(NamedTuple):
    """Everything the alternation loop accumulates for the verdict builder."""

    windows: list[WindowRow]
    external_state_report: ExternalStateReport
    cost_usd_total: float | None
    cost_unknown_count: int
    max_cost_usd_observed: float
    aborted_on_budget: bool
    producer_attrition_detected: bool
    subscriber_attrition_detected: bool
    producer_emit_count_total: int
    producer_late_count_total: int
    producer_error_count_total: int
    subscriber_framework_mix: dict[str, int]


def _run_window_loop(
    *,
    daemon_pid: int,
    loaded_runner: Any,
    n_per_arm: int,
    max_cost_usd: float,
    external_state_report: ExternalStateReport,
    progress_fh: Any,
) -> _WindowLoopResult:
    """Alternate idle / loaded windows, accumulating per-window aggregates.

    Builds a PYTHONHASHSEED-seeded idle/loaded alternation order, then for
    each window measures the daemon's CPU under the arm-appropriate runner.
    Loaded windows additionally fold their producer / subscriber counters
    and per-row cost into running totals, abort early when the cost-budget
    circuit breaker trips, and merge observed OpenAI model ids onto the
    external-state report. Returns the captured windows plus every total
    the verdict builder needs.
    """
    # Build the alternation order. Seed from PYTHONHASHSEED so the
    # order is deterministic across re-runs of the same bench.
    rng = random.Random(_hashseed_or_default())
    arms = ["idle"] * n_per_arm + ["loaded"] * n_per_arm
    rng.shuffle(arms)

    windows: list[WindowRow] = []
    budget_tracker = CostBudgetTracker(max_usd=max_cost_usd)
    aborted_on_budget = False
    # Per-window producer + subscriber aggregates. The producer
    # swarm + LLM-agent pool are built per loaded window inside
    # ``_make_loaded_runner``; we accumulate their counters and
    # attrition flags into the verdict's top-level sums.
    producer_attrition_detected = False
    subscriber_attrition_detected = False
    producer_emit_count_total = 0
    producer_late_count_total = 0
    producer_error_count_total = 0
    subscriber_framework_mix_acc: dict[str, int] = {}

    for window_id, arm in enumerate(arms):
        if arm == "loaded":
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
        runner = loaded_runner if arm == "loaded" else _idle_runner
        row, workload_result = _measure_window(
            daemon_pid=daemon_pid,
            arm=arm,
            window_id=window_id,
            workload_runner=runner,
        )
        windows.append(row)
        if arm == "loaded":
            producer_emit_count_total += workload_result.producer_emit_count
            producer_late_count_total += workload_result.producer_late_count
            producer_error_count_total += workload_result.producer_error_count
            if workload_result.producer_attrition:
                producer_attrition_detected = True
            if workload_result.subscriber_attrition:
                subscriber_attrition_detected = True
            for framework, count in workload_result.subscriber_framework_mix.items():
                subscriber_framework_mix_acc[framework] = subscriber_framework_mix_acc.get(framework, 0) + count
            for swarm_row in row.swarm_rows:
                _record_row_cost(budget_tracker, swarm_row)
        merged_openai_models = external_state_report.openai_response_model_set
        for r in row.swarm_rows:
            if r.openai_env is not None:
                merged_openai_models = merge_observed_models(merged_openai_models, r.openai_env.model)
        external_state_report = msgspec.structs.replace(
            external_state_report,
            openai_response_model_set=merged_openai_models,
            agent_tool_call_count_per_iter=external_state_report.agent_tool_call_count_per_iter
            + ([len(row.swarm_rows)] if row.arm == "loaded" else []),
        )
        any_invariant_failed = any(r.invariant_failed for r in row.swarm_rows)
        append_jsonl_record(
            progress_fh,
            {
                "kind": "window",
                "window_id": window_id,
                "arm": arm,
                "utime_ns": row.daemon_utime_delta_ns,
                "stime_ns": row.daemon_stime_delta_ns,
                "schedstat_run_ns": row.daemon_schedstat_run_delta_ns,
                "schedstat_wait_ns": row.daemon_schedstat_wait_delta_ns,
                "schedstat_pcount": row.daemon_schedstat_pcount_delta,
                "schedstat_tid_count": row.daemon_schedstat_tid_count_end,
                "vmrss_end_kb": row.daemon_vmrss_end_kb,
                "events": len(row.swarm_rows),
                "rejected": row.rejected,
                "invariant_failed": any_invariant_failed,
                "producer_emits": workload_result.producer_emit_count if arm == "loaded" else 0,
            },
        )

    return _WindowLoopResult(
        windows=windows,
        external_state_report=external_state_report,
        # The budget tracker is the canonical source for both observed
        # spend AND the unknown-cost-call count (gemini free-tier path
        # increments unknown_usd_call_count). The verdict reads them
        # directly rather than tracking dead locals.
        cost_usd_total=budget_tracker.observed_usd,
        cost_unknown_count=budget_tracker.unknown_usd_call_count,
        max_cost_usd_observed=budget_tracker.observed_usd,
        aborted_on_budget=aborted_on_budget,
        producer_attrition_detected=producer_attrition_detected,
        subscriber_attrition_detected=subscriber_attrition_detected,
        producer_emit_count_total=producer_emit_count_total,
        producer_late_count_total=producer_late_count_total,
        producer_error_count_total=producer_error_count_total,
        subscriber_framework_mix=subscriber_framework_mix_acc,
    )


def _terminate_daemon_with_grace(
    proc: subprocess.Popen[bytes],
    *,
    term_sec: float = 5.0,
    kill_sec: float = 2.0,
) -> Literal["reaped", "zombie_after_sigkill"]:
    """Tear down the daemon process and RETURN the post-kill outcome.

    Runs the standard grace dance: ``terminate()`` then ``wait(term_sec)``;
    on ``TimeoutExpired`` it escalates to ``kill()`` then ``wait(kill_sec)``.
    The FINAL post-SIGKILL wait does NOT swallow its timeout: a
    daemon that survives SIGKILL (a genuine zombie / kernel-stuck process)
    is reported back as ``"zombie_after_sigkill"`` so the caller can surface
    it, rather than being silently absorbed by a suppress. A clean exit at
    either wait returns ``"reaped"``.

    ``ProcessLookupError`` / ``OSError`` raised by ``terminate()`` or
    ``kill()`` on an already-dead pid are benign (the process is gone) and
    map to ``"reaped"``.
    """
    try:
        proc.terminate()
    except (OSError, ProcessLookupError):
        # Already dead -- terminate on a reaped pid raises; nothing to wait on.
        return "reaped"
    try:
        proc.wait(timeout=term_sec)
        return "reaped"
    except subprocess.TimeoutExpired:
        pass
    # SIGTERM did not land within the grace window; escalate to SIGKILL.
    try:
        proc.kill()
    except (OSError, ProcessLookupError):
        # The process exited between the wait timeout and the kill; reaped.
        return "reaped"
    try:
        proc.wait(timeout=kill_sec)
    except subprocess.TimeoutExpired:
        # Survived SIGKILL: a real zombie / leak. Do NOT swallow -- report it.
        return "zombie_after_sigkill"
    return "reaped"


def _run_bench(
    *,
    n_per_arm: int,
    include_real_llm: bool,
    openai_api_key: str,
    output: Path | None,
    smoke: bool,
    max_cost_usd: float,
    allow_unpinned: bool = False,
    producer_count: int = 0,
    producer_event_rate_hz: float = 0.0,
    agent_frameworks: tuple[str, ...] = (),
) -> ExperimentBVerdict:
    """Drive the full bench end-to-end.

    Spawns the daemon, waits for it, runs the pilot, then alternates
    idle / loaded windows in a deterministic but PYTHONHASHSEED-seeded
    order. Writes verdict.json + progress.jsonl at the end.
    """
    started_ns = time.time_ns()
    # Run-scoped cache-bust salt. The Anthropic / OpenAI prompt caches are
    # scoped per-API-key and content-addressed by a prefix hash, NOT
    # per-process, so a byte-identical cold-cache prefix from a separate bench
    # run hits the prior run's cached prefix within the provider TTL.
    # ``secrets.token_hex`` is cryptographically random so two runs never
    # collide; ``force_cold_cache_prefix(run_salt, iter_id)`` mixes it into
    # each iteration's prefix at driver-spawn time (see _do_workload_iteration).
    run_salt = secrets.token_hex(8)
    # Establish the half-half core split FIRST so the environment report
    # below records the isolated measurement-time affinity (orchestrator
    # on the first-half cores; the daemon is pinned separately to the
    # second half at spawn) rather than the pre-pin inherited mask. The
    # split keeps daemon schedstat samples uncontaminated by orchestrator
    # + LLM-CLI co-tenancy; capturing the report afterwards means a
    # canonical run records a 2-core orchestrator affinity (no spurious
    # "covers N cores" warning) while a smoke / offline / dev-bypass run
    # -- which leaves the inherited affinity alone -- still records and
    # warns about the un-pinned mask.
    orchestrator_cores: set[int] | None = None
    daemon_cores: set[int] | None = None
    if include_real_llm and not allow_unpinned:
        orchestrator_cores, daemon_cores = compute_orchestrator_and_daemon_cores()
        os.sched_setaffinity(0, orchestrator_cores)
        structured(
            _logger,
            logging.INFO,
            "bench_affinity_pinned",
            orchestrator_cores=sorted(orchestrator_cores),
            daemon_cores=sorted(daemon_cores),
        )
    env_report = environment_report()
    # Probe ``CONFIG_SCHEDSTATS`` availability ONCE at bench startup
    # using the orchestrator's own /proc entry. A kernel without
    # ``CONFIG_SCHEDSTATS=y`` exposes neither
    # ``/proc/self/schedstat`` nor ``/proc/<pid>/task/<tid>/schedstat``
    # so the per-window aggregation path will silently return the
    # all-zero unavailable sentinel. The verdict surfaces this probe
    # so a downstream reader sees "kernel does not expose substrate"
    # rather than misreading the zero samples as "daemon did nothing."
    schedstat_kernel_available = schedstat_substrate_available()
    # Run the isolation gate AFTER establishing the pin -- so it verifies
    # the measurement-time affinity recipe -- and BEFORE the keyring /
    # CLI probes so the operator sees a host-config failure before any
    # token is spent.
    assert_cpu_isolation_for_baselines(
        env_report,
        include_real_llm=include_real_llm,
        allow_unpinned=allow_unpinned,
    )
    # Only require the LLM CLIs whose framework actually appears in
    # the agent-pool. The default agent_frameworks excludes claude-cli
    # and gemini-cli (they require OAuth subscription auth that does
    # not transplant to a fresh remote VM); requiring the binaries on
    # PATH regardless would block headless runs that never spawn
    # those drivers.
    needs_claude = include_real_llm and "claude-cli" in agent_frameworks
    needs_gemini = include_real_llm and "gemini-cli" in agent_frameworks
    needs_openai = include_real_llm and any(f in agent_frameworks for f in ("pydantic", "langgraph"))
    external_state_report = run_preflight_assertions(
        bench_name=_BENCH_NAME,
        require_openai=needs_openai,
        require_claude_cli=needs_claude,
        require_gemini_cli=needs_gemini,
    )
    verdict_path, progress_path, log_path = resolve_bench_log_paths(bench_name=_BENCH_NAME, output=output)
    progress_fh = progress_path.open("w", encoding="utf-8")
    log_fh = log_path.open("w", encoding="utf-8")

    handler = logging.StreamHandler(log_fh)
    handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(handler)
    _logger.setLevel(logging.INFO)

    structured(
        _logger,
        logging.INFO,
        "bench_multistream_started",
        n_per_arm=n_per_arm,
        smoke=smoke,
        include_real_llm=include_real_llm,
    )

    import tempfile

    tmp_dir = Path(tempfile.mkdtemp(prefix="waitbus-bench-multistream-"))
    daemon_handle = _spawn_pinned_daemon(
        tmp_dir=tmp_dir,
        daemon_cores=daemon_cores,
        env_report=env_report,
    )
    daemon_proc = daemon_handle.daemon_proc
    daemon_pid = daemon_handle.daemon_pid
    daemon_env = daemon_handle.daemon_env
    socket_path = daemon_handle.socket_path
    doorbell_path = daemon_handle.doorbell_path
    db_path = daemon_handle.db_path
    env_report = daemon_handle.env_report

    seed_scope_id = f"bench-multistream-{uuid.uuid4().hex[:12]}"

    try:
        _wait_for_daemon_socket(socket_path)
        # Socket-bound does NOT imply schema-migrated, so wait for the
        # events table to exist before reading the DB. After this returns
        # the PRAGMA snapshot reads a fully-migrated DB rather than racing
        # a half-initialised file.
        _wait_for_daemon_schema(db_path)
        external_state_report = msgspec.structs.replace(
            external_state_report,
            waitbus_daemon_pragmas=capture_daemon_pragmas(db_path),
        )
        append_jsonl_record(progress_fh, {"kind": "daemon_ready", "pid": daemon_pid})

        # Pilot for outlier threshold. Skipped when the bench's
        # downstream Mann-Whitney U test is structurally inapplicable.
        thresholds = _compute_outlier_thresholds(
            daemon_pid=daemon_pid,
            smoke=smoke,
            include_real_llm=include_real_llm,
            progress_fh=progress_fh,
        )
        pilot_skipped = thresholds.pilot_skipped
        pilot_skipped_reason = thresholds.pilot_skipped_reason
        outlier_threshold_ns = thresholds.outlier_threshold_ns
        outlier_threshold_schedstat_ns = thresholds.outlier_threshold_schedstat_ns

        agent_stderr_root = tmp_dir / "agent-stderr"
        loaded_runner = _make_loaded_runner(
            iter_id_offset=0,
            run_salt=run_salt,
            daemon_env=daemon_env,
            socket_path=socket_path,
            db_path=db_path,
            doorbell_path=doorbell_path,
            seed_scope_id=seed_scope_id,
            include_real_llm=include_real_llm,
            producer_count=producer_count,
            producer_event_rate_hz=producer_event_rate_hz,
            agent_frameworks=agent_frameworks,
            stderr_root=agent_stderr_root,
        )

        loop_result = _run_window_loop(
            daemon_pid=daemon_pid,
            loaded_runner=loaded_runner,
            n_per_arm=n_per_arm,
            max_cost_usd=max_cost_usd,
            external_state_report=external_state_report,
            progress_fh=progress_fh,
        )
        external_state_report = loop_result.external_state_report

        finished_ns = time.time_ns()
        verdict = _build_verdict(
            started_ns=started_ns,
            finished_ns=finished_ns,
            env_report=env_report,
            external_state=external_state_report,
            windows=loop_result.windows,
            outlier_threshold_ns=outlier_threshold_ns,
            outlier_threshold_schedstat_ns=outlier_threshold_schedstat_ns,
            n_per_arm=n_per_arm,
            cost=CostSummary(
                cost_usd_total=loop_result.cost_usd_total,
                cost_unknown_count=loop_result.cost_unknown_count,
                max_cost_usd_budget=max_cost_usd,
                max_cost_usd_observed=loop_result.max_cost_usd_observed,
                aborted_on_budget=loop_result.aborted_on_budget,
            ),
            swarm=SwarmAggregates(
                producer_count=producer_count,
                producer_event_rate_hz=producer_event_rate_hz,
                subscriber_agent_count=len(agent_frameworks),
                subscriber_framework_mix=loop_result.subscriber_framework_mix,
                producer_attrition_detected=loop_result.producer_attrition_detected,
                subscriber_attrition_detected=loop_result.subscriber_attrition_detected,
                producer_emit_count_total=loop_result.producer_emit_count_total,
                producer_late_count_total=loop_result.producer_late_count_total,
                producer_error_count_total=loop_result.producer_error_count_total,
            ),
            schedstat_kernel_available=schedstat_kernel_available,
            pilot_skipped=pilot_skipped,
            pilot_skipped_reason=pilot_skipped_reason,
        )
        encoded = msgspec.json.encode(verdict)
        verdict_path.write_bytes(encoded)
        append_jsonl_record(
            progress_fh,
            {"kind": "verdict_written", "path": str(verdict_path)},
        )
        structured(
            _logger,
            logging.INFO,
            "bench_multistream_finished",
            verdict_path=str(verdict_path),
            perturbation_detected=verdict.perturbation_detected,
        )
        return verdict
    finally:
        # Tear down daemon first; remove tmp on best-effort.
        teardown_outcome = _terminate_daemon_with_grace(daemon_proc)
        if teardown_outcome == "zombie_after_sigkill":
            # The daemon survived SIGKILL -- a genuine process leak. Surface
            # it as a WARNING rather than swallowing the post-kill timeout,
            # but do not raise: the bench result (if any) already completed
            # and a teardown leak must not mask a real verdict.
            structured(
                _logger,
                logging.WARNING,
                "bench_daemon_zombie_after_sigkill",
                pid=daemon_proc.pid,
            )
        progress_fh.close()
        log_fh.close()
        _logger.removeHandler(handler)


def main(argv: list[str] | None = None) -> int:
    """Bench entry point. Returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        description="Daemon CPU-side perturbation detection under the v2 swarm.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--n",
        type=int,
        default=None,
        help=f"per-arm window count (default: {_DEFAULT_N_PER_ARM} main; {_SMOKE_N_PER_ARM} smoke).",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="quick run: 5 windows per arm; skips real-LLM unless --include-real-llm passed.",
    )
    parser.add_argument(
        "--include-real-llm",
        action="store_true",
        help="invoke real OpenAI/claude/gemini calls in the loaded arm (incurs cost / quota burn).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="path to write the verdict JSON (default: .local-stress-logs/<ts>.bench_multistream_proof.verdict.json).",
    )
    parser.add_argument(
        "--max-cost-usd",
        type=float,
        default=5.0,
        help=(
            "Upper budget for accumulated USD cost across all OpenAI and claude calls. "
            "The bench aborts before the next loaded window when projected cost would breach this value. "
            "Default 5.00 USD."
        ),
    )
    parser.add_argument(
        "--allow-unpinned-for-dev",
        action="store_true",
        help=(
            "Bypass the substrate-isolation gate (CPU governor pinned to 'performance' "
            "AND orchestrator/daemon half-half CPU-affinity split). The resulting "
            "verdict is NOT a publishable baseline and the bench prints a banner "
            "to stderr. Use only for development iteration on a non-pinned host."
        ),
    )
    parser.add_argument(
        "--producer-count",
        type=int,
        default=_DEFAULT_PRODUCER_COUNT,
        help=(
            "Number of synthetic CI-event producer threads firing into the bus "
            "during the loaded arm. Producers draw their (source, event_type) "
            f"from the soak taxonomy. Default {_DEFAULT_PRODUCER_COUNT}. "
            "Smoke / --skip-real-llm runs effectively zero this (no producers "
            "spawn when the bench's main loop is offline). Set to 0 explicitly "
            "to skip producer measurement entirely."
        ),
    )
    parser.add_argument(
        "--producer-event-rate-hz",
        type=float,
        default=_DEFAULT_PRODUCER_EVENT_RATE_HZ,
        help=(
            "Aggregate emit rate across all producer threads in events per "
            f"wall-clock second. Default {_DEFAULT_PRODUCER_EVENT_RATE_HZ}. "
            "The OpenLoopScheduler is coordinated-omission-safe; the actual "
            "realized rate is reported on the verdict's producer_emit_count_total."
        ),
    )
    parser.add_argument(
        "--agent-frameworks",
        type=str,
        default=_DEFAULT_AGENT_FRAMEWORKS,
        help=(
            "Comma-separated list of LLM-agent driver frameworks to spawn as "
            "subscribers during the loaded arm. One driver subprocess per "
            f"framework in the list. Default '{_DEFAULT_AGENT_FRAMEWORKS}'. "
            "claude and gemini CLIs require OAuth subscription auth that does "
            "not transplant to a fresh remote VM, so they are excluded by "
            "default; pass --agent-frameworks pydantic,langgraph,claude-cli,"
            "gemini-cli,shell-control when the host has local CLI auth."
        ),
    )
    args = parser.parse_args(argv)
    if args.producer_count < 0:
        print(f"--producer-count must be >= 0, got {args.producer_count}", file=sys.stderr)
        return 2
    if args.producer_event_rate_hz < 0:
        print(
            f"--producer-event-rate-hz must be >= 0, got {args.producer_event_rate_hz}",
            file=sys.stderr,
        )
        return 2
    agent_frameworks = tuple(s.strip() for s in args.agent_frameworks.split(",") if s.strip())
    if not agent_frameworks:
        print("--agent-frameworks must list at least one framework", file=sys.stderr)
        return 2
    unknown = [f for f in agent_frameworks if f not in FRAMEWORK_ORDER]
    if unknown:
        print(
            f"--agent-frameworks contains unknown name(s): {unknown}. Valid: {','.join(FRAMEWORK_ORDER)}",
            file=sys.stderr,
        )
        return 2

    if args.smoke and args.n is None:
        n_per_arm = _SMOKE_N_PER_ARM
    elif args.n is not None:
        n_per_arm = int(args.n)
    else:
        n_per_arm = _DEFAULT_N_PER_ARM

    include_real_llm = bool(args.include_real_llm) and not args.smoke

    # Resolve OpenAI key BEFORE preflight; pass it through so the bench's
    # only keyring access lives at this entry point (security gate).
    openai_api_key = ""
    if include_real_llm:
        key = read_openai_key_from_keyring()
        if key is None:
            print(
                "preflight: OPENAI_API_KEY not in keyring; "
                "store via `secret-tool store --label='OpenAI API Key' "
                "service openai account api-key`",
                file=sys.stderr,
            )
            return 1
        openai_api_key = key

    try:
        # Smoke / offline modes zero both producer + subscriber counts:
        # there is no workload shape to measure when the loaded arm is
        # identity-vs-identity, and spawning producers/agents adds
        # measurement noise without exercising the path under test.
        if args.smoke or not include_real_llm:
            effective_producer_count = 0
            effective_rate_hz = 0.0
            effective_frameworks: tuple[str, ...] = ()
        else:
            effective_producer_count = int(args.producer_count)
            effective_rate_hz = float(args.producer_event_rate_hz)
            effective_frameworks = agent_frameworks
        verdict = _run_bench(
            n_per_arm=n_per_arm,
            include_real_llm=include_real_llm,
            openai_api_key=openai_api_key,
            output=args.output,
            smoke=bool(args.smoke),
            max_cost_usd=float(args.max_cost_usd),
            allow_unpinned=bool(args.allow_unpinned_for_dev),
            producer_count=effective_producer_count,
            producer_event_rate_hz=effective_rate_hz,
            agent_frameworks=effective_frameworks,
        )
    except PreflightError as exc:
        print(f"preflight failure: {exc}", file=sys.stderr)
        return 1
    print(
        f"bench_multistream_proof: windows={len(verdict.windows)} "
        f"perturbation_detected={verdict.perturbation_detected} "
        f"median_idle_us={verdict.median_idle_utime_ns / 1000:.1f} "
        f"median_loaded_us={verdict.median_loaded_utime_ns / 1000:.1f} "
        f"median_idle_schedstat_us={verdict.median_idle_schedstat_ns / 1000:.1f} "
        f"median_loaded_schedstat_us={verdict.median_loaded_schedstat_ns / 1000:.1f} "
        f"p_schedstat_per_sec={verdict.mann_whitney_p_schedstat_per_sec:.4g} "
        f"p_schedstat_raw={verdict.mann_whitney_p_schedstat_raw:.4g} "
        f"p_pcount={verdict.mann_whitney_p_pcount:.4g} "
        f"rejected={verdict.rejected_window_count}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
