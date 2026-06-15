"""Soak harness data shapes: frozen ctx + mutable accumulators + scalar state.

stdlib + msgspec only.  Importing this module must not trigger any
other soak-sibling import so the package DAG stays acyclic.
"""

from __future__ import annotations

import argparse
import dataclasses
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import msgspec


class SoakSignalFailure(msgspec.Struct, kw_only=True, frozen=True):
    """Per-signal failure record carried by ``_VerdictDoc.failures``.

    Discriminated by ``signal`` name; ``threshold`` and ``observed`` are
    machine-comparable so a consumer can compute deltas without parsing
    ``detail``. ``sample_index`` is an optional forensic pointer into
    the matching sample list (``None`` when the failure is aggregate).
    No traceback: these are bounds-violations from ``_compute_verdict_doc``'s
    in-process computation, not Python exceptions.  The struct serialises
    to JSON via ``msgspec.to_builtins(doc)`` alongside the rest of
    ``_VerdictDoc``; consumers see ``{"failures": [...]}`` in the verdict
    JSON.
    """

    signal: str
    threshold: float
    observed: float
    detail: str = ""
    sample_index: int | None = None


class _VerdictDoc(msgspec.Struct, kw_only=True, frozen=True):
    """Typed verdict document replacing the 22-key dict literal.

    Serialised via ``msgspec.to_builtins(doc)`` at the write site —
    idiomatic msgspec, no bespoke ``to_dict()`` method.  Reduces
    ``_compute_verdict_doc`` from C(13) to ~A(5).

    The ``failures`` field carries per-signal failure records so consumers
    can group correlated failures without parsing ``detail`` strings.  The
    JSON wire contract (consumed by ``.github/workflows/soak.yml``,
    ``docs/SOAK_TEST.md``, and the in-process exit-code dispatch) is
    preserved: ``_write_verdict`` writes ``msgspec.to_builtins(doc)`` as
    before; consumers wanting correlated-failure grouping can inspect
    ``doc.failures`` before the write, or ``verdict["failures"]`` after
    loading the JSON.
    """

    started_at_ns: int
    ended_at_ns: int
    duration_sec: float
    emit_rate_hz: float
    sample_interval_sec: float
    n_samples: int
    events_emitted: int
    is_partial: bool
    verdicts: list[dict[str, Any]]
    suspend_cycles: list[dict[str, Any]]
    suspend_recovery_per_cycle: list[dict[str, Any]]
    p99_samples_ns: list[dict[str, Any]]
    gc_samples: list[dict[str, Any]]
    log_size_samples: list[dict[str, Any]]
    source_counts: dict[str, int]
    samples: list[dict[str, Any]]
    overall_passed: bool
    failures: tuple[SoakSignalFailure, ...] = ()
    fault_injection_outcomes: list[dict[str, Any]] = []
    subscriber_close_reasons: dict[str, int] = {}
    # Drain-path smoke pre-phase results (throwaway-daemon probes run before
    # the measured soak). Empty when the pre-phase was skipped.
    drain_smoke_outcomes: list[dict[str, Any]] = []
    drain_smoke_close_reasons: dict[str, int] = {}


class _SoakContext(msgspec.Struct, kw_only=True, frozen=True):
    """Immutable per-run configuration threaded by reference.

    Fields are the per-run startup values that do not change after
    ``main()`` startup.  Passing a single frozen struct instead of
    a kwarg fan eliminates the fan, makes call sites read left-to-right,
    and catches missing-field errors at construction time.
    """

    proc: subprocess.Popen[bytes]
    db_path: Path
    progress_path: Path
    socket_path: Path
    daemon_stderr_path: Path
    configured_fault_axes: frozenset[str]
    args: argparse.Namespace
    start_monotonic: float
    started_at_ns: int
    total_seconds: float
    corpus_iter: Iterator[dict[str, Any] | None] | None
    # ``subscriber`` is the ``scripts.soak._main._SubscriberThread`` instance
    # owned by the orchestrator.  Typed as ``Any`` rather than a forward
    # reference because ``_context`` imports from ``_main`` would create a
    # cycle (``_main`` imports every sibling, including this module), and
    # ``msgspec.inspect.type_info`` would NameError on an unresolved string
    # forward-reference at probe time.  Runtime use is well-typed via the
    # construction site in ``_main.main``; consumers narrow as needed.
    subscriber: Any
    sample_interval_sec: float
    progress_fh: Any  # open file handle for long-lived FD pattern (IO[str])
    # ``DrainSmokeResult`` from the pre-phase, or None when skipped. Typed
    # ``Any`` to avoid importing ``_drain_smoke`` here (it imports ``_verdict``
    # -> ``_context``, which would create a cycle). Consumed by
    # ``_compute_verdict_doc`` to fold the pre-phase verdicts into the JSON.
    drain_smoke: Any = None


# Soak monitor sample type forward-reference.  Imported here for the
# field type of ``_SoakAccumulators.rss_samples`` so consumers of this
# module do not need to import scripts.soak_monitor just for the
# annotation.
from scripts.soak_monitor import Sample, ThresholdVerdict  # noqa: E402


class _SoakAccumulators(msgspec.Struct, kw_only=True):
    """Mutable container holding the appendable sample lists.

    NOT frozen: a frozen struct holding mutable list fields is a footgun
    (the list *reference* is frozen, the list *contents* are not, leading
    to surprising sharing semantics). Owned by ``main``, mutated in-place
    by the run-step helpers.
    """

    rss_samples: list[Sample]
    p99_samples: list[tuple[int, float]]
    gc_samples: list[tuple[int, int, int]]
    log_size_samples: list[tuple[int, int]]
    source_counts: dict[str, int]
    suspend_outcomes: list[dict[str, Any]]
    suspend_verdicts: list[ThresholdVerdict]
    fault_injection_outcomes: list[dict[str, Any]] = []
    corpus_decode_fallthroughs: int = 0


class SuspendCycle(msgspec.Struct, kw_only=True, frozen=True):
    """One scheduled suspend window."""

    offset_sec: float
    duration_sec: float


# The standard suspend-cycle schedule (one 30-min cycle + six 5-min
# cycles) used when ``--inject-suspend-cycles standard`` is passed.
_STANDARD_SUSPEND_CYCLES: tuple[SuspendCycle, ...] = (
    SuspendCycle(offset_sec=8 * 3600 + 60 * 60, duration_sec=30 * 60),
    *(SuspendCycle(offset_sec=10 * 3600 + 15 * 60 * i, duration_sec=5 * 60) for i in range(6)),
)


class FaultInjectionRecord(msgspec.Struct, kw_only=True, frozen=True):
    """One scheduled fault-injection probe.

    ``axis`` selects which probe function runs; ``offset_sec`` is the
    monotonic offset from soak start at which the probe fires. The
    probe opens its own short-lived socket against the broadcast
    daemon, drives a specific drain-path scenario, and records an
    outcome record in ``_SoakAccumulators.fault_injection_outcomes``.
    """

    offset_sec: float
    axis: str  # token_reject | version_reject | replay_lag_eviction


# Standard schedule fits inside a 30-minute Hetzner pre-24h smoke run.
# All three probes fire in the first 25 minutes, leaving 5 minutes of
# clean post-probe samples before the verdict is written. Each probe
# completes in seconds, so the p99_drift signal is not contaminated by
# probe windows (the 30-minute p99 sample cadence collects at run end,
# after every probe has long completed).
_STANDARD_FAULT_INJECTIONS: tuple[FaultInjectionRecord, ...] = (
    FaultInjectionRecord(offset_sec=2 * 60, axis="version_reject"),
    FaultInjectionRecord(offset_sec=10 * 60, axis="token_reject"),
    FaultInjectionRecord(offset_sec=20 * 60, axis="replay_lag_eviction"),
)


# Fast schedule fires every probe in the first 30 seconds so a
# sub-minute local smoke covers every drain path without waiting for
# the standard offsets. Use ``--duration 60s --inject-fault-scenarios fast``.
_FAST_FAULT_INJECTIONS: tuple[FaultInjectionRecord, ...] = (
    FaultInjectionRecord(offset_sec=5.0, axis="version_reject"),
    FaultInjectionRecord(offset_sec=10.0, axis="token_reject"),
    FaultInjectionRecord(offset_sec=20.0, axis="replay_lag_eviction"),
)


@dataclasses.dataclass
class _SoakState:
    """Mutable scalar state threaded through ``_run_soak_step``."""

    i: int
    next_emit: float
    next_sample: float
    next_p99_sample: float
    next_gc_sample: float
    next_log_sample: float
    corpus_exhausted: bool
    preserve_warned: bool
    json_decode_warned: bool = False


# ---------------------------------------------------------------------------
# Sample-interval cadences for the four monitor signals.
# ---------------------------------------------------------------------------

#: Cadence (seconds) at which the soak captures one p99 sample from the
#: subscriber thread's HdrHistogram. 30 min over a 24-h run gives 48
#: points, ample for a linear-regression slope.
_P99_SAMPLE_INTERVAL_SEC: float = 30 * 60.0

#: Cadence (seconds) at which the soak samples ``gc.get_stats`` and the
#: log-file size. 60 s for log size, 5 min for GC — surfaced as
#: separate intervals because GC stats are coarse-grained per-generation
#: counters that do not need a per-minute read.
_GC_SAMPLE_INTERVAL_SEC: float = 5 * 60.0
_LOG_SIZE_SAMPLE_INTERVAL_SEC: float = 60.0
