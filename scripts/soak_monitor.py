"""24-hour soak monitoring helpers: sample, threshold, verdict.

This module is the testable core of ``scripts/soak.py``. Each function
takes raw inputs (a PID, a path, a list of samples) and returns
structured outputs (a metric dict, a slope, a pass/fail decision). The
orchestrator wires them into the running soak; the unit tests in
``tests/test_soak_monitor.py`` exercise them in isolation.

Threshold rationale lives in ``docs/SOAK_TEST.md`` next to the
operator-facing instructions.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Iterable
from pathlib import Path


@dataclasses.dataclass(frozen=True)
class Sample:
    """One monitor sample. ``ts_ns`` is :func:`time.time_ns` at capture."""

    ts_ns: int
    rss_bytes: int
    fd_count: int
    wal_bytes: int


@dataclasses.dataclass(frozen=True)
class ThresholdVerdict:
    """Per-signal pass/fail with the metric driving the decision."""

    signal: str
    passed: bool
    detail: str


def read_vmrss_bytes(pid: int) -> int:
    """Return ``VmRSS`` from ``/proc/<pid>/status``, in bytes.

    Linux-only. Caller checks ``sys.platform == 'linux'`` before invoking;
    on macOS the soak harness skips RSS sampling rather than failing.
    """
    status_path = Path(f"/proc/{pid}/status")
    text = status_path.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            return int(parts[1]) * 1024
    raise RuntimeError(f"VmRSS not found in {status_path}")


def count_open_fds(pid: int) -> int:
    """Return the number of open file descriptors for ``pid``.

    Counts entries in ``/proc/<pid>/fd``. Wraps the ``listdir`` so a
    transient ``ENOENT`` (raced by a daemon exit) surfaces as a clear
    error rather than a stale negative count.
    """
    fd_dir = Path(f"/proc/{pid}/fd")
    return len(list(fd_dir.iterdir()))


def wal_size_bytes(db_path: Path) -> int:
    """Return the SQLite WAL file's size, or 0 if absent.

    The WAL file is ``<db>.-wal``. A zero return means the daemon
    checkpointed since the last sample (or the WAL never existed,
    which is the case at startup).
    """
    wal_path = db_path.with_suffix(db_path.suffix + "-wal")
    try:
        return wal_path.stat().st_size
    except FileNotFoundError:
        return 0


def linear_regression_slope(ts: Iterable[float], values: Iterable[float]) -> float:
    """Ordinary-least-squares slope of ``values`` vs ``ts``.

    Returns the slope only -- intercept is not needed for the soak
    drift signal. ``ts`` and ``values`` must have identical length and
    at least two points; raises ``ValueError`` otherwise so a caller
    misuse surfaces rather than silently producing zero.
    """
    xs = list(ts)
    ys = list(values)
    if len(xs) != len(ys):
        raise ValueError(f"length mismatch: ts={len(xs)} values={len(ys)}")
    n = len(xs)
    if n < 2:
        raise ValueError(f"need at least 2 points, got {n}")
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0.0:
        raise ValueError("ts has zero variance (all samples at same timestamp)")
    return numerator / denominator


def rss_slope_threshold(samples: list[Sample]) -> ThresholdVerdict:
    """Pass if RSS slope <= 0.5 MiB/hr AND max RSS <= 2x initial.

    Two parts because a slow leak (low slope, never hits 2x) is still
    a slow leak and must surface; a sudden 2x spike with subsequent
    GC could net a near-zero slope but the workstation operator
    already paged at the spike.
    """
    if len(samples) < 2:
        return ThresholdVerdict("rss", True, "fewer than 2 samples; skipping")
    ts_hours = [(s.ts_ns - samples[0].ts_ns) / 3.6e12 for s in samples]
    rss_mib = [s.rss_bytes / (1024 * 1024) for s in samples]
    slope = linear_regression_slope(ts_hours, rss_mib)
    initial = samples[0].rss_bytes
    max_rss = max(s.rss_bytes for s in samples)
    if slope > 0.5:
        return ThresholdVerdict("rss", False, f"slope {slope:.3f} MiB/hr > 0.5")
    if initial > 0 and max_rss > 2 * initial:
        return ThresholdVerdict("rss", False, f"max {max_rss} > 2x initial {initial}")
    return ThresholdVerdict("rss", True, f"slope={slope:.3f} MiB/hr; max/initial={max_rss / max(initial, 1):.2f}")


def fd_growth_threshold(samples: list[Sample]) -> ThresholdVerdict:
    """Pass if final FD count <= baseline + 5 AND max <= 2x baseline."""
    if len(samples) < 2:
        return ThresholdVerdict("fd", True, "fewer than 2 samples; skipping")
    baseline = samples[0].fd_count
    final = samples[-1].fd_count
    peak = max(s.fd_count for s in samples)
    if final - baseline > 5:
        return ThresholdVerdict("fd", False, f"final - baseline = {final - baseline} > 5")
    if baseline > 0 and peak > 2 * baseline:
        return ThresholdVerdict("fd", False, f"peak {peak} > 2x baseline {baseline}")
    return ThresholdVerdict("fd", True, f"baseline={baseline} final={final} peak={peak}")


def wal_size_threshold(samples: list[Sample]) -> ThresholdVerdict:
    """Pass if max WAL <= 100 MiB AND final <= initial + 5 MiB."""
    if not samples:
        return ThresholdVerdict("wal", True, "no samples")
    initial = samples[0].wal_bytes
    final = samples[-1].wal_bytes
    peak = max(s.wal_bytes for s in samples)
    mib = 1024 * 1024
    if peak > 100 * mib:
        return ThresholdVerdict("wal", False, f"peak {peak / mib:.1f} MiB > 100")
    if final > initial + 5 * mib:
        return ThresholdVerdict("wal", False, f"final - initial = {(final - initial) / mib:.1f} MiB > 5")
    return ThresholdVerdict("wal", True, f"initial={initial / mib:.1f} final={final / mib:.1f} peak={peak / mib:.1f}")


def suspend_recovery_threshold(
    *,
    pre_suspend_p99_ns: float,
    post_suspend_p99_ns: float,
    integrity_ok: bool,
    events_lost_post_resume: int,
) -> ThresholdVerdict:
    """Pass if post-resume p99 within +/-15% of pre-suspend, integrity OK, no lost events.

    Used after each SIGSTOP/SIGCONT cycle in the realism-mode soak.
    Events that arrived DURING the freeze are an expected data-loss
    window (documented in SOAK_TEST.md); ``events_lost_post_resume``
    counts only events emitted AFTER SIGCONT that the subscriber
    did not observe. Any non-zero is a fail.
    """
    if pre_suspend_p99_ns <= 0:
        return ThresholdVerdict("suspend_recovery", False, f"invalid pre-suspend p99={pre_suspend_p99_ns}")
    ratio = post_suspend_p99_ns / pre_suspend_p99_ns
    if not (0.85 <= ratio <= 1.15):
        return ThresholdVerdict(
            "suspend_recovery",
            False,
            f"post/pre ratio {ratio:.3f} outside [0.85, 1.15]",
        )
    if not integrity_ok:
        return ThresholdVerdict("suspend_recovery", False, "PRAGMA integrity_check did not return 'ok'")
    if events_lost_post_resume != 0:
        return ThresholdVerdict("suspend_recovery", False, f"{events_lost_post_resume} events lost post-SIGCONT")
    return ThresholdVerdict("suspend_recovery", True, f"ratio={ratio:.3f}; no post-resume loss")


def p99_drift_threshold(
    p99_samples: list[tuple[int, float]],
    *,
    max_slope_pct_per_hour: float = 0.0625,
) -> ThresholdVerdict:
    """Pass if |p99 slope| <= ``max_slope_pct_per_hour`` of the initial p99.

    ``p99_samples`` is a list of ``(ts_ns, p99_ns)`` tuples captured by
    the subscriber thread on a fixed cadence (30 min in the soak).
    Mirrors :func:`rss_slope_threshold` shape: regression slope is
    computed over hours-since-start vs the p99 values, expressed as a
    percentage of the initial p99 to normalise across baseline.

    Default 0.0625%/hr translates to <=1.5%/24h, the operator-facing
    bound from the soak design decision. A small p99_samples (<2)
    skips the check rather than failing.
    """
    if len(p99_samples) < 2:
        return ThresholdVerdict("p99_drift", True, "fewer than 2 p99 samples; skipping")
    ts_hours = [(ts - p99_samples[0][0]) / 3.6e12 for ts, _ in p99_samples]
    values = [v for _, v in p99_samples]
    initial = values[0]
    if initial <= 0:
        return ThresholdVerdict("p99_drift", False, f"invalid initial p99={initial}")
    slope = linear_regression_slope(ts_hours, values)
    slope_pct_per_hour = abs(slope / initial) * 100.0
    if slope_pct_per_hour > max_slope_pct_per_hour:
        return ThresholdVerdict(
            "p99_drift",
            False,
            f"|slope| {slope_pct_per_hour:.4f}%/hr > {max_slope_pct_per_hour:.4f}%/hr",
        )
    return ThresholdVerdict(
        "p99_drift",
        True,
        f"|slope|={slope_pct_per_hour:.4f}%/hr; initial_p99_ns={initial:.1f}",
    )


def gc_threshold(gc_samples: list[tuple[int, int, int]]) -> ThresholdVerdict:
    """Pass if uncollectable is always 0 AND collected_cumulative is monotone non-decreasing.

    Each tuple is ``(ts_ns, uncollectable, collected_cumulative)``
    captured every 5 min from ``gc.get_stats()``. A non-zero
    uncollectable count at ANY sample means the GC found a cycle it
    could not break -- a memory leak class the RSS slope alone would
    miss on a soak short enough for the leak to not yet dominate.

    The monotone-non-decreasing check on collected_cumulative is a
    sanity gate: an absolute decrease would mean a counter wrap or
    instrumentation bug; the threshold catches that rather than
    silently reading a corrupt series.
    """
    if not gc_samples:
        return ThresholdVerdict("gc", True, "no gc samples; skipping")
    for ts_ns, uncoll, _ in gc_samples:
        if uncoll != 0:
            return ThresholdVerdict("gc", False, f"uncollectable={uncoll} at ts_ns={ts_ns}")
    cumulative_values = [collected for _, _, collected in gc_samples]
    for i in range(1, len(cumulative_values)):
        if cumulative_values[i] < cumulative_values[i - 1]:
            return ThresholdVerdict(
                "gc",
                False,
                f"collected_cumulative decreased at index {i}: {cumulative_values[i - 1]} -> {cumulative_values[i]}",
            )
    return ThresholdVerdict(
        "gc",
        True,
        f"n={len(gc_samples)}; final_collected={cumulative_values[-1]}; no uncollectable",
    )


def log_size_threshold(
    log_samples: list[tuple[int, int]],
    *,
    max_mib_per_hour: float = 1.0,
) -> ThresholdVerdict:
    """Pass if log-file slope <= ``max_mib_per_hour``.

    Each tuple is ``(ts_ns, size_bytes)`` sampled every 60 s from a
    file the soak controls (typically the progress JSONL). A slope
    above 1 MiB/hr in a 24-h soak (24 MiB total) suggests runaway
    sampling, a leak in the JSONL writer, or an instrumentation bug.
    """
    if len(log_samples) < 2:
        return ThresholdVerdict("log_size", True, "fewer than 2 log samples; skipping")
    ts_hours = [(ts - log_samples[0][0]) / 3.6e12 for ts, _ in log_samples]
    size_mib = [size / (1024 * 1024) for _, size in log_samples]
    slope = linear_regression_slope(ts_hours, size_mib)
    if slope > max_mib_per_hour:
        return ThresholdVerdict(
            "log_size",
            False,
            f"slope {slope:.3f} MiB/hr > {max_mib_per_hour:.3f}",
        )
    return ThresholdVerdict("log_size", True, f"slope={slope:.3f} MiB/hr")


def per_source_share_threshold(
    window_counts: dict[str, int],
    target_shares: dict[str, float],
    *,
    tolerance_pct: float = 10.0,
) -> ThresholdVerdict:
    """Pass if every source's observed share is within ``tolerance_pct`` of its target.

    ``window_counts`` is a per-source emit-count dict over an
    observation window (typically 30 min). ``target_shares`` is the
    desired fraction per source (e.g. ``{"github": 0.5, "pytest":
    0.2, "docker": 0.2, "fs": 0.1}``). Both dicts must share the same
    key set; an asymmetry is an instrumentation bug, not a soak
    failure, and is surfaced as such.

    Tolerance is an ABSOLUTE percentage-point delta, not a relative
    one: target 50% with 10% tolerance means observed must land in
    [40%, 60%]. Same shape for every source so the operator does not
    have to track per-source relative widths.
    """
    if not window_counts:
        return ThresholdVerdict("per_source_share", True, "no window counts; skipping")
    if set(window_counts) != set(target_shares):
        return ThresholdVerdict(
            "per_source_share",
            False,
            f"key mismatch: counts={sorted(window_counts)} targets={sorted(target_shares)}",
        )
    total = sum(window_counts.values())
    if total <= 0:
        return ThresholdVerdict("per_source_share", False, f"total emits in window = {total}")
    deltas: list[str] = []
    for source, target in target_shares.items():
        observed = window_counts[source] / total
        delta_pct = abs(observed - target) * 100.0
        if delta_pct > tolerance_pct:
            deltas.append(
                f"{source}: observed={observed * 100:.1f}% target={target * 100:.1f}% delta={delta_pct:.1f}pp"
            )
    if deltas:
        return ThresholdVerdict("per_source_share", False, "; ".join(deltas))
    return ThresholdVerdict(
        "per_source_share",
        True,
        f"n={total}; all sources within +/-{tolerance_pct:.1f}pp",
    )


def collect_sample(*, pid: int, db_path: Path) -> Sample:
    """Capture one ``Sample`` from a live daemon.

    Composed of :func:`read_vmrss_bytes`, :func:`count_open_fds`, and
    :func:`wal_size_bytes` so the soak orchestrator's per-minute loop
    is a single function call. Linux-only.
    """
    return Sample(
        ts_ns=time.time_ns(),
        rss_bytes=read_vmrss_bytes(pid),
        fd_count=count_open_fds(pid),
        wal_bytes=wal_size_bytes(db_path),
    )
