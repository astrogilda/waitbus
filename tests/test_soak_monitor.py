"""Unit tests for the soak-harness threshold helpers.

The soak orchestrator itself (``scripts/soak.py``) needs a full 24-h
run on a tuned host to exercise; the threshold helpers in
``scripts/soak_monitor.py`` are decision functions over plain data,
testable in isolation here.

These tests document the threshold contract so a future change to
``soak_monitor.py`` (tighter slopes, new signals) cannot land
without an explicit edit of the assertions below.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from scripts.soak_monitor import (
    Sample,
    fd_growth_threshold,
    gc_threshold,
    linear_regression_slope,
    log_size_threshold,
    p99_drift_threshold,
    per_source_share_threshold,
    rss_slope_threshold,
    suspend_recovery_threshold,
    wal_size_threshold,
)

_MIB = 1024 * 1024
_HOUR_NS = 3600 * 1_000_000_000


def _sample(t_hours: float, rss_mib: float, fd_count: int, wal_mib: float) -> Sample:
    """Construct a Sample with conveniently human-readable units."""
    return Sample(
        ts_ns=int(t_hours * _HOUR_NS),
        rss_bytes=int(rss_mib * _MIB),
        fd_count=fd_count,
        wal_bytes=int(wal_mib * _MIB),
    )


# ---------------------------------------------------------------------------
# linear_regression_slope
# ---------------------------------------------------------------------------


class TestLinearRegressionSlope:
    """Slope helper handles the canonical happy path and the misuse paths."""

    def test_positive_slope(self) -> None:
        slope = linear_regression_slope([0.0, 1.0, 2.0, 3.0], [10.0, 20.0, 30.0, 40.0])
        assert slope == pytest.approx(10.0)

    def test_negative_slope(self) -> None:
        slope = linear_regression_slope([0.0, 1.0, 2.0], [100.0, 50.0, 0.0])
        assert slope == pytest.approx(-50.0)

    def test_length_mismatch_raises(self) -> None:
        with pytest.raises(ValueError, match="length mismatch"):
            linear_regression_slope([0.0, 1.0], [10.0])

    def test_single_point_raises(self) -> None:
        with pytest.raises(ValueError, match="at least 2 points"):
            linear_regression_slope([0.0], [10.0])

    def test_zero_variance_raises(self) -> None:
        with pytest.raises(ValueError, match="zero variance"):
            linear_regression_slope([1.0, 1.0, 1.0], [10.0, 20.0, 30.0])


# ---------------------------------------------------------------------------
# rss_slope_threshold
# ---------------------------------------------------------------------------


class TestRssSlopeThreshold:
    """RSS verdict reads the slope AND the peak; either signal can fail."""

    def test_flat_rss_passes(self) -> None:
        samples = [_sample(t, 100.0, 10, 0.0) for t in (0.0, 6.0, 12.0, 18.0, 24.0)]
        verdict = rss_slope_threshold(samples)
        assert verdict.passed, verdict.detail

    def test_high_slope_fails(self) -> None:
        # 5 MiB/hour growth: > 0.5 MiB/hr threshold.
        samples = [_sample(t, 100.0 + 5.0 * t, 10, 0.0) for t in (0.0, 6.0, 12.0, 18.0, 24.0)]
        verdict = rss_slope_threshold(samples)
        assert not verdict.passed
        assert "slope" in verdict.detail

    def test_peak_above_2x_fails_even_when_slope_low(self) -> None:
        # Start 100, spike to 300, return to 100 -- near-zero net slope
        # but a 3x peak which the threshold must catch.
        samples = [
            _sample(0.0, 100.0, 10, 0.0),
            _sample(12.0, 300.0, 10, 0.0),
            _sample(24.0, 100.0, 10, 0.0),
        ]
        verdict = rss_slope_threshold(samples)
        assert not verdict.passed
        assert "max" in verdict.detail

    def test_one_sample_passes_with_note(self) -> None:
        samples = [_sample(0.0, 100.0, 10, 0.0)]
        verdict = rss_slope_threshold(samples)
        assert verdict.passed
        assert "fewer than 2" in verdict.detail


# ---------------------------------------------------------------------------
# fd_growth_threshold
# ---------------------------------------------------------------------------


class TestFdGrowthThreshold:
    """FD verdict refuses any growth > 5 OR a peak > 2x baseline."""

    def test_stable_count_passes(self) -> None:
        samples = [_sample(t, 100.0, 12, 0.0) for t in (0.0, 12.0, 24.0)]
        verdict = fd_growth_threshold(samples)
        assert verdict.passed

    def test_growth_above_5_fails(self) -> None:
        samples = [
            _sample(0.0, 100.0, 10, 0.0),
            _sample(12.0, 100.0, 12, 0.0),
            _sample(24.0, 100.0, 16, 0.0),
        ]
        verdict = fd_growth_threshold(samples)
        assert not verdict.passed
        assert "final - baseline" in verdict.detail

    def test_peak_above_2x_fails(self) -> None:
        samples = [
            _sample(0.0, 100.0, 10, 0.0),
            _sample(12.0, 100.0, 25, 0.0),  # peak; 2.5x baseline
            _sample(24.0, 100.0, 12, 0.0),  # final still <= baseline+5
        ]
        verdict = fd_growth_threshold(samples)
        assert not verdict.passed
        assert "peak" in verdict.detail


# ---------------------------------------------------------------------------
# wal_size_threshold
# ---------------------------------------------------------------------------


class TestWalSizeThreshold:
    """WAL verdict refuses peaks > 100 MiB OR final >= initial + 5 MiB."""

    def test_zero_wal_passes(self) -> None:
        samples = [_sample(t, 100.0, 10, 0.0) for t in (0.0, 12.0, 24.0)]
        verdict = wal_size_threshold(samples)
        assert verdict.passed

    def test_peak_above_100_mib_fails(self) -> None:
        samples = [
            _sample(0.0, 100.0, 10, 0.0),
            _sample(12.0, 100.0, 10, 150.0),
            _sample(24.0, 100.0, 10, 0.0),
        ]
        verdict = wal_size_threshold(samples)
        assert not verdict.passed
        assert "peak" in verdict.detail

    def test_growth_above_5_mib_fails(self) -> None:
        samples = [
            _sample(0.0, 100.0, 10, 1.0),
            _sample(24.0, 100.0, 10, 10.0),  # final - initial = 9 MiB > 5
        ]
        verdict = wal_size_threshold(samples)
        assert not verdict.passed
        assert "final - initial" in verdict.detail


# ---------------------------------------------------------------------------
# suspend_recovery_threshold
# ---------------------------------------------------------------------------


class TestSuspendRecoveryThreshold:
    """Suspend-recovery passes iff p99 ratio in band AND integrity OK AND no loss."""

    def test_clean_recovery_passes(self) -> None:
        verdict = suspend_recovery_threshold(
            pre_suspend_p99_ns=1_000_000,
            post_suspend_p99_ns=1_100_000,
            integrity_ok=True,
            events_lost_post_resume=0,
        )
        assert verdict.passed

    def test_p99_outside_band_fails(self) -> None:
        verdict = suspend_recovery_threshold(
            pre_suspend_p99_ns=1_000_000,
            post_suspend_p99_ns=1_500_000,  # 1.5x: outside [0.85, 1.15]
            integrity_ok=True,
            events_lost_post_resume=0,
        )
        assert not verdict.passed
        assert "ratio" in verdict.detail

    def test_integrity_failure_fails(self) -> None:
        verdict = suspend_recovery_threshold(
            pre_suspend_p99_ns=1_000_000,
            post_suspend_p99_ns=1_000_000,
            integrity_ok=False,
            events_lost_post_resume=0,
        )
        assert not verdict.passed
        assert "integrity_check" in verdict.detail

    def test_post_resume_loss_fails(self) -> None:
        verdict = suspend_recovery_threshold(
            pre_suspend_p99_ns=1_000_000,
            post_suspend_p99_ns=1_000_000,
            integrity_ok=True,
            events_lost_post_resume=3,
        )
        assert not verdict.passed
        assert "events lost" in verdict.detail

    def test_invalid_pre_suspend_p99_fails(self) -> None:
        verdict = suspend_recovery_threshold(
            pre_suspend_p99_ns=0,
            post_suspend_p99_ns=1_000_000,
            integrity_ok=True,
            events_lost_post_resume=0,
        )
        assert not verdict.passed
        assert "invalid" in verdict.detail


# ---------------------------------------------------------------------------
# p99_drift_threshold
# ---------------------------------------------------------------------------


class TestP99DriftThreshold:
    """p99 drift slope must stay within the +/- 0.0625%/hr band (1.5% per 24h)."""

    def test_flat_p99_passes(self) -> None:
        samples = [(int(t * _HOUR_NS), 1_000_000.0) for t in (0.0, 6.0, 12.0, 18.0, 24.0)]
        verdict = p99_drift_threshold(samples)
        assert verdict.passed, verdict.detail

    def test_high_drift_fails(self) -> None:
        # 1%/hour drift: well above 0.0625%/hr default.
        samples = [(int(t * _HOUR_NS), 1_000_000.0 * (1.0 + 0.01 * t)) for t in (0.0, 6.0, 12.0, 18.0, 24.0)]
        verdict = p99_drift_threshold(samples)
        assert not verdict.passed
        assert "slope" in verdict.detail

    def test_within_band_passes(self) -> None:
        # ~0.05%/hr drift: within 0.0625%/hr default.
        samples = [(int(t * _HOUR_NS), 1_000_000.0 * (1.0 + 0.0005 * t)) for t in (0.0, 6.0, 12.0, 18.0, 24.0)]
        verdict = p99_drift_threshold(samples)
        assert verdict.passed, verdict.detail

    def test_fewer_than_2_samples_skipped(self) -> None:
        verdict = p99_drift_threshold([(0, 1_000_000.0)])
        assert verdict.passed
        assert "fewer than 2" in verdict.detail

    def test_invalid_initial_fails(self) -> None:
        verdict = p99_drift_threshold([(0, 0.0), (_HOUR_NS, 1_000.0)])
        assert not verdict.passed
        assert "invalid initial" in verdict.detail


# ---------------------------------------------------------------------------
# gc_threshold
# ---------------------------------------------------------------------------


class TestGcThreshold:
    """gc verdict fails on ANY uncollectable AND on a non-monotone cumulative."""

    def test_clean_run_passes(self) -> None:
        samples = [
            (0, 0, 10),
            (_HOUR_NS, 0, 15),
            (2 * _HOUR_NS, 0, 22),
        ]
        verdict = gc_threshold(samples)
        assert verdict.passed, verdict.detail

    def test_uncollectable_nonzero_fails(self) -> None:
        samples = [
            (0, 0, 10),
            (_HOUR_NS, 1, 15),
            (2 * _HOUR_NS, 0, 22),
        ]
        verdict = gc_threshold(samples)
        assert not verdict.passed
        assert "uncollectable" in verdict.detail

    def test_non_monotone_cumulative_fails(self) -> None:
        samples = [
            (0, 0, 10),
            (_HOUR_NS, 0, 15),
            (2 * _HOUR_NS, 0, 12),  # decreased
        ]
        verdict = gc_threshold(samples)
        assert not verdict.passed
        assert "decreased" in verdict.detail

    def test_empty_samples_skipped(self) -> None:
        verdict = gc_threshold([])
        assert verdict.passed
        assert "no gc samples" in verdict.detail


# ---------------------------------------------------------------------------
# log_size_threshold
# ---------------------------------------------------------------------------


class TestLogSizeThreshold:
    """log_size verdict refuses growth rate above 1 MiB/hr by default."""

    def test_flat_log_passes(self) -> None:
        samples = [(int(t * _HOUR_NS), 100 * _MIB) for t in (0.0, 6.0, 12.0, 24.0)]
        verdict = log_size_threshold(samples)
        assert verdict.passed, verdict.detail

    def test_high_growth_fails(self) -> None:
        # 5 MiB/hr growth.
        samples = [(int(t * _HOUR_NS), int((10.0 + 5.0 * t) * _MIB)) for t in (0.0, 6.0, 12.0, 18.0, 24.0)]
        verdict = log_size_threshold(samples)
        assert not verdict.passed
        assert "slope" in verdict.detail

    def test_modest_growth_passes(self) -> None:
        # 0.2 MiB/hr growth.
        samples = [(int(t * _HOUR_NS), int((10.0 + 0.2 * t) * _MIB)) for t in (0.0, 6.0, 12.0, 18.0, 24.0)]
        verdict = log_size_threshold(samples)
        assert verdict.passed, verdict.detail

    def test_fewer_than_2_samples_skipped(self) -> None:
        verdict = log_size_threshold([(0, 1024)])
        assert verdict.passed
        assert "fewer than 2" in verdict.detail


# ---------------------------------------------------------------------------
# per_source_share_threshold
# ---------------------------------------------------------------------------


class TestPerSourceShareThreshold:
    """per_source_share verdict refuses any source's observed share outside +/-10pp."""

    _TARGETS: ClassVar[dict[str, float]] = {"github": 0.5, "pytest": 0.2, "docker": 0.2, "fs": 0.1}

    def test_balanced_share_passes(self) -> None:
        counts = {"github": 500, "pytest": 200, "docker": 200, "fs": 100}
        verdict = per_source_share_threshold(counts, self._TARGETS)
        assert verdict.passed, verdict.detail

    def test_modest_skew_passes(self) -> None:
        # github 55% (target 50%, delta 5pp), within 10pp band.
        counts = {"github": 550, "pytest": 180, "docker": 180, "fs": 90}
        verdict = per_source_share_threshold(counts, self._TARGETS)
        assert verdict.passed, verdict.detail

    def test_heavy_skew_fails(self) -> None:
        # github 80% (target 50%, delta 30pp).
        counts = {"github": 800, "pytest": 100, "docker": 50, "fs": 50}
        verdict = per_source_share_threshold(counts, self._TARGETS)
        assert not verdict.passed
        assert "github" in verdict.detail

    def test_key_mismatch_fails(self) -> None:
        counts = {"github": 500, "pytest": 500}  # missing docker, fs
        verdict = per_source_share_threshold(counts, self._TARGETS)
        assert not verdict.passed
        assert "key mismatch" in verdict.detail

    def test_zero_total_fails(self) -> None:
        counts = {"github": 0, "pytest": 0, "docker": 0, "fs": 0}
        verdict = per_source_share_threshold(counts, self._TARGETS)
        assert not verdict.passed
        assert "total emits" in verdict.detail

    def test_empty_counts_skipped(self) -> None:
        verdict = per_source_share_threshold({}, self._TARGETS)
        assert verdict.passed
        assert "no window counts" in verdict.detail
