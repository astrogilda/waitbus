"""Tests for the daemon per-delivery-cost calibration microbench.

The pure per-delivery arithmetic is exercised on synthetic deltas with
no daemon. A daemon-gated end-to-end test (skipped when ``waitbus`` is not
on PATH) runs a tiny calibration and asserts a positive, finite
per-delivery cost.
"""

from __future__ import annotations

import math
import shutil
import sys

import pytest

from benchmarks.bench_daemon_per_delivery_cost import (
    _WindowSample,
    median_per_delivery,
    per_delivery_cost,
    run_calibration,
)

# Per-delivery cost reads /proc daemon CPU counters; Linux-only.
pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="reads /proc daemon CPU counters; Linux-only",
)


def _make_sample(*, utime_delta_ns: int, n_events: int, m_subscribers: int) -> _WindowSample:
    """Build a synthetic window sample with the given utime delta."""
    deliveries = n_events * m_subscribers
    return _WindowSample(
        n_events=n_events,
        m_subscribers=m_subscribers,
        deliveries=deliveries,
        utime_delta_ns=utime_delta_ns,
        stime_delta_ns=0,
        schedstat_run_delta_ns=utime_delta_ns * 2,
        schedstat_pcount_delta=deliveries,
    )


class TestPerDeliveryCost:
    def test_basic_quotient(self) -> None:
        # 1_000_000 ns over 2000 events x 3 subs = 6000 deliveries.
        assert per_delivery_cost(cpu_delta_ns=6_000_000, n_events=2000, m_subscribers=3) == pytest.approx(1000.0)

    def test_single_subscriber(self) -> None:
        assert per_delivery_cost(cpu_delta_ns=5000, n_events=100, m_subscribers=1) == pytest.approx(50.0)

    def test_zero_events_raises(self) -> None:
        with pytest.raises(ValueError, match="n_events must be > 0"):
            per_delivery_cost(cpu_delta_ns=1000, n_events=0, m_subscribers=3)

    def test_zero_subscribers_raises(self) -> None:
        with pytest.raises(ValueError, match="m_subscribers must be > 0"):
            per_delivery_cost(cpu_delta_ns=1000, n_events=100, m_subscribers=0)

    def test_sample_per_delivery_properties(self) -> None:
        sample = _make_sample(utime_delta_ns=6_000_000, n_events=2000, m_subscribers=3)
        assert sample.deliveries == 6000
        assert sample.per_delivery_utime_ns == pytest.approx(1000.0)
        assert sample.per_delivery_schedstat_ns == pytest.approx(2000.0)
        assert sample.per_delivery_stime_ns == pytest.approx(0.0)


class TestMedianPerDelivery:
    def test_median_over_windows(self) -> None:
        samples = [
            _make_sample(utime_delta_ns=6_000_000, n_events=2000, m_subscribers=3),  # 1000 ns/delivery
            _make_sample(utime_delta_ns=12_000_000, n_events=2000, m_subscribers=3),  # 2000 ns/delivery
            _make_sample(utime_delta_ns=18_000_000, n_events=2000, m_subscribers=3),  # 3000 ns/delivery
        ]
        median = median_per_delivery(samples, accessor=lambda s: s.per_delivery_utime_ns)
        assert median == pytest.approx(2000.0)

    def test_median_schedstat_accessor(self) -> None:
        samples = [
            _make_sample(utime_delta_ns=3_000_000, n_events=1000, m_subscribers=1),  # schedstat 6000 ns/delivery
            _make_sample(utime_delta_ns=6_000_000, n_events=1000, m_subscribers=1),  # schedstat 12000 ns/delivery
        ]
        median = median_per_delivery(samples, accessor=lambda s: s.per_delivery_schedstat_ns)
        assert median == pytest.approx(9000.0)

    def test_empty_returns_zero_sentinel(self) -> None:
        assert median_per_delivery([], accessor=lambda s: s.per_delivery_utime_ns) == 0.0


@pytest.mark.skipif(shutil.which("waitbus") is None, reason="waitbus not on PATH; daemon-gated test skipped")
def test_tiny_calibration_produces_positive_finite_cost() -> None:
    """A tiny unpinned calibration produces a positive, finite per-delivery cost."""
    verdict = run_calibration(
        n_events=200,
        subscriber_sweep=(1,),
        emit_rate_hz=1000.0,
        repeats=2,
        allow_unpinned=True,
    )
    assert verdict["bench"] == "bench_daemon_per_delivery_cost"
    results = verdict["results"]
    assert isinstance(results, list)
    assert len(results) == 1
    row = results[0]
    assert isinstance(row, dict)
    assert row["m_subscribers"] == 1
    assert row["windows_kept"] >= 1
    util = row["median_per_delivery_utime_ns"]
    sched = row["median_per_delivery_schedstat_ns"]
    assert isinstance(util, float)
    assert isinstance(sched, float)
    assert math.isfinite(util)
    assert math.isfinite(sched)
    # The daemon does real fan-out work per delivery; schedstat run-time
    # (nanosecond resolution, aggregated across TIDs) must be strictly
    # positive. utime can quantize to 0 on a tiny burst (1-jiffy floor),
    # so only schedstat is asserted strictly positive here.
    assert sched > 0.0
    assert util >= 0.0
