"""Unit tests for the soak fault-injection verdict thresholds.

These are pure-function tests over the probe-outcome dicts and the
daemon close-reason tally -- no daemon or socket required. The probes
themselves (which need a live daemon) are exercised end-to-end by
``tests/test_drain_smoke.py``.
"""

from __future__ import annotations

from scripts.soak._fault_injection import (
    fault_injection_close_reason_consistency_threshold,
    fault_injection_coverage_threshold,
)


def _outcome(
    axis: str,
    *,
    observed: bool,
    skipped_intentionally: bool = False,
    detail: str = "",
) -> dict[str, object]:
    return {
        "axis": axis,
        "offset_sec": 0.0,
        "observed": observed,
        "observed_reason": None,
        "skipped_intentionally": skipped_intentionally,
        "detail": detail,
    }


# --- coverage threshold -----------------------------------------------------


def test_coverage_passes_when_no_axes_configured() -> None:
    verdict = fault_injection_coverage_threshold([], frozenset())
    assert verdict.passed
    assert "no fault-injection probes configured" in verdict.detail


def test_coverage_passes_when_every_axis_observed() -> None:
    outcomes = [
        _outcome("version_reject", observed=True),
        _outcome("heartbeat_lag", observed=True),
    ]
    verdict = fault_injection_coverage_threshold(outcomes, frozenset({"version_reject", "heartbeat_lag"}))
    assert verdict.passed


def test_coverage_passes_on_intentional_skip() -> None:
    outcomes = [_outcome("replay_lag_eviction", observed=False, skipped_intentionally=True)]
    verdict = fault_injection_coverage_threshold(outcomes, frozenset({"replay_lag_eviction"}))
    assert verdict.passed
    assert "intentional skips" in verdict.detail


def test_coverage_fails_when_probe_never_ran() -> None:
    verdict = fault_injection_coverage_threshold([], frozenset({"heartbeat_lag"}))
    assert not verdict.passed
    assert "never ran" in verdict.detail


def test_coverage_fails_on_wrong_frame() -> None:
    outcomes = [_outcome("token_reject", observed=False, skipped_intentionally=False, detail="kind=foo")]
    verdict = fault_injection_coverage_threshold(outcomes, frozenset({"token_reject"}))
    assert not verdict.passed
    assert "token_reject" in verdict.detail


# --- close-reason consistency threshold -------------------------------------


def test_consistency_passes_with_no_outcomes() -> None:
    assert fault_injection_close_reason_consistency_threshold([], {}).passed


def test_consistency_passes_when_eviction_reason_present() -> None:
    outcomes = [
        _outcome("replay_lag_eviction", observed=True),
        _outcome("heartbeat_lag", observed=True),
    ]
    close_reasons = {"replay_lag_limit_exceeded": 1, "heartbeat_lag": 1}
    assert fault_injection_close_reason_consistency_threshold(outcomes, close_reasons).passed


def test_consistency_fails_when_observed_eviction_missing_from_tally() -> None:
    outcomes = [_outcome("heartbeat_lag", observed=True)]
    verdict = fault_injection_close_reason_consistency_threshold(outcomes, {"lag_limit_exceeded": 3})
    assert not verdict.passed
    assert "heartbeat_lag" in verdict.detail


def test_consistency_ignores_skipped_eviction_probes() -> None:
    # A skipped (not observed) eviction probe evicted nothing, so there is
    # nothing to account for in the close-reason tally.
    outcomes = [_outcome("heartbeat_lag", observed=False, skipped_intentionally=True)]
    assert fault_injection_close_reason_consistency_threshold(outcomes, {}).passed


def test_consistency_ignores_reject_class_axes() -> None:
    # token/version are rejected pre-registration -> no subscriber_closed
    # event, so they are out of scope for the close-reason check even when
    # observed and absent from the tally.
    outcomes = [
        _outcome("token_reject", observed=True),
        _outcome("version_reject", observed=True),
    ]
    assert fault_injection_close_reason_consistency_threshold(outcomes, {}).passed
