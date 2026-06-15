"""Unit tests for ``benchmarks.bench_polling_vs_subscribe_llm_agent``.

Two test scopes:

1. Bench-local helpers (percentile + bootstrap CI + Mann-Whitney
   wrapper + cache-state classifier + aggregator) — fully network-free
   and credential-free, exercised directly.
2. End-to-end smoke: the bench's ``main`` is invoked under ``--smoke``
   with the real LLM gates off (``--skip-real-llm``) and the
   subprocess + bus + OpenAI integration points patched so the bench
   exercises its full orchestration body without spending any tokens.

Tests SKIP cleanly (rather than fail) when:

- the host is not Linux (the bench is Linux-only by construction);
- ``OPENAI_API_KEY`` is not readable from the keyring AND the test
  needs real-LLM gating;
- ``claude`` / ``gemini`` are not on PATH AND the test needs them;
- the bench's preflight assertions raise for any reason on this host.
"""

from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import msgspec
import pytest

from benchmarks._bench_preflight import PreflightError
from benchmarks._bench_shared import (
    ClaudeEnvelope,
    GeminiEnvelope,
    OpenAIEnvelope,
    _classify_claude_cache_state,
    resolve_bench_log_paths,
)
from benchmarks.bench_polling_vs_subscribe_llm_agent import (
    _SWARM_SPEC,
    ExperimentAVerdict,
    IterationRow,
    _aggregate_external_state,
    _bootstrap_median_ci_ns,
    _build_limitations,
    _cache_state_distribution,
    _compute_cost,
    _drain_swarm_reactions,
    _invariant_failure_rates,
    _percentile_ns,
    _summarise_per_driver,
    main,
)
from scripts.stress._controller import _Child
from scripts.stress._real_drivers import FRAMEWORK_ORDER

# ---------------------------------------------------------------------
# Helper: build a synthetic IterationRow with sensible defaults.
# ---------------------------------------------------------------------


def _row(
    *,
    iter_id: int = 0,
    arm: str = "subscribe",
    driver: str = "shell-control",
    latency_ns: int = 1_000_000,
    invariant_failed: bool = False,
    cache_state: str = "NA",
    claude_env: ClaudeEnvelope | None = None,
    gemini_env: GeminiEnvelope | None = None,
    openai_env: OpenAIEnvelope | None = None,
    delivery_mode: str = "live",
) -> IterationRow:
    """Construct a minimal IterationRow for aggregator unit tests."""
    return IterationRow(
        iter_id=iter_id,
        arm=arm,
        driver=driver,
        sentinel="abcd" * 50,
        t_send_ns=1_000_000_000,
        t_observe_ns=1_000_000_000 + latency_ns,
        latency_ns=latency_ns,
        cache_state=cache_state,
        claude_env=claude_env,
        gemini_env=gemini_env,
        openai_env=openai_env,
        invariant_failed=invariant_failed,
        invariant_failure_field=("err" if invariant_failed else None),
        delivery_mode=delivery_mode,
    )


def _claude_env(
    *,
    cost_usd: float | None = None,
    stop_reason: str | None = "end_turn",
    is_error: bool = False,
    model: str = "claude-haiku-4-5-20251001",
) -> ClaudeEnvelope:
    """Build a minimal ClaudeEnvelope for fixture rows."""
    return ClaudeEnvelope(
        input_tokens_visible=10,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
        output_tokens=5,
        billed_input_tokens=10,
        total_billed_tokens=15,
        cost_usd=cost_usd,
        model=model,
        stop_reason=stop_reason,
        is_error=is_error,
        api_error_status=None,
        terminal_reason="completed",
        num_turns=1,
    )


# ---------------------------------------------------------------------
# Percentile helpers.
# ---------------------------------------------------------------------


def test_percentile_ns_empty_returns_zero() -> None:
    assert _percentile_ns([], 0.50) == 0
    assert _percentile_ns([], 0.99) == 0


def test_percentile_ns_single_value() -> None:
    assert _percentile_ns([42], 0.50) == 42
    assert _percentile_ns([42], 0.99) == 42


def test_percentile_ns_monotonic_in_p() -> None:
    """A wider percentile (p) yields a value greater than or equal to a smaller p."""
    values = list(range(1, 101))
    p50 = _percentile_ns(values, 0.50)
    p95 = _percentile_ns(values, 0.95)
    p99 = _percentile_ns(values, 0.99)
    assert p50 <= p95 <= p99
    assert p50 == 50


def test_percentile_ns_extremes() -> None:
    values = [10, 20, 30, 40, 50]
    assert _percentile_ns(values, 0.0) == 10
    assert _percentile_ns(values, 1.0) == 50


# ---------------------------------------------------------------------
# Bootstrap CI.
# ---------------------------------------------------------------------


def test_bootstrap_median_ci_empty_returns_zeros() -> None:
    lo, hi = _bootstrap_median_ci_ns([])
    assert (lo, hi) == (0, 0)


def test_bootstrap_median_ci_contains_true_median() -> None:
    """A 95% bootstrap CI on a tight distribution brackets the true median."""
    values = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109] * 5
    lo, hi = _bootstrap_median_ci_ns(values, iterations=500)
    true_median = 104  # ~median of 100..109 over 50 samples
    assert lo <= true_median <= hi


def test_bootstrap_median_ci_is_deterministic_under_pythonhashseed_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two runs with PYTHONHASHSEED=0 produce identical CI bands."""
    monkeypatch.setenv("PYTHONHASHSEED", "0")
    values = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    a = _bootstrap_median_ci_ns(values, iterations=200)
    b = _bootstrap_median_ci_ns(values, iterations=200)
    assert a == b


# ---------------------------------------------------------------------
# Cache-state classifier.
# ---------------------------------------------------------------------


def test_classify_claude_cache_state_cold_on_zero_cache_read() -> None:
    assert _classify_claude_cache_state(visible=100, cache_read=0, billed_input=100) == "COLD"


def test_classify_claude_cache_state_warming_below_half() -> None:
    assert _classify_claude_cache_state(visible=100, cache_read=20, billed_input=200) == "WARMING"


def test_classify_claude_cache_state_warm_at_or_above_half() -> None:
    assert _classify_claude_cache_state(visible=100, cache_read=150, billed_input=200) == "WARM"


def test_classify_claude_cache_state_zero_billed_falls_back_to_cold() -> None:
    """A malformed / refusal envelope with zero billed input maps to COLD, not NA."""
    assert _classify_claude_cache_state(visible=0, cache_read=0, billed_input=0) == "COLD"


# ---------------------------------------------------------------------
# Aggregators (per-driver stats, marginals, distributions, cost).
# ---------------------------------------------------------------------


def test_summarise_per_driver_excludes_invariant_failed() -> None:
    rows = [
        _row(driver="pydantic", latency_ns=10_000_000),
        _row(driver="pydantic", latency_ns=20_000_000),
        _row(driver="pydantic", latency_ns=30_000_000, invariant_failed=True),
    ]
    stats = _summarise_per_driver(rows, driver="pydantic")
    assert stats.n_iterations == 2
    # Median over [10ms, 20ms] is 15ms in ns.
    assert stats.median_end_to_end_latency_ns in {10_000_000, 15_000_000, 20_000_000}
    # The bus metric is computed against the same row set; the test
    # _row() helper leaves the monotonic anchors at their default 0
    # so the bus latency is 0 here. The aggregator path is exercised
    # by the dedicated bus-only test below.
    assert stats.median_bus_latency_ns == 0


def test_summarise_per_driver_empty_returns_zero_stats() -> None:
    stats = _summarise_per_driver([], driver="pydantic")
    assert stats.n_iterations == 0
    assert stats.median_end_to_end_latency_ns == 0
    assert stats.p95_end_to_end_latency_ns == 0
    assert stats.median_bus_latency_ns == 0
    assert stats.p95_bus_latency_ns == 0


def test_summarise_per_driver_bus_latency_decoupled_from_end_to_end() -> None:
    """Bus latency is monotonic-pair (seed-emit -> wake), independent of LLM call time.

    Two rows: same end-to-end latency (50ms each) but very different bus
    latencies (20ms vs 80ms). The aggregator must surface both medians
    correctly and independently. Validates the launch-claim narrative
    that subscribe-arm bus latency can be reported separately from the
    full driver-lifecycle reaction latency.
    """
    rows = [
        IterationRow(
            iter_id=0,
            arm="subscribe",
            driver="pydantic",
            sentinel="a" * 200,
            t_send_ns=1_000_000_000,
            t_observe_ns=1_050_000_000,
            latency_ns=50_000_000,
            cache_state="NA",
            claude_env=None,
            gemini_env=None,
            openai_env=None,
            invariant_failed=False,
            invariant_failure_field=None,
            delivery_mode="live",
            t_seed_emit_monotonic_ns=2_000_000_000,
            wake_monotonic_ns=2_020_000_000,
        ),
        IterationRow(
            iter_id=1,
            arm="subscribe",
            driver="pydantic",
            sentinel="b" * 200,
            t_send_ns=3_000_000_000,
            t_observe_ns=3_050_000_000,
            latency_ns=50_000_000,
            cache_state="NA",
            claude_env=None,
            gemini_env=None,
            openai_env=None,
            invariant_failed=False,
            invariant_failure_field=None,
            delivery_mode="live",
            t_seed_emit_monotonic_ns=4_000_000_000,
            wake_monotonic_ns=4_080_000_000,
        ),
    ]
    stats = _summarise_per_driver(rows, driver="pydantic")
    assert stats.n_iterations == 2
    assert stats.median_end_to_end_latency_ns == 50_000_000
    # Median over [20ms, 80ms] bus latencies; _percentile_ns linear-
    # interp at p=0.5 over a 2-element sorted set returns the lower of
    # the two midpoints, so accept either bracketing value.
    assert stats.median_bus_latency_ns in {20_000_000, 50_000_000, 80_000_000}
    # p99 over a 2-element set is linear-interpolated at p=0.99 (per
    # ``_percentile_ns``) which lands strictly between the two values
    # and very close to the upper bound.
    assert 79_000_000 <= stats.p99_bus_latency_ns <= 80_000_000


def test_invariant_failure_rates_includes_all_frameworks() -> None:
    rows = [_row(driver="pydantic")]
    rates = _invariant_failure_rates(rows)
    for framework in FRAMEWORK_ORDER:
        assert framework in rates
    assert rates["pydantic"] == 0.0
    # A driver with no rows is reported as 0.0 (not NaN, not missing).
    assert rates["shell-control"] == 0.0


def test_invariant_failure_rate_one_third() -> None:
    rows = [
        _row(driver="pydantic", invariant_failed=False),
        _row(driver="pydantic", invariant_failed=False),
        _row(driver="pydantic", invariant_failed=True),
    ]
    rates = _invariant_failure_rates(rows)
    assert abs(rates["pydantic"] - (1.0 / 3.0)) < 1e-9


# ---------------------------------------------------------------------
# _classify_invariant pure helper.
# ---------------------------------------------------------------------


def _gemini_env(
    *,
    stop_reason: str | None = None,
    is_error: bool = False,
    model: str = "gemini-2.5-flash",
) -> GeminiEnvelope:
    """Build a minimal GeminiEnvelope fixture for invariant gate tests."""
    return GeminiEnvelope(
        prompt_tokens=12,
        candidates_tokens=4,
        thoughts_tokens=0,
        cached_tokens=0,
        tool_tokens=0,
        total_tokens_reported=16,
        total_tokens_recomputed=16,
        cost_usd=None,
        model=model,
        stop_reason=stop_reason,
        is_error=is_error,
        api_error_status=None,
        terminal_reason=None,
        num_turns=None,
    )


def _openai_env(
    *,
    finish_reason: str | None = "stop",
    stop_reason: str | None = None,
    is_error: bool = False,
    cost_usd: float | None = 1e-6,
    model: str = "openai-gpt-4o-mini",
) -> OpenAIEnvelope:
    """Build a minimal OpenAIEnvelope fixture for invariant gate tests."""
    return OpenAIEnvelope(
        model=model,
        input_tokens=10,
        output_tokens=4,
        cached_tokens=0,
        finish_reason=finish_reason,
        stop_reason=stop_reason,
        is_error=is_error,
        cost_usd=cost_usd,
    )


def _observed_reaction(framework: str = "pydantic") -> Any:
    """Build a minimal ObservedReaction stub for invariant gate tests."""
    from scripts.stress._context import ObservedReaction

    return ObservedReaction(
        framework=framework,
        fw_id=f"{framework}-1",
        seed_delivery_id="seed-fake",
        reaction_delivery_id="reaction-fake",
        received_wall_ns=1_700_000_000_000_000_000,
        reaction_latency_ms=5.0,
        token_usage=None,
    )


def test_classify_invariant_reaction_missing_no_early_wake() -> None:
    """No reaction + no early marker -> ``reaction_missing`` (subscribe race)."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="pydantic",
        reaction=None,
        claude_env=None,
        gemini_env=None,
        openai_env=None,
        early_wake_received=False,
    )
    assert failed is True
    assert field == "reaction_missing"


def test_classify_invariant_reaction_missing_with_early_wake() -> None:
    """No reaction + early marker present -> ``llm_timeout_or_crash``."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="pydantic",
        reaction=None,
        claude_env=None,
        gemini_env=None,
        openai_env=None,
        early_wake_received=True,
    )
    assert failed is True
    assert field == "llm_timeout_or_crash"


def test_classify_invariant_claude_passes_on_clean_completion() -> None:
    """A claude envelope with stop_reason=end_turn passes the gate."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="claude-cli",
        reaction=_observed_reaction("claude-cli"),
        claude_env=_claude_env(stop_reason="end_turn"),
        gemini_env=None,
        openai_env=None,
        early_wake_received=True,
    )
    assert failed is False
    assert field is None


def test_classify_invariant_claude_fails_on_refusal() -> None:
    """A claude envelope with stop_reason=refusal trips the gate."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="claude-cli",
        reaction=_observed_reaction("claude-cli"),
        claude_env=_claude_env(stop_reason="refusal", is_error=True),
        gemini_env=None,
        openai_env=None,
        early_wake_received=True,
    )
    assert failed is True
    assert field == "is_error"


def test_classify_invariant_claude_fails_on_unrecognised_stop_reason() -> None:
    """An unknown claude stop_reason flags an unrecognised terminal state."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="claude-cli",
        reaction=_observed_reaction("claude-cli"),
        claude_env=_claude_env(stop_reason="mystery"),
        gemini_env=None,
        openai_env=None,
        early_wake_received=True,
    )
    assert failed is True
    assert field == "stop_reason=mystery"


def test_classify_invariant_gemini_passes_on_clean_completion() -> None:
    """A gemini envelope with stop_reason=None passes the gate."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="gemini-cli",
        reaction=_observed_reaction("gemini-cli"),
        claude_env=None,
        gemini_env=_gemini_env(stop_reason=None),
        openai_env=None,
        early_wake_received=True,
    )
    assert failed is False
    assert field is None


def test_classify_invariant_gemini_fails_on_refusal() -> None:
    """A gemini envelope with stop_reason=refusal trips the gate."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="gemini-cli",
        reaction=_observed_reaction("gemini-cli"),
        claude_env=None,
        gemini_env=_gemini_env(stop_reason="refusal"),
        openai_env=None,
        early_wake_received=True,
    )
    assert failed is True
    assert field == "stop_reason=refusal"


def test_classify_invariant_openai_passes_on_clean_completion() -> None:
    """An openai envelope with finish_reason=stop passes the gate."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="pydantic",
        reaction=_observed_reaction("pydantic"),
        claude_env=None,
        gemini_env=None,
        openai_env=_openai_env(finish_reason="stop"),
        early_wake_received=True,
    )
    assert failed is False
    assert field is None


def test_classify_invariant_openai_fails_on_content_filter() -> None:
    """An openai envelope with finish_reason=content_filter trips the gate."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="pydantic",
        reaction=_observed_reaction("pydantic"),
        claude_env=None,
        gemini_env=None,
        openai_env=_openai_env(finish_reason="content_filter"),
        early_wake_received=True,
    )
    assert failed is True
    assert field == "finish_reason=content_filter"


def test_classify_invariant_openai_fails_on_is_error_outranks_finish_reason() -> None:
    """``is_error=True`` outranks a benign finish_reason; field is the deeper signal."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="pydantic",
        reaction=_observed_reaction("pydantic"),
        claude_env=None,
        gemini_env=None,
        openai_env=_openai_env(finish_reason="stop", is_error=True),
        early_wake_received=True,
    )
    assert failed is True
    assert field == "is_error"


def test_classify_invariant_shell_control_passes_with_reaction() -> None:
    """shell-control has no envelope; an observed reaction passes the gate."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _classify_invariant

    failed, field = _classify_invariant(
        framework="shell-control",
        reaction=_observed_reaction("shell-control"),
        claude_env=None,
        gemini_env=None,
        openai_env=None,
        early_wake_received=True,
    )
    assert failed is False
    assert field is None


def test_cache_state_distribution_per_driver_buckets() -> None:
    rows = [
        _row(driver="claude-cli", cache_state="COLD"),
        _row(driver="claude-cli", cache_state="COLD"),
        _row(driver="claude-cli", cache_state="WARM"),
        _row(driver="shell-control", cache_state="NA"),
    ]
    dist = _cache_state_distribution(rows)
    assert dist["claude-cli"] == {"COLD": 2, "WARM": 1}
    assert dist["shell-control"] == {"NA": 1}
    # Drivers with zero observations are present with empty buckets.
    assert dist["gemini-cli"] == {}


def test_compute_cost_sums_known_and_counts_unknown() -> None:
    gemini_env = GeminiEnvelope(
        prompt_tokens=10,
        candidates_tokens=5,
        thoughts_tokens=0,
        cached_tokens=0,
        tool_tokens=0,
        total_tokens_reported=15,
        total_tokens_recomputed=15,
        cost_usd=None,
        model="gemini-2.5-flash",
        stop_reason=None,
        is_error=False,
        api_error_status=None,
        terminal_reason=None,
        num_turns=1,
    )
    rows = [
        _row(driver="pydantic", openai_env=_openai_env(cost_usd=0.0005)),
        _row(driver="claude-cli", claude_env=_claude_env(cost_usd=0.0001)),
        _row(driver="claude-cli", claude_env=_claude_env(cost_usd=0.0002)),
        _row(driver="claude-cli", claude_env=_claude_env(cost_usd=None)),
        _row(driver="gemini-cli", gemini_env=gemini_env),
        _row(driver="shell-control"),
    ]
    metered, notional, unknown = _compute_cost(rows)
    # Only OpenAI is genuinely metered: the claude subscription cost is
    # NOT folded into the real-dollar total.
    assert metered == pytest.approx(0.0005)
    # claude subscription cost (notional) is surfaced separately.
    assert notional == pytest.approx(0.0003)
    # 1 claude None + 1 gemini = 2 unknown cost rows.
    assert unknown == 2


def test_verdict_has_no_marginal_test_machinery() -> None:
    """Bench A is a per-driver median description; it carries no marginal
    Mann-Whitney / Bonferroni fields on the verdict struct."""
    assert not hasattr(ExperimentAVerdict, "marginal_tests")
    assert not hasattr(ExperimentAVerdict, "bonferroni_alpha_per_marginal")
    assert not hasattr(ExperimentAVerdict, "h1_h2_h3_any_rejected")


# ---------------------------------------------------------------------
# ExternalStateReport aggregation.
# ---------------------------------------------------------------------


def test_aggregate_external_state_merges_observed_models() -> None:
    from benchmarks._bench_shared import capture_external_state

    base = capture_external_state(openai_api_key_present=False)
    row = _row(driver="claude-cli", claude_env=_claude_env(model="claude-haiku-4-5-20251001"))
    out = _aggregate_external_state(base, [row])
    assert "claude-haiku-4-5-20251001" in out.anthropic_response_model_set


def test_aggregate_external_state_increments_moderation_on_refusal() -> None:
    from benchmarks._bench_shared import capture_external_state

    base = capture_external_state(openai_api_key_present=False)
    refusal = _row(
        driver="claude-cli",
        claude_env=_claude_env(stop_reason="refusal", is_error=True),
    )
    out = _aggregate_external_state(base, [refusal])
    assert out.moderation_event_count >= 1
    assert out.stop_reason_distribution.get("refusal", 0) >= 1


def test_replay_contamination_rates_counts_replay_rows_per_driver() -> None:
    """Per-driver replay rate equals replay rows over total rows."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _replay_contamination_rates

    rows = [
        _row(driver="claude-cli", delivery_mode="live"),
        _row(driver="claude-cli", delivery_mode="replay"),
        _row(driver="claude-cli", delivery_mode="live"),
        _row(driver="claude-cli", delivery_mode="live"),
        _row(driver="gemini-cli", delivery_mode="live"),
        _row(driver="gemini-cli", delivery_mode="live"),
    ]
    rates = _replay_contamination_rates(rows)
    assert rates["claude-cli"] == 0.25
    assert rates["gemini-cli"] == 0.0
    # Drivers with no observations report 0.0 rather than raising.
    assert rates["shell-control"] == 0.0


def test_replay_contamination_rates_excludes_unknown_from_numerator() -> None:
    """``"unknown"`` rows (no early marker, driver crashed pre-subscribe)
    do not count as replay so the rate is not falsely inflated."""
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _replay_contamination_rates

    rows = [
        _row(driver="pydantic", delivery_mode="unknown"),
        _row(driver="pydantic", delivery_mode="live"),
    ]
    rates = _replay_contamination_rates(rows)
    assert rates["pydantic"] == 0.0


def test_replay_contamination_gate_passes_when_every_driver_is_under_threshold() -> None:
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _replay_contamination_gate_passed

    assert _replay_contamination_gate_passed(
        {"claude-cli": 0.04, "gemini-cli": 0.0, "pydantic": 0.05},
        threshold=0.05,
    )
    assert not _replay_contamination_gate_passed(
        {"claude-cli": 0.06, "gemini-cli": 0.0},
        threshold=0.05,
    )


def test_summarise_per_driver_excludes_replay_rows_from_latency_aggregate() -> None:
    """A replay row's latency is excluded from the per-driver median.

    Pins the live-fan-out aggregation contract: a row that arrived via
    the daemon's seq-replay window is operator-visible separately via
    the replay-contamination rate; the median latency reflects only
    the live ``_fan_out`` ingest cost the bench's hypothesis claims.
    """
    from benchmarks.bench_polling_vs_subscribe_llm_agent import _summarise_per_driver

    rows = [
        _row(driver="shell-control", latency_ns=1_000_000, delivery_mode="live"),
        _row(driver="shell-control", latency_ns=2_000_000, delivery_mode="live"),
        # The replay row's latency would skew the median upward; the
        # aggregation must skip it.
        _row(driver="shell-control", latency_ns=100_000_000, delivery_mode="replay"),
    ]
    stats = _summarise_per_driver(rows, driver="shell-control")
    assert stats.n_iterations == 2
    # Median over the two live rows -- the 100ms replay row is excluded.
    assert stats.median_end_to_end_latency_ns < 10_000_000


def test_iteration_row_carries_envelope_substruct_per_driver() -> None:
    """A claude row has claude_env populated, the others None."""
    claude_row = _row(driver="claude-cli", claude_env=_claude_env())
    assert claude_row.claude_env is not None
    assert claude_row.gemini_env is None
    assert claude_row.openai_env is None
    openai_capture = OpenAIEnvelope(
        model="gpt-4o-mini-2024-07-18",
        input_tokens=10,
        output_tokens=5,
        cached_tokens=0,
        finish_reason="stop",
    )
    openai_row = _row(driver="pydantic", openai_env=openai_capture)
    assert openai_row.openai_env is not None
    assert openai_row.claude_env is None
    assert openai_row.gemini_env is None


# ---------------------------------------------------------------------
# Limitations list.
# ---------------------------------------------------------------------


def test_build_limitations_contains_all_required_caveats() -> None:
    """Every fix-spec-required limitation appears in the recorded list."""
    from benchmarks._bench_shared import BENCH_GEMINI_MODEL

    items = _build_limitations()
    text = " ".join(items)
    assert "p99" in text  # p99-not-for-ranking limitation
    assert BENCH_GEMINI_MODEL in text  # floating-alias limitation tracks the canonical pin
    assert "force_cold_cache_prefix" in text  # cache-decay limitation
    assert "--seed" in text and "--temperature" in text  # sampling black-box
    assert "PYTHONHASHSEED" in text  # asyncio jitter limitation
    assert "OPENAI_API_KEY" in text  # key-not-persisted limitation
    # No marginal hypothesis test in Bench A.
    assert "Bonferroni" not in text
    assert "median latency description" in text
    assert "historical" in text  # historical artifacts unmodified
    assert "pilot" in text  # pilot gate limitation


# ---------------------------------------------------------------------
# Output-path resolution.
# ---------------------------------------------------------------------


def test_resolve_log_paths_with_directory_creates_timestamped_triple(tmp_path: Path) -> None:
    verdict_path, progress_path, log_path = resolve_bench_log_paths(
        bench_name="bench_xyz", output=tmp_path / "foo.verdict.json"
    )
    assert verdict_path.parent == tmp_path
    assert verdict_path.name.endswith(".verdict.json")
    assert progress_path.name.endswith(".progress.jsonl")
    assert log_path.name.endswith(".log")
    # All three files share a common stem.
    common_stem = verdict_path.name[: -len(".verdict.json")]
    assert progress_path.name == f"{common_stem}.progress.jsonl"
    assert log_path.name == f"{common_stem}.log"


# ---------------------------------------------------------------------
# Verdict struct round-trip.
# ---------------------------------------------------------------------


def test_experiment_a_verdict_msgspec_roundtrip() -> None:
    from benchmarks._bench_shared import capture_external_state

    verdict = ExperimentAVerdict(
        bench_name="poll_vs_subscribe_llm",
        started_at_ns=1_700_000_000_000_000_000,
        ended_at_ns=1_700_000_001_000_000_000,
        n_iterations_per_arm=2,
        smoke=True,
        external_state=capture_external_state(openai_api_key_present=False),
        rows=[_row()],
        per_arm_per_driver_stats_poll={},
        per_arm_per_driver_stats_subscribe={},
        per_driver_invariant_failure_rate={"shell-control": 0.0},
        per_driver_replay_contamination_rate={"shell-control": 0.0},
        replay_contamination_threshold=0.05,
        replay_contamination_gate_passed=True,
        cache_state_distribution={"shell-control": {"NA": 1}},
        cost_usd_total=None,
        notional_subscription_cost_usd=None,
        cost_unknown_count=0,
        cache_contaminated_count=0,
        pilot_sigma_ms=5.0,
        pilot_sigma_plan_ms=10.0,
        pilot_sigma_gate_factor=2.0,
        pilot_sigma_gate_ms_used=20.0,
        pilot_passed=True,
        max_cost_usd_budget=5.0,
        max_cost_usd_observed=0.0,
        aborted_on_budget=False,
        limitations=_build_limitations(),
        pilot_skipped=False,
        pilot_skipped_reason=None,
    )
    encoded = msgspec.json.encode(verdict)
    decoded = msgspec.json.decode(encoded, type=ExperimentAVerdict)
    assert decoded.bench_name == "poll_vs_subscribe_llm"
    assert decoded.n_iterations_per_arm == 2
    assert decoded.smoke is True


# ---------------------------------------------------------------------
# Swarm spec contract.
# ---------------------------------------------------------------------


def test_swarm_spec_is_five_drivers_one_each() -> None:
    assert {framework: 1 for framework in FRAMEWORK_ORDER} == _SWARM_SPEC
    assert sum(_SWARM_SPEC.values()) == 5


# ---------------------------------------------------------------------
# Preflight failure paths (the bench's main() returns non-zero on
# PreflightError without spending any tokens).
# ---------------------------------------------------------------------


def test_main_returns_two_on_preflight_failure(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Preflight failure aborts ``main`` with rc=2 BEFORE any subprocess spawn."""

    def fake_preflight(*args: Any, **kwargs: Any) -> Any:
        raise PreflightError("preflight: synthetic test failure")

    output_dir = tmp_path / "out"
    with patch(
        "benchmarks.bench_polling_vs_subscribe_llm_agent.run_preflight_assertions",
        fake_preflight,
    ):
        rc = main(["--smoke", "--n", "2", "--output", str(output_dir)])
    assert rc == 2
    captured = capsys.readouterr()
    assert "preflight failed" in captured.err
    assert "synthetic test failure" in captured.err


def test_main_returns_two_when_openai_keyring_missing_and_required_llm(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The real-LLM gate path aborts with rc=2 if the keyring lookup is empty."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench gate")
    output_dir = tmp_path / "out"
    with patch(
        "benchmarks._bench_preflight.read_openai_key_from_keyring",
        return_value=None,
    ):
        rc = main(["--smoke", "--n", "2", "--output", str(output_dir)])
    assert rc == 2
    captured = capsys.readouterr()
    assert "OPENAI_API_KEY" in captured.err or "preflight" in captured.err


# ---------------------------------------------------------------------
# Live preflight gate: the bench's ``main`` invocation with all gates
# off is run end-to-end only if the host can support it (no external
# CLIs / keys required). This validates the full body executes — minus
# the LLM calls and the daemon — and produces a verdict file.
# ---------------------------------------------------------------------


def _patch_bench_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows_per_iter: int = 5,
) -> None:
    """Patch the bench's runtime so a smoke invocation does not require a daemon.

    Replaces ``_spawn_daemon`` with a no-op, ``_emit_seed_event`` with
    a synthetic delivery id, and ``spawn_n_heterogeneous`` with a fake
    children list that returns the expected wake-marker lines. The
    bench's per-iteration latency capture, row assembly, and verdict
    serialisation still run.
    """
    import benchmarks.bench_polling_vs_subscribe_llm_agent as bench_module
    from scripts.stress._controller import _Child

    class _StaticProc:
        def __init__(self, framework: str) -> None:
            self.pid = 12345
            self.returncode: int | None = 0
            self._framework = framework

        def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
            # Two markers, in the order a real driver emits them: the
            # early ``WAKE_RECEIVED`` (pre-LLM, carries cross-process
            # monotonic anchors) and the canonical ``DRIVER_REACTED``
            # (post-LLM, carries the reaction id + token envelope).
            # ``t_sub_monotonic_ns`` is BELOW the fake
            # ``seed_emit_monotonic_ns=1_000_000_000_000_000`` returned
            # from ``fake_emit_seed`` so the bench classifies the
            # delivery mode as ``"live"`` (the common-case path).
            #
            # Drive the marker bytes through the real emitter via
            # ``redirect_stdout`` so the fake stays in lockstep with
            # any future change to the marker grammar -- no synthetic
            # hand-formatted strings in test code.
            import contextlib
            import io

            from scripts.stress._real_drivers import _emit_early_wake_marker, _emit_wake_marker

            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                _emit_early_wake_marker(
                    framework=self._framework,
                    fw_id=f"{self._framework}-1",
                    seed_id="BENCH_FAKE_SEED",
                    wake_monotonic_ns=1_100_000_000_000_000,
                    t_sub_monotonic_ns=500_000_000_000_000,
                    t_import_done_monotonic_ns=300_000_000_000_000,
                )
                _emit_wake_marker(
                    framework=self._framework,
                    fw_id=f"{self._framework}-1",
                    seed_id="BENCH_FAKE_SEED",
                    reaction_id="fake-reaction",
                    wall_ns=1_700_000_000_000_000_000,
                    token_usage=None,
                    provider="offline-fake",
                )
            return (buffer.getvalue().encode(), b"")

        def terminate(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> int:
            return 0

        def poll(self) -> int | None:
            return 0

    def fake_spawn_daemon(env: dict[str, str], waitbus_path: str, socket_path: Path) -> _Child:
        # Bench expects a _Child it can terminate at teardown. Return a
        # static fake whose terminate is a no-op.
        return _Child(role="daemon", proc=_StaticProc("daemon"))  # type: ignore[arg-type]

    def fake_emit_seed(
        *,
        seed_scope_id: str,
        db_path: Path,
        doorbell_path: Path,
        source: str = "agent",
        event_type: str = "agent_message",
    ) -> tuple[str, int, int]:
        # The bench's per-iteration loop threads the picked seed source
        # / event_type into _emit_seed_event and now also consumes the
        # cross-process monotonic anchor returned alongside the wall-
        # clock anchor; the fake matches the real shape so the bench's
        # source-mix codepath does not crash the patched-runtime tests.
        _ = source
        _ = event_type
        return "BENCH_FAKE_SEED", 1_700_000_000_000_000_000, 1_000_000_000_000_000

    def fake_emit_anchor(
        *,
        seed_scope_id: str,
        db_path: Path,
        doorbell_path: Path,
        repo: str = "bench",
        ingest_method: str = "",
        delivery_id_prefix: str = "",
    ) -> str:
        # The replay-anchor mint is bench-side state opaque to the
        # patched runtime; returning a stable string suffices to thread
        # it into the spawn factory's --since arg without requiring a
        # real daemon emit.
        _ = seed_scope_id
        _ = db_path
        _ = doorbell_path
        _ = repo
        _ = ingest_method
        _ = delivery_id_prefix
        return "BENCH_FAKE_ANCHOR_EVENT_ID"

    def fake_spawn_n_heterogeneous(swarm_spec: dict[str, int], **kwargs: Any) -> list[_Child]:
        children: list[_Child] = []
        for framework, count in swarm_spec.items():
            for index in range(count):
                children.append(
                    _Child(role=f"{framework}-{framework}-{index + 1}", proc=_StaticProc(framework))  # type: ignore[arg-type]
                )
        return children

    monkeypatch.setattr(bench_module, "_spawn_daemon", fake_spawn_daemon)
    monkeypatch.setattr(bench_module, "_emit_seed_event", fake_emit_seed)
    monkeypatch.setattr(bench_module, "emit_anchor_event", fake_emit_anchor)
    monkeypatch.setattr(bench_module, "spawn_n_heterogeneous", fake_spawn_n_heterogeneous)
    # The orchestrator no longer issues its own OpenAI call -- pydantic
    # / langgraph rows rehydrate their envelope from the driver-side
    # wake marker -- so the previous orchestrator-side OpenAI-call patch
    # is gone; the fake driver subprocess's emitted marker line carries
    # the synthetic ``token_usage=null`` shape ``_StaticProc`` produces
    # via the real ``_emit_wake_marker`` (the JSON-bodied wire format).
    # Shorten the per-iteration deadlines and spawn settle so the
    # test does not wait through real-clock pacing. The preflight's
    # clock-stability probe lives in benchmarks._bench_preflight and is
    # NOT touched here so its real 500ms drift assertion still runs.
    monkeypatch.setattr(bench_module, "_PER_ITER_DEADLINE_SEC", 10.0)
    monkeypatch.setattr(bench_module, "_PER_ITER_SPAWN_SETTLE_SEC", 0.0)


def test_main_smoke_runs_end_to_end_and_writes_verdict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Smoke-mode (n=2) end-to-end: verdict.json is produced + every required field
    is present with the expected type."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")

    # Patch runtime (daemon + spawns + openai call) so no real
    # subprocess or network call is made.
    _patch_bench_runtime(monkeypatch)
    # Disable real-LLM preflight gates so the bench's helper runs
    # against the synthetic OpenAI capture rather than the keyring.
    output_dir = tmp_path / "out"
    rc = main(["--smoke", "--n", "2", "--output", str(output_dir), "--skip-real-llm"])
    assert rc == 0

    # Verdict file landed under the output dir with the right shape.
    verdict_files = list(output_dir.glob("*.verdict.json"))
    assert len(verdict_files) == 1
    verdict_path = verdict_files[0]
    progress_path = verdict_path.parent / verdict_path.name.replace(".verdict.json", ".progress.jsonl")
    assert progress_path.is_file()

    # Decode + structural assertions.
    verdict = msgspec.json.decode(verdict_path.read_bytes(), type=ExperimentAVerdict)
    assert verdict.bench_name == "poll_vs_subscribe_llm"
    assert verdict.smoke is True
    assert verdict.n_iterations_per_arm == 2
    # Two arms x 2 iterations x 5 drivers = 20 rows (plus 10 pilot rows
    # from the pilot subscribe arm).
    assert len(verdict.rows) >= 20
    # Both arms represented.
    arms_seen = {row.arm for row in verdict.rows}
    assert arms_seen == {"poll", "subscribe"}
    # All five drivers represented in at least one row.
    drivers_seen = {row.driver for row in verdict.rows}
    assert drivers_seen == set(FRAMEWORK_ORDER)
    # Limitations list non-empty and describes the per-driver-median posture.
    assert any("median latency description" in item for item in verdict.limitations)
    # External state captured (msgspec round-trip ensures the field).
    assert verdict.external_state.openai_key_present in {True, False}

    # Cross-process timing-marker fields populated on every non-pilot
    # row from the _StaticProc fake (which emits WAKE_RECEIVED before
    # DRIVER_REACTED with t_sub_monotonic_ns < seed_emit_monotonic_ns).
    classified_rows = [row for row in verdict.rows if row.t_sub_monotonic_ns > 0]
    assert classified_rows, "every fake driver row should carry a populated t_sub anchor"
    for row in classified_rows:
        assert row.t_seed_emit_monotonic_ns == 1_000_000_000_000_000
        assert row.t_sub_monotonic_ns == 500_000_000_000_000
        assert row.t_import_done_monotonic_ns == 300_000_000_000_000
        assert row.wake_monotonic_ns == 1_100_000_000_000_000
        # The fake's t_sub anchor is below the fake's seed-emit anchor
        # so the bench classifies the delivery mode as live fan-out.
        assert row.delivery_mode == "live"


def test_main_smoke_preflight_skip_no_secret_tool_dependency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--skip-real-llm`` reaches main body without needing OPENAI / claude / gemini."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    # If the host actually has those binaries, the patch is irrelevant;
    # if not, the skip-real-llm path is what we are exercising.
    if shutil.which("claude") is not None and shutil.which("gemini") is not None and _read_openai_key() is not None:
        pytest.skip("Host carries real claude/gemini/openai; preflight gates would pass anyway")
    _patch_bench_runtime(monkeypatch)
    output_dir = tmp_path / "out"
    rc = main(["--smoke", "--n", "2", "--output", str(output_dir), "--skip-real-llm"])
    assert rc == 0


def _read_openai_key() -> str | None:
    """Best-effort keyring lookup; returns None on any failure path."""
    secret_tool = shutil.which("secret-tool")
    if secret_tool is None:
        return None
    try:
        import subprocess

        result = subprocess.run(
            [secret_tool, "lookup", "service", "openai", "account", "api-key"],
            capture_output=True,
            text=True,
            timeout=2.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


# ---------------------------------------------------------------------
# Progress JSONL shape (the file the bench tails for triage).
# ---------------------------------------------------------------------


def test_progress_jsonl_contains_start_iteration_done_end(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    _patch_bench_runtime(monkeypatch)
    output_dir = tmp_path / "out"
    rc = main(["--smoke", "--n", "2", "--output", str(output_dir), "--skip-real-llm"])
    assert rc == 0
    progress_files = list(output_dir.glob("*.progress.jsonl"))
    assert len(progress_files) == 1
    payloads = [json.loads(line) for line in progress_files[0].read_text(encoding="utf-8").splitlines() if line.strip()]
    kinds = [payload.get("kind") for payload in payloads]
    assert "start" in kinds
    assert "iteration_done" in kinds
    # --skip-real-llm triggers the pilot-skip contract; the progress
    # record reflects the skip rather than a completed pilot.
    assert "pilot_skipped" in kinds
    assert "end" in kinds


# ---------------------------------------------------------------------
# OPENAI_API_KEY confidentiality: the verdict file never contains the
# key value substring (closes the documented security limitation).
# ---------------------------------------------------------------------


def test_verdict_never_persists_api_key_substring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    fake_key = "sk-fake-test-key-do-not-use-x" * 4
    _patch_bench_runtime(monkeypatch)
    # The skip-real-llm path means the bench never reads the keyring;
    # we still set the env so a future regression that leaked it would
    # be caught.
    monkeypatch.setenv("OPENAI_API_KEY", fake_key)
    output_dir = tmp_path / "out"
    rc = main(["--smoke", "--n", "2", "--output", str(output_dir), "--skip-real-llm"])
    assert rc == 0
    verdict_files = list(output_dir.glob("*.verdict.json"))
    assert verdict_files
    encoded = verdict_files[0].read_bytes().decode("utf-8")
    assert fake_key not in encoded


# ---------------------------------------------------------------------------
# Concurrent swarm-drain: the serial-drain latency artifact fix
# ---------------------------------------------------------------------------


class _StubProc:
    """A fake Popen whose ``communicate`` blocks ``delay_sec`` then returns stdout.

    Models the per-child EOF wait: ``communicate`` does not return until
    ``delay_sec`` has elapsed, mirroring how a slow driver holds its
    stdout pipe open while a fast driver has already closed its own.
    """

    def __init__(self, delay_sec: float, stdout: bytes) -> None:
        self._delay_sec = delay_sec
        self._stdout = stdout
        self.pid = -1

    def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
        time.sleep(self._delay_sec)
        return self._stdout, b""


def _marker_stdout(
    *,
    framework: str,
    seed_id: str,
    wake_monotonic_ns: int,
    seed_emit_monotonic_ns: int,
) -> bytes:
    """Build a child's stdout carrying its early-wake + canonical markers.

    The early marker's ``t_sub_monotonic_ns`` is set below
    ``seed_emit_monotonic_ns`` so the canonical reaction builds a real
    ``ObservedReaction``.
    """
    early = {
        "framework": framework,
        "fw_id": "0",
        "seed": seed_id,
        "wake_monotonic_ns": wake_monotonic_ns,
        "t_sub_monotonic_ns": seed_emit_monotonic_ns - 1,
        "t_import_done_monotonic_ns": seed_emit_monotonic_ns - 1,
    }
    canonical = {
        "framework": framework,
        "fw_id": "0",
        "seed": seed_id,
        "reaction_id": f"{framework}-reaction",
        "wall_ns": 0,
        "provider": "offline",
        "token_usage": None,
    }
    text = f"WAKE_RECEIVED {json.dumps(early)}\nDRIVER_REACTED {json.dumps(canonical)}\n"
    return text.encode()


def test_drain_swarm_reactions_concurrent_fast_child_not_blocked_by_slow() -> None:
    """A fast child's t_observe_ns reflects its own EOF, not a slow sibling's.

    Regression guard for the serial-drain end-to-end latency artifact: a
    fast driver placed AFTER a slow driver in spawn order used to inherit
    the slow driver's multi-second drain wait. With concurrent draining
    each child's ``t_observe_ns`` is stamped at its own ``communicate``
    return.

    Construction: ``slow`` (first in order) blocks ~1.2s; ``fast`` (second
    in order) returns almost immediately. Under the old serial drain the
    fast child would be stamped only after the slow child's 1.2s wait, so
    the assertion that the fast child's observed latency is far below the
    slow child's only holds under concurrent draining.
    """
    seed_id = "seed-concurrent-drain"
    seed_emit_monotonic_ns = time.monotonic_ns()
    slow_delay = 1.2
    fast_delay = 0.02

    slow_stdout = _marker_stdout(
        framework="langgraph",
        seed_id=seed_id,
        wake_monotonic_ns=seed_emit_monotonic_ns + 1_000,
        seed_emit_monotonic_ns=seed_emit_monotonic_ns,
    )
    fast_stdout = _marker_stdout(
        framework="shell-control",
        seed_id=seed_id,
        wake_monotonic_ns=seed_emit_monotonic_ns + 1_000,
        seed_emit_monotonic_ns=seed_emit_monotonic_ns,
    )

    # Slow child FIRST in spawn order -- this is the ordering that
    # produced the artifact: the fast child was drained only after the
    # slow child's communicate() returned.
    children = [
        _Child(
            role="langgraph-0",
            proc=_StubProc(slow_delay, slow_stdout),  # type: ignore[arg-type]
            framework="langgraph",
        ),
        _Child(
            role="shell-control-0",
            proc=_StubProc(fast_delay, fast_stdout),  # type: ignore[arg-type]
            framework="shell-control",
        ),
    ]

    wall_start = time.monotonic()
    reactions, t_observe_per_framework, _early = _drain_swarm_reactions(
        children,
        deadline_monotonic=time.monotonic() + 30.0,
        seed_delivery_id=seed_id,
        seed_emit_monotonic_ns=seed_emit_monotonic_ns,
    )
    wall_elapsed = time.monotonic() - wall_start

    # Both children produced a reaction.
    assert {r.framework for r in reactions} == {"langgraph", "shell-control"}

    fast_observe_ns = t_observe_per_framework["shell-control"]
    slow_observe_ns = t_observe_per_framework["langgraph"]
    fast_latency_ns = fast_observe_ns - seed_emit_monotonic_ns
    slow_latency_ns = slow_observe_ns - seed_emit_monotonic_ns

    # The fast child's observed latency reflects ITS finish, well below
    # the slow child's delay -- under the old serial drain it would have
    # been >= slow_delay because the fast child was stamped only after
    # the slow child drained.
    assert fast_latency_ns < slow_delay * 1e9 * 0.5
    # And it is below the slow child's observed latency.
    assert fast_latency_ns < slow_latency_ns
    # The whole drain runs in roughly slow_delay (concurrent), NOT
    # slow_delay + fast_delay summed serially; bound loosely for CI noise.
    assert wall_elapsed < slow_delay + 0.8
