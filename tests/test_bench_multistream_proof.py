"""Gating tests for ``benchmarks.bench_multistream_proof``.

A smoke-mode invocation of the bench runs end-to-end against the real
local daemon (no mocks for the bus surface — testing philosophy
mandates real subscribers + real daemon). Tests that touch real LLM
calls SKIP cleanly when the OPENAI key, claude CLI, or gemini CLI is
unavailable; the shell-control-only smoke path still exercises the
verdict-shape contract.

Network-free helper tests pin the per-arm stats / verdict-aggregation
logic against synthetic ``WindowRow`` lists; they do not spawn the
daemon.
"""

from __future__ import annotations

import contextlib
import shutil
import subprocess
import sys
import time
from pathlib import Path

import msgspec
import pytest

from benchmarks._bench_shared import (
    CostBudgetTracker,
    ExternalStateReport,
    IterationRow,
    OpenAIEnvelope,
    capture_external_state,
    resolve_bench_log_paths,
)
from benchmarks._harness import environment_report
from benchmarks.bench_multistream_proof import (
    CostSummary,
    ExperimentBVerdict,
    WindowRow,
    _build_verdict,
    _compute_mann_whitney,
    _median_int,
    _read_voluntary_ctxt_switches,
    main,
)

# The bench substrate reads /proc/<pid>/{schedstat,stat,status}; those paths do
# not exist off Linux, so the whole module is Linux-only and skips elsewhere.
pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="benchmark substrate reads /proc; Linux-only",
)

# ---------------------------------------------------------------------
# Skip predicates.
# ---------------------------------------------------------------------


def _has_openai_key() -> bool:
    """True iff the system keyring returns a non-empty key for the openai service."""
    secret_tool = shutil.which("secret-tool")
    if secret_tool is None:
        return False
    try:
        result = subprocess.run(
            [secret_tool, "lookup", "service", "openai", "account", "api-key"],
            capture_output=True,
            text=True,
            timeout=3.0,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False
    return result.returncode == 0 and bool(result.stdout.strip())


def _has_claude_cli() -> bool:
    return shutil.which("claude") is not None


def _has_gemini_cli() -> bool:
    return shutil.which("gemini") is not None


def _has_waitbus() -> bool:
    return shutil.which("waitbus") is not None


# ---------------------------------------------------------------------
# Helper tests (no daemon spawn).
# ---------------------------------------------------------------------


def test_median_int_empty_list_returns_zero() -> None:
    """``_median_int`` returns 0 on empty input rather than raising."""
    assert _median_int([]) == 0


def test_median_int_odd_count_returns_middle() -> None:
    assert _median_int([1, 5, 3]) == 3


def test_median_int_even_count_returns_floor_mean() -> None:
    """Even-count median uses integer floor-division of the two middles."""
    assert _median_int([1, 2, 3, 4]) == 2  # (2+3)//2 == 2


def test_resolve_log_paths_strips_verdict_json_suffix(tmp_path: Path) -> None:
    """A canonical ``<stem>.verdict.json`` output yields sibling progress/log files."""
    verdict, progress, log = resolve_bench_log_paths(
        bench_name="bench_multistream_proof", output=tmp_path / "run.verdict.json"
    )
    assert verdict == tmp_path / "run.verdict.json"
    assert progress == tmp_path / "run.progress.jsonl"
    assert log == tmp_path / "run.log"


def test_resolve_log_paths_multi_dot_stem_not_over_trimmed(tmp_path: Path) -> None:
    """A multi-dotted output name does not over-strip the derived stem.

    The prior blind double ``with_suffix("")`` would turn ``foo.tar.gz``
    into ``foo`` and leave the three files on different stems. The
    verdict keeps the operator's exact path and progress/log share its
    single-suffix stem so all three sort together.
    """
    verdict, progress, log = resolve_bench_log_paths(
        bench_name="bench_multistream_proof", output=tmp_path / "foo.tar.gz"
    )
    assert verdict == tmp_path / "foo.tar.gz"
    assert progress == tmp_path / "foo.tar.progress.jsonl"
    assert log == tmp_path / "foo.tar.log"


def test_resolve_log_paths_default_layout_shares_stem() -> None:
    """The default (no-output) layout puts all three files on one stem."""
    verdict, progress, log = resolve_bench_log_paths(bench_name="bench_multistream_proof", output=None)
    assert verdict.name.endswith(".verdict.json")
    stem = verdict.name[: -len(".verdict.json")]
    assert progress.name == f"{stem}.progress.jsonl"
    assert log.name == f"{stem}.log"
    assert verdict.parent == progress.parent == log.parent


def test_compute_mann_whitney_empty_arms_returns_neutral_pvalue() -> None:
    """Empty idle arm: (U=0, p=1) sentinel + reason='empty_idle' so a downstream
    reader can distinguish the inapplicable path from real-sample 'p=1.0' evidence.
    """
    u, p, reason = _compute_mann_whitney([], [1, 2, 3])
    assert u == 0.0
    assert p == 1.0
    assert reason == "empty_idle"


def test_compute_mann_whitney_identical_arms_high_pvalue() -> None:
    """Two identical samples give the centred sentinel under the default one-sided alternative."""
    samples = [1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000]
    _u, p, _reason = _compute_mann_whitney(samples, samples)
    # Default ``alternative='less'`` on identical samples yields the
    # centred p-value (0.5 in the asymptotic limit; scipy emits a value
    # close to but not exactly 0.5 for small N).
    assert p >= 0.4


def test_compute_mann_whitney_distinct_arms_low_pvalue() -> None:
    """Strongly separated samples give a low p-value (detected effect)."""
    idle = [1_000_000, 1_500_000, 2_000_000, 1_200_000, 1_800_000]
    loaded = [50_000_000, 60_000_000, 70_000_000, 55_000_000, 65_000_000]
    _u, p, _reason = _compute_mann_whitney(idle, loaded)
    assert p < 0.05


def test_build_verdict_carries_limitations_list() -> None:
    """The verdict's limitations list is populated and includes the locked claims."""
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    windows: list[WindowRow] = []
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=1_000_000_000,
        outlier_threshold_schedstat_ns=1_000_000_000,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    assert len(verdict.limitations) >= 5
    assert any("1.5 sigma" in lim or "1.5 sigma ~ 15 ms" in lim for lim in verdict.limitations)
    assert any("Per-event granularity" in lim for lim in verdict.limitations)
    assert any("OPENAI_API_KEY" in lim for lim in verdict.limitations)


def test_build_verdict_utime_outlier_filter_excludes_idle_arm_above_threshold() -> None:
    """Idle-arm windows whose utime exceeds the threshold are rejected.

    The utime outlier filter is asymmetric: on the idle arm, a window
    above the noise floor is a scheduler spike and gets rejected; on
    the loaded arm, a window above the floor IS the signal and gets
    kept. This test pins the idle-arm rejection half of that contract.
    """
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    threshold = 10_000_000  # 10 ms
    windows = [
        WindowRow(
            window_id=0,
            arm="idle",
            t_window_start_ns=0,
            t_window_end_ns=1_000_000_000,
            daemon_utime_delta_ns=5_000_000,  # below threshold -> kept
            daemon_stime_delta_ns=1_000_000,
            daemon_schedstat_run_delta_ns=0,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
        WindowRow(
            window_id=1,
            arm="idle",
            t_window_start_ns=1_000_000_000,
            t_window_end_ns=2_000_000_000,
            daemon_utime_delta_ns=50_000_000,  # above threshold -> rejected
            daemon_stime_delta_ns=1_000_000,
            daemon_schedstat_run_delta_ns=50_000_000,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
    ]
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=threshold,
        outlier_threshold_schedstat_ns=threshold,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    assert len(verdict.windows) == 2
    assert verdict.rejected_window_count == 1


def test_build_verdict_utime_outlier_filter_keeps_loaded_arm_above_threshold() -> None:
    """Loaded-arm windows above the utime threshold are KEPT (they are signal, not noise)."""
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    threshold = 10_000_000  # 10 ms (jiffie floor)
    windows = [
        WindowRow(
            window_id=0,
            arm="idle",
            t_window_start_ns=0,
            t_window_end_ns=1_000_000_000,
            daemon_utime_delta_ns=0,
            daemon_stime_delta_ns=0,
            daemon_schedstat_run_delta_ns=0,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
        WindowRow(
            window_id=1,
            arm="loaded",
            t_window_start_ns=1_000_000_000,
            t_window_end_ns=2_000_000_000,
            daemon_utime_delta_ns=50_000_000,  # above threshold but loaded -> kept
            daemon_stime_delta_ns=1_000_000,
            daemon_schedstat_run_delta_ns=50_000_000,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
    ]
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=threshold,
        outlier_threshold_schedstat_ns=threshold,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    assert len(verdict.windows) == 2
    assert verdict.rejected_window_count == 0
    assert verdict.median_loaded_utime_ns == 50_000_000


def test_build_verdict_schedstat_substrate_unavailable_signals_via_inapplicable_reason() -> None:
    """When every schedstat sample is zero the substrate is unavailable; surface via the schedstat
    inapplicable reason rather than letting Mann-Whitney return a misleading p=1.0."""
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    windows = [
        WindowRow(
            window_id=0,
            arm="idle",
            t_window_start_ns=0,
            t_window_end_ns=1_000_000_000,
            daemon_utime_delta_ns=5_000_000,
            daemon_stime_delta_ns=1_000_000,
            daemon_schedstat_run_delta_ns=0,  # substrate unavailable -> 0
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
        WindowRow(
            window_id=1,
            arm="loaded",
            t_window_start_ns=1_000_000_000,
            t_window_end_ns=2_000_000_000,
            daemon_utime_delta_ns=6_000_000,
            daemon_stime_delta_ns=1_000_000,
            daemon_schedstat_run_delta_ns=0,  # substrate unavailable -> 0
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
    ]
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=1_000_000_000,
        outlier_threshold_schedstat_ns=1_000_000_000,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    assert verdict.mann_whitney_inapplicable_reason_schedstat_per_sec == "schedstat_substrate_unavailable"
    assert verdict.mann_whitney_inapplicable_reason_schedstat_raw == "schedstat_substrate_unavailable"
    assert verdict.h0_rejected_schedstat_per_sec is False
    assert verdict.h0_rejected_schedstat_raw is False


def test_build_verdict_schedstat_carries_signal_when_utime_below_jiffie_floor() -> None:
    """schedstat carries a Mann-Whitney signal when utime is jiffie-floored at 0."""
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    # Build 20 windows: 10 idle (low schedstat), 10 loaded (high schedstat),
    # all with utime=0 to simulate the sub-jiffie undershoot case.
    windows: list[WindowRow] = []
    for i in range(10):
        windows.append(
            WindowRow(
                window_id=i,
                arm="idle",
                t_window_start_ns=i * 1_000_000_000,
                t_window_end_ns=(i + 1) * 1_000_000_000,
                daemon_utime_delta_ns=0,  # sub-jiffie undershoot
                daemon_stime_delta_ns=0,
                daemon_schedstat_run_delta_ns=100_000 + i * 1_000,  # ~100us baseline
                daemon_voluntary_ctxt_delta=10,
                daemon_nonvoluntary_ctxt_delta=2,
                measurement_thread_time_delta_ns=900_000_000,
                wall_minus_thread_time_ns=100_000_000,
                swarm_rows=[],
                rejected=False,
                rejection_reason=None,
            )
        )
    for i in range(10, 20):
        windows.append(
            WindowRow(
                window_id=i,
                arm="loaded",
                t_window_start_ns=i * 1_000_000_000,
                t_window_end_ns=(i + 1) * 1_000_000_000,
                daemon_utime_delta_ns=0,  # sub-jiffie undershoot
                daemon_stime_delta_ns=0,
                # ~20 ms/s perturbed -- the Mann-Whitney diff test rejects
                # and the verdict labels the run ``perturbation_detected``.
                daemon_schedstat_run_delta_ns=20_000_000 + i * 1_000,
                daemon_voluntary_ctxt_delta=10,
                daemon_nonvoluntary_ctxt_delta=2,
                measurement_thread_time_delta_ns=900_000_000,
                wall_minus_thread_time_ns=100_000_000,
                swarm_rows=[],
                rejected=False,
                rejection_reason=None,
            )
        )
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=10_000_000,  # 10ms jiffie floor
        outlier_threshold_schedstat_ns=10_000_000,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    # utime path returns p=1.0 sentinel (all samples zero, MW degenerate)
    # OR carries a noise-floor p that does not reject H0; schedstat path
    # MUST detect the order-of-magnitude difference between arms.
    # Both per_sec and raw schedstat marginals carry signal for this
    # synthetic fixture (loaded windows have order-of-magnitude higher
    # schedstat samples than idle on identical 1s wall durations).
    assert verdict.mann_whitney_inapplicable_reason_schedstat_per_sec is None
    assert verdict.mann_whitney_inapplicable_reason_schedstat_raw is None
    assert verdict.mann_whitney_p_schedstat_per_sec < 0.05
    assert verdict.mann_whitney_p_schedstat_raw < 0.05
    assert verdict.h0_rejected_schedstat_per_sec is True
    assert verdict.h0_rejected_schedstat_raw is True
    assert verdict.perturbation_detected is True
    assert verdict.verdict == "perturbation_detected"


def test_build_verdict_invariant_failure_excludes_from_aggregate() -> None:
    """Windows containing ANY invariant-failed driver row are excluded from the U test."""
    from benchmarks._bench_shared import IterationRow

    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    failed_row = IterationRow(
        iter_id=0,
        arm="loaded",
        driver="claude_cli",
        sentinel="sentinel-0",
        t_send_ns=0,
        t_observe_ns=0,
        latency_ns=0,
        cache_state="NA",
        claude_env=None,
        gemini_env=None,
        openai_env=None,
        invariant_failed=True,
        invariant_failure_field="claude_is_error",
    )
    windows = [
        WindowRow(
            window_id=0,
            arm="loaded",
            t_window_start_ns=0,
            t_window_end_ns=1_000_000_000,
            daemon_utime_delta_ns=5_000_000,
            daemon_stime_delta_ns=1_000_000,
            daemon_schedstat_run_delta_ns=0,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[failed_row],
            rejected=False,
            rejection_reason=None,
        ),
    ]
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=1_000_000_000,
        outlier_threshold_schedstat_ns=1_000_000_000,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    assert verdict.rejected_window_count == 1
    # Median across empty kept arm is 0; no perturbation detected.
    assert verdict.median_loaded_utime_ns == 0
    assert verdict.perturbation_detected is False


def test_assert_cpu_isolation_skips_when_not_real_llm() -> None:
    """The isolation gate is a no-op for smoke / offline runs."""
    from benchmarks._bench_preflight import assert_cpu_isolation_for_baselines

    env_report = environment_report()
    # cpu_governor may be 'performance' or anything else on the dev host;
    # the no-op path must NOT raise regardless of the governor value.
    assert_cpu_isolation_for_baselines(
        env_report,
        include_real_llm=False,
        allow_unpinned=False,
    )


def test_assert_cpu_isolation_skips_when_allow_unpinned_bypass() -> None:
    """The --allow-unpinned-for-dev bypass returns without raising."""
    from benchmarks._bench_preflight import assert_cpu_isolation_for_baselines

    env_report = environment_report()
    assert_cpu_isolation_for_baselines(
        env_report,
        include_real_llm=True,
        allow_unpinned=True,
    )


def test_assert_cpu_isolation_raises_on_non_performance_governor() -> None:
    """The gate raises PreflightError for a non-performance governor + real LLM."""
    import msgspec as _msgspec

    from benchmarks._bench_preflight import (
        PreflightError,
        assert_cpu_isolation_for_baselines,
    )

    env_report = _msgspec.structs.replace(
        environment_report(),
        cpu_governor="powersave",
    )
    with pytest.raises(PreflightError, match="cpu governor is 'powersave'"):
        assert_cpu_isolation_for_baselines(
            env_report,
            include_real_llm=True,
            allow_unpinned=False,
        )


def test_compute_orchestrator_and_daemon_cores_returns_disjoint_halves() -> None:
    """The half-half core split partitions [0, cpu_count) into two non-overlapping sets."""
    import os as _os

    from benchmarks._bench_preflight import compute_orchestrator_and_daemon_cores

    orchestrator_cores, daemon_cores = compute_orchestrator_and_daemon_cores()
    cpu_count = _os.cpu_count() or 0
    midpoint = cpu_count // 2
    # The split is disjoint and covers every core (no orphan cores).
    assert orchestrator_cores & daemon_cores == set()
    assert orchestrator_cores | daemon_cores == set(range(cpu_count))
    assert orchestrator_cores == set(range(midpoint))
    assert daemon_cores == set(range(midpoint, cpu_count))


def test_read_voluntary_ctxt_switches_self() -> None:
    """The /proc/<pid>/status reader returns non-negative ints for the current process."""
    import os as _os

    vol, nonvol = _read_voluntary_ctxt_switches(_os.getpid())
    assert vol >= 0
    assert nonvol >= 0


def test_read_daemon_vmrss_kb_self_returns_positive_int() -> None:
    """The /proc/<pid>/status VmRSS reader returns a positive int for the current process."""
    import os as _os

    from benchmarks._bench_shared import read_daemon_vmrss_kb

    rss = read_daemon_vmrss_kb(_os.getpid())
    assert isinstance(rss, int)
    # The interpreter holds at least a few MB resident; a sub-100 kB
    # value would indicate the parser is mis-anchored on the wrong row.
    assert rss > 100


def test_read_daemon_vmrss_kb_missing_pid_returns_zero() -> None:
    """The /proc/<pid>/status reader returns 0 (not raise) on a non-existent pid."""
    from benchmarks._bench_shared import read_daemon_vmrss_kb

    # PID 2**30 is far above the kernel's max_pid; the proc entry cannot exist.
    rss = read_daemon_vmrss_kb(2**30)
    assert rss == 0


def test_schedstat_substrate_available_on_linux_dev_kernel() -> None:
    """Probe should return True on any dev kernel exposing /proc/self/schedstat."""
    from benchmarks._bench_shared import schedstat_substrate_available

    # True iff the kernel exposes
    # per-task schedstat. We assert on the Linux convention -- dev hosts
    # ship with CONFIG_SCHEDSTATS=y. If the bench is run on a stripped
    # kernel the probe returns False and the bench's verdict path is
    # exercised separately by the substrate-unavailable test below.
    assert schedstat_substrate_available() is True


def test_read_daemon_schedstat_self_aggregates_across_tids() -> None:
    """Per-TID schedstat aggregation reports tid_count >= 1 and non-negative sums."""
    import os as _os

    from benchmarks._bench_shared import SchedstatSnapshot, read_daemon_schedstat

    snap = read_daemon_schedstat(_os.getpid())
    assert isinstance(snap, SchedstatSnapshot)
    # The pytest process has at least one main thread (the test runner
    # itself). A tid_count of 0 indicates the TID walk failed entirely.
    assert snap.tid_count >= 1
    assert snap.run_time_ns >= 0
    assert snap.wait_time_ns >= 0
    assert snap.pcount >= 0


def test_read_daemon_schedstat_per_tid_sum_dominates_group_leader() -> None:
    """Per-TID aggregation captures sibling-thread CPU the group-leader-only read misses."""
    import os as _os
    import threading
    import time as _time

    from benchmarks._bench_shared import read_daemon_schedstat

    # Spin a busy sibling thread. The orchestrator's main TID is mostly
    # idle (waiting on this thread to join); the busy sibling accrues
    # nontrivial run_time_ns. The aggregated snapshot MUST exceed what
    # a group-leader-only read of /proc/<pid>/schedstat would return.
    stop_flag = threading.Event()

    def _burn() -> None:
        end_at = _time.monotonic() + 0.4
        while _time.monotonic() < end_at and not stop_flag.is_set():
            pass

    worker = threading.Thread(target=_burn, daemon=True)
    worker.start()
    # Sample mid-burn so the worker has accrued schedstat.run_time.
    _time.sleep(0.2)
    snap = read_daemon_schedstat(_os.getpid())
    stop_flag.set()
    worker.join(timeout=1.0)

    # Group-leader-only baseline: read parent /proc/<pid>/schedstat.
    with open(f"/proc/{_os.getpid()}/schedstat", encoding="utf-8") as fh:
        gl_run_ns = int(fh.read().split()[0])

    # tid_count must include the spawned worker.
    assert snap.tid_count >= 2, f"per-TID walk should see main + worker thread; got tid_count={snap.tid_count}"
    # Aggregated run_time MUST be strictly greater than the group-leader
    # read for a process whose work happens off the main thread.
    assert snap.run_time_ns > gl_run_ns, (
        f"per-TID aggregation should dominate group-leader-only; got aggregate={snap.run_time_ns} vs gl={gl_run_ns}"
    )


def test_read_daemon_schedstat_missing_pid_returns_unavailable_sentinel() -> None:
    """The per-TID walk returns the substrate-unavailable sentinel on a missing pid."""
    from benchmarks._bench_shared import read_daemon_schedstat

    snap = read_daemon_schedstat(2**30)
    assert snap.tid_count == 0
    assert snap.run_time_ns == 0
    assert snap.wait_time_ns == 0
    assert snap.pcount == 0


def test_build_verdict_pcount_substrate_carries_independent_signal() -> None:
    """The pcount substrate rejects H0 when loaded windows wake more often than idle."""
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    # Idle pcount ~0 (epoll-blocked daemon wakes rarely); loaded pcount
    # large (every event-emit wakes the doorbell thread once).
    windows: list[WindowRow] = []
    for i in range(10):
        windows.append(
            WindowRow(
                window_id=i,
                arm="idle",
                t_window_start_ns=i * 1_000_000_000,
                t_window_end_ns=(i + 1) * 1_000_000_000,
                daemon_utime_delta_ns=0,
                daemon_stime_delta_ns=0,
                daemon_schedstat_run_delta_ns=0,
                daemon_schedstat_pcount_delta=i % 2,  # 0 or 1
                daemon_voluntary_ctxt_delta=10,
                daemon_nonvoluntary_ctxt_delta=2,
                measurement_thread_time_delta_ns=900_000_000,
                wall_minus_thread_time_ns=100_000_000,
                swarm_rows=[],
                rejected=False,
                rejection_reason=None,
            )
        )
    for i in range(10, 20):
        windows.append(
            WindowRow(
                window_id=i,
                arm="loaded",
                t_window_start_ns=i * 1_000_000_000,
                t_window_end_ns=(i + 1) * 1_000_000_000,
                daemon_utime_delta_ns=0,
                daemon_stime_delta_ns=0,
                daemon_schedstat_run_delta_ns=0,
                daemon_schedstat_pcount_delta=50 + i,  # ~50-69 wakes per window
                daemon_voluntary_ctxt_delta=10,
                daemon_nonvoluntary_ctxt_delta=2,
                measurement_thread_time_delta_ns=900_000_000,
                wall_minus_thread_time_ns=100_000_000,
                swarm_rows=[],
                rejected=False,
                rejection_reason=None,
            )
        )
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=1_000_000_000,
        outlier_threshold_schedstat_ns=1_000_000_000,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    assert verdict.mann_whitney_inapplicable_reason_pcount is None
    assert verdict.mann_whitney_p_pcount < 0.05
    assert verdict.h0_rejected_pcount is True
    # Both schedstat-bearing paths are all-zero -> unavailable
    # sentinel on per_sec AND raw; pcount carries the signal on its own.
    assert verdict.mann_whitney_inapplicable_reason_schedstat_per_sec == "schedstat_substrate_unavailable"
    assert verdict.mann_whitney_inapplicable_reason_schedstat_raw == "schedstat_substrate_unavailable"
    assert verdict.perturbation_detected is True
    assert verdict.verdict == "perturbation_detected"
    assert verdict.median_loaded_pcount > verdict.median_idle_pcount


def test_build_verdict_schedstat_kernel_unavailable_forces_inapp_on_both_schedstat_paths() -> None:
    """schedstat_kernel_available=False forces both schedstat-bearing marginals to inapplicable."""
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    # Synthetic windows with non-zero schedstat samples -- the kernel-
    # availability override must STILL flip both paths to unavailable.
    windows = [
        WindowRow(
            window_id=0,
            arm="idle",
            t_window_start_ns=0,
            t_window_end_ns=1_000_000_000,
            daemon_utime_delta_ns=0,
            daemon_stime_delta_ns=0,
            daemon_schedstat_run_delta_ns=12_345,  # non-zero but unreliable
            daemon_schedstat_pcount_delta=7,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
        WindowRow(
            window_id=1,
            arm="loaded",
            t_window_start_ns=1_000_000_000,
            t_window_end_ns=2_000_000_000,
            daemon_utime_delta_ns=0,
            daemon_stime_delta_ns=0,
            daemon_schedstat_run_delta_ns=99_999,
            daemon_schedstat_pcount_delta=300,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
    ]
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=1_000_000_000,
        outlier_threshold_schedstat_ns=1_000_000_000,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
        schedstat_kernel_available=False,
    )
    assert verdict.schedstat_kernel_available is False
    assert verdict.mann_whitney_inapplicable_reason_schedstat_per_sec == "schedstat_substrate_unavailable"
    assert verdict.mann_whitney_inapplicable_reason_schedstat_raw == "schedstat_substrate_unavailable"
    assert verdict.mann_whitney_inapplicable_reason_pcount == "schedstat_substrate_unavailable"
    assert verdict.h0_rejected_schedstat_per_sec is False
    assert verdict.h0_rejected_schedstat_raw is False
    assert verdict.h0_rejected_pcount is False


def test_build_verdict_vmrss_leak_slope_positive_for_monotonic_loaded_rss() -> None:
    """A monotonically growing loaded-arm end-VmRSS yields a positive leak slope."""
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    # Build 10 idle (flat 10_000 kB) and 10 loaded (10_000 .. 10_900 kB)
    # so the OLS slope over the loaded arm is strongly positive.
    windows: list[WindowRow] = []
    for i in range(10):
        windows.append(
            WindowRow(
                window_id=i,
                arm="idle",
                t_window_start_ns=i * 1_000_000_000,
                t_window_end_ns=(i + 1) * 1_000_000_000,
                daemon_utime_delta_ns=0,
                daemon_stime_delta_ns=0,
                daemon_schedstat_run_delta_ns=0,
                daemon_voluntary_ctxt_delta=10,
                daemon_nonvoluntary_ctxt_delta=2,
                daemon_vmrss_start_kb=10_000,
                daemon_vmrss_end_kb=10_000,
                measurement_thread_time_delta_ns=900_000_000,
                wall_minus_thread_time_ns=100_000_000,
                swarm_rows=[],
                rejected=False,
                rejection_reason=None,
            )
        )
    for i in range(10, 20):
        windows.append(
            WindowRow(
                window_id=i,
                arm="loaded",
                t_window_start_ns=i * 1_000_000_000,
                t_window_end_ns=(i + 1) * 1_000_000_000,
                daemon_utime_delta_ns=0,
                daemon_stime_delta_ns=0,
                daemon_schedstat_run_delta_ns=0,
                daemon_voluntary_ctxt_delta=10,
                daemon_nonvoluntary_ctxt_delta=2,
                daemon_vmrss_start_kb=10_000,
                daemon_vmrss_end_kb=10_000 + (i - 10) * 100,  # +100 kB per loaded window
                measurement_thread_time_delta_ns=900_000_000,
                wall_minus_thread_time_ns=100_000_000,
                swarm_rows=[],
                rejected=False,
                rejection_reason=None,
            )
        )
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=1_000_000_000,
        outlier_threshold_schedstat_ns=1_000_000_000,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    # Slope is +100 kB per window (the synthetic generator's exact gradient).
    assert verdict.vmrss_leak_slope_kb_per_window == pytest.approx(100.0, abs=1e-6)
    assert verdict.median_idle_vmrss_kb == 10_000
    # Median loaded is the 5th/6th element of the 10-element ramp [10000..10900 by 100].
    assert verdict.median_loaded_vmrss_kb == 10_450
    assert verdict.vmrss_substrate_inapplicable_reason is None


def test_build_verdict_vmrss_substrate_unavailable_when_all_samples_zero() -> None:
    """All-zero VmRSS samples trigger the substrate-unavailable sentinel."""
    env_report = environment_report()
    external_state = capture_external_state(openai_api_key_present=False)
    windows = [
        WindowRow(
            window_id=0,
            arm="idle",
            t_window_start_ns=0,
            t_window_end_ns=1_000_000_000,
            daemon_utime_delta_ns=0,
            daemon_stime_delta_ns=0,
            daemon_schedstat_run_delta_ns=0,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            daemon_vmrss_start_kb=0,
            daemon_vmrss_end_kb=0,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
        WindowRow(
            window_id=1,
            arm="loaded",
            t_window_start_ns=1_000_000_000,
            t_window_end_ns=2_000_000_000,
            daemon_utime_delta_ns=0,
            daemon_stime_delta_ns=0,
            daemon_schedstat_run_delta_ns=0,
            daemon_voluntary_ctxt_delta=10,
            daemon_nonvoluntary_ctxt_delta=2,
            daemon_vmrss_start_kb=0,
            daemon_vmrss_end_kb=0,
            measurement_thread_time_delta_ns=900_000_000,
            wall_minus_thread_time_ns=100_000_000,
            swarm_rows=[],
            rejected=False,
            rejection_reason=None,
        ),
    ]
    verdict = _build_verdict(
        started_ns=1,
        finished_ns=2,
        env_report=env_report,
        external_state=external_state,
        windows=windows,
        outlier_threshold_ns=1_000_000_000,
        outlier_threshold_schedstat_ns=1_000_000_000,
        n_per_arm=10,
        cost=CostSummary(
            cost_usd_total=None,
            cost_unknown_count=0,
            max_cost_usd_budget=5.0,
            max_cost_usd_observed=0.0,
            aborted_on_budget=False,
        ),
    )
    assert verdict.vmrss_substrate_inapplicable_reason == "vmrss_substrate_unavailable"
    assert verdict.median_idle_vmrss_kb == 0
    assert verdict.median_loaded_vmrss_kb == 0
    assert verdict.vmrss_leak_slope_kb_per_window == 0.0


# ---------------------------------------------------------------------
# End-to-end smoke (real daemon, no real LLM).
# ---------------------------------------------------------------------


@pytest.mark.skipif(
    not _has_waitbus() or not sys.platform.startswith("linux"),
    reason="bench requires Linux + waitbus on PATH",
)
def test_smoke_mode_no_real_llm_produces_verdict_with_expected_shape(tmp_path: Path) -> None:
    """End-to-end smoke: 5 windows per arm; no real LLM calls.

    Exercises the daemon spawn, the per-window measurement loop, the
    outlier-pilot computation, and the verdict serialisation. The
    shell-control arm fires every window regardless of --include-real-llm,
    so the bus surface is non-trivially exercised even without LLM calls.

    Skips when waitbus is not on PATH (the bench depends on the daemon
    binary). Does NOT depend on OpenAI / claude / gemini -- the
    --include-real-llm flag is False in smoke mode.
    """
    output = tmp_path / "verdict.json"
    rc = main(
        argv=[
            "--smoke",
            "--output",
            str(output),
        ]
    )
    assert rc == 0
    assert output.exists()
    # Decode as the typed struct; this is the contract test.
    raw = output.read_bytes()
    verdict = msgspec.json.decode(raw, type=ExperimentBVerdict)

    # Identity (the typed decode above is the shape contract).
    assert verdict.bench_name == "bench_multistream_proof"
    assert verdict.started_ns < verdict.finished_ns

    # Producer + subscriber direction fields surface on the verdict.
    # Smoke mode zeros both the producer count and the subscriber
    # framework set; the verdict carries the field shapes.
    assert verdict.producer_count == 0
    assert verdict.producer_event_rate_hz == 0.0
    assert verdict.subscriber_agent_count == 0
    assert isinstance(verdict.subscriber_framework_mix, dict)
    assert verdict.subscriber_framework_mix == {}
    assert verdict.producer_attrition_detected is False
    assert verdict.subscriber_attrition_detected is False
    assert verdict.producer_emit_count_total == 0
    assert verdict.producer_late_count_total == 0
    assert verdict.producer_error_count_total == 0

    # Window counts: 5 per arm x 2 arms = 10 windows.
    assert len(verdict.windows) == 10
    idle_count = sum(1 for r in verdict.windows if r.arm == "idle")
    loaded_count = sum(1 for r in verdict.windows if r.arm == "loaded")
    assert idle_count == 5
    assert loaded_count == 5

    # Per-window invariants.
    for row in verdict.windows:
        assert row.t_window_start_ns < row.t_window_end_ns
        assert row.daemon_utime_delta_ns >= 0
        assert row.daemon_stime_delta_ns >= 0
        assert row.measurement_thread_time_delta_ns >= 0
        assert row.wall_minus_thread_time_ns >= 0
        # VmRSS samples are non-negative and -- for a real daemon
        # process -- always non-zero (the smoke run uses the live waitbus
        # binary). The unavailable-substrate path is exercised by the
        # synthetic helper tests.
        assert row.daemon_vmrss_start_kb >= 0
        assert row.daemon_vmrss_end_kb >= 0
        assert row.daemon_vmrss_end_kb > 0
        # Schedstat aggregation samples are non-negative and the tid
        # count should reflect at least the daemon's main + doorbell
        # threads (>=2). A tid_count of zero indicates a substrate
        # failure that the smoke run should not have produced.
        assert row.daemon_schedstat_run_delta_ns >= 0
        assert row.daemon_schedstat_wait_delta_ns >= 0
        assert row.daemon_schedstat_pcount_delta >= 0
        assert row.daemon_schedstat_tid_count_end >= 2
        # Smoke mode zeros producer_count + agent_frameworks so the
        # loaded arm's workload runner short-circuits to an empty
        # _WorkloadResult; the row carries an empty swarm_rows list.
        # The full-run path is exercised by a production invocation
        # rather than smoke.
        if row.arm == "loaded":
            assert isinstance(row.swarm_rows, list)

    # VmRSS aggregates surface on the verdict.
    assert verdict.median_idle_vmrss_kb > 0
    assert verdict.median_loaded_vmrss_kb > 0
    assert verdict.vmrss_substrate_inapplicable_reason is None
    # The slope is a float and the smoke-mode arms are short -- no
    # meaningful leak in 5 loaded windows -- so we assert numeric type,
    # not sign.
    assert isinstance(verdict.vmrss_leak_slope_kb_per_window, float)
    assert isinstance(verdict.vmrss_leak_intercept_kb, float)

    # Schedstat kernel-availability probe surfaces on the verdict.
    # On the dev box CONFIG_SCHEDSTATS=y so the probe is True; both
    # schedstat-bearing marginals carry real data (no unavailable
    # sentinel). pcount medians are integer and non-negative.
    assert verdict.schedstat_kernel_available is True
    assert verdict.median_idle_pcount >= 0
    assert verdict.median_loaded_pcount >= 0
    assert isinstance(verdict.median_idle_pcount, int)
    assert isinstance(verdict.median_loaded_pcount, int)

    # Mann-Whitney fields present + numeric.
    # Both raw + per-sec families are emitted on the verdict.
    assert isinstance(verdict.mann_whitney_u_utime_per_sec, float)
    assert isinstance(verdict.mann_whitney_u_utime_raw, float)
    assert 0.0 <= verdict.mann_whitney_p_utime_per_sec <= 1.0
    assert 0.0 <= verdict.mann_whitney_p_utime_raw <= 1.0
    assert 0.0 <= verdict.mann_whitney_p_stime_per_sec <= 1.0
    assert 0.0 <= verdict.mann_whitney_p_stime_raw <= 1.0
    # Per-sec medians surface on the verdict alongside raw.
    assert isinstance(verdict.median_idle_utime_per_sec_ns, int)
    assert isinstance(verdict.median_loaded_utime_per_sec_ns, int)
    assert verdict.median_idle_utime_per_sec_ns >= 0
    assert verdict.median_loaded_utime_per_sec_ns >= 0
    assert 0.0 <= verdict.mann_whitney_p_pcount <= 1.0

    # Power-floor pin.
    assert verdict.min_detectable_effect_sigma == 1.5
    assert verdict.min_detectable_effect_ms >= 0.0

    # Outlier filter is exposed and non-negative.
    assert verdict.outlier_threshold_ns >= 0
    assert verdict.rejected_window_count >= 0

    # GIL gap + per-event granularity exposed.
    assert verdict.mean_gil_gap_ns >= 0
    assert len(verdict.events_per_loaded_window) == loaded_count

    # External state report is present + has identity fields.
    assert isinstance(verdict.external_state, ExternalStateReport)
    # Smoke mode does NOT require an OpenAI key.
    assert verdict.external_state.openai_key_present in (True, False)

    # Limitations list non-empty + carries the locked claims.
    assert len(verdict.limitations) >= 5
    assert any("1.5 sigma" in lim for lim in verdict.limitations)
    assert any("Per-event granularity" in lim for lim in verdict.limitations)


@pytest.mark.skipif(
    not _has_waitbus() or not sys.platform.startswith("linux"),
    reason="bench requires Linux + waitbus on PATH",
)
def test_smoke_mode_writes_progress_jsonl_next_to_verdict(tmp_path: Path) -> None:
    """The progress.jsonl file lives next to verdict.json and is non-empty."""
    output = tmp_path / "myverdict.json"
    rc = main(
        argv=[
            "--smoke",
            "--output",
            str(output),
        ]
    )
    assert rc == 0
    progress_path = output.with_suffix("").with_suffix(".progress.jsonl")
    assert progress_path.exists(), f"progress.jsonl missing at {progress_path}"
    lines = progress_path.read_text(encoding="utf-8").splitlines()
    # Always at least: daemon_ready + outlier_pilot + 10 windows + verdict_written.
    assert len(lines) >= 12


@pytest.mark.skipif(
    not _has_waitbus() or not sys.platform.startswith("linux"),
    reason="bench requires Linux + waitbus on PATH",
)
def test_smoke_mode_no_api_key_substring_in_verdict(tmp_path: Path) -> None:
    """The verdict's serialised bytes must NOT contain ``sk-`` substrings.

    Defence-in-depth security check: even if a future bench commit
    accidentally records the OpenAI key into a struct field, the
    encoded verdict cannot contain ``sk-`` substrings.
    """
    output = tmp_path / "secured.json"
    rc = main(argv=["--smoke", "--output", str(output)])
    assert rc == 0
    raw = output.read_bytes()
    # No realistic OpenAI key prefix should appear anywhere.
    assert b"sk-" not in raw or raw.find(b"sk-") == -1


# ---------------------------------------------------------------------
# Preflight-failure path: missing CLI / missing key.
# ---------------------------------------------------------------------


def test_main_aborts_when_openai_key_missing_and_real_llm_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--include-real-llm`` without a keyring key exits 1, NOT raise."""
    monkeypatch.setattr(
        "benchmarks.bench_multistream_proof.read_openai_key_from_keyring",
        lambda: None,
    )
    rc = main(argv=["--include-real-llm", "--n", "2"])
    assert rc == 1


def test_main_smoke_skips_real_llm_even_if_flag_passed() -> None:
    """``--smoke --include-real-llm`` honours smoke (no LLM, no key required)."""
    if not _has_waitbus() or not sys.platform.startswith("linux"):
        pytest.skip("bench requires Linux + waitbus on PATH")
    # The smoke gate forces include_real_llm=False internally, so the
    # keyring is NOT looked up. The verdict is produced regardless of
    # whether an OPENAI key is configured on the host.
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        output = Path(td) / "verdict.json"
        rc = main(argv=["--smoke", "--include-real-llm", "--output", str(output)])
        assert rc == 0
        assert output.exists()


# ---------------------------------------------------------------------
# Loaded-arm cost capture: framework mapping, cost sink, unknown tally.
# ---------------------------------------------------------------------


def _make_openai_iteration_row(*, driver: str, input_tokens: int, output_tokens: int) -> IterationRow:
    """Build a loaded-arm IterationRow with an OpenAIEnvelope."""
    return IterationRow(
        iter_id=0,
        arm="loaded",
        driver=driver,
        sentinel="sentinel-0",
        t_send_ns=0,
        t_observe_ns=0,
        latency_ns=0,
        cache_state="COLD",
        claude_env=None,
        gemini_env=None,
        openai_env=OpenAIEnvelope(
            model="openai-gpt-4o-mini",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=0,
            finish_reason="stop",
        ),
        invariant_failed=False,
        invariant_failure_field=None,
    )


def _make_empty_iteration_row(*, driver: str) -> IterationRow:
    """Build a loaded-arm IterationRow with all envelopes None."""
    return IterationRow(
        iter_id=0,
        arm="loaded",
        driver=driver,
        sentinel="sentinel-0",
        t_send_ns=0,
        t_observe_ns=0,
        latency_ns=0,
        cache_state="NA",
        claude_env=None,
        gemini_env=None,
        openai_env=None,
        invariant_failed=False,
        invariant_failure_field=None,
    )


def test_cost_budget_tracker_record_unknown_increments_unknown_count() -> None:
    """``record_unknown`` advances the unknown-cost call tally."""

    tracker = CostBudgetTracker(max_usd=5.0)
    assert tracker.unknown_usd_call_count == 0
    tracker.record_unknown()
    tracker.record_unknown()
    assert tracker.unknown_usd_call_count == 2
    assert tracker.observed_usd == 0.0


def test_cost_sink_openai_row_advances_observed_usd() -> None:
    """A loaded row with an OpenAI envelope advances observed_usd."""
    from benchmarks.bench_multistream_proof import _record_row_cost

    tracker = CostBudgetTracker(max_usd=5.0)
    row = _make_openai_iteration_row(driver="pydantic", input_tokens=1000, output_tokens=500)
    _record_row_cost(tracker, row)
    assert tracker.observed_usd > 0.0
    assert tracker.unknown_usd_call_count == 0


def test_cost_sink_llm_row_with_no_envelope_advances_unknown() -> None:
    """A loaded LLM-driver row with all-None envelopes advances unknown_usd_call_count."""
    from benchmarks.bench_multistream_proof import _record_row_cost

    tracker = CostBudgetTracker(max_usd=5.0)
    row = _make_empty_iteration_row(driver="langgraph")
    _record_row_cost(tracker, row)
    assert tracker.unknown_usd_call_count == 1
    assert tracker.observed_usd == 0.0


def test_cost_sink_shell_control_row_advances_neither() -> None:
    """A non-LLM shell-control row contributes neither cost nor unknown."""
    from benchmarks.bench_multistream_proof import _record_row_cost

    tracker = CostBudgetTracker(max_usd=5.0)
    row = _make_empty_iteration_row(driver="shell-control")
    _record_row_cost(tracker, row)
    assert tracker.unknown_usd_call_count == 0
    assert tracker.observed_usd == 0.0


# ---------------------------------------------------------------------
# Attrition -> window rejection (consequence-free gate fix).
# ---------------------------------------------------------------------


def test_measure_window_producer_attrition_rejects_window() -> None:
    """A loaded window whose workload carries producer attrition is rejected."""
    import os

    from benchmarks.bench_multistream_proof import _measure_window, _WorkloadResult

    def _runner() -> _WorkloadResult:
        return _WorkloadResult(producer_attrition=True)

    row, _result = _measure_window(
        daemon_pid=os.getpid(),
        arm="loaded",
        window_id=0,
        workload_runner=_runner,
    )
    assert row.rejected is True
    assert row.rejection_reason == "loaded_arm_attrition"


def test_measure_window_subscriber_attrition_rejects_window() -> None:
    """A loaded window whose workload carries subscriber attrition is rejected."""
    import os

    from benchmarks.bench_multistream_proof import _measure_window, _WorkloadResult

    def _runner() -> _WorkloadResult:
        return _WorkloadResult(subscriber_attrition=True)

    row, _result = _measure_window(
        daemon_pid=os.getpid(),
        arm="loaded",
        window_id=0,
        workload_runner=_runner,
    )
    assert row.rejected is True
    assert row.rejection_reason == "loaded_arm_attrition"


def test_measure_window_no_attrition_does_not_reject() -> None:
    """A clean loaded window (no attrition) is not rejected."""
    import os

    from benchmarks.bench_multistream_proof import _measure_window, _WorkloadResult

    def _runner() -> _WorkloadResult:
        return _WorkloadResult()

    row, _result = _measure_window(
        daemon_pid=os.getpid(),
        arm="loaded",
        window_id=0,
        workload_runner=_runner,
    )
    assert row.rejected is False
    assert row.rejection_reason is None


class _FakeDaemonProc:
    """A subprocess.Popen stand-in for daemon-teardown tests.

    Models three reap outcomes via ``die_on``:

    - ``"terminate"``: SIGTERM lands; the first ``wait`` returns cleanly.
    - ``"kill"``: SIGTERM is ignored (first ``wait`` raises
      ``TimeoutExpired``) but SIGKILL lands; the second ``wait`` returns.
    - ``"never"``: both signals are ignored; every ``wait`` raises
      ``TimeoutExpired`` (a process that survives SIGKILL).
    """

    def __init__(self, die_on: str, *, pid: int = 4321) -> None:
        self.die_on = die_on
        self.pid = pid
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout: float | None = None) -> int:
        if self.die_on == "terminate" and self.terminated:
            return 0
        if self.die_on == "kill" and self.killed:
            return 0
        raise subprocess.TimeoutExpired(cmd="waitbus", timeout=timeout or 0.0)


def test_terminate_daemon_with_grace_reaped_on_terminate() -> None:
    """A daemon that exits on SIGTERM is reaped."""
    from typing import cast

    from benchmarks.bench_multistream_proof import _terminate_daemon_with_grace

    proc = _FakeDaemonProc(die_on="terminate")
    outcome = _terminate_daemon_with_grace(
        cast("subprocess.Popen[bytes]", proc),
        term_sec=0.01,
        kill_sec=0.01,
    )
    assert outcome == "reaped"
    assert proc.terminated is True
    assert proc.killed is False


def test_terminate_daemon_with_grace_reaped_on_kill() -> None:
    """A daemon that ignores SIGTERM but dies on SIGKILL is reaped."""
    from typing import cast

    from benchmarks.bench_multistream_proof import _terminate_daemon_with_grace

    proc = _FakeDaemonProc(die_on="kill")
    outcome = _terminate_daemon_with_grace(
        cast("subprocess.Popen[bytes]", proc),
        term_sec=0.01,
        kill_sec=0.01,
    )
    assert outcome == "reaped"
    assert proc.terminated is True
    assert proc.killed is True


def test_terminate_daemon_with_grace_zombie_survives_sigkill() -> None:
    """A daemon that survives SIGKILL raises an error."""
    from typing import cast

    from benchmarks.bench_multistream_proof import _terminate_daemon_with_grace

    proc = _FakeDaemonProc(die_on="never")
    outcome = _terminate_daemon_with_grace(
        cast("subprocess.Popen[bytes]", proc),
        term_sec=0.01,
        kill_sec=0.01,
    )
    assert outcome == "zombie_after_sigkill"
    assert proc.terminated is True
    assert proc.killed is True


def test_wait_for_daemon_schema_returns_when_events_table_present(tmp_path: Path) -> None:
    """The schema wait returns once the events table exists in the DB."""
    import sqlite3

    from benchmarks.bench_multistream_proof import _wait_for_daemon_schema

    db_path = tmp_path / "events.db"
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.execute("CREATE TABLE events (seq INTEGER PRIMARY KEY)")
        conn.commit()

    # Returns without raising; a missing/half-migrated DB would block
    # to the deadline and raise instead.
    _wait_for_daemon_schema(db_path)


def test_wait_for_daemon_schema_raises_when_table_never_appears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The schema wait raises if the events table never lands by the deadline."""
    import benchmarks.bench_multistream_proof as mod

    # A DB file that exists but carries no events table must NOT satisfy
    # the gate (socket-bound != schema-migrated). Shrink the deadline so
    # the failure path is fast.
    monkeypatch.setattr(mod, "_DAEMON_READY_TIMEOUT_SEC", 0.2)
    db_path = tmp_path / "events.db"
    import sqlite3

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.execute("CREATE TABLE not_events (x INTEGER)")
        conn.commit()

    with pytest.raises(RuntimeError, match="did not migrate schema"):
        mod._wait_for_daemon_schema(db_path)


def test_wait_for_daemon_schema_raises_when_db_file_absent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A DB file that never appears trips the schema wait deadline."""
    import benchmarks.bench_multistream_proof as mod

    monkeypatch.setattr(mod, "_DAEMON_READY_TIMEOUT_SEC", 0.2)
    with pytest.raises(RuntimeError, match="did not migrate schema"):
        mod._wait_for_daemon_schema(tmp_path / "never-created.db")


def test_make_loaded_runner_advances_window_index_per_call(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The per-window runner advances its iter_id by one on each call.

    Pins the closure counter contract after the single-element-list
    aliasing trick was replaced with a ``nonlocal`` counter: successive
    runner calls must produce iter_ids offset, offset+1, offset+2, ...
    """
    import benchmarks.bench_multistream_proof as mod
    from benchmarks.bench_multistream_proof import _make_loaded_runner, _WorkloadResult

    seen_iter_ids: list[int] = []

    def _record_iter(*, iter_id: int, **_kwargs: object) -> _WorkloadResult:
        seen_iter_ids.append(iter_id)
        return _WorkloadResult()

    monkeypatch.setattr(mod, "_do_workload_iteration", _record_iter)

    # include_real_llm=False short-circuits to the offline workload path,
    # which still increments the counter before returning -- so no daemon
    # / producer / agent spawn is needed to exercise the counter.
    runner = _make_loaded_runner(
        iter_id_offset=10,
        run_salt="test-salt",
        daemon_env={},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "bell",
        seed_scope_id="test-scope",
        include_real_llm=False,
        producer_count=0,
        producer_event_rate_hz=1.0,
        agent_frameworks=(),
        stderr_root=tmp_path / "stderr",
    )

    for _ in range(3):
        runner()

    assert seen_iter_ids == [10, 11, 12]


def test_make_loaded_runner_counters_are_independent_per_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each factory owns its own counter; two runners do not share state."""
    from typing import Any

    import benchmarks.bench_multistream_proof as mod
    from benchmarks.bench_multistream_proof import _make_loaded_runner, _WorkloadResult

    seen: list[int] = []

    def _record_iter(*, iter_id: int, **_kwargs: object) -> _WorkloadResult:
        seen.append(iter_id)
        return _WorkloadResult()

    monkeypatch.setattr(mod, "_do_workload_iteration", _record_iter)

    def _build() -> Any:
        return _make_loaded_runner(
            iter_id_offset=0,
            run_salt="test-salt",
            daemon_env={},
            socket_path=tmp_path / "sock",
            db_path=tmp_path / "db",
            doorbell_path=tmp_path / "bell",
            seed_scope_id="test-scope",
            include_real_llm=False,
            producer_count=0,
            producer_event_rate_hz=1.0,
            agent_frameworks=(),
            stderr_root=tmp_path / "stderr",
        )

    runner_a = _build()
    runner_b = _build()
    runner_a()  # iter 0 on A
    runner_a()  # iter 1 on A
    runner_b()  # iter 0 on B (independent)

    assert seen == [0, 1, 0]


# Silence unused-import warnings on the test-time imports that are kept
# for forward-compat with future test cases (verdict-shape evolution +
# capture-side helpers).
_capture_external_state_ref = capture_external_state
_time_ref = time
