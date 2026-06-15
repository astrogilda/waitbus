"""Contract tests for the bench pilot-skip semantic.

The two v2 benches that carry a pilot phase
(``bench_polling_vs_subscribe_llm_agent`` and ``bench_multistream_proof``)
SHALL skip the pilot when the bench's downstream measurement is
structurally inapplicable. Concretely::

    should_skip_pilot = args.smoke or not args.include_real_llm

When skipped, the verdict records ``pilot_skipped=True`` and
``pilot_skipped_reason`` is one of two pinned literals
(``"smoke_mode"`` or ``"real_llm_disabled"``; smoke takes precedence).
The main loop runs regardless so the operator gets a verdict.json that
confirms the wiring is alive.

The bench A cases reuse the patched-runtime fixture from
``test_bench_polling_vs_subscribe_llm_agent`` so no real daemon,
subprocess, or LLM call runs. The bench B cases run against the real
daemon (matching the testing philosophy for the cross-process bus
surface) and skip cleanly when ``waitbus`` is not on PATH.
"""

from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

import msgspec
import pytest

from benchmarks.bench_multistream_proof import (
    ExperimentBVerdict,
)
from benchmarks.bench_multistream_proof import (
    main as bench_b_main,
)
from benchmarks.bench_polling_vs_subscribe_llm_agent import (
    ExperimentAVerdict,
)
from benchmarks.bench_polling_vs_subscribe_llm_agent import (
    main as bench_a_main,
)
from tests.test_bench_polling_vs_subscribe_llm_agent import _patch_bench_runtime

# ---------------------------------------------------------------------
# Skip predicates for the bench B real-daemon cases.
# ---------------------------------------------------------------------


def _has_waitbus() -> bool:
    return shutil.which("waitbus") is not None


# ---------------------------------------------------------------------
# Bench A: pilot-skip contract under all flag combinations.
# ---------------------------------------------------------------------


def test_bench_a_smoke_with_skip_real_llm_skips_pilot_with_reason_smoke_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--smoke --skip-real-llm`` skips the pilot; smoke wins precedence."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    _patch_bench_runtime(monkeypatch)
    output_dir = tmp_path / "out"
    rc = bench_a_main(["--smoke", "--n", "2", "--output", str(output_dir), "--skip-real-llm"])
    assert rc == 0

    verdict_files = list(output_dir.glob("*.verdict.json"))
    assert len(verdict_files) == 1
    verdict = msgspec.json.decode(verdict_files[0].read_bytes(), type=ExperimentAVerdict)

    assert verdict.pilot_skipped is True
    assert verdict.pilot_skipped_reason == "smoke_mode"
    assert verdict.pilot_sigma_ms is None
    assert verdict.pilot_passed is True
    assert verdict.n_iterations_per_arm == 2


def test_bench_a_skip_real_llm_alone_skips_pilot_with_reason_real_llm_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--skip-real-llm`` without ``--smoke`` skips the pilot with the
    ``real_llm_disabled`` reason literal."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    _patch_bench_runtime(monkeypatch)
    output_dir = tmp_path / "out"
    rc = bench_a_main(["--n", "2", "--output", str(output_dir), "--skip-real-llm"])
    assert rc == 0

    verdict_files = list(output_dir.glob("*.verdict.json"))
    assert len(verdict_files) == 1
    verdict = msgspec.json.decode(verdict_files[0].read_bytes(), type=ExperimentAVerdict)

    assert verdict.pilot_skipped is True
    assert verdict.pilot_skipped_reason == "real_llm_disabled"
    assert verdict.pilot_sigma_ms is None
    assert verdict.pilot_passed is True


def test_bench_a_smoke_alone_skips_pilot_with_reason_smoke_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--smoke`` alone (real-LLM default ON, mocked) skips the pilot
    with the ``smoke_mode`` reason."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    _patch_bench_runtime(monkeypatch)
    # Mock the keyring + preflight so include_real_llm=True default path
    # does not need actual OPENAI / claude / gemini on the host.
    monkeypatch.setattr(
        "benchmarks._bench_preflight.read_openai_key_from_keyring",
        lambda: "sk-test-fixture-not-a-real-key",
    )

    import benchmarks._bench_preflight as preflight_module

    real_preflight = preflight_module.run_preflight_assertions

    def fake_preflight(**kwargs: object) -> object:
        kwargs["require_openai"] = False
        kwargs["require_claude_cli"] = False
        kwargs["require_gemini_cli"] = False
        return real_preflight(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        "benchmarks.bench_polling_vs_subscribe_llm_agent.run_preflight_assertions",
        fake_preflight,
    )
    output_dir = tmp_path / "out"
    rc = bench_a_main(["--smoke", "--n", "2", "--output", str(output_dir)])
    assert rc == 0

    verdict_files = list(output_dir.glob("*.verdict.json"))
    assert len(verdict_files) == 1
    verdict = msgspec.json.decode(verdict_files[0].read_bytes(), type=ExperimentAVerdict)

    assert verdict.pilot_skipped is True
    assert verdict.pilot_skipped_reason == "smoke_mode"


def test_bench_a_include_real_llm_runs_pilot_with_skipped_false(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--include-real-llm`` runs the pilot; the verdict carries
    ``pilot_skipped=False`` and the gate-widening behaviour from the
    real-LLM path is preserved."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    _patch_bench_runtime(monkeypatch)
    monkeypatch.setattr(
        "benchmarks._bench_preflight.read_openai_key_from_keyring",
        lambda: "sk-test-fixture-not-a-real-key",
    )

    import benchmarks._bench_preflight as preflight_module

    real_preflight = preflight_module.run_preflight_assertions

    def fake_preflight(**kwargs: object) -> object:
        kwargs["require_openai"] = False
        kwargs["require_claude_cli"] = False
        kwargs["require_gemini_cli"] = False
        return real_preflight(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        "benchmarks.bench_polling_vs_subscribe_llm_agent.run_preflight_assertions",
        fake_preflight,
    )
    output_dir = tmp_path / "out"
    rc = bench_a_main(["--n", "2", "--output", str(output_dir), "--include-real-llm"])
    assert rc == 0

    verdict_files = list(output_dir.glob("*.verdict.json"))
    assert len(verdict_files) == 1
    verdict = msgspec.json.decode(verdict_files[0].read_bytes(), type=ExperimentAVerdict)

    assert verdict.pilot_skipped is False
    assert verdict.pilot_skipped_reason is None
    # Pilot ran. Under the patched runtime every shell-control latency
    # is near-zero so the gate (40 ms when include_real_llm=True) is
    # comfortably under-shot.
    assert verdict.pilot_passed is True
    # The 40 ms real-LLM gate is preserved in the verdict.
    assert verdict.pilot_sigma_gate_ms_used == pytest.approx(40.0)


def test_bench_a_pilot_skipped_reason_smoke_dominates_when_both_flags_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--smoke --skip-real-llm`` pins the smoke-precedence rule."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    _patch_bench_runtime(monkeypatch)
    output_dir = tmp_path / "out"
    rc = bench_a_main(["--smoke", "--skip-real-llm", "--n", "2", "--output", str(output_dir)])
    assert rc == 0
    verdict_files = list(output_dir.glob("*.verdict.json"))
    verdict = msgspec.json.decode(verdict_files[0].read_bytes(), type=ExperimentAVerdict)
    assert verdict.pilot_skipped_reason == "smoke_mode"


def test_bench_a_pilot_skipped_emits_structured_log_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The pilot-skip path emits a ``bench_pilot_skipped`` structured-log
    line with the reason field."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench")
    _patch_bench_runtime(monkeypatch)
    output_dir = tmp_path / "out"
    with caplog.at_level(logging.INFO, logger="waitbus.bench.poll_vs_subscribe_llm"):
        rc = bench_a_main(["--smoke", "--n", "2", "--output", str(output_dir), "--skip-real-llm"])
    assert rc == 0
    matching = [record for record in caplog.records if "bench_pilot_skipped" in record.getMessage()]
    assert len(matching) >= 1, "expected at least one bench_pilot_skipped structured log line"
    # The reason literal is present in the JSON payload.
    assert any('"reason":"smoke_mode"' in record.getMessage() for record in matching)


# ---------------------------------------------------------------------
# Bench B: pilot-skip contract under all flag combinations.
#
# Bench B's smoke path runs against the real daemon; tests skip cleanly
# when ``waitbus`` is not on PATH. The verdict assertions are what we
# care about: the daemon + bus surface are exercised, the pilot is
# skipped, the outlier-rejection branch in ``_build_verdict`` honours
# the sentinel threshold.
# ---------------------------------------------------------------------


@pytest.mark.skipif(
    not _has_waitbus() or not sys.platform.startswith("linux"),
    reason="bench requires Linux + waitbus on PATH",
)
def test_bench_b_smoke_skips_pilot_with_reason_smoke_mode(tmp_path: Path) -> None:
    """``--smoke`` skips the pilot with reason ``smoke_mode``; outlier
    threshold is the sentinel 0; no window is rejected by an absent
    threshold."""
    output = tmp_path / "verdict.json"
    rc = bench_b_main(argv=["--smoke", "--output", str(output)])
    assert rc == 0
    verdict = msgspec.json.decode(output.read_bytes(), type=ExperimentBVerdict)

    assert verdict.pilot_skipped is True
    assert verdict.pilot_skipped_reason == "smoke_mode"
    assert verdict.outlier_threshold_ns == 0
    # No window is rejected by the absent threshold; the only rejection
    # path now is the per-row ``rejected`` flag from the measurement
    # loop (e.g. high context-switch deltas), which the smoke run does
    # not normally trip.
    rejected_by_threshold = sum(1 for row in verdict.windows if not row.rejected and row.daemon_utime_delta_ns > 0)
    # All such windows would have been rejected under the old
    # unconditional pilot (sentinel threshold 0); under the skip
    # contract they are accepted.
    assert verdict.rejected_window_count <= len(verdict.windows) - rejected_by_threshold


@pytest.mark.skipif(
    not _has_waitbus() or not sys.platform.startswith("linux"),
    reason="bench requires Linux + waitbus on PATH",
)
def test_bench_b_skip_real_llm_skips_pilot_with_reason_real_llm_disabled(
    tmp_path: Path,
) -> None:
    """Without ``--include-real-llm`` (bench B default), the pilot is
    skipped with reason ``real_llm_disabled``."""
    output = tmp_path / "verdict.json"
    rc = bench_b_main(argv=["--n", "5", "--output", str(output)])
    assert rc == 0
    verdict = msgspec.json.decode(output.read_bytes(), type=ExperimentBVerdict)

    assert verdict.pilot_skipped is True
    assert verdict.pilot_skipped_reason == "real_llm_disabled"
    assert verdict.outlier_threshold_ns == 0


@pytest.mark.skipif(
    not _has_waitbus() or not sys.platform.startswith("linux"),
    reason="bench requires Linux + waitbus on PATH",
)
def test_bench_b_progress_jsonl_carries_pilot_skipped_record(tmp_path: Path) -> None:
    """The progress.jsonl carries a ``kind=pilot_skipped`` record when
    the pilot is skipped."""
    output = tmp_path / "verdict.json"
    rc = bench_b_main(argv=["--smoke", "--output", str(output)])
    assert rc == 0
    progress_path = output.with_suffix("").with_suffix(".progress.jsonl")
    assert progress_path.exists()
    lines = progress_path.read_text(encoding="utf-8").splitlines()
    assert any('"pilot_skipped"' in line and '"kind"' in line for line in lines), (
        "expected a pilot_skipped progress record"
    )


# ---------------------------------------------------------------------
# Bench C: negative invariant. No pilot exists; the field is not
# propagated by "consistency" to a bench where it has no meaning.
# ---------------------------------------------------------------------
