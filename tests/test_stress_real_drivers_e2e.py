"""End-to-end real-mode driver test against an actual waitbus daemon.

Spawns the full `python -m scripts.stress._controller --real --sweep 2`
operator path (real daemon, real Pydantic AI subprocess + real LangGraph
subprocess, real `agent_message` seed event on a real AF_UNIX bus) and
asserts the orchestrator collected one reaction from each framework.

This is the regression gate for the bug class that previously slipped
through the 16 unit-shaped real-driver tests: the harness invented a
seed event_type (`stress_real_seed_<hex>`) that was not in
`event_types_supported()`, so the daemon's `_fan_out` rejected every
fan-out (`broadcast.py::_fan_out` skips frames whose event_type is not
in the subscriber's accepted set). Unit tests with mocked emit / wait
paths could not catch it; only a real-bus run can.

N=2 framework split is `{pydantic: 1, langgraph: 1}` -- both OpenAI-backed
roles. Under `--real` the silent offline fallback is now closed: an absent
`OPENAI_API_KEY` aborts at preflight (covered by the abort test); the live
happy path runs only when a real key is present. Skips if `claude` or
`gemini` is absent from PATH (the controller's fail-fast auth-smoke runs
unconditionally in --real mode; this test exercises the post-smoke flow and
so still depends on smoke passing).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_REQUIRES_AUTH_CLIS = pytest.mark.skipif(
    shutil.which("claude") is None or shutil.which("gemini") is None or shutil.which("waitbus") is None,
    reason="real-mode auth-smoke requires claude, gemini, and waitbus in PATH",
)


@_REQUIRES_AUTH_CLIS
def test_real_mode_n2_requires_openai_key_or_aborts(tmp_path: Path) -> None:
    """Without OPENAI_API_KEY, ``--real --sweep 2`` (pydantic+langgraph) MUST abort.

    Regression gate for the silent real-LLM driver fallback: the N=2 mix is
    ``{pydantic: 1, langgraph: 1}`` -- both OpenAI-backed roles. Under
    ``--real`` an absent ``OPENAI_API_KEY`` previously downgraded both drivers
    to offline fakes (``TestModel`` / ``FakeListChatModel``) silently while
    still counting them toward the reaction total. The fix makes that a loud
    preflight abort (controller exits 2, names ``OPENAI_API_KEY``, writes no
    verdict). This test runs ONLY when the key is absent; the happy path with
    the key present is covered by ``test_real_mode_n2_with_openai_key_e2e``.
    """
    if os.environ.get("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY present; the abort path requires it absent")

    output = tmp_path / "verdict.json"
    progress = tmp_path / "progress.jsonl"
    state_dir = tmp_path / "state"
    state_dir.mkdir()

    env = dict(os.environ)
    env.pop("OPENAI_API_KEY", None)
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.stress._controller",
            "--real",
            "--sweep",
            "2",
            "--duration",
            "10s",
            "--output",
            str(output),
            "--progress",
            str(progress),
            "--state-dir",
            str(state_dir),
        ],
        capture_output=True,
        text=True,
        timeout=90.0,
        check=False,
        env=env,
    )

    assert proc.returncode == 2, (
        f"expected preflight abort (exit 2) with OPENAI_API_KEY absent; "
        f"got {proc.returncode}; stderr={proc.stderr[-500:]!r}"
    )
    assert "OPENAI_API_KEY" in proc.stderr, f"abort message must name OPENAI_API_KEY; stderr={proc.stderr[-500:]!r}"
    assert not output.exists(), "no verdict.json must be written on the preflight-abort path"


@_REQUIRES_AUTH_CLIS
def test_real_mode_n2_with_openai_key_e2e(tmp_path: Path) -> None:
    """Real daemon + 1 pydantic + 1 langgraph; assert both reacted on the bus.

    Runs ONLY when a real OPENAI_API_KEY is present (the pydantic / langgraph
    drivers exercise the live OpenAI path). With the key absent the run aborts
    at preflight -- that path is covered by
    ``test_real_mode_n2_requires_openai_key_or_aborts``.
    """
    if not os.environ.get("OPENAI_API_KEY"):
        pytest.skip("real OPENAI_API_KEY required for the live N=2 e2e happy path")

    output = tmp_path / "verdict.json"
    progress = tmp_path / "progress.jsonl"
    state_dir = tmp_path / "state"
    state_dir.mkdir()

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.stress._controller",
            "--real",
            "--sweep",
            "2",
            "--duration",
            "10s",
            "--output",
            str(output),
            "--progress",
            str(progress),
            "--state-dir",
            str(state_dir),
        ],
        capture_output=True,
        text=True,
        timeout=90.0,
        check=False,
    )

    # The controller exits 1 when overall_passed=False; at N=2 the strict
    # "all 5 frameworks observed" gate cannot pass (only 2 frameworks
    # active), so a non-zero exit is expected. The substantive assertions
    # land on the verdict.json fields.
    assert output.exists(), (
        f"verdict.json not written; controller stdout={proc.stdout[-500:]!r} stderr={proc.stderr[-500:]!r}"
    )
    verdict = json.loads(output.read_text())
    assert verdict["mode"] == "real"
    assert len(verdict["real_curve_points"]) == 1
    point = verdict["real_curve_points"][0]

    # Both spawned drivers reacted on the bus.
    assert point["reactions_received"] == 2, (
        f"expected 2 reactions, got {point['reactions_received']}; "
        f"progress: {progress.read_text() if progress.exists() else '(no progress.jsonl)'}"
    )
    assert point["unique_frameworks_observed"] == 2

    observed_frameworks = {r["framework"] for r in point["observed_reactions"]}
    assert observed_frameworks == {"pydantic", "langgraph"}, (
        f"expected {{pydantic, langgraph}} reactions, got {observed_frameworks}"
    )

    # Latency numbers populated (sanity check: not zero / not unreasonably high).
    assert 0.0 < point["median_reaction_latency_ms"] < 10000.0
    assert 0.0 < point["p99_reaction_latency_ms"] < 10000.0

    # Per-reaction provider field reflects the real-mode driver behaviour:
    # the live OpenAI path when the key is present (this test only runs with
    # it present). The offline providers are admissible ONLY on a real-mode
    # SDK ImportError degradation (the pre-existing import-failure fallback,
    # distinct from the now-closed key-absent silent fallback); both are
    # permitted by the subset assertion.
    observed_providers = {r["provider"] for r in point["observed_reactions"]}
    from scripts.stress._real_drivers import (
        PROVIDER_OFFLINE_LANGGRAPH,
        PROVIDER_OFFLINE_PYDANTIC,
        PROVIDER_OPENAI_GPT_4_1_NANO,
    )

    assert observed_providers <= {
        PROVIDER_OPENAI_GPT_4_1_NANO,
        PROVIDER_OFFLINE_PYDANTIC,
        PROVIDER_OFFLINE_LANGGRAPH,
    }, f"unexpected providers: {observed_providers}"

    # The verdict-level provider distribution carries the same rollup.
    assert sum(verdict["provider_distribution"].values()) == 2

    # cross_broadcast_proven == False is correct at N<5 by design: the
    # proven flag requires ALL 5 frameworks (FRAMEWORK_ORDER length).
    # At N=2 the framework_mix is {pydantic:1, langgraph:1, claude-cli:0,
    # gemini-cli:0, shell-control:0} so only 2 of 5 unique frameworks
    # ever react. The assertion documents the design boundary.
    assert point["cross_broadcast_proven"] is False

    # Source-mix propagation: the per-window seed event was drawn from
    # the registered soak taxonomy via ``pick_source_for_iter``. The
    # sweep ran one window (N=2), so the histogram sums to 1 and every
    # key is in the registered taxonomy.
    assert "per_iter_source_distribution" in verdict
    source_dist = verdict["per_iter_source_distribution"]
    assert sum(source_dist.values()) == 1, f"expected exactly one iter pick at sweep=2, got {source_dist}"
    registered_source_names = {"github", "pytest", "docker", "fs", "agent"}
    assert set(source_dist.keys()) <= registered_source_names, f"unregistered source name in {source_dist.keys()}"

    # Refusal / upstream-error invariant: a clean N=2 window has no LLM
    # refusal (or no LLM call at all if OPENAI_API_KEY is absent), so
    # the invariant-failure aggregator must report zero. A non-zero
    # value here would indicate the moderation envelope wired into the
    # rehydration path is false-positiving on the offline / clean path.
    assert verdict["invariant_failure_count"] == 0
