"""Bench files must adopt the shared `_harness` helpers.

Two helpers in `benchmarks/_harness.py` centralise patterns that were
previously duplicated across every bench main:

* ``resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)``
  -- canonical output-path resolution. Every bench that takes
  ``--output`` calls this.
* ``print_percentile_summary(result, bench_name=_BENCH_NAME)`` --
  canonical p50/p90/p99 + sample-count summary print.

Adopting these helpers consistently is a contract: a future bench
author who copy-pastes the inline ``time.strftime(...)`` pattern from
an older file (instead of importing the helper) silently re-introduces
the output-path drift this convention prevents. This test AST-walks every
``benchmarks/bench_*.py`` and asserts each one either:

1. Imports the helper from ``._harness`` AND has at least one call
   site, OR
2. Is explicitly listed in ``_HELPER_EXEMPTIONS`` with a documented
   reason. The exemption list is intentionally small and each entry
   carries a one-line justification.

If a new bench is added the test fails on first run; the author
either wires the helper (the default) or extends the exemption list
with a justification reviewable in the diff.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_BENCH_DIR = _REPO_ROOT / "benchmarks"

#: Bench files that legitimately do NOT use ``resolve_output_path``
#: (each must justify why).
_RESOLVE_OUTPUT_PATH_EXEMPTIONS: dict[str, str] = {
    "bench_multistream_proof.py": (
        "writes verdict.json + progress.jsonl + .log under .local-stress-logs/<ts>.<bench>.* "
        "by design (matches the .local-stress-logs output convention shared with "
        "scripts/stress/_controller); resolve_output_path targets benchmarks/results/."
    ),
    "bench_polling_vs_subscribe_llm_agent.py": (
        "writes verdict.json + progress.jsonl + .log under .local-stress-logs/<ts>.<bench>.* "
        "by design (matches the .local-stress-logs output convention shared with "
        "scripts/stress/_controller); resolve_output_path targets benchmarks/results/."
    ),
    "bench_event_delivery_fidelity.py": (
        "writes verdict.json + progress.jsonl + .log under .local-stress-logs/<ts>.<bench>.* "
        "by design (matches the .local-stress-logs output convention shared with the other "
        "measurement benches); resolve_output_path targets benchmarks/results/."
    ),
    "bench_daemon_per_delivery_cost.py": (
        "calibration microbench; --output writes the verdict JSON to the exact caller-supplied "
        "path (or stdout only), not a timestamped benchmarks/results/ path. resolve_output_path "
        "generates the timestamped results-dir path and would change this output contract."
    ),
}

#: Bench files that legitimately do NOT use ``print_percentile_summary``
#: (each must justify why).
_PRINT_PERCENTILE_SUMMARY_EXEMPTIONS: dict[str, str] = {
    "bench_polling_baseline_github.py": (
        "prints latencies in seconds (/1e9) not milliseconds; the standard "
        "helper would change the unit of the published baseline."
    ),
    "bench_predicate_wait_under_mixed_load.py": (
        "prints an aggregate gc-enabled banner with extra bg=N + per-source "
        "breakdown; custom format cannot be collapsed into the standard helper."
    ),
    "bench_predicate_eval_latency_multi.py": (
        "does not print a percentile summary in the standard shape; prints a "
        "plugin-vs-builtin parity table that is exercised by its own dedicated "
        "regression assertion."
    ),
    "bench_idle_rss.py": (
        "measures RSS-in-MB, not latency in ns; has no p50/p90/p99 percentile tuple for the helper to print."
    ),
    "bench_throughput.py": (
        "prints throughput (events/sec) not latency percentiles; emits a different summary banner."
    ),
    "bench_tokens_saved.py": ("prints token-cost savings, not latency percentiles; emits a different summary shape."),
    "bench_notify_to_wake.py": ("specialised wake-latency banner; not a percentile-summary shape."),
    "bench_ttfae_first_match.py": ("first-match latency banner; not the standard p50/p90/p99 shape."),
    "bench_multistream_proof.py": (
        "v2 experiment bench: Mann-Whitney U on per-window daemon CPU deltas; "
        "no latency p50/p99 summary to print. Verdict contains perturbation_detected "
        "boolean + per-arm medians instead."
    ),
    "bench_polling_vs_subscribe_llm_agent.py": (
        "v2 experiment bench: per-driver median latency description + bootstrap "
        "CI bands across poll/subscribe arms; no single p50/p99 banner to print. "
        "Verdict carries per-driver _PerArmStats structs instead."
    ),
    "bench_event_delivery_fidelity.py": (
        "v2 experiment bench: per-arm event-delivery-latency / TTFT / wall-time medians + "
        "Wilcoxon paired-delta marginals across (alone / bus_idle / bus_swarm); no single "
        "p50/p99 banner to print. Verdict carries per-arm _ArmLatencyStats structs and the "
        "three Bonferroni-gated marginals instead."
    ),
    "bench_daemon_per_delivery_cost.py": (
        "calibration microbench: reports the daemon's per-delivery CPU-cost decomposition "
        "(per-subscriber marginal us + per-event fixed ms, utime and schedstat) as a JSON "
        "verdict, not a latency p50/p90/p99 tuple; there is no percentile summary to print."
    ),
}


def _bench_files() -> list[Path]:
    """Return every `benchmarks/bench_*.py` file."""
    return sorted(_BENCH_DIR.glob("bench_*.py"))


def _imports_from_harness(tree: ast.AST) -> set[str]:
    """Return the set of names imported from ``._harness`` (or its dotted form)."""
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.endswith("_harness") or module == "_harness":
                for alias in node.names:
                    imported.add(alias.asname or alias.name)
    return imported


def _calls_named(tree: ast.AST, fn_name: str) -> bool:
    """Return True if `tree` calls a function named `fn_name` (positional)."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == fn_name:
                return True
    return False


@pytest.mark.parametrize("bench_file", _bench_files(), ids=lambda p: p.name)
def test_bench_imports_and_uses_resolve_output_path(bench_file: Path) -> None:
    """Every bench file either uses ``resolve_output_path`` or is an exempted outlier."""
    if bench_file.name in _RESOLVE_OUTPUT_PATH_EXEMPTIONS:
        return
    tree = ast.parse(bench_file.read_text(encoding="utf-8"))
    imported = _imports_from_harness(tree)
    assert "resolve_output_path" in imported, (
        f"{bench_file.name} must import ``resolve_output_path`` from "
        f"``._harness`` (or be listed in the exemption dict at the top of "
        f"this test file with a written justification)"
    )
    assert _calls_named(tree, "resolve_output_path"), (
        f"{bench_file.name} imports ``resolve_output_path`` but never calls it; "
        f"replace the inline ``time.strftime(...)`` block with the helper call"
    )


@pytest.mark.parametrize("bench_file", _bench_files(), ids=lambda p: p.name)
def test_bench_imports_and_uses_print_percentile_summary(bench_file: Path) -> None:
    """Every percentile-shaped bench either uses ``print_percentile_summary`` or is exempted."""
    if bench_file.name in _PRINT_PERCENTILE_SUMMARY_EXEMPTIONS:
        return
    tree = ast.parse(bench_file.read_text(encoding="utf-8"))
    imported = _imports_from_harness(tree)
    assert "print_percentile_summary" in imported, (
        f"{bench_file.name} must import ``print_percentile_summary`` from "
        f"``._harness`` (or be listed in the exemption dict with a "
        f"justification)"
    )
    assert _calls_named(tree, "print_percentile_summary"), (
        f"{bench_file.name} imports ``print_percentile_summary`` but never "
        f"calls it; replace the inline p50/p99 print with the helper call"
    )
