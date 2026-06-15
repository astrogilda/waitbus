"""Tokens-saved bench: per-source modelled savings from a populated DB.

Not a latency bench. This bench seeds an isolated SQLite events DB
with a representative mixed-source distribution, invokes
``waitbus stats --json`` against it, and emits the captured tripartite
output as the bench result. The captured artefact lets a CI gate
detect drift in:

- The per-source default token costs (a regression in
  ``DEFAULT_POLL_COST_*`` would show up as different
  ``modelled_savings_tokens`` for the same seed).
- The output schema (the assertion that ``measured`` / ``estimated``
  / ``computed`` are the three banners with the expected per-source
  rows).
- The aggregate-equals-sum invariant (computed at this layer in
  addition to the test-level assertion).

This bench (rather than a plain pytest test) serves as the article 02
'Estimating your savings' worked example references the JSON shape
the bench produces. Committing ``benchmarks/baselines/tokens_saved.json``
gives the article a stable artefact to point at and gives the
``--check-regression`` gate a real number to compare against.

Sample posture
--------------
N=10000 synthetic events distributed across the four sources at the
plan's expected mix (50% github, 20% pytest, 20% docker, 10% fs).
Single phase (no gc-disabled split needed; this is not a latency
bench). Wall-clock ~3 s including emit + waitbus-stats subprocess.

Invocation
----------
::

    # Smoke
    uv run python -m benchmarks.bench_tokens_saved --smoke

    # Baseline
    uv run python -m benchmarks.bench_tokens_saved \\
        --output benchmarks/baselines/tokens_saved.json

    # Regression gate (compares aggregate_modelled_savings_tokens
    # +/- 25% per the standard threshold)
    uv run python -m benchmarks.bench_tokens_saved --check-regression
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Final

import msgspec

from waitbus import _emit as emit_mod
from waitbus._types import EventInsert

from ._harness import environment_report, resolve_output_path

_BENCH_NAME = "tokens_saved"
_DEFAULT_N = 10_000
_SMOKE_N = 100
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_SUBPROCESS_TIMEOUT_SEC: Final[float] = 60.0
"""Timeout for the ``waitbus stats --json`` subprocess call (seconds).

60 s is generous for a CLI that reads a seeded SQLite DB and writes
JSON to stdout; it protects against a hung subprocess without blocking
the bench result path indefinitely. The actual call typically
completes in under 1 s.
"""

# Plan-stated source distribution for the corpus replay (also used
# here for the synthetic seed). The numbers reflect the typical mix
# an agent encounters in a workstation session.
_SOURCE_DISTRIBUTION: dict[str, float] = {
    "github": 0.50,
    "pytest": 0.20,
    "docker": 0.20,
    "fs": 0.10,
}

_EVENT_TYPE_BY_SOURCE: dict[str, str] = {
    "github": "workflow_run",
    "pytest": "pytest_session",
    "docker": "docker_container",
    "fs": "fs_change",
}

# Regression gate: an aggregate that drifts more than this fraction
# from the committed baseline fails the --check-regression run. The
# 25% threshold matches the latency benches; the aggregate is the sum
# of per-source events x per-source cost, so a drift of >25% means
# either the seed distribution changed, the per-source default
# changed, or both.
_REGRESSION_THRESHOLD: float = 0.25


def _seed_events(db_path: Path, n: int) -> dict[str, int]:
    """Seed ``db_path`` with ``n`` synthetic events at the canonical mix.

    Returns the per-source counts so the caller can verify the seed
    matches the expected distribution before reading the stats output
    back. Source order is round-robin-weighted so consecutive events
    cover all four sources rather than emitting all of one then all
    of another.
    """
    targets = {source: round(n * fraction) for source, fraction in _SOURCE_DISTRIBUTION.items()}
    # Reconcile rounding so the per-source counts sum to exactly n.
    delta = n - sum(targets.values())
    if delta != 0:
        # Drop the slack onto the largest source (github).
        targets["github"] += delta

    events: list[EventInsert] = []
    now_ns = time.time_ns()
    for source, count in targets.items():
        event_type = _EVENT_TYPE_BY_SOURCE[source]
        for i in range(count):
            events.append(
                EventInsert(
                    delivery_id=f"tokens-saved-seed:{source}:{i}-{now_ns}",
                    source=source,
                    event_type=event_type,
                    owner="bench",
                    repo="tokens-saved",
                    received_at=now_ns + len(events),
                    payload_json="{}",
                    ingest_method="bench",
                    status="completed",
                    conclusion="success",
                )
            )

    emit_mod.emit_batch(events, db_path=db_path)
    return {source: count for source, count in targets.items()}


def _invoke_waitbus_stats(db_path: Path) -> dict[str, Any]:
    """Run ``waitbus stats --json --db <path>`` and return the parsed JSON.

    The bench invokes the actual installed console-script so the
    captured artefact reflects what an operator would see. If the
    script is absent (e.g. running outside an installed venv) the
    bench falls back to invoking ``waitbus.cli.stats`` via
    ``uv run`` so the test still produces a result.
    """
    waitbus_bin = shutil.which("waitbus")
    if waitbus_bin is not None:
        argv = [waitbus_bin, "stats", "--json", "--db", str(db_path)]
    else:
        argv = ["uv", "run", "waitbus", "stats", "--json", "--db", str(db_path)]
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=_SUBPROCESS_TIMEOUT_SEC, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"waitbus stats failed (rc={proc.returncode}): {proc.stderr.strip()}")
    return json.loads(proc.stdout)  # type: ignore[no-any-return]


def _check_invariants(stats_output: dict[str, Any]) -> None:
    """Assert the structural invariants the model promises.

    1. Three banners present: ``measured`` / ``estimated`` / ``computed``.
    2. ``computed.aggregate_modelled_savings_tokens`` equals the
       deterministic sum of per-source ``modelled_savings_tokens``.
    3. Every known source appears in the per-source rows of both
       estimated and computed.
    """
    banners = set(stats_output.keys())
    expected_banners = {"measured", "estimated", "computed"}
    if banners != expected_banners:
        raise RuntimeError(f"waitbus stats output banners {banners} != expected {expected_banners}")

    computed = stats_output["computed"]
    aggregate = computed["aggregate_modelled_savings_tokens"]
    summed = sum(row["modelled_savings_tokens"] for row in computed["per_source"])
    if aggregate != summed:
        raise RuntimeError(
            f"aggregate_modelled_savings_tokens ({aggregate}) does not equal sum of per-source ({summed})"
        )

    estimated_sources = {row["source"] for row in stats_output["estimated"]["per_source"]}
    computed_sources = {row["source"] for row in computed["per_source"]}
    expected_sources = set(_SOURCE_DISTRIBUTION)
    missing = expected_sources - (estimated_sources & computed_sources)
    if missing:
        raise RuntimeError(f"per-source rows missing from waitbus stats output: {missing}")


def _write_result(
    *,
    path: Path,
    stats_output: dict[str, Any],
    seed_counts: dict[str, int],
    n_events: int,
    started_at_ns: int,
    ended_at_ns: int,
    smoke: bool,
) -> None:
    """Write the bench result JSON atomically.

    The result carries the full stats output (so a downstream consumer
    has every per-source detail), the seed_counts (so the seed is
    reconstructable), and the environment report (so future re-runs
    can detect environment drift).
    """
    env = environment_report()
    payload: dict[str, Any] = {
        "bench_name": _BENCH_NAME,
        "started_at_ns": started_at_ns,
        "ended_at_ns": ended_at_ns,
        "n_events": n_events,
        "smoke": smoke,
        "seed_counts": seed_counts,
        "stats_output": stats_output,
        "environment": msgspec.to_builtins(env),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, indent=2, default=str).encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".partial")
    tmp.write_bytes(encoded)
    tmp.replace(path)


def _check_regression(current: dict[str, Any], baseline_path: Path) -> tuple[bool, str]:
    """Compare current aggregate against committed baseline.

    Returns ``(ok, message)``. Threshold is the standard 25% gate.
    If the baseline is absent the call returns (True, "no baseline").
    """
    if not baseline_path.exists():
        return True, f"no baseline at {baseline_path}; first run is the baseline"
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    current_agg = current["stats_output"]["computed"]["aggregate_modelled_savings_tokens"]
    baseline_agg = baseline["stats_output"]["computed"]["aggregate_modelled_savings_tokens"]
    if baseline_agg <= 0:
        return False, f"baseline aggregate is {baseline_agg} (invalid)"
    ratio = current_agg / baseline_agg
    if abs(ratio - 1.0) > _REGRESSION_THRESHOLD:
        return (
            False,
            f"aggregate drift: current={current_agg} vs baseline={baseline_agg} "
            f"(ratio={ratio:.3f}; threshold=+/-{_REGRESSION_THRESHOLD * 100:.0f}%)",
        )
    return (
        True,
        f"aggregate ok: current={current_agg} vs baseline={baseline_agg} (ratio={ratio:.3f})",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Tokens-saved bench: per-source modelled savings from a populated DB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--n",
        type=int,
        default=_DEFAULT_N,
        help=f"number of synthetic events to seed (default: {_DEFAULT_N}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(f"path to write the result JSON (default: benchmarks/results/{_BENCH_NAME}_<host>_<ts>.json)."),
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="quick run: N=100, skips regression check.",
    )
    parser.add_argument(
        "--check-regression",
        action="store_true",
        help=(
            f"after the run, compare aggregate against "
            f"{_BASELINE_PATH.relative_to(_BASELINE_PATH.parent.parent)}; "
            "exit non-zero on +/-25 percent drift."
        ),
    )
    args = parser.parse_args(argv)

    n_events = _SMOKE_N if args.smoke else args.n
    print(f"[{_BENCH_NAME}] n={n_events} (smoke={args.smoke})", file=sys.stderr)

    started_at_ns = time.time_ns()
    with tempfile.TemporaryDirectory(prefix=f"waitbus-bench-{_BENCH_NAME}-") as tmp_str:
        tmp_dir = Path(tmp_str)
        db_path = tmp_dir / "events.db"
        from waitbus import _db

        _db.ensure_schema(db_path)

        print(f"[{_BENCH_NAME}] seeding {n_events} events...", file=sys.stderr)
        seed_counts = _seed_events(db_path, n_events)
        print(
            f"[{_BENCH_NAME}] seed: " + ", ".join(f"{k}={v}" for k, v in seed_counts.items()),
            file=sys.stderr,
        )

        print(f"[{_BENCH_NAME}] invoking waitbus stats --json...", file=sys.stderr)
        stats_output = _invoke_waitbus_stats(db_path)

        print(f"[{_BENCH_NAME}] checking invariants...", file=sys.stderr)
        _check_invariants(stats_output)

    ended_at_ns = time.time_ns()

    env = environment_report()
    output_path = resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)

    _write_result(
        path=output_path,
        stats_output=stats_output,
        seed_counts=seed_counts,
        n_events=n_events,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        smoke=args.smoke,
    )
    print(f"[{_BENCH_NAME}] wrote {output_path}", file=sys.stderr)

    computed = stats_output["computed"]
    print(f"[{_BENCH_NAME}] aggregate_modelled_savings_tokens: {computed['aggregate_modelled_savings_tokens']}")
    print(f"[{_BENCH_NAME}] aggregate_polls_avoided: {computed['aggregate_polls_avoided']}")
    print(f"[{_BENCH_NAME}] per-source:")
    for row in computed["per_source"]:
        print(
            f"[{_BENCH_NAME}]   {row['source']}: "
            f"events={row['events_observed']} x cost={row['per_poll_tokens']} "
            f"= {row['modelled_savings_tokens']}"
        )

    if args.check_regression and not args.smoke:
        current = {"stats_output": stats_output}
        ok, msg = _check_regression(current, _BASELINE_PATH)
        print(f"[{_BENCH_NAME}] regression-check: {msg}", file=sys.stderr)
        if not ok:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
