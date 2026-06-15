"""Hawkes parameter sensitivity sweep for the benchmark corpus.

Quantifies how much the inter-arrival time distribution (p50/p95/p99)
changes when the Hawkes parameters (mu, alpha, beta) are perturbed
±10%, ±25%, or ±50% around the empirically-fit point estimates in
``benchmarks/data/gh_distributions.toml``.

Purpose
-------
This script defends against the objection that corpus inter-arrival
dynamics are sensitive to small errors in the fitted parameters.  If
±10% on any parameter shifts p99 by less than ±25%, the corpus shape
is stable enough for benchmark-latency comparisons; a larger shift
flags the need for a tighter fit.

The inter-arrival distribution is measured directly on the Hawkes
sampler (500 samples per config, seeded), not via a full benchmark
run.  Full benchmark latencies depend on many factors beyond
inter-arrival spacing; this sweep isolates the Hawkes parameter
contribution by measuring the quantity it directly controls.

Scope
-----
3 perturbation levels (+-10%, +-25%, +-50%)
x 3 parameters (mu, alpha, beta)
x 3 size classes (small, medium, large)
= 54 perturbed configs (27 upward, 27 downward)

Each config yields p50/p95/p99 inter-arrival times; the script
reports the percentage delta relative to the per-class baseline.

Invocation
----------
::

    # Quick smoke (one perturbation, one class):
    python scripts/sensitivity_sweep.py --smoke

    # Full sweep (all 54 configs):
    python scripts/sensitivity_sweep.py --full

    # Specify output paths:
    python scripts/sensitivity_sweep.py --full \\
        --json-out benchmarks/data/sensitivity-results.json \\
        --md-out benchmarks/HAWKES_SENSITIVITY.md

The full sweep is a maintainer-side workstation operation run on each
release cut.  CI only runs ``--smoke`` to verify the script is not
broken; the committed ``benchmarks/data/sensitivity-results.json`` is
regenerated from ``--full`` by the maintainer and committed alongside
the release.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Resolve the repo root from this file's location and put it on sys.path so
# the maintainer-only ``benchmarks.gen_corpus`` import below resolves when this
# script is run by path (``python scripts/sensitivity_sweep.py``) from any
# working directory. This is deliberate and load-bearing, NOT a redundant
# per-test insert: a standalone script run by path gets only its own directory
# on sys.path (pytest, by contrast, already adds the repo root via
# ``[tool.pytest.ini_options].pythonpath``, which is why the per-test inserts
# were removed). Module-mode (``python -m scripts.sensitivity_sweep``) would
# tie invocation to the repo root as cwd and lose that cwd-independence.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from benchmarks.gen_corpus import (  # noqa: E402
    _HAWKES_CLASSES,
    Distributions,
    HawkesState,
    _load_distributions,
)

_DEFAULT_TOML = _REPO_ROOT / "benchmarks" / "data" / "gh_distributions.toml"
_DEFAULT_JSON_OUT = _REPO_ROOT / "benchmarks" / "data" / "sensitivity-results.json"
_DEFAULT_MD_OUT = _REPO_ROOT / "benchmarks" / "HAWKES_SENSITIVITY.md"

# Perturbation levels as multipliers relative to baseline.
_PERTURBATIONS: dict[str, float] = {
    "-50%": 0.50,
    "-25%": 0.75,
    "-10%": 0.90,
    "+10%": 1.10,
    "+25%": 1.25,
    "+50%": 1.50,
}

# Parameters subject to perturbation.
_PARAMS: tuple[str, ...] = ("mu_per_sec", "alpha", "beta")

# Inter-arrival samples per config.  500 gives stable percentile
# estimates for p95 (rank 475) and adequate signal for p99 (rank 495).
_N_SAMPLES = 500
_SEED = 42

# Threshold-of-concern: if any p99 delta exceeds this under a ±10%
# parameter perturbation, the corpus is too sensitive and parameters
# need a stricter fit.
CONCERN_THRESHOLD_P99_AT_10PCT = 0.25  # 25%


@dataclass
class SweepPoint:
    """One perturbed configuration result."""

    size_class: str
    param: str
    perturbation_label: str
    multiplier: float
    perturbed_value: float
    baseline_p50_s: float
    baseline_p95_s: float
    baseline_p99_s: float
    perturbed_p50_s: float
    perturbed_p95_s: float
    perturbed_p99_s: float
    delta_p50_pct: float
    delta_p95_pct: float
    delta_p99_pct: float

    @property
    def concern(self) -> bool:
        """True if the p99 delta at a ±10% perturbation exceeds the concern threshold."""
        return self.perturbation_label in ("-10%", "+10%") and abs(self.delta_p99_pct) > CONCERN_THRESHOLD_P99_AT_10PCT

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict with stable output keys.

        Note: key ``perturbation`` intentionally differs from field name
        ``perturbation_label`` to preserve the published JSON schema.
        ``concern`` is a computed property so it is added explicitly after
        ``asdict`` (which only captures dataclass fields).
        """
        d = asdict(self)
        d["perturbation"] = d.pop("perturbation_label")
        d["concern"] = self.concern
        return d


@dataclass
class SweepResult:
    """Full sweep output (machine-readable JSON shape)."""

    generated_at_iso: str
    toml_path: str
    n_samples: int
    seed: int
    concern_threshold_p99_at_10pct: float
    derivation_mode: str
    points: list[SweepPoint] = field(default_factory=list)
    any_concern: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict; delegates each point to SweepPoint.as_dict.

        Uses ``dataclasses.asdict`` for the top-level scalar fields so a new
        field added to ``SweepResult`` is automatically included. The ``points``
        list is rebuilt via each point's own ``as_dict`` rather than the
        recursive ``asdict`` walk so the per-point ``perturbation`` rename and
        computed ``concern`` property are preserved.
        """
        d = asdict(self)
        d["points"] = [p.as_dict() for p in self.points]
        return d


def _sample_percentiles(
    mu: float,
    alpha: float,
    beta: float,
    *,
    n: int = _N_SAMPLES,
    seed: int = _SEED,
) -> tuple[float, float, float]:
    """Return (p50, p95, p99) inter-arrival times in seconds.

    Samples ``n`` inter-arrivals from a Hawkes process with the given
    parameters.  The RNG is seeded for reproducibility.
    """
    rng = random.Random(seed)
    state = HawkesState(mu=mu, alpha=alpha, beta=beta)
    arrivals = sorted(state.next_inter_arrival(rng) for _ in range(n))
    # statistics.quantiles(data, n=100) returns 99 cut points (percentile
    # boundaries); indices 49/94/98 correspond to p50/p95/p99.
    cuts = statistics.quantiles(arrivals, n=100)
    p50, p95, p99 = cuts[49], cuts[94], cuts[98]
    return p50, p95, p99


# CCN: 54-config parameter sweep; orchestration + algorithmic complexity
# is inherent to the sweep and accepted.
def run_sweep(
    toml_path: Path,
    *,
    perturbation_labels: list[str] | None = None,
    size_classes: list[str] | None = None,
    params: list[str] | None = None,
    derivation_mode: str = "full",
) -> SweepResult:
    """Run the parameter sensitivity sweep and return the result.

    Args:
        toml_path: path to ``gh_distributions.toml``.
        perturbation_labels: subset of ``_PERTURBATIONS`` keys to sweep
            (default: all six).
        size_classes: subset of ``["small", "medium", "large"]`` to
            sweep (default: all three).
        params: subset of ``_PARAMS`` to sweep (default: all three).

    Returns:
        :class:`SweepResult` with all :class:`SweepPoint` entries.
    """
    dists = _load_distributions(toml_path)

    pert_labels = perturbation_labels if perturbation_labels is not None else list(_PERTURBATIONS)
    classes = size_classes if size_classes is not None else list(dists.hawkes.keys())
    sweep_params = params if params is not None else list(_PARAMS)

    result = SweepResult(
        generated_at_iso=datetime.now(UTC).isoformat(timespec="seconds"),
        toml_path=str(toml_path),
        n_samples=_N_SAMPLES,
        seed=_SEED,
        concern_threshold_p99_at_10pct=CONCERN_THRESHOLD_P99_AT_10PCT,
        derivation_mode=derivation_mode,
    )

    # Pre-compute baselines for every requested class.
    baselines: dict[str, tuple[float, float, float]] = {}
    for cls in classes:
        hp = dists.hawkes[cls]
        baselines[cls] = _sample_percentiles(
            float(hp["mu_per_sec"]),
            float(hp["alpha"]),
            float(hp["beta"]),
        )

    for cls in classes:
        hp = dists.hawkes[cls]
        base_mu = float(hp["mu_per_sec"])
        base_alpha = float(hp["alpha"])
        base_beta = float(hp["beta"])
        b50, b95, b99 = baselines[cls]

        base_vals: dict[str, float] = {"mu_per_sec": base_mu, "alpha": base_alpha, "beta": base_beta}
        for param in sweep_params:
            base_val = base_vals[param]

            for label in pert_labels:
                mult = _PERTURBATIONS[label]
                new_val = base_val * mult

                # Build perturbed param set.
                mu = new_val if param == "mu_per_sec" else base_mu
                alpha = new_val if param == "alpha" else base_alpha
                beta = new_val if param == "beta" else base_beta

                # beta=0 is degenerate (no decay); skip with a clear
                # note rather than producing meaningless percentiles.
                if beta <= 0.0:
                    continue

                p50, p95, p99 = _sample_percentiles(mu, alpha, beta)

                d50 = (p50 - b50) / b50
                d95 = (p95 - b95) / b95
                d99 = (p99 - b99) / b99

                point = SweepPoint(
                    size_class=cls,
                    param=param,
                    perturbation_label=label,
                    multiplier=mult,
                    perturbed_value=new_val,
                    baseline_p50_s=b50,
                    baseline_p95_s=b95,
                    baseline_p99_s=b99,
                    perturbed_p50_s=p50,
                    perturbed_p95_s=p95,
                    perturbed_p99_s=p99,
                    delta_p50_pct=d50,
                    delta_p95_pct=d95,
                    delta_p99_pct=d99,
                )
                result.points.append(point)
                if point.concern:
                    result.any_concern = True

    return result


def _write_json(result: SweepResult, out_path: Path) -> None:
    """Atomically write sweep result as indented JSON to ``out_path``."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".json.partial")
    tmp.write_text(json.dumps(result.as_dict(), indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(out_path)


def _write_md(result: SweepResult, md_path: Path, *, toml_path: Path) -> None:
    """Regenerate ``HAWKES_SENSITIVITY.md`` from the sweep result.

    Overwrites the file unconditionally; the file is generated
    artefact, not hand-authored.
    """
    md_path.parent.mkdir(parents=True, exist_ok=True)
    dists = _load_distributions(toml_path)
    lines: list[str] = []
    lines += _md_header(result, toml_path)
    lines += _md_baseline_table(dists)
    lines += _md_sweep_table(result)
    lines += _md_footer(result)
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _md_header(result: SweepResult, toml_path: Path) -> list[str]:
    """Return markdown lines for the report header and purpose section."""
    verdict = "PASS - no concern flags raised." if not result.any_concern else "REVIEW - see concern flags below."
    return [
        "# Hawkes Parameter Sensitivity Analysis",
        "",
        "Methodology: empirical method-of-moments Hawkes fit; Ogata-thinning "
        "sampler; sensitivity quantified per +/-10/25/50% parameter perturbation.  ",
        "Generated by: `scripts/sensitivity_sweep.py`  ",
        f"Generated at: {result.generated_at_iso}  ",
        f"Sweep mode: `{result.derivation_mode}`  ",
        f"Samples per config: {result.n_samples}  ",
        f"Seed: {result.seed}  ",
        f"**Overall verdict: {verdict}**",
        "",
        "## Purpose",
        "",
        "This report quantifies how sensitive the benchmark corpus inter-arrival",
        "distribution is to errors in the fitted Hawkes parameters.  The parameters",
        f"come from an empirical method-of-moments fit (`{toml_path.name}`,",
        '`derivation_mode = "empirical-method-of-moments"`).  MoM is consistent but',
        "less efficient than MLE; this sweep answers: _how much does a ±10%/±25%/±50%",
        "parameter error shift the inter-arrival p50/p95/p99?_",
        "",
        "**Threshold of concern:** if any p99 delta exceeds ±25% under a ±10%",
        "parameter perturbation, the corpus is too sensitive and parameters need a",
        "stricter fit (or a larger calibration dataset).",
        "",
    ]


def _md_baseline_table(dists: Distributions) -> list[str]:
    """Return markdown lines for the baseline parameters table.

    Args:
        dists: loaded distributions object from ``_load_distributions``.
    """
    lines = [
        "## Baseline Parameters (empirical fit)",
        "",
        "| Size class | mu (per sec) | alpha | beta | eta (approx) |",
        "| ---------- | ------------ | ----- | ---- | ------------ |",
    ]
    for cls in _HAWKES_CLASSES:
        hp = dists.hawkes[cls]
        mu = float(hp["mu_per_sec"])
        alpha = float(hp["alpha"])
        beta = float(hp["beta"])
        eta = alpha / beta if beta != 0.0 else float("nan")
        lines.append(f"| {cls} | {mu:.7f} | {alpha:.6f} | {beta:.6f} | {eta:.3f} |")
    lines += [
        "",
        "_eta = alpha/beta (branching ratio; corpus is stationary when eta < 1.0)_",
        "",
    ]
    return lines


def _md_sweep_table(result: SweepResult) -> list[str]:
    """Return markdown lines for the per-config delta table."""
    if not result.points:
        return ["## Sweep Results", "", "_No points generated (empty sweep)._", ""]

    lines = [
        "## Sweep Results",
        "",
        f"_n={result.n_samples} samples per config, seed={result.seed}_",
        "",
        "| Class | Param | Perturbation | delta-p50 | delta-p95 | delta-p99 | Concern? |",
        "| ----- | ----- | ------------ | --------- | --------- | --------- | -------- |",
    ]
    for pt in result.points:
        flag = "YES" if pt.concern else "—"
        lines.append(
            f"| {pt.size_class} | {pt.param} | {pt.perturbation_label} "
            f"| {pt.delta_p50_pct:+.1%} "
            f"| {pt.delta_p95_pct:+.1%} "
            f"| {pt.delta_p99_pct:+.1%} "
            f"| {flag} |"
        )
    lines.append("")
    return lines


def _md_footer(result: SweepResult) -> list[str]:
    """Return markdown lines for the methodology and maintainer notes sections."""
    return [
        "## Methodology",
        "",
        "Each perturbed config holds two of the three Hawkes parameters at their",
        "baseline values and scales the third by the perturbation multiplier.",
        "Inter-arrival samples are drawn from the Ogata-thinning sampler in",
        "`benchmarks/gen_corpus.py::HawkesState.next_inter_arrival`.  The sampler",
        "is the same code path the corpus generator uses, so this sweep directly",
        "measures corpus sensitivity.",
        "",
        "**Sweep scope:** 3 perturbation levels x 3 parameters x 3 size classes",
        "= 54 perturbed configs (27 upward, 27 downward).",
        "",
        "## Maintainer Notes",
        "",
        "- **When to re-run:** annually, or on any release cut that changes",
        "  `benchmarks/data/gh_distributions.toml`.",
        "- **Full sweep command:**",
        "  ```",
        "  python scripts/sensitivity_sweep.py --full",
        "  ```",
        "  Outputs `benchmarks/data/sensitivity-results.json` and regenerates",
        "  this file.  Commit both alongside the updated TOML.",
        "- **Smoke (CI):** `python scripts/sensitivity_sweep.py --smoke` — one",
        "  perturbation x one class, verifies the script is not broken without",
        "  running all 54 configs.",
        "",
    ]


def main(argv: list[str] | None = None) -> int:
    """Entry point: parse args, run sweep, write JSON and markdown outputs.

    Returns 0 on clean sweep, 1 if any concern flag is raised.
    """
    ap = argparse.ArgumentParser(
        description=(
            "Hawkes parameter sensitivity sweep.  Run --smoke for a quick sanity check; --full for the complete sweep."
        )
    )
    mode_group = ap.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--full",
        action="store_true",
        help="Run all 54 perturbed configs (default: smoke).",
    )
    mode_group.add_argument(
        "--smoke",
        action="store_true",
        help="Run one perturbation x one class (fast CI check).",
    )
    ap.add_argument(
        "--toml",
        type=Path,
        default=_DEFAULT_TOML,
        help=f"TOML distribution file (default: {_DEFAULT_TOML.relative_to(_REPO_ROOT)}).",
    )
    ap.add_argument(
        "--json-out",
        type=Path,
        default=_DEFAULT_JSON_OUT,
        help="Output path for machine-readable JSON results.",
    )
    ap.add_argument(
        "--md-out",
        type=Path,
        default=_DEFAULT_MD_OUT,
        help="Output path for the markdown sensitivity report.",
    )
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output (useful in CI).",
    )
    args = ap.parse_args(argv)

    smoke = args.smoke or not args.full

    if not args.quiet:
        mode = "smoke (1 perturbation x 1 class)" if smoke else "full (54 configs)"
        print(f"[sensitivity_sweep] mode={mode}", file=sys.stderr)

    if smoke:
        result = run_sweep(
            args.toml,
            perturbation_labels=["+10%"],
            size_classes=["small"],
            params=["mu_per_sec"],
            derivation_mode="smoke",
        )
    else:
        result = run_sweep(args.toml)

    _write_json(result, args.json_out)
    _write_md(result, args.md_out, toml_path=args.toml)

    if not args.quiet:
        verdict = "PASS" if not result.any_concern else "CONCERN"
        print(
            f"[sensitivity_sweep] {verdict} — {len(result.points)} points, json={args.json_out}, md={args.md_out}",
            file=sys.stderr,
        )

    return 1 if result.any_concern else 0


if __name__ == "__main__":
    sys.exit(main())
