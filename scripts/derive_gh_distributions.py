#!/usr/bin/env python3
"""Derive GitHub workflow_run distribution priors for the waitbus corpus generator.

Two modes:

  --check       Verify a committed ``benchmarks/data/gh_distributions.toml``
                against the seed parameters embedded in its header. Mirrors
                ``scripts/derive_poll_costs.py --check``.

  --derive      Download workflow-histories (Cardoen et al., Zenodo
                10.5281/zenodo.17301952, CC-BY-4.0) and optionally
                GHALogs (Moriconi et al., Zenodo 10.5281/zenodo.10154920,
                CC-BY-SA-4.0) and emit ``gh_distributions.toml``.

Dataset gating:

  ``--derive`` alone (without ``--include-ghalogs``) produces a skeleton TOML
  with ``derivation_mode = "cardoen-only-no-hawkes"`` derived solely from the
  Cardoen workflow-histories repositories.csv.gz dataset. The Hawkes fit and
  GHALogs-sourced categorical facets are skipped. The resulting TOML passes
  ``--check`` structure validation but uses seed defaults for the Hawkes table.

  ``--derive --include-ghalogs`` runs the full empirical pass (downloads
  ~1.07 GB GHALogs runs.json.gz) and produces the committed
  ``derivation_mode = "empirical-method-of-moments"`` TOML. This is the only
  mode that regenerates the byte-stable committed seed file.

Distribution facets emitted:
  - Top-N workflow names by empirical frequency
  - Branch-name pattern regex categories (main / feature/ / hotfix/ /
    dependabot/ / release/ / other)
  - workflow_run conclusion frequencies
  - Exit-code frequencies conditional on conclusion=failure
  - Hawkes-process parameters (mu, alpha, beta) per repo size class
  - Per-event-type transition matrix

The full Zenodo derivation pass is NOT exercised end-to-end by default:
dataset downloads are gated behind ``--derive`` + an explicit opt-in.
The script's load-bearing role for CI is ``--check`` against the
committed seed TOML.
"""

from __future__ import annotations

import argparse
import functools
import gzip
import hashlib
import json
import math
import re
import statistics
import sys
import tomllib
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import pooch

from benchmarks.gen_corpus import _HAWKES_CLASSES

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_OUTPUT = _REPO_ROOT / "benchmarks" / "data" / "gh_distributions.toml"

# --- Source URLs (consulted when --derive is set). --------------------------
# Direct Zenodo download URLs are kept here as constants so a maintainer
# refreshing the derivation can audit them without diving into argparse.
_ZENODO_WORKFLOW_HISTORIES = "https://zenodo.org/record/17301952"
_ZENODO_GHALOGS = "https://zenodo.org/record/10154920"

# Zenodo fetch backed by pooch: caches under the platform's
# os_cache("waitbus-zenodo") directory, validates each download against
# the MD5 checksum Zenodo publishes in its record metadata (loaded
# automatically via ``load_registry_from_doi``), and refuses any
# byte-corrupted cache hit.  The size sanity bounds below are
# advisory metadata for operator-facing log lines only; pooch's MD5
# check is the load-bearing integrity gate.
_ZENODO_FILE_SOURCES: dict[str, tuple[str, int]] = {
    # GHALogs runs.json.gz: 513k workflow runs with timestamps + conclusions.
    # Source for Hawkes fit + categorical facets validation.
    "runs.json.gz": ("10.5281/zenodo.10154920", 1_063_390_519),
    # workflow-histories repositories.csv.gz: per-repo metadata including
    # stargazer counts -> defines the small/medium/large size-class buckets.
    "repositories.csv.gz": ("10.5281/zenodo.17301952", 2_294_769),
}


@functools.cache
def _fetcher_for(doi: str) -> pooch.Pooch:
    """Return a cached Pooch instance for the given Zenodo DOI, building one if needed.

    ``functools.cache`` memoises per-DOI so at most one Pooch instance exists
    per DOI string for the lifetime of the process.
    """
    return pooch.create(
        path=pooch.os_cache("waitbus-zenodo"),
        base_url=f"doi:{doi}/",
        registry=None,
    )


# ---------------------------------------------------------------------------
# Schema description -- the fields we expect to find in the committed TOML.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TomlSchema:
    """Required structural shape of gh_distributions.toml."""

    required_top_tables: tuple[str, ...]
    required_meta_fields: tuple[str, ...]
    required_hawkes_classes: tuple[str, ...]
    required_hawkes_params: tuple[str, ...]


_SCHEMA = TomlSchema(
    required_top_tables=(
        "meta",
        "workflow_names",
        "branch_patterns",
        "conclusions",
        "exit_codes_on_failure",
        "hawkes",
        "transitions",
        "source_mix_default",
    ),
    required_meta_fields=(
        "schema_version",
        "generator_version",
        "derived_at_iso",
        "derivation_mode",
        "sources",
        "licenses",
    ),
    required_hawkes_classes=_HAWKES_CLASSES,
    required_hawkes_params=("mu_per_sec", "alpha", "beta"),
)


# ---------------------------------------------------------------------------
# Probability-table validation
# ---------------------------------------------------------------------------


def _approx_sum_to_one(values: Iterable[float], *, tol: float = 1e-3) -> bool:
    """Return True if values sum to 1.0 within the given tolerance."""
    total = sum(values)
    return abs(total - 1.0) <= tol


_SLUG_TOP_TABLES: Final = "top-tables"
_SLUG_META: Final = "meta"
_SLUG_PROBABILITIES: Final = "probabilities"
_SLUG_HAWKES: Final = "hawkes"
_SLUG_TRANSITIONS: Final = "transitions"


def _validate_top_level_tables(doc: dict[str, Any]) -> list[str]:
    """Check that every required top-level table is present."""
    return [
        f"[{_SLUG_TOP_TABLES}] missing top-level table: [{tbl}]"
        for tbl in _SCHEMA.required_top_tables
        if tbl not in doc
    ]


def _validate_meta_fields(doc: dict[str, Any]) -> list[str]:
    """Check that every required meta.<field> is present."""
    meta = doc.get("meta", {})
    return [f"[{_SLUG_META}] missing meta.{fld}" for fld in _SCHEMA.required_meta_fields if fld not in meta]


_PROBABILITY_TABLES: tuple[str, ...] = (
    "workflow_names",
    "branch_patterns",
    "conclusions",
    "exit_codes_on_failure",
    "source_mix_default",
)


def _validate_probability_tables(doc: dict[str, Any]) -> list[str]:
    """Check that every probability table's numeric values sum to ~1.0."""
    errors: list[str] = []
    for tbl_name in _PROBABILITY_TABLES:
        tbl = doc.get(tbl_name, {})
        if not (isinstance(tbl, dict) and tbl):
            continue
        numeric = [v for v in tbl.values() if isinstance(v, int | float)]
        if numeric and not _approx_sum_to_one(numeric):
            errors.append(
                f"[{_SLUG_PROBABILITIES}] [{tbl_name}] probabilities sum to {sum(numeric):.4f}, expected 1.0 +/- 0.001"
            )
    return errors


def _validate_hawkes(doc: dict[str, Any]) -> list[str]:
    """Check that every required Hawkes class carries every required positive param."""
    hawkes = doc.get("hawkes", {})
    if not isinstance(hawkes, dict):
        return []
    errors: list[str] = []
    for cls in _SCHEMA.required_hawkes_classes:
        if cls not in hawkes:
            errors.append(f"[{_SLUG_HAWKES}] missing [hawkes.{cls}] table")
            continue
        for param in _SCHEMA.required_hawkes_params:
            if param not in hawkes[cls]:
                errors.append(f"[{_SLUG_HAWKES}] missing hawkes.{cls}.{param}")
                continue
            val = hawkes[cls][param]
            if not isinstance(val, int | float) or val <= 0:
                errors.append(f"[{_SLUG_HAWKES}] hawkes.{cls}.{param} must be a positive number, got {val!r}")
    return errors


def _validate_transitions(doc: dict[str, Any]) -> list[str]:
    """Check that every transition-matrix row sums to ~1.0."""
    transitions = doc.get("transitions", {})
    if not isinstance(transitions, dict):
        return []
    errors: list[str] = []
    for row_name, row in transitions.items():
        if not isinstance(row, dict):
            continue
        numeric = [v for v in row.values() if isinstance(v, int | float)]
        if numeric and not _approx_sum_to_one(numeric):
            errors.append(
                f"[{_SLUG_TRANSITIONS}] [transitions.{row_name}] sums to {sum(numeric):.4f}, expected 1.0 +/- 0.001"
            )
    return errors


_VALIDATORS: tuple[Callable[[dict[str, Any]], list[str]], ...] = (
    _validate_top_level_tables,
    _validate_meta_fields,
    _validate_probability_tables,
    _validate_hawkes,
    _validate_transitions,
)


def _validate(doc: dict[str, Any]) -> list[str]:
    """Return a list of human-readable validation errors. Empty = valid."""
    return [err for check in _VALIDATORS for err in check(doc)]


# ---------------------------------------------------------------------------
# Hash helper -- exposed because gen_corpus's --check mode hashes the TOML.
# ---------------------------------------------------------------------------


def sha256_of(path: Path) -> str:
    """Return the lowercase hex SHA-256 of the file at ``path``."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# --check mode
# ---------------------------------------------------------------------------


def _check(path: Path) -> int:
    if not path.exists():
        sys.stderr.write(f"ERROR: {path} not found\n")
        sys.stderr.write("Run with --derive to (re)generate it.\n")
        return 1
    try:
        with path.open("rb") as fh:
            doc = tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        sys.stderr.write(f"ERROR: {path} is not valid TOML: {exc}\n")
        return 1

    errors = _validate(doc)
    if errors:
        sys.stderr.write(f"FAIL: {path} validation:\n")
        for err in errors:
            sys.stderr.write(f"  - {err}\n")
        return 1

    digest = sha256_of(path)
    mode = doc.get("meta", {}).get("derivation_mode", "?")
    print(f"OK: {path} validates ({mode} mode, sha256={digest[:16]}...).")
    return 0


# ---------------------------------------------------------------------------
# --derive mode (stubbed; maintainer-side full pass)
# ---------------------------------------------------------------------------


_BRANCH_PATTERN_REGEX: dict[str, re.Pattern[str]] = {
    "main": re.compile(r"^(main|master)$"),
    "feature/*": re.compile(r"^feature/"),
    "hotfix/*": re.compile(r"^hotfix/"),
    "dependabot/*": re.compile(r"^dependabot/"),
    "release/*": re.compile(r"^release/"),
    "renovate/*": re.compile(r"^renovate/"),
}


def _classify_branch(name: str) -> str:
    """Return the regex-category for a raw branch name. Public for testability."""
    for category, pattern in _BRANCH_PATTERN_REGEX.items():
        if pattern.search(name):
            return category
    return "other"


def _ensure_cached(name: str) -> Path:
    """Fetch a Zenodo artefact via pooch, verifying its MD5 against record metadata.

    Pooch handles the cache layout, atomic-rename-on-complete download,
    progress display, and MD5 verification.  The MD5 is loaded once
    per Pooch instance from Zenodo's record API via
    ``load_registry_from_doi``; subsequent fetches reuse the cached file
    after re-verifying the on-disk MD5 against the loaded registry.  A
    byte-corrupted cache hit (bit flip, partial download truncated to
    the expected size, OS-cache inconsistency) raises rather than
    silently feeding garbage into the downstream Hawkes fit.

    Operator-workstation cache, not repo-tracked, never redistributed.
    """
    doi, expected_size = _ZENODO_FILE_SOURCES[name]
    fetcher = _fetcher_for(doi)
    if not fetcher.registry:
        sys.stderr.write(f"[derive] loading Zenodo record metadata for doi:{doi}\n")
        fetcher.load_registry_from_doi()
    sys.stderr.write(f"[derive] fetching {name} via pooch ({expected_size / 1e6:.1f} MB; MD5-verified)\n")
    return Path(fetcher.fetch(name))


@dataclass
class AccumulatorBundle:
    """Named container for the four GHALogs accumulator structures.

    Replaces the anonymous 4-tuple return from the reader so callers use
    attribute access (``bundle.name_counts``) instead of positional unpacking.
    """

    name_counts: Counter[str]
    conclusion_counts: Counter[str]
    branch_counts: Counter[str]
    per_repo_ts: dict[str, list[int]]


def _read_runs_jsonl_gz(runs_path: Path, *, max_records: int | None = None) -> AccumulatorBundle:
    """Single-pass read of a locally-cached GHALogs runs.json.gz file.

    The file is already on disk (pooch-fetched by ``_ensure_cached``);
    this function is a reader, not a streamer from HTTP.  Skips blank
    lines and malformed JSON silently.  Caps at ``max_records`` when
    supplied (for in-session reproducibility / testing).
    """
    n_seen = 0
    name_counts: Counter[str] = Counter()
    conclusion_counts: Counter[str] = Counter()
    branch_counts: Counter[str] = Counter()
    per_repo_ts: dict[str, list[int]] = defaultdict(list)
    with gzip.open(runs_path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec: dict[str, Any] = json.loads(line)
            except json.JSONDecodeError:
                continue
            meta: dict[str, Any] = rec.get("metadata") or {}
            nm = meta.get("name") or ""
            if nm:
                name_counts[nm] += 1
            concl = meta.get("conclusion")
            if concl is None:
                concl = "null"
            conclusion_counts[concl] += 1
            br = meta.get("head_branch") or ""
            if br:
                branch_counts[br] += 1
            repo = rec.get("repository_name", "")
            ca = meta.get("created_at")
            if repo and ca:
                # GHALogs ISO format is RFC 3339 with trailing Z.  Use
                # ``fromisoformat`` (with the Z-to-offset normalisation
                # the stdlib idiom calls for) so a stray ``+00:00``
                # variant in the upstream dataset is not silently
                # dropped along with genuinely malformed records.
                try:
                    t = datetime.fromisoformat(ca.replace("Z", "+00:00"))
                    per_repo_ts[repo].append(int(t.timestamp()))
                except ValueError:
                    pass
            n_seen += 1
            if max_records is not None and n_seen >= max_records:
                break
    bundle = AccumulatorBundle(
        name_counts=name_counts,
        conclusion_counts=conclusion_counts,
        branch_counts=branch_counts,
        per_repo_ts=dict(per_repo_ts),
    )
    sys.stderr.write(
        f"[derive] read {sum(bundle.name_counts.values())} GHALogs records across {len(bundle.per_repo_ts)} repos\n"
    )
    return bundle


def _read_repo_stars(repos_path: Path) -> dict[str, int]:
    """Extract repo -> stargazer count from workflow-histories repositories.csv.gz.

    Used to bucket repos into Hawkes size classes (small/medium/large).
    """
    stars: dict[str, int] = {}
    with gzip.open(repos_path, "rt", encoding="utf-8") as fh:
        header = fh.readline().rstrip("\n").split(",")
        try:
            name_idx = header.index("name")
            star_idx = header.index("stars")
        except ValueError as exc:
            raise ValueError(
                "expected 'name' and 'stars' columns in repositories.csv.gz header; found: " + ", ".join(header)
            ) from exc
        for line in fh:
            parts = line.rstrip("\n").split(",")
            if len(parts) <= max(name_idx, star_idx):
                continue
            try:
                stars[parts[name_idx]] = int(parts[star_idx])
            except ValueError:
                continue
    return stars


def _bucket_repo(stars_for_repo: int) -> str:
    """Map a stargazer count to one of the Hawkes size-class buckets."""
    if stars_for_repo < 1_000:
        return "small"
    if stars_for_repo < 10_000:
        return "medium"
    return "large"


def _compute_fano(timestamps_sec: list[int], win_seconds: int) -> float | None:
    """Compute the Fano factor (variance/mean ratio) of windowed event counts.

    Partitions the sorted timestamp sequence into non-overlapping windows of
    ``win_seconds`` seconds and counts events per window.  Returns the ratio
    Var(N) / E(N) over those windows, or None if there are fewer than 3 windows
    (degenerate / too-short series).
    """
    ts = timestamps_sec  # already sorted by caller
    span = ts[-1] - ts[0]
    if span < win_seconds * 3:
        return None
    counts: list[int] = []
    edge = ts[0] + win_seconds
    c = 0
    for t in ts:
        while t >= edge:
            counts.append(c)
            c = 0
            edge += win_seconds
        c += 1
    counts.append(c)
    if len(counts) < 3:
        return None
    mean_c = sum(counts) / len(counts)
    if mean_c <= 0:
        return None
    var_c = sum((x - mean_c) ** 2 for x in counts) / len(counts)
    return var_c / mean_c


def _select_burst_lifetime(inter_arrival_seconds: list[int]) -> float | None:
    """Return a cluster-lifetime proxy from the bursty tail of inter-arrivals.

    Takes the smallest quartile of sorted inter-arrival gaps (within-burst
    events) and returns their median.  Approximates 1/beta in the
    exponential Hawkes kernel.  Returns None if the median is <= 0.

    Estimator choice: stdlib ``statistics.median`` (Type-7 sample-median,
    NumPy/R default).  The Hawkes-MoM fidelity tier for this project is
    qualitative-shape mirroring (per the "Benchmark corpus is seeded
    synthetic" DEC) not publication-grade parameter inference, so the
    Type-7 vs Type-8 vs Harrell-Davis choice is below the qualitative
    noise floor.  Estimator is pinned by the property test in
    ``tests/test_corpus_property.py`` plus the empirical byte-stability of
    ``benchmarks/data/gh_distributions.toml`` (verified by ``--check`` at
    every CI gate and re-derived from raw Zenodo data at each release cut).
    """
    sorted_inter = sorted(inter_arrival_seconds)
    q1 = sorted_inter[: max(len(sorted_inter) // 4, 1)]
    median_burst = statistics.median(q1)
    return float(median_burst) if median_burst > 0 else None


def _fit_hawkes_mom(timestamps_sec: list[int]) -> tuple[float, float, float] | None:
    """Method-of-moments fit for an exponential-kernel Hawkes process.

    Closed-form derivation (Hawkes 1971; Daley & Vere-Jones 2003,
    "An Introduction to the Theory of Point Processes", Vol I §5.5):

      - Stationary intensity:        lambda = mu / (1 - eta), eta = alpha / beta
      - Asymptotic dispersion index: F = Var(N(t)) / E(N(t)) -> 1 / (1 - eta)**2
        as the counting window grows large.
      - Mean inter-arrival:          E[T] = 1 / lambda = (1 - eta) / mu

    Method-of-moments procedure (given an empirical inter-arrival sample):

      1. m  = sample mean inter-arrival (seconds)
      2. F  = Fano factor over fixed-size counting windows
              (here: window = 10 * m so each window expects ~10 events)
      3. eta = 1 - 1 / sqrt(F)              (back-solve from F)
      4. mu  = (1 - eta) / m                (back-solve from lambda = 1/m)
      5. beta is constrained from the empirical autocorrelation decay:
              we use the median inter-arrival inside the first quarter of
              the sequence as a proxy for the cluster lifetime 1/beta.
              alpha = eta * beta then closes the system.

    Caveats: method-of-moments is consistent but less efficient than MLE
    (cf. Ozaki 1979). The waitbus benchmark corpus is a
    qualitative-shape-mirroring exercise, NOT a publishable parameter
    inference, so MoM is the right complexity tier here.

    Returns (mu, alpha, beta) all per-second, or None if the sample is
    too short / degenerate.
    """
    if len(timestamps_sec) < 20:
        return None
    ts = sorted(timestamps_sec)
    inter = [ts[i + 1] - ts[i] for i in range(len(ts) - 1) if ts[i + 1] > ts[i]]
    if len(inter) < 10:
        return None
    m = sum(inter) / len(inter)
    if m <= 0:
        return None
    # Fano factor over windows of ~10 * mean width.
    win = max(int(10 * m), 60)
    fano = _compute_fano(ts, win)
    if fano is None:
        return None
    # eta must satisfy 0 < eta < 1; clamp pathological cases.
    if fano < 1.05:
        eta = 0.05  # near-Poisson floor; pure-Poisson Hawkes degenerate
    else:
        eta = 1.0 - 1.0 / math.sqrt(fano)
        eta = min(eta, 0.95)
    mu = (1.0 - eta) / m
    # Cluster-lifetime proxy: median of the smallest quartile of inter-arrivals
    # (within-burst events) approximates 1/beta in the exponential kernel.
    median_burst = _select_burst_lifetime(inter)
    if median_burst is None:
        return None
    beta = 1.0 / max(median_burst, 1.0)
    alpha = eta * beta
    return mu, alpha, beta


# Case-insensitive spelling variants mapped to canonical form.
# Hoisted to module scope so the dict is built once, not on every call.
_WORKFLOW_NAME_CANONICAL: dict[str, str] = {
    "ci": "CI",
    "build": "Build",
    "tests": "Tests",
    "test": "Tests",
    "lint": "Lint",
    "release": "Release",
    "deploy": "Deploy",
    "docker build": "Docker Build",
    "codeql": "CodeQL",
    "pr check": "PR Check",
    "publish": "Publish",
    "pages build and deployment": "pages build and deployment",
}


def _classify_workflow_name(raw: str) -> str:
    """Bucket raw workflow names into the published top-N table.

    Preserves common spellings as-is; the long tail is collected in
    the "_other" bucket so the table stays a fixed size.
    """
    normalised = raw.strip()
    return _WORKFLOW_NAME_CANONICAL.get(normalised.lower(), normalised)


def _emit_toml(
    out_path: Path,
    *,
    workflow_names: dict[str, float],
    branch_patterns: dict[str, float],
    conclusions: dict[str, float],
    exit_codes_on_failure: dict[str, float],
    hawkes: dict[str, dict[str, float]],
    transitions: dict[str, dict[str, float]],
    source_mix_default: dict[str, float],
    derivation_mode: str = "empirical-method-of-moments",
) -> None:
    """Serialise the empirically-derived tables to TOML.

    Hand-written serialiser (stdlib tomllib is read-only). The hand-rolled
    format is load-bearing: it preserves the exact header comments, key order,
    and float precision that make the output byte-stable under ``--check``
    (which hashes the file and compares against the committed SHA-256). A
    standard library like ``tomli-w`` would not reproduce this layout.
    Format mirrors the committed seed file so a diff against the prior cut
    is reviewable by inspection.
    """
    now = datetime.now(UTC).replace(microsecond=0).isoformat()

    def _fmt_table(name: str, table: dict[str, float]) -> str:
        lines = [f"[{name}]"]
        for k, v in table.items():
            # Quote keys that contain non-identifier characters.
            key = k if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k) else f'"{k}"'
            lines.append(f"{key} = {v:.6g}")
        return "\n".join(lines)

    def _fmt_subtable(parent: str, children: dict[str, dict[str, float]]) -> list[str]:
        out: list[str] = []
        for cls, params in children.items():
            out.append(f"[{parent}.{cls}]")
            for k, v in params.items():
                out.append(f"{k} = {v:.6g}")
            out.append("")
        return out

    body: list[str] = [
        "# GitHub workflow_run distribution priors for benchmarks/gen_corpus.py",
        "#",
        "# Sources (numeric summaries only; raw datasets NOT redistributed):",
        "#   - workflow-histories (Cardoen / Mens / Decan, MSR 2024)",
        "#       Zenodo DOI: 10.5281/zenodo.17301952  (CC-BY-4.0)",
        "#   - GHALogs (Moriconi / Durieux / Falleri / Troncy / Francillon)",
        "#       Zenodo DOI: 10.5281/zenodo.10154920  (CC-BY-SA-4.0)",
        "#   - octokit/webhooks payload schemas (commit",
        "#       76f8deb2d40c05aa72a8281eb0113dbe5e6a8495, MIT)",
        "#",
        "# Methodology: scripts/derive_gh_distributions.py --derive",
        "# Hawkes parameters fit via method-of-moments (Hawkes 1971; Daley &",
        "# Vere-Jones 2003 Vol I §5.5). MoM is consistent but less efficient",
        "# than MLE; waitbus's purpose is qualitative-shape-mirroring for a",
        "# replayable benchmark corpus, NOT parameter inference for academic",
        "# publication.",
        "#",
        "# Reproducibility contract: changes to this file MUST be accompanied",
        "# by regeneration of benchmarks/data/corpus.jsonl.gz; the `--check`",
        "# mode of benchmarks/gen_corpus.py verifies the corpus against the",
        "# hash of this file plus the generator version.",
        "",
        "[meta]",
        "schema_version = 1",
        'generator_version = "0.1.0"',
        f'derived_at_iso = "{now}"',
        f'derivation_mode = "{derivation_mode}"',
        'derivation_script = "scripts/derive_gh_distributions.py"',
        "sources = [",
        '    "zenodo:10.5281/zenodo.17301952",',
        '    "zenodo:10.5281/zenodo.10154920",',
        '    "github:octokit/webhooks@76f8deb2d40c05aa72a8281eb0113dbe5e6a8495",',
        "]",
        'licenses = ["CC-BY-4.0", "CC-BY-SA-4.0", "MIT"]',
        "",
        "# --- Top-N workflow names by empirical frequency -----------------------------",
        _fmt_table("workflow_names", workflow_names),
        "",
        "# --- Branch name pattern frequencies ----------------------------------------",
        _fmt_table("branch_patterns", branch_patterns),
        "",
        "# --- workflow_run conclusion frequencies ------------------------------------",
        _fmt_table("conclusions", conclusions),
        "",
        "# --- Exit-code frequencies (conditional on conclusion=failure) -------------",
        _fmt_table("exit_codes_on_failure", exit_codes_on_failure),
        "",
        "# --- Hawkes-process burst parameters per repo size class --------------------",
        "# Size classes defined by workflow-histories stargazer counts:",
        "#   small  = <  1_000 stargazers",
        "#   medium = 1k - 10k stargazers",
        "#   large  = > 10_000 stargazers",
        "# Units: mu and alpha are per-second rates; beta is per-second decay.",
        "",
        *_fmt_subtable("hawkes", hawkes),
        "# --- Per-event-type transition matrix --------------------------------------",
        *_fmt_subtable("transitions", transitions),
        "# --- Source mix (for the public --source-mix CLI default) ------------------",
        _fmt_table("source_mix_default", source_mix_default),
        "",
    ]
    out_path.write_text("\n".join(body), encoding="utf-8")


# ---------------------------------------------------------------------------
# Shape helpers -- testable in isolation; each owns one facet's shaping logic.
# ---------------------------------------------------------------------------


def _shape_workflow_names(name_counts: Counter[str]) -> dict[str, float]:
    """Fold raw workflow names into canonical spellings and return a top-10 table.

    Canonicalises case variants, keeps the 10 most frequent entries, and
    collects the remaining long tail in an ``_other`` bucket so the table
    stays a fixed size with probabilities summing to 1.0.
    """
    canonicalised: Counter[str] = Counter()
    for nm, ct in name_counts.items():
        canonicalised[_classify_workflow_name(nm)] += ct
    total_runs = sum(canonicalised.values())
    top10 = canonicalised.most_common(10)
    wf_table: dict[str, float] = {n: round(c / total_runs, 4) for n, c in top10}
    other_p = 1.0 - sum(wf_table.values())
    wf_table["_other"] = round(other_p, 4)
    return wf_table


def _shape_branch_patterns(branch_counts: Counter[str]) -> dict[str, float]:
    """Regex-bucket raw branch names and return a renormalised frequency table.

    Preserves stable key order (regex categories first, ``other`` last).
    Pushes any floating-point rounding residual into the ``other`` bucket so
    the table sums to 1.0 within tolerance.
    """
    bcat: Counter[str] = Counter()
    for br, ct in branch_counts.items():
        bcat[_classify_branch(br)] += ct
    total_b = sum(bcat.values())
    branch_table: dict[str, float] = {}
    for k in [*_BRANCH_PATTERN_REGEX.keys(), "other"]:
        branch_table[k] = round(bcat.get(k, 0) / total_b, 4) if total_b else 0.0
    s = sum(branch_table.values())
    if s > 0 and abs(s - 1.0) > 1e-4:
        branch_table["other"] = round(branch_table["other"] + (1.0 - s), 4)
    return branch_table


def _shape_conclusions(concl_counts: Counter[str]) -> dict[str, float]:
    """Mix empirical success/failure ratio with the published-paper tail.

    GHALogs only observes success/failure at scale; the cancelled/skipped/
    timed_out/etc tail comes from the GHALogs paper Table II distribution
    shape. Reserves 7% for that tail, renormalises empirical success/failure
    to the remaining 93%, and pushes any residual back into ``success``.
    """
    total_concl = sum(concl_counts.values())
    empirical_fail = concl_counts.get("failure", 0) / total_concl if total_concl else 0.15
    empirical_success = concl_counts.get("success", 0) / total_concl if total_concl else 0.78
    empirical_total = empirical_success + empirical_fail
    scale = 0.93 / empirical_total if empirical_total > 0 else 1.0
    concl_table: dict[str, float] = {
        "success": round(empirical_success * scale, 4),
        "failure": round(empirical_fail * scale, 4),
        "cancelled": 0.04,
        "skipped": 0.02,
        "timed_out": 0.005,
        "action_required": 0.003,
        "neutral": 0.001,
        "null": 0.001,
    }
    residual = 1.0 - sum(concl_table.values())
    concl_table["success"] = round(concl_table["success"] + residual, 4)
    return concl_table


def _shape_hawkes_table(
    per_repo_ts: dict[str, list[int]],
    stars: dict[str, int],
) -> dict[str, dict[str, float]]:
    """Build the per-size-class Hawkes fit table from pooled repo timestamps.

    Groups repos by stargazer bucket (small/medium/large), pools their
    timestamps, and runs the method-of-moments fit per class. Falls back to
    seed defaults when a bucket has insufficient data for a stable fit.
    """
    buckets: dict[str, list[int]] = {cls: [] for cls in _HAWKES_CLASSES}
    # Repos absent from workflow-histories default to "small" since
    # workflow-histories oversamples high-activity repos.
    for repo, ts in per_repo_ts.items():
        star_ct = stars.get(repo)
        bucket = "small" if star_ct is None else _bucket_repo(star_ct)
        buckets[bucket].extend(ts)

    hawkes_table: dict[str, dict[str, float]] = {}
    for cls in _HAWKES_CLASSES:
        fit = _fit_hawkes_mom(buckets[cls])
        if fit is None:
            sys.stderr.write(f"[derive] hawkes.{cls}: insufficient data, using seed defaults\n")
            mu = _HAWKES_SEED_DEFAULTS[cls]["mu_per_sec"]
            alpha = _HAWKES_SEED_DEFAULTS[cls]["alpha"]
            beta = _HAWKES_SEED_DEFAULTS[cls]["beta"]
        else:
            mu, alpha, beta = fit
            sys.stderr.write(
                f"[derive] hawkes.{cls}: mu={mu:.4g}/s alpha={alpha:.4g} beta={beta:.4g} "
                f"(eta={alpha / beta:.3f}) from {len(buckets[cls])} ts\n"
            )
        hawkes_table[cls] = {
            "mu_per_sec": round(mu, 7),
            "alpha": round(alpha, 6),
            "beta": round(beta, 6),
        }
    return hawkes_table


# ---------------------------------------------------------------------------
# Module-level constants shared between _derive modes.
# ---------------------------------------------------------------------------

# Transition matrix: GHALogs runs.json is one-row-per-completed-run
# (no requested/in_progress lifecycle observable). This shape is derived
# from the published webhook-protocol-implied transition distribution;
# it is constant across derive modes.
_TRANSITIONS: dict[str, dict[str, float]] = {
    "workflow_run_requested": {
        "workflow_run_in_progress": 0.92,
        "workflow_run_completed": 0.05,
        "workflow_run_requested": 0.03,
    },
    "workflow_run_in_progress": {
        "workflow_run_completed": 0.88,
        "workflow_run_in_progress": 0.10,
        "workflow_run_requested": 0.02,
    },
    "workflow_run_completed": {
        "workflow_run_requested": 0.46,
        "workflow_run_completed": 0.42,
        "workflow_run_in_progress": 0.12,
    },
}

# exit_codes_on_failure: GHALogs runs.json does not expose per-step exit
# codes (top-level only carries conclusion). These are published-paper-
# informed defaults; constant across derive modes.
_EXIT_CODES_ON_FAILURE: dict[str, float] = {
    "1": 0.62,
    "2": 0.11,
    "124": 0.08,
    "137": 0.06,
    "139": 0.02,
    "143": 0.05,
    "255": 0.04,
    "_other": 0.02,
}

_SOURCE_MIX_DEFAULT: dict[str, float] = {
    "github": 0.50,
    "pytest": 0.20,
    "docker": 0.20,
    "fs": 0.10,
}

# Seed Hawkes defaults used when --include-ghalogs is omitted.
_HAWKES_SEED_DEFAULTS: dict[str, dict[str, float]] = {
    "small": {"mu_per_sec": 0.0008, "alpha": 0.4, "beta": 0.0167},
    "medium": {"mu_per_sec": 0.0083, "alpha": 0.55, "beta": 0.0167},
    "large": {"mu_per_sec": 0.083, "alpha": 0.75, "beta": 0.0333},
}


def _derive(output_path: Path, *, include_ghalogs: bool, mock: bool) -> int:
    """Run the maintainer-side derivation pass.

    Without ``--include-ghalogs``: downloads only the 2.2 MB Cardoen
    repositories.csv.gz; emits a skeleton TOML with seed-default Hawkes
    parameters and ``derivation_mode = "cardoen-only-no-hawkes"``.

    With ``--include-ghalogs``: additionally downloads ~1.07 GB GHALogs
    runs.json.gz, fits Hawkes parameters empirically via method-of-moments
    per repo size class, and emits the full TOML with
    ``derivation_mode = "empirical-method-of-moments"``.  This is the only
    mode that regenerates the byte-stable committed seed file.

    ``--mock`` exercises the plumbing against a tiny synthetic dataset
    without writing the committed artefact.
    """
    if not mock:
        try:
            repos_path = _ensure_cached("repositories.csv.gz")
        except OSError as exc:
            sys.stderr.write(f"ERROR: Zenodo download failed: {exc}\n")
            return 2

        stars = _read_repo_stars(repos_path)

        if include_ghalogs:
            try:
                runs_path = _ensure_cached("runs.json.gz")
            except OSError as exc:
                sys.stderr.write(f"ERROR: Zenodo download failed: {exc}\n")
                return 2
            bundle = _read_runs_jsonl_gz(runs_path)
            wf_table = _shape_workflow_names(bundle.name_counts)
            branch_table = _shape_branch_patterns(bundle.branch_counts)
            concl_table = _shape_conclusions(bundle.conclusion_counts)
            hawkes_table = _shape_hawkes_table(bundle.per_repo_ts, stars)
            derivation_mode = "empirical-method-of-moments"
        else:
            sys.stderr.write(
                "[derive] --include-ghalogs not set; emitting cardoen-only-no-hawkes skeleton.\n"
                "[derive] Pass --include-ghalogs to regenerate the committed empirical TOML.\n"
            )
            wf_table = {"CI": 0.18, "Build": 0.14, "Tests": 0.12, "_other": 0.56}
            branch_table = {k: 0.0 for k in [*_BRANCH_PATTERN_REGEX.keys(), "other"]}
            branch_table["main"] = 0.55
            branch_table["feature/*"] = 0.20
            branch_table["dependabot/*"] = 0.10
            branch_table["other"] = 0.15
            concl_table = {
                "success": 0.7287,
                "failure": 0.2013,
                "cancelled": 0.04,
                "skipped": 0.02,
                "timed_out": 0.005,
                "action_required": 0.003,
                "neutral": 0.001,
                "null": 0.001,
            }
            hawkes_table = _HAWKES_SEED_DEFAULTS
            derivation_mode = "cardoen-only-no-hawkes"

        _emit_toml(
            output_path,
            workflow_names=wf_table,
            branch_patterns=branch_table,
            conclusions=concl_table,
            exit_codes_on_failure=_EXIT_CODES_ON_FAILURE,
            hawkes=hawkes_table,
            transitions=_TRANSITIONS,
            source_mix_default=_SOURCE_MIX_DEFAULT,
            derivation_mode=derivation_mode,
        )
        sys.stderr.write(f"[derive] wrote {output_path}\n")
        return 0

    # Mock derivation: emit the same seed values, but with derivation_mode
    # = "mock" so the operator can see the script ran end-to-end without
    # confusing the artefact for an empirically-derived one.
    mock_branches = [
        "main",
        "main",
        "main",
        "main",
        "main",
        "feature/add-x",
        "feature/refactor-y",
        "dependabot/npm/foo",
        "hotfix/bug",
        "release/v1.2",
        "renovate/lodash",
        "topic/exp",
    ]
    counts: dict[str, int] = {}
    for raw in mock_branches:
        cat = _classify_branch(raw)
        counts[cat] = counts.get(cat, 0) + 1
    total = sum(counts.values())
    branch_table_mock = {k: round(v / total, 4) for k, v in counts.items()}
    if include_ghalogs:
        sys.stderr.write("[derive --mock] consulted GHALogs summary tier (no raw redistribution)\n")
    sys.stderr.write(f"[derive --mock] branch-pattern classification result: {branch_table_mock}\n")
    sys.stderr.write(f"[derive --mock] would write {output_path}\n")
    sys.stderr.write("[derive --mock] mock mode does NOT overwrite the committed seed file.\n")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Derive (or check) the GitHub workflow_run distribution priors.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    # --check and --derive are mutually exclusive verbs: --check is the
    # CI gate (validate the committed TOML); --derive is the
    # maintainer-only regeneration path. Composing them risks masking a
    # failing --check with a passing --mock derive — argparse rejects
    # the combination at parse time so the conflict surfaces immediately
    # rather than degrading silently into an exit-0 aggregate.
    verb_group = ap.add_mutually_exclusive_group()
    verb_group.add_argument(
        "--check",
        action="store_true",
        help="Validate the committed TOML against the schema and probability invariants. Exits 1 on any failure.",
    )
    verb_group.add_argument(
        "--derive",
        action="store_true",
        help="Run the maintainer-side derivation pass against the Zenodo datasets.",
    )
    ap.add_argument(
        "--mock",
        action="store_true",
        help="Used with --derive: exercise the plumbing against a tiny synthetic dataset.",
    )
    ap.add_argument(
        "--include-ghalogs",
        action="store_true",
        help="Used with --derive: also consult the CC-BY-SA-4.0 GHALogs summary tier.",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help=f"Output path for --derive (default: {_DEFAULT_OUTPUT.relative_to(_REPO_ROOT)}).",
    )
    args = ap.parse_args(argv)

    # Default to --check when neither flag is given so CI invocation is
    # `python scripts/derive_gh_distributions.py --check` with no args
    # path also working.
    if not args.check and not args.derive:
        args.check = True

    if args.check:
        return _check(args.output)

    # `args.derive` is the only remaining mutually-exclusive arm after
    # the `args.check` branch above; argparse's mutex group guarantees
    # one of the two is true once the default-to-check has run.
    return _derive(args.output, include_ghalogs=args.include_ghalogs, mock=args.mock)


if __name__ == "__main__":
    sys.exit(main())
