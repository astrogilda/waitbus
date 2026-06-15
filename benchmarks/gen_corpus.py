"""Seeded synthetic event-corpus generator for waitbus benchmarks.

Outputs gzipped JSONL where each line is one event with a
canonical shape:

    {
      "delivery_id": str,
      "source": "github" | "pytest" | "docker" | "fs",
      "event_type": str,
      "owner": str,
      "repo": str,
      "received_at_ns": int,
      "inter_arrival_ns": int,
      "payload": {...},
      "ingest_method": "corpus_replay"
    }

Reproducibility contract
------------------------

The output is byte-reproducible from the triple

    (--seed, sha256(gh_distributions.toml), _GENERATOR_VERSION)

The ``--check`` mode verifies a committed corpus against that triple by
re-generating in-memory and comparing line counts + SHA-256s.

Burst-arrival modelling
-----------------------

Inter-arrival times for github events follow a Hawkes process (self-
exciting point process) parameterised by ``hawkes.{small,medium,large}``
tables in the TOML. Non-github sources sample a per-repo size class and
re-use the same Hawkes path so the joint event timeline preserves
cross-source burst correlation when ``--source-mix`` is non-trivial.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import random
import sys
import tomllib
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, TypeAlias

import msgspec

from benchmarks._source_taxonomy import SOAK_SOURCE_REGISTRY
from waitbus._types import NS_PER_SECOND

#: Convenience alias for webhook payload dicts threaded through the
#: per-source generators and the top-level event composer.
_PayloadDict: TypeAlias = dict[str, Any]

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_TOML = _REPO_ROOT / "benchmarks" / "data" / "gh_distributions.toml"
_DEFAULT_OUTPUT = _REPO_ROOT / "benchmarks" / "data" / "corpus.jsonl.gz"
_PAYLOAD_EXAMPLES_DIR = _REPO_ROOT / "benchmarks" / "data" / "payload_templates" / "examples"

#: Bump on any change to the generator's wire output. Part of the
#: reproducibility triple ``(seed, toml_sha, _GENERATOR_VERSION)``.
_GENERATOR_VERSION = "0.1.0"

#: Anchor wall-clock for ``received_at_ns``. Fixed so the corpus is
#: byte-reproducible across machines: real wall-clock time would
#: introduce per-run drift. 2026-01-01T00:00:00Z in nanoseconds.
_ANCHOR_NS = 1_767_225_600_000_000_000

#: Canonical source names in corpus-generation order, derived from the
#: shared soak taxonomy so this module and scripts/soak/_main.py stay in sync.
_SOURCES: tuple[str, ...] = tuple(s.name for s in SOAK_SOURCE_REGISTRY)

#: Primary event type each source emits, derived from the shared taxonomy.
_EVENT_TYPES: dict[str, str] = {s.name: s.event_type for s in SOAK_SOURCE_REGISTRY}

#: Repo-size classes used by the Hawkes sampler.  Mirrored in the TOML
#: ``hawkes.{small,medium,large}`` tables; declaring them here makes the
#: three-class assumption explicit and prevents silent drift if the TOML
#: gains a fourth class without a matching code-side branch.
_HAWKES_CLASSES: Final[tuple[str, ...]] = ("small", "medium", "large")


# ---------------------------------------------------------------------------
# TOML loading
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Distributions:
    """Parsed view of gh_distributions.toml."""

    workflow_names: dict[str, float]
    branch_patterns: dict[str, float]
    conclusions: dict[str, float]
    exit_codes_on_failure: dict[str, float]
    hawkes: dict[str, dict[str, float]]
    transitions: dict[str, dict[str, float]]
    source_mix_default: dict[str, float]
    toml_sha256: str


def _load_distributions(toml_path: Path) -> Distributions:
    with toml_path.open("rb") as fh:
        raw_bytes = fh.read()
    doc = tomllib.loads(raw_bytes.decode("utf-8"))
    # TOML emits ``int`` when a probability value is whole (e.g. ``0`` /
    # ``1``); the Distributions dataclass annotates float-valued tables.
    # Coerce at load so the annotation matches runtime reality and any
    # downstream arithmetic stays bit-stable across edits to the TOML.
    return Distributions(
        workflow_names={k: float(v) for k, v in doc["workflow_names"].items()},
        branch_patterns={k: float(v) for k, v in doc["branch_patterns"].items()},
        conclusions={k: float(v) for k, v in doc["conclusions"].items()},
        exit_codes_on_failure={k: float(v) for k, v in doc["exit_codes_on_failure"].items()},
        hawkes={k: {pk: float(pv) for pk, pv in v.items()} for k, v in doc["hawkes"].items()},
        transitions={k: {tk: float(tv) for tk, tv in v.items()} for k, v in doc["transitions"].items()},
        source_mix_default={k: float(v) for k, v in doc["source_mix_default"].items()},
        toml_sha256=hashlib.sha256(raw_bytes).hexdigest(),
    )


# ---------------------------------------------------------------------------
# Weighted draw helpers
# ---------------------------------------------------------------------------


def _weighted_draw(rng: random.Random, table: dict[str, float]) -> str:
    """Sample one key from ``table`` proportional to its float weight."""
    keys = list(table.keys())
    weights = [float(table[k]) for k in keys]
    return rng.choices(keys, weights=weights, k=1)[0]


# ---------------------------------------------------------------------------
# Hawkes-process inter-arrival sampler
# ---------------------------------------------------------------------------


@dataclass
class HawkesState:
    """Self-exciting point process state, Ogata-thinning sampler.

    The conditional intensity at time t is
        lambda(t) = mu + sum_{t_i < t} alpha * exp(-beta * (t - t_i))
    Inter-arrival times are sampled via thinning against a piecewise
    upper bound.
    """

    mu: float
    alpha: float
    beta: float
    # ``last_event_t`` is the running absolute timestamp of the sampler
    # (in seconds since the first draw). It is provably UNREAD by the
    # current exponential-Hawkes accumulator path: ``current_excitation``
    # carries all cross-call memory via the ``exp(-beta * dt)`` decay in
    # ``next_inter_arrival``. We keep the field as staging for future
    # variants whose state-update equation consumes absolute timestamps
    # rather than inter-arrival deltas -- specifically NHP (Mei & Eisner,
    # NeurIPS 2017, arXiv:1612.09328), SAHP/THP (ICML 2020), and AttNHP
    # (EMNLP 2022).
    last_event_t: float = 0.0
    current_excitation: float = 0.0

    def next_inter_arrival(self, rng: random.Random) -> float:
        """Return the next inter-arrival in seconds and advance state."""
        t = 0.0
        # Upper bound on intensity = mu + current_excitation (before any
        # decay since last event). Re-derived after each thinning reject.
        while True:
            lam_bar = self.mu + self.current_excitation
            if lam_bar <= 0.0:
                lam_bar = self.mu  # numerical floor
            u = rng.random()
            # Exponential candidate inter-arrival at rate lam_bar.
            dt = -((1.0 / lam_bar) * _log(u, rng))
            t += dt
            # Decay excitation across dt and compute actual intensity.
            self.current_excitation *= math.exp(-self.beta * dt)
            lam = self.mu + self.current_excitation
            # Accept with probability lam / lam_bar.
            if rng.random() <= lam / lam_bar:
                self.current_excitation += self.alpha
                self.last_event_t += t
                return t


def _log(x: float, rng: random.Random) -> float:
    """math.log(x); re-draws u from rng on the pathological u==0 case."""
    while x <= 0.0:
        x = rng.random()
    return math.log(x)


# ---------------------------------------------------------------------------
# Per-source generators
# ---------------------------------------------------------------------------


# workflow_run subtype string -> workflow_run.status value. Subtype values
# not in this map fall back to "requested" via dict.get(..., "requested").
_STATUS_BY_SUBTYPE: dict[str, str] = {
    "workflow_run_completed": "completed",
    "workflow_run_in_progress": "in_progress",
}


#: Fields kept from the vendored repository / workflow_run blobs. The
#: full octokit payload carries ~80 URL templates per repository; the
#: waitbus listener parses only a small subset, so the corpus carries the
#: subset to keep the gzipped artefact under the < 500 KB budget while
#: still exercising the JSON-parse / field-extraction hot path.
_REPO_KEEP_FIELDS: tuple[str, ...] = (
    "id",
    "name",
    "full_name",
    "owner",
    "private",
    "html_url",
    "description",
    "default_branch",
    "fork",
)
_WORKFLOW_RUN_KEEP_FIELDS: tuple[str, ...] = (
    "id",
    "name",
    "node_id",
    "head_branch",
    "head_sha",
    "run_number",
    "event",
    "status",
    "conclusion",
    "workflow_id",
    "url",
    "html_url",
    "created_at",
    "updated_at",
    "run_attempt",
    "run_started_at",
)


def _prune(d: _PayloadDict, keep: tuple[str, ...]) -> _PayloadDict:
    return {k: d[k] for k in keep if k in d}


def _load_workflow_run_template() -> _PayloadDict:
    """Load the completed.payload.json template from vendored octokit data.

    Note: the vendored ``benchmarks/data/payload_templates/examples/``
    directory ships four octokit/webhooks fixtures
    (``completed.payload.json``, ``completed.with-pull-requests.payload.json``,
    ``requested.payload.json``, ``requested.with-conclusion.payload.json``).
    The generator reads only the ``completed`` shape and reshapes its
    ``status`` / ``conclusion`` facets per Hawkes-driven dispatch in
    ``_gen_github_workflow_run``. The other three files ship as
    **schema anchors** — they document the per-status canonical JSON
    shape octokit publishes upstream and let the corpus stay schema-
    true if a future sampler ever wants to draw per-status templates
    (which would change the rng-draw order and break the determinism
    contract, so it is deferred). They are not currently sampled.
    """
    path = _PAYLOAD_EXAMPLES_DIR / "completed.payload.json"
    with path.open("rb") as fh:
        raw = fh.read()
    decoded: _PayloadDict = msgspec.json.decode(raw)
    # Prune to the listener-relevant subset so the gzipped corpus fits
    # the < 500 KB budget without dropping the field-extraction hot path.
    if "repository" in decoded and isinstance(decoded["repository"], dict):
        decoded["repository"] = _prune(decoded["repository"], _REPO_KEEP_FIELDS)
    if "workflow_run" in decoded and isinstance(decoded["workflow_run"], dict):
        decoded["workflow_run"] = _prune(decoded["workflow_run"], _WORKFLOW_RUN_KEEP_FIELDS)
    # Sender is small enough to keep verbatim.
    return decoded


def _gen_github_workflow_run(
    rng: random.Random,
    dists: Distributions,
    index: int,
    template: _PayloadDict,
    prev_event_type: str | None,
) -> _PayloadDict:
    """Construct one synthetic workflow_run event using the vendored template."""
    workflow_name = _weighted_draw(rng, dists.workflow_names)
    branch_category = _weighted_draw(rng, dists.branch_patterns)
    head_branch = _materialise_branch(rng, branch_category, index)
    conclusion = _weighted_draw(rng, dists.conclusions)
    # status / event-type transition driven by the transition matrix.
    next_subtype = _next_subtype(rng, dists, prev_event_type)
    status = _STATUS_BY_SUBTYPE.get(next_subtype, "requested")
    if status != "completed":
        conclusion = "null"

    exit_code: int | None = None
    if conclusion == "failure":
        exit_code = _draw_exit_code(rng, dists)

    # Reshape: start from the template (canonical shape) and override the
    # facets we model. msgspec.json round-trip would re-encode; keep it
    # as a plain dict so the gzipped corpus stays JSON-only.
    workflow_run = dict(template.get("workflow_run", {}))
    workflow_run.update(
        {
            "name": workflow_name,
            "head_branch": head_branch,
            "status": status,
            "conclusion": conclusion if conclusion != "null" else None,
            "id": 12_000_000_000 + index,
            "run_number": (index % 10_000) + 1,
        }
    )
    payload: _PayloadDict = {
        "action": status,
        "workflow_run": workflow_run,
        "repository": template.get("repository", {}),
        "sender": template.get("sender", {}),
    }
    if exit_code is not None:
        # Custom out-of-band facet recorded so the corpus replay layer
        # can validate exit-code distribution without re-parsing the
        # workflow_run body. Not part of the upstream webhook shape.
        payload["_waitbus_exit_code"] = exit_code
    return payload


def _draw_exit_code(rng: random.Random, dists: Distributions) -> int:
    """Sample an exit code from the failure-conditional distribution."""
    key = _weighted_draw(rng, dists.exit_codes_on_failure)
    if key == "_other":
        return 1
    try:
        return int(key)
    except ValueError:
        return 1


def _materialise_branch(rng: random.Random, category: str, index: int) -> str:
    """Materialise a concrete branch name for a regex category."""
    match category:
        case "main":
            return "main"
        case "feature/*":
            return f"feature/synthetic-{index % 256}"
        case "hotfix/*":
            return f"hotfix/synthetic-{index % 64}"
        case "dependabot/*":
            return f"dependabot/npm/synthetic-{index % 32}"
        case "release/*":
            return f"release/v0.{index % 16}.0"
        case "renovate/*":
            return f"renovate/synthetic-{index % 32}"
        case _:
            return f"topic/synthetic-{index % 128}"


def _next_subtype(rng: random.Random, dists: Distributions, prev: str | None) -> str:
    """Sample the next github-event sub-type from the transition matrix."""
    if prev is None or prev not in dists.transitions:
        prev = "workflow_run_completed"
    return _weighted_draw(rng, dists.transitions[prev])


def _gen_pytest_session(rng: random.Random, index: int) -> _PayloadDict:
    n_tests = rng.randint(5, 200)
    n_failed = 0 if rng.random() < 0.82 else rng.randint(1, max(1, n_tests // 10))
    return {
        "node_id": f"tests/synthetic_{index % 64}.py",
        "n_tests": n_tests,
        "n_failed": n_failed,
        "n_skipped": rng.randint(0, max(1, n_tests // 20)),
        "duration_sec": round(rng.uniform(0.5, 240.0), 3),
        "outcome": "passed" if n_failed == 0 else "failed",
    }


def _gen_docker_container(rng: random.Random, index: int) -> _PayloadDict:
    images = ("python:3.12-slim", "node:20-alpine", "rust:1.81", "alpine:3.20", "ubuntu:24.04")
    return {
        "container_id": f"{rng.getrandbits(64):016x}",
        "image": rng.choice(images),
        "status": "exited" if rng.random() < 0.9 else "running",
        "exit_code": 0 if rng.random() < 0.85 else rng.choice([1, 2, 137, 139]),
        "duration_sec": round(rng.uniform(0.2, 1800.0), 3),
        "_index": index,
    }


def _gen_fs_change(rng: random.Random, index: int) -> _PayloadDict:
    return {
        "path": f"/tmp/waitbus-corpus/synthetic-{index % 256}.txt",
        "event_kind": rng.choice(("created", "modified", "deleted")),
        "size_bytes": rng.randint(0, 1024 * 1024),
        "st_mtime_ns": _ANCHOR_NS + index * 1_000_000,
    }


def _gen_agent(rng: random.Random, index: int) -> _PayloadDict:
    """Synthetic agent-coordination event payload.

    Mirrors the addressed-messaging facet a real ``request`` / ``respond``
    pair would carry on the wire: ``msg_from`` always present, ``msg_to``
    present for the addressed case (about 70% of traffic) and absent for
    the broadcast case, ``msg_correlation_id`` always present so the
    stress harness can pair requests with responses, and a small synthetic
    ``msg_body``. Agent IDs are drawn from a pool of 16 to give the
    fan-out machinery realistic recipient diversity at moderate N.
    """
    agents = tuple(f"agent-{i:02d}" for i in range(16))
    sender = rng.choice(agents)
    addressed = rng.random() < 0.70
    recipient: str | None = rng.choice(agents) if addressed else None
    payload: _PayloadDict = {
        "msg_from": sender,
        "msg_correlation_id": f"corr-{rng.getrandbits(64):016x}",
        "msg_body": f"synthetic-payload-{index}",
        "_index": index,
    }
    if recipient is not None:
        payload["msg_to"] = recipient
    return payload


# ---------------------------------------------------------------------------
# Top-level event composer
# ---------------------------------------------------------------------------


def _compose_event(
    *,
    rng: random.Random,
    dists: Distributions,
    index: int,
    source: str,
    received_at_ns: int,
    inter_arrival_ns: int,
    gh_template: _PayloadDict,
    prev_gh_subtype: str | None,
) -> tuple[_PayloadDict, str | None]:
    """Return ``(event, new_prev_gh_subtype)``."""
    new_prev: str | None = prev_gh_subtype
    match source:
        case "github":
            payload = _gen_github_workflow_run(rng, dists, index, gh_template, prev_gh_subtype)
            new_prev = "workflow_run_" + (payload["workflow_run"].get("status") or "completed")
        case "pytest":
            payload = _gen_pytest_session(rng, index)
        case "docker":
            payload = _gen_docker_container(rng, index)
        case "fs":
            payload = _gen_fs_change(rng, index)
        case "agent":
            payload = _gen_agent(rng, index)
        case _:  # pragma: no cover -- source comes from _SOURCES via rng.choices; structurally unreachable
            raise ValueError(f"unknown source {source!r}")

    event: _PayloadDict = {
        "delivery_id": f"corpus-{index:08d}-{rng.getrandbits(48):012x}",
        "source": source,
        "event_type": _EVENT_TYPES[source],
        "owner": f"synth-owner-{index % 32}",
        "repo": f"synth-repo-{index % 128}",
        "received_at_ns": received_at_ns,
        "inter_arrival_ns": inter_arrival_ns,
        "payload": payload,
        "ingest_method": "corpus_replay",
    }
    return event, new_prev


# ---------------------------------------------------------------------------
# Source-mix parsing
# ---------------------------------------------------------------------------


def _parse_source_mix(spec: str) -> dict[str, float]:
    """Parse ``"40/15/15/10/20"`` -> normalised dict over the registry order.

    Weights line up with ``_SOURCES`` (derived from ``SOAK_SOURCE_REGISTRY``):
    ``github / pytest / docker / fs / agent``.
    """
    parts = spec.split("/")
    expected = len(_SOURCES)
    if len(parts) != expected:
        names = "/".join(_SOURCES)
        raise argparse.ArgumentTypeError(
            f"--source-mix expects {expected} slash-separated weights ({names}), got {spec!r}"
        )
    try:
        weights = [float(p) for p in parts]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--source-mix weights must be numeric: {exc}") from exc
    total = sum(weights)
    if total <= 0:
        raise argparse.ArgumentTypeError("--source-mix weights must sum to a positive number")
    return {src: w / total for src, w in zip(_SOURCES, weights, strict=True)}


# ---------------------------------------------------------------------------
# Main generation pipeline
# ---------------------------------------------------------------------------


def _select_repo_size_class(rng: random.Random) -> str:
    """Per-corpus repo size sampled once per event so multi-source mix is
    aware of the same scale.
    """
    r = rng.random()
    if r < 0.50:
        return "small"
    if r < 0.85:
        return "medium"
    return "large"


def generate(
    *,
    seed: int,
    n: int,
    source_mix: dict[str, float],
    dists: Distributions,
) -> Iterator[_PayloadDict]:
    """Yield ``n`` synthetic events in deterministic order."""
    rng = random.Random(seed)
    gh_template = _load_workflow_run_template()
    sources_list = list(source_mix.keys())
    sources_weights = [source_mix[s] for s in sources_list]
    received_at_ns = _ANCHOR_NS
    prev_gh_subtype: str | None = None

    # Initial Hawkes state seeded from the "medium" class; the per-event
    # state is re-keyed to whichever class the next event lands in.
    hawkes_state_by_class: dict[str, HawkesState] = {
        cls: HawkesState(
            mu=float(dists.hawkes[cls]["mu_per_sec"]),
            alpha=float(dists.hawkes[cls]["alpha"]),
            beta=float(dists.hawkes[cls]["beta"]),
        )
        for cls in dists.hawkes
    }

    for index in range(n):
        size_class = _select_repo_size_class(rng)
        inter_arrival_sec = hawkes_state_by_class[size_class].next_inter_arrival(rng)
        inter_arrival_ns = max(1, int(inter_arrival_sec * NS_PER_SECOND))
        received_at_ns += inter_arrival_ns
        source = rng.choices(sources_list, weights=sources_weights, k=1)[0]
        event, prev_gh_subtype = _compose_event(
            rng=rng,
            dists=dists,
            index=index,
            source=source,
            received_at_ns=received_at_ns,
            inter_arrival_ns=inter_arrival_ns,
            gh_template=gh_template,
            prev_gh_subtype=prev_gh_subtype,
        )
        yield event


def _encode_event(event: _PayloadDict) -> bytes:
    """Encode one event as a sorted-key JSON line for stable byte output."""
    return json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"


def write_corpus(
    output_path: Path,
    events: Iterator[_PayloadDict],
) -> tuple[int, str]:
    """Write events to ``output_path`` (gzipped JSONL). Return (count, sha256_of_uncompressed)."""
    buf = io.BytesIO()
    hasher = hashlib.sha256()
    count = 0
    for ev in events:
        line = _encode_event(ev)
        buf.write(line)
        hasher.update(line)
        count += 1
    raw = buf.getvalue()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # mtime=0 + fixed compression level keeps the gzipped file
    # byte-reproducible across machines.
    with gzip.GzipFile(filename=str(output_path), mode="wb", compresslevel=6, mtime=0) as gz:
        gz.write(raw)
    return count, hasher.hexdigest()


def stream_to_stdout(events: Iterator[_PayloadDict]) -> None:
    """Write JSONL events to ``sys.stdout.buffer`` (uncompressed)."""
    out = sys.stdout.buffer
    for ev in events:
        out.write(_encode_event(ev))
    out.flush()


# ---------------------------------------------------------------------------
# --check mode
# ---------------------------------------------------------------------------


def _check(
    *,
    corpus_path: Path,
    seed: int,
    n: int,
    source_mix: dict[str, float],
    dists: Distributions,
) -> int:
    """Re-generate in memory and verify the committed corpus matches."""
    if not corpus_path.exists():
        sys.stderr.write(f"ERROR: corpus not found at {corpus_path}\n")
        return 1
    with gzip.open(corpus_path, "rb") as fh:
        committed_raw = fh.read()
    committed_sha = hashlib.sha256(committed_raw).hexdigest()

    expected_buf = io.BytesIO()
    expected_count = 0
    for ev in generate(seed=seed, n=n, source_mix=source_mix, dists=dists):
        expected_buf.write(_encode_event(ev))
        expected_count += 1
    expected_raw = expected_buf.getvalue()
    expected_sha = hashlib.sha256(expected_raw).hexdigest()

    if committed_sha != expected_sha:
        sys.stderr.write(
            "FAIL: committed corpus does not match re-derived output\n"
            f"  committed sha256: {committed_sha}\n"
            f"  expected  sha256: {expected_sha}\n"
            f"  committed bytes:  {len(committed_raw)}\n"
            f"  expected  bytes:  {len(expected_raw)}\n"
        )
        return 1
    print(
        f"OK: {corpus_path} matches (n={expected_count}, seed={seed}, "
        f"toml_sha256={dists.toml_sha256[:16]}..., generator={_GENERATOR_VERSION})"
    )
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Generate (or verify) a seeded synthetic waitbus event corpus.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--seed", type=int, default=0, help="RNG seed (default 0).")
    ap.add_argument("--n", type=int, default=5000, help="number of events (default 5000).")
    ap.add_argument(
        "--source-mix",
        type=str,
        default="40/15/15/10/20",
        help="github/pytest/docker/fs/agent weights (default 40/15/15/10/20).",
    )
    ap.add_argument(
        "--distributions",
        type=Path,
        default=_DEFAULT_TOML,
        help=f"TOML distribution priors (default {_DEFAULT_TOML.relative_to(_REPO_ROOT)}).",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Output path for gzipped JSONL. If omitted, uncompressed JSONL is "
            "streamed to stdout (the canonical reproducibility check piped to "
            "sha256sum)."
        ),
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help=(
            "Verify a committed corpus matches the in-memory regenerated bytes. "
            "Uses --output (or its default) as the corpus path."
        ),
    )
    args = ap.parse_args(argv)

    source_mix = _parse_source_mix(args.source_mix)
    dists = _load_distributions(args.distributions)

    if args.check:
        corpus_path = args.output if args.output is not None else _DEFAULT_OUTPUT
        return _check(
            corpus_path=corpus_path,
            seed=args.seed,
            n=args.n,
            source_mix=source_mix,
            dists=dists,
        )

    events = generate(seed=args.seed, n=args.n, source_mix=source_mix, dists=dists)
    if args.output is None:
        stream_to_stdout(events)
    else:
        count, sha = write_corpus(args.output, events)
        sys.stderr.write(
            f"[gen_corpus] wrote {args.output} ({count} events, uncompressed_sha256={sha[:16]}..., "
            f"toml_sha256={dists.toml_sha256[:16]}..., generator={_GENERATOR_VERSION})\n"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
