#!/usr/bin/env python3
"""Derive empirical token-cost defaults for each waitbus polling source.

Each source (github, pytest, docker, fs) emits two kinds of responses
during a polling session: a small poll (nothing has changed yet) and a
terminal poll (the awaited condition has been reached).  The per-source
default is a weighted average of those two costs modelled over a
realistic polling session.

Run standalone::

    python scripts/derive_poll_costs.py

Write the committed output artefact::

    python scripts/derive_poll_costs.py --output benchmarks/poll_cost_derivation.json

Check that the committed output still matches the current derivation::

    python scripts/derive_poll_costs.py --check

Verify that stats.py hard-codes the derived values::

    python scripts/derive_poll_costs.py --against waitbus/stats.py

All four modes exit 0 on success and 1 on any mismatch or error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import tiktoken

_ENCODER_NAME = "cl100k_base"
_OUTPUT_PATH = Path(__file__).resolve().parent.parent / "benchmarks" / "poll_cost_derivation.json"


# ---------------------------------------------------------------------------
# Synthetic payload definitions
# ---------------------------------------------------------------------------


def _github_small_poll_text() -> str:
    """Minimal GitHub Actions run payload returned while the run is still running.

    Emulates the JSON body returned by
    ``GET /repos/<owner>/<repo>/actions/runs/<id>`` when status is
    ``in_progress``.  ``gh run watch --interval 3`` reads only the
    ``status`` and ``conclusion`` fields to decide whether the run has
    finished; the remaining envelope fields are present in the wire
    response but are not re-processed on each poll.  This payload
    represents what an agent would extract and hand to its decision
    layer, not the full HTTP response body.
    """
    # The decision layer reads status and conclusion; the wire response
    # also includes run number, name, and updated_at for display.  This
    # five-field subset is what an agent would extract and evaluate per poll.
    payload = {
        "status": "in_progress",
        "conclusion": None,
        "run_number": 42,
        "name": "CI",
        "updated_at": "2026-05-20T10:05:00Z",
    }
    return json.dumps(payload)


def _github_terminal_poll_text() -> str:
    """Full workflow_run payload returned when the run completes.

    A completed run payload carries many more fields than the in-progress
    version.  The fields here are drawn from the GitHub REST API
    ``workflow_run`` object schema (subset used by ``gh run watch``).
    Approximately 3 KB serialised, around 750 cl100k tokens.
    """
    payload = {
        "id": 12345678901,
        "name": "CI",
        "node_id": "WFR_kwDOBQTxyz1234567890",
        "head_branch": "main",
        "head_sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
        "run_number": 42,
        "event": "push",
        "display_title": "fix: correct off-by-one in coalesce window",
        "status": "completed",
        "conclusion": "success",
        "workflow_id": 99887766,
        "check_suite_id": 11223344556,
        "check_suite_node_id": "CS_kwDOBQTxyz9876543210",
        "url": "https://api.github.com/repos/owner/repo/actions/runs/12345678901",
        "html_url": "https://github.com/owner/repo/actions/runs/12345678901",
        "pull_requests": [],
        "created_at": "2026-05-20T10:00:00Z",
        "updated_at": "2026-05-20T10:15:00Z",
        "run_attempt": 1,
        "referenced_workflows": [],
        "run_started_at": "2026-05-20T10:00:05Z",
        "triggering_actor": {
            "login": "octocat",
            "id": 1,
            "node_id": "MDQ6VXNlcjE=",
            "avatar_url": "https://github.com/images/error/octocat_happy.gif",
            "html_url": "https://github.com/octocat",
            "type": "User",
            "site_admin": False,
        },
        "jobs_url": "https://api.github.com/repos/owner/repo/actions/runs/12345678901/jobs",
        "logs_url": "https://api.github.com/repos/owner/repo/actions/runs/12345678901/logs",
        "check_suite_url": "https://api.github.com/repos/owner/repo/check-suites/11223344556",
        "artifacts_url": "https://api.github.com/repos/owner/repo/actions/runs/12345678901/artifacts",
        "cancel_url": "https://api.github.com/repos/owner/repo/actions/runs/12345678901/cancel",
        "rerun_url": "https://api.github.com/repos/owner/repo/actions/runs/12345678901/rerun",
        "workflow_url": "https://api.github.com/repos/owner/repo/actions/workflows/99887766",
        "head_commit": {
            "id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            "tree_id": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
            "message": "fix: correct off-by-one in coalesce window",
            "timestamp": "2026-05-20T09:59:50Z",
            "author": {"name": "octocat", "email": "octocat@github.com"},
            "committer": {"name": "octocat", "email": "octocat@github.com"},
        },
        "repository": {
            "id": 123456789,
            "node_id": "MDEwOlJlcG9zaXRvcnkxMjM0NTY3ODk=",
            "name": "repo",
            "full_name": "owner/repo",
            "private": False,
            "html_url": "https://github.com/owner/repo",
            "description": "Example repository",
            "fork": False,
            "url": "https://api.github.com/repos/owner/repo",
        },
        "head_repository": {
            "id": 123456789,
            "node_id": "MDEwOlJlcG9zaXRvcnkxMjM0NTY3ODk=",
            "name": "repo",
            "full_name": "owner/repo",
            "private": False,
            "html_url": "https://github.com/owner/repo",
        },
    }
    return json.dumps(payload, indent=2)


def _pytest_small_poll_text() -> str:
    """Response when report.xml has not changed since the last check.

    Emulates the output of a tail+parse loop that compares the file's
    mtime before re-parsing: if the mtime is unchanged, the loop emits a
    one-line status string rather than re-reading the file.
    """
    return "report.xml: no new content (mtime unchanged)"


def _pytest_terminal_poll_text() -> str:
    """Relevant portion of a pytest JUnit XML report for a clean run.

    Emulates the slice of report.xml that a parsing loop would extract
    after the run completes.  A clean 15-test run with no failures or
    skips produces roughly this shape.  The traceback variant (500-2000
    tokens for a medium failure report) is documented in the rationale
    but the clean-run terminal is used as the default because it
    represents the expected steady-state outcome and because using a
    worst-case traceback would over-estimate costs for successful
    pipelines.
    """
    return """\
<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="0" failures="0" skipped="0" tests="15"
             time="2.341" timestamp="2026-05-20T10:15:00.000000"
             hostname="builder-01">
    <testcase classname="tests.test_coalesce" name="test_window_basic" time="0.012"/>
    <testcase classname="tests.test_coalesce" name="test_window_overlap" time="0.009"/>
    <testcase classname="tests.test_coalesce" name="test_empty_window" time="0.005"/>
    <testcase classname="tests.test_predicate" name="test_simple_match" time="0.011"/>
    <testcase classname="tests.test_predicate" name="test_no_match" time="0.008"/>
    <testcase classname="tests.test_predicate" name="test_wildcard" time="0.014"/>
    <testcase classname="tests.test_db" name="test_insert_event" time="0.022"/>
    <testcase classname="tests.test_db" name="test_query_range" time="0.018"/>
    <testcase classname="tests.test_db" name="test_prune" time="0.031"/>
    <testcase classname="tests.test_emit" name="test_emit_basic" time="0.045"/>
    <testcase classname="tests.test_emit" name="test_emit_batch" time="0.052"/>
    <testcase classname="tests.test_listener" name="test_connect" time="0.041"/>
    <testcase classname="tests.test_listener" name="test_reconnect" time="0.038"/>
    <testcase classname="tests.test_broadcast" name="test_pub_sub" time="0.067"/>
    <testcase classname="tests.test_broadcast" name="test_multi_subscriber" time="0.059"/>
  </testsuite>
</testsuites>"""


def _docker_small_poll_text() -> str:
    """One container row from ``docker ps -a`` while the container is running.

    Emulates the default tabular output of::

        docker ps -a --filter id=a1b2c3d4e5f6 --no-trunc

    The default output format is a fixed-width table; an agent reads the
    STATUS column to check whether the container is still running.
    """
    return (
        "CONTAINER ID  IMAGE              COMMAND           CREATED        STATUS          NAMES\n"
        "a1b2c3d4e5f6  python:3.12-slim  /entrypoint.sh   5 minutes ago  Up 5 minutes    ci-job-runner"
    )


def _docker_terminal_poll_text() -> str:
    """Same tabular row with STATUS=Exited, returned when the container exits."""
    return (
        "CONTAINER ID  IMAGE              COMMAND           CREATED         STATUS                     NAMES\n"
        "a1b2c3d4e5f6  python:3.12-slim  /entrypoint.sh   20 minutes ago  Exited (0) 5 minutes ago   ci-job-runner"
    )


def _fs_small_poll_text() -> str:
    """Formatted ``os.stat`` result while the watched file has not changed.

    A waitbus filesystem-source poll calls ``os.stat`` on the watched path
    and compares the ``st_mtime`` and ``st_size`` fields against the
    previous values.  The formatted output an agent sees on an unchanged
    poll is the numeric stat fields only — no path repetition needed
    because the path is fixed for the session.
    """
    return "st_mtime=1747735200.0 st_size=1048576 st_ino=123456"


def _fs_terminal_poll_text() -> str:
    """Formatted ``os.stat`` result after the file's mtime advances.

    When the file changes, the agent receives the updated stat fields.
    The ``changed=true`` flag is appended by the source adapter to
    indicate that the terminal condition has been reached.
    """
    return "st_mtime=1747736100.0 st_size=2097152 st_ino=123456 changed=true"


# ---------------------------------------------------------------------------
# Session model parameters
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SessionModel:
    """Parameters describing a typical polling session for one source."""

    small_polls: int
    """Number of small (no-change) polls before the terminal poll."""
    description: str
    """Human-readable summary of the assumed session."""


# Rationale for each source:
#
# github: A 15-minute CI run at 3-second poll intervals yields
#   15*60/3 = 300 small polls before the single terminal poll.
#   This is the dominant scenario for a medium-sized project's test suite.
#   A 5-minute run at the same interval gives ~100 small polls and a lower
#   average (~37 tokens); a 30-minute run gives ~600 small polls.  The
#   15-minute midpoint is the most representative single value.
#
# pytest: A local test suite completes in 30-120 seconds.  Polling at
#   5-second intervals gives 6-24 small polls.  Using 20 small polls
#   models a 100-second suite (common for a mid-size project) checked
#   every 5 seconds.
#
# docker: A containerised build or test job runs for 2-10 minutes.
#   Polling at 5-second intervals gives 24-120 small polls.  Using
#   60 small polls models a 5-minute job checked every 5 seconds.
#
# fs: A file-write event (artifact generation, build output) completes
#   in 1-30 seconds.  Polling at 1-second intervals gives 1-30 small
#   polls.  Using 15 small polls models a 15-second operation.

_SESSION_MODELS: dict[str, SessionModel] = {
    "github": SessionModel(
        small_polls=300,
        description="300 small polls + 1 terminal poll (15-min CI run at 3-second poll interval)",
    ),
    "pytest": SessionModel(
        small_polls=20,
        description="20 small polls + 1 terminal poll (100-second test suite polled every 5 seconds)",
    ),
    "docker": SessionModel(
        small_polls=60,
        description="60 small polls + 1 terminal poll (5-minute container job polled every 5 seconds)",
    ),
    "fs": SessionModel(
        small_polls=15,
        description="15 small polls + 1 terminal poll (15-second file operation polled every second)",
    ),
}

# Map each source to its small-poll and terminal-poll text functions.
_PAYLOAD_FNS: dict[str, tuple[Callable[[], str], Callable[[], str]]] = {
    "github": (_github_small_poll_text, _github_terminal_poll_text),
    "pytest": (_pytest_small_poll_text, _pytest_terminal_poll_text),
    "docker": (_docker_small_poll_text, _docker_terminal_poll_text),
    "fs": (_fs_small_poll_text, _fs_terminal_poll_text),
}


# ---------------------------------------------------------------------------
# Derivation logic
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceResult:
    """Derived values for one polling source."""

    source: str
    small_poll_text: str
    small_poll_tokens: int
    terminal_poll_text: str
    terminal_poll_tokens: int
    assumed_session: str
    derived_default_tokens: int
    rationale: str
    small_poll_text_sha256: str
    terminal_poll_text_sha256: str


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _count_tokens(enc: tiktoken.Encoding, text: str) -> int:
    return len(enc.encode(text))


def _derive_source(enc: tiktoken.Encoding, source: str) -> SourceResult:
    """Compute the weighted-average default for one source."""
    small_fn, terminal_fn = _PAYLOAD_FNS[source]
    small_text: str = small_fn()
    terminal_text: str = terminal_fn()
    model = _SESSION_MODELS[source]

    small_tokens = _count_tokens(enc, small_text)
    terminal_tokens = _count_tokens(enc, terminal_text)

    n = model.small_polls
    # Weighted average across the session: n small polls + 1 terminal poll.
    total_tokens = n * small_tokens + terminal_tokens
    total_polls = n + 1
    weighted_avg = round(total_tokens / total_polls)

    rationale = (
        f"{n} * {small_tokens} (small) + 1 * {terminal_tokens} (terminal) "
        f"/ {total_polls} polls = {total_tokens / total_polls:.1f} -> {weighted_avg} tokens"
    )

    return SourceResult(
        source=source,
        small_poll_text=small_text,
        small_poll_tokens=small_tokens,
        terminal_poll_text=terminal_text,
        terminal_poll_tokens=terminal_tokens,
        assumed_session=model.description,
        derived_default_tokens=weighted_avg,
        rationale=rationale,
        small_poll_text_sha256=_sha256(small_text),
        terminal_poll_text_sha256=_sha256(terminal_text),
    )


def derive_all() -> dict[str, SourceResult]:
    """Derive defaults for all four sources and return a keyed mapping."""
    enc = tiktoken.get_encoding(_ENCODER_NAME)
    return {src: _derive_source(enc, src) for src in ("github", "pytest", "docker", "fs")}


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _to_json_dict(results: dict[str, SourceResult]) -> dict[str, object]:
    return {
        "derived_at_iso": datetime.now(UTC).isoformat(timespec="seconds"),
        "tiktoken_encoder": _ENCODER_NAME,
        "per_source": {
            src: {
                "small_poll_text_sha256": r.small_poll_text_sha256,
                "small_poll_tokens": r.small_poll_tokens,
                "terminal_poll_text_sha256": r.terminal_poll_text_sha256,
                "terminal_poll_tokens": r.terminal_poll_tokens,
                "assumed_session": r.assumed_session,
                "derived_default_tokens": r.derived_default_tokens,
                "rationale": r.rationale,
            }
            for src, r in results.items()
        },
    }


def _print_results(results: dict[str, SourceResult]) -> None:
    """Print per-source derivation details to stdout."""
    print(f"Encoder: {_ENCODER_NAME}\n")
    for src, r in results.items():
        print(f"--- {src} ---")
        print(f"  small-poll  ({r.small_poll_tokens:4d} tokens):  {r.small_poll_text[:80]!r}")
        print(f"  terminal    ({r.terminal_poll_tokens:4d} tokens):  (first 80 chars) {r.terminal_poll_text[:80]!r}")
        print(f"  session:    {r.assumed_session}")
        print(f"  default:    {r.derived_default_tokens} tokens")
        print(f"  math:       {r.rationale}")
        print()

    # The derived values ARE the authoritative source-of-truth for the
    # waitbus stats per-source poll-cost defaults: this script is the
    # derivation, not a consumer of an external plan. Cross-checking
    # against hand-waved reference values would either duplicate the
    # derivation (pointless) or measure drift against a stale dict
    # (worse than nothing). The ``--against`` mode against waitbus/
    # stats.py covers the constants-match-derivation invariant for the
    # shipped values; that is the only meaningful cross-check.


# ---------------------------------------------------------------------------
# --check mode
# ---------------------------------------------------------------------------


def _check(committed_path: Path, results: dict[str, SourceResult]) -> int:
    """Re-derive and assert the committed JSON still matches."""
    if not committed_path.exists():
        sys.stderr.write(f"ERROR: committed output not found at {committed_path}\n")
        sys.stderr.write("Run without --check to generate it.\n")
        return 1

    committed = json.loads(committed_path.read_text(encoding="utf-8"))
    mismatches: list[str] = []

    for src, r in results.items():
        committed_src = committed.get("per_source", {}).get(src, {})
        checks = [
            ("small_poll_tokens", r.small_poll_tokens),
            ("terminal_poll_tokens", r.terminal_poll_tokens),
            ("derived_default_tokens", r.derived_default_tokens),
            ("small_poll_text_sha256", r.small_poll_text_sha256),
            ("terminal_poll_text_sha256", r.terminal_poll_text_sha256),
        ]
        for field, expected in checks:
            actual = committed_src.get(field)
            if actual != expected:
                mismatches.append(f"  {src}.{field}: committed={actual!r} vs derived={expected!r}")

    if mismatches:
        sys.stderr.write("FAIL: committed output does not match current derivation:\n")
        for m in mismatches:
            sys.stderr.write(m + "\n")
        sys.stderr.write(f"\nRun `python scripts/derive_poll_costs.py --output {committed_path}` to refresh.\n")
        return 1

    print(f"OK: {committed_path} matches current derivation.")
    return 0


# ---------------------------------------------------------------------------
# --against mode
# ---------------------------------------------------------------------------

# Matches assignments like:
#   DEFAULT_POLL_COST_GITHUB = 37
#   DEFAULT_POLL_COST_PYTEST  = 15
_CONST_RE = re.compile(
    r"\bDEFAULT_POLL_COST_(GITHUB|PYTEST|DOCKER|FS)\b[^=]*=\s*(\d+)",
    re.IGNORECASE,
)

_SOURCE_KEY_MAP = {
    "GITHUB": "github",
    "PYTEST": "pytest",
    "DOCKER": "docker",
    "FS": "fs",
}


def _against(stats_path: Path, results: dict[str, SourceResult]) -> int:
    """Parse DEFAULT_POLL_COST_* constants from stats_path and verify them."""
    if not stats_path.exists():
        sys.stderr.write(f"ERROR: {stats_path} not found\n")
        return 1

    text = stats_path.read_text(encoding="utf-8")
    found: dict[str, int] = {}
    for m in _CONST_RE.finditer(text):
        src_key = _SOURCE_KEY_MAP[m.group(1).upper()]
        found[src_key] = int(m.group(2))

    expected_sources = set(_SOURCE_KEY_MAP.values())
    missing = expected_sources - found.keys()
    if missing:
        sys.stderr.write(
            f"ERROR: {stats_path} is missing DEFAULT_POLL_COST_* constants for: " + ", ".join(sorted(missing)) + "\n"
        )
        sys.stderr.write(
            "Add the missing constants and set each to the derived default from "
            "`python scripts/derive_poll_costs.py`.\n"
        )
        return 1

    mismatches: list[str] = []
    for src, code_val in found.items():
        derived = results[src].derived_default_tokens
        if code_val != derived:
            mismatches.append(f"  {src}: stats.py has {code_val}, derived default is {derived}")

    if mismatches:
        sys.stderr.write(f"FAIL: constants in {stats_path} do not match derived defaults:\n")
        for msg in mismatches:
            sys.stderr.write(msg + "\n")
        sys.stderr.write(
            "\nUpdate the constants to match the derived values, or re-run "
            "`python scripts/derive_poll_costs.py` to refresh the derivation.\n"
        )
        return 1

    print(f"OK: all DEFAULT_POLL_COST_* constants in {stats_path} match derived defaults.")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Compute and optionally write / verify poll-cost defaults."""
    ap = argparse.ArgumentParser(
        description="Derive per-source token-cost defaults for waitbus polling sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Write derivation JSON to PATH. "
            f"Defaults to {_OUTPUT_PATH.relative_to(_OUTPUT_PATH.parent.parent)} "
            "when omitted (print only)."
        ),
    )
    ap.add_argument(
        "--write-default",
        action="store_true",
        help=f"Write to the canonical output path ({_OUTPUT_PATH}).",
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help=("Re-derive and assert the committed JSON at the canonical path matches. Exits 1 if any value differs."),
    )
    ap.add_argument(
        "--against",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Parse DEFAULT_POLL_COST_{{GITHUB,PYTEST,DOCKER,FS}} constants "
            "from PATH and confirm each matches the derived default. "
            "Exits 1 with a remediation message if any constant diverges."
        ),
    )
    args = ap.parse_args(argv)

    results = derive_all()
    _print_results(results)

    if args.check:
        return _check(_OUTPUT_PATH, results)

    if args.against is not None:
        return _against(args.against, results)

    output_path: Path | None = None
    if args.write_default:
        output_path = _OUTPUT_PATH
    elif args.output is not None:
        output_path = args.output

    if output_path is not None:
        data = _to_json_dict(results)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
