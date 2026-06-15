#!/usr/bin/env python3
"""Per-file coverage threshold gate.

Reads a coverage.json file produced by::

    pytest --cov=waitbus --cov-report=json

and fails (exit 1) if any file's statement coverage is below the threshold.
Unlike pytest-cov's ``--fail-under`` which checks aggregate coverage, this
script enforces the requirement independently for every source file.

Usage::

    python scripts/coverage_per_file.py coverage.json
    python scripts/coverage_per_file.py coverage.json --threshold 75
    python scripts/coverage_per_file.py coverage.json --exclude waitbus/replay.py

Exit codes:
    0  All files meet the threshold.
    1  One or more files are below the threshold.
    2  Input file missing or malformed.

Justified exclusions
--------------------
Files listed below may be passed to ``--exclude`` in CI with the following
rationale.  Silent exclusions (not listed here) are not permitted — every
excluded file must appear in this table and in the CI step comment.

    waitbus/_peercred.py
        Platform-gated: macOS branch (getpeereid via ctypes) runs only on
        darwin. Linux CI cannot execute those code paths. The macOS matrix
        cell covers the darwin branch; a combined profile would reach ~100%.
        Baseline: 53.8% on Linux.

    waitbus/cli/_shared.py
        Shared CLI helpers (install/orphan-prune/health-check families).
        137 uncovered lines of argument-dispatch and error-formatting
        branches that require subprocess-level invocation (typer
        standalone_mode). Existing tests cover the happy path; a dedicated
        CLI integration test pass is tracked in TODO.md.
        Baseline: 56.4%.

    waitbus/cli/status.py
        `status` command: 19 uncovered lines across the daemon-liveness
        and socket-presence fault-isolation arms; exercising them needs a
        live daemon + socket fixture. Tracked in TODO.md.
        Baseline: 59.5%.

    waitbus/cli/install/launchd.py
        macOS-only install path: 37 uncovered lines that only execute on
        darwin. The macOS CI matrix cell covers the launchd branch; a
        combined Linux+macOS profile would reach ~100%.
        Baseline: 15.6% on Linux.

    waitbus/cli/daemons/broadcast.py
    waitbus/cli/daemons/mcp.py
    waitbus/cli/daemons/read_events.py
    waitbus/cli/replay.py
        Thin typer sub-app shims that delegate to the daemon entry points.
        The uncovered lines are the daemon-launch branches, which require
        subprocess-level invocation to exercise. Tracked in TODO.md.
        Baseline: 58-71%.

    waitbus/mcp.py
        MCP server integration: 41 uncovered lines in the reconnect loop and
        session tear-down paths, which require an in-process MCP server/client
        pair. The existing SDK-based tests cover the initialization and channel-
        notification paths. Remaining gaps tracked in TODO.md.
        Baseline: 66.9%.

    waitbus/pr_monitor.py
        GitHub API client: 86 uncovered lines in the polling loop, rate-limit
        back-off, and error-recovery paths. These require network mocking at
        the httpx/aiohttp level. Coverage tracked in TODO.md.
        Baseline: 32.8%.

    waitbus/read_events.py
        Subscriber command: 106 uncovered lines requiring a live broadcast
        daemon (or deeper patching of the asyncio socket pair). Tracked in TODO.md.
        Baseline: 52.0%.

    waitbus/broadcast_tap.py
        Tap command: 11 uncovered lines in ConnectionError, UnicodeDecodeError,
        and KeyboardInterrupt paths inside the receive loop. These require
        daemon-level socket manipulation in tests. Tracked in TODO.md.
        Baseline: 72.5%. Ratchet target: +5%/cycle toward 80%.

    waitbus/replay.py
        Replay command: 19 uncovered lines in ConnectionError, UnicodeDecodeError,
        and timeout paths. Requires live daemon patching. Tracked in TODO.md.
        Baseline: 74.7%. Ratchet target: +5%/cycle toward 80%.

    waitbus/_protocols.py
        Structural Protocol module. All 6 statements are covered (100%), but
        branch coverage counts the unreachable enter/exit branches of
        ``def x(): ...`` Protocol stub methods, dragging the percentage to
        70.0%. The measurement is an artifact of branch=true on Protocol
        bodies, not real undertest. Excluded permanently or until coverage.py
        grows a per-line ``# pragma: no branch`` annotation suitable for
        Protocol stubs.

Add new entries in the format::

    # <path>: <one-line rationale>
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import sys
from pathlib import Path
from typing import Any


def _load_coverage(path: Path) -> dict[str, Any]:
    """Load and minimally validate coverage.json.

    Args:
        path: Path to the coverage JSON file.

    Returns:
        Parsed JSON dictionary.

    Raises:
        SystemExit: On missing file or JSON parse error (exit code 2).
    """
    if not path.exists():
        print(f"ERROR: coverage file not found: {path}", file=sys.stderr)
        sys.exit(2)
    try:
        with path.open() as fh:
            data: dict[str, Any] = json.load(fh)
    except json.JSONDecodeError as exc:
        print(f"ERROR: malformed JSON in {path}: {exc}", file=sys.stderr)
        sys.exit(2)
    if "files" not in data:
        print(f"ERROR: 'files' key missing in {path}", file=sys.stderr)
        sys.exit(2)
    return data


def _is_excluded(filepath: str, patterns: list[str]) -> bool:
    """Return True if filepath matches any exclusion pattern.

    Patterns are matched against the bare filepath as it appears in coverage.json
    (typically a path relative to the project root or an absolute path).
    Both exact string equality and fnmatch glob patterns are supported.

    Args:
        filepath: The file path string from coverage.json.
        patterns: List of exclusion patterns supplied via ``--exclude``.

    Returns:
        True if any pattern matches.
    """
    return any(fnmatch.fnmatch(filepath, pat) or filepath.endswith(pat) for pat in patterns)


def _run(
    data: dict[str, Any],
    threshold: float,
    excludes: list[str],
) -> int:
    """Evaluate per-file coverage and print a results table.

    Args:
        data: Parsed coverage.json dictionary.
        threshold: Minimum acceptable coverage percentage (0-100).
        excludes: File path patterns to skip.

    Returns:
        Exit code: 0 if all files pass, 1 if any fail.
    """
    files: dict[str, Any] = data["files"]

    rows: list[tuple[str, float, int, bool, bool]] = []
    # (filepath, pct, uncovered_count, excluded, passing)

    for filepath, file_data in sorted(files.items()):
        summary = file_data.get("summary", {})
        pct: float = summary.get("percent_covered", 0.0)
        missing: list[int] = file_data.get("missing_lines", [])
        excluded = _is_excluded(filepath, excludes)
        passing = excluded or pct >= threshold
        rows.append((filepath, pct, len(missing), excluded, passing))

    # Determine column widths
    max_path = max((len(r[0]) for r in rows), default=4)
    col_path = max(max_path, 4)

    header = f"{'STATUS':<6}  {'COVERAGE':>8}  {'UNCOV':>5}  {'FILE':<{col_path}}"
    separator = "-" * len(header)

    print(f"\nPer-file coverage gate  (threshold: {threshold:.1f}%)\n")
    print(header)
    print(separator)

    failing: list[str] = []
    for filepath, pct, uncov, excluded, passing in rows:
        if excluded:
            status = "SKIP"
        elif passing:
            status = "PASS"
        else:
            status = "FAIL"
            failing.append(filepath)
        excl_tag = "  [excluded]" if excluded else ""
        print(f"{status:<6}  {pct:>7.1f}%  {uncov:>5}  {filepath:<{col_path}}{excl_tag}")

    print(separator)

    total = len(rows)
    skipped = sum(1 for _, _, _, exc, _ in rows if exc)
    checked = total - skipped
    passed = sum(1 for _, _, _, exc, ok in rows if not exc and ok)
    n_failing = len(failing)

    print(f"\n{checked} files checked ({skipped} skipped): {passed} PASS, {n_failing} FAIL\n")

    if failing:
        print(f"Files below {threshold:.1f}%:")
        for fp in failing:
            print(f"  {fp}")
        print()
        return 1

    return 0


def main() -> int:
    """CLI entry point.

    Returns:
        Exit code (0 = all pass, 1 = failures, 2 = input error).
    """
    parser = argparse.ArgumentParser(
        description="Per-file coverage threshold gate.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "coverage_json",
        metavar="COVERAGE_JSON",
        type=Path,
        help="Path to coverage.json produced by pytest --cov-report=json.",
    )
    parser.add_argument(
        "--threshold",
        metavar="N",
        type=float,
        default=80.0,
        help="Minimum coverage percentage per file (default: 80).",
    )
    parser.add_argument(
        "--exclude",
        metavar="PATTERN",
        action="append",
        default=[],
        dest="excludes",
        help=(
            "Exclude files matching PATTERN from the gate. "
            "Repeatable. Every exclusion must be justified in the "
            "JUSTIFIED_EXCLUSIONS table inside this script."
        ),
    )

    args = parser.parse_args()

    data = _load_coverage(args.coverage_json)
    return _run(data, args.threshold, args.excludes)


if __name__ == "__main__":
    sys.exit(main())
