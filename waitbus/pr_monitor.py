"""Push-driven PR CI state monitor.

Subscribes to the broadcast daemon's AF_UNIX SOCK_STREAM socket for
`workflow_job` events on a single repo and rolls them up into per-PR
state via the canonical `AGG_SQL` window-function query. Emits one
line per state-hash transition; exits when every watched PR has
reached a non-PENDING terminal state.

This is the SESSION-LEVEL subscriber shape: one invocation = one
consumer. Multi-session fan-out is the broadcast daemon's job — the
daemon delivers wake events to N concurrent invocations of this CLI.

`AGG_SQL` implements the canonical per-PR rollup: an earlier prototype
used `inotifywait` against the SQLite event store as the wake source;
this rewrite swaps that for the daemon's subscribe protocol while
preserving the rollup semantics — the AGG_SQL is the canonical
"PR rolled up to ALL_GREEN / FAIL / PENDING" definition the daemon
never owns.

Usage:
    python -m waitbus.pr_monitor --pr 7 --pr 9
    python -m waitbus.pr_monitor --owner foo --repo bar --pr 1
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, MutableMapping
from typing import Final

from . import _db
from ._broadcast_sub import (
    BroadcastConnectionError,
    SubscriberHandle,
    await_predicate,
    open_subscriber,
)
from ._paths import db_path, ensure_state_dirs
from ._terminal import FAILURE_CONCLUSIONS, SUCCESS_CONCLUSION

REMOTE_RE = re.compile(r"^(?:https?://github\.com/|git@github\.com:)(?P<owner>[^/]+)/(?P<repo>[^/.]+?)(?:\.git)?/?$")
DEFAULT_SAFETY_NET_SECONDS = 60
DEFAULT_SHA_REFRESH_SECONDS = 300

_GIT_REMOTE_TIMEOUT_SEC: Final[int] = 5
"""Timeout in seconds for the ``git remote get-url origin`` subprocess call.

Mirrors ``read_events._GIT_REMOTE_TIMEOUT_SEC``; kept as a module-local
constant so ``pr_monitor`` has no import dependency on ``read_events``.
The call reads a locally-cached config and completes in milliseconds
under normal conditions.
"""

_GH_PR_VIEW_TIMEOUT_SEC: Final[int] = 10
"""Timeout in seconds for ``gh pr view`` subprocess calls.

``gh pr view`` makes a live GitHub API request; 10 s gives enough margin
for a slow edge node or a brief rate-limit pause while still bounding the
PR-monitor's per-tick latency to a predictable ceiling.
"""

# The canonical GitHub `conclusion` bucketing (SUCCESS_CONCLUSION /
# FAILURE_CONCLUSIONS / NON_TERMINAL_CONCLUSIONS) is the single source
# of truth shared by `AGG_SQL` below, `waitbus.wait`, the
# coalesced replay delivery mode, and any future terminal-state
# consumer. The definitions live in `waitbus._terminal`; the
# SQL below interpolates the values, so the AGG_SQL text is unchanged
# (sorted enum literals, injection-safe by construction -- every
# element is a hardcoded enum string, never operator input).


def _sql_in_list(values: frozenset[str]) -> str:
    """Render a frozenset of known-safe enum literals as a SQL ``IN`` body.

    Sorted for deterministic SQL text (stable across runs / test snapshots).
    Injection-safe by construction: callers pass only the module-level
    conclusion frozensets, never operator input.
    """
    return ", ".join(f"'{v}'" for v in sorted(values))


_FAIL_IN = _sql_in_list(FAILURE_CONCLUSIONS)
_OK = SUCCESS_CONCLUSION
# Reusable scalar fragments keep the assembled SQL within the line budget
# while still deriving every literal from the canonical frozensets above.
_FAIL_COUNT = f"SUM(CASE WHEN conclusion IN ({_FAIL_IN}) THEN 1 ELSE 0 END)"
_OK_COUNT = f"SUM(CASE WHEN status='completed' AND conclusion='{_OK}' THEN 1 ELSE 0 END)"

AGG_SQL = f"""
WITH latest AS (
    SELECT job_id, status, conclusion,
           ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY received_at DESC) AS rn
    FROM events
    WHERE event_type = 'workflow_job'
      AND head_sha = ?
      AND owner = ?
      AND repo = ?
)
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 'NO_JOBS'
        WHEN {_FAIL_COUNT} > 0 THEN 'FAIL'
        WHEN {_OK_COUNT} = COUNT(*) THEN 'ALL_GREEN'
        ELSE 'PENDING'
    END,
    COUNT(*),
    {_OK_COUNT},
    {_FAIL_COUNT}
FROM latest WHERE rn = 1;
"""


def detect_repo() -> tuple[str, str] | None:
    """Mirror read_events.detect_repo so callers in a git checkout can omit --owner/--repo."""
    try:
        r = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            check=True,
            capture_output=True,
            text=True,
            timeout=_GIT_REMOTE_TIMEOUT_SEC,
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    m = REMOTE_RE.match(r.stdout.strip())
    if not m:
        return None
    return m.group("owner"), m.group("repo")


def head_sha(pr: int) -> str | None:
    """Fetch the current head_sha for `pr` via gh; None on any failure."""
    try:
        out = subprocess.check_output(
            ["gh", "pr", "view", str(pr), "--json", "headRefOid", "-q", ".headRefOid"],
            text=True,
            timeout=_GH_PR_VIEW_TIMEOUT_SEC,
        )
        return out.strip() or None
    except Exception:
        return None


def aggregate(conn: sqlite3.Connection, owner: str, repo: str, sha: str) -> tuple[str, int, int, int]:
    """Run AGG_SQL for one PR's head_sha; return (state, n, passed, failed)."""
    row = conn.execute(AGG_SQL, (sha, owner, repo)).fetchone()
    return (row[0] or "NO_JOBS", row[1] or 0, row[2] or 0, row[3] or 0)


def tick(
    conn: sqlite3.Connection,
    owner: str,
    repo: str,
    prs: list[int],
    pr_sha: Mapping[int, str | None],
    pr_state: MutableMapping[int, str],
    observed: MutableMapping[int, bool],
) -> bool:
    """Run one aggregation pass; return True when all PRs are terminal.

    ``pr_sha`` is read-only (Mapping); ``pr_state`` and ``observed`` are
    accumulator state mutated in place (MutableMapping).
    """
    any_pending = False
    all_observed = True
    for pr in prs:
        sha = pr_sha.get(pr)
        if not sha:
            all_observed = False
            continue
        state, n, p, f = aggregate(conn, owner, repo, sha)
        key = f"{sha[:7]}|{state}|{n}|{p}|{f}"
        if state in ("PENDING", "NO_JOBS"):
            any_pending = True
        if not observed[pr] or pr_state.get(pr) != key:
            print(
                f"PR#{pr} sha={sha[:7]} {state} jobs={n} pass={p} fail={f}",
                flush=True,
            )
            pr_state[pr] = key
            observed[pr] = True
    return all_observed and not any_pending


def _pr_monitor_tick(
    conn: sqlite3.Connection,
    sub: SubscriberHandle,
    owner: str,
    repo: str,
    prs: list[int],
    pr_sha: MutableMapping[int, str | None],
    pr_state: MutableMapping[int, str],
    observed: MutableMapping[int, bool],
    sha_at: list[float],
    sha_refresh_seconds: int,
    safety_net_seconds: int,
) -> int | None:
    """Execute one iteration of the monitor loop; return exit code or None to continue.

    Blocks for up to ``safety_net_seconds`` waiting for a wake frame from the
    broadcast daemon via ``await_predicate``, refreshes head SHAs on cadence,
    then runs one aggregation pass. Returns 0 when all PRs reach a terminal
    state, 1 on a lost connection (peer_closed), or None to keep looping.

    ``sha_at`` is a one-element list used as a mutable float cell so the
    caller's timestamp is updated in place without needing a nonlocal.

    The predicate always returns ``CONTINUE`` so the safety_net deadline is
    the only exit trigger: wake frames are consumed as activity signals and
    the outer loop runs sha-refresh + aggregation after each one returns.
    """
    now = time.time()
    if now - sha_at[0] > sha_refresh_seconds:
        for pr in prs:
            pr_sha[pr] = head_sha(pr)
        sha_at[0] = now

    # No decide predicate: every non-heartbeat frame is a wake signal, so
    # await_predicate returns only on the safety-net deadline (or peer close).
    # The outer loop runs sha-refresh + aggregation after each wake-and-return.
    outcome = await_predicate(sub, deadline_seconds=safety_net_seconds)
    if outcome.peer_closed:
        if outcome.framing_error:
            sys.stderr.write("pr_monitor: broadcast connection lost; exiting\n")
            return 1
        sys.stderr.write("pr_monitor: broadcast daemon shut down; exiting\n")
        return 0

    if tick(conn, owner, repo, prs, pr_sha, pr_state, observed):
        print("MONITOR_DONE", flush=True)
        return 0
    return None


def _resolve_owner_repo(args: argparse.Namespace) -> tuple[str, str] | int:
    """Return (owner, repo) from args or auto-detect; return 2 on failure."""
    owner, repo = args.owner, args.repo
    if not (owner and repo):
        detected = detect_repo()
        if detected is None:
            sys.stderr.write("pr_monitor: pass --owner/--repo or run inside a git repo with a github.com origin.\n")
            return 2
        owner, repo = detected
    return owner, repo


def _report_subscribe_error(exc: BroadcastConnectionError) -> int:
    """Write a clean one-line error for a failed subscribe/connection and return 2.

    BroadcastConnectionError (and its version/lag reject subclasses) carries
    a remediation hint.
    """
    detail = getattr(exc, "remediation", "")
    sys.stderr.write(f"pr_monitor: {exc} {detail}".rstrip() + "\n")
    return 2


def _run_monitor_loop(
    conn: sqlite3.Connection,
    sub: SubscriberHandle,
    owner: str,
    repo: str,
    prs: list[int],
    pr_sha: MutableMapping[int, str | None],
    pr_state: MutableMapping[int, str],
    observed: MutableMapping[int, bool],
    sha_at: list[float],
    sha_refresh_seconds: int,
    safety_net_seconds: int,
) -> int:
    """Run the first-pass check then the wake-and-aggregate loop to terminal exit.

    Returns the process exit code. Extracted from ``main`` so the loop is a
    direct test seam and ``main`` stays lean under its SIGINT handler.
    """
    # First pass before waiting — if everything is already terminal we exit.
    if tick(conn, owner, repo, prs, pr_sha, pr_state, observed):
        print("MONITOR_DONE", flush=True)
        return 0
    while True:
        result = _pr_monitor_tick(
            conn,
            sub,
            owner,
            repo,
            prs,
            pr_sha,
            pr_state,
            observed,
            sha_at,
            sha_refresh_seconds,
            safety_net_seconds,
        )
        if result is not None:
            return result


def main(
    argv: list[str] | None = None,
    *,
    subscriber_factory: Callable[..., SubscriberHandle] = open_subscriber,
) -> int:
    """Entry point for the ``waitbus-pr-monitor`` console-script.

    Long-lived foreground process: subscribes to the broadcast bus
    filtered on the PR's owner/repo, aggregates per-job state from
    the SQLite cache, and prints a one-line PR summary every time
    the aggregate transitions. Exits when the daemon closes the
    subscriber socket (EOF), on SIGINT (130), or when every PR is terminal.

    ``subscriber_factory`` defaults to ``open_subscriber``; tests inject a
    fake to drive the loop without a live daemon.
    """
    ensure_state_dirs()
    p = argparse.ArgumentParser(
        prog="pr_monitor",
        description="Push-driven PR CI state monitor; uses the waitbus "
        "broadcast daemon for wakeup and the local SQLite "
        "cache for aggregation.",
    )
    p.add_argument("--owner")
    p.add_argument("--repo")
    p.add_argument(
        "--pr",
        action="append",
        type=int,
        required=True,
        help="PR number to watch; pass multiple times to watch several.",
    )
    p.add_argument(
        "--safety-net-seconds",
        type=int,
        default=DEFAULT_SAFETY_NET_SECONDS,
        help="select() timeout — also the head_sha refresh cadence floor.",
    )
    p.add_argument(
        "--sha-refresh-seconds",
        type=int,
        default=DEFAULT_SHA_REFRESH_SECONDS,
        help="Re-fetch each PR's head_sha at this cadence (detects force-pushes).",
    )
    args = p.parse_args(argv)

    resolved = _resolve_owner_repo(args)
    if isinstance(resolved, int):
        return resolved
    owner, repo = resolved

    prs: list[int] = sorted(set(args.pr))
    pr_sha: dict[int, str | None] = {pr: head_sha(pr) for pr in prs}
    pr_state: dict[int, str] = {}
    observed = {pr: False for pr in prs}
    sha_at = [time.time()]

    print(
        f"MONITOR_START owner={owner} repo={repo} prs={prs} shas={ {pr: (s or '?')[:7] for pr, s in pr_sha.items()} }",
        flush=True,
    )

    try:
        sub = subscriber_factory(
            filters=[f"{owner}/{repo}"],
            event_types=["workflow_job"],
        )
    except BroadcastConnectionError as exc:
        return _report_subscribe_error(exc)

    with _db.connect(db_path(), readonly=True) as conn:
        try:
            return _run_monitor_loop(
                conn,
                sub,
                owner,
                repo,
                prs,
                pr_sha,
                pr_state,
                observed,
                sha_at,
                args.sha_refresh_seconds,
                args.safety_net_seconds,
            )
        except BroadcastConnectionError as exc:
            # The daemon's subscribe_rejected frame (token/version/lag) is read
            # by the first await_predicate call inside the loop, not by
            # subscriber_factory — so the reject surfaces here, not above.
            return _report_subscribe_error(exc)
        except KeyboardInterrupt:
            print("MONITOR_INTERRUPTED", flush=True)
            return 130
        finally:
            with contextlib.suppress(OSError):
                sub.sock.close()


if __name__ == "__main__":
    sys.exit(main())
