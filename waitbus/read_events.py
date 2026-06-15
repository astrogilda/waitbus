#!/usr/bin/env python3
"""Query CLI for waitbus: reports GitHub Actions CI state from SQLite.

Two modes:

- Default (query): read the latest N events from the local cache and
  print them as text or JSON. Used for one-shot questions like
  ``did CI go green?`` from inside a Claude session.
- ``--watch``: connect to the broadcast daemon's AF_UNIX SOCK_STREAM
  socket and stream matching events live. Each event prints as one
  ``stdout`` line so the ``Monitor`` skill primitive can treat the
  invocation as a wake stream. The subscriber's resume cursor is kept
  in the per-user state directory (resolved via platformdirs) and
  updated atomically after every consumed frame; reconnects pick up
  where the previous run left off, so a daemon restart does not replay
  a window of past events.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from . import _db
from ._broadcast_sub import (
    BookmarkCursor,
    BroadcastConnectionError,
    _emit_predicate,
    await_predicate,
    open_subscriber,
)
from ._cloudevents import rfc3339_from_epoch_ns
from ._paths import db_path, ensure_state_dirs
from ._secrets import SecretNotConfigured

_GIT_REMOTE_TIMEOUT_SEC: Final[int] = 5
"""Timeout in seconds for the ``git remote get-url origin`` subprocess call.

The call reads a locally-cached config; it should complete in milliseconds.
The timeout exists only to bound the ``detect_repo`` fast-path when the
git process hangs (e.g. a network-mounted repo with a slow fuse layer).
"""

_MAX_BODY_DISPLAY_CHARS: Final[int] = 160
"""Maximum displayed length (in characters) for an agent message body.

Bodies exceeding this length are truncated to ``_BODY_TRUNCATE_PREFIX_CHARS``
characters followed by ``"..."``, keeping the one-line summary compact.
"""

_BODY_TRUNCATE_PREFIX_CHARS: Final[int] = _MAX_BODY_DISPLAY_CHARS - 3
"""Characters to keep before the ``"..."`` truncation suffix.

Derived from ``_MAX_BODY_DISPLAY_CHARS - 3`` so the truncated string
(prefix + ``"..."``) is exactly ``_MAX_BODY_DISPLAY_CHARS`` characters long.
"""

REMOTE_RE = re.compile(
    r"^(?:git@github\.com:|https?://(?:[^@/]+@)?github\.com/)(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"
)


def detect_repo() -> tuple[str, str] | None:
    """Infer ``(owner, repo)`` from the current directory's git origin.

    Returns ``None`` when the cwd is not a git repo, ``git`` is not on
    PATH, or the origin URL is not a github.com remote. The caller
    treats ``None`` as "operator must pass --owner/--repo explicitly".
    Used so ``waitbus`` invoked inside a repo just works without
    flag plumbing.
    """
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


def fetch(owner: str, repo: str, event_type: str, limit: int) -> list[sqlite3.Row]:
    """Read the latest ``limit`` events for ``(owner, repo, event_type)``.

    Rows are returned newest-first. Returns an empty list when the
    events DB does not exist yet (operator hasn't run ``waitbus
    init``) — the CLI surface treats this as a "no events cached"
    state rather than an error.
    """
    if not db_path().exists():
        return []
    with _db.connect(db_path(), readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT delivery_id, owner, repo, run_id, workflow_name,
                   head_branch, head_sha, status, conclusion,
                   received_at, ingest_method, payload_json,
                   job_id, job_name, parent_run_id, event_type,
                   alert_name, alert_severity, alert_fingerprint,
                   msg_to, msg_from, msg_body
            FROM events
            WHERE owner=? AND repo=? AND event_type=?
            ORDER BY received_at DESC
            LIMIT ?
            """,
            (owner, repo, event_type, limit),
        )
        return list(cur.fetchall())


def fetch_jobs_for_run(owner: str, repo: str, run_id: int, *, limit: int = 1000) -> list[sqlite3.Row]:
    """Return the latest row per job_id for the given run, capped at ``limit``.

    Uses ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY received_at DESC)
    to keep only the most recent emission per job. Dedup happens in SQL so
    the result set never materialises duplicates in Python. The explicit
    LIMIT bounds worst-case allocation for runs with many re-runs or
    redeliveries.
    """
    if not db_path().exists() or run_id is None:
        return []
    with _db.connect(db_path(), readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    job_id, job_name, status, conclusion, received_at, ingest_method,
                    ROW_NUMBER() OVER (
                        PARTITION BY job_id
                        ORDER BY received_at DESC
                    ) AS rn
                FROM events
                WHERE event_type = 'workflow_job'
                  AND owner = ?
                  AND repo = ?
                  AND parent_run_id = ?
            )
            SELECT job_id, job_name, status, conclusion, received_at, ingest_method
            FROM ranked
            WHERE rn = 1
            ORDER BY job_name, job_id
            LIMIT ?
            """,
            (owner, repo, run_id, limit),
        ).fetchall()
        return rows


def _iso(ts: int) -> str:
    """Render a unix epoch nanosecond timestamp as a UTC RFC3339 ``...Z`` string.

    Delegates to the single renderer, :func:`_cloudevents.rfc3339_from_epoch_ns`,
    so the broadcast human summary and the CloudEvents ``time`` attribute
    share one implementation (microsecond precision, trailing ``Z``).
    """
    return rfc3339_from_epoch_ns(ts)


def _format_agent_message(row: sqlite3.Row, present: dict[str, Any]) -> str:
    """One-line summary for an agent addressing message: ``from -> to: body``.

    Collapses ``msg_body`` to a single wire line and bounds its length so a
    large body cannot bloat the frame summary.
    """
    frm = present.get("msg_from") or "?"
    to = present.get("msg_to") or "?"
    body = " ".join((present.get("msg_body") or "").split())
    if len(body) > _MAX_BODY_DISPLAY_CHARS:
        body = body[:_BODY_TRUNCATE_PREFIX_CHARS] + "..."
    return f"{frm} -> {to}: {body} at {_iso(row['received_at'])} [src={row['ingest_method']}]"


def _format_alert(row: sqlite3.Row, present: dict[str, Any]) -> str:
    """One-line summary for an Alertmanager event: ``name [severity] (fingerprint=...)``."""
    sev = present.get("alert_severity") or "?"
    fp = present.get("alert_fingerprint") or "?"
    return (
        f"{present['alert_name']} [{sev}] (fingerprint={fp}) at {_iso(row['received_at'])} [src={row['ingest_method']}]"
    )


def _format_workflow_job(row: sqlite3.Row, payload: dict[str, Any]) -> str:
    """One-line summary for a workflow_job event: ``branch job 'name' -- status/conclusion``."""
    job: dict[str, Any] = payload.get("workflow_job") or {}
    branch = row["head_branch"] or job.get("head_branch") or "?"
    jname = row["job_name"] or "?"
    status = row["status"] or "?"
    conc = row["conclusion"] or "pending"
    return (
        f"{branch} job '{jname}' (job_id={row['job_id']}, run={row['parent_run_id']}) "
        f"-- {status}/{conc} at {_iso(row['received_at'])} [src={row['ingest_method']}]"
    )


def _run_title(run: dict[str, Any]) -> str:
    """First line of a workflow_run's display title (or commit message), or ``""``."""
    raw = run.get("display_title") or run.get("head_commit", {}).get("message") or ""
    lines = raw.splitlines()
    return lines[0] if lines else ""


def _format_workflow_run(row: sqlite3.Row, payload: dict[str, Any]) -> str:
    """One-line summary for a workflow_run event (the default facet)."""
    run: dict[str, Any] = payload.get("workflow_run") or {}
    event = run.get("event") or "?"
    title_s = _run_title(run)
    branch = row["head_branch"] or "?"
    wf = row["workflow_name"] or "?"
    conc = row["conclusion"] or "pending"
    status = row["status"] or "?"
    return (
        f"{branch} {wf} run {row['run_id']} ({event}"
        f"{', ' + title_s if title_s else ''}) -- {status}/{conc} at {_iso(row['received_at'])}"
        f" [src={row['ingest_method']}]"
    )


def format_text(row: sqlite3.Row) -> str:
    """Build the one-line human summary the broadcast daemon ships in frames.

    A thin facet dispatcher over the per-facet renderers above: an agent
    addressing message renders ``from -> to: body``; an Alertmanager event
    renders ``name [severity] (fingerprint=...)``; a workflow_job renders
    ``branch job 'name' -- status/conclusion``; everything else is a
    workflow_run. All renderers end with the ``ingest_method`` tag so
    operators can tell webhook deliveries from etag-poll inferences at a
    glance, and use field-presence guards so they are safe on the narrow
    CLI-query rows as well as the full broadcast rows. Imported lazily by
    ``broadcast._summary_for``.
    """
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except json.JSONDecodeError:
        payload = {}
    present = {k: row[k] for k in row.keys()}  # noqa: SIM118 - sqlite3.Row needs .keys(); iterating yields values
    etype = row["event_type"]
    if etype == "agent_message" or present.get("msg_body") is not None:
        return _format_agent_message(row, present)
    if present.get("alert_name") is not None:
        return _format_alert(row, present)
    if etype == "workflow_job":
        return _format_workflow_job(row, payload)
    return _format_workflow_run(row, payload)


def format_job_line(owner: str, repo: str, job: sqlite3.Row) -> str:
    """Indented child-job line printed under a parent run when ``--include-jobs``.

    Mirrors the layout of ``format_text`` but uses a 4-space indent so
    the output stream reads as ``run / job / job / job`` blocks for
    each parent workflow.
    """
    status = job["status"] or "?"
    conc = job["conclusion"] or "pending"
    return (
        f"    job '{job['job_name'] or '?'}' (id={job['job_id']}) -- {status}/{conc} "
        f"at {_iso(job['received_at'])} [src={job['ingest_method']}]"
    )


def watch_bookmark_name(owner: str, repo: str) -> str:
    """Derive the unified ``BookmarkCursor`` name for a ``--watch`` slug.

    Replaces the deleted bespoke ``(owner, repo)``-keyed cursor file
    model: ``--watch`` now resumes through the same ``BookmarkCursor``
    every other subscriber uses. GitHub owner/repo characters
    (``[A-Za-z0-9._-]``) are a subset of the bookmark grammar
    (``^[A-Za-z0-9_.-]+$``), so the ``watch-<owner>-<repo>`` name is
    always valid without sanitisation.
    """
    return f"watch-{owner}-{repo}"


def watch(
    filters: list[str],
    event_types: list[str] | None,
    since: str | None,
    cursor: BookmarkCursor | None,
    socket_path: Path | None = None,
) -> int:
    """Stream matching events as one stdout line per frame.

    A thin adapter over the shared ``await_predicate`` engine: the
    daemon connection, the self-enforced read loop, heartbeat skipping,
    and cursor advance are engine-owned. The predicate prints one
    summary line per non-heartbeat frame and always returns
    ``CONTINUE`` (``--watch`` never "matches" -- it streams until EOF or
    SIGINT). No deadline (``deadline_seconds=None``): a watch stream is
    unbounded by design.

    Return codes (the documented ``Monitor``-stream contract):

    * ``0`` -- clean EOF (daemon shut down / closed the subscriber) or
      ``SIGINT``, so the ``Monitor`` skill primitive can re-arm without
      a non-zero exit polluting the conversation.
    * ``1`` -- the daemon violated the wire framing mid-stream.
    * ``2`` -- the broadcast socket is absent / refused, or a token is
      required but not configured.

    Two documented behaviour deltas vs the deleted hand-rolled loop:

    * ``--watch`` now flows the standard ``open_subscriber`` token path
      (a configured broadcast token is sent on the subscribe frame).
    * Resume is the unified ``BookmarkCursor``; the bespoke
      ``(owner, repo)``-keyed cursor-file model is deleted.
    """
    try:
        sub = open_subscriber(
            filters=filters,
            event_types=event_types,
            since=since,
            socket_path=str(socket_path) if socket_path is not None else None,
        )
    except BroadcastConnectionError as exc:
        sys.stderr.write(f"waitbus --watch: {exc}. {exc.remediation}\n")
        return 2
    except SecretNotConfigured as exc:
        sys.stderr.write(f"waitbus --watch: {exc}\n")
        return 2

    def _emit_summary(frame: dict[str, Any]) -> None:
        print(frame.get("summary") or _frame_fallback_summary(frame), flush=True)

    try:
        outcome = await_predicate(
            sub,
            decide=_emit_predicate(_emit_summary),
            deadline_seconds=None,
            cursor=cursor,
        )
    finally:
        sub.sock.close()

    if outcome.framing_error:
        sys.stderr.write("waitbus --watch: framing error (daemon broke the wire protocol)\n")
        return 1
    # Clean EOF, SIGINT (cancelled), or peer close all re-arm cleanly.
    return 0


def _frame_fallback_summary(frame: dict[str, Any]) -> str:
    """Last-resort line when the daemon's summary field is missing."""
    label = frame.get("event_type") or frame.get("kind", "?")
    return f"{frame.get('owner', '?')}/{frame.get('repo', '?')} {label} event_id={frame.get('event_id', '?')}"


@dataclass(frozen=True)
class _WatchMode:
    """Watch-mode parameters resolved from argparse for the main dispatcher.

    ``cursor`` is the unified ``BookmarkCursor`` resume handle (or
    ``None`` for ``--all-events`` without an explicit owner/repo, where
    no per-repo resume key exists). It replaces the deleted
    ``(owner, repo)``-keyed cursor-file model.
    """

    filters: list[str]
    event_types: list[str] | None
    since: str | None
    cursor: BookmarkCursor | None


@dataclass(frozen=True)
class _QueryMode:
    """Query-mode parameters resolved from argparse for the main dispatcher."""

    owner: str
    repo: str
    event_type: str
    limit: int
    include_jobs: bool
    as_json: bool


def _build_parser() -> argparse.ArgumentParser:
    """Construct the argparse spec for the read-events sub-command."""
    p = argparse.ArgumentParser(
        prog="read_events",
        description="Report GitHub Actions CI status from local event cache.",
    )
    p.add_argument("--owner")
    p.add_argument("--repo")
    p.add_argument("--event-type", default="workflow_run")
    p.add_argument(
        "--include-jobs",
        action="store_true",
        help="For each workflow_run row, also show the latest state of every child workflow_job",
    )
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--latest", action="store_true", help="Single latest event (default)")
    grp.add_argument("--last-n", type=int, metavar="N")
    grp.add_argument(
        "--watch",
        action="store_true",
        help="Subscribe to the broadcast daemon and stream matching events live.",
    )
    p.add_argument(
        "--all-events",
        action="store_true",
        help="With --watch, subscribe with filters=['*'] instead of the current repo's slug.",
    )
    p.add_argument(
        "--since",
        help="With --watch, resume from this ULID. Default: cursor cache, or no replay on first connect.",
    )
    fmt = p.add_mutually_exclusive_group()
    fmt.add_argument("--text", action="store_true")
    fmt.add_argument("--json", action="store_true")
    return p


def _resolve_mode(args: argparse.Namespace) -> _WatchMode | _QueryMode | None:
    """Resolve argparse Namespace to a typed mode for dispatch.

    Returns None when --watch=False and the owner/repo pair cannot be
    resolved from explicit args or detect_repo(); the caller exits 0
    after emitting a remediation hint to stderr.
    """
    owner, repo = args.owner, args.repo
    if args.watch and args.all_events:
        # --all-events has no per-repo resume key unless owner/repo were
        # given explicitly; without them there is nothing to bookmark.
        resume_owner_repo: tuple[str, str] | None = (owner, repo) if owner and repo else None
    else:
        if not (owner and repo):
            detected = detect_repo()
            if detected is None:
                sys.stderr.write("waitbus: pass --owner/--repo or run inside a git repo with a github.com origin.\n")
                return None
            owner, repo = detected
        resume_owner_repo = (owner, repo)

    if args.watch:
        filters = ["*"] if args.all_events else [f"{owner}/{repo}"]
        # event_types default = all supported. --event-type lets the
        # operator restrict; the query-mode default value 'workflow_run'
        # is intentionally NOT applied as a watch-mode filter.
        event_types: list[str] | None = None
        if args.event_type and args.event_type != "workflow_run":
            event_types = [args.event_type]
        # Unified BookmarkCursor resume (the deleted bespoke
        # (owner,repo) cursor-file model is gone). Explicit --since
        # always wins over the stored cursor.
        cursor: BookmarkCursor | None = None
        if resume_owner_repo is not None:
            cursor = BookmarkCursor(watch_bookmark_name(*resume_owner_repo))
        since = args.since
        if since is None and cursor is not None:
            since = cursor.load()
        return _WatchMode(
            filters=filters,
            event_types=event_types,
            since=since,
            cursor=cursor,
        )

    # owner and repo are non-None here: the not-watch branch above either
    # bound both from args or from detect_repo(), or returned None on the
    # detect-failed path. The assert pins this invariant for the type
    # checker AND surfaces any future refactor that introduces a fall-
    # through path with unbound owner/repo as a loud test failure.
    assert owner is not None and repo is not None
    return _QueryMode(
        owner=owner,
        repo=repo,
        event_type=args.event_type,
        limit=args.last_n if args.last_n else 1,
        include_jobs=args.include_jobs,
        as_json=bool(args.json),
    )


def _emit_query(mode: _QueryMode) -> int:
    """Execute the query branch: fetch + emit as JSON or text."""
    rows = fetch(mode.owner, mode.repo, mode.event_type, mode.limit)

    if mode.as_json:
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            if mode.include_jobs and r["event_type"] == "workflow_run" and r["run_id"] is not None:
                d["jobs"] = [dict(j) for j in fetch_jobs_for_run(mode.owner, mode.repo, r["run_id"])]
            out.append(d)
        print(json.dumps(out, indent=2, default=str))
        return 0

    if not rows:
        print(f"waitbus: no {mode.event_type} events cached for {mode.owner}/{mode.repo}.")
        print("Hint: register a webhook for this repo (see the project README) or add the")
        print("repo to your watched-repos file (path resolved via $WAITBUS_STATE_DIR or platformdirs).")
        return 0

    for row in rows:
        print(f"{mode.owner}/{mode.repo}: {format_text(row)}")
        if mode.include_jobs and row["event_type"] == "workflow_run" and row["run_id"] is not None:
            for job in fetch_jobs_for_run(mode.owner, mode.repo, row["run_id"]):
                print(format_job_line(mode.owner, mode.repo, job))
    return 0


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``read_events`` sub-command.

    Two modes selected by ``--watch``: query mode prints the latest
    N events (default 1) and exits; watch mode connects to the
    broadcast daemon and streams matching events one per line until
    EOF, SIGINT, or daemon shutdown. Exit code 0 in all clean-exit
    paths so the ``Monitor`` skill primitive can rearm without seeing
    a non-zero status pollute the conversation.
    """
    ensure_state_dirs()
    args = _build_parser().parse_args(argv)
    mode = _resolve_mode(args)
    if mode is None:
        return 0
    if isinstance(mode, _WatchMode):
        return watch(
            filters=mode.filters,
            event_types=mode.event_types,
            since=mode.since,
            cursor=mode.cursor,
        )
    return _emit_query(mode)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except sqlite3.Error as exc:
        sys.stderr.write(f"waitbus read_events: sqlite error: {exc}\n")
        sys.exit(1)
