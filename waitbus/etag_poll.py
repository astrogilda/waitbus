#!/usr/bin/env python3
"""ETag-aware polling fallback for waitbus.

For each `owner/repo` in the watched-repos file (path resolved via platformdirs), fetches
GET /repos/{owner}/{repo}/actions/runs?per_page=20 with If-None-Match.
304 is a no-op; 200 bodies are deduped into the SQLite store with
ingest_method='etag_poll'. One-shot: systemd timer drives cadence.

After regular polling, emits synthetic stall events for any workflow_job
that has been in_progress longer than WAITBUS_STALL_THRESHOLD_MIN
(default 60). The synthetic row uses status='stalled' and is keyed by
`etag:stall:{job_id}` so each stalled job produces exactly one event
regardless of how many polling cycles observe it. Monitors watching the
event store fire on the synthetic row, surfacing wedged jobs without
requiring an external query. Live runs that legitimately exceed the
threshold (a long-running job with a multi-hour budget) trip a single
stall event and then their normal completion event; this is intended —
the stall is a "you should be aware" signal, not an auto-action.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Final

import stamina

from . import _config, _db, _doorbell, _metrics
from ._db import insert_event
from ._log import structured
from ._paths import db_path, ensure_state_dirs, etag_state, watched_repos
from ._sdnotify import sd_notify
from ._types import NS_PER_SECOND, EventInsert

DEFAULT_STALL_THRESHOLD_MIN = 60

API = "https://api.github.com"

logger = logging.getLogger("waitbus.etag")

_GH_AUTH_TOKEN_TIMEOUT_SEC: Final[int] = 5
"""Timeout in seconds for the ``gh auth token`` subprocess call.

The call is expected to return immediately from a local credential store;
a longer timeout would only delay the fast-fail path when ``gh`` is absent
or the credential is corrupt.
"""

_GH_RETRY_ATTEMPTS: Final[int] = 3
"""Maximum number of attempts for a transient GitHub API call (initial + retries).

Passed to ``stamina.retry(attempts=...)``; the first attempt counts,
so this allows two retry cycles after an initial failure.
"""

_GH_RETRY_WAIT_MAX_SEC: Final[float] = 30.0
"""Maximum per-retry backoff ceiling in seconds for stamina's exponential wait.

Caps ``stamina.retry(wait_max=...)``; a floor of 1 s rises exponentially
toward this value, bounding the delay between attempts.
"""

_GH_URLOPEN_TIMEOUT_SEC: Final[int] = 15
"""Socket-level timeout for ``urllib.request.urlopen`` calls to the GitHub API.

Long enough to survive a slow LAN or a sleepy GitHub edge node; short
enough that a hung connection does not stall the systemd-timer-driven
one-shot poller for more than one interval.
"""

_GH_RETRY_AFTER_MAX_SEC: Final[float] = 60.0
"""Cap (in seconds) applied to a server-supplied ``Retry-After`` header value.

GitHub may request arbitrarily long back-offs on rate-limit (HTTP 429)
or overload responses. This cap bounds a single sleep inside the retry
loop so the poller does not stall for minutes on a misbehaving server.
"""

_RECENCY_WINDOW_SEC: Final[int] = 86400
"""Lookback window (in seconds) used to select candidate events from the DB.

24 hours expressed as seconds (86_400 = 60 * 60 * 24). Used as both the
``in_progress_run_ids`` recency cutoff and the stall-detection candidate
query window; keeping them the same constant avoids silent divergence.
"""


class EtagPollError(RuntimeError):
    """Raised when an unrecoverable precondition prevents polling.

    Library-mode callers (e.g. `waitbus doctor`, a notebook importing
    `etag_poll.fetch_runs` directly) catch this to recover or report.
    The `main()` entry point catches it and converts to `sys.exit(2)`.
    """


def gh_token() -> str:
    """Return a GitHub API token by shelling out to ``gh auth token``.

    The poller never embeds a token of its own; it inherits whichever
    identity the operator's `gh` CLI is authenticated as. Exits the
    process with code 2 on any failure (missing `gh`, unauthenticated,
    timeout) because the timer-driven daemon has no recovery path —
    systemd will retry on the next 45 s tick.
    """
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            check=True,
            capture_output=True,
            text=True,
            timeout=_GH_AUTH_TOKEN_TIMEOUT_SEC,
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise EtagPollError(f"gh auth token failed ({exc}); install gh and run `gh auth login`") from exc
    return result.stdout.strip()


def load_watched() -> list[tuple[str, str]]:
    """Parse the watched-repos file (path resolved via platformdirs) into (owner, repo) tuples.

    Lines that are blank, start with ``#``, or do not contain a single
    forward slash are silently skipped. The file format is intentionally
    permissive so operators can keep comments and section dividers in
    the file alongside the live entries. Returns an empty list when the
    file is absent — the poller treats no-repos-watched as a no-op
    rather than an error.
    """
    if not watched_repos().exists():
        return []
    out: list[tuple[str, str]] = []
    for line in watched_repos().read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "/" not in line:
            continue
        owner, _, repo = line.partition("/")
        out.append((owner.strip(), repo.strip()))
    return out


def load_etag_state() -> dict[str, str]:
    """Read the persisted ETag map (``key -> etag``) from disk.

    Returns an empty dict on missing file or malformed JSON so a
    corrupted state file degrades gracefully into a full refresh on
    the next cycle rather than blocking the timer.
    """
    if not etag_state().exists():
        return {}
    try:
        data: dict[str, str] = json.loads(etag_state().read_text(encoding="utf-8") or "{}")
        return data
    except json.JSONDecodeError:
        return {}


def save_etag_state(state: dict[str, str]) -> None:
    """Persist the ETag map for use on the next polling cycle.

    Written in sorted-key form so diffs between cycles are
    reviewable and CI-stable; the file is small enough that the
    sort cost is irrelevant.
    """
    etag_state().write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _parse_retry_after(value: str | None) -> float | None:
    """Parse an HTTP Retry-After header value to seconds-to-wait.

    Per RFC 9110 sec 10.2.3 the value is either a non-negative decimal
    integer of seconds, or an HTTP-date. Returns None for unparseable
    inputs (the caller falls back to stamina's normal backoff).
    """
    if not value:
        return None
    value = value.strip()
    # Integer-seconds form (the common case for GitHub).
    try:
        return float(int(value))
    except ValueError:
        pass
    # HTTP-date form. RFC 7231 IMF-fixdate; parsedate_to_datetime handles
    # the three legacy formats too.
    try:
        from email.utils import parsedate_to_datetime

        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    delta = (when - dt.datetime.now(dt.UTC)).total_seconds()
    return max(0.0, delta)


class _TransientGitHubError(Exception):
    """Transient GitHub-API failure worth retrying (5xx, 429, URLError, timeout).

    Permanent failures (4xx non-429, JSON-decode of a 200 body, 304 not-modified)
    return through `_conditional_get` directly without triggering a retry.
    """


@stamina.retry(
    on=_TransientGitHubError,
    attempts=_GH_RETRY_ATTEMPTS,
    wait_initial=1.0,
    wait_max=_GH_RETRY_WAIT_MAX_SEC,
)
def _do_conditional_get(
    url: str,
    token: str,
    etag: str | None,
) -> tuple[int, str | None, dict[str, Any]]:
    """Stamina-retried conditional GET.

    Returns (status, new_etag, parsed_json_or_{}). Retries on transient
    failures (5xx, 429, URLError, timeout) up to three attempts with
    exponential backoff, honoring server-supplied Retry-After headers.
    """
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    req.add_header("User-Agent", "waitbus-etag-poll/1.0")
    if etag:
        req.add_header("If-None-Match", etag)
    try:
        with urllib.request.urlopen(req, timeout=_GH_URLOPEN_TIMEOUT_SEC) as resp:
            new_etag = resp.headers.get("ETag")
            body = resp.read()
            data = json.loads(body.decode("utf-8"))
            return resp.status, new_etag, data
    except urllib.error.HTTPError as exc:
        # HTTPError extends addinfourl and holds an open response file
        # (SpooledTemporaryFile on Python 3.14+) whenever urllib actually
        # received bytes. Close it eagerly to prevent ResourceWarning at
        # GC time. fp may be absent / None on test-constructed errors, so
        # guard the close. Suppress secondary errors from a partially-
        # initialised file object — the response body is already discarded.
        try:
            if getattr(exc, "fp", None) is not None:
                exc.close()
        except Exception:
            pass
        if exc.code == 304:
            return 304, etag, {}
        if exc.code == 429 or exc.code >= 500:
            retry_after_raw = exc.headers.get("Retry-After") if exc.headers else None
            structured(
                logger,
                logging.WARNING,
                "http_transient",
                url=url,
                status=exc.code,
                retry_after=retry_after_raw,
            )
            # Honor server-supplied Retry-After (seconds OR HTTP-date per
            # RFC 9110 sec 10.2.3). Sleep before raising so stamina's own
            # exponential backoff stacks ON TOP, never below the server's
            # requested floor. Cap at 60s to bound a single retry cycle.
            retry_after_seconds = _parse_retry_after(retry_after_raw)
            if retry_after_seconds is not None:
                time.sleep(min(retry_after_seconds, _GH_RETRY_AFTER_MAX_SEC))
            raise _TransientGitHubError(f"{url} -> HTTP {exc.code}") from exc
        # 4xx non-429: permanent. Log once and surface to the caller.
        structured(logger, logging.WARNING, "http_error", url=url, status=exc.code)
        return exc.code, etag, {}
    except (urllib.error.URLError, TimeoutError) as exc:
        structured(logger, logging.WARNING, "http_transient", url=url, error=str(exc))
        raise _TransientGitHubError(f"{url} -> network: {exc}") from exc
    except json.JSONDecodeError as exc:
        # Server returned 200 with a malformed body; not retriable.
        structured(logger, logging.WARNING, "fetch_failed", url=url, error=str(exc))
        return 0, etag, {}


def _conditional_get(url: str, token: str, etag: str | None) -> tuple[int, str | None, dict[str, Any]]:
    """ETag-aware GET with stamina-backed retry on transient failures.

    Returns (status, new_etag, parsed_json_or_{}). After three transient
    attempts (up to ~30 s cumulative backoff), the failure surfaces as
    status=0 and the polling tick logs `fetch_failed_after_retries`.
    """
    try:
        return _do_conditional_get(url, token, etag)
    except _TransientGitHubError as exc:
        structured(
            logger,
            logging.WARNING,
            "fetch_failed_after_retries",
            url=url,
            error=str(exc),
        )
        return 0, etag, {}


def fetch_runs(owner: str, repo: str, token: str, etag: str | None) -> tuple[int, str | None, list[dict[str, Any]]]:
    """Fetch the latest 20 workflow_runs for ``owner/repo`` via ETag GET.

    Returns ``(status, new_etag, runs)``. A 304 leaves ``runs`` empty
    and signals the caller to skip the upsert pass entirely; a 200
    returns the parsed ``workflow_runs`` array (or empty on schema
    drift). Non-200/304 statuses already logged by ``_conditional_get``.
    """
    url = f"{API}/repos/{owner}/{repo}/actions/runs?per_page=20"
    status, new_etag, data = _conditional_get(url, token, etag)
    return status, new_etag, data.get("workflow_runs") or []


def fetch_jobs(
    owner: str, repo: str, run_id: int, token: str, etag: str | None
) -> tuple[int, str | None, list[dict[str, Any]]]:
    """Fetch the per-job state of one workflow run via ETag GET.

    Used by the main loop only for runs still in `in_progress` or
    `queued` state, so the per-job sub-poll is bounded by the live-run
    count (typically 1-3) and pays one conditional GET each.
    """
    url = f"{API}/repos/{owner}/{repo}/actions/runs/{run_id}/jobs"
    status, new_etag, data = _conditional_get(url, token, etag)
    return status, new_etag, data.get("jobs") or []


def in_progress_run_ids(owner: str, repo: str) -> list[int]:
    """Latest status per run_id; return those still in_progress or queued."""
    if not db_path().exists():
        return []
    # received_at is epoch nanoseconds; 86400 seconds = 86400 * NS_PER_SECOND ns.
    cutoff_ns = time.time_ns() - _RECENCY_WINDOW_SEC * NS_PER_SECOND
    with _db.connect(db_path(), readonly=True) as conn:
        cur = conn.execute(
            """
            SELECT run_id, status FROM events
            WHERE event_type='workflow_run' AND owner=? AND repo=? AND run_id IS NOT NULL
            AND received_at > ?
            ORDER BY received_at DESC
            """,
            (owner, repo, cutoff_ns),
        )
        seen: dict[int, str] = {}
        for row in cur.fetchall():
            rid = row[0]
            if rid not in seen:
                seen[rid] = row[1] or ""
    return [rid for rid, status in seen.items() if status in ("in_progress", "queued")]


def upsert_jobs(owner: str, repo: str, run_id: int, jobs: list[dict[str, Any]]) -> int:
    """Idempotently insert one event row per (job_id, status, conclusion) tuple.

    The delivery_id encodes the full state tuple so the same job in
    different states each produces a distinct row, but the same job in
    the same state across multiple polling cycles collapses to one row
    via the schema's UNIQUE(delivery_id) constraint. Returns the number
    of rows actually inserted on this invocation (i.e. new state
    transitions observed since the last poll).

    All inserts for the batch run inside a single BEGIN IMMEDIATE
    transaction. One doorbell ring fires at the end if any row was
    actually inserted, rather than one ring per inserted row.
    """
    if not jobs:
        return 0
    inserted = 0
    received_at_ns = time.time_ns()
    with _db.connect(db_path(), isolation_level=None) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for job in jobs:
                job_id = job.get("id")
                if job_id is None:
                    continue
                # Dedupe on (event_type, job_id, status, conclusion) tuple so
                # repeated polls during the same state are idempotent but each
                # state transition is recorded.
                delivery = f"etag:job:{job_id}:{job.get('status')}:{job.get('conclusion')}"
                body_text = json.dumps(
                    {
                        "workflow_job": job,
                        "repository": {"owner": {"login": owner}, "name": repo},
                    }
                )
                event = EventInsert(
                    delivery_id=delivery,
                    source="github",
                    event_type="workflow_job",
                    owner=owner,
                    repo=repo,
                    received_at=received_at_ns,
                    payload_json=body_text,
                    ingest_method="etag_poll",
                    head_branch=job.get("head_branch"),
                    head_sha=job.get("head_sha"),
                    status=job.get("status"),
                    conclusion=job.get("conclusion"),
                    job_id=job_id,
                    job_name=job.get("name"),
                    parent_run_id=run_id,
                )
                if insert_event(conn, event, commit=False):
                    inserted += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if inserted:
        _doorbell.ring()
    return inserted


def _parse_iso8601_to_epoch(iso_str: str) -> int | None:
    """Parse a GitHub-style ISO 8601 'Z' timestamp to epoch seconds."""
    try:
        return int(dt.datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp())
    except (ValueError, TypeError, AttributeError):
        return None


def emit_stall_synthetic_events(owner: str, repo: str, threshold_min: int) -> int:
    """Insert a synthetic stall row for each in-progress job past threshold.

    Looks at the latest stored state of every workflow_job for this repo
    in the last 24 hours. For each job whose latest state is 'in_progress'
    or 'queued' AND whose started_at (parsed from payload) is older than
    threshold_min, INSERT OR IGNORE a synthetic row with status='stalled'
    keyed by `etag:stall:{job_id}`. The IGNORE means a job emits exactly
    one stall row regardless of how many polling cycles observe the
    stall condition. Returns the number of stall rows inserted on this
    invocation.

    The threshold is a deliberately simple global heuristic. A job with
    a legitimately long, multi-hour budget trips a stall event past the
    threshold; that is the intended behavior — the stall row is a "this
    job has been running long enough that you should look at it" signal,
    not an auto-action.

    The read pass (SELECT candidates) runs first, then all stall inserts
    are batched in a single BEGIN IMMEDIATE transaction with one doorbell
    ring at the end.
    """
    if not db_path().exists():
        return 0
    now_s = int(time.time())
    now_ns = time.time_ns()
    threshold_s = threshold_min * 60
    # received_at is epoch nanoseconds; 86400 seconds = 86400 * NS_PER_SECOND ns.
    cutoff_ns = now_ns - 86400 * NS_PER_SECOND
    inserted = 0
    with _db.connect(db_path(), isolation_level=None) as conn:
        # Latest row per job_id within the recency window. The dedup key
        # in upsert_jobs is (job_id, status, conclusion), so a job with
        # multiple state transitions has multiple rows; MAX(received_at)
        # picks the most recent. Run the SELECT outside any write
        # transaction so we don't hold the write lock during the read.
        cur = conn.execute(
            """
            SELECT e.job_id, e.job_name, e.parent_run_id,
                   e.head_branch, e.head_sha, e.payload_json
            FROM events e
            JOIN (
                SELECT job_id, MAX(received_at) AS max_received
                FROM events
                WHERE event_type='workflow_job'
                  AND owner=? AND repo=?
                  AND job_id IS NOT NULL
                  AND received_at > ?
                GROUP BY job_id
            ) latest
              ON e.job_id = latest.job_id
             AND e.received_at = latest.max_received
            WHERE e.event_type='workflow_job'
              AND e.owner=? AND e.repo=?
              AND e.status IN ('in_progress', 'queued')
            """,
            (owner, repo, cutoff_ns, owner, repo),
        )
        candidates = cur.fetchall()

        # Build the list of stall rows to emit before opening the write
        # transaction so the SELECT result set is fully consumed.
        stall_rows: list[tuple[Any, ...]] = []
        for job_id, job_name, parent_run_id, branch, sha, payload_json in candidates:
            try:
                payload: dict[str, Any] = json.loads(payload_json) if payload_json else {}
            except json.JSONDecodeError:
                continue
            job_payload: dict[str, Any] = payload.get("workflow_job") or {}
            started_at_iso = job_payload.get("started_at")
            if not started_at_iso:
                continue
            started_at = _parse_iso8601_to_epoch(started_at_iso)
            if started_at is None:
                continue
            elapsed_s = now_s - started_at
            if elapsed_s < threshold_s:
                continue
            stall_rows.append((job_id, job_name, parent_run_id, branch, sha, job_payload, elapsed_s, started_at_iso))

        if not stall_rows:
            return 0

        conn.execute("BEGIN IMMEDIATE")
        try:
            for job_id, job_name, parent_run_id, branch, sha, job_payload, elapsed_s, started_at_iso in stall_rows:
                delivery = f"etag:stall:{job_id}"
                stall_body = json.dumps(
                    {
                        "workflow_job": job_payload,
                        "repository": {"owner": {"login": owner}, "name": repo},
                        "stall_detected": {
                            "elapsed_seconds": elapsed_s,
                            "threshold_minutes": threshold_min,
                            "started_at": started_at_iso,
                        },
                    }
                )
                stall_event = EventInsert(
                    delivery_id=delivery,
                    source="github",
                    event_type="workflow_job",
                    owner=owner,
                    repo=repo,
                    received_at=now_ns,
                    payload_json=stall_body,
                    ingest_method="etag_poll",
                    head_branch=branch,
                    head_sha=sha,
                    status="stalled",
                    conclusion="in_progress",
                    job_id=job_id,
                    job_name=job_name,
                    parent_run_id=parent_run_id,
                )
                if insert_event(conn, stall_event, commit=False):
                    inserted += 1
                    structured(
                        logger,
                        logging.INFO,
                        "stall_emitted",
                        repo=f"{owner}/{repo}",
                        job_id=job_id,
                        job_name=job_name,
                        elapsed_minutes=elapsed_s // 60,
                        threshold_minutes=threshold_min,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if inserted:
        _doorbell.ring()
    return inserted


def upsert_runs(owner: str, repo: str, runs: list[dict[str, Any]]) -> int:
    """Idempotently insert one event row per (run_id, status, conclusion) tuple.

    Same dedup contract as ``upsert_jobs`` but at the workflow_run
    granularity. Returns the number of new rows committed on this pass;
    a 200 response that re-reports the same state as last cycle yields
    zero new rows (the row already exists with that delivery_id).

    All inserts for the batch run inside a single BEGIN IMMEDIATE
    transaction. One doorbell ring fires at the end if any row was
    actually inserted, rather than one ring per inserted row.
    """
    if not runs:
        return 0
    inserted = 0
    received_at_ns = time.time_ns()
    with _db.connect(db_path(), isolation_level=None) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for run in runs:
                run_id = run.get("id")
                if run_id is None:
                    continue
                delivery = f"etag:{run_id}:{run.get('status')}:{run.get('conclusion')}"
                body_text = json.dumps(
                    {
                        "workflow_run": run,
                        "repository": {"owner": {"login": owner}, "name": repo},
                    }
                )
                event = EventInsert(
                    delivery_id=delivery,
                    source="github",
                    event_type="workflow_run",
                    owner=owner,
                    repo=repo,
                    received_at=received_at_ns,
                    payload_json=body_text,
                    ingest_method="etag_poll",
                    run_id=run_id,
                    workflow_name=run.get("name"),
                    head_branch=run.get("head_branch"),
                    head_sha=run.get("head_sha"),
                    status=run.get("status"),
                    conclusion=run.get("conclusion"),
                )
                if insert_event(conn, event, commit=False):
                    inserted += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if inserted:
        _doorbell.ring()
    return inserted


def main() -> int:
    """One-shot polling pass; entry point for ``waitbus etag-poll run``.

    Designed for systemd-timer invocation: runs one cycle across every
    repo in `watched_repos.txt`, refreshes the ETag map, emits stall
    synthetic events for in-progress jobs past the configured
    threshold, then exits. Exit codes: 0 on success or empty watch
    list, 2 when the events DB is absent (operator must run
    ``waitbus init`` first).

    Configuration is environment-driven via ``CiStatusConfig``; the
    daemon takes no positional or named CLI arguments.
    """
    cfg = _config.get_config()
    logging.basicConfig(
        level=cfg.log_level,
        format="%(message)s",
        stream=sys.stderr,
    )
    ensure_state_dirs()
    if not db_path().exists():
        sys.stderr.write(f"waitbus etag_poll: DB missing at {db_path()}; run `waitbus init` first\n")
        return 2
    watched = load_watched()
    if not watched:
        _metrics.incr("waitbus_etag_poll_runs_total", outcome="no_repos_watched")
        structured(logger, logging.INFO, "no_repos_watched", hint=str(watched_repos()))
        return 0
    _metrics.incr("waitbus_etag_poll_runs_total", outcome="started")
    try:
        token = gh_token()
    except EtagPollError as exc:
        sys.stderr.write(f"waitbus etag_poll: {exc}\n")
        return 2
    state = load_etag_state()
    sd_notify(b"READY=1\nSTATUS=polling\n")
    total_new = 0
    stall_threshold_min = cfg.stall_threshold_min
    for owner, repo in watched:
        total_new += _poll_one_repo(owner, repo, token, state, stall_threshold_min)
    save_etag_state(state)
    structured(logger, logging.INFO, "done", repos=len(watched), new_rows=total_new)
    return 0


def _poll_one_repo(
    owner: str,
    repo: str,
    token: str,
    state: dict[str, str],
    stall_threshold_min: int,
) -> int:
    """Poll a single repo's runs + per-run jobs + stall detection.

    Returns the number of newly-inserted event rows.  Updates
    ``state`` in place with any new ETag values.
    """
    key = f"{owner}/{repo}"
    repo_new = 0
    status, new_etag, runs = fetch_runs(owner, repo, token, state.get(key))
    _metrics.incr("waitbus_etag_poll_requests_total", endpoint="runs", status=str(status))
    if status == 200:
        n = upsert_runs(owner, repo, runs)
        repo_new += n
        if new_etag:
            state[key] = new_etag
        structured(logger, logging.INFO, "polled", repo=key, status=200, new_rows=n)
    elif status == 304:
        structured(logger, logging.INFO, "polled", repo=key, status=304, new_rows=0)
    else:
        structured(logger, logging.WARNING, "polled", repo=key, status=status, new_rows=0)

    # Per-job polling for runs still in progress. One GET per live run
    # (typically 1-3 concurrently) with ETag conditional to minimize
    # rate spend. Ignore fetch failures silently at job granularity.
    for run_id in in_progress_run_ids(owner, repo):
        job_key = f"{key}/jobs/{run_id}"
        j_status, j_new_etag, jobs = fetch_jobs(owner, repo, run_id, token, state.get(job_key))
        _metrics.incr("waitbus_etag_poll_requests_total", endpoint="jobs", status=str(j_status))
        if j_status == 200:
            jn = upsert_jobs(owner, repo, run_id, jobs)
            repo_new += jn
            if j_new_etag:
                state[job_key] = j_new_etag
            structured(logger, logging.INFO, "polled_jobs", repo=key, run_id=run_id, status=200, new_rows=jn)
        elif j_status == 304:
            structured(logger, logging.INFO, "polled_jobs", repo=key, run_id=run_id, status=304, new_rows=0)
        else:
            structured(logger, logging.WARNING, "polled_jobs", repo=key, run_id=run_id, status=j_status)

    # Stall detection: read-only against the local DB, no API spend.
    # Runs after the polling pass so it sees the latest job states.
    stall_new = emit_stall_synthetic_events(owner, repo, stall_threshold_min)
    repo_new += stall_new
    return repo_new


if __name__ == "__main__":
    sys.exit(main())
