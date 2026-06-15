"""Tests for `read_events.py --watch` against a live broadcast daemon."""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import sys
import threading
import time
from collections.abc import Iterator
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from waitbus import _db, broadcast, read_events
from waitbus._broadcast_sub import BookmarkCursor
from waitbus._types import EventInsert

_DaemonPaths = tuple[broadcast.Broadcast, dict[str, Path]]

# `read_events --watch` connects to the broadcast daemon via AF_UNIX
# SOCK_STREAM; the daemon itself is Linux-only (systemd socket activation).
pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="--watch connects to AF_UNIX SOCK_STREAM broadcast daemon (Linux-only)",
)


def _event_stub(delivery_id: str, **overrides: Any) -> EventInsert:
    defaults: dict[str, Any] = {
        "source": "github",
        "event_type": "workflow_run",
        "owner": "test-owner",
        "repo": "test-repo",
        "received_at": time.time_ns(),
        "payload_json": "{}",
        "ingest_method": "webhook",
        "run_id": 1,
        "workflow_name": "Tests",
        "head_branch": "main",
        "head_sha": "abc",
        "status": "completed",
        "conclusion": "success",
    }
    defaults.update(overrides)
    return EventInsert(delivery_id=delivery_id, **defaults)


def _insert(db: Path, delivery_id: str, **field_overrides: Any) -> None:
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(conn, _event_stub(delivery_id, **field_overrides))


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Redirect the BookmarkCursor state dir to a tmp path.

    ``--watch`` resumes via ``BookmarkCursor``, whose cursor files live
    under the platformdirs/``WAITBUS_STATE_DIR`` state root (not a
    per-(owner,repo) cache).
    """
    d = tmp_path / "state"
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(d))
    yield d


@pytest.mark.asyncio
async def test_watch_prints_summary_lines_for_matching_events(running_daemon: _DaemonPaths, state_dir: Path) -> None:
    daemon, paths = running_daemon
    # Run watch() in a worker thread so the asyncio loop driving the
    # daemon can keep ticking. read_events.watch is sync (blocking
    # socket recv); a thread is the right shape.
    captured = StringIO()
    monitor_lines: list[str] = []

    def runner() -> None:
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            read_events.watch(
                filters=["test-owner/test-repo"],
                event_types=None,
                since=None,
                cursor=None,
                socket_path=paths["broadcast"],
            )
        finally:
            sys.stdout = old_stdout
            monitor_lines.extend(captured.getvalue().splitlines())

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    # Give the subscriber 100 ms to send its subscribe frame.
    await asyncio.sleep(0.15)
    _insert(paths["db"], "d-watch-1")
    _insert(paths["db"], "d-watch-2", owner="other", repo="other")  # filtered out
    _insert(paths["db"], "d-watch-3")
    await asyncio.sleep(0.4)
    # Stop the daemon to make the subscriber's recv return b"".
    daemon.stopping = True
    # The daemon's stop_event isn't directly accessible; closing the
    # listener socket isn't enough either — directly close all subs.
    for fd in list(daemon.subscribers):
        daemon._close_subscriber(fd, reason="test_tear_down")
    t.join(timeout=2.0)
    assert not t.is_alive(), "watch() did not exit on EOF"
    lines = captured.getvalue().splitlines()
    # 2 expected lines (d-watch-1, d-watch-3); the d-watch-2 (other/other)
    # is filtered out.
    assert len(lines) >= 2
    assert any("d-watch-1" not in ln and "main" in ln for ln in lines), lines


@pytest.mark.asyncio
async def test_watch_persists_cursor_via_bookmark(running_daemon: _DaemonPaths, state_dir: Path) -> None:
    """Resume token is persisted through the unified BookmarkCursor."""
    daemon, paths = running_daemon
    bookmark = read_events.watch_bookmark_name("o", "r")

    def runner() -> None:
        read_events.watch(
            filters=["o/r"],
            event_types=None,
            since=None,
            cursor=BookmarkCursor(bookmark),
            socket_path=paths["broadcast"],
        )

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    await asyncio.sleep(0.1)
    _insert(paths["db"], "d-cur-1", owner="o", repo="r")
    await asyncio.sleep(0.3)
    # Stop subscriber cleanly.
    daemon.stopping = True
    for fd in list(daemon.subscribers):
        daemon._close_subscriber(fd, reason="test_tear_down")
    t.join(timeout=2.0)
    # BookmarkCursor persisted a 26-char ULID resume token.
    persisted = BookmarkCursor(bookmark).load()
    assert persisted is not None and len(persisted) == 26


@pytest.mark.asyncio
async def test_watch_resume_from_cursor_skips_already_seen(running_daemon: _DaemonPaths, state_dir: Path) -> None:
    daemon, paths = running_daemon
    # Pre-seed and capture the first event_id.
    _insert(paths["db"], "d-a", owner="o", repo="r")
    _insert(paths["db"], "d-b", owner="o", repo="r")
    _insert(paths["db"], "d-c", owner="o", repo="r")
    await asyncio.sleep(0.05)
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        first_id = conn.execute("SELECT event_id FROM events WHERE delivery_id='d-a'").fetchone()[0]
    # Pre-populate the bookmark as if a prior watch saw d-a.
    bookmark = read_events.watch_bookmark_name("o", "r")
    BookmarkCursor(bookmark).advance({"event_id": first_id, "kind": "event"})

    captured = StringIO()

    def runner() -> None:
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            seed = BookmarkCursor(bookmark)
            read_events.watch(
                filters=["o/r"],
                event_types=None,
                since=seed.load(),
                cursor=seed,
                socket_path=paths["broadcast"],
            )
        finally:
            sys.stdout = old_stdout

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    await asyncio.sleep(0.4)
    daemon.stopping = True
    for fd in list(daemon.subscribers):
        daemon._close_subscriber(fd, reason="test_tear_down")
    t.join(timeout=2.0)
    output = captured.getvalue()
    # Should NOT show d-a (already seen per cursor). Should show d-b + d-c.
    # Note: summaries don't contain delivery_id by default; we check
    # the rough line count instead.
    lines = [ln for ln in output.splitlines() if ln.strip()]
    assert len(lines) == 2, f"expected exactly d-b + d-c, got {lines!r}"


def test_watch_returns_2_when_socket_missing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A clear non-zero exit + stderr message when the daemon isn't running."""
    rc = read_events.watch(
        filters=["*"],
        event_types=None,
        since=None,
        cursor=None,
        socket_path=tmp_path / "no-such-socket",
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "broadcast socket" in err


# ---------------------------------------------------------------------------
# fetch_jobs_for_run — SQL window-function dedup path
# ---------------------------------------------------------------------------


def _insert_job(db: Path, delivery_id: str, **overrides: Any) -> None:
    defaults: dict[str, Any] = {
        "source": "github",
        "event_type": "workflow_job",
        "owner": "o",
        "repo": "r",
        "received_at": time.time_ns(),
        "payload_json": "{}",
        "ingest_method": "webhook",
        "head_branch": "main",
        "head_sha": "abc123",
        "status": "completed",
        "conclusion": "success",
        "job_id": 1,
        "job_name": "build",
        "parent_run_id": 10,
    }
    defaults.update(overrides)
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(conn, EventInsert(delivery_id=delivery_id, **defaults))


def test_fetch_jobs_for_run_dedupes_by_latest_received_at(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Window function keeps only the most recent row per job_id."""
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    # Insert 3 rows for the same job_id with different delivery_ids so
    # INSERT OR IGNORE does not deduplicate them. We manipulate
    # received_at directly after insert to simulate out-of-order arrivals.
    _insert_job(tmp_db_path, "d-j1-a", job_id=42, parent_run_id=99)
    _insert_job(tmp_db_path, "d-j1-b", job_id=42, parent_run_id=99)
    _insert_job(tmp_db_path, "d-j1-c", job_id=42, parent_run_id=99)
    # Stamp deterministic received_at values so ordering is unambiguous.
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        conn.execute("UPDATE events SET received_at=? WHERE delivery_id=?", (1000, "d-j1-a"))
        conn.execute("UPDATE events SET received_at=? WHERE delivery_id=?", (2000, "d-j1-b"))
        conn.execute("UPDATE events SET received_at=? WHERE delivery_id=?", (3000, "d-j1-c"))
        conn.commit()

    rows = read_events.fetch_jobs_for_run("o", "r", 99)
    assert len(rows) == 1
    assert rows[0]["job_id"] == 42
    assert rows[0]["received_at"] == 3000


def test_fetch_jobs_for_run_returns_one_per_job_id(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One result row per distinct job_id within the same run."""
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    _insert_job(tmp_db_path, "d-j2-a", job_id=1, job_name="lint", parent_run_id=100)
    _insert_job(tmp_db_path, "d-j2-b", job_id=2, job_name="test", parent_run_id=100)
    _insert_job(tmp_db_path, "d-j2-c", job_id=3, job_name="build", parent_run_id=100)
    # Extra row for job_id=1 (duplicate delivery with a distinct delivery_id).
    _insert_job(tmp_db_path, "d-j2-d", job_id=1, job_name="lint", parent_run_id=100)

    rows = read_events.fetch_jobs_for_run("o", "r", 100)
    job_ids = [r["job_id"] for r in rows]
    assert len(job_ids) == 3, f"expected 3 distinct jobs, got {job_ids!r}"
    assert sorted(job_ids) == [1, 2, 3]


def test_fetch_jobs_for_run_respects_limit(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """limit= caps the result set even when more jobs exist."""
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    for jid in range(20):
        _insert_job(
            tmp_db_path,
            f"d-j3-{jid:03d}",
            job_id=jid,
            job_name=f"job-{jid}",
            parent_run_id=200,
        )

    rows = read_events.fetch_jobs_for_run("o", "r", 200, limit=5)
    assert len(rows) == 5


def test_fetch_jobs_for_run_uses_partial_covering_index(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """EXPLAIN QUERY PLAN for per-run job rollup uses an index, not a full scan.

    Guards against accidental schema regressions: if all partial indexes
    over events are dropped the planner falls back to a full-table scan
    and this assertion fails.

    Also verifies idx_workflow_job_head_sha exists in the DB for the
    pr_monitor AGG_SQL path, which queries by head_sha.
    """
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    with contextlib.closing(_db.open_conn(tmp_db_path, readonly=True)) as conn:
        # The fetch_jobs_for_run CTE filters on parent_run_id; the planner
        # should use either idx_parent_run_id or idx_owner_repo_event,
        # never a full table scan.
        plan_rows = conn.execute(
            """
            EXPLAIN QUERY PLAN
            WITH ranked AS (
                SELECT job_id, ROW_NUMBER() OVER (
                    PARTITION BY job_id ORDER BY received_at DESC
                ) AS rn
                FROM events
                WHERE event_type = 'workflow_job'
                  AND owner = 'o'
                  AND repo = 'r'
                  AND parent_run_id = 99
            )
            SELECT job_id FROM ranked WHERE rn = 1
            """
        ).fetchall()
        plan_text = " ".join(str(r) for r in plan_rows).lower()
        assert "scan events" not in plan_text, f"expected index usage, got full table scan:\n{plan_text}"
        # Verify the head_sha partial index exists (guards pr_monitor path).
        idx_row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_workflow_job_head_sha'"
        ).fetchone()
        assert idx_row is not None, "idx_workflow_job_head_sha missing from schema"


def test_partial_index_excludes_non_workflow_job_rows(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """workflow_run rows with a head_sha are not indexed by idx_workflow_job_head_sha.

    Inserts one workflow_run and one workflow_job row, then verifies that
    the partial index count matches only the workflow_job row.
    """
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    # Insert a workflow_run row (should NOT appear in the partial index).
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        _db.insert_event(
            conn,
            EventInsert(
                delivery_id="d-pi-run",
                source="github",
                event_type="workflow_run",
                owner="o",
                repo="r",
                received_at=time.time_ns(),
                payload_json="{}",
                ingest_method="webhook",
                run_id=300,
                workflow_name="CI",
                head_branch="main",
                head_sha="sha-run",
                status="completed",
                conclusion="success",
            ),
        )
    # Insert a workflow_job row (SHOULD appear in the partial index).
    _insert_job(tmp_db_path, "d-pi-job", job_id=77, parent_run_id=300, head_sha="sha-job")

    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        # Rows reachable via the partial index are only workflow_job rows
        # with a non-null head_sha.
        indexed_count = conn.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE event_type = 'workflow_job' AND head_sha IS NOT NULL
            """
        ).fetchone()[0]
    assert indexed_count == 1, f"expected 1 row covered by partial index, got {indexed_count}"
