"""Tests for the per-PR rollup CLI."""

from __future__ import annotations

import contextlib
import shutil
import sqlite3
import tempfile
import time
from collections.abc import Generator
from pathlib import Path

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from waitbus import _db, pr_monitor
from waitbus._types import EventInsert


def _seed_job(
    db: Path,
    *,
    delivery_id: str,
    head_sha: str,
    status: str,
    conclusion: str | None,
    job_id: int,
    owner: str = "o",
    repo: str = "r",
    received_at_offset: int = 0,
) -> None:
    """Insert one workflow_job row via _db.insert_event."""
    event = EventInsert(
        delivery_id=delivery_id,
        source="github",
        event_type="workflow_job",
        owner=owner,
        repo=repo,
        received_at=time.time_ns(),
        payload_json="{}",
        ingest_method="webhook",
        head_branch="main",
        head_sha=head_sha,
        status=status,
        conclusion=conclusion,
        job_id=job_id,
        job_name=f"job-{job_id}",
    )
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(conn, event)
        # Backdate received_at so the AGG_SQL ROW_NUMBER ORDER BY
        # received_at picks the row we expect to be "latest" per job.
        # Offsets are in seconds; multiply by 1e9 to stay in nanosecond
        # units. contextlib.closing() does NOT auto-commit on exit (unlike
        # the sqlite3.Connection context manager); commit explicitly so the
        # UPDATE is visible to the next connection that reads this row.
        if received_at_offset:
            conn.execute(
                "UPDATE events SET received_at = ? WHERE delivery_id = ?",
                (time.time_ns() + received_at_offset * 1_000_000_000, delivery_id),
            )
            conn.commit()


# --- AGG_SQL semantics ------------------------------------------------------


def test_agg_sql_all_green_when_every_job_succeeds(tmp_db_path: Path) -> None:
    for i in range(3):
        _seed_job(tmp_db_path, delivery_id=f"d-{i}", head_sha="abc", status="completed", conclusion="success", job_id=i)
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        state, n, passed, failed = pr_monitor.aggregate(conn, "o", "r", "abc")
    assert state == "ALL_GREEN"
    assert (n, passed, failed) == (3, 3, 0)


def test_agg_sql_fail_when_any_job_fails(tmp_db_path: Path) -> None:
    _seed_job(tmp_db_path, delivery_id="d-1", head_sha="abc", status="completed", conclusion="success", job_id=1)
    _seed_job(tmp_db_path, delivery_id="d-2", head_sha="abc", status="completed", conclusion="failure", job_id=2)
    _seed_job(tmp_db_path, delivery_id="d-3", head_sha="abc", status="completed", conclusion="success", job_id=3)
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        state, n, passed, failed = pr_monitor.aggregate(conn, "o", "r", "abc")
    assert state == "FAIL"
    assert (n, passed, failed) == (3, 2, 1)


def test_agg_sql_pending_when_any_job_in_progress(tmp_db_path: Path) -> None:
    _seed_job(tmp_db_path, delivery_id="d-1", head_sha="abc", status="completed", conclusion="success", job_id=1)
    _seed_job(tmp_db_path, delivery_id="d-2", head_sha="abc", status="in_progress", conclusion=None, job_id=2)
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        state, n, passed, failed = pr_monitor.aggregate(conn, "o", "r", "abc")
    assert state == "PENDING"
    assert (n, passed, failed) == (2, 1, 0)


def test_agg_sql_no_jobs_for_unknown_sha(tmp_db_path: Path) -> None:
    _seed_job(tmp_db_path, delivery_id="d-1", head_sha="abc", status="completed", conclusion="success", job_id=1)
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        state, n, passed, failed = pr_monitor.aggregate(conn, "o", "r", "deadbeef")
    assert state == "NO_JOBS"
    assert (n, passed, failed) == (0, 0, 0)


def test_agg_sql_uses_latest_state_per_job_id(tmp_db_path: Path) -> None:
    """A job that transitions in_progress -> success must roll up as success.

    AGG_SQL uses ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY
    received_at DESC) so only the most recent row per job_id contributes.
    """
    _seed_job(
        tmp_db_path,
        delivery_id="d-queued",
        head_sha="abc",
        status="queued",
        conclusion=None,
        job_id=1,
        received_at_offset=-30,
    )
    _seed_job(
        tmp_db_path,
        delivery_id="d-inprogress",
        head_sha="abc",
        status="in_progress",
        conclusion=None,
        job_id=1,
        received_at_offset=-15,
    )
    _seed_job(
        tmp_db_path,
        delivery_id="d-completed",
        head_sha="abc",
        status="completed",
        conclusion="success",
        job_id=1,
        received_at_offset=0,
    )
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        state, n, passed, failed = pr_monitor.aggregate(conn, "o", "r", "abc")
    assert state == "ALL_GREEN"
    assert (n, passed, failed) == (1, 1, 0)


# --- tick() emit-on-transition ---------------------------------------------


def test_tick_emits_only_when_state_changes(tmp_db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed_job(tmp_db_path, delivery_id="d-1", head_sha="abc", status="in_progress", conclusion=None, job_id=1)
    pr_sha = {7: "abc"}
    pr_state: dict[int, str] = {}
    observed = {7: False}
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        # First tick: emits the PENDING state (first observation).
        done1 = pr_monitor.tick(conn, "o", "r", [7], pr_sha, pr_state, observed)
        # Same state on second tick: must NOT emit a second line.
        done2 = pr_monitor.tick(conn, "o", "r", [7], pr_sha, pr_state, observed)
        # Update to terminal: emits transition.
        _seed_job(
            tmp_db_path,
            delivery_id="d-2",
            head_sha="abc",
            status="completed",
            conclusion="success",
            job_id=1,
            received_at_offset=10,
        )
        done3 = pr_monitor.tick(conn, "o", "r", [7], pr_sha, pr_state, observed)
    out = capsys.readouterr().out
    assert done1 is False
    assert done2 is False
    assert done3 is True
    lines = [ln for ln in out.splitlines() if ln.strip()]
    assert len(lines) == 2
    assert "PENDING" in lines[0]
    assert "ALL_GREEN" in lines[1]


def test_tick_force_push_resets_aggregation(tmp_db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A new head_sha (operator force-pushed) starts a fresh AGG_SQL with
    NO_JOBS until events for the new sha arrive. tick() emits the state
    change.
    """
    _seed_job(tmp_db_path, delivery_id="d-old", head_sha="aaa", status="completed", conclusion="success", job_id=1)
    pr_sha = {9: "aaa"}
    pr_state: dict[int, str] = {}
    observed = {9: False}
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        pr_monitor.tick(conn, "o", "r", [9], pr_sha, pr_state, observed)
        # Operator force-pushes: now head_sha = bbb.
        pr_sha[9] = "bbb"
        pr_monitor.tick(conn, "o", "r", [9], pr_sha, pr_state, observed)
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    assert any("aaa"[:7] in ln and "ALL_GREEN" in ln for ln in lines)
    assert any("bbb"[:7] in ln and "NO_JOBS" in ln for ln in lines)


# ---------------------------------------------------------------------------
# Property-based tests for AGG_SQL rollup
# ---------------------------------------------------------------------------

# --- Strategies ---

_status_st = st.sampled_from(["queued", "in_progress", "completed"])
_conclusion_st = st.one_of(
    st.none(),
    st.sampled_from(["success", "failure", "cancelled", "timed_out", "skipped", "neutral"]),
)
_job_row_st = st.tuples(_status_st, _conclusion_st)
_job_list_st = st.lists(_job_row_st, min_size=0, max_size=20)


# --- Reference oracle ---


def _reference_rollup(
    latest_per_job: list[tuple[str, str | None]],
) -> tuple[str, int, int, int]:
    """Pure-Python implementation of the AGG_SQL rollup contract.

    Args:
        latest_per_job: one (status, conclusion) pair per distinct job_id,
            already de-duplicated to the most-recent row.

    Returns:
        (state, n_total, n_passed, n_failed)
    """
    n = len(latest_per_job)
    if n == 0:
        return ("NO_JOBS", 0, 0, 0)
    passed = sum(1 for s, c in latest_per_job if s == "completed" and c == "success")
    failed = sum(1 for s, c in latest_per_job if c in ("failure", "cancelled", "timed_out"))
    if failed > 0:
        state = "FAIL"
    elif passed == n:
        state = "ALL_GREEN"
    else:
        state = "PENDING"
    return (state, n, passed, failed)


# --- Shared seeding helper for property tests ---


def _seed_jobs_for_property(
    db: Path,
    jobs: list[tuple[str, str | None]],
    *,
    owner: str = "o",
    repo: str = "r",
    head_sha: str = "abc",
    job_id_offset: int = 0,
    received_at_base: int | None = None,
) -> None:
    """Insert one workflow_job row per element of *jobs* with distinct job_ids.

    All rows share the same (owner, repo, head_sha). job_ids are assigned
    sequentially starting at job_id_offset so callers can insert into the
    same DB without collisions.
    """
    base = received_at_base if received_at_base is not None else time.time_ns()
    for i, (status, conclusion) in enumerate(jobs):
        _seed_job(
            db,
            delivery_id=f"prop-{owner}-{repo}-{head_sha}-{job_id_offset + i}",
            head_sha=head_sha,
            status=status,
            conclusion=conclusion,
            job_id=job_id_offset + i,
            owner=owner,
            repo=repo,
            received_at_offset=0,
        )
        # Backdate to a deterministic epoch so that two inserts within the
        # same second still have distinct received_at values.  We use
        # conn.execute directly to avoid going through _db.insert_event a
        # second time — the update just patches the already-committed row.
        with contextlib.closing(sqlite3.connect(db)) as conn:
            conn.execute(
                "UPDATE events SET received_at = ? WHERE delivery_id = ?",
                (base + i, f"prop-{owner}-{repo}-{head_sha}-{job_id_offset + i}"),
            )
            conn.commit()


# --- Fresh DB context manager for Hypothesis tests ---

import contextlib as _contextlib


@_contextlib.contextmanager
def _fresh_db() -> Generator[Path, None, None]:
    """Yield a Path to a brand-new SQLite DB with the canonical schema.

    Uses tempfile so each Hypothesis example gets an isolated, empty DB —
    avoiding the function-scoped fixture health check that fires when
    ``tmp_path`` is used inside ``@given``-decorated tests.
    """
    tmpdir = tempfile.mkdtemp()
    try:
        db = Path(tmpdir) / "events.db"
        _db.ensure_schema(db)
        yield db
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Property 1: AGG_SQL agrees with the reference oracle for any job list
# ---------------------------------------------------------------------------


@settings(max_examples=150, deadline=None)
@given(jobs=_job_list_st)
@example(jobs=[])  # NO_JOBS
@example(jobs=[("completed", "success"), ("completed", "success")])  # ALL_GREEN
@example(jobs=[("completed", "success"), ("completed", "failure")])  # FAIL (one failure)
@example(jobs=[("in_progress", None), ("in_progress", None)])  # PENDING
@example(jobs=[("completed", "timed_out"), ("completed", "success")])  # FAIL via timed_out
def test_agg_sql_agrees_with_reference_rollup_for_any_job_list(
    jobs: list[tuple[str, str | None]],
) -> None:
    """AGG_SQL must agree with the reference oracle for any combination of jobs."""
    with _fresh_db() as db:
        _seed_jobs_for_property(db, jobs)
        with contextlib.closing(sqlite3.connect(db)) as conn:
            actual = pr_monitor.aggregate(conn, "o", "r", "abc")

    expected = _reference_rollup(jobs)
    assert actual == expected, f"AGG_SQL={actual!r} differs from reference={expected!r} for jobs={jobs!r}"


# ---------------------------------------------------------------------------
# Property 2: Latest row per job_id wins when the same job_id repeats
# ---------------------------------------------------------------------------


@settings(max_examples=150, deadline=None)
@given(transitions=st.lists(_job_row_st, min_size=2, max_size=10))
@example(transitions=[("queued", None), ("in_progress", None), ("completed", "success")])
@example(transitions=[("completed", "success"), ("completed", "failure")])
@example(transitions=[("in_progress", None), ("completed", "cancelled")])
def test_latest_row_per_job_wins_when_same_job_id_repeats(
    transitions: list[tuple[str, str | None]],
) -> None:
    """Window function must select the most recent row when a job_id has multiple rows.

    The rollup count must be exactly 1 (one logical job), and the state
    must reflect only the LAST element of *transitions*.
    """
    with _fresh_db() as db:
        base = time.time_ns()
        for i, (status, conclusion) in enumerate(transitions):
            _seed_job(
                db,
                delivery_id=f"trans-{i}",
                head_sha="abc",
                status=status,
                conclusion=conclusion,
                job_id=42,  # same job_id for all rows
                received_at_offset=0,
            )
            # Assign strictly increasing received_at so that row ordering is
            # deterministic regardless of wall-clock speed.
            with contextlib.closing(sqlite3.connect(db)) as conn:
                conn.execute(
                    "UPDATE events SET received_at = ? WHERE delivery_id = ?",
                    (base + i, f"trans-{i}"),
                )
                conn.commit()

        last_status, last_conclusion = transitions[-1]

        with contextlib.closing(sqlite3.connect(db)) as conn:
            state, n_total, n_passed, n_failed = pr_monitor.aggregate(conn, "o", "r", "abc")

    # Only the LAST transition should be visible.
    assert n_total == 1, f"Expected 1 job, got n_total={n_total} for transitions={transitions!r}"

    expected_state, _, exp_passed, exp_failed = _reference_rollup([(last_status, last_conclusion)])
    assert state == expected_state, f"state={state!r} but last transition is {transitions[-1]!r}"
    assert n_passed == exp_passed
    assert n_failed == exp_failed


# ---------------------------------------------------------------------------
# Property 3: Rows for a different sha do not leak into the aggregation
# ---------------------------------------------------------------------------


@settings(max_examples=150, deadline=None)
@given(target_jobs=_job_list_st, noise_jobs=_job_list_st)
@example(target_jobs=[], noise_jobs=[("completed", "failure")])  # noise alone should not flip NO_JOBS
@example(
    target_jobs=[("completed", "success")],
    noise_jobs=[("completed", "failure")],
)  # noise failure must not pollute target ALL_GREEN
def test_other_sha_rows_do_not_leak_into_aggregation(
    target_jobs: list[tuple[str, str | None]],
    noise_jobs: list[tuple[str, str | None]],
) -> None:
    """AGG_SQL WHERE head_sha = ? must exclude rows belonging to a different sha."""
    with _fresh_db() as db:
        _seed_jobs_for_property(db, target_jobs, head_sha="target", job_id_offset=0)
        _seed_jobs_for_property(db, noise_jobs, head_sha="other", job_id_offset=1000)

        with contextlib.closing(sqlite3.connect(db)) as conn:
            actual = pr_monitor.aggregate(conn, "o", "r", "target")

    expected = _reference_rollup(target_jobs)
    assert actual == expected, (
        f"Noise sha='other' leaked: AGG_SQL={actual!r}, reference={expected!r}, "
        f"target_jobs={target_jobs!r}, noise_jobs={noise_jobs!r}"
    )


# ---------------------------------------------------------------------------
# Property 4: Rows for a different owner/repo do not leak into the aggregation
# ---------------------------------------------------------------------------


@settings(max_examples=150, deadline=None)
@given(target_jobs=_job_list_st, noise_jobs=_job_list_st)
@example(target_jobs=[], noise_jobs=[("completed", "failure")])
@example(
    target_jobs=[("completed", "success")],
    noise_jobs=[("completed", "timed_out")],
)
def test_other_owner_repo_rows_do_not_leak(
    target_jobs: list[tuple[str, str | None]],
    noise_jobs: list[tuple[str, str | None]],
) -> None:
    """AGG_SQL WHERE owner=? AND repo=? must exclude rows from a different owner/repo."""
    with _fresh_db() as db:
        _seed_jobs_for_property(
            db,
            target_jobs,
            owner="target-owner",
            repo="target-repo",
            head_sha="sha1",
            job_id_offset=0,
        )
        _seed_jobs_for_property(
            db,
            noise_jobs,
            owner="noise-owner",
            repo="noise-repo",
            head_sha="sha1",  # same sha — only owner/repo differ
            job_id_offset=1000,
        )

        with contextlib.closing(sqlite3.connect(db)) as conn:
            actual = pr_monitor.aggregate(conn, "target-owner", "target-repo", "sha1")

    expected = _reference_rollup(target_jobs)
    assert actual == expected, (
        f"Noise owner/repo leaked: AGG_SQL={actual!r}, reference={expected!r}, "
        f"target_jobs={target_jobs!r}, noise_jobs={noise_jobs!r}"
    )


# ---------------------------------------------------------------------------
# Property 5 (stateful): Force-push resets aggregation per sha
# ---------------------------------------------------------------------------


class ForcePushResetRule(RuleBasedStateMachine):
    """State machine modelling per-PR rollup across two head shas.

    Simulates the force-push invariant: sha_a and sha_b are independent
    aggregation buckets. Any insert for sha_a must not affect sha_b's
    rollup, and vice versa.

    The model tracks the latest (status, conclusion) per job_id for each
    sha; the invariant asserts that AGG_SQL produces the same result as
    the reference oracle applied to the model.
    """

    def __init__(self) -> None:
        super().__init__()
        self._tmpdir = tempfile.mkdtemp()
        self._db = Path(self._tmpdir) / "events.db"
        _db.ensure_schema(self._db)
        # model: sha -> {job_id -> (status, conclusion)}
        self._model: dict[str, dict[int, tuple[str, str | None]]] = {
            "sha_a": {},
            "sha_b": {},
        }
        self._counter = 0  # monotonic delivery_id counter
        self._base_ts = time.time_ns()

    def teardown(self) -> None:
        import shutil

        shutil.rmtree(self._tmpdir, ignore_errors=True)

    # --- strategies for rule parameters ---
    _sha_st = st.sampled_from(["sha_a", "sha_b"])
    _job_id_st = st.integers(min_value=0, max_value=4)

    @initialize()
    def start(self) -> None:
        # Nothing to initialise beyond __init__; kept for clarity.
        pass

    @rule(
        sha=_sha_st,
        job_id=_job_id_st,
        status=_status_st,
        conclusion=_conclusion_st,
    )
    def insert_job(
        self,
        sha: str,
        job_id: int,
        status: str,
        conclusion: str | None,
    ) -> None:
        """Insert or update a job row; model tracks the latest per (sha, job_id)."""
        self._counter += 1
        delivery_id = f"sm-{self._counter}"
        _seed_job(
            self._db,
            delivery_id=delivery_id,
            head_sha=sha,
            status=status,
            conclusion=conclusion,
            job_id=job_id,
            owner="o",
            repo="r",
        )
        # Assign monotonically increasing received_at so the window function
        # always picks the most recently inserted row per job_id.
        with contextlib.closing(sqlite3.connect(self._db)) as conn:
            conn.execute(
                "UPDATE events SET received_at = ? WHERE delivery_id = ?",
                (self._base_ts + self._counter, delivery_id),
            )
            conn.commit()
        # Update model: latest row per (sha, job_id) wins.
        self._model[sha][job_id] = (status, conclusion)

    @invariant()
    def sql_agrees_with_model(self) -> None:
        """After every rule, both shas must agree between AGG_SQL and the model."""
        with contextlib.closing(sqlite3.connect(self._db)) as conn:
            for sha in ("sha_a", "sha_b"):
                actual = pr_monitor.aggregate(conn, "o", "r", sha)
                latest = list(self._model[sha].values())
                expected = _reference_rollup(latest)
                assert actual == expected, (
                    f"sha={sha!r}: AGG_SQL={actual!r}, reference={expected!r}, model={self._model[sha]!r}"
                )


# Expose as a standard pytest test class.
TestForcePushReset = ForcePushResetRule.TestCase
