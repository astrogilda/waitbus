"""Unit tests for etag_poll library functions.

Coverage targets:
- gh_token() raises EtagPollError on every gh-invocation failure mode
  (binary missing, non-zero exit, timeout). Library callers see the
  exception; the main() entry point converts to sys.exit(2).
- load_etag_state() handles missing file, valid JSON, and corrupt JSON.
- save_etag_state() round-trips dict to disk.
- load_watched() ignores blanks and comments, splits owner/repo.
- _conditional_get() handles 200, 304, HTTPError(non-304), and URLError.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import subprocess
import urllib.error
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from waitbus import etag_poll

# --- gh_token --------------------------------------------------------------


def test_gh_token_returns_stripped_stdout_on_success() -> None:
    completed = subprocess.CompletedProcess(
        args=["gh", "auth", "token"],
        returncode=0,
        stdout="ghp_abc123\n",
        stderr="",
    )
    with patch("waitbus.etag_poll.subprocess.run", return_value=completed):
        assert etag_poll.gh_token() == "ghp_abc123"


def test_gh_token_raises_when_gh_binary_missing() -> None:
    with (
        patch("waitbus.etag_poll.subprocess.run", side_effect=FileNotFoundError("gh")),
        pytest.raises(etag_poll.EtagPollError, match="gh auth token"),
    ):
        etag_poll.gh_token()


def test_gh_token_raises_on_nonzero_exit() -> None:
    err = subprocess.CalledProcessError(returncode=2, cmd=["gh"])
    with patch("waitbus.etag_poll.subprocess.run", side_effect=err), pytest.raises(etag_poll.EtagPollError):
        etag_poll.gh_token()


def test_gh_token_raises_on_timeout() -> None:
    err = subprocess.TimeoutExpired(cmd=["gh"], timeout=5)
    with patch("waitbus.etag_poll.subprocess.run", side_effect=err), pytest.raises(etag_poll.EtagPollError):
        etag_poll.gh_token()


def test_etag_poll_error_is_runtime_error_subclass() -> None:
    """Library callers can catch RuntimeError as the supertype."""
    assert issubclass(etag_poll.EtagPollError, RuntimeError)


# --- etag state round-trip -------------------------------------------------


def test_load_etag_state_missing_file_returns_empty_dict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(etag_poll, "etag_state", lambda: tmp_path / "absent.json")
    assert etag_poll.load_etag_state() == {}


def test_load_etag_state_parses_valid_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = tmp_path / "etag.json"
    f.write_text(json.dumps({"o/r": 'W/"abc"'}))
    monkeypatch.setattr(etag_poll, "etag_state", lambda: f)
    assert etag_poll.load_etag_state() == {"o/r": 'W/"abc"'}


def test_load_etag_state_returns_empty_on_corrupt_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = tmp_path / "etag.json"
    f.write_text("{not json at all")
    monkeypatch.setattr(etag_poll, "etag_state", lambda: f)
    assert etag_poll.load_etag_state() == {}


def test_save_etag_state_writes_sorted_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = tmp_path / "etag.json"
    monkeypatch.setattr(etag_poll, "etag_state", lambda: f)
    etag_poll.save_etag_state({"b/y": "2", "a/x": "1"})
    parsed = json.loads(f.read_text())
    assert parsed == {"a/x": "1", "b/y": "2"}
    # sort_keys=True invariant survives the dict round-trip
    assert list(parsed.keys()) == ["a/x", "b/y"]


# --- load_watched ----------------------------------------------------------


def test_load_watched_ignores_blanks_and_comments(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = tmp_path / "watched.txt"
    f.write_text("# header comment\n\nowner-a/repo-1\n  owner-b/repo-2  \n# trailing comment\nno-slash-here\n")
    monkeypatch.setattr(etag_poll, "watched_repos", lambda: f)
    assert etag_poll.load_watched() == [
        ("owner-a", "repo-1"),
        ("owner-b", "repo-2"),
    ]


def test_load_watched_missing_file_returns_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(etag_poll, "watched_repos", lambda: tmp_path / "absent.txt")
    assert etag_poll.load_watched() == []


# --- _conditional_get ------------------------------------------------------


class _StubResponse:
    """Minimal urllib response surface used by _conditional_get."""

    def __init__(self, status: int, body: bytes, etag: str | None) -> None:
        self.status = status
        self._body = body
        self.headers = {"ETag": etag} if etag else {}

    def __enter__(self) -> _StubResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_conditional_get_200_returns_etag_and_body() -> None:
    body = json.dumps({"workflow_runs": [{"id": 1}]}).encode("utf-8")
    resp = _StubResponse(200, body, etag='"new-etag"')
    with patch("waitbus.etag_poll.urllib.request.urlopen", return_value=resp):
        status, etag, data = etag_poll._conditional_get("https://example/x", "t", etag=None)
    assert status == 200
    assert etag == '"new-etag"'
    assert data == {"workflow_runs": [{"id": 1}]}


def test_conditional_get_304_preserves_supplied_etag() -> None:
    err = urllib.error.HTTPError(
        url="x",
        code=304,
        msg="Not Modified",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,
    )
    with patch("waitbus.etag_poll.urllib.request.urlopen", side_effect=err):
        status, etag, data = etag_poll._conditional_get(
            "https://example/x",
            "t",
            etag='"prev"',
        )
    assert status == 304
    assert etag == '"prev"'
    assert data == {}


def test_conditional_get_http_500_retries_through_real_stamina_loop_and_surfaces_zero() -> None:
    """Exercise the REAL stamina retry loop on HTTP 500.

    The prior version patched ``_do_conditional_get.__wrapped__`` to
    bypass stamina entirely; that left the retry path uncovered. This
    version patches ``urllib.request.urlopen`` to raise HTTPError(500)
    on every call and patches ``time.sleep`` to make backoff instant,
    then asserts urlopen was retried the configured 3 times and the
    outer call surfaces status=0 with the supplied etag preserved.
    """
    err = urllib.error.HTTPError(
        url="x",
        code=500,
        msg="Server Error",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,
    )
    with (
        patch("waitbus.etag_poll.urllib.request.urlopen", side_effect=err) as urlopen,
        # Make stamina's backoff instant; the retry COUNT, not the wall
        # time, is what the test pins.
        patch("time.sleep"),
    ):
        status, etag, data = etag_poll._conditional_get(
            "https://example/x",
            "t",
            etag='"prev"',
        )
    assert urlopen.call_count == 3  # stamina's configured attempts
    assert status == 0
    assert etag == '"prev"'
    assert data == {}


def test_conditional_get_url_error_retries_through_real_stamina_loop_and_surfaces_zero() -> None:
    """Exercise the REAL stamina retry loop on URLError.

    Same shape as the HTTP-500 test; URLError is the other transient
    branch (DNS / network unreachable).
    """
    err = urllib.error.URLError("dns failure")
    with (
        patch("waitbus.etag_poll.urllib.request.urlopen", side_effect=err) as urlopen,
        patch("time.sleep"),
    ):
        status, etag, data = etag_poll._conditional_get(
            "https://example/x",
            "t",
            etag=None,
        )
    assert urlopen.call_count == 3
    assert status == 0
    assert etag is None
    assert data == {}


def test_conditional_get_http_429_with_retry_after_sleeps_then_retries() -> None:
    """Server-supplied Retry-After is honored before stamina's backoff kicks in.

    Regression-fence: parse Retry-After per RFC 9110 sec 10.2.3
    and sleep up to 60s before raising the transient. Stamina's backoff
    stacks on top. This test patches urlopen to return 429 with
    Retry-After: 5 and asserts time.sleep was invoked with a value >= 5.0
    (the Retry-After-derived sleep) on at least one of the retry cycles.
    """
    from email.message import Message

    headers = Message()
    headers["Retry-After"] = "5"
    err = urllib.error.HTTPError(
        url="x",
        code=429,
        msg="Too Many Requests",
        hdrs=headers,
        fp=None,
    )
    with (
        patch("waitbus.etag_poll.urllib.request.urlopen", side_effect=err) as urlopen,
        patch("waitbus.etag_poll.time.sleep") as sleep_mock,
    ):
        status, etag, data = etag_poll._conditional_get(
            "https://example/x",
            "t",
            etag=None,
        )
    # urlopen retried per stamina config; on each retriable failure the
    # Retry-After sleep fired before the raise.
    assert urlopen.call_count == 3
    retry_after_sleeps = [c for c in sleep_mock.call_args_list if c.args and c.args[0] >= 5.0]
    assert retry_after_sleeps, f"expected at least one sleep(>=5.0); got {sleep_mock.call_args_list!r}"
    assert status == 0
    assert etag is None
    assert data == {}


def test_parse_retry_after_handles_integer_seconds() -> None:
    """RFC 9110 integer-seconds form (the GitHub-common case)."""
    assert etag_poll._parse_retry_after("60") == 60.0
    assert etag_poll._parse_retry_after("0") == 0.0
    assert etag_poll._parse_retry_after(" 30 ") == 30.0  # whitespace tolerated


def test_parse_retry_after_handles_http_date() -> None:
    """RFC 9110 HTTP-date form. Returns positive seconds-until."""
    future = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=30)
    http_date = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
    seconds = etag_poll._parse_retry_after(http_date)
    assert seconds is not None
    # Allow 5s slack for clock drift / test scheduling jitter.
    assert 25.0 <= seconds <= 35.0


def test_parse_retry_after_returns_none_on_garbage() -> None:
    assert etag_poll._parse_retry_after(None) is None
    assert etag_poll._parse_retry_after("") is None
    assert etag_poll._parse_retry_after("not-a-number-or-date") is None


def test_parse_iso8601_to_epoch_handles_z_suffix() -> None:
    epoch = etag_poll._parse_iso8601_to_epoch("2026-05-11T00:00:00Z")
    assert epoch is not None
    assert epoch > 0


def test_parse_iso8601_to_epoch_returns_none_on_garbage() -> None:
    assert etag_poll._parse_iso8601_to_epoch("not-a-date") is None
    assert etag_poll._parse_iso8601_to_epoch("") is None


# --- fetch_runs / fetch_jobs (thin wrappers over _conditional_get) ---------


def test_fetch_runs_unpacks_workflow_runs_list() -> None:
    body = json.dumps({"workflow_runs": [{"id": 7}]}).encode("utf-8")
    resp = _StubResponse(200, body, etag='"x"')
    with patch("waitbus.etag_poll.urllib.request.urlopen", return_value=resp):
        status, etag, runs = etag_poll.fetch_runs("o", "r", "t", None)
    assert status == 200
    assert etag == '"x"'
    assert runs == [{"id": 7}]


def test_fetch_runs_returns_empty_list_when_key_missing() -> None:
    body = json.dumps({}).encode("utf-8")
    resp = _StubResponse(200, body, etag='"x"')
    with patch("waitbus.etag_poll.urllib.request.urlopen", return_value=resp):
        _status, _etag, runs = etag_poll.fetch_runs("o", "r", "t", None)
    assert runs == []


def test_fetch_jobs_unpacks_jobs_list() -> None:
    body = json.dumps({"jobs": [{"id": 42, "name": "build"}]}).encode("utf-8")
    resp = _StubResponse(200, body, etag=None)
    with patch("waitbus.etag_poll.urllib.request.urlopen", return_value=resp):
        status, _etag, jobs = etag_poll.fetch_jobs("o", "r", 1, "t", None)
    assert status == 200
    assert jobs == [{"id": 42, "name": "build"}]


# --- upsert_runs / upsert_jobs --------------------------------------------


@pytest.fixture
def empty_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Per-test SQLite DB with the canonical schema applied."""
    from waitbus import listener as _listener

    db = tmp_path / "events.db"
    _listener.ensure_schema(db)
    monkeypatch.setattr(etag_poll, "db_path", lambda: db)
    return db


def test_upsert_runs_inserts_new_rows(empty_db: Path) -> None:
    runs = [
        {
            "id": 1,
            "name": "wf",
            "head_branch": "main",
            "head_sha": "abc",
            "status": "completed",
            "conclusion": "success",
        },
        {"id": 2, "name": "wf", "head_branch": "main", "head_sha": "def", "status": "in_progress", "conclusion": None},
    ]
    inserted = etag_poll.upsert_runs("owner", "repo", runs)
    assert inserted == 2


def test_upsert_runs_returns_zero_on_empty_list(empty_db: Path) -> None:
    assert etag_poll.upsert_runs("o", "r", []) == 0


def test_upsert_runs_dedup_idempotent(empty_db: Path) -> None:
    """Same (id, status, conclusion) tuple inserts once across two calls."""
    runs = [
        {"id": 99, "name": "w", "head_branch": "main", "head_sha": "x", "status": "completed", "conclusion": "success"}
    ]
    first = etag_poll.upsert_runs("o", "r", runs)
    second = etag_poll.upsert_runs("o", "r", runs)
    assert first == 1
    assert second == 0


def test_upsert_runs_skips_rows_with_none_id(empty_db: Path) -> None:
    runs = [{"id": None, "status": "completed"}]
    assert etag_poll.upsert_runs("o", "r", runs) == 0


def test_upsert_jobs_inserts_per_state_transition(empty_db: Path) -> None:
    jobs = [
        {"id": 100, "name": "j", "head_branch": "main", "head_sha": "x", "status": "in_progress", "conclusion": None},
        {
            "id": 100,
            "name": "j",
            "head_branch": "main",
            "head_sha": "x",
            "status": "completed",
            "conclusion": "success",
        },
    ]
    assert etag_poll.upsert_jobs("o", "r", 7, jobs) == 2


def test_upsert_jobs_skips_rows_with_none_id(empty_db: Path) -> None:
    jobs = [{"id": None, "status": "queued"}]
    assert etag_poll.upsert_jobs("o", "r", 1, jobs) == 0


def test_upsert_jobs_empty_list_returns_zero(empty_db: Path) -> None:
    assert etag_poll.upsert_jobs("o", "r", 1, []) == 0


# --- in_progress_run_ids ---------------------------------------------------


def test_in_progress_run_ids_returns_empty_when_db_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(etag_poll, "db_path", lambda: tmp_path / "missing.db")
    assert etag_poll.in_progress_run_ids("o", "r") == []


def test_in_progress_run_ids_yields_in_progress_run(empty_db: Path) -> None:
    runs = [{"id": 1, "name": "w", "head_branch": "main", "head_sha": "x", "status": "in_progress", "conclusion": None}]
    etag_poll.upsert_runs("o", "r", runs)
    assert etag_poll.in_progress_run_ids("o", "r") == [1]


def test_in_progress_run_ids_excludes_completed_run(empty_db: Path) -> None:
    runs = [
        {"id": 2, "name": "w", "head_branch": "main", "head_sha": "x", "status": "completed", "conclusion": "success"}
    ]
    etag_poll.upsert_runs("o", "r", runs)
    assert etag_poll.in_progress_run_ids("o", "r") == []


# --- emit_stall_synthetic_events -------------------------------------------


def test_emit_stall_returns_zero_when_db_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(etag_poll, "db_path", lambda: tmp_path / "missing.db")
    assert etag_poll.emit_stall_synthetic_events("o", "r", 60) == 0


def test_emit_stall_no_op_when_no_in_progress_jobs(empty_db: Path) -> None:
    assert etag_poll.emit_stall_synthetic_events("o", "r", 60) == 0


def test_emit_stall_skips_unparseable_payload(empty_db: Path) -> None:
    """A job row whose payload_json is invalid JSON is silently skipped."""
    import sqlite3
    import time as _t

    from waitbus._ulid import new as ulid_new

    with contextlib.closing(sqlite3.connect(empty_db)) as conn:
        conn.execute(
            "INSERT INTO events ("
            "delivery_id, source, event_type, owner, repo, status, "
            "received_at, payload_json, ingest_method, job_id, event_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "d-bad",
                "github",
                "workflow_job",
                "o",
                "r",
                "in_progress",
                _t.time_ns(),
                "not json",
                "etag_poll",
                555,
                ulid_new(),
            ),
        )
        conn.commit()
    assert etag_poll.emit_stall_synthetic_events("o", "r", 1) == 0


def test_emit_stall_emits_one_synthetic_row_past_threshold(
    empty_db: Path,
) -> None:
    """A job in_progress longer than threshold trips exactly one stall event."""
    import time as _t

    started_iso = "2020-01-01T00:00:00Z"  # ancient — well past any threshold
    job_payload: dict[str, dict[str, Any]] = {
        "workflow_job": {
            "id": 777,
            "name": "long-runner",
            "head_branch": "main",
            "head_sha": "abc",
            "started_at": started_iso,
            "status": "in_progress",
        },
        "repository": {"owner": {"login": "o"}, "name": "r"},
    }
    # Seed via upsert_jobs so the row passes all the latest-state filters.
    etag_poll.upsert_jobs("o", "r", 1, [job_payload["workflow_job"]])
    # The upsert_jobs path does not preserve started_at into payload_json the
    # way the listener does, so insert a real job row with the full payload.
    import sqlite3

    from waitbus._ulid import new as ulid_new

    with contextlib.closing(sqlite3.connect(empty_db)) as conn:
        conn.execute(
            "INSERT INTO events ("
            "delivery_id, source, event_type, owner, repo, status, "
            "received_at, payload_json, ingest_method, job_id, event_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "d-stall-seed",
                "github",
                "workflow_job",
                "o",
                "r",
                "in_progress",
                _t.time_ns(),
                json.dumps(job_payload),
                "etag_poll",
                777,
                ulid_new(),
            ),
        )
        conn.commit()
    inserted = etag_poll.emit_stall_synthetic_events("o", "r", 1)
    assert inserted == 1
    # Re-invocation MUST be idempotent: dedup keeps the count at 0.
    assert etag_poll.emit_stall_synthetic_events("o", "r", 1) == 0


def test_emit_stall_skips_payload_missing_started_at(
    empty_db: Path,
) -> None:
    """A job_payload without `started_at` is silently skipped."""
    import sqlite3
    import time as _t

    from waitbus._ulid import new as ulid_new

    job_payload = {
        "workflow_job": {"id": 888, "name": "no-start", "status": "in_progress"},
        "repository": {"owner": {"login": "o"}, "name": "r"},
    }
    with contextlib.closing(sqlite3.connect(empty_db)) as conn:
        conn.execute(
            "INSERT INTO events ("
            "delivery_id, source, event_type, owner, repo, status, "
            "received_at, payload_json, ingest_method, job_id, event_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "d-no-start",
                "github",
                "workflow_job",
                "o",
                "r",
                "in_progress",
                _t.time_ns(),
                json.dumps(job_payload),
                "etag_poll",
                888,
                ulid_new(),
            ),
        )
        conn.commit()
    assert etag_poll.emit_stall_synthetic_events("o", "r", 1) == 0


def test_emit_stall_skips_payload_with_bad_started_at(
    empty_db: Path,
) -> None:
    """A job_payload with un-parseable started_at is silently skipped."""
    import sqlite3
    import time as _t

    from waitbus._ulid import new as ulid_new

    job_payload = {
        "workflow_job": {
            "id": 889,
            "name": "bad-start",
            "status": "in_progress",
            "started_at": "not-an-iso-date",
        },
        "repository": {"owner": {"login": "o"}, "name": "r"},
    }
    with contextlib.closing(sqlite3.connect(empty_db)) as conn:
        conn.execute(
            "INSERT INTO events ("
            "delivery_id, source, event_type, owner, repo, status, "
            "received_at, payload_json, ingest_method, job_id, event_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "d-bad-start",
                "github",
                "workflow_job",
                "o",
                "r",
                "in_progress",
                _t.time_ns(),
                json.dumps(job_payload),
                "etag_poll",
                889,
                ulid_new(),
            ),
        )
        conn.commit()
    assert etag_poll.emit_stall_synthetic_events("o", "r", 1) == 0


def test_emit_stall_skips_fresh_job_under_threshold(
    empty_db: Path,
) -> None:
    """A job started seconds ago does NOT trip the stall."""
    import datetime as _dt
    import sqlite3
    import time as _t

    from waitbus._ulid import new as ulid_new

    # Use UTC explicitly; etag_poll parses "Z" as UTC.
    started_iso = _dt.datetime.fromtimestamp(int(_t.time()), tz=_dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    job_payload = {
        "workflow_job": {
            "id": 890,
            "name": "fresh",
            "status": "in_progress",
            "started_at": started_iso,
        },
        "repository": {"owner": {"login": "o"}, "name": "r"},
    }
    with contextlib.closing(sqlite3.connect(empty_db)) as conn:
        conn.execute(
            "INSERT INTO events ("
            "delivery_id, source, event_type, owner, repo, status, "
            "received_at, payload_json, ingest_method, job_id, event_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "d-fresh",
                "github",
                "workflow_job",
                "o",
                "r",
                "in_progress",
                _t.time_ns(),
                json.dumps(job_payload),
                "etag_poll",
                890,
                ulid_new(),
            ),
        )
        conn.commit()
    # 60-minute threshold; the job is fresh.
    assert etag_poll.emit_stall_synthetic_events("o", "r", 60) == 0


# --- main() happy path -----------------------------------------------------


def test_main_happy_path_polls_one_repo(empty_db: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """End-to-end main() with one watched repo, mocked gh + HTTP."""
    monkeypatch.setattr(etag_poll, "watched_repos", lambda: tmp_path / "watched.txt")
    monkeypatch.setattr(etag_poll, "etag_state", lambda: tmp_path / "etag.json")
    (tmp_path / "watched.txt").write_text("owner/repo\n")
    monkeypatch.setattr(etag_poll, "gh_token", lambda: "tok")
    body = json.dumps(
        {
            "workflow_runs": [
                {
                    "id": 1,
                    "name": "wf",
                    "head_branch": "main",
                    "head_sha": "x",
                    "status": "completed",
                    "conclusion": "success",
                },
            ]
        }
    ).encode("utf-8")
    resp = _StubResponse(200, body, etag='"new"')
    with patch("waitbus.etag_poll.urllib.request.urlopen", return_value=resp):
        rc = etag_poll.main()
    assert rc == 0
    state = json.loads((tmp_path / "etag.json").read_text())
    assert state.get("owner/repo") == '"new"'


def test_main_handles_304_branch(empty_db: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A 304 response logs the polled event but does not insert."""
    monkeypatch.setattr(etag_poll, "watched_repos", lambda: tmp_path / "watched.txt")
    monkeypatch.setattr(etag_poll, "etag_state", lambda: tmp_path / "etag.json")
    (tmp_path / "watched.txt").write_text("owner/repo\n")
    (tmp_path / "etag.json").write_text(json.dumps({"owner/repo": '"prev"'}))
    monkeypatch.setattr(etag_poll, "gh_token", lambda: "tok")
    err = urllib.error.HTTPError(
        url="x",
        code=304,
        msg="Not Modified",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,
    )
    with patch("waitbus.etag_poll.urllib.request.urlopen", side_effect=err):
        assert etag_poll.main() == 0


# --- main() error paths ----------------------------------------------------


def test_main_returns_2_when_db_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(etag_poll, "db_path", lambda: tmp_path / "missing.db")
    assert etag_poll.main() == 2


def test_main_returns_2_when_gh_token_fails(empty_db: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """gh_token raises EtagPollError → main converts to exit 2."""
    monkeypatch.setattr(etag_poll, "watched_repos", lambda: tmp_path / "watched.txt")
    (tmp_path / "watched.txt").write_text("owner/repo\n")

    def _raise_token() -> str:
        raise etag_poll.EtagPollError("gh failed")

    monkeypatch.setattr(etag_poll, "gh_token", _raise_token)
    assert etag_poll.main() == 2


def test_main_returns_0_when_no_repos_watched(empty_db: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(etag_poll, "watched_repos", lambda: tmp_path / "absent.txt")
    assert etag_poll.main() == 0


# --- doorbell ring batching invariants -------------------------------------


def test_upsert_jobs_rings_doorbell_once_per_batch(empty_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Regardless of how many jobs are inserted, exactly one doorbell ring
    fires per upsert_jobs call when at least one row is new.
    """
    from waitbus import _doorbell

    ring_calls: list[None] = []
    monkeypatch.setattr(_doorbell, "ring", lambda _path=None: ring_calls.append(None))

    jobs = [
        {"id": 201, "name": "j1", "head_branch": "main", "head_sha": "x", "status": "in_progress", "conclusion": None},
        {
            "id": 202,
            "name": "j2",
            "head_branch": "main",
            "head_sha": "x",
            "status": "completed",
            "conclusion": "success",
        },
        {
            "id": 203,
            "name": "j3",
            "head_branch": "main",
            "head_sha": "x",
            "status": "completed",
            "conclusion": "failure",
        },
    ]
    inserted = etag_poll.upsert_jobs("o", "r", 10, jobs)
    assert inserted == 3
    assert ring_calls == [None]


def test_upsert_jobs_does_not_ring_when_zero_inserts(empty_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When every job in the batch is a duplicate (IGNORE), no ring fires."""
    from waitbus import _doorbell

    ring_calls: list[None] = []
    monkeypatch.setattr(_doorbell, "ring", lambda _path=None: ring_calls.append(None))

    jobs = [
        {
            "id": 301,
            "name": "j",
            "head_branch": "main",
            "head_sha": "x",
            "status": "completed",
            "conclusion": "success",
        },
    ]
    # First call inserts; we reset ring_calls after.
    etag_poll.upsert_jobs("o", "r", 11, jobs)
    ring_calls.clear()

    # Second call: same delivery_id, all rows ignored — no ring.
    inserted = etag_poll.upsert_jobs("o", "r", 11, jobs)
    assert inserted == 0
    assert ring_calls == []
