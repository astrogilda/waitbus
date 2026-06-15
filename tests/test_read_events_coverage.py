"""Targeted unit tests for read_events query/format/dispatch paths.

Covers: fetch, fetch_jobs_for_run (edge cases), format_text, format_job_line,
_frame_fallback_summary, watch_bookmark_name, detect_repo, _build_parser,
_resolve_mode, _emit_query, and the watch() error paths.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import socket
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from waitbus import _db, read_events
from waitbus._broadcast_sub import (
    BookmarkCursor,
    BroadcastConnectionError,
    SubscriberHandle,
    SubscriberLaggedError,
    TokenRequiredError,
    WaitOutcome,
)
from waitbus._secrets import SecretNotConfigured
from waitbus._types import EventInsert

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ns() -> int:
    return time.time_ns()


def _make_row(
    tmp_db_path: Path,
    *,
    delivery_id: str = "d-test",
    event_type: str = "workflow_run",
    owner: str = "acme",
    repo: str = "widgets",
    run_id: int | None = 42,
    job_id: int | None = None,
    job_name: str | None = None,
    parent_run_id: int | None = None,
    head_branch: str = "main",
    head_sha: str = "abc123",
    status: str = "completed",
    conclusion: str = "success",
    workflow_name: str = "CI",
    payload_json: str = "{}",
    ingest_method: str = "webhook",
) -> sqlite3.Row:
    """Insert one event row and return it via SELECT so callers get sqlite3.Row."""
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        _db.insert_event(
            conn,
            EventInsert(
                delivery_id=delivery_id,
                source="github",
                event_type=event_type,
                owner=owner,
                repo=repo,
                received_at=_ns(),
                payload_json=payload_json,
                ingest_method=ingest_method,
                run_id=run_id,
                workflow_name=workflow_name,
                head_branch=head_branch,
                head_sha=head_sha,
                status=status,
                conclusion=conclusion,
                job_id=job_id,
                job_name=job_name,
                parent_run_id=parent_run_id,
            ),
        )
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        conn.row_factory = sqlite3.Row
        row: sqlite3.Row = conn.execute("SELECT * FROM events WHERE delivery_id=?", (delivery_id,)).fetchone()
        return row


# ---------------------------------------------------------------------------
# fetch()
# ---------------------------------------------------------------------------


def test_fetch_returns_empty_when_db_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    absent = tmp_path / "no-such.db"
    monkeypatch.setattr(read_events, "db_path", lambda: absent)
    rows = read_events.fetch("acme", "widgets", "workflow_run", 10)
    assert rows == []


def test_fetch_returns_rows_newest_first(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    _make_row(tmp_db_path, delivery_id="d-old", run_id=1)
    _make_row(tmp_db_path, delivery_id="d-new", run_id=2)
    rows = read_events.fetch("acme", "widgets", "workflow_run", 10)
    assert len(rows) == 2
    # Newest-first: d-new has a later received_at.
    assert rows[0]["delivery_id"] == "d-new"


def test_fetch_limits_result_count(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    for i in range(5):
        _make_row(tmp_db_path, delivery_id=f"d-{i}", run_id=i)
    rows = read_events.fetch("acme", "widgets", "workflow_run", 2)
    assert len(rows) == 2


def test_fetch_filters_by_event_type(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    _make_row(tmp_db_path, delivery_id="d-run", event_type="workflow_run", run_id=1)
    _make_row(
        tmp_db_path,
        delivery_id="d-job",
        event_type="workflow_job",
        job_id=7,
        job_name="build",
        parent_run_id=1,
        run_id=None,
    )
    rows = read_events.fetch("acme", "widgets", "workflow_job", 10)
    assert all(r["event_type"] == "workflow_job" for r in rows)


def test_fetch_filters_by_owner_repo(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    _make_row(tmp_db_path, delivery_id="d-acme", owner="acme", repo="widgets", run_id=1)
    _make_row(tmp_db_path, delivery_id="d-other", owner="other", repo="repo", run_id=2)
    rows = read_events.fetch("acme", "widgets", "workflow_run", 10)
    assert len(rows) == 1
    assert rows[0]["delivery_id"] == "d-acme"


# ---------------------------------------------------------------------------
# fetch_jobs_for_run() — edge cases not covered by watch test file
# ---------------------------------------------------------------------------


def test_fetch_jobs_for_run_returns_empty_when_run_id_none(tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    result = read_events.fetch_jobs_for_run("acme", "widgets", None)  # type: ignore[arg-type]
    assert result == []


def test_fetch_jobs_for_run_returns_empty_when_db_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_path / "absent.db")
    result = read_events.fetch_jobs_for_run("acme", "widgets", 99)
    assert result == []


# ---------------------------------------------------------------------------
# format_text()
# ---------------------------------------------------------------------------


def test_format_text_workflow_run_basic(tmp_db_path: Path) -> None:
    payload = json.dumps(
        {
            "workflow_run": {
                "event": "push",
                "display_title": "chore: bump deps",
            }
        }
    )
    row = _make_row(
        tmp_db_path,
        delivery_id="d-fmt-run",
        event_type="workflow_run",
        head_branch="main",
        workflow_name="CI",
        status="completed",
        conclusion="success",
        payload_json=payload,
    )
    result = read_events.format_text(row)
    assert "main" in result
    assert "CI" in result
    assert "completed/success" in result
    assert "chore: bump deps" in result
    assert "push" in result


def test_format_text_workflow_run_no_title(tmp_db_path: Path) -> None:
    payload = json.dumps({"workflow_run": {"event": "pull_request"}})
    row = _make_row(
        tmp_db_path,
        delivery_id="d-fmt-run2",
        event_type="workflow_run",
        payload_json=payload,
    )
    result = read_events.format_text(row)
    assert "pull_request" in result
    assert "completed/success" in result


def test_format_text_workflow_run_title_from_head_commit(tmp_db_path: Path) -> None:
    payload = json.dumps(
        {
            "workflow_run": {
                "event": "push",
                "head_commit": {"message": "fix: resolve race\nsecond line"},
            }
        }
    )
    row = _make_row(
        tmp_db_path,
        delivery_id="d-fmt-run3",
        event_type="workflow_run",
        payload_json=payload,
    )
    result = read_events.format_text(row)
    assert "fix: resolve race" in result
    assert "second line" not in result  # only first line of commit message


def test_format_text_workflow_run_pending_conclusion(tmp_db_path: Path) -> None:
    row = _make_row(
        tmp_db_path,
        delivery_id="d-fmt-pending",
        event_type="workflow_run",
        status="in_progress",
        conclusion=None,  # type: ignore[arg-type]
    )
    result = read_events.format_text(row)
    assert "in_progress/pending" in result


def test_format_text_workflow_job_basic(tmp_db_path: Path) -> None:
    payload = json.dumps({"workflow_job": {"head_branch": "feature/x"}})
    row = _make_row(
        tmp_db_path,
        delivery_id="d-fmt-job",
        event_type="workflow_job",
        job_id=101,
        job_name="unit-tests",
        parent_run_id=55,
        run_id=None,
        head_branch="feature/x",
        status="completed",
        conclusion="failure",
        payload_json=payload,
    )
    result = read_events.format_text(row)
    assert "feature/x" in result
    assert "unit-tests" in result
    assert "101" in result
    assert "55" in result
    assert "completed/failure" in result


def test_format_text_workflow_job_no_branch_falls_back_to_payload(
    tmp_db_path: Path,
) -> None:
    payload = json.dumps({"workflow_job": {"head_branch": "payload-branch"}})
    row = _make_row(
        tmp_db_path,
        delivery_id="d-fmt-job2",
        event_type="workflow_job",
        job_id=200,
        job_name="lint",
        parent_run_id=77,
        run_id=None,
        head_branch=None,  # type: ignore[arg-type]
        payload_json=payload,
    )
    result = read_events.format_text(row)
    assert "payload-branch" in result


def test_format_text_workflow_job_no_branch_uses_question_mark(
    tmp_db_path: Path,
) -> None:
    row = _make_row(
        tmp_db_path,
        delivery_id="d-fmt-job3",
        event_type="workflow_job",
        job_id=300,
        job_name="build",
        parent_run_id=88,
        run_id=None,
        head_branch=None,  # type: ignore[arg-type]
        payload_json="{}",
    )
    result = read_events.format_text(row)
    assert result.startswith("?")


def test_format_text_invalid_payload_json(tmp_db_path: Path) -> None:
    row = _make_row(
        tmp_db_path,
        delivery_id="d-fmt-bad-json",
        event_type="workflow_run",
        payload_json="not-valid-json",
    )
    result = read_events.format_text(row)
    # Should not raise; falls back to empty payload dict.
    assert isinstance(result, str)
    assert "?" in result  # event field defaults to "?"


# ---------------------------------------------------------------------------
# format_job_line()
# ---------------------------------------------------------------------------


def test_format_job_line_basic(tmp_db_path: Path) -> None:
    row = _make_row(
        tmp_db_path,
        delivery_id="d-jl-1",
        event_type="workflow_job",
        job_id=42,
        job_name="test-suite",
        parent_run_id=10,
        run_id=None,
        status="completed",
        conclusion="success",
    )
    result = read_events.format_job_line("acme", "widgets", row)
    assert result.startswith("    ")
    assert "test-suite" in result
    assert "42" in result
    assert "completed/success" in result


def test_format_job_line_pending_conclusion(tmp_db_path: Path) -> None:
    row = _make_row(
        tmp_db_path,
        delivery_id="d-jl-pending",
        event_type="workflow_job",
        job_id=99,
        job_name="build",
        parent_run_id=20,
        run_id=None,
        status="in_progress",
        conclusion=None,  # type: ignore[arg-type]
    )
    result = read_events.format_job_line("acme", "widgets", row)
    assert "in_progress/pending" in result


# ---------------------------------------------------------------------------
# _frame_fallback_summary()
# ---------------------------------------------------------------------------


def test_frame_fallback_summary_full_frame() -> None:
    frame: dict[str, Any] = {
        "owner": "acme",
        "repo": "widgets",
        "event_type": "workflow_run",
        "event_id": "01HZXXXYYY",
    }
    result = read_events._frame_fallback_summary(frame)
    assert "acme/widgets" in result
    assert "workflow_run" in result
    assert "01HZXXXYYY" in result


def test_frame_fallback_summary_missing_fields() -> None:
    result = read_events._frame_fallback_summary({})
    assert "?/?" in result
    assert "?" in result


def test_frame_fallback_summary_uses_kind_when_no_event_type() -> None:
    frame: dict[str, Any] = {"kind": "heartbeat", "event_id": "abc"}
    result = read_events._frame_fallback_summary(frame)
    assert "heartbeat" in result


# ---------------------------------------------------------------------------
# watch_bookmark_name()
# ---------------------------------------------------------------------------


def test_watch_bookmark_name_format() -> None:
    assert read_events.watch_bookmark_name("acme", "widgets") == "watch-acme-widgets"


def test_watch_bookmark_name_with_special_chars() -> None:
    # GitHub org/repo names allow dots and hyphens.
    name = read_events.watch_bookmark_name("my-org", "repo.name")
    assert name == "watch-my-org-repo.name"
    # Must be a valid BookmarkCursor name (no ValueError).
    BookmarkCursor.validate_name(name)


# ---------------------------------------------------------------------------
# detect_repo()
# ---------------------------------------------------------------------------


def test_detect_repo_parses_ssh_remote() -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="git@github.com:acme/widgets.git\n")
        result = read_events.detect_repo()
    assert result == ("acme", "widgets")


def test_detect_repo_parses_https_remote() -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="https://github.com/my-org/my-repo\n")
        result = read_events.detect_repo()
    assert result == ("my-org", "my-repo")


def test_detect_repo_returns_none_on_non_github_remote() -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="https://gitlab.com/acme/widgets.git\n")
        result = read_events.detect_repo()
    assert result is None


def test_detect_repo_returns_none_on_git_not_found() -> None:
    with patch("subprocess.run", side_effect=FileNotFoundError):
        result = read_events.detect_repo()
    assert result is None


def test_detect_repo_returns_none_on_called_process_error() -> None:
    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(128, "git")):
        result = read_events.detect_repo()
    assert result is None


def test_detect_repo_returns_none_on_timeout() -> None:
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("git", 5)):
        result = read_events.detect_repo()
    assert result is None


def test_detect_repo_parses_https_remote_with_trailing_git() -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="https://github.com/org/repo.git\n")
        result = read_events.detect_repo()
    assert result == ("org", "repo")


# ---------------------------------------------------------------------------
# _build_parser()
# ---------------------------------------------------------------------------


def test_build_parser_returns_argument_parser() -> None:
    p = read_events._build_parser()
    assert isinstance(p, argparse.ArgumentParser)


def test_build_parser_defaults() -> None:
    p = read_events._build_parser()
    args = p.parse_args([])
    assert args.event_type == "workflow_run"
    assert args.watch is False
    assert args.all_events is False
    assert args.include_jobs is False
    assert args.owner is None
    assert args.repo is None
    assert args.since is None


def test_build_parser_watch_flag() -> None:
    p = read_events._build_parser()
    args = p.parse_args(["--watch"])
    assert args.watch is True


def test_build_parser_last_n() -> None:
    p = read_events._build_parser()
    args = p.parse_args(["--last-n", "5"])
    assert args.last_n == 5


def test_build_parser_json_flag() -> None:
    p = read_events._build_parser()
    args = p.parse_args(["--json"])
    assert args.json is True


def test_build_parser_owner_repo() -> None:
    p = read_events._build_parser()
    args = p.parse_args(["--owner", "acme", "--repo", "widgets"])
    assert args.owner == "acme"
    assert args.repo == "widgets"


def test_build_parser_since() -> None:
    p = read_events._build_parser()
    args = p.parse_args(["--since", "01HZABCDEF"])
    assert args.since == "01HZABCDEF"


def test_build_parser_mutually_exclusive_watch_latest() -> None:
    p = read_events._build_parser()
    with pytest.raises(SystemExit):
        p.parse_args(["--watch", "--latest"])


# ---------------------------------------------------------------------------
# _resolve_mode()
# ---------------------------------------------------------------------------


def _ns_for_args(**kwargs: Any) -> argparse.Namespace:
    defaults = {
        "owner": None,
        "repo": None,
        "event_type": "workflow_run",
        "include_jobs": False,
        "latest": False,
        "last_n": None,
        "watch": False,
        "all_events": False,
        "since": None,
        "text": False,
        "json": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_resolve_mode_query_with_explicit_owner_repo() -> None:
    args = _ns_for_args(owner="acme", repo="widgets")
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._QueryMode)
    assert mode.owner == "acme"
    assert mode.repo == "widgets"
    assert mode.event_type == "workflow_run"
    assert mode.limit == 1
    assert mode.include_jobs is False
    assert mode.as_json is False


def test_resolve_mode_query_last_n() -> None:
    args = _ns_for_args(owner="acme", repo="widgets", last_n=5)
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._QueryMode)
    assert mode.limit == 5


def test_resolve_mode_query_as_json() -> None:
    args = _ns_for_args(owner="acme", repo="widgets", json=True)
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._QueryMode)
    assert mode.as_json is True


def test_resolve_mode_query_include_jobs() -> None:
    args = _ns_for_args(owner="acme", repo="widgets", include_jobs=True)
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._QueryMode)
    assert mode.include_jobs is True


def test_resolve_mode_returns_none_when_no_owner_repo_and_detect_fails(
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = _ns_for_args()
    with patch.object(read_events, "detect_repo", return_value=None):
        result = read_events._resolve_mode(args)
    assert result is None
    err = capsys.readouterr().err
    assert "--owner/--repo" in err


def test_resolve_mode_auto_detects_owner_repo() -> None:
    args = _ns_for_args()
    with patch.object(read_events, "detect_repo", return_value=("auto-org", "auto-repo")):
        mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._QueryMode)
    assert mode.owner == "auto-org"
    assert mode.repo == "auto-repo"


def test_resolve_mode_watch_with_explicit_owner_repo() -> None:
    args = _ns_for_args(owner="acme", repo="widgets", watch=True)
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._WatchMode)
    assert mode.filters == ["acme/widgets"]
    assert mode.event_types is None


def test_resolve_mode_watch_all_events_no_owner_repo() -> None:
    args = _ns_for_args(watch=True, all_events=True)
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._WatchMode)
    assert mode.filters == ["*"]
    assert mode.cursor is None


def test_resolve_mode_watch_all_events_with_owner_repo() -> None:
    args = _ns_for_args(owner="acme", repo="widgets", watch=True, all_events=True)
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._WatchMode)
    assert mode.filters == ["*"]
    assert mode.cursor is not None
    assert mode.cursor.name == "watch-acme-widgets"


def test_resolve_mode_watch_custom_event_type() -> None:
    args = _ns_for_args(owner="acme", repo="widgets", watch=True, event_type="workflow_job")
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._WatchMode)
    assert mode.event_types == ["workflow_job"]


def test_resolve_mode_watch_default_event_type_not_applied_as_filter() -> None:
    """The query-mode default 'workflow_run' must NOT become a watch filter."""
    args = _ns_for_args(owner="acme", repo="widgets", watch=True, event_type="workflow_run")
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._WatchMode)
    assert mode.event_types is None


def test_resolve_mode_watch_with_since() -> None:
    args = _ns_for_args(owner="acme", repo="widgets", watch=True, since="01HZXYZ")
    mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._WatchMode)
    assert mode.since == "01HZXYZ"


def test_resolve_mode_watch_detect_repo_when_no_owner_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WAITBUS_STATE_DIR", "/tmp/test-ci-state-dir")
    args = _ns_for_args(watch=True)
    with patch.object(read_events, "detect_repo", return_value=("detected-org", "detected-repo")):
        mode = read_events._resolve_mode(args)
    assert isinstance(mode, read_events._WatchMode)
    assert mode.filters == ["detected-org/detected-repo"]


def test_resolve_mode_watch_detect_fails_returns_none(
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = _ns_for_args(watch=True)
    with patch.object(read_events, "detect_repo", return_value=None):
        result = read_events._resolve_mode(args)
    assert result is None
    assert "--owner/--repo" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# _emit_query()
# ---------------------------------------------------------------------------


def test_emit_query_text_with_rows(
    tmp_db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    _make_row(tmp_db_path, delivery_id="d-eq1")
    mode = read_events._QueryMode(
        owner="acme",
        repo="widgets",
        event_type="workflow_run",
        limit=10,
        include_jobs=False,
        as_json=False,
    )
    rc = read_events._emit_query(mode)
    assert rc == 0
    out = capsys.readouterr().out
    assert "acme/widgets" in out


def test_emit_query_text_no_rows(
    tmp_db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    mode = read_events._QueryMode(
        owner="acme",
        repo="widgets",
        event_type="workflow_run",
        limit=10,
        include_jobs=False,
        as_json=False,
    )
    rc = read_events._emit_query(mode)
    assert rc == 0
    out = capsys.readouterr().out
    assert "no workflow_run events cached" in out
    assert "Hint:" in out


def test_emit_query_json_output(
    tmp_db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    _make_row(tmp_db_path, delivery_id="d-json1", run_id=7)
    mode = read_events._QueryMode(
        owner="acme",
        repo="widgets",
        event_type="workflow_run",
        limit=10,
        include_jobs=False,
        as_json=True,
    )
    rc = read_events._emit_query(mode)
    assert rc == 0
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert parsed[0]["delivery_id"] == "d-json1"


def test_emit_query_json_empty_db(
    tmp_db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    mode = read_events._QueryMode(
        owner="acme",
        repo="widgets",
        event_type="workflow_run",
        limit=10,
        include_jobs=False,
        as_json=True,
    )
    rc = read_events._emit_query(mode)
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed == []


def test_emit_query_include_jobs(
    tmp_db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    _make_row(tmp_db_path, delivery_id="d-run-ij", event_type="workflow_run", run_id=5)
    _make_row(
        tmp_db_path,
        delivery_id="d-job-ij",
        event_type="workflow_job",
        job_id=10,
        job_name="lint",
        parent_run_id=5,
        run_id=None,
        owner="acme",
        repo="widgets",
    )
    mode = read_events._QueryMode(
        owner="acme",
        repo="widgets",
        event_type="workflow_run",
        limit=10,
        include_jobs=True,
        as_json=False,
    )
    rc = read_events._emit_query(mode)
    assert rc == 0
    out = capsys.readouterr().out
    assert "lint" in out
    # Job line uses 4-space indent.
    assert any(ln.startswith("    ") for ln in out.splitlines())


def test_emit_query_include_jobs_json(
    tmp_db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    _make_row(tmp_db_path, delivery_id="d-run-json", event_type="workflow_run", run_id=6)
    _make_row(
        tmp_db_path,
        delivery_id="d-job-json",
        event_type="workflow_job",
        job_id=20,
        job_name="test",
        parent_run_id=6,
        run_id=None,
        owner="acme",
        repo="widgets",
    )
    mode = read_events._QueryMode(
        owner="acme",
        repo="widgets",
        event_type="workflow_run",
        limit=10,
        include_jobs=True,
        as_json=True,
    )
    rc = read_events._emit_query(mode)
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert len(parsed) == 1
    assert "jobs" in parsed[0]
    assert len(parsed[0]["jobs"]) == 1
    assert parsed[0]["jobs"][0]["job_name"] == "test"


# ---------------------------------------------------------------------------
# watch() — error paths (mocked; no real socket needed)
# ---------------------------------------------------------------------------


def _make_socket_pair() -> tuple[socket.socket, socket.socket]:
    """Return a connected AF_UNIX socketpair for in-process tests."""
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    return a, b


def _clean_outcome(
    *,
    matched: bool = False,
    timed_out: bool = False,
    cancelled: bool = False,
    peer_closed: bool = True,
    framing_error: bool = False,
) -> WaitOutcome:
    return WaitOutcome(
        matched=matched,
        timed_out=timed_out,
        cancelled=cancelled,
        peer_closed=peer_closed,
        framing_error=framing_error,
    )


def test_watch_returns_0_on_clean_eof(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client, server = _make_socket_pair()
    server.close()
    handle = SubscriberHandle(sock=client)
    clean = _clean_outcome(peer_closed=True, framing_error=False)
    with (
        patch.object(read_events, "open_subscriber", return_value=handle),
        patch.object(read_events, "await_predicate", return_value=clean),
    ):
        rc = read_events.watch(
            filters=["acme/widgets"],
            event_types=None,
            since=None,
            cursor=None,
        )
    assert rc == 0


def test_watch_returns_1_on_framing_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client, server = _make_socket_pair()
    server.close()
    handle = SubscriberHandle(sock=client)
    framing = _clean_outcome(peer_closed=True, framing_error=True)
    with (
        patch.object(read_events, "open_subscriber", return_value=handle),
        patch.object(read_events, "await_predicate", return_value=framing),
    ):
        rc = read_events.watch(
            filters=["acme/widgets"],
            event_types=None,
            since=None,
            cursor=None,
        )
    assert rc == 1
    err = capsys.readouterr().err
    assert "framing error" in err


def test_watch_returns_2_on_broadcast_connection_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    err_instance = BroadcastConnectionError("broadcast socket unreachable", remediation="start the daemon")
    with patch.object(read_events, "open_subscriber", side_effect=err_instance):
        rc = read_events.watch(
            filters=["acme/widgets"],
            event_types=None,
            since=None,
            cursor=None,
        )
    assert rc == 2
    err = capsys.readouterr().err
    assert "waitbus --watch" in err


def test_watch_returns_2_on_subscriber_lagged_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    err_instance = SubscriberLaggedError("subscriber lag exceeded", remediation="reconnect with narrower filters")
    with patch.object(read_events, "open_subscriber", side_effect=err_instance):
        rc = read_events.watch(
            filters=["acme/widgets"],
            event_types=None,
            since=None,
            cursor=None,
        )
    assert rc == 2


def test_watch_returns_2_on_token_required_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    err_instance = TokenRequiredError("token required", remediation="configure broadcast token")
    with patch.object(read_events, "open_subscriber", side_effect=err_instance):
        rc = read_events.watch(
            filters=["acme/widgets"],
            event_types=None,
            since=None,
            cursor=None,
        )
    assert rc == 2


def test_watch_returns_2_on_secret_not_configured(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with patch.object(
        read_events,
        "open_subscriber",
        side_effect=SecretNotConfigured("broadcast-token not configured"),
    ):
        rc = read_events.watch(
            filters=["acme/widgets"],
            event_types=None,
            since=None,
            cursor=None,
        )
    assert rc == 2
    err = capsys.readouterr().err
    assert "waitbus --watch" in err


# ---------------------------------------------------------------------------
# main() end-to-end dispatch
# ---------------------------------------------------------------------------


def test_main_returns_0_when_no_owner_repo_detected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(read_events, "detect_repo", lambda: None)
    # ensure_state_dirs must not fail in tmp env.
    with patch.object(read_events, "ensure_state_dirs"):
        rc = read_events.main(["--event-type", "workflow_run"])
    assert rc == 0


def test_main_dispatches_to_emit_query(
    tmp_db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(read_events, "db_path", lambda: tmp_db_path)
    with patch.object(read_events, "ensure_state_dirs"):
        rc = read_events.main(["--owner", "acme", "--repo", "widgets"])
    assert rc == 0
    out = capsys.readouterr().out
    # No events cached → remediation hint.
    assert "no workflow_run events cached" in out
