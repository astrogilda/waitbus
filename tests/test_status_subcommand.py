"""Tests for `waitbus status` sub-command."""

from __future__ import annotations

import contextlib
import sqlite3
import sys
import time
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from waitbus.cli import app

runner = CliRunner()


@pytest.fixture()
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Redirect all _paths factories to tmp_path via env overrides."""
    state = tmp_path / ".local" / "state" / "waitbus"
    runtime = tmp_path / "run" / "waitbus"
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(state))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(runtime))
    yield tmp_path


@pytest.fixture()
def db_with_events(isolated_home: Path) -> Path:
    """Create an events DB with one row and return the DB path."""
    from waitbus._db import ensure_schema
    from waitbus._paths import db_path, state_dir

    state_dir().mkdir(parents=True, exist_ok=True)
    db = db_path()
    ensure_schema(db)
    now_ns = time.time_ns()
    with contextlib.closing(sqlite3.connect(str(db))) as conn, conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO events
              (event_id, delivery_id, source, owner, repo, event_type,
               received_at, run_id, workflow_name, head_branch, head_sha,
               status, ingest_method, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "01JTEST000000000000000001",
                "delivery-1",
                "github",
                "testowner",
                "testrepo",
                "workflow_run",
                now_ns,
                1,
                "CI",
                "main",
                "abc123",
                "completed",
                "webhook",
                "{}",
            ),
        )
        conn.commit()
    return db


# ---------------------------------------------------------------------------
# daemon-up scenario (Linux)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform != "linux", reason="Linux-only daemon check")
@pytest.mark.usefixtures("isolated_home", "db_with_events")
def test_status_daemons_up() -> None:
    """All daemons active: exit 0, prints rows_in_db and daemon lines."""
    active_result = MagicMock()
    active_result.stdout = "active\n"
    active_result.returncode = 0

    with patch("subprocess.run", return_value=active_result):
        result = runner.invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    assert "rows_in_db: 1" in result.output
    assert "last_event_age_seconds:" in result.output
    assert "daemon_listener: active" in result.output
    assert "daemon_broadcast: active" in result.output
    assert "daemon_etag_poll: active" in result.output
    assert "daemon_watchdog: active" in result.output


# ---------------------------------------------------------------------------
# daemon-down scenario (Linux)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform != "linux", reason="Linux-only daemon check")
@pytest.mark.usefixtures("isolated_home", "db_with_events")
def test_status_daemon_down_exits_1() -> None:
    """One daemon inactive: exit 1."""

    def _mock_run(cmd: list[str], **_kw: object) -> MagicMock:
        result = MagicMock()
        if "waitbus-listener.service" in cmd:
            result.stdout = "inactive\n"
        else:
            result.stdout = "active\n"
        result.returncode = 0
        return result

    with patch("subprocess.run", side_effect=_mock_run):
        result = runner.invoke(app, ["status"])

    assert result.exit_code == 1, result.output
    assert "daemon_listener: inactive" in result.output


# ---------------------------------------------------------------------------
# empty DB scenario
# ---------------------------------------------------------------------------


def test_status_no_events(isolated_home: Path) -> None:
    """DB missing: rows_in_db shows db_missing, last_event_age shows no_events."""
    # DB does not exist at all
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "active\n"
        mock_run.return_value.returncode = 0
        result = runner.invoke(app, ["status"])

    assert "rows_in_db: db_missing" in result.output
    assert "last_event_age_seconds: no_events" in result.output


def test_status_empty_db(isolated_home: Path) -> None:
    """DB present but empty: rows_in_db: 0, last_event_age: no_events."""
    from waitbus._db import ensure_schema
    from waitbus._paths import db_path, state_dir

    state_dir().mkdir(parents=True, exist_ok=True)
    ensure_schema(db_path())

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "active\n"
        mock_run.return_value.returncode = 0
        result = runner.invoke(app, ["status"])

    assert "rows_in_db: 0" in result.output
    assert "last_event_age_seconds: no_events" in result.output


# ---------------------------------------------------------------------------
# broadcast socket reporting
# ---------------------------------------------------------------------------


def test_status_broadcast_socket_missing(isolated_home: Path) -> None:
    """broadcast_socket: missing when socket file doesn't exist."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "active\n"
        mock_run.return_value.returncode = 0
        result = runner.invoke(app, ["status"])

    assert "broadcast_socket: missing" in result.output


def test_status_broadcast_socket_exists(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """broadcast_socket: exists when socket file is present."""
    from waitbus._paths import runtime_dir

    rt = runtime_dir()
    rt.mkdir(parents=True, exist_ok=True)
    sock = rt / "broadcast.sock"
    sock.touch()

    # Patch the platform to linux so the path logic is deterministic
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(rt.parent))

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "active\n"
        mock_run.return_value.returncode = 0
        result = runner.invoke(app, ["status"])

    assert "broadcast_socket: exists" in result.output
