"""Unit tests for the DuckDB-backed `waitbus events analyze` path.

Exercises the real DuckDB sqlite-scanner attachment against a real
on-disk SQLite events DB — no mocking of the engine boundary. duckdb
ships behind the optional `analyze` extra; tests skip cleanly when it
is not installed so the base test run does not require the extra.
"""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path

import pytest

from waitbus import _db, events_analyze

duckdb = pytest.importorskip("duckdb")


def _seed(db_path: Path) -> None:
    _db.ensure_schema(db_path)
    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        conn.executemany(
            """
            INSERT INTO events (
                delivery_id, source, event_type, owner, repo,
                received_at, payload_json, ingest_method, event_id
            ) VALUES (?, 'github', ?, 'astro', 'csb',
                      1500000000000000000, '{}', 'webhook', ?)
            """,
            [
                ("d1", "workflow_run", "01HV0000000000000000000001"),
                ("d2", "workflow_run", "01HV0000000000000000000002"),
                ("d3", "workflow_job", "01HV0000000000000000000003"),
            ],
        )
        conn.commit()


def test_analyze_json_aggregate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = tmp_path / "events.db"
    _seed(db)
    rc = events_analyze.run_analyze(
        events_analyze.AnalyzeRequest(
            sql="SELECT event_type, count(*) AS n FROM ev.events GROUP BY event_type ORDER BY event_type",
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert '"event_type": "workflow_job"' in out
    assert '"n": 2' in out  # two workflow_run rows


def test_analyze_window_function(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A window function — unavailable in the plain sqlite query path."""
    db = tmp_path / "events.db"
    _seed(db)
    rc = events_analyze.run_analyze(
        events_analyze.AnalyzeRequest(
            sql="SELECT event_id, row_number() OVER (ORDER BY event_id) AS rn FROM ev.events QUALIFY rn <= 2",
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 0
    assert '"rn": 2' in capsys.readouterr().out


def test_analyze_rejects_writer(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = tmp_path / "events.db"
    _seed(db)
    rc = events_analyze.run_analyze(
        events_analyze.AnalyzeRequest(
            sql="DELETE FROM ev.events",
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 2
    assert "waitbus events analyze:" in capsys.readouterr().err


def test_analyze_missing_db(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = events_analyze.run_analyze(
        events_analyze.AnalyzeRequest(
            sql="SELECT 1",
            as_json=True,
            db_path=tmp_path / "nope.db",
        )
    )
    assert rc == 2
    assert "events DB not found" in capsys.readouterr().err


def test_analyze_text_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = tmp_path / "events.db"
    _seed(db)
    rc = events_analyze.run_analyze(
        events_analyze.AnalyzeRequest(
            sql="SELECT count(*) AS total FROM ev.events",
            as_json=False,
            db_path=db,
        )
    )
    assert rc == 0
    assert "total: 3" in capsys.readouterr().out


def test_analyze_remediation_hint_when_duckdb_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A missing ``analyze`` extra surfaces the documented remediation hint.

    Uses ``sys.modules['duckdb'] = None`` to force ``import duckdb`` inside
    ``run_analyze`` to raise ``ImportError`` even when the extra is actually
    installed in the test environment. Verifies the function catches it,
    prints ``_MISSING_DUCKDB_HINT`` to stderr, and returns exit code 2.
    """
    import sys

    db = tmp_path / "events.db"
    _seed(db)
    monkeypatch.setitem(sys.modules, "duckdb", None)
    rc = events_analyze.run_analyze(
        events_analyze.AnalyzeRequest(
            sql="SELECT 1",
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 2
    assert events_analyze._MISSING_DUCKDB_HINT in capsys.readouterr().err
