"""Contract tests for ``waitbus events query``.

Covers the parse-time safety gates, the LIMIT injection rules, the JSON
vs text emission, and the integration path through the typer CLI. The
heavy lifting is done by ``waitbus.events_query`` so most tests
exercise that module directly; one smoke test invokes the typer app via
``CliRunner`` to assert the wire-up at ``cli.events_app``.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from waitbus import _db, cli, events_query

# ---------------------------------------------------------------------------
# parse-time gates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "select event_id from events",
        "  SELECT *\n  FROM events",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "with cte as (select 1) select * from cte",
        "SELECT 1;",  # trailing semicolon tolerated
    ],
)
def test_validate_accepts_select_and_cte(sql: str) -> None:
    """SELECT and WITH-rooted statements pass the parse-time gate."""
    out = events_query.validate(sql)
    assert out  # non-empty


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO events VALUES (1)",
        "UPDATE events SET conclusion='success'",
        "DELETE FROM events",
        "DROP TABLE events",
        "CREATE TABLE foo (id INT)",
        "ALTER TABLE events ADD COLUMN x INT",
        "REPLACE INTO events VALUES (1)",
        "VACUUM",
        "ANALYZE",
        "REINDEX",
    ],
)
def test_validate_rejects_writers(sql: str) -> None:
    """INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/etc. fail at parse time."""
    with pytest.raises(events_query.QueryRejectedError):
        events_query.validate(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "PRAGMA journal_mode",
        "ATTACH DATABASE 'evil.db' AS evil",
        "DETACH DATABASE evil",
    ],
)
def test_validate_rejects_pragma_attach_detach_at_leading_position(sql: str) -> None:
    """PRAGMA/ATTACH/DETACH fail even before the forbidden-token scan runs."""
    with pytest.raises(events_query.QueryRejectedError):
        events_query.validate(sql)


def test_validate_rejects_pragma_inside_select() -> None:
    """PRAGMA hidden inside a SELECT body is rejected by the forbidden-token scan.

    SQLite does not actually allow PRAGMA inside a SELECT, but a future
    SQLite version could; the parse-time scan rejects the token
    regardless so the read-only connection is never the only line of
    defense.
    """
    with pytest.raises(events_query.QueryRejectedError, match="PRAGMA"):
        events_query.validate("SELECT * FROM events WHERE PRAGMA = 1")


def test_validate_rejects_multi_statement() -> None:
    """A second statement after the first semicolon is rejected."""
    with pytest.raises(events_query.QueryRejectedError, match="multi-statement"):
        events_query.validate("SELECT 1; SELECT 2")


def test_validate_accepts_pragma_inside_string_literal() -> None:
    """A column value containing 'PRAGMA' as a string literal is not flagged."""
    # This particular SELECT yields no rows but must pass the parse gate.
    out = events_query.validate("SELECT 'PRAGMA foo' AS x")
    assert "SELECT" in out.upper()


def test_validate_rejects_empty_sql() -> None:
    """An empty string is rejected with a clear message."""
    with pytest.raises(events_query.QueryRejectedError):
        events_query.validate("")
    with pytest.raises(events_query.QueryRejectedError):
        events_query.validate("   \n\t  ")


def test_validate_rejects_comment_only() -> None:
    """Comment-only input must not slip past the gate."""
    with pytest.raises(events_query.QueryRejectedError):
        events_query.validate("-- just a comment")
    with pytest.raises(events_query.QueryRejectedError):
        events_query.validate("/* block comment */")


def test_validate_strips_leading_comment_before_select() -> None:
    """A comment preceding the SELECT does not flip the leading-keyword check."""
    out = events_query.validate("-- header\nSELECT 1")
    assert out.lstrip().upper().startswith("SELECT")


# ---------------------------------------------------------------------------
# LIMIT injection
# ---------------------------------------------------------------------------


def test_apply_limit_appends_when_absent() -> None:
    out = events_query._apply_limit("SELECT * FROM events", 50)
    assert out.endswith("LIMIT 50")


def test_apply_limit_caps_existing() -> None:
    """Operator's LIMIT 5000 is reduced to the default 1000 cap."""
    out = events_query._apply_limit("SELECT * FROM events LIMIT 5000", 1000)
    assert "LIMIT 1000" in out
    assert "LIMIT 5000" not in out


def test_apply_limit_keeps_smaller_existing() -> None:
    """Operator's LIMIT 10 stays at 10 when the cap is 1000."""
    out = events_query._apply_limit("SELECT * FROM events LIMIT 10", 1000)
    assert out.endswith("LIMIT 10")


def test_apply_limit_preserves_offset() -> None:
    out = events_query._apply_limit("SELECT * FROM events LIMIT 5000 OFFSET 100", 1000)
    assert "LIMIT 1000 OFFSET 100" in out


def test_apply_limit_none_is_no_op() -> None:
    """--no-limit passes ``limit=None`` and the SQL is untouched."""
    sql = "SELECT * FROM events"
    assert events_query._apply_limit(sql, None) == sql


def test_apply_limit_strips_trailing_semicolon() -> None:
    """A trailing semicolon does not confuse the regex anchor."""
    out = events_query._apply_limit("SELECT * FROM events;", 50)
    assert out.endswith("LIMIT 50")


# ---------------------------------------------------------------------------
# end-to-end execution against a real (tiny) DB
# ---------------------------------------------------------------------------


def _seed_three_rows(db_path: Path) -> None:
    """Insert three minimal events so SELECT * returns a stable set.

    Bypasses ``insert_event`` to avoid the doorbell ring + magnitude
    guards; the test only cares that the rows surface via SELECT.
    """
    _db.ensure_schema(db_path)
    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        conn.executemany(
            """
            INSERT INTO events (
                delivery_id, source, event_type, owner, repo,
                received_at, payload_json, ingest_method, event_id
            ) VALUES (?, 'github', 'workflow_run', 'astro', 'csb',
                      1500000000000000000, '{}', 'webhook', ?)
            """,
            [
                ("d1", "01HV0000000000000000000001"),
                ("d2", "01HV0000000000000000000002"),
                ("d3", "01HV0000000000000000000003"),
            ],
        )
        conn.commit()


def test_run_query_json_emits_array(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "events.db"
    _seed_three_rows(db)
    rc = events_query.run_query(
        events_query.QueryRequest(
            sql="SELECT event_id, event_type FROM events ORDER BY event_id",
            limit=1000,
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert len(data) == 3
    assert data[0]["event_type"] == "workflow_run"
    assert "event_id" in data[0]


def test_run_query_text_emits_blocks(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "events.db"
    _seed_three_rows(db)
    rc = events_query.run_query(
        events_query.QueryRequest(
            sql="SELECT event_id FROM events ORDER BY event_id",
            limit=1000,
            as_json=False,
            db_path=db,
        )
    )
    assert rc == 0
    out = capsys.readouterr().out
    # Three blocks separated by blank lines: count event_id: prefixes.
    assert out.count("event_id:") == 3


def test_run_query_empty_result_zero_exit(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Zero-row result returns exit code 0 and emits empty JSON array / no text."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    rc = events_query.run_query(
        events_query.QueryRequest(
            sql="SELECT event_id FROM events",
            limit=1000,
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 0
    assert json.loads(capsys.readouterr().out) == []


def test_run_query_limit_caps_result_set(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """--limit 2 caps the result even though three rows are available."""
    db = tmp_path / "events.db"
    _seed_three_rows(db)
    rc = events_query.run_query(
        events_query.QueryRequest(
            sql="SELECT event_id FROM events ORDER BY event_id",
            limit=2,
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 2


def test_run_query_no_limit_returns_all(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """--no-limit (limit=None) skips injection so all rows surface."""
    db = tmp_path / "events.db"
    _seed_three_rows(db)
    rc = events_query.run_query(
        events_query.QueryRequest(
            sql="SELECT event_id FROM events ORDER BY event_id",
            limit=None,
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 3


def test_run_query_rejects_writer_with_exit_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """An INSERT is rejected at parse time before the DB is opened."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    rc = events_query.run_query(
        events_query.QueryRequest(
            sql="INSERT INTO events (delivery_id) VALUES ('x')",
            limit=1000,
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "INSERT" in err
    assert "events query" in err


def test_run_query_readonly_connection_blocks_writes_via_function(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Even if a writer slipped past the parser, the readonly connection blocks it.

    Smoke test for the defense-in-depth posture: we manually open the
    same readonly connection ``run_query`` uses and confirm SQLite
    refuses an INSERT.
    """
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    with contextlib.closing(_db.open_conn(db, readonly=True)) as conn, pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO events (delivery_id) VALUES ('x')")


def test_run_query_missing_db_hints_init(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A missing DB file surfaces a remediation hint, not SQLite's raw error."""
    rc = events_query.run_query(
        events_query.QueryRequest(
            sql="SELECT 1",
            limit=1000,
            as_json=True,
            db_path=tmp_path / "does-not-exist.db",
        )
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "waitbus init" in err


def test_run_query_syntax_error_surfaces_sqlite_message(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A malformed SELECT yields SQLite's OperationalError message verbatim."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    rc = events_query.run_query(
        events_query.QueryRequest(
            sql="SELECT no_such_column FROM events",
            limit=1000,
            as_json=True,
            db_path=db,
        )
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "sqlite error" in err
    assert "no_such_column" in err


# ---------------------------------------------------------------------------
# typer CLI wire-up
# ---------------------------------------------------------------------------


def test_cli_events_query_wired(tmp_path: Path) -> None:
    """``waitbus events query SELECT 1`` invokes through to run_query."""
    db = tmp_path / "events.db"
    _seed_three_rows(db)
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["events", "query", "SELECT event_id FROM events", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert len(data) == 3


def test_cli_events_query_help_lists_columns() -> None:
    """The --help output names the events table columns for operators."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["events", "query", "--help"])
    assert result.exit_code == 0
    assert "delivery_id" in result.output
    assert "event_id" in result.output


def test_cli_events_query_rejects_insert(tmp_path: Path) -> None:
    """The typer surface returns exit code 2 for a writer."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["events", "query", "INSERT INTO events VALUES (1)", "--db", str(db)],
    )
    assert result.exit_code == 2
