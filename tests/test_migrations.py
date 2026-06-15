"""Contract tests for the schema-migrations tooling.

Covers the public surface of ``waitbus.migrations``:

* discover_migrations picks up numbered .sql files and ignores anything
  else in the directory.
* apply_pending applies each file inside one transaction, records the
  row in schema_migrations, and is idempotent on a second call.
* read_applied returns rows ordered by sequence number.
* Checksum drift on a previously-applied file fails loud.
* Sequence gaps on disk fail loud.
* --dry-run prints SQL without mutating the DB.
* --status prints applied + pending without mutating the DB.
* --to NNNN bounds the apply pass.
* A same-stem .py hook fires after the SQL block inside the same
  transaction.
"""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from waitbus import migrations as migrations_pkg
from waitbus._db import ensure_schema, open_conn
from waitbus.cli import app


def _bootstrap_db(tmp_path: Path) -> Path:
    """Materialise a fresh events DB so the migrations tooling has a
    starting point that matches a real install. Mirrors what
    waitbus init does before mark_baseline_applied."""
    db = tmp_path / "events.db"
    ensure_schema(db)
    return db


def _write_migration(directory: Path, seq: int, slug: str, sql: str, py: str | None = None) -> None:
    """Helper that drops one migration into a tmp_path-rooted migrations
    directory. The .py hook is optional; tests that need one pass the
    function-body text."""
    (directory / f"{seq:04d}_{slug}.sql").write_text(sql)
    if py is not None:
        (directory / f"{seq:04d}_{slug}.py").write_text(py)


def test_discover_migrations_ignores_non_sql_files(tmp_path: Path) -> None:
    """discover_migrations skips __init__.py, README files, and any
    .sql file not matching the NNNN_<slug>.sql shape."""
    (tmp_path / "__init__.py").write_text("")
    (tmp_path / "README.md").write_text("# notes")
    (tmp_path / "0001_first.sql").write_text("SELECT 1;")
    (tmp_path / "garbage.sql").write_text("SELECT 1;")
    (tmp_path / "0002-bad-slug.sql").write_text("SELECT 1;")
    found = migrations_pkg.discover_migrations(tmp_path)
    assert [(m.sequence_number, m.slug) for m in found] == [(1, "first")]


def test_discover_migrations_rejects_duplicate_sequence(tmp_path: Path) -> None:
    """Two files with the same sequence number raise a clear error."""
    _write_migration(tmp_path, 1, "first", "SELECT 1;")
    _write_migration(tmp_path, 1, "second", "SELECT 1;")
    with pytest.raises(RuntimeError, match="duplicate migration sequence number"):
        migrations_pkg.discover_migrations(tmp_path)


def test_apply_pending_applies_files_and_records_them(tmp_path: Path) -> None:
    """apply_pending executes the SQL, records the row, and the second
    call is a no-op."""
    db = _bootstrap_db(tmp_path)
    migrations_dir = tmp_path / "mig"
    migrations_dir.mkdir()
    _write_migration(
        migrations_dir,
        1,
        "add_test_table",
        "CREATE TABLE migration_smoke (id INTEGER PRIMARY KEY);",
    )

    applied = migrations_pkg.apply_pending(db, migrations_dir=migrations_dir)
    assert [m.slug for m in applied] == ["add_test_table"]

    with contextlib.closing(open_conn(db, isolation_level=None)) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='migration_smoke'").fetchall()
        assert rows == [("migration_smoke",)]
        applied_rows = migrations_pkg.read_applied(conn)
        assert [r.sequence_number for r in applied_rows] == [1]
        assert applied_rows[0].slug == "add_test_table"

    # Second call: no-op.
    again = migrations_pkg.apply_pending(db, migrations_dir=migrations_dir)
    assert again == []


def test_apply_pending_is_atomic_per_file(tmp_path: Path) -> None:
    """A failing SQL statement rolls back the entire migration; the row
    in schema_migrations is NOT recorded, so a re-run sees the file as
    pending."""
    db = _bootstrap_db(tmp_path)
    migrations_dir = tmp_path / "mig"
    migrations_dir.mkdir()
    _write_migration(
        migrations_dir,
        1,
        "broken",
        "CREATE TABLE good_one (id INTEGER); CREATE TABLE good_one (id INTEGER);",
    )
    with pytest.raises(sqlite3.OperationalError):
        migrations_pkg.apply_pending(db, migrations_dir=migrations_dir)
    with contextlib.closing(open_conn(db, isolation_level=None)) as conn:
        applied = migrations_pkg.read_applied(conn)
        assert applied == []
        leftover = conn.execute("SELECT name FROM sqlite_master WHERE name='good_one'").fetchall()
        assert leftover == []


def test_checksum_drift_fails_loud(tmp_path: Path) -> None:
    """Editing a migration file post-apply makes the next plan call
    raise. Schema drift is a configuration bug, not silent state."""
    db = _bootstrap_db(tmp_path)
    migrations_dir = tmp_path / "mig"
    migrations_dir.mkdir()
    sql_path = migrations_dir / "0001_first.sql"
    sql_path.write_text("CREATE TABLE drift_a (id INTEGER);")
    migrations_pkg.apply_pending(db, migrations_dir=migrations_dir)

    # Operator edits the file post-apply.
    sql_path.write_text("CREATE TABLE drift_a (id INTEGER); -- edited")

    with (
        contextlib.closing(open_conn(db, isolation_level=None)) as conn,
        pytest.raises(RuntimeError, match="checksum drift"),
    ):
        migrations_pkg.plan(conn, migrations_dir=migrations_dir)


def test_sequence_gap_fails_loud(tmp_path: Path) -> None:
    """Discovering 0001 and 0003 but no 0002 raises a clear error."""
    db = _bootstrap_db(tmp_path)
    migrations_dir = tmp_path / "mig"
    migrations_dir.mkdir()
    _write_migration(migrations_dir, 1, "first", "CREATE TABLE one (id INTEGER);")
    _write_migration(migrations_dir, 3, "third", "CREATE TABLE three (id INTEGER);")
    with (
        contextlib.closing(open_conn(db, isolation_level=None)) as conn,
        pytest.raises(RuntimeError, match="migration gap detected"),
    ):
        migrations_pkg.plan(conn, migrations_dir=migrations_dir)


def test_target_argument_bounds_apply_pass(tmp_path: Path) -> None:
    """apply_pending(target=N) stops at sequence number N and leaves
    higher-numbered files pending for a later invocation."""
    db = _bootstrap_db(tmp_path)
    migrations_dir = tmp_path / "mig"
    migrations_dir.mkdir()
    _write_migration(migrations_dir, 1, "one", "CREATE TABLE t_one (id INTEGER);")
    _write_migration(migrations_dir, 2, "two", "CREATE TABLE t_two (id INTEGER);")
    _write_migration(migrations_dir, 3, "three", "CREATE TABLE t_three (id INTEGER);")

    applied = migrations_pkg.apply_pending(
        db,
        target=2,
        migrations_dir=migrations_dir,
    )
    assert [m.sequence_number for m in applied] == [1, 2]

    with contextlib.closing(open_conn(db, isolation_level=None)) as conn:
        plan = migrations_pkg.plan(conn, migrations_dir=migrations_dir)
        states = {entry.migration.sequence_number: entry.state for entry in plan}
        assert states == {1: "applied", 2: "applied", 3: "pending"}


def test_python_hook_runs_after_sql(tmp_path: Path) -> None:
    """A same-stem .py file's apply(conn) callable fires after the SQL
    block, inside the same transaction. The hook can see the schema
    changes from the .sql file."""
    db = _bootstrap_db(tmp_path)
    migrations_dir = tmp_path / "mig"
    migrations_dir.mkdir()
    _write_migration(
        migrations_dir,
        1,
        "with_hook",
        "CREATE TABLE hook_smoke (id INTEGER PRIMARY KEY, label TEXT);",
        py=("def apply(conn):\n    conn.execute(\"INSERT INTO hook_smoke (label) VALUES ('from-hook')\")\n"),
    )
    migrations_pkg.apply_pending(db, migrations_dir=migrations_dir)
    with contextlib.closing(open_conn(db, isolation_level=None)) as conn:
        rows = conn.execute("SELECT label FROM hook_smoke").fetchall()
        assert rows == [("from-hook",)]


def test_mark_baseline_applied_records_every_shipped_migration(
    tmp_path: Path,
) -> None:
    """mark_baseline_applied records every shipped migration as applied
    without executing its DDL, leaving the schema_migrations table in
    a state where the next apply_pending call is a no-op."""
    db = _bootstrap_db(tmp_path)
    migrations_pkg.mark_baseline_applied(db)
    with contextlib.closing(open_conn(db, isolation_level=None)) as conn:
        applied = migrations_pkg.read_applied(conn)
    discovered = migrations_pkg.discover_migrations()
    assert {r.sequence_number for r in applied} == {m.sequence_number for m in discovered}


def test_cli_status_prints_applied_and_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`waitbus migrate --status` prints two sections (applied +
    pending) and exits 0 without mutating the DB."""
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.setenv("WAITBUS_CURSORS_DIR", str(tmp_path / "cur"))
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "--status"])
    assert result.exit_code == 0, result.output
    assert "applied:" in result.output
    assert "pending:" in result.output
    assert "0001_initial_schema" in result.output


def test_cli_dry_run_prints_sql_without_mutating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`waitbus migrate --dry-run` against a fresh, un-baselined
    DB prints the SQL of every pending file and leaves the
    schema_migrations table empty."""
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.setenv("WAITBUS_CURSORS_DIR", str(tmp_path / "cur"))
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "CREATE TABLE IF NOT EXISTS events" in result.output

    # Confirm the DB exists but no rows were recorded in schema_migrations.
    from waitbus._paths import db_path

    db = db_path()
    with contextlib.closing(open_conn(db, isolation_level=None)) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()
        assert rows[0] == 0


def test_cli_apply_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Running `waitbus migrate` twice in a row: the first call
    applies the baseline, the second reports zero pending."""
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.setenv("WAITBUS_CURSORS_DIR", str(tmp_path / "cur"))
    runner = CliRunner()
    first = runner.invoke(app, ["migrate"])
    assert first.exit_code == 0, first.output
    assert "applied 0001_initial_schema" in first.output

    second = runner.invoke(app, ["migrate"])
    assert second.exit_code == 0, second.output
    assert "No pending migrations." in second.output


def test_cli_to_argument_bounds_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`waitbus migrate --to 0` is rejected by typer's min=1 guard
    so the operator cannot accidentally pass a sentinel that disables
    every migration."""
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.setenv("WAITBUS_CURSORS_DIR", str(tmp_path / "cur"))
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "--to", "0"])
    assert result.exit_code != 0


# --- 0002 daemon-sequence rebuild ------------------------------------------

# The events table shape as it existed BEFORE the seq column (delivery_id as
# PRIMARY KEY, no seq), with the full column set ensure_schema's additive ALTER
# pass brings an upgraded DB to before `waitbus migrate` runs 0002. Hand-built so
# the round-trip test exercises the real 0002 rebuild against a realistic
# pre-seq table.
_PRE_SEQ_EVENTS_DDL = """
CREATE TABLE events (
    delivery_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    run_id INTEGER,
    workflow_name TEXT,
    head_branch TEXT,
    head_sha TEXT,
    status TEXT,
    conclusion TEXT,
    received_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    ingest_method TEXT NOT NULL,
    job_id INTEGER,
    job_name TEXT,
    parent_run_id INTEGER,
    alert_name TEXT,
    alert_severity TEXT,
    alert_fingerprint TEXT,
    msg_to TEXT,
    msg_from TEXT,
    msg_correlation_id TEXT,
    msg_reply_to TEXT,
    msg_thread TEXT,
    msg_body TEXT,
    event_id TEXT
);
"""


def _insert_pre_seq_row(conn: sqlite3.Connection, delivery_id: str, event_id: str) -> None:
    conn.execute(
        "INSERT INTO events (delivery_id, source, event_type, owner, repo, "
        "received_at, payload_json, ingest_method, event_id) "
        "VALUES (?, 'agent', 'agent_message', 'local', 'agents', 1, '{}', 'api', ?)",
        (delivery_id, event_id),
    )


def test_migration_0002_backfills_seq_in_event_id_order(tmp_path: Path) -> None:
    """The real 0002 migration adds a monotonic seq and backfills it in
    event_id order, so a daemon upgrading an existing DB gets a sequence that
    agrees with the historical ULID ordering. Exercises the actual shipped
    0002 file (not a synthetic one) against a realistic pre-seq table."""
    db = tmp_path / "events.db"
    with contextlib.closing(sqlite3.connect(db)) as conn:
        conn.executescript(_PRE_SEQ_EVENTS_DDL)
        # Insert out of event_id order so an in-order backfill is observable.
        _insert_pre_seq_row(conn, "d-c", "C")
        _insert_pre_seq_row(conn, "d-a", "A")
        _insert_pre_seq_row(conn, "d-b", "B")
        conn.commit()

    # Apply the real shipped migrations (0001 is a no-op CREATE TABLE IF NOT
    # EXISTS against the existing table; 0002 performs the rebuild).
    migrations_pkg.apply_pending(db)

    with contextlib.closing(open_conn(db, isolation_level=None)) as conn:
        rows = conn.execute("SELECT seq, event_id, delivery_id FROM events ORDER BY seq").fetchall()
    # seq is gap-free 1..N, assigned in event_id order; no rows lost.
    assert [r[0] for r in rows] == [1, 2, 3]
    assert [r[1] for r in rows] == ["A", "B", "C"]
    assert {r[2] for r in rows} == {"d-a", "d-b", "d-c"}


def test_migration_0002_preserves_insert_or_ignore_dedup(tmp_path: Path) -> None:
    """After the rebuild, delivery_id is NOT NULL UNIQUE, so the daemon's
    INSERT OR IGNORE idempotency still dedups redeliveries (the dedup moved
    from the PRIMARY KEY to the UNIQUE constraint)."""
    db = tmp_path / "events.db"
    with contextlib.closing(sqlite3.connect(db)) as conn:
        conn.executescript(_PRE_SEQ_EVENTS_DDL)
        _insert_pre_seq_row(conn, "d1", "A")
        conn.commit()
    migrations_pkg.apply_pending(db)
    with contextlib.closing(open_conn(db, isolation_level=None)) as conn:
        # A redelivery of the same delivery_id is ignored, not duplicated.
        conn.execute(
            "INSERT OR IGNORE INTO events (delivery_id, source, event_type, owner, repo, "
            "received_at, payload_json, ingest_method, event_id) "
            "VALUES ('d1', 'agent', 'agent_message', 'local', 'agents', 2, '{}', 'api', 'Z')"
        )
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM events WHERE delivery_id = 'd1'").fetchone()[0]
    assert n == 1
