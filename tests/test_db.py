"""Tests for waitbus._db shared SQLite helpers."""

from __future__ import annotations

import contextlib
import sqlite3
import time
from pathlib import Path

import pytest

from waitbus import _db
from waitbus._types import EventInsert


def _new_db(tmp_path: Path) -> Path:
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    return db


# --- EVENT_COLUMNS invariant ------------------------------------------------


def test_event_columns_matches_schema_sql_exactly() -> None:
    """`_db.EVENT_COLUMNS` is the single-source-of-truth wire image of the
    events table column list. Drift between this tuple and schema.sql's
    CREATE TABLE block causes silent INSERT failures the moment a new
    column lands; this test catches that drift on every CI run.

    Consumes the production schema parser (`_db._expected_event_columns`)
    rather than a second hand-rolled regex: that parser strips `--`
    comments before extracting the CREATE TABLE body (so a `);` inside a
    column comment can't truncate the set) and excludes the daemon-assigned
    `seq` AUTOINCREMENT PK (never an INSERT column). A duplicate parser here
    would drift from the real one -- exactly the class this suite guards.
    """
    cols_in_schema = {name for name, _decl in _db._expected_event_columns()}
    assert set(_db.EVENT_COLUMNS) == cols_in_schema, (
        f"EVENT_COLUMNS missing from schema: "
        f"{set(_db.EVENT_COLUMNS) - cols_in_schema}; "
        f"schema columns missing from EVENT_COLUMNS: "
        f"{cols_in_schema - set(_db.EVENT_COLUMNS)}"
    )


def test_expected_event_columns_robust_to_semicolon_in_comment() -> None:
    """A `);` inside a column comment must not truncate the parsed column set.

    The CREATE-TABLE body regex is non-greedy (stops at the first `);`), so a
    comment like ``-- (addresses, not credentials);`` would otherwise silently
    drop every column after it from the ADD COLUMN migration diff. The parser
    strips `-- ...` line comments before body extraction to prevent that.
    """
    schema = (
        "CREATE TABLE IF NOT EXISTS events (\n"
        "    delivery_id TEXT PRIMARY KEY,\n"
        "    -- a comment with a stray close-paren-semicolon (like this);\n"
        "    msg_to TEXT,\n"
        "    event_id TEXT\n"
        ");\n"
        "CREATE INDEX idx ON events(msg_to);\n"
    )
    cols = [name for name, _decl in _db._expected_event_columns(schema)]
    assert cols == ["delivery_id", "msg_to", "event_id"], cols


# --- insert_event behavior --------------------------------------------------


def _event_stub(
    delivery_id: str = "d1",
    event_type: str = "workflow_run",
    received_at: int | None = None,
) -> EventInsert:
    """Minimal EventInsert for use in insert_event tests."""
    return EventInsert(
        delivery_id=delivery_id,
        source="github",
        event_type=event_type,
        owner="test-owner",
        repo="test-repo",
        received_at=received_at if received_at is not None else time.time_ns(),
        payload_json='{"x":1}',
        ingest_method="webhook",
        run_id=1,
        workflow_name="Tests",
        head_branch="main",
        head_sha="abc123",
        status="completed",
        conclusion="success",
    )


def test_insert_event_persists_all_columns(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(conn, _event_stub("d1"))
    with contextlib.closing(sqlite3.connect(db)) as conn:
        # Confirm the inserted row exposes every declared column.
        cols_in_query = ", ".join(_db.EVENT_COLUMNS)
        row = conn.execute(f"SELECT {cols_in_query} FROM events WHERE delivery_id = ?", ("d1",)).fetchone()
    assert row is not None
    assert len(row) == len(_db.EVENT_COLUMNS)


def test_insert_event_dedups_redelivery(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(sqlite3.connect(db)) as conn:
        for _ in range(5):
            _db.insert_event(conn, _event_stub("d-dup"))
        n = conn.execute("SELECT COUNT(*) FROM events WHERE delivery_id = ?", ("d-dup",)).fetchone()[0]
    assert n == 1


def test_insert_event_stamps_ulid_event_id(tmp_path: Path) -> None:
    """Every new INSERT receives a fresh 26-char ULID in `event_id`."""
    db = _new_db(tmp_path)
    with contextlib.closing(sqlite3.connect(db)) as conn:
        for i in range(5):
            _db.insert_event(conn, _event_stub(f"d-{i}"))
    with contextlib.closing(sqlite3.connect(db)) as conn:
        rows = conn.execute("SELECT delivery_id, event_id FROM events ORDER BY rowid").fetchall()
    assert len(rows) == 5
    event_ids = [r[1] for r in rows]
    # ULID shape: 26 chars, Crockford alphabet (verified separately in test_ulid).
    for eid in event_ids:
        assert eid is not None
        assert len(eid) == 26
    # All distinct.
    assert len(set(event_ids)) == 5
    # Monotonic (ULIDs sort lexically by insertion-time prefix).
    assert event_ids == sorted(event_ids)


def test_redelivery_does_not_stamp_new_event_id(tmp_path: Path) -> None:
    """A repeated INSERT OR IGNORE on the same delivery_id is dropped by
    SQLite without overwriting event_id. The row's first-issued event_id
    persists; a fresh ULID is generated and discarded on every redelivery.
    """
    db = _new_db(tmp_path)
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(conn, _event_stub("d-once"))
        first = conn.execute("SELECT event_id FROM events WHERE delivery_id = ?", ("d-once",)).fetchone()[0]
        # Re-insert; INSERT OR IGNORE drops the second row.
        _db.insert_event(conn, _event_stub("d-once"))
        second = conn.execute("SELECT event_id FROM events WHERE delivery_id = ?", ("d-once",)).fetchone()[0]
        n = conn.execute("SELECT COUNT(*) FROM events WHERE delivery_id = ?", ("d-once",)).fetchone()[0]
    assert first == second
    assert n == 1


def test_row_is_visible_to_fresh_connection_after_insert_event_returns(
    tmp_path: Path,
) -> None:
    """The commit-before-doorbell ordering inside insert_event guarantees
    that a separate connection (mimicking the broadcast daemon's SELECT
    after doorbell wake) sees the new row the moment insert_event returns.
    Without the explicit commit, the caller's implicit transaction would
    still be open and the new row would be invisible to a fresh SELECT.
    """
    db = _new_db(tmp_path)
    with contextlib.closing(sqlite3.connect(db)) as writer:
        _db.insert_event(writer, _event_stub("d-race"))
        # Fresh reader connection — must see the committed row even
        # though `writer` is still inside its `with` block.
        with contextlib.closing(sqlite3.connect(db)) as reader:
            n = reader.execute(
                "SELECT COUNT(*) FROM events WHERE delivery_id = ?",
                ("d-race",),
            ).fetchone()[0]
    assert n == 1


def test_insert_event_rings_doorbell_best_effort(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the doorbell socket is absent, insert_event still succeeds.
    The doorbell ring is fire-and-forget; the daemon recovers via its
    MAX(event_id) seed on next start.
    """
    db = _new_db(tmp_path)
    # Point the doorbell at a guaranteed-missing path.
    missing = tmp_path / "nonexistent.sock"
    monkeypatch.setattr(_db._doorbell, "doorbell_socket", lambda: missing)
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(conn, _event_stub("d-doorbell-miss"))
        n = conn.execute(
            "SELECT COUNT(*) FROM events WHERE delivery_id = ?",
            ("d-doorbell-miss",),
        ).fetchone()[0]
    assert n == 1


# --- insert_event bool return + commit=False semantics ---------------------


def test_insert_event_returns_true_on_insert(tmp_path: Path) -> None:
    """A fresh insert with a new delivery_id returns True."""
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        result = _db.insert_event(conn, _event_stub("d-bool-true"))
    assert result is True


def test_insert_event_returns_false_on_duplicate(tmp_path: Path) -> None:
    """A repeated insert on the same delivery_id returns False."""
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        _db.insert_event(conn, _event_stub("d-bool-dup"))
    with contextlib.closing(_db.open_conn(db)) as conn:
        result = _db.insert_event(conn, _event_stub("d-bool-dup"))
    assert result is False


def test_insert_event_with_commit_false_does_not_commit(tmp_path: Path) -> None:
    """With commit=False, the row is visible in the same connection but
    not yet committed, so a separate connection opened before the
    transaction closes cannot see it via WAL isolation.
    """
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db, isolation_level=None)) as writer:
        writer.execute("BEGIN IMMEDIATE")
        _db.insert_event(writer, _event_stub("d-no-commit"), commit=False)
        # In-transaction read on the same connection sees the row.
        in_tx = writer.execute("SELECT COUNT(*) FROM events WHERE delivery_id = ?", ("d-no-commit",)).fetchone()[0]
        # Fresh reader connection must NOT see the uncommitted row (WAL).
        with contextlib.closing(_db.open_conn(db, readonly=True)) as reader:
            not_yet = reader.execute("SELECT COUNT(*) FROM events WHERE delivery_id = ?", ("d-no-commit",)).fetchone()[
                0
            ]
        writer.commit()
    assert in_tx == 1
    assert not_yet == 0


def test_insert_event_with_commit_false_does_not_ring_doorbell(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With commit=False, no doorbell ring is emitted even on a new insert."""
    db = _new_db(tmp_path)
    ring_calls: list[None] = []
    monkeypatch.setattr(_db._doorbell, "ring", lambda _path=None: ring_calls.append(None))
    with contextlib.closing(_db.open_conn(db, isolation_level=None)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        _db.insert_event(conn, _event_stub("d-no-ring"), commit=False)
        conn.commit()
    assert ring_calls == []


def test_insert_event_with_commit_true_rings_doorbell_on_insert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With commit=True (default), exactly one ring fires on a new insert."""
    db = _new_db(tmp_path)
    ring_calls: list[None] = []
    monkeypatch.setattr(_db._doorbell, "ring", lambda _path=None: ring_calls.append(None))
    with contextlib.closing(_db.open_conn(db)) as conn:
        _db.insert_event(conn, _event_stub("d-ring-once"))
    assert ring_calls == [None]


def test_insert_event_with_commit_true_does_not_ring_on_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A duplicate insert (IGNORE branch) does not trigger a doorbell ring."""
    db = _new_db(tmp_path)
    ring_calls: list[None] = []
    monkeypatch.setattr(_db._doorbell, "ring", lambda _path=None: ring_calls.append(None))
    with contextlib.closing(_db.open_conn(db)) as conn:
        _db.insert_event(conn, _event_stub("d-ring-dedup"))
    # Second insert: same delivery_id, should be ignored, no second ring.
    with contextlib.closing(_db.open_conn(db)) as conn:
        _db.insert_event(conn, _event_stub("d-ring-dedup"))
    assert ring_calls == [None]


# --- insert_event ns magnitude guard ---------------------------------------


def test_insert_event_rejects_ms_received_at(tmp_path: Path) -> None:
    """received_at below 1e15 (milliseconds or seconds magnitude) raises ValueError."""
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn, pytest.raises(ValueError, match="nanoseconds"):
        _db.insert_event(conn, _event_stub("d-ms", received_at=1_715_000_000_000))


def test_insert_event_accepts_ns_received_at(tmp_path: Path) -> None:
    """received_at at ns magnitude (>= 1e15) is accepted without error."""
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        result = _db.insert_event(conn, _event_stub("d-ns", received_at=1_715_000_000_000_000_000))
    assert result is True
