"""Tests for waitbus._db.open_conn pragma configuration and _is_busy_or_locked."""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path

import pytest

from waitbus import _db


def _new_db(tmp_path: Path) -> Path:
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    return db


# --- open_conn pragma assertions --------------------------------------------


def test_open_conn_sets_busy_timeout(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        val = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    assert val == 5000


def test_open_conn_sets_journal_mode_wal(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


def test_open_conn_sets_synchronous_normal(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        # synchronous=NORMAL is value 1 in PRAGMA output.
        sync = conn.execute("PRAGMA synchronous").fetchone()[0]
    assert sync == 1


def test_open_conn_sets_foreign_keys_on(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    assert fk == 1


def test_open_conn_sets_temp_store_memory(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        # temp_store=MEMORY is value 2 in PRAGMA output.
        ts = conn.execute("PRAGMA temp_store").fetchone()[0]
    assert ts == 2


def test_open_conn_sets_cache_size(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        cs = conn.execute("PRAGMA cache_size").fetchone()[0]
    assert cs == -16000


def test_open_conn_sets_mmap_size(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db)) as conn:
        mm = conn.execute("PRAGMA mmap_size").fetchone()[0]
    assert mm == 268435456


# --- readonly mode ----------------------------------------------------------


def test_open_conn_readonly_rejects_writes(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with (
        contextlib.closing(_db.open_conn(db, readonly=True)) as conn,
        pytest.raises(sqlite3.OperationalError) as exc_info,
    ):
        conn.execute("INSERT INTO events (delivery_id) VALUES ('x')")
    code = getattr(exc_info.value, "sqlite_errorcode", None)
    assert code == sqlite3.SQLITE_READONLY


def test_open_conn_readonly_still_applies_busy_timeout(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    with contextlib.closing(_db.open_conn(db, readonly=True)) as conn:
        val = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    assert val == 5000


# --- _is_busy_or_locked helper ----------------------------------------------


def test_is_busy_or_locked_returns_true_for_busy() -> None:
    exc = sqlite3.OperationalError("database is locked")
    exc.sqlite_errorcode = sqlite3.SQLITE_BUSY
    assert _db._is_busy_or_locked(exc) is True


def test_is_busy_or_locked_returns_true_for_locked() -> None:
    exc = sqlite3.OperationalError("database is locked")
    exc.sqlite_errorcode = sqlite3.SQLITE_LOCKED
    assert _db._is_busy_or_locked(exc) is True


def test_is_busy_or_locked_returns_false_for_other_errors() -> None:
    exc = sqlite3.OperationalError("database disk image is malformed")
    exc.sqlite_errorcode = sqlite3.SQLITE_CORRUPT
    assert _db._is_busy_or_locked(exc) is False
