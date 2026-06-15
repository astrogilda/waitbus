"""Tests for the waitbus watchdog absence-detector.

Each test gets a fresh tmp SQLite DB (canonical schema via
`_db.ensure_schema`) and a fresh state dir so the operator's real
event store and state directory are never touched.
"""

from __future__ import annotations

import contextlib
import sqlite3
import time
from pathlib import Path

import pytest

from waitbus import _config, _db, watchdog_check


def _seed_watchdog_rows(db_path: Path, rows: list[tuple[str, str, int]]) -> None:
    """Seed `rows = [(event_type, alert_name, received_at)]` against the canonical schema."""
    _db.ensure_schema(db_path)
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        for i, (etype, aname, ts) in enumerate(rows):
            conn.execute(
                """
                INSERT INTO events
                  (delivery_id, source, event_type, owner, repo,
                   received_at, payload_json, ingest_method, alert_name)
                VALUES (?, 'alertmanager', ?, ?, ?,
                        ?, '{}', 'webhook', ?)
                """,
                (f"d-{i}", etype, _config.get_config().prom_owner, _config.get_config().prom_repo, ts, aname),
            )
        conn.commit()


@pytest.fixture
def now() -> int:
    return int(time.time())


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "state"


def _run(db: Path, state_dir: Path, *args: str) -> int:
    # --quiet because notify-send may or may not exist on the host.
    return watchdog_check.main(["--db", str(db), "--state-dir", str(state_dir), "--quiet", *args])


# --- pre-bootstrap ---------------------------------------------------------


def test_no_db_means_pre_bootstrap_silent(tmp_path: Path, state_dir: Path) -> None:
    db = tmp_path / "missing.db"
    rc = _run(db, state_dir)
    assert rc == 0
    assert not (state_dir / watchdog_check.SEEN_FLAG_FILENAME).exists()
    assert not (state_dir / watchdog_check.STALE_FLAG_FILENAME).exists()


def test_empty_events_table_is_pre_bootstrap_silent(tmp_db_path: Path, state_dir: Path) -> None:
    rc = _run(tmp_db_path, state_dir)
    assert rc == 0


# --- happy path ------------------------------------------------------------


def test_fresh_watchdog_returns_zero(tmp_db_path: Path, state_dir: Path, now: int) -> None:
    _seed_watchdog_rows(
        tmp_db_path,
        [(watchdog_check.WATCHDOG_EVENT_TYPE, watchdog_check.WATCHDOG_ALERT_NAME, now - 60)],
    )
    rc = _run(tmp_db_path, state_dir)
    assert rc == 0
    assert (state_dir / watchdog_check.SEEN_FLAG_FILENAME).exists()
    assert not (state_dir / watchdog_check.STALE_FLAG_FILENAME).exists()


def test_fresh_clears_existing_stale_flag(tmp_db_path: Path, state_dir: Path, now: int) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / watchdog_check.STALE_FLAG_FILENAME).touch()
    _seed_watchdog_rows(
        tmp_db_path,
        [(watchdog_check.WATCHDOG_EVENT_TYPE, watchdog_check.WATCHDOG_ALERT_NAME, now - 30)],
    )
    rc = _run(tmp_db_path, state_dir)
    assert rc == 0
    assert not (state_dir / watchdog_check.STALE_FLAG_FILENAME).exists()


# --- stale detection -------------------------------------------------------


def test_stale_watchdog_returns_one_and_writes_flag(tmp_db_path: Path, state_dir: Path, now: int) -> None:
    _seed_watchdog_rows(
        tmp_db_path,
        [(watchdog_check.WATCHDOG_EVENT_TYPE, watchdog_check.WATCHDOG_ALERT_NAME, now - 1800)],
    )
    rc = _run(tmp_db_path, state_dir, "--threshold", "600")
    assert rc == 1
    assert (state_dir / watchdog_check.SEEN_FLAG_FILENAME).exists()
    assert (state_dir / watchdog_check.STALE_FLAG_FILENAME).exists()


def test_stale_idempotent_on_repeat(tmp_db_path: Path, state_dir: Path, now: int) -> None:
    _seed_watchdog_rows(
        tmp_db_path,
        [(watchdog_check.WATCHDOG_EVENT_TYPE, watchdog_check.WATCHDOG_ALERT_NAME, now - 3600)],
    )
    rc1 = _run(tmp_db_path, state_dir, "--threshold", "600")
    assert rc1 == 1
    rc2 = _run(tmp_db_path, state_dir, "--threshold", "600")
    assert rc2 == 1
    assert (state_dir / watchdog_check.STALE_FLAG_FILENAME).exists()


def test_seen_flag_persists_across_db_purge(tmp_db_path: Path, state_dir: Path, now: int) -> None:
    _seed_watchdog_rows(
        tmp_db_path,
        [(watchdog_check.WATCHDOG_EVENT_TYPE, watchdog_check.WATCHDOG_ALERT_NAME, now - 60)],
    )
    rc1 = _run(tmp_db_path, state_dir)
    assert rc1 == 0
    assert (state_dir / watchdog_check.SEEN_FLAG_FILENAME).exists()

    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        conn.execute("DELETE FROM events")
        conn.commit()

    rc2 = _run(tmp_db_path, state_dir)
    assert rc2 == 1
    assert (state_dir / watchdog_check.STALE_FLAG_FILENAME).exists()


# --- error path ------------------------------------------------------------


def test_db_error_returns_two(tmp_path: Path, state_dir: Path) -> None:
    """Corrupted file (not a SQLite DB) → exit 2."""
    db = tmp_path / "events.db"
    db.write_bytes(b"not a sqlite db" * 100)
    rc = _run(db, state_dir)
    assert rc == 2
