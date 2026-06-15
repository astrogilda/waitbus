"""Live-loop integration tests for the fs watcher.

The frame-shape unit tests live in test_sources.py; these drive the real
watchdog observer over a tmpdir — the close-write and atomic-rename
terminal signals, the debounce flush into the store, and every exit
seam of ``watch`` — so the module can sit under the per-file coverage
gate instead of the integration-harness exclusion list.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

from waitbus.sources import fs_watch

pytest.importorskip("watchdog", reason="fs watcher integration needs the [fs] extra")


def _rows(db: Path) -> list[tuple[str, str]]:
    with contextlib.closing(sqlite3.connect(db)) as conn:
        return list(conn.execute("SELECT delivery_id, event_type FROM events ORDER BY delivery_id"))


def _wait_for_rows(db: Path, count: int, deadline_s: float = 10.0) -> list[tuple[str, str]]:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        rows = _rows(db)
        if len(rows) >= count:
            return rows
        time.sleep(0.05)
    return _rows(db)


@pytest.fixture
def fs_db(broadcast_paths: dict[str, Path]) -> Path:
    from waitbus import _db

    db = broadcast_paths["db"]
    _db.ensure_schema(db)
    return db


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Linux inotify semantics; macOS FSEvents differs (fs source works, assertions are Linux-specific)",
)
def test_watch_emits_on_close_write_and_atomic_rename(tmp_path: Path, fs_db: Path) -> None:
    """The two terminal signals each land exactly one completed-save row."""
    watched = tmp_path / "tree"
    watched.mkdir()
    stop = threading.Event()
    rc: list[int] = []
    thread = threading.Thread(
        target=lambda: rc.append(fs_watch.watch(watched, db_path=fs_db, stop_event=stop)),
        daemon=True,
    )
    thread.start()
    try:
        # Let the observer arm its inotify watches before the writes.
        time.sleep(0.5)

        # Terminal signal 1: plain write + close (IN_CLOSE_WRITE).
        saved = watched / "saved.txt"
        with open(saved, "w", encoding="utf-8") as f:
            f.write("complete")

        # Terminal signal 2: the atomic-save shape editors actually use —
        # temp written in the SAME directory, then renamed over the target
        # (a move within the watched tree is what watchdog reports as a
        # FileMovedEvent; a move from outside surfaces as created instead).
        temp = watched / ".renamed.txt.tmp"
        temp.write_text("renamed in")
        os.replace(temp, watched / "renamed.txt")

        rows = _wait_for_rows(fs_db, 2)
        ids = [r[0] for r in rows]
        assert any("saved.txt" in d for d in ids), rows
        assert any("renamed.txt" in d for d in ids), rows
        assert all(r[1] == "fs_change" for r in rows)
    finally:
        stop.set()
        thread.join(timeout=10.0)
    assert not thread.is_alive(), "watch() did not exit on the stop event"
    assert rc == [0], "the stop-event seam must return 0"


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Linux inotify semantics; macOS FSEvents differs (fs source works, assertions are Linux-specific)",
)
def test_watch_coalesces_duplicate_saves_by_delivery_id(tmp_path: Path, fs_db: Path) -> None:
    """Two notifications for the same on-disk state collapse to one row."""
    watched = tmp_path / "tree"
    watched.mkdir()
    target = watched / "burst.txt"
    stop = threading.Event()
    thread = threading.Thread(
        target=lambda: fs_watch.watch(watched, db_path=fs_db, stop_event=stop),
        daemon=True,
    )
    thread.start()
    try:
        time.sleep(0.5)
        with open(target, "w", encoding="utf-8") as f:
            f.write("v1")
        # A second save with a distinct mtime is a distinct delivery_id;
        # ensure the clock tick is visible to mtime_ns.
        time.sleep(0.05)
        with open(target, "w", encoding="utf-8") as f:
            f.write("v2-different")
        rows = _wait_for_rows(fs_db, 2)
        assert len(rows) == len({r[0] for r in rows}), f"duplicate delivery_id rows: {rows}"
        assert len(rows) >= 1
    finally:
        stop.set()
        thread.join(timeout=10.0)


def test_watch_missing_path_raises() -> None:
    with pytest.raises(FileNotFoundError):
        fs_watch.watch("/nonexistent/waitbus-fs-watch-test")


def test_watch_stop_after_returns_zero(tmp_path: Path, fs_db: Path) -> None:
    """The test-only timed exit routes through the same cleanup finally."""
    assert fs_watch.watch(tmp_path, db_path=fs_db, _stop_after=0.1) == 0
