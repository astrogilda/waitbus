"""Tests for the ``waitbus db-prune`` retention verb.

Covers the public surface:

* ``--dry-run`` is the default and reports the plan without writing.
* ``--max-size`` / ``--max-age`` parsers validate input.
* Age cap deletes rows by ``received_at``.
* Size cap deletes oldest rows when the file exceeds the byte budget.
* Under-budget DB is a no-op.
* Broadcaster-live guard refuses to run when the broadcast socket exists.
* ``--vacuum`` rewrites the file in-place via ``VACUUM INTO`` + rename.
* Without ``--vacuum`` the file is NOT shrunk (freelist pages survive).
* ``PRAGMA auto_vacuum`` is absent from the prune source
  (regression guard against re-introducing it).
"""

from __future__ import annotations

import importlib
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

# The package __init__ re-binds `prune` to the function, so a plain
# ``from waitbus.cli.db import prune`` returns the function, not
# the submodule. Use importlib to grab the submodule unambiguously.
prune_mod = importlib.import_module("waitbus.cli.db.prune")
from waitbus._db import connect, ensure_schema, insert_event
from waitbus._types import EventInsert
from waitbus.cli import app

# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------

_NS = 1_000_000_000


def _isolate_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point the path resolver at tmp_path and bootstrap an empty DB."""
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.setenv("WAITBUS_CURSORS_DIR", str(tmp_path / "cur"))

    db = tmp_path / "github.db"
    ensure_schema(db)
    return db


def _make_event(*, received_at_ns: int, payload_bytes: int, delivery_id: str) -> EventInsert:
    """Build an EventInsert with a payload sized to ``payload_bytes``.

    The payload is a JSON-shaped string; we pad an ``x``-filled field so
    every row contributes a known number of bytes to ``payload_json``.
    """
    pad = "x" * max(payload_bytes - 16, 1)
    return EventInsert(
        delivery_id=delivery_id,
        source="github",
        event_type="workflow_run",
        owner="owner",
        repo="repo",
        received_at=received_at_ns,
        payload_json=f'{{"pad":"{pad}"}}',
        ingest_method="webhook",
    )


def _seed_rows(db: Path, count: int, *, payload_bytes: int, base_age_s: float) -> None:
    """Seed ``count`` rows with monotonically increasing age.

    Row ``i`` has ``received_at = now - (base_age_s + i)`` seconds, so
    rows[0] is the oldest and rows[-1] the youngest.
    """
    now_ns = int(time.time() * _NS)
    with connect(db) as conn:
        for i in range(count):
            age_s = base_age_s + (count - 1 - i)
            insert_event(
                conn,
                _make_event(
                    received_at_ns=now_ns - int(age_s * _NS),
                    payload_bytes=payload_bytes,
                    delivery_id=f"d-{i:06d}",
                ),
                commit=False,
            )
        conn.commit()


def _row_count(db: Path) -> int:
    with connect(db, readonly=True) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])


# ---------------------------------------------------------------------------
# Pure-function tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1024", 1024),
        ("1KB", 1000),
        ("1KiB", 1024),
        ("1MB", 10**6),
        ("1MiB", 1024**2),
        ("1GB", 10**9),
        ("1GiB", 1024**3),
        ("500MiB", 500 * 1024**2),
        ("2.5MiB", int(2.5 * 1024**2)),
    ],
)
def test_parse_size_accepts_units(raw: str, expected: int) -> None:
    assert prune_mod._parse_size(raw) == expected


@pytest.mark.parametrize("bad", ["", "   ", "1XB", "abc", "-1MiB", "0"])
def test_parse_size_rejects_bad(bad: str) -> None:
    with pytest.raises(ValueError):
        prune_mod._parse_size(bad)


@pytest.mark.parametrize(
    "raw,expected",
    [("30d", 30 * 86400.0), ("12h", 12 * 3600.0), ("5m", 300.0), ("90", 90.0)],
)
def test_parse_duration_accepts_units(raw: str, expected: float) -> None:
    from waitbus._duration import parse_duration

    assert parse_duration(raw) == expected


# ---------------------------------------------------------------------------
# CLI behaviour
# ---------------------------------------------------------------------------


def test_dry_run_is_default_and_deletes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default invocation reports the plan and leaves the DB untouched."""
    db = _isolate_paths(monkeypatch, tmp_path)
    _seed_rows(db, count=5, payload_bytes=256, base_age_s=120 * 86400)
    before = _row_count(db)
    runner = CliRunner()
    result = runner.invoke(app, ["db-prune"])
    assert result.exit_code == 0, result.output
    assert "plan:" in result.output
    assert "dry_run=True" in result.output
    assert _row_count(db) == before


def test_under_budget_is_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A small, fresh DB triggers neither cap, so the plan is zero rows."""
    db = _isolate_paths(monkeypatch, tmp_path)
    _seed_rows(db, count=3, payload_bytes=128, base_age_s=1)
    runner = CliRunner()
    result = runner.invoke(app, ["db-prune", "--no-dry-run"])
    assert result.exit_code == 0, result.output
    assert "rows_to_delete=0" in result.output
    assert _row_count(db) == 3


def test_max_age_deletes_old_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rows older than ``--max-age`` are deleted by ``received_at``."""
    db = _isolate_paths(monkeypatch, tmp_path)
    # 4 rows: 2 old (>30d), 2 young (<1d).
    now_ns = int(time.time() * _NS)
    with connect(db) as conn:
        for i, age_s in enumerate([45 * 86400, 40 * 86400, 3600, 60]):
            insert_event(
                conn,
                _make_event(
                    received_at_ns=now_ns - int(age_s * _NS),
                    payload_bytes=128,
                    delivery_id=f"e-{i:04d}",
                ),
                commit=False,
            )
        conn.commit()

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["db-prune", "--max-age", "30d", "--max-size", "10GiB", "--no-dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert _row_count(db) == 2


def test_max_size_deletes_oldest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Size cap deletes oldest rows; survivors are the most recent ULIDs."""
    db = _isolate_paths(monkeypatch, tmp_path)
    # ~50 rows of ~2 KiB payload each → > 64 KiB; we then cap to 32 KiB.
    _seed_rows(db, count=50, payload_bytes=2048, base_age_s=1)
    with connect(db, readonly=True) as conn:
        ordered_ids = [row[0] for row in conn.execute("SELECT event_id FROM events ORDER BY event_id").fetchall()]

    runner = CliRunner()
    result = runner.invoke(
        app,
        # max-age large enough that the time cap is inactive.
        ["db-prune", "--max-size", "32KiB", "--max-age", "365d", "--no-dry-run"],
    )
    assert result.exit_code == 0, result.output
    after = _row_count(db)
    assert 0 < after < 50, f"expected partial trim, got {after}"
    # Survivors are the youngest rows (highest ULIDs).
    with connect(db, readonly=True) as conn:
        survivors = {row[0] for row in conn.execute("SELECT event_id FROM events").fetchall()}
    assert survivors == set(ordered_ids[-after:])


def test_max_rows_safety_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--max-rows N`` keeps at most N rows (the youngest)."""
    db = _isolate_paths(monkeypatch, tmp_path)
    _seed_rows(db, count=20, payload_bytes=64, base_age_s=1)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["db-prune", "--max-rows", "5", "--max-age", "365d", "--max-size", "10GiB", "--no-dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert _row_count(db) == 5


def test_broadcaster_live_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Presence of the broadcast socket file refuses the run with exit 2."""
    db = _isolate_paths(monkeypatch, tmp_path)
    _seed_rows(db, count=3, payload_bytes=64, base_age_s=1)
    from waitbus import _paths

    sock = _paths.broadcast_socket()
    sock.parent.mkdir(parents=True, exist_ok=True)
    sock.touch()
    runner = CliRunner()
    result = runner.invoke(app, ["db-prune", "--no-dry-run"])
    assert result.exit_code == 2, result.output
    assert "broadcast" in result.output
    # Sanity: DB untouched.
    assert _row_count(db) == 3


def test_vacuum_rewrites_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--vacuum`` shrinks the file after a destructive prune."""
    db = _isolate_paths(monkeypatch, tmp_path)
    _seed_rows(db, count=80, payload_bytes=2048, base_age_s=1)
    size_before = db.stat().st_size

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["db-prune", "--max-rows", "5", "--max-age", "365d", "--max-size", "10GiB", "--vacuum", "--no-dry-run"],
    )
    assert result.exit_code == 0, result.output
    size_after = db.stat().st_size
    assert size_after < size_before, (size_before, size_after)
    assert not db.with_suffix(db.suffix + ".new").exists()


def test_no_vacuum_keeps_file_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``--vacuum``, DELETE leaves freelist pages — file size
    does NOT shrink (operator must opt in to reclamation)."""
    db = _isolate_paths(monkeypatch, tmp_path)
    _seed_rows(db, count=80, payload_bytes=2048, base_age_s=1)
    # Pre-checkpoint to fold the seed WAL back into the main file so the
    # post-DELETE comparison is apples-to-apples.
    with connect(db) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    size_before = db.stat().st_size

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["db-prune", "--max-rows", "5", "--max-age", "365d", "--max-size", "10GiB", "--no-dry-run"],
    )
    assert result.exit_code == 0, result.output
    # Fold the DELETE's WAL back so we compare main-file footprints.
    with connect(db) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    size_after = db.stat().st_size
    assert size_after >= size_before * 0.9, (size_before, size_after)


def test_source_does_not_set_auto_vacuum() -> None:
    """Regression guard: ``PRAGMA auto_vacuum`` must never appear as an
    executable statement in the prune source. Setting it on an existing
    DB forces a full rewrite at a non-obvious time; the v0.4.0 design
    rejects that mode explicitly. (Comments / docstrings mentioning the
    pragma are allowed — they explain WHY we do not set it.)"""
    import ast

    source_path = prune_mod.__file__
    assert source_path is not None
    tree = ast.parse(Path(source_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            # Skip docstrings: a string Constant whose parent is a module /
            # function / class body's first Expr statement. The cheap proxy
            # here: skip strings longer than 200 chars (docstrings) and
            # multi-line strings; SQL literals we care about are short and
            # single-line.
            if "\n" in node.value or len(node.value) > 200:
                continue
            assert "auto_vacuum" not in node.value.lower(), f"PRAGMA auto_vacuum literal found: {node.value!r}"


def test_db_override_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--db`` points the verb at an explicit file regardless of the
    platform-default resolver."""
    _isolate_paths(monkeypatch, tmp_path)
    alt = tmp_path / "alt.db"
    ensure_schema(alt)
    _seed_rows(alt, count=5, payload_bytes=128, base_age_s=120 * 86400)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["db-prune", "--db", str(alt), "--max-age", "30d", "--no-dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert _row_count(alt) == 0


def test_missing_db_exits_two(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-existent ``--db`` path exits with code 2."""
    _isolate_paths(monkeypatch, tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["db-prune", "--db", str(tmp_path / "nope.db")],
    )
    assert result.exit_code == 2, result.output
