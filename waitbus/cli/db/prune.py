"""`db-prune` top-level command — operator-driven retention.

Opt-in trimming of the events table by age, byte budget, and/or row
count. The DEFAULT retention policy of the bus remains "keep everything"
(see ARCHITECTURE.md); this verb only acts when an operator runs it.

Design notes
------------
* ``--dry-run`` is on by default. Destructive action requires
  ``--no-dry-run``; the verb reports the plan in either mode.
* Refuses to run while the broadcast daemon is live: the daemon holds a
  long-lived writer on the WAL, and ``VACUUM INTO`` followed by an
  atomic rename would split readers across the old and new inode. The
  liveness probe is socket-presence based (cheap; matches the
  ``waitbus status`` convention).
* ``--vacuum`` reclaims disk via ``VACUUM INTO '<db>.new'`` followed by
  ``os.replace`` (atomic on the same filesystem). Without ``--vacuum``
  the DELETE leaves freelist pages — the operator opts into reclamation
  explicitly because a VACUUM rewrite is O(db-size).
* ``PRAGMA auto_vacuum`` is intentionally NOT set anywhere. Setting it
  on an existing DB requires a full rewrite (the page-map layout
  changes); making that automatic would surprise operators with a large
  blocking write on every prune.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import time
from pathlib import Path

import typer

from ... import _paths
from ..._db import _ENSURE_SCHEMA_BACKOFF_SEC, _ENSURE_SCHEMA_RETRIES, _is_busy_or_locked, connect
from ..._duration import parse_duration
from ..._log import structured

logger = logging.getLogger("waitbus.db.prune")

_NS_PER_SEC = 1_000_000_000
_DEFAULT_MAX_SIZE = "1GiB"
_DEFAULT_MAX_AGE = "30d"

_SIZE_UNITS: dict[str, int] = {
    "": 1,
    "b": 1,
    "k": 1000,
    "kb": 1000,
    "m": 1000**2,
    "mb": 1000**2,
    "g": 1000**3,
    "gb": 1000**3,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
}

_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]*)\s*$")


def _parse_size(raw: str) -> int:
    """Parse a byte budget with an optional binary/decimal suffix.

    Accepts a bare integer (bytes) or ``<number><unit>`` where unit is
    one of ``KB``/``MB``/``GB`` (decimal, ``1000^n``) or
    ``KiB``/``MiB``/``GiB`` (binary, ``1024^n``). Case-insensitive.

    Raises:
        ValueError: empty, malformed, non-positive, or unknown unit.
    """
    if not raw or not raw.strip():
        raise ValueError("size must be non-empty")
    match = _SIZE_RE.match(raw)
    if match is None:
        raise ValueError(f"unparseable size: {raw!r}")
    number_text, unit = match.group(1), match.group(2).lower()
    if unit not in _SIZE_UNITS:
        raise ValueError(f"unknown size unit {unit!r} in {raw!r}")
    value = float(number_text) * _SIZE_UNITS[unit]
    if value <= 0:
        raise ValueError(f"size must be positive, got {raw!r}")
    return int(value)


def _broadcaster_live(socket_path: Path) -> bool:
    """Return True if the broadcast daemon appears live.

    Presence of the AF_UNIX socket file is the signal. The socket is
    bound by the listener at daemon startup and removed at shutdown;
    a stale file after a crash is rare (systemd unit RuntimeDirectory
    handling clears it on restart). False positives here are safer than
    false negatives — refusing to prune while the daemon is up only
    inconveniences the operator, whereas running ``VACUUM INTO`` on a
    live WAL produces a corrupt destination file.
    """
    return socket_path.exists()


def _delete_ids_with_retry(db: Path, event_ids: list[str]) -> None:
    """Run the DELETE inside ``BEGIN IMMEDIATE`` with the project's
    shared SQLITE_BUSY retry budget. Mirrors ``migrations._apply_one_with_retry``.

    A no-op (empty id list) returns immediately without opening a
    transaction.
    """
    if not event_ids:
        return
    with connect(db, isolation_level=None) as conn:
        conn.execute("PRAGMA busy_timeout=0")
        for attempt in range(_ENSURE_SCHEMA_RETRIES):
            try:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    placeholders = ",".join("?" * len(event_ids))
                    conn.execute(
                        f"DELETE FROM events WHERE event_id IN ({placeholders})",
                        event_ids,
                    )
                    conn.execute("COMMIT")
                    return
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
            except sqlite3.OperationalError as exc:
                if not _is_busy_or_locked(exc) or attempt == _ENSURE_SCHEMA_RETRIES - 1:
                    raise
                time.sleep(_ENSURE_SCHEMA_BACKOFF_SEC)


def _collect_doomed_ids(
    conn: sqlite3.Connection,
    *,
    max_age_seconds: float,
    max_size_bytes: int,
    max_rows: int | None,
    current_db_bytes: int,
) -> tuple[list[str], int]:
    """Compute the union of event_ids targeted by each cap.

    Returns ``(ordered_event_ids, estimated_bytes_freed)``. ``event_ids``
    are returned in ascending-ULID (chronological) order so the caller
    can log oldest/newest deletion bounds if it wants.

    Cap semantics:
    * Time: every row with ``received_at`` older than the cutoff.
    * Size: estimate ``avg_row_bytes = current_db_bytes / row_count``
      (whole-file divided by row count so SQLite's index / page-header
      overhead is folded into the per-row footprint). If the file
      exceeds ``max_size_bytes``, delete the oldest rows until the
      survivors fit under the budget at that per-row estimate. The
      estimate is approximate; the post-VACUUM file may sit a few
      percent on either side of the cap, and operators who need a
      hard ceiling should re-run prune after the vacuum.
    * Row count: ``max_rows`` is a safety cap; delete oldest excess.
    """
    cutoff_ns = (time.time() - max_age_seconds) * _NS_PER_SEC
    doomed: set[str] = set()

    age_rows = conn.execute(
        "SELECT event_id FROM events WHERE event_id IS NOT NULL AND received_at < ? ORDER BY event_id",
        (int(cutoff_ns),),
    ).fetchall()
    doomed.update(row[0] for row in age_rows)

    if current_db_bytes > max_size_bytes:
        agg = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload_json)), 0) FROM events WHERE event_id IS NOT NULL"
        ).fetchone()
        row_count, payload_sum = int(agg[0]), int(agg[1])
        if row_count > 0 and payload_sum > 0:
            # Derive a per-row footprint that includes SQLite's overhead
            # (index pages, b-tree fanout, WAL frames). Using a pure
            # payload-only avg under-estimates the per-row cost on small
            # DBs where the schema and indexes dominate, and over-counts
            # rows-to-drop until the file would be empty.
            avg_row_bytes = max(current_db_bytes // row_count, 1)
            target_rows = max_size_bytes // avg_row_bytes
            rows_to_drop = max(row_count - target_rows, 0)
            size_rows = conn.execute(
                "SELECT event_id FROM events WHERE event_id IS NOT NULL ORDER BY event_id LIMIT ?",
                (rows_to_drop,),
            ).fetchall()
            doomed.update(row[0] for row in size_rows)

    if max_rows is not None:
        total = conn.execute("SELECT COUNT(*) FROM events WHERE event_id IS NOT NULL").fetchone()[0]
        excess = int(total) - max_rows
        if excess > 0:
            row_cap_rows = conn.execute(
                "SELECT event_id FROM events WHERE event_id IS NOT NULL ORDER BY event_id LIMIT ?",
                (excess,),
            ).fetchall()
            doomed.update(row[0] for row in row_cap_rows)

    if not doomed:
        return [], 0

    placeholders = ",".join("?" * len(doomed))
    doomed_list = sorted(doomed)
    freed_row = conn.execute(
        f"SELECT COALESCE(SUM(LENGTH(payload_json)), 0) FROM events WHERE event_id IN ({placeholders})",
        doomed_list,
    ).fetchone()
    return doomed_list, int(freed_row[0])


def _vacuum_into(db: Path) -> None:
    """``VACUUM INTO '<db>.new'`` then atomically replace the source.

    The destination must NOT exist (SQLite errors out otherwise); a
    leftover ``.new`` from a previous interrupted run is removed first.
    """
    target = db.with_suffix(db.suffix + ".new")
    if target.exists():
        target.unlink()
    with connect(db, isolation_level=None) as conn:
        conn.execute("VACUUM INTO ?", (str(target),))
    os.replace(target, db)


def prune(
    max_size: str = typer.Option(
        _DEFAULT_MAX_SIZE,
        "--max-size",
        help="Byte budget. Accepts bare bytes or KB/MB/GB/KiB/MiB/GiB.",
    ),
    max_age: str = typer.Option(
        _DEFAULT_MAX_AGE,
        "--max-age",
        help="Age budget. Accepts s/m/h/d suffix (e.g. 30d, 12h).",
    ),
    max_rows: int | None = typer.Option(
        None,
        "--max-rows",
        help="Safety row cap. No default; opt-in only.",
        min=1,
    ),
    vacuum: bool = typer.Option(
        False,
        "--vacuum",
        help="After DELETE, run VACUUM INTO + atomic rename to reclaim disk.",
    ),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--no-dry-run",
        help="Default ON. Report the plan without writing. Pass --no-dry-run to actually delete.",
    ),
    db: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None,
        "--db",
        help="Override the events DB path (default: platformdirs).",
    ),
) -> None:
    """Trim the events table by age, size, and/or row count.

    The default retention policy of the bus is "keep everything"; this
    verb is opt-in and dry-run by default. Refuses to run while the
    broadcast daemon's socket is present (the WAL writer must be down
    before VACUUM INTO is safe).
    """
    try:
        max_age_seconds = parse_duration(max_age)
        max_size_bytes = _parse_size(max_size)
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(2) from exc

    target_db = db if db is not None else _paths.db_path()
    if not target_db.exists():
        typer.echo(f"error: db not found: {target_db}", err=True)
        raise typer.Exit(2)

    if _broadcaster_live(_paths.broadcast_socket()):
        typer.echo(
            "error: broadcast daemon socket is present; stop the daemon "
            "before pruning (VACUUM INTO is unsafe against a live WAL writer).",
            err=True,
        )
        raise typer.Exit(2)

    current_bytes = target_db.stat().st_size
    with connect(target_db, readonly=True) as conn:
        doomed_ids, bytes_freed_est = _collect_doomed_ids(
            conn,
            max_age_seconds=max_age_seconds,
            max_size_bytes=max_size_bytes,
            max_rows=max_rows,
            current_db_bytes=current_bytes,
        )

    typer.echo(
        f"plan: rows_to_delete={len(doomed_ids)} "
        f"bytes_freed_est={bytes_freed_est} "
        f"db_size={current_bytes} max_size={max_size_bytes} "
        f"max_age_s={int(max_age_seconds)} vacuum={vacuum} dry_run={dry_run}"
    )

    if dry_run:
        raise typer.Exit(0)

    _delete_ids_with_retry(target_db, doomed_ids)
    if vacuum:
        _vacuum_into(target_db)

    structured(
        logger,
        logging.INFO,
        "db_prune",
        rows_deleted=len(doomed_ids),
        bytes_freed_est=bytes_freed_est,
        vacuumed=vacuum,
    )
    raise typer.Exit(0)
