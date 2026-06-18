"""Numbered SQL schema-migration tooling for the waitbus event store.

Migrations are sequence-numbered ``.sql`` files (optionally accompanied by
a same-stem ``.py`` hook exposing ``apply(conn) -> None``) that the
``waitbus migrate`` CLI applies idempotently inside one
``BEGIN IMMEDIATE`` transaction per file.

Layout (this directory):

  NNNN_<slug>.sql   — DDL applied via ``conn.executescript``.
  NNNN_<slug>.py    — optional Python hook for non-SQL operations
                      (e.g. backfilling a column from a Python expression).
                      Must export ``def apply(conn: sqlite3.Connection) -> None:``.

Tracking table (``schema_migrations``) records every applied entry's
sequence number, slug, applied-at timestamp (epoch ns), and SHA-256 of
the ``.sql`` file content for tamper detection.

Design boundary with ``_db.ensure_schema``: the bootstrap path (fresh DB)
remains owned by ``ensure_schema`` (which materialises the current
``schema.sql``). The migrations tooling handles *evolution* of an
already-bootstrapped DB; ``waitbus init`` runs both, marking the
baseline migration as applied on a fresh DB.

See the "Schema migrations" section of ``docs/ARCHITECTURE.md`` for the
operator-facing runbook.
"""

from __future__ import annotations

import hashlib
import importlib.util
import logging
import re
import sqlite3
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from .._db import (
    _ENSURE_SCHEMA_BACKOFF_SEC,
    _ENSURE_SCHEMA_RETRIES,
    _is_busy_or_locked,
    connect,
    split_sql_statements,
)
from .._log import structured

logger = logging.getLogger("waitbus.migrations")

MIGRATIONS_DIR: Path = Path(__file__).resolve().parent
"""Filesystem location of the shipped migration files. Kept package-relative
so editable installs and built wheels resolve the same set."""

_FILENAME_RE = re.compile(r"^(?P<seq>\d{4})_(?P<slug>[a-z][a-z0-9_]*)\.sql$")
"""Migration filenames must be ``NNNN_<snake_case>.sql``. Anything else
in this directory (e.g. ``__init__.py``, the optional ``.py`` hook) is
ignored by the discovery pass."""

_SCHEMA_MIGRATIONS_DDL = """\
CREATE TABLE IF NOT EXISTS schema_migrations (
    sequence_number INTEGER PRIMARY KEY,
    slug TEXT NOT NULL,
    applied_at_ns INTEGER NOT NULL,
    sha256 TEXT NOT NULL
)
"""


@dataclass(frozen=True)
class Migration:
    """One discovered migration on disk.

    Attributes:
        sequence_number: zero-padded integer parsed from the filename
            (``0001`` -> 1). Must be unique across the migrations dir.
        slug: snake_case identifier from the filename. Recorded in
            ``schema_migrations.slug`` for human-readable status output.
        sql_path: absolute path to the ``.sql`` DDL file.
        py_path: absolute path to the same-stem ``.py`` hook if it exists,
            otherwise None. The hook (when present) runs *after* the SQL
            block inside the same transaction.
        sha256: hex SHA-256 of the ``.sql`` file's UTF-8-encoded contents.
            Compared against ``schema_migrations.sha256`` to detect post-
            apply edits.
    """

    sequence_number: int
    slug: str
    sql_path: Path
    py_path: Path | None
    sha256: str

    @property
    def sql_text(self) -> str:
        """Return the SQL file's contents. Re-read on every access so a
        hot-reloaded migration during a single CLI invocation surfaces."""
        return self.sql_path.read_text(encoding="utf-8")


def _hash_file(path: Path) -> str:
    """Return the hex SHA-256 of ``path``'s bytes. Used to detect post-
    apply edits to a migration file; the operator who tampers with a
    landed migration gets a clear error from the next ``migrate`` call
    rather than a silent schema drift."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def discover_migrations(migrations_dir: Path | None = None) -> list[Migration]:
    """Return all migrations under ``migrations_dir`` sorted by sequence
    number. Skips ``__init__.py`` and any file not matching
    ``NNNN_<slug>.sql``. Raises ``RuntimeError`` if two files share a
    sequence number (the operator who lands two ``0003_*`` files at the
    same time gets a loud error here, not at apply time)."""
    directory = migrations_dir if migrations_dir is not None else MIGRATIONS_DIR
    by_seq: dict[int, Migration] = {}
    for entry in sorted(directory.iterdir()):
        if not entry.is_file():
            continue
        match = _FILENAME_RE.match(entry.name)
        if not match:
            continue
        seq = int(match.group("seq"))
        slug = match.group("slug")
        py_path = entry.with_suffix(".py")
        py_path_opt = py_path if py_path.exists() else None
        if seq in by_seq:
            raise RuntimeError(
                f"duplicate migration sequence number {seq:04d}: {by_seq[seq].sql_path.name} and {entry.name}"
            )
        by_seq[seq] = Migration(
            sequence_number=seq,
            slug=slug,
            sql_path=entry,
            py_path=py_path_opt,
            sha256=_hash_file(entry),
        )
    return [by_seq[k] for k in sorted(by_seq)]


@dataclass(frozen=True)
class AppliedRow:
    """One row from the ``schema_migrations`` tracking table."""

    sequence_number: int
    slug: str
    applied_at_ns: int
    sha256: str


def _ensure_tracking_table(conn: sqlite3.Connection) -> None:
    """Create the ``schema_migrations`` tracking table if it does not
    already exist. Idempotent on every call."""
    conn.execute(_SCHEMA_MIGRATIONS_DDL)


def read_applied(conn: sqlite3.Connection) -> list[AppliedRow]:
    """Return every applied migration sorted by sequence number. The
    tracking table is created lazily so callers do not need to bootstrap
    it before reading."""
    _ensure_tracking_table(conn)
    rows = conn.execute(
        "SELECT sequence_number, slug, applied_at_ns, sha256 FROM schema_migrations ORDER BY sequence_number"
    ).fetchall()
    return [AppliedRow(sequence_number=r[0], slug=r[1], applied_at_ns=r[2], sha256=r[3]) for r in rows]


def _load_py_hook(py_path: Path) -> Callable[[sqlite3.Connection], None]:
    """Import the ``.py`` companion file and return its ``apply`` callable.
    The hook is loaded by file path (not by package import) so a migration
    can be added without editing ``__init__.py``. Raises ``RuntimeError``
    if the file does not export ``apply``."""
    spec = importlib.util.spec_from_file_location(
        f"waitbus.migrations._hook_{py_path.stem}",
        py_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load migration hook: {py_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    hook = getattr(module, "apply", None)
    if not callable(hook):
        raise RuntimeError(f"migration hook {py_path} must export apply(conn: sqlite3.Connection)")
    return hook  # type: ignore[no-any-return]


def _verify_no_drift(applied: Iterable[AppliedRow], discovered: Iterable[Migration]) -> None:
    """Raise ``RuntimeError`` if an applied migration's recorded SHA-256
    no longer matches the on-disk file. Catches operator edits to
    landed migrations."""
    discovered_by_seq = {m.sequence_number: m for m in discovered}
    for row in applied:
        mig = discovered_by_seq.get(row.sequence_number)
        if mig is None:
            raise RuntimeError(
                f"applied migration {row.sequence_number:04d}_{row.slug} not found on disk; refusing to proceed"
            )
        if mig.sha256 != row.sha256:
            raise RuntimeError(
                f"checksum drift for migration {row.sequence_number:04d}_"
                f"{row.slug}: on-disk SHA-256 differs from the value "
                "recorded in schema_migrations. Refusing to proceed. "
                "Restore the file's original contents, or roll the change "
                "forward in a new numbered migration."
            )


def _verify_no_gaps(applied: Iterable[AppliedRow], discovered: Iterable[Migration]) -> None:
    """Raise ``RuntimeError`` if the discovered set contains a gap in the
    pending range (e.g. 0001 applied, 0003 on disk, 0002 missing). The
    apply path is strict: every sequence number from 1 up to the highest
    discovered must be present on disk."""
    discovered_seqs = sorted(m.sequence_number for m in discovered)
    if not discovered_seqs:
        return
    applied_set = {r.sequence_number for r in applied}
    for expected, actual in enumerate(discovered_seqs, start=1):
        if expected != actual:
            # Gap detected. Direct the operator to the missing file rather
            # than silently skipping it (that's how schema drift bugs are
            # born).
            raise RuntimeError(
                f"migration gap detected: expected sequence {expected:04d}, "
                f"found {actual:04d}. Out-of-order migrations are not "
                "supported. Add the missing file before running migrate."
            )
        # Bonus check: if a later number was already applied but a number
        # below it isn't on disk, that's also drift.
        if actual in applied_set:
            continue


def _apply_one(conn: sqlite3.Connection, migration: Migration) -> None:
    """Apply one migration's SQL + optional Python hook inside the current
    transaction. The caller owns ``BEGIN IMMEDIATE`` / ``COMMIT``."""
    sql_text = migration.sql_text
    # executescript() would issue an implicit COMMIT first, breaking the
    # outer transaction boundary; drive statements individually instead.
    for stmt in split_sql_statements(sql_text):
        conn.execute(stmt)
    if migration.py_path is not None:
        hook = _load_py_hook(migration.py_path)
        hook(conn)
    conn.execute(
        "INSERT INTO schema_migrations (sequence_number, slug, applied_at_ns, sha256) VALUES (?, ?, ?, ?)",
        (
            migration.sequence_number,
            migration.slug,
            time.time_ns(),
            migration.sha256,
        ),
    )


@dataclass(frozen=True)
class PlanEntry:
    """One row in a migration plan: the migration plus its apply state."""

    migration: Migration
    state: str  # "applied" | "pending"


def plan(
    conn: sqlite3.Connection,
    *,
    target: int | None = None,
    migrations_dir: Path | None = None,
) -> list[PlanEntry]:
    """Return the ordered plan of migrations and their current state.

    Args:
        conn: open SQLite connection to the events DB.
        target: optional ceiling — only migrations with
            ``sequence_number <= target`` are returned as plan entries.
            Pending migrations above the target stay on disk but do not
            appear in the plan.
        migrations_dir: override the package-relative migrations
            directory (test helper).

    Raises:
        RuntimeError: on checksum drift or sequence gaps.
    """
    discovered = discover_migrations(migrations_dir)
    applied = read_applied(conn)
    _verify_no_drift(applied, discovered)
    _verify_no_gaps(applied, discovered)
    applied_set = {r.sequence_number for r in applied}
    entries: list[PlanEntry] = []
    for mig in discovered:
        if target is not None and mig.sequence_number > target:
            continue
        state = "applied" if mig.sequence_number in applied_set else "pending"
        entries.append(PlanEntry(migration=mig, state=state))
    return entries


def pending_migrations(
    conn: sqlite3.Connection,
    *,
    target: int | None = None,
    migrations_dir: Path | None = None,
) -> list[Migration]:
    """Return the migrations that are on disk but not yet recorded in
    ``schema_migrations``, filtered by ``target`` and ordered."""
    return [
        entry.migration
        for entry in plan(conn, target=target, migrations_dir=migrations_dir)
        if entry.state == "pending"
    ]


def apply_pending(
    db_path: Path,
    *,
    target: int | None = None,
    migrations_dir: Path | None = None,
) -> list[Migration]:
    """Apply every pending migration in order, each inside its own
    ``BEGIN IMMEDIATE`` transaction. Returns the list of migrations that
    were applied (empty if the DB was already up to date).

    Two concurrent ``migrate`` invocations serialise cleanly: the loser
    of the ``BEGIN IMMEDIATE`` race gets SQLITE_BUSY, retries through
    the existing ``_ENSURE_SCHEMA_*`` budget, and on retry sees the
    winner's commit so its own apply pass becomes a no-op.
    """
    applied_now: list[Migration] = []
    # sqlite3.Connection's context manager commits/rolls back but does NOT
    # close the underlying handle. Wrap in contextlib.closing so the
    # connection is released on every exit path (otherwise Python 3.14's
    # ResourceWarning hook fires when the connection is GC'd in a later
    # test, surfacing as a PytestUnraisableExceptionWarning).
    with connect(db_path, isolation_level=None) as conn:
        conn.execute("PRAGMA busy_timeout=0")
        pending = pending_migrations(conn, target=target, migrations_dir=migrations_dir)
        for migration in pending:
            _apply_one_with_retry(conn, migration)
            applied_now.append(migration)
            structured(
                logger,
                logging.INFO,
                "migration_applied",
                sequence_number=migration.sequence_number,
                slug=migration.slug,
                sha256=migration.sha256,
            )
    return applied_now


def _apply_one_with_retry(conn: sqlite3.Connection, migration: Migration) -> None:
    """Wrap ``_apply_one`` in ``BEGIN IMMEDIATE`` plus the SQLITE_BUSY
    retry budget shared with ``ensure_schema``. On retry the connection
    re-reads ``schema_migrations`` and skips the migration if a
    concurrent invocation already landed it."""
    for attempt in range(_ENSURE_SCHEMA_RETRIES):
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                already = conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE sequence_number = ?",
                    (migration.sequence_number,),
                ).fetchone()
                if already is not None:
                    conn.execute("ROLLBACK")
                    return
                _apply_one(conn, migration)
                conn.execute("COMMIT")
                return
            except Exception:
                conn.execute("ROLLBACK")
                raise
        except sqlite3.OperationalError as exc:
            if not _is_busy_or_locked(exc) or attempt == _ENSURE_SCHEMA_RETRIES - 1:
                raise
            time.sleep(_ENSURE_SCHEMA_BACKOFF_SEC)


def mark_baseline_applied(db_path: Path) -> None:
    """Record every discovered migration as already-applied without
    executing its DDL. Used by ``waitbus init`` after
    ``ensure_schema`` has bootstrapped a fresh DB from ``schema.sql``
    (whose contents already match the highest-numbered migration)."""
    discovered = discover_migrations()
    with connect(db_path, isolation_level=None) as conn:
        conn.execute("PRAGMA busy_timeout=0")
        _ensure_tracking_table(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_ns = time.time_ns()
            for mig in discovered:
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations "
                    "(sequence_number, slug, applied_at_ns, sha256) "
                    "VALUES (?, ?, ?, ?)",
                    (mig.sequence_number, mig.slug, now_ns, mig.sha256),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
