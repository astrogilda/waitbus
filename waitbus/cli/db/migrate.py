"""`migrate` top-level command — apply numbered SQL schema migrations."""

from __future__ import annotations

from pathlib import Path

import typer

from ..._paths import db_path, ensure_state_dirs


def migrate(
    status: bool = typer.Option(
        False,
        "--status",
        help="Print applied and pending migrations; exit 0 without applying anything.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the SQL that would run for every pending migration; exit 0 without modifying the DB.",
    ),
    to: int | None = typer.Option(
        None,
        "--to",
        help="Apply migrations up to (and including) this sequence "
        "number. Pending migrations above the target stay on disk.",
        min=1,
    ),
) -> None:
    """Apply numbered SQL schema migrations to the events DB.

    Default: apply every pending migration in sequence order. Idempotent;
    re-running after every migration has landed is a no-op.

    --status prints the applied/pending breakdown without changing the
    DB. --dry-run prints the SQL each pending migration would issue.
    --to NNNN bounds the apply pass at sequence number NNNN.

    The migrations directory is the ``waitbus/migrations/``
    package; the on-disk SHA-256 of each ``.sql`` file is recorded in
    ``schema_migrations`` so post-apply edits surface as a clear error
    on the next call instead of silent schema drift.
    """
    from ..._db import ensure_schema

    db = db_path()
    # ensure_schema is idempotent and creates the events table on a fresh
    # DB so migrate works against either a brand-new install or one
    # already touched by waitbus init. The schema_migrations table
    # is created lazily by read_applied so callers do not need a separate
    # bootstrap step.
    if not db.exists():
        ensure_state_dirs()
        ensure_schema(db)

    if status:
        _print_status(db)
    if dry_run:
        _print_dry_run(db, to)
    _apply(db, to)


def _print_status(db: Path) -> None:
    """Print the applied/pending migration breakdown and exit 0."""
    from ... import migrations as migrations_pkg
    from ..._db import connect

    with connect(db, isolation_level=None) as conn:
        entries = migrations_pkg.plan(conn)
    applied_entries = [e for e in entries if e.state == "applied"]
    pending_entries = [e for e in entries if e.state == "pending"]
    typer.echo(f"applied: {len(applied_entries)}")
    for entry in applied_entries:
        mig = entry.migration
        typer.echo(f"  {mig.sequence_number:04d}_{mig.slug} sha256={mig.sha256[:12]}")
    typer.echo(f"pending: {len(pending_entries)}")
    for entry in pending_entries:
        mig = entry.migration
        typer.echo(f"  {mig.sequence_number:04d}_{mig.slug} sha256={mig.sha256[:12]}")
    raise typer.Exit(0)


def _print_dry_run(db: Path, to: int | None) -> None:
    """Print the SQL each pending migration would issue and exit 0."""
    from ... import migrations as migrations_pkg
    from ..._db import connect

    with connect(db, isolation_level=None) as conn:
        pending = migrations_pkg.pending_migrations(conn, target=to)
    if not pending:
        typer.echo("No pending migrations.")
        raise typer.Exit(0)
    for mig in pending:
        typer.echo(f"-- migration {mig.sequence_number:04d}_{mig.slug} (sha256={mig.sha256[:12]})")
        typer.echo(mig.sql_text.rstrip())
        if mig.py_path is not None:
            typer.echo(f"-- python hook: {mig.py_path.name}")
        typer.echo("")
    raise typer.Exit(0)


def _apply(db: Path, to: int | None) -> None:
    """Apply every pending migration up to ``to`` and report the result."""
    from ... import migrations as migrations_pkg

    applied_now = migrations_pkg.apply_pending(db, target=to)
    if not applied_now:
        typer.echo("No pending migrations.")
        raise typer.Exit(0)
    for mig in applied_now:
        typer.echo(f"applied {mig.sequence_number:04d}_{mig.slug}")
    typer.echo(f"applied {len(applied_now)} migration(s).")
