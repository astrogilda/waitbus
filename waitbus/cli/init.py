"""`init` top-level command — bootstrap state directories, schema, scaffolds."""

from __future__ import annotations

import typer

from .. import _paths
from .._paths import db_path, ensure_state_dirs, etag_state, watched_repos
from ._shared import (
    ETAG_STATE_TEMPLATE,
    WATCHED_REPOS_TEMPLATE,
    _check_binaries,
    _migrate_legacy_state_if_needed,
)


def init(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print what would be done, don't modify the filesystem or keyring.",
    ),
) -> None:
    """Bootstrap waitbus state for first use.

    Idempotent. Safe to re-run. Creates the platformdirs state and
    runtime directories, the SQLite schema (delegated to
    `_db.ensure_schema`), and scaffold files `watched_repos.txt`
    + `etag_state.json` if absent. On the first run after upgrading
    from a legacy install, also transparently moves any legacy state
    data to the resolved state directory.

    Does NOT install systemd units (use `waitbus install-systemd`)
    or generate keyring secrets (use `waitbus keygen`).
    """
    from .._db import ensure_schema  # avoid circular import at module load

    typer.echo(f"waitbus init (dry-run={dry_run})")

    # 0. Transparent migration from the legacy legacy state path.
    # Idempotent: no-op once the move has happened or if there was
    # nothing to migrate. Skipped under --dry-run because
    # systemctl stop + shutil.move have permanent side-effects.
    if not dry_run:
        _migrate_legacy_state_if_needed()

    # 1. Required binaries (advisory only — init never blocks on missing bins).
    _check_binaries()

    # 2. State directories
    if dry_run:
        typer.echo(f"  Would create: {_paths.state_dir()}/, {_paths.cursors_dir()}/, {_paths.runtime_dir()}/")
    else:
        ensure_state_dirs()
        typer.echo(
            f"  Created (or already present): {_paths.state_dir()}/, {_paths.cursors_dir()}/, {_paths.runtime_dir()}/"
        )

    # 3. SQLite schema
    if dry_run:
        typer.echo(f"  Would bootstrap SQLite schema at {db_path()}")
    else:
        ensure_schema(db_path())
        # Mark every shipped migration as already-applied so the next
        # `waitbus migrate` call against this fresh DB is a no-op.
        # ensure_schema materialises schema.sql, which is the contents of
        # the highest-numbered migration; recording the migrations as
        # applied keeps the schema_migrations tracking table consistent
        # with that on-disk state.
        from .. import migrations as migrations_pkg

        migrations_pkg.mark_baseline_applied(db_path())
        typer.echo(f"  SQLite schema bootstrapped at {db_path()}")

    # 4. Scaffolds
    if not watched_repos().exists():
        if dry_run:
            typer.echo(f"  Would create scaffold: {watched_repos()}")
        else:
            watched_repos().write_text(WATCHED_REPOS_TEMPLATE)
            typer.echo(f"  Created scaffold: {watched_repos()}")
    else:
        typer.echo(f"  Scaffold already present: {watched_repos()}")

    if not etag_state().exists():
        if dry_run:
            typer.echo(f"  Would create scaffold: {etag_state()}")
        else:
            etag_state().write_text(ETAG_STATE_TEMPLATE)
            typer.echo(f"  Created scaffold: {etag_state()}")
    else:
        typer.echo(f"  Scaffold already present: {etag_state()}")

    typer.echo("")
    typer.echo("Next steps:")
    typer.echo("  waitbus install-credentials github-webhook-secret   # encrypt + stage")
    typer.echo("  waitbus install-systemd  # copy units + daemon-reload + enable")
