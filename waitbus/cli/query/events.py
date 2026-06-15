"""`events` sub-app — direct event-store SQL passthrough (read-only)."""

from __future__ import annotations

from pathlib import Path

import typer

from .._shared import _sub_version_callback

events_app = typer.Typer(
    name="events",
    help="Direct event-store SQL passthrough (read-only).",
    no_args_is_help=True,
    add_completion=False,
)


@events_app.callback()
def _events_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """Direct event-store SQL passthrough sub-commands."""


@events_app.command(name="query")
def events_query(
    sql: str = typer.Argument(
        ...,
        help=(
            "Literal SQL to execute. Must be a single SELECT or WITH-rooted "
            "(CTE) statement; INSERT/UPDATE/DELETE/DROP/PRAGMA/ATTACH/DETACH "
            "are rejected at parse time and the connection is opened "
            "read-only. The events table columns are: delivery_id, source, "
            "event_type, owner, repo, run_id, workflow_name, head_branch, "
            "head_sha, status, conclusion, received_at, payload_json, "
            "ingest_method, job_id, job_name, parent_run_id, alert_name, "
            "alert_severity, alert_fingerprint, event_id."
        ),
    ),
    limit: int = typer.Option(
        1000,
        "--limit",
        "-n",
        help="Cap injected at the outer level of the statement. If the SQL "
        "already has a trailing LIMIT N, the smaller of N and this "
        "value is used. Defaults to 1000.",
    ),
    no_limit: bool = typer.Option(
        False,
        "--no-limit",
        help="Disable the outer LIMIT injection. Use when an unbounded scan "
        "is intentional; the operator owns the runtime in that case.",
    ),
    as_json: bool = typer.Option(
        True,
        "--json/--text",
        help="Output format. --json (default) prints a JSON array with one "
        "object per row; --text prints `key: value` blocks separated "
        "by blank lines.",
    ),
    db: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None,
        "--db",
        help="Path to the events SQLite DB. Defaults to the platformdirs-"
        "resolved location (typically ~/.local/state/waitbus/"
        "github.db on Linux).",
        exists=False,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
) -> None:
    """Run an operator-supplied SELECT against the local events store.

    The connection is opened read-only (``file:...?mode=ro``) so write
    DDL/DML cannot land even if the parse-time gates were bypassed. A
    trailing LIMIT is injected (or the existing one capped) unless
    ``--no-limit`` is passed. See SECURITY.md for the full threat model.
    """
    import waitbus.events_query as mod

    raise typer.Exit(
        mod.cli_entry(
            sql=sql,
            limit=limit,
            no_limit=no_limit,
            as_json=as_json,
            db_path=db,
        )
    )


@events_app.command(name="analyze")
def events_analyze(
    sql: str = typer.Argument(
        ...,
        help=(
            "Analytical SQL run through DuckDB against the events store "
            "(attached READ_ONLY as `ev`; query `ev.events`). Must be a "
            "single SELECT or WITH-rooted statement; the same parse-time "
            "gate as `events query` applies. DuckDB unlocks window "
            "functions, QUALIFY, PIVOT, and LIST/STRUCT aggregates. "
            "Requires the 'analyze' extra: pip install waitbus[analyze]."
        ),
    ),
    as_json: bool = typer.Option(
        True,
        "--json/--text",
        help="Output format. --json (default) prints a JSON array with one "
        "object per row; --text prints `key: value` blocks.",
    ),
    db: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None,
        "--db",
        help="Path to the events SQLite DB. Defaults to the platformdirs-resolved location.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
) -> None:
    """Run analytical SQL over the events store via DuckDB.

    DuckDB attaches the SQLite events database READ_ONLY and exposes
    ``ev.events``. The operator SQL passes the same single-statement
    SELECT/WITH gate as ``events query``; the READ_ONLY attach means
    no write can land. ``duckdb`` ships behind the optional ``analyze``
    extra and is imported lazily.
    """
    import waitbus.events_analyze as mod

    raise typer.Exit(
        mod.cli_entry(
            sql=sql,
            as_json=as_json,
            db_path=db,
        )
    )
