"""Analytical SQL over the events store via DuckDB's sqlite scanner.

``waitbus events analyze`` runs operator-supplied analytical SQL against
the local SQLite events store through DuckDB. DuckDB attaches the
SQLite database READ_ONLY via its bundled ``sqlite`` extension and
exposes ``ev.events`` to the operator's query, unlocking window
functions, ``QUALIFY``, ``PIVOT``, ``LIST``/``STRUCT`` aggregates and
the rest of DuckDB's analytical surface that plain SQLite lacks.

DuckDB is an optional dependency behind the ``analyze`` extra
(``pip install waitbus[analyze]``). It is not in the runtime closure:
the daemons and the read-only ``waitbus events query`` path stay
stdlib-plus-sqlite3 only. Import is lazy so a missing extra produces a
clear remediation hint rather than an ImportError traceback.

Security model is shared with ``events_query``: the operator SQL is
run through the same single-statement / SELECT-or-WITH-only /
forbidden-token gate (``events_query.validate``), and the SQLite
attachment is opened READ_ONLY so no write can land even if a gate
were bypassed. The ATTACH statement itself is emitted by this module,
never by the operator.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from . import _paths
from .events_query import QueryRejectedError, validate

_MISSING_DUCKDB_HINT = (
    "waitbus events analyze: the 'analyze' extra is not installed. "
    "Install it with `pip install waitbus[analyze]` (or "
    "`uv pip install waitbus[analyze]`) to enable DuckDB-backed analysis."
)


@dataclass(frozen=True, slots=True)
class AnalyzeRequest:
    """A validated request to run analytical SQL over the events store."""

    sql: str
    as_json: bool
    db_path: Path


def _emit_json(columns: list[str], rows: list[tuple[object, ...]]) -> None:
    """Print every row as one element in a JSON array.

    Each row is rendered as a ``{column: value}`` object zipped from the
    operator-supplied projection's column descriptions. ``default=str`` on
    ``json.dumps`` covers DuckDB types that do not natively round-trip
    through JSON (``datetime.datetime``, ``decimal.Decimal``, ``Interval``,
    etc.) — load-bearing here because DuckDB exposes a richer type system
    than the underlying SQLite events table.
    """
    import json

    payload = [dict(zip(columns, row, strict=True)) for row in rows]
    print(json.dumps(payload, default=str, indent=2))


def _emit_text(columns: list[str], rows: list[tuple[object, ...]]) -> None:
    """Print every row as a key: value block separated by blank lines.

    Mirrors ``events_query._emit_text``'s shape. Empty result sets print
    nothing — the CLI relies on the absence of output (and the exit
    code) to indicate the query ran but matched zero rows.
    """
    blocks = []
    for row in rows:
        blocks.append("\n".join(f"{col}: {val}" for col, val in zip(columns, row, strict=True)))
    print("\n\n".join(blocks))


def run_analyze(req: AnalyzeRequest) -> int:
    """Execute the analytical request; return the process exit code.

    Returns 0 on success (including zero rows), 2 on parse-time
    rejection, missing DB file, missing duckdb extra, or any DuckDB
    error. Errors print a single stderr line; success prints to
    stdout only.
    """
    try:
        validated = validate(req.sql)
    except QueryRejectedError as exc:
        print(f"waitbus events analyze: {exc}", file=sys.stderr)
        return 2

    if not req.db_path.exists():
        print(
            f"waitbus events analyze: events DB not found at {req.db_path}. Run `waitbus init` first.",
            file=sys.stderr,
        )
        return 2

    try:
        import duckdb
    except ImportError:
        print(_MISSING_DUCKDB_HINT, file=sys.stderr)
        return 2

    try:
        conn = duckdb.connect(database=":memory:")
        try:
            conn.execute("INSTALL sqlite; LOAD sqlite;")
            # ATTACH is a parser-level statement: DuckDB does not bind
            # `?` here. The path is operator-controlled (the --db flag
            # or the platformdirs default), not external input; double
            # any single quote per the SQL-standard escape so a path
            # containing a quote cannot break out of the literal.
            db_literal = str(req.db_path).replace("'", "''")
            conn.execute(f"ATTACH '{db_literal}' AS ev (TYPE sqlite, READ_ONLY);")
            cur = conn.execute(validated)
            columns = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
        finally:
            conn.close()
    except duckdb.Error as exc:
        print(f"waitbus events analyze: duckdb error: {exc}", file=sys.stderr)
        return 2

    if req.as_json:
        _emit_json(columns, rows)
    else:
        _emit_text(columns, rows)
    return 0


def cli_entry(
    sql: str,
    *,
    as_json: bool,
    db_path: Path | None,
) -> int:
    """Thin adapter from the typer command to ``run_analyze``."""
    effective_db = _paths.resolve_db_path(db_path)
    return run_analyze(AnalyzeRequest(sql=sql, as_json=as_json, db_path=effective_db))
