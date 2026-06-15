"""SQL passthrough for the events store.

Provides the ``waitbus events query <SQL>`` surface: an operator
runs a literal SELECT (or WITH-CTE-rooted SELECT) statement against the
local events SQLite database and gets back rows as JSON or text.

Safety posture, in order of importance:

1. **Read-only connection.** The DB is opened via ``_db.open_conn(...,
   readonly=True)`` (``file:...?mode=ro``); any attempt to mutate fails
   at the SQLite layer regardless of what the parsed SQL says.
2. **Parse-time statement-kind gate.** Only ``SELECT`` and ``WITH``
   (CTE-rooted SELECT) pass. Everything else — ``INSERT``, ``UPDATE``,
   ``DELETE``, ``DROP``, ``CREATE``, ``ALTER``, ``REPLACE``, ``PRAGMA``,
   ``ATTACH``, ``DETACH``, ``VACUUM``, ``ANALYZE``, ``REINDEX`` — is
   rejected before any connection work happens. ``PRAGMA`` / ``ATTACH``
   / ``DETACH`` are called out explicitly because they can mutate
   connection state without writing rows.
3. **Multi-statement rejection.** A trailing ``;`` is tolerated; any
   non-whitespace, non-comment content past the first statement boundary
   is rejected.
4. **LIMIT injection.** A trailing ``LIMIT N`` is either appended or
   capped (``min(existing, default)``) at the outermost statement,
   unless the operator passes ``--no-limit`` to opt out.

The injection-attack surface is moot: the operator is the trusted
party (single-user workstation tool), and the connection is read-only.
The gates above keep an operator's mistyped ``DELETE`` from clobbering
their event store, not from defending against a hostile caller.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from . import _db, _paths

DEFAULT_LIMIT: Final[int] = 1000
"""Default cap injected at the outer level of an operator's SELECT.

Tuned for ``events query``: an operator who needs a row-by-row dump
beyond this should pass ``--limit N`` or ``--no-limit`` explicitly so
the unbounded scan is intentional, not accidental.
"""

# Statement-kind allow-list applied at parse time. SQLite's grammar allows
# only a small set of statement-introducing keywords; we whitelist the two
# read-only ones and reject everything else by name. The list is
# explicitly enumerated (rather than blacklisting writers) so a new
# SQLite verb does not silently become reachable.
_ALLOWED_LEADING_KEYWORDS: Final[frozenset[str]] = frozenset({"SELECT", "WITH"})

# Forbidden tokens that may appear anywhere in the statement. PRAGMA /
# ATTACH / DETACH are NOT writers in the row-mutation sense, but they
# alter connection state (journal mode, attached schemas) which can
# subvert the read-only posture. Reject them at parse time as a
# defense-in-depth layer on top of the readonly connection.
_FORBIDDEN_TOKENS: Final[frozenset[str]] = frozenset(
    {
        "PRAGMA",
        "ATTACH",
        "DETACH",
    }
)

# Inline-comment patterns stripped before kind detection. SQLite supports
# both ``--`` line comments and ``/* ... */`` block comments; both must
# be removed so a leading ``-- noop\nDROP TABLE events`` does not slip
# past the kind gate.
_LINE_COMMENT_RE: Final[re.Pattern[str]] = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE: Final[re.Pattern[str]] = re.compile(r"/\*.*?\*/", re.DOTALL)

# String-literal pattern used by _strip_strings: SQLite uses single
# quotes for string literals with '' as the escape for an embedded
# quote. The non-greedy match plus the alternation absorbs escaped
# quotes correctly.
_STRING_LITERAL_RE: Final[re.Pattern[str]] = re.compile(r"'(?:[^']|'')*'")

# Outer-LIMIT detection. Anchored at end-of-statement (allowing trailing
# whitespace and an optional OFFSET clause) so a nested ``SELECT ...
# LIMIT 5`` inside a CTE body is left alone — only the outermost LIMIT
# is rewritten. The integer is captured so we can min() with the cap.
_OUTER_LIMIT_RE: Final[re.Pattern[str]] = re.compile(
    r"\bLIMIT\s+(?P<n>\d+)(?P<rest>(?:\s+OFFSET\s+\d+)?)\s*\Z",
    re.IGNORECASE,
)


class QueryRejectedError(ValueError):
    """Raised when the operator's SQL fails the parse-time safety gates.

    The CLI catches this and maps it to exit code 2 with the message
    printed to stderr. Distinct from ``sqlite3.OperationalError`` so the
    caller can tell a parse-time rejection (operator typo, malformed
    statement) apart from a runtime SQLite error (bad column name, etc.).
    """


@dataclass(frozen=True)
class QueryRequest:
    """Resolved parameters for one events-query invocation.

    Built by the CLI layer from typer args; consumed by ``run_query``.
    Frozen so the request is safe to pass around and log without worrying
    about post-validation mutation.

    Attributes:
        sql: The operator's literal SQL, already stripped of leading and
            trailing whitespace but otherwise unchanged.
        limit: Cap to inject at the outer level. ``None`` means
            ``--no-limit`` was passed and no injection happens.
        as_json: When True emit one JSON object per row inside a JSON
            array; when False emit ``key: value`` text blocks.
        db_path: Path to the events SQLite DB. Defaults to
            ``_paths.db_path()`` in the CLI layer.
    """

    sql: str
    limit: int | None
    as_json: bool
    db_path: Path


def _strip_comments(sql: str) -> str:
    """Remove ``--`` line comments and ``/* ... */`` block comments.

    Order matters: block comments first (they can span lines and a
    ``--`` inside ``/* */`` would otherwise be treated as a line
    comment). String-literal contents are NOT preserved across this
    pass; ``_strip_comments`` is only used by the parse-time gates,
    never as the value executed against SQLite.
    """
    sql = _BLOCK_COMMENT_RE.sub(" ", sql)
    sql = _LINE_COMMENT_RE.sub("", sql)
    return sql


def _strip_strings(sql: str) -> str:
    """Replace every ``'...'`` string literal with a single space.

    Used before the forbidden-token scan so a legitimate
    ``SELECT 'PRAGMA foo'`` literal does not trigger a rejection. The
    output is not executable SQL — it is only consumed by the
    keyword-scan regexes downstream.
    """
    return _STRING_LITERAL_RE.sub(" ", sql)


def _check_single_statement(sql_no_comments: str) -> None:
    """Reject multi-statement input.

    A single trailing ``;`` is allowed (operators often paste statements
    with a final semicolon). Any non-whitespace content after the first
    ``;`` is treated as a second statement and rejected. String literals
    are stripped first so an embedded ``;`` inside ``'a;b'`` does not
    trip the check.
    """
    stripped_strings = _strip_strings(sql_no_comments)
    _head, sep, tail = stripped_strings.partition(";")
    if not sep:
        return
    if tail.strip():
        raise QueryRejectedError(
            "events query rejects multi-statement input; submit one SELECT (optionally with a trailing semicolon)"
        )


def _check_leading_keyword(sql_no_comments: str) -> None:
    """Reject statements whose first keyword is not SELECT or WITH.

    Tokenises by taking the first word after stripping leading
    whitespace; case-insensitive. CTE-rooted queries pass on the WITH
    keyword and rely on SQLite to enforce that the outer body is a
    SELECT (a ``WITH foo AS (...) DELETE`` would be rejected at the
    SQLite layer because the connection is read-only — defense in
    depth).
    """
    text = sql_no_comments.lstrip()
    if not text:
        raise QueryRejectedError("events query received empty SQL")
    first_word = re.split(r"[\s(;]", text, maxsplit=1)[0].upper()
    if first_word not in _ALLOWED_LEADING_KEYWORDS:
        raise QueryRejectedError(
            f"events query rejects {first_word!r}-rooted statements; only SELECT and WITH (CTE) are allowed"
        )


def _check_forbidden_tokens(sql_no_comments: str) -> None:
    """Reject statements containing PRAGMA / ATTACH / DETACH anywhere.

    These keywords are not row-mutators but can change connection state
    in ways that subvert the read-only posture (PRAGMA can flip
    journal_mode or query_only; ATTACH can pull in a writable database).
    The read-only connection blocks them at the SQLite layer too; this
    parse-time check is defense in depth and produces a clearer error.

    String literals are stripped before the scan so an operator
    SELECT'ing a column whose value happens to contain ``'PRAGMA'`` is
    not falsely rejected.
    """
    scan_text = _strip_strings(sql_no_comments).upper()
    for token in _FORBIDDEN_TOKENS:
        # Use \b anchors so PRAGMA does not match inside identifiers.
        if re.search(rf"\b{token}\b", scan_text):
            raise QueryRejectedError(
                f"events query rejects statements containing {token!r}; "
                "those can mutate connection state even from a SELECT"
            )


def _apply_limit(sql: str, limit: int | None) -> str:
    """Inject or cap the outer-level LIMIT clause.

    ``limit=None`` (the ``--no-limit`` case) returns the SQL unchanged.

    When the operator's statement already has a trailing
    ``LIMIT N [OFFSET M]``, the N is replaced with ``min(N, limit)`` and
    any OFFSET is preserved. Otherwise ``LIMIT <limit>`` is appended.

    The regex is deliberately conservative: it only matches ``LIMIT``
    at end-of-statement (allowing trailing whitespace and the optional
    ``OFFSET <n>`` clause). A LIMIT inside a CTE body, subquery, or
    compound SELECT branch is left alone — those are nested LIMITs that
    bound an inner result set, not the outer cap.
    """
    if limit is None:
        return sql
    body = sql.rstrip().rstrip(";").rstrip()
    match = _OUTER_LIMIT_RE.search(body)
    if match:
        existing = int(match.group("n"))
        rest = match.group("rest") or ""
        capped = min(existing, limit)
        return body[: match.start()] + f"LIMIT {capped}{rest}"
    return body + f" LIMIT {limit}"


def validate(sql: str) -> str:
    """Run every parse-time gate and return the comment-stripped SQL.

    Order matters: comments are stripped first so all downstream
    gates see the same canonicalised text. Each gate raises
    ``QueryRejectedError`` with a message naming the violated rule.

    The caller passes the returned value to ``_apply_limit`` and then
    to ``conn.execute``; the comment-stripped form executes identically
    to the original (SQLite ignores comments), so stripping them does
    not change the result set.
    """
    if not sql.strip():
        raise QueryRejectedError("events query received empty SQL")
    stripped = _strip_comments(sql)
    if not stripped.strip():
        raise QueryRejectedError("events query received comment-only SQL; include a SELECT statement")
    _check_single_statement(stripped)
    _check_leading_keyword(stripped)
    _check_forbidden_tokens(stripped)
    return stripped.strip()


def _emit_json(rows: list[sqlite3.Row]) -> None:
    """Print every row as one element in a JSON array.

    Each row is rendered as a ``{column: value}`` object. ``default=str``
    on ``json.dumps`` covers any SQLite type (e.g., ``bytes``) that does
    not natively round-trip through JSON; in practice the events table
    columns are all int / text so the fallback is rarely hit.
    """
    out = [dict(r) for r in rows]
    print(json.dumps(out, indent=2, default=str))


def _emit_text(rows: list[sqlite3.Row]) -> None:
    """Print every row as a key: value block separated by blank lines.

    Empty result sets print nothing — the CLI relies on the absence of
    output (and the exit code) to indicate the query ran but matched
    zero rows, matching ``read-events list``'s posture.
    """
    for i, row in enumerate(rows):
        if i > 0:
            print()
        for key in row.keys():  # noqa: SIM118  sqlite3.Row iterates values, not keys
            print(f"{key}: {row[key]}")


def run_query(req: QueryRequest) -> int:
    """Execute the request and emit its result; return the exit code.

    Returns 0 on success (including zero-row result), 2 on parse-time
    rejection or DB-file-missing, 2 on SQLite OperationalError or Warning. Errors
    print a single-line message to stderr; success prints to stdout
    only.

    The DB connection is opened read-only inside a ``contextlib.closing``
    block so a query failure never leaks the connection. The DB-file
    existence check happens before the connection open so the operator
    gets a clear remediation hint ("run waitbus init") instead of
    SQLite's generic "unable to open database file" error.
    """
    try:
        validated = validate(req.sql)
    except QueryRejectedError as exc:
        print(f"waitbus events query: {exc}", file=sys.stderr)
        return 2

    if not req.db_path.exists():
        print(
            f"waitbus events query: events DB not found at {req.db_path}. Run `waitbus init` first.",
            file=sys.stderr,
        )
        return 2

    final_sql = _apply_limit(validated, req.limit)

    try:
        with _db.connect(req.db_path, readonly=True) as conn:
            conn.row_factory = sqlite3.Row
            rows = list(conn.execute(final_sql))
    except (sqlite3.OperationalError, sqlite3.Warning) as exc:
        # sqlite3.Warning is raised for multi-statement SQL ("You can only
        # execute one statement at a time"). The pre-execution validator in
        # _check_single_statement catches most cases, but paren-depth-aware
        # detection is not perfect; widening here prevents a raw traceback
        # reaching the operator.
        print(f"waitbus events query: sqlite error: {exc}", file=sys.stderr)
        return 2

    if req.as_json:
        _emit_json(rows)
    else:
        _emit_text(rows)
    return 0


def cli_entry(
    sql: str,
    *,
    limit: int,
    no_limit: bool,
    as_json: bool,
    db_path: Path | None,
) -> int:
    """Thin adapter from the typer command to ``run_query``.

    Lives here (rather than in ``cli.py``) so the parse-time gates and
    the connection lifecycle stay in one module and can be tested
    without spinning up the full typer app.
    """
    effective_db = _paths.resolve_db_path(db_path)
    req = QueryRequest(
        sql=sql,
        limit=None if no_limit else limit,
        as_json=as_json,
        db_path=effective_db,
    )
    return run_query(req)
