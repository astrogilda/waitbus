"""Shared SQLite helpers for the waitbus event store.

`EVENT_COLUMNS` is the single source of truth for the events table's
column list at INSERT time. Any new column landed in `schema.sql`
needs exactly one matching entry here; both the INSERT statement
(built from this tuple) and the broadcast daemon's row-deserialization
path (which reads the same tuple) inherit the change automatically.

`ensure_schema` is the single source of truth for the events store's
on-disk layout. Both the listener daemon and the broadcast daemon
call it at startup; on a fresh install the broadcast daemon may win
the activation race under systemd socket activation, so it can no
longer assume the listener has already provisioned the table.

`insert_event` does the dedup-safe INSERT OR IGNORE and then rings the
broadcast daemon's doorbell so an awaiting subscriber gets the row
within a single millisecond. The doorbell call is best-effort: if the
daemon isn't running the send silently no-ops, and the daemon catches
up via its `MAX(event_id)` seed cursor on next start.
"""

from __future__ import annotations

import contextlib
import logging
import re
import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

from . import _doorbell as _doorbell
from . import _metrics, _ulid
from ._log import structured
from ._types import EventInsert

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

logger = logging.getLogger("waitbus.db")

# `structured` is the canonical structured-JSON logger entry point shared
# by every daemon module; see waitbus/_log.py.

# Column list mirroring the canonical events-table schema. Order matters:
# the INSERT statement below uses this tuple verbatim for both the
# column-name list and the values list, so extension is one edit.
EVENT_COLUMNS: tuple[str, ...] = (
    "delivery_id",
    "source",
    "event_type",
    "owner",
    "repo",
    "run_id",
    "workflow_name",
    "head_branch",
    "head_sha",
    "status",
    "conclusion",
    "received_at",
    "payload_json",
    "ingest_method",
    "job_id",
    "job_name",
    "parent_run_id",
    "alert_name",
    "alert_severity",
    "alert_fingerprint",
    "msg_to",
    "msg_from",
    "msg_correlation_id",
    "msg_reply_to",
    "msg_thread",
    "msg_body",
    "event_id",
)


_TABLE_CONSTRAINT_KEYWORDS = ("CHECK", "FOREIGN", "PRIMARY", "UNIQUE", "CONSTRAINT")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+events\s*\((?P<body>.*?)\);",
    re.DOTALL | re.IGNORECASE,
)
# SQL `-- ...` line-comment matcher. The rationale and the safety invariant live
# once at the sole authoritative `.sub()` call site in `_expected_event_columns`.
_SQL_LINE_COMMENT_RE = re.compile(r"--[^\n]*")

# Column renames to apply before the additive ADD COLUMN diff. Each tuple
# is (old_name, new_name). The migration is idempotent: it fires only when
# the old column still exists and the new column does not yet exist on the
# events table.
#
# Lifecycle: an entry lands here in the same commit that renames the column
# in schema.sql, stays through one full listener-restart cycle on this
# workstation, and is then retired (deleted from this tuple). Leaving
# completed entries in place is a no-op on every subsequent start but
# inflates the migration surface for future readers, so the registry must
# stay tight.
_PENDING_RENAMES: tuple[tuple[str, str], ...] = ()

# SQLITE_BUSY retry policy for ensure_schema's BEGIN IMMEDIATE. Both the
# listener and broadcast daemons call ensure_schema at startup; under
# systemd socket activation they may race on the write lock. Fifteen
# tries at 200 ms backoff totals ~2.8 s of cumulative sleep budget, which
# absorbs the worst-case migration timing observed on the slowest GitHub-
# hosted runners (cold cache, first-boot disk I/O) while still landing
# well inside any reasonable daemon-startup budget. The contending writer
# typically commits its own migration pass within 200-300 ms; the longer
# tail accommodates the rare runner that takes longer.
_ENSURE_SCHEMA_RETRIES = 15
_ENSURE_SCHEMA_BACKOFF_SEC = 0.2

# SQLITE_BUSY retry policy for the journal_mode=WAL transition inside
# open_conn. The WAL-mode switch needs an exclusive lock; two concurrent
# openers on a fresh DB may collide. Five tries at 50 ms backoff is
# sufficient: the winning opener sets WAL in well under 10 ms, so the
# loser's second attempt always succeeds.
_ENSURE_WAL_RETRIES = 5
_ENSURE_WAL_BACKOFF_SEC = 0.05

# NS_RECEIVED_AT_MIN is the floor for plausible epoch-ns values used by
# insert_event's received_at guard. It corresponds to 2001-09-09 (when the
# Unix epoch first crossed 1e9 seconds, i.e. 1e15 nanoseconds). Any
# received_at below this floor is either negative or a value that has been
# silently down-converted from seconds / milliseconds.
NS_RECEIVED_AT_MIN = 1_000_000_000_000_000  # 2001-09-09 in epoch nanoseconds


def _expected_event_columns(sql_text: str | None = None) -> list[tuple[str, str]]:
    """Return the (column_name, column_decl) tuples declared inside the
    `CREATE TABLE IF NOT EXISTS events (...)` block of schema.sql.

    column_decl includes the SQLite type and any inline constraints
    (NOT NULL, DEFAULT, etc.) but excludes the column name itself, so
    callers can synthesize an idempotent migration via
    `ALTER TABLE events ADD COLUMN <name> <decl>`.

    The parser is intentionally conservative: it splits on commas at the
    top level of the body, drops trailing comments, and rejects lines
    that do not start with a bareword identifier or that begin with a
    table-level constraint keyword. Schema authors must therefore keep
    the events table declaration in the canonical one-column-per-line
    form already in use.

    Args:
        sql_text: pre-read schema.sql contents. When None the file is read
            from SCHEMA_PATH. Pass the already-read text to avoid a second
            disk read during ensure_schema.
    """
    sql = sql_text if sql_text is not None else SCHEMA_PATH.read_text(encoding="utf-8")
    # Strip `-- ...` line comments BEFORE locating the CREATE TABLE body. The
    # body regex stops at the first `);`, so a `);` inside a column comment
    # (e.g. "(addresses, not credentials);") would truncate the parsed column
    # set and silently drop every column after it from the ADD COLUMN migration
    # diff. Comments are re-stripped per-line below; removing them up front just
    # keeps the body-extraction honest.
    #
    # INVARIANT: this strip is NOT string-literal-aware -- it removes any `--` to
    # end-of-line, including one inside a SQL string literal.
    # It is safe only because schema.sql's events DDL contains no `--` inside a
    # string literal; a future literal containing `--` would mis-parse the column
    # set, so keep schema.sql free of `--` within quotes.
    sql = _SQL_LINE_COMMENT_RE.sub("", sql)
    match = _CREATE_TABLE_RE.search(sql)
    if not match:
        msg = "schema.sql does not declare CREATE TABLE IF NOT EXISTS events"
        raise RuntimeError(msg)
    body = match.group("body")
    columns = [parsed for raw_line in body.splitlines() if (parsed := _parse_column_line(raw_line)) is not None]
    if not columns:
        msg = "schema.sql events block parsed to zero columns"
        raise RuntimeError(msg)
    return columns


def _parse_column_line(raw_line: str) -> tuple[str, str] | None:
    """Parse one events-DDL body line into ``(name, decl)``, or ``None`` to skip.

    Returns ``None`` for blank lines, table-level constraint lines, and the
    daemon/SQLite-assigned monotonic PK (the ``AUTOINCREMENT`` ``seq`` column):
    that column cannot be added via ``ALTER TABLE ADD COLUMN`` (SQLite forbids
    adding a PRIMARY KEY / AUTOINCREMENT column) and is never an INSERT column,
    so it is excluded from the additive ensure_schema diff. It only comes into
    existence via the fresh-DB CREATE TABLE or the 0002 table-rebuild migration.
    Raises on a non-skippable line that does not parse to ``<identifier> <decl>``.
    """
    line = raw_line.split("--", 1)[0].strip().rstrip(",")
    if not line:
        return None
    if any(line.upper().startswith(kw) for kw in _TABLE_CONSTRAINT_KEYWORDS):
        return None
    head, _, tail = line.partition(" ")
    head = head.strip()
    decl = tail.strip()
    if not _IDENTIFIER_RE.match(head) or not decl:
        raise RuntimeError(f"unparseable column line: {raw_line!r}")
    if "AUTOINCREMENT" in decl.upper():
        return None
    return (head, decl)


def open_conn(
    db_path: str | Path,
    *,
    readonly: bool = False,
    isolation_level: Literal["DEFERRED", "EXCLUSIVE", "IMMEDIATE"] | None = "DEFERRED",
) -> sqlite3.Connection:
    """Open a SQLite connection with the project's canonical pragmas applied.

    Prefer :func:`connect` (the context manager) over this raw factory in
    new code — ``connect`` closes the connection automatically on scope
    exit. ``open_conn`` is retained as the underlying primitive for the
    rare callers that need fine-grained connection lifetime (currently:
    none in this codebase outside the test suite).

    Sets:
      busy_timeout=5000   — 5s SQLITE_BUSY wait before raising
      journal_mode=WAL    — concurrent reads + serialised writes
      synchronous=NORMAL  — WAL-safe durability, ~5x write throughput vs FULL
      foreign_keys=ON     — enforce constraints
      temp_store=MEMORY   — temp tables and indices live in RAM
      cache_size=-16000   — 16 MiB per-connection page cache
      mmap_size=268435456 — 256 MiB read-only mmap window

    readonly=True opens the database in URI read-only mode
    (file:...?mode=ro) for query-only consumers.

    isolation_level mirrors the sqlite3.connect kwarg; pass None for
    autocommit mode (required when driving explicit transactions with
    BEGIN IMMEDIATE). The default ``"DEFERRED"`` retains Python sqlite3's
    standard deferred-transaction behaviour.
    """
    if readonly:
        conn = sqlite3.connect(
            f"file:{db_path}?mode=ro",
            uri=True,
            isolation_level=isolation_level,
        )
    else:
        conn = sqlite3.connect(str(db_path), isolation_level=isolation_level)
    # Wrap PRAGMA initialisation in a single try/except that closes the
    # connection on ANY error before re-raising. The narrow
    # ``sqlite3.OperationalError`` catch inside the WAL retry loop
    # handled the busy/locked race but let ``sqlite3.DatabaseError``
    # ("file is not a database") and other failure classes escape
    # without closing — every caller hitting a corrupted DB then
    # leaked a connection until GC. This affects production: the
    # watchdog and health-check paths exercise corrupted-DB branches.
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        # journal_mode=WAL is a per-DB persistent setting; changing it
        # requires an exclusive lock so two concurrent openers racing
        # on a fresh DB can collide. The busy_timeout above handles
        # most contention, but the WAL transition itself is not
        # covered by busy_timeout on all SQLite versions (the PRAGMA
        # may return immediately with "locked" rather than sleeping).
        # Retry up to _ENSURE_WAL_RETRIES times with a short sleep to
        # absorb the race; once WAL is set it is a no-op on every
        # subsequent connection.
        for _attempt in range(_ENSURE_WAL_RETRIES):
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as exc:
                if not _is_busy_or_locked(exc) or _attempt == _ENSURE_WAL_RETRIES - 1:
                    raise
                time.sleep(_ENSURE_WAL_BACKOFF_SEC)
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-16000")
        conn.execute("PRAGMA mmap_size=268435456")
    except BaseException:
        conn.close()
        raise
    return conn


@contextlib.contextmanager
def connect(
    db_path: str | Path,
    *,
    readonly: bool = False,
    isolation_level: Literal["DEFERRED", "EXCLUSIVE", "IMMEDIATE"] | None = "DEFERRED",
) -> Iterator[sqlite3.Connection]:
    """Open and auto-close a SQLite connection with the project's canonical pragmas.

    Identical pragma configuration to :func:`open_conn`; closes the
    connection on scope exit (success or exception). Replaces the
    repeated ``with contextlib.closing(_db.open_conn(...)) as conn:``
    boilerplate at every call site.

    Example::

        with _db.connect(db_path(), readonly=True) as conn:
            conn.execute("SELECT 1").fetchone()
    """
    conn = open_conn(db_path, readonly=readonly, isolation_level=isolation_level)
    try:
        yield conn
    finally:
        conn.close()


def iter_events_above(
    conn: sqlite3.Connection,
    since_seq: int,
    *,
    until_seq: int | None = None,
    limit: int,
) -> Iterator[sqlite3.Row]:
    """Yield events whose internal ``seq`` is strictly greater than ``since_seq``.

    Ordering and the resume cursor are the daemon-assigned monotonic ``seq``,
    NOT the per-process ULID
    ``event_id``: ``seq`` is the true single-writer commit order, so replay
    is correct even across producer processes (which the cross-process ULID
    ordering was not). Callers translate a public ULID cursor to its ``seq``
    lower bound via :func:`seq_for_event_id` before calling. Each yielded row
    carries ``seq`` as well as the ``EVENT_COLUMNS`` set so the daemon can
    advance its cursor.

    Optionally bounds the upper end via ``until_seq`` (inclusive). Capped at
    ``limit`` rows. The caller must set ``conn.row_factory = sqlite3.Row``.

    Returns an iterator (rather than a list) so a large replay gap does
    not materialize the entire batch in memory; callers that need length
    can wrap with ``list(...)``. The cursor closes when the generator is
    exhausted or garbage-collected.

    The ``event_id IS NOT NULL`` predicate filters the partial-index
    case where a legacy row landed without a ULID (it cannot form a valid
    wire frame, so it is never replayed or fanned out).
    """
    cols = "seq, " + ", ".join(EVENT_COLUMNS)
    if until_seq is None:
        cursor = conn.execute(
            f"SELECT {cols} FROM events WHERE event_id IS NOT NULL AND seq > ? ORDER BY seq LIMIT ?",
            (since_seq, limit),
        )
    else:
        cursor = conn.execute(
            f"SELECT {cols} FROM events WHERE event_id IS NOT NULL AND seq > ? AND seq <= ? ORDER BY seq LIMIT ?",
            (since_seq, until_seq, limit),
        )
    yield from cursor


def seq_for_event_id(conn: sqlite3.Connection, event_id: str) -> int:
    """Translate a public ULID cursor to its internal ``seq`` lower bound.

    Returns the exact ``seq`` of the row with ``event_id`` (so replay/tail
    resume strictly after the event the consumer last saw, in true commit
    order). Returns ``0`` for an empty cursor OR an ``event_id`` not present
    in the table -- a cursor whose event was pruned (prune removes oldest
    first, so everything retained is newer) or never existed resumes from the
    start of the retained window, which is the safe superset. This exact
    lookup -- not a lexicographic ULID compare -- is what makes cross-process
    request/reply replay sound.
    """
    if not event_id:
        return 0
    row = conn.execute("SELECT seq FROM events WHERE event_id = ?", (event_id,)).fetchone()
    return int(row[0]) if row is not None else 0


def fetch_event_by_id(conn: sqlite3.Connection, event_id: str) -> sqlite3.Row | None:
    """Return the single stored event row for ``event_id`` (or ``None``).

    The re-fetch path for an oversize event whose wire frame was truncated:
    the broadcast wire replaces a payload exceeding ``MAX_FRAME_BYTES`` with a
    stub carrying only ``event_id`` (plus the addressing ``correlation_id`` for
    an agent reply), so a consumer needing the full row reads it back here by
    its ULID. Selects the same ``seq`` + ``EVENT_COLUMNS`` set as
    :func:`iter_events_above`, so the row feeds ``broadcast._row_to_frame``
    unchanged. The caller must set ``conn.row_factory = sqlite3.Row``.
    """
    cols = "seq, " + ", ".join(EVENT_COLUMNS)
    row: sqlite3.Row | None = conn.execute(f"SELECT {cols} FROM events WHERE event_id = ?", (event_id,)).fetchone()
    return row


def split_sql_statements(sql: str) -> list[str]:
    """Split a SQL script into individual statements with comments stripped.

    Strips ``--`` line comments line-by-line, then splits the resulting
    text on ``;``, then discards empty/whitespace-only fragments.

    ``sqlite3.Connection.executescript`` is not used here because it
    always issues an implicit ``COMMIT`` before its
    first statement and does not participate in the project's
    ``BEGIN IMMEDIATE`` retry loop. Splitting up-front lets
    :func:`ensure_schema` and the migrations runner drive statements
    one-at-a-time inside a single explicit transaction, so a
    ``SQLITE_BUSY`` retry replays the whole batch atomically and a
    process kill mid-batch leaves the DB in a consistent state.

    Does NOT handle block comments (``/* ... */``) or SQL string
    literals containing ``;`` — neither is used by schema.sql or any
    shipped migration, so the cheap line-comment-strip is sufficient.
    If a future migration needs richer parsing, replace this function
    with a real SQL tokenizer rather than extending the regex.
    """
    cleaned_lines: list[str] = []
    for line in sql.splitlines():
        idx = line.find("--")
        cleaned_lines.append(line[:idx] if idx != -1 else line)
    cleaned = "\n".join(cleaned_lines)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


def _is_busy_or_locked(exc: sqlite3.OperationalError) -> bool:
    """Return True iff exc is SQLITE_BUSY or SQLITE_LOCKED.

    Replaces fragile substring matches on `str(exc).lower()`. Available on
    Python 3.11+ (the project's minimum supported version) via the
    `sqlite_errorcode` attribute added in PEP 657-era sqlite3 work.
    """
    code = getattr(exc, "sqlite_errorcode", None)
    return code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)


def ensure_schema(db_path: Path) -> None:
    """Initialize or migrate the events store in place.

    On a fresh DB the `CREATE TABLE IF NOT EXISTS` block creates the
    events table with the latest column set declared in schema.sql.
    On a pre-existing DB the IF NOT EXISTS clause is a no-op, so this
    function compares the live `PRAGMA table_info` against schema.sql's
    declared columns and applies `ALTER TABLE events ADD COLUMN` for
    each missing one before executing the rest of schema.sql (which
    creates indexes that may reference the newly-added columns).

    Single source of truth: any column added to schema.sql is migrated
    into existing DBs by the next daemon startup.

    Concurrency model: both the listener daemon and the broadcast
    daemon invoke this function at startup. Under systemd socket
    activation either can win the race; whichever holds the
    `BEGIN IMMEDIATE` lock blocks the other, which returns
    `SQLITE_BUSY`. The retry loop below absorbs that contention
    (5 tries at 100 ms backoff). The retried call sees the post-commit
    table state, so the additive ALTER pass is a no-op the second
    time around.
    """
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    # isolation_level=None puts the connection into autocommit mode so we
    # can drive transactions explicitly. SQLite supports DDL inside an
    # explicit transaction; without manual control, Python's sqlite3
    # module auto-commits before each ALTER, so a process kill between
    # the RENAME and the subsequent ADD COLUMN would leave the events
    # table half-migrated and the next startup would silently skip the
    # rename half (idempotency guard fires on already-renamed column).
    with connect(db_path, isolation_level=None) as conn:
        # Reset busy_timeout to zero for this connection: ensure_schema drives
        # its own Python-level retry loop (_run_migration_in_transaction) with
        # 100 ms sleeps between attempts. Keeping the 5-second busy_timeout
        # from open_conn would make each BEGIN IMMEDIATE attempt block for up
        # to 5 s before raising SQLITE_BUSY, pushing worst-case startup to
        # ~25 s across five retries. Zero means "raise immediately on lock" so
        # the Python loop controls the entire wait budget.
        conn.execute("PRAGMA busy_timeout=0")
        rows = conn.execute("PRAGMA table_info(events)").fetchall()
        existing = {row[1] for row in rows}
        # Parse schema_sql into individual DDL statements once; reused in both
        # Cannot split naively on ';' because schema.sql comments may
        # contain semicolons. ``split_sql_statements`` handles the
        # comment-stripping and split-and-discard in one pass (same
        # helper used by the migrations runner, so behavior matches).
        schema_stmts = split_sql_statements(schema_sql)
        if existing:
            _run_migration_in_transaction(conn, existing, schema_stmts, schema_sql)
        else:
            # Fresh DB: no migration needed. Drive statements individually
            # inside BEGIN IMMEDIATE so two concurrent openers (listener and
            # broadcast under socket activation) absorb SQLITE_BUSY via the
            # retry wrapper. See ``split_sql_statements`` for why
            # ``executescript`` is unsuitable here.
            _run_schema_in_transaction(conn, schema_stmts)


def _run_schema_in_transaction(
    conn: sqlite3.Connection,
    schema_stmts: list[str],
) -> None:
    """Apply the initial schema DDL inside a BEGIN IMMEDIATE transaction.

    Used by the fresh-DB path of ensure_schema. Two concurrent openers
    (listener and broadcast under systemd socket activation) may race
    here; the SQLITE_BUSY retry loop absorbs the contention, and the
    second caller's `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT
    EXISTS` statements become no-ops after the first caller commits.
    """
    for attempt in range(_ENSURE_SCHEMA_RETRIES):
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for stmt in schema_stmts:
                    conn.execute(stmt)
                conn.execute("COMMIT")
                return
            except Exception:
                conn.execute("ROLLBACK")
                raise
        except sqlite3.OperationalError as exc:
            if not _is_busy_or_locked(exc) or attempt == _ENSURE_SCHEMA_RETRIES - 1:
                raise
            time.sleep(_ENSURE_SCHEMA_BACKOFF_SEC)


def _run_migration_in_transaction(
    conn: sqlite3.Connection,
    existing: set[str],
    schema_stmts: list[str],
    schema_sql: str,
) -> None:
    """Apply the additive ALTER pass and DDL replay inside one transaction.

    Wraps the `BEGIN IMMEDIATE` body of ensure_schema so the SQLITE_BUSY
    retry loop can re-issue the whole transaction on contention without
    duplicating the body. The retried call sees the post-commit table
    state, so the second pass's `existing` set is refreshed by the
    caller and the ALTER ADD COLUMN pass becomes a no-op naturally.
    """
    for attempt in range(_ENSURE_SCHEMA_RETRIES):
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                # Apply pending column renames before the additive ADD COLUMN
                # diff so the new name is visible when _expected_event_columns
                # is consulted. Each rename is idempotent: it fires only when
                # the old column still exists and the new column does not.
                for old_name, new_name in _PENDING_RENAMES:
                    if old_name in existing and new_name not in existing:
                        conn.execute(f"ALTER TABLE events RENAME COLUMN {old_name} TO {new_name}")
                        existing.discard(old_name)
                        existing.add(new_name)
                    elif old_name in existing and new_name in existing:
                        structured(
                            logger,
                            logging.WARNING,
                            "rename_both_cols_exist",
                            old_col=old_name,
                            new_col=new_name,
                            hint=(
                                f"Both '{old_name}' and '{new_name}' exist on the events "
                                "table. Skipping rename. Operator must DROP the orphaned "
                                f"column '{old_name}' manually before retiring this entry."
                            ),
                        )
                for name, decl in _expected_event_columns(sql_text=schema_sql):
                    if name not in existing:
                        conn.execute(f"ALTER TABLE events ADD COLUMN {name} {decl}")
                # Execute each schema DDL statement inside the same
                # transaction so a process kill never leaves indexes
                # missing after a successful column rename/add. See
                # ``split_sql_statements`` for why ``executescript`` is
                # unsuitable here.
                for stmt in schema_stmts:
                    conn.execute(stmt)
                conn.execute("COMMIT")
                return
            except Exception:
                conn.execute("ROLLBACK")
                raise
        except sqlite3.OperationalError as exc:
            # SQLITE_BUSY/SQLITE_LOCKED surfaces on the BEGIN IMMEDIATE call
            # when the contending writer holds the write lock; any other
            # error code is a real failure and must propagate.
            if not _is_busy_or_locked(exc):
                raise
            if attempt == _ENSURE_SCHEMA_RETRIES - 1:
                raise
            time.sleep(_ENSURE_SCHEMA_BACKOFF_SEC)
            # Refresh the existing-columns snapshot: the contending writer
            # may have completed its own ALTER pass while we slept.
            rows = conn.execute("PRAGMA table_info(events)").fetchall()
            existing = {row[1] for row in rows}


def insert_event(
    conn: sqlite3.Connection,
    event: EventInsert,
    *,
    commit: bool = True,
    doorbell_path: Path | None = None,
) -> bool:
    """Insert one event row; return True iff it was actually inserted.

    Uses INSERT OR IGNORE ... RETURNING delivery_id — RETURNING yields
    exactly one row on a successful insert and zero rows when the
    UNIQUE(delivery_id) constraint causes the IGNORE branch. This is
    more reliable than reading rowcount or running a separate
    SELECT changes() after the fact.

    Asserts that event.received_at is in epoch nanoseconds: values below
    1_000_000_000_000_000 (ns equivalent of 2001-09-09) are rejected with
    ValueError so callers that accidentally pass seconds or milliseconds
    are caught at the boundary.

    When commit=True (the default), the function commits the connection
    and rings the broadcast daemon's doorbell on a successful insert.
    The commit must precede the doorbell ring so the broadcast pass that
    follows the doorbell ping actually sees the new row; without the
    explicit commit, the caller's implicit transaction would still be
    open when the daemon's SELECT fires. When commit=False, neither
    commit nor ring happens — the caller owns the transaction boundary
    and is responsible for emitting one doorbell ring after the outer
    COMMIT closes the batch.

    The doorbell call is fire-and-forget; missed deliveries are recovered
    via the daemon's `MAX(event_id)` seed cursor on its next start.

    Args:
        conn: open SQLite connection (caller owns lifetime and transaction).
        event: typed EventInsert value built by the listener / poller.
        commit: when True (default), commit and ring doorbell on insert.
            Pass False when batching multiple inserts in one transaction;
            the caller commits and rings once after the loop.
        doorbell_path: explicit doorbell socket path for the ring; defaults to
            the env / XDG-resolved location. Lets an in-process caller target a
            daemon bound to a non-default runtime dir without an env override.
    """
    if event.received_at <= 0:
        raise ValueError(
            f"received_at must be a positive epoch-nanosecond value; got {event.received_at} (zero or negative)"
        )
    if event.received_at < NS_RECEIVED_AT_MIN:
        raise ValueError(
            f"received_at must be epoch nanoseconds; got {event.received_at} "
            "which appears to be seconds or milliseconds magnitude"
        )
    event_id = _ulid.new()
    # Build the values tuple in EVENT_COLUMNS order. event_id is generated
    # here at insert time; all other columns come directly from the EventInsert.
    col_to_value: dict[str, Any] = {
        "delivery_id": event.delivery_id,
        "source": event.source,
        "event_type": event.event_type,
        "owner": event.owner,
        "repo": event.repo,
        "run_id": event.run_id,
        "workflow_name": event.workflow_name,
        "head_branch": event.head_branch,
        "head_sha": event.head_sha,
        "status": event.status,
        "conclusion": event.conclusion,
        "received_at": event.received_at,
        "payload_json": event.payload_json,
        "ingest_method": event.ingest_method,
        "job_id": event.job_id,
        "job_name": event.job_name,
        "parent_run_id": event.parent_run_id,
        "alert_name": event.alert_name,
        "alert_severity": event.alert_severity,
        "alert_fingerprint": event.alert_fingerprint,
        "msg_to": event.msg_to,
        "msg_from": event.msg_from,
        "msg_correlation_id": event.msg_correlation_id,
        "msg_reply_to": event.msg_reply_to,
        "msg_thread": event.msg_thread,
        "msg_body": event.msg_body,
        "event_id": event_id,
    }
    values = tuple(col_to_value[col] for col in EVENT_COLUMNS)
    column_list = ", ".join(EVENT_COLUMNS)
    placeholders = ", ".join(["?"] * len(EVENT_COLUMNS))
    cur = conn.execute(
        f"INSERT OR IGNORE INTO events ({column_list}) VALUES ({placeholders}) RETURNING delivery_id",
        values,
    )
    inserted = cur.fetchone() is not None
    if inserted:
        _metrics.incr(
            "waitbus_db_inserted_total",
            event_type=event.event_type,
            source=event.source,
            ingest_method=event.ingest_method,
        )
    else:
        _metrics.incr(
            "waitbus_db_dedup_ignored_total",
            event_type=event.event_type,
            source=event.source,
            ingest_method=event.ingest_method,
        )
    if commit:
        # Commit before signalling the daemon so the broadcast pass that
        # follows the doorbell ping actually sees the new row. Without this,
        # the caller's `with sqlite3.connect(...) as conn` block defers the
        # commit until block exit — after `_doorbell.ring()` has already
        # fired, racing the daemon's SELECT against an as-yet-uncommitted
        # transaction.
        conn.commit()
        if inserted:
            _doorbell.ring(doorbell_path)
    return inserted
