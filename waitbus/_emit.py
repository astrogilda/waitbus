"""Public local-emit ingress seam.

This is the framework-neutral, MCP-independent public surface for
*writing* an event into the waitbus store from any local producer (a
pytest run, a docker-events watcher, an fs watcher, an operator one
shot from the shell). It is the ingress counterpart to the egress
``await_predicate`` primitive: the contract this plan extracts is "any
source -> the bus -> any agent", and this module is the "any source"
edge.

Concurrency safety
------------------
The shipped ``waitbus etag-poll`` ``Type=oneshot`` unit
(``systemd/waitbus-etag-poll.service``) is the live precedent: it is an
external short-lived writer that opens its own connection, does
``insert_event(commit=False)`` -> ``commit`` -> doorbell ring while the
long-lived listener / broadcast / watchdog daemons are running. The
emit path here is structurally identical and strictly weaker (one row,
``commit=True``, the ring folded into ``insert_event``). ``_db.open_conn``
sets ``busy_timeout=5000`` + ``journal_mode=WAL`` +
``synchronous=NORMAL``, so a concurrent daemon writer never produces a
corruption or a lost write — the writer that does not hold the lock
blocks up to 5 s and then commits. No new corruption or race surface is
introduced.

Delivery-delay-not-loss caveat
------------------------------
``insert_event`` rings the broadcast daemon's doorbell after the
commit. The ring is best-effort fire-and-forget. If the broadcast
daemon is mid-pass (or momentarily down) when the ring fires, *this one
event's* live fan-out is delayed by at most one ring cycle: the daemon's
start-time / next-pass ``MAX(event_id)`` sweep picks the row up. The row
itself is durably committed before the ring, so a missed ring is a
bounded **delivery delay, never data loss**. A caller that needs
synchronous delivery confirmation should subscribe and use
``await_predicate`` rather than inferring delivery from ``emit``.

Explicit delivery_id contract
-----------------------------
The caller MUST supply a stable, deterministic ``delivery_id``. It is
the events table PRIMARY KEY and the sole idempotency token: re-emitting
the same ``delivery_id`` is an ``INSERT OR IGNORE`` no-op (reported via
``EmitResult.inserted is False``), not an error and not a duplicate row.
Producers derive it from the natural key of what they observed (e.g.
``f"pytest:{session_id}:{nodeid}"``), exactly as ``etag_poll`` derives
``f"etag:{run_id}:{status}:{conclusion}"``. ``event_id`` is the
internally generated ULID — the caller MUST NOT supply it (the
write-shape :class:`~waitbus._types.EventInsert` has no
``event_id`` field, so this is enforced by the type).
"""

from __future__ import annotations

import math
import sqlite3
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import msgspec

from . import _db, _paths
from ._cloudevents import CloudEvent, to_cloudevent
from ._db import EVENT_COLUMNS
from ._types import NS_PER_SECOND, Event, EventInsert
from .sources._registry import is_known_source, known_sources


class EmitResult(msgspec.Struct, kw_only=True, frozen=True):
    """Outcome of one :func:`emit` call.

    ``inserted`` is ``True`` iff this call actually wrote a new row;
    ``False`` means the ``delivery_id`` already existed and the insert
    was an idempotent no-op (the row in ``event`` is the *pre-existing*
    canonical row, not a phantom). ``event`` is always the read-shape
    row that is now in the store for this ``delivery_id`` (so the
    CloudEvents projection is well-defined whether or not this call was
    the writer).
    """

    inserted: bool
    event: Event


def _read_back(conn: sqlite3.Connection, delivery_id: str) -> Event:
    """Return the canonical stored row for ``delivery_id`` as an Event.

    Called after ``insert_event`` regardless of insert-vs-noop so the
    returned :class:`Event` always carries the real generated
    ``event_id`` (the write-shape ``EventInsert`` does not). For a
    no-op this returns the *pre-existing* row, which is the correct
    idempotent semantics: the caller observes the row that won.
    """
    cols = ", ".join(EVENT_COLUMNS)
    # Connection-scoped mutation: _db.connect() opens a fresh short-lived
    # connection per call (never pooled or reused across emit() boundaries),
    # so setting row_factory here is local to this with-block and cannot
    # leak to another caller.
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        f"SELECT {cols} FROM events WHERE delivery_id = ?",
        (delivery_id,),
    ).fetchone()
    if row is None:  # pragma: no cover - insert_event guarantees the row exists
        raise RuntimeError(
            f"emit: row for delivery_id={delivery_id!r} vanished after "
            "insert_event; concurrent DELETE is not part of the contract"
        )
    # msgspec.convert (not Event(**...)) so the read-back path goes through
    # the ``Event.__post_init__`` validator. Direct ``Event(**row_dict)``
    # construction also triggers ``__post_init__``, but ``msgspec.convert``
    # is the documented coercion seam for sqlite Row -> Struct (handles the
    # row's ``Mapping`` interface uniformly with the JSON-decode path).
    return msgspec.convert({col: row[col] for col in EVENT_COLUMNS}, type=Event)


def emit(event: EventInsert, *, db_path: Path | None = None, doorbell_path: Path | None = None) -> EmitResult:
    """Persist one event and ring the broadcast doorbell. Idempotent.

    Wraps :func:`waitbus._db.insert_event` with ``commit=True``
    (commit-then-ring is handled inside ``insert_event``). Opens its own
    short-lived connection via the canonical pragma set, exactly like
    the ``etag-poll`` oneshot precedent — safe to call concurrently with
    the running daemons.

    Args:
        event: a fully-built write-shape :class:`EventInsert`. The caller
            owns the stable ``delivery_id`` (idempotency key) and MUST
            NOT attempt to supply ``event_id`` (the type has no such
            field; it is generated internally).
        db_path: events DB path; defaults to the platformdirs-resolved
            location (``_paths.db_path()``).
        doorbell_path: explicit doorbell socket path for the wake ring; defaults
            to the env / XDG-resolved location. The socket-path counterpart of
            ``db_path``, so an in-process caller can target a daemon on a
            non-default runtime dir without mutating ``WAITBUS_RUNTIME_DIR``.

    Returns:
        :class:`EmitResult` — ``inserted`` distinguishes a real write
        from an idempotent ``delivery_id`` no-op; ``event`` is the
        canonical stored read-shape row (so CloudEvents projection works
        either way).

    Raises:
        ValueError: if ``received_at`` is not a plausible epoch-ns value
            (``insert_event`` rejects sub-``NS_RECEIVED_AT_MIN`` magnitudes
            so a seconds/ms value cannot be silently persisted).
    """
    target = _paths.resolve_db_path(db_path)
    with _db.connect(target) as conn:
        inserted = _db.insert_event(conn, event, commit=True, doorbell_path=doorbell_path)
        stored = _read_back(conn, event.delivery_id)
    return EmitResult(inserted=inserted, event=stored)


def emit_cloudevent(event: EventInsert, *, db_path: Path | None = None) -> CloudEvent:
    """:func:`emit` then project the stored row into a CloudEvents v1.0 envelope.

    Convenience for producers / boundaries that speak CloudEvents: the
    row is persisted (same idempotency contract as :func:`emit`) and the
    canonical stored :class:`Event` is projected via
    :func:`waitbus._cloudevents.to_cloudevent`. The insert-vs-noop
    distinction is not surfaced here — a CloudEvent is a
    statement about the event's identity, which is stable across re-emits
    of the same ``delivery_id``; callers that need that bit call
    :func:`emit` directly.
    """
    return to_cloudevent(emit(event, db_path=db_path).event)


def emit_batch(events: Iterable[EventInsert], *, db_path: Path | None = None) -> int:
    """Persist many events in one transaction; ring the doorbell once.

    A single connection, one
    ``insert_event(commit=False)`` per event, a single ``conn.commit()``,
    then one best-effort ``_doorbell.ring()`` iff at least one row was
    actually inserted (re-emitted ``delivery_id``s are idempotent no-ops
    and do not re-ring). Structurally the multi-row counterpart of
    :func:`emit` -- same concurrency-safety and explicit-``delivery_id``
    contracts (see the module docstring) -- and the single seam the
    pytest source and the debounced fs watcher share instead of each
    re-deriving the connect/insert-loop/commit/ring sequence.

    Returns the number of rows actually inserted (idempotent
    ``delivery_id`` no-ops count as 0, matching
    :attr:`EmitResult.inserted`).

    **Partial-failure contract.** If iteration over ``events`` raises
    or any per-row ``insert_event`` call raises, the open transaction
    is discarded at context exit (SQLite default behaviour when
    ``conn.commit()`` is not reached) -- no row from this batch is
    committed and the doorbell is *not* rung. The idempotent retry
    shape is to re-emit the whole batch; rows that landed in a
    *previously committed* batch are absorbed as no-ops via
    ``delivery_id`` ``INSERT OR IGNORE``. **Callers' iterables must be
    re-iterable** (the production callers -- ``pytest_emit._Recorder``
    and ``fs_watch._Debouncer`` -- both hold their source data outside
    the generator passed in, so retries rebuild a fresh iterator
    naturally).
    """
    target = _paths.resolve_db_path(db_path)
    inserted = 0
    with _db.connect(target) as conn:
        for event in events:
            if _db.insert_event(conn, event, commit=False):
                inserted += 1
        conn.commit()
    if inserted:
        _db._doorbell.ring()
    return inserted


# ---------------------------------------------------------------------------
# CLI adapter — `waitbus emit`
#
# Kept in this module (not the typer shim) so the input-coercion gates
# and the connection lifecycle stay in one testable place, mirroring how
# `events_query.cli_entry` and `stats.cli_entry` are structured.
# ---------------------------------------------------------------------------

# Seconds-magnitude epoch values are <= this; ns-magnitude values are
# far above it. NS_RECEIVED_AT_MIN (1e15) is the ns floor; 1e12 is a
# comfortable seconds/ms ceiling (year ~33658 in seconds), so anything
# at or below it is unambiguously a *seconds* (or sub-ns) input we must
# scale up to nanoseconds, and anything above it is already ns.
_SECONDS_INPUT_CEILING = 1_000_000_000_000  # < this => treat input as seconds


def _resolve_received_at_ns(raw: str) -> int:
    """Coerce a CLI ``--received-at`` value to epoch nanoseconds.

    Accepts three forms and normalises every one to epoch ns (the single
    internal time base; ``insert_event`` rejects non-ns magnitudes):

    * an RFC3339 / ISO-8601 timestamp (``2026-05-17T12:00:00Z`` or
      ``...z`` -- RFC3339 §5.6 makes the offset designator
      case-insensitive -- or with an explicit numeric offset; a naive
      value is interpreted as UTC) -- parsed via
      :func:`datetime.fromisoformat` and scaled to ns;
    * an integer/float **seconds** epoch (``1763337600`` /
      ``1763337600.5``) -- values at or below ``_SECONDS_INPUT_CEILING``
      are scaled up by 1e9;
    * an integer epoch **nanoseconds** value (already > the seconds
      ceiling) -- passed through unchanged.

    A value that parses as neither a finite number nor an ISO timestamp
    raises ``ValueError`` (the CLI maps that to exit 2). Non-finite
    floats (``inf``/``nan``) are explicitly rejected as ``ValueError``
    rather than tunnelling through ``int()`` as an uncaught
    ``OverflowError`` (which would escape the CLI handler and produce
    exit 1 + a traceback). The final sub-``NS_RECEIVED_AT_MIN``
    rejection is left to ``insert_event`` so there is exactly one floor
    check in the codebase.
    """
    text = raw.strip()
    try:
        numeric = float(text)
    except ValueError:
        iso = text[:-1] + "+00:00" if text[-1:] in ("Z", "z") else text
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp() * NS_PER_SECOND)
    if not math.isfinite(numeric):
        raise ValueError(f"received_at must be finite; got {text!r}")
    if numeric <= _SECONDS_INPUT_CEILING:
        return int(numeric * NS_PER_SECOND)
    return int(numeric)


def _resolve_payload(spec: str) -> str:
    """Resolve a ``--payload-json`` spec to the literal JSON string.

    ``-`` or ``@-`` reads stdin; ``@<path>`` reads that file; anything
    else is taken as the literal JSON text. The value is stored verbatim
    in ``payload_json`` (the store does not re-encode it), so an operator
    can round-trip an exact upstream body.
    """
    if spec in ("-", "@-"):
        return sys.stdin.read()
    if spec.startswith("@"):
        return Path(spec[1:]).read_text(encoding="utf-8")
    return spec


def _parse_source(name: str) -> str:
    """Map a CLI ``--source`` token onto a registered source name.

    Normalises whitespace + case to the canonical lowercase form and
    validates against the source registry (built-in sources plus any
    entry-point-registered plugin sources). Rejects
    anything else with a ``ValueError`` listing the accepted set --
    an unknown source is a producer bug, not something to silently
    persist.
    """
    candidate = name.strip().lower()
    if not candidate:
        raise ValueError("--source must not be empty")
    if not is_known_source(candidate):
        accepted = ", ".join(sorted(known_sources()))
        raise ValueError(f"unknown --source {name!r}; accepted values: {accepted}")
    return candidate


def cli_entry(
    *,
    delivery_id: str,
    source: str,
    event_type: str,
    owner: str,
    repo: str,
    received_at: str,
    payload_json: str,
    ingest_method: str,
    output_format: str,
    db_path: Path | None,
) -> int:
    """Thin adapter from the ``waitbus emit`` typer command to :func:`emit`.

    Returns a process exit code:

    * ``0`` — a new row was inserted (or, for an idempotent
      ``delivery_id`` no-op, the row already existed: both are success;
      the human-readable line states which);
    * ``2`` — an input was malformed (bad ``--received-at`` /
      ``--source`` / ``--payload-json`` path / non-ns magnitude). ``2``
      matches the ``events query`` parse-error convention.

    ``--format json`` prints the :class:`EmitResult` (``inserted`` +
    the stored read-shape row). ``--format cloudevent`` prints the
    CloudEvents v1.0 envelope of the stored row (no ``inserted`` bit —
    a CloudEvent is an identity statement, stable across re-emits;
    documented in :func:`emit_cloudevent`).
    """
    try:
        src = _parse_source(source)
        received_at_ns = _resolve_received_at_ns(received_at)
        payload = _resolve_payload(payload_json)
    except (ValueError, OSError) as exc:
        print(f"emit: invalid input: {exc}", file=sys.stderr)
        return 2

    insert = EventInsert(
        delivery_id=delivery_id,
        source=src,
        event_type=event_type,
        owner=owner,
        repo=repo,
        received_at=received_at_ns,
        payload_json=payload,
        ingest_method=ingest_method,
    )
    try:
        result = emit(insert, db_path=db_path)
    except ValueError as exc:
        # insert_event's epoch-ns floor guard (the single source of truth
        # for the ns magnitude contract) fired.
        print(f"emit: invalid input: {exc}", file=sys.stderr)
        return 2

    if output_format == "cloudevent":
        sys.stdout.write(msgspec.json.encode(to_cloudevent(result.event)).decode())
        sys.stdout.write("\n")
    else:
        sys.stdout.write(msgspec.json.encode(result).decode())
        sys.stdout.write("\n")
    if not result.inserted:
        print(
            f"emit: delivery_id={delivery_id!r} already present — idempotent no-op (existing row reported)",
            file=sys.stderr,
        )
    return 0
