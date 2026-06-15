"""Typed event-row representations using msgspec.Struct.

msgspec is chosen over TypedDict (no runtime validation), pydantic
(transitive Rust dependency + ~5 MB closure), and attrs/dataclasses
(no JSON I/O) because the listener's hot path benefits from a
zero-overhead-decode primitive that doubles as the on-wire encoder.
A typed Struct eliminates ~50 LOC of defensive isinstance chains
in listener._event_from_webhook_payload by pushing the type
discipline to msgspec.json.decode, and the same Struct serialises 5-10x faster
than json.dumps on the broadcast emit path.

received_at is an int in EPOCH NANOSECONDS — single time base with
the ULID source clock (time.monotonic_ns()). The unit is documented
here, in schema.sql, and via a runtime assertion in _db.insert_event
so callers passing ms-magnitude values fail fast.

The ``source`` field is a bare ``str`` validated by ``__post_init__``
against the registry returned by
:func:`waitbus.sources._registry.is_known_source`. msgspec
calls ``__post_init__`` automatically at decode + convert time (per
``msgspec/docs/structs.rst:164-171``), so the validator runs on every
ingest path: direct construction, ``msgspec.json.decode``, and
``msgspec.convert``. The canonical source taxonomy (``"github"``,
``"alertmanager"``, ``"pytest"``, ``"docker"``, ``"fs"``) lives in
the registry as the single source of truth. Plugin-registered
sources are accepted by the same validator once they register via
the ``waitbus.sources.v1`` entry-point group.
"""

from __future__ import annotations

from typing import Final

import msgspec

from .sources._registry import is_known_source, known_sources

# Single source of truth for the nanosecond<->second conversion factor.
# received_at is epoch nanoseconds (see the module docstring); every
# ns->s or s->ns scale in the codebase imports this rather than
# repeating the 1e9 literal.
NS_PER_SECOND: Final[int] = 1_000_000_000


def _validate_source(source: str) -> None:
    """Raise ``ValueError`` if ``source`` is not a registered source name.

    Called from ``EventInsert.__post_init__`` and ``Event.__post_init__``.
    The validator consults the source registry (built-in sources plus
    any ``waitbus.sources.v1`` entry-point-registered plugin sources
    populated by ``discover_plugins_once``) and raises with the sorted
    known-source list so the operator can spot typos at a glance.
    """
    if not is_known_source(source):
        known = sorted(known_sources())
        raise ValueError(f"unknown source {source!r}; known: {known}")


class EventInsert(msgspec.Struct, kw_only=True, frozen=True):
    """Write-shape: the 20 columns the listener / poller populates.

    event_id is NOT included — it is generated at insert time by _db
    when the row is committed. Use Event (the read-shape) for query
    results which include event_id.

    All Optional[...] fields default to None so callers can build
    minimal EventInsert values for sparse rows (e.g., a workflow_run
    event has no job_id / job_name / parent_run_id).
    """

    delivery_id: str
    source: str
    event_type: str
    owner: str
    repo: str
    received_at: int  # epoch nanoseconds
    payload_json: str
    ingest_method: str
    # Optional fields, listed in schema order for readability.
    run_id: int | None = None
    workflow_name: str | None = None
    head_branch: str | None = None
    head_sha: str | None = None
    status: str | None = None
    conclusion: str | None = None
    job_id: int | None = None
    job_name: str | None = None
    parent_run_id: int | None = None
    alert_name: str | None = None
    alert_severity: str | None = None
    alert_fingerprint: str | None = None
    # Agent-message addressing facet (see schema.sql). Self-asserted agent
    # names (addresses, not credentials, under the same-UID trust model);
    # project into the wire
    # `fields` so a recipient/correlation filter is predicate-matchable.
    msg_to: str | None = None
    msg_from: str | None = None
    msg_correlation_id: str | None = None
    msg_reply_to: str | None = None
    msg_thread: str | None = None
    msg_body: str | None = None

    def __post_init__(self) -> None:
        """Validate ``source`` names a registered source.

        msgspec invokes ``__post_init__`` on direct construction and
        on decode / convert paths (``msgspec.json.decode``,
        ``msgspec.convert``), so this single validator covers every
        ingest seam without needing per-callsite checks.
        """
        _validate_source(self.source)


class Event(msgspec.Struct, kw_only=True, frozen=True):
    """Read-shape: an inserted row including the generated event_id.

    Inherits the write-shape but gains event_id (the ULID assigned at
    insert time). Used by broadcast for serialisation onto the wire
    and by subscribers (read_events, pr_monitor, mcp) for typed access.

    received_at is epoch NANOSECONDS — single time base with the ULID
    source clock (time.monotonic_ns()).
    """

    event_id: str
    delivery_id: str
    source: str
    event_type: str
    owner: str
    repo: str
    received_at: int  # epoch nanoseconds
    payload_json: str
    ingest_method: str
    run_id: int | None = None
    workflow_name: str | None = None
    head_branch: str | None = None
    head_sha: str | None = None
    status: str | None = None
    conclusion: str | None = None
    job_id: int | None = None
    job_name: str | None = None
    parent_run_id: int | None = None
    alert_name: str | None = None
    alert_severity: str | None = None
    alert_fingerprint: str | None = None
    # Agent-message addressing facet — mirrors EventInsert (see schema.sql).
    msg_to: str | None = None
    msg_from: str | None = None
    msg_correlation_id: str | None = None
    msg_reply_to: str | None = None
    msg_thread: str | None = None
    msg_body: str | None = None

    def __post_init__(self) -> None:
        """Validate ``source`` names a registered source.

        See :meth:`EventInsert.__post_init__` for the rationale and
        the msgspec decode-path coverage.
        """
        _validate_source(self.source)


# Pre-built msgspec encoders/decoders. Reusing these is significantly
# faster than constructing per-call (msgspec amortises type-graph
# inspection on the first call).

_event_encoder: msgspec.json.Encoder = msgspec.json.Encoder()
_event_decoder_event: msgspec.json.Decoder[Event] = msgspec.json.Decoder(Event)
_event_decoder_event_insert: msgspec.json.Decoder[EventInsert] = msgspec.json.Decoder(EventInsert)


def encode_event(event: Event) -> bytes:
    """Serialise an Event to compact JSON bytes (broadcast wire format)."""
    return _event_encoder.encode(event)


def decode_event(blob: bytes | str) -> Event:
    """Parse a JSON blob into an Event; raises msgspec.ValidationError
    on missing required fields or type mismatch. Source validity is
    enforced by ``Event.__post_init__`` and surfaces as a
    ``msgspec.ValidationError`` via msgspec's standard translation."""
    return _event_decoder_event.decode(blob)


def decode_event_insert(blob: bytes | str) -> EventInsert:
    """Parse a JSON blob into an EventInsert; raises msgspec.ValidationError
    on missing required fields or type mismatch. Source validity is
    enforced by ``EventInsert.__post_init__``."""
    return _event_decoder_event_insert.decode(blob)
