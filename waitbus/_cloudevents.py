"""CloudEvents v1.0 envelope projector for waitbus events.

This module projects an internal :class:`~waitbus._types.Event`
(the read-shape row, including its generated ULID ``event_id``) into a
CloudEvents v1.0-conformant envelope. It is the structural boundary
type for the public emit API and the MCP event projection; those
call sites are wired in later build-sequence steps. This module
deliberately ships only the envelope struct, the projection function,
and its tests.

Conformance is verified field-by-field against the cloned spec at
``git-clones/cloudevents-spec/cloudevents/spec.md`` (REQUIRED attrs
spec.md:286-365, OPTIONAL attrs from spec.md:372+):

REQUIRED attributes
-------------------
* ``id`` — the ULID ``event_id``. Non-empty, unique within the
  producer (a monotonic ULID), satisfying the spec's "MUST be unique
  within the scope of the producer" constraint. ``source`` + ``id``
  uniqueness holds because ``event_id`` alone is already globally
  unique.
* ``source`` — a non-empty URI-reference (spec.md:301-333). See the
  ``_SOURCE_URI_PREFIX`` docstring for the chosen scheme and the
  alternatives weighed.
* ``specversion`` — the fixed string ``"1.0"`` (spec.md:335-351;
  compliant producers MUST emit ``1.0``).
* ``type`` — the waitbus ``event_type`` (``workflow_run``,
  ``workflow_job``, ``prometheus_alert``, ``prometheus_watchdog``).
  The spec only constrains this to a non-empty string and *SHOULDs*
  a reverse-DNS prefix (spec.md:353-371); waitbus's event_type values
  are the established stable contract surface (CONSUMER_API.md,
  schema.sql indexes filter on event_type) so they are projected
  verbatim rather than rewritten into a reverse-DNS form that would
  fork the taxonomy and break the documented contract. "SHOULD" is
  advisory; verbatim projection is the greenfield-correct choice
  here because event_type is already the single source of truth.

OPTIONAL attributes projected
-----------------------------
* ``time`` — RFC3339 (spec.md:418-432). waitbus stores ``received_at``
  as epoch *nanoseconds* (single time base with the ULID clock).
  RFC3339 / Python ``datetime`` resolves to microseconds, so the
  sub-microsecond tail is truncated; this is lossless for ordering
  (the ULID ``id`` carries full-resolution ordering) and is the
  standard-conformant representation. Always UTC, suffixed ``Z``.
* ``datacontenttype`` — ``"application/json"`` (spec.md:378-416);
  ``data`` is a JSON object.
* ``data`` — the projected event fields as a JSON-serialisable
  mapping (spec.md, "Event Data"). waitbus's full row minus the
  envelope-promoted attributes, so no information is lost in the
  projection while the envelope stays a clean CloudEvents shape.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Final

import msgspec

from ._types import NS_PER_SECOND, Event

# Chosen ``source`` URI scheme: ``urn:waitbus:source:<enum>``
#
# The spec requires a non-empty URI-reference and RECOMMENDS an
# absolute URI (spec.md:320-323); it explicitly lists
# "Universally-unique URN" and "Application-specific identifiers" as
# acceptable schemes (spec.md:328-333). Alternatives weighed:
#
#   1. ``https://github.com/<owner>/<repo>`` — couples the CloudEvents
#      identity to the GitHub origin, which is wrong: the ``source``
#      attribute identifies *the producing context* (the waitbus ingest
#      system), not the upstream subject. The upstream repo is data,
#      not producer identity. Also non-uniform across non-GitHub
#      sources (alertmanager/pytest/docker/fs have no GitHub URL).
#   2. A relative path like ``/waitbus/<enum>`` — a valid URI-reference
#      but not an absolute URI, so it loses the RECOMMENDED absolute
#      form and is ambiguous when the envelope travels off-host.
#   3. ``urn:waitbus:source:<name>`` — an absolute, application-specific
#      URN. It is uniform across every ingest system, stable, opaque,
#      carries no host/transport coupling, and the suffix is the
#      canonical source name from the registry (built-ins plus, in a
#      later commit, entry-point-registered plugin sources). This
#      matches schema.sql:3's stated intent that ``source`` is an
#      extensible producer-defined taxonomy.
#
# Scheme #3 is selected: one stable absolute URN per ingest system.
_SOURCE_URI_PREFIX: Final[str] = "urn:waitbus:source:"

_CLOUDEVENTS_SPECVERSION: Final[str] = "1.0"
_CLOUDEVENTS_DATACONTENTTYPE: Final[str] = "application/json"

# Envelope-promoted Event fields: these become top-level CloudEvents
# attributes and are therefore excluded from the ``data`` payload to
# avoid redundant duplication. Everything else on the Event projects
# into ``data`` so the projection is lossless.
_PROMOTED_FIELDS: Final[frozenset[str]] = frozenset({"event_id", "source", "event_type", "received_at"})


def source_uri(source: str) -> str:
    """Return the CloudEvents ``source`` URI-reference for an ingest system.

    ``"github"`` -> ``"urn:waitbus:source:github"``. See the
    ``_SOURCE_URI_PREFIX`` rationale for why a URN (not a GitHub URL or
    a relative path) is the conformant, uniform choice. Validation of
    ``source`` happened upstream in ``Event.__post_init__``; this is a
    pure formatting seam that trusts its caller.
    """
    return f"{_SOURCE_URI_PREFIX}{source}"


def rfc3339_from_epoch_ns(received_at_ns: int) -> str:
    """Convert an epoch-nanosecond timestamp to an RFC3339 UTC string.

    waitbus stores ``received_at`` in nanoseconds (single time base with
    the ULID clock). RFC3339 / ``datetime`` resolves to microseconds,
    so the sub-microsecond tail is truncated — lossless for ordering
    (the ULID ``id`` carries full-resolution ordering). Always UTC,
    rendered with a trailing ``Z`` (the RFC3339 zero-offset form) rather
    than ``+00:00``.
    """
    dt = datetime.fromtimestamp(received_at_ns / NS_PER_SECOND, tz=UTC)
    return dt.isoformat().replace("+00:00", "Z")


class CloudEvent(msgspec.Struct, kw_only=True, frozen=True):
    """A CloudEvents v1.0-conformant envelope (structured JSON mode).

    Field order mirrors spec.md's REQUIRED-then-OPTIONAL ordering for
    readability. ``data`` is a JSON object (``datacontenttype`` is
    fixed to ``application/json``). All attribute names match the
    CloudEvents wire names exactly, so ``msgspec.json.encode`` of this
    struct is a conformant structured-mode CloudEvent.
    """

    id: str
    source: str
    specversion: str
    type: str
    time: str
    datacontenttype: str
    data: dict[str, Any]


def to_cloudevent(event: Event) -> CloudEvent:
    """Project an internal :class:`Event` into a CloudEvents v1.0 envelope.

    The four envelope-promoted fields (``event_id``, ``source``,
    ``event_type``, ``received_at``) become the CloudEvents ``id`` /
    ``source`` / ``type`` / ``time`` attributes; every remaining Event
    field projects into ``data`` so the projection is information-
    preserving (an Event can be reconstructed from the envelope).
    """
    full = msgspec.to_builtins(event)
    data = {k: v for k, v in full.items() if k not in _PROMOTED_FIELDS}
    return CloudEvent(
        id=event.event_id,
        source=source_uri(event.source),
        specversion=_CLOUDEVENTS_SPECVERSION,
        type=event.event_type,
        time=rfc3339_from_epoch_ns(event.received_at),
        datacontenttype=_CLOUDEVENTS_DATACONTENTTYPE,
        data=data,
    )
