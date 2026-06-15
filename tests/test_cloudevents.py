"""Tests for waitbus._cloudevents: CloudEvents v1.0 projection.

Conformance is checked against the CloudEvents spec §286-365 (REQUIRED
attributes) and the OPTIONAL ``time`` / ``datacontenttype`` / ``data``
attributes.
"""

from __future__ import annotations

import json
import re

import msgspec
import pytest

from waitbus._cloudevents import (
    CloudEvent,
    rfc3339_from_epoch_ns,
    source_uri,
    to_cloudevent,
)
from waitbus._types import Event

# RFC3339 (a profile of ISO8601) with UTC zero-offset rendered as `Z`.
_RFC3339_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def _make_event(**overrides: object) -> Event:
    base: dict[str, object] = dict(
        event_id="01HZAB0123456789ABCDEFGHJK",
        delivery_id="d-1",
        source="github",
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at=1_700_000_000_123_456_789,
        payload_json='{"k":"v"}',
        ingest_method="webhook",
        run_id=42,
        conclusion="success",
    )
    base.update(overrides)
    return Event(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# REQUIRED attributes
# ---------------------------------------------------------------------------


def test_required_attributes_present_and_correct() -> None:
    """id / source / specversion / type per spec.md:286-365."""
    ce = to_cloudevent(_make_event())
    assert ce.id == "01HZAB0123456789ABCDEFGHJK"  # ULID event_id
    assert ce.source == "urn:waitbus:source:github"  # URI-reference
    assert ce.specversion == "1.0"  # MUST be "1.0"
    assert ce.type == "workflow_run"  # waitbus event_type verbatim


def test_id_is_non_empty_string() -> None:
    """spec.md:295 — id MUST be a non-empty string."""
    ce = to_cloudevent(_make_event())
    assert isinstance(ce.id, str) and ce.id


@pytest.mark.parametrize(
    ("src", "expected"),
    [
        ("github", "urn:waitbus:source:github"),
        ("alertmanager", "urn:waitbus:source:alertmanager"),
        ("pytest", "urn:waitbus:source:pytest"),
        ("docker", "urn:waitbus:source:docker"),
        ("fs", "urn:waitbus:source:fs"),
    ],
)
def test_source_uri_scheme(src: str, expected: str) -> None:
    """source is the chosen absolute URN, uniform across ingest systems."""
    assert source_uri(src) == expected
    assert to_cloudevent(_make_event(source=src)).source == expected


def test_source_is_non_empty_uri_reference() -> None:
    """spec.md:322-323 — non-empty URI-reference; absolute URI RECOMMENDED.

    A ``urn:`` URI is absolute and a valid URI-reference.
    """
    ce = to_cloudevent(_make_event())
    assert ce.source
    assert ce.source.startswith("urn:")
    assert ":" in ce.source  # has a scheme -> absolute URI


# ---------------------------------------------------------------------------
# OPTIONAL attributes: time / datacontenttype / data
# ---------------------------------------------------------------------------


def test_time_is_rfc3339_utc_z() -> None:
    """time is RFC3339, UTC, rendered with a trailing Z (not +00:00)."""
    ce = to_cloudevent(_make_event())
    assert _RFC3339_Z.match(ce.time), ce.time
    assert "+00:00" not in ce.time


def test_time_value_matches_epoch_ns() -> None:
    """The RFC3339 time decodes back to the stored epoch second."""
    # 1_700_000_000.123456789 s -> microsecond-truncated RFC3339.
    assert rfc3339_from_epoch_ns(1_700_000_000_123_456_789) == ("2023-11-14T22:13:20.123457Z")
    # Whole-second timestamp has no fractional part but stays RFC3339.
    assert _RFC3339_Z.match(rfc3339_from_epoch_ns(1_700_000_000_000_000_000))


def test_datacontenttype_is_application_json() -> None:
    ce = to_cloudevent(_make_event())
    assert ce.datacontenttype == "application/json"


def test_data_excludes_promoted_attributes() -> None:
    """Envelope-promoted fields are not duplicated inside data."""
    ce = to_cloudevent(_make_event())
    for promoted in ("event_id", "source", "event_type", "received_at"):
        assert promoted not in ce.data


def test_data_carries_remaining_event_fields() -> None:
    """Projection is information-preserving for non-promoted fields."""
    ce = to_cloudevent(_make_event())
    assert ce.data["delivery_id"] == "d-1"
    assert ce.data["owner"] == "acme"
    assert ce.data["repo"] == "widgets"
    assert ce.data["run_id"] == 42
    assert ce.data["conclusion"] == "success"


# ---------------------------------------------------------------------------
# Round-trip / wire conformance
# ---------------------------------------------------------------------------


def test_envelope_encodes_to_conformant_json() -> None:
    """A structured-mode CloudEvent: all wire attribute names present."""
    ce = to_cloudevent(_make_event())
    obj = json.loads(msgspec.json.encode(ce))
    assert set(obj) == {
        "id",
        "source",
        "specversion",
        "type",
        "time",
        "datacontenttype",
        "data",
    }
    assert obj["specversion"] == "1.0"
    assert isinstance(obj["data"], dict)


def test_round_trip_reconstructs_event() -> None:
    """The Event is recoverable from the envelope (lossless projection)."""
    ev = _make_event()
    ce = to_cloudevent(ev)
    reconstructed = Event(
        event_id=ce.id,
        source=ce.source.removeprefix("urn:waitbus:source:"),
        event_type=ce.type,
        received_at=ev.received_at,  # full-precision lives in the ULID id
        **{k: v for k, v in ce.data.items()},
    )
    assert reconstructed == ev


def test_cloudevent_struct_is_frozen() -> None:
    ce = to_cloudevent(_make_event())
    assert isinstance(ce, CloudEvent)
    with pytest.raises((AttributeError, TypeError)):
        ce.id = "mutated"  # type: ignore[misc]
