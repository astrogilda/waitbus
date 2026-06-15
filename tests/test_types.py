"""Tests for waitbus._types: EventInsert / Event msgspec.Struct and helpers."""

from __future__ import annotations

import time

import msgspec
import pytest

from waitbus._types import (
    Event,
    EventInsert,
    decode_event,
    decode_event_insert,
    encode_event,
)
from waitbus.sources._registry import known_sources

# ---------------------------------------------------------------------------
# EventInsert construction
# ---------------------------------------------------------------------------


def test_event_insert_minimal_construction() -> None:
    """Required fields only; optional fields default to None."""
    ei = EventInsert(
        delivery_id="d1",
        source="github",
        event_type="workflow_run",
        owner="o",
        repo="r",
        received_at=time.time_ns(),
        payload_json="{}",
        ingest_method="webhook",
    )
    assert ei.delivery_id == "d1"
    assert ei.job_id is None
    assert ei.alert_name is None


def test_event_insert_is_frozen() -> None:
    """EventInsert must be immutable (frozen=True)."""
    ei = EventInsert(
        delivery_id="d1",
        source="github",
        event_type="workflow_run",
        owner="o",
        repo="r",
        received_at=time.time_ns(),
        payload_json="{}",
        ingest_method="webhook",
    )
    with pytest.raises((AttributeError, TypeError)):
        ei.delivery_id = "mutated"  # type: ignore[misc]


def test_event_insert_kw_only() -> None:
    """EventInsert must reject positional arguments (kw_only=True)."""
    with pytest.raises(TypeError):
        EventInsert(
            "d1",
            "github",
            "workflow_run",
            "o",
            "r",  # type: ignore[misc]
            time.time_ns(),
            "{}",
            "webhook",
        )


# ---------------------------------------------------------------------------
# Event construction and round-trip
# ---------------------------------------------------------------------------


def test_event_includes_event_id() -> None:
    """Event has event_id; EventInsert does not."""
    ev = Event(
        event_id="01HZAB0123456789ABCDEFGHJK",
        delivery_id="d1",
        source="github",
        event_type="workflow_run",
        owner="o",
        repo="r",
        received_at=time.time_ns(),
        payload_json="{}",
        ingest_method="webhook",
    )
    assert ev.event_id == "01HZAB0123456789ABCDEFGHJK"
    assert not hasattr(EventInsert, "__struct_fields__") or "event_id" not in EventInsert.__struct_fields__


def test_encode_decode_event_round_trip() -> None:
    """encode_event / decode_event must be lossless."""
    ts = time.time_ns()
    ev = Event(
        event_id="01HZAB0123456789ABCDEFGHJK",
        delivery_id="d-rt",
        source="github",
        event_type="workflow_job",
        owner="owner-x",
        repo="repo-y",
        received_at=ts,
        payload_json='{"key":"val"}',
        ingest_method="webhook",
        job_id=42,
        job_name="build",
        status="completed",
        conclusion="success",
    )
    blob = encode_event(ev)
    assert isinstance(blob, bytes)
    decoded = decode_event(blob)
    assert decoded == ev
    assert decoded.received_at == ts


def test_decode_event_insert_round_trip() -> None:
    """decode_event_insert must reconstruct an EventInsert from JSON bytes."""
    ts = time.time_ns()
    ei = EventInsert(
        delivery_id="d-ei",
        source="alertmanager",
        event_type="prometheus_watchdog",
        owner="prom-owner",
        repo="prom-repo",
        received_at=ts,
        payload_json="{}",
        ingest_method="webhook",
        alert_name="Watchdog",
    )
    blob = msgspec.json.encode(ei)
    decoded = decode_event_insert(blob)
    assert decoded == ei
    assert decoded.alert_name == "Watchdog"


def test_decode_event_raises_on_missing_required_field() -> None:
    """msgspec.ValidationError on incomplete JSON (missing required fields)."""
    incomplete = b'{"delivery_id":"d1","source":"github"}'
    with pytest.raises(msgspec.ValidationError):
        decode_event(incomplete)


def test_decode_event_raises_on_wrong_type() -> None:
    """msgspec.ValidationError when received_at is a string, not int.

    ``source`` is a valid enum member here so the decoder reaches and
    rejects on the ``received_at`` type mismatch (the case under test),
    not earlier on the source field.
    """
    bad_json = (
        b'{"event_id":"01HZAB0123456789ABCDEFGHJK","delivery_id":"d","source":"github",'
        b'"event_type":"workflow_run","owner":"o","repo":"r",'
        b'"received_at":"not-an-int","payload_json":"{}","ingest_method":"webhook"}'
    )
    with pytest.raises(msgspec.ValidationError):
        decode_event(bad_json)


# ---------------------------------------------------------------------------
# Built-in source registry membership
# ---------------------------------------------------------------------------


def test_builtin_sources_membership() -> None:
    """The built-in registry contains exactly the six canonical sources: the five
    external ingest systems plus the in-process ``agent`` emission source."""
    assert set(known_sources().keys()) == {
        "github",
        "alertmanager",
        "pytest",
        "docker",
        "fs",
        "agent",
    }


def test_source_field_is_plain_str() -> None:
    """The source field on EventInsert / Event is a plain str, not an enum."""
    ei = EventInsert(
        delivery_id="d-str",
        source="github",
        event_type="workflow_run",
        owner="o",
        repo="r",
        received_at=time.time_ns(),
        payload_json="{}",
        ingest_method="webhook",
    )
    assert isinstance(ei.source, str)
    assert ei.source == "github"


def test_event_source_encodes_as_bare_string() -> None:
    """msgspec encodes the source field as its bare string value."""
    ei = EventInsert(
        delivery_id="d-enc",
        source="alertmanager",
        event_type="prometheus_alert",
        owner="o",
        repo="r",
        received_at=time.time_ns(),
        payload_json="{}",
        ingest_method="webhook",
    )
    assert b'"source":"alertmanager"' in msgspec.json.encode(ei)


def test_decode_preserves_string_source() -> None:
    """A bare-string source on the wire decodes back as the same string."""
    ts = time.time_ns()
    ev = Event(
        event_id="01HZAB0123456789ABCDEFGHJK",
        delivery_id="d-coerce",
        source="github",
        event_type="workflow_run",
        owner="o",
        repo="r",
        received_at=ts,
        payload_json="{}",
        ingest_method="webhook",
    )
    decoded = decode_event(encode_event(ev))
    assert decoded.source == "github"
    assert isinstance(decoded.source, str)


def test_decode_rejects_unknown_source() -> None:
    """An out-of-taxonomy source fails fast with msgspec.ValidationError."""
    bad = (
        b'{"event_id":"01HZAB0123456789ABCDEFGHJK","delivery_id":"d","source":"slack",'
        b'"event_type":"workflow_run","owner":"o","repo":"r",'
        b'"received_at":1700000000000000000,"payload_json":"{}","ingest_method":"webhook"}'
    )
    with pytest.raises(msgspec.ValidationError):
        decode_event(bad)
