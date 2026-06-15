"""Property-based tests for length-prefix encode/decode in waitbus._frame.

Covers round-trip fidelity, oversize rejection, truncated_frame size bound,
and truncated-prefix rejection via Hypothesis.  Also covers Struct round-trip
properties for each of the five typed wire frames, an oversize-Struct fuzz,
a kind-discriminator invariant, and four reflective Struct-shape invariants.
"""

from __future__ import annotations

import importlib
import inspect
import json
import struct
from typing import Any

import msgspec
import msgspec.structs
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

# These tests import the private _KIND_* constants: asserting a
# round-tripped frame's kind against the constant pins the wire discriminator
# as a freeze invariant (a drift in EITHER the Struct default OR the constant
# fails the test). This is the code-vs-constant axis; the code-vs-doc axis
# (kind ↔ CONSUMER_API.md §2a heading) lives in test_frame_catalogue_consistency.
# The two provide non-redundant coverage.
from waitbus._frame import (
    _KIND_EVENT,
    _KIND_HEARTBEAT,
    _KIND_SUBSCRIBE_ACK,
    _KIND_SUBSCRIBE_REJECTED,
    _KIND_TRUNCATED,
    MAX_FRAME_BYTES,
    EventFrame,
    FrameTooLargeError,
    HeartbeatFrame,
    SubscribeAckFrame,
    SubscribeRejectedFrame,
    TruncatedFrame,
    encode_frame,
    encode_struct_frame,
    sync_read_frame,
    truncated_frame,
)

_LENGTH_STRUCT = struct.Struct(">I")


# --- round-trip property ------------------------------------------------------


@given(payload=st.binary(min_size=1, max_size=MAX_FRAME_BYTES))
@settings(max_examples=100, deadline=1000)
def test_encode_decode_round_trip(payload: bytes) -> None:
    """decode_frame(encode_frame(payload)) == payload for any valid payload."""
    import socket

    wire = encode_frame(payload)
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    a.setblocking(True)
    b.setblocking(True)
    try:
        a.sendall(wire)
        result = sync_read_frame(b)
    finally:
        a.close()
        b.close()
    assert result == payload


# --- oversize rejection property ---------------------------------------------


@given(extra=st.integers(min_value=1, max_value=1024))
@settings(max_examples=30, deadline=500)
def test_encode_frame_oversize_raises(extra: int) -> None:
    """encode_frame raises FrameTooLargeError for any payload > MAX_FRAME_BYTES."""
    payload = b"\x00" * (MAX_FRAME_BYTES + extra)
    with pytest.raises(FrameTooLargeError):
        encode_frame(payload)


# --- truncated_frame size property -------------------------------------------


@given(
    event_id=st.text(min_size=1, max_size=40),
    reason=st.text(min_size=1, max_size=80),
)
@settings(max_examples=60, deadline=500)
def test_truncated_frame_is_within_max_frame_bytes(event_id: str, reason: str) -> None:
    """truncated_frame() wire output must fit within MAX_FRAME_BYTES + 4-byte prefix."""
    wire = truncated_frame(event_id=event_id, reason=reason)
    # The full wire (prefix + payload) must be at most MAX_FRAME_BYTES + 4.
    assert len(wire) <= MAX_FRAME_BYTES + _LENGTH_STRUCT.size


# --- truncated-prefix rejection property -------------------------------------


@given(prefix_bytes=st.binary(min_size=1, max_size=3))
@settings(max_examples=50, deadline=500)
def test_truncated_length_prefix_raises(prefix_bytes: bytes) -> None:
    """sync_read_frame raises ConnectionError when the length prefix is incomplete."""
    import socket

    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    a.setblocking(True)
    b.setblocking(True)
    try:
        a.sendall(prefix_bytes)
        a.close()
        with pytest.raises(ConnectionError):
            sync_read_frame(b)
    finally:
        b.close()


# --- Struct round-trip properties --------------------------------------------
#
# For each of the five typed wire frames, generate an instance, encode it,
# strip the 4-byte length prefix, JSON-decode, re-encode to bytes, JSON-decode
# again to confirm idempotence, and assert field round-trip fidelity.

_TEXT = st.text(min_size=1, max_size=64)
_SHORT_INT = st.integers(min_value=0, max_value=10_000_000)


@given(
    event_id=_TEXT,
    event_type=_TEXT,
    owner=_TEXT,
    repo=_TEXT,
    received_at=_SHORT_INT,
    delivery_id=_TEXT,
    summary=_TEXT,
    fields=st.dictionaries(
        keys=st.text(min_size=1, max_size=16),
        values=st.text(min_size=0, max_size=32),
        max_size=4,
    ),
)
@settings(max_examples=60, deadline=1000)
def test_event_frame_round_trip(
    event_id: str,
    event_type: str,
    owner: str,
    repo: str,
    received_at: int,
    delivery_id: str,
    summary: str,
    fields: dict[str, Any],
) -> None:
    """encode_struct_frame(EventFrame(...)) round-trips all fields."""
    frame = EventFrame(
        event_id=event_id,
        event_type=event_type,
        owner=owner,
        repo=repo,
        received_at=received_at,
        delivery_id=delivery_id,
        summary=summary,
        fields=fields,
    )
    wire = encode_struct_frame(frame)
    payload = wire[4:]  # strip 4-byte length prefix
    decoded: dict[str, Any] = json.loads(payload)
    # idempotence: re-encode the dict back to bytes, then decode again
    redecoded: dict[str, Any] = json.loads(json.dumps(decoded))
    assert redecoded["event_id"] == event_id
    assert redecoded["event_type"] == event_type
    assert redecoded["owner"] == owner
    assert redecoded["repo"] == repo
    assert redecoded["received_at"] == received_at
    assert redecoded["delivery_id"] == delivery_id
    assert redecoded["summary"] == summary
    assert redecoded["fields"] == fields
    assert redecoded["kind"] == _KIND_EVENT


@given(
    event_id=_TEXT,
    reason=_TEXT,
)
@settings(max_examples=60, deadline=500)
def test_truncated_frame_round_trip(event_id: str, reason: str) -> None:
    """encode_struct_frame(TruncatedFrame(...)) round-trips all fields."""
    frame = TruncatedFrame(event_id=event_id, reason=reason)
    wire = encode_struct_frame(frame)
    payload = wire[4:]
    decoded: dict[str, Any] = json.loads(payload)
    redecoded: dict[str, Any] = json.loads(json.dumps(decoded))
    assert redecoded["event_id"] == event_id
    assert redecoded["reason"] == reason
    assert redecoded["kind"] == _KIND_TRUNCATED


@given(
    ts=_SHORT_INT,
    uptime_sec=_SHORT_INT,
)
@settings(max_examples=60, deadline=500)
def test_heartbeat_frame_round_trip(ts: int, uptime_sec: int) -> None:
    """encode_struct_frame(HeartbeatFrame(...)) round-trips all fields."""
    frame = HeartbeatFrame(ts=ts, uptime_sec=uptime_sec)
    wire = encode_struct_frame(frame)
    payload = wire[4:]
    decoded: dict[str, Any] = json.loads(payload)
    redecoded: dict[str, Any] = json.loads(json.dumps(decoded))
    assert redecoded["ts"] == ts
    assert redecoded["uptime_sec"] == uptime_sec
    assert redecoded["kind"] == _KIND_HEARTBEAT


@given(
    proto=_SHORT_INT,
    caught_up_at=st.one_of(st.none(), _TEXT),
    heartbeat_sec=_SHORT_INT,
    max_frame_bytes_val=_SHORT_INT,
)
@settings(max_examples=60, deadline=500)
def test_subscribe_ack_frame_round_trip(
    proto: int,
    caught_up_at: str | None,
    heartbeat_sec: int,
    max_frame_bytes_val: int,
) -> None:
    """encode_struct_frame(SubscribeAckFrame(...)) round-trips all fields."""
    frame = SubscribeAckFrame(
        proto=proto,
        caught_up_at=caught_up_at,
        heartbeat_sec=heartbeat_sec,
        max_frame_bytes=max_frame_bytes_val,
    )
    wire = encode_struct_frame(frame)
    payload = wire[4:]
    decoded: dict[str, Any] = json.loads(payload)
    redecoded: dict[str, Any] = json.loads(json.dumps(decoded))
    assert redecoded["proto"] == proto
    assert redecoded["caught_up_at"] == caught_up_at
    assert redecoded["heartbeat_sec"] == heartbeat_sec
    assert redecoded["max_frame_bytes"] == max_frame_bytes_val
    assert redecoded["kind"] == _KIND_SUBSCRIBE_ACK


@given(
    reason=_TEXT,
    remediation=st.text(min_size=0, max_size=64),
    supported=st.one_of(st.none(), st.lists(st.integers(min_value=1, max_value=10), max_size=5)),
)
@settings(max_examples=60, deadline=500)
def test_subscribe_rejected_frame_round_trip(
    reason: str,
    remediation: str,
    supported: list[int] | None,
) -> None:
    """encode_struct_frame(SubscribeRejectedFrame(...)) round-trips all fields."""
    frame = SubscribeRejectedFrame(reason=reason, remediation=remediation, supported=supported)
    wire = encode_struct_frame(frame)
    payload = wire[4:]
    decoded: dict[str, Any] = json.loads(payload)
    redecoded: dict[str, Any] = json.loads(json.dumps(decoded))
    assert redecoded["reason"] == reason
    assert redecoded["remediation"] == remediation
    assert redecoded["supported"] == supported
    assert redecoded["kind"] == _KIND_SUBSCRIBE_REJECTED


# --- oversize-Struct fuzz -----------------------------------------------------


# Build a fields dict that is guaranteed to push past MAX_FRAME_BYTES: one key
# mapped to a value longer than the cap itself so the encoded JSON always exceeds
# the limit regardless of the other (small) fields.  Hypothesis varies the
# non-dict fields to exercise the full EventFrame shape without generating
# entropy-intensive large strings on every draw.
_OVERSIZE_FIELDS: dict[str, Any] = {"pad": "x" * (MAX_FRAME_BYTES + 1)}


@given(
    event_id=_TEXT,
    event_type=_TEXT,
    owner=_TEXT,
    repo=_TEXT,
    received_at=_SHORT_INT,
    delivery_id=_TEXT,
    summary=_TEXT,
)
@settings(max_examples=20, deadline=1000)
def test_oversize_struct_raises_frame_too_large(
    event_id: str,
    event_type: str,
    owner: str,
    repo: str,
    received_at: int,
    delivery_id: str,
    summary: str,
) -> None:
    """encode_struct_frame raises FrameTooLargeError when EventFrame.fields is oversized."""
    frame = EventFrame(
        event_id=event_id,
        event_type=event_type,
        owner=owner,
        repo=repo,
        received_at=received_at,
        delivery_id=delivery_id,
        summary=summary,
        fields=_OVERSIZE_FIELDS,
    )
    with pytest.raises(FrameTooLargeError):
        encode_struct_frame(frame)


# --- kind-discriminator invariant --------------------------------------------


@pytest.mark.parametrize(
    "cls, expected_kind",
    [
        (EventFrame, _KIND_EVENT),
        (TruncatedFrame, _KIND_TRUNCATED),
        (HeartbeatFrame, _KIND_HEARTBEAT),
        (SubscribeAckFrame, _KIND_SUBSCRIBE_ACK),
        (SubscribeRejectedFrame, _KIND_SUBSCRIBE_REJECTED),
    ],
)
def test_kind_discriminator_matches_constant(cls: type[msgspec.Struct], expected_kind: str) -> None:
    """Each Struct's ``kind`` field default must equal the matching _KIND_* constant."""
    kind_field = msgspec.structs.fields(cls)[-1]
    assert kind_field.name == "kind", f"{cls.__name__} last field is {kind_field.name!r}, expected 'kind'"
    assert kind_field.default == expected_kind, (
        f"{cls.__name__}.kind default {kind_field.default!r} != {expected_kind!r}"
    )


# --- reflective Struct-shape invariants --------------------------------------

_EXPECTED_FRAME_STRUCTS = frozenset(
    {
        EventFrame,
        TruncatedFrame,
        HeartbeatFrame,
        SubscribeAckFrame,
        SubscribeRejectedFrame,
    }
)


def _collect_frame_structs() -> set[type[msgspec.Struct]]:
    """Return all msgspec.Struct subclasses defined in waitbus._frame."""
    module = importlib.import_module("waitbus._frame")
    found: set[type[msgspec.Struct]] = set()
    for _name, obj in inspect.getmembers(module, inspect.isclass):
        if obj is not msgspec.Struct and issubclass(obj, msgspec.Struct) and obj.__module__ == module.__name__:
            found.add(obj)
    return found


def test_exactly_five_frame_structs() -> None:
    """waitbus._frame must define exactly the five expected Struct subclasses."""
    found = _collect_frame_structs()
    assert len(found) == 5, f"Expected 5 frame Structs, found {len(found)}: {found}"
    assert found == _EXPECTED_FRAME_STRUCTS


def test_all_frames_are_frozen_and_kw_only() -> None:
    """Every frame Struct must be frozen and kw_only.

    ``frozen`` is exposed directly on ``__struct_config__``.  msgspec does not
    expose a ``kw_only`` attribute on ``StructConfig``; keyword-only
    construction is verified behaviourally: passing the first required field as
    a positional argument must raise ``TypeError``.
    """
    for cls in _EXPECTED_FRAME_STRUCTS:
        cfg = cls.__struct_config__
        assert cfg.frozen, f"{cls.__name__} is not frozen"
        # Verify kw_only: positional construction must be rejected.
        # Cast to Any so mypy accepts the heterogeneous Struct union from the frozenset.
        cls_any: Any = cls
        first_field = msgspec.structs.fields(cls_any)[0]
        with pytest.raises(TypeError, match=r"positional"):
            cls_any(first_field.default if first_field.default is not msgspec.NODEFAULT else "x")


def test_data_frames_carry_event_id() -> None:
    """EventFrame and TruncatedFrame must both have an ``event_id`` field."""
    for cls in (EventFrame, TruncatedFrame):
        field_names = {f.name for f in msgspec.structs.fields(cls)}
        assert "event_id" in field_names, f"{cls.__name__} is missing 'event_id' field"


def test_control_frames_have_no_event_id() -> None:
    """HeartbeatFrame, SubscribeAckFrame, SubscribeRejectedFrame must NOT have ``event_id``."""
    for cls in (HeartbeatFrame, SubscribeAckFrame, SubscribeRejectedFrame):
        field_names = {f.name for f in msgspec.structs.fields(cls)}
        assert "event_id" not in field_names, f"{cls.__name__} unexpectedly has 'event_id' field"
