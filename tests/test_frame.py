"""Tests for the waitbus._frame wire-framing primitives.

Covers: encode_frame, encode_struct_frame, truncated_frame,
sync_read_frame, read_frame (async), typed Struct frames (v1 protocol),
round-trips, oversize rejection, malformed-prefix errors,
partial-read reassembly, and clean-EOF handling.
"""

from __future__ import annotations

import asyncio
import json
import socket
import struct
import threading
import time

import pytest

from waitbus._frame import (
    FRAME_PROTO_VERSION,
    MAX_FRAME_BYTES,
    EventFrame,
    FrameTooLargeError,
    HeartbeatFrame,
    SubscribeAckFrame,
    SubscribeRejectedFrame,
    TruncatedFrame,
    encode_frame,
    encode_struct_frame,
    read_frame,
    read_frame_sock,
    sync_read_frame,
    truncated_frame,
)

_LENGTH_STRUCT = struct.Struct(">I")


def _make_socketpair() -> tuple[socket.socket, socket.socket]:
    """Return a connected (writer, reader) socketpair."""
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    a.setblocking(True)
    b.setblocking(True)
    return a, b


# ---------------------------------------------------------------------------
# test 1: round-trip simple bytes
# ---------------------------------------------------------------------------


def test_round_trip_simple() -> None:
    """encode_frame + sync_read_frame must reconstruct the original payload."""
    writer, reader = _make_socketpair()
    try:
        payload = b"hello"
        writer.sendall(encode_frame(payload))
        result = sync_read_frame(reader)
        assert result == payload
    finally:
        writer.close()
        reader.close()


# ---------------------------------------------------------------------------
# test 2: round-trip unicode / JSON payload
# ---------------------------------------------------------------------------


def test_round_trip_unicode_payload() -> None:
    """JSON-encoded Event bytes survive the encode/decode round-trip intact."""
    event = {
        "id": "01HZTEST0000000000000000AB",
        "kind": "workflow_run",
        "owner": "org",
        "repo": "repo",
        "summary": "CI passed on main ✓",
    }
    payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
    writer, reader = _make_socketpair()
    try:
        writer.sendall(encode_frame(payload))
        result = sync_read_frame(reader)
        assert result == payload
        decoded = json.loads(result.decode("utf-8"))
        assert decoded == event
    finally:
        writer.close()
        reader.close()


# ---------------------------------------------------------------------------
# test 3: oversize payload raises FrameTooLargeError
# ---------------------------------------------------------------------------


def test_oversize_payload_raises() -> None:
    """encode_frame must raise FrameTooLargeError when payload exceeds MAX_FRAME_BYTES."""
    oversize = b"x" * (MAX_FRAME_BYTES + 1)
    with pytest.raises(FrameTooLargeError, match="MAX_FRAME_BYTES"):
        encode_frame(oversize)


# ---------------------------------------------------------------------------
# test 4: truncated_frame decodes as stub
# ---------------------------------------------------------------------------


def test_truncated_frame_decodes_as_stub() -> None:
    """truncated_frame() must produce a decodable frame with kind='truncated'."""
    wire = truncated_frame(event_id="01HZTRUNC0000000000000000AB", reason="oversize")
    # Wire format: 4-byte prefix + payload.
    assert len(wire) >= 4
    (length,) = _LENGTH_STRUCT.unpack(wire[:4])
    payload = wire[4 : 4 + length]
    stub = json.loads(payload.decode("utf-8"))
    assert stub["kind"] == "truncated"
    assert stub["event_id"] == "01HZTRUNC0000000000000000AB"
    assert stub["reason"] == "oversize"
    assert stub["max_frame_bytes"] == MAX_FRAME_BYTES


# ---------------------------------------------------------------------------
# test 5: zero-length prefix raises ConnectionError
# ---------------------------------------------------------------------------


def test_zero_length_prefix_raises() -> None:
    """A frame with length=0 in the prefix must raise ConnectionError."""
    writer, reader = _make_socketpair()
    try:
        writer.sendall(_LENGTH_STRUCT.pack(0))
        with pytest.raises(ConnectionError, match="out of bounds"):
            sync_read_frame(reader)
    finally:
        writer.close()
        reader.close()


# ---------------------------------------------------------------------------
# test 6: max-uint32 prefix raises ConnectionError
# ---------------------------------------------------------------------------


def test_max_length_prefix_overflow_raises() -> None:
    """A frame length of 0xFFFFFFFF must raise ConnectionError."""
    writer, reader = _make_socketpair()
    try:
        writer.sendall(_LENGTH_STRUCT.pack(0xFFFFFFFF))
        with pytest.raises(ConnectionError, match="out of bounds"):
            sync_read_frame(reader)
    finally:
        writer.close()
        reader.close()


# ---------------------------------------------------------------------------
# test 7: partial write reassembly
# ---------------------------------------------------------------------------


def test_partial_read_resumes() -> None:
    """sync_read_frame must reassemble a payload written in multiple chunks.

    A separate thread writes the prefix and payload in two sends
    (with a small sleep between) to confirm the reader loops until
    all bytes are available.
    """
    payload = b"chunked-payload-data"
    wire = encode_frame(payload)
    prefix = wire[:4]
    body = wire[4:]

    writer, reader = _make_socketpair()
    errors: list[BaseException] = []

    def _slow_writer() -> None:
        try:
            writer.sendall(prefix)
            time.sleep(0.01)
            writer.sendall(body)
        except Exception as exc:
            errors.append(exc)

    t = threading.Thread(target=_slow_writer, daemon=True)
    t.start()
    result = sync_read_frame(reader)
    t.join(timeout=2.0)
    writer.close()
    reader.close()

    assert not errors, f"writer thread raised: {errors!r}"
    assert result == payload


# ---------------------------------------------------------------------------
# test 8: clean EOF returns None
# ---------------------------------------------------------------------------


def test_eof_on_clean_socket_returns_none() -> None:
    """sync_read_frame must return None when the writer closes without any bytes."""
    writer, reader = _make_socketpair()
    writer.close()
    result = sync_read_frame(reader)
    assert result is None
    reader.close()


# ---------------------------------------------------------------------------
# test 9: partial prefix on close raises ConnectionError
# ---------------------------------------------------------------------------


def test_eof_mid_prefix_raises_connection_error() -> None:
    """Writing only 2 bytes then closing must raise ConnectionError (partial prefix)."""
    writer, reader = _make_socketpair()
    try:
        writer.sendall(b"\x00\x00")  # Only 2 of 4 required prefix bytes
        writer.close()
        with pytest.raises(ConnectionError):
            sync_read_frame(reader)
    finally:
        reader.close()


# ---------------------------------------------------------------------------
# test 10: async read_frame round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_read_frame_round_trip() -> None:
    """read_frame(asyncio.StreamReader) must reconstruct the original payload."""
    payload = b"async-hello"
    wire = encode_frame(payload)

    # Use a socketpair; wrap reader side in asyncio.StreamReader.
    writer_sock, reader_sock = _make_socketpair()
    reader_sock.setblocking(False)

    loop = asyncio.get_running_loop()
    stream_reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(stream_reader)
    # ``create_connection(sock=...)`` takes ownership of ``reader_sock``;
    # the returned transport must be explicitly closed in the finally
    # block or the kernel fd leaks until GC fires the transport's
    # ``__del__`` in a later test (surfaces as
    # ``PytestUnraisableExceptionWarning`` under the strict gate).
    transport, _ = await loop.create_connection(lambda: protocol, sock=reader_sock)

    try:
        writer_sock.sendall(wire)
        result = await asyncio.wait_for(read_frame(stream_reader), timeout=2.0)
        assert result == payload
    finally:
        writer_sock.close()
        transport.close()


# ---------------------------------------------------------------------------
# Protocol v1 typed Struct tests
# ---------------------------------------------------------------------------


def test_frame_proto_version_is_one() -> None:
    """FRAME_PROTO_VERSION must be 1 per the frozen wire protocol v1."""
    assert FRAME_PROTO_VERSION == 1


# --- EventFrame -------------------------------------------------------------


def test_event_frame_fields_and_kind() -> None:
    """EventFrame must carry event_id (not 'id'), event_type, owner, repo,
    received_at, delivery_id, summary, fields, and kind='event'."""
    frame = EventFrame(
        event_id="01HZEV000000000000000000AB",
        event_type="workflow_run",
        owner="myorg",
        repo="myrepo",
        received_at=1_700_000_000,
        delivery_id="del-abc",
        summary="CI passed",
        fields={"conclusion": "success"},
    )
    assert frame.event_id == "01HZEV000000000000000000AB"
    assert frame.event_type == "workflow_run"
    assert frame.owner == "myorg"
    assert frame.repo == "myrepo"
    assert frame.received_at == 1_700_000_000
    assert frame.delivery_id == "del-abc"
    assert frame.summary == "CI passed"
    assert frame.fields == {"conclusion": "success"}
    assert frame.kind == "event"


def test_event_frame_encoded_has_event_id_not_id() -> None:
    """The JSON wire encoding of EventFrame must use 'event_id' as the key,
    not 'id' — enforces the v1 identity field rename."""
    frame = EventFrame(
        event_id="01HZEV000000000000000000AB",
        event_type="workflow_run",
        owner="org",
        repo="repo",
        received_at=0,
        delivery_id="d",
        summary="s",
        fields={},
    )
    wire = encode_struct_frame(frame)
    (length,) = _LENGTH_STRUCT.unpack(wire[:4])
    decoded = json.loads(wire[4 : 4 + length])
    assert "event_id" in decoded, "v1 wire must use 'event_id', not 'id'"
    assert "id" not in decoded, "legacy 'id' key must not appear in v1 EventFrame"
    assert decoded["event_id"] == "01HZEV000000000000000000AB"
    assert decoded["kind"] == "event"
    assert decoded["event_type"] == "workflow_run"


def test_event_frame_is_frozen() -> None:
    """EventFrame is a frozen msgspec.Struct — mutation must raise."""
    frame = EventFrame(
        event_id="x",
        event_type="t",
        owner="o",
        repo="r",
        received_at=0,
        delivery_id="d",
        summary="s",
        fields={},
    )
    with pytest.raises((AttributeError, TypeError)):
        frame.event_id = "mutated"  # type: ignore[misc]


# --- TruncatedFrame ---------------------------------------------------------


def test_truncated_frame_struct_fields() -> None:
    """TruncatedFrame must carry event_id, reason, max_frame_bytes, kind='truncated'."""
    frame = TruncatedFrame(event_id="01HZTRUNC000", reason="oversize")
    assert frame.event_id == "01HZTRUNC000"
    assert frame.reason == "oversize"
    assert frame.max_frame_bytes == MAX_FRAME_BYTES
    assert frame.kind == "truncated"


def test_truncated_frame_wire_via_truncated_frame_helper() -> None:
    """truncated_frame() must produce a length-prefix frame that decodes to
    a TruncatedFrame with event_id (not 'id') and kind='truncated'."""
    wire = truncated_frame(event_id="01HZTRUNC0000000000000000AB", reason="oversize")
    assert len(wire) >= 4
    (length,) = _LENGTH_STRUCT.unpack(wire[:4])
    decoded = json.loads(wire[4 : 4 + length].decode("utf-8"))
    assert decoded["kind"] == "truncated"
    assert decoded["event_id"] == "01HZTRUNC0000000000000000AB"
    assert "id" not in decoded, "legacy 'id' must not appear in TruncatedFrame wire encoding"
    assert decoded["reason"] == "oversize"
    assert decoded["max_frame_bytes"] == MAX_FRAME_BYTES


# --- HeartbeatFrame ---------------------------------------------------------


def test_heartbeat_frame_fields_and_kind() -> None:
    """HeartbeatFrame must carry ts, uptime_sec, and kind='daemon_heartbeat'.
    It must NOT carry an event_id (heartbeats must not advance resume cursors)."""
    frame = HeartbeatFrame(ts=1_700_000_000, uptime_sec=3600)
    assert frame.ts == 1_700_000_000
    assert frame.uptime_sec == 3600
    assert frame.kind == "daemon_heartbeat"
    assert not hasattr(frame, "event_id"), "HeartbeatFrame must not have event_id"


def test_heartbeat_frame_encode_struct_frame_round_trip() -> None:
    """encode_struct_frame(HeartbeatFrame) must produce a valid length-prefix frame."""
    frame = HeartbeatFrame(ts=1_700_000_001, uptime_sec=42)
    wire = encode_struct_frame(frame)
    (length,) = _LENGTH_STRUCT.unpack(wire[:4])
    decoded = json.loads(wire[4 : 4 + length])
    assert decoded["kind"] == "daemon_heartbeat"
    assert decoded["ts"] == 1_700_000_001
    assert decoded["uptime_sec"] == 42


# --- SubscribeRejectedFrame -------------------------------------------------


def test_subscribe_rejected_frame_fields_and_kind() -> None:
    """SubscribeRejectedFrame must carry reason, remediation, supported, and
    kind='subscribe_rejected'."""
    frame = SubscribeRejectedFrame(
        reason="version",
        remediation="upgrade",
        supported=[1],
    )
    assert frame.reason == "version"
    assert frame.remediation == "upgrade"
    assert frame.supported == [1]
    assert frame.kind == "subscribe_rejected"


def test_subscribe_rejected_frame_defaults() -> None:
    """remediation defaults to '' and supported defaults to None."""
    frame = SubscribeRejectedFrame(reason="token")
    assert frame.remediation == ""
    assert frame.supported is None
    assert frame.kind == "subscribe_rejected"


def test_subscribe_rejected_frame_encode_round_trip() -> None:
    """encode_struct_frame(SubscribeRejectedFrame) decodes correctly."""
    frame = SubscribeRejectedFrame(reason="version", supported=[1])
    wire = encode_struct_frame(frame)
    (length,) = _LENGTH_STRUCT.unpack(wire[:4])
    decoded = json.loads(wire[4 : 4 + length])
    assert decoded["kind"] == "subscribe_rejected"
    assert decoded["reason"] == "version"
    assert decoded["supported"] == [1]


# --- SubscribeAckFrame ------------------------------------------------------


def test_subscribe_ack_frame_fields_and_kind() -> None:
    """SubscribeAckFrame must carry proto, caught_up_at, heartbeat_sec,
    max_frame_bytes, and kind='subscribe_ack'."""
    frame = SubscribeAckFrame(
        proto=FRAME_PROTO_VERSION,
        caught_up_at="01HZEV000000000000000000FF",
        heartbeat_sec=30,
        max_frame_bytes=MAX_FRAME_BYTES,
    )
    assert frame.proto == 1
    assert frame.caught_up_at == "01HZEV000000000000000000FF"
    assert frame.heartbeat_sec == 30
    assert frame.max_frame_bytes == MAX_FRAME_BYTES
    assert frame.kind == "subscribe_ack"


def test_subscribe_ack_frame_caught_up_at_none() -> None:
    """caught_up_at=None is valid (no replay requested)."""
    frame = SubscribeAckFrame(
        proto=1,
        caught_up_at=None,
        heartbeat_sec=30,
        max_frame_bytes=MAX_FRAME_BYTES,
    )
    assert frame.caught_up_at is None


def test_subscribe_ack_frame_encode_round_trip() -> None:
    """encode_struct_frame(SubscribeAckFrame) decodes correctly."""
    frame = SubscribeAckFrame(
        proto=1,
        caught_up_at=None,
        heartbeat_sec=30,
        max_frame_bytes=65536,
    )
    wire = encode_struct_frame(frame)
    (length,) = _LENGTH_STRUCT.unpack(wire[:4])
    decoded = json.loads(wire[4 : 4 + length])
    assert decoded["kind"] == "subscribe_ack"
    assert decoded["proto"] == 1
    assert decoded["caught_up_at"] is None
    assert decoded["heartbeat_sec"] == 30
    assert decoded["max_frame_bytes"] == 65536


# --- encode_struct_frame: oversize raises FrameTooLargeError ----------------


def test_encode_struct_frame_raises_on_oversize() -> None:
    """encode_struct_frame must raise FrameTooLargeError when the encoded
    payload exceeds MAX_FRAME_BYTES. Uses a large EventFrame to trigger this."""
    # Build a frame whose fields dict is large enough to exceed MAX_FRAME_BYTES.
    big_value = "x" * (MAX_FRAME_BYTES + 1)
    frame = EventFrame(
        event_id="01HZBIG000000000000000000AB",
        event_type="workflow_run",
        owner="org",
        repo="repo",
        received_at=0,
        delivery_id="d",
        summary=big_value,
        fields={},
    )
    with pytest.raises(FrameTooLargeError, match="MAX_FRAME_BYTES"):
        encode_struct_frame(frame)


# --- encode_struct_frame: valid frame is a length-prefix wire frame ---------


def test_encode_struct_frame_length_prefix_integrity() -> None:
    """encode_struct_frame must produce a frame where the 4-byte prefix exactly
    equals the length of the JSON payload that follows."""
    frame = HeartbeatFrame(ts=100, uptime_sec=1)
    wire = encode_struct_frame(frame)
    (declared_length,) = _LENGTH_STRUCT.unpack(wire[:4])
    actual_payload = wire[4:]
    assert declared_length == len(actual_payload)
    assert 0 < declared_length <= MAX_FRAME_BYTES


# ---------------------------------------------------------------------------
# read_frame (async StreamReader) error-arm coverage
# ---------------------------------------------------------------------------


async def _make_stream_reader_from_bytes(data: bytes) -> asyncio.StreamReader:
    """Feed *data* into a fresh StreamReader and then signal EOF.

    Wraps asyncio.StreamReader.feed_data / feed_eof so tests can drive the
    reader without any socket or transport.
    """
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


@pytest.mark.asyncio
async def test_read_frame_partial_length_prefix_raises_connection_error() -> None:
    """Feeding only 2 bytes then EOF must raise ConnectionError matching
    'partial length prefix' — the connection closed before the 4-byte
    length prefix was complete."""
    reader = await _make_stream_reader_from_bytes(b"\x00\x00")
    with pytest.raises(ConnectionError, match="partial length prefix"):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_read_frame_zero_length_raises_connection_error() -> None:
    """A length prefix of 0 must raise ConnectionError matching 'out of bounds'."""
    reader = await _make_stream_reader_from_bytes(_LENGTH_STRUCT.pack(0))
    with pytest.raises(ConnectionError, match="out of bounds"):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_read_frame_oversize_length_raises_connection_error() -> None:
    """A length prefix exceeding MAX_FRAME_BYTES must raise ConnectionError."""
    reader = await _make_stream_reader_from_bytes(_LENGTH_STRUCT.pack(MAX_FRAME_BYTES + 1))
    with pytest.raises(ConnectionError):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_read_frame_short_payload_raises_connection_error() -> None:
    """Declaring length=10 but writing only 3 payload bytes then EOF must raise
    ConnectionError matching 'short read'."""
    data = _LENGTH_STRUCT.pack(10) + b"abc"
    reader = await _make_stream_reader_from_bytes(data)
    with pytest.raises(ConnectionError, match="short read"):
        await read_frame(reader)


# ---------------------------------------------------------------------------
# read_frame_sock (async raw-socket path) error-arm coverage
# ---------------------------------------------------------------------------


def _make_nonblocking_socketpair() -> tuple[socket.socket, socket.socket]:
    """Return a connected (writer, reader) socketpair with reader in non-blocking mode."""
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    a.setblocking(True)  # writer: blocking for simple sendall
    b.setblocking(False)  # reader: non-blocking for loop.sock_recv
    return a, b


@pytest.mark.asyncio
async def test_read_frame_sock_oversize_length_raises_connection_error() -> None:
    """Writing an oversize length prefix to the raw socket must raise ConnectionError."""
    writer, reader = _make_nonblocking_socketpair()
    try:
        writer.sendall(_LENGTH_STRUCT.pack(MAX_FRAME_BYTES + 1))
        loop = asyncio.get_running_loop()
        with pytest.raises(ConnectionError):
            await read_frame_sock(loop, reader)
    finally:
        writer.close()
        reader.close()


@pytest.mark.asyncio
async def test_read_frame_sock_mid_frame_eof_raises_connection_error() -> None:
    """Writing a valid length prefix + partial payload then closing the writer
    must raise ConnectionError matching 'mid-frame'."""
    writer, reader = _make_nonblocking_socketpair()
    try:
        payload_len = 20
        writer.sendall(_LENGTH_STRUCT.pack(payload_len))
        writer.sendall(b"partial")  # fewer bytes than declared length
        writer.close()
        loop = asyncio.get_running_loop()
        with pytest.raises(ConnectionError, match="mid-frame"):
            await read_frame_sock(loop, reader)
    finally:
        reader.close()


@pytest.mark.asyncio
async def test_read_frame_sock_zero_payload_after_prefix_raises_connection_error() -> None:
    """Writing a valid length prefix then closing the writer with no payload bytes
    must raise ConnectionError matching 'short read on payload'."""
    writer, reader = _make_nonblocking_socketpair()
    try:
        payload_len = 10
        writer.sendall(_LENGTH_STRUCT.pack(payload_len))
        writer.close()
        loop = asyncio.get_running_loop()
        with pytest.raises(ConnectionError, match="short read on payload"):
            await read_frame_sock(loop, reader)
    finally:
        reader.close()


@pytest.mark.asyncio
async def test_read_frame_sock_clean_eof_returns_none() -> None:
    """Closing the writer without sending any bytes must return None (clean EOF)."""
    writer, reader = _make_nonblocking_socketpair()
    writer.close()
    loop = asyncio.get_running_loop()
    result = await read_frame_sock(loop, reader)
    assert result is None
    reader.close()


# ---------------------------------------------------------------------------
# MAX_FRAME_BYTES boundary cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_frame_at_max_frame_bytes_succeeds() -> None:
    """A frame whose payload is exactly MAX_FRAME_BYTES bytes must be accepted.

    The producer-side guard is ``length > MAX_FRAME_BYTES`` (strict-greater),
    so equal is within bounds and must succeed.
    """
    payload = b"x" * MAX_FRAME_BYTES
    data = _LENGTH_STRUCT.pack(MAX_FRAME_BYTES) + payload
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    result = await asyncio.wait_for(read_frame(reader), timeout=2.0)
    assert result == payload


@pytest.mark.asyncio
async def test_read_frame_at_max_frame_bytes_plus_one_rejected() -> None:
    """A length prefix of MAX_FRAME_BYTES + 1 must be rejected with ConnectionError."""
    reader = await _make_stream_reader_from_bytes(_LENGTH_STRUCT.pack(MAX_FRAME_BYTES + 1))
    with pytest.raises(ConnectionError):
        await read_frame(reader)
