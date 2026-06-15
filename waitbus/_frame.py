"""4-byte big-endian length-prefix wire framing for AF_UNIX SOCK_STREAM.

The previous broadcast wire used AF_UNIX SOCK_SEQPACKET, which made
each send() / recv() one atomic message but limited the project to
Linux (Darwin returns ENOPROTOOPT). The current shape uses SOCK_STREAM
on both platforms with explicit length-prefix framing — the same
convention used by D-Bus, ZMTP, MQTT, and gRPC.

Wire format per frame:

    [4 bytes][N bytes]
    ^^^^^^^^ ^^^^^^^^
    length   payload (JSON-encoded Event)
    (big-endian unsigned 32-bit;
     0 < length <= MAX_FRAME_BYTES)

MAX_FRAME_BYTES is enforced producer-side in encode_frame(); a payload
that would overflow is replaced with a small "kind: truncated" stub
frame so the subscriber receives a deterministic signal rather than
an arbitrarily-truncated JSON blob.
"""

from __future__ import annotations

import asyncio
import socket
import struct
from typing import Any, Final

import msgspec

# Broadcast wire protocol version. Negotiated once on the subscribe
# envelope (the client sends ``proto``); the daemon rejects an unsupported
# version with a ``subscribe_rejected`` control frame rather than letting a
# mismatch surface frame-by-frame mid-stream. Data frames carry NO per-frame
# version — the version is connection-scoped, matching NATS INFO/CONNECT,
# MCP initialize, and RESP3 HELLO.
FRAME_PROTO_VERSION: Final[int] = 1

MAX_FRAME_BYTES: Final[int] = 65_536  # 64 KiB; matches the prior SEQPACKET kernel cap
_LENGTH_PREFIX_BYTES: Final[int] = 4
_LENGTH_STRUCT: Final[struct.Struct] = struct.Struct(">I")  # big-endian uint32

# One shared encoder for every typed wire frame (the Structs below). Reused
# rather than per-call constructed — msgspec amortises type-graph inspection
# on first use.
_FRAME_ENCODER: Final[msgspec.json.Encoder] = msgspec.json.Encoder()


class FrameTooLargeError(ValueError):
    """Raised when an encoder caller bypasses the producer-side bound.

    Should NOT surface to subscribers — encode_frame replaces oversize
    payloads with a truncated stub. This exception exists for tests and
    for callers that want to verify a frame fits.
    """


# --- typed wire frames (protocol v1) ----------------------------------------
#
# Every frame the broadcast daemon emits is one of these Structs, encoded to
# compact JSON via ``encode_struct_frame``. ``kind`` is the control-vs-data
# discriminator: data-plane frames are ``event`` / ``truncated`` (they carry
# an ``event_id``); control-plane frames are ``subscribe_ack`` /
# ``subscribe_rejected`` / ``daemon_heartbeat`` (no ``event_id``). The wire
# stays flat JSON with a string ``kind`` so the multilingual subscribers
# hand-decode without an exhaustive closed union, and an unknown future
# ``event_type`` value passes through additively.

# ``kind`` discriminator constants. Single source of truth for the wire's
# five frame kinds; consumers in this Python tree import these instead of
# repeating string literals at skip-set sites. The four multilingual snippet
# clients decode by inverted ``kind != "event"`` so they do not import these
# — they are self-contained.
_KIND_EVENT: Final[str] = "event"
_KIND_TRUNCATED: Final[str] = "truncated"
_KIND_HEARTBEAT: Final[str] = "daemon_heartbeat"
_KIND_SUBSCRIBE_ACK: Final[str] = "subscribe_ack"
_KIND_SUBSCRIBE_REJECTED: Final[str] = "subscribe_rejected"

# DATA_FRAME_KINDS / CONTROL_FRAME_KINDS / ALL_FRAME_KINDS are the PUBLIC
# consumer-facing partitioning of the wire kind catalogue. A consumer that
# distinguishes data frames (carry an event_id, advance the cursor) from
# control frames uses these instead of hard-coding the kind strings. They are
# daemon-authoritative: plugins extend the open event_type value-space, not
# the kind set. The
# frame-catalogue lint pins them against CONSUMER_API.md §2a.
DATA_FRAME_KINDS: Final[frozenset[str]] = frozenset({_KIND_EVENT, _KIND_TRUNCATED})
"""Public: frames that carry an ``event_id`` and advance a subscriber's resume cursor."""

CONTROL_FRAME_KINDS: Final[frozenset[str]] = frozenset({_KIND_HEARTBEAT, _KIND_SUBSCRIBE_ACK, _KIND_SUBSCRIBE_REJECTED})
"""Public: frames that carry NO ``event_id``: liveness, registration ack, terminal reject."""

ALL_FRAME_KINDS: Final[frozenset[str]] = DATA_FRAME_KINDS | CONTROL_FRAME_KINDS
"""Public: the complete wire kind catalogue (data + control)."""

# Subset consumed off the wire by ``await_predicate`` and the MCP tail loop
# but never passed to caller predicates: the engine treats them as pure
# liveness/handshake signals. ``subscribe_rejected`` is NOT in this set
# because the engine raises typed errors on it (terminal, not drainable).
# Shared across production modules (broadcast-sub engine + MCP projection),
# so it is public (no leading underscore) despite being engine-internal in spirit.
DRAINABLE_CONTROL_KINDS: Final[frozenset[str]] = frozenset({_KIND_HEARTBEAT, _KIND_SUBSCRIBE_ACK})


class EventFrame(msgspec.Struct, kw_only=True, frozen=True):
    """Data-plane frame for one event row. ``kind`` is always ``event``;
    ``event_type`` carries the (open-value-space) event class. ``event_id``
    is the canonical wire identity + resumable cursor key."""

    event_id: str
    event_type: str
    owner: str
    repo: str
    received_at: int
    delivery_id: str
    summary: str
    fields: dict[str, Any]
    kind: str = _KIND_EVENT


class TruncatedFrame(msgspec.Struct, kw_only=True, frozen=True):
    """Data-plane stub substituted for an event whose payload exceeds
    ``MAX_FRAME_BYTES``. Carries ``event_id`` so the subscriber re-fetches
    the full row via the SQL / CLI surface. For an addressed agent reply it
    also carries ``correlation_id`` so the requester's ``request()`` can still
    correlation-match the (otherwise fields-less) stub and re-fetch the body
    -- the degenerate Claim-Check for an oversize reply. The field is additive
    on the existing ``truncated`` data kind, so the wire stays ``proto=1``."""

    event_id: str
    reason: str
    correlation_id: str | None = None
    max_frame_bytes: int = MAX_FRAME_BYTES
    kind: str = _KIND_TRUNCATED


class HeartbeatFrame(msgspec.Struct, kw_only=True, frozen=True):
    """Control-plane liveness tick. Carries NO event identity — a
    subscriber's resume cursor must never advance on a heartbeat."""

    ts: int
    uptime_sec: int
    kind: str = _KIND_HEARTBEAT


class SubscribeRejectedFrame(msgspec.Struct, kw_only=True, frozen=True):
    """Control-plane terminal frame written once before the daemon closes a
    rejected subscribe. ``reason='token'`` for an auth failure;
    ``reason='version'`` (with ``supported``) for a proto mismatch."""

    reason: str
    remediation: str = ""
    supported: list[int] | None = None
    kind: str = _KIND_SUBSCRIBE_REJECTED


class SubscribeAckFrame(msgspec.Struct, kw_only=True, frozen=True):
    """Control-plane positive acknowledgement.

    Wire ordering invariant: ``subscribe_ack`` is the FIRST frame on the
    wire after envelope validation succeeds. With-``since`` replay frames
    follow the ack; live frames that landed during the registration window
    are captured into a server-side pre-ack buffer and drained after the
    replay tail. Consumers classify the post-ack stream by ``event_id``:
    ``event_id <= caught_up_at`` is replay, ``> caught_up_at`` is live.

    ``caught_up_at`` is a positional dedup cursor for replay (NOT a
    temporal "ack-then-live" barrier); it is ``None`` when the subscribe
    had no ``since`` replay. Advertises the daemon's liveness cadence +
    frame cap (NATS ``INFO``-greeting capability pattern)."""

    proto: int
    caught_up_at: str | None
    heartbeat_sec: int
    max_frame_bytes: int
    kind: str = _KIND_SUBSCRIBE_ACK


def encode_struct_frame(frame: msgspec.Struct) -> bytes:
    """Encode a typed wire frame to compact JSON wrapped in the length-prefix
    wire frame. Raises ``FrameTooLargeError`` if the encoded payload exceeds
    ``MAX_FRAME_BYTES`` (event-frame callers catch this and substitute
    ``truncated_frame``; control frames are always small).

    Delegates the bounds check + framing to :func:`encode_frame` so there
    is one canonical site that owns the wire's length prefix.
    """
    return encode_frame(_FRAME_ENCODER.encode(frame))


def encode_frame(payload: bytes) -> bytes:
    """Wrap a payload in the length-prefix wire frame.

    payload is the already-JSON-encoded Event bytes. If len(payload)
    exceeds MAX_FRAME_BYTES, raises FrameTooLargeError. Sole bounds-check
    site for both the typed Struct emit path (via
    :func:`encode_struct_frame`) and the legacy raw-bytes emit path.
    """
    if len(payload) > MAX_FRAME_BYTES:
        raise FrameTooLargeError(f"frame payload {len(payload)} bytes exceeds MAX_FRAME_BYTES {MAX_FRAME_BYTES}")
    return _LENGTH_STRUCT.pack(len(payload)) + payload


def truncated_frame(*, event_id: str, reason: str, correlation_id: str | None = None) -> bytes:
    """Build the stub frame that substitutes for an oversize payload.

    Subscribers decode this normally; the kind field tells them the
    original event is too large for the wire and they should fetch it
    via the SQL surface (read-events / pr-monitor / mcp tool) instead.
    ``correlation_id`` (set only for an addressed agent reply) lets the
    requester match the stub and re-fetch the full body by ``event_id``.
    """
    return encode_struct_frame(TruncatedFrame(event_id=event_id, reason=reason, correlation_id=correlation_id))


async def read_frame(reader: asyncio.StreamReader) -> bytes | None:
    """Read one frame from the stream. Returns None on EOF.

    Raises ConnectionError on a malformed length prefix (frame >
    MAX_FRAME_BYTES) — the connection should be closed in that case.
    Partial reads are handled transparently by readexactly.
    """
    try:
        prefix = await reader.readexactly(_LENGTH_PREFIX_BYTES)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None  # clean EOF
        raise ConnectionError(f"partial length prefix: {exc.partial!r}") from exc

    (length,) = _LENGTH_STRUCT.unpack(prefix)
    if length == 0 or length > MAX_FRAME_BYTES:
        raise ConnectionError(f"frame length {length} out of bounds (1..{MAX_FRAME_BYTES})")

    try:
        return await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionError(f"short read: expected {length} bytes, got {len(exc.partial)}") from exc


def sync_read_frame(sock: socket.socket) -> bytes | None:
    """Synchronous variant for subscribers that don't use asyncio.

    Reads one frame off the socket; returns None on EOF. Same bounds
    and error semantics as read_frame. The caller must have already
    set sock.setblocking(True) for this to behave correctly.
    """
    prefix = _read_exactly(sock, _LENGTH_PREFIX_BYTES)
    if prefix is None:
        return None
    (length,) = _LENGTH_STRUCT.unpack(prefix)
    if length == 0 or length > MAX_FRAME_BYTES:
        raise ConnectionError(f"frame length {length} out of bounds (1..{MAX_FRAME_BYTES})")
    payload = _read_exactly(sock, length)
    if payload is None:
        raise ConnectionError(f"short read on payload (expected {length} bytes)")
    return payload


def _read_exactly(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly n bytes from sock.

    Returns None on a clean EOF (zero bytes read before any data arrives).
    Raises ConnectionError on a partial read (some bytes received, then EOF).
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            if not buf:
                return None  # clean EOF before any bytes of this frame
            raise ConnectionError(f"connection closed mid-frame after {len(buf)} of {n} bytes")
        buf.extend(chunk)
    return bytes(buf)


async def read_frame_sock(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
) -> bytes | None:
    """Async variant of ``read_frame`` that operates on a raw socket.

    Uses ``loop.sock_recv`` for non-blocking awaitable reads with no
    intermediate ``asyncio.StreamReader`` / ``Transport``. This matters
    for one-shot reads (e.g. the broadcast daemon's subscribe-frame
    handshake): a ``loop.create_connection`` transport leaks a
    ``_SelectorSocketTransport`` if its lifetime overlaps event-loop
    teardown — Python 3.14+ surfaces this via ``ResourceWarning``
    under ``PytestUnraisableExceptionWarning`` and our pytest gate.
    Reading the frame off the raw socket sidesteps the transport
    entirely so there is nothing to leak.

    ``sock`` must be in non-blocking mode (``setblocking(False)``) for
    ``loop.sock_recv`` to behave correctly. Caller owns the socket's
    lifecycle; this function does not close it.

    Returns ``None`` on a clean EOF (zero bytes before any data of this
    frame). Raises ``ConnectionError`` on a malformed length prefix
    (out of bounds) or a partial read (bytes received then EOF).
    """
    prefix = await _sock_recv_exactly(loop, sock, _LENGTH_PREFIX_BYTES)
    if prefix is None:
        return None
    (length,) = _LENGTH_STRUCT.unpack(prefix)
    if length == 0 or length > MAX_FRAME_BYTES:
        raise ConnectionError(f"frame length {length} out of bounds (1..{MAX_FRAME_BYTES})")
    payload = await _sock_recv_exactly(loop, sock, length)
    if payload is None:
        raise ConnectionError(f"short read on payload (expected {length} bytes)")
    return payload


async def _sock_recv_exactly(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    n: int,
) -> bytes | None:
    """Read exactly ``n`` bytes from ``sock`` via ``loop.sock_recv``.

    Mirrors ``_read_exactly`` semantics for the asyncio path: returns
    ``None`` on clean pre-frame EOF, raises ``ConnectionError`` on
    mid-frame EOF after partial bytes were received.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = await loop.sock_recv(sock, n - len(buf))
        if not chunk:
            if not buf:
                return None
            raise ConnectionError(f"connection closed mid-frame after {len(buf)} of {n} bytes")
        buf.extend(chunk)
    return bytes(buf)
