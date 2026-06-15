"""waitbus broadcast hub.

A pure-stdlib AF_UNIX SOCK_STREAM daemon that tails the events table
and fans rows out to subscribed sockets within about a millisecond of
commit (measured locally). The companion `_doorbell.py` connects an AF_UNIX SOCK_STREAM socket and
writes a single byte to `_paths.doorbell_socket()` after every
`insert_event` commit; the daemon's read-side coalesces doorbell pings
and sweeps the events table via `SELECT ... WHERE seq > :cursor
ORDER BY seq LIMIT 500` on each wake (`seq` is the daemon-assigned
autoincrement key, monotonic in commit order). On Linux the daemon uses an ``os.eventfd`` as
the coalescing wake primitive; on macOS the listener fd serves directly.

Wire protocol (length-prefix framed SOCK_STREAM, both directions):

  subscribe (client -> server):
    <4-byte big-endian length><JSON bytes>
    {"proto": 1,
     "filters": ["owner/repo", "owner/*", "*", ...],
     "event_types": ["workflow_run", "workflow_job",
                     "prometheus_alert", "prometheus_watchdog"],
     "since": "01HZ...26chars" | null,
     "token": "..." | null}

  event frame (server -> client):
    <4-byte big-endian length><JSON bytes>
    {"kind": "event",
     "event_id": "01HZ...26chars",
     "event_type": "workflow_run" | "workflow_job" |
                   "prometheus_alert" | "prometheus_watchdog" | ...,
     "owner": "...", "repo": "...",
     "received_at": <epoch-nanoseconds>, "delivery_id": "...",
     "summary": "<one line, <= 400 chars>",
     "fields": {...}}

  control frames (server -> client) carry "kind" in
    {"daemon_heartbeat", "subscribe_ack", "subscribe_rejected", "truncated"}
    and (except "truncated") carry no event identity. The wire-protocol
    version is negotiated once via the subscribe envelope's "proto" field;
    data frames carry no per-frame version. See docs/CONSUMER_API.md for the
    full v1 frame catalogue.

Auth:
- `SO_PEERCRED` peer UID must equal `os.getuid()`; mismatched callers
  are closed without reply (single-user-laptop assumption).
- If the unit declares ``LoadCredentialEncrypted=broadcast-token:...``,
  the subscribe frame MUST carry a matching ``token`` (constant-time
  compare); absent credential = no token required. A failed token check
  (the only post-peer-cred reject) gets one ``subscribe_rejected`` frame
  before close; every pre-token / request-shape reject stays silent-EOF.

Backpressure: non-blocking sockets; on EAGAIN/EWOULDBLOCK the frame
is dropped for that subscriber only and a per-subscriber lag counter
increments. After `LAG_LIMIT` consecutive drops the subscriber is
closed; reconnect-on-EOF on the client re-establishes a fresh cursor.

systemd: `Type=notify` with `sd_notify(READY=1)` after both sockets
bind; on SIGTERM/SIGINT the daemon stops accepting, drains in-flight
broadcasts, closes subscribers (EOF), unlinks owned socket files,
and exits 0. Socket activation via fd 3 is preferred; manual bind at
`_paths.broadcast_socket()` is the fallback.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import hmac
import json
import logging
import os
import re
import selectors
import signal
import socket
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any, Final, cast

from . import _config, _db, _paths, _peercred
from . import _secrets as _secrets
from ._db import EVENT_COLUMNS, ensure_schema
from ._doorbell import _IS_LINUX, Doorbell
from ._frame import (
    _LENGTH_PREFIX_BYTES,
    FRAME_PROTO_VERSION,
    MAX_FRAME_BYTES,
    EventFrame,
    FrameTooLargeError,
    HeartbeatFrame,
    SubscribeAckFrame,
    SubscribeRejectedFrame,
    encode_struct_frame,
    truncated_frame,
)
from ._log import structured
from ._metrics import (
    BROADCAST_EMISSION_LATENCY_SECONDS,
    BROADCAST_SEND_SECONDS,
    BROADCAST_STALE_SUBSCRIPTION_COUNT,
    BROADCAST_SUBSCRIPTION_COUNT,
    SUBSCRIBER_COUNT,
    SUBSCRIBER_LAG_MAX,
    SUBSCRIBER_TX_BUFFER_BYTES,
    WATERMARK_REPLAY_EVENTS_TOTAL,
    incr,
)
from ._metrics import (
    snapshot as _metrics_snapshot,
)
from ._metrics_http import MetricsServer
from ._paths import broadcast_socket, doorbell_socket, ensure_state_dirs
from ._protocols import RowLike
from ._sdnotify import sd_notify as _sd_notify_impl
from .sources._registry import event_types_supported

# --- constants --------------------------------------------------------------

PER_SUBSCRIBER_SNDBUF = 1 << 20  # 1 MiB

_REJECT_DRAIN_LIMIT_BYTES: Final[int] = 32 * 1024
"""Maximum bytes drained from a rejected subscriber's socket before closing.

Draining prevents the peer from seeing a TCP RST (and the resulting
``PytestUnraisableExceptionWarning`` on Python 3.14+) by consuming any
data the peer may have sent before it saw the rejection. Bounded to 32 KiB
so a malicious peer cannot stall the daemon indefinitely.
"""

_REJECT_DRAIN_CHUNK_BYTES: Final[int] = 4096
"""Chunk size for each ``recv`` call while draining a rejected socket.

Small enough to avoid large stack allocations; large enough to empty a
typical subscribe-frame in one or two calls.
"""

_SUBSCRIBE_FRAME_TIMEOUT_SEC: Final[float] = 10.0
"""Seconds to wait for the subscribe frame from a newly-accepted client.

A client that connects but never sends the JSON handshake frame is
dropped after this deadline to prevent the daemon from accumulating
idle file descriptors.
"""

_LISTENER_BACKLOG: Final[int] = 32
"""Listen backlog depth for the broadcast subscriber listener socket.

Most subscribers connect at startup and maintain a persistent connection;
32 is comfortably above the burst at daemon restart when all subscribers
reconnect within one retry window.
"""
"""Per-accepted-subscriber send buffer. The kernel default is ~208 KB
(net.core.wmem_default), which at our ~1.5 KB average frame size only
absorbs ~140 frames before a non-blocking ``send`` returns EAGAIN. Bumping
to 1 MiB raises that to ~700 frames per subscriber, eating typical CI
event bursts without firing the drop-after-N-EAGAIN guard. SO_RCVBUF
is not set: unix(7) documents that AF_UNIX silently
ignores SO_RCVBUF; only SO_SNDBUF on the sender governs flow control.
See https://elixir.bootlin.com/linux/latest/source/net/unix/af_unix.c
for the kernel evidence on sk_wmem_alloc vs sk_sndbuf."""


LAG_LIMIT = 10
"""Drop a subscriber after N consecutive EAGAIN send failures."""

PRE_ACK_BUFFER_FRAMES = LAG_LIMIT
"""Max number of live frames a subscriber's pre-ack buffer can hold during
the registration-to-ack window. Sized equal to ``LAG_LIMIT`` because the
existing wire invariant is that a subscriber can be at most ``LAG_LIMIT``
deliveries behind before being dropped; the buffer cap mirrors that
invariant for the brief pre-ack interval. Overflow folds into the
existing ``lag_limit_exceeded`` reject path."""

PRE_ACK_BUFFER_BYTES = LAG_LIMIT * (MAX_FRAME_BYTES + _LENGTH_PREFIX_BYTES)
"""Max total wire bytes a subscriber's pre-ack buffer can hold. The gate
accounts ``len(blob)`` where ``blob`` is the framed wire payload
(``MAX_FRAME_BYTES`` payload + the ``_LENGTH_PREFIX_BYTES`` length prefix
from ``encode_frame``), so the cap is the framed-byte worst case of
``LAG_LIMIT`` max-sized frames. Sizing it as ``LAG_LIMIT * MAX_FRAME_BYTES``
(payload-only) would fire the byte cap one prefix-per-frame early, before
the frame-count cap; matching the framed size keeps the two caps coherent
with the live wire's lag window. Bounds per-subscriber memory while
ack-send is in flight."""

SWEEP_LIMIT = 500
"""Max rows per broadcast pass. Caps memory + lets new doorbell pings
interleave; the cursor advances per row so nothing is lost."""

REPLAY_LIMIT = 500
"""Max rows replayed when a subscriber sends `since`."""

FILTER_RE = re.compile(r"^([A-Za-z0-9_.-]+/([A-Za-z0-9_.-]+|\*)|\*)$")
"""Validates subscribe filters. Anchored, no shell-metachar surface."""

# Crockford-base32 alphabet excluding I, L, O, U. 26 chars is the canonical
# ULID encoding width. Pre-validating the cursor here means a malformed
# `since` value cannot reach the prepared statement as a partial LIKE-prefix
# and trigger a slow scan over the events table.
ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")

# Subscribe-time token length bounds. The token is generated as
# secrets.token_hex(32) (64 hex chars) elsewhere, so 16..128 is a generous
# envelope that still rejects empty strings and pathologically large inputs.
TOKEN_MIN_LEN = 16
TOKEN_MAX_LEN = 128

BROADCAST_TOKEN_CRED = "broadcast-token"
"""systemd-creds credential name for the optional subscribe-time bearer
token. When the unit declares ``LoadCredentialEncrypted=broadcast-token:...``,
the token is read from ``$CREDENTIALS_DIRECTORY/broadcast-token`` at
daemon startup; absence means token-less subscribers are accepted."""

logger = logging.getLogger("waitbus.broadcast")


def _sd_notify(payload: bytes) -> None:
    """Thin wrapper delegating to the shared sd_notify helper.

    Kept as an explicit module-level function so tests and internal callers
    can patch or reference ``broadcast._sd_notify`` without relying on the
    import alias (which ``--no-implicit-reexport`` would not re-export).
    """
    _sd_notify_impl(payload)


# --- credential token lookup (optional auth) -------------------------------


def _lookup_token() -> str | None:
    """Return the broadcast token from systemd-creds, or None when absent.

    The credential is exposed by the unit's
    ``LoadCredentialEncrypted=broadcast-token:...`` directive; reading is
    a plain file read under ``$CREDENTIALS_DIRECTORY``. An unreadable
    credential file is logged and treated as "no token configured" so
    the daemon still starts.
    """
    try:
        return _secrets.get_secret(BROADCAST_TOKEN_CRED)
    except _secrets.SecretNotConfigured as exc:
        structured(logger, logging.WARNING, "broadcast_token_lookup_failed", error=str(exc))
        return None


# --- peer-credential check --------------------------------------------------


def _peer_uid(sock: socket.socket) -> int | None:
    """Return the connecting peer's UID, or None on failure.

    Thin wrapper over ``_peercred.peer_uid`` that converts the
    platform-shim's ``OSError`` failure mode into ``None`` so the
    caller's ``peer is None or peer != self.uid`` reject path is
    unchanged. Linux uses ``SO_PEERCRED``; macOS uses ``getpeereid()``
    (see ``_peercred`` for the CVE rationale against ``LOCAL_PEERPID``).
    """
    try:
        return _peercred.peer_uid(sock)
    except OSError:
        return None


# --- formatting helper (reuses read_events.format_text) ---------------------


def _summary_for(row: RowLike) -> str:
    """Produce the one-line summary subscribers print under Monitor.

    Imported lazily so this module doesn't pay the read_events import cost
    just for the heartbeat-only test path.
    """
    from .read_events import format_text

    try:
        return format_text(cast(sqlite3.Row, row))
    except (KeyError, TypeError, AttributeError, ValueError) as exc:
        structured(logger, logging.WARNING, "format_text_failed", error=str(exc))
        return f"{row['owner']}/{row['repo']} {row['event_type']} {row['delivery_id']}"


# --- frame construction -----------------------------------------------------


def _row_to_frame(row: sqlite3.Row) -> EventFrame:
    """Build the typed wire frame for a single events-table row.

    ``kind`` is always ``event`` (the control-vs-data discriminator);
    ``event_type`` carries the (open-value-space) event class; ``event_id``
    is the canonical wire identity + resumable-cursor key.
    """
    fields: dict[str, Any] = {
        col: row[col] for col in EVENT_COLUMNS if col not in {"delivery_id", "received_at", "payload_json", "event_id"}
    }
    return EventFrame(
        event_id=row["event_id"],
        event_type=row["event_type"],
        owner=row["owner"],
        repo=row["repo"],
        received_at=row["received_at"],
        delivery_id=row["delivery_id"],
        summary=_summary_for(row),
        fields=fields,
    )


def _serialize(frame: EventFrame) -> bytes:
    """Encode a typed event frame to the length-prefix wire frame.

    Oversize payloads (exceeding ``MAX_FRAME_BYTES``) are replaced by a
    ``truncated`` stub naming the event the subscriber can re-fetch via
    the on-demand CLI / SQL surface.
    """
    try:
        return encode_struct_frame(frame)
    except FrameTooLargeError:
        # Preserve the addressing correlation id on the stub so an oversize
        # agent reply stays correlation-matchable; request() re-fetches the full
        # body from the event store by event_id (the degenerate Claim-Check --
        # the body is already durable in SQLite, only the wire frame truncates).
        return truncated_frame(
            event_id=frame.event_id,
            reason="payload exceeds MAX_FRAME_BYTES",
            correlation_id=frame.fields.get("msg_correlation_id"),
        )


# The two terminal reject frames the daemon writes back before closing a
# rejected subscribe. Each is emitted at most once, only AFTER the accept-time
# SO_PEERCRED gate has proven the peer is same-UID, so the disclosure leaks
# nothing to an unauthenticated surface (AF_UNIX has no network surface, and a
# foreign-UID peer is closed silently before this code is reachable). ``kind``
# is the control discriminator and never collides with the ``event`` data
# kind. See docs/CONSUMER_API.md §3.
_SUBSCRIBE_REJECT_TOKEN_FRAME: Final[bytes] = encode_struct_frame(
    SubscribeRejectedFrame(
        reason="token",
        remediation=(
            "Stage a matching broadcast-token credential (see "
            "docs/CONSUMER_API.md §3) and set "
            "WAITBUS_BROADCAST_TOKEN or "
            "$CREDENTIALS_DIRECTORY/broadcast-token."
        ),
    )
)

# Written once before close when a client sends an unsupported wire ``proto``.
# ``supported`` lets a future multi-version client negotiate down.
_SUBSCRIBE_REJECT_VERSION_FRAME: Final[bytes] = encode_struct_frame(
    SubscribeRejectedFrame(
        reason="version",
        remediation=(
            f"This daemon speaks broadcast wire protocol v{FRAME_PROTO_VERSION}; "
            f"send 'proto': {FRAME_PROTO_VERSION} (or omit it) in the subscribe envelope."
        ),
        supported=[FRAME_PROTO_VERSION],
    )
)

# Emitted best-effort before closing a subscriber that exceeded the lag
# limit. Three internal trigger sites converge on this one wire frame: the
# live fan-out path (reason "lag_limit_exceeded"), the heartbeat loop
# (reason "heartbeat_lag"), and the pre-ack buffer / replay drain during the
# registration->ack window (reasons "lag_limit_exceeded" /
# "replay_lag_limit_exceeded"). Collapsing all three onto one wire reason
# ("lag_limit_exceeded") keeps the consumer vocabulary minimal -- the
# consumer's recovery is identical (reconnect with backoff / narrower
# filters / a since cursor) regardless of which internal path lagged; the
# precise trigger reason lands in the operator's structured log instead.
_SUBSCRIBE_REJECT_LAG_LIMIT_FRAME: Final[bytes] = encode_struct_frame(
    SubscribeRejectedFrame(
        reason="lag_limit_exceeded",
        remediation=(
            "The daemon dropped consecutive sends to this subscriber; either "
            "drain the receive buffer faster, raise the subscriber's send "
            "buffer, or reconnect."
        ),
    )
)

# Map close-reason strings to their wire-emitted reject frames. A reason not
# in this map closes the subscriber socket silently (no diagnostic frame) --
# used for daemon shutdown, subscribe-ack send failure, internal faults
# (e.g. replay_db_error), and any pre-wire-protocol-commitment reject
# (envelope-shape / peer-cred / request-shape errors close before the ack,
# so there is no consumer state to diagnose against).
_TERMINAL_REJECT_FRAMES: Final[dict[str, bytes]] = {
    "lag_limit_exceeded": _SUBSCRIBE_REJECT_LAG_LIMIT_FRAME,
    "heartbeat_lag": _SUBSCRIBE_REJECT_LAG_LIMIT_FRAME,
    "replay_lag_limit_exceeded": _SUBSCRIBE_REJECT_LAG_LIMIT_FRAME,
}


# --- subscribe-frame validators --------------------------------------------


MAX_FILTERS_PER_SUBSCRIBER = 64
"""Hard cap on `filters` array length per subscribe envelope. Iterating
many filters on every fan-out is a CPU-DoS vector from a same-UID peer.
Sixty-four is generous for any realistic operator (one filter per repo
for a heavy user) while bounding worst-case match cost."""


def _validate_subscribe_filters(filters: object) -> list[str]:
    """Normalize and bounds-check the `filters` field of a subscribe frame.

    Returns the same list (or the default `["*"]`) if every element is a
    string matching FILTER_RE. Raises ValueError otherwise. The list is
    capped at MAX_FILTERS_PER_SUBSCRIBER entries.
    """
    if filters is None:
        return ["*"]
    if not isinstance(filters, list):
        raise ValueError("filters must be a JSON array of strings")
    if len(filters) > MAX_FILTERS_PER_SUBSCRIBER:
        raise ValueError(f"too many filters ({len(filters)} > {MAX_FILTERS_PER_SUBSCRIBER})")
    out: list[str] = []
    for f in filters:
        if not isinstance(f, str) or not FILTER_RE.match(f):
            raise ValueError(f"invalid filter element: {f!r}")
        out.append(f)
    if not out:
        return ["*"]
    return out


def _validate_subscribe_event_types(types: object) -> frozenset[str]:
    """Normalize and bounds-check the `event_types` field of a subscribe frame.

    Returns the set of recognized event-type strings. Raises ValueError
    when the field is the wrong shape or every element is unrecognized.
    """
    supported = event_types_supported()
    if types is None:
        return supported
    if not isinstance(types, list):
        raise ValueError("event_types must be a JSON array of strings")
    accepted = frozenset(t for t in types if isinstance(t, str) and t in supported)
    if not accepted:
        raise ValueError("event_types yielded zero recognized values")
    return accepted


def _validate_subscribe_envelope(envelope: object) -> None:
    """Reject any subscribe-frame envelope value other than None / ``"diffs"``.

    The envelope field is optional; absent or ``"diffs"`` selects the
    faithful per-event tail (the only delivery mode today). ``"upsert"``
    is RESERVED for a future Materialize-shaped ENVELOPE UPSERT projection
    (see ``docs/CONSUMER_API.md`` §2 for the additive-promise) and is
    rejected with a roadmap-aware message so a forward-looking consumer
    cannot silently subscribe to an unimplemented mode. Any other value is
    rejected as unknown. Returns ``None``; the caller discards the result
    (this is a validator, not a normaliser).
    """
    if envelope is None or envelope == "diffs":
        return
    if envelope == "upsert":
        raise ValueError(
            "envelope 'upsert' is reserved for a future delivery mode; the daemon does not implement it yet"
        )
    if not isinstance(envelope, str):
        raise ValueError("envelope must be a string")
    raise ValueError(f"unknown envelope: {envelope!r}")


def _validate_since_cursor(since: object) -> str | None:
    """Return the `since` ULID, or None when absent. Raises on bad shape.

    Pre-validating the cursor against the canonical 26-char Crockford-base32
    ULID alphabet here means a malformed value cannot reach the prepared
    statement as a partial LIKE-prefix and amplify into a slow scan over
    the events table. The cursor is compared with `>` on a primary-key-
    indexed column, so a well-formed cursor is an O(log n) seek.
    """
    if since is None:
        return None
    if not isinstance(since, str):
        raise ValueError("since must be a 26-char ULID string or null")
    if not ULID_RE.match(since):
        raise ValueError("since is not a well-formed ULID")
    return since


def _shutdown_close(sock: socket.socket) -> None:
    """Cleanly tear down ``sock`` from the server side without RST.

    Linux AF_UNIX SOCK_STREAM emits ``RST`` (peer recv() raises
    ``ECONNRESET``) when ``close()`` is called with unread bytes still
    in the kernel receive buffer. The subscribe-frame rejection paths
    routinely hit this: an oversize length-prefix rejection consumed 4
    bytes of the client's 16-byte send, leaving 12 unread. Closing
    immediately would let the asyncio client transport surface
    ``ConnectionResetError`` in its ``_read_ready__data_received``
    callback, which Python 3.14+ raises as a
    ``PytestUnraisableExceptionWarning``.

    Drain the kernel receive buffer first (non-blocking
    ``recv`` until ``EAGAIN``/EOF/limit), then ``shutdown(SHUT_WR)`` to
    send a clean ``FIN``, then ``close()``. The peer then sees EOF on
    its next read instead of a reset. The drain is bounded (32 KiB)
    so a malicious peer cannot stall the daemon by streaming data
    into a rejected subscriber's socket. All three calls suppress
    ``OSError`` for already-half-closed and double-close races.
    """
    _drain_limit = _REJECT_DRAIN_LIMIT_BYTES
    with contextlib.suppress(OSError):
        sock.setblocking(False)
        drained = 0
        while drained < _drain_limit:
            try:
                chunk = sock.recv(min(_REJECT_DRAIN_CHUNK_BYTES, _drain_limit - drained))
            except (BlockingIOError, InterruptedError):
                break
            if not chunk:
                break
            drained += len(chunk)
    with contextlib.suppress(OSError):
        sock.shutdown(socket.SHUT_WR)
    with contextlib.suppress(OSError):
        sock.close()


async def _recv_subscribe_frame(
    loop: asyncio.AbstractEventLoop,
    client_sock: socket.socket,
    peer_uid: int,
) -> dict[str, Any] | None:
    """Pull and parse the subscribe frame from a freshly-accepted client.

    Reads one length-prefix-framed JSON envelope directly off the raw
    non-blocking socket via ``loop.sock_recv`` — there is no
    intermediate ``asyncio.StreamReader`` or ``Transport``, so the
    one-shot handshake cannot leak a ``_SelectorSocketTransport`` if
    the daemon's event loop tears down before this coroutine finishes.

    Returns the parsed JSON object on success. Closes the socket and
    returns None on any of: receive timeout/ConnectionError, clean EOF,
    malformed JSON, or a non-object envelope. The caller treats None as
    "give up on this client".
    """
    from ._frame import read_frame_sock

    try:
        data = await asyncio.wait_for(read_frame_sock(loop, client_sock), timeout=_SUBSCRIBE_FRAME_TIMEOUT_SEC)
    except (TimeoutError, OSError, ConnectionError):
        _shutdown_close(client_sock)
        return None
    if data is None:
        _shutdown_close(client_sock)
        return None
    try:
        msg = json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        structured(logger, logging.WARNING, "subscribe_bad_json", peer=peer_uid)
        _shutdown_close(client_sock)
        return None
    if not isinstance(msg, dict):
        structured(logger, logging.WARNING, "subscribe_bad_envelope", peer=peer_uid)
        _shutdown_close(client_sock)
        return None
    return msg


def _validate_subscribe_token(token: object) -> str | None:
    """Return the supplied token string, or None when absent.

    Bounds-checks the length envelope; the caller does constant-time
    comparison against the credential-provisioned secret. Raises ValueError
    on bad type or out-of-range length.
    """
    if token is None:
        return None
    if not isinstance(token, str):
        raise ValueError("token must be a string or null")
    if not TOKEN_MIN_LEN <= len(token) <= TOKEN_MAX_LEN:
        raise ValueError(f"token length {len(token)} outside [{TOKEN_MIN_LEN}, {TOKEN_MAX_LEN}]")
    return token


# --- subscriber book-keeping ------------------------------------------------


class Subscriber:
    """One connected client's state."""

    __slots__ = (
        "_draining",
        "_tx_queue",
        "event_types",
        "filters",
        "lag_count",
        "pre_ack_buffer",
        "pre_ack_buffered_bytes",
        "remote_uid",
        "replay_watermark",
        "sock",
    )

    def __init__(
        self,
        sock: socket.socket,
        filters: list[str],
        event_types: frozenset[str],
        remote_uid: int,
    ) -> None:
        """Bind a freshly-accepted socket plus its subscription envelope.

        ``lag_count`` starts at zero; the fan-out path increments it on
        every EAGAIN and resets it on a successful send. The
        ``remote_uid`` is stored only for log correlation — the
        peer-credential check ran already at accept time.

        ``replay_watermark`` is None normally; set to the cursor snapshot
        seq (the daemon-assigned monotonic sequence) during a since-based
        replay and cleared on the first live delivery above the watermark
        (see ``_fan_out`` and ``_replay``).

        ``pre_ack_buffer`` is None outside the registration-to-ack
        window. While non-None it captures live frames that would
        otherwise be sent before the daemon emits ``subscribe_ack``;
        ``_read_subscribe`` drains the buffer after ack and any
        replay, then clears it back to None.
        """
        self.sock = sock
        self.filters = filters
        self.event_types = event_types
        self.lag_count = 0
        self.remote_uid = remote_uid
        self.replay_watermark: int | None = None
        self.pre_ack_buffer: list[bytes] | None = None
        self.pre_ack_buffered_bytes: int = 0
        # User-space outbound frame queue — the SINGLE source of truth for
        # everything this subscriber has been handed but the kernel has not
        # yet accepted. Each entry is ``(remaining_bytes, counts_as_delivered)``
        # where ``remaining_bytes`` is a memoryview over the unsent portion of
        # ONE wire frame (the head entry may be a mid-frame tail after a
        # partial write; every later entry is a whole frame). Byte and frame
        # gauges are DERIVED from this queue (``tx_buffered_bytes``), never
        # tracked in parallel cells — parallel counters drift on error paths.
        # ``_draining`` tracks whether the loop's writability callback
        # (``add_writer``) is armed.
        self._tx_queue: collections.deque[tuple[memoryview, bool]] = collections.deque()
        self._draining: bool = False

    def matches(self, owner: str, repo: str, event_type: str) -> bool:
        """Decide whether this subscriber wants the event.

        Match order: event_type membership first (cheap set test),
        then filter list. Filter syntax: ``*`` (catch-all),
        ``owner/repo`` (exact), ``owner/*`` (owner prefix). No regex,
        no glob — the broadcast hub validates filter strings at
        subscribe time with ``FILTER_RE``.
        """
        if event_type not in self.event_types:
            return False
        slug = f"{owner}/{repo}"
        for f in self.filters:
            if f == "*":
                return True
            if f == slug:
                return True
            if f.endswith("/*") and f[:-2] == owner:
                return True
        return False

    def tx_buffered_bytes(self) -> int:
        """Unsent outbound bytes, derived from the queue (never a parallel cell)."""
        return sum(len(view) for view, _ in self._tx_queue)

    def enqueue(self, blob: bytes, *, counts_as_delivered: bool) -> bool:
        """The ONE outbound-send API; return True iff fully sent synchronously.

        ``blob`` is already the fully encoded wire frame (4-byte prefix +
        payload). The socket is NON-BLOCKING: ``socket.sendall`` does NOT
        loop to completion on a non-blocking socket — on a partial write it
        sends a prefix, raises ``BlockingIOError`` (EAGAIN), and discards the
        sent-byte count, tearing the length-prefixed frame and permanently
        desyncing the subscriber's wire. So this uses the byte-count-returning
        ``socket.send`` only when the queue is empty (the CPython-transport
        inline fast path) and otherwise queues the whole frame behind the
        unsent remainder, completing it from the event loop's writability
        callback (``add_writer`` -> :meth:`_drain`). FIFO is structural — a
        frame enters the queue as one indivisible entry — so the wire byte
        stream stays contiguous and whole-frame-or-nothing: the receiver
        never observes interleaved or truncated frames except a clean EOF if
        the subscriber is dropped mid-buffer (indistinguishable from a peer
        crash, which any framed reader already tolerates).

        ``counts_as_delivered`` is True for EVENT frames only (fan-out,
        pre-ack drain, replay); control frames (heartbeat, subscribe_ack,
        subscribe_rejected) pass False and never move
        ``waitbus_broadcast_events_delivered_total``. Delivery is counted at
        kernel-accept — here for a synchronous full send, in :meth:`_drain`
        at that frame's flush completion otherwise — so the counter has ONE
        consistent boundary across every send site.

        Lag accounting is unchanged from the prior ``sendall`` contract: a
        full send resets ``lag_count`` to 0; an EAGAIN / partial write
        increments it (the caller drops the subscriber after LAG_LIMIT
        consecutive non-clean sends); ``BrokenPipeError`` /
        ``ConnectionResetError`` / any other ``OSError`` force-trips the
        counter to ``LAG_LIMIT`` for an immediate drop.
        """
        view = memoryview(blob)
        # Already mid-frame or backlogged: append behind the unsent tail.
        # The non-empty queue counts as one more delivery the subscriber
        # failed to keep up with — the same lag signal the old
        # consecutive-EAGAIN path produced. Re-arming here lets a transient
        # arm failure (no loop yet, non-registerable fd) self-heal on the
        # next send instead of stranding the queue until lag eviction; when
        # the watcher is already armed it is a no-op.
        if self._tx_queue:
            self._tx_queue.append((view, counts_as_delivered))
            self.lag_count += 1
            self._start_draining()
            return False
        try:
            n = self.sock.send(view)
        except BlockingIOError:
            # Kernel buffer full before any byte left: queue the whole frame.
            self._tx_queue.append((view, counts_as_delivered))
            self._start_draining()
            self.lag_count += 1
            return False
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.lag_count = LAG_LIMIT
            return False
        if n == len(view):
            self.lag_count = 0
            if counts_as_delivered:
                incr("waitbus_broadcast_events_delivered_total")
            return True
        # Partial write: queue the unsent tail and complete it on writability.
        self._tx_queue.append((view[n:], counts_as_delivered))
        self._start_draining()
        self.lag_count += 1
        return False

    def _start_draining(self) -> None:
        """Arm the event loop's writability callback to flush ``_tx_queue``.

        Best-effort and idempotent. Degrades gracefully where no event loop
        is running (pure synchronous unit tests that exercise only the lag
        accounting) or where the socket cannot be registered (``fileno`` < 0
        stub sockets): the remainder simply stays queued, which is a no-op
        without a loop to flush it.
        """
        if self._draining:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        try:
            fd = self.sock.fileno()
        except OSError:
            return
        if fd < 0:
            return
        try:
            loop.add_writer(fd, self._drain)
        except (OSError, ValueError):
            # fd is not a loop-registerable descriptor (e.g. a non-socket
            # stub fd in a unit test, or a fd the selector cannot poll).
            # Without a writability watcher the remainder cannot be flushed
            # here; the queued bytes and the already-incremented lag_count
            # stand, and the next pass evicts a saturated subscriber.
            return
        self._draining = True

    def _drain(self) -> None:
        """Flush queued frames when the event loop reports the socket writable.

        The ONE flush path. Sends the queue head-first, as much as the
        kernel accepts; each frame that completes is popped, and a
        ``counts_as_delivered`` frame is counted toward the delivered total
        at exactly that moment (the fan-out pass skipped it). When the queue
        empties the writability callback is disarmed and ``lag_count``
        resets (the subscriber caught up). A peer that died mid-drain
        discards the queue — the peer never received those frames, and a
        cleared queue zeroes the derived gauges with it — trips the lag
        counter to LAG_LIMIT, and disarms the callback; the next
        broadcast/heartbeat pass observes the saturated counter and evicts
        it via the standard ``_close_subscriber`` path.
        """
        while self._tx_queue:
            head_view, counts_as_delivered = self._tx_queue[0]
            try:
                n = self.sock.send(head_view)
            except BlockingIOError:
                return
            except (BrokenPipeError, ConnectionResetError, OSError):
                self.lag_count = LAG_LIMIT
                self._tx_queue.clear()
                self._stop_draining()
                return
            if n < len(head_view):
                # Mid-frame partial: keep the tail at the head and wait for
                # the next writability signal.
                self._tx_queue[0] = (head_view[n:], counts_as_delivered)
                return
            self._tx_queue.popleft()
            if counts_as_delivered:
                incr("waitbus_broadcast_events_delivered_total")
        self.lag_count = 0
        self._stop_draining()

    def _stop_draining(self) -> None:
        """Disarm the writability callback. Idempotent and close-safe.

        Must run BEFORE the socket is closed (see ``_close_subscriber``) so the
        loop deregisters a still-valid fd rather than raising on a stale one.
        """
        if not self._draining:
            return
        with contextlib.suppress(RuntimeError, OSError, ValueError):
            asyncio.get_running_loop().remove_writer(self.sock.fileno())
        self._draining = False


# --- daemon -----------------------------------------------------------------


class Broadcast:
    """Owns the listener socket, the doorbell, and every connected subscriber.

    One instance per ``waitbus-broadcast`` process. Drives an asyncio
    event loop with readers for listener accept and doorbell wake, plus
    one periodic task (heartbeat). On Linux a daemon thread pulls bytes
    off the doorbell listener and feeds them into an ``os.eventfd``; the
    asyncio loop registers the eventfd fd. On macOS the listener fd is
    the wake primitive directly. All state is in-memory; the SQLite
    events table is the source of truth and the cursor is reseeded from
    MAX(event_id) on startup so a restart never reissues already-broadcast
    frames.
    """

    def __init__(
        self,
        db_path: str | None = None,
        *,
        socket_path: str | None = None,
        doorbell_path: str | None = None,
    ) -> None:
        """Construct daemon state; bind no sockets until ``run`` is awaited.

        Looks up the optional broadcast token from systemd-creds at
        construction time so an operator rotating the token must
        restart the daemon to pick up the new value (fail-loud, no
        silent stale auth).

        ``db_path``, ``socket_path`` (the AF_UNIX listener subscribers connect
        to), and ``doorbell_path`` (the wake socket emitters ring) each default
        to their ``_paths`` factory evaluated at construction time, so a
        ``monkeypatch.setenv`` in tests applies to this daemon's resolved paths.
        ``_paths`` factories re-read env on every call (no lru_cache), so the
        per-test env override is honoured without a companion invalidation hook.
        Passing the paths EXPLICITLY lets a self-contained in-process caller (a
        demo, a test) bind a daemon to a temporary runtime dir without mutating
        the process-global ``WAITBUS_RUNTIME_DIR`` env -- the explicit-injection
        counterpart of ``db_path``. (Under systemd socket activation the listener
        fd is inherited and ``socket_path`` is not used for binding; it still
        names the path reported in status and unlinked on shutdown.)
        """
        self.db_path = db_path if db_path is not None else str(_paths.db_path())
        self.socket_path = socket_path if socket_path is not None else str(broadcast_socket())
        self.doorbell_path = doorbell_path if doorbell_path is not None else str(doorbell_socket())
        self.subscribers: dict[int, Subscriber] = {}  # fd -> Subscriber
        self._pending_subscribes: set[asyncio.Task[None]] = set()
        # Broadcast cursor: the daemon-assigned seq of the last row fanned out.
        # Seeded from MAX(seq) at
        # startup, advanced per broadcast pass; the daemon streams only rows
        # whose seq exceeds it.
        self.cursor: int = 0
        self.token: str | None = _lookup_token()
        self.uid: int = os.getuid()
        self.listener_sock: socket.socket | None = None
        self._doorbell: Doorbell | None = None
        self._doorbell_thread: threading.Thread | None = None
        self._doorbell_shutdown = threading.Event()
        self.owns_listener_path: bool = False
        self.started_at: float = time.time()
        self.stopping: bool = False
        _cfg = _config.get_config()
        self.heartbeat_sec: float = _cfg.heartbeat_sec
        self.metrics_snapshot_period_sec: float = _cfg.metrics_snapshot_period_sec
        self.metrics_port: int | None = _cfg.metrics_port
        self._metrics_server: MetricsServer | None = None
        # Public stop signal. ``run()`` awaits ``self.stop_event.wait()``;
        # callers (tests, host applications) trigger graceful exit by
        # awaiting ``self.stop()`` rather than cancelling the run task.
        # Cancellation-driven teardown leaves done-callbacks scheduled
        # on the event loop, which pytest-asyncio's per-test loop
        # destroys before they fire — the late ``__del__`` then raises
        # ``ResourceWarning`` in a later test under
        # ``PytestUnraisableExceptionWarning``. Explicit stop avoids the
        # window entirely.
        self.stop_event: asyncio.Event = asyncio.Event()

    # --- bootstrap ---------------------------------------------------------

    def _seed_cursor(self) -> None:
        """Initialise the broadcast cursor from MAX(seq) on the events table.

        The cursor is the daemon-assigned monotonic ``seq``, not the
        per-process ULID.
        A fresh start streams only events that arrive AFTER the daemon
        comes up; the historical backlog is reachable only via the
        explicit ``since=...`` replay on subscribe. This is intentional:
        a restarted daemon should not flood every subscriber with
        weeks of cached rows.
        """
        with _db.connect(self.db_path) as conn:
            row = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM events").fetchone()
            self.cursor = int(row[0])

    def _bind_listener(self) -> socket.socket:
        """Return the AF_UNIX SOCK_STREAM socket subscribers connect to.

        Prefers systemd socket activation (fd 3 inherited from the
        unit's `.socket`); falls back to a manual bind at
        ``broadcast_socket()`` when run outside systemd. Owning the
        bound path is tracked so ``_shutdown`` can unlink only what
        this process created — under socket activation systemd owns
        the path and must not be unlinked here.
        """
        # systemd socket activation: fd 3 is ours if LISTEN_FDS=1 + LISTEN_PID.
        if os.environ.get("LISTEN_FDS") == "1" and os.environ.get("LISTEN_PID") == str(os.getpid()):
            sock = socket.socket(fileno=3, family=socket.AF_UNIX, type=socket.SOCK_STREAM)
            sock.setblocking(False)
            structured(logger, logging.INFO, "listener_from_systemd", fd=3)
            return sock
        # Manual fallback.
        path = self.socket_path
        if os.path.exists(path):
            os.unlink(path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(path)
        os.chmod(path, 0o600)
        sock.listen(_LISTENER_BACKLOG)
        sock.setblocking(False)
        self.owns_listener_path = True
        structured(logger, logging.INFO, "listener_bound", path=path, mode="0600")
        return sock

    def _open_doorbell(self) -> Doorbell:
        """Open the doorbell listener at the canonical path.

        On Linux also creates an ``os.eventfd`` for kernel-coalesced
        wake delivery; the eventfd is the fd registered with the asyncio
        loop. On macOS the listener fd itself is the wake primitive.
        """
        path = Path(self.doorbell_path)
        doorbell = Doorbell.open(path)
        os.chmod(str(path), 0o600)
        structured(logger, logging.INFO, "doorbell_bound", path=str(path))
        return doorbell

    # --- broadcast pass ----------------------------------------------------

    def _current_max_snapshot(self) -> tuple[int, str | None]:
        """Return ``(max seq, its event_id)`` at this instant, ``(0, None)`` when empty.

        Called just before replay to capture a cursor snapshot. The ``seq`` is
        the internal replay/watermark bound (rows with ``seq`` at or below it
        are delivered via ``_replay``; rows above via ``_fan_out``, with the
        watermark dedup handling the overlap). The ``event_id`` is the public
        ULID echoed to the consumer as ``caught_up_at`` -- the wire cursor
        stays a ULID. Restricted to
        rows with a non-null ``event_id`` so ``caught_up_at`` is a real ULID
        and the bound matches the replay set (which excludes null-id rows).
        """
        with _db.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT seq, event_id FROM events WHERE event_id IS NOT NULL ORDER BY seq DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return (0, None)
        return (int(row[0]), row[1])

    def _broadcast_pass(self) -> None:
        """Pull newly-committed rows above the cursor and fan out."""
        with _db.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = list(_db.iter_events_above(conn, self.cursor, limit=SWEEP_LIMIT))
        if not rows:
            return
        for row in rows:
            frame = _row_to_frame(row)
            blob = _serialize(frame)
            incr("waitbus_broadcast_events_emitted_total")
            self._fan_out(row["seq"], frame.event_id, frame.owner, frame.repo, frame.event_type, blob)
            self.cursor = row["seq"]

    def _fan_out(self, seq: int, event_id: str, owner: str, repo: str, event_type: str, blob: bytes) -> None:
        """Send ``blob`` to every subscriber whose filters match.

        Skips frames at or below each subscriber's ``replay_watermark``
        (already delivered via replay). The watermark is the daemon-assigned
        ``seq``; the integer compare
        is O(1) and correct across producer processes. Clears the watermark
        on the first frame whose ``seq`` exceeds it. Drops EAGAIN-lagged
        subscribers after LAG_LIMIT failures.

        Updates the per-fan-out subscription-health gauges as a side
        effect: the matched-target count, the slowest single send in this
        pass, and the count of subscribers currently lagging (lag_count >
        0) but still under the drop threshold.
        """
        to_drop: list[int] = []
        matched = 0
        max_send_seconds = 0.0
        for fd, sub in self.subscribers.items():
            if not sub.matches(owner, repo, event_type):
                continue
            # Watermark dedup: frames at or below the watermark seq were
            # already delivered via replay. The first post-watermark frame
            # clears the gate so subsequent live frames flow through.
            if sub.replay_watermark is not None:
                if seq <= sub.replay_watermark:
                    continue
                sub.replay_watermark = None
            # Pre-ack gate: while the subscriber's ack handshake is still
            # in flight, buffer live frames instead of sending. The buffer
            # is drained by _read_subscribe once the ack lands. Bound
            # the buffer; overflow folds into the existing lag_limit_exceeded
            # drop path so consumers see a unified diagnostic.
            if sub.pre_ack_buffer is not None:
                if (
                    len(sub.pre_ack_buffer) >= PRE_ACK_BUFFER_FRAMES
                    or sub.pre_ack_buffered_bytes + len(blob) > PRE_ACK_BUFFER_BYTES
                ):
                    to_drop.append(fd)
                    continue
                sub.pre_ack_buffer.append(blob)
                sub.pre_ack_buffered_bytes += len(blob)
                matched += 1
                continue
            matched += 1
            t0 = time.monotonic()
            # Delivered accounting lives inside enqueue/_drain (the single
            # owner): a synchronous full send counts here and now, an
            # EAGAIN-queued frame counts at its flush completion.
            sub.enqueue(blob, counts_as_delivered=True)
            elapsed = time.monotonic() - t0
            BROADCAST_SEND_SECONDS.observe(elapsed)
            max_send_seconds = max(max_send_seconds, elapsed)
            if sub.lag_count >= LAG_LIMIT:
                to_drop.append(fd)
        BROADCAST_SUBSCRIPTION_COUNT.set(matched)
        BROADCAST_EMISSION_LATENCY_SECONDS.set(max_send_seconds)
        BROADCAST_STALE_SUBSCRIPTION_COUNT.set(sum(1 for s in self.subscribers.values() if 0 < s.lag_count < LAG_LIMIT))
        self._update_backlog_gauges()
        for fd in to_drop:
            self._close_subscriber(fd, reason="lag_limit_exceeded")

    def _update_backlog_gauges(self) -> None:
        """Refresh the aggregate backlog gauges from current subscriber state.

        Runs on the event loop (per fan-out pass and per heartbeat tick),
        so a quiescent daemon still reports drain progress. Aggregates
        only: per-subscriber label sets would be unbounded cardinality.
        """
        SUBSCRIBER_LAG_MAX.set(max((s.lag_count for s in self.subscribers.values()), default=0))
        SUBSCRIBER_TX_BUFFER_BYTES.set(sum(s.tx_buffered_bytes() for s in self.subscribers.values()))

    # --- subscribe handling ------------------------------------------------

    async def _handle_accept(self, loop: asyncio.AbstractEventLoop) -> None:
        """Accept one queued connection, enforce SO_PEERCRED, queue subscribe-read.

        Runs the peer-uid check before any data flows; mismatched
        peers are closed without a reply. The 1 MiB SO_SNDBUF bump
        is best-effort: a kernel that caps it (net.core.wmem_max)
        still serves the drop-after-N-EAGAIN contract.

        The subscribe-frame handshake reads directly off the raw
        non-blocking socket via ``loop.sock_recv`` (see
        ``_frame.read_frame_sock``) so the one-shot handshake never
        wraps the socket in an ``asyncio.create_connection`` transport —
        an earlier shape that leaked a ``_SelectorSocketTransport`` when
        the event loop's teardown raced the per-subscriber done-callback.
        """
        assert self.listener_sock is not None
        try:
            client_sock, _ = self.listener_sock.accept()
        except BlockingIOError:
            return
        client_sock.setblocking(False)
        # Kernel may cap below the request via net.core.wmem_max; the
        # call is best-effort. The default ~208 KB still serves the
        # drop-after-N-EAGAIN contract.
        with contextlib.suppress(OSError):
            client_sock.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_SNDBUF,
                PER_SUBSCRIBER_SNDBUF,
            )
        # Peer-credential check.
        peer = _peer_uid(client_sock)
        if peer is None or peer != self.uid:
            structured(logger, logging.WARNING, "peercred_mismatch", peer=peer, expected=self.uid)
            _shutdown_close(client_sock)
            return
        task = loop.create_task(self._read_subscribe(loop, client_sock, peer))
        self._pending_subscribes.add(task)
        task.add_done_callback(self._pending_subscribes.discard)

    async def _read_subscribe(
        self,
        loop: asyncio.AbstractEventLoop,
        client_sock: socket.socket,
        peer_uid: int,
    ) -> None:
        """Read the subscribe envelope, validate every field, register the subscriber.

        A 10-second receive timeout bounds the cost of a peer that
        connects and then never speaks. Every PRE-token and request-shape
        reject (recv timeout/OSError, clean pre-subscribe EOF, bad JSON,
        non-object envelope, bad filter/event_type/since) stays
        silent-EOF: operators debug via the daemon's structured logs, not
        a client-side error channel. The ONE exception is the post-peer-
        cred token failure: the peer is already proven same-UID by the
        accept-time SO_PEERCRED gate, so a single ``subscribe_rejected``
        frame (``reason: "token"``) is written best-effort before close
        so an honest operator gets a real auth error instead of a bare
        EOF. This covers both ``_check_subscribe_token`` False sub-paths
        (bad token length AND token mismatch); both are post-peer-cred.

        Watermark replay ordering (fixes missed-delivery race):
        1. Register subscriber in self.subscribers FIRST.
        2. Capture cursor_snapshot = MAX(event_id) at this moment.
        3. Set sub.replay_watermark = cursor_snapshot.
        4. Send replay rows WHERE event_id BETWEEN since AND snapshot.
        5. _fan_out skips frames <= watermark (covered by replay), clears
           watermark on the first frame > watermark (live delivery).

        The old "replay-then-register" shape let rows inserted between
        the SELECT snapshot and the dict insertion fall through both
        paths — missed forever for the new subscriber.
        """
        msg = await _recv_subscribe_frame(loop, client_sock, peer_uid)
        if msg is None:
            return
        # Wire-protocol version negotiation (connection-scoped, NATS/MCP
        # shape; data frames carry no per-frame version). A client MAY send
        # ``proto``; absence means v1 (the only version today). A present-
        # but-unsupported value gets one ``subscribe_rejected{reason:
        # "version"}`` frame before close, then the peer can negotiate down
        # against ``supported``.
        client_proto = msg.get("proto")
        # Python's ``1 == 1.0 == True`` would silently accept JSON ``1.0``
        # and ``true`` as v1; require a strict int (excluding bool, which
        # is an int subclass) so the wire contract -- documented as a
        # connection-scoped integer -- is enforced at the validator seam.
        if client_proto is not None and (
            not isinstance(client_proto, int) or isinstance(client_proto, bool) or client_proto != FRAME_PROTO_VERSION
        ):
            with contextlib.suppress(OSError, ConnectionError):
                await asyncio.wait_for(
                    loop.sock_sendall(client_sock, _SUBSCRIBE_REJECT_VERSION_FRAME),
                    timeout=2.0,
                )
            incr("waitbus_subscriber_rejected_total", reason="version")
            structured(logger, logging.WARNING, "subscribe_bad_proto", peer=peer_uid, proto=client_proto)
            _shutdown_close(client_sock)
            return
        if self.token is not None and not self._check_subscribe_token(msg.get("token"), peer_uid):
            # The peer cleared the accept-time SO_PEERCRED gate (proven
            # same-UID), so disclosing "your token is wrong" leaks
            # nothing to an unauthenticated surface. Write the single
            # reject frame best-effort and bounded (2s) so a slow/half-
            # dead peer cannot stall the accept loop, then close.
            with contextlib.suppress(OSError, ConnectionError):
                await asyncio.wait_for(
                    loop.sock_sendall(client_sock, _SUBSCRIBE_REJECT_TOKEN_FRAME),
                    timeout=2.0,
                )
            incr("waitbus_subscriber_rejected_total", reason="token")
            _shutdown_close(client_sock)
            return
        try:
            filters = _validate_subscribe_filters(msg.get("filters"))
            event_types = _validate_subscribe_event_types(msg.get("event_types"))
            since = _validate_since_cursor(msg.get("since"))
            # envelope is optional today; only ``diffs`` is implemented.
            # Reject any other value (including the reserved ``upsert``)
            # so a future consumer cannot silently opt in to an
            # unimplemented mode. The returned value is unused for now;
            # the validation IS the contract enforcement.
            _validate_subscribe_envelope(msg.get("envelope"))
        except ValueError as exc:
            structured(logger, logging.WARNING, "subscribe_bad_field", peer=peer_uid, error=str(exc))
            _shutdown_close(client_sock)
            return
        sub = Subscriber(client_sock, filters, event_types, peer_uid)
        # Reserve a pre-ack buffer BEFORE registration so the _fan_out
        # pass that runs between the dict insertion and the ack send
        # captures live frames into the buffer instead of writing them to
        # the wire ahead of the ack. The buffer is drained after the ack
        # (and after any since-replay) below, then cleared to None so
        # subsequent _fan_out writes go direct.
        sub.pre_ack_buffer = []
        sub.pre_ack_buffered_bytes = 0
        # Register so live deliveries via _fan_out can reach this
        # subscriber immediately. Without prior registration the window
        # between the SELECT snapshot and dict insertion loses all rows
        # that _broadcast_pass sweeps during that gap.
        self.subscribers[client_sock.fileno()] = sub
        SUBSCRIBER_COUNT.inc()
        incr("waitbus_subscriber_opened_total")
        until_seq: int | None = None
        if since is not None:
            # Capture the high-water mark before replay. Rows above the
            # snapshot arrive via _fan_out; the watermark dedup prevents
            # double-delivery of any row that replay and _fan_out both
            # see in the overlap window. The internal watermark/replay bound
            # is the daemon-assigned seq; the wire ``caught_up_at`` echoed to
            # the consumer is the corresponding public ULID.
            until_seq, caught_up_at = self._current_max_snapshot()
            sub.replay_watermark = until_seq
        else:
            # Null on no-since subscribes: caught_up_at is a positional
            # dedup cursor for replay, not a temporal "ack-then-live"
            # barrier. A consumer with no since cursor has nothing to
            # dedup, so the watermark is undefined; absence is the right
            # signal. This aligns the wire with the SubscribeAckFrame
            # docstring and the consumer-facing contract in
            # CONSUMER_API.md.
            caught_up_at = None
        structured(
            logger,
            logging.INFO,
            "subscribed",
            fd=client_sock.fileno(),
            filters=filters,
            event_types=sorted(event_types),
            since=since,
            peer=peer_uid,
        )
        # Positive subscribe acknowledgement (wire protocol v1), emitted
        # FIRST on the wire after envelope validation. Replay frames (if
        # ``since`` was supplied) follow the ack; live frames that landed
        # during the registration->ack window were captured by _fan_out
        # into ``sub.pre_ack_buffer`` and are drained after the replay
        # tail. Consumers classify the post-ack stream by event_id:
        # ``event_id <= caught_up_at`` is replay; ``> caught_up_at`` is
        # live. Best-effort
        # + bounded so a peer that vanished mid-handshake is dropped rather
        # than stalling the accept loop.
        ack = encode_struct_frame(
            SubscribeAckFrame(
                proto=FRAME_PROTO_VERSION,
                caught_up_at=caught_up_at,
                heartbeat_sec=int(self.heartbeat_sec),
                max_frame_bytes=MAX_FRAME_BYTES,
            )
        )
        try:
            await asyncio.wait_for(loop.sock_sendall(client_sock, ack), timeout=2.0)
        except (OSError, ConnectionError, TimeoutError):
            self._close_subscriber(client_sock.fileno(), reason="subscribe_ack_send_failed")
            return

        # Replay (if requested) AFTER the ack lands on the wire. _replay
        # calls sub.send directly, bypassing the pre_ack_buffer gate; replay
        # frames land between ack and the buffer drain on the wire and are
        # classified as replay by the consumer's caught_up_at dedup.
        if since is not None:
            try:
                replay_alive = self._replay(sub, since, until_seq=until_seq)
            except sqlite3.Error as exc:
                # A DB fault mid-replay must not crash the _read_subscribe
                # task and leak the subscriber. Close it (map removal + count
                # + silent wire close — an internal fault is not a consumer-
                # actionable reject reason) and bail.
                structured(
                    logger,
                    logging.ERROR,
                    "subscriber_replay_db_error",
                    fd=client_sock.fileno(),
                    peer=peer_uid,
                    error=str(exc),
                )
                incr("waitbus_db_error_total", path="broadcast_replay", source="broadcast")
                self._close_subscriber(client_sock.fileno(), reason="replay_db_error")
                return
            if not replay_alive:
                # _replay hit the lag limit, emitted the reject frame, and
                # already removed the subscriber from the map.
                return

        # Drain the pre-ack buffer onto the wire after replay. Use sub.send
        # so EAGAIN/BrokenPipe route through the existing lag-count semantics;
        # a drain that saturates the lag limit drops the subscriber via the
        # standard reject path (lag_limit_exceeded reject frame + close).
        # The buffer's contents are necessarily ``event_id > caught_up_at``
        # (captured strictly after cursor_snapshot was assigned) so no extra
        # dedup is needed on drain.
        buffered = sub.pre_ack_buffer
        sub.pre_ack_buffer = None
        sub.pre_ack_buffered_bytes = 0
        if buffered:
            for buffered_blob in buffered:
                sub.enqueue(buffered_blob, counts_as_delivered=True)
                if sub.lag_count >= LAG_LIMIT:
                    self._close_subscriber(client_sock.fileno(), reason="lag_limit_exceeded")
                    return

    def _check_subscribe_token(self, raw_token: object, peer_uid: int) -> bool:
        """Constant-time compare against the credential-provisioned token.

        Returns True iff the frame's token field has a well-formed length
        AND matches the daemon's configured token. The caller closes the
        client connection on False.
        """
        assert self.token is not None
        try:
            supplied = _validate_subscribe_token(raw_token)
        except ValueError as exc:
            structured(logger, logging.WARNING, "subscribe_bad_token", peer=peer_uid, error=str(exc))
            return False
        if supplied is None or not hmac.compare_digest(supplied, self.token):
            structured(logger, logging.WARNING, "subscribe_token_mismatch", peer=peer_uid)
            return False
        return True

    def _replay(self, sub: Subscriber, since: str, until_seq: int | None = None) -> bool:
        """Stream rows after the public ULID ``since`` cursor, up to ``until_seq``.

        ``since`` is the public ULID cursor the consumer supplied; it is
        translated to its exact internal ``seq`` lower bound via
        :func:`_db.seq_for_event_id` (an exact lookup, not a lexicographic
        ULID compare — this is what makes cross-process request/reply replay
        sound). ``until_seq`` is the
        seq snapshot captured just before replay; it bounds the replay set so
        rows arriving AFTER the snapshot are left for ``_fan_out``. The
        watermark dedup in ``_fan_out`` handles any overlap.

        Returns ``True`` if the subscriber survived replay, ``False`` if it
        hit the lag limit and was evicted. On eviction this routes through
        ``_close_subscriber`` (capturing the fd BEFORE close) so the map
        removal, ``SUBSCRIBER_COUNT`` decrement, and ``lag_limit_exceeded``
        wire diagnostic all fire exactly once — matching the live fan-out
        path. The caller must not touch ``sub`` after a ``False`` return.
        """
        with _db.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            since_seq = _db.seq_for_event_id(conn, since)
            rows = list(_db.iter_events_above(conn, since_seq, until_seq=until_seq, limit=REPLAY_LIMIT))
        for row in rows:
            if not sub.matches(row["owner"], row["repo"], row["event_type"]):
                continue
            frame = _row_to_frame(row)
            sub.enqueue(_serialize(frame), counts_as_delivered=True)
            WATERMARK_REPLAY_EVENTS_TOTAL.inc()
            if sub.lag_count >= LAG_LIMIT:
                # Capture fd BEFORE close so the close key is the registered
                # fd, not the post-close -1. _close_subscriber pops the map
                # entry, decrements the count once, and emits the reject frame.
                self._close_subscriber(sub.sock.fileno(), reason="replay_lag_limit_exceeded")
                return False
        return True

    def _close_subscriber(self, fd: int, reason: str) -> None:
        """Remove the subscriber, close its socket, log the reason.

        Idempotent: a double-close (e.g., lag-limit then shutdown)
        is a no-op rather than a KeyError. The reason string lands
        in the structured log so operators can correlate disconnects
        with lag-counter behaviour over time.

        For reasons mapped in ``_TERMINAL_REJECT_FRAMES`` the daemon
        emits the corresponding ``subscribe_rejected`` frame best-effort
        before closing — consumers see a wire-level diagnostic instead
        of a silent disconnect — but ONLY when the tx queue is empty, i.e.
        the wire sits at a frame boundary. With buffered bytes the reject
        would land mid-frame and corrupt the stream; the subscriber gets a
        clean EOF instead, the contract's named legitimate outcome (NATS
        and Redis likewise skip the goodbye entirely on write-path
        eviction). The single non-blocking ``send`` may itself be cut
        short when the kernel buffer is nearly full; since the socket
        closes immediately after, a truncated reject is indistinguishable
        from the mid-buffer-drop EOF every framed reader already tolerates.
        """
        sub = self.subscribers.pop(fd, None)
        if sub is None:
            return
        SUBSCRIBER_COUNT.dec()
        incr("waitbus_subscriber_closed_total")
        incr("waitbus_subscriber_evicted_total", reason=reason)
        # Disarm any armed writability callback BEFORE closing the socket so
        # the event loop deregisters a still-valid fd; closing first would
        # leave a writer registered against a stale/recycled descriptor.
        sub._stop_draining()
        reject_frame = _TERMINAL_REJECT_FRAMES.get(reason)
        if reject_frame is not None and not sub._tx_queue:
            with contextlib.suppress(BlockingIOError, OSError):
                sub.sock.send(reject_frame)
            # The rejected counter is labeled by the WIRE-level reject reason
            # (token / version / lag_limit_exceeded) — every reason in
            # _TERMINAL_REJECT_FRAMES emits the lag-limit wire frame, so the
            # internal trigger (heartbeat vs replay vs fan-out) stays in
            # subscriber_evicted_total above, not here.
            incr("waitbus_subscriber_rejected_total", reason="lag_limit_exceeded")
        # Unsent queued frames are discarded with the connection: the
        # receiver-visible contract is whole-frame-or-clean-EOF on a
        # mid-buffer drop (see ``Subscriber.enqueue``), and nothing may
        # retain a tx queue for a subscriber that has left the map. The
        # cleared queue zeroes the derived byte/frame gauges with it.
        sub._tx_queue.clear()
        with contextlib.suppress(OSError):
            sub.sock.close()
        structured(logger, logging.INFO, "subscriber_closed", fd=fd, reason=reason)

    # --- heartbeat ---------------------------------------------------------

    async def _metrics_snapshot_loop(self) -> None:
        """Emit a structured ``metrics_snapshot`` log line every period seconds.

        The line carries the current prometheus_client registry contents in
        JSON form (see :func:`waitbus._metrics.snapshot`). It is the
        channel the in-tree stress and soak harnesses use to read per-tick
        metric state from a subprocess daemon without depending on the
        optional HTTP ``/metrics`` endpoint (off by default; see
        :mod:`waitbus._metrics_http`). This workstation-local equivalent
        ships unconditionally because the cost is one JSON line per period
        and the harness side already tails the daemon log.
        """
        while not self.stopping:
            await asyncio.sleep(self.metrics_snapshot_period_sec)
            if self.stopping:
                return
            structured(logger, logging.INFO, "metrics_snapshot", families=_metrics_snapshot())

    async def _heartbeat_loop(self) -> None:
        """Send a ``daemon_heartbeat`` frame to every subscriber every ``self.heartbeat_sec`` seconds.

        Heartbeats ignore subscriber filters — liveness is global, not
        per-stream. A heartbeat is a control frame and carries NO event
        identity: a subscriber's resume cursor must never advance on a
        heartbeat. ``ts`` + ``uptime_sec`` convey liveness.
        """
        while not self.stopping:
            await asyncio.sleep(self.heartbeat_sec)
            if self.stopping:
                return
            try:
                blob = encode_struct_frame(
                    HeartbeatFrame(
                        ts=int(time.time()),
                        uptime_sec=int(time.time() - self.started_at),
                    )
                )
            except FrameTooLargeError:
                # Heartbeat should never be oversize; log and skip.
                structured(logger, logging.WARNING, "heartbeat_oversize")
                continue
            # Send to every subscriber regardless of filter — liveness is global.
            to_drop: list[int] = []
            for fd, sub in self.subscribers.items():
                sub.enqueue(blob, counts_as_delivered=False)
                if sub.lag_count >= LAG_LIMIT:
                    to_drop.append(fd)
            for fd in to_drop:
                self._close_subscriber(fd, reason="heartbeat_lag")
            # Refresh the backlog gauges on every tick so a quiescent
            # daemon (no fan-out passes) still reports drain progress.
            self._update_backlog_gauges()

    # --- doorbell accept loop (Linux only) ---------------------------------

    def _doorbell_accept_loop(self) -> None:
        """Pull bytes off the SOCK_STREAM listener and feed the eventfd.

        Runs in a daemon thread on Linux. Uses a ``selectors.DefaultSelector``
        with a 0.5-second timeout so the loop exits promptly when
        ``_doorbell_shutdown`` is set during daemon shutdown. Each accepted
        connection is consumed by ``Doorbell.accept_one``; the byte forwarded
        to the eventfd makes the asyncio loop's ``add_reader`` callback fire.
        Multiple rings arriving between two eventfd reads increment the same
        64-bit counter — no ring is lost.
        """
        assert self._doorbell is not None
        sel = selectors.DefaultSelector()
        sel.register(self._doorbell.listener_fd, selectors.EVENT_READ)
        try:
            while not self._doorbell_shutdown.is_set():
                events = sel.select(timeout=0.5)
                if not events:
                    continue
                # Drain all pending accepts to maximise coalescing before the
                # eventfd read in the asyncio callback fires.
                while self._doorbell.accept_one():
                    pass
        finally:
            sel.close()

    # --- lifecycle ---------------------------------------------------------

    def _shutdown(self) -> None:
        """Stop accepting, close subscribers cleanly, unlink owned socket paths.

        Idempotent via the ``self.stopping`` guard so a SIGINT during
        an active SIGTERM-driven shutdown is a no-op. Only paths
        bound manually by this daemon (not socket-activated) are
        unlinked — systemd owns its own socket path lifecycle.
        """
        if self.stopping:
            return
        self.stopping = True
        _sd_notify(b"STOPPING=1\n")
        structured(logger, logging.INFO, "shutdown_begin", subscribers=len(self.subscribers))
        # Drain in-flight broadcasts one last time before tearing down
        # subscriber sockets. Rows committed to SQLite just before
        # SIGTERM (after the last doorbell ping was processed) would
        # otherwise be lost. Suppressing any error keeps shutdown
        # robust against a misbehaving DB or wedged subscriber.
        with contextlib.suppress(Exception):
            self._broadcast_pass()
        for fd in list(self.subscribers):
            self._close_subscriber(fd, reason="shutdown")
        if self.listener_sock is not None:
            with contextlib.suppress(OSError):
                self.listener_sock.close()
        if self.owns_listener_path:
            with contextlib.suppress(OSError):
                os.unlink(self.socket_path)
        # Signal the doorbell accept-thread to stop (Linux), then close
        # the Doorbell which unlinks the socket path.
        self._doorbell_shutdown.set()
        if self._doorbell is not None:
            with contextlib.suppress(OSError):
                self._doorbell.close()

    @staticmethod
    def _open_death_watch() -> int | None:
        """Return the inherited spawner-death pipe fd, or ``None``.

        Bench/soak only. ``spawn_waitbus_daemon`` (``benchmarks/_harness.py``)
        passes the read end of a pipe down via ``pass_fds`` and names it in
        ``WAITBUS_DEATH_FD``; the harness keeps the write end. PEP 446 makes
        inherited fds non-inheritable in the child by default, so we mark the
        fd non-inheritable here too -- the daemon's own future children must
        not keep the watch alive by holding an open copy of the read end.

        Production (systemd) never sets the env var, so this returns ``None``
        and ``run()`` registers no death watch.
        """
        raw = os.environ.get("WAITBUS_DEATH_FD")
        if not raw:
            return None
        fd = int(raw)
        os.set_inheritable(fd, False)
        return fd

    async def stop(self) -> None:
        """Trigger graceful shutdown of the daemon from another task.

        Sets ``self.stop_event`` so ``run()`` exits its
        ``await self.stop_event.wait()`` and falls into its ``finally``
        block, which cancels in-flight subscribe tasks, runs the final
        broadcast pass, closes every subscriber socket, and unlinks the
        listener path. Callers that want to wait for shutdown to
        complete should ``await``-join the task that hosts ``run()``
        after calling ``stop()``.

        This method exists primarily for tests: production code drives
        shutdown via SIGTERM / SIGINT, which the ``run()`` body wires to
        the same event. Tests that cancel the ``run()`` task instead of
        calling ``stop()`` leave ``_handle_accept`` done-callbacks
        scheduled on the per-test event loop; pytest-asyncio reclaims
        the loop before those callbacks fire and the late ``__del__``
        raises ``ResourceWarning`` in a later test under the
        ``PytestUnraisableExceptionWarning`` gate.
        """
        self.stop_event.set()

    def _start_metrics_server(self) -> None:
        """Start the optional loopback Prometheus scrape endpoint.

        No-op unless ``metrics_port`` is configured. Loopback-only by
        construction; runs on a daemon thread so the event loop is
        untouched. A busy or unbindable port must not take the daemon
        down -- the scrape endpoint is an optional observability
        surface, not a load-bearing one -- so a bind ``OSError`` logs a
        structured ``metrics_bind_failed`` warning and the daemon runs
        without metrics.
        """
        if self.metrics_port is None:
            return
        self._metrics_server = MetricsServer(self.metrics_port)
        try:
            self._metrics_server.start()
        except OSError as exc:
            self._metrics_server = None
            structured(
                logger,
                logging.WARNING,
                "metrics_bind_failed",
                port=self.metrics_port,
                error=str(exc),
            )
        else:
            structured(
                logger,
                logging.INFO,
                "metrics_listening",
                host=self._metrics_server.host,
                port=self._metrics_server.port,
            )

    async def run(self, *, install_signal_handlers: bool = True) -> int:
        """Main event loop: bind sockets, register readers, await stop signal.

        Returns 0 after a clean shutdown for systemd's Type=notify
        unit. SIGTERM and SIGINT both trigger graceful exit;
        SIGKILL bypasses cleanup, in which case socket-activation
        paths are reclaimed by systemd and the manual-bind paths
        are unlinked on the next ``ensure_state_dirs`` cycle.

        ``install_signal_handlers=False`` leaves SIGINT/SIGTERM alone
        for embedders (the ``waitbus serve`` supervisor) that own the
        process-wide handlers and route their own stop event into
        ``stop()``; the standalone ``broadcast serve`` path keeps the
        default and installs its own.
        """
        loop = asyncio.get_running_loop()
        # Bootstrap. Under systemd socket activation the broadcast service
        # can start before the listener has ever processed a webhook, so
        # we cannot assume the events table exists; ensure_schema is
        # idempotent and races safely against the listener's own call.
        ensure_schema(Path(self.db_path))
        self._seed_cursor()
        self.listener_sock = self._bind_listener()
        self._doorbell = self._open_doorbell()

        # Doorbell handler — platform-dispatched.
        #
        # Linux: an accept-thread pulls bytes off the listener and feeds
        # them into the eventfd (Doorbell.accept_one). The asyncio loop
        # registers the eventfd fd and calls _on_doorbell_wake when the
        # counter goes non-zero. Multiple rings coalesce into one counter
        # value; drain() reads and resets it atomically.
        #
        # macOS: no eventfd available. The asyncio loop registers the
        # listener fd directly; on readable the callback accepts all
        # pending connections inline (Doorbell.accept_one) then runs
        # the broadcast pass.

        def _run_broadcast_pass() -> None:
            """Run one broadcast pass; log errors without crashing the daemon.

            Runs directly in the asyncio reader callback so any uncaught
            exception would abort the event loop. Fault-isolating here
            means the next doorbell ping retries the broadcast pass.
            """
            try:
                self._broadcast_pass()
            except sqlite3.Error as exc:
                structured(logger, logging.ERROR, "broadcast_pass_db_error", error=str(exc))
            except Exception as exc:
                structured(logger, logging.ERROR, "broadcast_pass_error", error=str(exc), error_type=type(exc).__name__)

        def _on_doorbell_wake() -> None:
            """Linux event-loop callback: eventfd became readable."""
            assert self._doorbell is not None
            self._doorbell.drain()
            _run_broadcast_pass()

        def _on_doorbell_wake_macos() -> None:
            """macOS event-loop callback: listener fd became readable."""
            assert self._doorbell is not None
            while self._doorbell.accept_one():
                pass
            _run_broadcast_pass()

        if _IS_LINUX:
            # Spawn accept-thread that funnels listener bytes into the eventfd.
            self._doorbell_shutdown.clear()
            self._doorbell_thread = threading.Thread(
                target=self._doorbell_accept_loop,
                daemon=True,
            )
            self._doorbell_thread.start()
            loop.add_reader(self._doorbell.fd, _on_doorbell_wake)
        else:
            loop.add_reader(self._doorbell.fd, _on_doorbell_wake_macos)

        def _on_accept() -> None:
            task = asyncio.create_task(self._handle_accept(loop))
            self._pending_subscribes.add(task)
            task.add_done_callback(self._pending_subscribes.discard)

        loop.add_reader(self.listener_sock.fileno(), _on_accept)

        # Signal handlers wire SIGTERM/SIGINT to the public stop event.
        # ``self.stop_event`` is also reachable by tests calling
        # ``await daemon.stop()`` for deterministic shutdown.
        if install_signal_handlers:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.stop_event.set)

        # Bench/soak orphan-leak guard. When the harness
        # passes the read end of a pipe via ``WAITBUS_DEATH_FD``, watch it the
        # same way the doorbell fd is watched: the harness holds the write end
        # for the daemon's whole lifetime, so its death by ANY means (including
        # SIGKILL) closes that write end and our read end becomes readable at
        # EOF. The reader sets ``stop_event``, routing through the graceful
        # unlink path in the ``finally`` below. Env-gated: with no env var the
        # systemd-managed production daemon registers no reader and is
        # unaffected.
        death_fd = self._open_death_watch()
        if death_fd is not None:
            loop.add_reader(death_fd, self.stop_event.set)

        heartbeat = asyncio.create_task(self._heartbeat_loop())
        metrics_snapshot_task = asyncio.create_task(self._metrics_snapshot_loop())

        self._start_metrics_server()

        _sd_notify(b"READY=1\nSTATUS=accepting subscribers\n")
        structured(
            logger,
            logging.INFO,
            "ready",
            db=self.db_path,
            listener=self.socket_path,
            doorbell=self.doorbell_path,
            token_configured=self.token is not None,
        )

        try:
            await self.stop_event.wait()
        finally:
            heartbeat.cancel()
            metrics_snapshot_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            with contextlib.suppress(asyncio.CancelledError):
                await metrics_snapshot_task
            # Cancel any in-flight subscribe-read tasks before shutdown so
            # they do not outlive the daemon's socket cleanup.
            for t in list(self._pending_subscribes):
                t.cancel()
            if self._pending_subscribes:
                await asyncio.gather(*self._pending_subscribes, return_exceptions=True)
            # Deregister the loop readers BEFORE _shutdown() closes their
            # fds: under the serve supervisor's shared long-lived loop a
            # closed-but-registered fd leaves a stale selector entry that a
            # recycled descriptor number can collide with.
            with contextlib.suppress(OSError, ValueError, RuntimeError):
                loop.remove_reader(self.listener_sock.fileno())
            if self._doorbell is not None:
                with contextlib.suppress(OSError, ValueError, RuntimeError):
                    loop.remove_reader(self._doorbell.fd)
            if death_fd is not None:
                loop.remove_reader(death_fd)
                os.close(death_fd)
            if self._metrics_server is not None:
                self._metrics_server.stop()
                self._metrics_server = None
            self._shutdown()
        return 0


# --- entry point ------------------------------------------------------------


def main() -> int:
    """Entry point for the ``waitbus broadcast serve`` sub-command.

    Configures stderr structured logging, ensures the state
    directories exist (so a fresh systemd boot doesn't race on a
    missing parent dir for the doorbell path), then hands control to
    ``Broadcast.run``. KeyboardInterrupt (Ctrl-C in a foreground run)
    returns 0 so the operator's terminal doesn't show a Python
    traceback for a deliberate stop.

    Configuration is environment-driven via ``WaitbusConfig``. The only CLI sugar the
    sub-command accepts is ``--metrics-port``, which sets
    ``WAITBUS_METRICS_PORT`` before the cached config first loads; the
    env var remains the canonical path for systemd units.
    """
    cfg = _config.get_config()
    logging.basicConfig(
        level=cfg.log_level,
        format="%(message)s",
        stream=sys.stderr,
    )
    ensure_state_dirs()
    # Discover entry-point plugin sources before any subscriber can connect.
    # Validates each plugin's SourceSpec, verifies PEP 740 attestation against
    # the TOFU allowlist, and registers it in the process-singleton registry
    # that the subscriber filter (event_types_supported) and EventInsert
    # validator (is_known_source) consult. Idempotent + thread-safe; the
    # corresponding call in the listener daemon shares the same registry.
    # A typed PluginShadowError / PluginVersionMismatchError / similar from
    # plugin policy failure is intentionally allowed to propagate and abort
    # daemon startup -- per the registry's "fail-fast on policy violations"
    # contract.
    from .sources._registry import discover_plugins_once

    discover_plugins_once()
    daemon = Broadcast()
    try:
        return asyncio.run(daemon.run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
