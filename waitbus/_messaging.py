"""Public addressed-messaging SDK: ``request`` / ``respond``.

Request-reply layered on the broadcast bus via the agent-message addressing
facet (``msg_to`` / ``msg_from`` / ``msg_correlation_id`` / ``msg_reply_to``) --
the canonical Enterprise-Integration *Correlation Identifier* + *Return Address*
composition that NATS, RabbitMQ, MQTT 5, and AMQP all realize, here over a local
fan-out bus with NO server-side routing. It composes the existing public
:func:`waitbus.emit` (producer) and :func:`waitbus.wait_for`
(consumer) primitives; it owns no transport, no callback registry, and no state.

Identity is a SELF-ASSERTED agent name -- an address, not a credential -- under
the same-UID trust model: the kernel UID boundary is the trust boundary, exactly
as MCP's STDIO transport and the Akka / Erlang actor runtimes treat local names.
There is no PKI / CA / OAuth here; a same-UID peer that could spoof a
name can already read every peer's socket, keys, and memory, so cryptographic
agent-auth would raise no ceiling the UID boundary has not already set.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec

from . import _db, _ulid
from ._broadcast_sub import BroadcastConnectionError, FrameDecision, await_predicate, open_subscriber
from ._emit import EmitResult, emit
from ._frame import EventFrame
from ._types import EventInsert

__all__ = ("request", "respond")

_AGENT_SOURCE = "agent"
_AGENT_MESSAGE = "agent_message"
# Agent messages are not GitHub-bound, but owner/repo are required NOT NULL
# columns (a CI-era constraint). Addressed messages carry these synthetic labels
# -- the same shape the alertmanager source uses for its non-GitHub rows. The
# routing lives entirely in the msg_* addressing fields, never in owner/repo.
_SYNTHETIC_OWNER = "local"
_SYNTHETIC_REPO = "agents"


def _emit_agent_message(
    *,
    delivery_id: str,
    to: str,
    sender: str,
    correlation_id: str,
    reply_to: str | None,
    body: str,
    thread: str | None = None,
    db_path: Path | None = None,
    doorbell_path: Path | None = None,
) -> EmitResult:
    """Build and emit the shared ``agent_message`` envelope.

    ``request()`` and ``respond()`` MUST stay in lockstep on this envelope; the
    only per-call differences are the ``delivery_id`` verb, the ``to`` / ``sender``
    direction, and the ``reply_to`` source. The message content rides ``msg_body``
    (the lean wire frame drops ``payload_json``, so the body could not otherwise
    reach the recipient); ``payload_json`` is an unused ``NOT NULL`` sentinel.
    """
    return emit(
        EventInsert(
            delivery_id=delivery_id,
            source=_AGENT_SOURCE,
            event_type=_AGENT_MESSAGE,
            owner=_SYNTHETIC_OWNER,
            repo=_SYNTHETIC_REPO,
            received_at=time.time_ns(),
            payload_json="{}",
            ingest_method="api",
            msg_to=to,
            msg_from=sender,
            msg_reply_to=reply_to,
            msg_correlation_id=correlation_id,
            msg_thread=thread,
            msg_body=body,
        ),
        db_path=db_path,
        doorbell_path=doorbell_path,
    )


def _reply_decide(
    correlation_id: str, sender: str, captured: list[dict[str, Any]]
) -> Callable[[dict[str, Any]], FrameDecision]:
    """Build the reply-matching decide closure for :func:`_await_reply`.

    Matches this request's reply either as a normal agent frame (by
    ``correlation_id`` + recipient, the sole correctness invariant) OR as an
    oversize reply that arrived as a fields-less ``truncated`` stub carrying the
    same ``correlation_id`` (then re-fetched by ``event_id``). Matching the
    correlation id directly off the decoded frame -- rather than building a
    string ``fields.msg_correlation_id=...`` match-spec -- keeps the value out
    of the predicate grammar entirely.
    """

    def _decide(frame: dict[str, Any]) -> FrameDecision:
        fields = frame.get("fields")
        if isinstance(fields, dict):
            # AND-compose both predicates; do not drop either.
            # The correlation_id is a per-request unique ULID and is the
            # primary match, but the msg_to == sender check ensures the frame
            # is addressed back to THIS requester -- so a reply meant for a
            # different requester (a reused or forged correlation_id at the
            # same-UID trust boundary) is not mis-captured. See the inter-agent
            # confidentiality note in SECURITY.md for the same-UID forge model.
            if fields.get("msg_correlation_id") == correlation_id and fields.get("msg_to") == sender:
                captured.append(frame)
                return FrameDecision.MATCHED
            return FrameDecision.CONTINUE
        # An oversize reply is dropped to a fields-less truncated stub; it carries
        # the correlation id so it is still matchable (the body is re-fetched).
        if frame.get("kind") == "truncated" and frame.get("correlation_id") == correlation_id:
            captured.append(frame)
            return FrameDecision.MATCHED
        return FrameDecision.CONTINUE

    return _decide


def _refetch_reply(event_id: str, db_path: Path | None) -> EventFrame | None:
    """Re-fetch an oversize reply's full row by ``event_id`` and project it.

    The wire truncates a body beyond ``MAX_FRAME_BYTES`` to a stub, but the full
    body is durable in the event store -- the degenerate Claim-Check, since the
    store is waitbus's own SQLite. Reuses ``broadcast._row_to_frame``: the same
    row->frame projection the daemon replay, coalescing, and CLI paths use.
    """
    from ._paths import db_path as resolve_db_path  # lazy: avoid the paths import on the hot path
    from .broadcast import _row_to_frame  # the shared row->frame projection

    path = db_path if db_path is not None else resolve_db_path()
    with _db.connect(path, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        row = _db.fetch_event_by_id(conn, event_id)
    return _row_to_frame(row) if row is not None else None


def _await_reply(
    *,
    correlation_id: str,
    sender: str,
    since: str,
    timeout: float | None,
    socket_path: str | None,
    token: str | None,
    db_path: Path | None,
) -> EventFrame | None:
    """Block for this request's correlated reply, tolerating an oversize reply.

    A normal reply is returned decoded off the wire; an oversize reply arrives
    as a truncated stub and its full body is re-fetched from the event store.
    Returns ``None`` if no reply matched before ``timeout`` (a slow/absent peer);
    raises :class:`BroadcastConnectionError` if the daemon connection drops (dead).
    """
    handle = open_subscriber(since=since, socket_path=socket_path, token=token)
    try:
        captured: list[dict[str, Any]] = []
        outcome = await_predicate(
            handle, decide=_reply_decide(correlation_id, sender, captured), deadline_seconds=timeout
        )
        if outcome.framing_error or outcome.peer_closed:
            # A dropped connection is a dead daemon/peer -> abort (raise); distinct
            # from a timeout / no reply, which returns None (a slow peer -> retry).
            raise BroadcastConnectionError(
                "daemon closed the connection before the reply arrived",
                remediation="A dropped connection is a dead daemon/peer; a slow peer times out to None instead.",
            )
        if not (outcome.matched and captured):
            return None
        frame = captured[0]
        if frame.get("kind") == "truncated":
            return _refetch_reply(frame["event_id"], db_path)
        return msgspec.convert(frame, EventFrame, strict=False)
    finally:
        handle.sock.close()


def request(
    to: str,
    body: str,
    *,
    sender: str,
    timeout: float | None = None,
    reply_to: str | None = None,
    correlation_id: str | None = None,
    thread: str | None = None,
    socket_path: str | None = None,
    token: str | None = None,
    db_path: Path | None = None,
    doorbell_path: Path | None = None,
) -> EventFrame | None:
    """Send one addressed message to ``to`` and block for the correlated reply.

    Emits an ``agent_message`` addressed to ``to`` (from ``sender``) carrying a
    fresh ``correlation_id`` and a unique ``reply_to``, then blocks until a reply
    addressed back to ``sender`` with the same ``correlation_id`` arrives, or
    ``timeout`` elapses.

    Race-free without a subscribe-before-send handshake: correctness rests on
    the ``correlation_id`` + recipient match, not on event ordering. A reply is
    causally *after* the request (the responder must receive it first), and the
    daemon assigns every row a monotonic ``seq`` in commit order, so the reply's
    ``seq`` is strictly greater than the request's. The ``since=<request event_id>``
    replay is translated daemon-side to that request's exact ``seq`` and streams
    everything after it, so the reply is caught even if it lands before this call
    begins waiting -- and the guarantee holds across processes because ``seq`` is
    the single writer's order, not the per-process ULID clock. Even if ``seq``
    translation ever missed, the ``correlation_id`` filter means a stale or
    duplicate frame can never be mistaken for this request's reply.

    An oversize reply (a body beyond the wire frame cap) arrives as a truncated
    stub carrying the correlation id; ``request()`` matches it and re-fetches the
    full body from the event store, so a large reply is delivered rather than
    silently timing out.

    Args:
        to: recipient agent name (self-asserted address).
        body: the message payload (an opaque string; JSON by convention).
        sender: this agent's name; replies are addressed back to it.
        timeout: seconds to wait for the reply; ``None`` blocks indefinitely.
        reply_to: override the return address (defaults to a unique
            ``<sender>.<correlation_id>``).
        correlation_id: override the correlation id (defaults to a fresh ULID).
        thread: optional conversation-grouping key set on ``msg_thread``;
            ``None`` (the default) leaves the message unthreaded.
        socket_path / token: broadcast subscribe seams (see :func:`wait_for`).
        db_path / doorbell_path: emit seams (see :func:`waitbus.emit`).

    Returns:
        the reply :class:`EventFrame`, or ``None`` if no reply arrived within
        ``timeout`` -- a slow or absent peer, safe to retry. Raises
        :class:`BroadcastConnectionError` if the daemon connection drops mid-wait
        -- a dead daemon/peer, abort rather than retry.
    """
    correlation_id = correlation_id or _ulid.new()
    reply_to = reply_to or f"{sender}.{correlation_id}"
    result = _emit_agent_message(
        delivery_id=f"agent:{sender}:request:{correlation_id}",
        to=to,
        sender=sender,
        correlation_id=correlation_id,
        reply_to=reply_to,
        body=body,
        thread=thread,
        db_path=db_path,
        doorbell_path=doorbell_path,
    )
    return _await_reply(
        correlation_id=correlation_id,
        sender=sender,
        since=result.event.event_id,
        timeout=timeout,
        socket_path=socket_path,
        token=token,
        db_path=db_path,
    )


def respond(
    request_frame: EventFrame,
    body: str,
    *,
    sender: str | None = None,
    thread: str | None = None,
    db_path: Path | None = None,
    doorbell_path: Path | None = None,
) -> None:
    """Reply to a request frame, echoing its correlation id back to the requester.

    The reply is addressed to the request's ``msg_from`` and copies the request's
    ``msg_correlation_id`` verbatim (the Correlation Identifier pattern) so the
    requester's :func:`request` call matches it. ``sender`` defaults to the
    request's recipient (``msg_to``) -- the agent the request was addressed to.
    The reply echoes the request's ``msg_thread`` by default; pass ``thread=`` to
    override (or to start a thread the request did not carry).

    Raises:
        ValueError: ``request_frame`` is not an addressed message (it lacks
            ``msg_from`` / ``msg_correlation_id``), or ``sender`` could not be
            inferred and was not supplied.
    """
    fields = request_frame.fields
    correlation_id = fields.get("msg_correlation_id")
    recipient = fields.get("msg_from")
    if correlation_id is None or recipient is None:
        raise ValueError("respond() requires a request frame carrying msg_correlation_id and msg_from")
    sender = sender if sender is not None else fields.get("msg_to")
    if sender is None:
        raise ValueError("respond() could not infer the sender (request frame has no msg_to); pass sender=")
    thread = thread if thread is not None else fields.get("msg_thread")
    _emit_agent_message(
        delivery_id=f"agent:{sender}:reply:{correlation_id}",
        to=recipient,
        sender=sender,
        correlation_id=correlation_id,
        reply_to=fields.get("msg_reply_to"),
        body=body,
        thread=thread,
        db_path=db_path,
        doorbell_path=doorbell_path,
    )
