"""Tests for the agent-message addressing facet.

The facet adds typed ``msg_to`` / ``msg_from`` / ``msg_correlation_id`` /
``msg_reply_to`` / ``msg_thread`` columns (mirroring the ``alert_*`` facet) so
addressing keys project into the broadcast wire ``fields`` and a recipient /
correlation filter is predicate-matchable on the wire -- which a payload-only
convention cannot achieve, because ``broadcast._row_to_frame`` drops
``payload_json`` from the wire.

This module starts with the data-layer facet tests (no daemon, no SDK). The
SDK ``request`` / ``respond`` round-trip tests live alongside once that layer
lands.
"""

from __future__ import annotations

import sqlite3
import sys
import threading
import time
from pathlib import Path

import msgspec
import pytest

from tests._daemon_helpers import (
    await_subscribers as _await_subscribers,
)
from tests._daemon_helpers import (
    await_thread as _await_thread,
)
from waitbus import _db, broadcast, request, respond, subscribe, wait_for
from waitbus._frame import MAX_FRAME_BYTES, EventFrame
from waitbus._predicate import parse_match
from waitbus._types import EventInsert
from waitbus.broadcast import _row_to_frame


def _insert(db: Path, **overrides: object) -> sqlite3.Row:
    """Insert one event carrying addressing fields; return its stored row."""
    base: dict[str, object] = {
        "delivery_id": "d-addr-1",
        "source": "github",  # facet is generic -- any event row may carry msg_*
        "event_type": "workflow_run",
        "owner": "local",
        "repo": "swarm",
        "received_at": time.time_ns(),
        "payload_json": '{"body": "ping"}',
        "ingest_method": "api",
    }
    base.update(overrides)
    with _db.connect(db) as conn:
        _db.insert_event(conn, EventInsert(**base), commit=True)  # type: ignore[arg-type]
        conn.row_factory = sqlite3.Row
        row: sqlite3.Row | None = conn.execute(
            "SELECT * FROM events WHERE delivery_id = ?", (base["delivery_id"],)
        ).fetchone()
    assert row is not None
    return row


def test_msg_columns_project_into_the_wire_fields(tmp_path: Path) -> None:
    """An emitted ``msg_*`` value must surface in the wire ``EventFrame.fields``.

    Unlike
    ``payload_json`` (dropped by ``_row_to_frame``), the typed columns are part
    of the ``EVENT_COLUMNS`` projection, so they reach the consumer on the wire.
    """
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    row = _insert(
        db,
        msg_to="agent_b",
        msg_from="agent_a",
        msg_correlation_id="corr-1",
        msg_reply_to="agent_a.r1",
        msg_thread="t-1",
    )
    frame = _row_to_frame(row)
    assert frame.fields["msg_to"] == "agent_b"
    assert frame.fields["msg_from"] == "agent_a"
    assert frame.fields["msg_correlation_id"] == "corr-1"
    assert frame.fields["msg_reply_to"] == "agent_a.r1"
    assert frame.fields["msg_thread"] == "t-1"


def test_recipient_predicate_matches_on_the_wire_frame(tmp_path: Path) -> None:
    """``fields.msg_to=...`` selects only events addressed to that recipient."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    frame_for_b = msgspec.to_builtins(_row_to_frame(_insert(db, msg_to="agent_b")))

    assert parse_match(['fields.msg_to="agent_b"']).evaluate(frame_for_b) is True
    assert parse_match(['fields.msg_to="agent_c"']).evaluate(frame_for_b) is False
    # A non-addressed event (msg_to NULL) never matches a recipient filter.
    db2 = tmp_path / "events2.db"
    _db.ensure_schema(db2)
    plain = msgspec.to_builtins(_row_to_frame(_insert(db2)))
    assert parse_match(['fields.msg_to="agent_b"']).evaluate(plain) is False


def test_addressing_columns_migrate_into_a_pre_facet_db(tmp_path: Path) -> None:
    """An events DB created before the facet gains the columns via ensure_schema's
    idempotent ``ALTER TABLE ADD COLUMN`` diff -- no data loss, no manual step."""
    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE events ("
        "delivery_id TEXT PRIMARY KEY, source TEXT NOT NULL, event_type TEXT NOT NULL, "
        "owner TEXT NOT NULL, repo TEXT NOT NULL, received_at INTEGER NOT NULL, "
        "payload_json TEXT NOT NULL, ingest_method TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    _db.ensure_schema(db)

    conn = sqlite3.connect(db)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
    conn.close()
    for col in ("msg_to", "msg_from", "msg_correlation_id", "msg_reply_to", "msg_thread", "msg_body"):
        assert col in cols, f"{col} not migrated into the pre-facet DB"


# ---------------------------------------------------------------------------
# SDK request / respond + subscribe(to=) inbox -- end to end over a real daemon
# ---------------------------------------------------------------------------

# The daemon's SO_PEERCRED accept-gate is Linux-only (mirrors test_subscribe_sdk).
_e2e = pytest.mark.skipif(sys.platform != "linux", reason="broadcast daemon SO_PEERCRED check is Linux-only")


@_e2e
@pytest.mark.asyncio
async def test_request_respond_round_trip(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """agent_a requests agent_b and receives agent_b's correlated reply."""
    daemon, paths = running_daemon
    sock, db, doorbell = str(paths["broadcast"]), paths["db"], paths["doorbell"]

    def responder() -> None:
        msg = wait_for(to="agent_b", source="agent", timeout=5.0, socket_path=sock)
        if msg is not None:
            respond(msg, '{"answer": 42}', db_path=db, doorbell_path=doorbell)

    reply: list[EventFrame | None] = []

    def requester() -> None:
        reply.append(
            request(
                "agent_b",
                '{"ask": "meaning"}',
                sender="agent_a",
                timeout=5.0,
                socket_path=sock,
                db_path=db,
                doorbell_path=doorbell,
            )
        )

    rt = threading.Thread(target=responder, daemon=True)
    rt.start()
    await _await_subscribers(daemon)  # responder is subscribed before the request is sent
    qt = threading.Thread(target=requester, daemon=True)
    qt.start()
    await _await_thread(qt, timeout=6.0)
    rt.join(timeout=2.0)
    qt.join(timeout=2.0)

    assert not qt.is_alive(), "request() hung"
    got = reply[0]
    assert isinstance(got, EventFrame)
    assert got.event_type == "agent_message"
    assert got.fields["msg_from"] == "agent_b"
    assert got.fields["msg_to"] == "agent_a"
    assert got.fields["msg_body"] == '{"answer": 42}'  # the reply BODY reached the requester on the wire
    assert got.fields["msg_correlation_id"]  # correlated


@_e2e
@pytest.mark.asyncio
async def test_request_times_out_when_no_responder(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """With nobody answering, request() returns None after the timeout (no hang)."""
    _daemon, paths = running_daemon
    sock, db, doorbell = str(paths["broadcast"]), paths["db"], paths["doorbell"]
    reply: list[EventFrame | None] = []

    def requester() -> None:
        reply.append(
            request("ghost", "{}", sender="agent_a", timeout=0.5, socket_path=sock, db_path=db, doorbell_path=doorbell)
        )

    qt = threading.Thread(target=requester, daemon=True)
    qt.start()
    await _await_thread(qt, timeout=4.0)
    qt.join(timeout=2.0)
    assert not qt.is_alive(), "request() hung past its timeout"
    assert reply == [None]


@_e2e
@pytest.mark.asyncio
async def test_subscribe_to_is_a_recipient_inbox(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """subscribe(to="agent_b") yields only messages addressed to agent_b, not agent_c's."""
    daemon, paths = running_daemon
    sock, db, doorbell = str(paths["broadcast"]), paths["db"], paths["doorbell"]
    inbox: list[str] = []

    def reader() -> None:
        for msg in subscribe(to="agent_b", source="agent", socket_path=sock):
            inbox.append(msg.fields["msg_from"])
            break  # one message is enough for this assertion

    rt = threading.Thread(target=reader, daemon=True)
    rt.start()
    await _await_subscribers(daemon)
    # A message for agent_c (must NOT wake the agent_b inbox) then one for agent_b.
    request("agent_c", "{}", sender="x", timeout=0.1, socket_path=sock, db_path=db, doorbell_path=doorbell)
    request("agent_b", "{}", sender="agent_a", timeout=0.1, socket_path=sock, db_path=db, doorbell_path=doorbell)
    await _await_thread(rt, timeout=4.0)
    rt.join(timeout=2.0)
    assert inbox == ["agent_a"], inbox


def test_respond_rejects_a_non_message_frame() -> None:
    """respond() refuses a frame that is not an addressed message (no msg_* fields)."""
    plain = EventFrame(
        event_id="e1",
        event_type="workflow_run",
        owner="o",
        repo="r",
        received_at=1,
        delivery_id="d1",
        summary="ci",
        fields={"source": "github"},
    )
    with pytest.raises(ValueError, match="msg_correlation_id"):
        respond(plain, "{}")


def test_respond_raises_when_sender_cannot_be_inferred() -> None:
    """respond() needs a sender. A well-formed request carries msg_to (the agent
    it was addressed to, which becomes the reply's sender); a frame with a
    correlation id and msg_from but NO msg_to, and no explicit sender=, leaves
    the reply sender undecidable and must raise rather than emit a reply from an
    empty name."""
    frame = EventFrame(
        event_id="e2",
        event_type="agent_message",
        owner="local",
        repo="agents",
        received_at=1,
        delivery_id="d2",
        summary="agent",
        fields={"msg_correlation_id": "c1", "msg_from": "agent_a"},  # no msg_to
    )
    with pytest.raises(ValueError, match="could not infer the sender"):
        respond(frame, "reply body")


# ---------------------------------------------------------------------------
# Conversation threading: request(thread=) / respond() thread propagation
# ---------------------------------------------------------------------------


def _request_frame(correlation_id: str = "c1", *, thread: str | None = None) -> EventFrame:
    fields: dict[str, object] = {"msg_correlation_id": correlation_id, "msg_from": "agent_a", "msg_to": "agent_b"}
    if thread is not None:
        fields["msg_thread"] = thread
    return EventFrame(
        event_id="e1",
        event_type="agent_message",
        owner="local",
        repo="agents",
        received_at=1,
        delivery_id="req-d1",
        summary="agent",
        fields=fields,
    )


def _stored_reply(db: Path, sender: str, correlation_id: str) -> sqlite3.Row:
    with _db.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row: sqlite3.Row | None = conn.execute(
            "SELECT * FROM events WHERE delivery_id = ?", (f"agent:{sender}:reply:{correlation_id}",)
        ).fetchone()
    assert row is not None
    return row


def test_respond_propagates_request_thread(tmp_path: Path) -> None:
    """respond() echoes the request frame's msg_thread onto the reply by default."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    respond(_request_frame(thread="conv-1"), '{"a": 1}', db_path=db, doorbell_path=tmp_path / "nope.sock")
    assert _stored_reply(db, "agent_b", "c1")["msg_thread"] == "conv-1"


def test_respond_thread_override(tmp_path: Path) -> None:
    """An explicit thread= overrides the request frame's msg_thread."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    respond(
        _request_frame(thread="conv-1"), '{"a": 1}', thread="conv-2", db_path=db, doorbell_path=tmp_path / "nope.sock"
    )
    assert _stored_reply(db, "agent_b", "c1")["msg_thread"] == "conv-2"


def test_respond_unthreaded_request_stays_unthreaded(tmp_path: Path) -> None:
    """A request with no thread yields an unthreaded reply (msg_thread NULL)."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    respond(_request_frame(), '{"a": 1}', db_path=db, doorbell_path=tmp_path / "nope.sock")
    assert _stored_reply(db, "agent_b", "c1")["msg_thread"] is None


@_e2e
@pytest.mark.asyncio
async def test_request_thread_round_trips(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """request(thread=...) sets msg_thread on the request; respond() propagates it to the reply."""
    daemon, paths = running_daemon
    sock, db, doorbell = str(paths["broadcast"]), paths["db"], paths["doorbell"]

    def responder() -> None:
        msg = wait_for(to="agent_b", source="agent", timeout=5.0, socket_path=sock)
        if msg is not None:
            respond(msg, '{"answer": 42}', db_path=db, doorbell_path=doorbell)

    reply: list[EventFrame | None] = []

    def requester() -> None:
        reply.append(
            request(
                "agent_b",
                '{"ask": "x"}',
                sender="agent_a",
                thread="conv-1",
                timeout=5.0,
                socket_path=sock,
                db_path=db,
                doorbell_path=doorbell,
            )
        )

    rt = threading.Thread(target=responder, daemon=True)
    rt.start()
    await _await_subscribers(daemon)
    qt = threading.Thread(target=requester, daemon=True)
    qt.start()
    await _await_thread(qt, timeout=6.0)
    rt.join(timeout=2.0)
    qt.join(timeout=2.0)

    got = reply[0]
    assert isinstance(got, EventFrame)
    assert got.fields.get("msg_thread") == "conv-1"  # request set it; respond propagated it


@_e2e
@pytest.mark.asyncio
async def test_request_receives_oversize_reply_via_refetch(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """An oversize reply (> MAX_FRAME_BYTES) is truncated on the wire, but request()
    matches the stub by correlation id and re-fetches the full body from the event
    store -- delivered, not silently lost."""
    daemon, paths = running_daemon
    sock, db, doorbell = str(paths["broadcast"]), paths["db"], paths["doorbell"]
    big = "x" * (MAX_FRAME_BYTES + 4096)  # the body alone exceeds the wire frame cap

    def responder() -> None:
        msg = wait_for(to="agent_b", source="agent", timeout=5.0, socket_path=sock)
        if msg is not None:
            respond(msg, big, db_path=db, doorbell_path=doorbell)

    reply: list[EventFrame | None] = []

    def requester() -> None:
        reply.append(
            request(
                "agent_b", "{}", sender="agent_a", timeout=5.0, socket_path=sock, db_path=db, doorbell_path=doorbell
            )
        )

    rt = threading.Thread(target=responder, daemon=True)
    rt.start()
    await _await_subscribers(daemon)
    qt = threading.Thread(target=requester, daemon=True)
    qt.start()
    await _await_thread(qt, timeout=6.0)
    rt.join(timeout=2.0)
    qt.join(timeout=2.0)

    assert not qt.is_alive(), "request() hung on an oversize reply"
    got = reply[0]
    assert isinstance(got, EventFrame), "oversize reply was lost (silent timeout)"
    assert got.fields["msg_from"] == "agent_b"
    assert got.fields["msg_body"] == big  # the full body was re-fetched from the store


def test_request_reply_wait_raises_on_dead_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    """request()'s reply wait raises BroadcastConnectionError when the daemon
    connection drops mid-wait (a dead daemon/peer -> abort), distinct from a
    timeout (which returns None for a slow/absent peer)."""
    import socket as socketmod

    from waitbus import _messaging
    from waitbus._broadcast_sub import BroadcastConnectionError, SubscriberHandle, WaitOutcome

    a, b = socketmod.socketpair()
    monkeypatch.setattr(_messaging, "open_subscriber", lambda **_k: SubscriberHandle(sock=a))
    monkeypatch.setattr(
        _messaging,
        "await_predicate",
        lambda *_a, **_k: WaitOutcome(
            matched=False, timed_out=False, cancelled=False, peer_closed=True, framing_error=False
        ),
    )
    try:
        with pytest.raises(BroadcastConnectionError):
            _messaging._await_reply(
                correlation_id="c1", sender="agent_a", since="x", timeout=1.0, socket_path="s", token=None, db_path=None
            )
    finally:
        a.close()
        b.close()
