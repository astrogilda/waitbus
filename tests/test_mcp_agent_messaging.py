"""Tests for the v0.1.5 agent-to-agent messaging MCP surface.

Covers the SWARM_DESIGN.md contract:

- emit_agent_message hardcodes event_type=agent_message / source=agent and
  populates msg_to / msg_from / msg_correlation_id; the round-trip
  emit -> store -> doorbell -> read.
- read_agent_messages cursor pagination (msg_to == agent OR '*'); nothing
  re-delivers history.
- the waitbus://agent/{name} doorbell read returns the stub, NOT the inbox.
- _emit_to_subscribed_sessions routes an agent_message to the doorbell
  subscribers with per-session dedup on to='*', and does NOT double-ping
  waitbus://current subscribers.
- event-stream partitioning: tail_events default EXCLUDES agent_message and
  INCLUDES it when explicitly requested.
- output-schema conformance (top-level type:object) for the new tools.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mcp import types
from pydantic import AnyUrl

from waitbus import _db
from waitbus import mcp as mcp_mod
from waitbus._mcp_constants import (
    AGENT_MESSAGE_EVENT_TYPE,
    AGENT_MESSAGE_SOURCE,
    TOOL_EMIT_AGENT_MESSAGE,
    TOOL_READ_AGENT_MESSAGES,
)
from waitbus._mcp_models import schema_emit_agent_message, schema_read_agent_messages
from waitbus._mcp_subscriptions import URI_AGENT_PREFIX, URI_CURRENT, URI_REPO_PREFIX


@pytest.fixture
def events_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Small events DB with _paths.db_path pointed at it."""
    db_path = tmp_path / "events.db"
    _db.ensure_schema(db_path)
    monkeypatch.setattr("waitbus._paths.db_path", lambda: db_path)
    return db_path


def _insert_agent_message(
    db_path: Path,
    *,
    event_id: str,
    msg_to: str,
    msg_from: str = "alice",
    msg_body: str = "hello",
    msg_thread: str | None = None,
    msg_correlation_id: str = "C1",
    received_at: int = 1_700_000_000_000_000_000,
) -> None:
    """Insert an agent_message row directly via SQL."""
    with _db.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (delivery_id, source, event_type, owner, repo, "
            "received_at, payload_json, ingest_method, "
            "msg_to, msg_from, msg_body, msg_thread, msg_correlation_id, event_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                event_id,
                AGENT_MESSAGE_SOURCE,
                AGENT_MESSAGE_EVENT_TYPE,
                "local",
                "agents",
                received_at,
                "{}",
                "api",
                msg_to,
                msg_from,
                msg_body,
                msg_thread,
                msg_correlation_id,
                event_id,
            ),
        )
        conn.commit()


def _insert_workflow_run(db_path: Path, *, event_id: str, received_at: int = 1_700_000_000_000_000_000) -> None:
    with _db.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (delivery_id, source, event_type, owner, repo, "
            "received_at, payload_json, ingest_method, conclusion, event_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                event_id,
                "github_webhook",
                "workflow_run",
                "org",
                "proj",
                received_at,
                "{}",
                "webhook",
                "success",
                event_id,
            ),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# emit_agent_message: hardcoded lane + populated addressing fields
# ---------------------------------------------------------------------------


def test_emit_agent_message_hardcodes_event_type_and_source(events_db: Path) -> None:
    """The tool writes exactly one agent_message/agent row with the addressing facet."""
    result = mcp_mod._emit_agent_message_impl(to="bob", body="hi", from_agent="alice", thread_id="T1")
    assert result["inserted"] is True
    assert result["event_id"]

    with _db.connect(events_db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM events WHERE event_id=?", (result["event_id"],)).fetchone()
    assert row["event_type"] == AGENT_MESSAGE_EVENT_TYPE
    assert row["source"] == AGENT_MESSAGE_SOURCE
    assert row["msg_to"] == "bob"
    assert row["msg_from"] == "alice"
    assert row["msg_body"] == "hi"
    assert row["msg_thread"] == "T1"
    # A fresh correlation id is stamped so the message is referenceable.
    assert row["msg_correlation_id"]


def test_emit_agent_message_thread_id_optional(events_db: Path) -> None:
    """Omitting thread_id leaves msg_thread NULL."""
    result = mcp_mod._emit_agent_message_impl(to="bob", body="hi", from_agent="alice", thread_id=None)
    with _db.connect(events_db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT msg_thread FROM events WHERE event_id=?", (result["event_id"],)).fetchone()
    assert row["msg_thread"] is None


# ---------------------------------------------------------------------------
# read_agent_messages: round-trip + cursor pagination + wildcard inclusion
# ---------------------------------------------------------------------------


def test_emit_then_read_round_trip(events_db: Path) -> None:
    """emit_agent_message -> read_agent_messages returns the message to its recipient."""
    emitted = mcp_mod._emit_agent_message_impl(to="bob", body="ping", from_agent="alice", thread_id=None)
    read = mcp_mod._read_agent_messages_impl(agent="bob", since_cursor=None, limit=100)
    assert len(read["messages"]) == 1
    msg = read["messages"][0]
    assert msg["msg_to"] == "bob"
    assert msg["msg_from"] == "alice"
    assert msg["msg_body"] == "ping"
    assert msg["event_id"] == emitted["event_id"]
    assert read["next_cursor"] == emitted["event_id"]


def test_read_includes_wildcard_recipient(events_db: Path) -> None:
    """A message addressed to '*' is delivered to every agent's read."""
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000001", msg_to="bob", msg_body="direct")
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000002", msg_to="*", msg_body="broadcast")
    read = mcp_mod._read_agent_messages_impl(agent="bob", since_cursor=None, limit=100)
    bodies = {m["msg_body"] for m in read["messages"]}
    assert bodies == {"direct", "broadcast"}


def test_read_excludes_other_recipients(events_db: Path) -> None:
    """A message addressed to a different agent is NOT delivered."""
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000003", msg_to="carol", msg_body="not yours")
    read = mcp_mod._read_agent_messages_impl(agent="bob", since_cursor=None, limit=100)
    assert read["messages"] == []
    # next_cursor stays the input cursor (None) when the window is empty.
    assert read["next_cursor"] is None


def test_read_cursor_pagination(events_db: Path) -> None:
    """The cursor advances; a second read returns only the delta, nothing re-delivers."""
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000010", msg_to="bob", msg_body="m1", received_at=1)
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000011", msg_to="bob", msg_body="m2", received_at=2)

    first = mcp_mod._read_agent_messages_impl(agent="bob", since_cursor=None, limit=1)
    assert [m["msg_body"] for m in first["messages"]] == ["m1"]
    cursor = first["next_cursor"]

    second = mcp_mod._read_agent_messages_impl(agent="bob", since_cursor=cursor, limit=10)
    assert [m["msg_body"] for m in second["messages"]] == ["m2"]

    # A third read above the new cursor is empty: nothing re-delivers history.
    third = mcp_mod._read_agent_messages_impl(agent="bob", since_cursor=second["next_cursor"], limit=10)
    assert third["messages"] == []


def test_read_missing_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing DB returns an empty result rather than raising."""
    monkeypatch.setattr("waitbus._paths.db_path", lambda: tmp_path / "absent.db")
    read = mcp_mod._read_agent_messages_impl(agent="bob", since_cursor=None, limit=10)
    assert read["messages"] == []
    assert read["next_cursor"] is None


# ---------------------------------------------------------------------------
# event-stream partitioning: tail_events default excludes agent_message
# ---------------------------------------------------------------------------


def test_tail_events_default_excludes_agent_message(events_db: Path) -> None:
    """The default tail (no event_types) excludes agent_message rows."""
    _insert_workflow_run(events_db, event_id="01HZWFRUN0000000000000001", received_at=1)
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000020", msg_to="bob", received_at=2)
    result = mcp_mod._tail_events_read(repo=None, since_cursor=None, limit=100)
    types_seen = {e["event_type"] for e in result["events"]}
    assert "workflow_run" in types_seen
    assert AGENT_MESSAGE_EVENT_TYPE not in types_seen


def test_tail_events_includes_agent_message_when_requested(events_db: Path) -> None:
    """tail_events with event_types=['agent_message'] returns only agent messages."""
    _insert_workflow_run(events_db, event_id="01HZWFRUN0000000000000002", received_at=1)
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000021", msg_to="bob", received_at=2)
    result = mcp_mod._tail_events_read(repo=None, since_cursor=None, limit=100, event_types=[AGENT_MESSAGE_EVENT_TYPE])
    types_seen = {e["event_type"] for e in result["events"]}
    assert types_seen == {AGENT_MESSAGE_EVENT_TYPE}


def test_tail_events_explicit_ci_excludes_agent(events_db: Path) -> None:
    """tail_events with event_types=['workflow_run'] never returns agent messages."""
    _insert_workflow_run(events_db, event_id="01HZWFRUN0000000000000003", received_at=1)
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000022", msg_to="bob", received_at=2)
    result = mcp_mod._tail_events_read(repo=None, since_cursor=None, limit=100, event_types=["workflow_run"])
    assert {e["event_type"] for e in result["events"]} == {"workflow_run"}


def test_tail_events_empty_list_is_default_exclude(events_db: Path) -> None:
    """An empty event_types list is treated as the default (exclude agent_message)."""
    _insert_workflow_run(events_db, event_id="01HZWFRUN0000000000000004", received_at=1)
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000023", msg_to="bob", received_at=2)
    result = mcp_mod._tail_events_read(repo=None, since_cursor=None, limit=100, event_types=[])
    assert AGENT_MESSAGE_EVENT_TYPE not in {e["event_type"] for e in result["events"]}


def test_event_type_filter_default_excludes_agent_message() -> None:
    """_event_type_filter(None) renders an exclude clause for agent_message."""
    sql, params = mcp_mod._event_type_filter(None)
    assert "!=" in sql
    assert params == (AGENT_MESSAGE_EVENT_TYPE,)


def test_event_type_filter_explicit_in_clause() -> None:
    """_event_type_filter(list) renders an IN allow-list."""
    sql, params = mcp_mod._event_type_filter(["workflow_run", "agent_message"])
    assert "IN (?, ?)" in sql
    assert params == ("workflow_run", "agent_message")


def test_subscribe_event_types_default_drops_agent_message() -> None:
    """The daemon subscribe allow-list excludes agent_message by default."""
    allow = mcp_mod._subscribe_event_types(None)
    assert AGENT_MESSAGE_EVENT_TYPE not in allow
    assert "workflow_run" in allow


def test_subscribe_event_types_explicit_passthrough() -> None:
    """An explicit agent_message request maps to the agent_message envelope."""
    allow = mcp_mod._subscribe_event_types([AGENT_MESSAGE_EVENT_TYPE])
    assert allow == [AGENT_MESSAGE_EVENT_TYPE]


# ---------------------------------------------------------------------------
# doorbell read returns the stub, NOT the inbox
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_doorbell_read_returns_stub_not_messages(events_db: Path) -> None:
    """Reading waitbus://agent/{name} returns the read-tool stub, never bodies."""
    _insert_agent_message(events_db, event_id="01HZAGENT00000000000000030", msg_to="bob", msg_body="SECRET BODY")
    contents = await mcp_mod._read_resource_handler(AnyUrl(f"{URI_AGENT_PREFIX}bob"))
    assert len(contents) == 1
    body = json.loads(contents[0].content)
    assert body["agent"] == "bob"
    assert body["read_tool"] == TOOL_READ_AGENT_MESSAGES
    assert "read_agent_messages" in body["instruction"]
    # The message body must NOT appear anywhere in the stub.
    assert "SECRET BODY" not in contents[0].content


# ---------------------------------------------------------------------------
# doorbell fan-out + dedup via _emit_to_subscribed_sessions
# ---------------------------------------------------------------------------


def _agent_frame(msg_to: str, event_id: str = "01HZFRAME0000000000000001") -> dict[str, Any]:
    return {
        "kind": "event",
        "event_type": AGENT_MESSAGE_EVENT_TYPE,
        "owner": "local",
        "repo": "agents",
        "event_id": event_id,
        "fields": {"msg_to": msg_to, "msg_from": "alice"},
    }


@pytest.mark.asyncio
async def test_doorbell_pings_directed_recipient_only() -> None:
    """A directed agent_message pings the matching agent doorbell subscriber only."""
    mcp_mod._sessions.clear()
    bob_session = AsyncMock()
    carol_session = AsyncMock()
    bob_state = mcp_mod._get_state(bob_session)
    bob_state.initialized = True
    bob_state.subscriptions.add(f"{URI_AGENT_PREFIX}bob")
    carol_state = mcp_mod._get_state(carol_session)
    carol_state.initialized = True
    carol_state.subscriptions.add(f"{URI_AGENT_PREFIX}carol")
    try:
        await mcp_mod._emit_to_subscribed_sessions(_agent_frame("bob"))
        assert bob_session.send_resource_updated.await_count == 1
        bob_session.send_resource_updated.assert_awaited_with(AnyUrl(f"{URI_AGENT_PREFIX}bob"))
        assert carol_session.send_resource_updated.await_count == 0
    finally:
        mcp_mod._sessions.clear()


@pytest.mark.asyncio
async def test_doorbell_wildcard_pings_each_session_once() -> None:
    """to='*' fires EXACTLY one ping per session that holds any agent subscription."""
    mcp_mod._sessions.clear()
    session = AsyncMock()
    state = mcp_mod._get_state(session)
    state.initialized = True
    # The session holds TWO agent subscriptions; a broadcast must still ping once.
    state.subscriptions.add(f"{URI_AGENT_PREFIX}bob")
    state.subscriptions.add(f"{URI_AGENT_PREFIX}worker-7")
    try:
        await mcp_mod._emit_to_subscribed_sessions(_agent_frame("*"))
        assert session.send_resource_updated.await_count == 1
    finally:
        mcp_mod._sessions.clear()


@pytest.mark.asyncio
async def test_doorbell_does_not_ping_current_subscriber() -> None:
    """An agent_message must NOT ping a waitbus://current (CI) subscriber."""
    mcp_mod._sessions.clear()
    ci_session = AsyncMock()
    ci_state = mcp_mod._get_state(ci_session)
    ci_state.initialized = True
    ci_state.subscriptions.add(URI_CURRENT)
    ci_state.subscriptions.add(f"{URI_REPO_PREFIX}org/proj")
    try:
        await mcp_mod._emit_to_subscribed_sessions(_agent_frame("bob"))
        assert ci_session.send_resource_updated.await_count == 0
    finally:
        mcp_mod._sessions.clear()


@pytest.mark.asyncio
async def test_doorbell_wildcard_does_not_ping_current_subscriber() -> None:
    """to='*' still excludes a current-only subscriber (no agent subscription)."""
    mcp_mod._sessions.clear()
    ci_session = AsyncMock()
    ci_state = mcp_mod._get_state(ci_session)
    ci_state.initialized = True
    ci_state.subscriptions.add(URI_CURRENT)
    agent_session = AsyncMock()
    agent_state = mcp_mod._get_state(agent_session)
    agent_state.initialized = True
    agent_state.subscriptions.add(f"{URI_AGENT_PREFIX}bob")
    try:
        await mcp_mod._emit_to_subscribed_sessions(_agent_frame("*"))
        assert ci_session.send_resource_updated.await_count == 0
        assert agent_session.send_resource_updated.await_count == 1
    finally:
        mcp_mod._sessions.clear()


@pytest.mark.asyncio
async def test_doorbell_queues_pre_init() -> None:
    """A pre-init session queues the doorbell ping rather than sending it."""
    mcp_mod._sessions.clear()
    session = AsyncMock()
    state = mcp_mod._get_state(session)
    state.initialized = False
    state.subscriptions.add(f"{URI_AGENT_PREFIX}bob")
    try:
        await mcp_mod._emit_to_subscribed_sessions(_agent_frame("bob"))
        assert session.send_resource_updated.await_count == 0
        assert len(state.pending) == 1
        assert state.pending[0].uri == f"{URI_AGENT_PREFIX}bob"
    finally:
        mcp_mod._sessions.clear()


@pytest.mark.asyncio
async def test_workflow_run_does_not_reach_agent_doorbell() -> None:
    """A non-agent frame never pings an agent doorbell subscriber."""
    mcp_mod._sessions.clear()
    session = AsyncMock()
    state = mcp_mod._get_state(session)
    state.initialized = True
    state.subscriptions.add(f"{URI_AGENT_PREFIX}bob")
    frame = {
        "kind": "event",
        "event_type": "workflow_run",
        "owner": "org",
        "repo": "proj",
        "event_id": "01HZWF0000000000000000001",
    }
    try:
        await mcp_mod._emit_to_subscribed_sessions(frame)
        assert session.send_resource_updated.await_count == 0
    finally:
        mcp_mod._sessions.clear()


# ---------------------------------------------------------------------------
# call_tool dispatch for the two new tools
# ---------------------------------------------------------------------------


def _registered_call_tool() -> Any:
    """Return the registered _call_tool handler off a built server."""
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.CallToolRequest]
    return handler


@pytest.mark.asyncio
async def test_call_tool_emit_then_read_via_handler(events_db: Path) -> None:
    """The registered tools/call handler round-trips emit + read."""
    handler = _registered_call_tool()

    emit_req = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(
            name=TOOL_EMIT_AGENT_MESSAGE,
            arguments={"to": "bob", "body": "wire-hi", "from_agent": "alice"},
        ),
    )
    emit_result = (await handler(emit_req)).root
    assert isinstance(emit_result, types.CallToolResult)
    assert emit_result.isError in (False, None)
    assert emit_result.structuredContent is not None
    assert emit_result.structuredContent["inserted"] is True

    read_req = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(name=TOOL_READ_AGENT_MESSAGES, arguments={"agent": "bob"}),
    )
    read_result = (await handler(read_req)).root
    assert isinstance(read_result, types.CallToolResult)
    assert read_result.structuredContent is not None
    messages = read_result.structuredContent["messages"]
    assert len(messages) == 1
    assert messages[0]["msg_body"] == "wire-hi"


# ---------------------------------------------------------------------------
# output-schema conformance for the new tools
# ---------------------------------------------------------------------------


def test_new_tool_output_schemas_are_top_level_objects() -> None:
    """Both new output schemas are top-level object types, not bare $ref wrappers."""
    for schema in (schema_read_agent_messages(), schema_emit_agent_message()):
        assert schema.get("type") == "object"
        assert "$ref" not in schema
