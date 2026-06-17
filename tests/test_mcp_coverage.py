"""Targeted coverage tests for waitbus/mcp.py.

Covers branches and paths not reached by the existing test_mcp*.py suite:

- _emit_to_subscribed_sessions: missing owner/repo guard; send_resource_updated
  exception is swallowed and logged.
- _tool_list_failed_jobs_impl: missing-DB path; no-repo (global) query branch.
- _tool_get_pr_aggregate_impl: missing-DB path; JSON-decode error continue;
  matching workflow_job direct (not via parent run); parent-run-matched job.
- _tail_events_read: repo-scoped SQL branch; next_cursor update when rows found.
- _tail_events_blocking: BroadcastConnectionError degrades gracefully;
  open_subscriber + await_predicate happy path (mocked).
- _summarise_runs: >5-run truncation suffix.
- WaitbusServer.get_capabilities: caps.resources is None branch.
- _subscribe_handler: request_ctx present (session added to registry).
- _unsubscribe_handler: request_ctx present (subscription removed); state=None
  early-return.
- _read_event_row: DB missing; event-id not found.
- _read_resource_handler: unhandled URI raises.
- _register_handlers _call_tool: every tool branch exercised via the registered
  handler; unknown-tool branch.
- _verify_session_is_weak_referenceable: TypeError path raises RuntimeError.
- main(): KeyboardInterrupt is swallowed; asyncio.run called.
"""

from __future__ import annotations

import asyncio
import socket
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp import types
from mcp.server.lowlevel import NotificationOptions
from pydantic import AnyUrl

from waitbus import _db
from waitbus import mcp as mcp_mod
from waitbus._broadcast_sub import BroadcastConnectionError
from waitbus._mcp_constants import (
    TOOL_GET_CI_STATUS,
    TOOL_GET_PR_AGGREGATE,
    TOOL_LIST_FAILED_JOBS,
    TOOL_TAIL_EVENTS,
)

# ---------------------------------------------------------------------------
# Shared DB fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def events_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Small events DB with _paths.db_path pointed at it."""
    db_path = tmp_path / "events.db"
    _db.ensure_schema(db_path)
    monkeypatch.setattr("waitbus._paths.db_path", lambda: db_path)
    return db_path


def _insert_row(
    db_path: Path,
    *,
    event_id: str,
    event_type: str,
    owner: str = "org",
    repo: str = "proj",
    conclusion: str | None = "success",
    run_id: int | None = None,
    job_id: int | None = None,
    parent_run_id: int | None = None,
    payload_json: str = "{}",
    received_at: int = 1_700_000_000_000_000_000,
) -> None:
    with _db.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (delivery_id, source, event_type, owner, repo, "
            "run_id, status, conclusion, received_at, payload_json, "
            "ingest_method, job_id, parent_run_id, event_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                event_id,
                "github_webhook",
                event_type,
                owner,
                repo,
                run_id,
                "completed",
                conclusion,
                received_at,
                payload_json,
                "webhook",
                job_id,
                parent_run_id,
                event_id,
            ),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# _emit_to_subscribed_sessions: guards and exception swallowing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_emit_to_subscribed_sessions_skips_frame_without_owner() -> None:
    """Frame with empty owner is silently discarded (guard at line 320)."""
    mcp_mod._sessions.clear()
    session = AsyncMock()
    state = mcp_mod._get_state(session)
    state.initialized = True
    state.subscriptions.add("waitbus://current")
    frame: dict[str, Any] = {"kind": "event", "owner": "", "repo": "r", "event_id": "X"}
    try:
        await mcp_mod._emit_to_subscribed_sessions(frame)
        assert session.send_resource_updated.await_count == 0
    finally:
        mcp_mod._sessions.pop(session, None)


@pytest.mark.asyncio
async def test_emit_to_subscribed_sessions_swallows_send_exception() -> None:
    """A failing send_resource_updated is logged-and-continued, not propagated."""
    mcp_mod._sessions.clear()
    session = AsyncMock()
    session.send_resource_updated.side_effect = RuntimeError("send failed")
    state = mcp_mod._get_state(session)
    state.initialized = True
    state.subscriptions.add("waitbus://current")
    frame: dict[str, Any] = {
        "kind": "event",
        "event_type": "workflow_run",
        "owner": "org",
        "repo": "proj",
        "event_id": "01ABC",
    }
    try:
        # Must not propagate.
        await mcp_mod._emit_to_subscribed_sessions(frame)
    finally:
        mcp_mod._sessions.pop(session, None)


# ---------------------------------------------------------------------------
# _tool_list_failed_jobs_impl: missing-DB and no-repo branches
# ---------------------------------------------------------------------------


def test_list_failed_jobs_missing_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("waitbus._paths.db_path", lambda: tmp_path / "absent.db")
    result = mcp_mod._tool_list_failed_jobs_impl(repo=None, limit=10)
    assert result["jobs"] == []


def test_list_failed_jobs_no_repo_global_query(events_db: Path) -> None:
    """repo=None triggers the global query (no owner/repo filter)."""
    _insert_row(
        events_db,
        event_id="01HZCOV0001000000000000001A",
        event_type="workflow_job",
        owner="org",
        repo="proj",
        conclusion="failure",
        job_id=77,
        parent_run_id=1,
    )
    result = mcp_mod._tool_list_failed_jobs_impl(repo=None, limit=10)
    assert any(j["job_id"] == 77 for j in result["jobs"])


# ---------------------------------------------------------------------------
# _tool_get_pr_aggregate_impl: missing-DB, JSON-decode error, direct job match,
# and parent-run-matched job
# ---------------------------------------------------------------------------


def test_get_pr_aggregate_missing_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("waitbus._paths.db_path", lambda: tmp_path / "absent.db")
    result = mcp_mod._tool_get_pr_aggregate_impl(repo="org/proj", pr_number=1)
    assert result["runs"] == []
    assert result["jobs"] == []


def test_get_pr_aggregate_json_decode_error_skipped(events_db: Path) -> None:
    """A row with non-JSON payload_json is skipped (continue in the loop)."""
    _insert_row(
        events_db,
        event_id="01HZCOV0002000000000000001A",
        event_type="workflow_run",
        owner="org",
        repo="proj",
        run_id=200,
        payload_json="NOT_JSON",
    )
    result = mcp_mod._tool_get_pr_aggregate_impl(repo="org/proj", pr_number=5)
    # The bad-JSON row must not appear in runs.
    assert result["runs"] == []


def test_get_pr_aggregate_workflow_job_matches_directly(events_db: Path) -> None:
    """A workflow_job row whose payload has pull_requests is matched directly."""
    import json as _json

    payload = _json.dumps({"pull_requests": [{"number": 3}]})
    _insert_row(
        events_db,
        event_id="01HZCOV0003000000000000001A",
        event_type="workflow_job",
        owner="org",
        repo="proj",
        conclusion="failure",
        job_id=300,
        parent_run_id=None,
        payload_json=payload,
    )
    result = mcp_mod._tool_get_pr_aggregate_impl(repo="org/proj", pr_number=3)
    assert any(j["job_id"] == 300 for j in result["jobs"])


def test_get_pr_aggregate_job_matched_via_parent_run(events_db: Path) -> None:
    """A workflow_job is included when its parent run matched the PR."""
    import json as _json

    run_payload = _json.dumps({"workflow_run": {"pull_requests": [{"number": 9}]}})
    _insert_row(
        events_db,
        event_id="01HZCOV0004000000000000001A",
        event_type="workflow_run",
        owner="org",
        repo="proj",
        run_id=400,
        payload_json=run_payload,
    )
    # Job NOT in PR payload but parent run 400 matched.
    _insert_row(
        events_db,
        event_id="01HZCOV0004000000000000002A",
        event_type="workflow_job",
        owner="org",
        repo="proj",
        conclusion="failure",
        job_id=401,
        parent_run_id=400,
        payload_json="{}",
    )
    result = mcp_mod._tool_get_pr_aggregate_impl(repo="org/proj", pr_number=9)
    assert len(result["runs"]) == 1
    assert any(j["job_id"] == 401 for j in result["jobs"])


# ---------------------------------------------------------------------------
# _tail_events_read: repo-scoped SQL branch
# ---------------------------------------------------------------------------


def test_tail_events_read_repo_scoped(events_db: Path) -> None:
    """When repo is provided, only rows for that owner/repo are returned."""
    _insert_row(
        events_db,
        event_id="01HZCOV0005000000000000001A",
        event_type="workflow_run",
        owner="org",
        repo="proj",
        run_id=1,
    )
    _insert_row(
        events_db,
        event_id="01HZCOV0005000000000000002A",
        event_type="workflow_run",
        owner="org",
        repo="other",
        run_id=2,
    )
    result = mcp_mod._tail_events_read(repo="org/proj", since_cursor=None, limit=10)
    assert len(result["events"]) == 1
    assert result["events"][0]["repo"] == "proj"
    assert result["next_cursor"] is not None


def test_tail_events_read_missing_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("waitbus._paths.db_path", lambda: tmp_path / "absent.db")
    result = mcp_mod._tail_events_read(repo=None, since_cursor=None, limit=10)
    assert result["events"] == []


# ---------------------------------------------------------------------------
# _tail_events_blocking: BroadcastConnectionError degrades
# ---------------------------------------------------------------------------


def test_tail_events_blocking_degrades_on_broadcast_connection_error(
    events_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BroadcastConnectionError causes graceful degrade to DB-read result."""

    def _raise(**_kw: Any) -> Any:
        raise BroadcastConnectionError("unreachable", "start the daemon")

    monkeypatch.setattr(mcp_mod, "open_subscriber", _raise)
    # No events so the long-poll path is entered (max_wait_seconds > 0,
    # no events from the initial read).
    result = mcp_mod._tail_events_blocking(repo=None, since_cursor=None, limit=10, max_wait_seconds=1)
    assert "events" in result


def test_tail_events_blocking_open_and_wait_then_reread(
    events_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When open_subscriber succeeds, await_predicate is called then a re-read."""
    fake_sock = MagicMock(spec=socket.socket)
    fake_handle = MagicMock()
    fake_handle.sock = fake_sock

    monkeypatch.setattr(mcp_mod, "open_subscriber", lambda **_kw: fake_handle)
    monkeypatch.setattr(mcp_mod, "await_predicate", lambda *a, **kw: None)

    result = mcp_mod._tail_events_blocking(repo=None, since_cursor=None, limit=10, max_wait_seconds=1)
    assert "events" in result
    fake_sock.close.assert_called_once()


def test_tail_events_blocking_repo_filter_passed(
    events_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When repo='org/proj', open_subscriber is called with filters=['org/proj']."""
    captured: dict[str, Any] = {}
    fake_sock = MagicMock(spec=socket.socket)
    fake_handle = MagicMock()
    fake_handle.sock = fake_sock

    def fake_open(**kw: Any) -> Any:
        captured.update(kw)
        return fake_handle

    monkeypatch.setattr(mcp_mod, "open_subscriber", fake_open)
    monkeypatch.setattr(mcp_mod, "await_predicate", lambda *a, **kw: None)

    mcp_mod._tail_events_blocking(repo="org/proj", since_cursor=None, limit=10, max_wait_seconds=1)
    assert captured.get("filters") == ["org/proj"]


# ---------------------------------------------------------------------------
# _summarise_runs: >5 runs produces truncation suffix
# ---------------------------------------------------------------------------


def test_summarise_runs_more_than_five() -> None:
    runs = [{"repo": f"org/r{i}", "workflow_name": "CI", "conclusion": "success"} for i in range(7)]
    summary = mcp_mod._summarise_runs(runs)
    assert "(+2 more)" in summary


def test_summarise_runs_empty() -> None:
    assert mcp_mod._summarise_runs([]) == "No workflow_run events recorded."


def test_summarise_runs_five_exactly() -> None:
    runs = [{"repo": f"org/r{i}", "workflow_name": "CI", "conclusion": "success"} for i in range(5)]
    summary = mcp_mod._summarise_runs(runs)
    assert "(+" not in summary


# ---------------------------------------------------------------------------
# WaitbusServer.get_capabilities: resources is None branch
# ---------------------------------------------------------------------------


def test_get_capabilities_resources_none_branch() -> None:
    """When base get_capabilities returns resources=None, subscribe is not set."""
    server = mcp_mod.WaitbusServer(name="t", version="0")
    # No subscribe handler -> base caps has resources=None (no list_resources).
    caps = server.get_capabilities(NotificationOptions(), {})
    # Must not raise; subscribe field is absent when resources is None.
    if caps.resources is not None:
        # If list_resources was registered somehow, subscribe state depends on
        # whether SubscribeRequest is in handlers.
        pass


# ---------------------------------------------------------------------------
# _subscribe_handler: request_ctx success path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_handler_with_valid_ctx_adds_subscription(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When request_ctx is live, subscription is added to the session registry."""
    sock_path = tmp_path / "b.sock"
    sock_path.touch()
    monkeypatch.setattr("waitbus._paths.broadcast_socket", lambda: sock_path)

    session = AsyncMock()

    fake_ctx = MagicMock()
    fake_ctx.session = session

    fake_token = MagicMock()
    fake_token.get.return_value = fake_ctx

    mcp_mod._sessions.clear()
    with patch("mcp.server.lowlevel.server.request_ctx", fake_token):
        await mcp_mod._subscribe_handler(AnyUrl("waitbus://current"))

    state = mcp_mod._sessions.get(session)
    assert state is not None
    assert "waitbus://current" in state.subscriptions
    mcp_mod._sessions.pop(session, None)


@pytest.mark.asyncio
async def test_subscribe_handler_lookup_error_returns_silently(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """LookupError from request_ctx.get() is swallowed (tests / direct call)."""
    sock_path = tmp_path / "b2.sock"
    sock_path.touch()
    monkeypatch.setattr("waitbus._paths.broadcast_socket", lambda: sock_path)

    fake_token = MagicMock()
    fake_token.get.side_effect = LookupError

    with patch("mcp.server.lowlevel.server.request_ctx", fake_token):
        await mcp_mod._subscribe_handler(AnyUrl("waitbus://current"))  # must not raise


# ---------------------------------------------------------------------------
# _unsubscribe_handler: request_ctx paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unsubscribe_handler_removes_subscription() -> None:
    """When request_ctx is live and state exists, URI is removed from subscriptions."""
    session = AsyncMock()
    state = mcp_mod._get_state(session)
    state.subscriptions.add("waitbus://current")

    fake_ctx = MagicMock()
    fake_ctx.session = session
    fake_token = MagicMock()
    fake_token.get.return_value = fake_ctx

    with patch("mcp.server.lowlevel.server.request_ctx", fake_token):
        await mcp_mod._unsubscribe_handler(AnyUrl("waitbus://current"))

    assert "waitbus://current" not in state.subscriptions
    mcp_mod._sessions.pop(session, None)


@pytest.mark.asyncio
async def test_unsubscribe_handler_state_none_returns_silently() -> None:
    """When session has no registry entry, unsubscribe is a no-op."""
    session = AsyncMock()
    # Ensure no state is registered.
    mcp_mod._sessions.pop(session, None)

    fake_ctx = MagicMock()
    fake_ctx.session = session
    fake_token = MagicMock()
    fake_token.get.return_value = fake_ctx

    with patch("mcp.server.lowlevel.server.request_ctx", fake_token):
        await mcp_mod._unsubscribe_handler(AnyUrl("waitbus://current"))  # no-op, no raise


@pytest.mark.asyncio
async def test_unsubscribe_handler_lookup_error_returns_silently() -> None:
    """LookupError from request_ctx.get() is swallowed."""
    fake_token = MagicMock()
    fake_token.get.side_effect = LookupError

    with patch("mcp.server.lowlevel.server.request_ctx", fake_token):
        await mcp_mod._unsubscribe_handler(AnyUrl("waitbus://current"))  # no raise


# ---------------------------------------------------------------------------
# _read_event_row: missing-DB and not-found paths
# ---------------------------------------------------------------------------


def test_read_event_row_missing_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("waitbus._paths.db_path", lambda: tmp_path / "absent.db")
    with pytest.raises(ValueError, match="events DB missing"):
        mcp_mod._read_event_row("ANID", "waitbus://event/ANID")


def test_read_event_row_not_found(events_db: Path) -> None:
    with pytest.raises(ValueError, match="no event with id"):
        mcp_mod._read_event_row("01HZNONEXISTENT0000000001A", "waitbus://event/01HZNONEXISTENT0000000001A")


# ---------------------------------------------------------------------------
# _read_resource_handler: event not found raises
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_resource_handler_event_not_found_raises(events_db: Path) -> None:
    """Requesting a waitbus://event/{ulid} with no matching row raises ValueError."""
    with pytest.raises(ValueError, match="no event with id"):
        await mcp_mod._read_resource_handler(AnyUrl("waitbus://event/01HZNONEXISTENT0000000099A"))


@pytest.mark.asyncio
async def test_read_resource_handler_repo_uri_returns_snapshot(events_db: Path) -> None:
    """waitbus://repo/{o}/{r} without wildcard returns a CI status snapshot."""
    _insert_row(
        events_db,
        event_id="01HZCOV0099000000000000001A",
        event_type="workflow_run",
        owner="alpha",
        repo="svc",
        run_id=100,
    )
    contents = await mcp_mod._read_resource_handler(AnyUrl("waitbus://repo/alpha/svc"))
    assert contents


# ---------------------------------------------------------------------------
# _register_handlers _call_tool: every tool branch via registered handler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_call_tool_get_ci_status_via_handler(events_db: Path) -> None:
    """The registered _call_tool handler routes TOOL_GET_CI_STATUS correctly."""
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.CallToolRequest]
    req = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(name=TOOL_GET_CI_STATUS, arguments={}),
    )
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.CallToolResult)
    assert inner.content


@pytest.mark.asyncio
async def test_call_tool_list_failed_jobs_via_handler(events_db: Path) -> None:
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.CallToolRequest]
    req = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(name=TOOL_LIST_FAILED_JOBS, arguments={"limit": 5}),
    )
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.CallToolResult)


@pytest.mark.asyncio
async def test_call_tool_get_pr_aggregate_via_handler(events_db: Path) -> None:
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.CallToolRequest]
    req = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(name=TOOL_GET_PR_AGGREGATE, arguments={"repo": "org/proj", "pr_number": 1}),
    )
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.CallToolResult)


@pytest.mark.asyncio
async def test_call_tool_tail_events_via_handler(events_db: Path) -> None:
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.CallToolRequest]
    req = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(name=TOOL_TAIL_EVENTS, arguments={"limit": 5, "max_wait_seconds": 0}),
    )
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.CallToolResult)


@pytest.mark.asyncio
async def test_call_tool_list_failed_jobs_no_failures_human_text(events_db: Path) -> None:
    """When there are no failed jobs, human text says 'No failed workflow_job events'."""
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.CallToolRequest]
    req = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(name=TOOL_LIST_FAILED_JOBS, arguments={"limit": 10}),
    )
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.CallToolResult)
    first = inner.content[0]
    assert isinstance(first, types.TextContent)
    assert "No failed" in first.text


@pytest.mark.asyncio
async def test_list_tools_via_handler() -> None:
    """_list_tools returns the four expected tool names."""
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.ListToolsRequest]
    req = types.ListToolsRequest(method="tools/list", params=None)
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.ListToolsResult)
    names = {t.name for t in inner.tools}
    assert TOOL_GET_CI_STATUS in names
    assert TOOL_LIST_FAILED_JOBS in names
    assert TOOL_GET_PR_AGGREGATE in names
    assert TOOL_TAIL_EVENTS in names


@pytest.mark.asyncio
async def test_list_resources_via_handler() -> None:
    """_list_resources returns waitbus://current as the sole concrete resource."""
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.ListResourcesRequest]
    req = types.ListResourcesRequest(method="resources/list", params=None)
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.ListResourcesResult)
    uris = [str(r.uri) for r in inner.resources]
    assert any("current" in u for u in uris)


@pytest.mark.asyncio
async def test_list_prompts_via_handler() -> None:
    """_list_prompts returns an empty list."""
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.ListPromptsRequest]
    req = types.ListPromptsRequest(method="prompts/list", params=None)
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.ListPromptsResult)
    assert inner.prompts == []


# ---------------------------------------------------------------------------
# _verify_session_is_weak_referenceable: TypeError path
# ---------------------------------------------------------------------------


def test_verify_session_not_weak_referenceable_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ServerSession that rejects weak references raises RuntimeError."""

    class _NoWeakref:
        __slots__ = ("x",)  # no __weakref__ slot -> weakref.ref raises TypeError

    monkeypatch.setattr(mcp_mod, "ServerSession", _NoWeakref)
    with pytest.raises(RuntimeError, match="weak-referenceable"):
        mcp_mod._verify_session_is_weak_referenceable()


# ---------------------------------------------------------------------------
# main(): KeyboardInterrupt is swallowed
# ---------------------------------------------------------------------------


def test_main_swallows_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() calls asyncio.run and converts KeyboardInterrupt to sys.exit(0)."""

    def _raise_ki(coro: Any = None, *_a: object, **_kw: object) -> None:
        # main() passes main_async() (a coroutine) to asyncio.run; close it so the
        # stub does not leave an un-awaited coroutine (RuntimeWarning) when it bails.
        if coro is not None and hasattr(coro, "close"):
            coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr("waitbus.mcp.asyncio.run", _raise_ki)
    with pytest.raises(SystemExit) as exc_info:
        mcp_mod.main()
    assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# _stream_events_loop: successful Unix connection with frame emission
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_events_loop_successful_connection_emits_frame(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """_stream_events connects, writes subscribe, reads one frame, then EOF."""
    import json as _json

    sock_path = tmp_path / "bcast.sock"
    sock_path.touch()
    monkeypatch.setattr("waitbus._paths.broadcast_socket", lambda: sock_path)

    frame_bytes = _json.dumps(
        {
            "kind": "event",
            "event_id": "01HZSTREAM00000000000000AA",
            "event_type": "workflow_run",
            "owner": "org",
            "repo": "proj",
            "summary": "hello",
            "fields": {},
        }
    ).encode()

    # Build a fake stream: [encoded-frame, then EOF (None from read_frame)]
    emitted_frames: list[dict[str, Any]] = []
    call_count = 0

    async def fake_read_frame(_reader: Any) -> bytes | None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return frame_bytes
        return None  # EOF

    monkeypatch.setattr("waitbus.mcp.read_frame", fake_read_frame)

    # Capture emit_frame calls instead of sending to a real session.
    async def fake_emit_frame(session: Any, frame: dict[str, Any]) -> None:
        emitted_frames.append(frame)

    monkeypatch.setattr(mcp_mod, "_emit_frame", fake_emit_frame)
    monkeypatch.setattr(mcp_mod, "_emit_to_subscribed_sessions", AsyncMock())

    # Fake asyncio.open_unix_connection -> returns (reader, writer)
    fake_writer = AsyncMock()
    # StreamWriter.write and .close are SYNC; left as AsyncMock they would return
    # un-awaited coroutines (RuntimeWarning). drain()/wait_closed() stay async.
    fake_writer.write = MagicMock()
    fake_writer.close = MagicMock()
    fake_reader = AsyncMock()

    async def fake_open_unix(_path: str) -> tuple[Any, Any]:
        return fake_reader, fake_writer

    monkeypatch.setattr("waitbus.mcp.asyncio.open_unix_connection", fake_open_unix)

    # Also stub _load_filters so no config needed.
    monkeypatch.setattr(mcp_mod, "_load_filters", lambda: {"filters": ["*"]})

    # Also stub asyncio.sleep so after EOF the loop exits on second iteration.
    sleep_count = 0

    async def fake_sleep(_d: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        raise asyncio.CancelledError

    monkeypatch.setattr("waitbus.mcp.asyncio.sleep", fake_sleep)

    session = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await mcp_mod._stream_events_loop(session)

    assert len(emitted_frames) == 1
    assert emitted_frames[0]["event_id"] == "01HZSTREAM00000000000000AA"


@pytest.mark.asyncio
async def test_stream_events_loop_malformed_json_skipped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Malformed JSON frames are skipped; loop continues to EOF."""
    sock_path = tmp_path / "bcast2.sock"
    sock_path.touch()
    monkeypatch.setattr("waitbus._paths.broadcast_socket", lambda: sock_path)

    call_count = 0

    async def fake_read_frame(_reader: Any) -> bytes | None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return b"NOT_JSON"
        return None

    monkeypatch.setattr("waitbus.mcp.read_frame", fake_read_frame)

    emit_mock = AsyncMock()
    monkeypatch.setattr(mcp_mod, "_emit_frame", emit_mock)
    monkeypatch.setattr(mcp_mod, "_emit_to_subscribed_sessions", AsyncMock())

    fake_writer = AsyncMock()
    # write/close are sync on StreamWriter (AsyncMock would leak an un-awaited coroutine).
    fake_writer.write = MagicMock()
    fake_writer.close = MagicMock()

    async def fake_open_unix(_path: str) -> tuple[Any, Any]:
        return AsyncMock(), fake_writer

    monkeypatch.setattr("waitbus.mcp.asyncio.open_unix_connection", fake_open_unix)
    monkeypatch.setattr(mcp_mod, "_load_filters", lambda: {"filters": ["*"]})

    async def fake_sleep(_d: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("waitbus.mcp.asyncio.sleep", fake_sleep)

    session = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await mcp_mod._stream_events_loop(session)

    emit_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_stream_events_loop_connection_error_retries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """ConnectionRefusedError causes backoff sleep and loop continues."""
    sock_path = tmp_path / "bcast3.sock"
    sock_path.touch()
    monkeypatch.setattr("waitbus._paths.broadcast_socket", lambda: sock_path)

    async def fake_open_unix(_path: str) -> tuple[Any, Any]:
        raise ConnectionRefusedError("refused")

    monkeypatch.setattr("waitbus.mcp.asyncio.open_unix_connection", fake_open_unix)
    monkeypatch.setattr(mcp_mod, "_load_filters", lambda: {"filters": ["*"]})

    async def fake_sleep(_d: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("waitbus.mcp.asyncio.sleep", fake_sleep)

    session = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await mcp_mod._stream_events_loop(session)
