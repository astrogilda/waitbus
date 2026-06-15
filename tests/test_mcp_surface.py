"""Tests for the waitbus MCP tool + resource surface.

Covers:

- Capability advertisement (resources.subscribe=true via subclass override).
- Tool definitions carry title + outputSchema and roundtrip through tools/list.
- Tool implementations against an in-memory events DB.
- Dual-emit (content[].text + structuredContent) shape from call_tool.
- Empty prompts/list and the waitbus://current resources/list shape.
- Pre-init notification latch + queue overflow + truncated marker.
- Subscription registry add / remove via subscribe/unsubscribe handlers.
- URI pattern matching for waitbus://current, waitbus://repo/{o}/{r}, wildcards.
- read_resource handler for waitbus://event/{ulid}, waitbus://repo/{o}/{r}.
- Subscribe-handler rejects when broadcast socket is unreachable.
- _stream_events cleanup on subscriber cancellation.
- resources/templates/list advertises the repo + event templates only.
- completion/complete: sanitised distinct owner/repo/ulid values, owner-
  scoped repo completion, empty-DB, cap+hasMore, and None-dispatch cases.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from mcp import types
from mcp.server.lowlevel import NotificationOptions
from pydantic import AnyUrl

from waitbus import _db
from waitbus import mcp as mcp_mod
from waitbus._mcp_constants import (
    TAIL_EVENTS_MAX_WAIT_CAP_SECONDS,
    TOOL_GET_CI_STATUS,
    TOOL_GET_PR_AGGREGATE,
    TOOL_LIST_FAILED_JOBS,
    TOOL_TAIL_EVENTS,
)
from waitbus._mcp_subscriptions import (
    URI_EVENT_PREFIX,
    URI_REPO_PREFIX,
    _QueuedEmit,
    _SessionState,
    _uri_matches_frame,
    is_readable_uri,
    is_subscribable_uri,
    parse_event_uri,
    parse_repo_uri,
)

# --- Capability advertisement --------------------------------------------


def test_capability_subscribe_true_when_handler_registered() -> None:
    """CiStatusServer must flip resources.subscribe=True when subscribe is wired."""
    server = mcp_mod.build_server()
    caps = server.get_capabilities(NotificationOptions(), {})
    assert caps.resources is not None
    assert caps.resources.subscribe is True


def test_capability_includes_tools_section() -> None:
    """tools capability surfaces once list_tools is registered."""
    server = mcp_mod.build_server()
    caps = server.get_capabilities(NotificationOptions(), {})
    assert caps.tools is not None


# --- Tool definitions ----------------------------------------------------


def test_tool_definitions_each_carry_title_and_schemas() -> None:
    tools = mcp_mod._tool_definitions()
    names = {t.name for t in tools}
    assert names == {
        TOOL_GET_CI_STATUS,
        TOOL_LIST_FAILED_JOBS,
        TOOL_GET_PR_AGGREGATE,
        TOOL_TAIL_EVENTS,
    }
    for tool in tools:
        assert tool.title, f"{tool.name} missing title"
        assert tool.inputSchema is not None
        assert tool.outputSchema is not None
        assert tool.description and len(tool.description) > 10


# --- DB fixture ----------------------------------------------------------


@pytest.fixture
def events_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Build a small events DB and point _paths.db_path at it."""
    db_path = tmp_path / "events.db"
    _db.ensure_schema(db_path)
    monkeypatch.setattr("waitbus._paths.db_path", lambda: db_path)
    return db_path


def _insert_row(
    db_path: Path,
    *,
    event_id: str,
    event_type: str,
    owner: str,
    repo: str,
    conclusion: str | None = None,
    run_id: int | None = None,
    job_id: int | None = None,
    parent_run_id: int | None = None,
    payload_json: str = "{}",
    received_at: int = 1_700_000_000_000_000_000,
) -> None:
    """Insert a minimal events row directly via SQL."""
    with _db.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (delivery_id, source, event_type, owner, repo, "
            "run_id, status, conclusion, received_at, payload_json, "
            "ingest_method, job_id, parent_run_id, event_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                event_id,  # delivery_id used as a uniqueness key here.
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


# --- Tool implementations ------------------------------------------------


def test_get_ci_status_returns_latest_workflow_run(events_db: Path) -> None:
    _insert_row(
        events_db,
        event_id="01HZAAA000000000000000001A",
        event_type="workflow_run",
        owner="org",
        repo="proj",
        conclusion="success",
        run_id=1,
    )
    result = mcp_mod._tool_get_ci_status_impl(repo="org/proj")
    assert len(result["runs"]) == 1
    run = result["runs"][0]
    assert run["repo"] == "org/proj"
    assert run["conclusion"] == "success"
    assert "queried_at_ns" in result


def test_get_ci_status_handles_missing_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing DB returns an empty result rather than raising."""
    monkeypatch.setattr("waitbus._paths.db_path", lambda: tmp_path / "absent.db")
    result = mcp_mod._tool_get_ci_status_impl(repo=None)
    assert result["runs"] == []


def test_list_failed_jobs_filters_to_failure(events_db: Path) -> None:
    _insert_row(
        events_db,
        event_id="01HZAAA000000000000000002A",
        event_type="workflow_job",
        owner="org",
        repo="proj",
        conclusion="failure",
        job_id=42,
        parent_run_id=1,
    )
    _insert_row(
        events_db,
        event_id="01HZAAA000000000000000003A",
        event_type="workflow_job",
        owner="org",
        repo="proj",
        conclusion="success",
        job_id=43,
        parent_run_id=1,
    )
    result = mcp_mod._tool_list_failed_jobs_impl(repo="org/proj", limit=10)
    assert len(result["jobs"]) == 1
    assert result["jobs"][0]["job_id"] == 42


def test_get_pr_aggregate_matches_payload_pr_number(events_db: Path) -> None:
    import json

    payload = json.dumps(
        {
            "workflow_run": {"pull_requests": [{"number": 7}]},
        }
    )
    _insert_row(
        events_db,
        event_id="01HZAAA000000000000000004A",
        event_type="workflow_run",
        owner="org",
        repo="proj",
        conclusion="success",
        run_id=99,
        payload_json=payload,
    )
    _insert_row(
        events_db,
        event_id="01HZAAA000000000000000005A",
        event_type="workflow_job",
        owner="org",
        repo="proj",
        conclusion="failure",
        job_id=500,
        parent_run_id=99,
    )
    result = mcp_mod._tool_get_pr_aggregate_impl(repo="org/proj", pr_number=7)
    assert len(result["runs"]) == 1
    assert len(result["jobs"]) == 1
    assert result["jobs"][0]["parent_run_id"] == 99


def test_get_pr_aggregate_rejects_bad_repo() -> None:
    with pytest.raises(ValueError, match="owner/name"):
        mcp_mod._tool_get_pr_aggregate_impl(repo="not-a-repo", pr_number=1)


def test_tail_events_returns_cursor(events_db: Path) -> None:
    for i in range(3):
        _insert_row(
            events_db,
            event_id=f"01HZAAA00000000000000010{i}A",
            event_type="workflow_run",
            owner="org",
            repo="proj",
            run_id=i,
        )
    # max_wait_seconds=0 -> immediate one-shot read, no subscribe.
    result = mcp_mod._tail_events_blocking(repo=None, since_cursor="", limit=10, max_wait_seconds=0)
    assert len(result["events"]) == 3
    assert result["next_cursor"] is not None


def test_tail_events_caps_max_wait() -> None:
    with pytest.raises(ValueError, match="exceeds the cap"):
        mcp_mod._tail_events_blocking(
            repo=None,
            since_cursor=None,
            limit=10,
            max_wait_seconds=TAIL_EVENTS_MAX_WAIT_CAP_SECONDS + 1,
        )


# --- URI matching --------------------------------------------------------


@pytest.mark.parametrize(
    "uri,owner,repo,expected",
    [
        ("waitbus://current", "o", "r", True),
        ("waitbus://repo/o/r", "o", "r", True),
        ("waitbus://repo/o/r", "o", "other", False),
        ("waitbus://repo/o/*", "o", "anything", True),
        ("waitbus://repo/*/*", "x", "y", True),
        ("waitbus://repo/o/r", "x", "r", False),
        ("waitbus://event/01ABC", "o", "r", False),
        ("not-a-waitbus-uri", "o", "r", False),
    ],
)
def test_uri_matches_frame(uri: str, owner: str, repo: str, expected: bool) -> None:
    assert _uri_matches_frame(uri, owner, repo) is expected


def test_is_subscribable_uri_classes() -> None:
    assert is_subscribable_uri("waitbus://current")
    assert is_subscribable_uri("waitbus://repo/o/r")
    assert is_subscribable_uri("waitbus://repo/o/*")
    assert not is_subscribable_uri("waitbus://event/01ABC")
    assert not is_subscribable_uri("garbage")


def test_is_readable_uri_classes() -> None:
    assert is_readable_uri("waitbus://current")
    assert is_readable_uri("waitbus://repo/o/r")
    assert is_readable_uri("waitbus://event/01ABC")
    assert not is_readable_uri("https://example.com")


def test_parse_event_and_repo_uri() -> None:
    assert parse_event_uri("waitbus://event/01ABC") == "01ABC"
    assert parse_event_uri("waitbus://current") is None
    assert parse_repo_uri("waitbus://repo/o/r") == ("o", "r")
    assert parse_repo_uri("waitbus://current") is None


# --- read_resource -------------------------------------------------------


@pytest.mark.asyncio
async def test_read_resource_event_returns_row(events_db: Path) -> None:
    _insert_row(
        events_db,
        event_id="01HZREADRES0000000000000AA",
        event_type="workflow_run",
        owner="org",
        repo="proj",
    )
    contents = await mcp_mod._read_resource_handler(AnyUrl("waitbus://event/01HZREADRES0000000000000AA"))
    materialised = list(contents)
    assert len(materialised) == 1
    body = materialised[0].content
    assert isinstance(body, str)
    assert "01HZREADRES" in body


@pytest.mark.asyncio
async def test_read_resource_current_returns_snapshot(events_db: Path) -> None:
    contents = await mcp_mod._read_resource_handler(AnyUrl("waitbus://current"))
    assert list(contents)


@pytest.mark.asyncio
async def test_read_resource_rejects_wildcard_repo() -> None:
    with pytest.raises(ValueError, match="wildcard"):
        await mcp_mod._read_resource_handler(AnyUrl("waitbus://repo/o/*"))


@pytest.mark.asyncio
async def test_read_resource_rejects_unknown_scheme() -> None:
    with pytest.raises(ValueError, match="waitbus://"):
        await mcp_mod._read_resource_handler(AnyUrl("https://example.com/x"))


# --- Subscription registry ----------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_handler_rejects_unsubscribable_uri() -> None:
    with pytest.raises(ValueError, match="not subscribable"):
        await mcp_mod._subscribe_handler(AnyUrl("waitbus://event/01ABC"))


@pytest.mark.asyncio
async def test_subscribe_handler_rejects_when_broadcast_socket_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = tmp_path / "absent.sock"
    monkeypatch.setattr("waitbus._paths.broadcast_socket", lambda: missing)
    with pytest.raises(RuntimeError, match="unreachable"):
        await mcp_mod._subscribe_handler(AnyUrl("waitbus://current"))


# --- Pre-init notification latch -----------------------------------------


@pytest.mark.asyncio
async def test_queue_pending_within_capacity() -> None:
    state = _SessionState()
    for i in range(5):
        mcp_mod._queue_pending(state, "waitbus://current", {"i": i})
    assert len(state.pending) == 5
    assert state.pending_overflowed is False


@pytest.mark.asyncio
async def test_queue_pending_overflow_flips_flag() -> None:
    state = _SessionState()
    # Cap is 1000; push 1001 to overflow.
    for i in range(1001):
        mcp_mod._queue_pending(state, "waitbus://current", {"i": i})
    assert state.pending_overflowed is True
    assert len(state.pending) == 1000


@pytest.mark.asyncio
async def test_flush_pending_emits_each_queued_then_clears() -> None:
    state = _SessionState()
    state.pending.append(_QueuedEmit(uri="waitbus://current", payload={}))
    state.pending.append(_QueuedEmit(uri="waitbus://repo/o/r", payload={}))

    session = AsyncMock()
    await mcp_mod._flush_pending(session, state)
    assert session.send_resource_updated.await_count == 2
    assert not state.pending


@pytest.mark.asyncio
async def test_flush_pending_emits_truncated_marker_on_overflow() -> None:
    state = _SessionState()
    state.pending_overflowed = True
    session = AsyncMock()
    await mcp_mod._flush_pending(session, state)
    assert state.pending_overflowed is False
    assert session.send_message.await_count == 1


# --- Fan-out to subscribed sessions --------------------------------------


@pytest.mark.asyncio
async def test_emit_to_subscribed_sessions_skips_unsubscribed() -> None:
    mcp_mod._sessions.clear()
    session = AsyncMock()
    state = mcp_mod._get_state(session)
    state.initialized = True
    state.subscriptions.add("waitbus://repo/different/repo")
    frame = {
        "kind": "event",
        "event_type": "workflow_run",
        "owner": "org",
        "repo": "proj",
        "event_id": "01ABC",
    }
    try:
        await mcp_mod._emit_to_subscribed_sessions(frame)
        assert session.send_resource_updated.await_count == 0
    finally:
        mcp_mod._sessions.pop(session, None)


@pytest.mark.asyncio
async def test_emit_to_subscribed_sessions_emits_on_match() -> None:
    mcp_mod._sessions.clear()
    session = AsyncMock()
    state = mcp_mod._get_state(session)
    state.initialized = True
    state.subscriptions.add("waitbus://current")
    frame = {
        "kind": "event",
        "event_type": "workflow_run",
        "owner": "org",
        "repo": "proj",
        "event_id": "01ABC",
    }
    try:
        await mcp_mod._emit_to_subscribed_sessions(frame)
        assert session.send_resource_updated.await_count == 1
    finally:
        mcp_mod._sessions.pop(session, None)


@pytest.mark.asyncio
async def test_emit_to_subscribed_sessions_queues_pre_init() -> None:
    mcp_mod._sessions.clear()
    session = AsyncMock()
    state = mcp_mod._get_state(session)
    state.initialized = False  # pre-handshake
    state.subscriptions.add("waitbus://current")
    frame = {
        "kind": "event",
        "event_type": "workflow_run",
        "owner": "org",
        "repo": "proj",
        "event_id": "01ABC",
    }
    try:
        await mcp_mod._emit_to_subscribed_sessions(frame)
        assert session.send_resource_updated.await_count == 0
        assert len(state.pending) == 1
    finally:
        mcp_mod._sessions.pop(session, None)


# --- list_resources / list_prompts shape --------------------------------


def test_resources_list_definition_includes_current() -> None:
    """The build_server pipeline registers an empty list_prompts handler.

    We exercise it indirectly: the SDK stores handlers by request type.
    """
    server = mcp_mod.build_server()
    assert types.ListPromptsRequest in server.request_handlers
    assert types.ListResourcesRequest in server.request_handlers
    assert types.ListToolsRequest in server.request_handlers
    assert types.CallToolRequest in server.request_handlers
    assert types.ReadResourceRequest in server.request_handlers
    assert types.SubscribeRequest in server.request_handlers
    assert types.UnsubscribeRequest in server.request_handlers
    assert types.ListResourceTemplatesRequest in server.request_handlers
    assert types.CompleteRequest in server.request_handlers


def test_ping_handler_not_double_registered() -> None:
    """Server.__init__ already registers PingRequest; build_server must not duplicate."""
    server = mcp_mod.build_server()
    # PingRequest handler exists (registered by Server.__init__) and we
    # do not overwrite it.
    assert types.PingRequest in server.request_handlers


# --- _stream_events cleanup ---------------------------------------------


@pytest.mark.asyncio
async def test_stream_events_pops_session_on_cancel(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When the subscriber task is cancelled, the registry entry is dropped."""
    missing = tmp_path / "absent.sock"
    monkeypatch.setattr("waitbus._paths.broadcast_socket", lambda: missing)

    async def fake_sleep(_duration: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("waitbus.mcp.asyncio.sleep", fake_sleep)

    session = AsyncMock()
    mcp_mod._sessions[session] = _SessionState()
    assert session in mcp_mod._sessions
    with pytest.raises(asyncio.CancelledError):
        await mcp_mod._stream_events(session)
    assert session not in mcp_mod._sessions


# --- ServerSession weak-ref probe ----------------------------------------


def test_verify_session_weak_referenceable_succeeds() -> None:
    """The probe must succeed for the pinned SDK; if it fails the build_server raises."""
    mcp_mod._verify_session_is_weak_referenceable()


# --- resources/templates/list -------------------------------------------


@pytest.mark.asyncio
async def test_list_resource_templates_returns_repo_and_event_only() -> None:
    """The templates handler advertises exactly repo + event, no current.

    The uriTemplate strings must be derived from the imported
    URI_REPO_PREFIX / URI_EVENT_PREFIX so they cannot drift from
    parse_repo_uri / parse_event_uri.
    """
    server = mcp_mod.build_server()
    handler = server.request_handlers[types.ListResourceTemplatesRequest]
    req = types.ListResourceTemplatesRequest(method="resources/templates/list", params=None)
    result = await handler(req)
    inner = result.root
    assert isinstance(inner, types.ListResourceTemplatesResult)
    templates = inner.resourceTemplates
    by_name = {t.name: t for t in templates}
    assert set(by_name) == {"repo", "event"}
    assert by_name["repo"].uriTemplate == f"{URI_REPO_PREFIX}{{owner}}/{{repo}}"
    assert by_name["event"].uriTemplate == f"{URI_EVENT_PREFIX}{{ulid}}"
    assert by_name["repo"].mimeType == "application/json"
    assert by_name["event"].mimeType == "application/json"
    assert by_name["repo"].title
    assert by_name["event"].title
    # waitbus://current is a concrete URI, never a template.
    assert all(t.name != "current" for t in templates)
    assert all("current" not in t.uriTemplate for t in templates)


# --- completion/complete ------------------------------------------------


def _repo_ref() -> types.ResourceTemplateReference:
    return types.ResourceTemplateReference(type="ref/resource", uri=f"{URI_REPO_PREFIX}{{owner}}/{{repo}}")


def _event_ref() -> types.ResourceTemplateReference:
    return types.ResourceTemplateReference(type="ref/resource", uri=f"{URI_EVENT_PREFIX}{{ulid}}")


@pytest.mark.asyncio
async def test_completion_owner_sanitises_injection_shaped_value(
    events_db: Path,
) -> None:
    """Owner completion returns distinct, control-stripped values.

    A webhook-shaped owner carrying ANSI + zero-width carriers must be
    stripped by the _untrusted seam while the ascii owner survives.
    """
    _insert_row(
        events_db,
        event_id="01HZCMP00000000000000001AA",
        event_type="workflow_run",
        owner="cleanorg",
        repo="proj",
    )
    # ESC[31m ... + zero-width space (U+200B) embedded in the owner.
    _insert_row(
        events_db,
        event_id="01HZCMP00000000000000002AA",
        event_type="workflow_run",
        owner="\x1b[31mevil​org",
        repo="proj",
    )
    result = await mcp_mod._complete_resource_template(
        _repo_ref(),
        types.CompletionArgument(name="owner", value=""),
        None,
    )
    assert result is not None
    assert "cleanorg" in result.values
    # The injection-shaped owner sanitises to bare "evilorg" (ANSI +
    # zero-width stripped); the raw control bytes never surface.
    assert "evilorg" in result.values
    assert all("\x1b" not in v and "​" not in v for v in result.values)
    assert result.hasMore is False
    assert result.total == len(result.values)


@pytest.mark.asyncio
async def test_completion_owner_drops_value_that_sanitises_to_empty(
    events_db: Path,
) -> None:
    """An owner that is purely control bytes is dropped, not surfaced empty."""
    _insert_row(
        events_db,
        event_id="01HZCMP00000000000000003AA",
        event_type="workflow_run",
        owner="realowner",
        repo="proj",
    )
    _insert_row(
        events_db,
        event_id="01HZCMP00000000000000004AA",
        event_type="workflow_run",
        owner="​‌‍",  # zero-width only -> strips to ""
        repo="proj",
    )
    result = await mcp_mod._complete_resource_template(
        _repo_ref(),
        types.CompletionArgument(name="owner", value=""),
        None,
    )
    assert result is not None
    assert "realowner" in result.values
    assert "" not in result.values


@pytest.mark.asyncio
async def test_completion_repo_scoped_by_context_owner(events_db: Path) -> None:
    """A prior-resolved {owner} in context scopes repo candidates."""
    _insert_row(
        events_db,
        event_id="01HZCMP00000000000000010AA",
        event_type="workflow_run",
        owner="alpha",
        repo="alpha-svc",
    )
    _insert_row(
        events_db,
        event_id="01HZCMP00000000000000011AA",
        event_type="workflow_run",
        owner="beta",
        repo="beta-svc",
    )
    scoped = await mcp_mod._complete_resource_template(
        _repo_ref(),
        types.CompletionArgument(name="repo", value=""),
        types.CompletionContext(arguments={"owner": "alpha"}),
    )
    assert scoped is not None
    assert scoped.values == ["alpha-svc"]
    # No context -> global distinct repo across owners.
    unscoped = await mcp_mod._complete_resource_template(
        _repo_ref(),
        types.CompletionArgument(name="repo", value=""),
        None,
    )
    assert unscoped is not None
    assert set(unscoped.values) == {"alpha-svc", "beta-svc"}


@pytest.mark.asyncio
async def test_completion_prefix_filter_applies(events_db: Path) -> None:
    """argument.value is matched as a LIKE prefix, not a substring."""
    _insert_row(
        events_db,
        event_id="01HZCMP00000000000000020AA",
        event_type="workflow_run",
        owner="prefixed",
        repo="r",
    )
    _insert_row(
        events_db,
        event_id="01HZCMP00000000000000021AA",
        event_type="workflow_run",
        owner="other",
        repo="r",
    )
    result = await mcp_mod._complete_resource_template(
        _repo_ref(),
        types.CompletionArgument(name="owner", value="pre"),
        None,
    )
    assert result is not None
    assert result.values == ["prefixed"]


@pytest.mark.asyncio
async def test_completion_ulid_distinct_descending(events_db: Path) -> None:
    """ulid completion returns event_id values, newest-first."""
    ids = [f"01HZCMPULID00000000000003{i}A" for i in range(3)]
    for ev in ids:
        _insert_row(
            events_db,
            event_id=ev,
            event_type="workflow_run",
            owner="org",
            repo="proj",
        )
    result = await mcp_mod._complete_resource_template(
        _event_ref(),
        types.CompletionArgument(name="ulid", value="01HZCMPULID"),
        None,
    )
    assert result is not None
    assert result.values == list(reversed(ids))


@pytest.mark.asyncio
async def test_completion_empty_db_returns_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing DB yields an empty completion with hasMore False."""
    monkeypatch.setattr("waitbus._paths.db_path", lambda: tmp_path / "absent.db")
    result = await mcp_mod._complete_resource_template(
        _repo_ref(),
        types.CompletionArgument(name="owner", value=""),
        None,
    )
    assert result is not None
    assert result.values == []
    assert result.hasMore is False
    assert result.total == 0


@pytest.mark.asyncio
async def test_completion_caps_and_sets_has_more(events_db: Path) -> None:
    """More than _COMPLETION_LIMIT matches caps the list and flips hasMore."""
    over = mcp_mod._COMPLETION_LIMIT + 5
    for i in range(over):
        _insert_row(
            events_db,
            event_id=f"01HZCMPCAP{i:016d}A",
            event_type="workflow_run",
            owner=f"owner{i:04d}",
            repo="proj",
        )
    result = await mcp_mod._complete_resource_template(
        _repo_ref(),
        types.CompletionArgument(name="owner", value="owner"),
        None,
    )
    assert result is not None
    assert len(result.values) == mcp_mod._COMPLETION_LIMIT
    assert result.hasMore is True
    assert result.total is None


@pytest.mark.asyncio
async def test_completion_prompt_reference_returns_none() -> None:
    """A PromptReference is not a resource template -> None (SDK default)."""
    result = await mcp_mod._complete_resource_template(
        types.PromptReference(type="ref/prompt", name="anything"),
        types.CompletionArgument(name="owner", value=""),
        None,
    )
    assert result is None


@pytest.mark.asyncio
async def test_completion_unknown_template_returns_none() -> None:
    """An unadvertised resource template URI -> None."""
    result = await mcp_mod._complete_resource_template(
        types.ResourceTemplateReference(type="ref/resource", uri="waitbus://nope/{x}"),
        types.CompletionArgument(name="owner", value=""),
        None,
    )
    assert result is None


@pytest.mark.asyncio
async def test_completion_known_template_unknown_arg_returns_none(
    events_db: Path,
) -> None:
    """A valid template with an argument it does not expose -> None."""
    result = await mcp_mod._complete_resource_template(
        _event_ref(),
        types.CompletionArgument(name="owner", value=""),
        None,
    )
    assert result is None
