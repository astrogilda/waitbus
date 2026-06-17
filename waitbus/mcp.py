"""Push GitHub workflow_run / workflow_job events into Claude Code (and any
MCP-spec-compliant client) as native MCP notifications.

The wire layer is the official ``mcp`` Python SDK at v1.27.1 exact, driven
through its low-level ``mcp.server.lowlevel.Server`` interface (NOT
``FastMCP``).

Two notification methods are emitted on every broadcast frame:

- ``notifications/resources/updated`` — MCP-spec-standard; every compliant
  client consumes it. Emitted via the SDK's typed
  ``ServerSession.send_resource_updated`` helper.
- ``notifications/claude/channel`` — Anthropic-private extension; Claude
  Code renders the payload as a chat-injected channel line. The method
  name is not in the closed ``ServerNotification`` pydantic union, so we
  emit it via ``ServerSession.send_message`` with a bare
  ``JSONRPCNotification``; pydantic's RootModel auto-coercion produces
  the correct wire envelope. Spec-compliant clients ignore the unknown
  method per JSON-RPC 2.0 rules.

On macOS the broadcast bus is supported (via launchd); on Linux the
systemd path remains. There is no macOS-specific idle mode — every
platform has a daemon stack.

On daemon shutdown or socket close, retry with capped exponential backoff
(1s -> 30s) so a ``systemctl --user restart`` or launchd restart does not
require relaunching the MCP client.

Optional operator config lives at ``config.toml`` (resolved by ``_paths``)
under the ``[mcp]`` section:

    [mcp]
    filter = ["owner/repo", "owner/*", "*"]
    event_types = ["workflow_run", "workflow_job"]  # optional
    since = "01HZ...26chars"                         # optional

Defaults (no operator config required): ``filter: ["*"]``,
``event_types`` omitted (daemon falls back to its full supported set),
``since: null``.

The separate ``filters.json`` config surface was retired; config now
uses two surfaces (env + TOML).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import signal
import sqlite3
import sys
import time
import weakref
from typing import Any, Final

from mcp import types
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.models import InitializationOptions
from mcp.server.session import ServerSession
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCNotification
from pydantic import AnyUrl

from . import _columns, _config, _db, _paths, _ulid, _untrusted
from ._broadcast_sub import (
    BroadcastConnectionError,
    FrameDecision,
    await_predicate,
    open_subscriber,
)
from ._frame import DRAINABLE_CONTROL_KINDS, encode_frame, read_frame
from ._log import structured
from ._mcp_constants import (
    AGENT_BROADCAST_RECIPIENT,
    AGENT_MESSAGE_EVENT_TYPE,
    AGENT_MESSAGE_SOURCE,
    LIST_FAILED_JOBS_DEFAULT_LIMIT,
    PROTOCOL_VERSION,
    PROTOCOL_VERSIONS_SUPPORTED,
    READ_AGENT_MESSAGES_DEFAULT_LIMIT,
    TAIL_EVENTS_DEFAULT_LIMIT,
    TAIL_EVENTS_DEFAULT_MAX_WAIT_SEC,
    TAIL_EVENTS_MAX_WAIT_CAP_SECONDS,
    TOOL_EMIT_AGENT_MESSAGE,
    TOOL_GET_CI_STATUS,
    TOOL_GET_PR_AGGREGATE,
    TOOL_LIST_FAILED_JOBS,
    TOOL_READ_AGENT_MESSAGES,
    TOOL_TAIL_EVENTS,
)
from ._mcp_models import (
    schema_ci_status,
    schema_emit_agent_message,
    schema_failed_jobs,
    schema_input_emit_agent_message,
    schema_input_get_ci_status,
    schema_input_get_pr_aggregate,
    schema_input_list_failed_jobs,
    schema_input_read_agent_messages,
    schema_input_tail_events,
    schema_pr_aggregate,
    schema_read_agent_messages,
    schema_tail_events,
)
from ._mcp_subscriptions import (
    URI_AGENT_PREFIX,
    URI_CURRENT,
    URI_EVENT_PREFIX,
    URI_REPO_PREFIX,
    _QueuedEmit,
    _SessionState,
    _uri_matches_frame,
    agent_doorbell_uri_for_session,
    is_readable_uri,
    is_subscribable_uri,
    parse_agent_uri,
    parse_event_raw_uri,
    parse_event_uri,
    parse_repo_uri,
)
from ._sdnotify import sd_notify
from ._version import PACKAGE_VERSION
from .sources._registry import event_types_supported

_BACKOFF_INITIAL_S = 1.0
_BACKOFF_MAX_S = 30.0

# Channel meta-key validator per the Anthropic channels-reference contract.
# Hyphenated or dotted keys are silently dropped by Claude Code's renderer.
_META_KEY_RE: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9_]+$")

# Anthropic-private extension method (not in the MCP spec).
# Source: https://code.claude.com/docs/en/channels-reference.
_CLAUDE_CHANNEL_METHOD: Final[str] = "notifications/claude/channel"

#: RFC-6570 argument completion (completion/complete) for the two
# subscribable/readable resource templates. The keys are built from the
# same URI_REPO_PREFIX/URI_EVENT_PREFIX constants the templates are
# advertised with, so the completer can never drift from the advertised
# uriTemplate strings or from parse_repo_uri/parse_event_uri.
_TEMPLATE_REPO: Final[str] = f"{URI_REPO_PREFIX}{{owner}}/{{repo}}"
_TEMPLATE_EVENT: Final[str] = f"{URI_EVENT_PREFIX}{{ulid}}"
_TEMPLATE_AGENT: Final[str] = f"{URI_AGENT_PREFIX}{{name}}"
_COMPLETABLE_TEMPLATES: Final[dict[str, frozenset[str]]] = {
    _TEMPLATE_REPO: frozenset({"owner", "repo"}),
    _TEMPLATE_EVENT: frozenset({"ulid"}),
}
#: Per-argument completion cap. The MCP spec caps Completion.values at 100;
# 50 stays well under while leaving headroom for the +1 has-more probe.
_COMPLETION_LIMIT: Final[int] = 50

#: Cap on the fenced payload_json byte length returned through
#: waitbus://event/{ulid}. Webhook payloads can exceed an agent's per-tool
#: context budget (workflow_run with hundreds of jobs lands in the
#: hundreds of KiB to low MiB range). Over-cap reads return a marker
#: pointing at the opt-in waitbus://event/{ulid}/raw sibling URI plus a
#: 64 KiB fenced preview so a tiny-task agent rarely needs the second
#: read. The cap is measured in UTF-8 bytes (not codepoints) since the
#: downstream consumer's budget is byte- or token-shaped, not character-
#: shaped.
_EVENT_PAYLOAD_CAP_BYTES: Final[int] = 64 * 1024

logger = logging.getLogger("waitbus.mcp")


def _load_filters() -> dict[str, Any]:
    """Read the operator MCP filter from the canonical pydantic-settings tree.

    Reads ``[mcp] filter = [...]`` (plus optional ``event_types`` and ``since``)
    from ``config.toml`` via ``WaitbusConfig.get_config()``. The default
    filter is ``["*"]`` (all events) when the operator has not configured a
    narrower scope. Malformed config surfaces as a ``RuntimeError`` at
    ``get_config()`` invocation (loud-fail config semantics); this
    function does not catch — the daemon refuses to start on a bad config
    rather than silently widening the filter.

    Replaces the legacy ``filters.json`` file (retired).
    """
    cfg = _config.get_config()
    subscribe: dict[str, Any] = {"filters": list(cfg.mcp_filter) or ["*"]}
    if cfg.mcp_event_types:
        subscribe["event_types"] = list(cfg.mcp_event_types)
    if cfg.mcp_since:
        subscribe["since"] = cfg.mcp_since
    return subscribe


def _build_frame_emissions(frame: dict[str, Any]) -> list[tuple[str, dict[str, str], str]]:
    """Translate one broadcast frame into (content, meta, event_id) triples.

    Returns an empty list for heartbeats.
    Each entry describes one event to emit as both notification methods.
    """
    kind = frame.get("kind")
    if kind in DRAINABLE_CONTROL_KINDS:
        return []

    summary = frame.get("summary") or frame.get("event_id") or ""
    repo = f"{frame.get('owner', '')}/{frame.get('repo', '')}"
    event_id = str(frame.get("event_id", ""))
    fields: dict[str, Any] = frame.get("fields") or {}

    # ``summary`` is webhook-derived free text (commit/display_title,
    # workflow/job name, branch). It is fenced so a consuming LLM treats
    # it as inert external data, never as instructions. The ``[truncated]``
    # prefix is waitbus-generated and stays outside the fence so the marker
    # itself remains trustworthy. ``repo`` is control-stripped defensively.
    if kind == "truncated":
        content = f"[truncated] {_untrusted.fence(str(summary), label='event-summary')}"
        meta: dict[str, str] = {
            "repo": _untrusted.strip_control(repo),
            "kind": "truncated",
            "id": event_id,
        }
    else:
        content = _untrusted.fence(str(summary), label="event-summary")
        meta = {
            "repo": _untrusted.strip_control(repo),
            "kind": str(frame.get("event_type", "")),
            "id": event_id,
            "run_id": str(fields.get("run_id") or ""),
            "conclusion": str(fields.get("conclusion") or "pending"),
        }

    return [(content, meta, event_id)]


def _validate_channel_meta(meta: dict[str, str]) -> None:
    """Reject channel meta-keys Claude Code's renderer would silently drop.

    The contract is enforced at the emission boundary rather than in
    ``_build_frame_emissions`` so the failure mode is visible to any
    future caller adding a non-conforming meta key.
    """
    for key, value in meta.items():
        if not _META_KEY_RE.match(key):
            raise ValueError(
                f"claude/channel meta key {key!r} must match [a-zA-Z0-9_]+; "
                "hyphenated/dotted keys are silently dropped by Claude Code's renderer"
            )
        if not isinstance(value, str):
            raise ValueError(f"claude/channel meta value for {key!r} must be a string; got {type(value).__name__}")


async def emit_claude_channel(session: ServerSession, content: str, *, meta: dict[str, str]) -> None:
    """Emit a ``notifications/claude/channel`` notification via the SDK.

    The method name is Anthropic-private and not part of the closed
    ``ServerNotification`` pydantic union. We construct a bare
    ``JSONRPCNotification`` (whose ``method: str`` field is open) and
    pass it through ``ServerSession.send_message``; pydantic's RootModel
    auto-coercion produces a wire envelope semantically equivalent to
    the explicit ``JSONRPCMessage(root=...)`` form. The equivalence is
    verified at runtime by the MCP wire-fixture test suite.
    """
    _validate_channel_meta(meta)
    notification = JSONRPCNotification(
        jsonrpc="2.0",
        method=_CLAUDE_CHANNEL_METHOD,
        params={"content": content, "meta": meta},
    )
    await session.send_message(SessionMessage(message=notification))  # type: ignore[arg-type]


async def emit_resource_updated(session: ServerSession, uri: str) -> None:
    """Emit a spec-standard ``notifications/resources/updated`` notification."""
    await session.send_resource_updated(AnyUrl(uri))


def _client_negotiated_channel(session: ServerSession) -> bool:
    """Return True iff the client advertised the ``claude/channel`` extension.

    The negotiated client capabilities live on
    ``session.client_params.capabilities.experimental`` (the
    ``InitializeRequestParams`` captured by the SDK during the
    initialize handshake). Per the MCP lifecycle MUST clause, a server
    may only use a capability the peer negotiated, so the Anthropic-
    private ``notifications/claude/channel`` extension is emitted only
    when the client listed ``claude/channel`` in its experimental block.

    ``client_params`` is ``None`` before the initialize request is
    processed; callers must gate the emit on ``state.initialized`` so
    this check runs only once the handshake has populated it.
    """
    params = session.client_params
    if params is None:
        return False
    experimental = params.capabilities.experimental or {}
    return "claude/channel" in experimental


async def _emit_frame(session: ServerSession, frame: dict[str, Any]) -> None:
    """Emit the capability-gated claude/channel notification per frame.

    The spec-standard ``notifications/resources/updated`` is NOT emitted
    here: ``waitbus://event/{id}`` is not a subscribable URI, so firing
    ``resources/updated`` for it unconditionally would violate the MCP
    rule that a server MUST NOT send ``resources/updated`` for a resource
    the client has not subscribed to. The subscription-checked fan-out
    lives in ``_emit_to_subscribed_sessions``.

    Pre-init the claude/channel notification is queued in the session's
    bounded pending deque: the negotiated experimental capabilities are
    unavailable until the initialize handshake populates
    ``client_params``, so the capability check (and the send) is deferred
    to ``_flush_pending``.
    """
    emissions = _build_frame_emissions(frame)
    if not emissions:
        return
    state = _get_state(session)
    for content, meta, _event_id in emissions:
        if not state.initialized:
            _queue_pending_channel(state, content, meta)
            continue
        if _client_negotiated_channel(session):
            await emit_claude_channel(session, content, meta=meta)


# =============================================================
# WaitbusServer subclass, subscription registry, and per-session
# subscriber fan-out for the waitbus tool + resource surface.
# =============================================================


_sessions: weakref.WeakKeyDictionary[ServerSession, _SessionState] = weakref.WeakKeyDictionary()
"""Module-level subscription registry, keyed weakly on each ServerSession.

A weak key means a session dropped without an explicit cleanup pass
(daemon crash, abrupt stdio close) is reaped by GC rather than
leaking forever. ``_stream_events`` also performs explicit
``finally:`` cleanup as belt-and-suspenders.
"""


def _get_state(session: ServerSession) -> _SessionState:
    """Return the registry entry for ``session``, creating it on demand.

    Callers that mutate the entry (subscribe, unsubscribe, flush) must
    keep a strong reference to ``session`` while they operate, otherwise
    the WeakKeyDictionary may evict mid-operation. In practice every
    caller is inside a handler scoped to one live session.
    """
    state = _sessions.get(session)
    if state is None:
        state = _SessionState()
        _sessions[session] = state
    return state


async def _ping_session_uri(session: ServerSession, state: _SessionState, uri: str, frame_id: Any) -> None:
    """Emit (or pre-init queue) one resources/updated for ``uri`` on a session."""
    if not state.initialized:
        _queue_pending(state, uri, {"uri": uri, "frame_id": frame_id})
        return
    try:
        await session.send_resource_updated(AnyUrl(uri))
    except Exception as exc:
        logger.debug("resource_updated emit failed for %s: %s", uri, exc)


async def _emit_agent_doorbell(frame: dict[str, Any]) -> None:
    """Route an agent_message frame to the agent-doorbell subscribers.

    Per SWARM_DESIGN.md "Wildcard fan-out + dedup": each subscribed
    ``ServerSession`` receives EXACTLY ONE ``resources/updated`` ping for
    this message, regardless of how many ``waitbus://agent/...``
    subscriptions it holds, and ``waitbus://current`` subscribers are NOT
    pinged for an agent_message (the agent stream is partitioned out of
    the CI stream). ``agent_doorbell_uri_for_session`` resolves the single
    URI to ping (or None) for each session from its subscription set and
    the message's ``msg_to``.
    """
    fields = frame.get("fields") or {}
    msg_to = str(fields.get("msg_to") or "")
    if not msg_to:
        return
    frame_id = frame.get("event_id")
    for session, state in list(_sessions.items()):
        uri = agent_doorbell_uri_for_session(state.subscriptions, msg_to)
        if uri is None:
            continue
        await _ping_session_uri(session, state, uri, frame_id)


async def _emit_to_subscribed_sessions(
    frame: dict[str, Any],
) -> None:
    """Fan out one frame to every session subscribed to a matching URI.

    An ``agent_message`` frame is routed exclusively through the
    agent-doorbell path (one ping per session, CI subscribers excluded);
    every other frame fans out to the ``waitbus://current`` /
    ``waitbus://repo/...`` subscribers. For each matched (session, URI),
    emit one ``notifications/resources/updated``. Pre-init sessions queue
    the emission in their bounded deque; the flush pass replays them once
    the initialize handshake fires.
    """
    if frame.get("kind") in DRAINABLE_CONTROL_KINDS:
        return
    if frame.get("event_type") == AGENT_MESSAGE_EVENT_TYPE:
        # Agent messages NEVER fall through to the repo/current matcher:
        # they are partitioned out of the CI stream, so a current/repo
        # subscriber must not be pinged for one. The doorbell path owns
        # the per-session dedup.
        await _emit_agent_doorbell(frame)
        return
    owner = str(frame.get("owner", ""))
    repo = str(frame.get("repo", ""))
    if not owner or not repo:
        return

    # Snapshot to avoid mutation-during-iteration if a peer session
    # closes mid-fanout.
    for session, state in list(_sessions.items()):
        matched_uris = [uri for uri in state.subscriptions if _uri_matches_frame(uri, owner, repo)]
        for uri in matched_uris:
            await _ping_session_uri(session, state, uri, frame.get("event_id"))


def _mark_overflow_if_saturated(state: _SessionState) -> None:
    """Flip ``pending_overflowed`` when the bounded deque is at capacity.

    The deque is bounded; on overflow the oldest entry is dropped (deque
    semantics) and ``pending_overflowed`` flips True so the post-init
    flush can emit a synthetic truncated marker telling the client it has
    a gap to recover.
    """
    maxlen = state.pending.maxlen
    if maxlen is not None and len(state.pending) >= maxlen:
        state.pending_overflowed = True


def _queue_pending(state: _SessionState, uri: str, payload: dict[str, Any]) -> None:
    """Enqueue a pre-init ``resources/updated`` notification."""
    _mark_overflow_if_saturated(state)
    state.pending.append(_QueuedEmit(kind="resource_updated", uri=uri, payload=payload))


def _queue_pending_channel(state: _SessionState, content: str, meta: dict[str, str]) -> None:
    """Enqueue a pre-init ``claude/channel`` notification.

    Queued in the SAME bounded deque as ``resources/updated`` so the
    FIFO ordering across both notification methods matches the order the
    broadcast stream produced the frames. The capability check that gates
    this emit runs at flush time, by which point ``client_params`` is
    populated.
    """
    _mark_overflow_if_saturated(state)
    state.pending.append(_QueuedEmit(kind="claude_channel", content=content, meta=dict(meta)))


async def _flush_pending(session: ServerSession, state: _SessionState) -> None:
    """Drain the pre-init queue into the live session, plus overflow marker.

    Called once per session immediately after the registry observes the
    client's ``notifications/initialized``. Emits the queued
    notifications in FIFO order across BOTH methods:

    - ``resource_updated`` entries replay via the SDK typed helper.
    - ``claude_channel`` entries replay only when the client negotiated
      the ``claude/channel`` experimental capability. By flush time the
      handshake has populated ``client_params``, so this capability check
      is valid (unlike at queue time, when ``client_params`` is ``None``).

    Then a single truncated marker is emitted if any entry was evicted by
    the bounded queue.
    """
    channel_negotiated = _client_negotiated_channel(session)
    while state.pending:
        emit = state.pending.popleft()
        if emit.kind == "claude_channel":
            if channel_negotiated:
                with contextlib.suppress(Exception):
                    await emit_claude_channel(session, emit.content, meta=emit.meta)
            continue
        with contextlib.suppress(Exception):
            await session.send_resource_updated(AnyUrl(emit.uri))
    if state.pending_overflowed and channel_negotiated:
        state.pending_overflowed = False
        # Emit the spec-defined overflow signal via the channel side
        # (the spec has no resources/truncated method). Gated on the
        # client having negotiated claude/channel, same as every other
        # channel emit. Clients subscribed to waitbus://current will see
        # the gap and can replay via tail_events.
        marker = JSONRPCNotification(
            jsonrpc="2.0",
            method="notifications/claude/channel",
            params={
                "content": "[truncated] pre-init notification queue overflowed",
                "meta": {"kind": "truncated"},
            },
        )
        with contextlib.suppress(Exception):
            await session.send_message(SessionMessage(message=marker))  # type: ignore[arg-type]


# --- Tool implementations -------------------------------------------------


def _row_to_run_status(row: sqlite3.Row) -> dict[str, Any]:
    """Project an events row into the RunStatus shape consumed by clients.

    ``workflow_name``/``head_branch`` are attacker-influenceable (fork-PR
    workflow YAML / branch names) and are control-stripped; the structured
    fields (sha, status enum, ids) are payload-constrained and pass through.
    """
    return {
        "repo": _untrusted.strip_control(f"{row['owner']}/{row['repo']}"),
        "run_id": row["run_id"],
        "workflow_name": _untrusted.clean_opt(row["workflow_name"]),
        "head_branch": _untrusted.clean_opt(row["head_branch"]),
        "head_sha": row["head_sha"],
        "status": row["status"],
        "conclusion": row["conclusion"],
        "event_id": row["event_id"],
        "received_at": row["received_at"],
    }


def _row_to_job_status(row: sqlite3.Row) -> dict[str, Any]:
    """Project an events row into the JobStatus shape consumed by clients.

    ``job_name`` is attacker-influenceable (workflow YAML) and is
    control-stripped; ids/conclusion are payload-constrained.
    """
    return {
        "repo": _untrusted.strip_control(f"{row['owner']}/{row['repo']}"),
        "job_id": row["job_id"],
        "job_name": _untrusted.clean_opt(row["job_name"]),
        "parent_run_id": row["parent_run_id"],
        "conclusion": row["conclusion"],
        "event_id": row["event_id"],
        "received_at": row["received_at"],
    }


def _split_repo(repo: str | None) -> tuple[str, str] | None:
    """Split owner/repo or return None if the form is malformed."""
    if repo is None:
        return None
    parts = repo.split("/")
    if len(parts) != 2 or not all(parts):
        return None
    return parts[0], parts[1]


def _tool_get_ci_status_impl(repo: str | None) -> dict[str, Any]:
    """Query the events DB for the most recent workflow_run per repo.

    When ``repo`` is None, returns one RunStatus per (owner, repo) pair
    present in the events DB. When ``repo`` is set, returns at most one
    RunStatus for that repo (the newest workflow_run row).
    """
    db = _paths.db_path()
    runs: list[dict[str, Any]] = []
    if not db.exists():
        return {"runs": runs, "queried_at_ns": time.time_ns()}
    with _db.connect(db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        if (split := _split_repo(repo)) is not None:
            owner, name = split
            rows = conn.execute(
                "SELECT * FROM events WHERE event_type='workflow_run' "
                "AND owner=? AND repo=? "
                "ORDER BY received_at DESC LIMIT 1",
                (owner, name),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events e WHERE event_type='workflow_run' "
                "AND received_at = (SELECT MAX(received_at) FROM events "
                "WHERE event_type='workflow_run' AND owner=e.owner AND repo=e.repo) "
                "ORDER BY e.owner, e.repo"
            ).fetchall()
        runs = [_row_to_run_status(r) for r in rows]
    return {"runs": runs, "queried_at_ns": time.time_ns()}


def _tool_list_failed_jobs_impl(repo: str | None, limit: int) -> dict[str, Any]:
    """Query the events DB for the most recent failed workflow_job rows."""
    db = _paths.db_path()
    jobs: list[dict[str, Any]] = []
    if not db.exists():
        return {"jobs": jobs, "queried_at_ns": time.time_ns()}
    with _db.connect(db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        if (split := _split_repo(repo)) is not None:
            owner, name = split
            rows = conn.execute(
                "SELECT * FROM events WHERE event_type='workflow_job' "
                "AND conclusion='failure' AND owner=? AND repo=? "
                "ORDER BY received_at DESC LIMIT ?",
                (owner, name, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events WHERE event_type='workflow_job' "
                "AND conclusion='failure' "
                "ORDER BY received_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        jobs = [_row_to_job_status(r) for r in rows]
    return {"jobs": jobs, "queried_at_ns": time.time_ns()}


def _tool_get_pr_aggregate_impl(repo: str, pr_number: int) -> dict[str, Any]:
    """Aggregate every workflow_run / workflow_job event for one PR.

    The PR linkage is inferred from the payload_json's
    ``pull_requests[].number`` array — GitHub embeds the PR number(s)
    associated with each workflow run in the webhook payload. A miss
    on payload_json (older schema, missing field) yields an empty
    aggregate rather than raising.
    """
    queried_at = time.time_ns()
    split = _split_repo(repo)
    if split is None:
        raise ValueError(f"repo {repo!r} is not in owner/name form; get_pr_aggregate requires a fully-qualified repo")
    owner, name = split
    runs: list[dict[str, Any]] = []
    jobs: list[dict[str, Any]] = []
    db = _paths.db_path()
    if not db.exists():
        return {
            "repo": repo,
            "pr_number": pr_number,
            "runs": runs,
            "jobs": jobs,
            "queried_at_ns": queried_at,
        }
    with _db.connect(db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        all_rows = conn.execute(
            "SELECT * FROM events WHERE owner=? AND repo=? "
            "AND event_type IN ('workflow_run','workflow_job') "
            "ORDER BY event_id",
            (owner, name),
        ).fetchall()
    matched_run_ids: set[int] = set()
    for row in all_rows:
        payload_text = row["payload_json"] or ""
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        if _payload_matches_pr(payload, pr_number):
            if row["event_type"] == "workflow_run":
                runs.append(_row_to_run_status(row))
                if row["run_id"] is not None:
                    matched_run_ids.add(int(row["run_id"]))
            elif row["event_type"] == "workflow_job":
                jobs.append(_row_to_job_status(row))
        elif row["event_type"] == "workflow_job":
            # Jobs inherit the PR linkage of their parent run, which
            # may have been added to matched_run_ids above this row.
            parent = row["parent_run_id"]
            if parent is not None and int(parent) in matched_run_ids:
                jobs.append(_row_to_job_status(row))
    return {
        "repo": repo,
        "pr_number": pr_number,
        "runs": runs,
        "jobs": jobs,
        "queried_at_ns": queried_at,
    }


def _payload_matches_pr(payload: dict[str, Any], pr_number: int) -> bool:
    """Return True iff payload's pull_requests array contains pr_number."""
    workflow_run = payload.get("workflow_run") or payload
    prs = workflow_run.get("pull_requests")
    if not isinstance(prs, list):
        return False
    return any(isinstance(pr, dict) and pr.get("number") == pr_number for pr in prs)


def _event_type_filter(event_types: list[str] | None) -> tuple[str, tuple[Any, ...]]:
    """Build the event_type WHERE fragment + params for tail_events.

    Two modes, per SWARM_DESIGN.md "Event-stream partitioning":

    - ``event_types`` omitted (None): the DEFAULT lane EXCLUDES
      ``agent_message`` so a CI-watching agent never ingests agent
      cross-talk. Renders ``AND event_type != 'agent_message'``.
    - ``event_types`` given (a non-empty list): an explicit allow-list,
      rendered ``AND event_type IN (?, ?, ...)``. Passing
      ``["agent_message"]`` is how an agent opts INTO the chatter lane;
      passing ``["workflow_run"]`` narrows to CI. An empty list is treated
      as the default (exclude agent_message) rather than "match nothing",
      so a caller cannot accidentally silence the whole stream.
    """
    if not event_types:
        return " AND event_type != ?", (AGENT_MESSAGE_EVENT_TYPE,)
    placeholders = ", ".join(["?"] * len(event_types))
    return f" AND event_type IN ({placeholders})", tuple(event_types)


def _tail_events_read(
    repo: str | None,
    since_cursor: str | None,
    limit: int,
    event_types: list[str] | None = None,
) -> dict[str, Any]:
    """One-shot windowed read of events above ``since_cursor``.

    Pure synchronous DB read with no waiting; returns whatever rows are
    currently above the cursor. ``_tail_events_blocking`` calls this
    before and after the optional bounded wait.

    ``event_types`` partitions the stream (see :func:`_event_type_filter`):
    when omitted, ``agent_message`` rows are excluded so agent cross-talk
    stays off the default CI tail.
    """
    queried_at = time.time_ns()
    db = _paths.db_path()
    rows: list[dict[str, Any]] = []
    next_cursor: str | None = since_cursor
    if not db.exists():
        return {
            "events": rows,
            "next_cursor": next_cursor,
            "queried_at_ns": queried_at,
        }
    split = _split_repo(repo)
    type_sql, type_params = _event_type_filter(event_types)
    with _db.connect(db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        # The MCP tail cursor stays a public ULID; ordering and the resume
        # window are the internal daemon-assigned seq (translated from the
        # ULID via an exact lookup), so the tail is correct across producer
        # processes. next_cursor is
        # the last row's ULID, so the public contract is unchanged.
        since_seq = _db.seq_for_event_id(conn, since_cursor or "")
        if split is not None:
            owner, name = split
            sql = (
                "SELECT * FROM events WHERE event_id IS NOT NULL "
                "AND seq > ? AND owner=? AND repo=?" + type_sql + " ORDER BY seq LIMIT ?"
            )
            sql_rows = conn.execute(sql, (since_seq, owner, name, *type_params, limit)).fetchall()
        else:
            sql = "SELECT * FROM events WHERE event_id IS NOT NULL AND seq > ?" + type_sql + " ORDER BY seq LIMIT ?"
            sql_rows = conn.execute(sql, (since_seq, *type_params, limit)).fetchall()
        for r in sql_rows:
            rows.append(_event_row_to_dict(r))
        if rows:
            next_cursor = str(rows[-1]["event_id"])
    return {
        "events": rows,
        "next_cursor": next_cursor,
        "queried_at_ns": queried_at,
    }


def _subscribe_event_types(event_types: list[str] | None) -> list[str]:
    """Map the read-side event_types filter to a daemon subscribe allow-list.

    The broadcast subscribe envelope's ``event_types`` is an ALLOW-list
    (the daemon ships only matching frames). The read-side default
    EXCLUDES ``agent_message``; the daemon has no exclude grammar, so the
    default is expressed positively as "every supported type except
    agent_message". An explicit read-side list is passed through verbatim,
    intersected with the supported set so an unknown type cannot empty the
    envelope (the daemon rejects a subscribe whose event_types yield zero
    recognized values).
    """
    supported = event_types_supported()
    if not event_types:
        keep = sorted(supported - {AGENT_MESSAGE_EVENT_TYPE})
        # Defensive: if agent_message were ever the ONLY supported type the
        # exclude would empty the envelope; fall back to the full set so the
        # long-poll still wakes (the re-read still applies the exclude).
        return keep or sorted(supported)
    requested = sorted(set(event_types) & supported)
    return requested or sorted(supported)


def _tail_events_blocking(
    repo: str | None,
    since_cursor: str | None,
    limit: int,
    max_wait_seconds: int,
    event_types: list[str] | None = None,
) -> dict[str, Any]:
    """Windowed read with a bounded long-poll.

    Reads immediately; if the window is empty and ``max_wait_seconds``
    is positive, subscribes to the broadcast daemon and blocks (via the
    shared ``await_predicate`` engine) until one matching frame arrives
    or the deadline elapses, then re-reads so the durable DB row is what
    is returned (the frame is only the wake signal -- the response shape
    is unchanged).

    BLOCKING by design. The MCP ``_call_tool`` path runs this inside
    ``asyncio.to_thread`` so the server's single event loop stays
    responsive; ``await_predicate`` self-enforces ``max_wait_seconds``
    (it is built on the bounded select+deadline loop, NOT an unbounded
    ``recv``), so the worker thread is guaranteed to terminate by the
    deadline with no leak even though a ``to_thread`` worker cannot be
    cancel-killed. If the broadcast daemon is unreachable the call
    degrades to the immediate one-shot read (the durable DB is the
    source of truth; the daemon is only the wake optimisation).
    """
    if max_wait_seconds > TAIL_EVENTS_MAX_WAIT_CAP_SECONDS:
        raise ValueError(
            f"max_wait_seconds={max_wait_seconds} exceeds the cap of "
            f"{TAIL_EVENTS_MAX_WAIT_CAP_SECONDS}s (Cursor client cancels at 5min)"
        )
    result = _tail_events_read(repo, since_cursor, limit, event_types)
    if result["events"] or max_wait_seconds <= 0:
        return result

    split = _split_repo(repo)
    filters = [f"{split[0]}/{split[1]}"] if split is not None else None
    # Mirror the read-side event_type partition onto the broadcast
    # subscription's envelope so the long-poll only WAKES on a frame the
    # re-read would actually return. Without this, a default tail
    # (agent_message excluded) would wake on an agent_message frame and
    # then re-read empty -- a spurious wake. The daemon envelope is an
    # allow-list, so the default (exclude agent_message) is expressed by
    # asking for every supported type EXCEPT agent_message via the same
    # filter helper the read uses, sharing one partition definition.
    event_types_filter = _subscribe_event_types(event_types)
    try:
        sub = open_subscriber(filters=filters, since=since_cursor, event_types=event_types_filter)
    except BroadcastConnectionError:
        # Daemon unreachable or version/lag-rejected: either way the MCP
        # tool degrades gracefully to the durable one-shot DB read already
        # done above. ProtocolVersionError / SubscriberLaggedError are
        # BroadcastConnectionError subclasses, so the base catches them.
        return result

    def _decide(_frame: dict[str, Any]) -> FrameDecision:
        # Any real (non-heartbeat) frame matching the subscription is a
        # wake signal; the durable row is fetched by the re-read below.
        return FrameDecision.MATCHED

    try:
        await_predicate(
            sub,
            decide=_decide,
            deadline_seconds=float(max_wait_seconds),
            idle_reset=False,
        )
    finally:
        sub.sock.close()
    # Re-read regardless of the outcome (matched / timed_out / closed):
    # the wait only optimises latency; the DB is authoritative.
    return _tail_events_read(repo, since_cursor, limit, event_types)


# --- Agent-message tools (emit_agent_message / read_agent_messages) -------


def _emit_agent_message_impl(to: str, body: str, from_agent: str, thread_id: str | None) -> dict[str, Any]:
    """Commit one agent_message row and return its committed identity.

    The event_type (``agent_message``) and source (``agent``) are
    HARDCODED here -- they are not tool inputs, so the model has exactly
    one typed, validated lane it cannot fat-finger into the CI stream
    (SWARM_DESIGN.md "Emit"). The body rides ``msg_body`` (the lean wire
    frame drops ``payload_json``, so the body could not otherwise reach the
    recipient); a fresh ULID correlation id is stamped so every message is
    uniquely referenceable, and ``thread_id`` maps to ``msg_thread``.

    Identity (``msg_from``/``msg_to``) is self-asserted under the same-UID
    trust model -- no spoof protection (SWARM_DESIGN.md "Out of scope").
    """
    from ._emit import emit
    from ._types import EventInsert

    correlation_id = _ulid.new()
    # delivery_id is the idempotency token; a fresh correlation_id per call
    # makes it unique, so re-issuing the same logical send is a new row
    # (fire-and-forget messages are distinct events, not idempotent retries
    # of one delivery the way a webhook re-POST is).
    result = emit(
        EventInsert(
            delivery_id=f"agent:{from_agent}:msg:{correlation_id}",
            source=AGENT_MESSAGE_SOURCE,
            event_type=AGENT_MESSAGE_EVENT_TYPE,
            owner="local",
            repo="agents",
            received_at=time.time_ns(),
            payload_json="{}",
            ingest_method="api",
            msg_to=to,
            msg_from=from_agent,
            msg_correlation_id=correlation_id,
            msg_thread=thread_id,
            msg_body=body,
        )
    )
    return {
        "event_id": result.event.event_id,
        "inserted": result.inserted,
        "queried_at_ns": time.time_ns(),
    }


def _row_to_agent_message(row: sqlite3.Row) -> dict[str, Any]:
    """Project an events row into the AgentMessage shape.

    The ``msg_*`` addressing fields are self-asserted free text and are
    routed through :func:`_untrusted.clean_opt` exactly like every other
    untrusted facet before reaching an LLM.
    """
    return {
        "event_id": row["event_id"] or "",
        "msg_to": _untrusted.clean_opt(row["msg_to"]),
        "msg_from": _untrusted.clean_opt(row["msg_from"]),
        "msg_body": _untrusted.clean_opt(row["msg_body"]),
        "msg_thread": _untrusted.clean_opt(row["msg_thread"]),
        "msg_correlation_id": _untrusted.clean_opt(row["msg_correlation_id"]),
        "received_at": row["received_at"],
    }


def _read_agent_messages_impl(agent: str, since_cursor: str | None, limit: int) -> dict[str, Any]:
    """Cursor-paginated read of this agent's messages above ``since_cursor``.

    Returns ``agent_message`` rows whose ``msg_to`` is the caller's agent
    name OR the ``*`` broadcast lane, ordered oldest-first by the
    daemon-assigned commit order (``seq``) so the cursor advances
    monotonically. This is the PULL side of the doorbell: the doorbell
    fires a ``resources/updated`` ping, and the client drains the delta
    here -- nothing re-delivers history (SWARM_DESIGN.md "Receive").
    """
    queried_at = time.time_ns()
    db = _paths.db_path()
    rows: list[dict[str, Any]] = []
    next_cursor: str | None = since_cursor
    if not db.exists():
        return {"messages": rows, "next_cursor": next_cursor, "queried_at_ns": queried_at}
    with _db.connect(db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        since_seq = _db.seq_for_event_id(conn, since_cursor or "")
        sql_rows = conn.execute(
            "SELECT * FROM events WHERE event_id IS NOT NULL AND seq > ? "
            "AND event_type = ? AND (msg_to = ? OR msg_to = ?) "
            "ORDER BY seq LIMIT ?",
            (since_seq, AGENT_MESSAGE_EVENT_TYPE, agent, AGENT_BROADCAST_RECIPIENT, limit),
        ).fetchall()
        for r in sql_rows:
            rows.append(_row_to_agent_message(r))
        if rows:
            next_cursor = str(rows[-1]["event_id"])
    return {"messages": rows, "next_cursor": next_cursor, "queried_at_ns": queried_at}


def _event_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Project an events row into the EventRow shape (drops payload_json).

    Driven by the single column catalogue in :mod:`._columns`: every column
    flagged ``in_mcp_dict`` is emitted, and every column flagged ``untrusted``
    (attacker-influenceable free text -- ``workflow_name``/``head_branch``/
    ``job_name``/the ``alert_*`` and agent ``msg_*`` facets) is routed through
    :func:`_untrusted.clean_opt`; ids/enums/methods pass through unchanged. A
    new column facet is covered by construction, so it cannot be silently
    dropped from the projection or left uncleaned.
    """
    present = set(row.keys())
    projected: dict[str, Any] = {
        col.name: (_untrusted.clean_opt(row[col.name]) if col.untrusted else row[col.name])
        for col in _columns.MCP_DICT_COLUMNS
        if col.name in present
    }
    # event_id is the one required (non-optional) EventRow field; the schema
    # permits NULL event_id on legacy rows (the unique index is partial), so
    # coalesce None to "" to satisfy the EventRow contract.
    if projected.get("event_id") is None:
        projected["event_id"] = ""
    return projected


def _summarise_runs(runs: list[dict[str, Any]]) -> str:
    """Render a one-line human summary for the get_ci_status content text."""
    if not runs:
        return "No workflow_run events recorded."
    parts = []
    for run in runs[:5]:
        parts.append(f"{run['repo']}: {run.get('workflow_name') or '?'} [{run.get('conclusion') or 'pending'}]")
    more = f" (+{len(runs) - 5} more)" if len(runs) > 5 else ""
    return "; ".join(parts) + more


# --- WaitbusServer subclass ----------------------------------------------


class WaitbusServer(Server[Any, Any]):
    """Server subclass that propagates the resources.subscribe capability.

    Works around an upstream mcp-python-sdk hardcode (at server.py:212
    in the pinned v1.27.1 release) that pins resources.subscribe=False
    regardless of whether a subscribe_resource handler is registered.
    Per the MCP 2025-06-18 lifecycle MUST clause around using only
    negotiated capabilities, a server that emits
    notifications/resources/updated without first advertising
    subscribe=true is non-conformant.

    Upstream state (verified 2026-05-19): the fix landed on the SDK's
    ``main`` branch as commit ``fa9c59b`` ("fix: advertise subscribe
    capability when handler is registered", authored 2026-02-10) and
    was later refactored into commit ``0a22a9d`` (PR #1985). Neither
    commit is in any released SDK version yet -- ``git tag --contains
    fa9c59b`` returns empty, and the latest release ``v1.27.1``
    (2026-05-08) was cut from the ``v1.x`` maintenance branch which
    has not received the backport. The fix reaches PyPI users only
    when a release that includes it is cut.

    Installing from main is not viable: PyPI rejects published
    distributions whose ``Requires-Dist`` references a git URL,
    so the waitbus published artifact must pin to a PyPI-resolvable
    version. The subclass override is the in-tree path until a fixed
    release ships.

    Removal criteria (do all together):

    1. Confirm a released ``mcp`` version on PyPI contains either
       ``fa9c59b`` or its ``0a22a9d`` refactor. Verify with
       ``git -C <sdk-clone> tag --contains fa9c59b`` and check the
       released version actually exposes the derivation
       (``subscribe="resources/subscribe" in self._request_handlers``
       in ``server.py::get_capabilities``).
    2. Bump the ``mcp`` pin in ``pyproject.toml`` to require that
       version (e.g. ``mcp>=1.28,<2.0``).
    3. Delete this subclass, drop its imports, and have
       ``build_server()`` return a bare ``Server``.
    4. Re-run the MCP surface tests: ``tests/test_mcp.py``,
       ``tests/test_mcp_surface.py``, ``tests/test_mcp_event_payload_cap.py``.
    """

    def get_capabilities(
        self,
        notification_options: NotificationOptions,
        experimental_capabilities: dict[str, dict[str, Any]],
    ) -> types.ServerCapabilities:
        caps = super().get_capabilities(notification_options, experimental_capabilities)
        if types.SubscribeRequest in self.request_handlers and caps.resources is not None:
            caps.resources = types.ResourcesCapability(
                subscribe=True,
                listChanged=caps.resources.listChanged,
            )
        return caps


# --- Handler registration -------------------------------------------------


async def _subscribe_handler(uri: AnyUrl) -> None:
    """Record a session's subscription to a waitbus:// URI.

    Per the design, the broadcast daemon's reachability is verified
    before accepting the subscription so the client receives a clear
    error rather than a silent no-emit subscription when the daemon
    is down.
    """
    uri_str = str(uri)
    if not is_subscribable_uri(uri_str):
        raise ValueError(
            f"URI {uri_str!r} is not subscribable; use {URI_CURRENT}, "
            f"{URI_REPO_PREFIX}{{owner}}/{{repo}}, or {URI_AGENT_PREFIX}{{name}}"
        )
    if not _paths.broadcast_socket().exists():
        raise RuntimeError(
            "broadcast daemon socket is unreachable at "
            f"{_paths.broadcast_socket()}; start it via "
            "`systemctl --user start waitbus-broadcast.service` "
            "before subscribing"
        )
    # The current SDK lowlevel handler signature drops the session
    # reference (the Server.run path uses contextvars). We rely on the
    # session being addressable through the request_context.
    try:
        from mcp.server.lowlevel.server import request_ctx

        ctx = request_ctx.get()
        session = ctx.session
    except (LookupError, AttributeError):
        # No request context available (tests, direct invocation).
        return
    state = _get_state(session)
    state.subscriptions.add(uri_str)


async def _unsubscribe_handler(uri: AnyUrl) -> None:
    """Drop a session's subscription to a waitbus:// URI."""
    uri_str = str(uri)
    try:
        from mcp.server.lowlevel.server import request_ctx

        ctx = request_ctx.get()
        session = ctx.session
    except (LookupError, AttributeError):
        return
    state = _sessions.get(session)
    if state is None:
        return
    state.subscriptions.discard(uri_str)


def _read_event_row(ulid: str, uri_str: str) -> sqlite3.Row:
    """Fetch a single events row by ULID for the read_resource path.

    Extracted so the waitbus://event/{ulid} (capped) and
    waitbus://event/{ulid}/raw (uncapped) branches share the lookup and
    error semantics — both raise ValueError with the originating URI
    in the message so a misuse surfaces at the resource boundary
    rather than as a NoneType attribute error deeper in.
    """
    db = _paths.db_path()
    if not db.exists():
        raise ValueError(f"events DB missing; cannot read {uri_str}")
    with _db.connect(db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM events WHERE event_id=?",
            (ulid,),
        ).fetchone()
    if row is None:
        raise ValueError(f"no event with id {ulid!r}")
    return row  # type: ignore[no-any-return]


def _agent_doorbell_stub(agent_name: str, uri_str: str) -> list[ReadResourceContents]:
    """Return the doorbell read stub for ``waitbus://agent/{name}``.

    The doorbell read returns ONLY a stub, never the inbox. A
    ``resources/read`` has no cursor and no ack, so returning the messages
    here would dump the whole durable history on every read and exhaust the
    model's context. The stub directs the client to the cursor-paginated
    ``read_agent_messages`` tool (SWARM_DESIGN.md "Receive -- a cursor
    tool, NOT a resource").
    """
    stub = {
        "agent": agent_name,
        "doorbell": uri_str,
        "instruction": (
            "This is a doorbell, not an inbox. Call the read_agent_messages "
            f"tool with agent={agent_name!r} (and your last since_cursor) to "
            "pull new messages addressed to you or to '*'. Reading this "
            "resource never returns message bodies."
        ),
        "read_tool": TOOL_READ_AGENT_MESSAGES,
    }
    return [ReadResourceContents(content=json.dumps(stub, indent=2, default=str), mime_type="application/json")]


async def _read_resource_handler(uri: AnyUrl) -> list[ReadResourceContents]:
    """Synthesise a JSON snapshot for a waitbus:// URI.

    - ``waitbus://current`` returns the latest get_ci_status result.
    - ``waitbus://repo/{owner}/{repo}`` returns get_ci_status filtered to
      that repo (wildcards are rejected on the read path; wildcards
      are subscription-only).
    - ``waitbus://event/{ulid}`` returns the single matching event row
      including payload_json.
    - ``waitbus://agent/{name}`` returns the read_agent_messages stub
      (the doorbell, never the inbox).
    """
    uri_str = str(uri)
    if not is_readable_uri(uri_str):
        raise ValueError(f"URI {uri_str!r} is not in the waitbus:// scheme")
    if uri_str == URI_CURRENT:
        snapshot = _tool_get_ci_status_impl(repo=None)
        return [
            ReadResourceContents(
                content=json.dumps(snapshot, indent=2, default=str),
                mime_type="application/json",
            )
        ]
    if (raw_ulid := parse_event_raw_uri(uri_str)) is not None:
        # Opt-in uncapped sibling. Discovery contract: marker-only — this
        # URI does NOT appear in list_resources or list_resource_templates,
        # so the only way an agent reaches it is by following the raw_uri
        # field on a truncation marker (or by reading waitbus source).
        row = _read_event_row(raw_ulid, uri_str)
        body = _event_row_to_dict(row)
        body["payload_json"] = _untrusted.fence(row["payload_json"] or "", label="raw-webhook-payload")
        return [
            ReadResourceContents(
                content=json.dumps(body, indent=2, default=str),
                mime_type="application/json",
            )
        ]
    if (ev_ulid := parse_event_uri(uri_str)) is not None:
        row = _read_event_row(ev_ulid, uri_str)
        body = _event_row_to_dict(row)
        # The raw webhook payload is wholly attacker-controlled (PR
        # title/body, actor, commit messages). It is exposed for
        # debugging but fenced as a single untrusted string rather than
        # spliced back as a live JSON object an agent might treat as
        # authoritative or instruction-bearing.
        fenced = _untrusted.fence(row["payload_json"] or "", label="raw-webhook-payload")
        fenced_bytes = fenced.encode("utf-8")
        if len(fenced_bytes) > _EVENT_PAYLOAD_CAP_BYTES:
            # Truncate at the byte boundary; ``errors="replace"`` turns a
            # split multi-byte sequence at the cap into U+FFFD rather than
            # raising UnicodeDecodeError. The marker itself is waitbus-
            # generated JSON (not webhook-controlled text) and so skips
            # the fence wrapping — fencing is hygiene for attacker text,
            # orthogonal to size, and applying it to a trusted dict would
            # confuse the consumer about provenance.
            preview = fenced_bytes[:_EVENT_PAYLOAD_CAP_BYTES].decode("utf-8", errors="replace")
            body["payload_json"] = {
                "truncated": True,
                "full_size_bytes": len(fenced_bytes),
                "raw_uri": f"{URI_EVENT_PREFIX}{ev_ulid}/raw",
                "fenced_preview": preview,
            }
        else:
            body["payload_json"] = fenced
        return [
            ReadResourceContents(
                content=json.dumps(body, indent=2, default=str),
                mime_type="application/json",
            )
        ]
    if (agent_name := parse_agent_uri(uri_str)) is not None:
        return _agent_doorbell_stub(agent_name, uri_str)
    if (parsed := parse_repo_uri(uri_str)) is not None:
        owner, name = parsed
        if "*" in (owner, name):
            raise ValueError("wildcard repo URIs are subscription-only; read_resource requires a concrete owner/repo")
        snapshot = _tool_get_ci_status_impl(repo=f"{owner}/{name}")
        return [
            ReadResourceContents(
                content=json.dumps(snapshot, indent=2, default=str),
                mime_type="application/json",
            )
        ]
    raise ValueError(f"unhandled URI {uri_str!r}")


def _escape_like_prefix(value: str) -> str:
    r"""Escape LIKE metacharacters so the value is matched as a literal prefix.

    ``\``, ``%`` and ``_`` are escaped with a backslash; the query uses
    ``ESCAPE '\'``. Without this, an attacker-influenceable ``owner`` like
    ``a%`` would widen the scan to every owner starting with ``a``.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _completion_query(
    arg_name: str,
    like: str,
    fetch: int,
    prior_owner: str | None,
) -> tuple[str, tuple[Any, ...], str]:
    """Return (sql, params, column) for one completion argument.

    Pure dispatch table over the three completable arguments. Every value
    is a bound parameter; the LIKE pattern is pre-escaped by the caller
    with ``ESCAPE '\\'``. ``repo`` is scoped to ``prior_owner`` when an
    RFC-6570 prior-resolved ``{owner}`` is present, else completed
    globally. Indexes used: ``idx_owner_repo_event`` (owner/repo prefix
    scans) and the partial unique ``idx_event_id`` (ulid scan).
    """
    if arg_name == "owner":
        return (
            "SELECT DISTINCT owner FROM events WHERE owner LIKE ? ESCAPE '\\' ORDER BY owner LIMIT ?",
            (like, fetch),
            "owner",
        )
    if arg_name == "repo":
        if prior_owner:
            return (
                "SELECT DISTINCT repo FROM events WHERE owner=? AND repo LIKE ? ESCAPE '\\' ORDER BY repo LIMIT ?",
                (prior_owner, like, fetch),
                "repo",
            )
        return (
            "SELECT DISTINCT repo FROM events WHERE repo LIKE ? ESCAPE '\\' ORDER BY repo LIMIT ?",
            (like, fetch),
            "repo",
        )
    # "ulid": newest-first over the partial-unique event_id index.
    return (
        "SELECT event_id FROM events "
        "WHERE event_id IS NOT NULL AND event_id LIKE ? ESCAPE '\\' "
        "ORDER BY event_id DESC LIMIT ?",
        (like, fetch),
        "event_id",
    )


def _is_completable(
    ref: types.PromptReference | types.ResourceTemplateReference,
    argument: types.CompletionArgument,
) -> bool:
    """True iff ``ref`` is one of the two advertised resource templates and
    ``argument.name`` is a completable argument of that template.

    A ``PromptReference`` (waitbus advertises no prompts) and any unknown
    template/argument pair are non-completable, so the caller returns
    ``None`` and the SDK supplies its empty-completion default.
    """
    if not isinstance(ref, types.ResourceTemplateReference):
        return False
    arg_names = _COMPLETABLE_TEMPLATES.get(ref.uri)
    return arg_names is not None and argument.name in arg_names


async def _complete_resource_template(
    ref: types.PromptReference | types.ResourceTemplateReference,
    argument: types.CompletionArgument,
    context: types.CompletionContext | None,
) -> types.Completion | None:
    """RFC-6570 argument completion for the waitbus resource templates.

    Dispatches on ``ref`` being a ``ResourceTemplateReference`` whose
    ``ref.uri`` is one of the two advertised ``uriTemplate`` strings
    (PromptReference and unknown templates/arguments return ``None`` so the
    SDK falls back to its empty-completion default). Every returned value is
    a DISTINCT, prefix-filtered column read from the read-only events DB and
    passed through ``_untrusted.strip_control`` — ``owner``/``repo`` are
    attacker-influenceable webhook free text, so a value that strips to
    empty is dropped rather than surfaced as a completion.
    """
    if not _is_completable(ref, argument):
        return None

    db = _paths.db_path()
    if not db.exists():
        # Completion fired against a config where the events DB is not
        # provisioned (operator hasn't run `waitbus init`, OR state was
        # wiped after the daemon started). One WARN per occurrence so
        # this is operator-visible. Other early-return paths (unknown
        # ref / unknown arg) stay silent: completion is keystroke-paced
        # and logging every routine prefix-miss would be anti-signal.
        structured(
            logger,
            logging.WARNING,
            "completion_db_missing",
            db_path=str(db),
            argument=argument.name,
        )
        return types.Completion(values=[], total=0, hasMore=False)

    like = _escape_like_prefix(argument.value) + "%"
    fetch = _COMPLETION_LIMIT + 1
    prior_owner = (context.arguments or {}).get("owner") if context else None
    sql, params, column = _completion_query(argument.name, like, fetch, prior_owner)
    with _db.connect(db, readonly=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    raw = [r[column] for r in rows]

    has_more = len(raw) > _COMPLETION_LIMIT
    raw = raw[:_COMPLETION_LIMIT]
    # MANDATORY: owner/repo are attacker-influenceable webhook free text.
    # Strip control/ANSI/zero-width carriers (same field-level seam as
    # _row_to_run_status/_row_to_job_status) and drop anything that
    # sanitises to empty so an injection-shaped value cannot surface.
    values = [cleaned for value in raw if (cleaned := _untrusted.strip_control(value))]
    return types.Completion(
        values=values,
        total=None if has_more else len(values),
        hasMore=has_more,
    )


def _tool_definitions() -> list[types.Tool]:
    """Return the static Tool list advertised on tools/list.

    Every entry sets ``title`` (VS Code/Cursor prefer rendering title
    over name), an ``inputSchema``, and an
    ``outputSchema`` that matches the structured payload emitted by
    each tool implementation.
    """
    return [
        types.Tool(
            name=TOOL_GET_CI_STATUS,
            title="Get CI status",
            description=(
                "Return the latest workflow_run state for one repo (or every configured repo when repo is null)."
            ),
            inputSchema=schema_input_get_ci_status(),
            outputSchema=schema_ci_status(),
            # Pure read of the events DB with no side effects, and the same
            # arguments always yield the same row set for a given DB state.
            annotations=types.ToolAnnotations(readOnlyHint=True, idempotentHint=True),
        ),
        types.Tool(
            name=TOOL_LIST_FAILED_JOBS,
            title="List failed jobs",
            description=("Return recent failing workflow_job rows, capped at limit."),
            inputSchema=schema_input_list_failed_jobs(),
            outputSchema=schema_failed_jobs(),
            # Pure read; idempotent for a fixed DB state.
            annotations=types.ToolAnnotations(readOnlyHint=True, idempotentHint=True),
        ),
        types.Tool(
            name=TOOL_GET_PR_AGGREGATE,
            title="Get PR run aggregate",
            description=(
                "Aggregate every workflow_run and workflow_job event associated with one pull-request number."
            ),
            inputSchema=schema_input_get_pr_aggregate(),
            outputSchema=schema_pr_aggregate(),
            # Pure read; idempotent for a fixed DB state.
            annotations=types.ToolAnnotations(readOnlyHint=True, idempotentHint=True),
        ),
        types.Tool(
            name=TOOL_TAIL_EVENTS,
            title="Tail events",
            description=(
                "One-shot windowed read of events above an opaque cursor. "
                "Returns events plus next_cursor. max_wait_seconds is "
                "capped at 270s to stay below Cursor's 5-minute cancel. "
                "agent_message rows are excluded unless event_types "
                "explicitly requests them."
            ),
            inputSchema=schema_input_tail_events(),
            outputSchema=schema_tail_events(),
            # Read-only, but a windowed cursor read can advance the window
            # and the bounded long-poll makes repeat calls observe new rows,
            # so idempotentHint is intentionally left unset.
            annotations=types.ToolAnnotations(readOnlyHint=True),
        ),
        types.Tool(
            name=TOOL_EMIT_AGENT_MESSAGE,
            title="Send agent message",
            description=(
                "Send one agent-to-agent message addressed to an agent name "
                "(or '*' to broadcast). Hardcodes event_type=agent_message so "
                "the message rides the typed lane, never the CI stream."
            ),
            inputSchema=schema_input_emit_agent_message(),
            outputSchema=schema_emit_agent_message(),
            # This tool WRITES a row -- it is not read-only and is not
            # idempotent (every send is a distinct message).
            annotations=types.ToolAnnotations(readOnlyHint=False),
        ),
        types.Tool(
            name=TOOL_READ_AGENT_MESSAGES,
            title="Read agent messages",
            description=(
                "Cursor-paginated read of messages addressed to this agent "
                "(or to '*'). Returns messages plus next_cursor; nothing "
                "re-delivers history. The pull side of the agent doorbell."
            ),
            inputSchema=schema_input_read_agent_messages(),
            outputSchema=schema_read_agent_messages(),
            # Read-only; the cursor advances on repeat calls so idempotentHint
            # is intentionally left unset (same reasoning as tail_events).
            annotations=types.ToolAnnotations(readOnlyHint=True),
        ),
    ]


def _register_handlers(server: WaitbusServer) -> None:
    """Wire every tool and resource handler onto the server.

    Called once from ``build_server``. The SDK already registers a
    ping handler in ``Server.__init__`` so we do NOT double-register
    ping here.
    """
    tools = _tool_definitions()
    tool_index = {t.name: t for t in tools}

    # The SDK's lowlevel decorators are not typed (Callable returns
    # Any). We accept that locally rather than blanket-suppressing
    # untyped-call across the module.

    @server.list_tools()  # type: ignore[no-untyped-call, untyped-decorator]
    async def _list_tools() -> list[types.Tool]:
        return tools

    @server.call_tool()  # type: ignore[untyped-decorator]
    async def _call_tool(name: str, arguments: dict[str, Any]) -> tuple[list[types.TextContent], dict[str, Any]]:
        if name == TOOL_GET_CI_STATUS:
            result = _tool_get_ci_status_impl(arguments.get("repo"))
            human = _summarise_runs(result["runs"])
        elif name == TOOL_LIST_FAILED_JOBS:
            result = _tool_list_failed_jobs_impl(
                arguments.get("repo"),
                int(arguments.get("limit", LIST_FAILED_JOBS_DEFAULT_LIMIT)),
            )
            jobs = result["jobs"]
            human = f"{len(jobs)} failed job(s)" if jobs else "No failed workflow_job events recorded."
        elif name == TOOL_GET_PR_AGGREGATE:
            result = _tool_get_pr_aggregate_impl(
                str(arguments["repo"]),
                int(arguments["pr_number"]),
            )
            human = (
                f"PR #{result['pr_number']} on {result['repo']}: "
                f"{len(result['runs'])} run(s), {len(result['jobs'])} job(s)"
            )
        elif name == TOOL_TAIL_EVENTS:
            # _tail_events_blocking can block up to max_wait_seconds
            # (capped at 270s). Run it in a worker thread so the MCP
            # server's single event loop keeps answering concurrent
            # tools/list / notifications. await_predicate self-enforces
            # the deadline so the worker terminates on time (no leak).
            result = await asyncio.to_thread(
                _tail_events_blocking,
                arguments.get("repo"),
                arguments.get("since_cursor"),
                int(arguments.get("limit", TAIL_EVENTS_DEFAULT_LIMIT)),
                int(arguments.get("max_wait_seconds", TAIL_EVENTS_DEFAULT_MAX_WAIT_SEC)),
                arguments.get("event_types"),
            )
            human = f"{len(result['events'])} event(s) read; next_cursor={result.get('next_cursor')}"
        elif name == TOOL_EMIT_AGENT_MESSAGE:
            result = _emit_agent_message_impl(
                str(arguments["to"]),
                str(arguments["body"]),
                str(arguments["from_agent"]),
                arguments.get("thread_id"),
            )
            human = f"agent_message {result['event_id']} sent to {arguments['to']!r}"
        elif name == TOOL_READ_AGENT_MESSAGES:
            result = _read_agent_messages_impl(
                str(arguments["agent"]),
                arguments.get("since_cursor"),
                int(arguments.get("limit", READ_AGENT_MESSAGES_DEFAULT_LIMIT)),
            )
            human = f"{len(result['messages'])} message(s) read; next_cursor={result.get('next_cursor')}"
        else:
            raise ValueError(f"unknown tool {name!r}")
        if name not in tool_index:  # pragma: no cover  - defensive
            raise ValueError(f"tool {name!r} is not advertised")
        return ([types.TextContent(type="text", text=human)], result)

    @server.list_resources()  # type: ignore[no-untyped-call, untyped-decorator]
    async def _list_resources() -> list[types.Resource]:
        return [
            types.Resource(
                uri=AnyUrl(URI_CURRENT),
                name="current",
                title="Current CI status",
                description="Aggregate latest workflow_run state across configured filters.",
                mimeType="application/json",
            ),
        ]

    @server.list_resource_templates()  # type: ignore[no-untyped-call, untyped-decorator]
    async def _list_resource_templates() -> list[types.ResourceTemplate]:
        # Built FROM URI_REPO_PREFIX / URI_EVENT_PREFIX so the advertised
        # templates can never drift from parse_repo_uri / parse_event_uri.
        # waitbus://current is a concrete URI (not a template) and stays in
        # _list_resources only.
        return [
            types.ResourceTemplate(
                uriTemplate=_TEMPLATE_REPO,
                name="repo",
                title="Per-repo CI status",
                description=(
                    "Latest workflow_run state for one owner/repo. "
                    "Subscribable; wildcards ({owner}/* , */*) are "
                    "subscription-only and rejected on read."
                ),
                mimeType="application/json",
            ),
            types.ResourceTemplate(
                uriTemplate=_TEMPLATE_EVENT,
                name="event",
                title="Single event",
                description=(
                    "Read-only snapshot of one stored event row by its "
                    "opaque ULID, including the fenced raw webhook payload."
                ),
                mimeType="application/json",
            ),
            types.ResourceTemplate(
                uriTemplate=_TEMPLATE_AGENT,
                name="agent",
                title="Agent message doorbell",
                description=(
                    "Subscribe to be pinged (resources/updated) when an "
                    "agent_message addressed to {name} (or '*') is committed. "
                    "Reading this resource returns only a stub directing you "
                    "to the read_agent_messages tool -- never the messages "
                    "themselves (that would dump the whole inbox)."
                ),
                mimeType="application/json",
            ),
        ]

    @server.list_prompts()  # type: ignore[no-untyped-call, untyped-decorator]
    async def _list_prompts() -> list[types.Prompt]:
        # VS Code Copilot (microsoft/vscode#300262) and a handful of
        # other clients probe prompts/list during discovery; returning
        # method-not-found surfaces as a user-facing error banner.
        # Registering an empty handler is the spec-conformant suppress.
        return []

    server.read_resource()(_read_resource_handler)  # type: ignore[no-untyped-call]
    server.subscribe_resource()(_subscribe_handler)  # type: ignore[no-untyped-call]
    server.unsubscribe_resource()(_unsubscribe_handler)  # type: ignore[no-untyped-call]
    server.completion()(_complete_resource_template)  # type: ignore[no-untyped-call]


async def _stream_events(session: ServerSession) -> None:
    """Long-running subscriber: connect, subscribe, fan-out, retry.

    Wrapped in an outer try/finally so the per-session registry entry
    is popped on subscriber cancellation. The WeakKeyDictionary would
    eventually reclaim it via GC but explicit cleanup prevents
    accumulation across long-lived sessions that reconnect repeatedly.
    """
    try:
        await _stream_events_loop(session)
    finally:
        # Explicit cleanup so a long-lived registry does not retain a
        # stale per-session entry after the subscriber task exits.
        _sessions.pop(session, None)


async def _stream_events_loop(session: ServerSession) -> None:
    """Core reconnect loop body, factored so the outer cleanup is unambiguous."""
    backoff = _BACKOFF_INITIAL_S

    while True:
        if not _paths.broadcast_socket().exists():
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX_S)
            continue

        try:
            reader, writer = await asyncio.open_unix_connection(str(_paths.broadcast_socket()))
            try:
                subscribe = _load_filters()
                payload = json.dumps(subscribe).encode()
                writer.write(encode_frame(payload))
                await writer.drain()

                backoff = _BACKOFF_INITIAL_S  # reset on successful subscribe.

                while True:
                    data = await read_frame(reader)
                    if data is None:
                        break  # daemon EOF
                    try:
                        frame = json.loads(data)
                    except json.JSONDecodeError:
                        continue  # malformed frame; daemon shouldn't emit these
                    await _emit_frame(session, frame)
                    # Fan out to every other session subscribed to a
                    # matching waitbus:// URI. Failures per-session are
                    # logged-and-continued so one broken peer cannot
                    # starve the others.
                    await _emit_to_subscribed_sessions(frame)
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
        except (
            ConnectionRefusedError,
            ConnectionError,
            FileNotFoundError,
            BrokenPipeError,
            OSError,
        ) as exc:
            logger.debug("waitbus broadcast disconnected: %s", exc)

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _BACKOFF_MAX_S)


def build_server() -> Server[Any, Any]:
    """Construct the lowlevel SDK Server with the waitbus tool + resource surface.

    Returns a ``WaitbusServer`` instance (a Server subclass that
    overrides ``get_capabilities`` to flip resources.subscribe=true
    when a subscribe handler is registered — the upstream SDK hardcodes
    that field to False regardless).

    Runtime probe: a fresh ServerSession must be weak-referenceable so
    the module-level subscription registry (a WeakKeyDictionary) works
    as designed. The probe runs at construction time so an SDK change
    that adds __slots__ without __weakref__ fails loudly here rather
    than producing a silent memory leak at runtime.
    """
    # Probe assertion: ServerSession must support weak references.
    _verify_session_is_weak_referenceable()

    server = WaitbusServer(name="waitbus", version=PACKAGE_VERSION)
    _register_handlers(server)
    return server


def _verify_session_is_weak_referenceable() -> None:
    """Assert ServerSession instances accept weak references.

    The check creates a probe via ``ServerSession.__new__`` (which
    skips __init__'s stream wiring) and asks ``weakref.ref`` for a
    reference. A future SDK change that adds ``__slots__`` without
    including ``__weakref__`` would surface here as a TypeError;
    catching it at construction time prevents the registry from
    silently leaking sessions at runtime.
    """
    try:
        probe = ServerSession.__new__(ServerSession)
        weakref.ref(probe)
    except TypeError as exc:
        raise RuntimeError(
            "ServerSession is no longer weak-referenceable; "
            "the waitbus subscription registry relies on WeakKeyDictionary "
            "semantics and will leak sessions. Investigate whether the "
            "pinned mcp SDK added __slots__ without __weakref__."
        ) from exc


def build_initialization_options(server: Server[Any, Any]) -> InitializationOptions:
    """Build InitializationOptions with the channels-reference experimental block.

    The ``experimental.claude/channel`` block is the Anthropic-private
    signal that activates the Claude Code channel-rendering UI. Spec-
    compliant clients ignore the experimental block per the spec's
    open-set convention.
    """
    return server.create_initialization_options(
        notification_options=NotificationOptions(),
        experimental_capabilities={"claude/channel": {}},
    )


def _is_initialized_notification(message: Any) -> bool:
    """Return True iff ``message`` is the client's notifications/initialized.

    The SDK's receive loop validates each inbound notification into a
    ``types.ClientNotification`` RootModel before handing it to
    ``incoming_messages``; the initialized notification surfaces with
    ``.root`` being an ``types.InitializedNotification``. A bare
    ``JSONRPCNotification`` with the matching ``method`` is also accepted
    defensively in case a future SDK delivers the unparsed form.
    """
    root = getattr(message, "root", None)
    if isinstance(root, types.InitializedNotification):
        return True
    return getattr(message, "method", None) == "notifications/initialized"


async def main_async() -> None:
    """Construct the SDK server and drive the stdio loop.

    The broadcast subscriber runs concurrently with the SDK's incoming-
    message dispatch loop. The subscriber is started via ``asyncio.create_task``
    once stdio is open; on stdin EOF the dispatch loop returns, the
    surrounding async context exits, and we cancel the subscriber.
    """
    server = build_server()
    init_options = build_initialization_options(server)

    # We construct ServerSession directly (rather than via Server.run) so the
    # broadcast subscriber owns a session handle for outbound emission for
    # the full session lifetime. The SDK's _receive_loop runs inside the
    # session's task group; the initialize handshake and ping replies are
    # handled internally.
    stop_event: asyncio.Event = asyncio.Event()

    async with (
        stdio_server() as (read_stream, write_stream),
        ServerSession(read_stream, write_stream, init_options) as session,
    ):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)

        # Fire READY=1 once stdio is open and the session task group is
        # running. The mcp-python-sdk has no sd_notify integration of its
        # own; waitbus owns this primitive. NOTE: this fires BEFORE the
        # client's initialize handshake completes — ServerSession.__aenter__
        # only starts the receive loop, it does not await the client's
        # initialize. The session is not marked initialized until the
        # client's notifications/initialized arrives below.
        sd_notify(b"READY=1\nSTATUS=serving MCP notifications\n")

        subscriber = asyncio.create_task(_stream_events(session))
        # Dispatch each incoming client request to the registered tool /
        # resource handlers. Server.run() would normally own this loop, but we
        # drive it by hand so the broadcast subscriber above keeps its own
        # session handle for outbound push emission for the full session
        # lifetime. _handle_message routes tools/list, tools/call, and the
        # resources/* requests to the @server.* handlers exactly as Server.run
        # does internally; lifespan_context is None because build_server installs
        # no lifespan and no handler reads the request context. Each request runs
        # as its own task so a blocking tool (tail_events) cannot stall a
        # concurrent tools/list or an outbound push notification.
        #
        # The client's notifications/initialized is the real handshake-
        # complete signal (NOT ServerSession.__aenter__). When it arrives we
        # mark the session initialized and flush the pre-init queue BEFORE
        # dispatching it onward, so by flush time client_params is populated
        # and the claude/channel capability gate in _flush_pending is valid.
        handlers: set[asyncio.Task[None]] = set()
        try:
            async for message in session.incoming_messages:
                if stop_event.is_set():
                    break
                if _is_initialized_notification(message):
                    state = _get_state(session)
                    state.initialized = True
                    await _flush_pending(session, state)
                handler = asyncio.create_task(server._handle_message(message, session, None))
                handlers.add(handler)
                handler.add_done_callback(handlers.discard)
        finally:
            sd_notify(b"STOPPING=1\n")
            subscriber.cancel()
            for handler in handlers:
                handler.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await subscriber


def info() -> dict[str, object]:
    """Return a dict describing the server's identity and protocol version range.

    The returned mapping is suitable for JSON serialisation by any caller.
    Fields::

        name                    - server name as advertised in serverInfo
        version                 - waitbus package version (semver)
        protocolVersion         - the MCP spec version the server targets
                                  (PROTOCOL_VERSIONS_SUPPORTED[-1])
        supportedProtocolVersions - all MCP spec versions the server accepts
                                  during initialize negotiation; mirrors the
                                  SDK's mcp.shared.version.SUPPORTED_PROTOCOL_VERSIONS
                                  for the pinned SDK release

    Operators can use this to verify that the SDK pin matches the advertised
    range without starting a live MCP session::

        waitbus mcp info
    """
    return {
        "name": "waitbus",
        "version": PACKAGE_VERSION,
        "protocolVersion": PROTOCOL_VERSION,
        "supportedProtocolVersions": list(PROTOCOL_VERSIONS_SUPPORTED),
    }


def main(argv: list[str] | None = None) -> None:
    """Entry point: ``waitbus mcp serve`` umbrella sub-command.

    Args:
        argv: Reserved for future argparse integration; currently unused.
            Callers (e.g., the umbrella ``waitbus mcp serve`` sub-app)
            pass extra args here instead of mutating ``sys.argv``.
    """
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
