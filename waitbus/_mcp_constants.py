"""Constants derived from the MCP spec schema by scripts/gen_mcp_constants.py.

Source: schema 2025-11-25. Regenerate by
running ``python3 scripts/gen_mcp_constants.py`` after a spec
advance; the file is checked in deliberately so the build does
not depend on the spec clone being present.
"""

from __future__ import annotations

from typing import Final

PROTOCOL_VERSIONS_SUPPORTED: Final[tuple[str, ...]] = (
    "2024-11-05",
    "2025-03-26",
    "2025-06-18",
    "2025-11-25",
)
"""MCP protocol versions that waitbus accepts during initialize negotiation.

These mirror the ``mcp.shared.version.SUPPORTED_PROTOCOL_VERSIONS`` list in the
pinned SDK (``mcp==1.27.1``).  The SDK handles the actual wire negotiation: if
the client's requested ``protocolVersion`` is in this set the server echoes it
back; otherwise it falls back to the latest entry.

Operators can compare this tuple against the SDK constant to verify the pin
has not drifted::

    python -c "from mcp.shared.version import SUPPORTED_PROTOCOL_VERSIONS; \\
               print(SUPPORTED_PROTOCOL_VERSIONS)"
    python -c "from waitbus._mcp_constants import \\
               PROTOCOL_VERSIONS_SUPPORTED; print(list(PROTOCOL_VERSIONS_SUPPORTED))"

A non-empty invariant is enforced by an assert below so a future edit that
accidentally empties the tuple fails at import time rather than silently.
"""

assert len(PROTOCOL_VERSIONS_SUPPORTED) > 0, "PROTOCOL_VERSIONS_SUPPORTED must contain at least one entry"

PROTOCOL_VERSION: Final[str] = PROTOCOL_VERSIONS_SUPPORTED[-1]
"""The MCP protocol version waitbus targets.  Always the latest entry in
``PROTOCOL_VERSIONS_SUPPORTED``; bumping the SDK pin requires adding a new entry
to that tuple rather than editing this constant directly.
"""

# === Spec-derived method names ============================
INITIALIZE_REQUEST_METHOD: Final[str] = "initialize"
INITIALIZED_NOTIFICATION_METHOD: Final[str] = "notifications/initialized"
RESOURCE_UPDATED_NOTIFICATION_METHOD: Final[str] = "notifications/resources/updated"
LOGGING_MESSAGE_NOTIFICATION_METHOD: Final[str] = "notifications/message"

# === Anthropic-private method names (NOT in spec) ==========
# Source: https://code.claude.com/docs/en/channels-reference
# These are extensions Claude Code recognises; spec-compliant
# clients ignore unknown method names per JSON-RPC 2.0 rules.
CLAUDE_CHANNEL_NOTIFICATION_METHOD: Final[str] = "notifications/claude/channel"
CLAUDE_CHANNEL_PERMISSION_REQUEST_METHOD: Final[str] = "notifications/claude/channel/permission_request"
CLAUDE_CHANNEL_PERMISSION_METHOD: Final[str] = "notifications/claude/channel/permission"

# === Required-field tuples for outgoing-envelope sanity ====
INITIALIZE_RESULT_REQUIRED: Final[tuple[str, ...]] = ("capabilities", "protocolVersion", "serverInfo")
RESOURCE_UPDATED_PARAMS_REQUIRED: Final[tuple[str, ...]] = ("uri",)
IMPLEMENTATION_REQUIRED: Final[tuple[str, ...]] = ("name", "version")

# === Tool names (single source of truth) ===================
# The three CI read tools below are kept as constants because their
# _tool_*_impl functions still back the consolidated query_ci tool (and the
# resource read path / unit tests); they are no longer advertised as
# standalone tools in the MCP catalogue.
TOOL_GET_CI_STATUS: Final[str] = "get_ci_status"
TOOL_LIST_FAILED_JOBS: Final[str] = "list_failed_jobs"
TOOL_GET_PR_AGGREGATE: Final[str] = "get_pr_aggregate"
TOOL_QUERY_CI: Final[str] = "query_ci"
TOOL_GET_EVENT: Final[str] = "get_event"
TOOL_TAIL_EVENTS: Final[str] = "tail_events"
TOOL_EMIT_AGENT_MESSAGE: Final[str] = "emit_agent_message"
TOOL_READ_AGENT_MESSAGES: Final[str] = "read_agent_messages"

# query_ci view selector values (single source of truth). The required
# ``view`` enum picks which CI projection the consolidated tool returns.
QUERY_CI_VIEW_STATUS: Final[str] = "status"
QUERY_CI_VIEW_FAILED_JOBS: Final[str] = "failed_jobs"
QUERY_CI_VIEW_PR_AGGREGATE: Final[str] = "pr_aggregate"
QUERY_CI_VIEWS: Final[tuple[str, ...]] = (
    QUERY_CI_VIEW_STATUS,
    QUERY_CI_VIEW_FAILED_JOBS,
    QUERY_CI_VIEW_PR_AGGREGATE,
)

# === Agent-message event class (single source of truth) ====
# emit_agent_message hardcodes this event_type and the "agent" source on
# insert -- the typed lane that keeps agent chatter out of the CI
# (workflow_run) stream. The same literal lives in waitbus._messaging
# (the request/respond SDK); the MCP tool and the SDK agree on it here.
AGENT_MESSAGE_EVENT_TYPE: Final[str] = "agent_message"
AGENT_MESSAGE_SOURCE: Final[str] = "agent"

# The wildcard recipient: a message addressed to "*" is delivered to every
# agent that holds an agent doorbell subscription.
AGENT_BROADCAST_RECIPIENT: Final[str] = "*"

# Bound on tail_events.max_wait_seconds. Cursor (the editor client) issues a
# hard cancel at 5 minutes per its MCP integration; staying comfortably under
# means a slow tail does not race the client cancel.
TAIL_EVENTS_MAX_WAIT_CAP_SECONDS: Final[int] = 270

LIST_FAILED_JOBS_DEFAULT_LIMIT: Final[int] = 20
"""Default ``limit`` for ``list_failed_jobs`` when the caller omits the field.

Matches the JSON schema ``default`` so the tool and the handler agree on
the fallback without duplicating the literal in both places.
"""

LIST_FAILED_JOBS_MAX_LIMIT: Final[int] = 500
"""Upper bound on the ``limit`` parameter for ``list_failed_jobs``.

Constrains the JSON schema ``maximum`` and the handler's clamp; raising it
should be a deliberate decision (larger responses affect MCP frame size).
"""

TAIL_EVENTS_DEFAULT_LIMIT: Final[int] = 100
"""Default number of events returned by ``tail_events`` when ``limit`` is omitted."""

TAIL_EVENTS_MAX_LIMIT: Final[int] = 1000
"""Hard cap on ``tail_events`` ``limit``.  Raising it affects both the JSON
schema ``maximum`` and the DB query's LIMIT clause simultaneously.
"""

TAIL_EVENTS_DEFAULT_MAX_WAIT_SEC: Final[int] = 30
"""Default long-poll wait for ``tail_events`` in seconds.

The field's ``maximum`` is ``TAIL_EVENTS_MAX_WAIT_CAP_SECONDS``; this
default sits well below that cap so callers that omit the field do not
inadvertently tie up the MCP channel for nearly five minutes.
"""

READ_AGENT_MESSAGES_DEFAULT_LIMIT: Final[int] = 100
"""Default number of messages returned by ``read_agent_messages``."""

READ_AGENT_MESSAGES_MAX_LIMIT: Final[int] = 1000
"""Hard cap on ``read_agent_messages`` ``limit`` (mirrors ``tail_events``)."""
