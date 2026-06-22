"""msgspec.Struct schemas for the MCP tool surface.

Each Struct carries a corresponding JSON Schema (built via
``msgspec.json.schema``) that the SDK consumes as both ``inputSchema``
(arguments) and ``outputSchema`` (structuredContent shape). The
matching schemas mean the SDK's pre-call argument validation and
post-call structured-output validation share a single source of truth.

Schemas are flat-ish dicts of primitives so they serialise cleanly to
JSON Schema; nested Structs are used sparingly because the SDK's
``jsonschema`` validator follows ``$ref`` only in the bundled mode.
``msgspec.json.schema`` returns a ``(schema, definitions)`` pair that
the helpers below collapse into a single inline ``$defs`` block so the
SDK's validator does not need a separate resolver registry.
"""

from __future__ import annotations

from typing import Annotated, Any

import msgspec

from ._mcp_constants import (
    LIST_FAILED_JOBS_DEFAULT_LIMIT,
    LIST_FAILED_JOBS_MAX_LIMIT,
    QUERY_CI_VIEW_FAILED_JOBS,
    QUERY_CI_VIEW_PR_AGGREGATE,
    QUERY_CI_VIEW_STATUS,
    QUERY_CI_VIEWS,
    READ_AGENT_MESSAGES_DEFAULT_LIMIT,
    READ_AGENT_MESSAGES_MAX_LIMIT,
    TAIL_EVENTS_DEFAULT_LIMIT,
    TAIL_EVENTS_DEFAULT_MAX_WAIT_SEC,
    TAIL_EVENTS_MAX_LIMIT,
    TAIL_EVENTS_MAX_WAIT_CAP_SECONDS,
)

# --- Shared field descriptions ---------------------------------------------
# Reusable Annotated aliases so a field that recurs across structs carries the
# SAME wording everywhere (the documentation contract an agent reads off the
# generated JSON Schema). msgspec.Meta(description=...) is emitted verbatim into
# the schema; at runtime ``Annotated[T, Meta]`` is still ``T``.

_EventId = Annotated[
    str,
    msgspec.Meta(
        description=(
            "Opaque ULID for this event; also its cursor key. Pass it back as "
            "since_cursor to page forward without gaps or repeats."
        )
    ),
]
_ReceivedAtNs = Annotated[
    int,
    msgspec.Meta(description="Daemon ingest time, in nanoseconds since the Unix epoch."),
]
_QueriedAtNs = Annotated[
    int,
    msgspec.Meta(description="Time the daemon answered this query, in nanoseconds since the Unix epoch."),
]
_Status = Annotated[
    str | None,
    msgspec.Meta(description=("GitHub workflow status: queued, in_progress, or completed; null if unknown.")),
]
_Conclusion = Annotated[
    str | None,
    msgspec.Meta(
        description=(
            "GitHub workflow conclusion: success, failure, cancelled, timed_out, "
            "skipped, neutral, action_required, or stale; null while the run/job "
            "is still in progress."
        )
    ),
]
_NextCursor = Annotated[
    str | None,
    msgspec.Meta(
        description=(
            "Opaque cursor: the last event_id in this batch. Pass it back as "
            "since_cursor on the next call; null when the batch was empty."
        )
    ),
]
_Repo = Annotated[str, msgspec.Meta(description="Repository slug, owner/name (e.g. octocat/hello-world).")]


# --- Event row shape -------------------------------------------------------


class EventRow(msgspec.Struct, kw_only=True, frozen=True):
    """Single events-table row as exposed to MCP clients.

    Mirrors the ``EVENT_COLUMNS`` tuple in ``_db.py`` modulo the
    ``payload_json`` blob, which is dropped from MCP exposure — clients
    that need the raw GitHub payload can call the read_resource path
    on ``waitbus://event/{ulid}`` and receive it there.
    """

    event_id: _EventId
    delivery_id: Annotated[
        str,
        msgspec.Meta(description="Upstream-unique delivery id; the idempotency key for this event."),
    ]
    source: Annotated[
        str,
        msgspec.Meta(description="Event source: one of github, pytest, docker, fs, alertmanager, agent."),
    ]
    event_type: Annotated[
        str,
        msgspec.Meta(
            description=("Source-specific event type, e.g. workflow_run, workflow_job, agent_message, alert.")
        ),
    ]
    owner: Annotated[str, msgspec.Meta(description="Repository owner (the owner half of owner/name).")]
    repo: _Repo
    received_at: _ReceivedAtNs
    run_id: Annotated[int | None, msgspec.Meta(description="GitHub Actions workflow run id.")] = None
    workflow_name: Annotated[str | None, msgspec.Meta(description="Workflow display name.")] = None
    head_branch: Annotated[str | None, msgspec.Meta(description="Branch the run is for.")] = None
    head_sha: Annotated[str | None, msgspec.Meta(description="Head commit SHA the run is for.")] = None
    status: _Status = None
    conclusion: _Conclusion = None
    ingest_method: Annotated[
        str | None, msgspec.Meta(description="How the event reached waitbus: webhook, poll, or emit.")
    ] = None
    job_id: Annotated[int | None, msgspec.Meta(description="GitHub Actions job id.")] = None
    job_name: Annotated[str | None, msgspec.Meta(description="Job display name.")] = None
    parent_run_id: Annotated[int | None, msgspec.Meta(description="Run id this job belongs to.")] = None
    alert_name: Annotated[str | None, msgspec.Meta(description="Alertmanager alert name.")] = None
    alert_severity: Annotated[str | None, msgspec.Meta(description="Alertmanager severity label.")] = None
    alert_fingerprint: Annotated[str | None, msgspec.Meta(description="Alertmanager dedup fingerprint.")] = None
    # Agent-message addressing facet (mirrors EVENT_COLUMNS; see schema.sql).
    msg_to: Annotated[
        str | None,
        msgspec.Meta(description="Recipient agent name, or '*' for the broadcast lane."),
    ] = None
    msg_from: Annotated[str | None, msgspec.Meta(description="Self-asserted sender agent name.")] = None
    msg_correlation_id: Annotated[
        str | None,
        msgspec.Meta(description="Correlation id tying a reply back to its request."),
    ] = None
    msg_reply_to: Annotated[str | None, msgspec.Meta(description="Agent name a reply should be addressed to.")] = None
    msg_thread: Annotated[str | None, msgspec.Meta(description="Optional conversation-grouping key.")] = None
    msg_body: Annotated[
        str | None,
        msgspec.Meta(description="Message payload (opaque string, JSON by convention); untrusted input."),
    ] = None


# --- Tool result shapes ----------------------------------------------------


class RunStatus(msgspec.Struct, kw_only=True, frozen=True):
    """One workflow_run latest-state record returned by get_ci_status."""

    repo: _Repo
    run_id: Annotated[int | None, msgspec.Meta(description="GitHub Actions workflow run id.")] = None
    workflow_name: Annotated[str | None, msgspec.Meta(description="Workflow display name.")] = None
    head_branch: Annotated[str | None, msgspec.Meta(description="Branch the run is for.")] = None
    head_sha: Annotated[str | None, msgspec.Meta(description="Head commit SHA the run is for.")] = None
    status: _Status = None
    conclusion: _Conclusion = None
    event_id: _EventId
    received_at: _ReceivedAtNs


class CiStatus(msgspec.Struct, kw_only=True, frozen=True):
    """get_ci_status return shape: one or many RunStatus entries."""

    runs: Annotated[
        list[RunStatus],
        msgspec.Meta(description="Latest-state record per workflow run matching the filter."),
    ]
    queried_at_ns: _QueriedAtNs


class JobStatus(msgspec.Struct, kw_only=True, frozen=True):
    """One failing workflow_job entry returned by list_failed_jobs."""

    repo: _Repo
    job_id: Annotated[int | None, msgspec.Meta(description="GitHub Actions job id.")] = None
    job_name: Annotated[str | None, msgspec.Meta(description="Job display name.")] = None
    parent_run_id: Annotated[int | None, msgspec.Meta(description="Run id this job belongs to.")] = None
    conclusion: _Conclusion = None
    event_id: _EventId
    received_at: _ReceivedAtNs


class FailedJobs(msgspec.Struct, kw_only=True, frozen=True):
    """list_failed_jobs return shape."""

    jobs: Annotated[
        list[JobStatus],
        msgspec.Meta(description="Failing jobs, most recent first, up to the requested limit."),
    ]
    queried_at_ns: _QueriedAtNs


class PrAggregate(msgspec.Struct, kw_only=True, frozen=True):
    """get_pr_aggregate return shape.

    The events list is ordered by ``event_id`` ascending so a client
    can replay the chronological state machine without re-sorting.
    """

    repo: _Repo
    pr_number: Annotated[int, msgspec.Meta(description="Pull request number this aggregate covers.")]
    runs: Annotated[list[RunStatus], msgspec.Meta(description="Workflow runs for the PR, oldest first.")]
    jobs: Annotated[list[JobStatus], msgspec.Meta(description="Workflow jobs for the PR, oldest first.")]
    queried_at_ns: _QueriedAtNs


class TailEvents(msgspec.Struct, kw_only=True, frozen=True):
    """tail_events return shape; next_cursor is the last event_id read."""

    events: Annotated[list[EventRow], msgspec.Meta(description="Events above since_cursor, oldest first.")]
    next_cursor: _NextCursor = None
    queried_at_ns: _QueriedAtNs


# --- Agent-message shapes --------------------------------------------------


class AgentMessage(msgspec.Struct, kw_only=True, frozen=True):
    """One agent-to-agent message as returned by read_agent_messages.

    A projection of the ``msg_*`` addressing facet plus the ``event_id``
    cursor key and ``received_at`` ordering field. The bodies and
    addresses are self-asserted free text under the same-UID trust model
    and are control-stripped at the emission seam exactly like every other
    untrusted facet (see ``_columns.UNTRUSTED``).
    """

    event_id: _EventId
    msg_to: Annotated[
        str | None,
        msgspec.Meta(description="Recipient agent name, or '*' for the broadcast lane."),
    ] = None
    msg_from: Annotated[str | None, msgspec.Meta(description="Self-asserted sender agent name.")] = None
    msg_body: Annotated[
        str | None,
        msgspec.Meta(description="Message payload (opaque string, JSON by convention); untrusted input."),
    ] = None
    msg_thread: Annotated[str | None, msgspec.Meta(description="Optional conversation-grouping key.")] = None
    msg_correlation_id: Annotated[
        str | None,
        msgspec.Meta(description="Correlation id tying a reply back to its request."),
    ] = None
    received_at: _ReceivedAtNs


class ReadAgentMessages(msgspec.Struct, kw_only=True, frozen=True):
    """read_agent_messages return shape.

    ``messages`` are the agent's messages above ``since_cursor`` (those
    addressed to the caller's agent name OR the ``*`` broadcast lane),
    ordered oldest-first by the daemon-assigned commit order so the client
    can advance its cursor monotonically. ``next_cursor`` is the last
    message's ``event_id`` (or the input cursor when the window was empty),
    exactly mirroring the ``tail_events`` cursor contract.
    """

    messages: Annotated[
        list[AgentMessage],
        msgspec.Meta(description="Messages to this agent or the '*' lane, oldest first."),
    ]
    next_cursor: _NextCursor = None
    queried_at_ns: _QueriedAtNs


class EmitAgentMessage(msgspec.Struct, kw_only=True, frozen=True):
    """emit_agent_message return shape.

    ``event_id`` is the committed message's ULID (the cursor the
    recipient will see it above); ``inserted`` is False on an idempotent
    re-emit of the same delivery_id.
    """

    event_id: _EventId
    inserted: Annotated[
        bool,
        msgspec.Meta(
            description=(
                "True on first insert; False when this delivery_id was already committed (idempotent re-emit)."
            )
        ),
    ]
    queried_at_ns: _QueriedAtNs


# --- Schema helpers --------------------------------------------------------


def _schema_for(cls: type) -> dict[str, Any]:
    """Return a self-contained, MCP-conformant JSON Schema for ``cls``.

    msgspec returns a (schema, components) pair whose top-level entry is a
    bare ``$ref`` into the components. MCP requires a tool ``outputSchema`` to
    be a top-level object-type JSON Schema (``type: "object"`` at the root):
    the official TypeScript SDK validates this with a Zod literal, so a strict
    client (the MCP Inspector, and TS-SDK consumers such as Claude Desktop)
    rejects ``tools/list`` when the schema is a ``$ref`` wrapper -- even though
    the Python SDK's jsonschema validator resolves the ref transparently.

    Inline the referenced root definition at the top level so ``type`` is
    present at the root, and keep the remaining components under ``$defs`` for
    any nested ``$ref`` to resolve against.
    """
    schema, components = msgspec.json.schema_components([cls], ref_template="#/$defs/{name}")
    root = dict(schema[0])
    ref = root.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        components = dict(components)
        root = dict(components.pop(ref.rsplit("/", 1)[-1]))
    if components:
        root["$defs"] = components
    return root


def schema_event_row() -> dict[str, Any]:
    """JSON Schema for the get_event return value (a single EventRow).

    Mirrors the event resource shape (waitbus://event/{ulid}): the same
    EventRow projection, including ``payload_json`` which the tool
    populates with either the fenced raw payload or, when it would exceed
    the payload cap, a truncation marker carrying a raw_uri pointer. The
    payload field is left out of the strict struct (it is provenance-typed
    at runtime: a fenced string OR a marker object), so it is added here as
    an explicit, loosely-typed property.
    """
    schema = _schema_for(EventRow)
    props = schema.setdefault("properties", {})
    props["payload_json"] = {
        "description": (
            "The event's raw webhook payload, fenced as untrusted external "
            "data. Oversize payloads return a truncation marker object with "
            "a raw_uri pointer to waitbus://event/{ulid}/raw instead."
        ),
    }
    return schema


def schema_ci_status() -> dict[str, Any]:
    """JSON Schema for the get_ci_status return value."""
    return _schema_for(CiStatus)


def schema_failed_jobs() -> dict[str, Any]:
    """JSON Schema for the list_failed_jobs return value."""
    return _schema_for(FailedJobs)


def schema_pr_aggregate() -> dict[str, Any]:
    """JSON Schema for the get_pr_aggregate return value."""
    return _schema_for(PrAggregate)


def schema_tail_events() -> dict[str, Any]:
    """JSON Schema for the tail_events return value."""
    return _schema_for(TailEvents)


def schema_read_agent_messages() -> dict[str, Any]:
    """JSON Schema for the read_agent_messages return value."""
    return _schema_for(ReadAgentMessages)


def schema_emit_agent_message() -> dict[str, Any]:
    """JSON Schema for the emit_agent_message return value."""
    return _schema_for(EmitAgentMessage)


# --- Input schemas (hand-rolled; tools have very small arg surfaces) ------


def schema_input_query_ci() -> dict[str, Any]:
    """Input schema for the consolidated query_ci tool.

    ``view`` is the required selector over the three CI projections that
    were previously three standalone tools. The per-view parameters
    (repo, pr_number, limit) all live in one flat object; which of them
    apply (and which are required) depends on ``view`` and is validated by
    the handler rather than by JSON Schema, so the structured error can
    name the missing-per-view params explicitly:

    - ``status`` -- repo optional (null returns every configured repo).
    - ``failed_jobs`` -- repo optional, limit optional.
    - ``pr_aggregate`` -- repo AND pr_number required.
    """
    return {
        "type": "object",
        "properties": {
            "view": {
                "type": "string",
                "enum": list(QUERY_CI_VIEWS),
                "description": (
                    f"Which CI projection to return. {QUERY_CI_VIEW_STATUS!r}: latest "
                    f"workflow_run per repo. {QUERY_CI_VIEW_FAILED_JOBS!r}: recent failing "
                    f"workflow_job rows. {QUERY_CI_VIEW_PR_AGGREGATE!r}: every run and job "
                    "for one pull request (requires repo and pr_number)."
                ),
            },
            "repo": {
                "type": ["string", "null"],
                "description": (
                    "owner/repo to filter; null returns all configured filters (required for the pr_aggregate view)."
                ),
                "examples": ["octocat/hello-world"],
            },
            "pr_number": {
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Pull request number; required only for the pr_aggregate view.",
                "examples": [42],
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": LIST_FAILED_JOBS_MAX_LIMIT,
                "default": LIST_FAILED_JOBS_DEFAULT_LIMIT,
                "description": "Maximum failing jobs to return for the failed_jobs view (most recent first).",
            },
        },
        "required": ["view"],
        "additionalProperties": False,
    }


def schema_input_get_event() -> dict[str, Any]:
    """Input schema for get_event.

    A single required ``ulid`` selects the stored event row. The tool is
    the tool-surface parity for the waitbus://event/{ulid} resource, so a
    tool-biased client that does not read resources can still fetch one
    event by id.
    """
    return {
        "type": "object",
        "properties": {
            "ulid": {
                "type": "string",
                "minLength": 1,
                "description": "Opaque ULID of the stored event to fetch (a prior event_id / cursor value).",
                "examples": ["01HXZZZZZZZZZZZZZZZZZZZZZZ"],
            },
        },
        "required": ["ulid"],
        "additionalProperties": False,
    }


def schema_input_tail_events() -> dict[str, Any]:
    """Input schema for tail_events."""
    return {
        "type": "object",
        "properties": {
            "repo": {
                "type": ["string", "null"],
                "description": "owner/repo to filter; null returns events for every configured repo",
                "examples": ["octocat/hello-world"],
            },
            "since_cursor": {
                "type": ["string", "null"],
                "description": (
                    "Opaque cursor (a prior next_cursor / event_id); returns events "
                    "strictly above it. Null starts from the current tail."
                ),
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": TAIL_EVENTS_MAX_LIMIT,
                "default": TAIL_EVENTS_DEFAULT_LIMIT,
            },
            "max_wait_seconds": {
                "type": "integer",
                "minimum": 0,
                "maximum": TAIL_EVENTS_MAX_WAIT_CAP_SECONDS,
                "default": TAIL_EVENTS_DEFAULT_MAX_WAIT_SEC,
                "description": "Bounded at 270s (below Cursor's 5-min client cancel)",
            },
            "event_types": {
                "type": ["array", "null"],
                "items": {"type": "string"},
                "description": (
                    "Restrict the read to these event_type values "
                    "(e.g. ['workflow_run'] or ['agent_message']). When "
                    "omitted (null), agent_message rows are EXCLUDED so a "
                    "CI-watching agent never ingests cross-talk; every "
                    "other event_type is returned."
                ),
            },
        },
        "additionalProperties": False,
    }


def schema_input_emit_agent_message() -> dict[str, Any]:
    """Input schema for emit_agent_message.

    ``event_type`` and ``source`` are NOT inputs: the tool hardcodes
    ``agent_message`` / ``agent`` on insert so the model cannot fat-finger
    the typed lane (see docs/AGENT_MESSAGING.md).
    """
    return {
        "type": "object",
        "properties": {
            "to": {
                "type": "string",
                "minLength": 1,
                "description": "Recipient agent name, or '*' to broadcast to every agent.",
            },
            "body": {
                "type": "string",
                "description": "The message payload (an opaque string; JSON by convention).",
            },
            "from_agent": {
                "type": "string",
                "minLength": 1,
                "description": "This agent's self-asserted name (the msg_from address).",
            },
            "thread_id": {
                "type": ["string", "null"],
                "description": "Optional conversation-grouping key set on msg_thread.",
            },
        },
        "required": ["to", "body", "from_agent"],
        "additionalProperties": False,
    }


def schema_input_read_agent_messages() -> dict[str, Any]:
    """Input schema for read_agent_messages.

    ``agent`` is the caller's self-asserted name; the read returns
    messages addressed to it OR to the ``*`` broadcast lane, above the
    opaque ``since_cursor`` (the ULID of the last message the client saw).
    """
    return {
        "type": "object",
        "properties": {
            "agent": {
                "type": "string",
                "minLength": 1,
                "description": "This agent's self-asserted name; selects msg_to == agent OR '*'.",
            },
            "since_cursor": {
                "type": ["string", "null"],
                "description": "Opaque cursor (an event_id); returns messages strictly above it.",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": READ_AGENT_MESSAGES_MAX_LIMIT,
                "default": READ_AGENT_MESSAGES_DEFAULT_LIMIT,
            },
        },
        "required": ["agent"],
        "additionalProperties": False,
    }
