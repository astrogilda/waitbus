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

from typing import Any

import msgspec

from ._mcp_constants import (
    LIST_FAILED_JOBS_DEFAULT_LIMIT,
    LIST_FAILED_JOBS_MAX_LIMIT,
    TAIL_EVENTS_DEFAULT_LIMIT,
    TAIL_EVENTS_DEFAULT_MAX_WAIT_SEC,
    TAIL_EVENTS_MAX_LIMIT,
    TAIL_EVENTS_MAX_WAIT_CAP_SECONDS,
)

# --- Event row shape -------------------------------------------------------


class EventRow(msgspec.Struct, kw_only=True, frozen=True):
    """Single events-table row as exposed to MCP clients.

    Mirrors the ``EVENT_COLUMNS`` tuple in ``_db.py`` modulo the
    ``payload_json`` blob, which is dropped from MCP exposure — clients
    that need the raw GitHub payload can call the read_resource path
    on ``waitbus://event/{ulid}`` and receive it there.
    """

    event_id: str
    delivery_id: str
    source: str
    event_type: str
    owner: str
    repo: str
    received_at: int
    run_id: int | None = None
    workflow_name: str | None = None
    head_branch: str | None = None
    head_sha: str | None = None
    status: str | None = None
    conclusion: str | None = None
    ingest_method: str | None = None
    job_id: int | None = None
    job_name: str | None = None
    parent_run_id: int | None = None
    alert_name: str | None = None
    alert_severity: str | None = None
    alert_fingerprint: str | None = None
    # Agent-message addressing facet (mirrors EVENT_COLUMNS; see schema.sql).
    msg_to: str | None = None
    msg_from: str | None = None
    msg_correlation_id: str | None = None
    msg_reply_to: str | None = None
    msg_thread: str | None = None
    msg_body: str | None = None


# --- Tool result shapes ----------------------------------------------------


class RunStatus(msgspec.Struct, kw_only=True, frozen=True):
    """One workflow_run latest-state record returned by get_ci_status."""

    repo: str
    run_id: int | None = None
    workflow_name: str | None = None
    head_branch: str | None = None
    head_sha: str | None = None
    status: str | None = None
    conclusion: str | None = None
    event_id: str
    received_at: int


class CiStatus(msgspec.Struct, kw_only=True, frozen=True):
    """get_ci_status return shape: one or many RunStatus entries."""

    runs: list[RunStatus]
    queried_at_ns: int


class JobStatus(msgspec.Struct, kw_only=True, frozen=True):
    """One failing workflow_job entry returned by list_failed_jobs."""

    repo: str
    job_id: int | None = None
    job_name: str | None = None
    parent_run_id: int | None = None
    conclusion: str | None = None
    event_id: str
    received_at: int


class FailedJobs(msgspec.Struct, kw_only=True, frozen=True):
    """list_failed_jobs return shape."""

    jobs: list[JobStatus]
    queried_at_ns: int


class PrAggregate(msgspec.Struct, kw_only=True, frozen=True):
    """get_pr_aggregate return shape.

    The events list is ordered by ``event_id`` ascending so a client
    can replay the chronological state machine without re-sorting.
    """

    repo: str
    pr_number: int
    runs: list[RunStatus]
    jobs: list[JobStatus]
    queried_at_ns: int


class TailEvents(msgspec.Struct, kw_only=True, frozen=True):
    """tail_events return shape; next_cursor is the last event_id read."""

    events: list[EventRow]
    next_cursor: str | None = None
    queried_at_ns: int


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


# --- Input schemas (hand-rolled; tools have very small arg surfaces) ------


def schema_input_get_ci_status() -> dict[str, Any]:
    """Input schema for get_ci_status."""
    return {
        "type": "object",
        "properties": {
            "repo": {
                "type": ["string", "null"],
                "description": "owner/repo to filter; null returns all configured filters",
            },
        },
        "additionalProperties": False,
    }


def schema_input_list_failed_jobs() -> dict[str, Any]:
    """Input schema for list_failed_jobs."""
    return {
        "type": "object",
        "properties": {
            "repo": {"type": ["string", "null"]},
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": LIST_FAILED_JOBS_MAX_LIMIT,
                "default": LIST_FAILED_JOBS_DEFAULT_LIMIT,
            },
        },
        "additionalProperties": False,
    }


def schema_input_get_pr_aggregate() -> dict[str, Any]:
    """Input schema for get_pr_aggregate."""
    return {
        "type": "object",
        "properties": {
            "repo": {"type": "string"},
            "pr_number": {"type": "integer", "minimum": 1},
        },
        "required": ["repo", "pr_number"],
        "additionalProperties": False,
    }


def schema_input_tail_events() -> dict[str, Any]:
    """Input schema for tail_events."""
    return {
        "type": "object",
        "properties": {
            "repo": {"type": ["string", "null"]},
            "since_cursor": {"type": ["string", "null"]},
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
        },
        "additionalProperties": False,
    }
