"""Single source of truth for the ``events`` column catalogue.

The event column set was historically re-declared in five places with four
different orderings (``EventInsert`` / ``Event`` in ``_types``, ``EVENT_COLUMNS``
in ``_db``, ``EventRow`` in ``_mcp_models``, and the ``events`` DDL in
``schema.sql``), and the attacker-influenceable free-text set was re-hardcoded at
every MCP projection site. Each new column facet therefore had to be wired into
many places by hand, and a missed site failed silently -- the agent-message
``msg_*`` facet landed in the schema + write path but was dropped from the MCP
projection (never exposed), left uncleaned, and mis-rendered in the one-line
summary.

This module names every column once with the metadata the projection and
rendering seams need, so a new facet is covered by construction. Consumers keep
their own column ORDER (the orderings legitimately differ); they are bound to
this catalogue by set-equality drift tests (see ``tests/test_columns.py``).

Boundary note (load-bearing): the ``untrusted`` flag is consulted ONLY by the
MCP emission seam (``mcp._event_row_to_dict`` and friends), which control-strips
attacker free text before it reaches an LLM. It is deliberately NOT consulted by
``broadcast._row_to_frame``: the broadcast wire ships ``fields`` byte-faithfully
so predicate matching (``fields.msg_to=...``) is exact. Cleaning is an emission
concern, not a storage or wire concern.
"""

from __future__ import annotations

from typing import Final, Literal, NamedTuple

Facet = Literal["core", "ci", "job", "alert", "msg"]


class Col(NamedTuple):
    """One ``events`` column and the metadata the projection seams need.

    Attributes:
        name: the column name (matches ``schema.sql`` / ``EVENT_COLUMNS``).
        untrusted: the value is attacker-influenceable free text and MUST be
            routed through ``_untrusted.clean_opt`` at the MCP emission seam.
        facet: which event facet the column belongs to (drives the one-line
            summary renderer's per-facet dispatch).
        in_mcp_dict: the column is exposed by ``mcp._event_row_to_dict`` (every
            column except the raw ``payload_json`` blob, which is fenced
            separately on the resource-read path).
        in_frame_fields: the column appears in the broadcast wire frame's
            ``fields`` map (every column except the ones carried as typed
            top-level ``EventFrame`` attributes or never put on the wire).
    """

    name: str
    untrusted: bool
    facet: Facet
    in_mcp_dict: bool
    in_frame_fields: bool


# Columns carried as typed top-level EventFrame attributes (event_id/event_type/
# owner/repo/received_at/delivery_id) or never wired (payload_json) are excluded
# from the frame ``fields`` map by broadcast._row_to_frame. Keep this set in sync
# with that exclusion; the drift test asserts it.
_FRAME_FIELD_EXCLUDED: Final = frozenset({"delivery_id", "received_at", "payload_json", "event_id"})


def _col(name: str, *, untrusted: bool, facet: Facet) -> Col:
    return Col(
        name=name,
        untrusted=untrusted,
        facet=facet,
        in_mcp_dict=name != "payload_json",
        in_frame_fields=name not in _FRAME_FIELD_EXCLUDED,
    )


#: The canonical column catalogue, in ``schema.sql`` declaration order. This is
#: the single source of truth; ``_db.EVENT_COLUMNS`` keeps its own INSERT order
#: and the msgspec Structs keep theirs, all bound to this set by drift tests.
COLUMNS: Final[tuple[Col, ...]] = (
    _col("delivery_id", untrusted=False, facet="core"),
    _col("source", untrusted=False, facet="core"),
    _col("event_type", untrusted=False, facet="core"),
    _col("owner", untrusted=False, facet="core"),
    _col("repo", untrusted=False, facet="core"),
    _col("run_id", untrusted=False, facet="ci"),
    _col("workflow_name", untrusted=True, facet="ci"),
    _col("head_branch", untrusted=True, facet="ci"),
    _col("head_sha", untrusted=False, facet="ci"),
    _col("status", untrusted=False, facet="ci"),
    _col("conclusion", untrusted=False, facet="ci"),
    _col("received_at", untrusted=False, facet="core"),
    _col("payload_json", untrusted=False, facet="core"),
    _col("ingest_method", untrusted=False, facet="core"),
    _col("job_id", untrusted=False, facet="job"),
    _col("job_name", untrusted=True, facet="job"),
    _col("parent_run_id", untrusted=False, facet="job"),
    _col("alert_name", untrusted=True, facet="alert"),
    _col("alert_severity", untrusted=True, facet="alert"),
    _col("alert_fingerprint", untrusted=True, facet="alert"),
    _col("msg_to", untrusted=True, facet="msg"),
    _col("msg_from", untrusted=True, facet="msg"),
    _col("msg_correlation_id", untrusted=True, facet="msg"),
    _col("msg_reply_to", untrusted=True, facet="msg"),
    _col("msg_thread", untrusted=True, facet="msg"),
    _col("msg_body", untrusted=True, facet="msg"),
    _col("event_id", untrusted=False, facet="core"),
)

#: All column names (set form, for drift checks).
COLUMN_NAMES: Final[frozenset[str]] = frozenset(c.name for c in COLUMNS)

#: Attacker-influenceable free-text columns that the MCP emission seam must
#: control-strip via ``_untrusted.clean_opt``.
UNTRUSTED: Final[frozenset[str]] = frozenset(c.name for c in COLUMNS if c.untrusted)

#: Columns exposed by ``mcp._event_row_to_dict`` (drops the raw ``payload_json``).
MCP_DICT_COLUMNS: Final[tuple[Col, ...]] = tuple(c for c in COLUMNS if c.in_mcp_dict)
