"""Drift + hygiene guards binding every event-column declaration to the single
source of truth in :mod:`waitbus._columns`.

These tests prevent that class of regression: a new column facet added to
the schema but missed in the MCP projection, the untrusted-cleaning set, or
one of the msgspec Structs will fail one of these tests rather than silently
shipping a dropped/uncleaned facet.
"""

from __future__ import annotations

import sqlite3

from waitbus import _columns, _db
from waitbus._mcp_models import EventRow
from waitbus._types import Event, EventInsert
from waitbus.mcp import _event_row_to_dict


def test_sot_matches_event_columns() -> None:
    # _db.EVENT_COLUMNS keeps its own INSERT order; bind it to the SoT by set.
    # (test_db.py separately binds EVENT_COLUMNS to schema.sql, so the SoT is
    # transitively bound to the DDL.)
    assert set(_db.EVENT_COLUMNS) == _columns.COLUMN_NAMES


def test_sot_matches_event_insert_struct() -> None:
    # EventInsert is the write shape: every column except the daemon-stamped event_id.
    assert set(EventInsert.__struct_fields__) == _columns.COLUMN_NAMES - {"event_id"}


def test_sot_matches_event_struct() -> None:
    assert set(Event.__struct_fields__) == _columns.COLUMN_NAMES


def test_sot_matches_event_row_struct() -> None:
    # EventRow is the MCP shape: every column except the raw payload_json blob.
    assert set(EventRow.__struct_fields__) == _columns.COLUMN_NAMES - {"payload_json"}


def test_mcp_dict_columns_drop_only_payload_json() -> None:
    assert {c.name for c in _columns.MCP_DICT_COLUMNS} == _columns.COLUMN_NAMES - {"payload_json"}


def _row(**vals: object) -> sqlite3.Row:
    """Build a real sqlite3.Row carrying every events column for projection tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cols = list(_db.EVENT_COLUMNS)
    conn.execute(f"CREATE TABLE events ({', '.join(cols)})")
    conn.execute(
        f"INSERT INTO events ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
        [vals.get(c) for c in cols],
    )
    row = conn.execute("SELECT * FROM events").fetchone()
    conn.close()
    assert isinstance(row, sqlite3.Row)
    return row


def test_event_row_to_dict_cleans_every_untrusted_column_and_passes_others_verbatim() -> None:
    # A control/ANSI/zero-width-laden value in every column. The projection must
    # strip exactly the untrusted (attacker-influenceable free-text) columns and
    # leave structured columns byte-faithful. This is the real regression net for
    # the facet that can otherwise be silently dropped or left uncleaned.
    dirty = "x\x00\x1b[31m​y"
    cleaned = "xy"
    row = _row(**{c.name: dirty for c in _columns.COLUMNS})
    projected = _event_row_to_dict(row)
    assert "payload_json" not in projected
    for col in _columns.MCP_DICT_COLUMNS:
        if col.untrusted:
            assert projected[col.name] == cleaned, f"{col.name} not control-stripped"
        else:
            assert projected[col.name] == dirty, f"{col.name} altered but is not flagged untrusted"


def test_msg_facet_is_projected_and_cleaned() -> None:
    # regression: the agent addressing facet must reach the MCP
    # projection AND be control-stripped.
    row = _row(event_id="01ABC", msg_to="agent_b", msg_from="agent_a", msg_body="hi\x00there")
    projected = _event_row_to_dict(row)
    assert projected["msg_to"] == "agent_b"
    assert projected["msg_from"] == "agent_a"
    assert projected["msg_body"] == "hithere"
