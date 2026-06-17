"""Cross-implementation conformance guard for the MCP tool surface.

Two contracts the Python-SDK e2e tests cannot catch because the Python
``jsonschema`` validator resolves ``$ref`` transparently and never reads
``annotations``:

1. Every tool ``outputSchema`` is a top-level object-type JSON Schema
   (``type == "object"`` at the root, no top-level ``$ref``). The
   official TypeScript SDK validates this with a Zod literal, so a strict
   client (the MCP Inspector, Claude Desktop) rejects ``tools/list`` when
   the schema is a bare ``$ref`` wrapper. ``_schema_for`` inlines the root
   definition to satisfy this (FIX-1).

2. Every tool carries ``annotations.readOnlyHint is True``. All four
   waitbus tools are pure reads; advertising the read-only hint lets a
   client surface them without a write-confirmation prompt (ADD-1).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from waitbus import mcp as mcp_mod
from waitbus._mcp_models import (
    schema_ci_status,
    schema_failed_jobs,
    schema_pr_aggregate,
    schema_tail_events,
)

_SCHEMA_BUILDERS: list[Callable[[], dict[str, Any]]] = [
    schema_ci_status,
    schema_failed_jobs,
    schema_pr_aggregate,
    schema_tail_events,
]


@pytest.mark.parametrize("builder", _SCHEMA_BUILDERS, ids=lambda b: b.__name__)
def test_output_schema_has_top_level_object_type(builder: Callable[[], dict[str, Any]]) -> None:
    """Each output schema is a top-level object, not a bare $ref wrapper."""
    schema = builder()
    assert isinstance(schema, dict)
    assert schema.get("type") == "object", (
        f"{builder.__name__} top-level type must be 'object' for strict TS-SDK clients; got {schema.get('type')!r}"
    )
    assert "$ref" not in schema, (
        f"{builder.__name__} must not be a top-level $ref wrapper; the root "
        "definition must be inlined so strict clients see type:object"
    )


def test_every_tool_advertises_read_only_hint() -> None:
    """All four advertised tools carry annotations.readOnlyHint is True."""
    tools = mcp_mod._tool_definitions()
    assert len(tools) == 4
    for tool in tools:
        assert tool.annotations is not None, f"{tool.name} missing annotations"
        assert tool.annotations.readOnlyHint is True, f"{tool.name} must set readOnlyHint=True"
