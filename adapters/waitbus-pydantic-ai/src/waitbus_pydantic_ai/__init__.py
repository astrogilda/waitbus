"""Pydantic AI tools for the waitbus workstation event bus.

Public surface: :func:`wait_tool` (block an agent on a bus predicate) and
:func:`emit_tool` (publish an agent event onto the bus). Both return
:class:`pydantic_ai.Tool` objects wrapping the public waitbus SDK.
"""

from __future__ import annotations

from ._tools import emit_tool, wait_tool

__version__ = "0.1.0"
__all__ = ("emit_tool", "wait_tool")
