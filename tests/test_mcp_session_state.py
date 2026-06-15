"""Lint-test enforcing _SessionState has no strong ServerSession reference.

The waitbus MCP subscription registry is a WeakKeyDictionary keyed on
ServerSession. If a future field annotation on ``_SessionState`` ever
mentions ServerSession (even indirectly via a type alias) the
dictionary's weak-key semantics break and sessions leak forever. The
test below is the single canonical guard; the corresponding runtime
helper lives in ``_mcp_subscriptions.assert_no_session_back_reference``.
"""

from __future__ import annotations

import dataclasses

from waitbus._mcp_subscriptions import (
    _SessionState,
    assert_no_session_back_reference,
)


def test_session_state_fields_have_no_server_session_back_reference() -> None:
    """Every _SessionState field annotation must avoid ServerSession."""
    fields = dataclasses.fields(_SessionState)
    for f in fields:
        # Annotations may be strings (PEP 563 / from __future__) or types;
        # repr covers both.
        text = repr(f.type)
        assert "ServerSession" not in text, (
            f"field {f.name!r} carries 'ServerSession' in its annotation "
            f"({text!r}); WeakKeyDictionary semantics require no strong "
            "back-references"
        )


def test_assert_no_session_back_reference_helper_passes() -> None:
    """The runtime helper agrees with the static dataclass introspection."""
    assert_no_session_back_reference()
