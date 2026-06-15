"""Open-core boundary test for the SourceSpec plugin contract.

The `SourceSpec` dataclass is the public registration shape for
third-party event-source plugins via the `waitbus.sources.v1` entry-point
group. Per the open-core separability rule, the plugin
surface MUST NEVER carry network-coordination / relay / multi-tenant
fields — 

This test is the binding enforcement of that invariant. It walks the
`SourceSpec` dataclass via the AST (not via `inspect.signature`, which
could be shadowed by a metaclass) and asserts that none of its field
names match a forbidden prefix. The forbidden list is sourced from
`_protocol._FORBIDDEN_SOURCESPEC_FIELD_PREFIXES` so a single edit can
extend the rule.

If this test fails, revert the offending `SourceSpec` change.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from waitbus.sources._protocol import (
    _FORBIDDEN_SOURCESPEC_FIELD_PREFIXES,
    SourceSpec,
)

_PROTOCOL_FILE = Path(__file__).resolve().parent.parent / "waitbus" / "sources" / "_protocol.py"


def _sourcespec_fields_from_ast() -> tuple[str, ...]:
    """Parse `_protocol.py` and return the SourceSpec field names.

    Walking the AST catches field names a future refactor introduces even if the runtime `dataclasses.fields()` view is filtered or proxied.
    """
    tree = ast.parse(_PROTOCOL_FILE.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "SourceSpec":
            fields: list[str] = []
            for stmt in node.body:
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                    fields.append(stmt.target.id)
            return tuple(fields)
    raise AssertionError("SourceSpec class not found in _protocol.py")


def test_sourcespec_field_names_have_no_forbidden_prefix() -> None:
    """Every SourceSpec field name must not match a forbidden prefix.

    The forbidden-prefix list encodes the open-core separability rule:
    no relay, auth, account, OIDC, tenant, cluster, or endpoint fields
    in the plugin contract.
    """
    fields_via_ast = _sourcespec_fields_from_ast()
    assert fields_via_ast, "SourceSpec has no fields (parse error)"

    violations: list[tuple[str, str]] = []
    for field_name in fields_via_ast:
        for prefix in _FORBIDDEN_SOURCESPEC_FIELD_PREFIXES:
            if field_name.startswith(prefix):
                violations.append((field_name, prefix))

    assert not violations, (
        "SourceSpec field names violate the open-core separability rule. "
        "These names carry forbidden prefixes that signal a network-"
        "coordination / relay / multi-tenant role:\n"
        + "\n".join(f"  - {name!r} matched forbidden prefix {prefix!r}" for name, prefix in violations)
        + "\nRevert the SourceSpec field change. The plugin surface MUST stay open-core-safe."
    )


def test_sourcespec_ast_view_matches_runtime_struct_fields() -> None:
    """The AST-derived field list matches msgspec's ``__struct_fields__``.

    Cross-check guards against a future refactor that introduces a
    field via a metaclass / dynamic mechanism that the AST walk would
    miss. If runtime and AST views diverge, the test must be updated
    to reflect the new mechanism so the open-core check stays binding.

    Uses ``SourceSpec.__struct_fields__`` (msgspec.Struct's field
    inventory) rather than ``dataclasses.fields()`` because
    ``SourceSpec`` migrated from ``@dataclass(frozen=True)`` to
    ``msgspec.Struct(frozen=True)`` -- the dataclass introspector
    no longer applies.
    """
    fields_via_ast = set(_sourcespec_fields_from_ast())
    fields_via_runtime = set(SourceSpec.__struct_fields__)

    extra_ast = fields_via_ast - fields_via_runtime
    extra_runtime = fields_via_runtime - fields_via_ast

    assert not extra_ast and not extra_runtime, (
        "AST view of SourceSpec fields diverges from ``__struct_fields__``. "
        f"AST-only: {sorted(extra_ast)}; runtime-only: {sorted(extra_runtime)}. "
        "A field has been added via a non-AST-visible mechanism; update this "
        "test to traverse the new mechanism."
    )


def test_forbidden_prefix_list_is_non_empty() -> None:
    """Smoke check: the forbidden-prefix list itself is non-empty.

    Catches the failure mode where a future edit accidentally clears
    the list (which would make `test_sourcespec_field_names_have_no_forbidden_prefix`
    pass trivially).
    """
    assert _FORBIDDEN_SOURCESPEC_FIELD_PREFIXES, (
        "Forbidden-prefix list in _protocol.py is empty; the open-core boundary test is now vacuous. Restore the list."
    )
    # Floor the list at 5 prefixes (the original set: relay_, auth_,
    # account_, oidc_, tenant_, cluster_, endpoint_). If a future edit
    # legitimately shrinks the list, bump the floor down.
    assert len(_FORBIDDEN_SOURCESPEC_FIELD_PREFIXES) >= 5


@pytest.mark.parametrize(
    "candidate_name",
    [
        "relay_endpoint",
        "auth_token",
        "account_id",
        "oidc_issuer",
        "tenant_id",
        "cluster_name",
        "endpoint_url",
    ],
)
def test_forbidden_prefix_catches_canonical_violations(candidate_name: str) -> None:
    """Cross-check: each canonical violation is caught by at least one prefix.

    Sanity check that the prefix list actually covers the names it
    intends to cover. Catches the failure mode where a prefix is
    silently shortened (e.g. ``relay_`` → ``rly_``) so canonical
    violations would escape.
    """
    matched = any(candidate_name.startswith(prefix) for prefix in _FORBIDDEN_SOURCESPEC_FIELD_PREFIXES)
    assert matched, (
        f"Canonical violation {candidate_name!r} matched no forbidden "
        f"prefix in {_FORBIDDEN_SOURCESPEC_FIELD_PREFIXES!r}. The prefix "
        f"list has drifted from the open-core boundary contract; restore "
        f"coverage for this name shape."
    )
