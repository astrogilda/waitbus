"""Local-primitive boundary test for the public subscribe SDK.

Per the local-primitive separability rule, the public ``waitbus.subscribe``
SDK MUST NOT assume, in signature / behaviour / imports, a managed relay,
account, multi-tenant, or network-coordination context. The subscribe surface
stays a pure local primitive: relay / auth-token / tenant / cluster concepts
must not leak into the SDK.

This test is the binding enforcement: it AST-walks each public-SDK module
(``_subscribe.py`` and the addressed-messaging ``_messaging.py``) and asserts no
identifier (parameter, local, attribute, or imported name) carries a forbidden
network-coordination prefix. The forbidden vocabulary is sourced from
``_protocol._FORBIDDEN_SOURCESPEC_FIELD_PREFIXES`` so the SDK and the plugin
contract share ONE local-primitive word-list.

If this fails, the fix is to revert the offending SDK change, NOT to weaken
the test.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from waitbus.sources._protocol import _FORBIDDEN_SOURCESPEC_FIELD_PREFIXES

_PKG = Path(__file__).resolve().parent.parent / "waitbus"
# Every module that is part of the local SDK surface and must stay free of
# network-coordination vocabulary. Addressing (``msg_to`` / ``msg_from`` / etc.)
# is LOCAL routing, not relay/tenant/account, so it does not match the word-list.
_SDK_FILES = (_PKG / "_subscribe.py", _PKG / "_messaging.py")

# `socket_path` is the SANCTIONED local seam (a local proxy is reached by a
# different path). It is local, not network-coordination, so it is explicitly
# allowed.
_ALLOWED_IDENTIFIERS = frozenset({"socket_path", "socket", "_ASUBSCRIBE_QUEUE_MAXSIZE"})


def _all_identifiers(source: str) -> set[str]:
    """Every Name id, arg name, attribute attr, and keyword arg in the module."""
    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.keyword) and node.arg is not None:
            names.add(node.arg)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add((alias.asname or alias.name).split(".")[0])
    return names


@pytest.mark.parametrize("sdk_file", _SDK_FILES, ids=lambda p: p.name)
def test_subscribe_sdk_has_no_network_coordination_symbols(sdk_file: Path) -> None:
    source = sdk_file.read_text(encoding="utf-8")
    identifiers = _all_identifiers(source)
    violations: list[tuple[str, str]] = []
    for ident in identifiers:
        if ident in _ALLOWED_IDENTIFIERS:
            continue
        low = ident.lower()
        for prefix in _FORBIDDEN_SOURCESPEC_FIELD_PREFIXES:
            if low.startswith(prefix):
                violations.append((ident, prefix))
    assert not violations, (
        f"{sdk_file.name} carries network-coordination symbols, violating the "
        "local-primitive separability rule (the SDK must stay a LOCAL primitive):\n"
        + "\n".join(f"  - {name!r} matched forbidden prefix {prefix!r}" for name, prefix in violations)
        + "\nKeep the subscribe SDK a local primitive; do not add network-coordination symbols."
    )
