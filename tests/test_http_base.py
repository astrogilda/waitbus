"""The shared HTTP server base stays stdlib-only.

The base in waitbus/_http.py is consumed by both the webhook listener
and the broadcast daemon's metrics surface; a convenience import added
there would silently pull the listener's import graph (config, secrets,
DB) into the daemon process. This walks the module's imports and fails
on anything outside the standard library, enforcing the constraint the
decision log records.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from waitbus import _http

_MODULE_PATH = Path(_http.__file__)


def _imported_top_level_names() -> set[str]:
    tree = ast.parse(_MODULE_PATH.read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_http_base_imports_only_the_standard_library() -> None:
    imported = _imported_top_level_names()
    non_stdlib = {name for name in imported if name not in sys.stdlib_module_names}
    assert not non_stdlib, (
        f"waitbus/_http.py imports non-stdlib modules {sorted(non_stdlib)}; "
        "the shared HTTP base must stay stdlib-only so the metrics surface "
        "never pulls the listener's import graph into the daemon."
    )


def test_http_base_has_no_relative_imports() -> None:
    tree = ast.parse(_MODULE_PATH.read_text())
    relative = [node.module or "." for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.level > 0]
    assert not relative, (
        f"waitbus/_http.py carries relative imports {relative}; any waitbus-internal "
        "dependency would couple both HTTP surfaces to it."
    )


def test_both_http_surfaces_consume_the_shared_base() -> None:
    """The consolidation holds: no surface regrows a private subclass."""
    from waitbus import _metrics_http, listener

    for module in (listener, _metrics_http):
        source = Path(module.__file__).read_text()  # type: ignore[arg-type]
        tree = ast.parse(source)
        imports_base = any(
            isinstance(node, ast.ImportFrom)
            and node.module == "_http"
            and any(alias.name == "ReusableThreadingServer" for alias in node.names)
            for node in ast.walk(tree)
        )
        assert imports_base, f"{module.__name__} no longer imports the shared HTTP base"
        redefines = any(
            isinstance(node, ast.ClassDef)
            and any(isinstance(base, ast.Name) and base.id == "ThreadingHTTPServer" for base in node.bases)
            for node in ast.walk(tree)
        )
        assert not redefines, f"{module.__name__} regrew a private ThreadingHTTPServer subclass"
