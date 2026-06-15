#!/usr/bin/env python3
"""Maintainer-import / sdist-exclude pairing invariant.

The waitbus sdist ships a small, explicit set of paths (the
``[tool.hatch.build.targets.sdist].only-include`` list in
``pyproject.toml``): ``waitbus/``, the systemd / launchd assets,
and the entire ``tests/`` tree. The maintainer-only trees ``scripts/``
and ``benchmarks/`` are deliberately NOT shipped.

That creates an invariant: any test module that imports a maintainer-
only top-level package (``scripts`` or ``benchmarks``) at module scope
MUST itself be excluded from the sdist. Otherwise an unpacked sdist's
``pytest --collect-only`` raises ``ModuleNotFoundError`` against the
missing ``scripts.*`` / ``benchmarks.*`` import at collection time and
a downstream rebuild from the published sdist fails.

Historically the pairing was maintained as two hand-mirrored lists in
``pyproject.toml`` (``[tool.hatch.build.targets.sdist].exclude`` and
``[tool.check-manifest].ignore``) whose "source of truth" comment
claimed a ``grep -rln "^from scripts\\." tests/`` derivation. The grep
form missed two real classes:

* tests that ``import benchmarks...`` (not ``from scripts.``) — the
  whole ``test_bench_*`` family,
* tests that ``from scripts.<sub>`` import a submodule rather than the
  top package literally as ``from scripts.``.

and it over-matched tests that merely reference a ``scripts/...`` path
string for subprocess invocation (no module import → no collection-time
``ModuleNotFoundError`` → must NOT be excluded). The lists drifted.

This module replaces the grep with an AST walk that is the single
source of truth, consumed by both ``tests/test_pyproject_invariants.py``
(the 4th structural invariant) and the ``sdist-test-pairing`` pre-commit
hook. It is maintainer-only: it lives under ``scripts/`` and is itself
sdist-excluded, so importing it from the invariant test pairs that test
into the excluded set too (which is correct — the invariant is a
maintainer-side gate, not a consumer surface).
"""

from __future__ import annotations

import ast
import sys
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, TypeGuard

# Top-level packages that are present in the git tree but NOT shipped in
# the sdist. Any test importing one of these at module scope must be
# sdist-excluded. Derived structurally below via
# ``maintainer_only_roots`` rather than hardcoded at call sites.
_NON_SHIPPED_ROOTS = frozenset({"scripts", "benchmarks"})


def repo_root() -> Path:
    """Return the project root (the directory containing ``pyproject.toml``)."""
    return Path(__file__).resolve().parents[1]


def shipped_top_level_roots(pyproject: dict[str, Any]) -> frozenset[str]:
    """Return the set of top-level package roots shipped in the sdist.

    Read from ``[tool.hatch.build.targets.sdist].only-include``. A path
    entry's first path segment is its top-level root (e.g. ``waitbus``
    from ``waitbus``, ``tests`` from ``tests``). Only single-segment
    importable roots matter for the import-resolution check; file entries
    like ``README.md`` are harmless to include.
    """
    only_include = pyproject["tool"]["hatch"]["build"]["targets"]["sdist"]["only-include"]
    return frozenset(entry.split("/")[0] for entry in only_include)


def maintainer_only_roots(pyproject: dict[str, Any]) -> frozenset[str]:
    """Return the non-shipped maintainer-only roots, confirmed against pyproject.

    The candidate roots (``scripts``, ``benchmarks``) are confirmed to be
    genuinely absent from the sdist ``only-include`` so the determination
    is not hardcoded blindly: a future pyproject change that starts
    shipping one of them would shrink this set automatically.
    """
    shipped = shipped_top_level_roots(pyproject)
    return frozenset(root for root in _NON_SHIPPED_ROOTS if root not in shipped)


# File-loader callables that, when invoked at module scope against a
# maintainer-only path, execute that maintainer file at pytest collection
# time — exactly the failure ``import scripts...`` causes, but via the
# importlib file-loader API rather than the import statement. Matched on
# the call's attribute leaf name (``spec_from_file_location``,
# ``exec_module``, ``SourceFileLoader``) so both
# ``importlib.util.spec_from_file_location`` and an aliased import resolve.
_FILE_LOADER_LEAVES = frozenset({"spec_from_file_location", "SourceFileLoader", "exec_module"})


def _import_statement_roots(tree: ast.Module) -> set[str]:
    """Return top-level package roots imported via ``import`` / ``from`` at module scope.

    Walks the whole module (``ast.walk``) so imports grouped near a
    fixture section — the project's ``E402`` test convention — are still
    captured. Relative imports (``from . import x``) carry ``level > 0``
    and resolve within ``tests/`` itself, so they never reference a
    maintainer-only top-level package and are skipped.
    """
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def _iter_module_scope_nodes(tree: ast.Module) -> Iterator[ast.AST]:
    """Yield every AST node that executes at module scope.

    Nodes nested inside a ``def`` / ``async def`` / ``class`` body run at
    test-run time, not at pytest collection time, so they cannot break
    ``--collect-only`` and are skipped along with their whole subtree. The
    traversal is otherwise a full descent, so a module-scope expression
    arbitrarily deep inside a comprehension or call chain is still yielded.
    This single walker is the shared substrate for the two module-scope
    detectors below (the file-loader-call probe and the path-component
    collector), which differ only in what they extract from the yielded nodes.
    """

    def walk(node: ast.AST) -> Iterator[ast.AST]:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            yield child
            yield from walk(child)

    return walk(tree)


def _call_leaf(call: ast.Call) -> str:
    """Return the trailing name of a call target (``Path``, ``join``, ...).

    ``foo.bar.Baz(...)`` and ``Baz(...)`` both resolve to ``Baz``; anything
    else (a call on a subscript, a lambda result, ...) resolves to ``""``.
    """
    func = call.func
    return func.attr if isinstance(func, ast.Attribute) else func.id if isinstance(func, ast.Name) else ""


def _has_module_scope_file_loader_call(tree: ast.Module) -> bool:
    """True when a module-scope statement calls an importlib file-loader.

    Only module-scope calls count — a loader call inside a function or class
    body executes at test-run time, not at collection time, so it does not
    break ``--collect-only``. Matches the call's attribute / name leaf
    against ``_FILE_LOADER_LEAVES`` (``spec_from_file_location`` /
    ``SourceFileLoader`` / ``exec_module``).
    """
    return any(
        isinstance(node, ast.Call) and _call_leaf(node) in _FILE_LOADER_LEAVES
        for node in _iter_module_scope_nodes(tree)
    )


def _string_components(node: ast.expr) -> set[str]:
    """Return the ``/``-split string-literal components of a path-builder node.

    A *path-builder* node is either a ``Path(...)`` / ``os.path.join(...)``
    call (each string-literal argument is a component) or a ``/`` ``BinOp``
    chain rooted at such a call (each string-literal operand is a component).
    Walking the node's whole subtree collects every string literal that
    participates in building the path, including nested ``Path("a") / "b"``
    forms. Each literal is split on ``"/"`` so a single ``"a/scripts/b"``
    argument still yields the ``scripts`` component.
    """
    components: set[str] = set()
    for inner in ast.walk(node):
        if isinstance(inner, ast.Constant) and isinstance(inner.value, str):
            components.update(inner.value.split("/"))
    return components


def _is_path_builder(node: ast.AST) -> TypeGuard[ast.expr]:
    """True when ``node`` constructs a filesystem path from string components.

    Recognises the two idioms a test uses to point an importlib file-loader
    at a maintainer tree: a ``Path(...)`` / ``os.path.join(...)`` call, or a
    ``/`` ``BinOp`` whose operand chain bottoms out in a ``Path(...)`` call
    (the ``Path(__file__).parent / "scripts" / "x.py"`` form). Restricting the
    component scan to these nodes is what keeps an unrelated module-scope
    string literal (a log message, a marker name) from being mistaken for a
    maintainer-tree path component.
    """
    if isinstance(node, ast.Call):
        return _call_leaf(node) in {"Path", "join"}
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        return any(isinstance(sub, ast.Call) and _call_leaf(sub) == "Path" for sub in ast.walk(node))
    return False


def _module_scope_path_components(tree: ast.Module) -> set[str]:
    """Return string-literal components of module-scope path-builder expressions.

    Captures the ``"scripts"`` / ``"benchmarks"`` literals in the
    ``Path(__file__).parent.parent / "scripts" / "x.py"`` idiom (and the
    ``os.path.join(..., "scripts", ...)`` / ``Path("scripts", "x.py")``
    forms), where the maintainer-tree name is a standalone path component. The
    scan is restricted to path-builder nodes (see :func:`_is_path_builder`):
    an arbitrary module-scope string Constant that merely happens to contain
    ``"scripts"`` — a marker name, a docstring fragment — is NOT a path
    component and must not contribute, or it would spuriously pair a test that
    holds an unrelated literal plus any file-loader call. Function / class
    bodies are skipped (they run at test time, not collection time).
    """
    components: set[str] = set()
    for node in _iter_module_scope_nodes(tree):
        if _is_path_builder(node):
            components.update(_string_components(node))
    return components


def _module_scope_loads_maintainer_path(tree: ast.Module, maintainer_roots: frozenset[str]) -> bool:
    """True when a module-scope file-loader call targets a maintainer-only path.

    A test can break collection without an ``import scripts`` statement:
    ``SCRIPT = Path(__file__).parent.parent / "scripts" / "x.py"`` then
    ``importlib.util.spec_from_file_location(name, SCRIPT)`` +
    ``loader.exec_module(...)`` at module scope executes the maintainer file
    at collection time, raising ``FileNotFoundError`` when the ``scripts/``
    tree is absent from an unpacked sdist. This catches that arm.

    Conservative: fires only when the module BOTH performs a module-scope
    importlib file-loader call AND references a maintainer-root name
    (``scripts`` / ``benchmarks``) as a module-scope path component. Either
    alone is insufficient — a loader call against a shipped path, or a stray
    ``"scripts"`` string with no loader, must not trip the gate.
    """
    if not _has_module_scope_file_loader_call(tree):
        return False
    return bool(_module_scope_path_components(tree) & maintainer_roots)


def compute_maintainer_importer_tests(root: Path, pyproject: dict[str, Any]) -> set[str]:
    """Return the canonical set of ``tests/*.py`` files that must be sdist-excluded.

    A test file qualifies iff, at module scope, it EITHER imports a
    maintainer-only top-level package (``scripts`` / ``benchmarks``) via an
    ``import`` / ``from`` statement, OR executes a maintainer-only file via
    an importlib file-loader call (``spec_from_file_location`` /
    ``exec_module`` / ``SourceFileLoader``) against a ``scripts/`` or
    ``benchmarks/`` path. Both break ``pytest --collect-only`` on an
    unpacked sdist when the maintainer tree is absent. Returned paths are
    repo-relative POSIX strings (``tests/test_foo.py``) matching the
    pyproject exclude / ignore entry style.
    """
    maintainer_roots = maintainer_only_roots(pyproject)
    result: set[str] = set()
    for test_file in sorted((root / "tests").glob("*.py")):
        tree = ast.parse(test_file.read_text(), filename=str(test_file))
        imports_maintainer = bool(_import_statement_roots(tree) & maintainer_roots)
        loads_maintainer = _module_scope_loads_maintainer_path(tree, maintainer_roots)
        if imports_maintainer or loads_maintainer:
            result.add(f"tests/{test_file.name}")
    return result


def sdist_exclude_test_set(pyproject: dict[str, Any]) -> set[str]:
    """Return the ``tests/*`` entries from ``[sdist].exclude``."""
    exclude = pyproject["tool"]["hatch"]["build"]["targets"]["sdist"]["exclude"]
    return {entry for entry in exclude if entry.startswith("tests/")}


def check_manifest_ignore_test_set(pyproject: dict[str, Any]) -> set[str]:
    """Return the ``tests/*`` entries from ``[tool.check-manifest].ignore``."""
    ignore = pyproject["tool"]["check-manifest"]["ignore"]
    return {entry for entry in ignore if entry.startswith("tests/")}


def pairing_mismatches(root: Path | None = None) -> list[str]:
    """Return human-readable mismatch messages; empty list means the invariant holds.

    Compares the AST-computed canonical set against both pyproject lists
    and verifies the two lists agree with each other.
    """
    root = root or repo_root()
    pyproject = tomllib.loads((root / "pyproject.toml").read_text())
    computed = compute_maintainer_importer_tests(root, pyproject)
    sdist_set = sdist_exclude_test_set(pyproject)
    manifest_set = check_manifest_ignore_test_set(pyproject)

    problems: list[str] = []
    if computed != sdist_set:
        missing = sorted(computed - sdist_set)
        stale = sorted(sdist_set - computed)
        problems.append(
            "[tool.hatch.build.targets.sdist].exclude drifted from the AST-computed "
            "maintainer-importer set:\n"
            f"  missing (import a maintainer module but not excluded): {missing}\n"
            f"  stale (excluded but no longer import a maintainer module): {stale}"
        )
    if computed != manifest_set:
        missing = sorted(computed - manifest_set)
        stale = sorted(manifest_set - computed)
        problems.append(
            "[tool.check-manifest].ignore drifted from the AST-computed "
            "maintainer-importer set:\n"
            f"  missing: {missing}\n"
            f"  stale: {stale}"
        )
    if sdist_set != manifest_set:
        problems.append(
            "[sdist].exclude and [check-manifest].ignore tests/* entries disagree:\n"
            f"  in sdist not manifest: {sorted(sdist_set - manifest_set)}\n"
            f"  in manifest not sdist: {sorted(manifest_set - sdist_set)}"
        )
    return problems


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the ``sdist-test-pairing`` pre-commit hook.

    Exit 0 when the pairing invariant holds, 1 with the mismatch report on
    stderr otherwise. ``argv`` is accepted for symmetry with the other
    hook scripts; no flags are consumed.
    """
    problems = pairing_mismatches()
    if problems:
        sys.stderr.write(
            "sdist-test-pairing invariant FAILED. The two pyproject lists "
            "([tool.hatch.build.targets.sdist].exclude and "
            "[tool.check-manifest].ignore) must each equal the set of "
            "tests/*.py files that import a maintainer-only package "
            "(scripts / benchmarks) at module scope.\n\n"
        )
        for problem in problems:
            sys.stderr.write(problem + "\n\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
