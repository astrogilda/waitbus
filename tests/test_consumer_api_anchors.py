"""Lint test: backticked anchors in docs/CONSUMER_API.md and the launch articles.

Rules enforced for every backtick-quoted token in the document:

1. ``path.py:line`` form (e.g. ``_frame.py:34``) — FAIL.  Line-number
   anchors rot on every additive change.  Use symbol-name-only anchors
   instead.

2. ``path.py::symbol`` form (e.g.
   ``waitbus/broadcast.py::_validate_subscribe_filters``) —
   PASS only if ``ast.parse`` on the target file finds the symbol at
   module or class scope.  A missing symbol is a failure.

3. ``path.py`` only (file-only anchor) — assert the file exists under
   the repository root.

Non-Python anchors (e.g. ``docs/whatever.md``, ``schema.sql``) are
checked for existence (rule 3) but never for symbol presence (rule 2).

Coverage scope: rules 1 and 2 run over BOTH ``docs/CONSUMER_API.md`` and
every ``docs/launch-articles/*.md`` article — those are the rot-prone
anchors that point into the codebase.  Rule 3 (bare-file existence) runs
over ``CONSUMER_API.md`` ONLY: the launch articles legitimately name
external client-config files (``config.toml``, ``claude_desktop_config.json``,
``cline_mcp_settings.json``, ...) that do not — and must not — exist in this
repository, so a file-existence assertion over the articles is wrong by
construction.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DOC_PATH = _REPO_ROOT / "docs" / "CONSUMER_API.md"

# Launch articles also cite codebase anchors and must obey rules 1 and 2.
_ARTICLE_PATHS = sorted(p for p in (_REPO_ROOT / "docs" / "launch-articles").glob("*.md") if "OUTLINE" not in p.name)

# Docs subject to the rot-prone-anchor rules (path:line ban + path::symbol resolve).
_SYMBOL_RULE_DOCS = [_DOC_PATH, *_ARTICLE_PATHS]
_SYMBOL_RULE_IDS = [p.name for p in _SYMBOL_RULE_DOCS]

# Matches any backtick-quoted token that looks like a file path.
# Captures the raw inner text; the rules classify it further.
_BACKTICK_RE = re.compile(r"`([^`]+)`")

# path.py:line — e.g. `_frame.py:34` or `broadcast.py:118,125`
_PATH_LINE_RE = re.compile(r"^[A-Za-z0-9_./-]+\.py:\d")

# path.py::symbol — e.g. `waitbus/broadcast.py::_validate_subscribe_filters`
_PATH_SYMBOL_RE = re.compile(r"^([A-Za-z0-9_./-]+\.py)::([A-Za-z_][A-Za-z0-9_]*)$")

# Bare file path (no colons) — e.g. `_frame.py` or `docs/CONSUMER_API.md`
_FILE_ONLY_RE = re.compile(r"^[A-Za-z0-9_./-]+\.[A-Za-z]+$")


def _module_and_class_symbols(py_path: Path) -> frozenset[str]:
    """Return every name defined at module or class scope in *py_path*.

    Also recurses into top-level ``if`` / ``elif`` / ``else`` blocks so
    that platform-dispatch patterns (e.g. ``if sys.platform == 'linux':
    def peer_uid(...): ...``) are captured correctly.
    """
    src = py_path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src, filename=str(py_path))
    except SyntaxError as exc:
        raise AssertionError(f"ast.parse failed on {py_path}: {exc}") from exc
    names: set[str] = set()

    def _scan_body(stmts: list[ast.stmt]) -> None:
        for node in stmts:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                names.add(node.name)
            elif isinstance(node, ast.ClassDef):
                names.add(node.name)
                for child in node.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        names.add(child.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        names.add(target.id)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names.add(node.target.id)
            elif isinstance(node, ast.If):
                # Platform-dispatch and TYPE_CHECKING guards live here.
                _scan_body(node.body)
                _scan_body(node.orelse)

    _scan_body(tree.body)
    return frozenset(names)


def _collect_anchors(text: str) -> list[tuple[int, str]]:
    """Return (line_number, anchor_text) for every backtick token."""
    results: list[tuple[int, str]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for m in _BACKTICK_RE.finditer(line):
            results.append((lineno, m.group(1)))
    return results


@pytest.mark.parametrize("doc", _SYMBOL_RULE_DOCS, ids=_SYMBOL_RULE_IDS)
def test_no_path_line_anchors(doc: Path) -> None:
    """No backtick anchor of the form path.py:line should remain."""
    text = doc.read_text(encoding="utf-8")
    violations: list[str] = []
    for lineno, anchor in _collect_anchors(text):
        if _PATH_LINE_RE.match(anchor):
            violations.append(f"  line {lineno}: `{anchor}` — path:line anchors rot; use path::symbol instead")
    if violations:
        raise AssertionError(f"{doc.name}: {len(violations)} path:line anchor(s) found:\n" + "\n".join(violations))


@pytest.mark.parametrize("doc", _SYMBOL_RULE_DOCS, ids=_SYMBOL_RULE_IDS)
def test_path_symbol_anchors_resolve(doc: Path) -> None:
    """Every path::symbol anchor must resolve to an actual symbol via ast.parse."""
    text = doc.read_text(encoding="utf-8")
    violations: list[str] = []
    for lineno, anchor in _collect_anchors(text):
        m = _PATH_SYMBOL_RE.match(anchor)
        if not m:
            continue
        rel_path, symbol = m.group(1), m.group(2)
        # Resolve against repo root; also try with waitbus/ prefix stripped.
        candidates = [
            _REPO_ROOT / rel_path,
            _REPO_ROOT / "waitbus" / Path(rel_path).name,
        ]
        py_path = next((c for c in candidates if c.exists()), None)
        if py_path is None:
            violations.append(f"  line {lineno}: `{anchor}` — file not found (tried {[str(c) for c in candidates]})")
            continue
        # Skip non-Python files that somehow match the pattern.
        if py_path.suffix != ".py":
            continue
        names = _module_and_class_symbols(py_path)
        if symbol not in names:
            violations.append(
                f"  line {lineno}: `{anchor}` — symbol '{symbol}' not found "
                f"in {py_path.relative_to(_REPO_ROOT)} "
                f"(module-scope symbols: {sorted(names)[:10]}…)"
            )
    if violations:
        raise AssertionError(
            f"{doc.name}: {len(violations)} unresolvable path::symbol anchor(s):\n" + "\n".join(violations)
        )


def test_file_only_anchors_exist() -> None:
    """Every bare-file anchor (path with extension, no colons) must exist."""
    text = _DOC_PATH.read_text(encoding="utf-8")
    violations: list[str] = []
    for lineno, anchor in _collect_anchors(text):
        # Skip if it matches the other patterns.
        if _PATH_LINE_RE.match(anchor) or _PATH_SYMBOL_RE.match(anchor):
            continue
        if not _FILE_ONLY_RE.match(anchor):
            continue
        # Only lint if it looks like a path (contains a slash or a known extension).
        if "/" not in anchor and not anchor.endswith((".py", ".sql", ".md", ".toml", ".json")):
            continue
        candidates = [
            _REPO_ROOT / anchor,
            _REPO_ROOT / "waitbus" / anchor,
            _REPO_ROOT / "docs" / anchor,
        ]
        if not any(c.exists() for c in candidates):
            violations.append(
                f"  line {lineno}: `{anchor}` — file not found "
                f"(tried {[str(c.relative_to(_REPO_ROOT)) for c in candidates]})"
            )
    if violations:
        raise AssertionError(f"{_DOC_PATH.name}: {len(violations)} missing file anchor(s):\n" + "\n".join(violations))
