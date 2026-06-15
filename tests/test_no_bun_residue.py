"""Regression scanner: fails if any shipped artifact re-introduces Bun /
mcp-server residue.

The Bun-based MCP server was retired in favour of the Python implementation.
Any reintroduction of Bun primitives, the retired npm package name, or the
retired directory path in shipped artifacts is a packaging regression.

Scanned scope (mirrors the sdist only-include list in pyproject.toml):
  waitbus/**/*.py, systemd/**, SKILL.md, README.md, SECURITY.md,
  pyproject.toml, server.json, .mcp.json, .claude-plugin/**,
  .pre-commit-config.yaml

Whitelist (history / design docs that legitimately discuss the retired impl):
  CHANGELOG.md, DECISION_LOG.md,
  docs/AUDIT_*.md, docs/PLAN_*.md, docs/research/**, docs/launch-articles/**,
  any path containing "FAIL-", docs/adversarial-review/**,
  tests/test_no_bun_residue.py (contains the patterns by necessity).
"""

from __future__ import annotations

from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[1]

# Patterns that must never appear in shipped artifacts.
_BANNED_PATTERNS: tuple[str, ...] = (
    "bun ",  # bun CLI invocation (trailing space avoids false word hits)
    "bunx ",  # bunx package runner
    "bun:ffi",  # Bun FFI import specifier
    "@waitbus/mcp-server",  # retired npm package name
    "mcp-server/",  # retired directory path
    "seqpacket.ts",  # source file from retired Bun implementation
)

# Glob patterns that define the shipped-artifact scope.
_INCLUDED_GLOBS: tuple[str, ...] = (
    "waitbus/**/*.py",
    "systemd/**",
    "SKILL.md",
    "README.md",
    "SECURITY.md",
    "pyproject.toml",
    "server.json",
    ".mcp.json",
    ".claude-plugin/**",
    ".pre-commit-config.yaml",
)

# Path prefixes / exact paths that are whitelisted (historical / design docs).
_EXCLUDED_PREFIXES: tuple[str, ...] = (
    "CHANGELOG.md",
    "DECISION_LOG.md",
    "docs/AUDIT_",
    "docs/PLAN_",
    "docs/research/",
    "docs/launch-articles/",
    "docs/adversarial-review/",
    "__pycache__/",
    ".venv/",
    "dist/",
)

# Exact relative path for this file itself; it must not self-trigger.
_THIS_FILE = "tests/test_no_bun_residue.py"


def _is_excluded(rel: str) -> bool:
    if rel == _THIS_FILE:
        return True
    if "FAIL-" in rel:
        return True
    return any(rel.startswith(p) for p in _EXCLUDED_PREFIXES)


def _collect_cases() -> list[tuple[Path, str]]:
    """Return (file_path, banned_pattern) pairs for parameterisation."""
    files: set[Path] = set()
    for pattern in _INCLUDED_GLOBS:
        for path in _ROOT.glob(pattern):
            if not path.is_file():
                continue
            rel = path.relative_to(_ROOT).as_posix()
            if _is_excluded(rel):
                continue
            files.add(path)

    cases: list[tuple[Path, str]] = []
    for path in sorted(files):
        for pat in _BANNED_PATTERNS:
            cases.append((path, pat))
    return cases


_CASES = _collect_cases()
_IDS = [f"{p.relative_to(_ROOT).as_posix()}::{pat!r}" for p, pat in _CASES]


@pytest.mark.parametrize("file_path,pattern", _CASES, ids=_IDS)
def test_no_bun_residue(file_path: Path, pattern: str) -> None:
    """Fail with an actionable message if banned pattern appears in file."""
    try:
        text = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        pytest.skip(f"{file_path}: not UTF-8, skipping bun-residue scan")

    matches: list[tuple[int, str]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if pattern in line:
            matches.append((lineno, line.rstrip()))

    rel = file_path.relative_to(_ROOT).as_posix()
    assert not matches, (
        f"Bun / mcp-server residue found in {rel!r} (pattern={pattern!r}):\n"
        + "\n".join(f"  L{ln}: {body}" for ln, body in matches)
        + "\n\nThe Bun MCP server is retired. Remove the reference or add the "
        "file to the whitelist in tests/test_no_bun_residue.py if it is a "
        "legitimate historical document."
    )
