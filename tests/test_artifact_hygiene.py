"""Fails if any production-shipped artifact embeds an absolute
home-directory path.

Scans the shipped artifact set and trips on any
`/home/<lower-letter>...` substring, preventing operator-specific paths
from leaking into the distributed wheel.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[1]
_ABSOLUTE_HOME_RE = re.compile(r"/home/[a-z][^/\s]*")

_INCLUDED_GLOBS: tuple[str, ...] = (
    "waitbus/**/*.py",
    "systemd/**/*",
    ".claude-plugin/**/*",
    "SKILL.md",
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CHANGELOG.md",
    "pyproject.toml",
    "server.json",
    ".mcp.json",
)

_EXCLUDED_DIRS: tuple[str, ...] = (
    "docs/",
    "__pycache__/",
    ".venv/",
    "node_modules/",
)
_EXCLUDED_FILES: tuple[str, ...] = ("tests/test_artifact_hygiene.py",)


def _is_excluded(rel_path: str) -> bool:
    if rel_path in _EXCLUDED_FILES:
        return True
    return any(rel_path.startswith(d) for d in _EXCLUDED_DIRS)


def _collect_files() -> list[Path]:
    files: set[Path] = set()
    for pattern in _INCLUDED_GLOBS:
        for path in _ROOT.glob(pattern):
            if not path.is_file():
                continue
            rel = path.relative_to(_ROOT).as_posix()
            if _is_excluded(rel):
                continue
            files.add(path)
    return sorted(files)


@pytest.mark.parametrize("path", _collect_files(), ids=lambda p: p.name)
def test_no_absolute_home_paths(path: Path) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        pytest.skip(f"{path}: not utf-8, skipping artifact-hygiene scan")
    matches: list[tuple[int, str]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if _ABSOLUTE_HOME_RE.search(line):
            matches.append((lineno, line.rstrip()))
    assert not matches, (
        f"{path.relative_to(_ROOT)} contains hardcoded /home/<user>/ paths:\n"
        + "\n".join(f"  L{ln}: {body}" for ln, body in matches)
        + "\nReplace with %h/ or %S/ specifiers (systemd) or `Path.home()` "
        "/ `platformdirs` (Python) so the artifact ships portably."
    )
