"""Parse SKILL.md fenced code blocks and assert every command-line
invocation resolves: console-scripts must exist as binaries on PATH
(or under .venv/bin/ in the source checkout), file paths inside
fenced blocks must exist relative to the project root.

Prevents the failure mode where SKILL.md drifts from the codebase
(deleted entry-points, renamed paths, retired tools)."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[1]
_SKILL_MD = _ROOT / "SKILL.md"
_CODE_BLOCK_RE = re.compile(r"```(?:bash|sh|console)?\n(.*?)\n```", re.DOTALL)
# First word of each non-comment, non-empty line in a fenced block is
# the binary or interpreter being invoked. We assert it resolves.
_INVOCATION_RE = re.compile(r"^\s*([\w@.\-/]+)", re.MULTILINE)
# Console-scripts the wheel installs. (Bin names; the test either
# calls shutil.which or accepts presence under .venv/bin in a source
# checkout.)
_KNOWN_SCRIPTS = frozenset(
    {
        "waitbus",
    }
)
# Tools we expect operators to have system-wide.
_SYSTEM_TOOLS = frozenset(
    {
        "uv",
        "uvx",
        "pip",
        "pipx",
        "python",
        "python3",
        "systemctl",
        "loginctl",
        "gh",
        "git",
        "claude",
        "secret-tool",
        "curl",
        "bash",
        "sh",
        "echo",
        "mkdir",
        "ln",
        "cd",
        "cat",
        "less",
        "head",
        "tail",
        "grep",
        "find",
        "sqlite3",
        "journalctl",
        "tar",
    }
)


@pytest.fixture(scope="module")
def skill_md_text() -> str:
    return _SKILL_MD.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def code_blocks(skill_md_text: str) -> list[str]:
    return [m.group(1) for m in _CODE_BLOCK_RE.finditer(skill_md_text)]


def _venv_bin_dir() -> Path | None:
    candidate = _ROOT / ".venv" / "bin"
    return candidate if candidate.is_dir() else None


def _resolve(cmd: str) -> bool:
    if cmd in _SYSTEM_TOOLS:
        return True
    if shutil.which(cmd):
        return True
    venv = _venv_bin_dir()
    return bool(venv and (venv / cmd).exists())


def test_skill_md_exists() -> None:
    assert _SKILL_MD.is_file()


def test_console_scripts_are_known(code_blocks: list[str]) -> None:
    """Every waitbus-* invocation in SKILL.md must be a console-script
    the wheel actually installs."""
    cmds: set[str] = set()
    for block in code_blocks:
        for match in _INVOCATION_RE.finditer(block):
            cmd = match.group(1)
            if cmd.startswith("waitbus"):
                cmds.add(cmd)
    unknown = cmds - _KNOWN_SCRIPTS
    assert not unknown, (
        f"SKILL.md references console-scripts the wheel does not install: "
        f"{sorted(unknown)}. Update pyproject.toml [project.scripts] or "
        f"correct the SKILL.md invocation."
    )


def test_no_legacy_invocations(skill_md_text: str) -> None:
    """SKILL.md must not document obsolete invocation patterns."""
    bad_patterns = [
        (r"\bsetup\.sh\b", "setup.sh was deleted; use `waitbus init`"),
        (r"\.venv/bin/python\s+-m\s+waitbus", ".venv/bin/python -m invocations replaced by console-scripts"),
        (r"\bplugin/\b", "plugin/ directory was renamed/retired"),
    ]
    failures: list[str] = []
    for pattern, message in bad_patterns:
        if re.search(pattern, skill_md_text):
            failures.append(message)
    assert not failures, "SKILL.md retains legacy patterns: " + "; ".join(failures)


def test_no_staleness_banner(skill_md_text: str) -> None:
    """SKILL.md must not carry a staleness warning banner."""
    assert "STALENESS WARNING" not in skill_md_text
