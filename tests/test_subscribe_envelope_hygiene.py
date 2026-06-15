"""Grep-guard: assert that every hand-built subscribe envelope site is on the
explicit allow-list.

``encode_frame(json.dumps(...))`` is the low-level pattern for constructing
subscribe envelopes directly on the wire.  The canonical Python site is
``waitbus/_broadcast_sub.py``; new Python consumer code must use
``open_subscriber`` instead.  Non-Python snippets and the tests that probe
daemon edge cases (reject, ack framing) are explicitly allowed.

Adding a new hand-built site anywhere in the repo requires updating
``ALLOWED_HAND_BUILT_ENVELOPE_FILES`` below —
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Explicit allow-list of files permitted to hand-build subscribe envelopes
# via encode_frame(json.dumps(...)) or the language-equivalent pattern.
# All paths are relative to REPO_ROOT.
# Every entry MUST currently contain the pattern (enforced by
# test_allow_list_contains_no_phantom_entries). The snippets and
# test_wire_v1_conformance.py used to hand-build envelopes but now build via
# language-idiom / the hoisted tests/_wire_helpers.subscribe, so they no
# longer match and were removed — the "must-match" invariant keeps this list
# self-cleaning instead of accumulating vestigial entries.
ALLOWED_HAND_BUILT_ENVELOPE_FILES = frozenset(
    {
        "waitbus/_broadcast_sub.py",  # canonical Python site
        "tests/test_mcp_e2e.py",
        "tests/test_coalesce_collapse_property.py",
        "tests/test_coalesce.py",
        "tests/test_broadcast_sub.py",
        "tests/_wire_helpers.py",
        "tests/test_subscribe_envelope_hygiene.py",  # this file -- references the pattern in docstrings
    }
)

# Directories to skip entirely during the walk.
_SKIP_DIRS = frozenset(
    {
        ".venv",
        ".git",
        "htmlcov",
        "__pycache__",
        "dist",
        "build",
        "benchmarks/baselines",
        "docs/audits",
        "docs/research",
        "docs/adversarial-review",
        ".serena",
        "node_modules",
    }
)

# Root-level narrative process docs that quote the pattern in prose (handoff
# evidence, the hygiene-rule description itself). They are not code and are
# excluded from the distribution; skip them like docs/audits + docs/research.
_SKIP_FILES = frozenset(
    {
        "SESSION_HANDOFF.md",
        "PROJECT_CONTEXT.md",
        "DECISION_LOG.md",
        "TODO.md",
    }
)

# Extensions to scan.
_SCAN_EXTENSIONS = frozenset({".py", ".go", ".rs", ".ts", ".md", ".sh"})

# Pattern: ``encode_frame`` immediately followed (same line) by ``json.dumps``.
# Captures the Python form; MD files that quote it verbatim are also caught.
_PATTERN = re.compile(r"encode_frame.*json\.dumps")


def _should_skip(rel: Path) -> bool:
    """Return True when *rel* is inside a directory that should not be scanned."""
    parts = rel.parts
    for skip in _SKIP_DIRS:
        skip_parts = tuple(skip.split("/"))
        # Check whether any prefix of parts matches the skip tuple.
        if parts[: len(skip_parts)] == skip_parts:
            return True
    return False


def _candidate_rel_paths() -> list[str]:
    """Posix rel paths of files git tracks or would track (respecting .gitignore).

    The guard scans SOURCE, not local artifacts. Enumerating via git
    (``--cached`` tracked + ``--others --exclude-standard`` untracked-but-not-
    ignored) means gitignored artifacts -- a local ``crawl-output/`` context
    dump, the ``.serena`` cache, the gitignored ``docs/research`` cache -- are
    never scanned, while a new untracked-but-not-ignored source file (exactly
    the drift this guard exists to catch before it ships) still is. Falls back
    to a filesystem walk only if git is unavailable (e.g. an unpacked sdist with
    no .git), preserving the guard there too.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return [p.relative_to(REPO_ROOT).as_posix() for p in REPO_ROOT.rglob("*") if p.is_file()]
    return [rel for rel in result.stdout.split("\0") if rel]


def _find_violations() -> list[tuple[str, int, str]]:
    """Return ``[(rel_path, lineno, line_text), ...]`` for every match NOT on the allow-list."""
    violations: list[tuple[str, int, str]] = []
    for rel_str in _candidate_rel_paths():
        rel = Path(rel_str)
        if rel.suffix not in _SCAN_EXTENSIONS:
            continue
        if _should_skip(rel) or rel_str in _SKIP_FILES:
            continue
        try:
            text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _PATTERN.search(line) and rel_str not in ALLOWED_HAND_BUILT_ENVELOPE_FILES:
                violations.append((rel_str, lineno, line.strip()))
    return violations


def test_no_unlisted_hand_built_subscribe_envelopes() -> None:
    """Every hand-built subscribe envelope site must be on the allow-list.

    New consumer code must use ``open_subscriber`` from
    ``waitbus._broadcast_sub`` instead of constructing subscribe
    frames by hand.  If a new allowed site is genuinely needed (e.g. a
    new language snippet or a test that probes the raw wire), add it to
    ``ALLOWED_HAND_BUILT_ENVELOPE_FILES`` in this file.
    """
    violations = _find_violations()
    if not violations:
        return
    lines = [f"  {rel}:{lineno}  {text}" for rel, lineno, text in sorted(violations)]
    pytest.fail(
        "Hand-built encode_frame(json.dumps(...)) found outside the allow-list.\n"
        "Add the file to ALLOWED_HAND_BUILT_ENVELOPE_FILES in\n"
        "tests/test_subscribe_envelope_hygiene.py, or refactor to use\n"
        "open_subscriber() from waitbus._broadcast_sub.\n\n"
        "Violations:\n" + "\n".join(lines)
    )


def test_allow_list_contains_no_phantom_entries() -> None:
    """Every allow-list entry must exist AND currently contain the pattern.

    Checking existence alone let entries go stale: a file that stopped
    hand-building envelopes (refactored to ``open_subscriber`` / the shared
    helper, or had the pattern removed from its prose) would linger on the
    list forever. Requiring a live pattern match makes the list self-cleaning
    — a stale entry is flagged and must be trimmed.
    """
    stale: list[str] = []
    for entry in ALLOWED_HAND_BUILT_ENVELOPE_FILES:
        path = REPO_ROOT / entry
        if not path.exists():
            stale.append(f"{entry} (missing on disk)")
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if not any(_PATTERN.search(line) for line in text.splitlines()):
            stale.append(f"{entry} (no longer matches the pattern)")
    if stale:
        pytest.fail(
            "Stale allow-list entries (trim them from ALLOWED_HAND_BUILT_ENVELOPE_FILES):\n"
            + "\n".join(f"  {e}" for e in sorted(stale))
        )
