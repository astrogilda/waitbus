#!/usr/bin/env python3
"""Propagate the canonical waitbus version across all manifests.

Canonical source: pyproject.toml [project].version.
Targets:
  - .claude-plugin/plugin.json (top-level Claude Code plugin manifest)
  - server.json (MCP Registry server manifest, including packages[].version)
  - uv.lock (the editable self-package's own version entry)
  - CITATION.cff (the `version:` line GitHub reads for "Cite this repository")

Usage:
  scripts/sync-versions.py          # propagate (mutates files; exit 0)
  scripts/sync-versions.py --check  # exit 1 if any manifest diverges

The --check mode is run by .pre-commit-config.yaml's hook and by CI to
block drift from landing on main.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

_SUMMARY = "Propagate the canonical waitbus version across all manifests."

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
PLUGIN_JSON = ROOT / ".claude-plugin" / "plugin.json"
SERVER_JSON = ROOT / "server.json"
UV_LOCK = ROOT / "uv.lock"
CITATION_CFF = ROOT / "CITATION.cff"


def _pyproject() -> dict[str, Any]:
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)


def canonical_version() -> str:
    """Read [project].version from pyproject.toml."""
    return str(_pyproject()["project"]["version"])


def package_name() -> str:
    """Read [project].name from pyproject.toml."""
    return str(_pyproject()["project"]["name"])


def _uv_lock_self_version_match(text: str, name: str) -> re.Match[str] | None:
    """Locate the editable self-package's version line in uv.lock.

    Matches the project's own ``[[package]]`` entry (``name`` then ``version``),
    not the ``{ name = ... }`` dependency references elsewhere in the lock.
    """
    return re.search(rf'(?m)^name = "{re.escape(name)}"\nversion = "([^"]+)"', text)


# --- Per-manifest drift checks -------------------------------------------
#
# Each returns ``(path, current_version)`` when the manifest diverges from the
# canonical ``v`` (writing the fix first when ``apply`` is set), or ``None`` when
# it is already in sync. Keeping each manifest's read/detect/write on its own
# function keeps ``main`` a thin orchestrator; the per-file branching that used
# to accumulate inline no longer piles up in a single function.


def _sync_plugin_json(v: str, apply: bool) -> tuple[Path, str | None] | None:
    """Sync ``.claude-plugin/plugin.json``'s top-level ``version``."""
    data = json.loads(PLUGIN_JSON.read_text())
    current = data.get("version")
    if current == v:
        return None
    if apply:
        data["version"] = v
        PLUGIN_JSON.write_text(json.dumps(data, indent=2) + "\n")
    return (PLUGIN_JSON, current)


def _sync_server_json(v: str, apply: bool) -> tuple[Path, str | None] | None:
    """Sync ``server.json``'s top-level version PLUS every ``packages[].version``."""
    s = json.loads(SERVER_JSON.read_text())
    current = s.get("version")
    drift = current != v or any(pkg.get("version") != v for pkg in s.get("packages", []))
    if not drift:
        return None
    if apply:
        s["version"] = v
        for pkg in s.get("packages", []):
            pkg["version"] = v
        SERVER_JSON.write_text(json.dumps(s, indent=2) + "\n")
    return (SERVER_JSON, current)


def _sync_uv_lock(v: str, apply: bool) -> tuple[Path, str | None] | None:
    """Sync ``uv.lock``'s editable self-package version entry.

    release-please bumps pyproject but not the lock, so guard it here too.
    """
    if not UV_LOCK.exists():
        return None
    lock_text = UV_LOCK.read_text()
    m = _uv_lock_self_version_match(lock_text, package_name())
    if m is None or m.group(1) == v:
        return None
    if apply:
        UV_LOCK.write_text(lock_text[: m.start(1)] + v + lock_text[m.end(1) :])
    return (UV_LOCK, m.group(1))


def _sync_citation_cff(v: str, apply: bool) -> tuple[Path, str | None] | None:
    """Sync ``CITATION.cff``'s top-level ``version:`` line.

    This is the line GitHub's "Cite this repository" reads. release-please's
    generic updater bumps it on release via the ``# x-release-please-version``
    annotation; this guard trips CI if it ever drifts from pyproject between
    releases. The write splices only the captured group, so any inline
    annotation comment on the line is preserved.
    """
    if not CITATION_CFF.exists():
        return None
    cff_text = CITATION_CFF.read_text()
    m = re.search(r"(?m)^version:[ \t]*(\S+)", cff_text)
    if m is None or m.group(1) == v:
        return None
    if apply:
        CITATION_CFF.write_text(cff_text[: m.start(1)] + v + cff_text[m.end(1) :])
    return (CITATION_CFF, m.group(1))


def main() -> int:
    ap = argparse.ArgumentParser(description=_SUMMARY)
    ap.add_argument("--check", action="store_true", help="Exit 1 if any manifest diverges from pyproject.toml.")
    args = ap.parse_args()

    v = canonical_version()
    apply = not args.check
    checks = (_sync_plugin_json, _sync_server_json, _sync_uv_lock, _sync_citation_cff)
    diverged = [result for check in checks if (result := check(v, apply)) is not None]

    if args.check and diverged:
        sys.stderr.write(f"Version drift detected (canonical = {v}):\n")
        for path, cur in diverged:
            sys.stderr.write(f"  {path}: {cur!r}\n")
        sys.stderr.write("\nRun `scripts/sync-versions.py` (without --check) to fix.\n")
        return 1
    if apply and diverged:
        print(f"Synchronized {len(diverged)} manifest(s) to version {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
