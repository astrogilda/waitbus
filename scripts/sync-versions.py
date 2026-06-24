#!/usr/bin/env python3
"""Propagate the canonical waitbus version across all manifests.

Canonical source: pyproject.toml [project].version.
Targets:
  - .claude-plugin/plugin.json (top-level Claude Code plugin manifest)
  - server.json (MCP Registry server manifest, including packages[].version)

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

_SUMMARY = "Propagate the canonical waitbus version across all manifests."

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
PLUGIN_JSON = ROOT / ".claude-plugin" / "plugin.json"
SERVER_JSON = ROOT / "server.json"
UV_LOCK = ROOT / "uv.lock"


def _pyproject() -> dict:
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


def main() -> int:
    ap = argparse.ArgumentParser(description=_SUMMARY)
    ap.add_argument("--check", action="store_true", help="Exit 1 if any manifest diverges from pyproject.toml.")
    args = ap.parse_args()

    v = canonical_version()
    diverged: list[tuple[Path, str | None]] = []

    for path in (PLUGIN_JSON,):
        data = json.loads(path.read_text())
        if data.get("version") != v:
            diverged.append((path, data.get("version")))
            if not args.check:
                data["version"] = v
                path.write_text(json.dumps(data, indent=2) + "\n")

    # server.json has top-level version PLUS per-package versions in packages[]
    s = json.loads(SERVER_JSON.read_text())
    server_drift = s.get("version") != v or any(pkg.get("version") != v for pkg in s.get("packages", []))
    if server_drift:
        diverged.append((SERVER_JSON, s.get("version")))
        if not args.check:
            s["version"] = v
            for pkg in s.get("packages", []):
                pkg["version"] = v
            SERVER_JSON.write_text(json.dumps(s, indent=2) + "\n")

    # uv.lock carries the project's own version in its editable self-package entry.
    # release-please bumps pyproject but not the lock, so guard it here too.
    if UV_LOCK.exists():
        lock_text = UV_LOCK.read_text()
        m = _uv_lock_self_version_match(lock_text, package_name())
        if m is not None and m.group(1) != v:
            diverged.append((UV_LOCK, m.group(1)))
            if not args.check:
                UV_LOCK.write_text(lock_text[: m.start(1)] + v + lock_text[m.end(1) :])

    if args.check and diverged:
        sys.stderr.write(f"Version drift detected (canonical = {v}):\n")
        for path, cur in diverged:
            sys.stderr.write(f"  {path}: {cur!r}\n")
        sys.stderr.write("\nRun `scripts/sync-versions.py` (without --check) to fix.\n")
        return 1
    if not args.check and diverged:
        print(f"Synchronized {len(diverged)} manifest(s) to version {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
