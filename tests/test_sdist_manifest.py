"""Sdist hygiene regression test.

Builds the source distribution from the project root and verifies three
contracts:

1. The set of shipped files matches `tests/data/expected-sdist-manifest.txt`
   exactly. Any new file (intentional or accidental) appears as a diff that
   must be reviewed and the snapshot regenerated per CONTRIBUTING.md.
2. No shipped file declares a GPL/LGPL/AGPL `SPDX-License-Identifier`. The
   project ships under MIT and license-incompatible content (e.g., kernel
   source mirrors copied into a research cache) must never leak into the
   sdist.
3. Neither `mcp-server/node_modules` nor `docs/research-cache` paths appear
   anywhere in the tarball. These are the two historical sources of bloat
   and license contamination this test guards against, including the
   `.gitignore` negate-rule attack where a downstream operator could add a
   negation entry and re-ship the cache.
An explicit compressed-tarball size ceiling (formerly `SIZE_BUDGET_BYTES`) was
retired when the test suite was added to the sdist: the tests-in-sdist practice favours shipping
the test suite over ratcheting a byte budget, so this test no longer asserts a
size limit. The manifest snapshot in contract 1 still catches any unexpected
file growth on review.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPECTED_MANIFEST = PROJECT_ROOT / "tests" / "data" / "expected-sdist-manifest.txt"

# Every sdist member is rooted at `waitbus-<version>/`. Strip that prefix so the
# snapshot is a version-agnostic file set. The contract this test guards is
# *which files ship*, not *what version* (sync-versions owns the version), so a
# pure version bump must not churn this snapshot line-for-line.
_SDIST_ROOT_RE = re.compile(r"^waitbus-[^/]+/")


def _strip_root(name: str) -> str:
    """Drop the ``waitbus-<version>/`` sdist root prefix from a member path."""
    return _SDIST_ROOT_RE.sub("", name, count=1)


COPYLEFT_RE = re.compile(
    rb"SPDX-License-Identifier:\s*(GPL|LGPL|AGPL)",
    re.IGNORECASE,
)
# Paths that must never appear in the sdist. `docs/research/` is a
# development-only documentation tree (not a
# shipped consumer asset; the shipped surface is the code under
# `waitbus/` plus the consumer-facing top-level docs). The
# legacy `docs/research-cache` entry is kept for contributor trees that
# may still carry the old cache path.
FORBIDDEN_PATH_FRAGMENTS = (
    "mcp-server/node_modules",
    "docs/research-cache",
    "docs/research/",
)

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="sdist hygiene is validated on POSIX runners; Windows packaging not in scope",
    ),
]


def _build_sdist(out_dir: Path) -> Path:
    """Invoke `python -m build --sdist` and return the produced tarball path.

    `python -m build` provisions an isolated build environment by installing the
    pinned backend over the network, which can fail transiently. Retry a bounded
    number of times so a one-off provisioning hiccup does not flake the suite; a
    persistent failure still surfaces with the captured stderr.
    """
    last_exc: subprocess.CalledProcessError | None = None
    for _ in range(3):
        try:
            subprocess.run(
                [sys.executable, "-m", "build", "--sdist", "--outdir", str(out_dir)],
                cwd=str(PROJECT_ROOT),
                check=True,
                capture_output=True,
            )
            break
        except subprocess.CalledProcessError as exc:
            last_exc = exc
    else:
        stderr = last_exc.stderr.decode(errors="replace") if last_exc else ""
        raise AssertionError(
            f"python -m build --sdist failed after 3 attempts (likely transient isolated-env provisioning):\n{stderr}"
        )
    candidates = sorted(out_dir.glob("waitbus-*.tar.gz"))
    if not candidates:
        raise AssertionError(f"no sdist produced in {out_dir}; got {list(out_dir.iterdir())}")
    return candidates[-1]


def test_sdist_manifest_matches_snapshot(tmp_path: Path) -> None:
    """Shipped file list equals the committed snapshot, sorted line-for-line."""
    sdist_path = _build_sdist(tmp_path)
    with tarfile.open(sdist_path, "r:gz") as tar:
        actual_members = sorted(_strip_root(m.name) for m in tar.getmembers() if m.isfile())

    expected = EXPECTED_MANIFEST.read_text().splitlines()
    expected = [line for line in expected if line.strip()]
    expected.sort()

    if actual_members != expected:
        added = sorted(set(actual_members) - set(expected))
        removed = sorted(set(expected) - set(actual_members))
        raise AssertionError(
            "sdist manifest drift detected.\n"
            f"  added (in sdist, not snapshot): {added}\n"
            f"  removed (in snapshot, not sdist): {removed}\n"
            "Regenerate via the procedure in CONTRIBUTING.md "
            "(`uv build --sdist && tar tzf dist/waitbus-*.tar.gz | "
            "sed -E 's|^waitbus-[^/]+/||' | grep -v '^$' | sort > "
            "tests/data/expected-sdist-manifest.txt`)."
        )


def test_sdist_has_no_copyleft_headers(tmp_path: Path) -> None:
    """No shipped file declares a GPL/LGPL/AGPL SPDX header."""
    sdist_path = _build_sdist(tmp_path)
    offenders: list[tuple[str, str]] = []
    with tarfile.open(sdist_path, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            handle = tar.extractfile(member)
            if handle is None:
                continue
            data = handle.read()
            match = COPYLEFT_RE.search(data)
            if match:
                offenders.append((member.name, match.group(0).decode("ascii", "replace")))
    assert not offenders, (
        f"license-contamination guard tripped — copyleft SPDX header found in shipped files: {offenders}"
    )


def test_sdist_excludes_forbidden_paths(tmp_path: Path) -> None:
    """node_modules and research-cache paths never appear in the tarball."""
    sdist_path = _build_sdist(tmp_path)
    with tarfile.open(sdist_path, "r:gz") as tar:
        member_names = [m.name for m in tar.getmembers()]
    leaked = [name for name in member_names if any(fragment in name for fragment in FORBIDDEN_PATH_FRAGMENTS)]
    assert not leaked, (
        f"forbidden paths leaked into sdist: {leaked}; "
        "check `.gitignore` for negate-rules and `[tool.hatch.build.targets.sdist].exclude` "
        "for missing patterns."
    )


def test_sdist_contains_contributing_md(tmp_path: Path) -> None:
    """Sdist must include CONTRIBUTING.md from its actual repository path.

    Hatchling's ``only-include`` silently omits paths that don't resolve at
    build time. The repository has CONTRIBUTING.md at ``.github/CONTRIBUTING.md``,
    not at the top level — pointing ``only-include`` at the wrong path made
    the file disappear from the sdist without any warning. This test fails
    fast if the file is missing again.
    """
    sdist_path = _build_sdist(tmp_path)
    with tarfile.open(sdist_path, "r:gz") as tar:
        members = {m.name for m in tar.getmembers() if m.isfile()}
    contributing_members = [m for m in members if m.endswith("CONTRIBUTING.md")]
    assert contributing_members, (
        "CONTRIBUTING.md is missing from the sdist. Check pyproject.toml "
        "[tool.hatch.build.targets.sdist] only-include — the entry must "
        "match the repository's actual path (.github/CONTRIBUTING.md)."
    )
