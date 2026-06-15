"""Tests for ``_classify_branch`` in ``scripts/derive_gh_distributions``.

The function maps raw branch names to one of the six regex categories
defined in ``_BRANCH_PATTERN_REGEX``, falling back to ``"other"`` when no
pattern matches.  It is marked public-for-testability in its docstring.
"""

from __future__ import annotations

import pytest

from scripts.derive_gh_distributions import _classify_branch


@pytest.mark.parametrize(
    ("branch", "expected"),
    [
        # Happy paths — one per category in the pattern table.
        ("main", "main"),
        ("master", "main"),
        ("feature/my-thing", "feature/*"),
        ("hotfix/urgent-patch", "hotfix/*"),
        ("dependabot/npm_and_yarn/lodash-4.17.21", "dependabot/*"),
        ("release/v1.2.3", "release/*"),
        ("renovate/eslint-8.x", "renovate/*"),
        # Edge cases — no match → "other".
        ("chore/update-readme", "other"),
        ("bugfix/off-by-one", "other"),
        ("", "other"),
        # Prefix that contains a keyword but does not start with it.
        ("refs/heads/main", "other"),
    ],
)
def test_classify_branch(branch: str, expected: str) -> None:
    """Each branch name maps to the expected category."""
    assert _classify_branch(branch) == expected
