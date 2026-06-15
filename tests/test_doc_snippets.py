"""Forward-looking guard: docs/snippets is the single source of truth.

Two assertions:

1. **Snippet-vs-articles drift.** When ``docs/launch-articles/`` exists,
   the Python code block headlined in ``01-problem-narrative.md`` must
   byte-match ``docs/snippets/minimal_subscriber.py``. The narrative
   walks the reader through that code; if the article diverges from
   the file the test fails.

2. **No "waitbus saves $X" claim.** When ``docs/launch-articles/`` exists,
   no article contains case-insensitive variants of ``"saved $"``,
   ``"saves $"``, ``"waitbus saved"``, or ``"waitbus saves"``. Tokens-saved
   is a per-deployment, per-source estimate computed by the user via
   ``waitbus stats``; the article surface ships a worked example only,
   never a headline savings number. The guard fires the moment a
   launch article lands so the constraint survives careless edits.

Both assertions skip when ``docs/launch-articles/`` is absent (it lands
as part of the launch-article work). The test files
themselves ship now so the guards apply automatically once articles
exist; no follow-up plumbing is needed when the article work lands.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_ARTICLES_DIR = _ROOT / "docs" / "launch-articles"
_PYTHON_SNIPPET = _ROOT / "docs" / "snippets" / "minimal_subscriber.py"
_PROBLEM_NARRATIVE = _ARTICLES_DIR / "01-problem-narrative.md"

# Case-insensitive patterns the guard refuses. Word-boundaries are NOT
# used because the savings-claim shapes embed punctuation (``$``,
# spaces) that breaks ``\b`` semantics; a substring match is what we
# want.
_NO_SAVINGS_CLAIM_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"saved \$", re.IGNORECASE),
    re.compile(r"saves \$", re.IGNORECASE),
    re.compile(r"waitbus saved", re.IGNORECASE),
    re.compile(r"waitbus saves", re.IGNORECASE),
)


def _extract_python_code_blocks(markdown: str) -> list[str]:
    """Return every fenced ```python ... ``` block in ``markdown``.

    The narrative may carry several Python blocks; the snippet-drift
    assertion requires AT LEAST ONE to byte-match the canonical
    snippet file. Matching exactly-one would be too strict because
    earlier sections may print short examples first.
    """
    pattern = re.compile(r"```python\n(.*?)```", re.DOTALL)
    return [m.group(1).rstrip("\n") for m in pattern.finditer(markdown)]


def test_python_snippet_matches_article_code_block() -> None:
    """01-problem-narrative.md embeds the canonical Python snippet verbatim."""
    if not _PROBLEM_NARRATIVE.exists():
        pytest.skip(f"{_PROBLEM_NARRATIVE} does not exist yet (article surface not landed)")
    markdown = _PROBLEM_NARRATIVE.read_text(encoding="utf-8")
    snippet_body = _PYTHON_SNIPPET.read_text(encoding="utf-8").rstrip("\n")
    blocks = _extract_python_code_blocks(markdown)
    assert blocks, "01-problem-narrative.md has no fenced ```python``` block"
    matches = [b for b in blocks if b == snippet_body]
    assert matches, (
        "no fenced ```python``` block in 01-problem-narrative.md matches "
        f"docs/snippets/minimal_subscriber.py byte-for-byte ({len(blocks)} blocks examined)"
    )


def test_no_waitbus_saves_x_claim_in_articles() -> None:
    """No launch article makes a 'waitbus saves $X' headline claim."""
    if not _ARTICLES_DIR.exists():
        pytest.skip(f"{_ARTICLES_DIR} does not exist yet (article surface not landed)")
    violations: list[str] = []
    for article in sorted(_ARTICLES_DIR.glob("*.md")):
        text = article.read_text(encoding="utf-8")
        for pattern in _NO_SAVINGS_CLAIM_PATTERNS:
            for match in pattern.finditer(text):
                # Show enough surrounding context for the failure
                # message to point a reviewer at the offending sentence.
                start = max(0, match.start() - 40)
                end = min(len(text), match.end() + 40)
                violations.append(f"{article.name}: ...{text[start:end]!r}...")
    assert not violations, (
        "launch articles contain forbidden 'waitbus saves $X' claim "
        "(tokens-saved is a per-deployment estimate; articles use a worked "
        "example only). Offending matches:\n  " + "\n  ".join(violations)
    )


def test_no_wrong_entry_point_group_name_in_articles() -> None:
    """No launch article uses the wrong entry-point group name `waitbus_status_bus.sources`.

    The canonical group name is `waitbus.sources.v1` (dot-separated, with
    the `.v1` major-version suffix). An earlier draft of article 05
    advertised `waitbus_status_bus.sources` (underscores, no version) as
    the public extension API; that name would not be enumerated by
    waitbus's actual ``discover_plugins_once`` walker and would silently
    leave plugin-author packages invisible. This guard fires the moment
    any launch article re-introduces the wrong name.

    Also flags the wrong CLI verb form `waitbus sources list` (plural);
    the canonical verb is `waitbus source list` (singular) per the
    existing typer wiring.
    """
    if not _ARTICLES_DIR.exists():
        pytest.skip(f"{_ARTICLES_DIR} does not exist yet (article surface not landed)")
    wrong_group = "waitbus_status_bus.sources"
    wrong_verb = "waitbus sources list"
    violations: list[str] = []
    for article in sorted(_ARTICLES_DIR.glob("*.md")):
        text = article.read_text(encoding="utf-8")
        if wrong_group in text:
            violations.append(f"{article.name}: contains wrong entry-point group {wrong_group!r}")
        if wrong_verb in text:
            violations.append(f"{article.name}: contains wrong verb {wrong_verb!r} (correct: `waitbus source list`)")
    assert not violations, (
        "launch articles contain wrong public-API names. The canonical "
        "entry-point group is `waitbus.sources.v1` and the canonical CLI "
        "verb is `waitbus source list` (singular). Fix:\n  " + "\n  ".join(violations)
    )
