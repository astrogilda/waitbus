"""Enforce that the complexity-exceptions table is the exact set of C+
functions in ``waitbus``.

This is the test ``docs/docs/COMPLEXITY.md`` references as its
enforcement mechanism: the table is the single source of truth for
accepted C+ complexity, and this test asserts the table equals
``radon cc waitbus -s -n C`` output. Either side drifting --
an untabled C+ function, a phantom row whose function has dropped
below C+ (e.g. via a refactor), or a stale grade -- fails the test
and the responsible commit has to update both.

The previous "verified at every release gate by radon" line was prose
that no automation backed (the project's CI has no radon step). This
test is the real enforcement.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DD_PATH = REPO_ROOT / "docs" / "COMPLEXITY.md"
PACKAGE = "waitbus"

# radon is a dev-only dependency; some CI runners (e.g. the macOS cell) do not
# install it. Skip the whole module when it is absent rather than erroring at
# setup with FileNotFoundError. The canonical Linux cell runs the gate.
pytestmark = pytest.mark.skipif(
    shutil.which("radon") is None,
    reason="radon is not on PATH on this runner (dev dependencies not installed)",
)


def _radon_env() -> dict[str, str]:
    """A copy of ``os.environ`` safe to hand to ``subprocess``.

    GitHub-hosted runners can carry an environment variable whose name is empty
    or contains ``=``; CPython's ``subprocess`` re-encodes the env and raises
    ``ValueError: illegal environment variable name`` on such a key. radon needs
    no special environment, so drop any illegal keys before exec.
    """
    return {k: v for k, v in os.environ.items() if k and "=" not in k}


def _run_radon(*args: str) -> subprocess.CompletedProcess[str]:
    """Run radon with the sanitized environment so every call site inherits
    the illegal-key filter; a new radon call cannot forget ``env=``."""
    return subprocess.run(
        ["radon", *args],
        capture_output=True,
        text=True,
        check=False,
        cwd=REPO_ROOT,
        env=_radon_env(),
    )


#: Matches one row of the live (non-retired) section of the table.
#: The function spec uses the existing convention
#: ``relative/path.py::function_name`` (qualifiers like ``Class.method``
#: are accepted via a slightly looser symbol regex).
_TABLE_ROW = re.compile(r"^\|\s*`(?P<path>[\w./]+)::(?P<func>[\w.]+)`\s*\|\s*C\((?P<grade>\d+)\)\s*\|")

#: Heading that opens the retired section. Anything below this line in
#: the table file is NOT enforced (it is historical -- the
#: functions are below C+ or no longer exist).
_RETIRED_HEADING = re.compile(r"^###\s+Retired entries\b", re.IGNORECASE)


def _parse_dd_table(text: str) -> dict[tuple[str, str], int]:
    """Return ``{(path, function): grade}`` for the LIVE table rows only.

    Stops at the "Retired entries" heading so the retired section is
    not enforced (those functions are below C+ or removed).
    """
    rows: dict[tuple[str, str], int] = {}
    for line in text.splitlines():
        if _RETIRED_HEADING.match(line):
            break
        match = _TABLE_ROW.match(line)
        if match is None:
            continue
        path = match.group("path")
        func = match.group("func")
        grade = int(match.group("grade"))
        rows[(path, func)] = grade
    return rows


def _parse_radon(output: str) -> dict[tuple[str, str], int]:
    """Parse ``radon cc ... -s -n C`` text output to ``{(path, function): grade}``.

    radon's text output is:

        waitbus/wait.py
            F 115:0 _wait - C (14)

    Each ``F``/``M`` line carries the function/method name and grade;
    the most recent path heading scopes the following ``F``/``M``
    entries until the next path heading.
    """
    rows: dict[tuple[str, str], int] = {}
    current_path: str | None = None
    line_re = re.compile(r"^\s+[FMC]\s+\d+:\d+\s+(?P<func>[\w.]+)\s+-\s+C\s+\((?P<grade>\d+)\)")
    # radon paths are package-rooted (`waitbus/wait.py`); the
    # the complexity table table strips the package prefix
    # (`wait.py::_wait`). Normalise radon to match the table convention
    # so the two sides are directly comparable.
    package_prefix = f"{PACKAGE}/"
    for raw in output.splitlines():
        if not raw.startswith(" "):
            stripped = raw.strip()
            if stripped:
                if stripped.startswith(package_prefix):
                    stripped = stripped[len(package_prefix) :]
                current_path = stripped
            continue
        match = line_re.match(raw)
        if match is None or current_path is None:
            continue
        rows[(current_path, match.group("func"))] = int(match.group("grade"))
    return rows


@pytest.fixture(scope="module")
def radon_table() -> dict[tuple[str, str], int]:
    """Run ``radon cc -s -n C`` once and parse it for every assertion."""
    proc = _run_radon("cc", PACKAGE, "-s", "-n", "C")
    assert proc.returncode == 0, f"radon exited {proc.returncode}; stderr=\n{proc.stderr}"
    return _parse_radon(proc.stdout)


@pytest.fixture(scope="module")
def dd_table() -> dict[tuple[str, str], int]:
    """Parse the live (non-retired) complexity table from docs/COMPLEXITY.md."""
    return _parse_dd_table(DD_PATH.read_text(encoding="utf-8"))


def test_dd_table_equals_radon_output(
    radon_table: dict[tuple[str, str], int],
    dd_table: dict[tuple[str, str], int],
) -> None:
    """The complexity-exceptions table must be EXACTLY the C+ set radon
    reports (no extras, no missing, every grade matches).

    Failure modes this catches:

    * a new C+ function was added without a table row -- DD says less
      than what is true and the doc cannot vouch for the codebase;
    * a previously-C+ function dropped to <C (refactor / convergence)
      and the row is now a phantom -- DD says more than what is true;
    * a function changed grade (e.g. C16 -> C17) and the row is stale.

    In every case the responsible commit updates both sides.
    """
    radon_keys = set(radon_table)
    dd_keys = set(dd_table)
    missing_from_dd = radon_keys - dd_keys
    phantom_in_dd = dd_keys - radon_keys
    grade_mismatch = {
        key: (dd_table[key], radon_table[key]) for key in radon_keys & dd_keys if dd_table[key] != radon_table[key]
    }
    problems: list[str] = []
    if missing_from_dd:
        problems.append(
            "C+ functions present in radon but missing from the complexity table:\n  "
            + "\n  ".join(f"{p}::{f} = C({radon_table[(p, f)]})" for p, f in sorted(missing_from_dd))
        )
    if phantom_in_dd:
        problems.append(
            "Phantom the complexity table rows (no longer C+ in radon):\n  "
            + "\n  ".join(f"{p}::{f}" for p, f in sorted(phantom_in_dd))
        )
    if grade_mismatch:
        problems.append(
            "Stale grades in the complexity table vs radon:\n  "
            + "\n  ".join(
                f"{p}::{f}: DD says C({dd}), radon says C({rad})"
                for (p, f), (dd, rad) in sorted(grade_mismatch.items())
            )
        )
    assert not problems, "\n\n".join(problems)


# ---------------------------------------------------------------------------
# scripts/ ratchet at D+ (anticipated by docs/COMPLEXITY.md).
# ---------------------------------------------------------------------------


_SCRIPTS_DIR = "scripts"


@pytest.fixture(scope="module")
def scripts_d_or_worse() -> str:
    """Run ``radon cc scripts -s -n D`` once and return its raw output.

    `-n D` filters to only D-grade or worse (cyclomatic >= 21) so the
    test ignores the C-grade orchestration that the complexity table line
    456 documents as acceptable for benchmarks / scripts / tests.
    """
    proc = _run_radon("cc", _SCRIPTS_DIR, "-s", "-n", "D")
    assert proc.returncode == 0, f"radon exited {proc.returncode}; stderr=\n{proc.stderr}"
    return proc.stdout


def test_scripts_dir_has_no_d_or_worse_functions(scripts_d_or_worse: str) -> None:
    """Maintainer-side ``scripts/`` automation must stay below the D-grade
    threshold (cyclomatic >= 21).

    ``docs/COMPLEXITY.md`` line 456 documents the rationale: the
    ratchet's primary scope is ``waitbus/`` (the shipped library
    surface where long-term maintenance discipline matters), but the
    same DEC explicitly anticipates the trigger condition for this
    second-tier check::

        If a future audit needs the ratchet extended (e.g. a bench
        grows to operator-visible C(20)+ and needs review), extend
        `tests/test_complexity_table.py`'s radon invocation rather
        than carving exceptions in this table.

    The 2026-05-22 audit hit that trigger: ``scripts/soak.py::main``
    jumped from D(22) to E(37) in one session. This test catches
    that class of regression at gate-time. The threshold is D rather
    than C so the test does not flag the inherent orchestration
    complexity ``scripts/`` legitimately carries (multi-stage
    automation, parameter-sweep loops, validation rule-groups) and
    only fires when complexity crosses the operator-visible C(20)+
    line the DEC names verbatim.

    If a script genuinely needs D-grade orchestration in the future,
    refactor to drop below D OR document the exception in docs/COMPLEXITY.md alongside
    the change documenting why the refactor is wrong.
    """
    found = scripts_d_or_worse.strip()
    assert not found, (
        "scripts/ ratchet (D+ threshold) tripped. The following functions "
        "are D-grade or worse; refactor each below the D-threshold (per "
        "docs/COMPLEXITY.md) or document the exception in docs/COMPLEXITY.md:\n\n"
        f"{found}"
    )


_BENCHMARKS_DIR = "benchmarks"


@pytest.fixture(scope="module")
def benchmarks_d_or_worse() -> str:
    """Run ``radon cc benchmarks -s -n D`` once and return its raw output.

    Same second-tier D+ ratchet as ``scripts/``: ``-n D`` filters to
    D-grade or worse (cyclomatic >= 21) so the test ignores the C-grade
    multi-step orchestration that bench mains legitimately carry.
    """
    proc = _run_radon("cc", _BENCHMARKS_DIR, "-s", "-n", "D")
    assert proc.returncode == 0, f"radon exited {proc.returncode}; stderr=\n{proc.stderr}"
    return proc.stdout


def test_benchmarks_dir_has_no_d_or_worse_functions(benchmarks_d_or_worse: str) -> None:
    """``benchmarks/`` must stay below the D-grade threshold (cyclomatic >= 21).

    Third tier of the same ratchet that already covers ``waitbus/``
    (the C+ table) and ``scripts/`` (the D+ empty-set). The 2026-06-04
    audit hit exactly the trigger anticipated for
    benchmarks: the measurement suite sat outside both quality gates, and
    ``bench_multistream_proof.py::_build_verdict`` had accreted to F(118)
    -- six times the operator-visible C(20) line -- while
    ``bench_polling_vs_subscribe_llm_agent.py::_run_one_iteration``
    regrew F(30) -> F(54) since the prior audit. The DEC's own remedy is
    to "extend the test's radon invocation rather than carving exceptions
    in this table", so this is the empty-set form (no per-function
    exemption dict), matching the scripts/ tier.

    The threshold is D rather than C so the inherent multi-step
    orchestration every bench carries ("set up host, spawn daemon, drive
    workload, sample, write artefact") is not flagged; only pathological
    growth past the operator-visible C(20)+ line trips it. A bench that
    genuinely needs D-grade orchestration refactors below D OR lands an
    documented exception in docs/COMPLEXITY.md explaining why the refactor is wrong.
    """
    found = benchmarks_d_or_worse.strip()
    assert not found, (
        "benchmarks/ ratchet (D+ threshold) tripped. The following functions "
        "are D-grade or worse; refactor each below the D-threshold or land an "
        "a documented exception in docs/COMPLEXITY.md:\n\n"
        f"{found}"
    )
