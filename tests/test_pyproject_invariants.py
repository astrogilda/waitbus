"""Structural-invariant tests for the project configuration surface.

Four orthogonal invariants pinned here, each catching a specific class
of drain-cycle hygiene bug that the 2026-05-23 self-audit surfaced:

1. The ``[tool.pytest.ini_options]`` section in ``pyproject.toml`` must
   exist and carry the load-bearing keys that the rest of the test
   infrastructure depends on.  A drain commit that accidentally drops
   the section header silently neutralises the entire test runner —
   pytest collects zero tests, every gate passes vacuously, and a
   broken regression rides a release.

2. The ``CiStatusConfig.settings_customise_sources`` callback parameter
   names must match the pydantic-settings keyword-call contract.  The
   framework calls the method with keyword arguments matching the
   canonical parameter names; renaming any of the four source-factory
   parameters to silence a dead-arg lint would raise ``TypeError`` on
   every config construction.  This invariant catches the rename
   pressure at test-time before it can ride a commit.

3. The ``.github/workflows/ci.yml`` workflow must trigger on both
   ``pull_request`` and ``push`` to main.  A pull-request-only trigger
   leaves the gate open whenever the maintainer pushes directly; the
   ``push`` trigger added alongside this test guards against an
   accidental removal in a future workflow edit.

4. The two hand-mirrored sdist-exclusion lists in ``pyproject.toml``
   (``[tool.hatch.build.targets.sdist].exclude`` and
   ``[tool.check-manifest].ignore``) must each equal the set of
   ``tests/*.py`` files that import a maintainer-only package
   (``scripts`` / ``benchmarks``) at module scope, computed by an AST
   walk.  Any test importing such a package must be sdist-excluded or an
   unpacked-sdist ``pytest --collect-only`` raises ``ModuleNotFoundError``
   at collection time.  Maintaining the pairing by hand let the lists
   drift; this invariant makes the AST walk the single source of truth.

These four invariants together close the gap the 2026-05-23 self-
audit identified as the drain-cycle write-side hygiene failure mode:
three sibling commits (a kwarg rename, an extract-halfway, an
eulogy-in-same-commit) shared the templated-pass-with-structural-
error class.  The CI hook + this surgical structural lint catch
sibling defects at write-time rather than at post-drain audit-time.
"""

from __future__ import annotations

import inspect
import tomllib
from pathlib import Path
from typing import Any

import pytest

_REPO = Path(__file__).resolve().parents[1]
_PYPROJECT = _REPO / "pyproject.toml"
_CI_WORKFLOW = _REPO / ".github" / "workflows" / "ci.yml"


@pytest.fixture(scope="module")
def pyproject_data() -> dict[str, Any]:
    """Parse pyproject.toml once per module."""
    return tomllib.loads(_PYPROJECT.read_text())


def test_pytest_ini_options_section_has_load_bearing_keys(pyproject_data: dict[str, Any]) -> None:
    """``[tool.pytest.ini_options]`` exists and carries the keys the rest of the suite depends on.

    The five required keys are the ones that, when missing, silently
    break test discovery or fixture wiring.  ``testpaths`` and
    ``pythonpath`` configure collection; ``addopts`` carries the
    project's ``--strict-markers`` and other gate flags; ``asyncio_mode``
    is required by pytest-asyncio for any of the project's async tests
    to run; ``filterwarnings`` carries the warning-to-error escalations
    that the gate relies on to surface deprecations.
    """
    pytest_section = pyproject_data.get("tool", {}).get("pytest", {}).get("ini_options", {})
    assert pytest_section, (
        "[tool.pytest.ini_options] section is missing from pyproject.toml. "
        "Without it, pytest collects zero tests under strict-mode pytest-asyncio "
        "and the entire test runner becomes vacuous."
    )
    required_keys = {"testpaths", "pythonpath", "addopts", "asyncio_mode", "filterwarnings"}
    missing = required_keys - set(pytest_section.keys())
    assert not missing, (
        f"[tool.pytest.ini_options] is missing load-bearing keys: {sorted(missing)}. "
        "Restore each missing key to its prior value rather than relying on the "
        "pytest defaults; the project's gate behaviour depends on these explicitly."
    )


def test_settings_customise_sources_signature_matches_pydantic_settings_contract() -> None:
    """The four source-factory parameter names match the pydantic-settings keyword contract.

    pydantic-settings calls
    ``cls.settings_customise_sources(cls, init_settings=..., env_settings=...,
    dotenv_settings=..., file_secret_settings=...)`` by keyword.  Renaming
    any of the four parameters (e.g. to underscore-prefixed forms to silence
    a vulture dead-arg lint) would raise ``TypeError`` on the very first
    config construction.  Lock the signature.

    See: https://docs.pydantic.dev/latest/concepts/pydantic_settings/
    """
    from waitbus._config import CiStatusConfig

    sig = inspect.signature(CiStatusConfig.settings_customise_sources)
    parameter_names = list(sig.parameters.keys())
    expected = ["settings_cls", "init_settings", "env_settings", "dotenv_settings", "file_secret_settings"]
    assert parameter_names == expected, (
        f"CiStatusConfig.settings_customise_sources parameter names drifted from the "
        f"pydantic-settings keyword-call contract.\n  expected: {expected}\n  got:      {parameter_names}\n"
        "Restore the canonical names; pydantic-settings binds these by keyword and a "
        "rename raises TypeError on every config construction."
    )


def test_ci_workflow_triggers_on_both_pull_request_and_push() -> None:
    """The ``.github/workflows/ci.yml`` workflow fires on both pull-request and push to main.

    Pull-request-only triggers leave the gate open whenever the maintainer
    merges locally and pushes directly to main, which is the single-developer
    push model this project operates under.  A drain commit that accidentally
    drops the ``push`` trigger from the workflow would silently re-open the
    gate.  Lock both triggers as present.
    """
    workflow_text = _CI_WORKFLOW.read_text()
    # Use line-prefix structural checks rather than a full YAML parse to
    # avoid pulling PyYAML into the test dep closure; the YAML structure
    # is small and the prefix shape is stable.
    lines = workflow_text.splitlines()
    in_on_block = False
    saw_pull_request = False
    saw_push = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("on:"):
            in_on_block = True
            continue
        if in_on_block:
            if line and not line.startswith((" ", "\t")) and not stripped.startswith("#"):
                # Left the ``on:`` block (new top-level key).
                break
            if stripped.startswith("pull_request:"):
                saw_pull_request = True
            elif stripped.startswith("push:"):
                saw_push = True
    assert saw_pull_request, (
        ".github/workflows/ci.yml is missing the pull_request trigger. "
        "The PR-side gate is the primary review path; do not drop it."
    )
    assert saw_push, (
        ".github/workflows/ci.yml is missing the push trigger.  Direct pushes "
        "to main bypass the test matrix without it.  Restore "
        "`push:\\n    branches: [main]` to the on: block."
    )


def test_sdist_exclude_matches_maintainer_importer_ast_set(pyproject_data: dict[str, Any]) -> None:
    """Both sdist-exclusion lists equal the AST-computed maintainer-importer set.

    A ``tests/*.py`` file that imports ``scripts`` or ``benchmarks`` at module
    scope must be excluded from the sdist, or an unpacked-sdist
    ``pytest --collect-only`` raises ``ModuleNotFoundError`` at collection
    time.  The AST walk in ``scripts.sdist_test_pairing`` is the single source
    of truth; this asserts the two hand-mirrored pyproject lists
    (``[tool.hatch.build.targets.sdist].exclude`` and
    ``[tool.check-manifest].ignore``) both equal it and equal each other, so
    the lists can never silently drift again.

    Importing the helper from ``scripts`` makes this test itself a
    maintainer-importer; it is therefore in the excluded set too, which is
    correct — the pairing invariant is a maintainer-side gate, not a consumer
    surface.
    """
    from scripts.sdist_test_pairing import (
        check_manifest_ignore_test_set,
        compute_maintainer_importer_tests,
        repo_root,
        sdist_exclude_test_set,
    )

    computed = compute_maintainer_importer_tests(repo_root(), pyproject_data)
    sdist_set = sdist_exclude_test_set(pyproject_data)
    manifest_set = check_manifest_ignore_test_set(pyproject_data)

    assert computed == sdist_set, (
        "[tool.hatch.build.targets.sdist].exclude drifted from the AST-computed "
        "maintainer-importer set.\n"
        f"  missing (import a maintainer module but not excluded): {sorted(computed - sdist_set)}\n"
        f"  stale (excluded but no longer import a maintainer module): {sorted(sdist_set - computed)}\n"
        "Reconcile the list to the AST set (run `python scripts/sdist_test_pairing.py`)."
    )
    assert computed == manifest_set, (
        "[tool.check-manifest].ignore drifted from the AST-computed "
        "maintainer-importer set.\n"
        f"  missing: {sorted(computed - manifest_set)}\n"
        f"  stale: {sorted(manifest_set - computed)}\n"
        "Reconcile the list to the AST set (run `python scripts/sdist_test_pairing.py`)."
    )
