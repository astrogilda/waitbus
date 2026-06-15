"""Executable-doc tests for the shell emit recipes.

Extracts each marker-tagged fenced bash block from
``docs/emitters/recipes.md``, shellchecks all of them, and executes the
two non-docker recipes against a throwaway provisioned store. The doc
cannot drift from the ``waitbus emit`` CLI surface without failing
here at the same commit.

The docker forwarder recipe is shellcheck-only (no docker daemon in the
test environment); the doorbell ring inside ``waitbus emit`` fires into
the void by design (best-effort, the daemon is not running here).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from waitbus import _db

_RECIPES_MD = Path(__file__).resolve().parents[1] / "docs" / "emitters" / "recipes.md"
_MARKERS = ("command-finished", "long-job-wrapper", "docker-events-forwarder")
_BLOCK_RE = r"<!-- recipe:{name} -->\n```bash\n(.*?)```"


def _recipe(name: str) -> str:
    """Return the fenced bash block tagged ``<!-- recipe:<name> -->``."""
    text = _RECIPES_MD.read_text(encoding="utf-8")
    match = re.search(_BLOCK_RE.format(name=name), text, re.DOTALL)
    assert match is not None, f"recipe marker {name!r} not found in {_RECIPES_MD}"
    return match.group(1)


def _shellcheck(script: str, tmp_path: Path, name: str) -> None:
    """Run shellcheck over ``script`` (with a bash shebang prelude)."""
    shellcheck = shutil.which("shellcheck")
    if shellcheck is None:
        pytest.skip("shellcheck not available")
    body = script if script.startswith("#!") else "#!/usr/bin/env bash\n" + script
    script_path = tmp_path / f"{name}.sh"
    script_path.write_text(body, encoding="utf-8")
    proc = subprocess.run(
        [shellcheck, str(script_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"shellcheck failed for {name}: {proc.stdout}\n{proc.stderr}"


def _provisioned_env(tmp_path: Path) -> tuple[dict[str, str], Path]:
    """Provision a throwaway store and return (env, db_path) for recipe runs.

    The ``waitbus emit`` CLI opens the store but does not create schema
    (verified empirically: a missing state dir is an error), so the test
    provisions via :func:`waitbus._db.ensure_schema` first — the same
    contract the daemons fulfil on a real install. The console script is
    resolved from the test venv's bin dir, prepended to PATH.
    """
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    db = state / "github.db"
    _db.ensure_schema(db)
    env = os.environ.copy()
    env["WAITBUS_STATE_DIR"] = str(state)
    env["WAITBUS_RUNTIME_DIR"] = str(tmp_path / "run")
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env['PATH']}"
    return env, db


def _rows(db: Path) -> list[tuple[str, str, str, str]]:
    """Return (source, event_type, repo, payload_json) for every stored row."""
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute("SELECT source, event_type, repo, payload_json FROM events").fetchall()
    finally:
        conn.close()


@pytest.mark.parametrize("name", _MARKERS)
def test_recipe_is_shellcheck_clean(name: str, tmp_path: Path) -> None:
    """Every marker-tagged recipe block passes shellcheck."""
    _shellcheck(_recipe(name), tmp_path, name)


def test_command_finished_recipe_emits_matching_row(tmp_path: Path) -> None:
    """The command-finished block runs verbatim and stores a consistent row.

    ``make build`` fails in the empty tmp cwd (no makefile) — the recipe
    must still exit 0 (the emit is the last command) and the stored
    event_type must agree with the recorded exit_code.
    """
    if shutil.which("make") is None:
        pytest.skip("make not available")
    env, db = _provisioned_env(tmp_path)
    proc = subprocess.run(
        ["bash", "-c", _recipe("command-finished")],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    rows = _rows(db)
    assert len(rows) == 1
    source, event_type, repo, payload = rows[0]
    assert (source, repo) == ("agent", "shell")
    assert '"command": "make build"' in payload
    exit_code = int(payload.rsplit(":", 1)[1].strip(" }"))
    expected_type = "agent_message" if exit_code == 0 else "agent_task_failed"
    assert event_type == expected_type


@pytest.mark.parametrize(
    ("wrapped", "expected_rc", "expected_type"),
    [("true", 0, "agent_message"), ("false", 1, "agent_task_failed")],
)
def test_long_job_wrapper_propagates_rc_and_emits(
    wrapped: str, expected_rc: int, expected_type: str, tmp_path: Path
) -> None:
    """The wrapper exits with the wrapped command's rc and stores the matching event."""
    env, db = _provisioned_env(tmp_path)
    script = tmp_path / "waitbus-run"
    script.write_text(_recipe("long-job-wrapper"), encoding="utf-8")
    script.chmod(0o755)
    proc = subprocess.run(
        ["bash", str(script), "demo-job", wrapped],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == expected_rc, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    rows = _rows(db)
    assert len(rows) == 1
    source, event_type, repo, payload = rows[0]
    assert (source, repo) == ("agent", "shell")
    assert event_type == expected_type
    assert '"job": "demo-job"' in payload
    assert f'"exit_code": {expected_rc}' in payload


def test_long_job_wrapper_payload_roundtrips_quotes_and_backslashes(tmp_path: Path) -> None:
    """Args carrying double quotes and backslashes still emit valid JSON.

    The payload is built by python3's ``json.dumps``, not raw shell
    interpolation -- a JSON-hostile command line must round-trip exactly
    instead of producing invalid JSON (and, with the emit's output
    discarded, a silently lost event).
    """
    env, db = _provisioned_env(tmp_path)
    script = tmp_path / "waitbus-run"
    script.write_text(_recipe("long-job-wrapper"), encoding="utf-8")
    script.chmod(0o755)
    job_name = 'job "quoted" back\\slash'
    args = ["printf", "%s", 'he said "hi"', "C:\\temp\\new", 'tricky "x\\y" arg']
    proc = subprocess.run(
        ["bash", str(script), job_name, *args],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    rows = _rows(db)
    assert len(rows) == 1
    source, event_type, repo, payload_json = rows[0]
    assert (source, repo) == ("agent", "shell")
    assert event_type == "agent_message"
    payload = json.loads(payload_json)  # must be VALID json despite hostile args
    assert payload["job"] == job_name
    assert payload["command"] == " ".join(args)
    assert payload["exit_code"] == 0
