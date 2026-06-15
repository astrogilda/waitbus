"""Tests for the example emitters under ``examples/emitters/``.

Part 1 — the Claude Code lifecycle-hook script: runs it as a subprocess
with a fake hook JSON on stdin against a provisioned throwaway store
and asserts the stored row shape; then asserts the never-block contract
(exit 0 even with a broken store or empty stdin).

Part 2 — the GitHub Action skeleton: structural asserts on
``action.yml`` (composite shape, required inputs, the three header
names the listener's webhook route validates) plus a shellcheck pass
over the embedded run script. Skips with reason when pyyaml or
shellcheck is absent, mirroring ``tests/test_multilingual_snippets.py``.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from waitbus import _db

_ROOT = Path(__file__).resolve().parents[1]
_HOOK_SCRIPT = _ROOT / "examples" / "emitters" / "claude_code" / "emit_lifecycle.py"
_ACTION_YML = _ROOT / "examples" / "emitters" / "github_action" / "action.yml"

_LISTENER_HEADERS = ("X-GitHub-Event", "X-GitHub-Delivery", "X-Hub-Signature-256")


def _run_hook(stdin_text: str, *, state_dir: Path, runtime_dir: Path) -> subprocess.CompletedProcess[str]:
    """Run the hook script as a subprocess against the given store dirs."""
    env = os.environ.copy()
    env["WAITBUS_STATE_DIR"] = str(state_dir)
    env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
    return subprocess.run(
        [sys.executable, str(_HOOK_SCRIPT)],
        input=stdin_text,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_hook_emits_lifecycle_row(tmp_path: Path) -> None:
    """A fake Stop-hook payload lands as a documented agent_message row."""
    state = tmp_path / "state"
    state.mkdir()
    db = state / "github.db"
    _db.ensure_schema(db)
    hook_json = json.dumps({"session_id": "abc123", "hook_event_name": "Stop", "cwd": "/tmp/x"})
    proc = _run_hook(hook_json, state_dir=state, runtime_dir=tmp_path / "run")
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert proc.stdout.strip().startswith("claude-code:abc123:Stop:")

    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute(
            "SELECT source, event_type, owner, repo, msg_from, msg_body, payload_json FROM events"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    source, event_type, owner, repo, msg_from, msg_body, payload_json = rows[0]
    assert (source, event_type, owner, repo) == ("agent", "agent_message", "local", "claude-code")
    assert msg_from == "claude-code:abc123"
    assert msg_body == "Stop"
    assert json.loads(payload_json) == json.loads(hook_json)


def test_hook_never_blocks_on_broken_store(tmp_path: Path) -> None:
    """With an unusable store the hook still exits 0 with a stderr line."""
    garbage = tmp_path / "not-a-dir"
    garbage.write_text("a file where the state dir should be", encoding="utf-8")
    proc = _run_hook(
        json.dumps({"session_id": "s", "hook_event_name": "Stop"}),
        state_dir=garbage,
        runtime_dir=tmp_path / "run",
    )
    assert proc.returncode == 0
    assert "ignored" in proc.stderr


@pytest.mark.parametrize(
    "stdin_text",
    ["", "{not json", '["a", "list", "not", "a", "dict"]'],
    ids=["empty", "malformed", "non-dict"],
)
def test_hook_tolerates_empty_and_malformed_stdin(stdin_text: str, tmp_path: Path) -> None:
    """Empty, malformed, and non-dict stdin all emit an unknown event and exit 0."""
    state = tmp_path / "state"
    state.mkdir()
    db = state / "github.db"
    _db.ensure_schema(db)
    proc = _run_hook(stdin_text, state_dir=state, runtime_dir=tmp_path / "run")
    assert proc.returncode == 0, proc.stderr
    conn = sqlite3.connect(str(db))
    try:
        (msg_body, payload_json) = conn.execute("SELECT msg_body, payload_json FROM events").fetchone()
    finally:
        conn.close()
    assert msg_body == "unknown"
    # The stored payload_json must ALWAYS be valid JSON: undecodable stdin
    # is replaced by the validated fallback dict, never stored raw.
    json.loads(payload_json)


def test_hook_malformed_stdin_stores_the_fallback_dict_not_raw_text(tmp_path: Path) -> None:
    """Undecodable stdin lands as the validated ``{}``, not the raw text."""
    state = tmp_path / "state"
    state.mkdir()
    db = state / "github.db"
    _db.ensure_schema(db)
    proc = _run_hook("{not json", state_dir=state, runtime_dir=tmp_path / "run")
    assert proc.returncode == 0, proc.stderr
    conn = sqlite3.connect(str(db))
    try:
        (payload_json,) = conn.execute("SELECT payload_json FROM events").fetchone()
    finally:
        conn.close()
    assert json.loads(payload_json) == {}


def _load_action() -> dict[str, Any]:
    """Parse action.yml, skipping when pyyaml is absent."""
    yaml = pytest.importorskip("yaml")
    loaded: dict[str, Any] = yaml.safe_load(_ACTION_YML.read_text(encoding="utf-8"))
    return loaded


def test_action_yml_is_a_composite_action_with_required_inputs() -> None:
    """The skeleton declares a composite action and the documented inputs."""
    action = _load_action()
    assert action["runs"]["using"] == "composite"
    inputs = action["inputs"]
    for required in ("listener-url", "webhook-secret", "conclusion"):
        assert required in inputs, f"missing input {required!r}"
        assert inputs[required].get("required") is True, f"input {required!r} must be required"


def test_action_run_script_speaks_the_listener_contract(tmp_path: Path) -> None:
    """The run step posts with exactly the headers the webhook route validates."""
    action = _load_action()
    steps = action["runs"]["steps"]
    scripts = [step["run"] for step in steps if "run" in step]
    assert scripts, "composite action has no run step"
    combined = "\n".join(scripts)
    for header in _LISTENER_HEADERS:
        assert header in combined, f"run script is missing the {header} header"

    shellcheck = shutil.which("shellcheck")
    if shellcheck is None:
        pytest.skip("shellcheck not available")
    for index, script in enumerate(scripts):
        script_path = tmp_path / f"action-step-{index}.sh"
        script_path.write_text("#!/usr/bin/env bash\n" + script, encoding="utf-8")
        proc = subprocess.run(
            [shellcheck, str(script_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, f"shellcheck failed for run step {index}: {proc.stdout}\n{proc.stderr}"
