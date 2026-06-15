"""Tests for `waitbus verify-plugin` sub-command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from waitbus.cli import app

runner = CliRunner()


@pytest.fixture()
def plugin_root(tmp_path: Path) -> Path:
    """Create a temporary plugin root directory."""
    plugin_dir = tmp_path / ".claude-plugin"
    plugin_dir.mkdir(parents=True)
    return tmp_path


def _write_plugin_json(plugin_root: Path, data: object) -> None:
    """Write plugin.json to .claude-plugin/ under plugin_root."""
    path = plugin_root / ".claude-plugin" / "plugin.json"
    path.write_text(json.dumps(data), encoding="utf-8")


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------


def test_verify_plugin_happy_path(plugin_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Valid plugin.json with all required fields exits 0."""
    _write_plugin_json(
        plugin_root,
        {
            "name": "waitbus",
            "version": "0.1.0",
            "schemaVersion": 1,
        },
    )
    monkeypatch.setenv("CLAUDE_PLUGIN_ROOT", str(plugin_root))
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 0, result.stdout
    assert "name: waitbus" in result.stdout
    assert "version: 0.1.0" in result.stdout
    assert "schemaVersion: 1" in result.stdout
    assert "plugin.json valid" in result.stdout


def test_verify_plugin_extra_fields_allowed(plugin_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Extra fields beyond the required set are allowed."""
    _write_plugin_json(
        plugin_root,
        {
            "name": "waitbus",
            "version": "0.1.0",
            "schemaVersion": 1,
            "description": "optional extra field",
        },
    )
    monkeypatch.setenv("CLAUDE_PLUGIN_ROOT", str(plugin_root))
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 0, result.stdout


def test_verify_plugin_uses_cwd_default(plugin_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Without CLAUDE_PLUGIN_ROOT, defaults to cwd."""
    _write_plugin_json(
        plugin_root,
        {
            "name": "waitbus",
            "version": "0.1.0",
            "schemaVersion": 1,
        },
    )
    monkeypatch.delenv("CLAUDE_PLUGIN_ROOT", raising=False)
    monkeypatch.chdir(plugin_root)
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 0, result.stdout


# ---------------------------------------------------------------------------
# failure paths
# ---------------------------------------------------------------------------


def test_verify_plugin_missing_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing plugin.json exits 1."""
    monkeypatch.setenv("CLAUDE_PLUGIN_ROOT", str(tmp_path))
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_verify_plugin_invalid_json(plugin_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Malformed JSON exits 1."""
    (plugin_root / ".claude-plugin" / "plugin.json").write_text("{not valid json}", encoding="utf-8")
    monkeypatch.setenv("CLAUDE_PLUGIN_ROOT", str(plugin_root))
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 1
    assert "not valid JSON" in result.output


def test_verify_plugin_missing_name_field(plugin_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """JSON missing 'name' field exits 1."""
    _write_plugin_json(plugin_root, {"version": "0.1.0", "schemaVersion": 1})
    monkeypatch.setenv("CLAUDE_PLUGIN_ROOT", str(plugin_root))
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 1
    assert "name" in result.output


def test_verify_plugin_missing_version_field(plugin_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """JSON missing 'version' field exits 1."""
    _write_plugin_json(plugin_root, {"name": "waitbus", "schemaVersion": 1})
    monkeypatch.setenv("CLAUDE_PLUGIN_ROOT", str(plugin_root))
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 1
    assert "version" in result.output


def test_verify_plugin_missing_schema_version_field(plugin_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """JSON missing 'schemaVersion' field exits 1."""
    _write_plugin_json(plugin_root, {"name": "waitbus", "version": "0.1.0"})
    monkeypatch.setenv("CLAUDE_PLUGIN_ROOT", str(plugin_root))
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 1
    assert "schemaVersion" in result.output


def test_verify_plugin_missing_multiple_fields(plugin_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """All three required fields missing: error lists all of them."""
    _write_plugin_json(plugin_root, {"other": "stuff"})
    monkeypatch.setenv("CLAUDE_PLUGIN_ROOT", str(plugin_root))
    result = runner.invoke(app, ["verify-plugin"])
    assert result.exit_code == 1
    assert "name" in result.output
    assert "version" in result.output
    assert "schemaVersion" in result.output
