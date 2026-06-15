"""Contract tests for ``waitbus config validate`` and ``config schema``.

The pure-function internals (``_validate_config_file``,
``_emit_toml_template``, ``_emit_json_schema``) are exercised directly;
one smoke test invokes the typer CLI to confirm the wire-up at
``waitbus.cli.app``. Exit-code semantics:

- 0 on validation success and on schema emission.
- 2 on any validation failure (file missing, malformed TOML, field error)
  or unknown ``--format`` value.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from waitbus import _config, cli, config_validate

runner = CliRunner()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_valid_toml(path: Path) -> None:
    path.write_text(
        "[prometheus]\n"
        'owner = "my-org"\n'
        'repo = "infra-alerts"\n'
        "\n"
        'log_level = "DEBUG"\n'
        "stall_threshold_min = 30\n"
        "heartbeat_sec = 45.0\n"
    )


# ---------------------------------------------------------------------------
# validate: pure-function path
# ---------------------------------------------------------------------------


def test_validate_pure_accepts_valid_config(tmp_path: Path) -> None:
    """A well-formed config.toml passes with zero errors."""
    cfg = tmp_path / "config.toml"
    _write_valid_toml(cfg)
    assert config_validate._validate_config_file(cfg) == []


def test_validate_pure_reports_bad_field(tmp_path: Path) -> None:
    """A bad log_level surfaces as a pydantic validation error including the field name."""
    cfg = tmp_path / "config.toml"
    cfg.write_text('log_level = "TRACE"\n')
    errors = config_validate._validate_config_file(cfg)
    assert len(errors) == 1
    err = errors[0]
    assert "log_level" in ".".join(str(p) for p in err["loc"])
    assert "TRACE" in err["msg"] or "valid logging level" in err["msg"]


def test_validate_pure_reports_malformed_toml(tmp_path: Path) -> None:
    """Garbled TOML surfaces as a single toml_decode_error entry."""
    cfg = tmp_path / "config.toml"
    cfg.write_text('not = valid toml = "x"\n[[[\n')
    errors = config_validate._validate_config_file(cfg)
    assert len(errors) == 1
    assert errors[0]["type"] == "toml_decode_error"
    assert "malformed TOML" in errors[0]["msg"]


def test_validate_pure_reports_missing_file(tmp_path: Path) -> None:
    """A non-existent path is reported as file_not_found, not raised."""
    cfg = tmp_path / "absent.toml"
    errors = config_validate._validate_config_file(cfg)
    assert len(errors) == 1
    assert errors[0]["type"] == "file_not_found"
    assert str(cfg) in errors[0]["msg"]


def test_validate_pure_negative_stall_threshold(tmp_path: Path) -> None:
    """ge=1 on stall_threshold_min surfaces in errors with the field name."""
    cfg = tmp_path / "config.toml"
    cfg.write_text("stall_threshold_min = 0\n")
    errors = config_validate._validate_config_file(cfg)
    assert errors
    assert any("stall_threshold_min" in ".".join(str(p) for p in e["loc"]) for e in errors)


# ---------------------------------------------------------------------------
# validate: CLI path
# ---------------------------------------------------------------------------


def test_validate_cli_success_prints_path(tmp_path: Path) -> None:
    cfg = tmp_path / "config.toml"
    _write_valid_toml(cfg)
    result = runner.invoke(cli.app, ["config", "validate", str(cfg)])
    assert result.exit_code == 0
    assert "config valid:" in result.stdout
    assert str(cfg) in result.stdout


def test_validate_cli_quiet_suppresses_success(tmp_path: Path) -> None:
    cfg = tmp_path / "config.toml"
    _write_valid_toml(cfg)
    result = runner.invoke(cli.app, ["config", "validate", "--quiet", str(cfg)])
    assert result.exit_code == 0
    assert result.stdout == ""


def test_validate_cli_failure_returns_2_with_field_name(tmp_path: Path) -> None:
    cfg = tmp_path / "config.toml"
    cfg.write_text('log_level = "NOPE"\n')
    result = runner.invoke(cli.app, ["config", "validate", str(cfg)])
    assert result.exit_code == 2
    assert "log_level" in result.stderr


def test_validate_cli_malformed_toml_clear_message(tmp_path: Path) -> None:
    cfg = tmp_path / "config.toml"
    cfg.write_text("=== not valid ===\n")
    result = runner.invoke(cli.app, ["config", "validate", str(cfg)])
    assert result.exit_code == 2
    assert "malformed TOML" in result.stderr


def test_validate_cli_json_emits_parseable_array(tmp_path: Path) -> None:
    cfg = tmp_path / "config.toml"
    cfg.write_text('log_level = "BAD"\n')
    result = runner.invoke(cli.app, ["config", "validate", "--json", str(cfg)])
    assert result.exit_code == 2
    payload = json.loads(result.stderr)
    assert isinstance(payload, list)
    assert payload
    assert "loc" in payload[0]
    assert "msg" in payload[0]
    assert "type" in payload[0]


def test_validate_cli_defaults_to_platformdirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With no PATH argument, the default platformdirs config file is used."""
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(tmp_path))
    _config._reset_for_test()
    _write_valid_toml(tmp_path / "config.toml")
    result = runner.invoke(cli.app, ["config", "validate"])
    assert result.exit_code == 0
    assert str(tmp_path / "config.toml") in result.stdout


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


def test_schema_json_is_valid_jsonschema_with_id_and_title() -> None:
    text = config_validate._emit_json_schema()
    schema = json.loads(text)
    assert schema.get("$id", "").startswith("https://")
    assert schema.get("title") == "waitbus config"
    assert "properties" in schema
    for required_field in (
        "prom_owner",
        "prom_repo",
        "log_level",
        "stall_threshold_min",
        "heartbeat_sec",
    ):
        assert required_field in schema["properties"], required_field


def test_schema_toml_example_round_trips_back_into_model() -> None:
    """The emitted TOML template parses cleanly and validates against the model."""
    schema = _config.CiStatusConfig.model_json_schema()
    text = config_validate._emit_toml_template(schema)
    # Every assignment is commented out, so the parsed dict contains
    # only the empty [prometheus] section header (no field assignments).
    # The point is that the template is syntactically valid TOML.
    parsed = tomllib.loads(text)
    assert isinstance(parsed, dict)
    assert parsed == {"prometheus": {}}
    # Sanity: the prometheus section header and the named keys appear.
    assert "[prometheus]" in text
    assert "# owner =" in text
    assert "# repo =" in text
    assert "# log_level =" in text
    assert "# stall_threshold_min =" in text
    assert "# heartbeat_sec =" in text


def test_schema_toml_example_uncommented_round_trips(tmp_path: Path) -> None:
    """Uncommenting every assignment produces a TOML that validates against the model."""
    schema = _config.CiStatusConfig.model_json_schema()
    text = config_validate._emit_toml_template(schema)
    uncommented_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.lstrip()
        # Lines that match "# <key> = <value>" become "<key> = <value>".
        if stripped.startswith("# ") and " = " in stripped and not stripped.startswith("# type:"):
            uncommented_lines.append(stripped[2:])
        elif stripped.startswith("[") and stripped.endswith("]"):
            uncommented_lines.append(stripped)
        else:
            # Comments and blank lines drop out; not needed for round-trip.
            continue
    rendered = "\n".join(uncommented_lines) + "\n"
    cfg = tmp_path / "rendered.toml"
    cfg.write_text(rendered)
    assert config_validate._validate_config_file(cfg) == []


def test_schema_cli_json_format(tmp_path: Path) -> None:
    result = runner.invoke(cli.app, ["config", "schema", "--format", "json"])
    assert result.exit_code == 0
    schema = json.loads(result.stdout)
    assert "properties" in schema


def test_schema_cli_toml_example_format() -> None:
    result = runner.invoke(cli.app, ["config", "schema", "--format", "toml-example"])
    assert result.exit_code == 0
    assert "[prometheus]" in result.stdout
    assert "# owner =" in result.stdout


def test_schema_cli_out_writes_file(tmp_path: Path) -> None:
    target = tmp_path / "schema.json"
    result = runner.invoke(cli.app, ["config", "schema", "--format", "json", "--out", str(target)])
    assert result.exit_code == 0
    written = json.loads(target.read_text())
    assert "properties" in written


def test_schema_cli_unknown_format_returns_2() -> None:
    result = runner.invoke(cli.app, ["config", "schema", "--format", "yaml"])
    assert result.exit_code == 2
    assert "unknown format" in result.stderr
