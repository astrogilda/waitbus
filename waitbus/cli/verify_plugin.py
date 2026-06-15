"""`verify-plugin` top-level command — validate .claude-plugin/plugin.json."""

from __future__ import annotations

import os
from pathlib import Path

import typer


def verify_plugin() -> None:
    """Validate .claude-plugin/plugin.json.

    Resolves the plugin root via the CLAUDE_PLUGIN_ROOT environment variable
    (defaults to the current working directory). Validates that plugin.json
    parses as JSON and contains the required fields: name, version,
    schemaVersion. Exit 0 if valid; exit 1 otherwise.
    """
    import json

    plugin_root = Path(os.environ.get("CLAUDE_PLUGIN_ROOT", str(Path.cwd())))
    plugin_json_path = plugin_root / ".claude-plugin" / "plugin.json"

    typer.echo(f"plugin_root: {plugin_root}")
    typer.echo(f"plugin_json: {plugin_json_path}")

    if not plugin_json_path.exists():
        typer.secho(
            f"ERROR: plugin.json not found at {plugin_json_path}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    raw = plugin_json_path.read_text(encoding="utf-8")
    try:
        data: dict[str, object] = json.loads(raw)
    except json.JSONDecodeError as exc:
        typer.secho(
            f"ERROR: plugin.json is not valid JSON: {exc}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1) from exc

    required_fields = ("name", "version", "schemaVersion")
    missing = [f for f in required_fields if f not in data]
    if missing:
        typer.secho(
            f"ERROR: plugin.json missing required fields: {', '.join(missing)}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    for field in required_fields:
        typer.echo(f"{field}: {data[field]}")

    typer.secho("plugin.json valid", fg=typer.colors.GREEN)
