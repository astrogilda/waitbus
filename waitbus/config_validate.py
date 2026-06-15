"""``waitbus config`` subcommand implementation.

Operator-facing pre-flight validation and schema emission for the
``config.toml`` file consumed by :class:`waitbus._config.WaitbusConfig`.
The daemon already validates on startup (loud-fail); this module surfaces
the same checks at operator time so a bad config can be caught before any
service is restarted.

Two verbs:

- ``config validate [PATH]`` — loads PATH (or the platformdirs-resolved
  ``~/.config/waitbus/config.toml``), feeds it through the
  pydantic-settings model, and reports validation errors as either
  human-readable text (default) or a JSON array (``--json``). Exit 0 on
  success, 2 on any failure (parser error, validation error, file not
  found).
- ``config schema`` — emits the canonical JSON schema (default) or a
  commented TOML template (``--format=toml-example``) covering every
  field with its type and default. The TOML template is intended for
  operators to paste, uncomment, and customise.

The module exposes the typer sub-app ``config_app`` (wired into the
umbrella CLI in ``waitbus.cli``) and a small set of pure functions
that the contract tests exercise directly. The pure-function split keeps
the TOML-template emitter and the validation-error formatter
testable without spinning up the full typer runner.
"""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path
from typing import Any

import typer
from pydantic import ValidationError

from . import _paths
from ._config import WaitbusConfig, _flatten_toml

# ---------------------------------------------------------------------------
# typer sub-app
# ---------------------------------------------------------------------------

config_app = typer.Typer(
    name="config",
    help="Configuration validation and schema emission.",
    no_args_is_help=True,
    add_completion=False,
)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


def _format_validation_errors_text(errors: list[dict[str, Any]]) -> str:
    """Render pydantic ``ValidationError.errors()`` as a human-readable block.

    Each line is ``field_path: error_message (type=error_type)``. Field
    paths use dot notation; nested fields surface as ``parent.child``.
    """
    lines: list[str] = []
    for err in errors:
        loc = ".".join(str(part) for part in err.get("loc", ())) or "<root>"
        msg = err.get("msg", "invalid value")
        typ = err.get("type", "value_error")
        lines.append(f"  {loc}: {msg} (type={typ})")
    return "\n".join(lines)


def _validate_config_file(toml_path: Path) -> list[dict[str, Any]]:
    """Validate ``toml_path`` against ``WaitbusConfig``.

    Returns an empty list on success, otherwise a list of pydantic
    error dicts (``.loc`` / ``.msg`` / ``.type``). Missing file or
    malformed TOML is reported as a single-element error list with
    ``loc=("<file>",)`` so the caller's emitter does not need to branch.
    """
    if not toml_path.exists():
        return [
            {
                "loc": ("<file>",),
                "msg": f"config file not found: {toml_path}",
                "type": "file_not_found",
            }
        ]
    try:
        with toml_path.open("rb") as fp:
            toml_data = tomllib.load(fp)
    except tomllib.TOMLDecodeError as exc:
        return [
            {
                "loc": ("<file>",),
                "msg": f"malformed TOML: {exc}",
                "type": "toml_decode_error",
            }
        ]
    except PermissionError as exc:
        return [
            {
                "loc": ("<file>",),
                "msg": f"unreadable config file: {exc}",
                "type": "permission_error",
            }
        ]

    flat = _flatten_toml(toml_data)
    try:
        # model_validate bypasses the env-var / TOML source merge so the
        # caller's PATH (not the platformdirs file) is what's tested.
        WaitbusConfig.model_validate(flat)
    except ValidationError as exc:
        # exc.errors() is the public pydantic v2 API; each element is a
        # TypedDict (ErrorDetails) compatible with dict[str, Any] at runtime.
        # Cast through Any to keep mypy --strict happy without leaking
        # pydantic's internal TypedDict shape into our return type.
        return [dict(err) for err in exc.errors()]
    return []


def run_validate(
    path: Path | None,
    *,
    as_json: bool,
    quiet: bool,
) -> int:
    """Validate ``path`` (or the default config) and return the exit code.

    Exit 0 on success, 2 on any failure. ``--quiet`` suppresses the
    success line; failure output is never suppressed.
    """
    effective = path if path is not None else _paths.config_file()
    errors = _validate_config_file(effective)
    if not errors:
        if not quiet:
            print(f"config valid: {effective}")
        return 0

    if as_json:
        # Serialise loc tuples as lists so the output is plain JSON.
        payload = [
            {
                "loc": list(err.get("loc", ())),
                "msg": err.get("msg", ""),
                "type": err.get("type", "value_error"),
            }
            for err in errors
        ]
        print(json.dumps(payload, indent=2), file=sys.stderr)
    else:
        print(
            f"waitbus config validate: {effective} is invalid:",
            file=sys.stderr,
        )
        print(_format_validation_errors_text(errors), file=sys.stderr)
    return 2


@config_app.command(name="validate")
def validate_cmd(
    path: Path | None = typer.Argument(  # noqa: B008  (typer idiom)
        None,
        help="Path to the config.toml file to validate. Defaults to the "
        "platformdirs-resolved location "
        "(typically ~/.config/waitbus/config.toml on Linux).",
        exists=False,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    as_json: bool = typer.Option(
        False,
        "--json",
        help="Emit validation errors as a JSON array on stderr. Useful for "
        "editor plugins and CI lint hooks that prefer structured input "
        "over human-readable text.",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Suppress the success line on stdout. Failures still print to "
        "stderr. Useful when piping into scripts that only care about "
        "the exit code.",
    ),
) -> None:
    """Pre-flight validate a waitbus config.toml.

    Loads the file, parses the TOML, and runs the pydantic-settings model
    over it. Reports the same validation errors the daemon would raise at
    startup, but at operator time so a bad config can be caught before any
    service is restarted.

    Exits 0 on success and 2 on any failure (file missing, malformed TOML,
    field validation error). Use ``--json`` for structured error output
    consumable by editor plugins or CI.
    """
    raise typer.Exit(run_validate(path, as_json=as_json, quiet=quiet))


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


def _emit_json_schema() -> str:
    """Return the canonical JSON schema for WaitbusConfig as a pretty string.

    Uses pydantic v2's ``model_json_schema`` (default ``validation`` mode);
    augments the schema with ``$id`` and the package title so editor JSON
    Schema linters can pin the document.
    """
    schema = WaitbusConfig.model_json_schema()
    schema.setdefault("$id", "https://waitbus/config.schema.json")
    # Override pydantic's class-name-derived title so editor pickers show
    # the operator-facing name rather than the Python class name.
    schema["title"] = "waitbus config"
    return json.dumps(schema, indent=2, sort_keys=True)


def _toml_value_literal(value: Any) -> str:
    """Encode a Python default into its TOML literal form.

    Supports the subset of types the current WaitbusConfig uses (str,
    int, float, bool, None). ``None`` becomes a commented placeholder
    because TOML has no null literal; the emitted line is still syntactically
    valid when commented out. Anything else falls back to a JSON literal so
    the operator at least sees the value, even if they have to edit syntax.

    The fallback ``json.dumps(value)`` is called WITHOUT a ``default=str``
    coercion on purpose. Every WaitbusConfig field defaults to one of the
    JSON-native scalar types above, so the fallback is unreachable for the
    current schema; it exists only as a preventive guard for a future
    field whose default is a list or dict. A ``default=str`` argument
    would silently stringify an unexpected non-serialisable default
    instead of raising — masking a schema mistake the maintainer should
    see at template-generation time, not discover from a malformed
    config template in the field.
    """
    if value is None:
        return "# <no default — unset to use system default>"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value)


def _emit_toml_template(schema_dict: dict[str, Any]) -> str:
    """Render a commented config.toml template from a pydantic JSON schema.

    Every field becomes a commented assignment ``# field = <default>`` with
    a preceding ``# <description>`` line and a ``# type: <type>`` line so
    operators can read the schema without consulting the source. The
    output is itself valid TOML (every assignment is commented out), so
    operators uncomment the lines they want to override.

    The two ``prom_*`` fields are emitted under a ``[prometheus]`` section
    using the ``owner`` / ``repo`` names recognised by the loader's
    ``_flatten_toml`` (see ``_config._flatten_toml``); all other fields
    are top-level. This mirrors the precedence + key-naming the daemon
    actually consumes.
    """
    properties: dict[str, Any] = schema_dict.get("properties", {})

    def _emit_field(toml_key: str, prop: dict[str, Any]) -> list[str]:
        lines: list[str] = []
        description = prop.get("description", "")
        if description:
            for line in description.splitlines():
                lines.append(f"# {line.rstrip()}")
        type_label = prop.get("type", "any")
        lines.append(f"# type: {type_label}")
        default = prop.get("default")
        literal = _toml_value_literal(default)
        lines.append(f"# {toml_key} = {literal}")
        return lines

    out: list[str] = [
        "# waitbus config.toml template",
        "# Every field is shown commented out at its default value.",
        "# Uncomment a line and edit the value to override.",
        "# Environment variables (prefix WAITBUS_) always win over this file.",
        "",
    ]

    top_level_fields = ("log_level", "stall_threshold_min", "heartbeat_sec")
    for field_name in top_level_fields:
        if field_name in properties:
            out.extend(_emit_field(field_name, properties[field_name]))
            out.append("")

    if "prom_owner" in properties or "prom_repo" in properties:
        out.append("[prometheus]")
        if "prom_owner" in properties:
            out.extend(_emit_field("owner", properties["prom_owner"]))
        if "prom_repo" in properties:
            out.extend(_emit_field("repo", properties["prom_repo"]))
        out.append("")

    return "\n".join(out).rstrip() + "\n"


def run_schema(fmt: str, out_path: Path | None) -> int:
    """Emit the schema in ``fmt`` to ``out_path`` (or stdout). Return exit code."""
    if fmt == "json":
        text = _emit_json_schema()
    elif fmt == "toml-example":
        text = _emit_toml_template(WaitbusConfig.model_json_schema())
    else:
        print(
            f"waitbus config schema: unknown format {fmt!r}; expected 'json' or 'toml-example'.",
            file=sys.stderr,
        )
        return 2

    if out_path is None:
        print(text)
    else:
        out_path.write_text(text, encoding="utf-8")
    return 0


@config_app.command(name="schema")
def schema_cmd(
    fmt: str = typer.Option(
        "json",
        "--format",
        "-f",
        help="Output format. 'json' (default) emits the canonical JSON "
        "schema for config.toml; 'toml-example' emits a commented "
        "TOML template with every field, its type, and its default.",
    ),
    out_path: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None,
        "--out",
        help="Write to PATH instead of stdout.",
    ),
) -> None:
    """Emit the canonical config schema.

    Two formats:

    - ``--format=json`` (default): JSON Schema for the config.toml; pin
      this in editor settings (``schemastore``-style) to get linting and
      autocomplete on the config file.
    - ``--format=toml-example``: a commented config.toml template that
      lists every supported field with its type and default. Operators
      paste it into ``~/.config/waitbus/config.toml`` and uncomment
      the lines they want to override.
    """
    raise typer.Exit(run_schema(fmt, out_path))
