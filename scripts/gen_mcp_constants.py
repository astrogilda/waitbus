#!/usr/bin/env python3
"""Generate waitbus/_mcp_constants.py from the local MCP spec schema.

Reads the JSON schema at ~/Documents/git-clones/mcp-spec/schema/2025-11-25/schema.json
and emits a Python module with method-name constants and required-field
tuples. The generated module is checked in to the repo; regenerate on
demand when the spec advances. Idempotent: re-running with no spec
change produces a byte-identical output.

Usage:
    python3 scripts/gen_mcp_constants.py
    # or, to validate the checked-in file matches the spec:
    python3 scripts/gen_mcp_constants.py --check
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
# Clone https://github.com/modelcontextprotocol/modelcontextprotocol and set
# WAITBUS_MCP_SPEC_PATH to the schema JSON path, or pass --spec explicitly.
DEFAULT_SPEC = Path(
    os.environ.get(
        "WAITBUS_MCP_SPEC_PATH",
        "mcp-spec/schema/2025-11-25/schema.json",
    )
)
DEFAULT_OUT = REPO_ROOT / "waitbus" / "_mcp_constants.py"

# Method-name definitions to emit. Each maps a Python constant name to
# the spec definition key. The spec uses const: "<method-name>" inside
# the method field — we extract that const and emit it as a string
# constant on the Python side.
_METHODS = {
    "INITIALIZE_REQUEST": "InitializeRequest",
    "INITIALIZED_NOTIFICATION": "InitializedNotification",
    "RESOURCE_UPDATED_NOTIFICATION": "ResourceUpdatedNotification",
    "LOGGING_MESSAGE_NOTIFICATION": "LoggingMessageNotification",
    # NOTE: ClaudeChannelNotification is Anthropic-private — NOT in the
    # MCP spec. The constant is hard-coded below; it does not come from
    # the spec schema.
}

# Required-field tuples emitted so callers can do a final sanity check
# on outgoing envelopes (defense-in-depth on top of producer validation).
_REQUIRED_FIELD_DEFS = {
    "INITIALIZE_RESULT_REQUIRED": "InitializeResult",
    "RESOURCE_UPDATED_PARAMS_REQUIRED": "ResourceUpdatedNotificationParams",
    "IMPLEMENTATION_REQUIRED": "Implementation",
}

# Spec protocolVersion — the date string from the schema's filename /
# top-level identifier. Pinned here so a future regeneration updates it
# in one place.
_PROTOCOL_VERSION = "2025-11-25"

# Anthropic-private extension method names. NOT in the spec schema;
# documented at code.claude.com/docs/en/channels-reference.
_ANTHROPIC_PRIVATE_METHODS = {
    "CLAUDE_CHANNEL_NOTIFICATION_METHOD": "notifications/claude/channel",
    # The permission-relay pair is gated by a separate experimental
    # capability and waitbus does not implement it — but the
    # constants are emitted for future use without needing to regenerate.
    "CLAUDE_CHANNEL_PERMISSION_REQUEST_METHOD": "notifications/claude/channel/permission_request",
    "CLAUDE_CHANNEL_PERMISSION_METHOD": "notifications/claude/channel/permission",
}


def render(spec: dict[str, Any]) -> str:
    """Build the generated module text from the spec dict."""
    defs = spec["$defs"]
    lines: list[str] = []
    lines.append(
        '"""Constants derived from the MCP spec schema by '
        "scripts/gen_mcp_constants.py.\n\n"
        f"Source: schema {_PROTOCOL_VERSION}. Regenerate by\n"
        "running ``python3 scripts/gen_mcp_constants.py`` after a spec\n"
        "advance; the file is checked in deliberately so the build does\n"
        "not depend on the spec clone being present.\n"
        '"""\n'
    )
    lines.append("from __future__ import annotations\n\n")
    lines.append("from typing import Final\n\n")

    lines.append(f'PROTOCOL_VERSION: Final[str] = "{_PROTOCOL_VERSION}"\n\n')

    lines.append("# === Spec-derived method names ============================\n")
    for const_name, def_name in _METHODS.items():
        method_const = defs[def_name]["properties"]["method"]["const"]
        lines.append(f'{const_name}_METHOD: Final[str] = "{method_const}"\n')
    lines.append("\n")

    lines.append("# === Anthropic-private method names (NOT in spec) ==========\n")
    lines.append(
        "# Source: https://code.claude.com/docs/en/channels-reference\n"
        "# These are extensions Claude Code recognises; spec-compliant\n"
        "# clients ignore unknown method names per JSON-RPC 2.0 rules.\n"
    )
    for const_name, method_str in _ANTHROPIC_PRIVATE_METHODS.items():
        lines.append(f'{const_name}: Final[str] = "{method_str}"\n')
    lines.append("\n")

    lines.append("# === Required-field tuples for outgoing-envelope sanity ====\n")
    for const_name, def_name in _REQUIRED_FIELD_DEFS.items():
        required = tuple(defs[def_name]["required"])
        lines.append(f"{const_name}: Final[tuple[str, ...]] = " + repr(required) + "\n")
    lines.append("\n")

    return "".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--spec",
        type=Path,
        default=DEFAULT_SPEC,
        help="Path to the MCP spec schema.json.",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="Output module path.",
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="Compare the freshly-rendered output against the checked-in file; exit 1 on drift.",
    )
    args = ap.parse_args()

    if not args.spec.exists():
        print(f"::error::spec schema not found at {args.spec}", file=sys.stderr)
        return 2

    with args.spec.open("rb") as fp:
        spec = json.load(fp)
    rendered = render(spec)

    if args.check:
        if not args.out.exists():
            print(f"::error::{args.out} does not exist; run without --check first", file=sys.stderr)
            return 1
        existing = args.out.read_text()
        if existing != rendered:
            print(
                f"::error::{args.out} is out of date vs {args.spec}; run scripts/gen_mcp_constants.py to regenerate.",
                file=sys.stderr,
            )
            return 1
        return 0

    args.out.write_text(rendered)
    print(f"wrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
