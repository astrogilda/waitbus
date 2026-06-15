#!/usr/bin/env python3
"""Claude Code lifecycle-hook emitter for waitbus.

Reads the hook JSON Claude Code passes on stdin, then emits one
``agent``-source event via the public :func:`waitbus.emit` API (the
``EventInsert`` row type rides the private ``waitbus._types`` module,
pending a package-root export) so any
local subscriber (another agent, ``waitbus wait``, a dashboard) can
react to the session's lifecycle without polling.

Never-block contract
--------------------
This script ALWAYS exits 0. A hook that fails (daemons down, store
missing, malformed stdin) must never block or annoy the agent session,
so every failure is reported on stderr and swallowed. The emit is a
courtesy broadcast, not a critical path.

Install / event shape / uninstall: docs/emitters/claude-code-hook.md.
"""

from __future__ import annotations

import json
import sys
import time


def read_hook_input() -> tuple[str, str, str]:
    """Return ``(session_id, hook_event_name, payload_json)`` from the hook JSON.

    Tolerates empty or malformed stdin (both fall back to ``unknown``
    identifiers and a ``{}`` payload) — the never-block contract means a
    surprising hook shape degrades the event quality, never the exit code.
    ``payload_json`` is always valid JSON: stdin that parses passes through
    verbatim; stdin that does not is replaced by the fallback ``{}``.
    """
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        # Malformed stdin: emit the validated fallback dict, NEVER the raw
        # text -- the stored payload_json column must always be valid JSON.
        payload = {}
        raw = json.dumps(payload)
    if not isinstance(payload, dict):
        payload = {}
    session_id = str(payload.get("session_id", "unknown"))
    hook_event = str(payload.get("hook_event_name", "unknown"))
    return session_id, hook_event, raw.strip() or "{}"


def main() -> int:
    """Emit one lifecycle event; print the delivery_id on success. Always 0."""
    try:
        from waitbus import emit
        from waitbus._types import EventInsert

        session_id, hook_event, raw = read_hook_input()
        now_ns = time.time_ns()
        result = emit(
            EventInsert(
                delivery_id=f"claude-code:{session_id}:{hook_event}:{now_ns}",
                source="agent",
                event_type="agent_message",
                owner="local",
                repo="claude-code",
                received_at=now_ns,
                payload_json=raw,
                ingest_method="claude_code_hook",
                msg_from=f"claude-code:{session_id}",
                msg_body=hook_event,
            )
        )
        print(result.event.delivery_id)
    except Exception as exc:  # the never-block contract: report and swallow
        print(f"waitbus lifecycle emit failed (ignored): {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
