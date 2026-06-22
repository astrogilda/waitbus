# Claude Code lifecycle hook

Emit a waitbus event whenever a Claude Code session reaches a lifecycle
point (`Stop`, `SubagentStop`, `SessionEnd`, ...). Other agents, or a
plain `waitbus wait` in a terminal, can then block on "that session
finished a turn" without polling.

The emitter is
[`examples/emitters/claude_code/emit_lifecycle.py`](../../examples/emitters/claude_code/README.md). It
emits through the public `waitbus.emit` API (the `EventInsert` row type
it passes currently rides the private `waitbus._types` module, pending a
package-root export), so the environment running the hook
needs `waitbus` importable (e.g. `pipx install waitbus`, or point the
hook command at a venv's python).

## Install

1. Copy `emit_lifecycle.py` somewhere stable, e.g.
   `~/.local/bin/waitbus-claude-hook.py`.
2. Add hook entries to your `.claude/settings.json` (user-level or
   per-project). This is documentation only; nothing in waitbus writes
   to your settings file:

```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 /home/you/.local/bin/waitbus-claude-hook.py"
          }
        ]
      }
    ],
    "SubagentStop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 /home/you/.local/bin/waitbus-claude-hook.py"
          }
        ]
      }
    ],
    "SessionEnd": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 /home/you/.local/bin/waitbus-claude-hook.py"
          }
        ]
      }
    ]
  }
}
```

3. Make sure the daemons are running (`waitbus serve --all` or the
   systemd units); a hook firing with no daemon emits a row that is
   delivered on the daemon's next start, and a hook firing with no
   store at all prints to stderr and exits 0 (see below).

## Event shape

One row per hook firing, using the built-in `agent` source taxonomy:

| Column | Value |
|---|---|
| `source` | `agent` |
| `event_type` | `agent_message` |
| `owner` | `local` |
| `repo` | `claude-code` |
| `msg_from` | `claude-code:<session_id>` |
| `msg_body` | the hook event name (`Stop`, `SubagentStop`, `SessionEnd`, ...) |
| `payload_json` | the full hook JSON exactly as Claude Code provided it |
| `delivery_id` | `claude-code:<session_id>:<hook_event>:<emit time ns>` |
| `ingest_method` | `claude_code_hook` |

The live broadcast frame is lean: it drops `payload_json`, so live
subscribers match on `msg_body` / `msg_from` (which do ride the wire)
and re-fetch the full hook payload by `event_id` via
`waitbus read-events` if they need it.

`Stop` fires once per response turn and carries no per-turn natural
key, so the timestamp component of the `delivery_id` makes each
occurrence a distinct event by design (idempotency protects against
re-delivery of the *same* occurrence, which a hook does not do).

## Consuming

Block until any Claude Code session ends a turn:

```sh
waitbus wait --source agent --match 'fields.msg_body="Stop"' --timeout 30m
```

Or from Python, stream every lifecycle event:

```python
from waitbus import subscribe

for frame in subscribe(source="agent", match='fields.msg_from="claude-code:my-session-id"'):
    print(frame.fields["msg_body"], frame.delivery_id)
```

The `--match` grammar is documented in `docs/CONSUMER_API.md`
(`<dotted.key>=<json_literal>`, AND across repeats).

## Never-block contract

The script always exits 0. Emit failures (daemons down, store missing,
unreadable stdin) print one line to stderr and are otherwise swallowed.
A broken bus must never block a Claude Code session, because hook
failures surface as noise in the agent transcript.

## Uninstall

Delete the hook entries from `.claude/settings.json` and optionally the
copied script. Emitted rows need no cleanup; they age out via the
normal `waitbus db prune` retention path.
