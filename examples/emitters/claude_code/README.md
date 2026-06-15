# Claude Code lifecycle-hook emitter

`emit_lifecycle.py` emits one waitbus event per Claude Code lifecycle
hook firing (`Stop`, `SubagentStop`, `SessionEnd`, ...). It always
exits 0 so a failing emit can never block the agent session.

Install instructions, the emitted event shape, consuming examples, and
uninstall: [docs/emitters/claude-code-hook.md](../../../docs/emitters/claude-code-hook.md).
