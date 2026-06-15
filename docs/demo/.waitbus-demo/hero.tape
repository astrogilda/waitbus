# VHS tape -- the HERO cross-harness swarm demo.
#
# Records the load-bearing proof: TWO different real agent frameworks
# (Pydantic AI + LangGraph) running as SEPARATE OS processes on ONE local
# waitbus bus; one agent fails and a PEER on the OTHER framework -- plus a live
# `waitbus top` view -- react to the failure broadcast. Fully offline (fake
# models), real OS processes (not coroutines).
#
# Render with:
#
#     vhs hero.tape          # produces hero.gif + hero.mp4
#
# Or via the Makefile, which version-checks vhs + the render font + tmux:
#
#     make hero
#
# VHS install: https://github.com/charmbracelet/vhs
#
# This tape drives the demo inside tmux so the multi-process orchestration
# (daemon + two agent processes + `waitbus top`) renders as one coherent
# session. `Require tmux` makes the render fail loudly if tmux is absent
# rather than silently producing a single-shell recording.
#
# Geometry / font / theme are pinned to demo.tape so the two recordings are
# visually consistent.

Require tmux

Output hero.gif
Output hero.mp4

Set FontFamily "JetBrains Mono"
Set FontSize 28
Set Width 2800
Set Height 1400
Set Theme "Dracula"
# VHS compresses idle frames, so rendered duration tracks frames where the
# screen changes (somewhat noisy run to run), not wall-clock. Quarter-speed
# playback stretches the orchestrator's steady output so each line lands
# readably. Recording-only; the demo itself runs in real time.
Set PlaybackSpeed 0.25
Set Shell "bash"
Set TypingSpeed 60ms

# Opening: a clean terminal, then a single tmux session for the whole demo.
Hide
Type "clear"
Enter
Sleep 200ms
# Two stacked panes: the orchestrator (daemon log + banner + cross-harness
# result) on top, and a free shell below that ends up showing the temp state
# being cleaned up. The orchestrator already spawns the daemon, both agent
# frameworks, and `waitbus top` as real child processes, so the top pane is the
# full multi-process story in one stream.
Type "tmux kill-session -t hero 2>/dev/null; tmux new-session -d -s hero -x 220 -y 46"
Enter
Type "tmux split-window -v -t hero"
Enter
Type "tmux select-pane -t hero.0"
Enter
Show

# Pitch line, then the one command. The orchestrator is self-contained: it
# launches a temporary broadcast daemon, parks a Pydantic AI agent and a
# LangGraph agent (two DIFFERENT frameworks, two processes) on the bus, starts
# `waitbus top`, drives one agent to fail, and shows the peer + the live view
# reacting -- then tears every child down with no orphans.
Type "tmux send-keys -t hero.0 '# waitbus HERO: two different agent frameworks on one local bus; one fails, the peer reacts.' Enter"
Enter
Sleep 800ms
Type "tmux send-keys -t hero.0 'cd $(git rev-parse --show-toplevel) && uv run --group agent-recipes python -m examples.hero_swarm' Enter"
Enter

# The orchestrator paces its own output (daemon bind, the honesty banner, the
# `agent/agent_task_failed` row from `waitbus top`, then the PROVEN verdict).
# Attach so VHS captures the live session; sleep past the demo's wall time
# (~5s of work plus the agents' settle window) with a settle on the verdict.
Type "tmux attach -t hero"
Enter
Sleep 16s
