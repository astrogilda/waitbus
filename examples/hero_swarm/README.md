# HERO cross-harness swarm demo

Shows that waitbus is a cross-harness coordination bus, not a
single-process toy: two different agent frameworks (Pydantic AI
and LangGraph) run as separate OS processes subscribed to one local
broadcast daemon. A third process (a Pydantic AI worker) fails and announces
it on the bus; a peer on the other framework (LangGraph) wakes the instant
the failure lands, and a live `waitbus top` view reacts at the same time.
Nothing polls: each peer is parked in the waitbus SDK's blocking `wait_for`
until the failure event arrives.

## Differences from `waitbus swarm-demo`

`waitbus swarm-demo` proves the coordination primitive, but it runs every "agent"
as an in-process coroutine against one in-process daemon. It **cannot** prove
*cross-harness* failure broadcast, because there is one process and one
framework. This demo launches the real `waitbus broadcast serve`, `waitbus top`, and
the two agent frameworks as **distinct `subprocess` children** connecting over an
AF_UNIX socket. A green run is genuinely "framework A in process X woke framework
B in process Y".

## Real vs faked

- The agents are **synthesized with fake models** -- Pydantic AI's `TestModel`
  and LangGraph's `FakeListChatModel`. No real LLM, no network, no account, no
  cloud. The waitbus integration (the `wait_for` subscribe and the `emit` failure
  broadcast) is **real**; only the model is faked, exactly like the committed
  [`examples/agent_pydantic_ai`](../agent_pydantic_ai/) and
  [`examples/agent_langgraph`](../agent_langgraph/) canaries.
- The failure event is **injected**: the failing worker does not crash a real
  build; it deterministically emits one `agent_task_failed` event so the demo is
  reproducible. The orchestrator banner names every agent process's PID.
- The `agent` source is a **demo convention**, not a committed waitbus product
  source. It is registered **in-process** (for the demo daemon and the emitting
  worker only) -- never added to the built-in source taxonomy and never shipped
  as a `waitbus.sources.v1` plugin. A future agent-coordination vocabulary, if
  demand confirms it, defines its own source and event types when that work
  lands.

## Run it

From the repo root, with the `agent-recipes` group installed (it provides the
Pydantic AI + LangGraph frameworks the agents use):

```bash
uv run --group agent-recipes python -m examples.hero_swarm
```

(Use `uv run` / the project venv, not a bare `python` — the demo imports
`waitbus` and the two agent frameworks, none of which a system Python has.)

You will see: the broadcast daemon bind, the honesty banner (with PIDs), the
`agent/agent_task_failed` row appear in the `waitbus top` view, and a final
verdict:

```
[hero-swarm] PROVEN: two DIFFERENT frameworks woke on ONE peer's failure --
cross-harness failure broadcast on a single local bus.
```

The orchestrator supervises every child process in its **own process group** and
tears them all down on exit (`SIGTERM` -> bounded grace -> group `SIGKILL`), so a
clean or aborted run leaves **zero orphan processes** and removes its temporary
state directory.

## The proof test

The end-to-end proof lives in
[`tests/test_hero_swarm_e2e.py`](../../tests/test_hero_swarm_e2e.py). It runs the
full real-process topology and asserts both frameworks observably woke on the
single failure event, and that teardown left no sockets behind. Run it:

```bash
uv run python -m pytest tests/test_hero_swarm_e2e.py -q
```

It is marked `slow` and skips on non-Linux (the broadcast daemon's AF_UNIX
`SO_PEERCRED` and own-process-group supervision are Linux-only), and skips
cleanly when `pydantic_ai` or `langgraph` is not installed.

## Recording (GIF / MP4)

A [VHS](https://github.com/charmbracelet/vhs) tape records the demo inside a
tmux session. From `docs/demo/.waitbus-demo/`:

```bash
make hero          # version-checks vhs + render font + tmux, then renders
# or directly:
vhs hero.tape      # produces hero.gif + hero.mp4
```

`make hero` refuses to render if VHS is too old, the pinned render font
(JetBrains Mono) is missing, or tmux is absent -- the same refuse-to-render
posture the `demo` target uses, so a stale or garbled recording never ships.
Rendering needs `vhs`, `tmux`, and `ttyd` on PATH; the tape is the source of
truth and the GIF/MP4 are derived outputs (`make clean` removes them).
