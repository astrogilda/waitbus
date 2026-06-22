# waitbus-pydantic-ai

Pydantic AI tools for the [waitbus](https://github.com/astrogilda/waitbus)
workstation event bus: let an agent block on a bus predicate
(CI / pytest / Docker / filesystem / agent events) and emit agent events,
with zero polling.

## Install

```bash
pip install waitbus-pydantic-ai
```

Requires a running waitbus broadcast daemon (`waitbus broadcast serve`).

## Usage

```python
from pydantic_ai import Agent
from waitbus_pydantic_ai import emit_tool, wait_tool

agent = Agent(
    model="anthropic:claude-sonnet-4-5",
    tools=[
        wait_tool('fields.conclusion="failure"', source="github", timeout=600.0),
        emit_tool(agent_name="ci-fixer"),
    ],
)
result = agent.run_sync("Wait for the next CI failure, then announce you are on it.")
```

`wait_tool` wraps the public `waitbus.wait_for` (the tool returns the
delivered event frame as a dict, or a timed-out notice); `emit_tool` wraps
the public `waitbus.emit` with the bus's agent-event envelope, publishing
the model's message as an addressed `agent_message` event.

## Offline-test guarantee

The package's e2e suite runs a **real in-process broadcast daemon** and a
deterministic `TestModel`: no network, no LLM. A green suite means the
bus delivered and the agent woke. The daemon-backed tests are Linux-only
(the daemon's peer-credential check uses `SO_PEERCRED`); on macOS they
skip.

## License

MIT
