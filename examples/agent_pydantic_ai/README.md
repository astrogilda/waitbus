# Pydantic AI + waitbus — agent subscribe proof

This example shows a [Pydantic AI](https://ai.pydantic.dev/) agent that
subscribes to waitbus and reacts to a workstation-local event. It runs fully
offline, with no LLM network calls.

## Real and faked components

The agent uses a genuine `pydantic_ai.Agent` with a registered tool, run
through the actual agent graph via `agent.run_sync(...)`. The waitbus
integration is also real: the `wait_for_waitbus_event` tool calls the public
waitbus SDK — `waitbus.wait_for(...)` — to block on the broadcast bus and
capture the delivered `EventFrame`.

The LLM is replaced by `pydantic_ai.models.test.TestModel`, a deterministic
stand-in that drives the agent graph and calls each registered tool exactly
once. This makes the example reproducible and free of network calls — no API
keys, no tokens, no flakiness.

The pattern any framework follows is identical: build a real agent, register a
tool that calls `wait_for(...)`, and run it with the framework's deterministic
fake model.

## Install

This example needs the `agent-recipes` optional group (it pulls
`pydantic-ai-slim`, the core package — not the provider-laden meta-package):

```sh
pip install 'waitbus[agent-recipes]'
```

## Run it standalone

The agent blocks on `wait_for`, so something must emit a matching event while it
waits. Start the broadcast daemon, run the agent (it blocks), and emit a
`docker_container` event from another shell:

```python
from examples.agent_pydantic_ai import run

# Blocks until a docker_container event arrives on the broadcast socket
# (or the timeout elapses), then returns the capture.
capture = run("/path/to/broadcast.sock", timeout=5.0)
print(capture.reacted, capture.events)
```

`run` returns an `EventCapture`; `capture.reacted` is `True` once the agent's
tool received an event, and `capture.events` holds the salient fields waitbus
delivered (`event_type`, `owner`, `repo`, `delivery_id`, ...).

## Testing

`tests/test_agent_integration_pydantic_ai.py` is the canary: it stands up the
broadcast daemon, starts this agent on a worker thread (the agent blocks on
`wait_for`), emits one `docker_container` event, and asserts the agent woke and
captured it. The assertion lives at the **waitbus boundary** — event delivered,
agent reacted — never on Pydantic AI's internal state, so the test stays robust
to upstream framework churn.
