# waitbus-langgraph

LangGraph nodes for the [waitbus](https://github.com/astrogilda/waitbus)
workstation event bus: block a `StateGraph` on a bus predicate
(CI / pytest / Docker / filesystem / agent events) and route on delivery
vs timeout, with zero polling.

## Install

```bash
pip install waitbus-langgraph
```

Requires a running waitbus broadcast daemon (`waitbus broadcast serve`).

## Usage

```python
from langgraph.graph import END, START, StateGraph
from waitbus_langgraph import event_router, wait_node

builder = StateGraph(MyState)
builder.add_node("wait_on_bus", wait_node('fields.conclusion="failure"', source="github", timeout=600.0))
builder.add_node("triage", my_triage_node)
builder.add_edge(START, "wait_on_bus")
builder.add_conditional_edges("wait_on_bus", event_router(on_event="triage", on_timeout=END))
builder.add_edge("triage", END)
graph = builder.compile()
```

`wait_node` wraps the public `waitbus.wait_for` and writes two state
channels: `event` (the delivered frame as a dict, `None` on timeout) and
`reacted` (the boolean `event_router` branches on). Both channel names are
configurable via `event_key` / `reacted_key`.

## Offline-test guarantee

The package's e2e suite runs a **real in-process broadcast daemon** and a
deterministic `FakeListChatModel`: no network, no LLM. A green suite
means the bus delivered and the graph routed. The daemon-backed tests are
Linux-only (the daemon's peer-credential check uses `SO_PEERCRED`); on
macOS they skip.

## License

MIT
