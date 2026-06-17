# agent_langgraph — a real LangGraph agent that reacts to a waitbus event

This example shows a [LangGraph](https://github.com/langchain-ai/langgraph)
agent subscribing to [waitbus](https://github.com/astrogilda/waitbus) and
reacting to an event, fully offline, with no LLM network calls.

## The graph

`agent.py` builds a genuine `langgraph.graph.StateGraph` with two nodes:

1. **`wait_on_waitbus`** — calls the waitbus SDK's blocking
   `waitbus.wait_for(...)` against the broadcast daemon's socket and
   writes the received event into graph state. This is the real
   integration: a LangGraph node subscribed to the bus, woken by an event the
   daemon fans out.
2. **`summarize`** — feeds the event into a LangChain chat model and records the
   reply.

## Real and faked components

- **Real:** the LangGraph graph (build → compile → `invoke`) and the waitbus
  integration (the `wait_on_waitbus` node calling `wait_for`).
- **Faked for determinism:** the chat model in the `summarize` node defaults to
  a `langchain_core.language_models.fake_chat_models.FakeListChatModel`, so the
  graph runs reproducibly and never touches the network. Pass a real
  `ChatAnthropic` / `ChatOpenAI` via `build_graph(..., model=...)` for a live
  agent — the waitbus wiring is unchanged.

## Run it

The graph blocks in `wait_on_waitbus` until a matching event arrives on the bus,
so you need a running waitbus broadcast daemon and something emitting a
`docker_container` event. The canary test
`tests/test_agent_integration_langgraph.py` shows the full dance (stand up the
daemon, run the graph on a worker thread, emit one event, assert it reacted):

```python
from examples.agent_langgraph import build_graph

graph = build_graph(socket_path="/path/to/broadcast.sock", timeout=5.0)
final_state = graph.invoke({"event_type": None, "summary": None, "reacted": False})
assert final_state["reacted"] is True
print(final_state["event_type"])  # "docker_container"
print(final_state["summary"])     # the fake model's deterministic reply
```

`run(socket_path)` is a convenience wrapper that compiles and invokes in one
call.
