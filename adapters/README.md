# waitbus framework adapters

Self-published sibling packages that embed waitbus into agent frameworks.
Each directory here is a **standalone installable project** with its own
`pyproject.toml`, lockfile, README, and test suite:

| Package | Import name | Surface |
|---------|-------------|---------|
| [`waitbus-pydantic-ai`](waitbus-pydantic-ai/) | `waitbus_pydantic_ai` | `wait_tool` / `emit_tool` factories returning `pydantic_ai.Tool` objects |
| [`waitbus-langgraph`](waitbus-langgraph/) | `waitbus_langgraph` | `wait_node` / `event_router` factories for `StateGraph` graphs |

## Publish-separately model

- The adapters version and release **independently of waitbus**. A waitbus
  release never forces an adapter release, and vice versa.
- They are **never shipped inside the waitbus sdist or wheel**: the waitbus
  sdist is built from an explicit `only-include` list and the wheel packages
  only `waitbus/`, so nothing under `adapters/` enters either artifact.
- At install time each adapter depends on `waitbus` from PyPI
  (`waitbus>=0.1.0,<0.2`). At development time, inside this repository,
  each resolves `waitbus` as an editable path dependency on the repo root
  (`[tool.uv.sources]`), so adapter tests always run against the checkout.

## Upstream pull requests come after traction

No permission is needed to ship these: they live here, one `pip install`
away. Pull requests to the frameworks themselves come only after the
self-published packages demonstrate real usage — maintainers merge what
their users ask for, so users come first and upstream PRs second.

## Running the adapter test suites

The main waitbus test suite never collects or imports these packages.
Each adapter's suite runs inside its own project:

```bash
uv sync --directory adapters/waitbus-pydantic-ai --all-groups
uv run --directory adapters/waitbus-pydantic-ai python -m pytest -q
```

or run both in one shot from the repo root:

```bash
scripts/run_adapter_tests.sh
```

The suites are offline: they drive a real in-process broadcast daemon with
deterministic fake models (`TestModel` for Pydantic AI, `FakeListChatModel`
for LangGraph) — no network, no LLM. The daemon-backed tests are
Linux-only (the broadcast daemon's peer-credential check uses
`SO_PEERCRED`); on macOS they skip.
