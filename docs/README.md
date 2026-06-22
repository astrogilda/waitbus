# waitbus documentation

Reference documentation for [waitbus](../README.md), the workstation-local,
cross-harness event bus. Start with the top-level [README](../README.md) for
install and quick-start; the documents below go deeper.

## Using waitbus

| Document | What it covers |
|---|---|
| [CONSUMER_API.md](CONSUMER_API.md) | The stable public contracts: the broadcast wire protocol, frame kinds, the subscribe handshake, and the reject taxonomy. Read this to write a subscriber. |
| [AGENT_MESSAGING.md](AGENT_MESSAGING.md) | Agent-to-agent request/reply over the bus: the `request` / `respond` SDK, the inbox stream, the message envelope, and the same-UID trust model. The Python companion to the MCP `emit_agent_message` / `read_agent_messages` tools. |
| [CUSTOM_SOURCES.md](CUSTOM_SOURCES.md) | Writing a custom event source as a versioned entry-point plugin. |
| [EXIT_CODES.md](EXIT_CODES.md) | The CLI exit-code contract for `waitbus wait` / `on` / `serve`. |
| [snippets/](snippets/) | Minimal subscriber snippets in Python, Rust, Go, TypeScript, and bash. |
| [emitters/](emitters/) | Recipes for emitting events from external producers (Claude Code hooks, GitHub Actions, shell, Docker). |

## Understanding waitbus

| Document | What it covers |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | The daemon, the AF_UNIX broadcast bus, predicate waits, the MCP surface, and the platform port. |
| [COMPETITIVE_LANDSCAPE.md](COMPETITIVE_LANDSCAPE.md) | Where waitbus fits relative to MCP Tasks, local agent message buses, and orchestration platforms. |

## Operating and testing

| Document | What it covers |
|---|---|
| [SOAK_TEST.md](SOAK_TEST.md) | The 24-hour soak methodology and pass/fail thresholds (longevity). |
| [ROBUSTNESS_TESTS.md](ROBUSTNESS_TESTS.md) | The per-defect correctness track record and property-based robustness layer. |
| [LOGGING_CONVENTIONS.md](LOGGING_CONVENTIONS.md) | The structured-logging conventions for the daemon and CLI. |
| [monitoring/](monitoring/) | Prometheus `/metrics` scrape config and dashboards. |
| [release/](release/) | Release tooling (branch-protection script and related). |

## Project reference

| Document | What it covers |
|---|---|
| [COMPLEXITY.md](COMPLEXITY.md) | The accepted cyclomatic-complexity exceptions table, verified against `radon` by `tests/test_complexity_table.py`. |
| [DEPENDENCY_LICENSES.md](DEPENDENCY_LICENSES.md) | The dependency license audit. |
| [demo/](demo/) | A runnable sample demo project and the recorded hero/demo media. |
