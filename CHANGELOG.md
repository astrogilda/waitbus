# Changelog

All notable changes to **waitbus** are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning

Pre-1.0 releases may refine the API based on real-world usage; v1.0 will
declare API stability after a period of stable public use.

## [0.1.0] — first public release

waitbus is a workstation-local async event bus for your machine's agents:
wait on anything from any source, and let your agents coordinate over the
same local bus. It is local-only (an `AF_UNIX` socket, no network egress),
durable (events persist to SQLite with replay), and zero-polling (an
`eventfd` doorbell wakes subscribers instead of a clock loop).

### Sources — wait on what already finished or failed

- **CI jobs** — a GitHub workflow run finishing or failing.
- **Test runs** — a pytest run passing or failing.
- **Containers** — docker container lifecycle events.
- **Filesystem** — file and directory changes.
- Third-party sources register through the `waitbus.sources.v1` entry-point group.

### Subscribers — any agent or script can wait and react

- **Agent frameworks** — Pydantic AI and LangGraph agents subscribe and react over the public SDK.
- **Claude Code** — receives pushes over the MCP notification channel.
- **Any MCP client** — pulls events via tool calls or a `tail_events` long-poll.
- **Scripts / CLI** — `waitbus wait`, plus hand-decoding subscriber snippets in Python, Go, Rust, and TypeScript.

### Core

- `waitbus wait <predicate>` blocks until a matching event arrives, with a
  `since=` cursor for durable offline catch-up.
- Daemon broadcast fan-out delivers each event to every subscriber over its
  own buffer.
- Cross-agent failure broadcast: when one peer fails, the rest of the swarm
  is notified on the same bus.
- Broadcast wire protocol v1 (frozen): typed frames with an open `kind`
  discriminator, an ack-first subscribe handshake, and an `event_id` identity
  carried on every data frame.
- Backpressure with whole-frame delivery: a subscriber that falls behind is
  evicted with a `subscribe_rejected{lag_limit_exceeded}` frame when the wire
  sits at a frame boundary, or a clean EOF otherwise — never a torn frame.
- Opt-in loopback Prometheus `/metrics` on the broadcast daemon;
  `waitbus_broadcast_events_delivered_total` counts event frames only, at
  kernel-accept (control frames are never counted).
- `waitbus serve` supervises the broadcast daemon plus the configured source
  watchers in one foreground process: a daemon crash or a startup failure
  exits 1, and the docker watcher stops gracefully at shutdown.

### Install

```
pip install waitbus
```

[0.1.0]: https://github.com/astrogilda/waitbus/releases/tag/v0.1.0
