# Changelog

All notable changes to **waitbus** are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning

Pre-1.0 releases may refine the API based on real-world usage; v1.0 will
declare API stability after a period of stable public use.

## [0.1.3]

### Changed

- Secrets are stored in a single `0600`-mode `secrets.json` under the user
  state directory and read with a stdlib JSON parse, replacing the
  `systemd-creds` encrypted-credential backend. At-rest protection of the file
  is delegated to host full-disk encryption (FileVault / LUKS); local access is
  bounded by the file's owner-only permissions and, for the broadcast socket,
  the kernel's same-UID peer-credential check. `install-credentials` reads the
  value from `--file` or stdin and merges it with an atomic replace; the
  `--value` flag is gone because it leaked secrets into shell history.
- The webhook listener is now opt-in. A default install ships secret-free; the
  broadcast and wait paths need no secret. Staging the GitHub webhook secret
  (`install-credentials github-webhook-secret`) is what enables the listener.

### Removed

- The broadcast subscribe protocol no longer carries a bearer token. The
  `AF_UNIX` broadcast socket is already restricted to the connecting user by the
  kernel's peer-credential check, so the token guarded nothing a same-UID peer
  could not already reach. The subscribe frame's `token` field and the
  `subscribe_rejected` `reason="token"` reject are removed. This is a breaking
  wire-protocol change (acceptable pre-1.0).

## [0.1.2]

### Fixed

- The systemd daemons failed to start under an interpreter with an executable
  stack. uv and pyenv standalone Python builds ship the interpreter without a
  non-executable `GNU_STACK` header, so the kernel assumes an executable stack
  and glibc allocates writable-and-executable thread stacks; the units'
  `MemoryDenyWriteExecute` setting blocks that, so thread creation failed with
  "can't start new thread". `install-systemd` now detects an executable-stack
  interpreter and writes a drop-in that disables only that protection for the
  affected units, with a notice on how to restore it. Interpreters with a
  non-executable stack are unaffected and keep the protection.

## [0.1.1]

### Added

- MCP Registry listing: the package-ownership metadata (`mcp-name`) is now
  carried in the project README so `waitbus` can be discovered and installed
  through the Model Context Protocol registry. No code or API changes.

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
