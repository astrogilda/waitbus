# Changelog

All notable changes to **waitbus** are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning

Pre-1.0 releases may refine the API based on real-world usage; v1.0 will
declare API stability after a period of stable public use.

## [0.2.1](https://github.com/astrogilda/waitbus/compare/v0.2.0...v0.2.1) (2026-07-18)


### Bug Fixes

* **build:** relax the uv pin to a floor so uv run works on current uv ([590d55f](https://github.com/astrogilda/waitbus/commit/590d55f61a76cf047356ebc452d6dfea6c4f8649))
* **ci:** close the pedantic-persona zizmor findings ([ef30d75](https://github.com/astrogilda/waitbus/commit/ef30d75f0f8f07b5be271b7d44e292e44345be63))
* **ci:** close the zizmor workflow audit and pin the uv version ([fc5da59](https://github.com/astrogilda/waitbus/commit/fc5da593d45252b4c3585991a12195ada091f02c))
* **ci:** keep the nightly bench in budget and always upload results ([f5fbc3b](https://github.com/astrogilda/waitbus/commit/f5fbc3b65b3c3aea3abe86c2ead9c0033e4aaf0c))
* **ci:** pin uv to 0.10.8 consistently across CI and the resolver gate ([3dfaccb](https://github.com/astrogilda/waitbus/commit/3dfaccb7d7b8fe7890a3a1cfe2000ca6709584ca))
* **demo:** refuse to render when waitbus is not on PATH ([bc27ea3](https://github.com/astrogilda/waitbus/commit/bc27ea3dab2b2efdd14df579d04d432283f7d76c))
* **deps:** bump mcp and setuptools to clear the OSV advisories ([447b64e](https://github.com/astrogilda/waitbus/commit/447b64e34afb70a2987c66b3103f10684e68e4a1))
* **release:** bump CITATION.cff and guard it against version drift ([271af7a](https://github.com/astrogilda/waitbus/commit/271af7a8e3ca88eeabb5302af33eabab2fc55ecd))


### Documentation

* **examples:** correct the hero_swarm agent-source description ([a4e9634](https://github.com/astrogilda/waitbus/commit/a4e96343d3ea0fcc4067bd17cf960173b0d9cf53))
* **readme:** lead with the working proof and fix the reviewer paths ([194d75c](https://github.com/astrogilda/waitbus/commit/194d75c004c6b22c519db78db8c9c24f0a2651b3))
* tighten the demo captions to plain punctuation ([002c65d](https://github.com/astrogilda/waitbus/commit/002c65d4d7a84fc8b0ffd97dfc09c937259f4dc3))

## [0.2.0](https://github.com/astrogilda/waitbus/compare/waitbus-v0.1.6...waitbus-v0.2.0) (2026-06-24)


### Features

* **mcp:** agent-to-agent messaging over MCP ([6e49efb](https://github.com/astrogilda/waitbus/commit/6e49efb47e00275c290573d503b212a593c4f618))
* **mcp:** orient agents with server-level instructions ([a45cfe6](https://github.com/astrogilda/waitbus/commit/a45cfe6e7c2cca66093811888b2f13c0c220588b))
* **mcp:** refine the MCP surface for tool-biased clients ([bdbc405](https://github.com/astrogilda/waitbus/commit/bdbc4056cd180b93730c41e1716cb3c3e815152d))


### Bug Fixes

* **api:** correct package docstring and expose __version__ ([caf99c8](https://github.com/astrogilda/waitbus/commit/caf99c86e0c38293b757d6e15a84a2151f4c74b2))
* **install:** keep the daemons runnable under executable-stack interpreters ([f3d35d0](https://github.com/astrogilda/waitbus/commit/f3d35d043a572a863d824bc3e6bb8aa3691ee2d4))
* **mcp:** conform the tool and notification surface to strict MCP clients ([3c29e19](https://github.com/astrogilda/waitbus/commit/3c29e195815fbb42118f1c92444fcfbd1f3a04de))


### Documentation

* add AGENTS.md, llms.txt, and an agent doc-QA loop ([b1d077d](https://github.com/astrogilda/waitbus/commit/b1d077daa2b3e1995e69324db661456e7460ea01))
* add Context7/DeepWiki indexing and copy-edit prose ([d45e365](https://github.com/astrogilda/waitbus/commit/d45e3652f21f1c7e53b34ed64537284e518ed78c))
* **code:** repoint stale doc pointers and add usage examples ([a69f646](https://github.com/astrogilda/waitbus/commit/a69f646873a60403719a668bcf4c27cf7cd33190))
* **examples:** correct the waitbus repository URL in example READMEs ([b865c7c](https://github.com/astrogilda/waitbus/commit/b865c7c8ef874004a15f6a988d400eef85781504))
* **mcp:** describe output schema fields and add input examples ([0a170db](https://github.com/astrogilda/waitbus/commit/0a170dbb646de2a4519af4c56ddfbf1ce920998e))
* **messaging:** add public agent-messaging guide and examples ([77e8e31](https://github.com/astrogilda/waitbus/commit/77e8e3153b195b1acb71eeed7dd49de90b908d2e))
* **readme:** point at the live MCP specification URL ([bb5a897](https://github.com/astrogilda/waitbus/commit/bb5a89710656bca46f1a50a87ac88a8bfec7c7c8))
* **skill:** link the agent-messaging guide by relative path ([68d3420](https://github.com/astrogilda/waitbus/commit/68d34205dd499c227641677a5f7900c09f10d7d8))
* tighten agent-facing wording and headings ([eddb283](https://github.com/astrogilda/waitbus/commit/eddb283e2c8cb50c399a6bc2c2e557eb6f26c369))

## [0.1.6]

### Changed

- The MCP tool surface is refined for tool-biased clients. The three CI read
  tools (`get_ci_status`, `list_failed_jobs`, `get_pr_aggregate`) are replaced
  by a single `query_ci` tool with a required `view` selector
  (`status`, `failed_jobs`, or `pr_aggregate`) and the same per-view
  parameters. As a 0.x beta this is an intentional surface change; a client
  that called the old tools now passes the matching `view` to `query_ci`.
- The agent-to-agent messaging tools (`emit_agent_message`,
  `read_agent_messages`) are now gated behind a `waitbus mcp serve`
  `--enable-agent-messaging` flag. The flag defaults on, so existing setups
  are unchanged; pass `--no-enable-agent-messaging` to hide the facet. Both
  tool descriptions now state explicitly that they are for agent-to-agent
  messages only, never for querying CI status or events.

### Added

- A `get_event` read tool that fetches one stored event by its ULID, giving a
  tool-only client parity with the `waitbus://event/{ulid}` resource (events
  were previously resource-only). Oversize payloads return the same truncation
  marker with a `raw_uri` pointer to `waitbus://event/{ulid}/raw`.

### Security

- Attacker-controllable event fields returned by `get_event`, `tail_events`,
  and `query_ci` (PR titles, commit messages, workflow and job names) are now
  wrapped in explicit `<external_event_data>` delimiters so a consuming model
  treats them as inert external data rather than instructions. The existing
  control-character sanitisation still applies; waitbus-controlled metadata
  (ids, enums, repo slug) is left unwrapped.

## [0.1.5]

### Added

- Agent-to-agent messaging over MCP. A new `emit_agent_message` tool sends a
  message to a named agent (or `*` for everyone), and the recipient reads it
  with the cursor-paginated `read_agent_messages` tool. A `waitbus://agent/{name}`
  resource acts as a doorbell that pings when a message arrives, so the agent
  reads only when there is something to read. Messages share the event store but
  stay out of the default `tail_events` and `waitbus://current` views; `tail_events`
  gains an `event_types` filter so a client can opt into them. Agent identities are
  cooperative and self-asserted. The bus is single-user and same-UID, so this is
  not a security boundary.

## [0.1.4]

### Fixed

- MCP tool `outputSchema`s are now top-level object-type JSON Schemas instead of
  bare `$ref` wrappers. The Python SDK resolved the `$ref` transparently, but the
  official TypeScript SDK validates the schema strictly, so MCP clients built on it
  (the MCP Inspector, and likely Claude Desktop) rejected `tools/list`. The schemas
  now pass strict cross-implementation validation.
- The MCP server no longer sends `notifications/resources/updated` for the
  non-subscribable `waitbus://event/{id}` URI. That notification is reserved for
  resources the client has explicitly subscribed to; the unsolicited push could
  trip a strict client's protocol checks.
- Server-to-client notifications are held until the client completes the initialize
  handshake (`notifications/initialized`) rather than being flushed when the session
  opens.
- The `notifications/claude/channel` extension is sent only to clients that
  negotiated the `claude/channel` experimental capability, so a non-Claude client is
  never sent the vendor notification.

### Added

- The four read tools carry `readOnlyHint` annotations (the three point reads also
  carry `idempotentHint`), so MCP clients can recognize them as safe, side-effect-free
  calls.

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

## [0.1.0]: first public release

waitbus is a workstation-local async event bus for your machine's agents:
wait on anything from any source, and let your agents coordinate over the
same local bus. It is local-only (an `AF_UNIX` socket, no network egress),
durable (events persist to SQLite with replay), and zero-polling (an
`eventfd` doorbell wakes subscribers instead of a clock loop).

### Sources: wait on what already finished or failed

- **CI jobs**: a GitHub workflow run finishing or failing.
- **Test runs**: a pytest run passing or failing.
- **Containers**: docker container lifecycle events.
- **Filesystem**: file and directory changes.
- Third-party sources register through the `waitbus.sources.v1` entry-point group.

### Subscribers: any agent or script can wait and react

- **Agent frameworks**: Pydantic AI and LangGraph agents subscribe and react over the public SDK.
- **Claude Code**: receives pushes over the MCP notification channel.
- **Any MCP client**: pulls events via tool calls or a `tail_events` long-poll.
- **Scripts / CLI**: `waitbus wait`, plus hand-decoding subscriber snippets in Python, Go, Rust, and TypeScript.

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
  sits at a frame boundary, or a clean EOF otherwise, never a torn frame.
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
