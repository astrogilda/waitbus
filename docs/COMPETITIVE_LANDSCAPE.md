# Competitive landscape — where waitbus fits, and where it does not

*(Data as of 2026-05-28; star counts and spec versions may change.)*

## The one-paragraph map

waitbus is a **single-machine, cross-harness, multi-source status bus** with a
dumb `wait`/`emit` primitive — the local opt-in event-broadcast model the OS
already ships (D-Bus signals, inotify, journald), with a durable replay log and
a wait predicate added. Its technical scope is narrow and specific:
**normalizing events the agent did not initiate — a webhook, a CI run, a file
change, a container exit — into one wait-predicate surface on a single
peer-credential-gated machine, and fanning them (plus any agent's own `emit()`)
out to every local subscriber, so a finish or a failure reaches every tool on
the box at once.** Everything outside that corner belongs to a
neighbour.

## vs. MCP Tasks (SEP-1686) — the protocol, not a product

MCP Tasks shipped experimental in the 2025-11-25 MCP spec. It is a
**requestor-driven, call-now/fetch-later** primitive: a client augments a
request *it makes* with a task, gets back a receiver-generated `taskId`, and
**polls** `tasks/get` for the result. The optional `notifications/tasks/status`
push exists, but the spec is explicit that requestors must not rely on it and
should keep polling. FastMCP exposes it as `task=True` on an async decorator.

What this means for waitbus, source by source:

- **Agent-initiated local work (pytest, docker the agent kicked off): absorbable.**
  If your agent runs the tool through MCP, `task=True` is one decorator and needs
  no daemon. These are waitbus's *most* substitutable sources.
- **External / OS events the agent did not initiate (a pushed-branch CI run, an
  inbound webhook, a filesystem change, a container that exited on its own):
  not expressible as a Task.** There is no originating request, so there is no
  `taskId`. Server-initiated push ("Triggers and Event-Driven Updates") is no
  longer only a roadmap line: a **Triggers & Events Working Group** is now
  chartered and incubating (`modelcontextprotocol/experimental-ext-triggers-events`).
  While MCP server-initiated push triggers are being incubated (the Triggers & Events Working Group), waitbus is designed to consume such a trigger as just another input source when it ships.
- **Multi-source conjunction** ("wake when pytest passes *and* a file changed")
  is not a Tasks concept.

For agent-started work, `task=True` is often the right tool. For events the
agent did not initiate, Tasks has no answer — the gap waitbus fills.

## vs. local agent-to-agent buses (different data model)

- **agent-message-queue (AMQ).** A file-based (Maildir-style), MIT, *addressed*
  messaging bus: `--to`, threads, replies, handoff state — "the conversation
  between agents." waitbus has no `to:`/reply/thread model; it is broadcast
  source-ingestion. Low functional overlap. If you want delegation and
  handoff, AMQ's model is the right shape, and waitbus could feed events into it.
- **claude-code-inter-session.** Same-machine agent-to-agent messaging. Overlaps
  only waitbus's *status-broadcast* slice (one agent emits, others wake); waitbus
  adds the source-ingestion-and-normalization layer that inter-session does not
  have.
- **agent-event-bus.** The closest analog — a broadcast pub/sub MCP server with
  optional webhook push — but at an early adoption stage. waitbus differs in being
  a peer-cred-gated local daemon with multi-source normalization and a
  fault-injected longevity record rather than an HTTP MCP server.

## vs. orchestration platforms (different scale)

**Ruflo** (formerly claude-flow) and similar are heavyweight, cross-machine
agent-orchestration platforms — routers, swarms, shared vector memory,
federation. That is a different category and a different scale; waitbus is the
opposite end (one machine, one primitive) and could be a *source* feeding such
an orchestrator, not a competitor to it. Cross-machine relay is out of scope for the waitbus core (one machine, one
primitive).

## Kernel-enforced local trust boundary

The one thing none of the above can copy on waitbus's own axis: a Unix-domain
socket delivers the connecting peer's UID via `SO_PEERCRED` as a **kernel
fact**, at `accept()`, with zero userspace participation. TCP/TLS — what every
hosted relay and every Streamable-HTTP MCP server runs on — can only present a
token or certificate. "This byte came from UID 1000 on this machine" is not
reproducible across a network boundary. For a bus gating on machine-local OS
events, that is structural, not a feature.

## The precedent: this is the OS model, not a new invention

waitbus did not invent local event broadcast. Unsolicited delivery of local
events to opt-in, server-side-filtered subscribers is the explicit design of
D-Bus signals (a client installs a match rule; the bus delivers only matching
signals, and waking idle clients without a match is exactly what the design
avoids), and it recurs by deliberate design across every major local OS event
facility: Linux inotify, netlink/udev multicast, journald's change
notification, macOS FSEvents, and Windows ETW. That is roughly fifteen years of
shipped, foundational IPC. waitbus is "D-Bus for agents," and what it adds on top is a durable replay log
(`since=`), cross-harness normalization (CI / webhook / container / file / agent
into one surface), the wait-predicate primitive, zero-config, and an
agent-facing API. The seam: waitbus filters per-subscriber on the client
side at a bounded (~16-subscriber, single-workstation) scale, not with
D-Bus-grade server-side match rules -- appropriate for one machine, and not
positioned as a scalable or multi-tenant fabric.

## Out of scope

waitbus does **not** claim:

- that MCP clients auto-handle Tasks without polling (they poll; the status push
  is non-authoritative);
- a specific ship date for any vendor's agent-teams feature not verified against
  a primary source;
- that MCP or A2A deliberately avoid unsolicited local broadcast because it is
  harmful. The accurate statement is that it is **absent today and now moving**: MCP is
  request-response (server-push to idle agents was requested and closed
  not-planned, and event-driven push is now in a chartered Triggers & Events
  Working Group — as a per-server callback, not a local cross-harness bus), and
  A2A's push is task-scoped to client-initiated tasks. That is absence by scope and architecture, not a harm
  verdict;
- that waitbus invented local broadcast, offers a novel security model (it is the
  D-Bus session-bus model -- same-UID `SO_PEERCRED`, opt-in subscription), or is
  a scalable / multi-tenant broadcast fabric (sequential fan-out is bounded
  at roughly sixteen local subscribers).
