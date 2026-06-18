# waitbus agent messaging -- request/reply over the bus

Agent messaging lets one agent send a message *to* another named agent and
block for its reply, layered entirely on the broadcast bus. It is the
Enterprise-Integration **Correlation Identifier + Return Address** pattern --
the same shape NATS request-reply, RabbitMQ direct-reply-to, and MQTT 5
response-topic realize -- composed from the existing public `emit` +
`wait_for` primitives. There is **no** server-side routing, **no** durable
mailbox, and **no** callback registry: a request is one `agent_message`
addressed to a recipient, a reply is another `agent_message` echoing the
request's correlation id back. Identity is a self-asserted agent name (an
address, not a credential), sound under the same-UID workstation trust model.

This is the Python-SDK companion to the MCP `emit_agent_message` /
`read_agent_messages` tools (see [MCP integration](ARCHITECTURE.md#mcp-integration)
and the server's own `instructions`); both write the same `agent_message`
event facet, so an SDK agent and an MCP agent coordinate on one bus.

---

## Quickstart

`request` / `respond` are re-exported at the package root:

```python
from waitbus import request, respond
```

### Request / reply

A responder agent (`agent_b`) reads its inbox and answers; a requester
(`agent_a`) sends one message and blocks for the correlated reply. Both run
against the same local daemon.

```python
from waitbus import request, respond, wait_for

# --- agent_b (responder), e.g. in its own process / thread ---
msg = wait_for(to="agent_b", source="agent", timeout=5.0)
if msg is not None:
    respond(msg, '{"answer": 42}')          # sender defaults to msg's recipient

# --- agent_a (requester) ---
reply = request("agent_b", '{"ask": "meaning"}', sender="agent_a", timeout=5.0)
if reply is None:
    ...                                      # timed out; nobody answered
else:
    assert reply.fields["msg_from"] == "agent_b"
    assert reply.fields["msg_to"] == "agent_a"
```

`request` returns the reply `EventFrame`, or `None` on timeout / connection
close. `body` is an opaque string (JSON by convention); waitbus never parses it.

### Inbox stream

`subscribe(to=...)` / `wait_for(to=...)` filter the bus down to messages
addressed to one recipient -- a recipient inbox over the wire:

```python
from waitbus import subscribe

for msg in subscribe(to="agent_b", source="agent"):
    sender = msg.fields["msg_from"]
    body = msg.fields["msg_body"]      # the message content, carried on the wire
    handle(sender, body)
```

The `to=` filter is a predicate over the wire `fields.msg_to`; a non-addressed
event (one with `msg_to` NULL) never matches an inbox filter.

---

## Envelope

The agent-message facet is six nullable typed columns on the `events` table,
projected into the broadcast wire `EventFrame.fields` exactly like the
`alert_*` facet -- so each is read (and `msg_to` is predicate-matchable) off
the wire as `fields.msg_*`. (A bare-`payload_json` convention could not do
this: `_row_to_frame` drops `payload_json` from the wire, so a body or address
in the payload would never reach the recipient.) The `msg_` prefix is
load-bearing because bare `to` / `from` are SQL reserved words.

| Field | Semantics |
|---|---|
| `msg_to` | Recipient agent name -- the inbox address. `subscribe(to=...)` / `wait_for(to=...)` filter on this. |
| `msg_from` | Sender agent name. `respond()` addresses the reply back to this. |
| `msg_correlation_id` | Pairs a reply to its request. `respond()` copies it verbatim; `request()` waits on it. A fresh ULID per request by default. |
| `msg_reply_to` | The requester's unique return address (defaults to `<sender>.<correlation_id>`). **Informational in this SDK:** the reply is matched by `msg_correlation_id`, not routed on `msg_reply_to` -- the daemon broadcasts every frame and the inbox filter is client-side, so a return address cannot create point-to-point delivery here. Kept as staged surface for a future per-subscriber server-side filter. |
| `msg_thread` | Optional conversation-grouping key for multi-message exchanges (the A2A `contextId` shape). `request(thread=...)` sets it; `respond()` echoes the request's value by default (pass `thread=` to override). |
| `msg_body` | The message content itself, carried on the wire (the lean frame drops `payload_json`, so the body rides here). Subject to the 64 KiB frame cap. |

Carrier facts: `agent_message` is one of three `event_type` values owned by
the first-class built-in `agent` source (alongside the `agent_claim` and
`agent_task_failed` coordination broadcasts). Addressed rows carry synthetic
`owner="local"` / `repo="agents"` labels (owner/repo are NOT NULL CI-era
columns); all routing lives in the `msg_*` fields, never in owner/repo.

### Race-free requests

`request()` needs **no** subscribe-before-send handshake. Correctness rests on
the `msg_correlation_id` + recipient match, **not** on event ordering. The
daemon assigns every row a monotonic sequence in commit order, so a reply
(which the responder can only emit after receiving the request) has a strictly
greater sequence; the `since=<request event_id>` replay is translated
daemon-side to that request's exact sequence lower bound, so the reply is
caught even if it lands before the wait begins -- and the guarantee holds
**across processes** because the sequence is the single writer's order, not the
per-process ULID clock. Even if that translation ever missed, the
`msg_correlation_id` filter means a stale or duplicate frame can never be
mistaken for this request's reply.

An oversize reply (a body beyond the 64 KiB wire frame cap) arrives as a
truncated stub carrying the correlation id; `request()` matches it and
re-fetches the full body from the event store, so a large reply is delivered
rather than silently timing out.

---

## Pitfalls

- **Use a unique `reply_to` per requestor.** The default
  (`<sender>.<correlation_id>`) is already unique. Reusing one shared reply
  address across many requestors turns a reply into an O(N) fan-out that wakes
  every listener on that address -- the request/reply amplification the
  broadcast model invites. Keep the default unless you have a reason not to.
- **The timeout is requestor-owned.** A reply that arrives after the
  requester gave up (or never arrives) is an orphaned event on the bus -- the
  bus will not redeliver or expire it for you. Always pass a `timeout`; treat
  `None` (returned on timeout) as "no answer" and move on.
- **Handle replies idempotently.** There is no per-message ack or
  exactly-once delivery. A correlation id can in principle be matched more
  than once across replay windows; make reply handling safe to run twice.
- **Override `correlation_id` only with a unique value.** The default is a
  fresh ULID, always unique. If you pass your own `correlation_id`, it MUST be
  unique per in-flight request from a sender -- a collision with a concurrent
  or replayed message to the same sender can cross-match, since the reply wait
  returns the first frame carrying that correlation id.
- **Mind the fan-out ceiling.** The daemon writes to each subscriber socket
  in a sequential loop, so sustained throughput degrades past roughly
  **16 concurrent subscribers**. Fine for ~10 bursty coordinating agents; for
  larger swarms this is the load-bearing limit, not agent messaging itself.

---

## Identity and trust model

An agent name is a **self-asserted address, not a credential**. There is no
PKI, CA, or OAuth, by design. The kernel UID boundary IS the trust boundary:
the broadcast socket already enforces a `SO_PEERCRED` UID gate
([CONSUMER_API §3](CONSUMER_API.md)), so every peer on the bus runs as the same
UID. A same-UID peer that could spoof an agent name can already read every
peer's socket, keys, and memory -- so cryptographic agent-auth would raise no
ceiling the UID boundary has not already set. This is the same stance MCP's
STDIO transport and the Akka / Erlang actor runtimes take: local names are
addresses, not credentials. See [SECURITY.md](../SECURITY.md) for the full
same-UID threat model, including the inter-agent forge note.

A hardening ladder exists but is **future-only**, warranted solely if a
concrete intra-UID spoofing threat is ever named -- not pre-emptively:

1. Pre-shared HMAC + nonce on the envelope.
2. An EdDSA-signed envelope.
3. A capability-token scheme.

Until such a threat is named, the self-asserted name is the contract.

---

## What this is NOT

- **Not server-routed unicast.** The daemon does no routing; a message is
  broadcast to every subscriber and the `to=` filter selects the inbox
  client-side. Addressing is a convention over fan-out, not a delivery target.
- **Not a durable mailbox.** There is no per-recipient queue, no redelivery,
  no dead-letter, no offline accumulation beyond what the shared replay log +
  `since=` cursor already provide. A reply nobody is waiting for is just an
  event on the log.
- **Not cross-machine.** Agent messaging is single-machine, single-UID,
  exactly like the rest of the local core. Crossing machines or teams is
  explicitly out of scope here.

---

## Related documents

- [`CONSUMER_API.md`](CONSUMER_API.md) -- the broadcast wire frame, the
  `events` schema, and the subscribe/filter contracts this builds on.
- [`ARCHITECTURE.md`](ARCHITECTURE.md) -- the daemon, the broadcast bus, and
  the MCP surface that exposes the same agent-message facet to MCP clients.
- [`../SECURITY.md`](../SECURITY.md) -- the same-UID trust model and the
  inter-agent forge note that bounds this SDK.
