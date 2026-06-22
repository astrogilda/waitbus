# waitbus Consumer API: stable public contracts

This document specifies the wire-level and storage-level contracts a
third-party consumer may depend on. Everything described here is a
**stable public contract**: it will not change in a backward-incompatible
way without a major-version bump and a migration note in CHANGELOG.md.

Anything not listed here (daemon-internal module layout, SQLite rowid
ordering, log line formats, metric label sets) is **not** a contract and
may change in any release.

All file:line references were verified against the source at the time of
writing; they are navigation aids, not part of the contract.

---

## 1. Broadcast wire frame (STABLE)

The broadcast daemon binds an `AF_UNIX` `SOCK_STREAM` socket (mode
`0600`) under the runtime directory. Every payload in both directions is
length-prefix framed:

```
[4 bytes big-endian uint32 length N] [N bytes UTF-8 JSON body]
```

- Length prefix is `struct.Struct(">I")`, big-endian unsigned 32-bit
  (`waitbus/_frame.py::_LENGTH_STRUCT`).
- `0 < N <= 65536`. `MAX_FRAME_BYTES = 65_536` (64 KiB) is enforced
  producer-side in `waitbus/_frame.py::encode_frame`. A consumer MUST reject a length prefix of `0` or one greater
  than 65536 rather than attempt to read it.
- The body is always a single UTF-8 JSON object.

This framing is identical on Linux and macOS.

## 2. Subscribe frame (STABLE)

The first frame a subscriber sends after connecting is a subscribe
request:

```json
{
  "proto": 1,
  "filters": ["owner/repo", "owner/*", "*"],
  "event_types": ["workflow_run", "workflow_job",
                  "prometheus_alert", "prometheus_watchdog"],
  "since": "01HZXXXXXXXXXXXXXXXXXXXXXX",
  "envelope": "diffs"
}
```

Field contracts (validators in `waitbus/broadcast.py`):

- **`proto`**: wire-protocol version the subscriber speaks. A
  connection-scoped integer negotiated once at subscribe time (NATS
  `INFO`/`CONNECT`, MCP `initialize`, RESP3 `HELLO` precedent), **not** a
  per-frame discriminator. The only supported value today is `1`. An
  unsupported value is terminally rejected with a
  `subscribe_rejected{reason:"version", supported:[1]}` frame (see §3).
  Versioning lives here, on the connection handshake, so no individual
  fan-out frame carries a `specversion` tax. Omitting `proto` is treated
  as the implicit v1 contract for backward compatibility with subscribe
  frames produced before this field existed.
- **`filters`**: list of match patterns. Each pattern is validated
  against the anchored regex
  `^([A-Za-z0-9_.-]+/([A-Za-z0-9_.-]+|\*)|\*)$`
  (`waitbus/broadcast.py::FILTER_RE`). Three shapes are accepted:
  - `owner/repo`: exact repository
  - `owner/*`: every repository under an owner
  - `*`: every event (global). This is a bare `*`, **not**
    `*/*`.
  No shell-metacharacter surface; anything else is rejected.
- **`event_types`**: optional list restricting delivery to those
  event types (`waitbus/broadcast.py::_validate_subscribe_event_types`).
  Omit for all supported types.
  The supported set covers the GitHub stream (`workflow_run`,
  `workflow_job`), the Prometheus/Alertmanager stream
  (`prometheus_alert`, `prometheus_watchdog`), AND the in-tree local
  sources (`docker_container`, `fs_change`, `pytest_session`) so the
  default subscriber receives every event the bus ingests, matching
  the universal-ingress contract on the read path. A subscriber that
  wants a narrower set (e.g. only GitHub) passes `event_types=[...]`
  explicitly.
- **`since`**: optional resumable cursor. A 26-character Crockford
  base32 ULID matching `^[0-9A-HJKMNP-TV-Z]{26}$`
  (validated by `waitbus/broadcast.py::_validate_since_cursor`).
  The daemon replays persisted
  events strictly after this ULID before switching to live delivery.
- **`envelope`**: optional delivery-mode selector. Absent or
  `"diffs"` (the only allow-listed value today) selects the faithful
  per-event tail every subscribe frame produced before this field
  existed. **`"upsert"` is a reserved name** for a future server-side
  latest-per-entity projection over the same wire (Materialize
  ``ENVELOPE UPSERT`` shape); the daemon **rejects it today** so a
  forward-looking consumer cannot silently subscribe to an
  unimplemented mode. Any other string is also rejected. Validator:
  `_validate_subscribe_envelope` in `waitbus/broadcast.py`.
  When the upsert mode ships in a future release it will be a new
  stable contract row in §6 alongside the existing client-side
  coalesce mode, NOT a breaking change to the diffs default. Upsert
  frames will carry the same `kind`/`proto` discriminator pair as the
  diffs wire, so shipping the mode is purely additive: an existing v1
  subscriber that never requests `"envelope":"upsert"` sees no wire
  change.

## 2a. Daemon→subscriber frame catalogue (STABLE)

Every frame the daemon sends is a flat JSON object dispatched on its
string `kind` field. `kind` is the control-vs-data discriminator: a
consumer reads `kind` first and routes the frame accordingly. The
wire is deliberately an **open** string discriminator (no closed tagged
union), but the additivity rule is **asymmetric**:

- **New _control_ kinds are non-breaking additive.** A control frame
  carries no `event_id` and has no resume-cursor implication, so the
  four hand-decoding language clients
  (`docs/snippets/minimal_subscriber.{py,go,rs,ts}`) can safely ignore
  an unrecognised control `kind` and the consumer's `since` cursor is
  unaffected. No `proto` bump is required.
- **New _data_ kinds (carrying `event_id`) are a breaking change and
  require a `proto` bump** (see §2). A consumer that silently ignored an
  unknown data frame would skip the underlying event's `event_id`,
  desync the `since` resume cursor on resumption, and effectively drop
  the event with no signal, the opposite of "safely ignore." The two
  data kinds today (`event`, `truncated`) are pinned to `proto=1`;
  adding a third requires `proto=2` and a parallel update to the
  hand-decoding snippets.

Two frame kinds carry **data** (an event identity under `event_id`);
three are **control** frames carrying no event identity.

| `kind` | Axis | Carries `event_id`? | Meaning |
|---|---|---|---|
| `event` | data | Yes | A delivered event. |
| `truncated` | data | Yes | Stub standing in for an oversize event (re-fetch by `event_id`). |
| `daemon_heartbeat` | control | No | Liveness tick. |
| `subscribe_ack` | control | No | Positive registration signal (emitted once). |
| `subscribe_rejected` | control (terminal) | No | Subscribe was refused; connection closes. |

### `event` frame (data)

```json
{
  "kind": "event",
  "event_id": "01HZXXXXXXXXXXXXXXXXXXXXXX",
  "event_type": "workflow_run",
  "owner": "octocat",
  "repo": "hello-world",
  "received_at": 1716500000000000000,
  "delivery_id": "<upstream delivery id>",
  "summary": "<short human-readable line>",
  "fields": { }
}
```

- `kind` is **always** the literal string `"event"`; it is never the
  event class. The event class is `event_type` (`workflow_run`,
  `workflow_job`, `prometheus_alert`, `prometheus_watchdog`, the local
  `docker_container` / `fs_change` / `pytest_session`, or any plugin
  value). A consumer dispatches data-vs-control on `kind` and then
  branches on `event_type`.
- `event_id` is the wire identity and the resume cursor (a ULID; same
  value as the `event_id` schema column of §4, same value passed back
  as `since`).
- `received_at` is epoch **nanoseconds** (matching §4).
- `delivery_id` is the upstream-correlation / dedup key (§4).
- `summary` is a short projection line; `fields` is the structured
  projection object. The lean wire deliberately does **not** ship the
  raw `payload_json`; re-fetch the full row by `event_id` via the
  SQL/CLI surface (§4) when the projection is not enough.
- The agent-message facet projects into `fields` as `msg_to`, `msg_from`,
  `msg_correlation_id`, `msg_reply_to`, `msg_thread`, and `msg_body` (all
  nullable; non-null only on `agent` / `agent_message` frames). A recipient
  or correlation filter is therefore predicate-matchable on the wire (e.g.
  `fields.msg_to="agent_b"`), and the message content rides `msg_body` (the
  lean wire drops `payload_json`). These are additive `fields` keys on the
  existing `event` data-kind (no `proto` bump).

### `truncated` frame (data stub)

```json
{
  "kind": "truncated",
  "event_id": "01HZXXXXXXXXXXXXXXXXXXXXXX",
  "reason": "<why the event was truncated>",
  "max_frame_bytes": 65536,
  "correlation_id": null
}
```

Emitted in place of an `event` frame whose encoded body would exceed
`max_frame_bytes` (§1). It carries the same `event_id` as the event it
stands in for, so the consumer re-fetches the full row by `event_id`
via the SQL/CLI surface (§4). `reason` is a human-readable hint;
`max_frame_bytes` echoes the producer-side cap. `correlation_id` is
`null` for an ordinary oversize event; for an oversize *addressed agent
reply* it echoes the request's correlation id, so the requester's
`request()` matches the stub and re-fetches the full body.

### `daemon_heartbeat` frame (control)

```json
{
  "kind": "daemon_heartbeat",
  "ts": 1716500000,
  "uptime_sec": 3600
}
```

A control liveness tick with no event identity, sent every
`heartbeat_sec` seconds (the value is reported in `subscribe_ack`) to
all subscribers regardless of filter. Heartbeats do not advance any
resume cursor.

### `subscribe_ack` frame (control)

```json
{
  "kind": "subscribe_ack",
  "proto": 1,
  "caught_up_at": "01HZXXXXXXXXXXXXXXXXXXXXXX",
  "heartbeat_sec": 60,
  "max_frame_bytes": 65536
}
```

`subscribe_ack` is the **positive registration signal** AND the
**first frame on the wire** after envelope validation. The daemon
emits it exactly once; the consumer state machine is:

```
subscribe → subscribe_ack → [optional replay frames] → live frames
```

When `since` was supplied, replay frames follow the ack on the wire;
live frames that arrived during the registration window are captured
into a server-side pre-ack buffer (bounded by `LAG_LIMIT * MAX_FRAME_BYTES`)
and drained after the replay tail. A consumer that receives
`subscribe_ack` knows it is registered for live delivery; there is no
separate probe or warm-up frame.

- `proto` echoes the negotiated wire-protocol version (always `1`
  today).
- `caught_up_at` is the `event_id` **dedup cursor** separating replay
  from live: a frame whose `event_id` is **≤** `caught_up_at` is a
  replayed historical event; a frame whose `event_id` is **>**
  `caught_up_at` is live. The cursor is a positional dedup tool, NOT
  a temporal "ack-then-live" barrier: the ack itself is structurally
  first on the wire, and consumers classify subsequent frames by
  `event_id` against `caught_up_at`. It is `null` when no `since`
  cursor was supplied (nothing was replayed, so every subsequent frame
  is live).
- `heartbeat_sec` is the interval at which `daemon_heartbeat` frames
  arrive.
- `max_frame_bytes` is the producer-side frame cap (§1), echoed so a
  consumer can size its read buffer without hardcoding the constant.

A consumer that fails to drain the wire fast enough during the
registration window (i.e., overflows the pre-ack buffer) is dropped
with `subscribe_rejected{reason: "lag_limit_exceeded"}` before the
connection closes; the same diagnostic applies when a consumer
falls behind on the live wire after registration. The lag reject
frame is best-effort and is emitted only when the wire sits at a
frame boundary (no partially-sent frame is queued for that
subscriber); with bytes still queued the daemon closes with a clean
EOF instead, so the reject can never land mid-frame and tear the
stream. A consumer MUST treat a clean EOF as an equally legitimate
lag-eviction outcome.

### `subscribe_rejected` frame (control, terminal)

```json
{
  "kind": "subscribe_rejected",
  "reason": "version",
  "remediation": "<operator hint string>",
  "supported": [1]
}
```

Terminal control frame written exactly once before the daemon closes a
rejected subscribe. `reason` is one of `"version"` or
`"lag_limit_exceeded"`. A `"version"` reject populates `"supported"`
with the list of accepted protocol versions (e.g. `[1]`); all other
rejects emit `"supported": null`. Full field semantics are in §3.

`"lag_limit_exceeded"` is the single consumer-facing reason for ALL
backpressure drops, regardless of which internal path lagged (live fan-out,
the heartbeat loop, or the pre-ack / replay drain during the
registration→ack window). The consumer's recovery is identical in every
case (reconnect with backoff, narrower filters, or a `since` cursor), so the
wire vocabulary stays minimal. The precise internal trigger (`heartbeat_lag`,
`replay_lag_limit_exceeded`, …) appears only in the daemon's structured
`subscriber_closed` log line for operators, never on the wire. The lag
reject frame itself is best-effort, frame-boundary-only (see the
`subscribe_ack` section above): an evicted subscriber with queued
unsent bytes gets a clean EOF, not a reject frame. Internal
faults such as a replay-time database error close the socket SILENTLY (no
reject frame); the consumer sees a clean EOF and reconnects.

## 3. Subscriber authentication contract (STABLE)

One gate protects the broadcast socket:

1. **Peer-credential UID check (always on).** The connecting peer's
   UID must equal the daemon's own UID (`os.getuid()`). Linux reads it
   via `SO_PEERCRED`; macOS via `getpeereid()`
   (`waitbus/_peercred.py::peer_uid`). The reject condition is
   `peer is None or peer != self.uid`
   (`waitbus/broadcast.py::_peer_uid`). A consumer running as a
   different UID cannot subscribe; this is by design (single-user
   workstation tool).

The AF_UNIX socket's kernel-attested same-UID peer credential is the
entire ingress boundary. There is no application-level subscribe token:
any process that can reach the socket is already proven same-UID, so a
bearer token would re-check an identity the kernel has already attested.
The gate is server-enforced; a consumer cannot bypass it.

### Subscribe-rejected frame (STABLE)

When the subscribe envelope's `proto` names an unsupported wire version,
the daemon writes exactly one length-prefix-framed `subscribe_rejected`
control frame back to the subscriber and then closes the connection
(clean FIN; the next read is EOF). The frame is terminal: it is the
last thing the daemon sends on that connection.

Version reject (unsupported `proto`):

```json
{
  "kind": "subscribe_rejected",
  "reason": "version",
  "remediation": "<operator hint string>",
  "supported": [1]
}
```

- `kind` is always `subscribe_rejected` and deliberately does **not**
  collide with any other frame kind (`event`, `truncated`,
  `daemon_heartbeat`, `subscribe_ack`), so a consumer can dispatch on it
  unambiguously.
- `reason` is `version` (the envelope's `proto` is not a supported wire
  version), the only reason that emits a frame at subscribe time. The
  other framed reason, `lag_limit_exceeded`, is emitted at eviction
  time, best-effort and only at a frame boundary (§2a).
- `remediation` is a non-empty human-readable hint: for `version` it
  states which protocol versions the daemon speaks.
- `supported` is present only on a `version` reject and lists the
  protocol versions the daemon accepts (`[1]` today), so a forward- or
  backward-skewed client can renegotiate without guessing.
- It is the **only** frame the daemon ever sends back on a reject. The
  version variant is emitted when the parsed envelope carries an
  unsupported `proto`. Every other request-shape reject (peer-cred UID
  mismatch, receive timeout, bad JSON, non-object envelope, bad
  filter/event_type/since) stays **silent-EOF**, no frame. A
  lag-limit eviction emits the `lag_limit_exceeded` variant
  best-effort when the wire sits at a frame boundary, and otherwise
  closes with a clean EOF (§2a).
- **Why this is safe to disclose:** the peer-credential UID gate runs
  at accept time, *before* the subscribe frame is read. A peer that
  reaches the version check has already been proven to run as the
  daemon's own UID, so the frame leaks nothing to an unauthenticated
  surface (and AF_UNIX has no network surface to begin with). The write
  is best-effort and bounded (2s) so a slow/half-dead peer cannot stall
  the daemon's accept loop.

The reference client (`waitbus/_broadcast_sub.py::open_subscriber`)
turns this frame into a typed `ProtocolVersionError` carrying the
`remediation` string; a real event delivered inside the client's
post-subscribe probe window is preserved (re-injected), never dropped.

## 4. Events table schema (STABLE)

The persisted event store is a single SQLite table `events`
(`waitbus/schema.sql`). The column set below is a stable
contract; consumers reading the DB directly (read-only) or via
`waitbus events query` / `waitbus events analyze` may depend on these
columns and types:

| Column | Type | Notes |
|---|---|---|
| `delivery_id` | TEXT PRIMARY KEY | Upstream-correlation + `INSERT OR IGNORE` dedup key |
| `source` | TEXT NOT NULL | One of the built-in canonical names (`github`, `alertmanager`, `pytest`, `docker`, `fs`, `agent`) OR a value registered by a third-party plugin against the `waitbus.sources.v1` entry-point group. See `docs/CUSTOM_SOURCES.md` for the plugin contract. Validated at construction time by `EventInsert.__post_init__` against the live registry. |
| `event_type` | TEXT NOT NULL | A built-in value (`workflow_run`, `workflow_job`, `prometheus_alert`, `prometheus_watchdog`, `pytest_session`, `docker_container`, `fs_change`, `agent_message`, `agent_claim`, `agent_task_failed`) OR a value declared in a registered plugin's `SourceSpec.event_types`. The broadcaster's default subscriber filter accepts the union via `event_types_supported()`. |
| `owner` | TEXT NOT NULL | GitHub owner, or synthetic label for non-GitHub rows |
| `repo` | TEXT NOT NULL | GitHub repo, or synthetic label |
| `run_id` | INTEGER | nullable |
| `workflow_name` | TEXT | nullable |
| `head_branch` | TEXT | nullable |
| `head_sha` | TEXT | nullable |
| `status` | TEXT | nullable |
| `conclusion` | TEXT | nullable |
| `received_at` | INTEGER NOT NULL | epoch **nanoseconds** |
| `payload_json` | TEXT NOT NULL | raw upstream payload |
| `ingest_method` | TEXT NOT NULL | `webhook`, `etag-poll`, … |
| `job_id` | INTEGER | `workflow_job` extension |
| `job_name` | TEXT | `workflow_job` extension |
| `parent_run_id` | INTEGER | `workflow_job` extension |
| `alert_name` | TEXT | prometheus extension |
| `alert_severity` | TEXT | prometheus extension |
| `alert_fingerprint` | TEXT | Alertmanager stable id across re-fires |
| `msg_to` | TEXT | addressing facet: recipient agent name (self-asserted address) |
| `msg_from` | TEXT | addressing facet: sender agent name |
| `msg_correlation_id` | TEXT | addressing facet: pairs a reply to its request |
| `msg_reply_to` | TEXT | addressing facet: unique-per-requestor return address |
| `msg_thread` | TEXT | addressing facet: conversation grouping key |
| `msg_body` | TEXT | addressing facet: the message content, carried on the wire (`payload_json` is not) |
| `event_id` | TEXT | locally-generated ULID; broadcast wire identity + resumable cursor; NULL on rows predating the column |

Contract guarantees:

- `received_at` is epoch **nanoseconds** (not seconds/millis).
- `event_id` is an opaque ULID; it is the broadcast wire identity and
  the resumable subscriber cursor. The on-the-wire field name is
  `event_id`: the `event` frame (§2a) exposes identity under the key
  `event_id`, matching this schema column exactly, and the same value is
  what a subscriber passes back as the `since` resume cursor (§2).
  Consumers MUST treat it as opaque; the daemon-internal rowid layout
  is deliberately hidden.
- `delivery_id` (not `event_id`) is the dedup key.
- Consumers opening the SQLite DB directly MUST open it read-only.
  `waitbus events query` enforces this (`file:...?mode=ro`); the DuckDB
  `waitbus events analyze` path attaches `READ_ONLY`.

Indexes are an implementation detail and **not** a contract.

## 5. Filter syntax (STABLE)

The filter language used by subscribe frames and the daemon-side
matcher (`waitbus/_broadcast_sub.py`) is exactly the three
shapes in §2: `owner/repo`, `owner/*`, `*`. The same anchored regex
governs both the wire-accepted patterns and the local matcher, so a
filter that the daemon accepts is the same filter the matcher applies.

---

## 6. Coalesced replay delivery mode (STABLE, opt-in, separate)

`waitbus replay --coalesce` is a **separate, explicitly-named delivery
mode**. It is opt-in and never the default. The faithful replay mode
(`waitbus replay`, no flag, == `--faithful`) is **unchanged** and remains
the STABLE contract of §2 / §4: every persisted frame strictly after
the cursor, in `event_id` order, byte-for-byte.

Coalesced mode is a **client-side projection**. The broadcast daemon,
the subscribe frame, the wire frame, and the SQLite log are byte-
identical across both modes; coalescing happens entirely in the
consuming process (`waitbus.coalesce.coalesce_replay`). The
immutable event log remains the single source of truth; the coalesced
view is a disposable read model derived from it. This is the CQRS
shape: one immutable log, multiple delivery modes.

**Semantics:** "A snapshot of the latest event per entity over the
replay backlog window, emitted in `event_id` order, optionally
followed by a faithful live tail."

- **Entity is per-source** (see `waitbus._terminal.entity_key`):
  - GitHub `workflow_run` → one entry per `run_id`
  - GitHub `workflow_job` → one entry per `job_id`
  - Alertmanager `prometheus_alert` → one entry per `alert_fingerprint`
  - Every other source (`prometheus_watchdog` liveness, the local
    watcher sources `pytest` / `docker` / `fs`, and any
    GitHub / Alertmanager row missing its identity column) has **no
    entity** and is delivered verbatim, uncollapsed, in `event_id`
    order (pass-through). Coalescing never silently drops a non-CI
    event.
- **Latest is decided strictly by `event_id`** (the monotonic ULID
  cursor of §4), never by arrival order or wall clock. A frame whose
  `event_id` is not greater than the retained frame for its entity is
  discarded. This makes `success → re-run → failure` resolve to
  `failure` (the higher `event_id`), never to a stale `success`.
- **Lossy by design.** Intermediate states of a collapsed entity
  (e.g. `queued`, `in_progress`) are NOT delivered in coalesced mode.
  Consumers that require every state transition (audit, compliance,
  billing, temporal analytics) MUST use the default faithful mode.
- **Ordering guarantee.** The flushed backlog snapshot is emitted in
  ascending `event_id` order (collapsed entities at the `event_id` of
  their surviving (latest) frame, interleaved with pass-through
  frames in `event_id` order). The subsequent live tail is faithful
  and in `event_id` order. The emitted stream is therefore monotonic
  in `event_id` end-to-end; a `--bookmark` cursor advances only on
  emitted (flushed / live) frames.
- **Backlog window.** Coalescing applies only to the offline replay
  backlog (everything the server replays strictly after the cursor up
  to the daemon's replay cap). Once caught up, live frames are
  faithful and uncollapsed. If the server's replay cap truncates the
  backlog, the snapshot reflects only the replayed window; the cursor
  advances to the last flushed frame so a subsequent
  `waitbus replay --coalesce` resumes correctly.
- **Default is and stays faithful.** `--coalesce` is a distinct mode
  selected per invocation; it does not modify the faithful path and is
  not a wire-level subscribe-frame field.

---

## 7. `waitbus wait` CLI contract (STABLE)

`waitbus wait` blocks until a broadcast frame matches a composed
predicate, then exits with a code carrying the verdict (coreutils
`timeout` convention). Operator-facing surface; the daemon wire is
covered in §1 to §5.

### Invocation shapes

```bash
waitbus wait --sha <SHA> [--repo owner/repo] [--timeout 30s]   # GitHub CI (sugar)
waitbus wait --source <s> --match <dotted.key>=<json_literal>... [--timeout 30s]
waitbus wait --cond <named-condition>           [--timeout 30s]
waitbus wait --match-cel '<expr>'               # extras-gated; see below
waitbus wait --match-jmespath '<expr>'          # extras-gated; see below
```

Every shape reduces internally to one AND-composed predicate against
the broadcast frame. At least one of `--sha` / `--match` / `--cond` /
`--match-cel` / `--match-jmespath` is required.

### `--sha` is sugar (STABLE)

`--sha X` is sugar for `--source github` plus a **git-style prefix match**
on `fields.head_sha`: `X` must be 7 to 40 hex characters and matches any
frame whose `head_sha` starts with it (case-insensitive), mirroring
`git show <prefix>`. GitHub stores the full 40-char SHA, so the 7-char
abbreviation you copy from a commit URL resolves. A sub-7-char or non-hex
`--sha`, or a conflicting `--source <non-github>`, is a startup error.

The generic `--match fields.head_sha=<json.dumps(X)>` path is a distinct
EXACT match (no prefixing); use it when you have the full SHA and want
exact-equality semantics. On a live stream there is no static object set
to verify prefix uniqueness against, so `--sha` resolves on the first
matching frame.

### `--match` grammar (STABLE)

`--match <dotted.key>=<json_literal>`. Repeatable. AND across distinct
keys; OR within a repeated key (Docker `--filter` precedent). The RHS
is parsed by `json.loads` so types are precise:

- `fields.head_sha="abc"` matches the string `"abc"` (not the bare word).
- `fields.run_id=12345` matches the integer `12345` (not the string `"12345"`).
- `fields.merged=true` matches the bool `True`.
- `fields.parent_run_id=null` matches an explicit JSON `null` value
  but does NOT match a missing key (distinct from the absent-key case
  by design, sentinel-guarded in `waitbus._predicate`).

Dotted keys traverse nested dicts and lists (`fields.tags.0`). A
missing key, ill-typed mid-path traversal, or RHS not equal to any
allowed value yields no match; the wait continues until a real match
or the deadline. The predicate source text length is capped at 256
bytes.

### `--cond` named conditions (STABLE)

`--cond <name>` looks up a named predicate registered via
`waitbus.register_condition(name, factory)`. Re-export available
on `waitbus` package top-level so plugin packages can register
from a stable import path. Names match `[a-z][a-z0-9_-]{0,31}`.

### `--match-cel` / `--match-jmespath` (STABLE-flag, evaluator-gated)

Layer-2 expressive predicates routed through
`waitbus.register_evaluator(name, factory)`. The flags
themselves are part of the STABLE surface; the evaluator
implementations are provided by opt-in extras packages
(`pip install waitbus[cel]` / `pip install waitbus[jmespath]`). Using
either flag without the corresponding evaluator registered raises a
startup-time error with the install hint verbatim:

```
to use --match-cel, install waitbus[cel]
```

No `[project.optional-dependencies]` is declared in the base install
today and no entry-points group is registered; the registry is the
forward-compatibility seam.

### Exit codes (STABLE)

| Code | Meaning |
|---|---|
| `0`   | Matched (any source) AND, for GitHub frames, terminal `conclusion == "success"`; for non-GitHub frames any match exits 0 |
| `1`   | Matched a GitHub frame whose conclusion is terminal `failure` / `cancelled` / `timed_out` |
| `124` | `--timeout` elapsed with no match (coreutils `timeout` convention) |
| `130` | SIGINT (Ctrl-C); clean teardown, no spurious match (`128 + SIGINT(2)`) |
| `2`   | Startup failure (daemon down, bad `--repo`, malformed `--match`, evaluator extra not installed, no predicate supplied) |

GitHub `skipped` / `neutral` / `action_required` / `stale` are
non-terminal: the wait keeps streaming. There are deliberately no
`--treat-<x>=` override knobs (closed bucketing table; add only behind
a stated real-consumer trigger).

### `--repo` (GitHub-only, STABLE)

`--repo owner/repo` scopes the daemon subscription to that repo. The
GitHub-source path keeps the existing `read_events.detect_repo()`
fallback (the wait surfaces a helpful exit-2 when invoked outside a
github.com clone with no `--repo`). For non-GitHub sources `--repo`
is ignored with a stderr `note:` (the daemon filter syntax is
GitHub-only), and `detect_repo()` is not consulted at all.

---

## 8. MCP `waitbus://event/{ulid}` resource cap (STABLE)

Reading a `waitbus://event/{ulid}` MCP resource returns the row's
`payload_json` fenced via `_untrusted.fence` (the untrusted-content hygiene
contract). The fenced bytes are capped at **64 KiB**. Over-cap reads
return a truncation marker dict in place of the fenced string:

```json
{
  "truncated": true,
  "full_size_bytes": <int>,
  "raw_uri": "waitbus://event/{ulid}/raw",
  "fenced_preview": "<first 64 KiB of fenced text>"
}
```

- `fenced_preview` is included so a tiny-task agent can skip the
  second read for the common case where the head of the payload is
  enough.
- The marker is waitbus-generated server-trusted JSON and is NOT
  wrapped in `_untrusted.fence`; fencing it would lie about
  provenance. The under-cap happy path keeps the fence unchanged.
- `waitbus://event/{ulid}/raw` returns the full fenced payload
  uncapped. It is intentionally **absent** from `resources/list` and
  `list_resource_templates`; discoverability is exclusively via the
  truncation marker, signalling explicit consent.

Cap value derivation: 64 KiB ≈ ~16k tokens (typical JSON, ~4 chars/
token). Below Cloudflare's MCP `truncate.ts` 10k-token server cap once
the waitbus envelope is added; well below the community-observed
effective 25k-token client wall on agentic MCP consumers; covers >99%
of real GitHub `workflow_run` / `workflow_job` / pytest / Docker
payloads. The 10 MiB listener ingress ceiling is a different surface
(`MAX_BODY_BYTES`, defends ingest from malicious producers); the cap
here defends consumers from oversized rows.

UTF-8 mid-codepoint truncation uses `errors="replace"` so a split
multi-byte sequence becomes U+FFFD rather than raising.

---

## Stability summary

| Contract | Symbol anchor | Stable? |
|---|---|---|
| 4-byte BE length-prefix frame | `waitbus/_frame.py::encode_frame` | Yes |
| Subscribe-frame shape + field validators (incl. `proto`) | `waitbus/broadcast.py::FILTER_RE`, `waitbus/broadcast.py::_validate_since_cursor`, `waitbus/broadcast.py::_validate_subscribe_filters`, `waitbus/broadcast.py::_validate_subscribe_event_types`, `waitbus/broadcast.py::_validate_subscribe_envelope` | Yes |
| Frame catalogue: `kind` discriminator, five frame kinds, `subscribe_ack` watermark | `waitbus/_frame.py::ALL_FRAME_KINDS`, `waitbus/_frame.py::SubscribeAckFrame` | Yes |
| Peercred UID gate (same-UID); `subscribe_rejected{version}` | `waitbus/_peercred.py::peer_uid`, `waitbus/broadcast.py::_peer_uid` | Yes |
| `events` column set + types | `waitbus/schema.sql` | Yes |
| Filter syntax (`owner/repo`, `owner/*`, `*`) | `waitbus/broadcast.py::FILTER_RE` | Yes |
| Coalesced replay mode (opt-in, client-side) | `waitbus/coalesce.py::coalesce_replay`, `waitbus/_terminal.py::entity_key` | Yes |
| `waitbus wait` invocation shapes, exit codes, `--sha` sugar, `--match` grammar, `--cond` / `--match-cel` / `--match-jmespath` flags | `waitbus/wait.py::_wait`, `waitbus/_predicate.py::Predicate` | Yes |
| MCP `waitbus://event/{ulid}` 64 KiB cap + marker + `/raw` opt-in URI (not in `resources/list`) | `waitbus/mcp.py::_get_state`, `waitbus/_mcp_subscriptions.py::parse_event_uri` | Yes |
| Module layout, rowid order, log/metric formats | n/a | **No** |

---

## Related Documents

- [`CUSTOM_SOURCES.md`](CUSTOM_SOURCES.md) -- plugin contract for third-
  party event sources (extends the `source` field surface above).
