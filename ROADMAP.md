# Roadmap

This document tracks future work for waitbus. Each item is gated on an external trigger; the project does not commit to a delivery timeline. Contributions welcome.

---

## Webhook delivery reliability

**Why:** Under some conditions, live webhook deliveries silently drop while every process is running — distinct from the boot-ordering problem where the forwarder is simply dead. Observed with `gh webhook forward` acting as a single-threaded relay: a brief stall can drop deliveries before they reach the listener. The `etag_poll` daemon (45 s cadence) may not close the gap before a PR merges.

**Instrumentation already shipped** (`/metrics` endpoint on the listener; see `waitbus/_metrics.py` and `listener.py:do_GET`): exposes
`ci_status_webhook_received_total{path}`,
`ci_status_webhook_hmac_rejected_total{path,reason}`,
`ci_status_webhook_bad_json_total{path}`,
`ci_status_db_inserted_total{event_type,source,ingest_method}`, and
`ci_status_db_dedup_ignored_total{event_type,source,ingest_method}`.
The dedup-collision class is now directly observable as a non-zero `db_dedup_ignored_total` without a matching upstream re-delivery.

**Remaining hypotheses (to verify with the counters in real traffic):**

1. **Forwarder buffer overflow under bursty deliveries.** `gh webhook forward` runs single-threaded; a brief stall could drop deliveries before they hit the listener. Counter: `received_total` lags total deliveries on the GitHub side (check via `gh api repos/.../hooks/deliveries`).
2. **HMAC verification failure on specific payloads.** Now visible as `hmac_rejected_total{reason="mismatch"}`.
3. **`etag_poll` cadence too slow for close-out window.** 45 s default leaves a window where webhooks have dropped but the poll has not run.
4. **Schema-level dedupe collision.** Now visible as `db_dedup_ignored_total` > expected redelivery count.

**What triggers it (any one suffices):**

1. Counters surface a non-zero `db_dedup_ignored_total` rate that does NOT match the expected etag-poll re-delivery rate, OR
2. Reproducible cache-vs-GitHub divergence on two or more webhook deliveries during a single CI run, OR
3. The `etag_poll` backfill fails to close the gap within 90 s after a `workflow_run.completed` event.

---

## Honker adoption when upstream production-ready

**Why:** Honker (`russellromney/honker`) is a Rust SQLite extension that implements exactly the doorbell and per-consumer-cursor pattern the waitbus broadcast daemon was designed around, with multi-language bindings (Python, Bun, Go, Node, Rust, .NET, Ruby, Elixir, JVM, Kotlin). If it reaches production maturity it is a strict superset of the planned daemon and a candidate to replace `broadcast.py`, `_doorbell.py`, and the systemd `.socket`/`.service` units.

**Why deferred:** Honker v0.2.3 publishes exactly two files on PyPI — a `cp311-macosx_11_0_arm64` wheel and a source tarball. No `manylinux*_x86_64` wheel exists for any supported Python version. Installing on a Linux x86_64 / Python 3.13 box triggers a from-source build via maturin and Cargo, which requires a Rust toolchain. Adopting Honker today would mean teaching the installer to install `rustup` and build a Rust extension on every operator machine, which violates the idempotent no-toolchain-dependency installation contract.

**Capability validated.** A build-from-source evaluation against a real waitbus database (10,098 rows) confirmed: schema cohabitation works (Honker adds 9 namespaced `_honker_*` tables; our `events` DDL is byte-identical pre/post); `tx.notify()` is atomic with our `INSERT OR IGNORE` in a single `db.transaction()` block (strictly cleaner than our planned `_doorbell.ring()`-after-commit race); cross-process notify-to-wake latency p50 = 1.07 ms / p90 = 1.33 ms / p99 = 11.63 ms (50 samples) — verifies the README's "1-2 ms median" claim and beats our planned daemon's sub-100 ms fan-out target by two orders of magnitude.

**What triggers it (ALL of the following must hold):**

1. Honker publishes a `manylinux2014_x86_64` (or `manylinux_2_28_x86_64` or newer) wheel for the operator's target Python version on PyPI.
2. The `@russellthehippo/honker-bun` npm package vendors a prebuilt `libhonker_ext.so` for Linux x86_64 (currently it ships only the TS wrapper and expects the operator to `cargo build -p honker-extension` manually).
3. The Bun binding stops requiring `libsqlite3-dev`-provided `libsqlite3.so` with `SQLITE_ENABLE_LOAD_EXTENSION`, OR documents a reliable system-package recipe that does not need sudo on a fresh operator workstation.
4. The project leaves alpha (README no longer disclaims beta-quality).

**Active monitoring probe:** Probe script: [scripts/honker-upstream-probe.sh](scripts/honker-upstream-probe.sh). An optional probe script can be run on a schedule to watch the upstream triggers.

---

## Upstream Honker cibuildwheel contribution

**Why:** the Honker adoption trigger #1 (PyPI manylinux wheel) is the single biggest blocker. The upstream maintainer ships a macOS arm64 wheel only; adding `cibuildwheel` to the project's `pyproject.toml` would self-unblock that trigger for everyone on Linux x86_64 in a single PR. The work is mechanical: add a `tool.cibuildwheel` section, add a GitHub Actions matrix step, push.

**What:** open a PR against `russellromney/honker` adding `cibuildwheel` to the build matrix. Linux x86_64 (manylinux_2_28), macOS x86_64, macOS arm64, Windows x86_64 — the standard quartet. Bundles maturin under the hood; the existing build script needs no changes.

**What triggers it:** a contributor adds cibuildwheel support upstream. This is entirely optional.

---

## NATS reconsideration (12-month cadence)

**Status:** deferred indefinitely; re-evaluate on a 12-month cadence.

**Why:** NATS Server is a long-term-canonical pub-sub broker. If it grows native AF_UNIX listener, `sd_notify`, and `SO_PEERCRED` support in a single release, the planned waitbus broadcast daemon becomes redundant.

**Why deferred:** as of the last upstream check, the three required features remain three separate unmerged items with no maintainer engagement and clear pushback against the underlying requests:

- PR `nats-io/nats-server#7800` — one participant, no review, ships `SOCK_STREAM` not `SOCK_SEQPACKET`, stale.
- Issue `#7507` (`sd_notify`) — labeled `stale`, no maintainer movement.
- Discussion `#7677` (`SO_PEERCRED`) — closed by author after maintainer pushback against adding peer-credential enforcement at the broker layer.

**What triggers it:** all three items ship in a single NATS Server release, AND `nats-py` gains stable `unix://` URL-scheme support, AND a single-node JetStream deployment for ~100 events/min idles under 50 MiB RSS (the planned stdlib daemon idles under 15 MiB).

---

## Latency-based slow-consumer detection

**Why:** the broadcast daemon currently detects slow subscribers solely by counting consecutive `EAGAIN` returns from `send()` (`LAG_LIMIT = 10`). This is the same policy systemd's varlink json-stream and NATS Core use, and it works because a saturated peer reliably surfaces `EAGAIN`. Two failure modes it does not catch:

1. A subscriber that is barely draining — enough to keep `lag_count` < 10 but lagging real-time by minutes.
2. A subscriber whose recv loop is wedged on an unrelated blocking call (file I/O, DB query) but whose recv buffer happens to drain slowly via some background path.

NATS JetStream's `ack_wait` and Google Pub/Sub's `subscription/oldest_unacked_message_age` solve this with explicit latency thresholds.

**What:** add a per-subscriber `bytes_pending_since` timestamp. Whenever `send()` succeeds and the subscriber's notional buffer occupancy drops to zero, clear the timestamp. Whenever `send()` returns `EAGAIN`, set or keep the timestamp. If `now() - bytes_pending_since > T_LATENCY_THRESHOLD_S` (default 30s), close the subscriber the same way `lag_count > LAG_LIMIT` does.

**What triggers it (any one suffices):**

1. An operator reports a subscriber that fell behind real-time by minutes without ever getting disconnected.
2. waitbus is deployed in a multi-tenant or multi-user environment, where slow consumers cannot be assumed benign.
3. A `/metrics` consumer reports `ci_status_broadcast_subscriber_lag_seconds` exceeding the threshold for any active subscriber for more than 5 minutes.

---

## Per-subscriber lag metrics

**Why:** today an operator who sees the daemon dropping subscribers in `journalctl` cannot tell from the existing `/metrics` endpoint which subscriber stalled or for how long. The webhook-reliability instrumentation covers upstream-arrival debugging but is silent on downstream fan-out health. Aeron's `AeronStat` per-publication/per-subscription position counters are the gold standard for identifying which subscriber is the bottleneck during a fan-out slowdown.

**What:** expose four broadcast-specific gauges on the existing loopback `:9000/metrics` endpoint:

- `ci_status_broadcast_subscribers_total` — gauge, current count
- `ci_status_broadcast_subscriber_lag_count{remote_uid=...}` — gauge per active subscriber, current `lag_count`
- `ci_status_broadcast_subscriber_dropped_total{reason="lag_limit"|"peer_closed"|"protocol_error"}` — counter
- `ci_status_broadcast_frames_sent_total{kind=...}` — counter, partitioned by event_type and synthetic `daemon_heartbeat`

Wires through the existing `_metrics` module so the counter plumbing is reused.

**What triggers it (any one suffices):**

1. The latency-based slow-consumer item above lands — that work benefits directly from the per-subscriber gauges.
2. An operator wants to chart fan-out health alongside ingest health on the same Prometheus dashboard.
3. waitbus is deployed in a multi-tenant or multi-user environment.

---

## Per-subscriber ring buffer evaluation

**Why:** a per-subscriber lock-free ring buffer in userspace would let the daemon absorb arbitrarily long bursts without relying on the kernel's per-socket send buffer. The current 1 MiB `SO_SNDBUF` gives ~700 frames of headroom per subscriber, which empirically clears every realistic CI burst. A userspace ring buffer matters when either subscriber count grows to 20 or more (kernel-buffer-per-socket becomes a memory problem) or burst arrival rate exceeds ~1000 events/sec (kernel allocation cost dominates).

Research across LMAX Disruptor, Aeron, and ZeroMQ ring buffer designs concluded none of them are warranted at typical waitbus scale (~10 subscribers, O(100 events/sec) burst).

**What:** prototype an asyncio-friendly bounded queue per subscriber (e.g. `collections.deque(maxlen=N)`) sitting between the daemon's broadcast pass and the actual `socket.send()`. Compare:

- Latency: p50 / p99 frame-arrival under burst vs. the kernel-only current design.
- Memory: per-subscriber RSS impact at N=500, N=2000.
- Backpressure policy: drop-newest (current effective semantics) vs. drop-oldest (subscriber sees the tail of the burst) vs. block publishers (forces synchronous fan-out).


**What triggers it (any one suffices):**

1. Subscriber count grows to 20 or more (e.g. one subscriber per editor tab, IDE, watch process, and remote pair-programming peers).
2. Peak event rate exceeds 500 events/sec.
3. A consumer reports that frames are arriving outside expected bounded latency (more than 1 second from doorbell).

---

## MCP SDK upgrade to the SEP-2575 spec

**Why:** SEP-2575 ("Make MCP Stateless") is in the MCP draft changelog as four of the five Major changes for the next revision after `2025-11-25`. Tentative cut June 2026 per the published MCP 2026 roadmap. The changes are: remove the `initialize` / `notifications/initialized` handshake, add a mandatory `server/discover` RPC, replace HTTP GET + `resources/subscribe` with `subscriptions/listen`, and remove `ping` / `logging/setLevel` / `notifications/roots/list_changed`. Per-request envelopes carry protocol version, client identity, and client capabilities in `_meta` (`io.modelcontextprotocol/protocolVersion`, `clientInfo`, `clientCapabilities`).

The official `mcp` Python SDK as of `1.27.x` is still on the `2025-11-25` spec (verified directly against the cloned SDK source: `LATEST_PROTOCOL_VERSION = "2025-11-25"`; zero `server/discover` references; `stateless_http=True` on `Server` / `FastMCP` is StreamableHTTP-transport-only, unrelated to SEP-2575's wire shape).

**What:** when the SDK ships SEP-2575 support (likely as a `2.x` cut), bump `mcp>=2,<3` in `pyproject.toml`; remove the `notifications/initialized` handler from `waitbus/mcp.py`; rewire the notification emit path to carry the new `_meta` keys; implement `server/discover` advertisement. The `notifications/claude/channel` Anthropic-private extension survives unchanged at the payload level; only the wrapping envelope changes.

**What triggers it:** the `mcp` PyPI release notes call out SEP-2575 support. The wire shape changes are not optional for spec-compliant clients; Claude Code releases shipping the new spec will silently drop notifications from servers that still emit the `2025-11-25` envelope.

**Note:** the MCP SDK integration targets the `2025-11-25` spec. The SEP-2575 transition lands as a separate upgrade on top of that baseline whenever the SDK is ready.

---

## OpenTelemetry instrumentation

**Why:** workstation-local daemons benefit from trace correlation across `listener` ingest → `_db` insert → `broadcast` fan-out → `mcp` notification emit. Today the structured logs carry no trace identifiers; an operator debugging a delayed event walks logs by `event` keys and timestamps.

**What:** two phases.

1. **Logging trace-id injection.** Adopt `opentelemetry-instrumentation-logging` against the stdlib `logging` handler the daemons already use. `LogRecord.extra` gets `trace_id` and `span_id` injected automatically for any `logger.X(...)` call inside an active span; the `structured(...)` helper in `waitbus/_log.py` then includes them in the JSON output without any code change at the call sites. Minimal change; ships independently.

2. **Prometheus + OTel metrics convergence.** `opentelemetry-exporter-prometheus.PrometheusMetricReader` registers into the existing `prometheus_client.core.REGISTRY` directly (verified in source at `opentelemetry-exporter-prometheus/__init__.py`). One `/metrics` endpoint serves both pipelines; no double-counting. Adopting OTel for metrics means defining instruments via the OTel `MeterProvider` and letting the exporter expose them through the prometheus_client registry.

If OpenTelemetry instrumentation lands, the `structlog` adoption decision becomes a re-evaluation candidate: `structlog.contextvars.merge_contextvars()` auto-injects `trace_id` and `span_id` more ergonomically than the stdlib-instrumented path, and the combined RSS / cold-start cost is justified by the integration the standalone change couldn't pay for. structlog is not used: its measured runtime cost (about +8 MiB RSS and +100 ms cold start on CPython 3.13) is not justified against the daemon's 20 MiB budget, with no JSON-shape win over the stdlib-logging JSON output already shipped.

**What triggers it:** an operator wants distributed-tracing-style correlation across the daemon stack, OR an OTel-shaped Grafana dashboard becomes the canonical monitoring surface, OR the SEP-2575 transition above lands (OTel `traceparent` / `tracestate` propagation through `notifications/claude/channel`'s `_meta` is a minor change in the same spec cycle).

---

See [`CHANGELOG.md`](CHANGELOG.md) for the prometheus_client adoption rationale.
