# waitbus Grafana Dashboard — Operator Runbook

Dashboard file: `monitoring/grafana/waitbus-backpressure.json`
Dashboard UID: `waitbus-backpressure`

---

## Prerequisites

### 1. Running the listener with metrics enabled

The waitbus listener exposes Prometheus metrics at:

```
http://127.0.0.1:9000/metrics
```

This endpoint is available as long as `waitbus listener serve` is running. No
extra flag is needed — the `/metrics` route is always active on the listener's HTTP port.

The broadcast-daemon metrics (the Broadcast and Backpressure rows: subscriber
count, send-latency histogram, watermark replay, delivered/emitted counters)
come from the broadcast daemon's own opt-in loopback `/metrics` endpoint,
which is OFF by default. Enable it by setting `WAITBUS_METRICS_PORT` (or
passing `--metrics-port` to `waitbus broadcast serve`) and add a second scrape
target for that port. `waitbus_broadcast_events_delivered_total` counts EVENT
frames only, at kernel-accept; control frames (heartbeat, subscribe_ack,
subscribe_rejected) are never counted.

If the listener is managed by systemd:

```bash
systemctl --user status waitbus-listener.service
```

### 2. Prometheus scrape configuration

Add a scrape job to your Prometheus configuration:

```yaml
scrape_configs:
  - job_name: waitbus
    static_configs:
      - targets: ["127.0.0.1:9000"]
    scrape_interval: 30s
```

A 30-second scrape interval is sufficient. The listener is not a high-frequency
computation source; scrape latency is single-digit milliseconds.

Restart Prometheus after editing `prometheus.yml`. Verify the target appears green
under `Status > Targets` in the Prometheus UI.

### 3. Grafana setup

Grafana 11 or later. The dashboard uses the `prometheus` datasource type.

**Import steps:**

1. In Grafana, go to `Dashboards > Import`.
2. Upload `monitoring/grafana/waitbus-backpressure.json` or paste its contents.
3. When prompted for the `DS_PROMETHEUS` input, select your Prometheus datasource.
4. Click `Import`.

The dashboard opens with a 6-hour time window and auto-refreshes every 30 seconds.

---

## Panel Reference

The dashboard has five row groups. All counter panels use `rate(...[5m])` to show
events per second smoothed over a 5-minute window.

### Throughput row

**DB Insert Rate by Event Type**
: `rate(waitbus_db_inserted_total[5m])` split by `event_type`.
  Shows events actually committed to the events table per second.
  A flat zero after a webhook delivery means either dedup is absorbing all deliveries
  (check the Dedup-Ignored panel) or the listener is not running.

**Dedup-Ignored Rate by Event Type**
: `rate(waitbus_db_dedup_ignored_total[5m])` split by `event_type`.
  Shows deliveries dropped by `INSERT OR IGNORE` because the `delivery_id` was already
  present. A steady non-zero rate is normal (GitHub re-delivers on webhook failures).
  A rate that matches or exceeds the insert rate suggests a configuration problem
  (e.g., duplicate webhook endpoints delivering the same events).

**DB Error Rate by Source**
: `rate(waitbus_db_error_total[5m])` split by `source`.
  Any non-zero value here is a problem. Causes include: disk full, corrupted SQLite
  file, or a concurrent writer holding a lock too long. Check
  `journalctl --user -u waitbus-listener` for the full error.

### Broadcast row

**Broadcast Send Latency (p50 / p95 / p99)**
: `histogram_quantile(...)` over `waitbus_broadcast_send_seconds_bucket`.
  End-to-end producer-side time to serialise and deliver one frame to all subscribers.
  Typical idle values: p50 < 1 ms, p99 < 10 ms. Rising p99 without a corresponding
  rise in subscriber count suggests a slow subscriber blocking the fan-out loop;
  check subscriber connection count and look for subscribers that are not reading frames.

**Active Subscriber Count**
: `waitbus_subscriber_count` (Gauge, direct value).
  Current number of connected subscribers. Displayed as a stat panel with colour
  thresholds: green < 5, yellow 5–19, red >= 20. On a single workstation the
  typical value is 1–3 (read-events, pr-monitor, mcp serve). A value of 0 during
  normal operation means all consumers have disconnected; check each consumer's
  process state.

### Webhook Health row

**Webhook Received Rate by Path**
: `rate(waitbus_webhook_received_total[5m])` split by `path`.
  Deliveries arriving at `/webhook`, `/alertmanager`, and `/watchdog`. A drop to
  zero during a known CI run means GitHub is not reaching the listener — check
  ngrok / proxy / firewall configuration.

**HMAC Rejection Rate by Path and Reason**
: `rate(waitbus_webhook_hmac_rejected_total[5m])` split by `path` and `reason`.
  This is a security signal. Any non-zero rate indicates either:
  - `reason=missing`: deliveries arriving without a signature header (scraper or
    misconfigured sender).
  - `reason=mismatch`: correct header present but signature does not match (wrong
    shared secret; check `github-webhook-secret` credential).
  Alert on any sustained non-zero value.

**Bad-Payload Rejection Rates by Path**
: Four overlaid series:
  - `bad-json`: body is not valid JSON after HMAC passes.
  - `bad-length`: `Content-Length` is zero or exceeds the listener's limit.
  - `ignored`: valid event but unsupported `X-GitHub-Event` type (e.g., `push`).
    This is expected for event types the operator has not restricted in the webhook
    delivery settings. Reduce it by configuring the GitHub webhook to send only
    `workflow_run` and `workflow_job`.
  - `read-timeout`: body read timed out before the listener received the full payload.
    Indicates network latency or a very large payload.

### ETag Poll row

**ETag Poll Runs by Outcome**
: `rate(waitbus_etag_poll_runs_total[5m])` split by `outcome`.
  The etag-poll timer fires every 45 seconds (systemd timer). Outcomes:
  - `started`: poller ran normally.
  - `no_repos_watched`: `watched_repos.txt` is empty; no polling attempted.
  A zero rate here means the timer is not firing or the service has crashed — check
  `systemctl --user status waitbus-etag-poll.timer`.

**ETag Poll Requests by HTTP Status**
: `rate(waitbus_etag_poll_requests_total[5m])` split by `status`.
  GitHub API HTTP responses per second:
  - `304 Not Modified`: expected when nothing has changed since last poll; the
    ETag mechanism is working correctly. A high fraction of 304 responses is good.
  - `200`: fresh data returned; new runs or jobs found.
  - `401` / `403`: credential problem. Check the GitHub token in the credential store.
  - `429`: rate limited. Reduce the poll frequency or the number of watched repos.
  - `5xx`: GitHub-side error; the poller will retry on the next timer tick.

### Backpressure row

**Watermark Replay Events Rate**
: `rate(waitbus_watermark_replay_events_total[5m])`.
  Events delivered via watermark replay rather than live broadcast. A non-zero rate
  means at least one subscriber reconnected and requested a historical backlog via
  the `since=<ulid>` subscribe field. Sustained high values indicate subscribers
  are frequently disconnecting and reconnecting (e.g., a flapping consumer or a
  consumer that crashes and restarts). This is a backpressure signal: replay walks
  the SQLite events table and can saturate the broadcast daemon's asyncio loop if
  the replay window is very large.
  Alert threshold suggestion: > 10 events/s sustained for > 5 minutes.

**Broadcast Send Latency p99 (Backpressure Indicator)**
: `histogram_quantile(0.99, ...)` over `waitbus_broadcast_send_seconds_bucket`.
  The p99 latency in isolation as a dedicated backpressure panel. Normal range:
  < 10 ms. A rising p99 that crosses 50 ms indicates the broadcast loop is under
  load — correlate with the Watermark Replay rate and the Subscriber Count to
  distinguish a slow subscriber from a replay-induced spike.
  Alert threshold suggestion: p99 > 100 ms for > 2 minutes.

---

## Interpreting Backpressure

The two primary backpressure indicators are:

1. **Watermark replay rate** (Backpressure row, left panel) — measures reconnection
   frequency. Each reconnect with a `since` cursor triggers a replay walk. If
   subscribers are stable and connected, this is zero.

2. **Broadcast send p99** (Backpressure row, right panel) — measures head-of-line
   blocking in the fan-out loop. The loop is single-threaded asyncio; one slow
   subscriber stalls the send to all other subscribers until the slow send times out
   or the subscriber disconnects.

**Diagnosis flow:**

- Rising p99 + stable subscriber count + zero replay rate: one subscriber is reading
  frames slowly. Identify it with `journalctl --user -u waitbus-broadcast` and
  restart or remove it.
- Zero p99 rise + rising replay rate: consumers are reconnecting frequently. Check
  the consumer processes for crash-restart loops.
- Both rising together: a subscriber crashed, reconnected with a large `since` cursor,
  and the replay is saturating the loop. Wait for the replay to complete or reduce
  the `since` cursor to a more recent ULID.

---

## Alerting Suggestions

These are starting points; tune thresholds to your traffic volume.

| Alert | Expression | Threshold | Severity |
|---|---|---|---|
| DB errors | `sum(rate(waitbus_db_error_total[5m]))` | > 0 for 2m | critical |
| HMAC rejections | `sum(rate(waitbus_webhook_hmac_rejected_total[5m]))` | > 0 for 5m | warning |
| Broadcast p99 high | `histogram_quantile(0.99, sum(rate(waitbus_broadcast_send_seconds_bucket[5m])) by (le))` | > 0.1s for 2m | warning |
| Replay rate high | `rate(waitbus_watermark_replay_events_total[5m])` | > 10 for 5m | warning |
| No inserts | `sum(rate(waitbus_db_inserted_total[5m]))` | == 0 for 30m | info |
| Zero subscribers | `waitbus_subscriber_count` | == 0 for 5m | info |

---

## Related

- `docs/ARCHITECTURE.md` — Runtime topology and observability endpoint description.
- `SECURITY.md` — HMAC credential configuration and threat model.
- `waitbus doctor` — CLI health check that validates the metrics endpoint is reachable.
