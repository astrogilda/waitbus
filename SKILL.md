---
name: waitbus
description: The workstation-local, cross-harness status bus — a blocking wait/emit primitive that lets any agent or script wait on, or emit, CI / pytest / Docker / filesystem events (finish or fail) without polling, framework-agnostically. GitHub Actions is source #1: subscribes to workflow_run + workflow_job (sub-second matrix-cell failure) from a local webhook cache, auto-detecting the repo via git remote. Use when the user asks "is CI green?", "did the deploy pass?", "what's the status of the latest run?", "which job failed?", wants a multi-repo CI overview, wants to block until a commit's checks finish (`waitbus wait --sha`), feed local test/build/container events to an agent (`waitbus emit` / `waitbus source`), or see push-vs-poll savings (`waitbus stats`). Event-driven; faster than `gh run list` polling.
homepage: https://github.com/astrogilda/waitbus
repository: https://github.com/astrogilda/waitbus
---

# waitbus

waitbus is the workstation-local, cross-harness status bus: a blocking
wait/emit primitive that lets any agent or script (across Cursor, Claude
Code, and any tool on the box) wait on, or emit, events from five
built-in sources — GitHub Actions CI, pytest sessions, Docker container
lifecycle, filesystem changes, and Prometheus Alertmanager — plus any
plugin source registered under the `waitbus.sources.v1` entry-point group.
Events land in a local SQLite event store the moment they arrive, and a
broadcast daemon fans each row to every connected consumer within a
millisecond, so an agent blocks on `waitbus wait` with zero polling and
idle CPU until the thing it cares about happens. Because every agent on
the box shares that one bus, it doubles as a same-machine coordination
backplane: one agent emits, the others wake.

The core verbs are framework-agnostic:

- `waitbus wait` blocks until a predicate over the event stream is
  satisfied (for example, a commit's checks finish, or a named source
  fails), then exits. No polling loop.
- `waitbus emit` puts an event on the bus from any source — a local test
  run, a build, a container, or an agent's own progress signal — so
  other agents and scripts can react to it.
- `waitbus on` runs a command when a matching event arrives.
- `waitbus source list` / `show` / `verify` introspect the built-in and
  plugin-registered source taxonomy.
- `waitbus stats` reports push-vs-poll savings.

GitHub Actions was the first source wired — which is why the examples
below lead with CI — but waitbus is not CI-only. For CI specifically, two
GitHub event types are tracked:

- `workflow_run` — run-level state (queued/in_progress/completed for the
  whole workflow). Fires at run start and run end only.
- `workflow_job` — per-job state, one entry per matrix cell, sub-second
  on failure. Closes the 30+ min blind spot where a job failed at minute
  5 but the parent run didn't complete until minute 32. Pass
  `--include-jobs` to `waitbus read-events list` to unroll child jobs under
  each run.

## When to invoke

- "Is CI green on `<branch>`?"
- "Did the latest push pass?"
- "What's the status of run 12345?"
- "Give me a CI overview across my repos."

If the user is inside a git checkout with a `github.com` origin, invoke
without flags — `waitbus read-events list` auto-detects `owner/repo`.

## How to use

Query the cache:

```bash
waitbus read-events list --text
# or for a specific repo:
waitbus read-events list --owner OWNER --repo REPO --last-n 5 --text
# JSON for downstream parsing:
waitbus read-events list --json
```

Text output shape (one event per line):

```
your-org/your-repo: main CI run 24680316882 (push, fix(ci): ...) -- completed/success at 2026-04-20T17:48:00Z [src=webhook]
```

`src=webhook` means the event came in live. `src=etag_poll` means the
poller backfilled it. Identical `run_id` is deduped.

## How Claude should interpret results

- `status=completed` + `conclusion=success` -> CI is green.
- `status=completed` + `conclusion=failure|cancelled|timed_out` -> red.
- `status=in_progress|queued` + `conclusion=null` -> still running.
- "no events cached" -> webhook not registered yet OR repo not in
  `watched_repos.txt`. Tell the user exactly that; do not fabricate a
  status.

If the user asks "is CI passing?" and the latest row is `in_progress`,
answer "still running" and include the run id, not a guess.

## Setup (one-time)

Install the package from any of the standard Python distribution
channels:

```bash
pip install waitbus
# or:
uv tool install waitbus
# or:
pipx install waitbus
```

Then run the three idempotent bootstrap commands:

```bash
waitbus init
waitbus install-credentials github-webhook-secret
waitbus install-systemd
```

`waitbus init` creates the per-user state directory (resolved by
`platformdirs`), provisions the SQLite schema, and transparently migrates
any legacy event data from a previous default location if present to the new
location on first run. `waitbus install-credentials
github-webhook-secret` reads the HMAC secret from `--value`, `--file`, or
stdin, shells out to `systemd-creds encrypt --name=github-webhook-secret`,
and writes the ciphertext to
`/etc/credstore.encrypted/waitbus.github-webhook-secret.cred`.
`waitbus install-systemd` copies the eight systemd-user units
shipped in the wheel into `~/.config/systemd/user/` and runs
`daemon-reload`.

`uvx` must be on PATH — it is the runtime that launches the
`waitbus mcp serve` sub-command when an MCP client spawns the server.

## Architecture

| Component | File | Purpose |
|---|---|---|
| HTTP listener | `waitbus/listener.py` (`waitbus listener serve`) | Verifies `X-Hub-Signature-256` in constant time, persists `workflow_run` / `workflow_job` payloads to SQLite. Stdlib only. Binds `127.0.0.1:9000`. |
| Broadcast daemon | `waitbus/broadcast.py` (`waitbus broadcast serve`) | AF_UNIX SOCK_STREAM hub with length-prefix framing, fans each persisted row to every connected subscriber. Runs on Linux (systemd) and macOS (launchd). |
| Poller | `waitbus/etag_poll.py` (`waitbus etag-poll run`) | Durability fallback via `If-None-Match`; fills gaps when webhook delivery is missed. |
| Watchdog | `waitbus/watchdog_check.py` (`waitbus watchdog-check run`) | Ingestion-silence detector; fires when nothing has been inserted in N minutes. |
| Query CLI | `waitbus/read_events.py` (`waitbus read-events list/watch`) | Reads SQLite; auto-detects repo from `git remote get-url origin`. |
| PR rollup | `waitbus/pr_monitor.py` (`waitbus pr-monitor tick`) | Aggregates job events into per-PR `ALL_GREEN / FAIL / PENDING / NO_JOBS` state. |
| MCP server | `waitbus/mcp.py` (`waitbus mcp serve`) | Subscribes to the broadcast daemon and re-emits frames as notifications. Runs on Linux + macOS. |
| Umbrella CLI | `waitbus/cli/` package (`waitbus`) | `init`, `install-systemd` (Linux), `install-launchd` (macOS), `install-credentials`, `doctor`, `status`, `verify-plugin`. |
| Schema | `waitbus/schema.sql` | Single `events` table keyed by `delivery_id`. `source` column reserved for future ingest types (Linux journal, Slack, PagerDuty). |

The per-user state directory is resolved at runtime by `platformdirs`.
On Linux that defaults to `~/.local/state/waitbus/`; on macOS to
`~/Library/Application Support/waitbus/`. The broadcast socket lives
under `$XDG_RUNTIME_DIR/waitbus/` on Linux and under
`$TMPDIR/waitbus-$UID/` on macOS. Set `WAITBUS_STATE_DIR` to
override either default.

## Troubleshooting

- **Listener not running:** `systemctl --user status waitbus-listener`
  then `journalctl --user -u waitbus-listener -n 50`. Or run
  `waitbus status` for a quick liveness overview.
- **No events for a repo that has CI runs:** check the webhook is
  registered (`gh api repos/<owner>/<repo>/hooks`) and points at the
  local forwarder. Or add the repo to `watched_repos.txt` under the
  state directory for polling-only coverage.
- **ETag poll silent:** `systemctl --user list-timers waitbus-etag-poll.timer`
  and `journalctl --user -u waitbus-etag-poll -n 50`.
- **Signature rejected (401):** credential mismatch — rotate via
  `waitbus install-credentials github-webhook-secret`, restart the
  listener (`systemctl --user restart waitbus-listener.service`),
  and re-run `gh webhook forward` with the new value.
- **DB missing:** re-run `waitbus init`; it is idempotent.

## Non-goals / scope

- Only `workflow_run` and `workflow_job` events are persisted; other
  event types return 200 and are silently dropped.
- No Cloudflare Tunnel, no Smee.io. `gh webhook forward` runs locally.
- The runtime depends on `typer` for the umbrella CLI only; every
  internal module is pure stdlib.

## Push mode — broadcast daemon and live subscribers

Three subscriber surfaces consume the broadcast daemon today:

- `waitbus read-events watch` prints one stdout line per matching
  event — the canonical shape for a background monitoring loop. Filters default to the current git checkout's
  `owner/repo`; `--all-events` broadens to every repo. A local ULID
  cursor at `<state-dir>/cursors/<owner>_<repo>.ulid` is rewritten
  atomically after every consumed frame so reconnects resume from the
  last seen event.
- `waitbus pr-monitor tick --pr 7 --pr 9` rolls workflow_job events up
  into per-PR `ALL_GREEN / FAIL / PENDING / NO_JOBS` state via the
  canonical AGG_SQL window-function query, emitting one line per
  state-hash transition and exiting `MONITOR_DONE` when every PR
  reaches a terminal state. Force-push detection runs client-side at
  the 5-minute cadence via `gh pr view --json headRefOid`.
- `waitbus mcp serve` subscribes to the same broadcast socket and re-emits
  each non-heartbeat event as both a vendor-specific Claude Code
  notification frame and a standard `notifications/resources/updated`
  frame. MCP clients receive these as background context injections
  without an explicit monitoring command. The broadcast daemon runs
  on both Linux (systemd) and macOS (launchd), so the MCP server is
  active on both platforms.

The broadcast daemon binds an AF_UNIX SOCK_STREAM socket (mode 0600)
under the runtime directory and frames every wire payload with a
4-byte big-endian length prefix. It accepts subscribe frames of the
shape

```json
{"filters": ["owner/repo", "*", "owner/*"],
 "event_types": ["workflow_run", "workflow_job",
                 "prometheus_alert", "prometheus_watchdog"],
 "since": "01HZ...26chars",
 "token": "..."}
```

— validated against an anchored regex (no shell-metachar surface).
The peer-credential UID check restricts subscribers to the daemon's
own UID (Linux uses `SO_PEERCRED`; macOS uses `getpeereid()` via
ctypes); an optional `broadcast-token` credential (staged via
`waitbus install-credentials broadcast-token`) adds a token check
on top.

Operator-side observability lives at `http://127.0.0.1:9000/metrics`
on the listener: per-source ingress counters (`received`, `inserted`,
`dedup_ignored`, `hmac_rejected`, `bad_json`, `bad_length`,
`etag_poll_requests`) in Prometheus 0.0.4 text format.

systemd:

```bash
systemctl --user start waitbus-broadcast.socket
systemctl --user status waitbus-broadcast.service
```

The `.socket` unit pulls in `.service` on first subscriber connect; the
service has no `WantedBy=default.target` to avoid a boot-time ordering
cycle with the listener and watchdog units (both anchored via
`BindsTo`).

### Why `pr-monitor` ships no systemd unit (manual-invoke by design)

`waitbus pr-monitor tick` is the only subcommand with no shipped
`.service`/`.timer`, and that is deliberate — not an oversight.
`pr-monitor` is a session-scoped subscriber: one invocation watches a
specific set of PRs (`--pr 7 --pr 9` / `--owner foo --repo bar`) and
self-exits once every watched PR reaches a terminal state. A static
unit cannot supply per-invocation PR arguments, and a process that
exits on completion is not a service-shaped or timer-shaped workload.
The long-running infrastructure (`listener`, `broadcast`, `etag-poll`,
`watchdog`) ships units; `pr-monitor` *consumes* that infrastructure as
an interactive operator tool and is invoked directly (or armed under
a monitoring loop), e.g. `waitbus pr-monitor tick --pr 7`.

## Platform support

| Surface | Linux | macOS |
|---|---|---|
| Library (`import waitbus`) | yes | yes |
| `waitbus listener serve` | yes | yes |
| `waitbus read-events list` (query) | yes | yes |
| `waitbus etag-poll run` (one-shot) | yes | yes |
| `waitbus broadcast serve` daemon | yes (systemd) | yes (launchd) |
| `waitbus pr-monitor tick` | yes | yes |
| `waitbus mcp serve` | yes (active) | yes (active) |
| `waitbus install-systemd` | yes | no (use install-launchd) |
| `waitbus install-launchd` | no (use install-systemd) | yes |
