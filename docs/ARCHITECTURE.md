# Architecture

waitbus is a single-user, single-workstation daemon stack that caches
GitHub Actions and Prometheus webhook deliveries in a loopback SQLite
database and fans every committed row out to local subscribers over a
length-prefixed SOCK_STREAM broadcast socket. This document describes the runtime
topology end-to-end, the IPC primitives between surfaces, the on-wire
frame shape, the path-resolution rules, the authentication model, the
MCP-integration story, and the observability endpoint.

---

## Surface map

```
GitHub / Alertmanager (HTTPS)
        |
        v
  waitbus listener serve  loopback :9000  (HMAC-SHA256 routes)
        |
        | INSERT OR IGNORE
        v
  events table              SQLite, WAL mode, single-file DB
        |
        | doorbell (AF_UNIX SOCK_DGRAM, 1 byte)
        v
  waitbus broadcast serve AF_UNIX SOCK_STREAM hub
        |
        +-- waitbus read-events watch   one line per event, stdout
        +-- waitbus pr-monitor tick     per-PR aggregate, prints transitions
        +-- waitbus mcp serve           MCP client bridge (Claude Code / Desktop)
        +-- future subscribers            same wire protocol
```

Plus two timer-driven daemons that run in parallel with the listener:

```
  waitbus etag-poll run      45 s systemd timer; ETag-aware GETs against api.github.com
  waitbus watchdog-check run 60 s systemd timer; watches for missing prometheus_watchdog rows
```

And the operator-facing umbrella CLI:

```
  waitbus                 typer dispatch:
                              init             bootstrap state dirs + schema
                              install-systemd  copy + enable units
                              install-launchd  copy launchd plists (macOS)
                              install-credentials  stage a secret into the 0600 secrets.json
                              doctor           validate the live install end-to-end
                              status           operational dashboard
                              verify-plugin    validate .claude-plugin/plugin.json
```

### `waitbus listener serve`: webhook ingress

Pure-stdlib HTTP server binding `127.0.0.1:9000`. Threading via the
standard library `ThreadingHTTPServer` for concurrent webhook
deliveries; one thread per request, no thread pool. Routes:

- `POST /webhook`: GitHub workflow events. HMAC-SHA256 over the
  raw body using the `github-webhook-secret` secret (read from the 0600
  `secrets.json` via `_secrets.get_secret`), lifted via
  `X-Hub-Signature-256`. Workflow_run and workflow_job event
  types are stored; everything else returns 200 ignored.
- `POST /alertmanager`: Prometheus alerts. HMAC over the body
  using the `alertmanager-hmac` credential, same delivery mechanism.
- `POST /watchdog`: same HMAC contract as /alertmanager; distinct
  event_type so the absence detector can query for it independently.
- `GET /healthz`: JSON liveness probe.
- `GET /metrics`: Prometheus text-format counters.

On commit, the listener writes one byte to the doorbell socket
to wake the broadcast daemon. The write is fire-and-forget (the
listener does not block on broadcast availability).

Linux + macOS supported. Both the listener and the SQLite
backend are pure stdlib so the surface works identically on either
OS.

### `waitbus broadcast serve`: fan-out hub

Asyncio-driven AF_UNIX SOCK_STREAM daemon (4-byte length-prefix framing;
see the Wire protocol section below). Owns three sockets:

- A listener socket subscribers connect to. Bound at
  `/run/user/<uid>/waitbus/broadcast.sock` (Linux, i.e.
  `$XDG_RUNTIME_DIR/waitbus/broadcast.sock`) or the
  platform-specific runtime path (see path-resolution table),
  or inherited via systemd socket activation (fd 3 + `LISTEN_FDS=1`).
- A doorbell socket the listener writes to on every commit.
  SOCK_DGRAM; the broadcast hub drains the FIFO on every wake.
- One client socket per connected subscriber, kept in the
  `subscribers: dict[fd, Subscriber]` map.

The fan-out loop:

1. Seed `cursor = MAX(seq)` on startup (`seq` is a daemon-assigned
   `INTEGER PRIMARY KEY AUTOINCREMENT`, monotonic in commit order,
   so concurrent inserts cannot be skipped).
2. On every doorbell ping (or batch of pings, coalesced via FIFO
   drain), `SELECT * FROM events WHERE seq > :cursor ORDER BY
   seq LIMIT 500`.
3. For each row, build the frame, serialise to bytes, send to
   every matching subscriber. Advance the cursor row-by-row.

Subscriber state is in-memory only; the SQLite table is the
source of truth. A daemon restart re-seeds the cursor from
MAX(seq), so a restart never reissues already-broadcast
frames. (The public `since=<ulid>` cursor resolves to its exact
`seq` for replay.) Subscribers that want the historical backlog send
`since=<ulid>` in their subscribe envelope; the daemon's
`_replay` walks the table from that cursor up to `REPLAY_LIMIT`
rows.

Heartbeat: every `WAITBUS_HEARTBEAT_SEC` seconds (default 60)
the daemon sends a `kind: "daemon_heartbeat"` frame to every
subscriber regardless of filters. The interval is loaded from
`WaitbusConfig` at daemon startup and held on the broadcast
instance as `self.heartbeat_sec`; changes require a daemon restart.
Consumers that want only event frames discard heartbeats;
consumers that want liveness signals use the
heartbeat's ULID as a "the daemon is alive at this moment" probe.

Runs on Linux (systemd) and macOS (launchd). The wire protocol is
SOCK_STREAM with 4-byte length-prefix framing on both platforms. The
peer-credential UID check dispatches at module-import time:
`SO_PEERCRED` on Linux, `getpeereid()` via ctypes on macOS (see
`waitbus/_peercred.py`).

### `waitbus etag-poll run`: fallback ingress

systemd-timer-driven, one-shot. Walks
`~/.local/state/waitbus/watched_repos.txt`, conditionally GETs
`api.github.com` for fresh workflow_runs and per-run jobs, dedups
into the events table via `INSERT OR IGNORE` with synthesised
`delivery_id`s. The poller covers repos for which the operator
cannot register a webhook (upstreams, forks, organisation repos
without admin access).

### Stall detection

Each poller cycle runs a second pass after the primary ETag fetch. The
poller queries the events table for any `workflow_job` whose latest state is
`in_progress` or `queued` and whose `started_at` is older than the stall
threshold. For each such job, the poller inserts one synthetic
`status=stalled` row keyed by `etag:stall:{job_id}` so the broadcast bus
delivers a "this job is wedged" signal to every subscriber. The
`INSERT OR IGNORE` contract means each stall is reported exactly once per
job; later polling cycles encounter the same key and produce no-ops.

**Threshold knob:** Set `WAITBUS_STALL_THRESHOLD_MIN` (integer, minutes) to
override the default of `60`. A job that has been in `in_progress` or `queued`
state for longer than this value triggers a synthetic stall event. Setting the
variable to a lower value makes stall detection more aggressive; raising it
reduces false positives on legitimately long-running matrix jobs.

### `waitbus read-events` and `waitbus pr-monitor`: sample consumers

Both subscribe to the broadcast bus with the same wire protocol.
They serve as reference implementations of the consumer surface:

- `read-events` is a CLI: query mode prints the latest N events
  for the current repo; watch mode subscribes and streams matching
  events one per line until EOF. The watch mode persists a cursor
  per `(owner, repo)` at `~/.local/state/waitbus/cursors/`, so
  reconnects pick up where the previous run left off.
- `pr-monitor` is a long-lived consumer that aggregates per-job
  state for a specific PR head_sha and prints a transition line
  whenever the aggregate moves (e.g., "PR #42: 12 in_progress ->
  10 in_progress, 2 success"). Designed for a tmux pane or
  shell-prompt indicator.

### Subscriber bookmark / resume cursors

`_broadcast_sub` exposes a named-bookmark mechanism so any subscriber can
opt into persistent resume without operator-managed ULID tracking.

**How it works:**

1. The subscriber calls `open_subscriber(bookmark_id="my-name", ...)`.
2. `open_subscriber` loads the cursor from
   `cursors_dir() / "bookmark-my-name.txt"` (returning `None` if the
   file is absent; first run starts at the daemon's live tail).
3. If a cursor is found and no explicit `since=` was supplied, it is
   injected into the subscribe envelope as `since=<stored_ulid>`.
4. After each non-heartbeat frame the caller invokes
   `save_bookmark("my-name", frame["id"])`. The write is atomic
   (same-directory tempfile + `os.replace`) so an interrupted run never
   leaves a corrupted cursor.
5. On reconnect, `open_subscriber` loads the updated cursor and the
   daemon replays only the events missed since the last clean shutdown.

**Bookmark name rules:**

A bookmark name must match `^[A-Za-z0-9_.-]+$`. The name is used as a
filename literal (`bookmark-{name}.txt`) inside `cursors_dir()`, so the
character set is constrained to prevent directory traversal, whitespace
injection, and shell metacharacter expansion. `open_subscriber`,
`save_bookmark`, and `load_bookmark` all raise `ValueError` on names that
fail this check, before any I/O.

**Consumer integration:**

Both `waitbus broadcast tap` and `waitbus replay` expose a
`--bookmark NAME` flag. When passed, the subscriber subscribes with the
stored cursor (if any) and advances the cursor on each received frame.
The `read-events --watch` mode uses a separate `(owner, repo)` cursor
scheme under the same `cursors_dir()` directory; both cursor families are
co-located but have distinct filename prefixes (`{owner}_{repo}.ulid` vs
`bookmark-{name}.txt`) so they never collide.

### `waitbus events query`: direct SQL passthrough

A read-only escape hatch for ad-hoc queries the prefab consumers
(`read-events`, `pr-monitor`, `mcp serve`) do not cover. The operator
supplies a literal SQL statement; waitbus parses, gates, and
forwards it to the events SQLite database opened in read-only mode:

- Only `SELECT` and `WITH`-rooted (CTE) statements pass the
  parse-time gate. `INSERT` / `UPDATE` / `DELETE` / `DROP` /
  `CREATE` / `ALTER` / `REPLACE` / `VACUUM` / `ANALYZE` / `REINDEX`
  / `PRAGMA` / `ATTACH` / `DETACH` are rejected with a clear
  message before any connection work happens.
- Multi-statement input (a second statement after the first `;`) is
  rejected; a single trailing semicolon is tolerated.
- A trailing `LIMIT N` is injected at the outermost level (default
  1000); an existing outer LIMIT is capped at `min(operator_value,
  default)`. `--no-limit` opts out of injection entirely when an
  unbounded scan is intentional.
- The connection is opened via `file:...?mode=ro`, so the SQLite
  layer rejects writes even if a parse-time rule were ever
  bypassed (defense in depth).
- Output is JSON-by-default (one object per row inside a JSON
  array); `--text` switches to `key: value` blocks separated by
  blank lines.

Threat model: the operator IS the trusted party (this is a
single-user workstation tool). The parse-time gates exist to catch
typos before they reach the DB; the read-only connection is the
load-bearing safety property. See [`../SECURITY.md`](../SECURITY.md)
for the full discussion.

### `waitbus mcp serve`: MCP client bridge

AF_UNIX subscriber that re-emits broadcast frames as MCP
notifications. Driven by the official `mcp` Python SDK at v1.27.1
exact (pinned in `pyproject.toml`), using the low-level
`mcp.server.lowlevel.Server` interface. Two methods per frame:

- `notifications/resources/updated`: public MCP spec, consumed by
  Claude Desktop and any spec-compliant generic client. Emitted via
  the SDK's typed `ServerSession.send_resource_updated` helper.
- `notifications/claude/channel`: Claude Code vendor-specific extension,
  consumed by Claude Code's experimental channel-capability
  surface. The method name is NOT part of the public MCP spec.
  Emitted by constructing
  a bare `JSONRPCNotification` (whose `method: str` field is the open
  path the SDK uses for raw message sends) and passing it through
  `ServerSession.send_message`; the closed `ServerNotification`
  pydantic union does not gate this egress path.

The SDK pin is exact (not a range) so the wire shape is bound to
the fixture corpus committed alongside the rewrite. A 30-day
refresh cadence governs the bump path; the two-tier wire fixture
under `tests/data/mcp_wire_*.jsonl` is the regression fence.

On Linux, the daemon connects to the broadcast socket on startup
and stays connected for the life of the MCP session. On macOS or
any host where the broadcast daemon is unreachable, the daemon
logs one info-level message and exits cleanly so MCP clients
treat the integration as unavailable rather than failed.

### `waitbus watchdog-check run`: absence detector

systemd-timer-driven, one-shot. Reads the most recent
`prometheus_watchdog` row's `received_at` and toggles a flag
file under the user-state directory:

- `seen.flag` exists when the most recent watchdog row is fresher
  than `--threshold-min` (default 5 minutes).
- `stale.flag` exists when the most recent watchdog row is older
  than the threshold or absent entirely.

The flag-file model lets a shell-prompt indicator `stat` the flag
once per prompt rather than re-running a SQLite query on every
prompt render. Indicator implementations are out of scope for the
package itself; the flag-file contract is the consumer surface.

### `waitbus`: operator-facing CLI

Typer-based umbrella. All daemon entry points, plus operator
subcommands:

- `init`: bootstrap state dirs, run schema migration via
  `_db.ensure_schema`, scaffold `watched_repos.txt` and
  `etag_state.json`, transparently migrate any event store found at a
  legacy default location onto the platformdirs target if one
  exists.
- `install-systemd`: copy `share/systemd/user/` units from the
  wheel install prefix to `~/.config/systemd/user/`. Required for
  `uv tool install` / `pipx install` (their isolated tool prefixes
  are not on systemd's load path). Idempotent; `--sync` removes
  orphans for the downgrade case; `--dry-run` previews actions
  without modifying anything and exits 0.
- `install-launchd`: macOS equivalent of `install-systemd`.
- `install-credentials`: operator command that reads a secret from
  `--file` or stdin and merges it (key = credential name) into the 0600
  `secrets.json` under the state dir, writing atomically
  (`secrets.json.tmp` chmod-0600-then-`os.replace`). Staging
  `github-webhook-secret` also enables the opt-in webhook listener. At-rest
  protection is delegated to host full-disk encryption + UNIX DAC; the
  daemon reads the value via `_secrets.get_secret`.
- `doctor`: health check (config, paths, binaries, secrets file,
  systemd/launchd-unit presence, metrics endpoint). Exits 0 when every
  section reports clean; exits 1 on any issue so the command is usable
  in pre-commit hooks, shell-prompt indicators, and post-restart
  health probes.
- `status`: operational dashboard with event counts, last event
  timestamp, daemon liveness state per platform.
- `verify-plugin`: validate `.claude-plugin/plugin.json` fields
  (`name`, `version`, `schemaVersion`). Uses `CLAUDE_PLUGIN_ROOT`
  env var to locate the plugin directory. Unrelated to
  `source verify` below. `verify-plugin` validates a Claude plugin
  manifest; `source verify` validates a waitbus source plugin's PEP 740
  attestation.
- `source list / show / verify`: inspect the live source registry
  (built-in sources plus any plugins discovered via the
  `waitbus.sources.v1` entry-point group). `source verify` wraps
  `pypi_attestations.Attestation.verify` in-process against a
  plugin's installed wheel. See `docs/CUSTOM_SOURCES.md`.
- `allowlist list / add / remove / verify`: manage publisher-bound
  TOFU pins (`~/.config/waitbus/plugins.allowlist.toml`). The pin file
  is updated automatically on first-install of an attested plugin;
  these verbs let the operator audit, manually add, or drop pins.
- `config validate [PATH]` / `config schema`: pre-flight config
  validation and schema emission; see the next section.

### `waitbus config`: pre-flight validation and schema emission

The daemon validates `config.toml` on startup and loud-fails on any
error, but that surfaces problems
late, after the operator has already pointed a unit at the bad file.
The `config` sub-app moves the same checks earlier:

- `config validate [PATH]`: loads `PATH` (or the platformdirs
  default at `~/.config/waitbus/config.toml`), runs the file
  through `tomllib` and the `WaitbusConfig` pydantic-settings model,
  and reports the result. Exit 0 on success, 2 on any failure (file
  missing, malformed TOML, field validation error). Default output is
  human-readable (`field_path: msg (type=...)` lines on stderr);
  `--json` switches to a structured JSON array consumable by editor
  plugins and CI lint hooks. `--quiet` suppresses the success line on
  stdout for scripts that only care about the exit code.
- `config schema`: emits the canonical config schema. `--format=json`
  (default) prints a JSON Schema document (pydantic v2
  `model_json_schema()` output, augmented with a stable `$id` and an
  operator-facing `title`). `--format=toml-example` prints a commented
  `config.toml` template covering every supported field with its
  description, type, and default value, including the `[prometheus]`
  section header consumed by the loader's `_flatten_toml`. `--out PATH`
  writes to a file instead of stdout.

The pure-function implementations (`_validate_config_file`,
`_emit_toml_template`, `_emit_json_schema`) live in
`waitbus/config_validate.py` and are tested independently of the
typer wire-up; the wire-up smoke tests live in
`tests/test_config_validate.py`. The TOML-template emitter is
hand-rolled (the supported field set is small and stable) to avoid
pulling in a TOML-writer dependency for a one-off rendering task; the
output is parsed by `tomllib` in the test suite as a round-trip
correctness check.

---

## Data flow

A single GitHub workflow_run event takes this path through the
stack:

1. **HTTPS POST arrives** at `127.0.0.1:9000/webhook` from
   GitHub's webhook delivery infrastructure (`gh webhook forward`
   tunnels deliveries here in local development; production
   uses a public ingress that proxies to the loopback listener).
2. **HMAC verification.** The listener reads the body, computes
   `HMAC-SHA256(body, key=github_webhook_secret)`, compares
   constant-time against `X-Hub-Signature-256`. Mismatch returns
   401 and increments `waitbus_webhook_hmac_rejected_total`.
3. **JSON parse + field extraction.** `_event_from_webhook_payload(event_type,
   payload, ...)` flattens the payload into the columns the events
   table indexes on: `(owner, repo, run_id, workflow_name,
   head_branch, head_sha, status, conclusion, job_id, job_name,
   parent_run_id, alert_*)`. Container accesses tolerate non-dict
   values at every level so malformed payloads degrade to
   nullable fields rather than crash the handler.
4. **`INSERT OR IGNORE` into the events table.** The dedup key is
   `delivery_id`, supplied by GitHub as the `X-GitHub-Delivery`
   header. A redelivery of the same `X-GitHub-Delivery` value is a
   no-op (the schema enforces uniqueness); a fresh delivery
   commits one row and assigns a monotonically increasing
   `event_id` (a 26-character ULID).
5. **Doorbell ping.** Inside `insert_event`, after the commit
   returns, `_doorbell.ring()` writes a single byte to
   `waitbus-doorbell.sock`. Fire-and-forget; broadcast
   availability does not block the listener path.
6. **Broadcast wake.** The broadcast daemon's `on_doorbell`
   reader drains the doorbell FIFO and runs one
   `_broadcast_pass`. The pass `SELECT`s rows above the cursor,
   builds a frame per row, and sends to every matching subscriber.
7. **Per-subscriber send.** Each frame is JSON-encoded with
   `_frame.MAX_FRAME_BYTES = 65_536` bytes as the envelope.
   Frames larger than the envelope are replaced by a `kind:
   "truncated"` stub referencing the rowid; consumers re-fetch the
   full row via `read_events --json`.
8. **Subscriber prints, persists cursor, advances.** A
   `read_events --watch` consumer prints the frame's `summary`
   string on stdout, atomically updates the per-(owner, repo)
   cursor file, and resumes the recv loop.

A `workflow_job` event follows the same path; the listener's
`_event_from_webhook_payload` branches on `event_type` to populate the
job-specific columns. A Prometheus alert event takes the same
path through `/alertmanager` with the alertmanager HMAC key, and
populates `alert_name`, `alert_severity`, `alert_fingerprint`
instead of the GitHub-shaped columns.

The ETag-poll path lands at the same `insert_event` call from
`upsert_runs` / `upsert_jobs` with `ingest_method="etag_poll"`;
the doorbell rings the same way; downstream consumers cannot
distinguish a webhook-sourced row from a poll-sourced row
except via the `ingest_method` column. That symmetry is
deliberate: every consumer surface treats both ingest paths
as equivalent event sources.

---

## Wire protocol

The broadcast bus uses AF_UNIX `SOCK_STREAM` with **4-byte big-endian
length-prefix framing**: each frame is a `uint32` length followed by
exactly that many payload bytes. A stream socket does NOT preserve
message boundaries, so the length prefix is the framing layer
consumers read against (`waitbus/_frame.py::read_frame` /
`read_frame_sock`). This is wire protocol **v1**, frozen; it ships frozen at
v0.1.0, the first public release.

> The frozen, normative consumer contract (every frame field, the
> `proto` negotiation, the `subscribe_ack` / `subscribe_rejected`
> handshake, and the replay-vs-live `caught_up_at` cursor) lives in
> [`docs/CONSUMER_API.md`](CONSUMER_API.md) (sections 1 to 3). That document is
> the single source of truth for anyone implementing a subscriber; the
> summary below orients a reader of this architecture doc and defers to
> it on any discrepancy.

### Frame envelope

- One frame = a 4-byte length prefix + a payload of at most 65,536
  bytes (`_frame.MAX_FRAME_BYTES`). A length prefix of `0` or one
  greater than `MAX_FRAME_BYTES` is a protocol violation the consumer
  rejects by closing the connection.
- Encoding: UTF-8 JSON with compact separators (`,`, `:`).
- Data frames (`kind` ∈ `event`, `truncated`) carry a 26-character
  ULID `event_id` that is both the wire identity and the replay
  cursor; ULIDs are monotonic across the events table, so
  `event_id > :cursor` gives strict-ordering replay semantics.
  Control frames carry no `event_id`.

### Subscribe frame (client → server)

```json
{
  "proto": 1,
  "filters": ["owner/repo", "owner/*", "*"],
  "event_types": ["workflow_run", "workflow_job",
                  "prometheus_alert", "prometheus_watchdog"],
  "since": "01HZ...26chars"
}
```

- `proto` is the wire-protocol version the subscriber speaks; omitting
  it is treated as `1`. An unsupported value is refused with a
  `subscribe_rejected{reason:"version", supported:[1]}` frame.
- `filters` validates against `^([A-Za-z0-9_.-]+/([A-Za-z0-9_.-]+|\*)|\*)$`.
  No shell metachars, no regex. A malformed envelope closes the
  subscriber without a reply.
- `event_types` defaults to all supported types when omitted.
- `since` is optional; when present the daemon replays up to
  `REPLAY_LIMIT` matching rows above that ULID before joining the live
  stream, then sends the `subscribe_ack`.

There is no subscribe token: the AF_UNIX socket's same-UID peer-credential
check (`SO_PEERCRED` / `getpeereid`) is the entire ingress boundary.

### Daemon → subscriber frames

Every daemon-sent frame carries an open string `kind` discriminator. A
consumer reads `kind` first and routes data-vs-control on it. The set
is **not** a closed/exhaustive union: a future `kind` can be added
additively, and the four hand-decoding language snippets ignore frames
whose `kind` they do not recognise rather than break.

| `kind` | Axis | Carries `event_id`? | Meaning |
|--------|------|---------------------|---------|
| `event` | data | yes | A real event. `event_type` (not `kind`) is `workflow_run` / `workflow_job` / `prometheus_alert` / etc. |
| `truncated` | data | yes | Stub for a row exceeding `MAX_FRAME_BYTES`; consumer re-fetches via `read-events --json`. |
| `daemon_heartbeat` | control | no | Liveness ping every `heartbeat_sec`; reaches every subscriber regardless of filters. |
| `subscribe_ack` | control | no | Positive registration signal, emitted exactly once after registration + replay; carries `proto`, `caught_up_at` (replay/live dedup cursor, `null` when no `since`), and `heartbeat_sec`. |
| `subscribe_rejected` | control (terminal) | no | Subscribe refused (`reason` ∈ `version` / `lag_limit_exceeded`); the connection then closes. |

An `event` frame's `summary` is the human-friendly one-line rendering
from `read_events.format_text`; consumers wanting a different layout
read `fields` and ignore `summary`. `DATA_FRAME_KINDS` /
`CONTROL_FRAME_KINDS` are exported from `_frame.py` as the documented
consumer partitioning constants.

---

## State path resolution

waitbus reads its state-, runtime-, and config-directory paths
from `platformdirs.PlatformDirs("waitbus")`. Operator overrides
take precedence via environment variables; the platformdirs
defaults are the fallback.

| Variable | Default (Linux) | Default (macOS) | Default (Windows) |
|----------|-----------------|-----------------|-------------------|
| `WAITBUS_STATE_DIR` | `~/.local/state/waitbus` | `~/Library/Application Support/waitbus` | `%LOCALAPPDATA%\waitbus\state` |
| `WAITBUS_RUNTIME_DIR` | `/run/user/<uid>/waitbus` | `$(tempfile.gettempdir())/waitbus-<uid>` (typically `/tmp/waitbus-<uid>` when `$TMPDIR` unset)[^macos-runtime] | `%LOCALAPPDATA%\waitbus\runtime` |
| `WAITBUS_CONFIG_DIR` | `~/.config/waitbus` | `~/Library/Application Support/waitbus` | `%APPDATA%\waitbus` |

Notes:

- The SQLite DB lives in the state directory.
- The broadcast and doorbell socket paths follow the platform's
  runtime convention. On Linux that is `XDG_RUNTIME_DIR`
  (tmpfs-backed and per-session); on macOS the code uses
  `tempfile.gettempdir()` instead of the platformdirs default
  (`user_runtime_dir`) because on macOS platformdirs resolves
  `user_runtime_dir` to an Apple-evictable `Library/Caches/TemporaryItems`
  path, which is unsuitable for long-lived Unix sockets. Platform-
  specific dispatch lives inline at three leaf sites: `_paths.py`
  (macOS socket branch), `_doorbell.py` (eventfd vs SOCK_STREAM ping),
  and `_peercred.py` (`SO_PEERCRED` vs `getpeereid()`). No
  `IPCBackend`-style abstraction layer; the three leaf sites are
  the only platform-divergent code, so an abstraction layer would
  add indirection without reducing the divergent surface.

[^macos-runtime]: platformdirs' macOS `user_runtime_dir` resolves to an Apple-evictable cache path (`Library/Caches/TemporaryItems`), which is unsuitable for Unix sockets. `_paths.py` branches to `tempfile.gettempdir() / "waitbus-<uid>"` on macOS instead.
- Windows resolution exists in `_paths.py` because `platformdirs`
  resolves it cleanly, but no daemon stack is shipped for
  Windows. The library + read-events surfaces work for an
  operator who wants to manually populate a `github.db` and query
  it on Windows.

---

## Authentication model

Two independently-scoped mechanisms, each loaded once at daemon startup
(rotation requires a daemon restart).

### Per-route HMAC for webhook ingress

- `/webhook`: HMAC-SHA256 keyed on the `github-webhook-secret`
  secret. Required. The listener is opt-in (enabled only when this
  secret is staged) and exits 2 at startup if it is missing.
- `/alertmanager` and `/watchdog`: HMAC-SHA256 keyed on the
  `alertmanager-hmac` secret. Optional. When absent, both routes
  return 503; the GitHub path is unaffected so a single missing
  secret never disables the whole listener.

### Peer-credential UID gate for daemon subscribers

- Every accepted connection to the broadcast socket reads the peer
  UID and rejects peers whose UID differs from `os.getuid()`. Linux
  uses `SO_PEERCRED`; macOS uses `getpeereid()` via ctypes (see
  `waitbus/_peercred.py`). `LOCAL_PEERPID` is deliberately not
  used; see SECURITY.md for the CVE rationale.
- This kernel-attested same-UID check is the entire broadcast ingress
  boundary. There is no application-level subscribe token: any process
  that can reach the socket is already proven same-UID, so a bearer
  token would re-check an identity the kernel has already attested.

### Secret staging conventions

Two secret names the listener reads:

```
github-webhook-secret   listener  load-bearing
alertmanager-hmac       listener  optional
```

Staging via `waitbus install-credentials <name>` (reads the plaintext
from `--file` or stdin, never `--value`, which would leak to shell
history) merges it (key = `<name>`) into the 0600 `secrets.json` under
the state dir, written atomically (`secrets.json.tmp` chmod-0600 then
`os.replace`). Staging `github-webhook-secret` also enables the opt-in
listener unit. The daemon reads the value via `_secrets.get_secret`
(stdlib `json.loads`, no external tool); `waitbus doctor` reports
per-key presence. At-rest protection is delegated to host full-disk
encryption (FileVault / LUKS) + UNIX DAC (0600); see SECURITY.md.

The secrets file is read once and cached at daemon startup; the daemon
holds the key material in process memory for its lifetime. There is no
runtime refresh path, so a compromised disk write cannot quietly rotate
the listener's HMAC key under a running daemon. Rotation is
`install-credentials <name>` followed by a unit restart.

---

## MCP integration

`waitbus mcp serve` is the MCP-side bridge: an AF_UNIX subscriber that
translates broadcast frames into MCP notifications. The wire layer is
the official `mcp` Python SDK at v1.27.1 exact, driven through its
low-level `Server` interface (NOT `FastMCP`). Two methods are emitted
for every event so the integration works against the public MCP spec
(Claude Desktop, generic clients) and the Claude Code vendor-specific
channel capability (Claude Code with the flag enabled) simultaneously.

Emission per event:

- `notifications/resources/updated`: public MCP method. Consumed
  by Claude Desktop and any spec-compliant generic client. The
  resource URI references the waitbus event by ULID;
  consumers fetch the full event via the standard `resources/read`
  flow.
- `notifications/claude/channel`: Claude Code vendor-specific extension
  method. Consumed by Claude Code's experimental
  channel-capability surface. Carries a richer payload
  (per-event-type routing hints, summary string inline) and a
  lower latency budget than the resources-updated path.

The method-name caveat: `notifications/claude/channel` is NOT
part of the public MCP spec. The dual emission is exactly the
mitigation: if Anthropic renames or removes the method, the
public-spec path keeps serving every other client. Operators
deploying against Claude Code see the faster path; operators
deploying against Claude Desktop or a generic client see the
spec-compliant path. Neither path is the operator's
responsibility to configure; both are emitted automatically.

The integration installs via the standard MCP Registry namespace
`io.github.astrogilda/waitbus` (Python entry, `runtimeHint:
"uvx"`). A Claude Code or Claude Desktop config that references the
MCP Registry name picks up the Python entry automatically.

### Protocol version range introspection

`waitbus._mcp_constants.PROTOCOL_VERSIONS_SUPPORTED` mirrors the
pinned SDK's `mcp.shared.version.SUPPORTED_PROTOCOL_VERSIONS` tuple
verbatim. `PROTOCOL_VERSION` is derived as the last entry so a future
SDK bump that adds or drops entries cannot silently misadvertise.
Operators verify pin/range alignment via `waitbus mcp info`,
which emits:

```json
{
  "name": "waitbus",
  "version": "0.1.0",
  "protocolVersion": "2025-11-25",
  "supportedProtocolVersions": ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"]
}
```

The constant's docstring carries a two-line shell recipe for comparing
this tuple against the SDK's own list, giving drift detection without a live
session.

---

## Observability

Every daemon emits one-line structured JSON via the single
`waitbus._log.structured` helper. The field-naming contract
(reserved keys, the `status` vs `code` distinction, the
silent-fallback rule) is documented separately in
[LOGGING_CONVENTIONS.md](LOGGING_CONVENTIONS.md); treat an operator's
`jq` filter joining on those fields as a contract.

The listener exposes `GET /metrics` at `http://127.0.0.1:9000/metrics`
in the Prometheus text exposition format. The endpoint reports the
in-process counter map; counters reset on daemon restart by design
(restarts are infrequent and operator-driven).

Counters reported on the listener side:

```
waitbus_webhook_received_total{path="/webhook|/alertmanager|/watchdog"}
waitbus_webhook_bad_length_total{path="..."}
waitbus_webhook_bad_json_total{path="..."}
waitbus_webhook_read_timeout_total{path="..."}
waitbus_webhook_hmac_rejected_total{path="...", reason="missing|mismatch"}
waitbus_webhook_ignored_total{path="...", event_type="..."}
waitbus_db_inserted_total{event_type="...", source="...", ingest_method="..."}
waitbus_db_dedup_ignored_total{event_type="...", source="...", ingest_method="..."}
waitbus_db_error_total{path="...", source="github|alertmanager"}
waitbus_etag_poll_runs_total{outcome="started|no_repos_watched"}
waitbus_etag_poll_requests_total{endpoint="runs|jobs", status="200|304|..."}
```

A scrape job that hits `http://127.0.0.1:9000/metrics` once per
minute is sufficient for the visible counter set. The listener is
not a long-running computation source, so scrape latency is
single-digit milliseconds.

The broadcast daemon exposes its own **opt-in** loopback `/metrics`
endpoint (`waitbus/_metrics_http.py`). It is OFF by default: no
socket opens unless `WAITBUS_METRICS_PORT` is set (or
`--metrics-port` is passed to `waitbus broadcast serve`), the bind
host is hardcoded to `127.0.0.1`, and a bind failure logs a
structured warning rather than crashing the daemon. It serves
the subscriber-lifecycle and fan-out instrumentation: the
`waitbus_subscriber_count` gauge, the
`waitbus_subscriber_{opened,closed,rejected,evicted}_total`
counters, the `waitbus_broadcast_send_seconds` histogram,
`waitbus_watermark_replay_events_total`, and the
`waitbus_broadcast_events_{emitted,delivered}_total` pair.
`waitbus_broadcast_events_delivered_total` counts EVENT frames only
(control frames such as heartbeat, subscribe_ack, and subscribe_rejected are
never counted) at kernel-accept: a synchronous full send, or that
frame's flush completion when it was queued, uniformly across
fan-out, the pre-ack drain, and replay. Subscriber
connect/disconnect events are additionally logged for
`journalctl --user -u waitbus-broadcast` correlation.

### OpenTelemetry tracing

Tracing is an **opt-in** observability channel that lives alongside the
Prometheus scrape surface. The OpenTelemetry SDK and the OTLP HTTP/protobuf
exporter live in the `[otel]` extras group (`uv sync --extra otel` or
`pip install 'waitbus[otel]'`), so the base runtime closure stays
small for hosts that do not export traces. When the extras are absent
*or* `WAITBUS_OTEL_ENDPOINT` is unset, every daemon installs a NoOp
tracer and the `start_as_current_span` call sites cost essentially zero.

Configuration knobs on `WaitbusConfig`:

| Field | Default | Env var | Meaning |
|-------|---------|---------|---------|
| `otel_endpoint` | `None` | `WAITBUS_OTEL_ENDPOINT` | OTLP collector URL (e.g. `http://localhost:4318/v1/traces`). `None` keeps the NoOp tracer. |
| `otel_service_name` | `waitbus` | `WAITBUS_OTEL_SERVICE_NAME` | `service.name` resource attribute on every span. |
| `otel_sample_rate` | `1.0` | `WAITBUS_OTEL_SAMPLE_RATE` | `TraceIdRatioBased` sampler ratio in `[0.0, 1.0]`. |
| `otel_export_protocol` | `http/protobuf` | `WAITBUS_OTEL_EXPORT_PROTOCOL` | OTLP wire protocol. Only `http/protobuf` is bundled in the extras group; gRPC would require `grpcio` in the runtime dep closure. |

The instrumentation is two-phase:

**Phase 1, explicit spans at daemon entry points.** Each daemon's
`main()` calls `setup_tracer_provider()` at startup and
`shutdown_tracer_provider()` at exit (flushing the OTLP batch
processor). Spans wrap the daemon's natural request / event boundary:

| Daemon | Span name | Key attributes |
|--------|-----------|----------------|
| `listener` | `listener.do_POST` | `http.method`, `http.route`, `webhook.event_type`, `webhook.outcome` (`committed` / `bad_json` / `hmac_rejected` / `ignored` / ...) |
| `broadcast` | `broadcast.doorbell_wake` | `broadcast.subscribers`, `broadcast.frames_sent` |
| `etag_poll` | `etag.cycle` | `etag.repos`, `etag.new_rows` |
| `etag_poll` | `etag.poll_repo` (child) | `etag.repo`, `etag.repo_new_rows` |
| `etag_poll` | `etag.api_request` (child) | `github.api.path`, `github.api.has_etag`, `github.api.status`, `github.api.cache_hit` |
| `watchdog_check` | `watchdog.check_cycle` | `watchdog.outcome` (`fresh` / `stale` / `pre_bootstrap` / `db_error`), `watchdog.age_seconds` |

The broadcast hub intentionally emits **one span per doorbell wake**
rather than one span per fan-out frame. A CI-event burst can sweep up
to 500 rows per wake; one span per frame would overwhelm a collector
without adding actionable signal. The per-wake span instead carries
`broadcast.frames_sent` as a numeric attribute so the cardinality
stays bounded while the throughput remains visible.

Stamina-driven HTTP retries inside `etag_poll._do_conditional_get`
share a single span: one OTel span per logical API call, not one
per HTTP round.

**Phase 2, stdlib auto-instrumentation.** `setup_tracer_provider()`
also enables `opentelemetry-instrumentation-sqlite3` and
`opentelemetry-instrumentation-urllib` when those packages are
importable (they ship in the `[otel]` extras group). Auto-instrumentation
adds child spans for every `_db.py` query and every outbound GitHub
API call without further source-level changes.

**Log-trace correlation.** When a tracer is configured AND a span is
active at the call site, `_log.structured` adds `trace_id` (32-hex)
and `span_id` (16-hex) fields to the emitted record. Operators jump
from any log line to the matching span in Jaeger / Tempo / Honeycomb
without reconstructing the request from timestamps alone. When no
tracer is configured the helper skips the injection cheaply, so the
JSON shape stays identical to the pre-OTel record.

**Secret redaction.** HMAC-verify failures and GitHub-API credential
loads call `_otel.add_redacted_secret_attribute(span, key)` to
pre-populate secret-bearing attribute keys with `"<redacted>"`. This
defuses any future urllib / sqlite3 instrumentation that introspects
callable arguments: the redacted sentinel reaches the SDK first, so
the credential value cannot leak as a span attribute.

The exporter is the OTLP HTTP/protobuf path (no gRPC) so the runtime
dep closure stays free of `grpcio`. Operators who need gRPC point an
OpenTelemetry Collector at the HTTP/protobuf endpoint and let the
collector relay; this is the canonical 2026 pattern and trades one
extra hop for a far smaller daemon footprint.

---

## Monitoring

A pre-built Grafana dashboard is shipped with the repository:

- **Dashboard JSON**: [`monitoring/grafana/waitbus-backpressure.json`](../monitoring/grafana/waitbus-backpressure.json)
  (Grafana 11+, uid `waitbus-backpressure`, datasource variable `${DS_PROMETHEUS}`)
- **Operator runbook**: [`docs/monitoring/waitbus-grafana.md`](monitoring/waitbus-grafana.md)

The dashboard surfaces five row groups:

| Row | Key signals |
|-----|-------------|
| Throughput | DB insert rate by event type, dedup-ignored rate, DB error rate |
| Broadcast | Send latency p50/p95/p99, active subscriber count |
| Webhook Health | Received rate by path, HMAC rejection rate, bad-payload rejection rates |
| ETag Poll | Poll runs by outcome, GitHub API requests by HTTP status |
| Backpressure | Watermark replay event rate, broadcast send latency p99 |

**Import:** `Dashboards > Import` in Grafana, upload the JSON file, select the
Prometheus datasource. The dashboard auto-refreshes every 30 seconds over a
default 6-hour window.

**Prerequisite:** the listener's `GET /metrics` endpoint at `http://127.0.0.1:9000/metrics`
must be reachable from Prometheus. See the runbook for the scrape configuration snippet.

---

## Schema migrations

The events DB has two distinct startup paths:

1. **Bootstrap** (`_db.ensure_schema`) materialises the canonical
   `waitbus/schema.sql` against a fresh database. Both the
   listener and the broadcast daemon call this at startup; the
   `BEGIN IMMEDIATE` retry loop absorbs the socket-activation race.
   `ensure_schema` also runs an in-place additive ADD-COLUMN pass for
   any column declared in `schema.sql` but missing from the live
   table; that path remains for column-shape evolution that a
   listener restart on a single workstation can absorb without a
   separate migrate step.

2. **Evolution** (`waitbus.migrations`) applies numbered SQL
   files (`waitbus/migrations/NNNN_<slug>.sql`) against an
   already-bootstrapped database. Each applied migration is recorded
   in a `schema_migrations` table (sequence number, slug, applied-at
   epoch ns, SHA-256 of the file contents). The operator drives this
   pass with `waitbus migrate`; the same command exposes
   `--status` (print applied + pending), `--dry-run` (print SQL
   without executing), and `--to NNNN` (bound the apply pass at a
   sequence number).

   Filename shape: `NNNN_<snake_case>.sql` where NNNN is a zero-padded
   sequence number. An optional same-stem `.py` file may export
   `def apply(conn: sqlite3.Connection) -> None:` for non-SQL
   operations (column backfill from a Python expression, multi-step
   data normalisation). The hook runs after the SQL block inside the
   same transaction.

   Tamper detection: the SHA-256 of every applied `.sql` file is
   recorded in `schema_migrations`. A post-apply edit makes the next
   `migrate` invocation refuse to run with a clear `checksum drift`
   error; the operator must restore the original contents or roll
   the change forward in a new numbered file.

   Gap detection: the discovery pass requires every sequence number
   from 1 up to the highest discovered file to be present on disk.
   A missing intermediate file (0001 + 0003 with no 0002) aborts
   with a `migration gap detected` error.

   Concurrency: `apply_pending` wraps each file in its own
   `BEGIN IMMEDIATE` transaction and reuses the shared SQLITE_BUSY
   retry budget. Two concurrent `migrate` invocations serialise
   cleanly; the loser sees the winner's commit on retry and its own
   apply pass becomes a no-op.

   Fresh-install pairing: `waitbus init` calls `ensure_schema`
   (which materialises `schema.sql`) and then `mark_baseline_applied`
   (which records every shipped migration as already-applied without
   re-executing its DDL). This keeps the tracking table consistent
   with the bootstrapped on-disk schema so the next evolutionary
   migration runs against a tracked starting point.

The split is deliberate: `ensure_schema` is the daemon-startup
fastpath (idempotent on every boot, no operator action required for
additive column changes); `migrate` is the operator-driven evolution
verb (anything beyond additive ADD COLUMN, such as index drops, table
renames, and data backfills, lands as a numbered file).

---

## Boundary: what waitbus does not do

The architecture is deliberately bounded; some adjacent capabilities
were considered and explicitly left out:

- **No multi-host fan-out.** The broadcast bus is loopback only.
  An operator with multiple workstations runs an independent
  waitbus stack on each. Cross-host event federation is not in
  scope.
- **No public API surface.** The events table is queryable directly
  via SQLite, and the broadcast bus is queryable via any
  subscriber. There is no HTTP API for "give me events as JSON."
  `read-events --json` covers that surface on the local box.
- **No retention policy enforcement (default).** The events table
  grows indefinitely. The default is no retention because
  workstation-scale event volume from the GitHub/Prometheus
  streams is small enough that a year of events fits comfortably
  in single-digit megabytes of SQLite. An opt-in `waitbus db-prune`
  verb (with `--dry-run` default-on, `--max-size 1GiB --max-age
  30d` validated defaults, and `--vacuum` for `VACUUM INTO` + atomic
  rename to reclaim freelist pages) is available for long-lived
  `fs_watch` / `docker_container` deployments where the workstation
  scale premise no longer holds. Operators who want bounded
  retention without the verb can still run a cron that
  `DELETE FROM events WHERE received_at < strftime('%s', 'now',
  '-30 days')`. The default stays "no retention"; the verb is the
  explicit opt-in.
- **No web UI.** waitbus is a CLI + bus, not a dashboard. The
  consumer surface assumes the operator's tools (tmux, shell
  prompt, editor) are the UI.

---

## Related Documents

- [`CHANGELOG.md`](../CHANGELOG.md): Release notes.
- [`ROADMAP.md`](../ROADMAP.md): Future work, externally gated.
- [`SECURITY.md`](../SECURITY.md): Threat model and security posture.
