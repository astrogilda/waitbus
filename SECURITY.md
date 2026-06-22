# Security policy

## Reporting a vulnerability

**Preferred channel:** Use GitHub's private vulnerability reporting at
<https://github.com/astrogilda/waitbus/security/advisories/new>.
This keeps the report private until a coordinated disclosure is agreed.

**Fallback:** Email `sankalp.gilda@gmail.com` with subject `[waitbus security]`.
Please include reproduction steps, affected version, and impact assessment.

**Response timeline:** Acknowledgement within 72 hours of receipt. A fix
or status update within 30 days. We follow a coordinated-disclosure
process and request that you do not publish details until either (a) a fix
has shipped, or (b) 90 days have passed since your report, whichever
comes first.

**Release signing key:** Annotated tags are signed with the EDDSA key
`8E9996CF3080D3F3EF3106072339DC796A50C5BF`. Verify with
`git verify-tag <tag>` after importing the key from a public keyserver.

There is no bug-bounty program. Acknowledgement in `CHANGELOG.md` is the
extent of recognition.

## Threat model

waitbus is a **single-user, single-workstation daemon stack** (Linux
systemd or macOS launchd) that caches GitHub webhook deliveries and
Prometheus alerts in a loopback SQLite database. It is not multi-tenant
and does not authenticate end-users; its security perimeter is the
operator's user account.

### In scope

- **HMAC-SHA256 verification** of all inbound webhook deliveries on the
  loopback `:9000` listener:
  - `/webhook` (GitHub deliveries): HMAC keyed on the
    `github-webhook-secret` secret read from the 0600 `secrets.json`.
  - `/alertmanager`, `/watchdog` (Prometheus deliveries): HMAC keyed on
    the `alertmanager-hmac` secret, same delivery mechanism.
- **Peer-credential UID check** on every connection to the AF_UNIX
  broadcast socket: only connections from the daemon's own UID are
  accepted. Linux uses `SO_PEERCRED` (returns the connecting peer's
  `struct ucred`); macOS uses `getpeereid()` via ctypes (returns the
  EUID + EGID), matching dbus-on-Darwin's documented posture. See the
  "macOS peer-credential model" section below for the
  `LOCAL_PEERPID`-not-used rationale. This kernel-attested same-UID
  check is the entire broadcast ingress boundary: there is **no
  application-level subscribe token**, because any process that can
  reach the socket is already proven same-UID, and a bearer token would
  only re-check an identity the kernel has already attested.
- **At-rest secret protection is delegated to host full-disk encryption
  (FileVault / LUKS); local secrets rely on UNIX DAC 0600 files;
  UNIX-socket IPC relies on kernel peer-cred (SO_PEERCRED /
  LOCAL_PEERCRED).** Secrets live in one 0600 `secrets.json` under the
  state dir, readable only by the owning user; a lifted disk image is
  covered by the host FDE the operator already runs. waitbus adds no
  app-level encryption layer (that would be a second key on the same
  disk, no defense an attacker with the disk image cannot also lift),
  matching the posture of `gh auth`, `aws`, and `docker login`.
- **Bounded version-failure disclosure.** On an unsupported wire
  `proto` the daemon writes a single `subscribe_rejected` frame (see
  CONSUMER_API.md §3) before closing, so an honest operator gets a real
  protocol error instead of a bare EOF. This is not an information leak:
  the SO_PEERCRED UID gate runs at accept time, *before* the subscribe
  frame is read, so any peer reaching the version check is already
  proven to run as the daemon's own UID (and AF_UNIX exposes no network
  surface). The reply is best-effort and time-bounded (2s
  `sock_sendall`) so a slow or half-dead peer cannot stall the accept
  loop. Every request-shape reject (peer-cred UID mismatch, receive
  timeout, bad JSON, non-object envelope, bad filter/event_type/since,
  lag-limit) remains silent-EOF; operators debug those via the daemon's
  structured logs, not a client-visible error channel.
- **Filter-string regex validation** on the broadcast subscribe path
  prevents shell-metachar injection in filter values.
- **systemd hardening directives** on every shipped unit: `PrivateTmp`,
  `ProtectSystem=strict`, `ProtectHome=read-only`, `ReadWritePaths`
  limited to the systemd-managed `StateDirectory=waitbus` (i.e.
  `~/.local/state/waitbus/`) and `RuntimeDirectory=waitbus` (i.e.
  `/run/user/$UID/waitbus/`), `NoNewPrivileges`, `Protect{Kernel,
  Control}*`, `RestrictNamespaces`, `SystemCallFilter` allow-list.
- **Supply-chain signing**: every PyPI release wheel is signed via
  `sigstore-python` with the workflow's OIDC identity. The signature is
  attached to the GitHub Release alongside the wheel.

### Out of scope

- **Multi-host or multi-tenant deployment.** The broadcast socket is a
  loopback AF_UNIX endpoint, not a network listener. There is no
  remote-attestation, no TLS, no RBAC. The threat model assumes the
  workstation account is trusted.
- **Claude Desktop `.mcpb` extension surface.** Not supported. The
  packaged MCP server (`waitbus mcp serve` over stdio)
  assumes a Linux systemd-user session as the host environment.
- **Operator-supplied configuration files.** A malformed
  `~/.config/waitbus/config.toml` fails LOUD at daemon startup
  (refuses to start with a clear error pointing at the offending file).
  We do not attempt to auto-repair operator-authored config.
- **Native Windows.** Not supported. The trust model reduces to an
  AF_UNIX socket plus a `SO_PEERCRED` same-UID peer check, which has no
  Win32 equivalent. Windows users run waitbus under **WSL2** (a real
  Linux kernel) where `pip install waitbus` works exactly as on native
  Linux and the same AF_UNIX + same-UID security model holds.

### Network egress and telemetry

waitbus has **zero default egress and ships no telemetry.** The broadcast
daemon, every subscriber (`waitbus wait`, `waitbus read-events`, `waitbus mcp
serve`), and the `emit()` ingress path make no outbound network calls.
Nothing is reported to the maintainer or any third party. The only
network surfaces are:

- the **optional** inbound webhook listener on loopback
  (`127.0.0.1:9000`, HMAC-verified), which only receives;
- local 0600 `secrets.json` access for secret storage (no network);
- the **opt-in** ETag polling fallback, which calls the GitHub API only
  for the repos the operator explicitly lists in `watched_repos.txt`; and
- the **opt-in** broadcast-daemon metrics endpoint (`WAITBUS_METRICS_PORT`
  / `waitbus broadcast serve --metrics-port`), which binds `127.0.0.1`
  only and serves read-only Prometheus text; no socket is opened unless
  the operator sets it.

No code path sends data anywhere the operator has not configured it to.

### Optional inbound metrics endpoint

The broadcast daemon's `/metrics` endpoint is off by default and its
loopback bind is hardcoded: there is no configuration path to a public
interface. The surface is read-only GET: no request body is consumed and
no state is mutated. The exposition carries operational counters,
gauges, and histograms only, with no event payloads or repository content
(the broadcast metrics carry no repo-name labels). Intended consumers
are same-host scrapers: a local Prometheus or the operator's `curl`.

### Reliance on a vendor-specific MCP extension

The `waitbus mcp serve` server emits each broadcast event as TWO MCP
notifications:

- `notifications/claude/channel`: a vendor-specific MCP extension
  used by Claude Code. Surfaces in Claude Code as a
  `<channel source="waitbus">` injection in the next conversation
  turn. **This is not part of the public MCP spec.** Documented at
  the Anthropic-private reference
  `https://code.claude.com/docs/en/channels-reference` and emitted via
  the official `mcp` Python SDK's `ServerSession.send_message`
  low-level escape hatch (which the SDK itself marks as experimental
  and subject to change without notice). If the extension is renamed,
  removed, or the SDK closes the `send_message` path, the Claude Code
  channel integration silently stops working: the server keeps
  running, but Claude Code no longer surfaces the events as channel
  injections.
- `notifications/resources/updated`: a standard MCP notification.
  This fallback continues to function for Claude Desktop and every
  other spec-compliant MCP client regardless of changes to the
  vendor-specific method.

The fallback path is unconditional: every frame is sent on both
methods, so a regression in the experimental method does not break
the standard-MCP consumer path. A 30-day SDK refresh cadence is the
operational mitigation; the
two-tier wire fixture under `tests/data/mcp_wire_*.jsonl` is the
automated regression fence.

### Secret storage: a 0600 JSON file

waitbus stores HMAC secrets in one `secrets.json` object at
`<state-dir>/secrets.json` (Linux default
`~/.local/state/waitbus/secrets.json`; macOS
`~/Library/Application Support/waitbus/secrets.json`), mode 0600. The
operator stages each secret once via:

```
waitbus install-credentials <name> [--file PATH]   # or value on stdin
```

which reads the value from `--file` or stdin (never an inline `--value`
flag, which would leak to shell history), merges it under the key
`<name>` without clobbering siblings, and writes the file atomically
(`secrets.json.tmp` chmod-0600 *before* the payload, then `os.replace`,
so a concurrent daemon never reads a torn file). The daemon reads it via
stdlib `json.loads`: there is no in-process key material, no D-Bus
session, no external tool, no Python `cryptography` dependency.

**At-rest protection is delegated to host full-disk encryption**
(FileVault on macOS, LUKS on Linux) **plus UNIX discretionary access
control** (the 0600 file is readable only by the owning user). waitbus
deliberately adds no app-level encryption layer: an app key sealed next
to the ciphertext on the same disk is no defense against an attacker who
can lift the disk image (they lift the key too), and host FDE already
covers the lifted-image threat. This matches the posture of `gh auth`,
`aws`, and `docker login`, which all rely on 0600 files + host FDE
rather than a redundant app-level cipher. A future always-on server
deployment with a TPM can re-add a host-sealed backend behind the same
one-function `_secrets.get_secret` seam without a daemon rewrite.

The threat model is **single-user single-workstation**:

- **In scope.** Confidentiality at rest on disk (host FDE) and
  confidentiality of the secrets file on a multi-user host (0600 DAC,
  readable only by the owning user).
- **Out of scope.** A same-UID adversary (e.g., malicious shell command
  the operator runs) can read `secrets.json` and can call
  `waitbus install-credentials` to rotate. The SQLite event store at the
  platformdirs-resolved state path (Linux default
  `~/.local/state/waitbus/github.db`) is `chmod 600` but otherwise
  relies on the operator's home-directory ACLs.

### Events-query SQL passthrough

`waitbus events query <SQL>` lets the operator run a literal SQL
statement against the local events SQLite database. The surface is
trust-the-operator-by-design: this is a single-user workstation tool
and the operator is also the only caller. Two safety properties hold:

- **Read-only connection.** The DB is opened via
  `file:...?mode=ro`, so every write DDL/DML fails at the SQLite
  layer regardless of what the parsed SQL says.
- **Parse-time statement-kind gate.** Only `SELECT` and `WITH`
  (CTE-rooted SELECT) statements pass; `INSERT` / `UPDATE` /
  `DELETE` / `DROP` / `CREATE` / `ALTER` / `REPLACE` / `VACUUM` /
  `ANALYZE` / `REINDEX` are rejected before any connection work
  happens. `PRAGMA` / `ATTACH` / `DETACH` are additionally
  rejected anywhere in the statement (defense in depth, since these
  can mutate connection state without writing rows). Multi-
  statement input is rejected; a single trailing `;` is tolerated.

The operator writes literal SQL with no parameterised binds.
SQL-injection is moot because (a) there is no untrusted caller to
inject through, and (b) the read-only connection blocks the
post-injection write that would matter. The injection-style gates
exist to catch operator typos (`DELETE` for `SELECT`) before they
reach a DB that, in a future code path, might no longer be
read-only.

A trailing `LIMIT N` is injected at the outer level (default 1000)
or an existing outer LIMIT is capped at `min(N, default)`. Operators
who need an unbounded scan pass `--no-limit` and own the runtime.

### Schema-migration trust model

`waitbus/migrations/*.py` files (and the `.sql` peers they may
shell out) execute arbitrary Python and SQL against the operator's
local SQLite events database whenever the daemon detects a schema
version drift on startup. The migration runner is intentionally
permissive: any Python file under `waitbus/migrations/` with
a `migrate(conn: sqlite3.Connection)` entry-point becomes a code-
execution surface bundled into the wheel.

Two controls bound this trust:

- **CODEOWNERS gate.** `.github/CODEOWNERS` requires explicit reviewer
  approval (`@astrogilda`) on any change to
  `waitbus/migrations/*.py` before a PR can merge. This makes
  introducing a malicious migration via opportunistic PR observably
  hard: every diff against the migrations tree blocks until owner-sign.
- **SQL-only-when-feasible policy.** New migrations SHOULD ship as a
  pure `.sql` file invoked from a one-line Python wrapper
  (`conn.executescript(Path(__file__).with_suffix(".sql").read_text())`)
  rather than as arbitrary Python. The Python surface stays available
  for schema changes that genuinely need procedural logic (data
  reshape, conditional branches based on row content), but pure DDL
  must take the SQL path. This narrows the audit surface to declarative
  SQL the reviewer can read top-to-bottom in seconds.

Operators consuming the wheel from PyPI rely on the PEP 740 attestation
chain (sigstore + SLSA build provenance via the L3 generator workflow +
CycloneDX SBOM) to verify the wheel they install matches the audited
source. Note: builds run on GitHub-hosted runners which are shared
infrastructure, so the pipeline is best described as SLSA L2 with
strong L3-leaning controls (signed provenance, builder identity
pinned, reproducible sdist), not full SLSA L3 (which requires builder
isolation we do not claim). The `waitbus db migrate --dry-run`
subcommand prints the SQL/Python that would run before applying any
changes, giving the operator a chance to review at install time.

### Event-driven command execution (`waitbus on`)

`waitbus on <predicate> -- <argv>` blocks until an event matches the
predicate, then runs an operator-supplied command with the matched
event handed to it as context. This is intentional command execution
on the operator's machine -- the watchexec / entr adjacency -- and is
trust-the-operator-by-design, like the SQL passthrough and migration
surfaces above. Three properties bound the surface so that an event or
predicate an agent or LLM can influence cannot escalate into command
injection:

- **No shell; argv-vector exec.** The command is the operator-supplied
  argv, executed without a shell. There is no shell-metacharacter
  surface and no word-splitting of untrusted data into the command
  line.
- **Event fields never reach argv.** The matched event is passed only
  through the environment and an events file (`$WAITBUS_EVENT_FILE` plus
  scalar `WAITBUS_*` convenience variables); field values are never
  interpolated or substituted into the command line. A hostile or
  attacker-influenced event payload therefore cannot inject a command
  -- the worst it can do is place hostile *content* in the event file
  that the operator's own command chose to read.
- **Foreground only; the daemon never execs.** The command runs in the
  foreground CLI process the operator launched, never in the broadcast
  daemon (the daemon owns no subprocesses, by design). `--loop
  --restart` runs each child in its own process group and tears it down
  with `SIGTERM` -> `--stop-timeout` grace -> unconditional `SIGKILL`.

Residual: a grandchild that `setsid()`s into its own session escapes
the group `SIGKILL` (only cgroup v2 `cgroup.kill` is fully leak-proof;
out of scope for a single-user workstation tool). The operator owns
whatever argv they pass, exactly as they own the argv of any shell
command they type -- the guarantee is that waitbus adds no injection
channel on top of that.

### Inter-agent confidentiality (addressed messaging)

waitbus ships addressed agent-to-agent messaging (`request` / `respond`,
`subscribe(to=)`, and the `msg_*` event facet). **The bus provides no
inter-agent confidentiality or authenticity at the same UID, by design.**
The `msg_to` / `msg_from` addressing keys are *names, not credentials*: any
same-UID peer can subscribe to another agent's inbox, read every broadcast
frame, and emit a frame that names any `msg_from`. This is sound only
because the kernel UID boundary is the trust boundary -- `SO_PEERCRED`
(Linux) / `getpeereid()` (macOS) authenticate the user, and a same-UID peer
that could forge a name can already read every peer's socket, keys, and
memory directly (see the same-UID notes above and the "macOS
peer-credential model" section below). It matches MCP's STDIO transport and
the Akka / Erlang actor model, which treat local names as addresses, not
credentials.

**Future trigger (when the reserved hardening ladder becomes mandatory).**
A hardening ladder -- pre-shared HMAC + nonce, then an EdDSA-signed
envelope, then a capability token -- is deliberately *not built* today
because the single-UID model makes it redundant. The moment waitbus crosses
that boundary -- **any network transport, or a multi-UID deployment where
agents run under different users** -- that ladder becomes **mandatory, not
optional**: addressing names stop being backed by the kernel UID gate, and
an unauthenticated `msg_from` becomes a real spoofing surface. A
network or multi-UID tier MUST NOT ship without it.

### Known acceptable risks
- **No rate-limiting on `/webhook`.** The listener trusts the
  HMAC-verifying upstream (typically `gh webhook forward` or a vetted
  Prometheus alertmanager). A compromised upstream could exhaust the
  loopback HTTPS server's accept queue; mitigation is to restart the
  listener.
- **Broadcast frame size cap (64 KiB).** Frames exceeding this are
  truncated to a `{"kind":"truncated", ...}` stub and the consumer is
  expected to re-fetch via `read_events.py --json --last-n 1`. No
  outbound rate-limiting on the broadcast fan-out path.

## macOS peer-credential model

On macOS the broadcast daemon authenticates connecting subscribers
via `getpeereid()` (UID-only). `LOCAL_PEERPID` is deliberately not
used; PID-based identity is exploitable per CVE-2017-7004 (Ian Beer,
2017), CVE-2020-14977 (F-Secure / Reguła / Alkemade, 2020), and
Apple's AMFI tightening trail.

The single-user-workstation threat model assumes peer = self; a
same-UID compromise (e.g., a malicious tool running in the operator's
account) trivially bypasses any peer-credential check anyway, so the
UID-only posture is sufficient. Operators concerned about same-UID
attack vectors should not rely on the broadcast bus as a trust
boundary.

The ctypes binding for `getpeereid()` is constructed at module-import
time via `ctypes.CDLL(None, use_errno=True)`. The `argtypes` /
`restype` triplet matches the documented signature
`getpeereid(int s, uid_t *euid, gid_t *egid)`; a regression in the
binding surfaces as either a `TypeError` at call time (caught by
`tests/test_peercred.py::test_getpeereid_ctypes_signature`) or as
silent corruption of the return values (caught by the
same-process-socketpair test that asserts the recovered UID equals
`os.getuid()`).

## Deployment topology

The listener binds `127.0.0.1:9000` by default and accepts unauthenticated
HTTP from anything on the loopback interface. Two supported deployment
modes:

**Mode A: Reverse proxy (recommended).** A TLS-terminating reverse
proxy (cloudflared, tailscale-funnel, local nginx) terminates external
TLS and proxies decrypted HTTP to `127.0.0.1:9000`. The proxy is
expected to enforce hardening that the listener does not bother
replicating (HSTS, redirect-loop guards, TLS 1.3 floor). The listener
trusts the proxy to deliver well-formed Content-Length-framed requests.

**Mode B: LAN-isolated workstation.** The operator runs waitbus
on a dev box reachable only over WireGuard or Tailscale (not the public
internet). GitHub Enterprise or a self-hosted Prometheus instance
delivers webhooks directly to `127.0.0.1:9000` across the encrypted
tunnel without a reverse proxy. In this mode the listener's built-in
hardening is the primary defense rather than belt-and-suspenders. The
defaults are tuned for Mode B; Mode A is safer because the proxy adds
a second filtering layer.

Both modes share:

- HMAC-SHA256 verification on `/webhook` and `/alertmanager`
  (constant-time via `hmac.compare_digest`)
- `REQUEST_READ_TIMEOUT_SEC=30` slow-loris guard applied before both
  header parsing and body reading
- `MAX_BODY_BYTES` payload cap (10 MiB)
- Transfer-Encoding chunked rejection (411), which prevents cap bypass
- Duplicate Content-Length rejection (400) per RFC 9112 §6.3.3
- Method allowlist with JSON 405 for HEAD, OPTIONS, PUT, DELETE, PATCH
- JSON error envelopes on all 4xx/5xx responses (no HTML pages)
- No `Server` response header (info-disclosure prevention)

## Verifying release artifacts

waitbus emits three independent supply-chain attestations on every
release. Verify any of them on a fresh machine without trusting the
local install path:

**SLSA build provenance (slsa-github-generator v2.1.0):**

```bash
gh attestation verify <artifact> \
  --repo astrogilda/waitbus \
  --signer-workflow slsa-framework/slsa-github-generator/.github/workflows/generator_generic_slsa3.yml@v2.1.0
```

The `--signer-workflow` assertion is load-bearing: it pins which builder
emitted the provenance, preventing a compromised contributor from
substituting their own workflow output.

**PEP 740 PyPI attestations (sigstore-keyless):**

```bash
python -m sigstore verify identity waitbus-<version>-py3-none-any.whl \
  --cert-identity https://github.com/astrogilda/waitbus/.github/workflows/release.yml@refs/tags/v<version> \
  --cert-oidc-issuer https://token.actions.githubusercontent.com
```

**CycloneDX SBOM attestation (GitHub Artifact Attestations API):**

```bash
gh attestation verify <artifact> \
  --repo astrogilda/waitbus \
  --predicate-type https://cyclonedx.org/bom
```

All three attestations land in the public Rekor transparency log; the
`gh` CLI fetches them at verify time without operator-side key storage.

## Supported versions

We backport security fixes only to the most recent minor release.
Older versions are end-of-life.

| Version | Supported |
|---|---|
| 0.1.x | YES |
| < 0.1.0 | no |

## Disclosure timeline

| Time | Action |
|---|---|
| T+0 | Report received |
| T+3 days | Acknowledgement (within 72 hours) |
| T+30 days | Fix prepared and tested |
| T+60 days | Public release with `CHANGELOG.md` advisory |
| T+90 days | Reporter may publish details (or sooner if patched and disclosed) |
