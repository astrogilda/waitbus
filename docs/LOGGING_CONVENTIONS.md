# Logging Conventions

waitbus daemons emit one-line structured JSON via the single
`waitbus._log.structured(logger, level, event, **fields)` helper
(structlog and similar libraries were considered and rejected in favour of a small in-house helper so the log format stays under the project's direct control). This page is the field-naming + discipline contract
that keeps those lines greppable across modules. It is consumer-facing
in the operational sense (an operator's `jq` filter is a contract): a
field rename is a breaking change to anyone joining on it.

## 1. The event name

- `event` is a `snake_case` `verb_object` string, present on every
  call. It is the primary group-by key for log analysis.
- Event names are **not globally unique** by design. The same `event`
  recurs at distinct severities / phases (e.g. `polled` fires at INFO
  for 200/304 and WARNING for an error status; `http_transient` fires
  for both an HTTP 5xx and a URLError). A downstream consumer that
  needs to disambiguate joins on the `(event, status, path)` tuple,
  not on `event` alone. New code MAY reuse an existing `event` name
  when the semantics match; it MUST NOT invent a near-synonym for an
  existing one.

## 2. Reserved field keys

These keys have a fixed meaning. Do not repurpose them; do not
introduce a synonym for one that already exists.

| Key | Meaning | Notes |
|---|---|---|
| `error` | `str(exc)` of a caught exception | The universal error-detail field. Never a structured object. |
| `status` | An **HTTP response status code** (int) | GitHub / Alertmanager / any HTTP poll. `200`, `304`, `429`, `500`, ... |
| `code` | An **application / process exit code** (int) | NOT an HTTP status. Reserved for future exit-code logging; do not use it for HTTP. |
| `path` | An **HTTP request path** | The listener's request path (`/webhook`, `/alertmanager`, ...). NOT a filesystem path. |
| `socket_path` | An **AF_UNIX socket filesystem path** | The broadcast / doorbell / docker socket path. Distinct from `path` so a consumer joining on `path` never mixes HTTP-request-paths with socket paths. |
| `url` | A full HTTP(S) URL | The etag-poll target URL. |
| `peer` | A peer UID (int) from `SO_PEERCRED` / `getpeereid` | Broadcast-daemon subscriber identity. |
| `fd` | A socket file descriptor (int) | Broadcast-daemon subscriber fd. |
| `delivery` | A GitHub `X-GitHub-Delivery` id (str) | Listener idempotency key. |
| `repo` | An `owner/repo` slug (str) | etag-poll / pr-monitor target. |
| `attempt` | A 1-based retry attempt count (int) | Reconnect / backoff loops. |
| `backoff_s` | A sleep duration in seconds (float) | Capped-exponential backoff. |
| `retry_after` | A raw `Retry-After` header value (str) | Server-supplied backoff floor (seconds or HTTP-date). |
| `new_rows` | Count of rows inserted this tick (int) | etag-poll progress. |
| `repos` | Count of repos watched (int) | etag-poll summary. |

**`status` vs `code` is the load-bearing distinction.** Both are
integers and both look like "a status number," which is exactly why a
consumer that joins on the wrong one silently mixes HTTP statuses with
exit codes. HTTP → `status`. Application/process exit → `code`.
HTTP response status codes map to `status=` so every HTTP-status field in the codebase uses one key.

## 3. Silent-fallback rule

Every `except` block whose handler is a bare `pass` / `continue` /
silent `return` (i.e. swallows the exception without re-raising) MUST
either:

- emit a `structured(...)` line at `DEBUG` or higher describing what
  was swallowed and why it is recoverable, **or**
- carry an inline `# ` comment on the swallowing line explaining the
  deliberate silence (e.g. "double-close idempotency", "best-effort
  decode as the legacy loops did").

A swallowed exception with neither a log nor a rationale comment is a
review-blocking defect: it is indistinguishable from a forgotten
`raise`.

## 4. Levels

- `ERROR`: the daemon could not perform its core duty for this
  item (DB write failed, broadcast pass crashed). Operator action
  likely required.
- `WARNING`: a recoverable degradation the operator should see (a
  reconnect, a transient HTTP failure, a wedged-watcher backoff, a
  rejected subscribe field, a missing-DB completion).
- `INFO`: lifecycle + progress (bound, listening, polled, done,
  shutdown).
- `DEBUG`: swallowed-but-explained internal detail (per §3).

## 5. Adding a field

Before adding a NEW field key:

1. Check the table in §2. If an existing key fits, use it.
2. If it is genuinely new, add a row to §2 in the same change, with
   its type and meaning. An undocumented field key is a future drift
   source.
3. Never overload an existing key with a second meaning. If you need
   "a path but a different kind of path," introduce a distinct key
   (this is why `socket_path` exists alongside `path`).

This page is intentionally short and is enforced by review.

---

## Related Documents

- [`../README.md`](../README.md) -- project overview and quick start.
  and key files reference (this page is one of them).
- [`ARCHITECTURE.md`](ARCHITECTURE.md) -- daemon layout (where these log
  fields are emitted from).
