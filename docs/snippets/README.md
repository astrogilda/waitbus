# waitbus wire snippets

Minimal end-to-end subscriber code, one file per language, sharing one
wire contract, plus one dependency-free emitter. The Python subscriber
file is authoritative; the other subscribers mirror it
line-for-line where the target language allows. They all do the same
thing: connect to the broadcast daemon's AF_UNIX socket, send a
`subscribe everything` envelope, and print each event frame's
`delivery_id`, `source`, and `event_type` as it arrives.

## Repository colocation

The subscriber wire protocol (4-byte big-endian length prefix + UTF-8
JSON payload, max 65536 bytes per frame) is defined by
`waitbus/_frame.py`. The snippets live in the same repository so
the wire contract and its consumer examples ship together; a
backwards-incompatible change to `_frame.py` is rejected by
`tests/test_multilingual_snippets.py` at the same commit that
introduces it. A sibling `waitbus-examples` repo would invite drift.

The snippets cover **both directions** of the local wire surface:
subscribe (the read-side broadcast protocol) and emit
(`minimal_emitter.py`, a named-column `INSERT OR IGNORE` into the
events store followed by a best-effort doorbell ring; this is the
supported external emit path). Listener and daemon implementations stay
in-tree only; this directory remains the sole sanctioned external
wire surface.

## Files

| Language   | File                        | How to run                                                  |
|------------|-----------------------------|-------------------------------------------------------------|
| Python     | `minimal_subscriber.py`     | `python docs/snippets/minimal_subscriber.py`                |
| Rust       | `minimal_subscriber.rs`     | `rustc minimal_subscriber.rs -o /tmp/ms && /tmp/ms`         |
| Go         | `minimal_subscriber.go`     | `go run minimal_subscriber.go`                              |
| TypeScript | `minimal_subscriber.ts`     | `node --experimental-strip-types minimal_subscriber.ts`     |
| Bash       | `wait_for_any_source.sh`    | `./wait_for_any_source.sh [timeout]`                        |
| Python (emit) | `minimal_emitter.py`     | `python docs/snippets/minimal_emitter.py "message"`         |

The bash file is not a subscriber from scratch; it wraps `waitbus wait
--match` three times in parallel (one per source) and exits as soon as
the first match arrives. It demonstrates the cross-source predicate
surface without re-implementing the wire protocol.

## Wire contract (canonical)

Documented in `waitbus/_frame.py`. In short:

1. Connect to AF_UNIX SOCK_STREAM at `$WAITBUS_BROADCAST_SOCKET`, or the
   default `$XDG_RUNTIME_DIR/waitbus/broadcast.sock` (Linux) /
   `$HOME/Library/Application Support/waitbus/broadcast.sock` (macOS).
2. Send a length-prefixed frame containing the UTF-8-encoded JSON
   subscribe envelope. The envelope **should** include `"proto": 1`
   (omitting it is accepted today for backward compatibility with
   v0.4-era subscribe frames and is treated as v1 implicitly; sending
   it explicitly lets a future v2-only daemon cleanly reject a v1
   client instead of running it silently against a v2 wire). The
   minimal envelope is `{"proto": 1}` (for "everything from now"; add
   `"filters"` or `"event_types"` keys to narrow). The daemon responds
   with a `subscribe_ack` control frame, or closes the connection with
   a `subscribe_rejected` frame on version or token error.
3. Read length-prefixed frames in a loop. Dispatch on the string field
   `kind`:
   - `"event"`: a real event frame; fields are `event_id` (ULID),
     `event_type`, `owner`, `repo`, `received_at`, `delivery_id`,
     `summary`, `fields`.
   - `"truncated"`: the event exceeded the wire cap (`max_frame_bytes`);
     re-fetch the full payload by `event_id` via the SQL/CLI surface.
   - `"daemon_heartbeat"`, `"subscribe_ack"`, `"subscribe_rejected"`:
     control frames.

   The minimal rule: **skip any frame whose `kind` is not `"event"`**
   (`if kind != "event": continue`). This correctly ignores all control
   frames and truncated stubs. The three fields these snippets read
   (`delivery_id`, `event_type`, `fields.source`) are stable across the
   v1 wire protocol.

## Protocol version pinning

Each snippet validates against the protocol via
`tests/test_multilingual_snippets.py`, which compiles (or `shellcheck`s)
the file and runs it against a `running_daemon` fixture. The compile
step on `ubuntu-latest` runners uses the pre-installed `rustc` / `go`
/ `node` / `shellcheck` binaries; self-hosted runners without those
toolchains hit the skip-with-reason path.
