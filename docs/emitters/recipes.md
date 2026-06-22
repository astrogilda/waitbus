# Shell and docker emit recipes

Small, copy-pasteable ways to put events on the bus from a shell. Every
recipe needs the daemons running (`waitbus serve --all` or the systemd
units). An emitter with no daemon emits into the void.

Before reaching for a recipe, check the built-in sources:

- **docker**: the in-tree watcher (`waitbus/sources/docker_watch.py`)
  already tails the Docker events API with reconnect-and-replay; the
  forwarder recipe below is the manual / remote-host variant only.
- **pytest**: `waitbus/sources/pytest_emit.py` ships as a worked
  emitter example: a pytest plugin that emits on session finish. Read
  it before writing a new emitter; it shows the deterministic
  `delivery_id` and batching conventions in ~200 lines.
- **agent lifecycle hooks**: see
  [claude-code-hook.md](claude-code-hook.md) for the Claude Code
  session-lifecycle emitter.
- **GitHub Actions**: the zero-setup path is the built-in `github`
  source; for the deliberate push-from-the-runner case there is a
  relay-action skeleton at
  [`examples/emitters/github_action/`](../../examples/emitters/github_action/README.md).

The shell recipes use the `agent` source vocabulary: `agent_message`
for a successful completion, `agent_task_failed` for a nonzero exit,
with the synthetic `owner=local` convention the in-tree local sources
established. The `waitbus emit` CLI does not expose the `msg_*`
addressing columns; a producer that needs those uses the Python API
(`waitbus.emit` with an `EventInsert` row, as the
[Claude Code lifecycle hook](claude-code-hook.md) emitter does) instead.

The fenced blocks below are extracted and verified by
`tests/test_emitter_recipes.py` (shellcheck on all three; the two
non-docker recipes also execute against a throwaway store), so they
cannot drift from the CLI surface silently.

## Command-finished emit

Run a command, then emit one event carrying its exit code. Replace
`make build` with your command (in both the run line and the
`WAITBUS_CMD` payload text). The payload is built by `python3` (already
on every waitbus host) with `json.dumps` over environment variables, so
a command containing double quotes or backslashes still produces valid
JSON. Raw shell interpolation into a JSON template would silently
break on those characters.

<!-- recipe:command-finished -->
```bash
make build
rc=$?
if [ "$rc" -eq 0 ]; then event_type=agent_message; else event_type=agent_task_failed; fi
payload=$(WAITBUS_CMD="make build" WAITBUS_RC="$rc" python3 -c \
  'import json, os; print(json.dumps({"command": os.environ["WAITBUS_CMD"], "exit_code": int(os.environ["WAITBUS_RC"])}))')
waitbus emit \
  --delivery-id "shell:build:$(date +%s%N)" \
  --source agent \
  --event-type "$event_type" \
  --owner local \
  --repo shell \
  --received-at "$(date +%s)" \
  --payload-json "$payload" \
  --ingest-method shell
```

Then any consumer can block on it:

```sh
waitbus wait --source agent --timeout 30m
```

## Long-job wrapper

A wrapper script that runs an arbitrary command, emits the matching
event, and exits with the wrapped command's status (so it composes
with `&&` / `set -e` exactly like the bare command). Because the
wrapped command is arbitrary, the payload is built by `python3` with
`json.dumps` over the environment and the surviving `"$@"` argv, so
arguments carrying double quotes or backslashes round-trip exactly,
where raw shell interpolation into a JSON template would emit invalid
JSON and (with the emit's output discarded) silently lose the event.

<!-- recipe:long-job-wrapper -->
```bash
#!/usr/bin/env bash
# usage: waitbus-run <job-name> <command> [args...]
set -u
job_name="$1"
shift
"$@"
rc=$?
if [ "$rc" -eq 0 ]; then event_type=agent_message; else event_type=agent_task_failed; fi
payload=$(WAITBUS_JOB="$job_name" WAITBUS_RC="$rc" python3 -c \
  'import json, os, sys; print(json.dumps({"job": os.environ["WAITBUS_JOB"], "command": " ".join(sys.argv[1:]), "exit_code": int(os.environ["WAITBUS_RC"])}))' \
  "$@")
waitbus emit \
  --delivery-id "shell:${job_name}:$(date +%s%N)" \
  --source agent \
  --event-type "$event_type" \
  --owner local \
  --repo shell \
  --received-at "$(date +%s)" \
  --payload-json "$payload" \
  --ingest-method shell >/dev/null
exit "$rc"
```

The doorbell ring inside `waitbus emit` is best-effort: if the
broadcast daemon is down, the row still commits and is delivered on
the daemon's next sweep: a bounded delay, never a lost event.

## docker events forwarder

The built-in docker source is the default path (it reconnects and
replays gaps). This one-liner is for the cases the watcher does not
cover, such as a remote host streaming into a local bus over SSH, or a
one-off debugging tail. It reuses the watcher's
`docker:<id>:<action>:<timestamp>` delivery-id scheme, taking the
timestamp from the event's own `TimeNano` (the same value the watcher's
`_received_at_ns` encodes into its delivery id), so the two paths
genuinely dedup against each other when both see the same event. Note
the scope difference: this recipe filters `event=die` only, while the
in-tree watcher also emits on `stop` and `kill`.

<!-- recipe:docker-events-forwarder -->
```bash
docker events --filter type=container --filter event=die --format '{{.ID}} {{.Status}} {{.TimeNano}} {{json .}}' |
  while IFS=' ' read -r cid action tnano payload; do
    waitbus emit \
      --delivery-id "docker:${cid}:${action}:${tnano}" \
      --source docker \
      --event-type docker_container \
      --owner local \
      --repo docker \
      --received-at "$(date +%s)" \
      --payload-json "$payload" \
      --ingest-method docker_events
  done
```

## CloudEvents envelope

Every stored event projects onto a CloudEvents v1.0 envelope
(`waitbus/_cloudevents.py`; `waitbus emit --format cloudevent` prints
it). The projection promotes four columns:

| CloudEvents attribute | waitbus column | Notes |
|---|---|---|
| `id` | `event_id` | ULID, generated by the bus at insert |
| `source` | `source` | rendered as `urn:waitbus:source:<name>` |
| `type` | `event_type` | projected verbatim |
| `time` | `received_at` | epoch ns rendered RFC3339 (truncated to microseconds) |
| `datacontenttype` | (none) | fixed `application/json` |
| `data` | every remaining column | lossless remainder |

An external producer holding a CloudEvents envelope maps it *inversely*
onto an emit call: use the envelope's `id` as the `delivery_id`
idempotency key (the bus generates a fresh ULID `event_id`), `type` as
`event_type`, the URN suffix as `--source`, `time` scaled to epoch
nanoseconds as `received_at`, and the serialized `data` object as
`payload_json`. For producers that cannot install waitbus at all, the
dependency-free [`docs/snippets/minimal_emitter.py`](../snippets/minimal_emitter.py)
shows the raw store-plus-doorbell emit path the CLI wraps.
