# 24-hour soak test

This document describes how to run the waitbus soak test.
A soak run verifies longevity before a new release; the verdict is
informational and does not publish anything automatically.

The soak is the **longevity** layer. The complementary **robustness**
layer lives in `tests/test_broadcast_robustness.py` and the rest of the
`tests/test_broadcast*` suites, with the per-defect track record at
[`ROBUSTNESS_TESTS.md`](ROBUSTNESS_TESTS.md). The two layers answer
different questions and run on different cadences: robustness proves
correctness invariants in seconds on every push, while the soak proves
resource stability over a continuous-uptime window.

## Soak test invariants

A 24-hour synthetic mixed-source replay against the broadcast daemon
holds the following invariants:

| Signal | Pass threshold | Why |
|--------|----------------|-----|
| RSS slope | <= 0.5 MiB/hr (linear-regression over all samples) | Catches monotonic memory leaks across the realistic continuous-uptime window of a workstation daemon. |
| RSS peak | <= 2x initial | Catches transient spikes the slope can hide. |
| FD count delta | final - baseline <= 5 | Catches descriptor leaks. |
| FD peak | <= 2x baseline | Catches transient descriptor leaks. |
| WAL peak | <= 100 MiB | Catches checkpoint-failure regressions. |
| WAL final delta | <= initial + 5 MiB | Catches monotonic WAL growth. |
| Suspend recovery: p99 ratio | post / pre in [0.85, 1.15] | Each workstation-suspend cycle recovers to pre-suspend latency. |
| Suspend recovery: integrity | `PRAGMA integrity_check` returns `ok` | SQLite survives SIGSTOP / SIGCONT. |
| Suspend recovery: lost events | 0 post-SIGCONT | Events emitted AFTER resume are not dropped. |

## Duration rationale

waitbus is a workstation daemon. Realistic continuous uptime is
hours-to-days (laptop sleeps, kernel reboots, IDE restarts), not
weeks. A 24-hour soak rigorously covers the realistic operational
window: at 5 events/sec the run produces ~432,000 events with 1,440
samples (at the default 60-second sample cadence). Memory, FD, and
WAL signals manifest within that window if present. The bugs a
7-day soak catches and a 24-hour soak misses (sub-10-MB/day drip
leaks) are extremely rare in stdlib-Python code and would surface in
the OSS feedback loop within days of a user running waitbus
continuously. These would surface in user feedback and can be addressed
in a patch release if reported.

Distributed-broker projects (Envoy, NATS, Pulsar) run multi-week
soaks. Their deployment shape (always-on server) is different from
waitbus's (workstation daemon).

## Out of scope

The soak verdict is informational. It does NOT:

- Push a tag, publish a wheel, or publish any artifact.
- Trigger a release workflow.
A passing soak is one signal that the project is ready for a release;
publishing steps remain manual.

## Running the soak

### Drain-path smoke pre-phase (runs automatically before every soak)

Every `scripts.soak` invocation first runs a fast (~7 s) **drain-path smoke
pre-phase** against a *throwaway* daemon: a separate short-lived broadcaster
in its own temp dirs with an aggressive sub-second heartbeat. It seeds a
backlog and drives all four subscriber-lifecycle drain paths (`token_reject`,
`version_reject`, `replay_lag_eviction`, and `heartbeat_lag`), then verifies
coverage and that every wire-observed eviction matches the daemon's internal
`subscriber_closed` reason tally. **If the pre-phase fails, the soak aborts
before the measured run starts** (writes a failure verdict, exits 1). This is
the "smoke must pass before the long soak" gate, self-contained in one
invocation and folded into the final verdict as the `drain_smoke_coverage` and
`drain_smoke_close_reason_consistency` signals.

The throwaway daemon exists because the measured daemon is pinned to a
3600-second heartbeat (so heartbeat frames never disturb the RSS/p99
measurements), which makes the `heartbeat_lag` eviction path unreachable
against it. The pre-phase's aggressive heartbeat fires that path without
touching the measured run. Pass `--skip-drain-smoke` to run the measured loop
in isolation (debugging only).

### Local smoke (sub-minute; develop the harness)

```bash
python -m scripts.soak --duration 60s --rate 5 --output /tmp/soak.json
```

### Hetzner pre-24h smoke (recommended before the full 24h)

A pre-24h smoke run on the same Hetzner CCX23 hardware shape the 24h
run will use validates the new daemon control flow under real steal-time,
kernel-config, and network-stack characteristics. The local-process
smoke above does not give that signal. The recommended pattern:

```bash
# 30-minute pre-24h smoke with fault injection on the same hardware
# shape the 24h soak will use. If the verdict's overall_passed=true,
# proceed to the 24h run; otherwise investigate before burning 24h.
scripts/run_soak_on_hetzner.sh \
    --duration 30m \
    --inject-fault-scenarios standard \
    --server-type ccx23
```

The `--inject-fault-scenarios standard` flag schedules three
subscriber-lifecycle probes during the run (token reject at 2h, version
reject at 4h, replay-lag eviction at 6h; adjust offsets via
`scripts/soak/_context.py::_STANDARD_FAULT_INJECTIONS` if you want them
to fire inside a shorter duration). For a sub-minute developer iteration,
`--inject-fault-scenarios fast` fires every probe within the first 30
seconds.

The `fault_injection_coverage` verdict signal joins the existing eight
signals; `overall_passed` requires it to pass too. An axis that cannot
exercise its arm in the deployment (e.g. token reject with no token
configured) is recorded as `skipped_intentionally=true` and counts
toward coverage. An axis with `observed=false` AND
`skipped_intentionally=false` fails the verdict.

### Full 24-hour soak on a tuned host

Recommended host: a fresh Hetzner CCX23 VM (4 dedicated vCPU, 16 GB, ~EUR 0.087/h). Matches the canonical benchmark-baseline hardware in `benchmarks/BENCHMARKING.md` so soak verdicts and benchmark numbers are cross-comparable on the same hardware shape. **Avoid the shared-vCPU lines (CX, CPX, CAX) per `benchmarks/BENCHMARKING.md:200`**: shared vCPU contributes noisy-neighbour steal time that defeats the p99-drift signal. Provide a Hetzner Cloud API key via your preferred secret manager / the `HCLOUD_TOKEN` env var before running hcloud commands.

```bash
# Provision via the hcloud CLI; soak runs under systemd-run --user --scope.
hcloud server create --type ccx23 --image ubuntu-24.04 --name waitbus-soak
```

A local machine is an acceptable alternative for overnight runs.

On the soak host:

```bash
# Clone, install, then run:
git clone https://github.com/astrogilda/waitbus.git
cd waitbus
uv sync --all-extras

# Two-mode with standard suspend cycles:
nohup python -m scripts.soak \
    --duration 24h \
    --rate 5 \
    --inject-suspend-cycles standard \
    --output soak-verdict.json > soak.log 2>&1 &

# Watch progress:
tail -F soak.log

# When done:
jq '.overall_passed, .verdicts' soak-verdict.json
```

### After the soak

1. Inspect `soak-verdict.json` for per-signal verdicts.
2. If `overall_passed` is false: investigate the failing signal in
   the sample series (each sample's RSS / FD / WAL is in the JSON).
3. If `overall_passed` is true: the soak criterion is met. Other readiness checks may apply
   (benchmarks, integration tests, demo repo).
4. Publishing a release is a separate manual step. The
   `waitbus broadcast serve` daemon, the `waitbus stats` CLI, and the
   GitHub-side publish workflow run only on operator-triggered tag
   pushes; the soak never pushes anything.

## Workstation-suspend data loss

The soak's three suspend-cycle scenarios surface a real waitbus
deployment limitation: events that arrive at the HTTP webhook
listener DURING the SIGSTOP window are 500'd by the kernel (the
listener is frozen alongside the daemon). This is not a soak bug;
it is the deployment shape's inherent constraint. Mitigations
available to operators:

- GitHub webhook redelivery covers the window for github events.
- The `waitbus replay` CLI reconciles SQLite state against GitHub's
  API on resume, papering over short freezes.

The soak proves recovery; it does NOT claim freeze-window
durability.

## Corpus replay contract

The `--corpus PATH` mode replays a gzipped JSONL event corpus
generated by `python -m benchmarks.gen_corpus`. The producer
(`benchmarks._harness.replay_corpus`) yields one of:

- `dict[str, Any]` for a well-formed JSON line, or
- `None` for a line that fails `json.loads` (truncated, mid-soak
  schema bump, manual edit).

The soak consumer (`scripts/soak::_emit_corpus_event`) pattern-
matches `event is None` to (a) emit a one-time stderr warning via
`state.json_decode_warned`, (b) increment
`accums.corpus_decode_fallthroughs` for the verdict-doc surface, and
(c) fall back to a synthetic emit so a single malformed line does
not abort the 24-hour run.

The contract is structurally pinned by `tests/test_corpus_property.py`
(Hypothesis property test asserting `_emit_corpus_event` never raises,
emits exactly one row per call, and increments the fallthrough counter
iff the input is `None`).

---

## Related Documents

- [`../README.md`](../README.md) -- project overview and quick start.
  and tool idioms (where the soak runner fits into the gate matrix).
- [`ROBUSTNESS_TESTS.md`](ROBUSTNESS_TESTS.md) -- the complementary
  correctness layer: Hypothesis state machine plus an etcd-style
  per-defect reproduction track record.
