# waitbus benchmark methodology

This document is the **executable methodology contract** for every
script under `benchmarks/`. It is intentionally separate from the
code so the methodology can be reviewed independently. Substantive
changes are recorded in the CHANGES section at the bottom; editorial
changes (typos, link rot, formatting) are not.

This methodology follows established benchmark practice: open-loop
generation to avoid Coordinated Omission (Gil Tene), >=5000 samples
for a p99 with bounded CI, HdrHistogram recording, and
MEASURED/ESTIMATED/SELF-REPORTED labeling (METR). The load-bearing
references cited throughout are external and durable.

---

## 1. The core posture

waitbus is a single-machine event-bus daemon. The benchmark suite
measures three things across four event sources (github webhooks,
pytest, Docker, filesystem):

1. **Source-ingress-to-subscriber-recv latency (TTFAE).** The
   wall-clock interval from a defined per-source ingress event to
   the moment a subscriber's `recv()` returns the corresponding
   broadcast frame. **Per source**, never as a single cross-source
   average.
2. **Polling counterfactual latency.** The wall-clock between a
   state change and a polling agent observing it, for each source's
   polling alternative (`gh run watch` default 3s for github, etc.).
   This is the comparison the launch articles cite, not "we are
   faster than another daemon."
3. **Operational properties** (idle RSS, throughput,
   memory/FD/WAL drift under sustained load). One bench script per
   property; thresholds documented in this file.

The harness in `benchmarks/_harness.py` is the executable expression
of the rules below. Every bench script MUST go through the harness;
ad-hoc timing loops are forbidden.

---

## 2. Coordinated Omission (the canonical methodology trap)

**Coordinated Omission (CO)** is the systematic under-sampling of
the latency tail that happens when a closed-loop load generator
("send, then wait for response, then send next") stalls. Gil Tene's
"How NOT to Measure Latency" Strange Loop talk
(https://www.youtube.com/watch?v=lJ8ydIuPFeU) is the canonical
reference; see also `giltene/wrk2`
(https://github.com/giltene/wrk2) for the reference open-loop
implementation.

The fix the harness enforces:

- **Open-loop scheduler.** `_harness.OpenLoopScheduler` yields
  `t_intended_ns = t0 + i / rate_hz`. If iteration N is slow,
  iteration N+1 is **not** delayed -- the bench is "behind
  schedule" and records the next sample's lateness in the latency,
  not in a hidden wait.
- **Record `t_response - t_intended`**, NOT
  `t_response - t_actual_dispatch`. The latter is the closed-loop
  number that hides the stall in the inter-iteration wait.
- **Do not discard "late" samples.** If the system is overloaded,
  the tail must reflect that.

Closed-loop benches (`for _ in range(N): send(); recv()`) are
**forbidden**. Reviewers checking a new bench script can grep for
the `OpenLoopScheduler` import; a bench without it is suspect.

---

## 3. Sample-size discipline

The harness reports percentiles with a **Wilson Score confidence
interval on the rank position of the percentile's order statistic**.
The reference is the Wikipedia
`Binomial_proportion_confidence_interval` article and the
HdrHistogram `getValueAtPercentile()` documentation.

For sample size N and target percentile p, Wilson Score 95% CI on
the rank position spans roughly +/- a half-width that shrinks with
sqrt(N). The harness reports both endpoints as actual latency
values (looked up at the corresponding ranks).

Practical defaults:

| Target | N | CI half-width (rank, approx) |
|---|---|---|
| p50 | 5000 | +/-1.4 percentile points |
| p90 | 5000 | +/-0.83 pp |
| p99 | 5000 | +/-0.27 pp |
| p99.9 | 5000 | +/-0.13 pp (CI half **value** widens because the rank slice is so thin) |

**N=5000 is the canonical bench size.** For p99 it gives a CI
half-width of ~+/-0.3 percentile points -- tight enough to defend
in adversarial review.

**Stop at p99.** p99.9 needs N ~ 15,000 to be defensible, and that
extra duration buys little: for a workstation daemon, p99 is
already the longest-tail signal users care about. Reporting p99.9
on N=5000 invites criticism that the rank-slice contains 5 samples
and the CI swamps the point estimate.

Warmup discard: the first 500 samples (or 10% of N, whichever is
larger) are discarded. The discard is reported in the result JSON
so reviewers can see it.

---

## 4. Tool selection

| Phase | Tool | Why |
|---|---|---|
| Recording | `hdrh` (HdrHistogram Python binding) | Fixed-cost (3-6 ns) recording, percentile lookup is O(1), industry-standard. |
| One-shot CLI timing | `hyperfine` (out of tree) | Use for `waitbus init` cold-start measurement only; `pyperf` would be overkill. |
| In-process Python statistical | `pyperf` (out of tree) | For algorithm-level micros (e.g. predicate evaluation per event). Not used for I/O-bound benches. |
| Open-loop steady-state | The custom harness in `_harness.py` | The harness reuses `time.time_ns()` cross-process and `time.monotonic_ns()` intra-process; nothing in the tool ecosystem combines those with HdrHistogram and CO-aware scheduling without bringing in heavier deps (locust, k6) that overweight a single-machine daemon's bench surface. |

**Forbidden for headline numbers:** `pytest-benchmark`. It is unit-
level only; it neither models concurrent producers nor records
HdrHistogram-grade percentiles. It is appropriate for micro-bench
of stdlib helpers, not for headline TTFAE.

---

## 5. The clock posture

- **Cross-process timing** (producer-process records t=0; subscriber
  records t=end): `time.time_ns()`. Wall clock. NTP can theoretically
  jump it, but over the 5000-sample window the jump is bounded by
  whatever step `chronyd`/`systemd-timesyncd` will tolerate (mtu of
  64 ms typical) -- negligible relative to multi-millisecond
  latencies being measured.
- **Intra-process timing** (scheduler tick, GC pause measurement):
  `time.monotonic_ns()`. Immune to NTP.

The harness's `OpenLoopScheduler` uses `monotonic_ns()` for tick
anchoring; the per-sample latency record uses `time.time_ns()` so
cross-process comparison works.

---

## 6. GC discipline

Every bench reports two numbers:

- **gc-enabled** (representative of production). The user runs waitbus
  with cyclic GC enabled; this is what they actually see.
- **gc-disabled** (algorithmic-cost figure). With `gc.disable()` in
  effect for the measurement loop. Strips out the rare large GC
  pauses that would otherwise dominate the tail.

The result JSON carries both under `percentiles_gc_enabled` and
`percentiles_gc_disabled`. Regression-gate comparisons run only on
the gc-enabled side (the production-representative number).

Bench scripts use the `_harness.gc_disabled()` context manager,
which is unconditional-restore on exception.

---

## 7. Reproducibility recipe

For canonical baselines, the bench host MUST be configured as
follows:

```bash
# 1. CPU pin to two isolated cores
taskset -c 2,3 uv run python -m benchmarks.bench_ttfae_pytest ...

# 2. CPU governor = performance (Linux only)
sudo cpupower frequency-set -g performance

# 3. ASLR disabled (Linux only)
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space

# 4. CPU isolation via isolcpus= kernel cmdline (optional, advanced)
# Add `isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3` to GRUB_CMDLINE_LINUX.
```

The harness's `environment_report()` collects these knobs at
startup and warns loudly (to stderr) when any is not set. The
warnings are NOT fatal -- a developer iterating on the bench
script can run without them -- but the result JSON records them
so a reviewer of `benchmarks/baselines/main.json` (or per-bench
baseline file) can verify the canonical run was done correctly.

A baseline run that triggered any reproducibility warning should
NOT be committed.

### Recommended capture host

The operator's daily-driver workstation is acceptable in principle
but conflicts with browser / IDE CPU usage during the capture
window. The recommended canonical-baseline host is a fresh Hetzner
**CCX23** dedicated-vCPU cloud server (4 vCPU, 16 GB, ~EUR 0.044/h
at May 2026 pricing). Bump to **CCX33** (8 vCPU, 32 GB) when
capturing `bench_throughput.py` at the 64-subscriber cell, which
benefits from extra core headroom. Avoid the shared-vCPU lines (CX,
CPX, CAX) -- noisy-neighbour steal time defeats the isolation goal.

The Hetzner API token is stored in keyring under
`service hcloud account api-key` (same key the soak test uses).
`scripts/capture_baselines.sh` provisions the VM, applies the tuning
knobs (ASLR off; governor=performance best-effort -- CCX dedicated
already runs at fixed performance), runs every bench at default N
with `taskset -c 2,3`, pulls the resulting JSONs back into
`benchmarks/baselines/`, and destroys the VM. End-to-end cost per
capture is well under EUR 1.

---

## 7a. Reproduce the cross-harness proof yourself

The headline cross-harness claim — N=5..10 heterogeneous agent
frameworks subscribing, emitting, and reacting on one local bus — is
reproducible on your own machine with your own agent CLIs. `waitbus
stress --real` spawns real `claude -p` and `gemini -p` driver
subprocesses alongside the in-process Pydantic AI / LangGraph drivers
and a shell control, mints one seed event, and verifies every driver
reacts cross-bus.

Requirements (the bench preflight asserts each before running):

```bash
pip install -e .            # exposes the `waitbus` CLI on PATH
claude --version            # your Claude Code CLI, signed in
gemini --version            # your Gemini CLI, signed in
export OPENAI_API_KEY=...    # optional: enables the gpt-4o-mini / nano driver
```

Then run the sweep:

```bash
waitbus stress --real --sweep "5,10" --duration 60s
```

The run writes a `verdict.json` with, per window, `cross_broadcast_proven`
(true iff every spawned driver reacted and all frameworks were observed),
median/p99 reaction latency, and measured token usage per LLM driver. The
`claude -p` / `gemini -p` calls are subscription-billed; `gpt-4o-mini` is
metered but cheap. None of this is required for normal waitbus use — the
core sources (pytest, docker, fs, GitHub), the MCP surface, and the
subscribe SDK need no agent CLI.

---

## 8. Per-source t=0 definitions (TTFAE)

Each source has a canonical t=0 (the instant a real consumer could
first observe the event); the bench scripts implement them
identically.

| Source | t=0 | Excluded leg |
|---|---|---|
| github | First byte of `HMACHandler.do_POST` execution (listener entry) | GitHub -> listener network (~80-200 ms transcontinental) |
| pytest | `pytest_emit._Recorder.pytest_sessionfinish` entry | None (in-process) |
| docker | Docker engine `/events` stream timestamp for container-exit | Engine -> `docker_watch` socket transit (~microseconds) |
| fs | `os.utime()` syscall completion on the watched path | watchdog inotify-event delivery (~microseconds) |

The github row's excluded network leg is the externality the per-
source comparison matrix in Article 02 annotates explicitly; for
the other three sources there is no external network leg, so the
comparison is clean.

---

## 9. Result JSON shape

Every bench writes a `BenchResult` (see `_harness.py`) to
`benchmarks/results/{bench_name}_{host}_{timestamp}.json`. The
shape is stable across benches:

```json
{
  "bench_name": "ttfae_pytest",
  "waitbus_version": "0.5.0",
  "started_at_ns": ...,
  "ended_at_ns": ...,
  "n_samples": 5000,
  "n_warmup_discarded": 500,
  "rate_hz": 100.0,
  "percentiles_gc_enabled": {
    "p10": {
      "value_ns": ...,
      "ci_low_ns": ...,
      "ci_high_ns": ...,
      "ci_z": 1.96,
      "p_low_eff": ...,
      "p_high_eff": ...
    },
    "p25": {...},
    "p50": {...},
    "p75": {...},
    "p90": {...},
    "p95": {...},
    "p99": {...}
  },
  "percentiles_gc_disabled": {...},
  "environment": {
    "hostname": "...",
    "python_version": "3.13.x",
    "platform": "Linux 6.x.y",
    "cpu_model": "...",
    "cpu_governor": "performance",
    "aslr_disabled": true,
    "taskset_mask": "2,3",
    "waitbus_version": "0.1.0",
    "warnings": []
  },
  "extra": {...},
  "histogram_b64_gc_enabled": "<base64 HdrHistogram V2; reconstructs any percentile offline>",
  "histogram_b64_gc_disabled": "<base64 HdrHistogram V2, or null>"
}
```

The `environment.warnings` list MUST be empty for a result file to
be committed as a baseline. The harness writes the file atomically
(tmp + rename) so a crashed bench cannot leave a corrupt JSON
behind.

---

## 10. The regression gate

`_harness.check_regression(result, baseline_path)` compares the
current `p99` (gc-enabled) against the committed baseline. >25%
degradation is a hard fail (CI exit non-zero).

The 25% threshold trades off two failure modes:

- **Too tight (e.g. 10%):** noise from runner-to-runner variance
  triggers spurious failures; CI flaps.
- **Too loose (e.g. 50%):** real regressions (a doubling of latency
  in a hot path) slip through.

25% sits comfortably above the noise floor of the canonical recipe
(CPU-pinned, governor=performance, ASLR-off) and below the size of
any real regression that would matter. The threshold is documented
here so it can be revisited if the noise floor changes.

The gate only runs when `--check-regression` is passed AND not in
`--smoke` mode. Smoke runs (N=100) are too small for the CI to be
meaningful.

---

## 11. CHANGES

This section records substantive methodology changes (touching the
open-loop scheduler, sample-size formula, warmup discard, threshold
values, percentile reported, or CI methodology). Editorial fixes do
NOT appear here.

The claim waitbus makes about its methodology is **"open-source
and reproducible -- re-run on identical hardware and verify."** Git
history of this file is supporting evidence. The CHANGES list below
is the human-readable summary; the git log is the authoritative
record.

- **2026-05-19** (initial). Open-loop scheduler from Gil Tene's
  CO-fix posture; HdrHistogram via `hdrh`; Wilson Score CIs on
  percentile rank positions; N=5000 default; first 500 samples
  warmup discard; CPU/governor/ASLR reproducibility recipe;
  `time.time_ns()` cross-process clock; `time.monotonic_ns()`
  intra-process; >25% p99 regression gate.

---

## 12. Reading list

- Gil Tene, "How NOT to Measure Latency" (Strange Loop):
  https://www.youtube.com/watch?v=lJ8ydIuPFeU
- `giltene/wrk2` reference open-loop implementation:
  https://github.com/giltene/wrk2
- HdrHistogram project: https://hdrhistogram.github.io/HdrHistogram/
- `hdrh` PyPI binding: https://pypi.org/project/hdrh/
- Wikipedia, "Binomial proportion confidence interval":
  https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval
- pyperf system tuning (the canonical CPU-pin/governor/ASLR recipe):
  https://pyperf.readthedocs.io/en/latest/system.html
- ScyllaDB benchmark methodology guide (prior art for prominently
  documenting CO):
  https://docs.scylladb.com/manual/stable/operating-scylla/procedures/tips/benchmark-tips.html
