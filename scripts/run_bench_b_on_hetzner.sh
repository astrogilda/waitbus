#!/usr/bin/env bash
# One-shot bench_multistream_proof N=50 capture on a freshly provisioned
# Hetzner CCX dedicated-vCPU box (canonical pinned host for the
# publishable baseline). Mirrors scripts/run_soak_on_hetzner.sh and
# shares the provision pipeline (scripts/_hetzner_provision_lib.sh).
#
# The bench's preflight raises PreflightError unless governor is
# pinned to 'performance' AND cpu_count >= 2 AND every required LLM
# CLI is on PATH. The CCX23 image satisfies governor + cores via the
# library's hetzner_apply_tuning_knobs; the default
# ``--agent-frameworks pydantic,langgraph,shell-control`` skips the
# subscription-OAuth CLIs (claude-cli, gemini-cli) so the remote
# host needs only an OpenAI API key.
#
# Required keyring entries (set up on the OPERATOR'S workstation
# BEFORE running this script):
#
#   secret-tool store --label='OpenAI API Key' \
#       service openai account api-key
#   secret-tool store --label='Hetzner Cloud API' \
#       service hcloud account api-key
#
# Optional (only required when overriding --agent-frameworks to
# include claude or gemini with API-key-mode auth):
#
#   secret-tool store --label='Anthropic API Key' \
#       service anthropic account api-key
#   secret-tool store --label='Gemini API Key' \
#       service gemini account api-key
#
# Usage:
#   scripts/run_bench_b_on_hetzner.sh \
#       [--server-type ccx23] [--location fsn1]
#       [--ssh-key NAME] [--ssh-key-path PATH] [--keep]
#       [--n 50] [--producer-count 50]
#       [--producer-event-rate-hz 200]
#       [--agent-frameworks pydantic,langgraph,shell-control]
#       [--max-cost-usd 50]
#       [--max-wall-clock-min 120]
#       [--snapshot-interval-sec 60]
#       [--skip-smoke] [--smoke-only]
#       [--output bench_multistream_results.json]
#
# Default flow: smoke (N=3, producer_count=3, max-cost=$2) on the
# freshly provisioned VM, validate verdict shape (perturbation_detected,
# producer_count) against the contract, then the full N=50
# producer_count=50 run on the SAME VM. ``--skip-smoke`` skips the
# smoke step; ``--smoke-only`` runs smoke and exits.
#
# Resilience:
#   * --max-wall-clock-min (default 120) kills the remote tmux session
#     + pulls partials when the bench exceeds this deadline. Catches
#     a wedged bench mid-run.
#   * On any exit path (success / failure / SIGINT), the trap pulls
#     verdict + progress + log + agent stderr + producer stderr dirs
#     from the remote BEFORE the VM is destroyed. Use --keep to
#     inspect remote state manually.
#   * Polls re-establish ssh on each iteration so a network blip
#     costs ~one poll interval, not the whole run.
#
# Observability:
#   * --snapshot-interval-sec (default 60) writes a per-snapshot
#     daemon-state line to the local terminal: daemon RSS / FD count /
#     producer emit count delta / subscriber alive count.
#   * Per-window progress.jsonl rows stream in real time via the
#     poll loop; each line is parsed + reformatted for readability.
#   * On completion, prints last 50 lines of the bench log + last 5
#     progress.jsonl rows for at-a-glance triage.
#   * Operator can ssh into the tmux session at any time:
#         ssh -t -i <ssh-key> root@<vm-ip> tmux attach -t <session>
#     The script prints this command on startup.
#
# Cost: CCX23 is ~EUR 0.087/h; smoke ~5 min + full ~30-60 min = ~EUR
# 0.10 infra. Real LLM out-of-pocket: ~$0.05 (OpenAI tokens; the
# default 3-framework agent set runs pydantic-ai + langgraph against
# OpenAI plus a no-LLM shell-control driver).
#
# This script does not commit or publish results. Verdict + progress
# files are pulled back to the operator's workstation; the operator
# decides what to do with them. The Hetzner VM is destroyed on exit
# unless --keep is set.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/_hetzner_provision_lib.sh
source "${REPO_ROOT}/scripts/_hetzner_provision_lib.sh"

# ----- defaults -----
SERVER_TYPE="ccx23"
SSH_KEY_NAME="waitbus-ci-runner"
SSH_KEY_PATH="${HOME}/.ssh/hetzner_waitbus"
LOCATION="fsn1"
KEEP_SERVER="false"
BENCH_N="50"
BENCH_PRODUCER_COUNT="50"
BENCH_PRODUCER_EVENT_RATE_HZ="200"
BENCH_AGENT_FRAMEWORKS="pydantic,langgraph,shell-control"
BENCH_MAX_COST_USD="50"
SMOKE_N="3"
SMOKE_PRODUCER_COUNT="3"
SMOKE_PRODUCER_EVENT_RATE_HZ="50"
SMOKE_MAX_COST_USD="2"
SKIP_SMOKE="false"
SMOKE_ONLY="false"
MAX_WALL_CLOCK_MIN="120"
SNAPSHOT_INTERVAL_SEC="60"
LOCAL_OUTPUT="bench_multistream_results.json"

# ----- arg parse -----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-type)               SERVER_TYPE="$2"; shift 2 ;;
    --ssh-key)                   SSH_KEY_NAME="$2"; shift 2 ;;
    --ssh-key-path)              SSH_KEY_PATH="$2"; shift 2 ;;
    --location)                  LOCATION="$2"; shift 2 ;;
    --keep)                      KEEP_SERVER="true"; shift ;;
    --n)                         BENCH_N="$2"; shift 2 ;;
    --producer-count)            BENCH_PRODUCER_COUNT="$2"; shift 2 ;;
    --producer-event-rate-hz)    BENCH_PRODUCER_EVENT_RATE_HZ="$2"; shift 2 ;;
    --agent-frameworks)          BENCH_AGENT_FRAMEWORKS="$2"; shift 2 ;;
    --max-cost-usd)              BENCH_MAX_COST_USD="$2"; shift 2 ;;
    --output)                    LOCAL_OUTPUT="$2"; shift 2 ;;
    --skip-smoke)                SKIP_SMOKE="true"; shift ;;
    --smoke-only)                SMOKE_ONLY="true"; shift ;;
    --max-wall-clock-min)        MAX_WALL_CLOCK_MIN="$2"; shift 2 ;;
    --snapshot-interval-sec)     SNAPSHOT_INTERVAL_SEC="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,57p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown flag $1" >&2
      exit 2
      ;;
  esac
done

# ----- token + LLM-key prereqs -----
hetzner_check_prereqs

OPENAI_KEY="$(secret-tool lookup service openai account api-key 2>/dev/null || true)"
ANTHROPIC_KEY="$(secret-tool lookup service anthropic account api-key 2>/dev/null || true)"
GEMINI_KEY="$(secret-tool lookup service gemini account api-key 2>/dev/null || true)"

if [[ -z "${OPENAI_KEY}" ]]; then
  echo "error: OPENAI_API_KEY missing in keyring. Stage with:" >&2
  echo "  secret-tool store --label='OpenAI API Key' service openai account api-key" >&2
  exit 2
fi

# claude / gemini keys are optional unless --agent-frameworks names them
if [[ "${BENCH_AGENT_FRAMEWORKS}" == *"claude"* && -z "${ANTHROPIC_KEY}" ]]; then
  echo "error: --agent-frameworks includes 'claude' but ANTHROPIC_API_KEY is missing in keyring." >&2
  exit 2
fi
if [[ "${BENCH_AGENT_FRAMEWORKS}" == *"gemini"* && -z "${GEMINI_KEY}" ]]; then
  echo "error: --agent-frameworks includes 'gemini' but GEMINI_API_KEY is missing in keyring." >&2
  exit 2
fi

COMMIT_SHA="$(cd "${REPO_ROOT}" && git rev-parse --short HEAD)"
HETZNER_SERVER_NAME="waitbus-bench-multistream-$(date +%Y%m%d-%H%M%S)-${COMMIT_SHA}"
export HETZNER_SERVER_NAME KEEP_SERVER

mapfile -t SSH_OPTS < <(hetzner_default_ssh_opts)

# ----- provision + trap-guarded cleanup -----
hetzner_provision_server "${HETZNER_SERVER_NAME}" "${SERVER_TYPE}" "${LOCATION}" "${SSH_KEY_NAME}"
trap hetzner_cleanup EXIT INT TERM

# ----- wait, tune, ship, install -----
hetzner_wait_for_ssh SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"
hetzner_apply_tuning_knobs SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"
hetzner_ship_repo SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}" "${REPO_ROOT}" "${COMMIT_SHA}"
hetzner_install_deps SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"

# ----- install LLM CLIs only when --agent-frameworks names them -----
# The default agent-frameworks (pydantic,langgraph,shell-control) does not
# need either claude-cli or gemini-cli on the remote. Install on
# demand when the operator overrides the default to include them.
if [[ "${BENCH_AGENT_FRAMEWORKS}" == *"claude"* || "${BENCH_AGENT_FRAMEWORKS}" == *"gemini"* ]]; then
  echo "[hetzner-bench] installing claude + gemini CLIs..." >&2
  ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
    set -euo pipefail
    apt-get update -qq >/dev/null 2>&1 || true
    apt-get install -y -qq nodejs npm >/dev/null 2>&1
    if [[ \"${BENCH_AGENT_FRAMEWORKS}\" == *\"claude\"* ]] && ! command -v claude >/dev/null 2>&1; then
      npm install -g @anthropic-ai/claude-code --silent >/dev/null 2>&1
    fi
    if [[ \"${BENCH_AGENT_FRAMEWORKS}\" == *\"gemini\"* ]] && ! command -v gemini >/dev/null 2>&1; then
      npm install -g @google/gemini-cli --silent >/dev/null 2>&1 || true
    fi
  "
fi

# Phase function: run one bench invocation on the remote and pull
# verdict back. Args: PHASE_NAME N PRODUCER_COUNT PRODUCER_RATE MAX_COST_USD LOCAL_OUT
run_remote_bench() {
  local phase_name="$1"
  local b_n="$2"
  local b_producers="$3"
  local b_rate="$4"
  local b_max_cost="$5"
  local local_out="$6"
  local tmux_session="waitbus-bench-multistream-${phase_name}"
  local remote_verdict="/root/waitbus/bench-multistream-${phase_name}-verdict.json"
  local remote_progress="/root/waitbus/bench-multistream-${phase_name}-verdict.progress.jsonl"
  local remote_done_marker="/root/waitbus/bench-multistream-${phase_name}-done.exit-code"

  echo "[hetzner-bench:${phase_name}] ===== running on ${HETZNER_SERVER_IP} ====="
  echo "[hetzner-bench:${phase_name}] n=${b_n} producer_count=${b_producers} rate_hz=${b_rate} max_cost_usd=${b_max_cost} frameworks=${BENCH_AGENT_FRAMEWORKS}"
  echo "[hetzner-bench:${phase_name}] attach manually: ssh -t -i ${SSH_KEY_PATH} root@${HETZNER_SERVER_IP} tmux attach -t ${tmux_session}"
  echo "[hetzner-bench:${phase_name}] wall-clock cap: ${MAX_WALL_CLOCK_MIN}min; snapshot interval: ${SNAPSHOT_INTERVAL_SEC}s"

  ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
    set -euo pipefail
    if ! command -v tmux >/dev/null 2>&1; then
      apt-get install -y -qq tmux >/dev/null
    fi
    rm -f ${remote_done_marker}
    tmux kill-session -t ${tmux_session} 2>/dev/null || true
    tmux new-session -d -s ${tmux_session} \
      'cd /root/waitbus && \
       export PATH=/root/.local/bin:/usr/local/bin:\$PATH && \
       export UV_LINK_MODE=copy && \
       export OPENAI_API_KEY=${OPENAI_KEY} && \
       export ANTHROPIC_API_KEY=${ANTHROPIC_KEY} && \
       export GEMINI_API_KEY=${GEMINI_KEY} && \
       stdbuf -oL -eL uv run python -u -m benchmarks.bench_multistream_proof \
         --include-real-llm \
         --producer-count ${b_producers} \
         --producer-event-rate-hz ${b_rate} \
         --agent-frameworks ${BENCH_AGENT_FRAMEWORKS} \
         --n ${b_n} \
         --max-cost-usd ${b_max_cost} \
         --output ${remote_verdict}; \
       echo \$? > ${remote_done_marker}'
    echo '[remote] ${phase_name} launched in tmux session ${tmux_session}'
  "

  local deadline_epoch=$(( $(date +%s) + MAX_WALL_CLOCK_MIN * 60 ))
  local last_snapshot_epoch=0

  echo "[hetzner-bench:${phase_name}] (per-window progress streams below; snapshots every ${SNAPSHOT_INTERVAL_SEC}s)"
  local progress_offset=0
  while true; do
    if (( $(date +%s) >= deadline_epoch )); then
      echo "[hetzner-bench:${phase_name}] WALL-CLOCK CAP REACHED (${MAX_WALL_CLOCK_MIN}min); killing remote tmux + pulling partials" >&2
      ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
        tmux kill-session -t ${tmux_session} 2>/dev/null || true
        echo TIMEOUT > ${remote_done_marker}
      " 2>/dev/null || true
      break
    fi

    # Periodic daemon-state snapshot. Reads the daemon pid file the
    # bench writes at spawn (matches scripts/run_soak_on_hetzner.sh
    # convention) and reports RSS / FD count / event count for live
    # health visibility. A snapshot failure is non-fatal -- just print
    # the error line and keep polling.
    local now_epoch
    now_epoch=$(date +%s)
    if (( now_epoch - last_snapshot_epoch >= SNAPSHOT_INTERVAL_SEC )); then
      last_snapshot_epoch=$now_epoch
      local snapshot
      snapshot="$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
        # daemon pid: look up waitbus broadcast serve process
        dpid=\$(pgrep -f 'waitbus broadcast serve' | head -1 || true)
        if [[ -n \"\$dpid\" ]]; then
          rss_kb=\$(awk '/VmRSS:/ {print \$2}' /proc/\$dpid/status 2>/dev/null || echo 0)
          fd_count=\$(ls /proc/\$dpid/fd 2>/dev/null | wc -l)
          tid_count=\$(ls /proc/\$dpid/task 2>/dev/null | wc -l)
          load1=\$(awk '{print \$1}' /proc/loadavg 2>/dev/null || echo ?)
          uptime_s=\$(awk -F. '{print \$1}' /proc/uptime 2>/dev/null || echo ?)
          prog_lines=\$(wc -l < ${remote_progress} 2>/dev/null || echo 0)
          echo \"daemon_pid=\$dpid rss_mb=\$((rss_kb/1024)) fd=\$fd_count tids=\$tid_count load1=\$load1 uptime_s=\$uptime_s progress_rows=\$prog_lines\"
        else
          echo \"daemon_pid=NONE progress_rows=\$(wc -l < ${remote_progress} 2>/dev/null || echo 0)\"
        fi
      " 2>/dev/null || true)"
      if [[ -n "${snapshot}" ]]; then
        echo "[hetzner-bench:${phase_name}:snap] ${snapshot}"
      fi
    fi

    local new_bytes
    local new_bytes
    new_bytes="$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
      if [[ -f ${remote_progress} ]]; then
        tail -c +$((progress_offset + 1)) ${remote_progress}
      fi
    " 2>/dev/null || true)"
    if [[ -n "${new_bytes}" ]]; then
      while IFS= read -r line; do
        [[ -z "${line}" ]] && continue
        python3 -c "
import json, sys
rec = json.loads(sys.argv[1])
kind = rec.get('kind', '?')
if kind == 'window':
    print(f\"[${phase_name}] window={rec.get('window_id','?'):3} arm={rec.get('arm','?'):6} utime_ns={rec.get('utime_ns',0):>10} schedstat_run_ns={rec.get('schedstat_run_ns',0):>10} pcount={rec.get('schedstat_pcount',0):>4} events={rec.get('events',0):>4}\")
else:
    print(f\"[${phase_name}] {kind}: {rec}\")
" "${line}"
      done <<< "${new_bytes}"
      progress_offset=$((progress_offset + $(echo -n "${new_bytes}" | wc -c)))
    fi
    local exit_code
    exit_code="$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
      if [[ -f ${remote_done_marker} ]]; then cat ${remote_done_marker}; fi
    " 2>/dev/null || true)"
    if [[ -n "${exit_code}" ]]; then
      echo "[hetzner-bench:${phase_name}] remote bench exited with code ${exit_code}"
      break
    fi
    sleep 30
  done

  local local_progress="${local_out}.progress.jsonl"
  local local_log="${local_out}.log"
  local local_stderr_dir="${local_out%.json}.stderr"
  mkdir -p "${local_stderr_dir}"

  echo "[hetzner-bench:${phase_name}] pulling verdict + progress + log + tmux pane + stderr dirs back..."
  scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:${remote_verdict}" "${local_out}" 2>/dev/null || true
  scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:${remote_progress}" "${local_progress}" 2>/dev/null || true
  scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:/root/waitbus/bench-multistream-${phase_name}-verdict.log" "${local_log}" 2>/dev/null || true
  # Capture the tmux pane's scrollback so we always get the bench's
  # stdout/stderr, even when the bench dies before writing the log file.
  local local_pane="${local_out%.json}.tmux-pane.txt"
  ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
    tmux capture-pane -p -S -- -3000 -t ${tmux_session} 2>/dev/null || true
  " > "${local_pane}" 2>/dev/null || true
  # Pull every stderr file the bench's RAII pools produced. Tolerate
  # missing dirs (smoke runs may not have spawned anything).
  scp -r -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:/tmp/waitbus-bench-multistream-*/swarm-stderr" "${local_stderr_dir}/" 2>/dev/null || true
  scp -r -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:/tmp/waitbus-bench-multistream-*/agent-stderr" "${local_stderr_dir}/" 2>/dev/null || true

  # Triage tail: print last 50 log lines + last 5 progress rows.
  if [[ -s "${local_log}" ]]; then
    echo "[hetzner-bench:${phase_name}] ---- last 50 log lines ----" >&2
    tail -n 50 "${local_log}" >&2 || true
    echo "[hetzner-bench:${phase_name}] ---- end log tail ----" >&2
  fi
  if [[ -s "${local_progress}" ]]; then
    echo "[hetzner-bench:${phase_name}] ---- last 5 progress rows ----" >&2
    tail -n 5 "${local_progress}" >&2 || true
    echo "[hetzner-bench:${phase_name}] ---- end progress tail ----" >&2
  fi

  if [[ ! -s "${local_out}" ]]; then
    echo "[hetzner-bench:${phase_name}] WARNING: verdict file is empty. Check ${local_log} + ${local_stderr_dir}." >&2
    return 1
  fi
  return 0
}

# ----- smoke phase (low-cost shape-validation before full run) -----
if [[ "${SKIP_SMOKE}" != "true" ]]; then
  SMOKE_OUTPUT="${LOCAL_OUTPUT%.json}.smoke.json"
  run_remote_bench "smoke" "${SMOKE_N}" "${SMOKE_PRODUCER_COUNT}" "${SMOKE_PRODUCER_EVENT_RATE_HZ}" "${SMOKE_MAX_COST_USD}" "${SMOKE_OUTPUT}" || {
    echo "[hetzner-bench] SMOKE FAILED. Aborting before full N=${BENCH_N} run." >&2
    exit 1
  }
  SMOKE_VERDICT_LABEL="$(python3 -c "import json; d=json.load(open('${SMOKE_OUTPUT}')); print(d.get('verdict','?'))")"
  SMOKE_HAS_PERTURB="$(python3 -c "import json; d=json.load(open('${SMOKE_OUTPUT}')); print('perturbation_detected' in d)")"
  SMOKE_PRODUCERS="$(python3 -c "import json; d=json.load(open('${SMOKE_OUTPUT}')); print(d.get('producer_count',-1))")"
  echo "[hetzner-bench] smoke verdict=${SMOKE_VERDICT_LABEL} has_perturbation_field=${SMOKE_HAS_PERTURB} producer_count=${SMOKE_PRODUCERS}"
  if [[ "${SMOKE_HAS_PERTURB}" != "True" ]]; then
    echo "[hetzner-bench] SMOKE: verdict missing perturbation_detected field (stale binary?); aborting." >&2
    exit 1
  fi
  echo "[hetzner-bench] smoke shape contract OK; proceeding to full N=${BENCH_N} run."
fi

if [[ "${SMOKE_ONLY}" == "true" ]]; then
  echo "[hetzner-bench] --smoke-only set; skipping full run."
  exit 0
fi

# ----- full N=50 production phase -----
run_remote_bench "full" "${BENCH_N}" "${BENCH_PRODUCER_COUNT}" "${BENCH_PRODUCER_EVENT_RATE_HZ}" "${BENCH_MAX_COST_USD}" "${LOCAL_OUTPUT}" || {
  echo "[hetzner-bench] FULL RUN FAILED." >&2
  exit 1
}
LOCAL_PROGRESS="${LOCAL_OUTPUT}.progress.jsonl"
LOCAL_LOG="${LOCAL_OUTPUT}.log"

# Parse top-level fields without requiring uv on local
VERDICT_LABEL="$(python3 -c "import json; d=json.load(open('${LOCAL_OUTPUT}')); print(d.get('verdict','?'))")"
PRODUCER_OK="$(python3 -c "import json; d=json.load(open('${LOCAL_OUTPUT}')); print(d.get('producer_count',-1) == ${BENCH_PRODUCER_COUNT})")"
EQ_ESTABLISHED="$(python3 -c "import json; d=json.load(open('${LOCAL_OUTPUT}')); print(d.get('equivalence_established'))")"
PRODUCER_ATTRITION="$(python3 -c "import json; d=json.load(open('${LOCAL_OUTPUT}')); print(d.get('producer_attrition_detected'))")"
SUBSCRIBER_ATTRITION="$(python3 -c "import json; d=json.load(open('${LOCAL_OUTPUT}')); print(d.get('subscriber_attrition_detected'))")"

echo "[hetzner-bench] done. verdict=${LOCAL_OUTPUT} progress=${LOCAL_PROGRESS} log=${LOCAL_LOG}"
echo "[hetzner-bench] verdict.label=${VERDICT_LABEL}"
echo "[hetzner-bench] verdict.producer_count==${BENCH_PRODUCER_COUNT}: ${PRODUCER_OK}"
echo "[hetzner-bench] verdict.equivalence_established=${EQ_ESTABLISHED}"
echo "[hetzner-bench] verdict.producer_attrition_detected=${PRODUCER_ATTRITION}"
echo "[hetzner-bench] verdict.subscriber_attrition_detected=${SUBSCRIBER_ATTRITION}"
echo "[hetzner-bench] NOTE: verdict is informational. No publish or tag-push step runs."
echo "[hetzner-bench]       The operator inspects + commits to benchmarks/baselines/ if approved."

if [[ "${VERDICT_LABEL}" == "equivalence_established_within_delta" ]]; then
  exit 0
fi
exit 1
