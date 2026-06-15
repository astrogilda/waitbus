#!/usr/bin/env bash
# One-shot delivery-fidelity bench capture on a freshly provisioned Hetzner
# CCX dedicated-vCPU box -- the canonical pinned host for the committed
# delivery-fidelity baseline (a clean, isolated, performance-governor
# environment, free of laptop background contention).
#
# The bench needs only an OpenAI API key on the remote (the reasoning text is
# generated once per iteration via gpt-4.1-nano; the bus-swarm load is N
# lightweight in-process waitbus subscribers -- no LLM CLIs on the remote).
#
# Prereqs (keyring):
#   secret-tool store --label='OpenAI API Key'  service openai account api-key
#   secret-tool store --label='Hetzner Cloud API' service hcloud account api-key
#
# Usage:
#   scripts/run_delivery_fidelity_on_hetzner.sh \
#       [--n 40] [--max-cost-usd 2.00] [--server-type ccx23] [--location fsn1] \
#       [--output bench_event_delivery_fidelity_hetzner.json] [--keep]
#
# The VM is destroyed on exit unless --keep. The verdict + progress + log are
# pulled back locally; this script does NOT commit them (the caller decides).
# Public-flip is NOT triggered by this script.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/_hetzner_provision_lib.sh
source "${REPO_ROOT}/scripts/_hetzner_provision_lib.sh"

# ----- defaults -----
SERVER_TYPE="ccx23"
SSH_KEY_NAME="matchlock-ci-runner"
SSH_KEY_PATH="${HOME}/.ssh/hetzner_matchlock"
LOCATION="fsn1"
KEEP_SERVER="false"
BENCH_N="40"
BENCH_MAX_COST_USD="2.00"
MAX_WALL_CLOCK_MIN="40"
LOCAL_OUTPUT="bench_event_delivery_fidelity_hetzner.json"

# ----- arg parse -----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-type)        SERVER_TYPE="$2"; shift 2 ;;
    --ssh-key)            SSH_KEY_NAME="$2"; shift 2 ;;
    --ssh-key-path)       SSH_KEY_PATH="$2"; shift 2 ;;
    --location)           LOCATION="$2"; shift 2 ;;
    --keep)               KEEP_SERVER="true"; shift ;;
    --n)                  BENCH_N="$2"; shift 2 ;;
    --max-cost-usd)       BENCH_MAX_COST_USD="$2"; shift 2 ;;
    --max-wall-clock-min) MAX_WALL_CLOCK_MIN="$2"; shift 2 ;;
    --output)             LOCAL_OUTPUT="$2"; shift 2 ;;
    -h|--help)            sed -n '2,30p' "$0"; exit 0 ;;
    *)                    echo "error: unknown flag $1" >&2; exit 2 ;;
  esac
done

# ----- token + LLM-key prereqs -----
hetzner_check_prereqs

OPENAI_KEY="$(secret-tool lookup service openai account api-key 2>/dev/null || true)"
if [[ -z "${OPENAI_KEY}" ]]; then
  echo "error: OPENAI_API_KEY missing in keyring. Stage with:" >&2
  echo "  secret-tool store --label='OpenAI API Key' service openai account api-key" >&2
  exit 1
fi

COMMIT_SHA="$(cd "${REPO_ROOT}" && git rev-parse --short HEAD)"
HETZNER_SERVER_NAME="waitbus-bench-fidelity-$(date +%Y%m%d-%H%M%S)-${COMMIT_SHA}"
export HETZNER_SERVER_NAME KEEP_SERVER

mapfile -t SSH_OPTS < <(hetzner_default_ssh_opts)

# ----- provision + trap-guarded cleanup (destroys box on any exit) -----
hetzner_provision_server "${HETZNER_SERVER_NAME}" "${SERVER_TYPE}" "${LOCATION}" "${SSH_KEY_NAME}"
trap hetzner_cleanup EXIT INT TERM

# ----- wait, tune, ship, install -----
hetzner_wait_for_ssh SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"
hetzner_apply_tuning_knobs SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"
hetzner_ship_repo SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}" "${REPO_ROOT}" "${COMMIT_SHA}"
hetzner_install_deps SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"

# ----- run the bench (tmux-detached + done-marker poll; survives ssh drop) -----
TMUX_SESSION="waitbus-bench-fidelity"
# The bench treats --output as a DIRECTORY and writes
# <dir>/<ts>.<bench>.verdict.json (+ .progress.jsonl) inside it.
REMOTE_OUT_DIR="/root/waitbus/fidelity-out"
REMOTE_LOG="/root/waitbus/fidelity-run.log"
REMOTE_DONE="/root/waitbus/fidelity-done.exit-code"

echo "[hetzner-fidelity] ===== running N=${BENCH_N} on ${HETZNER_SERVER_IP} (commit ${COMMIT_SHA}) ====="
echo "[hetzner-fidelity] attach: ssh -t -i ${SSH_KEY_PATH} root@${HETZNER_SERVER_IP} tmux attach -t ${TMUX_SESSION}"
echo "[hetzner-fidelity] wall-clock cap: ${MAX_WALL_CLOCK_MIN}min"

ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
  set -euo pipefail
  command -v tmux >/dev/null 2>&1 || apt-get install -y -qq tmux >/dev/null
  rm -f ${REMOTE_DONE}
  tmux kill-session -t ${TMUX_SESSION} 2>/dev/null || true
  tmux new-session -d -s ${TMUX_SESSION} \
    'cd /root/waitbus && \
     export PATH=/root/.local/bin:/usr/local/bin:\$PATH && \
     export UV_LINK_MODE=copy && \
     export OPENAI_API_KEY=${OPENAI_KEY} && \
     stdbuf -oL -eL uv run python -u -m benchmarks.bench_event_delivery_fidelity \
       --include-real-llm --n ${BENCH_N} --max-cost-usd ${BENCH_MAX_COST_USD} \
       --output ${REMOTE_OUT_DIR} > ${REMOTE_LOG} 2>&1; \
     echo \$? > ${REMOTE_DONE}'
  echo '[remote] launched in tmux session ${TMUX_SESSION}'
"

# ----- poll for completion -----
DEADLINE=$(( $(date +%s) + MAX_WALL_CLOCK_MIN * 60 ))
REMOTE_RC=""
while true; do
  if (( $(date +%s) >= DEADLINE )); then
    echo "[hetzner-fidelity] WALL-CLOCK CAP (${MAX_WALL_CLOCK_MIN}min) -- killing remote + pulling partials" >&2
    ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "tmux kill-session -t ${TMUX_SESSION} 2>/dev/null || true" 2>/dev/null || true
    REMOTE_RC="TIMEOUT"
    break
  fi
  REMOTE_RC="$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "cat ${REMOTE_DONE} 2>/dev/null || true" 2>/dev/null || true)"
  if [[ -n "${REMOTE_RC}" ]]; then
    echo "[hetzner-fidelity] remote finished, exit code: ${REMOTE_RC}"
    break
  fi
  PROG=$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "cat ${REMOTE_OUT_DIR}/*.progress.jsonl 2>/dev/null | wc -l || echo 0" 2>/dev/null || echo "?")
  echo "[hetzner-fidelity:snap] progress_rows=${PROG} ($(date -u +%H:%M:%S)Z)"
  sleep 30
done

# ----- fetch artifacts (resolve the verdict file inside the output dir) -----
echo "[hetzner-fidelity] pulling verdict + progress + log..."
REMOTE_VERDICT_PATH="$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "ls ${REMOTE_OUT_DIR}/*.verdict.json 2>/dev/null | head -1" 2>/dev/null || true)"
REMOTE_PROGRESS_PATH="$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "ls ${REMOTE_OUT_DIR}/*.progress.jsonl 2>/dev/null | head -1" 2>/dev/null || true)"
if [[ -n "${REMOTE_VERDICT_PATH}" ]]; then
  scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:${REMOTE_VERDICT_PATH}" "${REPO_ROOT}/${LOCAL_OUTPUT}" 2>/dev/null || echo "  (verdict scp failed)"
else
  echo "  (no verdict file found in ${REMOTE_OUT_DIR} -- run may have aborted)"
fi
if [[ -n "${REMOTE_PROGRESS_PATH}" ]]; then
  scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:${REMOTE_PROGRESS_PATH}" "${REPO_ROOT}/${LOCAL_OUTPUT}.progress.jsonl" 2>/dev/null || true
fi
scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:${REMOTE_LOG}" "${REPO_ROOT}/${LOCAL_OUTPUT}.log" 2>/dev/null || true

# ----- validate verdict shape locally -----
if [[ -f "${REPO_ROOT}/${LOCAL_OUTPUT}" ]]; then
  echo "[hetzner-fidelity] ===== verdict ====="
  python3 - "${REPO_ROOT}/${LOCAL_OUTPUT}" "${REMOTE_RC}" <<'PYEOF'
import json, sys
d = json.load(open(sys.argv[1])); rc = sys.argv[2]; g = d.get
ms = lambda ns: (ns or 0) / 1e6
deliv = [g('delivery_integrity_failures_lll_alone'), g('delivery_integrity_failures_bus_idle'), g('delivery_integrity_failures_bus_swarm')]
order = [g('ordering_inversions_lll_alone'), g('ordering_inversions_bus_idle'), g('ordering_inversions_bus_swarm')]
print(f"  remote_exit={rc}  n_triples={g('n_triples_actual')}  cost=${g('cost_usd_total')}  inapplicable={g('inapplicable_reason')}")
print(f"  delivery_integrity={deliv}  ordering_inversions={order}")
print(f"  latency_budget_passed={g('latency_budget_passed')}  bus_idle_p99={ms(g('bus_idle_p99_latency_ns')):.1f}ms  bus_swarm_p99={ms(g('bus_swarm_p99_latency_ns')):.1f}ms")
print(f"  median_per_event: bus_idle={ms(g('median_per_chunk_bus_latency_bus_idle_ns')):.1f}ms  bus_swarm={ms(g('median_per_chunk_bus_latency_bus_swarm_ns')):.1f}ms")
print(f"  perturbation_detected={g('perturbation_detected')}  subscribers_ready={g('swarm_subscribers_ready_total')}/{g('swarm_underload_floor')}")
clean = (rc == "0" and g('inapplicable_reason') is None and sum(x or 0 for x in deliv) == 0
         and sum(x or 0 for x in order) == 0 and g('latency_budget_passed') is True
         and g('perturbation_detected') is False)
print("  VERDICT:", "CLEAN PASS (baseline-ready)" if clean else "NOT CLEAN -- inspect before committing as baseline")
sys.exit(0 if clean else 3)
PYEOF
  VALIDATE_RC=$?
else
  echo "[hetzner-fidelity] ERROR: no verdict pulled back" >&2
  VALIDATE_RC=4
fi

echo "[hetzner-fidelity] local artifacts: ${LOCAL_OUTPUT}{,.progress.jsonl,.log}"
echo "[hetzner-fidelity] (VM destroyed on exit unless --keep)"
exit "${VALIDATE_RC}"
