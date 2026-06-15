#!/usr/bin/env bash
# One-shot 24-hour soak orchestrator on a freshly provisioned Hetzner
# CCX cloud server.
#
# The soak itself lives in scripts.soak (package) (the on-machine orchestrator
# that spawns the broadcast daemon, drives a synthetic mixed-source
# emitter, samples RSS / FD / WAL, optionally injects SIGSTOP /
# SIGCONT suspend cycles, and writes soak-verdict.json + a
# .progress.jsonl stream). THIS script is the off-machine provisioner:
# it stands up the VM, applies the same tuning knobs as the bench
# capture, ssh-invokes scripts.soak (package), streams the per-sample
# progress back to the operator's terminal in real time, pulls the
# verdict + progress JSONL back at end, and destroys the VM.
#
# The provisioning recipe is shared with scripts/capture_baselines.sh
# via scripts/_hetzner_provision_lib.sh. The two scripts only differ
# in the workload step.
#
# What this script DOES NOT DO
# ----------------------------
# It does NOT publish anything or push any tag regardless of the soak
# verdict. Publishing is a separate manual step. Soak's role is to
# surface drift; the verdict is informational.
#
# Default: CCX23 (4 dedicated vCPU, 16 GB, ~EUR 0.087/h). Matches the
# canonical benchmark-baseline hardware in benchmarks/BENCHMARKING.md
# for cross-workstream comparability of p99 measurements. Dedicated
# vCPU is required (not shared-vCPU CX/CPX/CAX) per the BENCHMARKING
# guidance: shared vCPU contributes noisy-neighbour steal time that
# defeats the p99-drift signal. Override via --server-type for a
# larger box if desired.
#
# Usage:
#   scripts/run_soak_on_hetzner.sh [--server-type ccx23]
#                                  [--ssh-key NAME] [--ssh-key-path PATH]
#                                  [--location fsn1] [--keep]
#                                  [--duration 24h] [--rate 5]
#                                  [--sample-interval 60]
#                                  [--inject-suspend-cycles none|standard]
#                                  [--inject-fault-scenarios none|standard|fast]
#                                  [--output soak-verdict.json]
#
# Pre-24h Hetzner smoke pattern (validates new daemon control flow on the
# exact hardware shape the 24h run will use):
#   scripts/run_soak_on_hetzner.sh --duration 30m \
#       --inject-fault-scenarios standard \
#       --server-type ccx23
# Then the 24h run if the smoke verdict is overall_passed=true:
#   scripts/run_soak_on_hetzner.sh --duration 24h \
#       --inject-suspend-cycles standard \
#       --server-type ccx23

set -euo pipefail

# --- Immutability guard: re-exec from a frozen copy --------------------------
# A 24-hour run can outlive an edit to this file in the repo. Bash re-reads a running
# script by byte offset, so a mid-run edit corrupts the executing process and
# aborts it AFTER the soak finished but BEFORE the verdict is pulled back
# ("syntax error near unexpected token" on a line that is valid on disk). Copy
# self to an immutable temp file and re-exec from there, so repo edits during
# the run cannot touch the executing image.
if [[ "${_SOAK_FROZEN:-}" != 1 ]]; then
  # Resolve the real repo root NOW (the frozen copy lives in /tmp and cannot
  # recompute it from its own path) and pass it through to the re-exec.
  export _SOAK_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  _frozen="$(mktemp "${TMPDIR:-/tmp}/run_soak_frozen.XXXXXX.sh")"
  cp -- "$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || echo "${BASH_SOURCE[0]}")" "${_frozen}"
  export _SOAK_FROZEN=1
  exec bash "${_frozen}" "$@"
fi

REPO_ROOT="${_SOAK_REPO_ROOT:-}"
if [[ -z "${REPO_ROOT}" ]]; then
  REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
# shellcheck source=scripts/_hetzner_provision_lib.sh
source "${REPO_ROOT}/scripts/_hetzner_provision_lib.sh"

# ----- defaults -----
SERVER_TYPE="ccx23"
SSH_KEY_NAME="waitbus-ci-runner"
SSH_KEY_PATH="${HOME}/.ssh/hetzner_waitbus"
LOCATION="fsn1"
KEEP_SERVER="false"
SOAK_DURATION="24h"
SOAK_RATE="5"
SOAK_SAMPLE_INTERVAL="60"
SOAK_SUSPEND_CYCLES="none"
SOAK_FAULT_SCENARIOS="none"
LOCAL_OUTPUT="soak-verdict.json"

# ----- arg parse -----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-type)            SERVER_TYPE="$2"; shift 2 ;;
    --ssh-key)                SSH_KEY_NAME="$2"; shift 2 ;;
    --ssh-key-path)           SSH_KEY_PATH="$2"; shift 2 ;;
    --location)               LOCATION="$2"; shift 2 ;;
    --keep)                   KEEP_SERVER="true"; shift ;;
    --duration)               SOAK_DURATION="$2"; shift 2 ;;
    --rate)                   SOAK_RATE="$2"; shift 2 ;;
    --sample-interval)        SOAK_SAMPLE_INTERVAL="$2"; shift 2 ;;
    --inject-suspend-cycles)  SOAK_SUSPEND_CYCLES="$2"; shift 2 ;;
    --inject-fault-scenarios) SOAK_FAULT_SCENARIOS="$2"; shift 2 ;;
    --output)                 LOCAL_OUTPUT="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,46p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown flag $1" >&2
      exit 2
      ;;
  esac
done

# ----- prereqs + token -----
hetzner_check_prereqs

COMMIT_SHA="$(cd "${REPO_ROOT}" && git rev-parse --short HEAD)"
HETZNER_SERVER_NAME="waitbus-soak-$(date +%Y%m%d-%H%M%S)-${COMMIT_SHA}"
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

# ----- run the soak inside a detached tmux session -----
# Why tmux: the soak runs for up to 24 hours. A bare `ssh -tt
# remote-cmd` model means the remote process is tethered to the
# operator's ssh session, and a transient network blip / laptop
# sleep > 90 s (the ServerAliveInterval grace) hangs up ssh, which
# propagates SIGHUP to the remote soak.py and kills it mid-run.
# tmux keeps the remote process alive in its own session; ssh just
# attaches as a viewer. Detach + reattach is free; ssh death is
# inconsequential.
TMUX_SESSION="waitbus-soak"
REMOTE_VERDICT="/root/waitbus/soak-verdict.json"
REMOTE_PROGRESS="/root/waitbus/soak-verdict.json.progress.jsonl"
REMOTE_DONE_MARKER="/root/waitbus/soak-done.exit-code"
echo "[soak] ===== running scripts.soak on ${HETZNER_SERVER_IP} (tmux: ${TMUX_SESSION}) ====="
echo "[soak] duration=${SOAK_DURATION} rate=${SOAK_RATE}/s"
echo "[soak] sample-interval=${SOAK_SAMPLE_INTERVAL}s suspend-cycles=${SOAK_SUSPEND_CYCLES} fault-scenarios=${SOAK_FAULT_SCENARIOS}"

# Step 1: install tmux if absent + launch the soak in a detached
# tmux session. The session command also writes the exit code to a
# marker file when soak.py returns, so the follow-loop below can
# distinguish "still running" from "finished cleanly" without
# inspecting tmux internals.
ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
  set -euo pipefail
  if ! command -v tmux >/dev/null 2>&1; then
    apt-get install -y -qq tmux >/dev/null
  fi
  rm -f ${REMOTE_DONE_MARKER}
  tmux kill-session -t ${TMUX_SESSION} 2>/dev/null || true
  tmux new-session -d -s ${TMUX_SESSION} \
    'cd /root/waitbus && \
     export PATH=/root/.local/bin:\$PATH && \
     export UV_LINK_MODE=copy && \
     stdbuf -oL -eL uv run python -u -m scripts.soak \
       --duration ${SOAK_DURATION} \
       --rate ${SOAK_RATE} \
       --sample-interval ${SOAK_SAMPLE_INTERVAL} \
       --inject-suspend-cycles ${SOAK_SUSPEND_CYCLES} \
       --inject-fault-scenarios ${SOAK_FAULT_SCENARIOS} \
       --output ${REMOTE_VERDICT}; \
     echo \$? > ${REMOTE_DONE_MARKER}'
  echo '[remote] soak launched in tmux session ${TMUX_SESSION}'
"

# Step 2: follow the progress JSONL until the soak finishes. The
# follow loop re-establishes ssh on each poll, so a network blip
# only costs us one poll's worth of latency (not the whole run).
# The JSONL is the source of truth for live progress; the operator
# can also `ssh ... tmux attach -t waitbus-soak` from another shell
# to see the soak's own stderr stream directly.
echo "[soak] (per-sample progress streams below; the soak survives ssh drops)"
echo "[soak] (to attach to the tmux session manually: ssh -t -i ${SSH_KEY_PATH} root@${HETZNER_SERVER_IP} tmux attach -t ${TMUX_SESSION})"
PROGRESS_OFFSET=0
while true; do
  # Pull any new bytes appended to the remote progress JSONL since
  # last poll. Using `tail -c +N+1` rather than `tail -f` keeps the
  # ssh invocation short-lived so a NAT timeout cannot kill the
  # follow loop itself.
  NEW_BYTES="$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
    if [[ -f ${REMOTE_PROGRESS} ]]; then
      tail -c +$((PROGRESS_OFFSET + 1)) ${REMOTE_PROGRESS}
    fi
  " 2>/dev/null || true)"
  if [[ -n "${NEW_BYTES}" ]]; then
    # Echo new JSONL records; one human-readable line per record.
    while IFS= read -r line; do
      [[ -z "${line}" ]] && continue
      python3 -c "
import json, sys
rec = json.loads(sys.argv[1])
print(f\"[soak] t={rec['offset_sec']/3600:7.3f}h  rss={rec['rss_bytes']/(1024*1024):7.2f} MiB  fd={rec['fd_count']:4d}  wal={rec['wal_bytes']/(1024*1024):6.2f} MiB  ({rec['kind']})\")
" "${line}"
    done <<< "${NEW_BYTES}"
    PROGRESS_OFFSET=$((PROGRESS_OFFSET + $(echo -n "${NEW_BYTES}" | wc -c)))
  fi
  # Has the soak finished?
  EXIT_CODE="$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
    if [[ -f ${REMOTE_DONE_MARKER} ]]; then cat ${REMOTE_DONE_MARKER}; fi
  " 2>/dev/null || true)"
  if [[ -n "${EXIT_CODE}" ]]; then
    echo "[soak] remote soak.py exited with code ${EXIT_CODE}"
    break
  fi
  sleep 30
done

# ----- pull verdict + progress back -----
LOCAL_PROGRESS="${LOCAL_OUTPUT}.progress.jsonl"
echo "[soak] pulling verdict + progress back to local..."
scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:${REMOTE_VERDICT}" "${LOCAL_OUTPUT}"
scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:${REMOTE_PROGRESS}" "${LOCAL_PROGRESS}" || true

OVERALL_PASSED="$(python3 -c "import json,sys; d=json.load(open('${LOCAL_OUTPUT}')); print(d.get('overall_passed', False))")"
N_SAMPLES="$(python3 -c "import json; d=json.load(open('${LOCAL_OUTPUT}')); print(d.get('n_samples', 0))")"

echo "[soak] done. verdict=${LOCAL_OUTPUT}  progress=${LOCAL_PROGRESS}"
echo "[soak] overall_passed=${OVERALL_PASSED}  n_samples=${N_SAMPLES}"
echo "[soak] NOTE: verdict is informational. No publish or tag-push step runs."
echo "[soak]       The operator inspects and acts manually."

if [[ "${OVERALL_PASSED}" == "True" ]]; then
  exit 0
else
  exit 1
fi
