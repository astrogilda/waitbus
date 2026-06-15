#!/usr/bin/env bash
# Capture canonical bench baselines on a freshly provisioned Hetzner
# CCX (dedicated-vCPU) cloud server, then destroy the server.
#
# The bench harness emits a clear warning when reproducibility-relevant
# knobs are not set (cpufreq governor, ASLR, CPU affinity); the
# baselines committed under benchmarks/baselines/*.json must be
# captured with those knobs applied so regression-gate comparisons
# carry signal, not noise. Running on the operator's daily-driver
# workstation conflicts with browser / IDE CPU usage; running on a
# shared-vCPU cloud (CX, CPX, CAX) introduces steal-time variance from
# other tenants. CCX dedicated avoids both problems.
#
# Default: CCX23 (4 dedicated vCPU, 16 GB RAM, 160 GB NVMe, ~EUR 0.044/h).
# Override via --server-type for a bigger box; CCX33 is the right pick
# if you intend to capture bench_throughput.py at the 64-subscriber
# cell, which exercises many emitter + subscriber threads.
#
# Usage:
#   scripts/capture_baselines.sh [--server-type ccx23] [--ssh-key NAME]
#                                [--ssh-key-path PATH] [--location fsn1]
#                                [--keep] [--benches "bench_a bench_b ..."]
#
# Verdict: prints the destination path of the captured baselines and
# exits 0. On any failure the VM is destroyed before exit (unless
# --keep is set) so a half-captured run does not silently consume
# Hetzner billing.

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
BENCH_LIST=""

# ----- arg parse -----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-type)  SERVER_TYPE="$2"; shift 2 ;;
    --ssh-key)      SSH_KEY_NAME="$2"; shift 2 ;;
    --ssh-key-path) SSH_KEY_PATH="$2"; shift 2 ;;
    --location)     LOCATION="$2"; shift 2 ;;
    --keep)         KEEP_SERVER="true"; shift ;;
    --benches)      BENCH_LIST="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,28p' "$0"
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
HETZNER_SERVER_NAME="waitbus-bench-$(date +%Y%m%d-%H%M%S)-${COMMIT_SHA}"
export HETZNER_SERVER_NAME KEEP_SERVER

# Build SSH_OPTS array from the lib's emit.
mapfile -t SSH_OPTS < <(hetzner_default_ssh_opts)

# ----- provision + trap-guarded cleanup -----
hetzner_provision_server "${HETZNER_SERVER_NAME}" "${SERVER_TYPE}" "${LOCATION}" "${SSH_KEY_NAME}"
trap hetzner_cleanup EXIT INT TERM

# ----- wait, tune, ship, install -----
hetzner_wait_for_ssh SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"
hetzner_apply_tuning_knobs SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"
hetzner_ship_repo SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}" "${REPO_ROOT}" "${COMMIT_SHA}"
hetzner_install_deps SSH_OPTS "${HETZNER_SERVER_IP}" "${SSH_KEY_PATH}"

# ----- pin nats image (one-shot remote step) -----
ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
  set -euo pipefail
  cd /root/waitbus
  export PATH=/root/.local/bin:\$PATH
  echo '[remote] pinning nats image...'
  bash benchmarks/setup/pin_nats_image.sh
  mkdir -p benchmarks/baselines
"

# ----- run benches (one ssh per bench, output streams in real-time) -----
if [[ -z "${BENCH_LIST}" ]]; then
  BENCH_LIST="bench_notify_to_wake bench_predicate_eval_latency_multi bench_ttfae_pytest bench_ttfae_fs bench_ttfae_first_match bench_predicate_wait_under_mixed_load bench_ttfae_github bench_ttfae_docker bench_polling_baseline_github bench_polling_baseline_pytest bench_polling_baseline_docker bench_polling_baseline_fs bench_polling_baseline_fs_inotifywait bench_idle_rss bench_throughput"
fi

# Pick the two highest-numbered cores on the remote for taskset pinning.
# The script previously hardcoded -c 2,3 which works on CCX23+ (4+ vCPU)
# but fails on CCX13 (2 vCPU; cores 0 and 1 only) with
# "taskset: failed to set ... affinity: Invalid argument". The auto-pick
# uses the two highest cores so the OS keeps the lower-numbered cores
# free for kernel + ssh + accept threads, the same separation the
# original 2,3 choice provided on 4-vCPU.
REMOTE_VCPU=$(ssh -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "nproc")
if [[ "${REMOTE_VCPU}" -ge 4 ]]; then
  PIN_CORES="$((REMOTE_VCPU - 2)),$((REMOTE_VCPU - 1))"
elif [[ "${REMOTE_VCPU}" -ge 2 ]]; then
  # 2-vCPU box: pin to core 1; leave 0 for kernel + ssh.
  PIN_CORES="1"
else
  PIN_CORES="0"
fi
echo "[baselines] remote has ${REMOTE_VCPU} vCPU; pinning bench to core(s) ${PIN_CORES}"

for bench in ${BENCH_LIST}; do
  name="${bench#bench_}"
  out="benchmarks/baselines/${name}.json"
  echo "[baselines] ===== ${bench} ====="
  echo "[baselines] (output streams below in real-time)"
  # -tt forces pty allocation so each remote stderr line flushes to
  # the local terminal immediately. stdbuf -oL -eL on the remote
  # bench command forces line-buffering at the OS level for the
  # Python stdout/stderr pipeline, defeating Python's block-buffer
  # default when stdout is not a terminal. Together they make the
  # bench's per-iteration progress visible in real-time.
  ssh -tt -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}" "
    set -euo pipefail
    cd /root/waitbus
    export PATH=/root/.local/bin:\$PATH
    export UV_LINK_MODE=copy
    stdbuf -oL -eL taskset -c ${PIN_CORES} uv run python -u -m benchmarks.${bench} --output ${out}
    echo '[remote] wrote ${out}'
  "
done

# ----- pull baselines back -----
LOCAL_BASELINES="${REPO_ROOT}/benchmarks/baselines"
echo "[baselines] pulling captured JSONs back to ${LOCAL_BASELINES}..."
mkdir -p "${LOCAL_BASELINES}"
scp -i "${SSH_KEY_PATH}" "${SSH_OPTS[@]}" "root@${HETZNER_SERVER_IP}:waitbus/benchmarks/baselines/*.json" "${LOCAL_BASELINES}/"

echo "[baselines] done. Captured files:"
ls -la "${LOCAL_BASELINES}"
echo "[baselines] review the JSONs, then commit with:"
echo "  git add benchmarks/baselines/*.json && git commit -m 'feat(benchmarks): capture canonical baselines from ${SERVER_TYPE}'"
