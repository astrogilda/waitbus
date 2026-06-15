# shellcheck shell=bash
# Sourceable helpers for Hetzner-Cloud-backed one-shot job scripts.
#
# Used by:
#   - scripts/capture_baselines.sh (canonical bench baseline capture)
#   - scripts/run_soak_on_hetzner.sh (24-hour soak orchestrator)
#
# Both scripts share the same provision -> ssh-wait -> apply-knobs ->
# ship-repo -> install-deps -> run -> pull-output -> destroy pipeline.
# Only the run step differs (a bench loop vs an scp + remote scripts.soak
# invocation). Factoring the shared body here keeps the recipe in one
# place; a change to the tuning recipe touches one file.
#
# Conventions:
# - Functions read variables exported by the calling script
#   (HETZNER_SERVER_NAME, HETZNER_SERVER_IP, HETZNER_SSH_OPTS array,
#    HETZNER_SSH_KEY_PATH, REPO_ROOT, COMMIT_SHA, KEEP_SERVER).
# - The cleanup trap is installed by the caller; this lib provides
#   ``hetzner_cleanup`` as the trap target.
# - Stderr is the operator's terminal; the lib never silently
#   swallows errors that an operator would want to see.

# ----- token + prereq checks -----

hetzner_check_prereqs() {
  # Required: hcloud CLI, secret-tool, ssh, scp, a Hetzner API token
  # in the keyring under service=hcloud / account=api-key. Emits a
  # clear remediation message on each missing piece.
  if ! command -v hcloud >/dev/null 2>&1; then
    echo "error: hcloud CLI not on PATH (install: https://github.com/hetznercloud/cli)" >&2
    return 2
  fi
  if ! command -v secret-tool >/dev/null 2>&1; then
    echo "error: secret-tool not on PATH (apt install libsecret-tools)" >&2
    return 2
  fi
  HCLOUD_TOKEN="$(secret-tool lookup service hcloud account api-key 2>/dev/null || true)"
  if [[ -z "${HCLOUD_TOKEN}" ]]; then
    echo "error: Hetzner Cloud API token absent from keyring. Stage with:" >&2
    echo "  secret-tool store --label='Hetzner Cloud API' service hcloud account api-key" >&2
    return 2
  fi
  export HCLOUD_TOKEN
}

# ----- provisioning -----

hetzner_provision_server() {
  # Args: SERVER_NAME SERVER_TYPE LOCATION SSH_KEY_NAME
  # Side effect: sets HETZNER_SERVER_IP global.
  local name="$1"
  local stype="$2"
  local location="$3"
  local ssh_key_name="$4"
  echo "[hetzner] provisioning ${stype} ${name} in ${location}..." >&2
  hcloud server create \
    --name "${name}" \
    --type "${stype}" \
    --image ubuntu-24.04 \
    --location "${location}" \
    --ssh-key "${ssh_key_name}" \
    >/dev/null
  HETZNER_SERVER_IP="$(hcloud server ip "${name}")"
  export HETZNER_SERVER_IP
  echo "[hetzner] server up at ${HETZNER_SERVER_IP}" >&2
}

# ----- ssh options helper -----

hetzner_default_ssh_opts() {
  # Echo the shared SSH option array, one option per line. Caller
  # captures into a bash array via ``mapfile``. ServerAliveInterval
  # defends long-running bench/soak ssh sessions from NAT timeouts.
  cat <<'EOF'
-o
StrictHostKeyChecking=accept-new
-o
ConnectTimeout=10
-o
UserKnownHostsFile=/dev/null
-o
LogLevel=ERROR
-o
ServerAliveInterval=30
-o
ServerAliveCountMax=3
EOF
}

# ----- cleanup trap target -----

hetzner_cleanup() {
  # Called by the trap installed in the calling script. Reads
  # HETZNER_SERVER_NAME + KEEP_SERVER from the caller's scope.
  local exit_code=$?
  if [[ -z "${HETZNER_SERVER_NAME:-}" ]]; then
    exit "${exit_code}"
  fi
  if [[ "${KEEP_SERVER:-false}" == "true" ]]; then
    echo "[hetzner] --keep set; leaving ${HETZNER_SERVER_NAME} running. Delete with:" >&2
    echo "  hcloud server delete ${HETZNER_SERVER_NAME}" >&2
  else
    echo "[hetzner] destroying ${HETZNER_SERVER_NAME}..." >&2
    hcloud server delete "${HETZNER_SERVER_NAME}" >/dev/null || true
  fi
  exit "${exit_code}"
}

# ----- wait for ssh + cloud-init -----

hetzner_wait_for_ssh() {
  # Args: array name of SSH_OPTS, SERVER_IP, SSH_KEY_PATH
  # Note: bash 'nameref' via local -n keeps the array semantics intact.
  local -n _opts="$1"
  local ip="$2"
  local key_path="$3"
  echo "[hetzner] waiting for SSH..." >&2
  for _ in $(seq 1 30); do
    if ssh -i "${key_path}" "${_opts[@]}" "root@${ip}" "true" 2>/dev/null; then
      break
    fi
    sleep 5
  done
  ssh -i "${key_path}" "${_opts[@]}" "root@${ip}" "cloud-init status --wait" >/dev/null
  echo "[hetzner] cloud-init done" >&2
}

# ----- tuning knobs -----

hetzner_apply_tuning_knobs() {
  # ASLR off in guest. Governor=performance best-effort: on
  # virtualised cloud the guest cannot change the host's cpufreq
  # governor; CCX dedicated already runs at fixed performance, so
  # this is a no-op that we still try for parity with bare-metal
  # tuning recipes.
  local -n _opts="$1"
  local ip="$2"
  local key_path="$3"
  echo "[hetzner] applying tuning knobs..." >&2
  ssh -i "${key_path}" "${_opts[@]}" "root@${ip}" '
    set -e
    echo "[remote] disabling ASLR..."
    echo 0 > /proc/sys/kernel/randomize_va_space
    echo "[remote] attempting governor=performance (best-effort)..."
    if command -v cpupower >/dev/null 2>&1; then
      cpupower frequency-set -g performance >/dev/null 2>&1 || true
    else
      apt-get update -qq
      apt-get install -y -qq linux-tools-common linux-tools-generic >/dev/null 2>&1 || true
      cpupower frequency-set -g performance >/dev/null 2>&1 || true
    fi
    echo "[remote] knobs applied."
  '
}

# ----- ship repo at current commit -----

hetzner_ship_repo() {
  # Args: SSH_OPTS array name, SERVER_IP, SSH_KEY_PATH, REPO_ROOT, COMMIT_SHA
  local -n _opts="$1"
  local ip="$2"
  local key_path="$3"
  local repo_root="$4"
  local commit_sha="$5"
  local archive="/tmp/waitbus-${commit_sha}.tar.gz"
  echo "[hetzner] shipping repo at ${commit_sha}..." >&2
  (cd "${repo_root}" && git archive --format=tar.gz --prefix="waitbus/" HEAD -o "${archive}")
  scp -i "${key_path}" "${_opts[@]}" "${archive}" "root@${ip}:/tmp/" >/dev/null
  rm -f "${archive}"
  ssh -i "${key_path}" "${_opts[@]}" "root@${ip}" "
    set -e
    cd /root
    tar xzf /tmp/waitbus-${commit_sha}.tar.gz
  "
}

# ----- install deps (uv + docker) -----

hetzner_install_deps() {
  # uv via the upstream installer; docker via apt (needed by
  # bench_idle_rss + bench_polling_baseline_docker; soak also needs
  # uv though not docker, but installing docker is cheap and keeps
  # the recipe one-shot).
  local -n _opts="$1"
  local ip="$2"
  local key_path="$3"
  echo "[hetzner] installing deps (uv + docker)..." >&2
  ssh -i "${key_path}" "${_opts[@]}" "root@${ip}" '
    set -euo pipefail
    cd /root/waitbus

    echo "[remote] installing uv..."
    if ! command -v uv >/dev/null 2>&1; then
      curl -LsSf https://astral.sh/uv/install.sh | sh
      export PATH="$HOME/.local/bin:$PATH"
    fi

    echo "[remote] uv sync..."
    export UV_LINK_MODE=copy
    /root/.local/bin/uv sync --all-extras --all-groups --quiet

    echo "[remote] installing inotify-tools (needed by bench_polling_baseline_fs_inotifywait)..."
    if ! command -v inotifywait >/dev/null 2>&1; then
      apt-get update -qq >/dev/null 2>&1 || true
      apt-get install -y -qq inotify-tools >/dev/null 2>&1 || \
        echo "[remote] WARNING: inotify-tools install failed; the fs_inotifywait bench will skip." >&2
    fi

    echo "[remote] installing docker (best-effort -- only bench_idle_rss + bench_polling_baseline_docker need it)..."
    if ! command -v docker >/dev/null 2>&1; then
      # Make universe explicit and refresh the apt index. Ubuntu 24.04
      # cloud images bundle universe but the in-image apt index can
      # be stale at first boot.
      apt-get update -qq >/dev/null 2>&1 || true
      if apt-get install -y -qq docker.io >/dev/null 2>&1; then
        systemctl enable --now docker >/dev/null 2>&1 || true
        echo "[remote] docker.io installed."
      elif curl -fsSL https://get.docker.com | sh >/dev/null 2>&1; then
        systemctl enable --now docker >/dev/null 2>&1 || true
        echo "[remote] docker installed via get.docker.com."
      else
        echo "[remote] WARNING: docker install failed; docker-dependent benches will skip or fail." >&2
      fi
    fi
    echo "[remote] deps ready."
  '
}
