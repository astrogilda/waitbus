#!/usr/bin/env bash
# Wait until ANY source emits an event matching one of three predicates.
#
# Demonstrates waitbus's source-agnostic `wait --match` surface: one
# subscriber, three OR-composed predicates spanning pytest, docker, and
# github. The wrapper calls `waitbus wait` repeatedly (one per source)
# from background subshells and exits as soon as the first one returns
# zero. SIGTERMs the rest to keep them from racing the shell prompt.
#
# Usage:
#   ./wait_for_any_source.sh [timeout]
#
# Default timeout is 30s. Exit codes:
#   0   first matching frame arrived from any source
#   124 timeout (no source matched within the window)
#   1   startup failure (e.g. daemon socket missing)
#   2   bad args

set -euo pipefail

TIMEOUT="${1:-30s}"

if ! command -v waitbus >/dev/null 2>&1; then
  echo "error: waitbus CLI not on PATH (install via 'pip install waitbus' or 'uv tool install waitbus')" >&2
  exit 1
fi

# Each background subshell waits on one source predicate. The first to
# return 0 wins the race; on the first success the parent kills the
# stragglers (they'd otherwise linger until --timeout elapses).
declare -a pids=()

waitbus wait --source pytest --match 'fields.event_type="pytest_session"' --timeout "$TIMEOUT" >/dev/null 2>&1 &
pids+=($!)

waitbus wait --source docker --match 'fields.event_type="docker_container"' --timeout "$TIMEOUT" >/dev/null 2>&1 &
pids+=($!)

waitbus wait --source github --match 'fields.event_type="workflow_run"' --timeout "$TIMEOUT" >/dev/null 2>&1 &
pids+=($!)

trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT INT TERM

# `wait -n` returns the exit status of the first job to finish.
# Bash 5.1+ supports `wait -n` with no PID args; we pass the PIDs
# explicitly to stay compatible with older 4.x macOS bash.
wait -n "${pids[@]}"
first_status=$?

if [[ $first_status -eq 0 ]]; then
  exit 0
fi

# All three returned non-zero (typically 124 timeout from each).
# Surface the timeout exit code rather than the last shell's status,
# which would obscure the "nothing matched" semantics.
exit "$first_status"
