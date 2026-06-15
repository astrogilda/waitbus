#!/usr/bin/env bash
# Run the full pytest suite with persistent, retro-inspectable output.
#
# Drops three timestamped files under .local-pytest-logs/ (gitignored):
#   <ts>.log         -- verbose stdout / stderr with stdbuf line-buffering so
#                       tail -f works while the run is in progress.
#   <ts>.junit.xml   -- machine-parseable per-test result (skip reason, fail
#                       traceback, duration). Survives reboot; query later
#                       with `xmllint --xpath` or any junit reader.
#   <ts>.warnings.log -- every Python warning surfaced at emit site (via
#                       `-W default`), not just the end-of-run summary.
#
# Pass-through: any extra args ("$@") are forwarded to pytest, so the same
# wrapper covers ad-hoc subset runs (e.g. `run_full_pytest.sh tests/test_x.py`).
#
# Exit code is pytest's exit code.

set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/.local-pytest-logs"
TS="$(date +%Y%m%d_%H%M%S)"

mkdir -p "${LOG_DIR}"

LOG="${LOG_DIR}/${TS}.log"
JUNIT="${LOG_DIR}/${TS}.junit.xml"
WARNINGS="${LOG_DIR}/${TS}.warnings.log"

echo "[run_full_pytest] starting at ${TS}" | tee "${LOG}"
echo "[run_full_pytest]   stdout/stderr: ${LOG}" | tee -a "${LOG}"
echo "[run_full_pytest]   junit-xml:     ${JUNIT}" | tee -a "${LOG}"
echo "[run_full_pytest]   warnings:      ${WARNINGS}" | tee -a "${LOG}"

cd "${PROJECT_ROOT}"

# `-W default` surfaces every warning category (Deprecation, Pending,
# Runtime, User, Resource, ...) at emit site, separately from the
# end-of-run pytest summary. `2>` separately captures pytest's own
# warning lines so the warnings file is grep-friendly.
stdbuf -oL uv run python -m pytest tests/ \
  -p no:xdist \
  -vv \
  -W default \
  --junit-xml="${JUNIT}" \
  "$@" \
  > >(tee -a "${LOG}") \
  2> >(tee -a "${LOG}" >&2)

RC=$?

# Pull every warning line out of the verbose log so it's a one-file lookup.
# Pattern matches both pytest's "<file>:<line>: <Category>: <msg>" and the
# end-of-run "warnings summary" block.
grep -iE "Warning|Deprecat" "${LOG}" > "${WARNINGS}" || true

echo "[run_full_pytest] finished rc=${RC}; logs at ${LOG_DIR}/${TS}.*"
exit "${RC}"
