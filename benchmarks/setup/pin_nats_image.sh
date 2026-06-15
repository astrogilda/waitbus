#!/usr/bin/env bash
# Pull the canonical nats-server Docker image and record its sha256
# digest into benchmarks/baselines/pinned_images.json so
# `bench_idle_rss.py` can run against a byte-reproducible reference.
#
# Run once per bench host; re-run any time you want to refresh the
# pin (e.g. to pick up a newer NATS minor/patch release).
#
# Pinned by digest for reproducible benchmark runs.
# and benchmarks/BENCHMARKING.md (idle-RSS comparison must pin the
# competing image so a future operator can re-run the exact same
# comparison; an unpinned `nats:2.10-alpine` would silently update
# under both feet).

set -euo pipefail

# Tag is intentionally not the latest NATS release: this is the
# in-tree comparison reference, so a major-release bump goes through
# a deliberate update commit (with the corresponding launch-article
# RSS number re-blessed). 2.10-alpine is the LTS-track Alpine variant
# at bench-suite-author time.
NATS_IMAGE_TAG="nats:2.10-alpine"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PIN_FILE="${REPO_ROOT}/benchmarks/baselines/pinned_images.json"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker not found on PATH" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "error: docker daemon unreachable (is the service running?)" >&2
  exit 1
fi

echo "Pulling ${NATS_IMAGE_TAG}..."
docker pull "${NATS_IMAGE_TAG}" >/dev/null

DIGEST="$(docker image inspect "${NATS_IMAGE_TAG}" --format '{{index .RepoDigests 0}}')"
if [[ -z "${DIGEST}" ]]; then
  echo "error: docker reported no RepoDigest for ${NATS_IMAGE_TAG}" >&2
  exit 1
fi

mkdir -p "$(dirname "${PIN_FILE}")"

# Compose a single-key JSON file. If other pins are ever added (e.g.
# a redis comparison), this script grows; for now it owns the file.
cat >"${PIN_FILE}" <<EOF
{
  "nats": "${DIGEST}"
}
EOF

echo "Pinned ${NATS_IMAGE_TAG} -> ${DIGEST}"
echo "Wrote ${PIN_FILE}"
