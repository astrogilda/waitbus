#!/usr/bin/env bash
# Probe Honker upstream for adoption-readiness triggers. Add to weekly cron.

set -euo pipefail

# Trigger 1: PyPI manylinux wheel
curl -s "https://pypi.org/simple/honker/" -H "Accept: application/vnd.pypi.simple.v1+json" \
  | jq -r '.files[].filename' | grep -q manylinux && echo "TRIGGER1_MET"

# Trigger 2: npm tarball contains a prebuilt .so
tarball=$(npm view @russellthehippo/honker-bun dist.tarball)
curl -sL "$tarball" | tar tz | grep -qE '\.(so|node)$' && echo "TRIGGER2_MET"

# Trigger 3: README of honker-bun no longer requires libsqlite3-dev
gh api repos/russellromney/honker/contents/packages/honker-bun/src/index.ts \
  --jq '.content' | base64 -d | grep -q 'libsqlite3-dev' || echo "TRIGGER3_MET"

# Trigger 4: alpha disclaimer dropped
gh api repos/russellromney/honker/contents/README.md --jq '.content' | base64 -d \
  | grep -q 'Alpha software' || echo "TRIGGER4_MET"
