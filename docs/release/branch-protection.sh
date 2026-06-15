#!/usr/bin/env bash
# branch-protection.sh — Apply GitHub branch-protection rules to refs/heads/main
#
# Run ONCE by the operator immediately after the repository is made
# public and the first release tag has been pushed.  Safe to re-run: the PUT
# endpoint is idempotent and overwrites the existing ruleset in full.
#
# Prerequisites:
#   - gh CLI authenticated with a token that has repo admin scope
#     (`gh auth status` should show the token's scopes)
#   - The repository must already be public (branch-protection rules
#     on private repos require a GitHub Pro/Team plan; this project
#     runs on the free tier)
#
# Usage:
#   bash docs/release/branch-protection.sh
#
# Dry-run (inspect the JSON payload without applying):
#   DRY_RUN=1 bash docs/release/branch-protection.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO="astrogilda/waitbus"
BRANCH="main"

# Required status-check contexts — these are the EXACT check names that
# GitHub records for each ci.yml job.  They come from the `name:` field
# of each job (matrix cells expand the template literally):
#
#   python job (matrix: os=[ubuntu-latest, macos-14])
#     → "Tests (py3.13, ubuntu-latest)"
#     → "Tests (py3.13, macos-14)"
#   The ubuntu-latest cell also exercises waitbus self-dogfooding and the
#   per-file coverage gate; both are sub-steps of the same job, so a
#   single context name covers them.
#
#   reproducibility job
#     → "Reproducible-build gate (byte-identical sdist + wheel)"
#   Required because build-determinism regressions must not reach a tag.
#
#   server-json job
#     → "server.json schema validation"
#   Required because a malformed MCP Registry manifest would fail the
#   publish-mcp-registry step in release.yml.
#
#   canary-mcp-sdk job (matrix: mcp-version=[1.26.0, 1.27.0])
#     → "MCP SDK canary (1.26.0)"
#     → "MCP SDK canary (1.27.0)"
#   Required because the two-tier MCP wire fixtures must pass against
#   both the prior-patch and prior-minor SDK releases before any merge.
#
# Intentionally NOT listed as required contexts:
#   - Individual ci.yml steps (pre-commit, mypy, coverage gate, etc.) —
#     these are sub-steps of the `python` job; their failure already
#     fails the job-level context.
#   - release.yml jobs — those run only on tag push, not on PRs.

REQUIRED_CONTEXTS='[
  "Tests (py3.13, ubuntu-latest)",
  "Tests (py3.13, macos-14)",
  "Reproducible-build gate (byte-identical sdist + wheel)",
  "server.json schema validation",
  "MCP SDK canary (1.26.0)",
  "MCP SDK canary (1.27.0)"
]'

# ---------------------------------------------------------------------------
# Build the protection payload
# ---------------------------------------------------------------------------

# strict=true: the branch must be up-to-date with main before merging.
# This prevents a PR from merging on a green-but-stale base while a
# concurrent push has broken the contexts above.
PAYLOAD="$(cat <<EOF
{
  "required_status_checks": {
    "strict": true,
    "contexts": ${REQUIRED_CONTEXTS}
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false,
    "required_approving_review_count": 1
  },
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false
}
EOF
)"

# enforce_admins=false rationale: this is a single-maintainer project.
# Enforcing admin bypass-prevention would block the maintainer from
# performing legitimate emergency hot-fixes (e.g., yanking a broken
# release tag) without adding any meaningful safety margin — there are
# no other admins whose bypass needs guarding against.  If additional
# admins are added in the future, revisit this setting.

# ---------------------------------------------------------------------------
# Apply (or preview) the protection rules
# ---------------------------------------------------------------------------

echo "==> Branch protection payload:"
echo "${PAYLOAD}" | python3 -m json.tool   # pretty-print for review

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo ""
  echo "==> DRY_RUN=1: payload printed above; no API call made."
  exit 0
fi

echo ""
echo "==> Applying branch protection to ${REPO}:${BRANCH} ..."

gh api \
  --method PUT \
  "repos/${REPO}/branches/${BRANCH}/protection" \
  --input - <<< "${PAYLOAD}"

echo ""
echo "==> Done.  Verify at:"
echo "    https://github.com/${REPO}/settings/branches"
