#!/usr/bin/env bash
# Run every adapter package's self-contained test suite.
#
# The adapters under adapters/ are standalone uv projects (see
# adapters/README.md); the main waitbus suite never collects them. This
# script is the advisory one-shot runner: sync each adapter's environment
# (which installs waitbus from the repo root as an editable path
# dependency) and run its pytest suite inside its own project.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"

for adapter in waitbus-pydantic-ai waitbus-langgraph; do
    echo "=== ${adapter} ==="
    uv sync --directory "$root/adapters/$adapter" --all-groups
    uv run --directory "$root/adapters/$adapter" python -m pytest -q
done
