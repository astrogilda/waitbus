# Contributing to waitbus

waitbus is a single-author project at present. Contributions are welcome via
GitHub issues (bug reports, feature requests) and pull requests. This document
covers the local development workflow, per-surface testing, version sync, and
release policy. For install and operation, see [README.md](../README.md).

---

## Quick start (local dev)

```bash
git clone https://github.com/astrogilda/waitbus && cd waitbus
uv sync --all-groups
uvx prek install -f
uv run pytest tests/ -p no:xdist
```

All runtime deps live in `[project.dependencies]`; dev deps (pytest, ruff,
mypy, hypothesis, build, tiktoken) live in `[dependency-groups].dev`.
`uv sync --all-groups` installs both. The git hook manager is `prek`
(Rust drop-in alternative to the pre-commit framework, schema-compatible
with the same `.pre-commit-config.yaml`); CI runs `uvx prek run --all-files`
on every push.

---

## Project conventions

- **Python target:** 3.11 minimum (`requires-python = ">=3.11"`). CI tests 3.11, 3.12, 3.13, and 3.14.
- **Ruff rule set:** `["E", "F", "I", "N", "UP", "B", "SIM", "RUF"]`. `tests/*` allows `E402` for property-test deferred imports.
- **Line length:** 120 characters.
- **Mypy strict** on `waitbus/` only; tests are excluded (`[tool.mypy] files = ["waitbus"]`).
- **Tests run serial:** `pytest -p no:xdist`. Hypothesis fixture races occur under xdist.
- **Runtime dependency closure:** `typer` (CLI), `platformdirs` (path resolution), `mcp` + `pydantic` + `pydantic-core` (MCP server).

### Per-surface dev commands

```bash
# Full test suite (serial)
uv run pytest tests/ -vv -p no:xdist

# Lint
uv run ruff check waitbus tests scripts

# Type-check
uv run mypy waitbus

# Integration test (wheel build + install-in-venv + entry-point assertions)
uv run pytest tests/test_install.py -vv -p no:xdist -m slow
```

The test suite has ~600 tests. Use `-p no:xdist` (serial mode) in development
and CI to avoid Hypothesis fixture races. The slow integration test builds a
wheel, installs it into a fresh venv, and asserts entry-points and
`systemd-analyze verify` output — it runs in roughly one second on a warm cache.

### MCP server

The MCP server lives in `waitbus/mcp.py` and is reachable via
`waitbus mcp serve`. It subscribes to the broadcast socket over stdlib
`socket.socket(AF_UNIX, SOCK_STREAM)` and re-emits each non-heartbeat frame
as both a vendor-specific Claude Code notification frame
(`notifications/claude/channel`) and a standard `notifications/resources/updated`
frame (for Claude Desktop and generic MCP clients).

```bash
# Run the MCP-server unit tests (fast, mocks the broadcast socket)
uv run pytest tests/test_mcp.py -vv -p no:xdist

# Run the server against a live broadcast daemon (Ctrl+D to stop)
uv run waitbus mcp serve
```

On macOS the server logs one info message and idles — the broadcast daemon
stack is Linux-only.

### Systemd units

Validate units before committing:

```bash
systemd-analyze --user verify systemd/*.service systemd/*.socket systemd/*.timer
```

CI runs this check as well. When adding or removing unit files, update
`systemd/MANIFEST.txt` — `waitbus install-systemd --sync` reads that file
to compute orphan units from previous package versions.

---

## Agent doc-QA

waitbus's primary users are coding agents, so the docs have a second reader:
the model that reads them to write code. Before finalising a change that
touches the public surface — the `waitbus/__init__.py` docstring, the MCP tool
descriptions / schemas in `waitbus/mcp.py` and `waitbus/_mcp_models.py`, the
SDK docstrings, `AGENTS.md`, the README, or `docs/` — QA it the way an agent
will consume it:

1. Build and install into a clean environment:
   ```bash
   uv build --wheel && uv tool install --force dist/waitbus-*.whl
   ```
2. In a fresh shell, give a coding agent a realistic task and no prior
   context — e.g. "wait until pytest passes in this repo, then tell me which
   job failed", or "have two agents coordinate a handoff over waitbus".
3. Watch which files and tool schemas it opens first, where it guesses (a gap
   in the published surface), and where it reaches for a private (`_`-prefixed)
   symbol.
4. Fix the surface it stumbled on — a docstring, a schema description, the
   MCP server `instructions`, a doc pointer — and repeat.

`uvx waitbus demo` and `uvx waitbus swarm-demo` run fully offline in a temp
directory, so this loop is cheap and needs no real CI wiring. The goal: an
agent can use waitbus correctly from the published surface alone, without
reading the source.

---

## Development install vs released install

**Working from a checkout:**

```bash
uv pip install --editable .
```

Console scripts point at your source tree — edits are picked up immediately.
However, editable installs do **not** place `share/systemd/user/` on disk.
Hatchling's `shared-data` directive only fires during a wheel build, not during
`--editable`. For systemd-unit testing during dev, build a wheel and install it
into a throwaway venv:

```bash
uv build --wheel
# Install the produced dist/waitbus-*.whl into a fresh venv
```

Or symlink the dev unit files to `~/.config/systemd/user/` manually for rapid
iteration without a wheel rebuild.

**Working from a released wheel:**

```bash
uv tool install waitbus
waitbus install-systemd
```

---

## Loading the plugin locally

To test the full Claude Code plugin without installing from the registry:

```bash
claude --plugin-dir /path/to/waitbus --dangerously-load-development-channels
```

`.mcp.json` invokes `uvx --from waitbus waitbus mcp serve`, which
resolves the latest published `waitbus` from PyPI. To exercise local
source instead, either `uv tool install --editable .` from this directory
(rebinds the `waitbus` shim onto your working tree) or temporarily swap the
`.mcp.json` `args` to `["run", "waitbus", "mcp", "serve"]`.

---

## Updating the sdist manifest snapshot

The sdist hygiene test in `tests/test_sdist_manifest.py` asserts that the file
list inside the produced source distribution matches a committed snapshot at
`tests/data/expected-sdist-manifest.txt`, plus a size budget, a copyleft-header
scan, and a forbidden-paths regression guard.

When you add or remove a file that ships in the sdist, regenerate the snapshot
in the same PR:

```bash
uv build --sdist
tar tzf dist/waitbus-*.tar.gz | sort > tests/data/expected-sdist-manifest.txt
```

Review the diff before committing — an unexpected addition is exactly what this
test is designed to catch.

---

## Linux + systemd assumption

The daemon stack is **Linux-only** at runtime. The Python library, listener, ETag
poller, and query CLI work on macOS, but without systemd integration.

Specific reasons:

- `broadcast.py` authenticates subscribers with `SO_PEERCRED` and coalesces
  doorbell wakeups with `os.eventfd`, both Linux-only. The wire itself is
  `AF_UNIX SOCK_STREAM` with length-prefix framing (portable; macOS lacks
  `SOCK_SEQPACKET`), but the broadcast, doorbell, and `read-events --watch`
  test modules carry `pytestmark = pytest.mark.skipif(sys.platform != "linux", ...)`
  for the eventfd and peer-credential paths.
- `broadcast._peer_uid` resolves `SO_PEERCRED` via
  `getattr(socket, "SO_PEERCRED", None)` so the package still imports cleanly
  on macOS for the library and listener surfaces.
- The MCP server (`waitbus/mcp.py`) subscribes to the broadcast bus; on macOS
  it emits one info-severity notification and exits cleanly because that
  broadcast daemon stack is Linux-only.

---

## Version sync

The canonical version lives in `pyproject.toml [project].version`. Three
manifests must agree: `pyproject.toml`, `.claude-plugin/plugin.json`, and
`server.json`. Propagate with:

```bash
scripts/sync-versions.py           # mutate all three manifests; exit 0
scripts/sync-versions.py --check   # exit 1 on any drift
```

The `--check` mode runs in the pre-commit hook and in CI. Never bump version
fields manually in `plugin.json` or `server.json` — only edit `pyproject.toml`
and then run `sync-versions.py`.

---

## Commit conventions

Commits and PR titles follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(waitbus): add broadcast lag counter metric
fix(waitbus): handle SIGTERM during fan-out drain
docs(waitbus): expand install-systemd shared-data note
test(waitbus): add Hypothesis property tests for ULID ordering
chore(waitbus): update pre-commit hook versions
ci(waitbus): add systemd-analyze verify step to workflow
```

Rules:
- No emoji.
- No internal jargon (no internal project jargon).
- Subject line under 72 characters, imperative mood.
- PR body explains *why*, not *what*.

---

## Commit signing

We recommend signing your commits. Release tags are already signed, and signed
commits extend that chain of trust back through the history they tag.

Enable signing locally with either an SSH key or a GPG key:

```bash
# SSH-key signing (simplest if you already push over SSH):
git config commit.gpgsign true
git config gpg.format ssh
git config user.signingkey ~/.ssh/id_ed25519.pub

# Or GPG-key signing:
git config commit.gpgsign true
git config user.signingkey <your-gpg-key-id>
```

This is a recommendation, not a CI gate: signed commits are not enforced on
pull requests (that would be premature ahead of a public, multi-contributor
flow). The signed release tag remains the enforced trust boundary.

---

## Console scripts (1)

The package ships a single entry point: `waitbus`. All previous per-daemon
scripts are now sub-commands of that umbrella CLI.

| Sub-command | Purpose |
|-------------|---------|
| `waitbus init` | Create state directory and SQLite schema |
| `waitbus install-systemd` | Copy systemd user units (Linux) |
| `waitbus install-launchd` | Copy launchd plists (macOS) |
| `waitbus keygen` | Store HMAC secret in OS keyring |
| `waitbus doctor` | Health-gate: config, keyring, binaries, daemons |
| `waitbus status` | Operational dashboard: event counts, daemon liveness |
| `waitbus verify-plugin` | Validate `.claude-plugin/plugin.json` fields |
| `waitbus listener serve` | HTTP webhook receiver (loopback :9000) |
| `waitbus broadcast serve` | AF_UNIX SOCK_STREAM fan-out daemon |
| `waitbus etag-poll run` | ETag-aware backup poller (one-shot) |
| `waitbus mcp serve` | MCP server (stdio), re-emits broadcast events |
| `waitbus read-events list` | Query the event store |
| `waitbus read-events watch` | Tail the event store (live) |
| `waitbus pr-monitor tick` | Roll job events into per-PR state |
| `waitbus watchdog-check run` | Alertmanager watchdog freshness probe |

---

## Systemd units (8)

| Unit file | Purpose |
|-----------|---------|
| `waitbus-listener.service` | Webhook HTTP server |
| `waitbus-broadcast.service` | Broadcast daemon (socket-activated) |
| `waitbus-broadcast.socket` | AF_UNIX SOCK_STREAM activation socket |
| `waitbus-etag-poll.service` | ETag poller one-shot service |
| `waitbus-etag-poll.timer` | 45-second timer for the poller |
| `waitbus-watchdog.service` | Ingestion-silence detector service |
| `waitbus-watchdog.timer` | Timer for the watchdog probe |
| `waitbus-forward@.service` | Templated per-repository forwarder |

All eight units use `ExecStart=%h/.local/bin/waitbus <subcmd> <verb>` and
carry systemd hardening directives (`PrivateTmp`, `ProtectSystem=strict`,
`NoNewPrivileges`, `SystemCallFilter` allow-list). `waitbus install-systemd`
copies them from the wheel's `share/systemd/user/` destination into
`~/.config/systemd/user/`.

---

## `--dry-run` vs `doctor`

- **`waitbus init --dry-run`** — prints intended actions and always exits 0.
  Use for inspection before committing to install actions.
- **`waitbus doctor`** — reads the *current* state of config, keyring,
  binaries, and systemd units. Exits 0 if everything resolves; exits 1 on any
  issue. Use as a health gate before and after deploys.

---

## Versioning policy

This project follows [Semantic Versioning](https://semver.org/). Pre-1.0
releases may refine the API based on real-world usage. After 6+ months of
stable public usage, v1.0 will declare API stability.

---

## Release process

1. Bump `pyproject.toml [project].version` to the target (e.g., `0.2.0`).
2. Run `scripts/sync-versions.py` to propagate to all manifests.
3. Update `CHANGELOG.md` — add a dated section for the new version.
4. Commit + push + open PR. CI must pass before merging.
5. After merge to main:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
6. The tag triggers `.github/workflows/release.yml`, which:
   - Publishes `waitbus` to PyPI (OIDC + sigstore attestations).
   - Registers `io.github.astrogilda/waitbus` with the MCP Registry.
   - Creates a GitHub Release with the plugin `.zip` attached.

Pre-release tags (`-rc1`, `-alpha`, `-beta`, `-dev`) trigger the full test
matrix as a rehearsal but skip every publish job, so the PyPI version slot is
not burned on a dry run. Only tags matching `vN.N.N` or `vN.N.N.postN` shapes
invoke the publish jobs.

### Partial-publish recovery

The three publish steps (PyPI, MCP Registry, GitHub Release) are
decoupled. If one succeeds and another fails:

- **PyPI** is immutable. Bump to `0.2.0.post1` in `pyproject.toml`, run
  `sync-versions.py`, and retag `v0.2.0.post1`. The `.post1` PEP 440
  post-release suffix signals the artifact identity is stable.
- **MCP Registry** accepts a retag of the same version number, so `.post1` is
  only required when PyPI was the registry that succeeded.

---

## Code of Conduct

A `CODE_OF_CONDUCT.md` lives at the repo root (Contributor Covenant 2.1 adoption).

---

## License

MIT. By contributing you agree that your contributions are MIT-licensed under
the same terms as the rest of this project. See [LICENSE](../LICENSE).
