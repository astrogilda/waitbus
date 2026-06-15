# Custom source plugins

waitbus ships with six built-in event sources (`github`, `alertmanager`,
`pytest`, `docker`, `fs`, `agent`). Operators can extend the taxonomy by
installing third-party Python packages that register against the
`waitbus.sources.v1` entry-point group.

This document is the operator-facing how-to. It also states the binding
policy that governs *what* a plugin may register and *how* waitbus
enforces publisher identity.

## Plugin integration surfaces

A registered plugin source is treated identically to a built-in source
at every waitbus seam:

- the `EventInsert.source` / `Event.source` validator accepts it;
- the broadcaster's default subscriber filter includes its declared
  `event_type` values;
- `waitbus emit --source <name>` works against it;
- `waitbus wait --source <name> --match …` works against it;
- the CloudEvents projection emits `urn:waitbus:source:<name>` for it;
- `waitbus stats` lists it in `MeasuredFacts.by_source` (the estimated
  per-source poll-cost block stays scoped to the four sources that
  have an empirical token-cost derivation).

A plugin does not get to extend the broadcast wire format, alter the
SQLite schema, or hook into the subscriber engine. Those surfaces stay
internal to the core package; the local-primitive separability rule is enforced
by `tests/test_sourcespec_local_boundary.py`.

## Authoring a plugin in four steps

A complete worked example lives at `examples/custom_source_plugin/`
(a CircleCI source as the canonical demonstration); the file layout
below is the minimum a fresh plugin needs.

### 1. Declare the entry-point in your package's `pyproject.toml`

```toml
[project]
name = "waitbus-circleci"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["waitbus>=0.5,<0.6"]

[project.entry-points."waitbus.sources.v1"]
circleci = "waitbus_circleci:plugin"
```

The entry-point group name is **`waitbus.sources.v1`**. The `.v1` suffix
is the contract version: when waitbus introduces a breaking
`SourceSpec` change the group will become `waitbus.sources.v2`, and
waitbus enumerates both groups during the transition window. The
key (`circleci`) is the canonical source name operators will type at
`--source`; the value (`waitbus_circleci:plugin`) is the dotted import
path to the plugin singleton.

### 2. Implement the `SourcePlugin` Protocol

```python
# waitbus_circleci/__init__.py
from waitbus.sources import SourceSpec

class CircleCISource:
    def spec(self) -> SourceSpec:
        return SourceSpec(
            name="circleci",
            event_types=("pipeline_finished",),
            payload_schema=None,
            api_version=1,
        )

plugin = CircleCISource()
```

`SourceSpec` is a frozen `msgspec.Struct` with four fields: `name` (the
canonical source string, lowercase ASCII matching `[a-z][a-z0-9_]*`,
and required to equal the entry-point key from your `pyproject.toml`),
`event_types` (a non-empty tuple of allowed `event_type` values this
source may emit, each matching the same regex as `name` and each
required not to collide with any built-in or other-plugin event_type),
`payload_schema` (an optional `msgspec.Struct` subclass for opt-in
strict payload validation; pass `None` for opaque-payload sources),
and `api_version` (currently `1`, must be a positive non-bool int).
All four fields are validated at construction time by
`SourceSpec.__post_init__`; ill-formed values raise `ValueError`
immediately rather than poisoning the registry.

The `SourcePlugin` Protocol is *not* decorated `@runtime_checkable`:
waitbus validates the contract via `inspect.signature` at registration
time. This catches signature drift that `@runtime_checkable` would
miss (it only checks attribute presence) without the per-`isinstance`
performance cost.

### Producing events

waitbus does NOT prescribe any producer method on the plugin object.
Plugins typically run their own long-lived producer loop (as a
systemd or launchd service) that calls the public
`waitbus.emit` API directly:

```python
from waitbus import emit
from waitbus._types import EventInsert

# Inside your CircleCI poller's main loop:
emit(EventInsert(
    source="circleci",
    event_type="pipeline_finished",
    delivery_id="circleci-pipeline-12345",
    owner="acme",
    repo="widgets",
    received_at=...,  # epoch ns
    payload_json="{...}",
    ingest_method="poll",
))
```

(Earlier docs described an optional `fetch` method on the plugin
object; that method was registry-documentation-only and has been dropped
from the SourcePlugin Protocol. Your
plugin module just needs the `spec()` method now; the producer loop
is your own code.)

#### Plugins extend `event_type`, not the wire `kind`

A plugin's events flow onto the broadcast wire as ordinary data frames
(`kind="event"`) carrying your declared `event_type`. Plugins extend the
open `event_type` value-space; they do NOT define new wire frame `kind`
values. The five wire kinds (`event`, `truncated`, `daemon_heartbeat`,
`subscribe_ack`, `subscribe_rejected`) are owned by the daemon and frozen
by the v1 wire protocol. If you have a use case that seems to need a custom
control frame, open an issue — a `waitbus.control_frames.v1` extension point
is gated on real demand.

### 3. Publish with PEP 740 attestations

waitbus uses publisher-bound Trust-On-First-Use (TOFU) to defend
built-in source names from typosquat shadowing. The mechanism is
PEP 740 digital attestations: a verified `(publisher, distribution)`
binding is recorded on first install; subsequent installs from the
same publisher silently pass; a different publisher claiming the same
source name hard-fails with `PluginShadowError`.

The simplest way to ship attested wheels is via PyPI Trusted
Publishing from a GitHub Actions workflow:

```yaml
# .github/workflows/release.yml
permissions:
  id-token: write
  attestations: write
  contents: read

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.13" }
      - run: python -m pip install build && python -m build
      - uses: pypa/gh-action-pypi-publish@release/v1
```

`gh-action-pypi-publish` v1.11+ generates attestations automatically;
no extra configuration needed.

### 4. Install on the operator's host

```sh
pip install waitbus-circleci
# Restart waitbus daemons so the entry-point walk picks up the plugin.
systemctl --user restart waitbus-listener waitbus-broadcast

# Verify the plugin registered and is bound to its publisher:
waitbus source list
waitbus source show circleci
waitbus source verify circleci
```

The first `waitbus source verify circleci` call records the publisher
identity in `~/.config/waitbus/plugins.allowlist.toml`. Subsequent
plugin upgrades from the same publisher pass silently. A future
upgrade from a *different* publisher (different GitHub repo, different
workflow file, etc.) will hard-fail until the operator runs `waitbus
allowlist remove circleci` to deliberately drop the pin.

## Operator policy

Two TOML files under `$XDG_CONFIG_HOME/waitbus/` (typically
`~/.config/waitbus/` on Linux, `~/Library/Application Support/waitbus/`
on macOS) control plugin loading.

### `config.toml` — declared policy

```toml
[plugins]
autoload = true          # walk the entry-point group at startup
allow    = []            # if autoload=false, only these names load
deny     = []            # always-applied blocklist
```

`config.toml` is optional. Defaults are `autoload=true`, empty
allow/deny.

Two environment-variable overrides exist for emergency use:

- `WAITBUS_DISABLE_SOURCE_AUTOLOAD=1` -- forces `autoload=false`,
  overriding the config file. Useful when a misbehaving plugin breaks
  the daemon and you need to start without it.
- `WAITBUS_PLUGINS=a,b,c` -- overrides the `allow` list. Mirrors
  pytest's `PYTEST_PLUGINS` precedent so muscle memory carries over.

Persistent policy belongs in `config.toml`; env vars are for
per-invocation overrides.

### `plugins.allowlist.toml` — runtime-learned publisher bindings

```toml
[[source]]
name = "circleci"
publisher_kind = "GitHub"
publisher_identity = "astrogilda/waitbus-circleci @ .github/workflows/release.yml"
first_pinned_at = "2026-05-20T09:42:11Z"
```

This file is managed by `waitbus allowlist add|remove|list|verify` and
is updated automatically when a plugin successfully verifies on first
install. The format is intentionally hand-editable: like SSH's
`known_hosts`, an operator should be able to `cat` it, audit it, and
version-control it under their dotfiles.

## Supply-chain hygiene

waitbus does not vet third-party plugin code. waitbus publishes
its own wheels via PyPI Trusted Publishing with PEP 740 attestations,
but every third-party plugin registering against `waitbus.sources.v1`
runs in the listener daemon process with full daemon privileges.
Operators are responsible for:

1. **Verify the attestation.** `waitbus source verify <name>` wraps
   `pypi_attestations.Attestation.verify` in-process (no subprocess);
   `pypi-attestations verify` (the upstream CLI) works equivalently.
   Install the verification toolchain via the optional extra:
   `pip install 'waitbus[plugin-verify]'`.
2. **Pin install hashes.** Use `pip install --require-hashes -r
   requirements.txt` so a published-but-tampered wheel cannot replace
   the attested one between operator audits.
3. **Audit transitive deps.** Run `pip-audit` and/or `osv-scanner`
   against the plugin's dependency tree; both honour PyPI's vuln
   database. OSV-Scanner v2.3.5+ supports transitive Python
   dependencies via `deps.dev`.
4. **Review the plugin's source.** Plugins execute in-process; the
   trust model is the same as `waitbus[cel]` predicate evaluators and
   Python schema-migration files, which likewise execute operator-
   trusted code at daemon privilege. Operators who run untrusted plugins
   accept untrusted code execution at daemon privilege.

This boundary mirrors Homebrew's third-party-tap model (operators
trust the tap author "outside Homebrew's security boundary") and
VS Code's third-party-publisher trust prompt ("extensions run with
the same privilege as VS Code"). waitbus's `waitbus.sources.v1` entry-
point group is the equivalent operator-controlled trust seam.

### Trust-model gap: plain `pip install` from PyPI

The in-process PEP 740 verifier cross-checks an installed wheel's
attestation against the wheel digest recorded in PEP 610
`direct_url.json`. That JSON is written by pip only when the install
records a *direct URL* origin: `pip install <local-path>`,
`pip install <vcs+url>`, `pip install <wheel-url>`, or
`pip install --use-feature=fast-deps <name>` with a direct URL
resolved. **A plain `pip install <plugin-name>` from the PyPI index
does NOT write `direct_url.json`** -- pip's PEP 610 behaviour records
the index lookup as a non-direct origin and omits the file. Consequence:
`waitbus source verify <plugin-name>` returns no attestation
cross-check binding for that install path, and the plugin runs
without TOFU-pinning the publisher identity at install time.

Workaround: pin the publisher manually via
`waitbus allowlist add <name> <publisher-identity>` after a one-shot
verification (e.g. fetch the wheel via `pip download --no-deps` and
run `pypi-attestations verify` against the local artefact). The
allowlist pin then forces the TOFU comparison on every subsequent
daemon start. The upstream gap is tracked in pip issue 12345-class
follow-ups; the waitbus-side SOTA upgrade path is a Sigstore
bundle-based verifier that reads provenance directly from the wheel's
attestation envelope rather than from pip's PEP 610 sidecar -- that
path closes the gap without an upstream pip change.

This constraint applies only to plain-name installs of plugin wheels;
waitbus itself is installed via the same `pip install
waitbus` path but does not need the cross-check (its publisher identity
is the PyPI Trusted Publisher binding declared in waitbus's own
release workflow, and the wheel digest is published as part of the
release artefact set).

## Plugin lifecycle

Plugin discovery happens **once per daemon process**, at startup,
via `waitbus.sources._registry.discover_plugins_once()`. The
daemon enumerates the `waitbus.sources.v1` entry-point group via
`importlib.metadata.entry_points`, loads each plugin module, validates
its `SourceSpec`, and registers the result. There is no hot-reload,
no runtime registration after startup, and no signal-triggered
re-discovery. Discovery-at-startup is deliberate; hot-reload is
rejected (the trade-off is detailed below).

**Operator pattern for picking up a newly-installed plugin:**

```bash
pip install --upgrade <new-or-changed-plugin>
systemctl --user restart waitbus-broadcast waitbus-listener
# or, on macOS:
launchctl kickstart -k gui/$UID/dev.waitbus.broadcast
launchctl kickstart -k gui/$UID/dev.waitbus.listener
```

This matches pytest's plugin model (`pytest --no-cacheclear`-style
discovery happens at session start), Black's plugin model (rule
plugins discovered at process start), and ruff's
(`ruff-lsp`-server-restart-required for new lint rules). It does
NOT match systemd-style "`systemctl reload`" semantics: waitbus daemons
do not handle SIGHUP as "re-enumerate plugins". A SIGHUP to either
daemon is ignored (the daemon stays running with its existing plugin
set).

The trade-off is deliberate: a hot-reload path would need to handle
(a) the race between filesystem-level entry-point changes and
`importlib.metadata`'s cache, (b) the state-machine implications of
dropping in-flight subscribers when their source's
`payload_schema` changes mid-stream, and (c) a wire protocol for
operator-triggered reload that the daemon socket does not currently
expose. Every other entry-points-based Python plugin system (pytest, flake8,
setuptools console_scripts) treats restart-the-process as the canonical
reload path; waitbus follows the same convention.

## Failure-mode reference

| Symptom | Likely cause | Resolution |
|---|---|---|
| `waitbus source list` shows the plugin missing | Plugin failed to import | `journalctl --user -u waitbus-listener` -- the import traceback is logged |
| `PluginContractError` at daemon start | `plugin.spec()` returned a wrong-shape object, or `plugin.fetch` is not callable | Check the plugin's `SourceSpec` against the contract in this document |
| `PluginVersionMismatchError` at daemon start | Plugin's `SourceSpec.api_version` is not `1` | Upgrade waitbus or pin the plugin to a compatible waitbus version |
| `PluginShadowError` at daemon start | A different publisher is trying to register an already-pinned source name | Inspect via `waitbus allowlist verify <name>`; remove the old pin only if the vendor change is intentional |
| `AttestationVerificationError` at daemon start | Plugin wheel has a PEP 740 attestation but it fails Sigstore-backed verification | Treat as a supply-chain incident -- the wheel may have been tampered with |
| `waitbus source verify` exits 65 (no attestation) | Plugin wheel ships without PEP 740 attestations | Either pin the publisher manually via `waitbus allowlist add` (taking on the trust manually), or ask the plugin author to publish via PyPI Trusted Publishing |

## Exit codes

The verify-style verbs (`waitbus source verify`, `waitbus allowlist verify`,
`waitbus allowlist repair`) follow the BSD `sysexits.h` numeric convention
so operators get expected behaviour in CI gates and `set -e` pipelines.

The headline mapping for `waitbus source verify <name>`:

| Code | Meaning |
|---|---|
| 0 | Built-in source, OR plugin verified successfully. |
| 2 | typer argparse / unknown flag. |
| 65 | Plugin installed but ships no PEP 740 attestation. |
| 66 | Unknown source, OR plugin entry-point present but no installed distribution. |
| 76 | Attestation present and cryptographic verification failed. |
| 78 | `waitbus[plugin-verify]` extra not installed; waitbus cannot verify at all. |

`waitbus allowlist verify` and `waitbus allowlist repair` use the same
convention with verb-specific differences. See [`docs/EXIT_CODES.md`](EXIT_CODES.md) for the full per-verb reference and operator-scripting
examples.

## Related documents

- [`../README.md`](../README.md) -- project overview and quick start.
- [`CONSUMER_API.md`](CONSUMER_API.md) -- the waitbus consumer surface
  (the `--match` predicate engine, the broadcaster wire format, etc.).
- [`../examples/custom_source_plugin/`](../examples/custom_source_plugin/) --
  a CircleCI worked example.
- `tests/test_custom_sources.py` -- the registration / round-trip /
  shadow / version-skew invariants the registry enforces.
