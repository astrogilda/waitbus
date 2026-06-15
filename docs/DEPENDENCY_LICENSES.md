# Dependency license audit

waitbus is licensed under the MIT License (see the `LICENSE` file). This document
records a license audit of every package waitbus pulls into a consumer install,
confirming the dependency closure is redistribution-compatible with that MIT
license. The machine-readable per-dependency enumeration lives in the
top-level `NOTICE` file; this document records how the audit was run and what
it found.

## Audit metadata

- **Date:** 2026-05-31
- **waitbus version audited:** 0.6.0
- **Resolved closure source:** `uv.lock` (the committed, hash-pinned lockfile)
- **License source of truth:** each package's installed distribution metadata,
  read directly from `*.dist-info/METADATA` (preferring the PEP 639
  `License-Expression` field, then the `License :: OSI Approved` trove
  classifiers, then the legacy `License` field). No license was inferred from
  package name or memory.
- **Closure derivation:** a breadth-first walk of `uv.lock` from the root
  project's `[project.dependencies]` for the base runtime closure, and
  separately from each `[project.optional-dependencies]` extra. The base
  runtime closure (what `pip install waitbus` resolves) is 40 packages; the
  optional extras add the packages listed under each extra below.

## Verdict

All packages in the runtime closure and in every optional extra are
redistribution-compatible with waitbus's MIT license. The closure contains no
strong-copyleft (GPL / AGPL / LGPL) dependency. A keyword scan of every
resolved package's license metadata for `gpl`, `agpl`, `lgpl`, `mpl`,
`mozilla`, `copyleft`, `eclipse`, `cddl`, and `epl` returned exactly one
match: `certifi` (MPL-2.0). See the residual note below.

The license families present are MIT, BSD-2-Clause, BSD-3-Clause, ISC,
Apache-2.0 (including the `Apache-2.0 AND BSD-2-Clause` of prometheus-client
and the dual-offered expressions of cryptography and packaging), the Python
Software Foundation License (typing-extensions, pywin32), and MPL-2.0
(certifi only). All are OSI-approved permissive or weak-copyleft licenses that
permit redistribution under an MIT-licensed superset.

## Residual note: certifi (MPL-2.0)

`certifi` enters the base runtime closure transitively through
`mcp -> httpx / httpcore -> certifi`, and again through the `bench` and
`plugin-verify` extras via `requests`. The Mozilla Public License 2.0 is
**file-level (weak) copyleft**: its share-alike obligation attaches only to
modifications of certifi's own MPL-covered files, not to a larger work that
merely depends on certifi. waitbus does not modify or vendor certifi; it
declares it transitively and installs it unmodified from PyPI. MPL-2.0 is
therefore redistribution-compatible with MIT for waitbus's use. This is recorded
as a known, accepted, benign weak-copyleft dependency rather than a blocker.

## Note on Windows-only entries

The runtime closure includes `colorama` and `pywin32`, both carrying a
`sys_platform == 'win32'` marker in `uv.lock`. They are installed only on
Windows and are absent from a Linux or macOS install. Both are permissive
(BSD-3-Clause and PSF-2.0 respectively).

## Direct runtime dependencies (from `pyproject.toml`)

| Package | Version | License |
|---|---|---|
| mcp | 1.27.1 | MIT |
| msgspec | 0.21.1 | BSD-3-Clause |
| platformdirs | 4.9.6 | MIT |
| prometheus-client | 0.25.0 | Apache-2.0 AND BSD-2-Clause |
| pydantic-settings | 2.14.1 | MIT |
| stamina | 26.1.0 | MIT |
| typer | 0.25.1 | MIT |

## Transitive runtime dependencies

| Package | Version | License |
|---|---|---|
| annotated-doc | 0.0.4 | MIT |
| annotated-types | 0.7.0 | MIT |
| anyio | 4.13.0 | MIT |
| attrs | 26.1.0 | MIT |
| certifi | 2026.4.22 | MPL-2.0 |
| click | 8.3.3 | BSD-3-Clause |
| colorama (Windows only) | 0.4.6 | BSD-3-Clause |
| h11 | 0.16.0 | MIT |
| httpcore | 1.0.9 | BSD-3-Clause |
| httpx | 0.28.1 | BSD-3-Clause |
| httpx-sse | 0.4.3 | MIT |
| idna | 3.15 | BSD-3-Clause |
| jsonschema | 4.26.0 | MIT |
| jsonschema-specifications | 2025.9.1 | MIT |
| markdown-it-py | 4.2.0 | MIT |
| mdurl | 0.1.2 | MIT |
| pydantic | 2.13.4 | MIT |
| pydantic-core | 2.46.4 | MIT |
| pygments | 2.20.0 | BSD-2-Clause |
| pyjwt | 2.12.1 | MIT |
| python-dotenv | 1.2.2 | BSD-3-Clause |
| python-multipart | 0.0.28 | Apache-2.0 |
| pywin32 (Windows only) | 311 | PSF-2.0 |
| referencing | 0.37.0 | MIT |
| rich | 14.3.4 | MIT |
| rpds-py | 0.30.0 | MIT |
| shellingham | 1.5.4 | ISC |
| sse-starlette | 3.4.4 | BSD-3-Clause |
| starlette | 1.0.0 | BSD-3-Clause |
| tenacity | 9.1.4 | Apache-2.0 |
| typing-extensions | 4.15.0 | PSF-2.0 |
| typing-inspection | 0.4.2 | MIT |
| uvicorn | 0.47.0 | BSD-3-Clause |

## Optional-extra dependencies

These install only when the matching extra is requested
(`pip install waitbus[<extra>]`). Versions are the `uv.lock` resolution.
Packages shared by more than one extra are listed once with all the extras
that pull them.

| Extra(s) | Package | Version | License |
|---|---|---|---|
| analyze | duckdb | 1.5.2 | MIT |
| fs | watchdog | 6.0.0 | Apache-2.0 |
| bench, soak | hdrhistogram | 0.10.3 | Apache-2.0 |
| bench, soak | pbr | 7.0.3 | Apache-2.0 |
| bench, soak | setuptools | 82.0.1 | MIT |
| bench | tiktoken | 0.13.0 | MIT |
| bench | regex | 2026.5.9 | Apache-2.0 AND CNRI-Python |
| bench, plugin-verify | requests | 2.34.2 | Apache-2.0 |
| bench, plugin-verify | charset-normalizer | 3.4.7 | MIT |
| plugin-verify | pypi-attestations | 0.0.29 | Apache-2.0 |
| plugin-verify | sigstore | 4.2.0 | Apache-2.0 |
| plugin-verify | sigstore-models | 0.0.6 | Apache-2.0 |
| plugin-verify | sigstore-rekor-types | 0.0.18 | Apache-2.0 |
| plugin-verify | cryptography | 46.0.7 | Apache-2.0 OR BSD-3-Clause |
| plugin-verify | pyopenssl | 26.2.0 | Apache-2.0 |
| plugin-verify | cffi | 2.0.0 | MIT |
| plugin-verify | pycparser | 3.0 | BSD-3-Clause |
| plugin-verify | pyasn1 | 0.6.3 | BSD-2-Clause |
| plugin-verify | securesystemslib | 1.3.1 | MIT |
| plugin-verify | tuf | 6.0.0 | Apache-2.0 OR MIT |
| plugin-verify | id | 1.6.1 | Apache-2.0 |
| plugin-verify | rfc3161-client | 1.0.6 | Apache-2.0 |
| plugin-verify | rfc3986 | 2.0.0 | Apache-2.0 |
| plugin-verify | rfc8785 | 0.1.4 | Apache-2.0 |
| plugin-verify | packaging | 26.2 | Apache-2.0 OR BSD-2-Clause |
| plugin-verify | urllib3 | 2.7.0 | MIT |

## Scope

This audit covers the runtime closure and the optional-extra closures only.
The `dev` and `agent-recipes` PEP 735 dependency-groups are out of scope: they
are never installed by `pip install waitbus`, never ship in the wheel or sdist,
and are tooling for contributors rather than redistributed runtime code.

## Re-running this audit

When `uv.lock` changes, regenerate the closure and re-read licenses:

1. Resolve the runtime and per-extra closures from `uv.lock`
   (breadth-first from `[project.dependencies]` and from each
   `[project.optional-dependencies]` entry).
2. Read each package's license from its installed `*.dist-info/METADATA`
   (`License-Expression`, then trove classifiers, then the `License` field).
3. Scan every license string for strong-copyleft keywords
   (`gpl`, `agpl`, `lgpl`) and flag any hit.
4. Update the tables above and the top-level `NOTICE` file to match.
