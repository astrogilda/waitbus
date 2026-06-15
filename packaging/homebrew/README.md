# waitbus Homebrew formula

**Status:** skeleton. First publish at v0.4.1.

## Tap

Distributed via the tap `astrogilda/homebrew-waitbus` (not homebrew-core
for v0.4.0 — core graduation has star/fork/age criteria out of scope
for the first release).

## Local test

```bash
brew install --build-from-source ./packaging/homebrew/waitbus.rb
waitbus --version
```

## Before publishing

Regenerate the pinned resource stanzas — do not hand-edit hashes:

```bash
brew update-python-resources Formula/waitbus.rb
```

Replace every `REPLACE_ME_*` placeholder (sdist URL/sha256 and the
per-resource hashes).

## Bottle matrix

Six Tier-1 platforms: `arm64_tahoe`, `arm64_sequoia`, `arm64_sonoma`,
Intel `sonoma`, `arm64_linux`, `x86_64_linux`. Intel `x86_64` drops to
Tier 3 in Sep 2026 (Homebrew 5.0.0) — cap the bottle budget
accordingly.

## Notes

- `depends_on "python@3.13"` is versioned deliberately. Never an
  unversioned `python` — Python 3.14 has Pydantic-V1 breakage.
- `livecheck strategy :pypi` because the package is PyPI-published;
  never the GitHub API.
- macOS uses launchd + Keychain for secrets; the systemd-creds path is
  Linux-only (see the formula `caveats`).
