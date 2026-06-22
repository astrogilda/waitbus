# waitbus TODO

Living backlog. Priorities: P0 (drop everything) / P1 (this week) / P2 (next) / P3 (nice to have).

## Known Issues

- [ ] **FLAKE-1 (P2): `test_concurrent_ensure_schema` SQLite "database is locked".**
  File: `tests/test_broadcast.py:1313`. Two threads call `ensure_schema` concurrently and one
  intermittently raises `OperationalError('database is locked')` (seen on py3.14 ubuntu CI under
  load). Fix: set `PRAGMA busy_timeout` (and/or WAL journal mode) on the schema connection, or make
  `ensure_schema` retry on a locked database. It is a real concurrency gap in the schema-init path,
  not just a test problem.

## Release / Ops

- [ ] **REL-1 (P2): add the `RELEASE_PLEASE_TOKEN` PAT secret for the release-please publish handoff.**
  release-please creates the `vX.Y.Z` tag with `GITHUB_TOKEN`, and a tag pushed by `GITHUB_TOKEN`
  does NOT trigger downstream workflows (GitHub anti-recursion), so the tag-triggered `release.yml`
  publish stays dormant. The release-please workflow already falls back to `github.token`, so the
  release PR is still maintained. To enable the automatic tag-to-publish handoff, create a PAT
  (classic `repo`, or fine-grained Contents + Pull requests read/write on this repo) and add it as
  the repo secret `RELEASE_PLEASE_TOKEN`. Until then, push the release tag manually as a stopgap.
