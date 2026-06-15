# waitbus CLI exit codes

waitbus aligns verb-style CLI commands with the BSD `sysexits.h` numeric
convention so operators with shell-scripting muscle memory get expected
behaviour in CI gates and `set -e` pipelines. The convention is enumerated
in `waitbus/cli/_exit_codes.py` and documented per-verb here.

## Convention

| Code | sysexits name  | Meaning                                                                                                                                                                       |
|------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0    | `EX_OK`        | Successful completion. Whatever the verb was asked to do, it did, and the result is "good".                                                                                  |
| 2    | `EX_USAGE`     | typer / click argparse error -- the operator typed an unknown flag, omitted a required positional, etc. Reserved so it cannot be confused with a semantic verification result. |
| 65   | `EX_DATAERR`   | The entity exists but is missing the data required for the requested operation. *Plugin is installed, but ships no PEP 740 attestation.*                                       |
| 66   | `EX_NOINPUT`   | The requested entity is absent. *Source name is not registered; plugin is not installed; allowlist has no recorded pin for this name.*                                         |
| 76   | `EX_PROTOCOL`  | Cryptographic verification failed. *Attestation signature invalid; allowlist pin disagrees with live publisher; Sigstore TUF transient error.*                                |
| 78   | `EX_CONFIG`    | Operator's setup blocks the operation. *`waitbus[plugin-verify]` extra is not installed; allowlist file unreadable in a way the operator must fix by hand.*                      |

waitbus uses small integers (`1`, `3`, `130` for SIGINT, etc.) for legacy or
verb-specific codes outside the sysexits.h range; the constants in
`_exit_codes.py` are the canonical reference for verify-shaped verbs going
forward. New verbs adopting verification-style semantics should reuse the
same constants rather than hard-coding small integers.

## Per-verb reference

The blocking verbs (`wait`, `on`) follow the coreutils `timeout` + shell
signal convention (124 = timed out, 128 + signum = signal-killed, 130 =
SIGINT) rather than sysexits.h: they wrap and pass through a CI run's or a
child command's status, so they live in the small-integer space. The
verify-shaped verbs (`source verify`, `allowlist verify` / `repair`) use the
sysexits.h codes from the table above. Interactive / long-running verbs
(`top`, `broadcast serve`) exit `0` on clean shutdown and `130` on SIGINT.

### `waitbus wait`

Block until any source's event matches the predicate.

| Code | Condition |
|------|-----------|
| 0    | Matched. For a GitHub frame the run's `conclusion` is `success`; for a non-GitHub frame any match exits 0. |
| 1    | Matched a GitHub frame whose `conclusion` is terminal `failure` / `cancelled` / `timed_out`. |
| 2    | Startup failure: daemon down, token required, bad `--repo`, malformed `--match`, expressive-evaluator extra not installed, or no predicate supplied. |
| 124  | The overall `--timeout` elapsed with no match (coreutils `timeout` convention). |
| 130  | SIGINT (Ctrl-C) — clean socket/cursor teardown, no spurious match (128 + SIGINT). |

GitHub conclusions `skipped` / `neutral` / `action_required` / `stale` are non-terminal: the wait keeps streaming rather than exiting.

### `waitbus on <predicate> -- <command>`

Block on a predicate (the same engine as `wait`), then run `<command>` on each match. The exit code is the supervised command's, with a few reserved codes for waitbus's own failures.

| Code | Condition |
|------|-----------|
| 0    | The command ran and exited 0. |
| 1–125 | The command exited with this status — passed through unchanged ("the child said so"). |
| 2    | Startup failure (no predicate / bad args), **or** the command itself exited 2 (see the ambiguity note). |
| 124  | The idle `--timeout` elapsed with no new match, **or** the command itself exited 124. |
| 126  | The command was found but could not be executed (not executable / permission denied). |
| 127  | The command was not found on PATH. |
| 128 + signum | The command was killed by a signal (e.g. SIGKILL → 137). |
| 130  | SIGINT (Ctrl-C), after terminating any running command (128 + SIGINT). |

**Ambiguity (documented, as GNU `timeout` documents its own):** codes 1–125 pass through unchanged, so a child that legitimately exits 2, 124, or 130 is indistinguishable from waitbus's own use of those codes. 126/127 always mean the command could not be exec'd; 128+ always means a signal. The contract: **1–125 = the child said so; 126/127 = exec failure; 128+ = signal.**

### `waitbus source verify <name>`

Verify a plugin source's PEP 740 attestation in-process.

| Code | Condition                                                                                                                                                                                                                                                                                                |
|------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0    | Built-in source (no attestation surface to check), OR plugin verified successfully.                                                                                                                                                                                                                       |
| 2    | typer argparse / unknown flag.                                                                                                                                                                                                                                                                            |
| 65   | Plugin is installed, but the wheel ships no PEP 740 attestation.                                                                                                                                                                                                                                          |
| 66   | Unknown source name (run `waitbus source list` to see registered sources), OR plugin entry-point present but has no installed distribution (operator should reinstall the wheel).                                                                                                                            |
| 76   | Attestation present but cryptographic verification failed.                                                                                                                                                                                                                                                |
| 78   | `waitbus[plugin-verify]` optional extra is not installed; waitbus cannot run the in-process verification at all. Install with `pip install 'waitbus[plugin-verify]'`.                                                                                                                                            |

### `waitbus allowlist verify <name>`

Compare the recorded TOFU pin against the plugin's live PEP 740 attestation.

| Code | Condition                                                                                                                                                                                                  |
|------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0    | Live attestation matches the recorded pin.                                                                                                                                                                  |
| 2    | typer argparse / unknown flag.                                                                                                                                                                              |
| 65   | Wheel carries no PEP 740 attestation, so there is nothing to compare against the recorded pin.                                                                                                              |
| 66   | `name` has no recorded pin in the allowlist, OR the plugin is not currently installed (so a live comparison is impossible).                                                                                 |
| 76   | Live attestation mismatches the recorded pin (typosquat / vendor-shadow / publisher rotation), OR the live verification itself failed cryptographically.                                                    |
| 78   | `waitbus[plugin-verify]` optional extra is not installed; waitbus cannot run the live comparison.                                                                                                               |

### `waitbus allowlist repair`

Rewrite a corrupt or partial allowlist file in canonical form.

| Code | Condition                                                                                                                                                                                                                                                                       |
|------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0    | File is valid (or was repaired); printed the canonical form either to stdout (`--dry-run`) or to disk.                                                                                                                                                                          |
| 2    | typer argparse / unknown flag, OR file is so badly malformed that the parser refused. The stderr message names the line and column. Operator action: open the file in an editor and fix the TOML syntax.                                                                       |

## Operator scripting examples

```bash
# CI gate: succeed only on a clean verify; treat missing-attestation
# (65) and unknown-source (66) as alerts but not hard failures.
if ! waitbus source verify circleci; then
    rc=$?
    case "$rc" in
        65) echo "WARN: circleci wheel has no attestation; pre-vet manually" ;;
        66) echo "ERROR: circleci plugin not installed; reinstall" && exit 1 ;;
        76) echo "ALERT: circleci attestation FAILED verification; suspect supply-chain compromise" && exit 1 ;;
        78) echo "ERROR: waitbus[plugin-verify] missing; install the extra" && exit 1 ;;
        *)  echo "ERROR: unexpected exit code $rc" && exit 1 ;;
    esac
fi
```

```bash
# Pre-deploy gate: fail closed if the live attestation disagrees
# with the recorded pin, OR if the live extra is not installed.
waitbus allowlist verify circleci
case $? in
    0)  echo "circleci publisher binding is intact" ;;
    65|78) echo "FAIL: cannot verify (no attestation OR no tooling)" && exit 1 ;;
    66) echo "FAIL: pin missing for circleci" && exit 1 ;;
    76) echo "FAIL: publisher mismatch -- inspect via `waitbus allowlist verify` output" && exit 1 ;;
esac
```

## See also

* `waitbus/cli/_exit_codes.py` -- the canonical integer constants.
* `docs/CUSTOM_SOURCES.md` -- operator workflow for managing the plugin
  source registry; links here for the exit-code reference.
* BSD `sysexits.h` reference:
  <https://man.freebsd.org/cgi/man.cgi?query=sysexits>.
