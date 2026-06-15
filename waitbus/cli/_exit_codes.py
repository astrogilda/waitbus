"""Canonical exit codes for waitbus verb-style CLI commands.

waitbus aligns with the BSD ``sysexits.h`` numeric conventions so
operators with shell-scripting muscle memory get expected behaviour
in CI gates and ``set -e`` pipelines. The two reserved low codes
(0 success, 2 typer argparse / usage error) preserve the typer
default behaviour: ``waitbus source verify --bad-flag`` continues to
exit 2 because typer raises ``typer.Exit(2)`` for argparse errors;
semantic verification outcomes use the higher sysexits ranges so
operators can disambiguate "I typed the command wrong" from "the
verification said the wheel is untrusted".

Verbs adopting this convention so far: ``waitbus source verify``,
``waitbus allowlist verify``, ``waitbus allowlist repair``. New
verify-shaped verbs should reuse these constants rather than
hard-coding small integers; see ``docs/EXIT_CODES.md`` for the
operator-facing reference.

References:

* `BSD sysexits.h
  <https://man.freebsd.org/cgi/man.cgi?query=sysexits>`_.
* `Python convention discussion in PEP-3151
  <https://peps.python.org/pep-3151/>`_ (the rationale for typed
  OS error mappings; waitbus does not adopt PEP 3151 but lifts the
  sysexits.h numeric values for the same reason).
"""

from __future__ import annotations

from typing import Final

#: Successful completion.
EX_OK: Final[int] = 0

#: Reserved for typer / click argparse errors (bad flag, missing
#: required positional, unknown option). waitbus verbs do NOT raise
#: ``typer.Exit(2)`` themselves so this code remains an unambiguous
#: signal of "operator typed the command wrong" rather than
#: "verification returned a semantic outcome".
EX_USAGE: Final[int] = 2

#: ``EX_DATAERR`` (65) -- "the input data was incorrect in some way".
#: waitbus uses this when an entity exists but is missing the data
#: required to complete the requested operation (e.g. a plugin
#: wheel that ships no PEP 740 attestation: the plugin is real,
#: the data is incomplete).
EX_DATAERR: Final[int] = 65

#: ``EX_NOINPUT`` (66) -- "an input file (not a system file) did not
#: exist or was not readable". waitbus uses this when the requested
#: entity is absent (e.g. ``allowlist verify`` against a name with
#: no recorded pin, or ``source verify`` against a plugin whose
#: entry-point is unknown / has no installed distribution).
EX_NOINPUT: Final[int] = 66

#: ``EX_PROTOCOL`` (76) -- "the remote system returned something
#: that was 'not possible' during a protocol exchange". waitbus uses
#: this when cryptographic verification fails (a present
#: attestation does not match the published wheel digest, a
#: Sigstore signature is invalid, a TOFU pin and the live publisher
#: disagree). The numeric value matches how openssl / gpg / ssh
#: report similar protocol-failure surfaces.
EX_PROTOCOL: Final[int] = 76

#: ``EX_CONFIG`` (78) -- "something was found in an unconfigured
#: or misconfigured state". waitbus uses this when the operator's
#: own setup blocks the verification path (e.g. the
#: ``waitbus[plugin-verify]`` optional extra is not installed, so
#: waitbus cannot run the in-process attestation check at all).
EX_CONFIG: Final[int] = 78


__all__ = [
    "EX_CONFIG",
    "EX_DATAERR",
    "EX_NOINPUT",
    "EX_OK",
    "EX_PROTOCOL",
    "EX_USAGE",
]
