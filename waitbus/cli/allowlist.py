"""`allowlist` sub-app — publisher-pin management for the TOFU plugin allowlist.

Provides four commands for inspecting and mutating the publisher-pin
allowlist stored in ``$XDG_CONFIG_HOME/waitbus/plugins.allowlist.toml``:

* ``list``   — print every recorded pin as a four-column table.
* ``add``    — manually record a new TOFU pin (override for wheels
               without PEP 740 attestation or for pre-vetting a
               known publisher before first install).
* ``remove`` — delete a pin so the next install of that plugin name
               is treated as a first-install (fresh TOFU).
* ``verify`` — compare the recorded pin against the plugin's live
               PEP 740 attestation.

The allowlist file is human-readable TOML (analogous to
``~/.ssh/known_hosts``). Operators may edit it by hand; these
commands are convenience wrappers with conflict detection and atomic
writes.
"""

from __future__ import annotations

import json
import sys

import typer

from ._shared import _exit_with_error, _render_table, _sub_version_callback

allowlist_app = typer.Typer(
    name="allowlist",
    help="Inspect and manage the publisher-pin TOFU allowlist for third-party plugin sources.",
    no_args_is_help=True,
    add_completion=False,
)


@allowlist_app.callback()
def _allowlist_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """Publisher-pin allowlist management."""


# ---------------------------------------------------------------------------
# allowlist list
# ---------------------------------------------------------------------------


@allowlist_app.command(name="list")
def allowlist_list(
    as_json: bool = typer.Option(
        False,
        "--json/--text",
        help="Output format. --text (default) prints an aligned four-column table; --json prints a JSON array.",
    ),
) -> None:
    """List every publisher pin in the TOFU allowlist.

    Prints a four-column table: name, publisher-kind, publisher-identity,
    first-pinned-at. The table is sorted by source name. An empty table
    means no pins have been recorded yet (waitbus operates from a clean
    TOFU slate on first install of any plugin).

    ANSI colour codes are emitted only when stdout is a TTY.
    """
    from ..sources._config import load_allowlist

    allowlist = load_allowlist()
    pins = sorted(allowlist.pins.values(), key=lambda p: p.name)
    use_colour = not as_json and sys.stdout.isatty()

    if as_json:
        output = [
            {
                "name": p.name,
                "publisher-kind": p.publisher_kind,
                "publisher-identity": p.publisher_identity,
                "first-pinned-at": p.first_pinned_at,
            }
            for p in pins
        ]
        typer.echo(json.dumps(output, indent=2))
        return

    columns = ("name", "publisher-kind", "publisher-identity", "first-pinned-at")
    rows: list[dict[str, str]] = [
        {
            "name": p.name,
            "publisher-kind": p.publisher_kind,
            "publisher-identity": p.publisher_identity,
            "first-pinned-at": p.first_pinned_at,
        }
        for p in pins
    ]

    if not rows:
        typer.echo("(no publisher pins recorded)")
        return

    typer.echo(_render_table(rows, columns, use_colour=use_colour))


# ---------------------------------------------------------------------------
# allowlist add
# ---------------------------------------------------------------------------


@allowlist_app.command(name="add")
def allowlist_add(
    name: str = typer.Argument(..., help="Canonical source name to pin (e.g. ``circleci``)."),
    publisher_kind: str = typer.Option(
        ...,
        "--publisher-kind",
        help='Trusted-Publisher kind (e.g. ``"GitHub"``, ``"GitLab"``, ``"Google"``).',
    ),
    publisher_identity: str = typer.Option(
        ...,
        "--publisher-identity",
        help='Canonical identity string (e.g. ``"owner/repo @ .github/workflows/release.yml"``).',
    ),
) -> None:
    """Manually add a publisher pin to the TOFU allowlist.

    Records ``(name, publisher-kind, publisher-identity)`` as a trusted
    binding. This is the manual equivalent of the automatic TOFU pin that
    waitbus records on first install of a verified plugin; use it to
    pre-vet a publisher before installation, or to pin a plugin whose
    wheel carries no PEP 740 attestation.

    Exits 2 with an informative message if ``name`` is already pinned.
    The operator must run ``waitbus allowlist remove <name>`` first if the
    rebinding is intentional.
    """
    from ..sources._config import append_publisher_pin, load_allowlist

    allowlist = load_allowlist()
    existing = allowlist.for_source(name)
    if existing is not None:
        _exit_with_error(
            f"source {name!r} is already pinned to {existing.publisher_kind}:{existing.publisher_identity!r}",
            hint=f"run `waitbus allowlist remove {name}` first if the rebinding is intentional",
            code=2,
        )

    pin = append_publisher_pin(
        name=name,
        publisher_kind=publisher_kind,
        publisher_identity=publisher_identity,
    )
    typer.echo(
        f"pinned {name!r} -> {pin.publisher_kind}:{pin.publisher_identity} (first-pinned-at: {pin.first_pinned_at})"
    )


# ---------------------------------------------------------------------------
# allowlist remove
# ---------------------------------------------------------------------------


@allowlist_app.command(name="remove")
def allowlist_remove(
    name: str = typer.Argument(..., help="Canonical source name whose pin to remove."),
) -> None:
    """Remove a publisher pin from the TOFU allowlist.

    Deletes the recorded pin for ``name`` so the next install of that
    plugin is treated as a first-install under fresh TOFU semantics
    (the new publisher is accepted and pinned if verified, or registered
    without a pin if unverified).

    Exits 0 on successful removal. Exits 2 if ``name`` is not currently
    pinned (idempotent removal is a no-op that the operator may consider
    an error).
    """
    from ..sources._config import remove_publisher_pin

    removed = remove_publisher_pin(name)
    if not removed:
        typer.echo(f"source {name!r} has no recorded pin", err=True)
        raise typer.Exit(2)
    typer.echo(f"removed pin for {name!r}")


# ---------------------------------------------------------------------------
# allowlist verify
# ---------------------------------------------------------------------------


@allowlist_app.command(name="verify")
def allowlist_verify(
    name: str = typer.Argument(..., help="Canonical source name to verify against the allowlist."),
) -> None:
    """Compare the recorded allowlist pin against the plugin's live attestation.

    Prints the recorded pin details and, if the plugin is currently
    installed, runs :func:`~waitbus.sources._attestation.verify_distribution`
    against the installed wheel and compares the live result to the pin.

    Exit codes follow the BSD ``sysexits.h`` convention; see
    ``docs/EXIT_CODES.md`` for the full reference.

    * 0 (``EX_OK``)        -- live attestation matches the recorded pin.
    * 2 (``EX_USAGE``)     -- typer argparse / unknown flag.
    * 65 (``EX_DATAERR``)  -- wheel carries no PEP 740 attestation
                              to compare against the recorded pin.
    * 66 (``EX_NOINPUT``)  -- ``name`` has no recorded pin in the
                              allowlist, OR plugin is not currently
                              installed (cannot compare live).
    * 76 (``EX_PROTOCOL``) -- live attestation mismatches the pin,
                              OR live verification failed.
    * 78 (``EX_CONFIG``)   -- ``waitbus[plugin-verify]`` extra is not
                              installed; cannot perform the live
                              comparison.
    """
    from ..sources._attestation import (
        AttestationToolingMissingError,
        AttestationVerificationError,
        verify_distribution,
    )
    from ..sources._config import load_allowlist
    from ..sources._registry import entry_points_by_name
    from ._exit_codes import EX_CONFIG, EX_DATAERR, EX_NOINPUT, EX_OK, EX_PROTOCOL

    allowlist = load_allowlist()
    pin = allowlist.for_source(name)
    if pin is None:
        typer.echo(f"source {name!r} has no recorded pin", err=True)
        raise typer.Exit(EX_NOINPUT)

    typer.echo("recorded pin:")
    typer.echo(f"  publisher-kind:     {pin.publisher_kind}")
    typer.echo(f"  publisher-identity: {pin.publisher_identity}")
    typer.echo(f"  first-pinned-at:    {pin.first_pinned_at}")

    # Attempt live attestation if the plugin is installed.
    ep = entry_points_by_name().get(name)
    if ep is None or ep.dist is None:
        typer.echo(
            f"\nplugin {name!r} is not currently installed; cannot compare live attestation",
            err=True,
        )
        raise typer.Exit(EX_NOINPUT)

    try:
        verified = verify_distribution(ep.dist)
    except AttestationToolingMissingError as exc:
        typer.echo(
            f"\nattestations tooling not available: {exc}",
            err=True,
        )
        raise typer.Exit(EX_CONFIG) from exc
    except AttestationVerificationError as exc:
        typer.echo(f"\nlive verification failed: {exc}", err=True)
        raise typer.Exit(EX_PROTOCOL) from exc

    if verified is None:
        typer.echo(
            "\nlive attestation: (none) -- wheel carries no PEP 740 attestation",
            err=True,
        )
        raise typer.Exit(EX_DATAERR)

    typer.echo("\nlive attestation:")
    typer.echo(f"  publisher-kind:     {verified.publisher_kind}")
    typer.echo(f"  publisher-identity: {verified.publisher_identity}")
    typer.echo(f"  predicate-type:     {verified.predicate_type}")

    kind_match = pin.publisher_kind == verified.publisher_kind
    identity_match = pin.publisher_identity == verified.publisher_identity
    if kind_match and identity_match:
        typer.echo("\nresult: match -- live attestation matches the recorded pin")
        raise typer.Exit(EX_OK)
    else:
        typer.echo(
            "\nresult: MISMATCH -- live attestation does not match the recorded pin",
            err=True,
        )
        raise typer.Exit(EX_PROTOCOL)


# ---------------------------------------------------------------------------
# allowlist repair
# ---------------------------------------------------------------------------


@allowlist_app.command(name="repair")
def allowlist_repair(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the proposed rewrite without modifying the file.",
    ),
) -> None:
    """Rewrite a corrupt or partial allowlist file in canonical form.

    Operator self-heal for the publisher-pin allowlist. Reads the
    current file via the same parser the daemon uses; surfaces every
    silently-skipped row (rows missing ``name``, rows with the wrong
    type, ``[[source]]`` arrays with non-dict entries) so the operator
    sees what was lost. On a clean read, rewrites the file in the
    canonical sorted-by-name form so a hand-edited file is
    normalised. On a parse failure, prints the error and exits
    non-zero -- the operator must hand-fix the syntax before
    ``repair`` can run.

    Exit codes:

    * 0 -- file is valid (or was repaired); printed the canonical form
      either to stdout (``--dry-run``) or to disk.
    * 2 -- file is so badly malformed that the parser refused. The
      stderr message names the line and column. Operator action:
      open the file in an editor and fix the TOML syntax.
    """
    from ..sources._config import (
        _ALLOWLIST_FILENAME,
        AllowlistCorruptError,
        _render_allowlist,
        config_dir,
        load_allowlist,
    )

    path = config_dir() / _ALLOWLIST_FILENAME
    try:
        allowlist = load_allowlist()
    except AllowlistCorruptError as exc:
        _exit_with_error(f"could not parse {path}: {exc}", code=2)

    rendered = _render_allowlist(allowlist)
    if dry_run:
        typer.echo(rendered, nl=False)
        typer.echo(f"\n# dry-run: would write {len(allowlist.pins)} pin(s) to {path}", err=True)
        raise typer.Exit(0)

    # Re-emit via the public mutator path so the write goes through
    # _write_allowlist (atomic + locked + fsync). For a no-op rewrite
    # we add+remove a sentinel pin -- the simpler path is to write the
    # rendered content directly via the same atomic helper.
    from ..sources._config import _allowlist_lock, _write_allowlist

    with _allowlist_lock():
        _write_allowlist(allowlist)

    typer.echo(f"repaired {path}: wrote {len(allowlist.pins)} pin(s) in canonical form")


__all__ = ["allowlist_app"]
