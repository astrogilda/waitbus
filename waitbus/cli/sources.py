"""`source` sub-app — source registry inspection and long-lived local-source watchers.

Provides three registry-inspection commands (``list``, ``show``,
``verify``) and two long-running watcher commands (``docker``, ``fs``).

Registry-inspection commands read the in-process source registry built
by :mod:`waitbus.sources._registry`, cross-reference the
persisted publisher allowlist from
:mod:`waitbus.sources._config`, and (for ``verify``) run the
PEP 740 attestation check from
:mod:`waitbus.sources._attestation`. They are read-only and safe
to run at any time without a running daemon.

The pytest emitter is intentionally **not** a verb here: it is an
explicit-opt-in pytest *plugin* (``pytest -p
waitbus.sources.pytest_emit --waitbus-emit``), not a standalone
process — a CLI verb would be the wrong invocation surface for
something that must run inside the pytest session it observes. Its
invocation is documented in ``waitbus.sources.pytest_emit``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import typer

from ._shared import _exit_with_error, _sub_version_callback

sources_app = typer.Typer(
    name="source",
    help="Local event-source watchers (docker container exits, "
    "filesystem writes) that emit into the waitbus event store.",
    no_args_is_help=True,
    add_completion=False,
)


@sources_app.callback()
def _sources_root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_sub_version_callback,
        is_eager=True,
        help="Print the waitbus version and exit.",
    ),
) -> None:
    """Local-source watcher sub-commands."""


@sources_app.command(name="docker")
def source_docker(  # pragma: no cover
    socket_path: str = typer.Option(
        "/var/run/docker.sock",
        "--socket",
        help="Docker Engine socket. Must be readable by this process "
        "(root:docker 0660 on a stock install — run as root or join "
        "the 'docker' group).",
    ),
    owner: str = typer.Option("local", "--owner", help="owner label."),
    repo: str = typer.Option("docker", "--repo", help="repo label."),
    db: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None, "--db", help="events DB path (default: platformdirs)."
    ),
) -> None:
    # pragma covers the whole body: this verb is a typer entry-point dispatch
    # shell over docker_watch.watch, which has its own test suite in
    # tests/test_sources.py; the verb body is pure wiring with no testable
    # logic (mock-patch tests would only assert on call-mock interactions).
    """Stream Docker container-exit events; emit one per die/stop/kill.

    Blocks until SIGINT. Reconnects with an advanced ``since`` cursor
    across transport drops so the disconnect window is replayed
    (idempotent). Exits 130 on SIGINT, 0 on clean EOF.
    """
    from ..sources import docker_watch as mod

    try:
        code = mod.watch(socket_path=socket_path, owner=owner, repo=repo, db_path=db)
    except mod.DockerSocketError as exc:
        typer.echo(f"waitbus source docker: {exc}", err=True)
        raise typer.Exit(2) from exc
    raise typer.Exit(code)


@sources_app.command(name="fs")
def source_fs(  # pragma: no cover
    path: Path = typer.Argument(  # noqa: B008  (typer idiom)
        ..., help="Directory tree to watch."
    ),
    recursive: bool = typer.Option(True, "--recursive/--no-recursive", help="Recurse into subdirs."),
    owner: str = typer.Option("local", "--owner", help="owner label."),
    repo: str = typer.Option("fs", "--repo", help="repo label."),
    db: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None, "--db", help="events DB path (default: platformdirs)."
    ),
) -> None:
    # pragma covers the whole body: this verb is a typer entry-point dispatch
    # shell over fs_watch.watch, which has its own test suite in
    # tests/test_sources.py; the verb body is pure wiring with no testable
    # logic (mock-patch tests would only assert on call-mock interactions).
    """Watch a directory and emit one event per completed file write.

    Reacts only to close-write / moved-in (atomic-save) signals, so
    editor temp-file churn is ignored. Requires the optional ``[fs]``
    extra (``pip install 'waitbus[fs]'``); a clear error is printed if it
    is missing. Blocks until SIGINT (exit 130).
    """
    from ..sources import fs_watch as mod

    try:
        code = mod.watch(path, recursive=recursive, owner=owner, repo=repo, db_path=db)
    except mod.FsWatchDependencyError as exc:
        typer.echo(f"waitbus source fs: {exc}", err=True)
        raise typer.Exit(2) from exc
    except FileNotFoundError as exc:
        typer.echo(f"waitbus source fs: {exc}", err=True)
        raise typer.Exit(2) from exc
    raise typer.Exit(code)


# ---------------------------------------------------------------------------
# Registry inspection helpers
# ---------------------------------------------------------------------------


def _source_row(
    name: str,
    *,
    use_colour: bool,
) -> dict[str, Any]:
    """Build the ten-column record for one source entry.

    The ``last-used`` column is a placeholder for a future per-source
    last-used log backed by the waitbus SQLite store. The column is
    included now so downstream tooling (shell scripts, dashboards) can
    rely on a stable ten-column shape; the value will change from
    ``"n/a"`` to an ISO 8601 timestamp when the backing store lands.
    """
    from ..sources._config import load_allowlist
    from ..sources._registry import (
        _BUILTIN_SOURCES,
        entry_points_by_name,
        known_sources,
        plugin_publishers,
    )

    is_builtin = name in _BUILTIN_SOURCES

    spec = known_sources()[name]
    api_version = str(spec.api_version)
    event_types_str = ",".join(spec.event_types)
    payload_schema: str
    if spec.payload_schema is None:
        payload_schema = "None"
    else:
        payload_schema = f"{spec.payload_schema.__module__}.{spec.payload_schema.__qualname__}"

    if is_builtin:
        kind = "builtin"
        registered_by = "waitbus"
        loaded_from = "waitbus.sources"
        sig_status = "none"
        pub_identity = "n/a"
        allowlist_status = "n/a"
    else:
        kind = "plugin"
        # Find the entry point so we can read its dist and module path.
        ep = entry_points_by_name().get(name)
        if ep is not None:
            dist = ep.dist
            if dist is not None:
                dist_name = dist.name
                dist_version = dist.metadata["Version"] or "unknown"
                registered_by = f"{dist_name} {dist_version}"
            else:
                registered_by = "unknown"
            loaded_from = ep.value
        else:
            registered_by = "unknown"
            loaded_from = "unknown"

        publishers = plugin_publishers()
        verified = publishers.get(name)
        if verified is not None:
            sig_status = "verified"
            pub_identity = verified.publisher_identity
        else:
            sig_status = "unverified"
            pub_identity = "n/a"

        allowlist = load_allowlist()
        pin = allowlist.for_source(name)
        allowlist_status = f"tofu-pinned-{pin.publisher_kind}" if pin is not None else "unknown"

    return {
        "name": name,
        "kind": kind,
        "api-version": api_version,
        "event-types": event_types_str,
        "registered-by": registered_by,
        "loaded-from": loaded_from,
        "signature-status": sig_status,
        "publisher-identity": pub_identity,
        "allowlist-status": allowlist_status,
        "last-used": "n/a",
        # extra fields for `show` only
        "_payload_schema": payload_schema,
    }


_SOURCE_LIST_COLUMNS: tuple[str, ...] = (
    "name",
    "kind",
    "api-version",
    "event-types",
    "registered-by",
    "loaded-from",
    "signature-status",
    "publisher-identity",
    "allowlist-status",
    "last-used",
)


def _format_table(rows: list[dict[str, Any]], *, use_colour: bool) -> str:
    """Render ``rows`` as the ten-column ``source list`` table.

    Thin wrapper over :func:`waitbus.cli._shared._render_table`:
    supplies the ``source list``-specific column order and the
    plugin-rows-coloured-CYAN row-style policy. ``allowlist list`` uses
    the same underlying renderer with different columns and no row
    colouring.
    """
    from ._shared import _render_table

    return _render_table(
        rows,
        _SOURCE_LIST_COLUMNS,
        use_colour=use_colour,
        row_style_fn=lambda r: typer.colors.CYAN if r.get("kind") == "plugin" else None,
    )


# ---------------------------------------------------------------------------
# source list
# ---------------------------------------------------------------------------


@sources_app.command(name="list")
def source_list(
    as_json: bool = typer.Option(
        False,
        "--json/--text",
        help="Output format. --text (default) prints an aligned "
        "ten-column table; --json prints a JSON array with one "
        "object per source.",
    ),
) -> None:
    """List every registered source (built-ins and plugins) as a ten-column table.

    Columns: name, kind, api-version, event-types, registered-by,
    loaded-from, signature-status, publisher-identity, allowlist-status,
    last-used.

    The ``last-used`` column is always ``n/a`` in this release. A
    per-source last-used log backed by the waitbus SQLite store is planned
    for a future release; the column is present now so scripts can rely
    on a stable ten-column shape.

    ANSI colour codes are emitted only when stdout is a TTY.
    """
    from ..sources._registry import known_sources
    from ._shared import _ensure_plugin_discovery_for_cli

    _ensure_plugin_discovery_for_cli()
    sources = known_sources()
    use_colour = not as_json and sys.stdout.isatty()

    rows = [_source_row(name, use_colour=use_colour) for name in sorted(sources)]
    if as_json:
        output = [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows]
        typer.echo(json.dumps(output, indent=2))
    else:
        typer.echo(_format_table(rows, use_colour=use_colour))


# ---------------------------------------------------------------------------
# source show
# ---------------------------------------------------------------------------


@sources_app.command(name="show")
def source_show(
    name: str = typer.Argument(..., help="Canonical source name to inspect (e.g. ``github``, ``pytest``)."),
    as_json: bool = typer.Option(
        False,
        "--json/--text",
        help="Output format. --text (default) prints labelled key:value pairs; --json prints one JSON object.",
    ),
) -> None:
    """Show a detailed record for one registered source.

    Includes all ten columns from ``source list`` plus the full PEP 740
    attestation JSON (read from disk if present), the payload schema
    type name, and the TOFU-pin first-seen timestamp.

    The attestation JSON is read from disk and printed verbatim; it is
    NOT re-verified by this command. Use ``waitbus source verify <name>``
    to run the live cryptographic check.
    """
    from ..sources._attestation import read_attestation_json
    from ..sources._config import load_allowlist
    from ..sources._registry import entry_points_by_name, known_sources
    from ._shared import _ensure_plugin_discovery_for_cli

    _ensure_plugin_discovery_for_cli()
    sources = known_sources()
    if name not in sources:
        _exit_with_error(
            f"unknown source {name!r}",
            hint="run `waitbus source list` to see registered sources",
        )

    use_colour = not as_json and sys.stdout.isatty()
    row = _source_row(name, use_colour=use_colour)

    payload_schema_str = row.pop("_payload_schema", "None")

    allowlist = load_allowlist()
    pin = allowlist.for_source(name)
    first_pinned_at = pin.first_pinned_at if pin is not None else "n/a"

    attestation_json: str | None = None
    if row["kind"] == "plugin":
        ep = entry_points_by_name().get(name)
        if ep is not None and ep.dist is not None:
            attestation_json = read_attestation_json(ep.dist)

    if as_json:
        output: dict[str, Any] = {k: v for k, v in row.items() if not k.startswith("_")}
        output["payload-schema"] = payload_schema_str
        output["first-pinned-at"] = first_pinned_at
        output["attestation-json"] = attestation_json
        typer.echo(json.dumps(output, indent=2))
    else:
        fields = [
            ("name", row["name"]),
            ("kind", row["kind"]),
            ("api-version", row["api-version"]),
            ("event-types", row["event-types"]),
            ("registered-by", row["registered-by"]),
            ("loaded-from", row["loaded-from"]),
            ("signature-status", row["signature-status"]),
            ("publisher-identity", row["publisher-identity"]),
            ("allowlist-status", row["allowlist-status"]),
            ("last-used", row["last-used"]),
            ("payload-schema", payload_schema_str),
            ("first-pinned-at", first_pinned_at),
        ]
        w = max(len(k) for k, _ in fields)
        for key, value in fields:
            typer.echo(f"  {key:<{w}}  {value}")
        if attestation_json is not None:
            typer.echo("\n  attestation-json:")
            typer.echo(attestation_json)
        else:
            typer.echo("\n  attestation-json: (none)")


# ---------------------------------------------------------------------------
# source verify
# ---------------------------------------------------------------------------


@sources_app.command(name="verify")
def source_verify(
    name: str = typer.Argument(..., help="Canonical source name to verify (e.g. ``circleci``)."),
) -> None:
    """Verify the PEP 740 attestation for a plugin source's installed wheel.

    Built-in sources (github, pytest, docker, fs, alertmanager) ship
    inside the waitbus wheel itself and have no separate attestation
    surface; this command prints an informational message and exits 0
    for them.

    Exit codes follow the BSD ``sysexits.h`` convention so operators
    can disambiguate verification outcomes from typer argparse errors
    (which keep exit 2). See ``docs/EXIT_CODES.md`` for the full
    reference.

    * 0 (``EX_OK``)        -- built-in source, or plugin verified.
    * 2 (``EX_USAGE``)     -- typer argparse / unknown flag.
    * 65 (``EX_DATAERR``)  -- plugin installed, but no PEP 740
                              attestation found alongside the wheel.
    * 66 (``EX_NOINPUT``)  -- unknown source name, or plugin
                              entry-point present but has no
                              installed distribution.
    * 76 (``EX_PROTOCOL``) -- attestation present and cryptographic
                              verification failed.
    * 78 (``EX_CONFIG``)   -- ``waitbus[plugin-verify]`` optional
                              extra is not installed; waitbus cannot
                              run the in-process verification at all.
    """
    from ..sources._registry import _BUILTIN_SOURCES, known_sources
    from ._exit_codes import EX_CONFIG, EX_DATAERR, EX_NOINPUT, EX_OK, EX_PROTOCOL
    from ._shared import _ensure_plugin_discovery_for_cli

    _ensure_plugin_discovery_for_cli()
    sources = known_sources()
    if name not in sources:
        _exit_with_error(
            f"unknown source {name!r}",
            hint="run `waitbus source list` to see registered sources",
            code=EX_NOINPUT,
        )

    if name in _BUILTIN_SOURCES:
        typer.echo("n/a -- built-in source has no separate attestation surface")
        raise typer.Exit(EX_OK)

    # Plugin: locate the distribution and run the live check.
    from ..sources._attestation import (
        AttestationToolingMissingError,
        AttestationVerificationError,
        verify_distribution,
    )
    from ..sources._registry import entry_points_by_name

    ep = entry_points_by_name().get(name)
    if ep is None or ep.dist is None:
        _exit_with_error(
            f"plugin {name!r} has no installed distribution; cannot verify",
            hint="reinstall the plugin wheel and retry",
            code=EX_NOINPUT,
        )

    try:
        verified = verify_distribution(ep.dist)
    except AttestationToolingMissingError as exc:
        _exit_with_error(
            str(exc),
            hint="install the optional extra: pip install 'waitbus[plugin-verify]'",
            code=EX_CONFIG,
        )
    except AttestationVerificationError as exc:
        typer.echo(f"attestation verification failed: {exc}", err=True)
        raise typer.Exit(EX_PROTOCOL) from exc

    if verified is None:
        typer.echo("no attestation found", err=True)
        raise typer.Exit(EX_DATAERR)

    typer.echo(f"verified publisher: {verified.publisher_kind}:{verified.publisher_identity}")
    typer.echo(f"predicate-type:     {verified.predicate_type}")
    raise typer.Exit(EX_OK)
