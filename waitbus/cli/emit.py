"""`emit` top-level command — local-source ingress into the event store.

Thin typer wrapper over ``waitbus.emit``. The idempotency
contract, the input-coercion gates (received-at unit normalisation,
``@file``/stdin payload resolution, ``--source`` parsing against the
source registry), the connection lifecycle, and the CloudEvents
projection all live in that module; this file only wires typer args
onto ``emit.cli_entry`` and maps its return value onto the process
exit code (mirroring how ``events query`` delegates to
``events_query.cli_entry``).
"""

from __future__ import annotations

from pathlib import Path

import typer


def emit(
    delivery_id: str = typer.Option(
        ...,
        "--delivery-id",
        help="STABLE, caller-owned idempotency key. It is the events "
        "table PRIMARY KEY: re-emitting the same value is an "
        "INSERT OR IGNORE no-op (reported on stderr, exit 0), not a "
        "duplicate row and not an error. Derive it deterministically "
        "from the natural key of what you observed (e.g. "
        "'pytest:<session>:<nodeid>'). event_id is generated "
        "internally and MUST NOT be supplied.",
    ),
    source: str = typer.Option(
        ...,
        "--source",
        help="Ingest system. One of: github, alertmanager, pytest, "
        "docker, fs (the enum NAME, e.g. GITHUB, is also accepted). "
        "This is the producing system, not the alert vendor.",
    ),
    event_type: str = typer.Option(
        ...,
        "--event-type",
        help="Event type, e.g. workflow_run, workflow_job, prometheus_alert, prometheus_watchdog.",
    ),
    owner: str = typer.Option(..., "--owner", help="Repository owner / org."),
    repo: str = typer.Option(..., "--repo", help="Repository name."),
    received_at: str = typer.Option(
        ...,
        "--received-at",
        help="When the event was observed. Accepts epoch SECONDS "
        "(1763337600), epoch NANOSECONDS, or an RFC3339/ISO-8601 "
        "timestamp (2026-05-17T12:00:00Z; naive => UTC). Normalised "
        "to epoch nanoseconds internally; a seconds/ms-magnitude "
        "value that cannot be a real ns timestamp is rejected.",
    ),
    payload_json: str = typer.Option(
        ...,
        "--payload-json",
        help="The raw event body, stored verbatim. '-' or '@-' reads "
        "stdin; '@<path>' reads that file; otherwise the literal "
        "value is used.",
    ),
    ingest_method: str = typer.Option(
        ...,
        "--ingest-method",
        help="How this event was acquired (free-text label, e.g. 'manual', 'pytest_sessionfinish', 'docker_events').",
    ),
    output_format: str = typer.Option(
        "json",
        "--format",
        help="'json' (default) prints {inserted, event} — the stored "
        "read-shape row plus whether this call wrote it. "
        "'cloudevent' prints the CloudEvents v1.0 envelope of the "
        "stored row (no 'inserted' bit: a CloudEvent is an identity "
        "statement, stable across re-emits of the same delivery-id).",
    ),
    db: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None,
        "--db",
        help="Path to the events SQLite DB. Defaults to the "
        "platformdirs-resolved location (typically "
        "~/.local/state/waitbus/github.db on Linux).",
        exists=False,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
) -> None:
    """Emit one local-source event into the waitbus store (idempotent).

    The public framework-neutral ingress seam. Persists one row via the
    same commit-then-doorbell-ring path the shipped ``etag-poll``
    oneshot uses, then prints the result. Safe to run while the daemons
    are up (WAL + busy_timeout). A missed broadcast ring is a bounded
    one-event delivery DELAY (the daemon's MAX(event_id) sweep recovers
    it), never data loss — the row is durably committed before the ring.
    See ``waitbus.emit`` for the full delivery-id idempotency
    contract.
    """
    import waitbus._emit as mod

    raise typer.Exit(
        mod.cli_entry(
            delivery_id=delivery_id,
            source=source,
            event_type=event_type,
            owner=owner,
            repo=repo,
            received_at=received_at,
            payload_json=payload_json,
            ingest_method=ingest_method,
            output_format=output_format,
            db_path=db,
        )
    )
