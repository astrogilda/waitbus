"""`status` top-level command — operational dashboard."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import typer

from .. import _db, _paths
from .._paths import db_path
from .._types import NS_PER_SECOND


def _status_db(db: Path) -> None:
    """Emit DB-metrics lines: rows_in_db and last_event_age_seconds."""
    import sqlite3
    import time

    if db.exists():
        try:
            with _db.connect(db, readonly=True) as conn:
                row_count: int = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                max_ts_row = conn.execute("SELECT MAX(received_at) FROM events").fetchone()
                max_ts: int | None = max_ts_row[0] if max_ts_row else None
        except sqlite3.Error:
            row_count = -1
            max_ts = None
    else:
        row_count = -1
        max_ts = None

    typer.echo(f"rows_in_db: {row_count if row_count >= 0 else 'db_missing'}")

    if max_ts is None or row_count <= 0:
        typer.echo("last_event_age_seconds: no_events")
    else:
        age = int((time.time_ns() - max_ts) / NS_PER_SECOND)
        typer.echo(f"last_event_age_seconds: {age}")


def _status_liveness(issues: list[str]) -> None:
    """Emit daemon-liveness lines and append issue strings to *issues*."""
    daemons = ("listener", "broadcast", "etag-poll", "watchdog")

    match sys.platform:
        case "linux":
            for daemon in daemons:
                svc = f"waitbus-{daemon}.service"
                result = subprocess.run(
                    ["systemctl", "--user", "is-active", svc],
                    capture_output=True,
                    text=True,
                )
                state = result.stdout.strip() or "unknown"
                typer.echo(f"daemon_{daemon.replace('-', '_')}: {state}")
                if state != "active":
                    issues.append(f"daemon {daemon} not active ({state})")
        case "darwin":
            uid = os.getuid()
            for daemon in daemons:
                label = f"dev.waitbus.{daemon}"
                result = subprocess.run(
                    ["launchctl", "print", f"gui/{uid}/{label}"],
                    capture_output=True,
                    text=True,
                )
                state = "unknown"
                for line in result.stdout.splitlines():
                    if "state =" in line:
                        state = line.split("=", 1)[1].strip()
                        break
                typer.echo(f"daemon_{daemon.replace('-', '_')}: {state}")
                if state != "running":
                    issues.append(f"daemon {daemon} not running ({state})")
        case _:
            for daemon in daemons:
                typer.echo(f"daemon_{daemon.replace('-', '_')}: unsupported_platform_{sys.platform}")


def _status_socket() -> None:
    """Emit broadcast-socket presence line.

    Delegates platform dispatch to _paths.broadcast_socket() rather than
    duplicating the linux/darwin path-construction logic here. The factory
    honors the WAITBUS_RUNTIME_DIR env override, which the status check
    must respect uniformly: a test or operator that points the daemon at
    a custom runtime dir expects this check to look in the same place.
    """
    sock = _paths.broadcast_socket()
    typer.echo(f"broadcast_socket: {'exists' if sock.exists() else 'missing'}")


def status() -> None:
    """Operational dashboard.

    Prints key:value lines reporting DB row count, last-event age,
    per-daemon liveness, and broadcast socket presence. Exit 0 if all
    daemons are healthy; exit 1 if any daemon is dead.
    """
    issues: list[str] = []

    _status_db(db_path())
    _status_liveness(issues)
    _status_socket()

    raise typer.Exit(1 if issues else 0)
