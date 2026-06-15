#!/usr/bin/env python3
"""Reciprocal heartbeat absence detector for the waitbus listener.

Runs as a 5-minute systemd-user timer on the operator workstation. Queries
the SQLite event store for the most recent prometheus_watchdog row sent
by a paired upstream alertmanager (whichever project the operator wired
to the /watchdog endpoint). If the gap exceeds the configured threshold,
posts a notify-send and touches a STALE flag file the Bash PS1 reads.

Bootstrap discipline: the check tracks a "first-watchdog-seen" state
file under the resolved state dir (platformdirs-based default, see
waitbus/_paths.py; overridable via the `--state-dir` CLI flag or the
`WAITBUS_STATE_DIR` env var).
Until at least one prometheus_watchdog row lands, the absence detector
stays silent — a fresh install with the listener up but no upstream
heartbeat yet is not "stale," just not yet wired. Once seen, the file
is created and never deleted, so subsequent startup periods that
observe staleness still alert.

Exit codes mirror the typical health-probe convention so a wrapper
script or another monitor can interpret them:

  0   fresh — last watchdog within threshold (or pre-bootstrap, no first-seen)
  1   stale — last watchdog older than threshold
  2   error — DB unreachable, schema mismatch, etc.

Stdlib-only.
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Final

from . import _config, _db
from ._paths import db_path, ensure_state_dirs, state_dir
from ._sdnotify import sd_notify
from ._types import NS_PER_SECOND

_NOTIFY_SEND_TIMEOUT_SEC: Final[int] = 5
"""Timeout in seconds for the ``notify-send`` subprocess call.

``notify-send`` dispatches a desktop notification via D-Bus; it normally
returns in milliseconds. The timeout guards against a hung D-Bus session
(e.g. the desktop environment has exited) that would otherwise block the
systemd-timer-driven one-shot watchdog indefinitely.
"""

# The watchdog absence-detector keeps two flag files (`watchdog_seen`,
# `watchdog_stale`) under this directory. The default is the events
# state dir resolved by `_paths` (platformdirs + `WAITBUS_STATE_DIR`
# env override). Operators override on a per-invocation basis with the
# `--state-dir` CLI flag.
DEFAULT_THRESHOLD_S = 600  # 10 min — matches the Watchdog alert group_interval

WATCHDOG_EVENT_TYPE = "prometheus_watchdog"
WATCHDOG_ALERT_NAME = "Watchdog"
SEEN_FLAG_FILENAME = "watchdog_seen"
STALE_FLAG_FILENAME = "watchdog_stale"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Build the argparse Namespace for the watchdog-check entry point.

    Split out from ``main`` so tests can build a Namespace with the
    same defaults without spawning a subprocess.
    """
    p = argparse.ArgumentParser(
        prog="watchdog_check",
        description="Detect missing prometheus_watchdog heartbeats from the upstream alertmanager.",
    )
    p.add_argument("--db", type=Path, default=db_path(), help="waitbus SQLite path")
    p.add_argument(
        "--state-dir",
        type=Path,
        default=state_dir(),
        help="directory for seen/stale flag files (XDG_STATE_HOME convention)",
    )
    p.add_argument(
        "--threshold",
        type=int,
        default=DEFAULT_THRESHOLD_S,
        help="seconds since last watchdog before declaring stale",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="suppress notify-send (file flag still toggled)",
    )
    return p.parse_args(argv)


def latest_watchdog_ts(db_path: Path) -> int | None:
    """Return epoch seconds of the most recent watchdog event, or None if none recorded.

    Converts from the stored epoch nanoseconds to seconds for use in
    threshold comparisons against time.time(). A non-zero return means: at
    some point an upstream alertmanager successfully delivered a Watchdog
    alert through the listener path.
    """
    if not db_path.exists():
        return None
    # contextlib.closing because sqlite3.Connection.__exit__ commits but does
    # NOT close — leaving the connection for GC produces ResourceWarnings.
    with _db.connect(db_path, readonly=True) as conn:
        cur = conn.execute(
            "SELECT MAX(received_at) FROM events WHERE event_type = ? AND alert_name = ?",
            (WATCHDOG_EVENT_TYPE, WATCHDOG_ALERT_NAME),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    raw = int(row[0])
    # received_at is epoch nanoseconds; convert to seconds for threshold math.
    # Values above 1e15 are ns-magnitude; divide by 1e9. Legacy rows (seconds
    # or ms magnitude) that predate this schema version are passed through
    # unchanged so operators are not silently misled during migration.
    if raw >= 1_000_000_000_000_000:
        return raw // NS_PER_SECOND
    return raw


def notify_send(message: str, urgency: str = "critical") -> None:
    """Best-effort desktop notification. Silent if notify-send is missing."""
    if shutil.which("notify-send") is None:
        return
    try:
        subprocess.run(
            ["notify-send", "--urgency", urgency, "waitbus watchdog", message],
            check=False,
            capture_output=True,
            timeout=_NOTIFY_SEND_TIMEOUT_SEC,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``waitbus-watchdog-check`` console-script.

    Single-shot: compares the most recent prometheus_watchdog row's
    timestamp against ``--threshold-min`` and toggles a flag file
    (seen/stale) in the state directory. Designed for systemd-timer
    invocation; the flag-file model is intentional so a prompt
    indicator can stat() the flag without re-querying SQLite on
    every shell prompt.
    """
    args = parse_args(argv)
    _config.get_config()  # validate config at startup; loud-fail on bad values
    sd_notify(b"READY=1\nSTATUS=checking watchdog\n")
    result = _check_once(args)
    sd_notify(b"STOPPING=1\n")
    return result


def _check_once(args: argparse.Namespace) -> int:
    """Single watchdog-check cycle.

    Compares the most recent prometheus_watchdog row's timestamp against
    the threshold and toggles flag files (seen/stale) in the state directory.
    The verdict (fresh / stale / pre_bootstrap / db_error) is logged to
    stdout/stderr.
    """
    ensure_state_dirs()
    args.state_dir.mkdir(parents=True, exist_ok=True)

    seen_flag = args.state_dir / SEEN_FLAG_FILENAME
    stale_flag = args.state_dir / STALE_FLAG_FILENAME

    try:
        latest = latest_watchdog_ts(args.db)
    except sqlite3.Error as exc:
        sys.stderr.write(f"watchdog_check: db error: {exc}\n")
        return 2

    now = int(time.time())

    # Bootstrap: have we ever observed a watchdog row?
    if latest is not None and not seen_flag.exists():
        seen_flag.touch()

    # Pre-bootstrap: never seen any watchdog yet. Do not alert; the
    # operator may still be provisioning the upstream alertmanager. Once
    # the first watchdog lands, the absence-detection arm engages on
    # subsequent runs. If `latest is None` but `seen_flag.exists()`, the
    # database emptied after we had seen events historically (purge or
    # corruption); fall through and alert on that transition.
    if latest is None and not seen_flag.exists():
        stale_flag.unlink(missing_ok=True)
        print("watchdog_check: pre-bootstrap (no watchdog seen yet); silent.")
        return 0

    age_s = (now - latest) if latest is not None else None
    fresh = age_s is not None and age_s <= args.threshold

    if fresh:
        if stale_flag.exists():
            stale_flag.unlink()
        print(f"watchdog_check: fresh (age={age_s}s, threshold={args.threshold}s).")
        return 0

    # Stale. Idempotent: notify only on transition (when stale_flag did not
    # already exist), update the flag mtime each cycle so PS1 readers can
    # show "stale since <ts>" if they want.
    transitioning = not stale_flag.exists()
    stale_flag.touch()
    msg = (
        f"prometheus_watchdog stale — no Watchdog alert in {age_s}s (threshold {args.threshold}s)."
        if age_s is not None
        else "prometheus_watchdog gone — historically seen but DB now empty."
    )
    if transitioning and not args.quiet:
        notify_send(msg, urgency="critical")
    sys.stderr.write(f"watchdog_check: STALE — {msg}\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
