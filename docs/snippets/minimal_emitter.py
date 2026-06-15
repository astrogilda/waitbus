"""Minimal waitbus emitter (Python, stdlib only).

Puts one event on the bus without the waitbus package installed: a
named-column ``INSERT OR IGNORE`` into the ``events`` table of the
SQLite store, a commit, then a best-effort one-byte ring on the
broadcast daemon's AF_UNIX doorbell socket. Any language with SQLite
bindings and a unix-socket API can do the same.

Emit path
---------
- The store is a SQLite database in WAL mode. Default path is
  ``$XDG_STATE_HOME/waitbus/github.db`` (Linux) or the macOS
  Application Support equivalent. The WAITBUS_DB env var overrides.
- The insert is ``INSERT OR IGNORE`` keyed on the caller-owned
  ``delivery_id`` (a ``NOT NULL UNIQUE`` column): re-emitting the same
  ``delivery_id`` is an idempotent no-op, never a duplicate row.
- ``event_id`` is a caller-generated 26-character Crockford ULID;
  ``received_at`` is epoch *nanoseconds*. The internal ``seq`` ordering
  column is daemon/SQLite-assigned and must never be supplied.
- After the commit, ring the doorbell: connect AF_UNIX SOCK_STREAM to
  ``$XDG_RUNTIME_DIR/waitbus/doorbell.sock`` (override:
  WAITBUS_DOORBELL_SOCKET) and send one byte. The ring is best-effort:
  a missed ring is a bounded delivery delay, never data loss, because
  the daemon sweeps committed rows on its next wake.
- This emitter never creates or migrates schema. Schema ownership stays
  with the in-tree daemons; a missing ``events`` table exits 2 with a
  pointer to start them.

This snippet is pinned by ``tests/test_emitter_snippet.py``, which runs
it end-to-end against a live daemon at the same commit that ships any
store/doorbell change -- the write-side counterpart of the multilingual
subscriber lockstep tests.

Usage
-----
::

    python docs/snippets/minimal_emitter.py "build finished"
    # or against a non-default store / doorbell:
    WAITBUS_DB=/tmp/bus/github.db \\
    WAITBUS_DOORBELL_SOCKET=/tmp/bus/doorbell.sock \\
        python docs/snippets/minimal_emitter.py "build finished"
"""

from __future__ import annotations

import contextlib
import json
import os
import secrets
import socket
import sqlite3
import sys
import time
from pathlib import Path

# Crockford base32 alphabet (no I, L, O, U) -- the ULID spec's encoding.
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

_EVENT_COLUMNS = (
    "delivery_id",
    "source",
    "event_type",
    "owner",
    "repo",
    "received_at",
    "payload_json",
    "ingest_method",
    "msg_from",
    "msg_body",
    "event_id",
)


def _default_db_path() -> Path:
    """Return the events-store path: WAITBUS_DB, else the platform default.

    Mirrors waitbus's own resolution: ``$WAITBUS_STATE_DIR/github.db``
    when that env var is set, else ``$XDG_STATE_HOME/waitbus/github.db``
    (Linux, default ``~/.local/state``) or
    ``~/Library/Application Support/waitbus/github.db`` (macOS).
    """
    override = os.environ.get("WAITBUS_DB")
    if override:
        return Path(override)
    state_dir = os.environ.get("WAITBUS_STATE_DIR")
    if state_dir:
        return Path(state_dir) / "github.db"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "waitbus" / "github.db"
    xdg_state = os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(xdg_state) / "waitbus" / "github.db"


def _default_doorbell_path() -> Path:
    """Return the doorbell-socket path: env override, else the XDG default.

    Honours WAITBUS_DOORBELL_SOCKET, then ``$WAITBUS_RUNTIME_DIR/doorbell.sock``,
    then ``$XDG_RUNTIME_DIR/waitbus/doorbell.sock`` (Linux; macOS uses the
    tempdir-based runtime dir waitbus itself resolves).
    """
    override = os.environ.get("WAITBUS_DOORBELL_SOCKET")
    if override:
        return Path(override)
    runtime_dir = os.environ.get("WAITBUS_RUNTIME_DIR")
    if runtime_dir:
        return Path(runtime_dir) / "doorbell.sock"
    runtime = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    return Path(runtime) / "waitbus" / "doorbell.sock"


def _new_ulid() -> str:
    """Return a 26-character Crockford ULID (48-bit ms timestamp + 80-bit random).

    One-shot generation: no within-millisecond monotonicity guarantee
    (the in-tree generator has one; an external emitter does not need it
    because broadcast ordering is authoritative on the internal ``seq``
    column -- ``event_id`` only needs uniqueness and cursor validity).
    """
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    value = (ms << 80) | int.from_bytes(secrets.token_bytes(10), "big")
    chars = []
    for shift in range(125, -1, -5):
        chars.append(_CROCKFORD[(value >> shift) & 0x1F])
    return "".join(chars)


def _ring_doorbell(path: Path) -> None:
    """Best-effort one-byte wake to the broadcast daemon. Never raises."""
    with contextlib.suppress(OSError), socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        # Bound the ring so a wedged daemon cannot hang the emitter; a
        # timeout surfaces as OSError and is suppressed like any other miss.
        s.settimeout(1.0)
        s.connect(str(path))
        s.sendall(b".")


def main(argv: list[str]) -> int:
    """Emit one ``agent``-source event whose body is ``argv[1]``."""
    if len(argv) != 2:
        print("usage: minimal_emitter.py <message body>", file=sys.stderr)
        return 2
    body = argv[1]
    db_path = _default_db_path()
    delivery_id = f"minimal-emitter:{socket.gethostname()}:{os.getpid()}:{time.time_ns()}"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        table = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'").fetchone()
        if table is None:
            print(
                f"error: no events table in {db_path}; start the waitbus daemons first -- "
                "this emitter never creates schema.",
                file=sys.stderr,
            )
            return 2
        columns = ", ".join(_EVENT_COLUMNS)
        placeholders = ", ".join(["?"] * len(_EVENT_COLUMNS))
        cursor = conn.execute(
            f"INSERT OR IGNORE INTO events ({columns}) VALUES ({placeholders})",
            (
                delivery_id,
                "agent",
                "agent_message",
                "local",
                "minimal-emitter",
                time.time_ns(),
                json.dumps({"body": body}),
                "minimal_emitter",
                "minimal-emitter",
                body,
                _new_ulid(),
            ),
        )
        inserted = cursor.rowcount == 1
        conn.commit()
    finally:
        conn.close()
    # Commit BEFORE the ring so the daemon's sweep sees the row.
    _ring_doorbell(_default_doorbell_path())
    print(f"{delivery_id}\tinserted={inserted}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
