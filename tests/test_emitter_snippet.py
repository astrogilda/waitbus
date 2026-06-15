"""End-to-end tests for the dependency-free emitter snippet.

Runs ``docs/snippets/minimal_emitter.py`` as a subprocess against the
``running_daemon`` fixture's store and doorbell, then asserts:

1. the row lands in the events table with the documented field values
   and a well-formed 26-character Crockford ULID ``event_id``;
2. the doorbell ring actually wakes the daemon -- a live in-process
   subscriber receives the event frame;
3. a store without the events table is refused with exit 2 (external
   emitters never create schema);
4. the snippet imports only stdlib modules (AST walk), so it stays
   copy-pasteable into environments without the waitbus package.

This is the write-side counterpart of ``tests/test_subscriber_snippet.py``:
any breaking change to the insert columns, the ``delivery_id``
idempotency semantics, or the doorbell wire fails here at the same
commit that introduces it.
"""

from __future__ import annotations

import ast
import asyncio
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from waitbus import _subscribe

pytestmark = pytest.mark.asyncio

_SNIPPET = Path(__file__).resolve().parents[1] / "docs" / "snippets" / "minimal_emitter.py"
_CROCKFORD = set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")


def _run_snippet(body: str, *, db: Path, doorbell: Path) -> subprocess.CompletedProcess[str]:
    """Run the snippet as a fully-isolated subprocess, the way an operator would."""
    env = os.environ.copy()
    env["WAITBUS_DB"] = str(db)
    env["WAITBUS_DOORBELL_SOCKET"] = str(doorbell)
    return subprocess.run(
        [sys.executable, str(_SNIPPET), body],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


async def test_emitter_snippet_row_lands_and_daemon_delivers(
    running_daemon: tuple[object, dict[str, Path]],
) -> None:
    """The snippet's insert + doorbell ring reaches a live subscriber.

    A ``wait_for`` subscriber is opened first; the snippet then emits in
    a retry loop (each run is a distinct ``delivery_id``, so retries are
    new rows, not dedup no-ops) until the frame arrives. The loop
    absorbs the subscribe-registration race the warmup loop in
    ``test_subscriber_snippet.py`` absorbs the same way.
    """
    _, paths = running_daemon
    waiter = asyncio.create_task(
        asyncio.to_thread(
            _subscribe.wait_for,
            'fields.msg_from="minimal-emitter"',
            socket_path=str(paths["broadcast"]),
            timeout=20.0,
        )
    )
    await asyncio.sleep(0.2)

    delivery_ids: list[str] = []
    frame = None
    for _ in range(10):
        proc = _run_snippet("hello from the emitter snippet", db=paths["db"], doorbell=paths["doorbell"])
        assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
        delivery_id, _, inserted_part = proc.stdout.strip().partition("\t")
        assert inserted_part == "inserted=True", proc.stdout
        delivery_ids.append(delivery_id)
        done, _pending = await asyncio.wait({waiter}, timeout=1.0)
        if done:
            frame = waiter.result()
            break
    assert frame is not None, "no frame delivered after 10 emit attempts"
    assert frame.delivery_id in delivery_ids
    assert frame.fields.get("source") == "agent"
    assert frame.fields.get("msg_from") == "minimal-emitter"
    assert frame.event_type == "agent_message"

    # Every emitted row is stored with the documented shape and a
    # well-formed Crockford ULID event_id.
    conn = sqlite3.connect(str(paths["db"]))
    try:
        rows = conn.execute(
            "SELECT delivery_id, source, event_type, owner, repo, msg_body, event_id "
            "FROM events WHERE msg_from = 'minimal-emitter'"
        ).fetchall()
    finally:
        conn.close()
    assert {row[0] for row in rows} == set(delivery_ids)
    for _did, source, event_type, owner, repo, msg_body, event_id in rows:
        assert (source, event_type, owner, repo) == ("agent", "agent_message", "local", "minimal-emitter")
        assert msg_body == "hello from the emitter snippet"
        assert len(event_id) == 26 and set(event_id) <= _CROCKFORD, event_id


async def test_emitter_snippet_documented_sql_is_idempotent(
    running_daemon: tuple[object, dict[str, Path]],
) -> None:
    """Re-running the documented INSERT OR IGNORE with a fixed delivery_id is a no-op.

    The snippet itself embeds ``time_ns`` in the ``delivery_id`` (each
    occurrence is a distinct event), so the idempotency
    contract is asserted on the documented SQL directly: a second
    insert of the same ``delivery_id`` changes nothing.
    """
    _, paths = running_daemon
    proc = _run_snippet("idempotency probe", db=paths["db"], doorbell=paths["doorbell"])
    assert proc.returncode == 0, proc.stderr
    delivery_id = proc.stdout.split("\t", 1)[0]

    conn = sqlite3.connect(str(paths["db"]))
    try:
        before = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        cursor = conn.execute(
            "INSERT OR IGNORE INTO events "
            "(delivery_id, source, event_type, owner, repo, received_at, payload_json, ingest_method, event_id) "
            "VALUES (?, 'agent', 'agent_message', 'local', 'minimal-emitter', 2000000000000000000, '{}', "
            "'minimal_emitter', '01ARZ3NDEKTSV4RRFFQ69G5FAV')",
            (delivery_id,),
        )
        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        conn.close()
    assert cursor.rowcount == 0
    assert after == before


async def test_emitter_snippet_refuses_schemaless_store(tmp_path: Path) -> None:
    """A store without the events table exits 2 and tells the operator why."""
    empty_db = tmp_path / "empty.db"
    sqlite3.connect(str(empty_db)).close()
    proc = _run_snippet("nope", db=empty_db, doorbell=tmp_path / "no-doorbell.sock")
    assert proc.returncode == 2
    assert "never creates schema" in proc.stderr


async def test_emitter_snippet_imports_stdlib_only() -> None:
    """Every import in the snippet resolves to the standard library.

    The snippet's whole point is emitting without the waitbus package;
    a convenience import of ``waitbus`` (or anything third-party) would
    silently break the copy-paste contract.
    """
    tree = ast.parse(_SNIPPET.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
            roots.add(node.module.split(".")[0])
    non_stdlib = roots - set(sys.stdlib_module_names)
    assert not non_stdlib, f"non-stdlib imports in minimal_emitter.py: {sorted(non_stdlib)}"
