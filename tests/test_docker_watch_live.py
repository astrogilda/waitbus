"""Live-loop integration tests for the docker watcher.

The frame-shape unit tests live in test_sources.py and the stop-seam
tests in test_serve_unit.py; these drive the real chunkless HTTP-over-UDS
read loop against a fake Engine /events endpoint — the happy emit path,
the reconnect-after-drop path, the non-200 reject, and the connect-error
taxonomy — so the module can sit under the per-file coverage gate.
"""

from __future__ import annotations

import json
import socket
import sqlite3
import threading
import time
from contextlib import closing, suppress
from pathlib import Path

import pytest

from waitbus.sources import docker_watch


def _die_event(container_id: str, exit_code: str, epoch: int) -> bytes:
    return (
        json.dumps(
            {
                "Type": "container",
                "Action": "die",
                "Actor": {"ID": container_id, "Attributes": {"name": f"job-{container_id}", "exitCode": exit_code}},
                "time": epoch,
            }
        ).encode()
        + b"\n"
    )


class _FakeEngine:
    """Minimal /events endpoint: per-accept scripted response bodies.

    Each accepted connection consumes the request head, sends a plain
    (close-delimited) HTTP response with the next scripted body, and
    closes — exactly the read-until-EOF shape the watcher's response
    loop consumes.
    """

    def __init__(self, sock_path: Path, bodies: list[bytes], *, status: bytes = b"HTTP/1.1 200 OK\r\n\r\n") -> None:
        self._path = sock_path
        self._bodies = bodies
        self._status = status
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(sock_path))
        self._server.listen(4)
        self._server.settimeout(10.0)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        for body in self._bodies:
            try:
                conn, _ = self._server.accept()
            except OSError:
                return
            with closing(conn):
                conn.settimeout(5.0)
                # Consume the request head; content is irrelevant.
                with suppress(OSError):
                    while b"\r\n\r\n" not in conn.recv(65536):
                        pass
                with suppress(OSError):
                    conn.sendall(self._status + body)

    def close(self) -> None:
        self._server.close()
        self._thread.join(timeout=5.0)


@pytest.fixture
def docker_db(broadcast_paths: dict[str, Path]) -> Path:
    from waitbus import _db

    db = broadcast_paths["db"]
    _db.ensure_schema(db)
    return db


def _rows(db: Path) -> list[tuple[str, str]]:
    with closing(sqlite3.connect(db)) as conn:
        return list(conn.execute("SELECT delivery_id, conclusion FROM events ORDER BY received_at"))


def test_watch_emits_terminal_events_and_grades_exit_codes(tmp_path: Path, docker_db: Path) -> None:
    """Two die events over the real read loop land graded rows, then exit."""
    sock = tmp_path / "docker.sock"
    epoch = int(time.time())
    engine = _FakeEngine(sock, [_die_event("aaa", "0", epoch) + _die_event("bbb", "1", epoch + 1)])
    try:
        rc = docker_watch.watch(socket_path=str(sock), db_path=docker_db, _max_events=2)
        assert rc == 0
        rows = _rows(docker_db)
        assert len(rows) == 2, rows
        assert rows[0][1] == "success" and "aaa" in rows[0][0]
        assert rows[1][1] == "failure" and "bbb" in rows[1][0]
    finally:
        engine.close()


def test_watch_reconnects_after_a_stream_drop(tmp_path: Path, docker_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An EOF mid-stream reconnects (zero backoff) and resumes emitting."""
    monkeypatch.setattr(docker_watch, "_RECONNECT_BACKOFF_BASE_S", 0.0)
    sock = tmp_path / "docker.sock"
    epoch = int(time.time())
    # First accept serves one event then EOF; the reconnect serves the second.
    engine = _FakeEngine(sock, [_die_event("one", "0", epoch), _die_event("two", "137", epoch + 2)])
    try:
        rc = docker_watch.watch(socket_path=str(sock), db_path=docker_db, _max_events=2)
        assert rc == 0
        rows = _rows(docker_db)
        assert len(rows) == 2, rows
        assert {r[1] for r in rows} == {"success", "failure"}
    finally:
        engine.close()


def test_watch_skips_non_terminal_and_malformed_lines(tmp_path: Path, docker_db: Path) -> None:
    """Non-container types, non-terminal actions, and junk lines are skipped."""
    sock = tmp_path / "docker.sock"
    epoch = int(time.time())
    noise = (
        b"not json at all\n"
        + json.dumps({"Type": "network", "Action": "connect"}).encode()
        + b"\n"
        + json.dumps({"Type": "container", "Action": "exec_die:sh", "time": epoch}).encode()
        + b"\n"
    )
    engine = _FakeEngine(sock, [noise + _die_event("real", "0", epoch)])
    try:
        rc = docker_watch.watch(socket_path=str(sock), db_path=docker_db, _max_events=1)
        assert rc == 0
        rows = _rows(docker_db)
        assert len(rows) == 1 and "real" in rows[0][0]
    finally:
        engine.close()


def test_events_non_200_raises_docker_socket_error(tmp_path: Path) -> None:
    sock = tmp_path / "docker.sock"
    engine = _FakeEngine(sock, [b"server on fire"], status=b"HTTP/1.1 500 Internal Server Error\r\n\r\n")
    try:
        with pytest.raises(docker_watch.DockerSocketError, match="HTTP 500"):
            list(docker_watch._iter_event_lines(str(sock), since=None, until=None))
    finally:
        engine.close()


def test_connect_to_absent_socket_raises_docker_socket_error(tmp_path: Path) -> None:
    with pytest.raises(docker_watch.DockerSocketError, match=r"cannot connect|no docker socket|does not exist"):
        list(docker_watch._iter_event_lines(str(tmp_path / "missing.sock"), since=None, until=None))


def test_until_window_returns_after_stream_end(tmp_path: Path, docker_db: Path) -> None:
    """A bounded until window exits 0 at stream end instead of reconnecting."""
    sock = tmp_path / "docker.sock"
    epoch = int(time.time())
    engine = _FakeEngine(sock, [_die_event("solo", "0", epoch)])
    try:
        rc = docker_watch.watch(socket_path=str(sock), db_path=docker_db, until=epoch + 5)
        assert rc == 0
        assert len(_rows(docker_db)) == 1
    finally:
        engine.close()
