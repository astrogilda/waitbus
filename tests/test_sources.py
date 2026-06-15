"""Tests for the three local event-source clients (docker_watch, fs_watch, pytest_emit).

Every watcher is a thin client of the public ``emit`` API: the
assertions are (a) it builds a correct ``EventInsert`` with the right
source string and (b) the row round-trips into the events DB. The
broadcast doorbell is patched to a no-op throughout (no daemon in unit
tests — a missed ring is a bounded delay, not an error), exactly as
``test_emit.py`` does.



* pytest emitter: ONE batched commit (no per-test commit), no per-test
  doorbell ring, correct opt-in gating, deterministic idempotent
  ``delivery_id``;
* docker watcher: die (exit 0 -> success / non-zero -> failure) and
  stop/kill (-> cancelled) parsing, the ``since`` cursor advance, and
  non-container/non-terminal frames skipped;
* fs watcher: a completed close-write fires exactly one event, atomic
  temp churn (modified/created) is ignored, and a missing ``[fs]``
  extra raises a clean actionable error.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Protocol, cast

import pytest

from waitbus import _db
from waitbus.sources import docker_watch, fs_watch, pytest_emit


@pytest.fixture(autouse=True)
def _silence_doorbell(monkeypatch: pytest.MonkeyPatch) -> None:
    """No daemon in unit tests — sources must not depend on a live ring."""
    monkeypatch.setattr(_db._doorbell, "ring", lambda _path=None: None)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "events.db"
    _db.ensure_schema(path)
    return path


def _rows(db_path: Path) -> list[tuple[Any, ...]]:
    with _db.connect(db_path, readonly=True) as conn:
        return conn.execute(
            "SELECT source, event_type, conclusion, delivery_id, payload_json FROM events ORDER BY event_id"
        ).fetchall()


# ===========================================================================
# pytest emitter
# ===========================================================================


class _ReportLike(Protocol):
    """Structural contract `pytest_runtest_logreport` actually consumes.

    The recorder only reads ``when``/``failed``/``nodeid``/``outcome``/
    ``duration`` off the report; this Protocol pins exactly that surface
    
    """

    nodeid: str
    when: str
    outcome: str
    duration: float

    @property
    def failed(self) -> bool: ...


class _FakeReport:
    def __init__(self, nodeid: str, when: str, outcome: str, duration: float):
        self.nodeid = nodeid
        self.when = when
        self.outcome = outcome
        self.duration = duration

    @property
    def failed(self) -> bool:
        return self.outcome == "failed"


def _fake_report(nodeid: str, when: str, outcome: str, duration: float) -> pytest.TestReport:
    """Build a `_FakeReport` and cast once at the test/production boundary.

    `pytest_runtest_logreport` is annotated `report: pytest.TestReport`;
    `_FakeReport` satisfies the consumed `_ReportLike` surface but is not
    a nominal `TestReport`. 
    """
    report: _ReportLike = _FakeReport(nodeid, when, outcome, duration)
    return cast("pytest.TestReport", report)


class _FakeConfig:
    """Minimal pytest.Config stand-in: non-xdist single-process run
    (no ``workerinput``, ``numprocesses`` falsy) so the recorder emits."""

    def getoption(self, name: str, default: object = None) -> object:
        if name == "numprocesses":
            return None
        return default


def _recorder(db: Path) -> pytest_emit._Recorder:
    return pytest_emit._Recorder(
        db_path=db,
        owner="o",
        repo="r",
        config=_FakeConfig(),  # type: ignore[arg-type]
    )


def test_pytest_emitter_batches_one_commit_no_per_test_ring(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """N tests -> every insert_event is commit=False (no per-test commit),
    exactly ONE explicit batch commit, and ONE doorbell ring (the
    junitxml / etag-poll batched pattern), not N."""
    insert_commit_kwargs: list[bool] = []
    rings = {"n": 0}
    real_insert = _db.insert_event

    def _spy_insert(conn: Any, event: Any, *, commit: bool = True) -> bool:
        insert_commit_kwargs.append(commit)
        return real_insert(conn, event, commit=commit)

    # The single batch commit happens on the connection object; count it
    # by spying on the connection factory's yielded conn.commit via a
    # thin proxy that records calls.
    real_connect = _db.connect
    commits = {"n": 0}

    import contextlib

    class _ConnProxy:
        def __init__(self, conn: Any) -> None:
            self._c = conn

        def commit(self) -> None:
            commits["n"] += 1
            self._c.commit()

        def __getattr__(self, name: str) -> Any:
            return getattr(self._c, name)

    @contextlib.contextmanager
    def _proxy_connect(*a: Any, **k: Any) -> Any:
        with real_connect(*a, **k) as conn:
            yield _ConnProxy(conn)

    monkeypatch.setattr(_db, "connect", _proxy_connect)
    monkeypatch.setattr(_db, "insert_event", _spy_insert)
    monkeypatch.setattr(_db._doorbell, "ring", lambda _path=None: rings.__setitem__("n", rings["n"] + 1))

    rec = _recorder(db)
    for i in range(5):
        rec.pytest_runtest_logreport(_fake_report(f"test_{i}", "call", "passed", 0.01))
    rec.pytest_sessionfinish(exitstatus=0)

    assert insert_commit_kwargs == [False] * 5, "no per-test commit"
    assert commits["n"] == 1, "expected exactly one batched commit"
    assert rings["n"] == 1, "expected exactly one doorbell ring for the batch"
    rows = _rows(db)
    assert len(rows) == 5
    assert {r[0] for r in rows} == {"pytest"}
    assert {r[2] for r in rows} == {"success"}


def test_pytest_emitter_maps_outcomes_and_is_idempotent(db: Path) -> None:
    """passed->success, failed->failure, skipped->skipped; a second
    sessionfinish on the same recorder is an idempotent no-op."""
    rec = _recorder(db)
    rec.pytest_runtest_logreport(_fake_report("t_pass", "call", "passed", 0.0))
    rec.pytest_runtest_logreport(_fake_report("t_fail", "call", "failed", 0.0))
    rec.pytest_runtest_logreport(_fake_report("t_skip", "call", "skipped", 0.0))
    rec.pytest_sessionfinish(exitstatus=1)
    rec.pytest_sessionfinish(exitstatus=1)  # replay -> no-op

    rows = _rows(db)
    assert len(rows) == 3, "second sessionfinish must not duplicate rows"
    by_conc = {r[2] for r in rows}
    assert by_conc == {"success", "failure", "skipped"}


def test_pytest_emitter_multi_phase_collapses_by_distinct_outcome(
    db: Path,
) -> None:
    """The contract is "one row per (nodeid, distinct outcome)", not per
    test: a test that fails identically in two phases (setup + teardown
    both ``"failed"``) collapses to one row via the idempotent
    ``delivery_id``; a test that fails in two phases with distinct
    outcomes produces one row per distinct outcome."""
    rec = _recorder(db)
    # Same nodeid, two phases, identical "failed" outcome -> 1 row
    # (delivery_id collides; insert-or-ignore is the idempotency gate).
    rec.pytest_runtest_logreport(_fake_report("t_same", "setup", "failed", 0.0))
    rec.pytest_runtest_logreport(_fake_report("t_same", "teardown", "failed", 0.0))
    # Same nodeid, two phases, distinct outcomes -> 2 rows
    # (different delivery_ids -- both signals are independently
    # actionable; the contract does NOT discard either).
    rec.pytest_runtest_logreport(_fake_report("t_diff", "call", "passed", 0.0))
    rec.pytest_runtest_logreport(_fake_report("t_diff", "teardown", "failed", 0.0))
    rec.pytest_sessionfinish(exitstatus=1)

    rows = _rows(db)
    # 1 (t_same: failure) + 2 (t_diff: success, failure) = 3
    assert len(rows) == 3
    by_node_conc = {(r[1], r[2]) for r in rows}
    # rows columns are (source, delivery_id, conclusion, ...); the
    # delivery_id encodes nodeid+outcome, so distinct (delivery_id,
    # conclusion) pairs prove the contract.
    assert {c for _, c in by_node_conc} == {"failure", "success"}


def test_pytest_emitter_no_results_emits_nothing(db: Path) -> None:
    """Controller process under xdist (no items) emits nothing."""
    rec = _recorder(db)
    rec.pytest_sessionfinish(exitstatus=0)
    assert _rows(db) == []


def test_session_id_uses_xdist_run_uid_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """When PYTEST_XDIST_TESTRUNUID is set, _session_id returns the xdist- prefix form."""
    monkeypatch.setenv(pytest_emit._XDIST_RUN_UID, "abc123")
    sid = pytest_emit._session_id()
    assert sid == "xdist-abc123"


def test_is_xdist_controller_returns_false_for_worker() -> None:
    """A config with ``workerinput`` is a worker, not the controller."""
    from typing import ClassVar

    class _WorkerConfig:
        workerinput: ClassVar[dict[str, str]] = {}

        def getoption(self, name: str, default: object = None) -> object:
            return default

    rec = pytest_emit._Recorder(
        db_path=None,
        owner="o",
        repo="r",
        config=_WorkerConfig(),  # type: ignore[arg-type]
    )
    assert rec._is_xdist_controller() is False


def test_pytest_plugin_requires_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plugin loaded but neither --waitbus-emit nor WAITBUS_PYTEST_EMIT set
    -> no recorder registered (a CI tool must not hijack every run)."""
    # This run may itself be under the self-dogfood opt-in
    # (WAITBUS_PYTEST_EMIT=1 on the Linux CI cell); the no-opt-in branch
    # is exactly what we assert here, so neutralise the ambient env var.
    monkeypatch.delenv("WAITBUS_PYTEST_EMIT", raising=False)
    registered: list[Any] = []

    class _PM:
        def register(self, obj: Any, name: str) -> None:
            registered.append((obj, name))

    class _Config:
        def __init__(self, opt_in: bool):
            self._opt_in = opt_in
            self.pluginmanager = _PM()

        def getoption(self, name: str) -> Any:
            if name == "--waitbus-emit":
                return self._opt_in
            if name == "--waitbus-db":
                return None
            return "x"

    pytest_emit.pytest_configure(_Config(opt_in=False))  # type: ignore[arg-type]
    assert registered == []
    pytest_emit.pytest_configure(_Config(opt_in=True))  # type: ignore[arg-type]
    assert len(registered) == 1
    assert registered[0][1] == "waitbus-pytest-emit-recorder"


# ===========================================================================
# docker watcher
# ===========================================================================


def test_docker_die_exit_zero_is_success(db: Path) -> None:
    msg = {
        "Type": "container",
        "Action": "die",
        "Actor": {"ID": "abc123", "Attributes": {"name": "job", "exitCode": "0"}},
        "time": 1763337600,
        "timeNano": 1763337600123456789,
    }
    insert = docker_watch._build_event(msg, owner="o", repo="r")
    assert insert is not None
    assert insert.source == "docker"
    assert insert.conclusion == "success"
    assert insert.delivery_id == "docker:abc123:die:1763337600123456789"


def test_docker_die_nonzero_is_failure() -> None:
    msg = {
        "Type": "container",
        "Action": "die",
        "Actor": {"ID": "x", "Attributes": {"exitCode": "137"}},
        "time": 100,
    }
    insert = docker_watch._build_event(msg, owner="o", repo="r")
    assert insert is not None and insert.conclusion == "failure"


def test_docker_stop_is_cancelled_and_nonterminal_skipped() -> None:
    stop = {"Type": "container", "Action": "stop", "Actor": {"ID": "y"}, "time": 1}
    insert = docker_watch._build_event(stop, owner="o", repo="r")
    assert insert is not None and insert.conclusion == "cancelled"
    # start / non-container frames are skipped (not terminal).
    assert docker_watch._build_event({"Type": "container", "Action": "start", "Actor": {}}, owner="o", repo="r") is None
    assert docker_watch._build_event({"Type": "image", "Action": "pull", "Actor": {}}, owner="o", repo="r") is None


def test_docker_watch_round_trips_and_advances_cursor(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The watch loop emits a real row and, across a transport drop,
    reconnects with the since cursor advanced to (last event time - 1)
    so the disconnect window is replayed, not skipped."""
    seen_since: list[Any] = []
    first = json.dumps(
        {
            "Type": "container",
            "Action": "die",
            "Actor": {"ID": "c1", "Attributes": {"exitCode": "0"}},
            "time": 1_700_000_042,
            "timeNano": 1_700_000_042_000_000_000,
        }
    ).encode()
    calls = {"n": 0}

    def _fake_lines(sock: str, *, since: Any, until: Any, stopper: Any = None):  # type: ignore[no-untyped-def]
        seen_since.append(since)
        calls["n"] += 1
        if calls["n"] == 1:
            yield first
            # Transport drops mid-stream -> watch() reconnects with the
            # advanced cursor.
            raise OSError("connection reset")
        # Second connect: nothing more; the test stops the loop.
        raise _StopWatch

    class _StopWatch(BaseException):
        pass

    monkeypatch.setattr(docker_watch, "_iter_event_lines", _fake_lines)
    # Zero the backoff base instead of patching time.sleep: the reconnect
    # backoff now waits on the stopper's event, not time.sleep.
    monkeypatch.setattr(docker_watch, "_RECONNECT_BACKOFF_BASE_S", 0.0)
    with pytest.raises(_StopWatch):
        docker_watch.watch(db_path=db, owner="o", repo="r")

    rows = _rows(db)
    assert len(rows) == 1
    assert rows[0][0] == "docker"
    assert rows[0][2] == "success"
    assert json.loads(rows[0][4])["Action"] == "die"
    # First connect: no cursor. Reconnect after the drop: cursor advanced
    # to (last seen event time - 1) so the boundary event replays
    # (idempotent) rather than being skipped.
    assert seen_since == [None, 1_700_000_042 - 1]


def test_docker_watch_max_events_bounds_the_loop(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The ``_max_events`` test seam stops the (otherwise infinite)
    stream loop after N emits and returns 0 — the documented bounded-
    stop contract used to make the blocking watcher testable."""
    stream = [
        json.dumps(
            {
                "Type": "container",
                "Action": "die",
                "Actor": {"ID": f"c{i}", "Attributes": {"exitCode": "0"}},
                "time": 1_700_000_000 + i,
                "timeNano": (1_700_000_000 + i) * 1_000_000_000,
            }
        ).encode()
        for i in range(5)
    ]

    def _fake_lines(sock: str, *, since: Any, until: Any, stopper: Any = None):  # type: ignore[no-untyped-def]
        yield from stream

    monkeypatch.setattr(docker_watch, "_iter_event_lines", _fake_lines)
    code = docker_watch.watch(db_path=db, owner="o", repo="r", _max_events=2)
    assert code == 0
    # Bounded at 2 even though 5 terminal events were available.
    assert len(_rows(db)) == 2


# ===========================================================================
# fs watcher
# ===========================================================================


def test_fs_build_event_for_completed_write(db: Path, tmp_path: Path) -> None:
    f = tmp_path / "result.txt"
    f.write_text("done")
    insert = fs_watch._build_event(str(f), owner="o", repo="r")
    assert insert is not None
    assert insert.source == "fs"
    assert insert.conclusion == "success"
    assert insert.delivery_id.startswith(f"fs:{f.resolve()}:")


def test_fs_handler_fires_on_close_write_ignores_modified(
    db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only close-write / moved-in are terminal; modified/created churn
    (the atomic-save temp writes) is ignored -> exactly one row."""
    pytest.importorskip("watchdog")
    from watchdog.events import FileSystemEventHandler

    f = tmp_path / "saved.txt"
    f.write_text("x")
    debouncer = fs_watch._Debouncer(db)
    handler = fs_watch._make_handler(FileSystemEventHandler, owner="o", repo="r", sink=debouncer.add)

    class _Evt:
        def __init__(self, src: str, *, is_dir: bool = False, dest: str = ""):
            self.src_path = src
            self.is_directory = is_dir
            self.dest_path = dest

    # Atomic-save temp churn: on_modified / on_created are NOT implemented,
    # so the base no-op runs -> nothing emitted.
    if hasattr(handler, "on_modified"):
        handler.on_modified(_Evt(str(f)))  # base class no-op
    # Terminal close-write -> exactly one row.
    handler.on_closed(_Evt(str(f)))
    # Atomic rename of a temp onto a final name -> one more row.
    g = tmp_path / "renamed.txt"
    g.write_text("y")
    handler.on_moved(_Evt(str(tmp_path / ".tmp"), dest=str(g)))

    # Events are coalesced by the debouncer and written as one batch;
    # flush() drains it synchronously (the loop/stop path does this in
    # production).
    debouncer.flush()
    rows = _rows(db)
    assert len(rows) == 2
    assert {r[0] for r in rows} == {"fs"}


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Linux inotify flush-on-stop semantics; macOS FSEvents differs (fs source works, Linux-specific)",
)
def test_fs_watch_stop_event_returns_zero_and_flushes(db: Path, tmp_path: Path) -> None:
    """Setting the stop event unblocks watch() with 0 and flushes pending saves.

    Runs watch() in a worker thread against a tmp dir, performs a real
    close-write inside the watched tree, then sets the stop event. The
    final debouncer flush on the stop path must have written the save
    before watch() returns.
    """
    pytest.importorskip("watchdog")
    import threading
    import time

    watched = tmp_path / "watched"
    watched.mkdir()
    stop = threading.Event()
    result: list[int] = []

    def _run() -> None:
        result.append(fs_watch.watch(watched, db_path=db, stop_event=stop))

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    # Give the observer time to install its inotify watch before writing.
    time.sleep(0.3)
    (watched / "saved.txt").write_text("done")
    # The write lands via the observer thread; allow the event to reach
    # the debouncer buffer before stopping (stop() flushes the remainder).
    time.sleep(0.5)
    stop.set()
    thread.join(timeout=10.0)
    assert not thread.is_alive(), "watch() did not return after stop_event was set"
    assert result == [0]
    rows = _rows(db)
    assert len(rows) == 1
    assert rows[0][0] == "fs"


def test_fs_watch_missing_extra_raises_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[fs]-absent gives a clean, actionable error naming the install
    command — """
    import builtins

    real_import = builtins.__import__

    def _no_watchdog(name: str, *a: Any, **k: Any) -> Any:
        if name == "watchdog" or name.startswith("watchdog."):
            raise ModuleNotFoundError("No module named 'watchdog'")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", _no_watchdog)
    with pytest.raises(fs_watch.FsWatchDependencyError) as exc:
        fs_watch.watch("/tmp")
    assert "waitbus[fs]" in str(exc.value)


# ===========================================================================
# pytest emitter — real end-to-end pytest invocation (plugin discovery,
# explicit -p opt-in, batched sessionfinish, xdist N-partial)
# ===========================================================================


def _run_pytest(args: list[str], *, cwd: Path, env_extra: dict[str, str] | None = None) -> int:
    import os
    import subprocess
    import sys

    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "pytest", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    ).returncode


def _write_inner_suite(d: Path, n: int) -> None:
    body = "\n".join(f"def test_case_{i}():\n    assert True" for i in range(n))
    (d / "test_inner.py").write_text(body + "\n")


def test_plugin_e2e_explicit_opt_in_batches_one_row(tmp_path: Path) -> None:
    """A real `pytest -p waitbus.sources.pytest_emit --waitbus-emit`
    invocation writes ONE batched pytest_session run (one row per inner
    test, one transaction) — exercising plugin discovery + hooks + the sessionfinish batch end-to-end."""
    work = tmp_path / "proj"
    work.mkdir()
    _write_inner_suite(work, 3)
    inner_db = tmp_path / "inner.db"
    _db.ensure_schema(inner_db)

    rc = _run_pytest(
        [
            "-p",
            "waitbus.sources.pytest_emit",
            "--waitbus-emit",
            "--waitbus-db",
            str(inner_db),
            "-p",
            "no:cacheprovider",
            "-q",
            "test_inner.py",
        ],
        cwd=work,
    )
    assert rc == 0
    rows = _rows(inner_db)
    assert len(rows) == 3
    assert {r[0] for r in rows} == {"pytest"}
    assert {r[2] for r in rows} == {"success"}


def test_plugin_e2e_loaded_but_no_opt_in_writes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Plugin loaded via -p but WITHOUT --waitbus-emit / WAITBUS_PYTEST_EMIT
    writes nothing — a CI tool must not hijack every pytest run even when
    its plugin is on a shared `addopts` -p line."""
    # The inner pytest inherits this process's env via _run_pytest's
    # `dict(os.environ)`. Under the Linux CI cell that env carries
    # WAITBUS_PYTEST_EMIT=1 (self-dogfood opt-in), which would make the
    # inner suite emit and defeat the no-opt-in assertion. Drop it so the
    # inner run exercises the not-opted-in path.
    monkeypatch.delenv("WAITBUS_PYTEST_EMIT", raising=False)
    work = tmp_path / "proj2"
    work.mkdir()
    _write_inner_suite(work, 2)
    inner_db = tmp_path / "inner2.db"
    _db.ensure_schema(inner_db)

    rc = _run_pytest(
        [
            "-p",
            "waitbus.sources.pytest_emit",
            "--waitbus-db",
            str(inner_db),
            "-p",
            "no:cacheprovider",
            "-q",
            "test_inner.py",
        ],
        cwd=work,
    )
    assert rc == 0
    assert _rows(inner_db) == []


def test_plugin_e2e_xdist_n_partial_union_is_one_row_per_test(
    tmp_path: Path,
) -> None:
    """Under `-n 2` each worker runs its own sessionfinish (N partial
    emits); the deterministic delivery_id makes the union exactly one
    row per inner test (xdist behaviour, idempotent)."""
    pytest.importorskip("xdist")
    work = tmp_path / "proj3"
    work.mkdir()
    _write_inner_suite(work, 6)
    inner_db = tmp_path / "inner3.db"
    _db.ensure_schema(inner_db)

    rc = _run_pytest(
        [
            "-p",
            "waitbus.sources.pytest_emit",
            "--waitbus-emit",
            "--waitbus-db",
            str(inner_db),
            "-p",
            "no:cacheprovider",
            "-n",
            "2",
            "-q",
            "test_inner.py",
        ],
        cwd=work,
    )
    assert rc == 0
    rows = _rows(inner_db)
    # 6 inner tests, split across 2 workers -> 2 partial commits, but the
    # union is exactly one row per test ().
    assert len(rows) == 6
    nodeids = {json.loads(r[4])["nodeid"] for r in rows}
    assert len(nodeids) == 6
