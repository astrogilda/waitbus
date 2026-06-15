"""pytest -> waitbus emitter (EXPLICIT opt-in plugin).

What it does
------------
Accumulates per-phase outcome records *in memory* during the run and
performs a **single batched emit at** ``pytest_sessionfinish``: one
``insert_event(commit=False)`` per record, one ``conn.commit()``, and
one broadcast doorbell ring for the whole session.

Recording shape (what counts as one row)
----------------------------------------
``pytest_runtest_logreport`` fires once per phase (setup, call,
teardown) per test. This plugin records ``(nodeid, outcome,
duration_ns)`` for the call phase *and* for any phase whose report
``failed`` flag is set, and ``delivery_id`` embeds ``outcome``. The
resulting invariant is therefore **one row per distinct outcome per
nodeid within a session**: a test that fails identically in two phases
(e.g. setup-fail + teardown-fail both ``"failed"``) collapses to one
row via the idempotent ``delivery_id``; a test that fails in two
phases with distinct outcomes (e.g. ``call="failed"`` and a fixture
``teardown`` reported as a separate state) produces one row per
distinct outcome. Independently-actionable per-phase failures are
surfaced rather than discarded. This is exactly pytest's
own ``_pytest.junitxml`` shape (build all records during the run, write
once at session finish) and exactly the shipped ``etag-poll`` oneshot
batched-write precedent (``insert_event(commit=False)`` in a loop ->
one commit -> one ``_doorbell.ring()``). There is **no per-test
commit and no per-test ring** — a 5 000-test suite produces one
transaction and one doorbell, not 5 000.

Explicit opt-in — NOT a ``pytest11`` autoload entry-point
---------------------------------------------------------
This plugin is **not** registered under the
``[project.entry-points.pytest11]`` group. A CI-status tool must not
silently hijack *every* ``pytest`` invocation on a developer's machine
and write rows into the event store on every unrelated test run. It is
enabled only when the operator explicitly asks for it, in one of two
idiomatic pytest ways:

* command line / ``addopts``::

      pytest -p waitbus.sources.pytest_emit ...

* an explicit import in the project's ``conftest.py``::

      pytest_plugins = ["waitbus.sources.pytest_emit"]

When the plugin is loaded it still does nothing unless
``--waitbus-emit`` is passed (or ``WAITBUS_PYTEST_EMIT=1`` is set), so even
`-p`-loading it in a shared ``addopts`` is safe by default — the
operator opts in per run.

pytest-xdist behaviour (documented)
------------------------------------------------
Under ``pytest-xdist`` each worker process runs ``pytest_sessionfinish``
for the subset of tests it executed, so an ``-n N`` run produces **N
partial emits** (N transactions, N doorbell rings), one per worker,
each covering that worker's tests. This is the same per-process
batching the ``etag-poll`` oneshot exhibits and is acceptable: the
``session_id`` is bound to xdist's shared ``PYTEST_XDIST_TESTRUNUID``
(see :func:`_session_id`) so the ``delivery_id`` is deterministic per
``(run, nodeid, outcome)`` *across workers* — the union of the N
partial emits is therefore exactly one row per ``(nodeid, distinct
outcome)`` (matching the per-process invariant above), and a re-run is
an idempotent no-op. The controller process' ``sessionfinish`` (which
sees no items) emits nothing. Coordinating a single cross-worker commit
would require an xdist-specific IPC channel that buys nothing the
run-scoped idempotent ``delivery_id`` does not already give.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec

from .. import _db, _paths
from .. import _emit as emit_mod
from .._types import EventInsert

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pytest


_INGEST_METHOD = "pytest_sessionfinish"
_EVENT_TYPE = "pytest_session"
_ENV_OPT_IN = "WAITBUS_PYTEST_EMIT"
# xdist exports this in every worker process of a single run
# (xdist/remote.py sets os.environ["PYTEST_XDIST_TESTRUNUID"]); it is
# the one identifier shared across all workers, so binding the session
# id to it makes the deterministic delivery_id collide *across workers*
# and the documented "union is one row per (nodeid, distinct outcome)"
# property actually hold (without it each worker's time/pid id diverges
# and the idempotency PK never matches).
_XDIST_RUN_UID = "PYTEST_XDIST_TESTRUNUID"


def _env_flag(name: str) -> bool:
    """Return True iff ``name`` is set to a truthy value (1/true/yes/on)."""
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _session_id() -> str:
    """A run-stable session identifier for the delivery_id natural key.

    Under ``pytest-xdist`` every worker is a separate process but shares
    one ``PYTEST_XDIST_TESTRUNUID`` — use it so a test that runs on any
    worker yields the same ``delivery_id`` and the per-worker partial
    emits idempotently converge to one row per ``(nodeid, distinct
    outcome)``. Outside xdist
    (single process) there is no shared run id, so ``time_ns()-pid`` is
    a sufficient unique session id.
    """
    run_uid = os.environ.get(_XDIST_RUN_UID, "").strip()
    if run_uid:
        return f"xdist-{run_uid}"
    return f"{int(time.time_ns())}-{os.getpid()}"


class _Recorder:
    """Per-process in-memory accumulator of test outcomes.

    One instance is registered as a pytest plugin object for the
    lifetime of the (worker) process. It holds nothing but a list of
    ``(nodeid, outcome, duration_ns)`` triples until ``sessionfinish``,
    at which point it builds the batch and emits once.
    """

    def __init__(
        self,
        *,
        db_path: Path | None,
        owner: str,
        repo: str,
        config: pytest.Config,
    ) -> None:
        self._db_path = db_path
        self._owner = owner
        self._repo = repo
        self._config = config
        self._session_id = _session_id()
        self._results: list[tuple[str, str, int]] = []

    def _is_xdist_controller(self) -> bool:
        """True iff this is the xdist *controller* of a distributed run.

        Under ``-n N`` the controller process *also* receives every
        forwarded test report and runs ``sessionfinish`` — but the
        workers are the processes that actually executed the tests and
        own the emit. If the controller emitted too, every test would be
        written twice (once by its worker, once by the controller, with
        a different session id). The controller is the process that has
        ``workerinput`` *absent* while xdist distribution is active
        (a worker has ``config.workerinput``; a plain non-xdist run has
        neither attribute *and* no ``numprocesses``). Detecting it lets
        the controller stand down so the per-worker emits are the single
        source of truth.
        """
        if hasattr(self._config, "workerinput"):
            return False  # this IS a worker — it must emit
        numprocesses = self._config.getoption("numprocesses", None)
        return bool(numprocesses)

    # --- collection -------------------------------------------------------

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        """Record the call-phase outcome of one test.

        Only the ``call`` phase is recorded for a passed test; a failure
        in ``setup``/``call``/``teardown`` is recorded with its phase so
        an errored test is not silently dropped. This mirrors how
        junitxml attributes a test's terminal state.
        """
        if report.when == "call" or report.failed:
            self._results.append((report.nodeid, report.outcome, int(report.duration * 1e9)))

    # --- single batched emit at session finish ----------------------------

    def pytest_sessionfinish(self, exitstatus: int) -> None:
        """Build one EventInsert per recorded test and emit the batch once.

        Delegates the connect / ``insert_event(commit=False)`` loop /
        single ``commit`` / single ``_doorbell.ring()`` sequence to the
        shared :func:`waitbus._emit.emit_batch` seam (the same
        hardened batched-write body ``etag-poll`` uses), so this source
        no longer re-implements the ingress lifecycle. One ``received_at``
        stamps the whole batch, exactly as before.

        The xdist controller stands down (the workers own the emit); see
        :meth:`_is_xdist_controller`.
        """
        if not self._results or self._is_xdist_controller():
            return
        # Apply the events-table schema on the target DB before the
        # first emit. Both daemons (broadcast, listener) call
        # ``ensure_schema`` at startup, but the dogfood path runs
        # pytest WITHOUT a daemon -- the plugin writes straight to
        # SQLite and ``waitbus stats`` reads it later. A fresh DB would
        # otherwise hit ``sqlite3.OperationalError: no such table:
        # events`` on the first insert. Idempotent: if the schema is
        # already present (because a daemon is also running against
        # this DB) the call is a cheap no-op.
        target_db = _paths.resolve_db_path(self._db_path)
        _db.ensure_schema(target_db)
        received_at = int(time.time_ns())
        emit_mod.emit_batch(
            (
                self._build(
                    nodeid=nodeid,
                    outcome=outcome,
                    duration_ns=duration_ns,
                    exitstatus=exitstatus,
                    received_at=received_at,
                )
                for nodeid, outcome, duration_ns in self._results
            ),
            db_path=self._db_path,
        )

    def _build(
        self,
        *,
        nodeid: str,
        outcome: str,
        duration_ns: int,
        exitstatus: int,
        received_at: int,
    ) -> EventInsert:
        """Construct the write-shape EventInsert for one test outcome.

        ``delivery_id`` is the deterministic natural key
        ``pytest:<session>:<nodeid>:<outcome>`` — stable across a re-run
        of the same session object and across xdist workers, so the
        idempotency PK collapses duplicates to a no-op. ``conclusion``
        is mapped onto the GitHub-conclusion vocabulary the rest of the
        bus already speaks (``success``/``failure``/``skipped``) so a
        ``waitbus wait`` predicate works against a pytest event with no
        special-casing.
        """
        conclusion = {
            "passed": "success",
            "failed": "failure",
            "skipped": "skipped",
        }.get(outcome, outcome)
        payload: dict[str, Any] = {
            "nodeid": nodeid,
            "outcome": outcome,
            "duration_ns": duration_ns,
            "session_exitstatus": exitstatus,
            "session_id": self._session_id,
        }
        return EventInsert(
            delivery_id=f"pytest:{self._session_id}:{nodeid}:{outcome}",
            source="pytest",
            event_type=_EVENT_TYPE,
            owner=self._owner,
            repo=self._repo,
            received_at=received_at,
            payload_json=msgspec.json.encode(payload).decode(),
            ingest_method=_INGEST_METHOD,
            status="completed",
            conclusion=conclusion,
        )


# ---------------------------------------------------------------------------
# pytest plugin hooks (module-level — pytest discovers these by name)
# ---------------------------------------------------------------------------


def pytest_addoption(
    parser: pytest.Parser,
) -> None:  # pragma: no cover - invoked by pytest internals, not callable in unit tests
    """Register the explicit opt-in flag and owner/repo overrides.

    The plugin being *loaded* (via ``-p`` or ``pytest_plugins``) is not
    enough; ``--waitbus-emit`` (or ``WAITBUS_PYTEST_EMIT=1``) must also be
    given. This makes it safe to put ``-p waitbus.sources.pytest_emit``
    in a shared ``addopts`` without every run writing events.
    """
    group = parser.getgroup("waitbus", "waitbus event-store emitter")
    group.addoption(
        "--waitbus-emit",
        action="store_true",
        default=False,
        help="Emit a batched pytest_session event per finished session "
        "into the waitbus event store (one commit + one doorbell ring "
        "at sessionfinish). Also enabled by WAITBUS_PYTEST_EMIT=1.",
    )
    group.addoption(
        "--waitbus-owner",
        action="store",
        default="local",
        help="owner label for the emitted events (default: 'local').",
    )
    group.addoption(
        "--waitbus-repo",
        action="store",
        default="pytest",
        help="repo label for the emitted events (default: 'pytest').",
    )
    group.addoption(
        "--waitbus-db",
        action="store",
        default=None,
        help="Path to the events SQLite DB (default: the platformdirs-resolved location).",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register the recorder plugin object iff the operator opted in.

    No opt-in -> no plugin object registered -> zero overhead and zero
    writes. This is the per-process registration point; under xdist it
    runs in every worker, which is exactly what produces the documented
    N-partial-emit behaviour.
    """
    if not (config.getoption("--waitbus-emit") or _env_flag(_ENV_OPT_IN)):
        return
    db_opt = config.getoption("--waitbus-db")
    recorder = _Recorder(
        db_path=Path(db_opt) if db_opt else None,
        owner=str(config.getoption("--waitbus-owner")),
        repo=str(config.getoption("--waitbus-repo")),
        config=config,
    )
    config.pluginmanager.register(recorder, "waitbus-pytest-emit-recorder")
