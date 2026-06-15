"""LLM-agent subscriber pool RAII helper for measurement benches.

Wraps the stress harness's ``spawn_n_heterogeneous`` so a measurement
bench can spawn M heterogeneous real-LLM driver subprocesses per
window. Each driver parks in ``waitbus.wait_for`` against an
owner-scoped predicate; on first matching event the driver runs its
framework's LLM call (pydantic-ai, langgraph, claude-cli, gemini-cli,
shell-control), emits stdout markers, and exits.

Per the operator's recommendation Q3 (per-window respawn matches
``bench_polling_vs_subscribe_llm_agent``'s lifecycle), the pool's
``spawn()`` is called once per loaded window and ``teardown()``
collects exit codes + marker output before the next window starts.
Subscribers do NOT persist across windows -- the per-iteration
moderation reset matches every other bench in the repo.

The pool is consumer-agnostic. It does not call into any bench file;
the alternation-loop integration lives in the consuming bench.
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import time
from pathlib import Path
from types import TracebackType
from typing import Self

from scripts.stress._controller import _Child, _spawn_real_driver


class LlmAgentPoolError(RuntimeError):
    """A pool subscriber failed to start, settle, or react cleanly."""


class LlmAgentPool:
    """Manage M LLM-agent subscriber subprocesses for one loaded window.

    Usage (per loaded window, mirroring ``bench_polling_vs_subscribe_llm_agent``'s
    per-iteration lifecycle)::

        pool = LlmAgentPool(
            agent_count=5,
            env=daemon_env,
            socket_path=runtime_dir / "broadcast.sock",
            db_path=db_path,
            doorbell_path=doorbell_path,
            seed_scope_id="bench-multistream-abc123:window-7",
            python_exe=sys.executable,
            stderr_dir=tmp_path / "agent-stderr" / "window-7",
        )
        with pool:
            pool.spawn(since_cursor=anchor_event_id)
            pool.settle(timeout_sec=5.0)
            # CI producer swarm fires here; drivers wake + run LLM calls
            results = pool.collect(timeout_sec=90.0)
            print(results.reacted_count, results.framework_mix)

    ``agent_count == 0`` is a no-op (idle arm). The pool re-uses the
    stress harness's spawner so per-framework auth + subprocess shape
    stays in one place.
    """

    def __init__(
        self,
        *,
        frameworks: tuple[str, ...],
        env: dict[str, str],
        socket_path: Path,
        db_path: Path,
        doorbell_path: Path,
        seed_scope_id: str,
        python_exe: str,
        stderr_dir: Path,
    ) -> None:
        if any(not isinstance(f, str) or not f for f in frameworks):
            raise ValueError(f"frameworks must be a tuple of non-empty strings, got {frameworks!r}")
        self.frameworks = frameworks
        self.env = env
        self.socket_path = socket_path
        self.db_path = db_path
        self.doorbell_path = doorbell_path
        self.seed_scope_id = seed_scope_id
        self.python_exe = python_exe
        self.stderr_dir = stderr_dir
        self._children: list[_Child] = []
        self._framework_mix: dict[str, int] = {}

    @property
    def agent_count(self) -> int:
        """Total agent count = ``len(frameworks)`` (one driver per framework)."""
        return len(self.frameworks)

    @property
    def framework_mix(self) -> dict[str, int]:
        """Per-framework spawned count (e.g. ``{'pydantic': 1, 'langgraph': 1, ...}``)."""
        return dict(self._framework_mix)

    @property
    def attrition_detected(self) -> bool:
        """True iff any child subprocess exited with a non-zero status.

        Polls each child once. A child still running (``poll()`` is None)
        or one that exited cleanly (status 0) is not attrition; a non-zero
        exit means the agent driver crashed and its subscriber reaction is
        missing from the window.
        """
        return any((rc := c.proc.poll()) is not None and rc != 0 for c in self._children)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        self.teardown()

    def spawn(self, *, since_cursor: str | None = None, cold_prefix: str = "") -> None:
        """Spawn one driver subprocess per framework in ``frameworks``.

        Children are owner-scoped to ``seed_scope_id`` so a co-tenant
        emit cannot accidentally wake them. ``since_cursor`` anchors
        each driver's ``wait_for`` against a replay cursor (matches
        the stress controller's anchor-event pattern). ``cold_prefix``
        (when non-empty) is a per-iteration cache-buster threaded into
        every driver's prompt so a separate benchmark process cannot hit
        this run's cached prompt prefix under the same API key.
        """
        if not self.frameworks:
            return
        self.stderr_dir.mkdir(parents=True, exist_ok=True)
        children: list[_Child] = []
        mix: dict[str, int] = {}
        for idx, framework in enumerate(self.frameworks, start=1):
            fw_id = str(idx)  # bare ordinal; see _Child.framework for why no prefix
            child = _spawn_real_driver(
                framework=framework,
                fw_id=fw_id,
                socket_path=self.socket_path,
                db_path=self.db_path,
                doorbell_path=self.doorbell_path,
                seed_scope_id=self.seed_scope_id,
                env=self.env,
                python_exe=self.python_exe,
                stderr_dir=self.stderr_dir,
                since=since_cursor,
                cold_prefix=cold_prefix,
            )
            children.append(child)
            mix[framework] = mix.get(framework, 0) + 1
        self._children = children
        self._framework_mix = mix

    def settle(self, *, timeout_sec: float = 5.0) -> None:
        """Block until every child is observed alive past the settle window.

        A child that exits during the settle window is treated as a
        startup failure (daemon unreachable, secret missing, framework
        import crash). The pool aborts the window with a
        ``LlmAgentPoolError`` so the bench's main loop can flag this
        window rather than silently measuring against a dead pool.
        """
        if not self._children:
            return
        # Give children a short headstart so the waitbus SDK subscribe
        # round-trip lands before the first poll.
        time.sleep(min(0.25, timeout_sec))
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            dead = [c for c in self._children if c.proc.poll() is not None]
            if not dead:
                return
            time.sleep(0.1)
        dead_now = [c for c in self._children if c.proc.poll() is not None]
        if dead_now:
            roles = [c.role for c in dead_now]
            self.teardown()
            raise LlmAgentPoolError(
                f"{len(dead_now)} agent(s) exited within settle window; roles={roles}; "
                f"check stderr files under {self.stderr_dir}"
            )

    def collect(self, *, timeout_sec: float = 90.0) -> LlmAgentPoolResults:
        """Wait for every child to exit, return per-driver result rows.

        Each child is expected to (a) emit a stdout WAKE_RECEIVED line
        when its ``wait_for`` returns, (b) emit a DRIVER_REACTED line
        after its LLM call completes, and (c) exit with code 0. The
        pool returns the raw stdout bytes per child for the bench's
        existing marker parser; this module does NOT itself parse
        framework-specific envelopes (the bench's existing
        ``parse_*`` helpers stay the consumer).
        """
        if not self._children:
            return LlmAgentPoolResults(per_child=[], reacted_count=0, framework_mix=self._framework_mix)
        per_child: list[LlmAgentChildResult] = []
        deadline = time.monotonic() + timeout_sec
        for child in self._children:
            remaining = max(0.5, deadline - time.monotonic())
            try:
                stdout, _stderr = child.proc.communicate(timeout=remaining)
            except subprocess.TimeoutExpired:
                # Kill the hung child; record empty stdout. The bench's
                # invariant gate flags this row.
                with contextlib.suppress(OSError, ProcessLookupError):
                    os.killpg(child.proc.pid, 15)  # SIGTERM
                try:
                    stdout, _stderr = child.proc.communicate(timeout=2.0)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(OSError, ProcessLookupError):
                        child.proc.kill()
                    stdout = b""
            per_child.append(
                LlmAgentChildResult(
                    role=child.role,
                    framework=child.framework,
                    exit_code=child.proc.returncode,
                    stdout_bytes=stdout or b"",
                )
            )
        reacted = sum(1 for r in per_child if r.exit_code == 0)
        return LlmAgentPoolResults(
            per_child=per_child,
            reacted_count=reacted,
            framework_mix=self._framework_mix,
        )

    def teardown(self) -> None:
        """SIGTERM each child's process group then SIGKILL stragglers."""
        for child in self._children:
            with contextlib.suppress(OSError, ProcessLookupError):
                child.terminate(grace_sec=2.0)
        self._children.clear()


class LlmAgentChildResult:
    """One driver subprocess's framework + exit code + raw stdout (for marker parsing).

    ``framework`` is the first-class driver-framework identity carried
    straight off the spawned ``_Child``; downstream cost attribution
    reads it directly rather than parsing the framework back out of the
    ``role`` display label.
    """

    __slots__ = ("exit_code", "framework", "role", "stdout_bytes")

    def __init__(self, *, role: str, framework: str, exit_code: int, stdout_bytes: bytes) -> None:
        self.role = role
        self.framework = framework
        self.exit_code = exit_code
        self.stdout_bytes = stdout_bytes


class LlmAgentPoolResults:
    """Aggregate of every child's result for one loaded window."""

    __slots__ = ("framework_mix", "per_child", "reacted_count")

    def __init__(
        self,
        *,
        per_child: list[LlmAgentChildResult],
        reacted_count: int,
        framework_mix: dict[str, int],
    ) -> None:
        self.per_child = per_child
        self.reacted_count = reacted_count
        self.framework_mix = framework_mix


__all__ = [
    "LlmAgentChildResult",
    "LlmAgentPool",
    "LlmAgentPoolError",
    "LlmAgentPoolResults",
]
