"""Bench swarm-spawn factory for the heterogeneous swarm.

Lifts the per-arm driver-spawn shape from
``scripts.stress._controller.spawn_n_heterogeneous`` and re-parameterises
it for the bench's needs:

- Adds ``OPENAI_API_KEY`` to the env for the OpenAI-needing drivers
  (pydantic-ai, langgraph) and STRIPS it from the env for drivers that
  do not need it (shell-control, claude-cli, gemini-cli) so the key
  cannot leak through ``ps eww`` or driver stderr files (ensures
  OPENAI_API_KEY does not leak to non-OpenAI drivers).
- Sets ``PYTHONHASHSEED=0``, ``PYTHONUNBUFFERED=1``, and ``WAITBUS_BENCH_GC_OFF=1``
  on every spawn so the bench's per-iteration loop runs in a deterministic
  Python process. ``python3 -u`` is the canonical spawn pattern for
  unbuffered stdout (the bench reads driver stdout line-by-line via
  ``communicate``); the ``-u`` flag is added to the spawn argv.
- Preserves the five-driver framework set the controller already wires:
  ``pydantic``, ``langgraph``, ``claude-cli``, ``gemini-cli``, ``shell-control``.

The bench's own iteration loop is responsible for assembling per-arm
mixes (poll vs subscribe; idle vs loaded). This factory takes the
desired mix as an explicit dict so the bench's arm definitions stay
self-documenting.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

from scripts.stress._controller import _Child
from scripts.stress._real_drivers import FRAMEWORK_ORDER, REAL_MODE_ENV_VAR
from waitbus._log import structured

_logger = logging.getLogger("waitbus.bench.swarm")

# Frameworks that need an OpenAI API key in their subprocess env. Every
# other framework has its key stripped (ensures OPENAI_API_KEY does not
# leak to non-OpenAI drivers).
_FRAMEWORKS_NEEDING_OPENAI_KEY: frozenset[str] = frozenset({"pydantic", "langgraph"})

# Env keys the bench forces ON every spawn for determinism.
_BENCH_FORCED_ENV: dict[str, str] = {
    "PYTHONHASHSEED": "0",
    "PYTHONUNBUFFERED": "1",
    "WAITBUS_BENCH_GC_OFF": "1",
}

# Hard cap on the cache-busting prefix length threaded into the driver
# argv. Production values are ``force_cold_cache_prefix`` output (~200
# chars); 4096 leaves ample headroom while staying far below the Linux
# per-argument length floor (MAX_ARG_STRLEN, ~128 KiB) so an oversized
# value fails fast at the factory rather than as an E2BIG from Popen.
_MAX_COLD_PREFIX_LEN: int = 4096


def _build_env_for_framework(
    *,
    framework: str,
    base_env: dict[str, str],
    openai_api_key: str | None,
) -> dict[str, str]:
    """Compose the per-driver env dict for ``framework``.

    - Inherits every key from ``base_env``.
    - Forces ``PYTHONHASHSEED`` / ``PYTHONUNBUFFERED`` / ``WAITBUS_BENCH_GC_OFF``
      to deterministic values.
    - Adds ``OPENAI_API_KEY`` only when the framework needs it AND the
      key is non-empty.
    - Removes any pre-existing ``OPENAI_API_KEY`` from the env for
      frameworks that do not need it. The base env may carry the key
      (the orchestrator sets it process-wide after the keyring lookup);
      stripping it here is the load-bearing security gate.
    """
    env = dict(base_env)
    env.update(_BENCH_FORCED_ENV)
    # The orchestrator passes a non-None ``openai_api_key`` only under
    # real-LLM mode (``--include-real-llm``); ``None`` is the offline path
    # where the OpenAI-backed drivers legitimately run the offline fakes.
    real_llm_mode = openai_api_key is not None
    env.pop(REAL_MODE_ENV_VAR, None)
    if framework in _FRAMEWORKS_NEEDING_OPENAI_KEY:
        if openai_api_key:
            env["OPENAI_API_KEY"] = openai_api_key
        # If the key is missing AND this framework needs it, the bench
        # caller has already failed preflight; the spawn proceeds with
        # whatever the operator's env carries (which is at most absent).
        if real_llm_mode:
            # Signal the OpenAI-backed driver it is under real mode so its
            # selector hard-fails on an absent / shape-invalid OPENAI_API_KEY
            # instead of silently substituting the offline fake -- the
            # defense-in-depth that closes the silent-fallback class even if
            # the key vanishes between preflight and spawn.
            env[REAL_MODE_ENV_VAR] = "1"
    else:
        env.pop("OPENAI_API_KEY", None)
    return env


def _spawn_bench_driver(
    *,
    framework: str,
    fw_id: str,
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    env: dict[str, str],
    python_exe: str,
    stderr_dir: Path,
    cold_prefix: str = "",
    since: str | None = None,
    arm: str = "subscribe",
) -> _Child:
    """Spawn one bench driver subprocess for ``framework``.

    Argv mirrors ``scripts.stress._controller._spawn_real_driver`` so the
    same per-driver bodies in ``scripts.stress._real_drivers`` handle
    the bench's spawns; the bench tests against the same driver code
    paths the stress controller does.

    ``python3 -u`` (``-u`` flag) is added so the driver's stdout is
    unbuffered — the bench reads each driver's stdout line-by-line via
    ``communicate``; without ``-u`` the driver's wake-marker line can
    stall in stdio buffers past the bench's per-iteration deadline.

    ``since`` is the waitbus replay cursor (ULID event_id) appended to the
    driver argv as ``--since <event_id>`` when non-None; absent (default)
    leaves the driver to subscribe from the live watermark.
    """
    # Bound the cache-busting prefix before it lands in argv. In
    # production it is ``force_cold_cache_prefix`` output (~200 chars),
    # but a single argv string longer than the kernel's MAX_ARG_STRLEN
    # (~128 KiB on Linux) makes the spawn fail with E2BIG. Cap it well
    # below that floor at the factory boundary so a future caller
    # threading an unbounded value fails fast here with a clear message
    # rather than deep inside ``Popen``.
    if len(cold_prefix) >= _MAX_COLD_PREFIX_LEN:
        raise ValueError(
            f"cold_prefix is {len(cold_prefix)} chars; cap is "
            f"{_MAX_COLD_PREFIX_LEN} to stay under the argv length floor"
        )
    # The driver subcommand IS the framework name (the entry-point
    # dispatch table in _real_drivers keys directly on it), so the
    # framework doubles as the role positional with no lookup table.
    argv = [
        python_exe,
        "-u",
        "-m",
        "scripts.stress._real_drivers",
        framework,
        "--socket",
        str(socket_path),
        "--db",
        str(db_path),
        "--doorbell",
        str(doorbell_path),
        "--seed-scope-id",
        seed_scope_id,
        "--fw-id",
        fw_id,
        "--cold-prefix",
        cold_prefix,
    ]
    if since is not None:
        argv.extend(["--since", since])
    argv.extend(["--arm", arm])
    stderr_path = stderr_dir / f"bench-driver-{framework}-{fw_id}.err"
    stderr_fh = stderr_path.open("wb")
    try:
        proc: subprocess.Popen[bytes] = subprocess.Popen(
            argv,
            env=env,
            stdout=subprocess.PIPE,
            stderr=stderr_fh,
            start_new_session=True,
        )
    finally:
        stderr_fh.close()
    structured(
        _logger,
        logging.INFO,
        "bench_swarm_driver_spawned",
        framework=framework,
        fw_id=fw_id,
        pid=proc.pid,
        openai_key_in_env="OPENAI_API_KEY" in env,
    )
    return _Child(role=f"{framework}-{fw_id}", proc=proc, framework=framework)


def spawn_n_heterogeneous(
    swarm_spec: dict[str, int],
    *,
    base_env: dict[str, str],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    python_exe: str,
    stderr_dir: Path,
    openai_api_key: str | None,
    cold_prefix: str = "",
    since: str | None = None,
    arm: str = "subscribe",
) -> list[_Child]:
    """Spawn the requested heterogeneous swarm for one bench arm.

    Args:
        swarm_spec: ``{framework_name: count}`` — the per-framework
            count for the arm. Unknown framework names raise
            ``ValueError`` so a typo fails fast. Counts of 0 are silently
            skipped.
        base_env: The orchestrator's env dict. The factory copies + mutates
            it per driver (forces ``PYTHONHASHSEED`` etc., adds or strips
            ``OPENAI_API_KEY``).
        socket_path / db_path / doorbell_path: Daemon-side endpoints the
            drivers connect to. Same paths the stress controller uses.
        seed_scope_id: Per-window scope id; rides the existing
            ``agent_message`` + ``owner=<scope>`` contract so the bench's
            subscribers never wake on a stray event from another session.
        python_exe: Absolute path to the Python interpreter the drivers
            spawn under. The bench's caller passes ``sys.executable``;
            this parameter exists so a future bench can swap interpreters
            without changing the factory's body.
        stderr_dir: Directory under which each driver's stderr file
            lands. The bench's caller is responsible for creating it.
        openai_api_key: The keyring-sourced OpenAI API key. Passed via
            env to OpenAI-needing drivers only.

    Returns:
        List of ``_Child`` handles in spawn order, one per driver in
        ``swarm_spec``. The caller is responsible for teardown.

    Raises:
        ValueError: if ``swarm_spec`` contains an unknown framework name.
    """
    unknown = set(swarm_spec) - set(FRAMEWORK_ORDER)
    if unknown:
        raise ValueError(
            f"spawn_n_heterogeneous: unknown framework(s) in swarm_spec: {sorted(unknown)}; "
            f"valid frameworks are {list(FRAMEWORK_ORDER)}"
        )
    children: list[_Child] = []
    counter = 0
    # Iterate FRAMEWORK_ORDER (not swarm_spec dict order) so the spawn
    # sequence is deterministic across calls with the same spec.
    for framework in FRAMEWORK_ORDER:
        count = int(swarm_spec.get(framework, 0))
        for _ in range(count):
            counter += 1
            fw_id = str(counter)  # bare ordinal; see _Child.framework for why no prefix
            env = _build_env_for_framework(
                framework=framework,
                base_env=base_env,
                openai_api_key=openai_api_key,
            )
            children.append(
                _spawn_bench_driver(
                    framework=framework,
                    fw_id=fw_id,
                    socket_path=socket_path,
                    db_path=db_path,
                    doorbell_path=doorbell_path,
                    seed_scope_id=seed_scope_id,
                    env=env,
                    python_exe=python_exe,
                    stderr_dir=stderr_dir,
                    cold_prefix=cold_prefix,
                    since=since,
                    arm=arm,
                )
            )
    return children


def default_python_executable() -> str:
    """Return the absolute path of the Python interpreter the bench runs under.

    The bench's caller is expected to use this default unless an
    intentional cross-interpreter run is in flight. Wrapping the
    ``sys.executable`` access in a helper keeps the bench's swarm-spec
    callers from importing ``sys`` directly for nothing else.
    """
    return os.fspath(sys.executable)
