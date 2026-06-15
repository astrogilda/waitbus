"""Preflight assertions for the heterogeneous-swarm bench.

A single entry point ``run_preflight_assertions`` runs the full
preflight list before any iteration loop starts. Each assertion that
fires raises a descriptive ``PreflightError`` so the operator's run
aborts before any token is spent.

The asserted invariants land in four classes:

1. **Host gates.** Linux platform, monotonic clock stable, OPENAI key
   readable from keyring.
2. **CLI gates.** claude/gemini/waitbus on PATH; no ``--temperature`` or
   ``--seed`` flags (sampling is black-box; recording the absence in
   the verdict is mandatory).
3. **Library gates.** Installed versions of the bench-internal closure
   match the bands recorded in ``benchmarks/CANONICAL_VERSIONS.toml``.
4. **Substrate-isolation gates.** CPU governor pinned to ``performance``
   and at least two cores available for the orchestrator/daemon split,
   exposed via ``assert_cpu_isolation_for_baselines``. This gate fires
   only when ``--include-real-llm`` is set; smoke / offline runs skip
   it because their CPU measurements are not load-bearing.

Successful return is an ``ExternalStateReport`` (the same struct the
verdict carries) with ``openai_key_present`` reflecting the keyring
probe. Callers MUST persist this report — every subsequent bench
iteration appends to its per-iteration lists.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from typing import Final

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from benchmarks._bench_shared import (
    ExternalStateReport,
    capture_external_state,
)
from benchmarks._harness import EnvironmentReport
from waitbus._log import structured

_logger = logging.getLogger("waitbus.bench.preflight")

# Path to the bench's canonical-versions file (sibling to this module).
CANONICAL_VERSIONS_PATH = Path(__file__).parent / "CANONICAL_VERSIONS.toml"

# Keyring lookup label for the OpenAI API key. Mirrors the standardised
# GNOME-keyring credential format used across the project.
KEYRING_SERVICE = "openai"
KEYRING_ACCOUNT = "api-key"

# Longer-window monotonic-clock-drift budget for the preflight gate.
# Named ``MONOTONIC_DRIFT`` (not a bare ``CLOCK``) so an operator reading
# a triage log never confuses it with the pilot's shell-control
# scheduler-jitter sigma budget (``_PILOT_SIGMA_*`` in
# ``bench_polling_vs_subscribe_llm_agent``): the two gate completely
# different signals (clock-source advance rate here vs cross-iteration
# scheduler jitter there) and an operator must be able to tell from the
# name alone which one a failure refers to. The per-call helper in
# ``_bench_shared`` uses a 50ms window with a permissive budget; the
# preflight here uses a 500ms window so scheduling jitter averages out
# and the bench fails fast on a genuinely unstable clock. 25,000 ppm
# (2.5%) is the empirical headroom for a busy dev-box scheduler with
# CPU governor active and other processes contending; the gate exists
# to catch clock-source disasters (kernel falling back to jiffies, NTP
# pegging the clock backwards) rather than to characterise scheduling
# latency. The bench's per-iteration latency measurements use the
# monotonic source directly; the only invariant this gate enforces is
# "monotonic_ns is advancing in the right direction at roughly the
# right rate."
PREFLIGHT_MONOTONIC_DRIFT_PPM_BUDGET = 25_000.0
PREFLIGHT_CLOCK_SAMPLE_WINDOW_SEC = 0.5

_SECRET_TOOL_TIMEOUT_SEC: Final[float] = 5.0
"""Maximum seconds to wait for the ``secret-tool lookup`` subprocess in
:func:`read_openai_key_from_keyring`.  The keyring lookup is a local IPC
call to the GNOME Secrets daemon; five seconds is more than enough for a
responsive daemon and avoids an indefinite hang when the daemon is locked or
unresponsive at bench startup."""

_CLI_HELP_PROBE_TIMEOUT_SEC: Final[float] = 30.0
"""Maximum seconds to wait for ``<cli> --help`` in
:func:`_check_cli_no_temperature_or_seed`.  The gemini CLI cold-start
measured at ~16 s on a warm host (Node.js startup + MCP extension/skill
discovery scans before the help text prints); a 30 s ceiling absorbs that
without masking a genuine hang.  The claude CLI is faster, but the same
ceiling does no harm and keeps the probe behaviour symmetric across both
CLIs."""


class PreflightError(RuntimeError):
    """Raised when any preflight assertion fails.

    Every raise includes the offending probe and a one-line
    suggested-fix; the bench's preflight policy is "abort early so the
    operator does not burn tokens against a partially-configured host."
    """


# Minimum CPU count for the orchestrator/daemon split. The bench
# pins the orchestrator to the first half of the available cores
# and the daemon to the second half; a host with fewer than two
# logical CPUs cannot enforce the recipe.
_MIN_CORES_FOR_BASELINE = 2


def compute_orchestrator_and_daemon_cores() -> tuple[set[int], set[int]]:
    """Return the (orchestrator, daemon) CPU-core sets for the half-half pin.

    The orchestrator (and the in-proc workload threads it hosts) take
    the FIRST half of the available cores; the daemon takes the SECOND
    half. This guarantees daemon CPU samples are not contaminated by
    orchestrator + LLM-CLI subprocess co-tenancy, which would
    otherwise invalidate sub-millisecond schedstat comparisons.

    Returns: ``(orchestrator_cores, daemon_cores)`` where each is a
    set of integer CPU indices. The caller passes ``orchestrator_cores``
    to ``os.sched_setaffinity(0, ...)`` at bench startup and uses
    ``daemon_cores`` as the daemon Popen's ``preexec_fn`` pin.

    Raises ``PreflightError`` when the host has fewer than two cores
    or the bench is running on a platform without ``sched_setaffinity``.
    """
    if not hasattr(os, "sched_setaffinity") or not sys.platform.startswith("linux"):
        raise PreflightError(
            "preflight: CPU-affinity pin requires Linux with os.sched_setaffinity; "
            f"sys.platform={sys.platform!r}. Pass --allow-unpinned-for-dev to bypass "
            "for development runs (results will NOT be publishable baselines)."
        )
    cpu_count = os.cpu_count() or 0
    if cpu_count < _MIN_CORES_FOR_BASELINE:
        raise PreflightError(
            f"preflight: host has cpu_count={cpu_count}, need at least "
            f"{_MIN_CORES_FOR_BASELINE} cores for the orchestrator/daemon affinity "
            "split; the bench cannot enforce its substrate-isolation contract. "
            "Pass --allow-unpinned-for-dev to bypass for development runs."
        )
    midpoint = cpu_count // 2
    orchestrator_cores = set(range(midpoint))
    daemon_cores = set(range(midpoint, cpu_count))
    return orchestrator_cores, daemon_cores


def assert_cpu_isolation_for_baselines(
    env_report: EnvironmentReport,
    *,
    include_real_llm: bool,
    allow_unpinned: bool,
) -> None:
    """Promote governor + affinity warnings to fatal errors for baseline runs.

    The bench's substrate-isolation contract is documented in
    ``BENCHMARKING.md``: ``cpupower -g performance`` + half-half CPU
    affinity split + Linux. Without these, sub-millisecond schedstat
    comparisons are contaminated by frequency scaling and cross-core
    daemon migration; the verdict's "does not perturb" claim is then
    structurally unfalsifiable.

    The gate fires ONLY when the operator opted into the real-LLM
    path (``--include-real-llm``). Smoke / offline runs skip it
    because they exercise the bench's shape, not its CPU verdict.

    ``allow_unpinned=True`` (set via the bench's
    ``--allow-unpinned-for-dev`` CLI escape) prints a banner and
    proceeds; the resulting verdict is not a publishable baseline.
    """
    if not include_real_llm:
        return
    if allow_unpinned:
        print("=" * 78, file=sys.stderr)
        print(
            "BENCH WARNING: --allow-unpinned-for-dev is set. The CPU governor + "
            "affinity gate is bypassed; the resulting verdict is NOT a publishable "
            "baseline. Use only for development iteration.",
            file=sys.stderr,
        )
        print("=" * 78, file=sys.stderr)
        return
    if env_report.cpu_governor is not None and env_report.cpu_governor != "performance":
        raise PreflightError(
            f"preflight: cpu governor is {env_report.cpu_governor!r}, not 'performance'. "
            "Sub-millisecond schedstat comparisons are confounded by frequency "
            "scaling; set via `sudo cpupower frequency-set -g performance` or pass "
            "--allow-unpinned-for-dev for a dev-only run."
        )
    if not sys.platform.startswith("linux") or not hasattr(os, "sched_setaffinity"):
        raise PreflightError(
            "preflight: CPU-affinity pin requires Linux with os.sched_setaffinity; "
            f"sys.platform={sys.platform!r}. Pass --allow-unpinned-for-dev to bypass."
        )
    cpu_count = os.cpu_count() or 0
    if cpu_count < _MIN_CORES_FOR_BASELINE:
        raise PreflightError(
            f"preflight: host has cpu_count={cpu_count}, need at least "
            f"{_MIN_CORES_FOR_BASELINE} cores for the orchestrator/daemon affinity "
            "split. Pass --allow-unpinned-for-dev for a dev-only run."
        )


# De-underscored deliberately: this is the bench suite's shared keyring-read
# entry point, imported by name from the sibling bench modules
# (bench_polling_vs_subscribe_llm_agent / bench_multistream_proof /
# bench_event_delivery_fidelity) and pinned by its public dotted path as
# a ``unittest.mock.patch`` target in their tests. It is a cross-bench-reuse
# helper, not a module-private detail, so it keeps the public spelling.
def read_openai_key_from_keyring() -> str | None:
    """Resolve the OpenAI API key, preferring local keyring then ``OPENAI_API_KEY``.

    Order:

    1. ``secret-tool lookup service openai account api-key`` (operator's
       GNOME-keyring entry on a local workstation; the canonical path).
    2. ``OPENAI_API_KEY`` environment variable. Required for remote
       headless hosts (e.g. the Hetzner publishable-baseline VM) where
       no GNOME-keyring daemon runs and the operator ships the key via
       env. The wrapper script reads from local keyring on the
       operator's box and re-exports as env on the remote.

    Returns the key string on success, ``None`` if neither source has
    one. The bench NEVER persists this value — the returned string is
    passed to the real-LLM driver subprocesses via their environment.
    """
    import os

    secret_tool = shutil.which("secret-tool")
    if secret_tool is not None:
        try:
            result = subprocess.run(
                [secret_tool, "lookup", "service", KEYRING_SERVICE, "account", KEYRING_ACCOUNT],
                capture_output=True,
                text=True,
                timeout=_SECRET_TOOL_TIMEOUT_SEC,
                check=False,
            )
        except subprocess.TimeoutExpired:
            structured(
                _logger,
                logging.WARNING,
                "bench_preflight_keyring_lookup_timeout",
            )
        else:
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
            # Non-zero exit (or empty stdout) is NOT the same as "key
            # absent": a dismissed unlock prompt, a locked keyring, or a
            # missing collection all surface here with a stderr message.
            # Capture it so the operator can distinguish a real lookup
            # failure from a genuinely empty keyring before the bench
            # falls through to the env-var path.
            structured(
                _logger,
                logging.WARNING,
                "bench_preflight_keyring_lookup_failed",
                returncode=result.returncode,
                stderr=result.stderr.strip(),
            )
    else:
        structured(
            _logger,
            logging.WARNING,
            "bench_preflight_secret_tool_missing",
        )
    env_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if env_key:
        structured(
            _logger,
            logging.INFO,
            "bench_preflight_openai_key_from_env",
        )
        return env_key
    return None


def _load_canonical_versions() -> dict[str, dict[str, str]]:
    """Read ``CANONICAL_VERSIONS.toml`` into a name -> {band, observed} map."""
    if not CANONICAL_VERSIONS_PATH.is_file():
        raise PreflightError(
            f"CANONICAL_VERSIONS.toml not found at {CANONICAL_VERSIONS_PATH!s}; "
            "the bench preflight requires the canonical-versions pin file"
        )
    with CANONICAL_VERSIONS_PATH.open("rb") as fh:
        loaded = tomllib.load(fh)
    packages = loaded.get("packages", {})
    if not isinstance(packages, dict):
        raise PreflightError("CANONICAL_VERSIONS.toml [packages] section missing or malformed")
    out: dict[str, dict[str, str]] = {}
    for name, entry in packages.items():
        if not isinstance(entry, dict) or "band" not in entry or "observed" not in entry:
            raise PreflightError(f"CANONICAL_VERSIONS.toml [packages.{name}] missing band/observed")
        out[name] = {"band": str(entry["band"]), "observed": str(entry["observed"])}
    return out


def _version_in_band(version: str, band: str) -> bool:
    """Return True iff ``version`` satisfies the PEP 440 version band.

    ``band`` is any PEP 440 specifier set -- one or more comma-joined
    clauses each using ``>=`` / ``>`` / ``<=`` / ``<`` / ``==`` / ``!=``
    (e.g. ``">=1.104,<2.0"``). Parsing is delegated to
    ``packaging.specifiers.SpecifierSet`` so every PEP 440 operator,
    operator combination, and pre-release / dev / post suffix is handled
    by the canonical implementation rather than a hand-rolled string
    slicer. ``prereleases=True`` is passed so a pre-release that lands
    inside the band (``1.2.3rc1`` in ``>=1.2,<2.0``) is accepted -- the
    bench gates on the major.minor band, not on release-channel status.

    A reversed band (``">=2.0,<1.0"``) is a satisfiable-by-nothing empty
    set and returns ``False`` rather than raising; the gate's job is to
    answer "is this version in band", and an empty band admits no
    version. A band that is not a valid specifier, or a ``version`` that
    is not a valid PEP 440 version, raises ``PreflightError`` so a
    malformed canonical-versions entry fails the bench fast rather than
    silently mis-gating.
    """
    try:
        spec = SpecifierSet(band.replace(" ", ""))
    except InvalidSpecifier as exc:
        raise PreflightError(f"malformed canonical-versions band {band!r}: {exc}") from exc
    try:
        parsed = Version(version)
    except InvalidVersion as exc:
        raise PreflightError(f"unparseable installed version {version!r}: {exc}") from exc
    return spec.contains(parsed, prereleases=True)


def _check_cli_no_temperature_or_seed(binary: str) -> None:
    """Assert ``<binary> --help`` advertises no ``--temperature`` / ``--seed`` flag.

    The bench's sampling-is-black-box claim depends on neither CLI
    accepting these flags. If a future CLI version adds them, the
    preflight catches the change and the bench's verdict text must
    update before the next run.
    """
    path = shutil.which(binary)
    if path is None:
        raise PreflightError(f"preflight: {binary!r} CLI not found on PATH")
    try:
        # The gemini CLI's --help cold-start measured at ~16 s on a
        # warm host (Node.js startup + extension/skill discovery scans
        # before the help text prints); a 30 s ceiling absorbs that
        # without masking a genuine hang. The claude CLI is faster but
        # the same ceiling does no harm.
        result = subprocess.run(
            [path, "--help"],
            capture_output=True,
            text=True,
            timeout=_CLI_HELP_PROBE_TIMEOUT_SEC,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise PreflightError(f"preflight: {binary!r} --help timed out") from exc
    combined = (result.stdout or "") + "\n" + (result.stderr or "")
    if re.search(r"--temperature\b", combined):
        raise PreflightError(
            f"preflight: {binary!r} --help advertises --temperature; "
            "the bench's black-box-sampling claim is invalidated"
        )
    if re.search(r"--seed\b", combined):
        raise PreflightError(
            f"preflight: {binary!r} --help advertises --seed; the bench's black-box-sampling claim is invalidated"
        )


def _check_path_binaries(*binaries: str) -> None:
    """Assert every requested binary resolves on PATH."""
    for binary in binaries:
        if shutil.which(binary) is None:
            raise PreflightError(f"preflight: {binary!r} not on PATH; install or expose it before re-running the bench")


def _check_canonical_versions(report: ExternalStateReport) -> None:
    """Assert every installed library matches the canonical-versions band.

    The probe maps each canonical-versions package to the corresponding
    ``ExternalStateReport`` field; a ``None`` field means the library
    was probed-missing and the bench aborts (the library is required by
    the bench's per-driver code paths).
    """
    pins = _load_canonical_versions()
    field_by_package: dict[str, str | None] = {
        "pydantic-ai-slim": report.pydantic_ai_version,
        "langgraph": report.langgraph_version,
        "langchain-core": report.langchain_core_version,
        "langchain-openai": report.langchain_openai_version,
        "openai": report.openai_sdk_version,
        "tiktoken": report.tiktoken_version,
        "msgspec": report.msgspec_version,
        "hdrhistogram": report.hdrhistogram_version,
    }
    for name, pin in pins.items():
        installed = field_by_package.get(name)
        if installed is None:
            raise PreflightError(
                f"preflight: required library {name!r} not installed "
                f"(canonical band {pin['band']}; observed at pin time {pin['observed']})"
            )
        if not _version_in_band(installed, pin["band"]):
            raise PreflightError(
                f"preflight: {name!r} version {installed!r} outside canonical band {pin['band']!r}; "
                "update CANONICAL_VERSIONS.toml together with a bench re-verification"
            )


def run_preflight_assertions(
    bench_name: str,
    *,
    require_openai: bool = True,
    require_claude_cli: bool = True,
    require_gemini_cli: bool = True,
) -> ExternalStateReport:
    """Run every preflight assertion and return the captured ExternalStateReport.

    The returned report carries the openai-key-presence bool but never
    the key value; the caller propagates the value separately to the
    swarm-spawn factory's per-driver env dict.

    Args:
        bench_name: Free-form label logged in every structured event so a
            multi-bench run can correlate preflight failures back to the
            specific experiment.
        require_openai: When True (default), abort if the keyring lookup
            for ``service openai account api-key`` returns empty. The
            shell-control-only bench arm sets this False.
        require_claude_cli: When True (default), abort if the ``claude``
            CLI is not on PATH or advertises ``--temperature`` / ``--seed``.
        require_gemini_cli: When True (default), abort if the ``gemini``
            CLI is not on PATH or advertises ``--temperature`` / ``--seed``.

    Returns:
        ExternalStateReport with empty per-iteration accumulators.

    Raises:
        PreflightError: on any failed assertion. The error message
        describes the failing probe and a suggested fix.
    """
    structured(
        _logger,
        logging.INFO,
        "bench_preflight_start",
        bench=bench_name,
    )
    # 1. Host gates.
    if not sys.platform.startswith("linux"):
        raise PreflightError(
            f"preflight: bench requires Linux for cross-process monotonic_ns; sys.platform={sys.platform!r}"
        )
    # In-line longer-window clock probe. The 50ms helper in
    # ``_bench_shared`` is too short to absorb normal scheduler jitter;
    # this preflight probe uses the documented longer window so the
    # fail-fast gate is calibrated for an idle Linux host.
    t0 = time.monotonic_ns()
    time.sleep(PREFLIGHT_CLOCK_SAMPLE_WINDOW_SEC)
    t1 = time.monotonic_ns()
    if t1 <= t0:
        raise PreflightError(f"preflight: monotonic_ns not advancing: t0={t0} t1={t1}")
    expected_ns = int(PREFLIGHT_CLOCK_SAMPLE_WINDOW_SEC * 1_000_000_000)
    drift_ppm = abs((t1 - t0) - expected_ns) / expected_ns * 1_000_000.0
    if drift_ppm > PREFLIGHT_MONOTONIC_DRIFT_PPM_BUDGET:
        raise PreflightError(
            f"preflight: monotonic-clock-drift gate tripped: monotonic_ns drift "
            f"{drift_ppm:.1f} ppm exceeds PREFLIGHT_MONOTONIC_DRIFT_PPM_BUDGET "
            f"{PREFLIGHT_MONOTONIC_DRIFT_PPM_BUDGET:.1f} ppm over "
            f"{PREFLIGHT_CLOCK_SAMPLE_WINDOW_SEC * 1000:.0f}ms sample; host clock is "
            "unstable, re-check NTP / scheduler / CPU governor"
        )

    openai_key = read_openai_key_from_keyring() if require_openai else None
    if require_openai and not openai_key:
        raise PreflightError(
            "preflight: OPENAI_API_KEY not in keyring; "
            f"run `secret-tool store --label='OpenAI API Key' service {KEYRING_SERVICE} "
            f"account {KEYRING_ACCOUNT}` and re-run the bench"
        )

    # 2. CLI gates.
    required_path_binaries: list[str] = ["waitbus"]
    if require_claude_cli:
        required_path_binaries.append("claude")
    if require_gemini_cli:
        required_path_binaries.append("gemini")
    _check_path_binaries(*required_path_binaries)
    if require_claude_cli:
        _check_cli_no_temperature_or_seed("claude")
    if require_gemini_cli:
        _check_cli_no_temperature_or_seed("gemini")

    # 3. External-state capture + library version gate.
    report = capture_external_state(openai_api_key_present=openai_key is not None)
    _check_canonical_versions(report)

    structured(
        _logger,
        logging.INFO,
        "bench_preflight_passed",
        bench=bench_name,
    )
    return report
