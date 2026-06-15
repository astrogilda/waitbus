"""Real-mode heterogeneous driver entry points and orchestration helpers.

This module supplies the per-framework driver subprocesses the
``--real`` controller spawns to prove orchestrator-observed
cross-broadcast at N>=5: every spawned driver subscribes to the
orchestrator's seed event on one local bus, reacts (real LLM call
for the ``claude-cli`` / ``gemini-cli`` roles; offline ``TestModel``
/ ``FakeListChatModel`` for the in-proc framework roles; bash echo
for ``shell-control``), and emits one ``agent_message`` reaction
event the orchestrator collects upstream.

Five driver roles, equal-mix split:

- ``pydantic``      -- a real ``pydantic_ai.Agent`` (``TestModel``).
- ``langgraph``     -- a real ``langgraph.graph.StateGraph``
                       (``FakeListChatModel``).
- ``claude-cli``    -- spawns ``claude -p ... --output-format=json``
                       so the verdict carries real token + cost
                       envelope fields.
- ``gemini-cli``    -- spawns ``gemini -p ... -o json`` so the
                       verdict carries real token-usage fields
                       (Gemini's free-tier envelope has no cost
                       field; ``cost_usd`` is reported as 0.0).
- ``shell-control`` -- a bash echo loop as the synthetic-control
                       baseline (no LLM, no real model dependency).

Each role subprocess prints one wake-marker line on stdout when it
observably wakes on the seed event; the orchestrator parses those
lines plus (for LLM roles) the embedded token envelope and rolls
them up into a ``RealCurvePoint``.

Lifts the supervision + delivery-id-mint patterns from
``examples.hero_swarm.orchestrate`` (the canonical N=2 cross-harness
proof) without modifying that file: ``_Child`` lives in
``_controller``; this module provides the role bodies and the
framework-mix factory the controller calls into.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Final, TypedDict

import msgspec

from benchmarks._bench_anchor import SEED_EVENT_TYPE, SEED_SOURCE
from scripts.stress._context import TokenUsage, envelope_is_refusal, openai_tokens_to_usd, scan_balanced_json
from waitbus import wait_for
from waitbus._emit import emit
from waitbus._log import structured
from waitbus._types import EventInsert

_logger = logging.getLogger("waitbus.stress.real_drivers")

# Model id used when the real-OpenAI path is engaged.
REAL_OPENAI_MODEL_ID = "gpt-4.1-nano"
# Deterministic-sampling kwargs passed to the OpenAI SDK for the
# real-OpenAI driver paths. The Chat Completions API documents
# ``seed`` as a best-effort determinism hint (responses with the same
# (model, seed, temperature, prompt) generally match modulo
# server-side fingerprint drift); ``temperature=0`` selects the
# highest-probability token at every step. Together they bound the
# bench's network-side non-determinism to OpenAI's published
# ``system_fingerprint`` rotation cadence -- the verdict reader can
# audit which fingerprint produced each row.
LLM_REAL_TEMPERATURE = 0.0
LLM_REAL_SEED = 0xC1B5
# Provider identifiers stamped onto the wake-marker line. Lets a
# downstream verdict see which model path actually ran on a per-driver
# basis without changing any struct shape.
PROVIDER_OFFLINE_PYDANTIC = "offline-testmodel"
PROVIDER_OFFLINE_LANGGRAPH = "offline-fakelistchatmodel"
PROVIDER_OFFLINE_OTHER = "offline"
PROVIDER_OPENAI_GPT_4_1_NANO = "openai-gpt-4.1-nano"
PROVIDER_CLAUDE_CLI = "claude-cli"
PROVIDER_GEMINI_CLI = "gemini-cli"

# Minimum length for a shape-valid ``OPENAI_API_KEY`` (the ``sk-`` prefix plus
# a handful of chars). A non-empty value at or below this length, or without
# the prefix, is treated as absent and routes to the offline fallback.
_MIN_OPENAI_KEY_LEN = 20

# Env var the controller / bench sets to "1" to signal the driver subprocess
# is running under REAL mode (``waitbus stress --real`` / Bench A
# ``--include-real-llm``). The driver subprocess has no other in-band signal of
# the parent's mode -- its argv carries no ``--real`` flag and it inherits only
# the parent's environment -- so this env var is the single mode signal the
# model selectors gate on. Under real mode an absent / shape-invalid
# ``OPENAI_API_KEY`` is a hard failure, NOT a silent downgrade to an offline
# fake; absent the flag (offline / smoke / unit paths) the offline fakes remain
# the legitimate absent-key fallback.
REAL_MODE_ENV_VAR: Final[str] = "WAITBUS_STRESS_REAL_MODE"

# The driver frameworks whose real path is an OpenAI-backed model
# (``OpenAIModel`` / ``ChatOpenAI``). These are the ONLY frameworks whose
# real-mode operation requires ``OPENAI_API_KEY``; the claude-cli / gemini-cli
# roles authenticate via their own CLI session and the shell-control role makes
# no LLM call at all, so a spec containing only those roles does not require an
# OpenAI key.
OPENAI_DRIVER_FRAMEWORKS: Final[frozenset[str]] = frozenset({"pydantic", "langgraph"})


def _real_mode_active() -> bool:
    """Return True iff the driver subprocess is running under REAL mode.

    Reads ``REAL_MODE_ENV_VAR`` from the environment the controller / bench
    threaded in. The flag is the driver's only signal of the parent's mode;
    when set, the model selectors hard-fail on an absent / shape-invalid
    ``OPENAI_API_KEY`` instead of silently substituting an offline fake.
    """
    return os.environ.get(REAL_MODE_ENV_VAR, "") == "1"


def _spec_requires_openai_key(frameworks: Iterable[str]) -> bool:
    """Return True iff ``frameworks`` includes any OpenAI-backed driver role.

    ``waitbus stress --real`` / Bench A only require ``OPENAI_API_KEY`` when the
    realized framework mix contains the pydantic and/or langgraph role; a spec
    of only claude-cli / gemini-cli / shell-control roles authenticates through
    the CLIs (or makes no LLM call) and does not need the key.
    """
    return any(fw in OPENAI_DRIVER_FRAMEWORKS for fw in frameworks)


def _openai_key_present() -> bool:
    """Return True iff a shape-valid ``OPENAI_API_KEY`` is in the driver's env.

    Single unified driver behaviour: drivers go real OpenAI whenever an
    operator-supplied key is reachable; offline fakes are the absent-key
    fallback. No internal toggle, no opt-in flag.

    "Present" means shape-valid, not merely non-empty: the value must
    start with the ``sk-`` prefix every OpenAI key carries and be longer
    than the prefix-plus-a-handful-of-chars floor. A truncated or
    mistyped value fails this check and routes to the offline fallback
    rather than reaching the live endpoint and burning budget on a 401.
    """
    key = os.environ.get("OPENAI_API_KEY") or ""
    present = len(key) > _MIN_OPENAI_KEY_LEN and key.startswith("sk-")
    if key and not present:
        # A non-empty key that fails the shape check is almost always a
        # truncated or mistyped value. Leave a breadcrumb so the otherwise
        # silent route to the offline fake is debuggable -- without logging
        # the secret itself.
        structured(
            _logger,
            logging.WARNING,
            "openai_key_shape_rejected",
            reason="non-empty OPENAI_API_KEY failed the sk-/length shape check; using offline fallback",
        )
    return present


def _select_pydantic_model(*, real_mode: bool = False) -> tuple[Any, str]:
    """Pick the Pydantic AI model + return its provider id.

    Returns ``(model, provider_id)``. Real path: ``OpenAIModel`` reading
    the API key from env, engaged whenever ``OPENAI_API_KEY`` is present.
    Offline path: ``TestModel``, engaged only as the absent-key fallback
    (informational structured log) or on real-mode SDK import failure
    (structured warning) so a key-less or partial-install host still
    exercises the wiring.

    ``real_mode`` closes the silent-fallback class: when True, an absent /
    shape-invalid ``OPENAI_API_KEY`` RAISES ``RuntimeError`` instead of
    silently returning ``TestModel``. Under ``waitbus stress --real`` /
    Bench A's ``--include-real-llm`` the offline fake must never stand in
    for a real OpenAI call and count toward ``cross_broadcast_proven`` /
    the bench baseline; the offline fake remains the legitimate fallback
    only on the offline / smoke / unit paths (``real_mode=False``).
    """
    if real_mode and not _openai_key_present():
        raise RuntimeError(
            "pydantic driver: real mode requires a shape-valid OPENAI_API_KEY "
            "(sk- prefix, >20 chars) in the environment; none was found. The "
            "pydantic driver is OpenAI-backed and must not silently fall back "
            "to the offline TestModel under --real / --include-real-llm. Set "
            "OPENAI_API_KEY (or remove the pydantic role from the spec) and "
            "re-run."
        )
    if _openai_key_present():
        try:
            from pydantic_ai.models.openai import OpenAIModel

            return OpenAIModel(REAL_OPENAI_MODEL_ID), PROVIDER_OPENAI_GPT_4_1_NANO
        except ImportError as exc:
            structured(
                _logger,
                logging.WARNING,
                "pydantic_driver_openai_unavailable",
                error=str(exc),
            )
    else:
        structured(
            _logger,
            logging.INFO,
            "pydantic_driver_offline_fallback",
            reason="openai_api_key_absent",
        )
    from pydantic_ai.models.test import TestModel

    return TestModel(), PROVIDER_OFFLINE_PYDANTIC


def _select_langgraph_chat_model(*, real_mode: bool = False) -> tuple[Any, str]:
    """Pick the LangGraph chat model + return its provider id.

    Returns ``(chat_model, provider_id)``. Real path: ``ChatOpenAI``
    reading the API key from env, engaged whenever ``OPENAI_API_KEY`` is
    present. Offline path: ``FakeListChatModel``, engaged only as the
    absent-key fallback (informational structured log) or on real-mode
    SDK import failure (structured warning).

    ``real_mode`` closes the silent-fallback class: when True, an absent /
    shape-invalid ``OPENAI_API_KEY`` RAISES ``RuntimeError`` instead of
    silently returning ``FakeListChatModel``. See ``_select_pydantic_model``
    for the full rationale; the offline fake remains the legitimate fallback
    only on the offline / smoke / unit paths (``real_mode=False``).
    """
    if real_mode and not _openai_key_present():
        raise RuntimeError(
            "langgraph driver: real mode requires a shape-valid OPENAI_API_KEY "
            "(sk- prefix, >20 chars) in the environment; none was found. The "
            "langgraph driver is OpenAI-backed and must not silently fall back "
            "to the offline FakeListChatModel under --real / --include-real-llm. "
            "Set OPENAI_API_KEY (or remove the langgraph role from the spec) and "
            "re-run."
        )
    if _openai_key_present():
        try:
            from langchain_openai import ChatOpenAI

            return (
                ChatOpenAI(
                    model=REAL_OPENAI_MODEL_ID,
                    temperature=LLM_REAL_TEMPERATURE,
                    model_kwargs={"seed": LLM_REAL_SEED},
                ),
                PROVIDER_OPENAI_GPT_4_1_NANO,
            )
        except ImportError as exc:
            structured(
                _logger,
                logging.WARNING,
                "langgraph_driver_openai_unavailable",
                error=str(exc),
            )
    else:
        structured(
            _logger,
            logging.INFO,
            "langgraph_driver_offline_fallback",
            reason="openai_api_key_absent",
        )
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    return FakeListChatModel(responses=["Reacted to stress seed."]), PROVIDER_OFFLINE_LANGGRAPH


# The seed + reaction events both ride the existing `agent_message`
# event type (registered in waitbus.sources._registry's built-in
# `agent` source). Per-run isolation comes from the `owner` field --
# the orchestrator mints a per-window scope id (a uuid prefix) and
# threads it through every driver as the predicate's owner clause, so
# a stray subscriber on the operator's machine cannot wake a driver
# by accident. Using `agent_message` (rather than a fresh per-run
# event_type) is structurally required: the daemon's `_fan_out` skips
# any frame whose event_type is not in `event_types_supported()` (see
# `broadcast.py::_fan_out` and `_validate_subscribe_event_types`), so
# an unregistered seed event_type would simply never reach the bus.
REACTION_EVENT_TYPE = "agent_message"
# Wake marker the orchestrator scans for on each driver's stdout.
WAKE_MARKER = "DRIVER_REACTED"
# Early wake marker emitted immediately after the driver's ``wait_for``
# returns, BEFORE any post-wake LLM exercise. Carries the cross-process
# monotonic timestamps the orchestrator uses to compute bus ingest
# latency and the live-vs-replay delivery-mode classification free of
# LLM-call jitter contamination.
EARLY_WAKE_MARKER = "WAKE_RECEIVED"

# Driver framework names in priority order for the round-robin split.
FRAMEWORK_ORDER: tuple[str, ...] = (
    "pydantic",
    "langgraph",
    "claude-cli",
    "gemini-cli",
    "shell-control",
)
# Driver-side timeouts. Generous enough to absorb a slow LLM call but
# bounded so a hung driver cannot pin the orchestrator's window.
DRIVER_WAIT_TIMEOUT_SEC = 90.0
LLM_CALL_TIMEOUT_SEC = 60.0
AUTH_SMOKE_TIMEOUT_SEC = 30.0

# Timeout in seconds for the ``/bin/bash -c "echo ..."`` subprocess the
# shell-control driver spawns to prove cross-broadcast without an LLM call.
# 5 s is deliberately short -- a bash echo that hangs for more than a few
# hundred milliseconds indicates a system-level fault, not normal latency.
_SHELL_DRIVER_TIMEOUT_SEC: Final[float] = 5.0


# ---------------------------------------------------------------------------
# Driver exit codes (CLI-driver failure taxonomy)
# ---------------------------------------------------------------------------
#
# The CLI drivers (claude-cli / gemini-cli) distinguish THREE failure
# classes that the prior ``subprocess.run(check=True)`` +
# ``except (CalledProcessError, TimeoutExpired)`` shape collapsed into a
# single shared exit code. A bench-side consumer reading
# ``_Child.exit_code`` (or ``child_result.exit_code`` in the bench's
# ``_do_workload_iteration``) can now tell a hung LLM call apart from an
# auth / quota / invocation failure apart from a model refusal:
#
#   0  -- success: the CLI returned a parseable envelope and reacted.
#   1  -- seed wait timeout (the driver never observed the seed event;
#         pre-subprocess, the LLM CLI was never invoked).
#   2  -- the CLI binary is not on PATH (pre-subprocess).
#   3  -- LLM-CALL TIMEOUT: the CLI subprocess ran past
#         ``LLM_CALL_TIMEOUT_SEC`` and was killed (``TimeoutExpired``).
#         No envelope is producible; the orchestrator sees no reaction.
#   4  -- AUTH / INVOCATION ERROR: the CLI exited non-zero AND produced
#         no parseable token envelope (auth / quota failure, a crash
#         before any result frame, a torn stdout). No reaction is
#         emitted; only this exit code distinguishes it from a timeout.
#   5  -- NON-CLEAN ENVELOPE (union: refusal / soft is_error / non-zero
#         exit with envelope): the CLI produced a parseable envelope that
#         ``envelope_is_refusal`` flags -- a moderation refusal
#         (``stop_reason="refusal"``), a soft upstream error on exit 0
#         (``is_error=True`` with no refusal marker), or the Anthropic
#         ``terminal_reason="error_during_execution"`` shape -- OR exited
#         non-zero while still emitting a usable result envelope. The soft
#         is_error case is folded into this code on PURPOSE, not split
#         out: the precise sub-kind is carried losslessly in the
#         envelope's own fields (which ride the bus), no runtime consumer
#         reads this exit code (only the contract tests do), and the
#         verdict aggregator groups refusal + soft is_error together via
#         the same discriminator. The driver STILL emits the reaction +
#         wake marker so the orchestrator's envelope path observes the
#         outcome via the ``TokenUsage`` fields; the distinct exit code
#         only lets a hypothetical exit-code reader tell this union class
#         apart from the auth (4) and timeout (3) classes.
EXIT_OK: Final[int] = 0
EXIT_SEED_TIMEOUT: Final[int] = 1
EXIT_NO_CLI: Final[int] = 2
EXIT_LLM_TIMEOUT: Final[int] = 3
EXIT_AUTH_OR_INVOCATION_ERROR: Final[int] = 4
EXIT_REFUSAL_OR_NONZERO_ENVELOPE: Final[int] = 5


# ---------------------------------------------------------------------------
# Auth smoke (fail-fast)
# ---------------------------------------------------------------------------


def auth_smoke_check(frameworks: Iterable[str] | None = None) -> dict[str, str]:
    """Verify ``claude``, ``gemini``, ``waitbus`` (and conditionally ``OPENAI_API_KEY``); report versions.

    Returns ``{"claude": "<version>", "gemini": "<version>",
    "waitbus": "<absolute path>", "openai_api_key": "present"}`` on success
    (the ``openai_api_key`` key is present only when an OpenAI-backed
    driver is in the spec). Raises ``RuntimeError`` on the first failure
    (missing CLI, non-zero exit, unparseable version output, or -- when
    the spec contains the pydantic / langgraph role -- an absent /
    shape-invalid ``OPENAI_API_KEY``) so the orchestrator aborts before
    spawning any drivers -- the fail-fast policy operator-decided on
    2026-06-01.

    ``frameworks`` is the realized framework set the controller is about
    to spawn (e.g. the union of the sweep's per-N mixes). When it contains
    any OpenAI-backed role (pydantic / langgraph) a shape-valid
    ``OPENAI_API_KEY`` is REQUIRED: the prior behaviour checked only the
    CLI binaries on PATH and never the key, so a ``--real`` run with no
    key silently downgraded the pydantic / langgraph drivers to offline
    fakes while still counting them toward ``cross_broadcast_proven``.
    Requiring the key here makes that a loud preflight abort. A spec with
    no OpenAI-backed role (only claude-cli / gemini-cli / shell-control)
    does NOT require the key. ``None`` (default) means "no spec supplied":
    the OpenAI-key requirement does not engage and only the CLI binaries
    are smoke-checked -- this keeps the bare ``auth-smoke`` CLI subcommand
    a pure CLI-version probe. The controller threads the realized spec
    explicitly so the real ``--real`` gate always carries the framework
    set; a caller that wants the OpenAI gate MUST pass ``frameworks``.

    No live LLM call is made here; that would burn tokens just to
    confirm auth. The presence of the CLI binary + its ``--version``
    (and the key's shape, when required) is sufficient.
    """
    required_frameworks: tuple[str, ...] = () if frameworks is None else tuple(frameworks)
    waitbus_path = shutil.which("waitbus")
    if waitbus_path is None:
        raise RuntimeError("auth_smoke_check: 'waitbus' CLI not found in PATH")
    claude_path = shutil.which("claude")
    if claude_path is None:
        raise RuntimeError("auth_smoke_check: 'claude' CLI not found in PATH (real-mode requires it)")
    gemini_path = shutil.which("gemini")
    if gemini_path is None:
        raise RuntimeError("auth_smoke_check: 'gemini' CLI not found in PATH (real-mode requires it)")

    try:
        claude_version_proc = subprocess.run(
            [claude_path, "--version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=AUTH_SMOKE_TIMEOUT_SEC,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"auth_smoke_check: 'claude --version' failed: {exc}") from exc
    claude_version = claude_version_proc.stdout.strip().splitlines()[0] if claude_version_proc.stdout else ""

    try:
        gemini_version_proc = subprocess.run(
            [gemini_path, "--version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=AUTH_SMOKE_TIMEOUT_SEC,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"auth_smoke_check: 'gemini --version' failed: {exc}") from exc
    gemini_version = gemini_version_proc.stdout.strip().splitlines()[0] if gemini_version_proc.stdout else ""

    provenance: dict[str, str] = {
        "claude": claude_version,
        "gemini": gemini_version,
        "waitbus": waitbus_path,
    }

    # OpenAI-backed roles (pydantic / langgraph) need a shape-valid key.
    # Mirror how the claude / gemini checks above conditionally require
    # their CLI on PATH: require the key ONLY when the spec actually
    # spawns an OpenAI-backed driver, naming the var + the drivers that
    # need it so the failure is loud and attributable.
    if _spec_requires_openai_key(required_frameworks):
        if not _openai_key_present():
            needing = sorted(fw for fw in required_frameworks if fw in OPENAI_DRIVER_FRAMEWORKS)
            raise RuntimeError(
                "auth_smoke_check: real-mode spec includes OpenAI-backed "
                f"driver(s) {needing} which require a shape-valid OPENAI_API_KEY "
                "(sk- prefix, >20 chars) in the environment, but none was found. "
                "Set OPENAI_API_KEY before re-running, or run a spec without the "
                "pydantic / langgraph roles. Refusing to silently downgrade those "
                "drivers to offline fakes under --real."
            )
        provenance["openai_api_key"] = "present"

    return provenance


# ---------------------------------------------------------------------------
# Framework-mix split factory
# ---------------------------------------------------------------------------


def framework_mix_for(n: int) -> dict[str, int]:
    """Distribute ``n`` driver slots across the five frameworks.

    Equal-share by default; remainder allocated round-robin starting
    with the first framework in ``FRAMEWORK_ORDER``. The contract:

    - n=5 -> 1 each of the five frameworks
    - n=10 -> 2 each of the five frameworks
    - n=7 -> {pydantic: 2, langgraph: 2, claude-cli: 1, gemini-cli: 1, shell-control: 1}
    - n=3 -> {pydantic: 1, langgraph: 1, claude-cli: 1, gemini-cli: 0, shell-control: 0}

    Operator-tunable in a future commit; the equal-mix default is the
    plan-locked starting point.
    """
    if n < 0:
        raise ValueError(f"framework_mix_for: n must be non-negative, got {n}")
    frameworks = FRAMEWORK_ORDER
    quotient, remainder = divmod(n, len(frameworks))
    mix: dict[str, int] = {fw: quotient for fw in frameworks}
    for index in range(remainder):
        mix[frameworks[index]] += 1
    return mix


# ---------------------------------------------------------------------------
# Token envelope parsers
# ---------------------------------------------------------------------------


def _claude_model_id(obj: dict[str, Any]) -> str:
    """Extract the Claude model id from a result envelope.

    The live ``claude -p --output-format=json`` result envelope does NOT
    carry a top-level ``model`` field. The model id is instead a KEY under
    the ``modelUsage`` map, e.g.
    ``"modelUsage": {"claude-haiku-4-5-20251001": {"inputTokens": ...}}``.
    Prefer that key; fall back to a top-level ``model`` field (older /
    alternate shapes) and finally to ``"unknown"`` so a shape change
    degrades to the prior behaviour rather than raising.

    When several models appear under ``modelUsage`` (a multi-model turn),
    the one with the largest ``inputTokens`` is returned as the dominant
    model; ties / non-dict leaves fall back to first-seen insertion order.
    """
    model_usage = obj.get("modelUsage")
    if isinstance(model_usage, dict) and model_usage:
        best_id: str | None = None
        best_input = -1
        for model_id, leaf in model_usage.items():
            if best_id is None:
                best_id = str(model_id)
            leaf_input = 0
            if isinstance(leaf, dict):
                try:
                    leaf_input = int(leaf.get("inputTokens", 0))
                except (TypeError, ValueError):
                    leaf_input = 0
            if leaf_input > best_input:
                best_input = leaf_input
                best_id = str(model_id)
        if best_id:
            return best_id
    top_level = obj.get("model")
    if top_level:
        return str(top_level)
    return "unknown"


def parse_claude_envelope(stdout: str) -> TokenUsage | None:
    """Parse the ``claude -p --output-format=json`` stdout into a TokenUsage.

    Source-of-truth shape (smoked 2026-06-01):
    ``{"type": "result", ..., "total_cost_usd": <float>,
    "stop_reason": <str>, "is_error": <bool>,
    "terminal_reason": <str>, "num_turns": <int>,
    "usage": {"input_tokens": <int>,
    "cache_creation_input_tokens": <int>,
    "cache_read_input_tokens": <int>, "output_tokens": <int>,
    "service_tier": <str>, ...}}``.

    Captures the Anthropic cache discount fields so
    ``billed_input_tokens`` (= visible + cache_creation + cache_read)
    surfaces in the verdict; on the live operator probe the visible
    figure was 10 tokens while the billed figure was 61019, a 6101x
    understatement before this fix.

    Captures the moderation / error envelope fields so a refusal
    (``stop_reason="refusal"`` + ``is_error=True``) is no longer
    silently a successful zero-cost iteration.

    The model id is read via ``_claude_model_id``: the live envelope
    carries it as a KEY under ``modelUsage`` (e.g.
    ``"claude-haiku-4-5-20251001"``), NOT as a top-level ``model``
    field, so the recorded model id is the concrete Claude model rather
    than ``"unknown"``.

    Returns ``None`` on any parse / shape failure -- callers treat
    missing usage as ``None`` rather than raising, so a transient
    upstream change does not crash the whole real-mode window.
    """
    if not stdout:
        return None
    # The result line is the LAST line of the json-format stream (the
    # earlier ones are tool_use / assistant frames); a parse over the
    # whole blob would multiple-match. Find the result envelope.
    for line in reversed(stdout.strip().splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(obj, dict) or obj.get("type") != "result":
            continue
        usage = obj.get("usage")
        if not isinstance(usage, dict):
            return None
        try:
            input_visible = int(usage.get("input_tokens", 0))
            cache_creation = int(usage.get("cache_creation_input_tokens", 0))
            cache_read = int(usage.get("cache_read_input_tokens", 0))
            output_tokens = int(usage.get("output_tokens", 0))
            billed_input = input_visible + cache_creation + cache_read
            raw_cost = obj.get("total_cost_usd")
            cost_usd: float | None = float(raw_cost) if raw_cost is not None else None
        except (TypeError, ValueError):
            return None
        return TokenUsage(
            input_tokens=input_visible,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
            cache_creation_input_tokens=cache_creation,
            cache_read_input_tokens=cache_read,
            billed_input_tokens=billed_input,
            model=_claude_model_id(obj),
            stop_reason=obj.get("stop_reason"),
            is_error=bool(obj.get("is_error", False)),
            api_error_status=obj.get("api_error_status"),
            terminal_reason=obj.get("terminal_reason") or obj.get("subtype"),
            num_turns=obj.get("num_turns"),
            service_tier=usage.get("service_tier"),
        )
    return None


def _sum_gemini_model_tokens(models: dict[str, Any]) -> tuple[int, int, int, int, int, int, str]:
    """Sum per-model token leaves across every model the gemini run touched.

    Returns ``(prompt, candidates, thoughts, cached, tool, total_reported,
    first_model_id)``. Gemini reports per-model tokens as
    ``{prompt, candidates, thoughts, cached, tool, total}``;
    summing every leaf captures a multi-model session
    (e.g. gemini-flash + gemini-flash-lite) without dropping any
    thinking-token or cached-input figure the verdict needs.
    """
    total_prompt = 0
    total_candidates = 0
    total_thoughts = 0
    total_cached = 0
    total_tool = 0
    total_reported = 0
    first_model_id = ""
    for model_id, model_stats in models.items():
        if not isinstance(model_stats, dict):
            continue
        if not first_model_id:
            first_model_id = str(model_id)
        tokens = model_stats.get("tokens")
        if not isinstance(tokens, dict):
            continue
        try:
            # Newer gemini-cli builds use ``prompt``; older builds used
            # ``input``. Fall back so neither shape silently zeros out.
            prompt_value = tokens.get("prompt")
            if prompt_value is None:
                prompt_value = tokens.get("input", 0)
            total_prompt += int(prompt_value)
            total_candidates += int(tokens.get("candidates", 0))
            total_thoughts += int(tokens.get("thoughts", 0))
            total_cached += int(tokens.get("cached", 0))
            total_tool += int(tokens.get("tool", 0))
            total_reported += int(tokens.get("total", 0))
        except (TypeError, ValueError):
            continue
    return total_prompt, total_candidates, total_thoughts, total_cached, total_tool, total_reported, first_model_id


def parse_gemini_envelope(stdout: str) -> TokenUsage | None:
    """Parse the ``gemini -p -o json`` stdout into a TokenUsage.

    Source-of-truth shape (smoked 2026-06-01):
    ``{"session_id": ..., "response": ..., "stats": {"models":
    {"<model_name>": {"tokens": {"prompt": <int>, "candidates": <int>,
    "thoughts": <int>, "cached": <int>, "tool": <int>,
    "total": <int>}}, ...}}}``.

    Captures the thinking / cached / tool token fields that the prior
    parser dropped: Gemini 2.5 Flash with thinking enabled routinely
    emits hundreds to thousands of thoughts tokens per call, which
    are billed line items and were going to zero in the verdict.
    ``total_tokens_recomputed`` is the parser's own sum
    (prompt + candidates + thoughts + tool) so a drift gate can flag
    a silent CLI-shape change.

    ``cost_usd`` is ``None`` rather than ``0.0`` (silently-false zero)
    because the gemini CLI does not surface a per-call cost on the
    free-tier auth path. The orchestrator's per-window sum skips
    ``None`` and surfaces the unknown-cost reaction count separately
    so a non-zero-cost driver is no longer made to look free.

    Uses ``scan_balanced_json`` to skip the cached-credentials line
    and any MCP-discovery error preamble before the JSON envelope;
    the naive ``stdout.find("{")`` + ``json.loads`` was breaking when
    a brace appeared inside an error-string literal. The scanner caps
    its input at 1 MiB; because ``stdout`` here is UNTRUSTED CLI output,
    an over-cap blob is treated as a soft parse failure (``None``) rather
    than propagating the scanner's ``ValueError``. Returns ``None`` on
    parse / shape / over-cap failure.
    """
    if not stdout:
        return None
    try:
        obj = scan_balanced_json(stdout)
    except ValueError:
        return None
    if obj is None:
        return None
    stats = obj.get("stats")
    if not isinstance(stats, dict):
        return None
    models = stats.get("models")
    if not isinstance(models, dict) or not models:
        return None
    (
        total_prompt,
        total_candidates,
        total_thoughts,
        total_cached,
        total_tool,
        total_reported,
        first_model_id,
    ) = _sum_gemini_model_tokens(models)
    total_recomputed = total_prompt + total_candidates + total_thoughts + total_tool
    return TokenUsage(
        input_tokens=total_prompt,
        output_tokens=total_candidates,
        # cost_usd=None is the documented gemini-CLI contract: the
        # free-tier envelope does not carry a per-call billing figure.
        cost_usd=None,
        thoughts_tokens=total_thoughts,
        cached_tokens=total_cached,
        tool_tokens=total_tool,
        total_tokens_reported=total_reported,
        total_tokens_recomputed=total_recomputed,
        model=first_model_id or "unknown",
        stop_reason=obj.get("stop_reason"),
        is_error=bool(obj.get("is_error", False)),
        api_error_status=obj.get("api_error_status"),
        terminal_reason=obj.get("terminal_reason"),
        num_turns=obj.get("num_turns"),
    )


# ---------------------------------------------------------------------------
# Wake-marker line emission helper
# ---------------------------------------------------------------------------


def _emit_wake_marker(
    *,
    framework: str,
    fw_id: str,
    seed_id: str,
    reaction_id: str,
    wall_ns: int,
    token_usage: TokenUsage | None,
    provider: str = PROVIDER_OFFLINE_OTHER,
) -> None:
    """Print the canonical one-line wake marker the orchestrator scans for.

    Format: ``DRIVER_REACTED <json>`` where ``<json>`` is a single-line
    JSON object carrying the framework + fw_id identity, the seed +
    reaction delivery ids, the driver-side wall_ns observation, the
    provider id, and the full ``TokenUsage`` struct (or ``null`` for
    non-LLM drivers) via ``msgspec.to_builtins``. All 18 ``TokenUsage``
    fields ride the wire so cache discount, moderation envelope, model
    id, and cost reach the orchestrator without truncation.

    ``provider`` is an additive identifier carrying which model path
    actually ran (e.g. ``openai-gpt-4.1-nano`` when the real-OpenAI
    path engaged the live call; ``offline-testmodel`` /
    ``offline-fakelistchatmodel`` when the offline fakes ran;
    ``claude-cli`` / ``gemini-cli`` for the CLI driver paths;
    ``offline`` for the shell-control baseline).

    JSON is the OOB telemetry channel (stdout pipe, kernel-level write).
    The structured form replaces the prior whitespace-delimited
    ``key=value`` grammar so additive ``TokenUsage`` fields slot in
    without parser changes and any string field can carry arbitrary
    content (the old grammar required whitespace escaping).
    """
    payload: dict[str, Any] = {
        "framework": framework,
        "fw_id": fw_id,
        "seed": seed_id,
        "reaction_id": reaction_id,
        "wall_ns": wall_ns,
        "provider": provider,
        "token_usage": (None if token_usage is None else msgspec.to_builtins(token_usage)),
    }
    print(f"{WAKE_MARKER} {json.dumps(payload)}", flush=True)


def _emit_early_wake_marker(
    *,
    framework: str,
    fw_id: str,
    seed_id: str,
    wake_monotonic_ns: int,
    t_sub_monotonic_ns: int,
    t_import_done_monotonic_ns: int,
) -> None:
    """Print the cross-process bus-latency anchor line on the driver's stdout.

    Format: ``WAKE_RECEIVED <json>`` where ``<json>`` is a single-line
    JSON object carrying the framework + fw_id identity, the seed
    delivery id, and the three monotonic anchors the bench correlates
    against ``t_seed_emit_monotonic_ns``.

    Emitted immediately after the driver's ``wait_for`` returns the
    matched frame, BEFORE any post-wake LLM exercise. The orchestrator
    reads the embedded monotonic timestamps to compute per-driver bus
    ingest latency (``wake_monotonic_ns - seed_emit_monotonic_ns``) and
    to classify per-row delivery mode as live ``_fan_out``
    (``t_sub_monotonic_ns < seed_emit_monotonic_ns``) vs. seq-replay
    (``t_sub_monotonic_ns >= seed_emit_monotonic_ns``). Both anchors
    are on the same Linux ``CLOCK_MONOTONIC`` the preflight asserts is
    stable cross-process.

    JSON shape mirrors ``_emit_wake_marker``: additive fields slot in
    without parser changes.
    """
    payload: dict[str, Any] = {
        "framework": framework,
        "fw_id": fw_id,
        "seed": seed_id,
        "wake_monotonic_ns": wake_monotonic_ns,
        "t_sub_monotonic_ns": t_sub_monotonic_ns,
        "t_import_done_monotonic_ns": t_import_done_monotonic_ns,
    }
    print(f"{EARLY_WAKE_MARKER} {json.dumps(payload)}", flush=True)


# Default polling cadence the poll-arm consumer uses. 100 ms is a
# common real-world short-cycle polling cadence; the average wake
# delay for a uniformly-distributed seed-emit moment is half the
# cadence (~50 ms). Subscribe-arm consumers should beat this by the
# full polling cadence in the limit.
_POLL_CADENCE_SEC = 0.1


def _wait_or_poll_for_seed(
    *,
    arm: str,
    seed_scope_id: str,
    since: str | None,
    socket_path: str,
    db_path: Path,
    timeout_sec: float,
    poll_cadence_sec: float = _POLL_CADENCE_SEC,
) -> tuple[str, int] | None:
    """Wait for the seed event via the arm's consumer posture.

    The waitbus bench's two consumer postures share this single dispatch
    point so every driver framework (pydantic, langgraph, claude-cli,
    gemini-cli, shell-control) measures the SAME bus-side wait
    semantics. Arm dispatch:

    - ``"subscribe"`` -- ``waitbus.wait_for(since=...)`` blocks on the
      daemon's broadcast push; the consumer thread is idle until a
      matching event lands. The replay cursor (``since``) bounds the
      seq-window the daemon scans when the subscribe register lands
      after the seed-emit moment (the race the waitbus daemon's
      assigned-sequence ordering covers).

    - ``"poll"`` -- direct SQL scan of the bus DB at ``poll_cadence_sec``
      intervals. No daemon round-trip; the consumer pays its own
      polling CPU and the wake delay is bounded by the cadence. This
      is the load-bearing comparison the bench's "polling vs subscribe"
      A/B exists to make.

    Returns ``(delivery_id, wake_monotonic_ns)`` on the matched seed;
    ``None`` when ``timeout_sec`` elapses without a match. Both arms
    capture ``wake_monotonic_ns`` at the moment the consumer observes
    the seed, on the same Linux ``CLOCK_MONOTONIC`` the preflight
    pins cross-process.
    """
    if arm == "subscribe":
        match = [f'fields.owner="{seed_scope_id}"']
        frame = wait_for(
            match,
            source=None,
            timeout=timeout_sec,
            socket_path=socket_path,
            since=since,
        )
        if frame is None:
            return None
        return frame.delivery_id, time.monotonic_ns()
    if arm == "poll":
        import sqlite3

        anchor_seq = 0
        if since:
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT seq FROM events WHERE event_id = ?",
                    (since,),
                ).fetchone()
            if row is not None:
                anchor_seq = int(row[0])
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT seq, delivery_id, owner FROM events "
                    "WHERE event_id IS NOT NULL AND seq > ? "
                    "ORDER BY seq LIMIT 100",
                    (anchor_seq,),
                ).fetchall()
            for seq, delivery_id, owner in rows:
                if owner == seed_scope_id:
                    return str(delivery_id), time.monotonic_ns()
                anchor_seq = max(anchor_seq, int(seq))
            time.sleep(poll_cadence_sec)
        return None
    raise ValueError(f"unknown arm: {arm!r}")


def _build_token_usage_or_none(
    input_t: Any,
    output_t: Any,
    *,
    provider: str,
) -> TokenUsage | None:
    """Construct a ``TokenUsage`` from raw SDK ints, or ``None`` if either is absent.

    Shared post-extract helper for the pydantic / langgraph extractors:
    both SDKs expose ``input_tokens`` / ``output_tokens`` on their own
    response shapes, but the waitbus ``TokenUsage`` construction +
    rate-card cost computation is identical regardless of which SDK
    sourced the counts. Lifting the shared block here keeps the
    extractors' SDK-specific parsing minimal -- they read the ints
    from the SDK shape, this helper builds the typed envelope.

    Returns ``None`` when either count is missing (``None``); the
    orchestrator's invariant gate consumes ``None`` cleanly so a
    missing usage envelope does not silently corrupt the verdict.
    """
    if input_t is None or output_t is None:
        return None
    input_tokens = int(input_t)
    output_tokens = int(output_t)
    return TokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=openai_tokens_to_usd(input_tokens, output_tokens, provider=provider),
        model=provider,
    )


def _extract_pydantic_token_usage(result: Any, *, provider: str) -> TokenUsage | None:
    """Map a pydantic-ai ``AgentRunResult.usage`` to a waitbus ``TokenUsage``.

    Returns ``None`` when the SDK did not surface usage (a stubbed model,
    a streaming-only path, an SDK version skew). The post-wake LLM call
    is the latency-exercise side of the bench's concurrency-load
    hypothesis; the bench's invariant gate consumes ``None`` cleanly so
    a missing usage envelope does not silently corrupt the verdict.

    Field names map to the pydantic-ai 1.x canonical
    ``input_tokens`` / ``output_tokens`` (the legacy 0.x
    ``request_tokens`` / ``response_tokens`` are aliased on the same
    struct so a downstream SDK rev that drops the deprecated names
    still resolves cleanly).
    """
    try:
        usage = result.usage  # 1.x: property; 0.x compat: deprecated_callable_property
    except (AttributeError, TypeError):
        # AttributeError: SDK rev dropped the property entirely.
        # TypeError: deprecated_callable_property raised on access
        # (an SDK rev that promotes the callable to a method without
        # a stable property facade).
        return None
    if usage is None:
        return None
    return _build_token_usage_or_none(
        getattr(usage, "input_tokens", None),
        getattr(usage, "output_tokens", None),
        provider=provider,
    )


def _emit_reaction(
    *,
    db_path: Path,
    doorbell_path: Path,
    framework: str,
    fw_id: str,
    seed_id: str,
    reaction_id: str,
    token_usage: TokenUsage | None,
) -> None:
    """Emit one ``agent_message`` reaction event the orchestrator collects upstream.

    The payload carries the driver-side proof: framework name,
    framework-instance id (per-spawn unique), seed delivery id the
    driver reacted to, and (for LLM drivers) the parsed token usage.
    """
    payload: dict[str, Any] = {
        "framework": framework,
        "fw_id": fw_id,
        "seed_delivery_id": seed_id,
    }
    if token_usage is not None:
        payload["token_usage"] = {
            "input_tokens": token_usage.input_tokens,
            "output_tokens": token_usage.output_tokens,
            "cost_usd": token_usage.cost_usd,
        }
    emit(
        EventInsert(
            delivery_id=reaction_id,
            source=SEED_SOURCE,
            event_type=REACTION_EVENT_TYPE,
            owner="local",
            repo="stress",
            received_at=time.time_ns(),
            payload_json=json.dumps(payload),
            ingest_method="waitbus_stress_real_driver",
            msg_from=f"{framework}:{fw_id}",
        ),
        db_path=db_path,
        doorbell_path=doorbell_path,
    )


# ---------------------------------------------------------------------------
# Per-role driver bodies
# ---------------------------------------------------------------------------


class _LangGraphReactState(TypedDict, total=False):
    """LangGraph driver's graph state (module-level so the StateGraph
    overload binds the TypedDict node-input TypeVar).

    The waitbus ``wait_for`` is called from the driver body BEFORE the
    graph is constructed, so the graph state no longer carries a
    ``reacted`` gate -- the only flow is ``react`` and the only
    state cell the node yields is the summary the chat call produced.
    """

    summary: str | None


def _extract_langgraph_token_usage(reply: Any, *, provider: str) -> TokenUsage | None:
    """Map a langchain ``BaseMessage.usage_metadata`` to a waitbus ``TokenUsage``.

    Returns ``None`` when the SDK did not surface usage. langchain's
    ``ChatOpenAI`` is documented to drop the ``usage_metadata`` field
    on some configurations (``with_structured_output``, partial-stream
    paths, an SDK version skew); the bench's invariant gate consumes
    ``None`` cleanly so a missing envelope does not silently corrupt
    the verdict. The orchestrator-side capture in ``_bench_shared`` remains
    the authoritative usage source while langchain-side telemetry
    surfaces best-effort.

    Field names map to the langchain ``usage_metadata`` schema:
    ``input_tokens`` / ``output_tokens`` (with ``total_tokens`` as the
    sum the schema also reports, but the waitbus ``TokenUsage`` shape
    only requires the input / output pair).
    """
    metadata = getattr(reply, "usage_metadata", None)
    if not isinstance(metadata, dict):
        return None
    return _build_token_usage_or_none(
        metadata.get("input_tokens"),
        metadata.get("output_tokens"),
        provider=provider,
    )


def run_pydantic_driver(
    *,
    socket_path: str,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    fw_id: str,
    arm: str = "subscribe",
    cold_prefix: str = "",
    since: str | None = None,
) -> int:
    """Pydantic AI driver: park on ``wait_for`` for the seed, then exercise the real LLM.

    The driver's lifecycle is five phases the orchestrator reads:

    1. **Cold start** -- import the framework SDK and select the model
       (real ``OpenAIModel('gpt-4.1-nano')`` when ``OPENAI_API_KEY`` is
       in env; offline ``TestModel`` otherwise). Captures
       ``t_import_done_monotonic_ns``.
    2. **Subscribe register** -- ``wait_for(since=since, ...)`` is
       called UNCONDITIONALLY from the driver body (not from inside an
       agent tool), so the wake gate is wiring + the replay cursor, not
       LLM decision quality. Captures ``t_sub_monotonic_ns`` just
       before the call.
    3. **Early wake marker** -- ``WAKE_RECEIVED`` is printed
       IMMEDIATELY after the frame returns, BEFORE any LLM call, so the
       orchestrator's bus-latency anchor is free of LLM-call jitter.
    4. **Real LLM exercise** -- ``agent.run_sync(...)`` is the host-
       perturbation workload the bench's concurrency-load hypothesis
       depends on; ``cold_prefix`` + the seed id bust the OpenAI prefix
       cache on every iteration.
    5. **Reaction + ``DRIVER_REACTED``** -- the reaction event is
       emitted and the canonical wake marker is printed with the real
       LLM call's token usage (or ``None`` when the offline path ran).

    ``since`` is the waitbus replay cursor (ULID event_id) threaded into
    ``wait_for``; absent (default) subscribes from the live watermark
    (kept for parity with the CLI-driver helper).
    """
    # Cold start: import the framework SDK and select the model. Under real
    # mode (REAL_MODE_ENV_VAR set by the controller / bench) an absent
    # OPENAI_API_KEY hard-fails here rather than silently selecting TestModel.
    from pydantic_ai import Agent

    model, provider_id = _select_pydantic_model(real_mode=_real_mode_active())
    t_import_done_monotonic_ns = time.monotonic_ns()

    # Subscribe to the waitbus bus directly from the driver body (not from
    # inside an agent tool) so the wake gate is wiring + the replay
    # cursor, not LLM decision quality.
    t_sub_monotonic_ns = time.monotonic_ns()
    _wake_result = _wait_or_poll_for_seed(
        arm=arm,
        seed_scope_id=seed_scope_id,
        since=since,
        socket_path=socket_path,
        db_path=db_path,
        timeout_sec=DRIVER_WAIT_TIMEOUT_SEC,
    )
    if _wake_result is None:
        structured(_logger, logging.WARNING, "pydantic_driver_seed_timeout", fw_id=fw_id, arm=arm)
        return 1
    seed_id, wake_monotonic_ns = _wake_result

    # Emit the early wake marker immediately, before any LLM call, so
    # the orchestrator's bus-latency anchor is free of LLM-call jitter.
    _emit_early_wake_marker(
        framework="pydantic",
        fw_id=fw_id,
        seed_id=seed_id,
        wake_monotonic_ns=wake_monotonic_ns,
        t_sub_monotonic_ns=t_sub_monotonic_ns,
        t_import_done_monotonic_ns=t_import_done_monotonic_ns,
    )

    # Run the real LLM call -- the host-perturbation workload the
    # bench measures the daemon under. Deterministic-sampling settings
    # (temperature=0 + seed) are best-effort hints to OpenAI's Chat
    # Completions API; the response's ``system_fingerprint`` is the
    # operator-visible signal for residual non-determinism.
    from pydantic_ai.settings import ModelSettings

    agent: Agent[None, str] = Agent(
        model=model,
        system_prompt="React to a stress-mode seed event on the waitbus bus.",
        model_settings=ModelSettings(temperature=LLM_REAL_TEMPERATURE, seed=LLM_REAL_SEED),
    )
    prompt = (
        f"{cold_prefix} Summarise stress seed {seed_id} in one word."
        if cold_prefix
        else f"Summarise stress seed {seed_id} in one word."
    )
    try:
        result = agent.run_sync(prompt)
    except Exception as exc:
        structured(
            _logger,
            logging.ERROR,
            "pydantic_driver_llm_failed",
            fw_id=fw_id,
            error=f"{type(exc).__name__}: {exc}",
        )
        return 3
    token_usage = _extract_pydantic_token_usage(result, provider=provider_id)

    # Emit the reaction event and the canonical DRIVER_REACTED marker
    # carrying the post-LLM token usage so the orchestrator can roll
    # cost + envelope into the bench verdict.
    reaction_id = f"stress-reaction:pydantic:{fw_id}:{uuid.uuid4()}"
    _emit_reaction(
        db_path=db_path,
        doorbell_path=doorbell_path,
        framework="pydantic",
        fw_id=fw_id,
        seed_id=seed_id,
        reaction_id=reaction_id,
        token_usage=token_usage,
    )
    _emit_wake_marker(
        framework="pydantic",
        fw_id=fw_id,
        seed_id=seed_id,
        reaction_id=reaction_id,
        wall_ns=time.time_ns(),
        token_usage=token_usage,
        provider=provider_id,
    )
    return 0


def run_langgraph_driver(
    *,
    socket_path: str,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    fw_id: str,
    arm: str = "subscribe",
    cold_prefix: str = "",
    since: str | None = None,
) -> int:
    """LangGraph driver: park on ``wait_for`` for the seed, then exercise the real LLM.

    Lifecycle mirrors the pydantic driver (the five phases live in
    ``run_pydantic_driver`` docstring): cold start, subscribe register,
    early wake marker, post-wake LLM exercise, reaction + canonical
    wake marker.

    The waitbus ``wait_for`` is called from the driver body BEFORE the
    StateGraph is constructed -- the previous shape buried the wait
    inside a ``wait_on_seed`` graph node so the post-wake LLM call
    was gated on graph construction completing within the bench's
    spawn-settle window, which empirically lost the race on cold
    starts. The graph now carries only a ``react`` node so the
    langchain wiring stays exercised end-to-end without coupling the
    wake gate to graph compilation timing.

    When ``OPENAI_API_KEY`` is present in env, the ``react`` node calls
    a real ``ChatOpenAI`` instance (``gpt-4.1-nano``); otherwise the
    offline ``FakeListChatModel`` fallback is used. ``cold_prefix`` is
    prepended to the user prompt so the real cache cannot match across
    iterations; for the offline fake the prompt content is a no-op.

    ``since`` is the waitbus replay cursor (ULID event_id) threaded into
    ``wait_for``; absent (default) subscribes from the live watermark.
    """
    # Cold start: import the framework SDK and select the chat model. Under
    # real mode (REAL_MODE_ENV_VAR set by the controller / bench) an absent
    # OPENAI_API_KEY hard-fails here rather than silently selecting the fake.
    from langgraph.graph import END, START, StateGraph

    chat, provider_id = _select_langgraph_chat_model(real_mode=_real_mode_active())
    t_import_done_monotonic_ns = time.monotonic_ns()

    # Subscribe to the waitbus bus directly from the driver body, BEFORE
    # the StateGraph is constructed, so the wake gate is wiring + the
    # replay cursor and is decoupled from graph-compilation timing.
    t_sub_monotonic_ns = time.monotonic_ns()
    _wake_result = _wait_or_poll_for_seed(
        arm=arm,
        seed_scope_id=seed_scope_id,
        since=since,
        socket_path=socket_path,
        db_path=db_path,
        timeout_sec=DRIVER_WAIT_TIMEOUT_SEC,
    )
    if _wake_result is None:
        structured(_logger, logging.WARNING, "langgraph_driver_seed_timeout", fw_id=fw_id, arm=arm)
        return 1
    seed_id, wake_monotonic_ns = _wake_result

    # Emit the early wake marker immediately, before any LLM call, so
    # the orchestrator's bus-latency anchor is free of LLM-call jitter.
    _emit_early_wake_marker(
        framework="langgraph",
        fw_id=fw_id,
        seed_id=seed_id,
        wake_monotonic_ns=wake_monotonic_ns,
        t_sub_monotonic_ns=t_sub_monotonic_ns,
        t_import_done_monotonic_ns=t_import_done_monotonic_ns,
    )

    # Build a single-node react graph and run the real LLM call -- the
    # host-perturbation workload the bench measures the daemon under.
    reply_holder: dict[str, Any] = {}

    def react(state: _LangGraphReactState) -> dict[str, Any]:
        prompt = (
            f"{cold_prefix} Summarise stress seed {seed_id} in one word."
            if cold_prefix
            else f"Summarise stress seed {seed_id} in one word."
        )
        reply = chat.invoke(prompt)
        reply_holder["reply"] = reply
        content = reply.content
        summary = content if isinstance(content, str) else str(content)
        return {"summary": summary}

    # LangGraph's add_node overloads do not bind a TypedDict node under
    # mypy --strict here (the hero-swarm precedent uses the same Any
    # escape hatch); the runtime graph shape is unchanged.
    builder: Any = StateGraph(_LangGraphReactState)
    builder.add_node("react", react)
    builder.add_edge(START, "react")
    builder.add_edge("react", END)
    graph = builder.compile()
    try:
        graph.invoke({"summary": None})
    except Exception as exc:
        structured(
            _logger,
            logging.ERROR,
            "langgraph_driver_llm_failed",
            fw_id=fw_id,
            error=f"{type(exc).__name__}: {exc}",
        )
        return 3
    token_usage = _extract_langgraph_token_usage(reply_holder.get("reply"), provider=provider_id)

    # Emit the reaction event and the canonical DRIVER_REACTED marker
    # carrying the post-LLM token usage so the orchestrator can roll
    # cost + envelope into the bench verdict.
    reaction_id = f"stress-reaction:langgraph:{fw_id}:{uuid.uuid4()}"
    _emit_reaction(
        db_path=db_path,
        doorbell_path=doorbell_path,
        framework="langgraph",
        fw_id=fw_id,
        seed_id=seed_id,
        reaction_id=reaction_id,
        token_usage=token_usage,
    )
    _emit_wake_marker(
        framework="langgraph",
        fw_id=fw_id,
        seed_id=seed_id,
        reaction_id=reaction_id,
        wall_ns=time.time_ns(),
        token_usage=token_usage,
        provider=provider_id,
    )
    return 0


def _wait_for_seed_via_subscribe(
    *,
    socket_path: str,
    db_path: Path,
    seed_scope_id: str,
    timeout_sec: float,
    since: str | None = None,
    framework: str,
    fw_id: str,
    arm: str = "subscribe",
) -> str | None:
    """Block on waitbus for one seed event; return its delivery_id or None on timeout.

    Used by the CLI-driver roles (claude-cli, gemini-cli, shell-control)
    that do not embed a framework agent at all -- they need the waitbus
    SDK wait only.

    Owner-only predicate; see ``run_pydantic_driver`` for the rationale.

    On a successful wake, emits the ``WAKE_RECEIVED`` early marker so
    the bench's per-row ``delivery_mode`` classification + bus-latency
    anchor cover the CLI drivers symmetrically with the in-proc
    framework drivers (a CLI driver that omitted the early marker
    would land in the bench verdict as ``delivery_mode="unknown"``
    and a crash-during-LLM would mislabel as ``reaction_missing``
    instead of ``llm_timeout_or_crash``).

    ``since`` is the waitbus replay cursor (ULID event_id): absent (default)
    subscribes from the live watermark; non-None replays the daemon's
    seq window so any matching frame emitted at or after the cursor is
    delivered regardless of subscribe-register wall-clock latency.

    ``framework`` and ``fw_id`` ride the early-marker line so the
    orchestrator's per-framework lookup resolves identically to the
    in-proc driver path.
    """

    # Captured at helper entry -- CLI drivers do no heavy SDK import,
    # so the helper-entry moment is a faithful proxy for the
    # ``t_import_done`` anchor the in-proc drivers capture explicitly.
    t_import_done_monotonic_ns = time.monotonic_ns()
    t_sub_monotonic_ns = time.monotonic_ns()
    result = _wait_or_poll_for_seed(
        arm=arm,
        seed_scope_id=seed_scope_id,
        since=since,
        socket_path=socket_path,
        db_path=db_path,
        timeout_sec=timeout_sec,
    )
    if result is None:
        return None
    seed_id, wake_monotonic_ns = result
    _emit_early_wake_marker(
        framework=framework,
        fw_id=fw_id,
        seed_id=seed_id,
        wake_monotonic_ns=wake_monotonic_ns,
        t_sub_monotonic_ns=t_sub_monotonic_ns,
        t_import_done_monotonic_ns=t_import_done_monotonic_ns,
    )
    return seed_id


def _run_cli_driver(
    *,
    framework: str,
    binary_name: str,
    argv_builder: Callable[[str, str], list[str]],
    envelope_parser: Callable[[str], TokenUsage | None],
    provider: str,
    socket_path: str,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    fw_id: str,
    arm: str = "subscribe",
    cold_prefix: str = "",
    since: str | None = None,
) -> int:
    """Shared CLI-driver lifecycle for the claude-cli / gemini-cli roles.

    The two CLI drivers differ ONLY in four deltas, all passed in:

    - ``framework`` -- the role label stamped on the markers / reaction.
    - ``binary_name`` -- the PATH binary to resolve (``claude`` / ``gemini``).
    - ``argv_builder`` -- ``(binary_path, prompt_text) -> argv`` builds the
      provider-specific command line (claude's
      ``--output-format=json`` vs gemini's ``-o json``).
    - ``envelope_parser`` -- ``parse_claude_envelope`` / ``parse_gemini_envelope``.
    - ``provider`` -- the provider id stamped on the wake marker.

    Everything else -- the seed wait, the PATH check, the cold-prefix
    prompt, the THREE-way failure taxonomy, the reaction emit, and the
    wake-marker emit -- is identical and lives here once.

    Failure taxonomy (the P0 the prior ``check=True`` shape collapsed).
    ``subprocess.run`` runs WITHOUT ``check=True`` so a non-zero exit no
    longer raises; the outcome is branched on explicitly:

    - ``TimeoutExpired`` -> ``EXIT_LLM_TIMEOUT`` (3). The CLI hung past
      ``LLM_CALL_TIMEOUT_SEC``; no envelope is producible.
    - returncode != 0 AND the envelope does not parse ->
      ``EXIT_AUTH_OR_INVOCATION_ERROR`` (4). Auth / quota failure or a
      crash before any result frame; no reaction is emitted.
    - the envelope parses AND ``envelope_is_refusal`` fires (a moderation
      refusal ``stop_reason="refusal"``, an exit-0 envelope flagged
      ``is_error=True`` with no refusal marker, or the Anthropic
      ``terminal_reason="error_during_execution"`` shape), OR returncode
      != 0 with a usable envelope -> ``EXIT_REFUSAL_OR_NONZERO_ENVELOPE``
      (5). This is a deliberate UNION class -- "the call ran but the
      envelope is not a clean success" -- not a refusal-only signal: a
      soft ``is_error``-on-exit-0 is folded in on purpose rather than
      given its own code, because (a) the precise sub-kind a consumer
      might want is carried losslessly in the envelope's own
      ``stop_reason`` / ``is_error`` / ``terminal_reason`` fields that
      ride the bus, (b) no runtime consumer reads this exit code -- only
      the driver-contract tests do -- and (c) the verdict aggregator
      ``_controller._summarize_real_curve_points`` already buckets refusal
      and soft is_error together via the same ``envelope_is_refusal``
      discriminator, so splitting here would diverge the driver branch
      from the verdict branch it mirrors. The reaction + wake marker ARE
      still emitted so the orchestrator's envelope path observes the
      outcome via the ``TokenUsage`` fields.
    - returncode == 0 with a clean envelope -> ``EXIT_OK`` (0).

    ``cold_prefix`` (when non-empty) is prepended to the LLM prompt so
    the provider-side prompt cache cannot match across iterations.

    ``since`` is the waitbus replay cursor (ULID event_id) forwarded to
    ``_wait_for_seed_via_subscribe``; absent (default) subscribes from
    the live watermark.
    """
    seed_id = _wait_for_seed_via_subscribe(
        socket_path=socket_path,
        db_path=db_path,
        seed_scope_id=seed_scope_id,
        timeout_sec=DRIVER_WAIT_TIMEOUT_SEC,
        since=since,
        framework=framework,
        fw_id=fw_id,
        arm=arm,
    )
    if seed_id is None:
        structured(_logger, logging.WARNING, "cli_driver_seed_timeout", framework=framework, fw_id=fw_id)
        return EXIT_SEED_TIMEOUT
    binary_path = shutil.which(binary_name)
    if binary_path is None:
        structured(_logger, logging.ERROR, "cli_driver_no_cli", framework=framework, fw_id=fw_id, binary=binary_name)
        return EXIT_NO_CLI
    prompt_text = f"{cold_prefix} Say 'ack' in one word." if cold_prefix else "Say 'ack' in one word."
    try:
        proc = subprocess.run(
            argv_builder(binary_path, prompt_text),
            check=False,
            capture_output=True,
            text=True,
            timeout=LLM_CALL_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired as exc:
        # Class 1: the LLM call hung past LLM_CALL_TIMEOUT_SEC. No
        # envelope is producible; the orchestrator sees no reaction.
        structured(_logger, logging.ERROR, "cli_driver_llm_timeout", framework=framework, fw_id=fw_id, error=str(exc))
        return EXIT_LLM_TIMEOUT

    token_usage = envelope_parser(proc.stdout)
    if proc.returncode != 0 and token_usage is None:
        # Class 2: the CLI exited non-zero AND produced no parseable
        # envelope -- auth / quota failure or a crash before the result
        # frame. No reaction to emit; only the exit code surfaces it.
        structured(
            _logger,
            logging.ERROR,
            "cli_driver_auth_or_invocation_error",
            framework=framework,
            fw_id=fw_id,
            returncode=proc.returncode,
            stderr_tail=(proc.stderr or "")[-400:],
        )
        return EXIT_AUTH_OR_INVOCATION_ERROR

    # Either a clean success, a refusal envelope, or a non-zero exit that
    # still carried a usable envelope. In all three cases a reaction +
    # wake marker are emitted so the orchestrator's envelope path sees
    # the outcome; the return code below distinguishes the refusal /
    # non-zero case from the clean path.
    reaction_id = f"stress-reaction:{framework}:{fw_id}:{uuid.uuid4()}"
    _emit_reaction(
        db_path=db_path,
        doorbell_path=doorbell_path,
        framework=framework,
        fw_id=fw_id,
        seed_id=seed_id,
        reaction_id=reaction_id,
        token_usage=token_usage,
    )
    _emit_wake_marker(
        framework=framework,
        fw_id=fw_id,
        seed_id=seed_id,
        reaction_id=reaction_id,
        wall_ns=time.time_ns(),
        token_usage=token_usage,
        provider=provider,
    )
    if proc.returncode != 0 or envelope_is_refusal(token_usage):
        # Class 3: a moderation refusal, an exit-0 envelope flagged
        # ``is_error=True`` (a soft upstream error with no refusal marker),
        # or a non-zero exit that still produced a usable envelope. All
        # three are folded into one exit code on purpose: this code is a
        # union "the call ran but the envelope is not a clean success"
        # class (hence its name, ``EXIT_REFUSAL_OR_NONZERO_ENVELOPE``),
        # NOT a refusal-only signal. The distinction a consumer actually
        # needs -- refusal vs soft is_error vs end_turn -- is carried
        # losslessly in the envelope itself (``stop_reason`` / ``is_error``
        # / ``terminal_reason`` on the ``TokenUsage`` that rode the bus),
        # and the orchestrator reads it there: no runtime consumer reads
        # this exit code (only the driver-contract tests do), and the
        # verdict aggregator ``_controller._summarize_real_curve_points``
        # already groups refusal and soft is_error into a single
        # ``invariant_failure_count`` via the same ``envelope_is_refusal``
        # discriminator. Splitting a soft is_error into its own exit code
        # would therefore add a code no consumer reads while diverging the
        # driver branch from the verdict branch it is meant to mirror. The
        # reaction is still on the bus; the exit code only lets a
        # hypothetical exit-code reader tell this union class apart from
        # the auth/invocation (4) and timeout (3) classes.
        structured(
            _logger,
            logging.WARNING,
            "cli_driver_refusal_or_nonzero_envelope",
            framework=framework,
            fw_id=fw_id,
            returncode=proc.returncode,
            stop_reason=token_usage.stop_reason if token_usage is not None else None,
            is_error=token_usage.is_error if token_usage is not None else None,
        )
        return EXIT_REFUSAL_OR_NONZERO_ENVELOPE
    return EXIT_OK


def _claude_cli_argv(binary_path: str, prompt_text: str) -> list[str]:
    """Build the ``claude -p ... --output-format=json`` argv."""
    return [binary_path, "-p", prompt_text, "--output-format=json"]


def _gemini_cli_argv(binary_path: str, prompt_text: str) -> list[str]:
    """Build the ``gemini -p ... -o json`` argv."""
    return [binary_path, "-p", prompt_text, "-o", "json"]


def run_claude_cli_driver(
    *,
    socket_path: str,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    fw_id: str,
    arm: str = "subscribe",
    cold_prefix: str = "",
    since: str | None = None,
) -> int:
    """claude -p driver: wait for the seed, then call ``claude -p --output-format=json``.

    Thin delegate over ``_run_cli_driver`` -- the shared lifecycle
    (seed wait, PATH check, the three-way failure taxonomy, reaction +
    wake-marker emit) lives there once; this entry point only supplies
    the claude-specific binary name, argv builder, envelope parser, and
    provider id.
    """
    return _run_cli_driver(
        framework="claude-cli",
        binary_name="claude",
        argv_builder=_claude_cli_argv,
        envelope_parser=parse_claude_envelope,
        provider=PROVIDER_CLAUDE_CLI,
        socket_path=socket_path,
        db_path=db_path,
        doorbell_path=doorbell_path,
        seed_scope_id=seed_scope_id,
        fw_id=fw_id,
        arm=arm,
        cold_prefix=cold_prefix,
        since=since,
    )


def run_gemini_cli_driver(
    *,
    socket_path: str,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    fw_id: str,
    arm: str = "subscribe",
    cold_prefix: str = "",
    since: str | None = None,
) -> int:
    """gemini -p driver: wait for the seed, then call ``gemini -p -o json``.

    Thin delegate over ``_run_cli_driver`` -- see
    ``run_claude_cli_driver`` for the shared-lifecycle rationale. This
    entry point supplies only the gemini-specific binary name, argv
    builder, envelope parser, and provider id.
    """
    return _run_cli_driver(
        framework="gemini-cli",
        binary_name="gemini",
        argv_builder=_gemini_cli_argv,
        envelope_parser=parse_gemini_envelope,
        provider=PROVIDER_GEMINI_CLI,
        socket_path=socket_path,
        db_path=db_path,
        doorbell_path=doorbell_path,
        seed_scope_id=seed_scope_id,
        fw_id=fw_id,
        arm=arm,
        cold_prefix=cold_prefix,
        since=since,
    )


def run_shell_control_driver(
    *,
    socket_path: str,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    fw_id: str,
    arm: str = "subscribe",
    once_then_exit: bool = False,
    cold_prefix: str = "",
    since: str | None = None,
) -> int:
    """Shell control driver: wait for the seed, then bash-echo + react.

    The synthetic-control baseline: no LLM, no framework, just a
    raw waitbus-SDK subscriber + a bash ``echo`` that proves the
    cross-broadcast also lands at the cheapest-possible consumer.

    ``once_then_exit`` short-circuits the wait/emit cycle for the
    no-bus dispatch-smoke test: the driver prints the wake-marker
    line with placeholder fields and exits 0, exercising the
    role-dispatch entry point without needing a daemon.

    ``cold_prefix`` is accepted for argv parity with the LLM-driver
    roles but the shell-control body issues no LLM call.

    ``since`` is the waitbus replay cursor (ULID event_id) forwarded to
    ``_wait_for_seed_via_subscribe``; absent (default) subscribes from
    the live watermark.
    """
    _ = cold_prefix  # parity-only; no LLM prompt to bust here.
    if once_then_exit:
        _emit_wake_marker(
            framework="shell-control",
            fw_id=fw_id,
            seed_id="dispatch-smoke",
            reaction_id="dispatch-smoke",
            wall_ns=time.time_ns(),
            token_usage=None,
            provider=PROVIDER_OFFLINE_OTHER,
        )
        return 0

    seed_id = _wait_for_seed_via_subscribe(
        socket_path=socket_path,
        db_path=db_path,
        seed_scope_id=seed_scope_id,
        timeout_sec=DRIVER_WAIT_TIMEOUT_SEC,
        since=since,
        framework="shell-control",
        fw_id=fw_id,
        arm=arm,
    )
    if seed_id is None:
        structured(_logger, logging.WARNING, "shell_driver_seed_timeout", fw_id=fw_id)
        return 1
    # The "shell" part: a bash echo. Captures shell-execution timing
    # parity with the other CLI drivers without any LLM cost.
    try:
        subprocess.run(
            ["/bin/bash", "-c", f"echo 'stress shell ack for {seed_id}'"],
            check=True,
            capture_output=True,
            text=True,
            timeout=_SHELL_DRIVER_TIMEOUT_SEC,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        structured(_logger, logging.ERROR, "shell_driver_failed", fw_id=fw_id, error=str(exc))
        return 3
    reaction_id = f"stress-reaction:shell-control:{fw_id}:{uuid.uuid4()}"
    _emit_reaction(
        db_path=db_path,
        doorbell_path=doorbell_path,
        framework="shell-control",
        fw_id=fw_id,
        seed_id=seed_id,
        reaction_id=reaction_id,
        token_usage=None,
    )
    _emit_wake_marker(
        framework="shell-control",
        fw_id=fw_id,
        seed_id=seed_id,
        reaction_id=reaction_id,
        wall_ns=time.time_ns(),
        token_usage=None,
        provider=PROVIDER_OFFLINE_OTHER,
    )
    return 0


# ---------------------------------------------------------------------------
# Module entry point (subprocess role dispatch)
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Entry point: dispatch to a per-role driver body.

    ``python -m scripts.stress._real_drivers <role> [args]`` — the
    orchestrator spawns one of these per driver slot. The
    ``auth-smoke`` role exists for the operator + the unit tests to
    invoke ``auth_smoke_check`` from the command line.
    """
    parser = argparse.ArgumentParser(prog="python -m scripts.stress._real_drivers")
    sub = parser.add_subparsers(dest="role")

    sub.add_parser("auth-smoke", help="Run auth_smoke_check and print the JSON result.")

    for role in ("pydantic", "langgraph", "claude-cli", "gemini-cli", "shell-control"):
        sp = sub.add_parser(role)
        sp.add_argument("--socket", required=True)
        sp.add_argument("--db", required=True)
        sp.add_argument("--doorbell", required=True)
        sp.add_argument("--seed-scope-id", required=True)
        sp.add_argument("--fw-id", required=True)
        sp.add_argument(
            "--cold-prefix",
            default="",
            help="Per-iteration cache-buster prepended to the LLM prompt; empty keeps the canonical prompt.",
        )
        sp.add_argument(
            "--since",
            default=None,
            type=str,
            help=(
                "Replay cursor (ULID event_id) for the driver's wait_for subscription. "
                "Absent (default) subscribes from the live watermark; supplying the "
                "event_id of an event the orchestrator emitted before spawning the "
                "drivers makes the seed delivery race-immune across subprocess start-up "
                "latency."
            ),
        )
        sp.add_argument(
            "--arm",
            default="subscribe",
            choices=("subscribe", "poll"),
            help=(
                "Consumer posture the driver uses to wait for the seed event. "
                "``subscribe`` (default) blocks on ``waitbus.wait_for`` -- daemon "
                "push, idle until match. ``poll`` SQL-scans the bus DB at a fixed "
                "cadence -- the consumer pays its own polling CPU. The bench's "
                "polling-vs-subscribe A/B threads the arm into the spawn argv."
            ),
        )
        if role == "shell-control":
            sp.add_argument("--once-then-exit", action="store_true")

    args = parser.parse_args(argv)
    if args.role == "auth-smoke":
        provenance = auth_smoke_check()
        print(json.dumps(provenance))
        return 0
    if args.role is None:
        parser.print_help()
        return 2

    db_path = Path(args.db)
    doorbell_path = Path(args.doorbell)

    cold_prefix = getattr(args, "cold_prefix", "") or ""
    since: str | None = getattr(args, "since", None)
    arm: str = getattr(args, "arm", "subscribe")
    common_kwargs: dict[str, Any] = {
        "socket_path": args.socket,
        "db_path": db_path,
        "doorbell_path": doorbell_path,
        "seed_scope_id": args.seed_scope_id,
        "fw_id": args.fw_id,
        "cold_prefix": cold_prefix,
        "since": since,
        "arm": arm,
    }
    role_dispatch: dict[str, Any] = {
        "pydantic": (run_pydantic_driver, {}),
        "langgraph": (run_langgraph_driver, {}),
        "claude-cli": (run_claude_cli_driver, {}),
        "gemini-cli": (run_gemini_cli_driver, {}),
        "shell-control": (
            run_shell_control_driver,
            {"once_then_exit": getattr(args, "once_then_exit", False)},
        ),
    }
    entry = role_dispatch.get(args.role)
    if entry is None:
        parser.error(f"unknown role: {args.role}")
        return 2  # unreachable; parser.error raises SystemExit
    fn, extra = entry
    return int(fn(**common_kwargs, **extra))


__all__ = [
    "AUTH_SMOKE_TIMEOUT_SEC",
    "DRIVER_WAIT_TIMEOUT_SEC",
    "EARLY_WAKE_MARKER",
    "EXIT_AUTH_OR_INVOCATION_ERROR",
    "EXIT_LLM_TIMEOUT",
    "EXIT_NO_CLI",
    "EXIT_OK",
    "EXIT_REFUSAL_OR_NONZERO_ENVELOPE",
    "EXIT_SEED_TIMEOUT",
    "FRAMEWORK_ORDER",
    "LLM_CALL_TIMEOUT_SEC",
    "OPENAI_DRIVER_FRAMEWORKS",
    "PROVIDER_CLAUDE_CLI",
    "PROVIDER_GEMINI_CLI",
    "PROVIDER_OFFLINE_LANGGRAPH",
    "PROVIDER_OFFLINE_OTHER",
    "PROVIDER_OFFLINE_PYDANTIC",
    "PROVIDER_OPENAI_GPT_4_1_NANO",
    "REACTION_EVENT_TYPE",
    "REAL_MODE_ENV_VAR",
    "REAL_OPENAI_MODEL_ID",
    "SEED_EVENT_TYPE",
    "SEED_SOURCE",
    "WAKE_MARKER",
    "auth_smoke_check",
    "framework_mix_for",
    "main",
    "parse_claude_envelope",
    "parse_gemini_envelope",
    "run_claude_cli_driver",
    "run_gemini_cli_driver",
    "run_langgraph_driver",
    "run_pydantic_driver",
    "run_shell_control_driver",
]


if __name__ == "__main__":
    sys.exit(main())
