"""Stress harness data shapes: signal failure, verdict doc, run context.

stdlib + msgspec only. Importing this module must not trigger any other
stress-sibling import so the package DAG stays acyclic.

The verdict-doc surface is declared in full here even though several
fields are populated by later modules (``zero_polling_verdict`` by the
zero-polling test, ``curve`` + ``usl_*`` + ``knee_*`` by ``_usl`` /
``_controller``). Declaring the shape up front avoids a surprise
field-set drift and makes the verdict consumer-facing JSON contract
explicit from day one.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import msgspec


class StressSignalFailure(msgspec.Struct, kw_only=True, frozen=True):
    """Per-signal failure record carried by ``_VerdictDoc.failures``.

    Discriminated by ``signal`` name; ``threshold`` / ``observed`` give
    a machine-comparable delta so a consumer does not need to parse
    ``detail``. ``sample_index`` is an optional forensic pointer into
    the matching sample list (``None`` when the failure is aggregate
    over the entire run, e.g. ``knee_not_found``).
    """

    signal: str
    threshold: float
    observed: float
    detail: str = ""
    sample_index: int | None = None


class TokenUsage(msgspec.Struct, kw_only=True, frozen=True):
    """Per-LLM-call token + cost accounting for a real-mode driver reaction.

    Union superset of the fields either driver populates: the visible /
    output / cost trio every driver reports plus the Anthropic
    cache-discount fields (claude billing semantics) and the Gemini
    thinking / cached / tool fields (Gemini 2.5 Flash with thinking on
    routinely emits ~500-3000 thoughts tokens per iteration). The
    moderation / error provenance fields (``stop_reason``,
    ``is_error``, ``terminal_reason``, ...) surface refusals and
    upstream errors that would otherwise be invisible at zero output.

    ``cost_usd`` is ``Optional[float]``: ``None`` means "tier does not
    surface a per-call billing figure" (the gemini free-tier path).
    Downstream summers must treat ``None`` as not-summable and surface
    the count of unknown-cost reactions separately rather than coercing
    to ``0.0`` (which silently makes a non-zero-cost driver look free).

    Parser sources:

    - ``claude -p --output-format=json``: ``usage.input_tokens``,
      ``usage.{cache_creation_input_tokens, cache_read_input_tokens}``,
      ``usage.output_tokens``, ``total_cost_usd``, plus the moderation
      envelope fields (``stop_reason``, ``is_error``,
      ``api_error_status``, ``terminal_reason`` / ``subtype``,
      ``num_turns``, ``service_tier``).
    - ``gemini -p -o json``: per-model leaf
      ``stats.models.<model_name>.tokens.{prompt, candidates, thoughts,
      cached, tool, total}``. The CLI does not surface a per-call cost
      on the free-tier auth path so ``cost_usd=None``.

    Every field beyond the visible / output trio defaults to a neutral
    value (``0`` for counts, ``None`` for optional strings, ``False``
    for ``is_error``) so existing constructors stay compatible and
    msgspec serialises the verdict shape additively.
    """

    input_tokens: int
    output_tokens: int
    cost_usd: float | None = None

    # Claude cache discount fields (Anthropic billing semantics).
    # billed_input_tokens == input_tokens + cache_creation + cache_read.
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    billed_input_tokens: int = 0

    # Gemini thinking + cached + tool fields.
    # ``total_tokens_reported`` is the per-leaf ``tokens.total`` the CLI
    # surfaced; ``total_tokens_recomputed`` is the sum the parser
    # reconstructs so a drift gate can flag silent CLI-shape changes.
    thoughts_tokens: int = 0
    cached_tokens: int = 0
    tool_tokens: int = 0
    total_tokens_reported: int = 0
    total_tokens_recomputed: int = 0

    # Moderation / error provenance (both drivers). A refusal envelope
    # has ``stop_reason="refusal"`` and ``is_error=True`` with
    # ``output_tokens=0``; without these fields the orchestrator cannot
    # distinguish a successful no-op from a moderated refusal.
    model: str = "unknown"
    stop_reason: str | None = None
    is_error: bool = False
    api_error_status: str | None = None
    terminal_reason: str | None = None
    num_turns: int | None = None
    service_tier: str | None = None


def envelope_is_refusal(token_usage: TokenUsage | None) -> bool:
    """Return True iff a parsed token envelope carries a moderation refusal / upstream error.

    The single source of truth for the refusal/error discriminator shared
    by the driver-side exit-code branch (``_real_drivers._run_cli_driver``)
    and the orchestrator-side invariant gate
    (``_controller._summarize_real_curve_points``). Both previously
    inlined the same three-clause test and were "kept in sync by comment
    only"; hoisting it here removes the drift risk at the root.

    A refusal is ``stop_reason="refusal"``, an upstream error is
    ``is_error=True``, and ``terminal_reason="error_during_execution"`` is
    the Anthropic refusal terminal-state marker. A ``None`` envelope (the
    CLI produced nothing parseable) is NOT a refusal -- that is the
    auth / invocation class, which the driver routes to
    ``EXIT_AUTH_OR_INVOCATION_ERROR``.
    """
    if token_usage is None:
        return False
    return (
        token_usage.is_error
        or token_usage.stop_reason == "refusal"
        or token_usage.terminal_reason == "error_during_execution"
    )


# Canonical OpenAI provider identifiers. Each constant is the rate-card
# key used by ``_OPENAI_PRICING_USD_PER_1M`` AND the value returned by
# ``_canonical_openai_provider`` for the corresponding model family --
# one source of truth, no string-literal drift between the rate table
# and the normalizer.
OPENAI_PROVIDER_GPT_4O_MINI = "openai-gpt-4o-mini"
OPENAI_PROVIDER_GPT_4_1_NANO = "openai-gpt-4.1-nano"

# Default canonical provider id for bench code paths that need to fall
# back when ``OpenAIEnvelope.model`` is the sentinel ``"unknown"``.
DEFAULT_OPENAI_PROVIDER = OPENAI_PROVIDER_GPT_4O_MINI

# OpenAI per-million-token rates indexed by the canonical provider id.
# The driver path stamps the provider onto every reaction; the
# orchestrator joins the table at cost-computation time. New models
# slot into the table without a driver change.
#
# Each row carries an "input" (fresh prompt), an "output" (completion),
# and a "cached" (prompt-cache read) rate. The cached-read discount is
# PER-MODEL on OpenAI, not a flat multiplier: gpt-4o-mini reads cached
# input at 0.5x (0.075 vs 0.15) and gpt-4.1-nano at 0.25x (0.025 vs
# 0.10). A GPT-5.x family would read at 0.1x if added. The provider
# returns the cached count on ``usage.prompt_tokens_details.cached_tokens``.
#
# Sources (accessed 2026-06-04):
#   - https://openai.com/api/pricing/ -- gpt-4o-mini / gpt-4.1-nano
#     per-model cached-input multipliers are documented on the OpenAI
#     pricing page referenced above (gpt-4o-mini 0.5x, gpt-4.1-nano 0.25x
#     cached read).
#
# Adding a new model: extend ``_OPENAI_PRICING_USD_PER_1M`` (input,
# output, AND cached) AND the corresponding ``OPENAI_PROVIDER_*``
# constant + a branch in ``_canonical_openai_provider`` in lockstep;
# the helper returns ``None`` for any unknown provider so an unmapped
# row surfaces as ``cost_usd=None`` rather than silently picking the
# wrong rate.
_OPENAI_PRICING_USD_PER_1M: dict[str, dict[str, float]] = {
    OPENAI_PROVIDER_GPT_4O_MINI: {"input": 0.15, "output": 0.60, "cached": 0.075},
    OPENAI_PROVIDER_GPT_4_1_NANO: {"input": 0.10, "output": 0.40, "cached": 0.025},
}


def _canonical_openai_provider(model: str) -> str | None:
    """Map an OpenAI model id (canonical or dated) to the rate-table key.

    The driver path stamps the canonical ``openai-gpt-*`` provider id
    on every reaction; the OpenAI Chat Completions API response stamps
    a dated id like ``gpt-4o-mini-2024-07-18``. The bench's verdict
    aggregation may see either shape depending on the envelope source.
    Returns ``None`` when the model id is neither canonical nor a
    known OpenAI family prefix -- the caller surfaces that as
    ``cost_usd=None`` rather than silently picking a wrong rate.
    """
    if model.startswith("openai-"):
        return model
    if model.startswith("gpt-4o-mini"):
        return OPENAI_PROVIDER_GPT_4O_MINI
    if model.startswith("gpt-4.1-nano"):
        return OPENAI_PROVIDER_GPT_4_1_NANO
    return None


def openai_tokens_to_usd(
    input_tokens: int,
    output_tokens: int,
    *,
    provider: str,
    cached_tokens: int = 0,
) -> float | None:
    """Map raw token counts to USD via the per-provider rate card.

    ``provider`` accepts either the canonical ``openai-gpt-*`` id the
    driver path stamps or a dated OpenAI Chat Completions model id;
    the helper normalizes via ``_canonical_openai_provider`` before
    rate-card lookup. Returns ``None`` for non-OpenAI providers or
    when the family is not in the pricing table -- the caller
    surfaces unknown-cost reactions separately (verdict-level
    ``cost_unknown_count``). Both bench and stress driver paths share
    this single source-of-truth for OpenAI pricing.

    Billing is DISJOINT across the three token classes: ``input_tokens``
    is the fresh (non-cached) visible prompt, ``cached_tokens`` is the
    prompt-cache read subset (billed at the per-model cached rate, a
    0.5x / 0.25x discount), and ``output_tokens`` is the completion. The
    waitbus convention is that ``input_tokens`` already EXCLUDES the cached
    subset (the OpenAI driver path never folds the cached read into the
    visible count), so the three terms do not overlap and summing them
    does not double-count the cached prefix.
    """
    canonical = _canonical_openai_provider(provider)
    if canonical is None:
        return None
    rates = _OPENAI_PRICING_USD_PER_1M[canonical]
    input_usd = input_tokens * rates["input"] / 1_000_000.0
    cached_usd = cached_tokens * rates["cached"] / 1_000_000.0
    output_usd = output_tokens * rates["output"] / 1_000_000.0
    return input_usd + cached_usd + output_usd


# Hard length cap on the brace scanner so an adversarial preamble (or a
# multi-megabyte stdout from a verbose CLI) cannot drag the parser into a
# pathological repeated-decode walk. Both callers pass through the default
# 1 MiB upper bound; the gemini-stdout driver path parses UNTRUSTED CLI
# stdout, so the cap is the bound that keeps an adversarial blob from
# blowing memory/time. Inputs over the cap raise ``ValueError`` so the
# caller decides whether that is a hard invariant failure (bench) or a
# soft parse failure (driver returns ``None``).
DEFAULT_SCAN_BALANCED_JSON_MAX_BYTES = 1_000_000


def scan_balanced_json(blob: str, max_bytes: int = DEFAULT_SCAN_BALANCED_JSON_MAX_BYTES) -> dict[str, Any] | None:
    """Find the first top-level JSON object in a noisy blob.

    Both real-mode CLI drivers (gemini, claude) prefix the JSON envelope
    with status chatter ("Loaded cached credentials.") and any
    MCP-discovery errors ("Error during discovery for MCP server '...':
    spawn ... ENOENT"). The naive ``blob.find("{")`` + ``json.loads``
    fails when a brace appears inside an error-string literal or when
    trailing log output follows the envelope.

    Each ``{`` offset is fed to :meth:`json.JSONDecoder.raw_decode`,
    which decodes exactly one value starting at that offset and stops at
    its closing brace -- it respects JSON string literals and escapes for
    free and tolerates arbitrary trailing content, so no hand-rolled
    depth/quote/escape walk is needed. The first offset that decodes to a
    ``dict`` is returned; a non-object value (a bare array or number that
    happens to start with ``{`` cannot, but defensively) advances to the
    next candidate rather than aborting the scan.

    When a candidate offset ``i`` fails to decode, the scan resumes at
    ``max(i + 1, exc.pos)`` rather than ``i + 1``: ``exc.pos`` is the
    offset the decoder reached before giving up, so every ``{`` it
    already consumed inside the failed span (including the many braces
    buried in a long or unterminated string literal) is skipped instead
    of re-attempted. This turns the worst case from O(n.k) -- k failing
    candidates each re-walked over the same consumed bytes -- into a
    single forward sweep, which is what bounds the pathological
    adversarial-preamble path the length cap below only partially
    guards. Skipping the consumed span also tightens the function to its
    stated "first TOP-LEVEL object" contract: bytes inside a malformed
    or unterminated outer value are no longer mined for a nested object
    the caller never asked for (the old ``i + 1`` walk would re-enter the
    failed span and surface such an inner object). Every offset at or
    past ``exc.pos`` is still scanned, so a genuine top-level object that
    follows the failed span is found unchanged.

    A hard length cap (``max_bytes``, default 1 MiB) is enforced before
    any scanning so an adversarial preamble on the untrusted gemini-stdout
    path cannot push the parser into a pathological repeated-decode walk.
    Inputs over the cap raise ``ValueError``; the bench records that as an
    invariant failure, while the driver treats it as a soft parse failure
    and returns ``None``.

    Returns the first balanced object as a ``dict``, or ``None`` if no
    object is found. Raises ``ValueError`` on input over the length cap.
    """
    if len(blob) > max_bytes:
        raise ValueError(f"scan_balanced_json input {len(blob)} bytes > max_bytes={max_bytes}")
    decoder = json.JSONDecoder()
    start = blob.find("{")
    while start != -1:
        try:
            obj, _end = decoder.raw_decode(blob, start)
        except json.JSONDecodeError as exc:
            # Resume past the bytes the decoder already consumed (``exc.pos``)
            # so braces inside the failed span are not re-attempted: O(n) sweep,
            # not an O(n.k) re-walk. ``max(start + 1, ...)`` guarantees forward
            # progress even when ``exc.pos`` lands at ``start`` (empty-prefix
            # failure).
            start = blob.find("{", max(start + 1, exc.pos))
            continue
        if isinstance(obj, dict):
            return obj
        start = blob.find("{", start + 1)
    return None


class ObservedReaction(msgspec.Struct, kw_only=True, frozen=True):
    """One driver's reaction to the orchestrator's seed event.

    Captured by the orchestrator from the driver's wake-marker stdout
    line + (for LLM drivers) the parsed token-usage envelope.

    ``reaction_latency_ms`` is the per-driver bus-ingest latency on
    the cross-process Linux ``CLOCK_MONOTONIC`` clock: the orchestrator
    captures ``seed_emit_monotonic_ns`` at seed-emit time, the driver
    captures ``wake_monotonic_ns`` immediately after its ``wait_for``
    returned, and the field is the difference in milliseconds. The
    same anchor pair drives the verdict-level aggregate latency so a
    row-level consumer matches the verdict-level summary; both are
    free of wall-clock skew and LLM-call jitter (the early-wake
    marker is emitted BEFORE any post-wake LLM exercise). The
    construction default ``0.0`` is the sentinel for "the driver did
    not emit a WAKE_RECEIVED marker so the monotonic anchor is
    missing".

    ``token_usage`` is ``None`` for the non-LLM drivers (pydantic /
    langgraph / shell-control) and populated for the LLM drivers
    (claude-cli / gemini-cli).

    ``provider`` carries the model path the driver actually ran on
    (``openai-gpt-4o-mini`` when a real OpenAI call landed for the
    pydantic / langgraph drivers; ``offline-testmodel`` /
    ``offline-fakelistchatmodel`` when the absent-key fallback engaged;
    ``claude-cli`` / ``gemini-cli`` for the CLI drivers; ``offline``
    for shell-control). Defaults to ``"unknown"`` so a legacy verdict
    file lacking the field decodes cleanly.
    """

    framework: str
    fw_id: str
    seed_delivery_id: str
    reaction_delivery_id: str
    received_wall_ns: int
    reaction_latency_ms: float
    token_usage: TokenUsage | None = None
    provider: str = "unknown"
    token_usage_parse_failed: bool = False
    """``True`` iff the driver's wake marker carried a ``token_usage``
    payload that could not be decoded into a ``TokenUsage`` (wrong shape
    or failed validation). Distinct from ``token_usage is None``, which
    means the driver legitimately reported no usage (a non-LLM driver).
    A parse failure is routed into the verdict's invariant-failure
    accounting so a dropped envelope is observable rather than silently
    swallowed. The construction default ``False`` is the clean shape and
    keeps a legacy verdict file decoding cleanly."""


class RealCurvePoint(msgspec.Struct, kw_only=True, frozen=True):
    """One (N, framework-mix, observed reactions) row from a real-mode window.

    Lives alongside the offline ``CurvePoint`` curve; the real-mode
    verdict captures heterogeneous-agent cross-broadcast proof
    semantics rather than synthetic-load throughput.

    Fields:

    - ``framework_mix``: per-framework driver-count breakdown the
      orchestrator spawned (`{"pydantic": 1, "langgraph": 1, ...}`).
    - ``observed_reactions``: every driver-reaction the orchestrator
      received back on the bus during the measurement window.
    - ``cross_broadcast_proven``: true iff every spawned framework
      produced at least one reaction AND the total reaction count
      equals the seeded driver count.
    - ``auth_provenance``: opaque-to-the-verdict provenance fields
      from ``auth_smoke_check`` so the verdict carries the CLI
      versions that produced the run.
    """

    n: int
    framework_mix: dict[str, int]
    seed_delivery_id: str
    observed_reactions: tuple[ObservedReaction, ...]
    cross_broadcast_proven: bool
    unique_frameworks_observed: int
    reactions_received: int
    reactions_expected: int
    median_reaction_latency_ms: float
    p99_reaction_latency_ms: float
    total_token_usage: TokenUsage
    duration_window_sec: float
    auth_provenance: dict[str, str]


class CurvePoint(msgspec.Struct, kw_only=True, frozen=True):
    """One ``(N, throughput, latency-percentiles)`` row in the curve sweep.

    Populated by ``_controller`` during the per-N sweep. ``p99_ci_low``
    / ``p99_ci_high`` are the bootstrap (or Wilson) CI bounds on p99;
    ``n_samples`` is the post-warmup-window sample count and gates the
    percentile via the ``N_samples >= 10 / (1 - p)`` rule (a
    ``CurvePoint`` with ``insufficient_samples=True`` carries
    indicative percentiles only).
    """

    n: int
    throughput_hz: float
    p50_seconds: float
    p99_seconds: float
    p99_ci_low_seconds: float
    p99_ci_high_seconds: float
    n_samples: int
    insufficient_samples: bool = False


class _VerdictDoc(msgspec.Struct, kw_only=True, frozen=True):
    """Typed verdict document; the JSON wire contract for ``verdict.json``.

    Serialised via ``msgspec.to_builtins(doc)`` at the write site --
    idiomatic msgspec, no bespoke ``to_dict()`` method. The full field
    surface is declared up front so consumers can rely on the schema
    even when a given run populates only a subset (e.g. a
    ``--signals zero_poll`` invocation leaves ``curve`` empty).
    """

    started_at_ns: int
    ended_at_ns: int
    duration_sec: float
    mode: str  # "offline" | "real"
    overall_passed: bool
    failures: tuple[StressSignalFailure, ...] = ()

    # Per-N curve sweep (populated by ``_controller``).
    curve: tuple[CurvePoint, ...] = ()

    # Per-N real-mode curve sweep -- empty in offline mode, populated by
    # ``_controller.run_real_window`` when the operator passes ``--real``.
    # Backwards-compatible default keeps the JSON wire contract additive.
    real_curve_points: tuple[RealCurvePoint, ...] = ()

    # USL fit results (populated by ``_usl.fit_usl``). All ``None``
    # when the sweep produced fewer than the minimum points required
    # for a stable 3-parameter regression.
    usl_alpha: float | None = None
    usl_beta: float | None = None
    usl_gamma: float | None = None
    knee_concurrency: float | None = None
    knee_throughput_hz: float | None = None

    # Zero-polling structural assertion (populated by the
    # ``tests/test_zero_polling.py`` harness when invoked through the
    # stress controller). ``syscall_count`` is the ``perf stat -e
    # raw_syscalls:sys_enter`` count over the idle window.
    zero_polling_verdict: dict[str, Any] | None = None

    # Per-N close-reason tally (populated by ``_scrape`` reading
    # ``waitbus_subscriber_evicted_total{reason}`` from the daemon's
    # ``metrics_snapshot`` log line).
    subscriber_close_reasons: dict[str, int] = {}

    # Optional per-N raw HDR percentile dump path (lifted from
    # ``benchmarks._harness.write_result``). ``None`` when the operator
    # passed ``--no-dump``.
    hdr_dump_path: str | None = None

    # Count of real-mode reactions whose driver did not surface a
    # per-call billing figure (gemini free-tier path). A non-zero count
    # means the corresponding driver's contribution to ``total_cost_usd``
    # is unknown rather than known-to-be-zero. Surfaces the gap the
    # gemini ``cost_usd=None`` contract requires so downstream
    # cost-ranking does not silently treat unknown-cost drivers as free.
    cost_unknown_count: int = 0

    # Count of real-mode reactions whose token envelope surfaced a
    # moderation refusal or upstream error (``stop_reason="refusal"``
    # or ``is_error=True``). A non-zero count means the corresponding
    # iteration was not a successful no-op; verdict aggregation that
    # ignores this field overcounts the success rate.
    invariant_failure_count: int = 0

    # Per-provider reaction count across every real-mode window. Keys
    # are the provider identifiers stamped on the wake-marker line
    # (``openai-gpt-4o-mini`` / ``offline-testmodel`` /
    # ``offline-fakelistchatmodel`` / ``claude-cli`` / ``gemini-cli`` /
    # ``offline``). Empty dict on offline-only runs.
    provider_distribution: dict[str, int] = {}

    # Per-iteration (source, event_type) draw histogram across every
    # real-mode window. Keys are the source names from
    # ``benchmarks._source_taxonomy.SOAK_SOURCE_REGISTRY``
    # (``github`` / ``pytest`` / ``docker`` / ``fs`` / ``agent``).
    # Values sum to the number of windows the sweep ran. Empty dict on
    # offline-only runs; a downstream consumer can use this to confirm
    # the daemon's fan-out was exercised across the registered taxonomy
    # at the run's representative load rather than only the historical
    # ``(agent, agent_message)`` slice.
    per_iter_source_distribution: dict[str, int] = {}


class _StressContext(msgspec.Struct, kw_only=True, frozen=True):
    """Immutable per-run configuration threaded by reference.

    Mirrors the ``_SoakContext`` shape: passing one frozen struct
    instead of a kwarg fan-out eliminates fan code, makes call sites
    read left-to-right, and catches missing-field errors at
    construction time. Fields are the per-run startup values that do
    not change after ``_controller.main`` startup.
    """

    proc: subprocess.Popen[bytes] | None
    db_path: Path
    progress_path: Path
    socket_path: Path
    daemon_stderr_path: Path
    args: argparse.Namespace
    start_monotonic: float
    started_at_ns: int
    total_seconds: float
    mode: str  # "offline" | "real"
    sweep_n: tuple[int, ...]
    corpus_iter: Iterator[dict[str, Any] | None] | None
    progress_fh: Any  # open file handle for long-lived FD pattern (IO[str])


class _StressAccumulators(msgspec.Struct, kw_only=True):
    """Mutable container holding the appendable sample lists.

    NOT frozen: a frozen struct holding mutable list fields is a footgun
    (the list reference is frozen, the list contents are not, leading to
    surprising sharing semantics). Owned by ``_controller.main``, mutated
    in-place by the run-step helpers in ``_sources`` / ``_ledger`` /
    ``_scrape``.
    """

    curve_points: list[CurvePoint] = []
    source_counts: dict[str, int] = {}
    subscriber_close_reasons: dict[str, int] = {}
    zero_polling_observations: list[dict[str, Any]] = []
    fault_injection_outcomes: list[dict[str, Any]] = []


@dataclasses.dataclass
class _StressState:
    """Mutable scalar state threaded through the per-N step helpers.

    Distinct from ``_StressAccumulators`` because these are scalar
    cursors (next-N, next-tick) rather than appendable sample lists,
    and the dataclass form keeps them assignment-friendly without
    msgspec's frozen-by-default discipline getting in the way.
    """

    current_n_index: int = 0
    next_scrape_monotonic: float = 0.0
    next_ledger_flush_monotonic: float = 0.0
