"""Tests for the real-mode driver entry points and token-envelope parsers.

Three concerns covered:

- Token envelope parsers (``parse_claude_envelope`` /
  ``parse_gemini_envelope``) handle the production envelope shapes and degrade to ``None`` on malformed input.
- The framework-mix split factory honours the equal-share +
  round-robin-remainder contract at the canonical N values the
  controller invokes it with.
- The msgspec shapes (``TokenUsage`` / ``ObservedReaction`` /
  ``RealCurvePoint``) round-trip through ``msgspec.to_builtins`` so
  the verdict JSON wire contract holds for the new captures.

The auth smoke check is exercised via the module entry point as a
subprocess invocation (no actual LLM call -- ``--version`` only).
The whole test module skips when ``claude`` and ``gemini`` are not
on ``PATH`` so a CI runner without the LLM CLIs still passes.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import msgspec
import pytest

from scripts.stress._context import (
    DEFAULT_SCAN_BALANCED_JSON_MAX_BYTES,
    ObservedReaction,
    RealCurvePoint,
    TokenUsage,
)
from scripts.stress._real_drivers import (
    FRAMEWORK_ORDER,
    WAKE_MARKER,
    framework_mix_for,
    parse_claude_envelope,
    parse_gemini_envelope,
)

# --- Token envelope parsing ------------------------------------------------


def test_parse_claude_envelope_recovers_input_output_cost() -> None:
    """The claude ``--output-format=json`` envelope round-trips into TokenUsage.

    Asserts on the visible / output / cost trio plus the cache-discount
    fields the parser now captures: ``billed_input_tokens`` must equal
    ``input + cache_creation + cache_read`` (Anthropic billing
    semantics). Before the fix the parser dropped the cache fields and
    underreported billed input by the full cache-creation + cache-read
    sum.
    """
    stdout = json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "result": "ok",
            "total_cost_usd": 0.26001375,
            "usage": {
                "input_tokens": 6,
                "output_tokens": 6,
                "cache_creation_input_tokens": 39581,
                "cache_read_input_tokens": 24905,
            },
        }
    )
    usage = parse_claude_envelope(stdout)
    assert usage is not None
    assert usage.input_tokens == 6
    assert usage.output_tokens == 6
    assert usage.cost_usd == pytest.approx(0.26001375)
    # Cache fields captured + billed-input derivation.
    assert usage.cache_creation_input_tokens == 39581
    assert usage.cache_read_input_tokens == 24905
    assert usage.billed_input_tokens == 6 + 39581 + 24905


# Live-recorded normal-path envelope (claude -p --output-format=json haiku).
# Operator-captured 2026-06-01; cache_creation=56727, visible=10, output=74,
# billed=56737. Source-of-truth shape for the cache-counted parser. The same
# captured JSON lives at tests/fixtures/claude_envelope_haiku.json as the
# operator's working reference; the inline copy here is what the test relies
# on so the suite stays self-contained.
_CLAUDE_ENVELOPE_HAIKU_LIVE = (
    '{"type":"result","subtype":"success","is_error":false,"api_error_status":null,'
    '"duration_ms":3296,"duration_api_ms":3082,"ttft_ms":2880,"num_turns":1,'
    '"result":"Hi","stop_reason":"end_turn",'
    '"session_id":"1bc43cb2-198d-4d6e-8ec8-dc9849204a63",'
    '"total_cost_usd":0.07128875,'
    '"usage":{"input_tokens":10,"cache_creation_input_tokens":56727,'
    '"cache_read_input_tokens":0,"output_tokens":74,'
    '"service_tier":"standard"},'
    '"modelUsage":{"claude-haiku-4-5-20251001":{"inputTokens":10,"outputTokens":74,'
    '"cacheReadInputTokens":0,"cacheCreationInputTokens":56727,"costUSD":0.07128875}},'
    '"terminal_reason":"completed"}'
)


def test_claude_envelope_cache_tokens_counted_live_haiku() -> None:
    """The live-recorded haiku envelope yields the cache-discounted billed_input.

    Asserts the parser captures the Anthropic billing surface that was
    previously dropped: with the recorded shape (visible=10,
    cache_creation=56727, cache_read=0), ``billed_input_tokens``
    must equal 56737 -- a 5673x understatement vs the prior parser
    that returned only ``input_tokens=10``.
    """
    usage = parse_claude_envelope(_CLAUDE_ENVELOPE_HAIKU_LIVE)
    assert usage is not None
    assert usage.input_tokens == 10
    assert usage.cache_creation_input_tokens == 56727
    assert usage.cache_read_input_tokens == 0
    assert usage.output_tokens == 74
    assert usage.billed_input_tokens == 56737
    assert (
        usage.billed_input_tokens
        == usage.input_tokens + usage.cache_creation_input_tokens + usage.cache_read_input_tokens
    )
    assert usage.cost_usd == pytest.approx(0.07128875)
    # The live envelope carries the model id as a KEY under modelUsage
    # (not a top-level "model" field); the parser now captures it.
    assert usage.model == "claude-haiku-4-5-20251001"
    assert usage.service_tier == "standard"
    assert usage.terminal_reason == "completed"
    assert usage.is_error is False


def test_claude_envelope_model_id_from_model_usage_key() -> None:
    """The model id is captured from the modelUsage key, not recorded as "unknown".

    Regression guard for the attribution defect: the real envelope carries
    the model id as a KEY under ``modelUsage`` and has no top-level
    ``model`` field. A single-model turn must surface that key.
    """
    stdout = json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "total_cost_usd": 0.01,
            "usage": {"input_tokens": 10, "output_tokens": 5},
            "modelUsage": {
                "claude-opus-4-8-20260101": {
                    "inputTokens": 10,
                    "outputTokens": 5,
                    "costUSD": 0.01,
                }
            },
        }
    )
    usage = parse_claude_envelope(stdout)
    assert usage is not None
    assert usage.model == "claude-opus-4-8-20260101"
    assert usage.model != "unknown"


def test_claude_envelope_model_id_picks_dominant_model_on_multi_model_turn() -> None:
    """On a multi-model turn, the model with the most input tokens is recorded."""
    stdout = json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "total_cost_usd": 0.02,
            "usage": {"input_tokens": 30, "output_tokens": 8},
            "modelUsage": {
                "claude-haiku-4-5-20251001": {"inputTokens": 5, "outputTokens": 2},
                "claude-opus-4-8-20260101": {"inputTokens": 25, "outputTokens": 6},
            },
        }
    )
    usage = parse_claude_envelope(stdout)
    assert usage is not None
    assert usage.model == "claude-opus-4-8-20260101"


def test_claude_envelope_model_id_falls_back_to_top_level_then_unknown() -> None:
    """Without modelUsage, fall back to a top-level model field, then "unknown"."""
    with_top_level = json.dumps(
        {
            "type": "result",
            "total_cost_usd": 0.01,
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "model": "claude-sonnet-4-5",
        }
    )
    usage = parse_claude_envelope(with_top_level)
    assert usage is not None
    assert usage.model == "claude-sonnet-4-5"

    no_model = json.dumps(
        {
            "type": "result",
            "total_cost_usd": 0.01,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
    )
    usage_no_model = parse_claude_envelope(no_model)
    assert usage_no_model is not None
    assert usage_no_model.model == "unknown"


# Synthetic claude refusal envelope. Mirrors the documented Anthropic
# refusal shape (stop_reason="refusal", is_error=True, output_tokens=0,
# terminal_reason="error_during_execution"). Operator should re-record
# this as a live envelope when next a refusal prompt fires; the
# structural assertions below would carry over verbatim.
_CLAUDE_ENVELOPE_REFUSAL_SYNTHETIC = (
    '{"type":"result","subtype":"error","is_error":true,'
    '"api_error_status":null,"num_turns":1,'
    '"stop_reason":"refusal","terminal_reason":"error_during_execution",'
    '"total_cost_usd":0.00031875,'
    '"usage":{"input_tokens":18,"cache_creation_input_tokens":0,'
    '"cache_read_input_tokens":0,"output_tokens":0,"service_tier":"standard"}}'
)


def test_claude_envelope_moderation_refusal_surfaces() -> None:
    """A refusal envelope surfaces stop_reason + is_error so the orchestrator can flag it.

    Before the fix the parser dropped these fields and the orchestrator
    saw a successful no-op (output_tokens=0 with cost_usd>0) -- an
    invariant failure that looked like a clean pass.
    """
    usage = parse_claude_envelope(_CLAUDE_ENVELOPE_REFUSAL_SYNTHETIC)
    assert usage is not None
    assert usage.is_error is True
    assert usage.stop_reason == "refusal"
    assert usage.terminal_reason == "error_during_execution"
    assert usage.output_tokens == 0


def test_claude_envelope_normal_path_no_cache() -> None:
    """An envelope without cache fields yields cache=0 and billed==visible."""
    stdout = json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "result": "ok",
            "total_cost_usd": 0.001,
            "stop_reason": "end_turn",
            "is_error": False,
            "usage": {"input_tokens": 7, "output_tokens": 5},
        }
    )
    usage = parse_claude_envelope(stdout)
    assert usage is not None
    assert usage.cache_creation_input_tokens == 0
    assert usage.cache_read_input_tokens == 0
    assert usage.billed_input_tokens == usage.input_tokens
    assert usage.is_error is False
    assert usage.stop_reason == "end_turn"


def test_parse_claude_envelope_returns_none_for_malformed_input() -> None:
    """Garbage / partial JSON / wrong type field all degrade to None, not raise."""
    assert parse_claude_envelope("") is None
    assert parse_claude_envelope("not json at all") is None
    assert parse_claude_envelope('{"type":"not_result"}') is None
    assert parse_claude_envelope('{"type":"result","usage":"oops"}') is None
    assert parse_claude_envelope('{"type":"result","usage":{"input_tokens":"oops"}}') is None


def test_parse_claude_envelope_picks_last_result_line_in_stream() -> None:
    """A streaming claude run may emit several JSON frames; only the result counts."""
    stream = (
        '{"type":"assistant","content":"thinking..."}\n'
        '{"type":"tool_use","name":"foo"}\n'
        '{"type":"result","total_cost_usd":0.05,"usage":{"input_tokens":1,"output_tokens":2}}\n'
    )
    usage = parse_claude_envelope(stream)
    assert usage is not None
    assert usage.input_tokens == 1
    assert usage.output_tokens == 2


def test_parse_gemini_envelope_sums_tokens_across_models() -> None:
    """Gemini's per-model token blocks are summed (multi-model runs are real)."""
    stdout = """Loaded cached credentials.
{
  "session_id": "abc",
  "response": "Ready.",
  "stats": {
    "models": {
      "gemini-3.1-flash-lite": {
        "tokens": {"prompt": 2816, "candidates": 31}
      },
      "gemini-3-flash-preview": {
        "tokens": {"prompt": 2773, "candidates": 2}
      }
    }
  }
}"""
    usage = parse_gemini_envelope(stdout)
    assert usage is not None
    assert usage.input_tokens == 2816 + 2773
    assert usage.output_tokens == 31 + 2
    # Gemini CLI does not surface a per-call cost on the free-tier path;
    # the parser reports cost_usd=None (NOT 0.0) so a non-zero-cost
    # driver cannot be silently treated as free. Downstream summers skip
    # None and surface the unknown-cost reaction count separately.
    assert usage.cost_usd is None


def test_parse_gemini_envelope_returns_none_for_missing_models_block() -> None:
    """An envelope without stats.models returns None."""
    assert parse_gemini_envelope("") is None
    assert parse_gemini_envelope('{"session_id":"x"}') is None
    assert parse_gemini_envelope('{"stats":{"no_models":true}}') is None


# Live-recorded gemini envelope (gemini -p -o json --model=gemini-2.5-flash).
# Operator-captured 2026-06-01; the stdout includes the cached-credentials
# preamble plus two MCP-discovery error lines before the JSON envelope. The
# tokens leaf carries the thinking surface (thoughts=20) the prior parser
# dropped. Same captured payload lives at tests/fixtures/gemini_envelope_flash.json
# as the operator's working reference.
_GEMINI_STDOUT_WITH_MCP_PREAMBLE_LIVE = (
    "Loaded cached credentials.\n"
    "Error during discovery for MCP server 'quint-code': "
    "spawn /tmp/go-build3017538251/b001/cmd.test ENOENT"
    "Error during discovery for MCP server 'quint': "
    "spawn /tmp/go-build1995564725/b559/cmd.test ENOENT"
    "{\n"
    '  "session_id": "c8981c8d-77bb-4196-8857-01ddca26cfcd",\n'
    '  "response": "hello",\n'
    '  "stats": {\n'
    '    "models": {\n'
    '      "gemini-2.5-flash": {\n'
    '        "api": {\n'
    '          "totalRequests": 1,\n'
    '          "totalErrors": 0,\n'
    '          "totalLatencyMs": 1373\n'
    "        },\n"
    '        "tokens": {\n'
    '          "input": 8840,\n'
    '          "prompt": 8840,\n'
    '          "candidates": 1,\n'
    '          "total": 8861,\n'
    '          "cached": 0,\n'
    '          "thoughts": 20,\n'
    '          "tool": 0\n'
    "        }\n"
    "      }\n"
    "    },\n"
    '    "tools": {\n'
    '      "totalCalls": 0\n'
    "    }\n"
    "  }\n"
    "}"
)


def test_gemini_envelope_thinking_and_cached_counted_live() -> None:
    """The live-recorded gemini envelope captures thoughts + cached + tool tokens.

    Asserts the parser now surfaces thoughts_tokens (the live capture
    recorded 20; Gemini 2.5 Flash with thinking enabled routinely
    produces hundreds to thousands per call, but the structural
    assertion is the same -- the field must be non-zero on the
    fixture and the drift gate ``total_tokens_recomputed`` must equal
    ``prompt + candidates + thoughts + tool``.
    """
    usage = parse_gemini_envelope(_GEMINI_STDOUT_WITH_MCP_PREAMBLE_LIVE)
    assert usage is not None
    assert usage.input_tokens == 8840  # prompt
    assert usage.output_tokens == 1  # candidates
    assert usage.thoughts_tokens == 20
    assert usage.cached_tokens == 0
    assert usage.tool_tokens == 0
    assert usage.total_tokens_reported == 8861
    # Drift gate: parser's own recompute matches reported total within +/- 1
    # (the reported total may include rounding that the per-leaf sum does not).
    assert abs(usage.total_tokens_recomputed - usage.total_tokens_reported) <= 1


def test_gemini_envelope_with_mcp_preamble_does_not_break_parser() -> None:
    """The MCP-discovery error preamble + cached-credentials line are skipped.

    Before the brace-balanced scanner, the parser used
    ``stdout.find("{")`` + ``json.loads(stdout[first_brace:])`` which
    failed when MCP errors put trailing content the json decoder
    cannot consume. The balanced-scan helper finds the first complete
    top-level object regardless of preamble.
    """
    usage = parse_gemini_envelope(_GEMINI_STDOUT_WITH_MCP_PREAMBLE_LIVE)
    assert usage is not None
    assert usage.model == "gemini-2.5-flash"


def test_gemini_envelope_strings_with_braces_dont_confuse_scanner() -> None:
    """A ``{`` character inside a JSON string literal must NOT confuse the brace scanner.

    Regression for the naive find("{") path. The balanced-scan helper
    must respect string literals and not count braces inside ``"..."``.
    """
    blob = (
        'Error: foo "{not json}" bar\n'
        '{"stats": {"models": {"gemini-2.5-flash": '
        '{"tokens": {"prompt": 5, "candidates": 7, "thoughts": 3, "cached": 0, "tool": 0, "total": 15}}}}}'
    )
    usage = parse_gemini_envelope(blob)
    assert usage is not None
    assert usage.input_tokens == 5
    assert usage.output_tokens == 7
    assert usage.thoughts_tokens == 3


def test_gemini_envelope_oversized_untrusted_stdout_bounded_to_none() -> None:
    """An over-cap (adversarial) gemini stdout is bounded, not parsed unboundedly.

    ``parse_gemini_envelope`` reads UNTRUSTED CLI stdout. The shared
    scanner caps its input at 1 MiB and raises ``ValueError`` over the
    cap; the gemini path swallows that into a soft ``None`` so an
    adversarial multi-megabyte preamble cannot push the parser into a
    pathological repeated-decode walk. This is the regression guard for
    the previously-uncapped scanner.
    """
    oversized = "{" * (DEFAULT_SCAN_BALANCED_JSON_MAX_BYTES + 1)
    assert len(oversized) > DEFAULT_SCAN_BALANCED_JSON_MAX_BYTES
    assert parse_gemini_envelope(oversized) is None


def test_gemini_cost_is_none_not_zero() -> None:
    """``cost_usd`` is ``None`` on the gemini path; explicit unknown, not silently zero.

    Before this fix the parser passed ``cost_usd=0.0`` even though the
    gemini CLI does not surface a per-call billing figure. That made
    gemini look free in cross-driver cost rankings. ``None`` makes the
    gap explicit so the orchestrator can surface the unknown-cost
    reaction count in the verdict instead.
    """
    usage = parse_gemini_envelope(_GEMINI_STDOUT_WITH_MCP_PREAMBLE_LIVE)
    assert usage is not None
    assert usage.cost_usd is None


# --- Framework mix split ---------------------------------------------------


def test_framework_mix_at_n5_is_one_each() -> None:
    """The canonical N=5 sweep gets exactly one driver per framework."""
    mix = framework_mix_for(5)
    assert mix == {fw: 1 for fw in FRAMEWORK_ORDER}
    assert sum(mix.values()) == 5


def test_framework_mix_at_n10_is_two_each() -> None:
    """The canonical N=10 sweep gets exactly two drivers per framework."""
    mix = framework_mix_for(10)
    assert mix == {fw: 2 for fw in FRAMEWORK_ORDER}
    assert sum(mix.values()) == 10


def test_framework_mix_round_robin_remainder() -> None:
    """N=7 distributes the 2-driver remainder starting with the first frameworks."""
    mix = framework_mix_for(7)
    assert sum(mix.values()) == 7
    # First two frameworks get the remainder.
    assert mix["pydantic"] == 2
    assert mix["langgraph"] == 2
    assert mix["claude-cli"] == 1
    assert mix["gemini-cli"] == 1
    assert mix["shell-control"] == 1


def test_framework_mix_rejects_negative_n() -> None:
    """A negative N is a programmer error; the factory raises rather than wrapping."""
    with pytest.raises(ValueError):
        framework_mix_for(-1)


def test_framework_mix_at_n0_is_all_zero() -> None:
    """N=0 is the no-op shape: every framework gets 0 driver slots."""
    mix = framework_mix_for(0)
    assert mix == {fw: 0 for fw in FRAMEWORK_ORDER}


# --- msgspec round-trip discipline ----------------------------------------


def test_token_usage_round_trips_through_msgspec_to_builtins() -> None:
    """``msgspec.to_builtins(TokenUsage)`` yields a JSON-encodable dict.

    Asserts on the visible / output / cost trio rather than full dict
    equality so the union-superset fields (cache discount, thinking,
    moderation provenance) can grow additively without churning this
    test on every shape addition.
    """
    usage = TokenUsage(input_tokens=10, output_tokens=20, cost_usd=0.123)
    obj = msgspec.to_builtins(usage)
    assert obj["input_tokens"] == 10
    assert obj["output_tokens"] == 20
    assert obj["cost_usd"] == 0.123
    # Defaults populate the rest of the union surface.
    assert obj["cache_creation_input_tokens"] == 0
    assert obj["cache_read_input_tokens"] == 0
    assert obj["billed_input_tokens"] == 0
    assert obj["thoughts_tokens"] == 0
    assert obj["is_error"] is False
    assert obj["stop_reason"] is None
    # The JSON encode must succeed without a custom encoder.
    assert json.loads(json.dumps(obj)) == obj


def test_observed_reaction_round_trips_with_optional_token_usage() -> None:
    """``ObservedReaction.token_usage`` is None for non-LLM drivers; the shape encodes."""
    reaction = ObservedReaction(
        framework="shell-control",
        fw_id="shell-1",
        seed_delivery_id="seed-1",
        reaction_delivery_id="reaction-1",
        received_wall_ns=1_000_000_000,
        reaction_latency_ms=12.5,
        token_usage=None,
    )
    obj = msgspec.to_builtins(reaction)
    assert obj["framework"] == "shell-control"
    assert obj["token_usage"] is None
    # The additive provider field rides every round-trip; default is "unknown".
    assert obj["provider"] == "unknown"
    assert json.loads(json.dumps(obj))["reaction_latency_ms"] == 12.5


def test_real_curve_point_round_trips_with_full_observed_reactions() -> None:
    """A populated ``RealCurvePoint`` serialises into the canonical wire shape."""
    usage = TokenUsage(input_tokens=10, output_tokens=20, cost_usd=0.01)
    reaction = ObservedReaction(
        framework="claude-cli",
        fw_id="claude-1",
        seed_delivery_id="seed-A",
        reaction_delivery_id="reaction-A",
        received_wall_ns=2_000_000_000,
        reaction_latency_ms=42.0,
        token_usage=usage,
    )
    point = RealCurvePoint(
        n=5,
        framework_mix={fw: 1 for fw in FRAMEWORK_ORDER},
        seed_delivery_id="seed-A",
        observed_reactions=(reaction,),
        cross_broadcast_proven=False,
        unique_frameworks_observed=1,
        reactions_received=1,
        reactions_expected=5,
        median_reaction_latency_ms=42.0,
        p99_reaction_latency_ms=42.0,
        total_token_usage=usage,
        duration_window_sec=60.0,
        auth_provenance={"claude": "2.1.0", "gemini": "0.29.0", "waitbus": "/usr/bin/waitbus"},
    )
    obj = msgspec.to_builtins(point)
    assert obj["n"] == 5
    assert obj["reactions_received"] == 1
    assert obj["observed_reactions"][0]["token_usage"]["input_tokens"] == 10
    # Provider rides into the inner observed-reaction dict additively.
    assert obj["observed_reactions"][0]["provider"] == "unknown"
    # Full JSON round-trip discipline.
    assert json.loads(json.dumps(obj))["n"] == 5


# --- Cross-broadcast proof truth table (semantic) --------------------------


def _proven(reactions_received: int, unique: int, n: int) -> bool:
    """Local helper mirroring the controller's truth-table semantic."""
    return reactions_received == n and unique == len(FRAMEWORK_ORDER)


def test_cross_broadcast_proven_truth_table() -> None:
    """The proof flag is true iff every framework woke AND every driver reacted."""
    # Healthy N=5: 5 reactions, 5 frameworks observed.
    assert _proven(reactions_received=5, unique=5, n=5) is True
    # One framework missing: not proven.
    assert _proven(reactions_received=4, unique=4, n=5) is False
    # All reactions received but only 4 frameworks (one driver doubled): not proven.
    assert _proven(reactions_received=5, unique=4, n=5) is False
    # N=10 healthy: 10 reactions, 5 distinct frameworks.
    assert _proven(reactions_received=10, unique=5, n=10) is True


# --- CLI-driver failure taxonomy (P0: timeout vs auth vs refusal) ----------
#
# The three failure classes the prior ``subprocess.run(check=True)`` +
# ``except (CalledProcessError, TimeoutExpired)`` shape collapsed into a
# single shared exit code are now distinguishable. These tests drive
# ``_run_cli_driver`` (via the public ``run_claude_cli_driver`` /
# ``run_gemini_cli_driver`` delegates) with a stubbed seed-wait,
# ``shutil.which``, and ``subprocess.run`` so no real LLM call fires, and
# assert BOTH the distinct exit code AND whether a reaction reached the
# bus per class.


class _FakeCompletedProcess:
    """Minimal stand-in for ``subprocess.CompletedProcess`` (text mode)."""

    def __init__(self, *, returncode: int, stdout: str, stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


_CLI_SUCCESS_ENVELOPE = json.dumps(
    {
        "type": "result",
        "subtype": "success",
        "total_cost_usd": 0.001,
        "stop_reason": "end_turn",
        "is_error": False,
        "usage": {"input_tokens": 7, "output_tokens": 5},
    }
)


def _patch_cli_driver_deps(
    monkeypatch: pytest.MonkeyPatch,
    *,
    run_result: Any,
    run_raises: BaseException | None = None,
) -> dict[str, Any]:
    """Stub the seed-wait, PATH lookup, subprocess call, and emit helpers.

    Returns a recorder dict capturing whether a reaction / wake marker was
    emitted (and the token_usage each carried) so a test can assert the
    orchestrator-observable side effects per failure class.
    """
    import scripts.stress._real_drivers as drv

    recorder: dict[str, Any] = {
        "reaction_emitted": False,
        "reaction_token_usage": None,
        "marker_emitted": False,
        "marker_token_usage": None,
    }

    monkeypatch.setattr(
        drv,
        "_wait_for_seed_via_subscribe",
        lambda **_kwargs: "seed-delivery-id-xyz",
    )
    monkeypatch.setattr(shutil, "which", lambda _binary: f"/usr/bin/{_binary}")

    def _fake_run(*_args: Any, **_kwargs: Any) -> Any:
        if run_raises is not None:
            raise run_raises
        return run_result

    monkeypatch.setattr(subprocess, "run", _fake_run)

    def _fake_emit_reaction(*, token_usage: Any, **_kwargs: Any) -> None:
        recorder["reaction_emitted"] = True
        recorder["reaction_token_usage"] = token_usage

    def _fake_emit_wake_marker(*, token_usage: Any, **_kwargs: Any) -> None:
        recorder["marker_emitted"] = True
        recorder["marker_token_usage"] = token_usage

    monkeypatch.setattr(drv, "_emit_reaction", _fake_emit_reaction)
    monkeypatch.setattr(drv, "_emit_wake_marker", _fake_emit_wake_marker)
    return recorder


_CLI_DRIVER_KWARGS: dict[str, Any] = {
    "socket_path": "NONE",
    "db_path": Path("/tmp/none.db"),
    "doorbell_path": Path("/tmp/none.sock"),
    "seed_scope_id": "stress-real-test",
    "fw_id": "cli-1",
}


@pytest.mark.parametrize(
    ("driver_fn_name", "binary"),
    [("run_claude_cli_driver", "claude"), ("run_gemini_cli_driver", "gemini")],
)
def test_cli_driver_timeout_returns_distinct_exit_code(
    monkeypatch: pytest.MonkeyPatch, driver_fn_name: str, binary: str
) -> None:
    """A hung LLM call (``TimeoutExpired``) maps to ``EXIT_LLM_TIMEOUT`` (3), no reaction.

    The timeout class is the one the prior shared ``except`` could not
    tell apart from an auth failure: both raised and returned the same
    code. Now ``TimeoutExpired`` is caught alone and routed to its own
    exit code, and no reaction reaches the bus (no envelope producible).
    """
    from scripts.stress import _real_drivers as drv

    recorder = _patch_cli_driver_deps(
        monkeypatch,
        run_result=None,
        run_raises=subprocess.TimeoutExpired(cmd=[binary], timeout=drv.LLM_CALL_TIMEOUT_SEC),
    )
    rc = getattr(drv, driver_fn_name)(**_CLI_DRIVER_KWARGS)
    assert rc == drv.EXIT_LLM_TIMEOUT
    assert rc == 3
    assert recorder["reaction_emitted"] is False
    assert recorder["marker_emitted"] is False


@pytest.mark.parametrize(
    ("driver_fn_name", "binary"),
    [("run_claude_cli_driver", "claude"), ("run_gemini_cli_driver", "gemini")],
)
def test_cli_driver_auth_error_returns_distinct_exit_code(
    monkeypatch: pytest.MonkeyPatch, driver_fn_name: str, binary: str
) -> None:
    """A non-zero exit with NO parseable envelope maps to ``EXIT_AUTH_OR_INVOCATION_ERROR`` (4).

    The auth / quota / invocation-crash class: the CLI exited non-zero
    and produced nothing the envelope parser can read. Distinct from a
    timeout (different exit code) and from a refusal (no reaction is
    emitted because there is no usable envelope).
    """
    from scripts.stress import _real_drivers as drv

    recorder = _patch_cli_driver_deps(
        monkeypatch,
        run_result=_FakeCompletedProcess(
            returncode=1,
            stdout="",
            stderr="Invalid API key. Please run /login",
        ),
    )
    rc = getattr(drv, driver_fn_name)(**_CLI_DRIVER_KWARGS)
    assert rc == drv.EXIT_AUTH_OR_INVOCATION_ERROR
    assert rc == 4
    assert recorder["reaction_emitted"] is False
    assert recorder["marker_emitted"] is False


def test_claude_cli_driver_refusal_returns_distinct_exit_code_and_still_reacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refusal envelope (exit 0) maps to ``EXIT_REFUSAL_OR_NONZERO_ENVELOPE`` (5) AND still reacts.

    A moderation refusal returns exit 0 from the CLI but carries
    ``stop_reason="refusal"`` / ``is_error=True`` in the envelope. The
    driver still emits the reaction + wake marker so the orchestrator's
    envelope path observes the refusal via the ``TokenUsage`` moderation
    fields; the distinct exit code lets an exit-code consumer tell it
    apart from the auth and timeout classes.
    """
    from scripts.stress import _real_drivers as drv

    recorder = _patch_cli_driver_deps(
        monkeypatch,
        run_result=_FakeCompletedProcess(returncode=0, stdout=_CLAUDE_ENVELOPE_REFUSAL_SYNTHETIC),
    )
    rc = drv.run_claude_cli_driver(**_CLI_DRIVER_KWARGS)
    assert rc == drv.EXIT_REFUSAL_OR_NONZERO_ENVELOPE
    assert rc == 5
    # The reaction reached the bus carrying the refusal envelope.
    assert recorder["reaction_emitted"] is True
    assert recorder["marker_emitted"] is True
    usage = recorder["marker_token_usage"]
    assert usage is not None
    assert usage.is_error is True
    assert usage.stop_reason == "refusal"


def test_cli_driver_nonzero_exit_with_usable_envelope_returns_refusal_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-zero exit that STILL produced a usable envelope routes to class 5, not 4.

    The boundary case: the CLI exited non-zero but emitted a parseable
    result frame. Because an envelope is available, the reaction is
    emitted and the outcome is the refusal-or-nonzero-envelope class
    (the orchestrator can read the envelope), NOT the auth/invocation
    class (which is reserved for the no-envelope case).
    """
    from scripts.stress import _real_drivers as drv

    recorder = _patch_cli_driver_deps(
        monkeypatch,
        run_result=_FakeCompletedProcess(returncode=2, stdout=_CLI_SUCCESS_ENVELOPE),
    )
    rc = drv.run_claude_cli_driver(**_CLI_DRIVER_KWARGS)
    assert rc == drv.EXIT_REFUSAL_OR_NONZERO_ENVELOPE
    assert recorder["reaction_emitted"] is True


@pytest.mark.parametrize(
    ("driver_fn_name", "binary"),
    [("run_claude_cli_driver", "claude"), ("run_gemini_cli_driver", "gemini")],
)
def test_cli_driver_clean_success_returns_exit_ok_and_reacts(
    monkeypatch: pytest.MonkeyPatch, driver_fn_name: str, binary: str
) -> None:
    """A clean exit-0 success with a non-refusal envelope returns ``EXIT_OK`` (0) and reacts."""
    from scripts.stress import _real_drivers as drv

    stdout = (
        _CLI_SUCCESS_ENVELOPE
        if binary == "claude"
        else json.dumps(
            {
                "session_id": "s",
                "stats": {"models": {"gemini-2.5-flash": {"tokens": {"prompt": 3, "candidates": 2, "total": 5}}}},
            }
        )
    )
    recorder = _patch_cli_driver_deps(
        monkeypatch,
        run_result=_FakeCompletedProcess(returncode=0, stdout=stdout),
    )
    rc = getattr(drv, driver_fn_name)(**_CLI_DRIVER_KWARGS)
    assert rc == drv.EXIT_OK
    assert rc == 0
    assert recorder["reaction_emitted"] is True
    assert recorder["marker_emitted"] is True


def test_cli_driver_three_failure_classes_have_distinct_exit_codes() -> None:
    """The three collapsed classes now map to three different exit codes.

    The regression guard for the P0: timeout / auth-error / refusal must
    not share a single exit value. This pins the contract at the
    constant level so a future edit that re-collapses any two trips the
    test.
    """
    from scripts.stress import _real_drivers as drv

    codes = {
        drv.EXIT_LLM_TIMEOUT,
        drv.EXIT_AUTH_OR_INVOCATION_ERROR,
        drv.EXIT_REFUSAL_OR_NONZERO_ENVELOPE,
    }
    assert len(codes) == 3
    # And none collides with the pre-subprocess classes.
    assert drv.EXIT_SEED_TIMEOUT not in codes
    assert drv.EXIT_NO_CLI not in codes
    assert drv.EXIT_OK not in codes


# --- Subprocess dispatch entry point --------------------------------------


def test_dispatch_entry_emits_wake_marker_for_shell_control_once_then_exit() -> None:
    """``python -m scripts.stress._real_drivers shell-control --once-then-exit`` emits the marker.

    Exercises the role-dispatch entry point + the wake-marker line
    format without needing a daemon: ``--once-then-exit`` short-
    circuits the wait/emit cycle. This covers the marker-line
    parser the orchestrator depends on.
    """
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.stress._real_drivers",
            "shell-control",
            "--socket",
            "NONE",
            "--db",
            "/tmp/none.db",
            "--doorbell",
            "/tmp/none.sock",
            "--seed-scope-id",
            "stress-real-test",
            "--fw-id",
            "test-shell-1",
            "--once-then-exit",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10.0,
    )
    assert proc.returncode == 0
    assert WAKE_MARKER in proc.stdout
    # Marker format: every required field present in the JSON payload.
    from scripts.stress._controller import _parse_wake_marker

    line = next(line for line in proc.stdout.splitlines() if line.startswith(WAKE_MARKER))
    fields = _parse_wake_marker(line)
    assert fields is not None
    assert fields["framework"] == "shell-control"
    assert fields["fw_id"] == "test-shell-1"
    assert fields["seed"] == "dispatch-smoke"
    assert fields["token_usage"] is None


def test_dispatch_entry_accepts_since_replay_cursor() -> None:
    """``--since <event_id>`` is accepted by the dispatch parser on every role.

    The shell-control ``--once-then-exit`` body short-circuits before any
    ``wait_for`` call so the dispatch parser is what gets exercised: a
    parse failure would prevent the wake-marker from being emitted, and
    a successful parse confirms ``--since`` rides through the per-role
    subparser added by the module's ``main``.
    """
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.stress._real_drivers",
            "shell-control",
            "--socket",
            "NONE",
            "--db",
            "/tmp/none.db",
            "--doorbell",
            "/tmp/none.sock",
            "--seed-scope-id",
            "stress-real-test",
            "--fw-id",
            "test-shell-since",
            "--since",
            "01H8XYZA01H8XYZA01H8XYZA01",
            "--once-then-exit",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10.0,
    )
    assert proc.returncode == 0
    assert WAKE_MARKER in proc.stdout


@pytest.mark.skipif(
    shutil.which("claude") is None or shutil.which("gemini") is None or shutil.which("waitbus") is None,
    reason="auth-smoke requires claude, gemini, and waitbus in PATH",
)
def test_auth_smoke_subcommand_returns_versions() -> None:
    """``python -m ... auth-smoke`` reports both LLM CLI versions and the waitbus path."""
    proc = subprocess.run(
        [sys.executable, "-m", "scripts.stress._real_drivers", "auth-smoke"],
        check=True,
        capture_output=True,
        text=True,
        timeout=60.0,
    )
    payload = json.loads(proc.stdout)
    # Don't pin specific versions; just confirm the keys + non-empty values.
    assert set(payload.keys()) == {"claude", "gemini", "waitbus"}
    assert all(isinstance(v, str) and v for v in payload.values())


# --- Verdict-level aggregation: cost_unknown_count + invariant_failure_count


def _make_reaction(
    framework: str,
    *,
    usage: TokenUsage | None,
    provider: str = "unknown",
) -> ObservedReaction:
    """Build a minimal ObservedReaction with the supplied token usage.

    Helper for the verdict-aggregation tests: the framework name plus a
    usage struct is all that matters for the per-reaction summary
    counts; the rest of the fields take sentinel values. ``provider``
    defaults to ``"unknown"`` so existing callers stay green; the
    provider-distribution tests pass explicit values.
    """
    return ObservedReaction(
        framework=framework,
        fw_id=f"{framework}-1",
        seed_delivery_id="seed-1",
        reaction_delivery_id=f"reaction-{framework}-1",
        received_wall_ns=1_000_000_000,
        reaction_latency_ms=10.0,
        token_usage=usage,
        provider=provider,
    )


def _make_curve_point(reactions: list[ObservedReaction]) -> RealCurvePoint:
    """Build a minimal RealCurvePoint wrapping the supplied reactions."""
    return RealCurvePoint(
        n=len(reactions),
        framework_mix={fw: 1 for fw in FRAMEWORK_ORDER},
        seed_delivery_id="seed-1",
        observed_reactions=tuple(reactions),
        cross_broadcast_proven=False,
        unique_frameworks_observed=len({r.framework for r in reactions}),
        reactions_received=len(reactions),
        reactions_expected=len(reactions),
        median_reaction_latency_ms=10.0,
        p99_reaction_latency_ms=10.0,
        total_token_usage=TokenUsage(input_tokens=0, output_tokens=0, cost_usd=None),
        duration_window_sec=10.0,
        auth_provenance={},
    )


def test_summarize_real_curve_points_counts_cost_unknown_reactions() -> None:
    """Every reaction whose ``cost_usd`` is ``None`` increments ``cost_unknown_count``.

    The gemini free-tier path returns ``cost_usd=None``; the verdict
    must surface the count so a non-zero-cost driver is no longer
    silently treated as free in cross-driver cost rankings.
    """
    from scripts.stress._controller import _summarize_real_curve_points

    claude_known = _make_reaction("claude-cli", usage=TokenUsage(input_tokens=5, output_tokens=3, cost_usd=0.001))
    gemini_unknown = _make_reaction("gemini-cli", usage=TokenUsage(input_tokens=10, output_tokens=2, cost_usd=None))
    cost_unknown, invariant_failures = _summarize_real_curve_points([_make_curve_point([claude_known, gemini_unknown])])
    assert cost_unknown == 1
    assert invariant_failures == 0


def test_summarize_real_curve_points_counts_invariant_failures() -> None:
    """A refusal or upstream error increments ``invariant_failure_count``.

    Before the moderation envelope fields were captured, a refusal
    (``stop_reason="refusal"``, ``is_error=True``, ``output_tokens=0``)
    looked like a clean zero-cost no-op in the verdict. The verdict
    must distinguish a moderated failure iteration from a successful
    one.
    """
    from scripts.stress._controller import _summarize_real_curve_points

    success = _make_reaction("claude-cli", usage=TokenUsage(input_tokens=5, output_tokens=3, cost_usd=0.001))
    refusal = _make_reaction(
        "claude-cli",
        usage=TokenUsage(
            input_tokens=18,
            output_tokens=0,
            cost_usd=0.0003,
            stop_reason="refusal",
            is_error=True,
            terminal_reason="error_during_execution",
        ),
    )
    cost_unknown, invariant_failures = _summarize_real_curve_points([_make_curve_point([success, refusal])])
    assert cost_unknown == 0
    assert invariant_failures == 1


def test_summarize_real_curve_points_ignores_non_llm_reactions() -> None:
    """Reactions with ``token_usage=None`` (non-LLM drivers) do not contribute to either count."""
    from scripts.stress._controller import _summarize_real_curve_points

    shell = _make_reaction("shell-control", usage=None)
    pyd = _make_reaction("pydantic", usage=None)
    cost_unknown, invariant_failures = _summarize_real_curve_points([_make_curve_point([shell, pyd])])
    assert cost_unknown == 0
    assert invariant_failures == 0


def test_summarize_real_curve_points_counts_token_parse_failures() -> None:
    """A reaction flagged ``token_usage_parse_failed`` is an invariant failure.

    A malformed token-usage payload was previously indistinguishable
    from a non-LLM driver (both yielded ``token_usage=None``) and was
    silently dropped. The dedicated flag routes the parse failure into
    the verdict's invariant-failure count so it is observable.
    """
    from scripts.stress._controller import _summarize_real_curve_points

    success = _make_reaction("claude-cli", usage=TokenUsage(input_tokens=5, output_tokens=3, cost_usd=0.001))
    parse_failed = ObservedReaction(
        framework="pydantic",
        fw_id="pydantic-1",
        seed_delivery_id="seed-1",
        reaction_delivery_id="reaction-pydantic-1",
        received_wall_ns=1_000_000_000,
        reaction_latency_ms=10.0,
        token_usage=None,
        token_usage_parse_failed=True,
    )
    cost_unknown, invariant_failures = _summarize_real_curve_points([_make_curve_point([success, parse_failed])])
    assert cost_unknown == 0
    assert invariant_failures == 1


# --- _token_usage_from_marker type-distinct outcomes ---------------------


def test_token_usage_from_marker_returns_none_for_non_llm_driver() -> None:
    """A marker carrying ``token_usage: null`` (or no key) yields ``None``."""
    from scripts.stress._controller import _token_usage_from_marker

    assert _token_usage_from_marker({"token_usage": None}) is None
    assert _token_usage_from_marker({"framework": "shell-control"}) is None


def test_token_usage_from_marker_returns_parse_failed_for_non_dict_payload() -> None:
    """A ``token_usage`` payload that is not a JSON object yields ``_ParseFailed``."""
    from scripts.stress._controller import _ParseFailed, _token_usage_from_marker

    result = _token_usage_from_marker({"token_usage": "oops-a-string"})
    assert isinstance(result, _ParseFailed)
    assert result.reason


def test_token_usage_from_marker_returns_parse_failed_on_validation_error() -> None:
    """A dict payload that fails ``TokenUsage`` validation yields ``_ParseFailed``."""
    from scripts.stress._controller import _ParseFailed, _token_usage_from_marker

    # input_tokens must be an int; a list is a validation error, not a
    # missing-field default fill.
    result = _token_usage_from_marker({"token_usage": {"input_tokens": [1, 2, 3], "output_tokens": 1}})
    assert isinstance(result, _ParseFailed)
    assert "validation" in result.reason


def test_token_usage_from_marker_returns_token_usage_for_valid_payload() -> None:
    """A valid ``token_usage`` dict rehydrates into a ``TokenUsage``."""
    from scripts.stress._controller import _token_usage_from_marker

    result = _token_usage_from_marker({"token_usage": {"input_tokens": 7, "output_tokens": 2}})
    assert isinstance(result, TokenUsage)
    assert result.input_tokens == 7
    assert result.output_tokens == 2


def test_observed_token_usage_from_marker_normalises_three_outcomes() -> None:
    """The normaliser maps the three outcomes onto ``(usage, parse_failed)``."""
    from scripts.stress._controller import observed_token_usage_from_marker

    # Non-LLM driver: (None, False).
    usage, failed = observed_token_usage_from_marker({"token_usage": None})
    assert usage is None
    assert failed is False

    # Valid payload: (TokenUsage, False).
    usage, failed = observed_token_usage_from_marker({"token_usage": {"input_tokens": 3, "output_tokens": 1}})
    assert isinstance(usage, TokenUsage)
    assert failed is False

    # Malformed payload: (None, True).
    usage, failed = observed_token_usage_from_marker({"token_usage": 123})
    assert usage is None
    assert failed is True


def test_roll_up_token_usage_skips_none_costs() -> None:
    """``_roll_up_token_usage`` sums known costs only; all-None reactions yield ``cost_usd=None``.

    The rolled-up window-level usage must surface ``None`` rather than
    coerce to ``0.0`` when every reaction has unknown cost. A known +
    unknown mix returns just the known sum so the verdict figure
    reflects the measured contribution.
    """
    from scripts.stress._controller import _roll_up_token_usage

    only_unknown = [_make_reaction("gemini-cli", usage=TokenUsage(input_tokens=1, output_tokens=1, cost_usd=None))]
    rolled = _roll_up_token_usage(only_unknown)
    assert rolled.cost_usd is None

    mixed = [
        _make_reaction("claude-cli", usage=TokenUsage(input_tokens=5, output_tokens=3, cost_usd=0.01)),
        _make_reaction("gemini-cli", usage=TokenUsage(input_tokens=10, output_tokens=2, cost_usd=None)),
    ]
    rolled = _roll_up_token_usage(mixed)
    assert rolled.cost_usd == pytest.approx(0.01)
    assert rolled.input_tokens == 15
    assert rolled.output_tokens == 5


def test_token_usage_from_marker_preserves_none_cost() -> None:
    """A wake-marker line carrying ``cost_usd=None`` rehydrates with ``cost_usd=None``.

    A gemini free-tier envelope reports ``cost_usd=None``; the
    rehydrator must surface the ``None`` rather than coerce to ``0.0``
    (which would silently make a non-zero-cost driver look free).
    Drives the assertion through a real emit + parse round-trip so
    the test pins the wire contract end-to-end.
    """
    import contextlib
    import io

    from scripts.stress._controller import _parse_wake_marker, _token_usage_from_marker
    from scripts.stress._real_drivers import _emit_wake_marker

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        _emit_wake_marker(
            framework="gemini-cli",
            fw_id="gemini-1",
            seed_id="seed-none-cost",
            reaction_id="reaction-none-cost",
            wall_ns=1_000_000_000,
            token_usage=TokenUsage(input_tokens=10, output_tokens=2, cost_usd=None),
            provider="gemini-cli",
        )
    line = next(line for line in buffer.getvalue().splitlines() if line.startswith(WAKE_MARKER))
    fields = _parse_wake_marker(line)
    assert fields is not None
    usage = _token_usage_from_marker(fields)
    assert isinstance(usage, TokenUsage)
    assert usage.input_tokens == 10
    assert usage.output_tokens == 2
    assert usage.cost_usd is None


# --- Moderation / upstream-error envelope rehydration --------------------


def test_emit_wake_marker_carries_envelope_metadata_for_refusal(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A refusal envelope's metadata fields ride the wake-marker line.

    The moderation / upstream-error fields (``stop_reason``,
    ``is_error``, ``terminal_reason``) cross the driver-to-orchestrator
    boundary as part of the JSON-bodied marker payload so a refusal
    envelope cannot rehydrate as a silent successful zero-cost no-op.
    """
    import json as _json

    from scripts.stress._real_drivers import _emit_wake_marker

    refusal_usage = TokenUsage(
        input_tokens=18,
        output_tokens=0,
        cost_usd=0.0003,
        stop_reason="refusal",
        is_error=True,
        terminal_reason="error_during_execution",
    )
    _emit_wake_marker(
        framework="claude-cli",
        fw_id="claude-1",
        seed_id="seed-refusal",
        reaction_id="reaction-refusal",
        wall_ns=1_000_000_000,
        token_usage=refusal_usage,
        provider="claude-cli",
    )
    captured = capsys.readouterr().out
    line = next(line for line in captured.splitlines() if line.startswith(WAKE_MARKER))
    body = _json.loads(line[len(WAKE_MARKER) :].lstrip())
    assert body["token_usage"]["stop_reason"] == "refusal"
    assert body["token_usage"]["is_error"] is True
    assert body["token_usage"]["terminal_reason"] == "error_during_execution"


def test_token_usage_from_marker_rehydrates_envelope_metadata() -> None:
    """A wake-marker carrying the moderation fields rehydrates them onto ``TokenUsage``.

    The full 18-field ``TokenUsage`` rides the JSON-bodied marker
    payload so ``stop_reason`` / ``is_error`` / ``api_error_status`` /
    ``terminal_reason`` survive the wake-marker round-trip and land
    on the orchestrator-side struct so the verdict aggregator sees
    the truthful refusal envelope.
    """
    import contextlib
    import io

    from scripts.stress._controller import _parse_wake_marker, _token_usage_from_marker
    from scripts.stress._real_drivers import _emit_wake_marker

    refusal_usage = TokenUsage(
        input_tokens=18,
        output_tokens=0,
        cost_usd=0.0003,
        stop_reason="refusal",
        is_error=True,
        terminal_reason="error_during_execution",
    )
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        _emit_wake_marker(
            framework="claude-cli",
            fw_id="claude-1",
            seed_id="seed-rehydrate",
            reaction_id="reaction-rehydrate",
            wall_ns=1_000_000_000,
            token_usage=refusal_usage,
            provider="claude-cli",
        )
    line = next(line for line in buffer.getvalue().splitlines() if line.startswith(WAKE_MARKER))
    fields = _parse_wake_marker(line)
    assert fields is not None
    usage = _token_usage_from_marker(fields)
    assert isinstance(usage, TokenUsage)
    assert usage.stop_reason == "refusal"
    assert usage.is_error is True
    assert usage.api_error_status is None
    assert usage.terminal_reason == "error_during_execution"


def test_summarize_real_curve_points_flips_overall_passed_on_rehydrated_refusal() -> None:
    """End-to-end rehydration: a wake-marker refusal line flows into invariant_failures.

    Emits a wake-marker line through ``_emit_wake_marker``, parses it
    via ``_parse_wake_marker``, rehydrates via
    ``_token_usage_from_marker``, builds an ``ObservedReaction`` +
    ``RealCurvePoint``, and asserts
    ``_summarize_real_curve_points`` flags the iteration. This is the
    end-to-end gate the prior in-process-only test
    (``test_summarize_real_curve_points_counts_invariant_failures``)
    could not cover -- it constructed the ``TokenUsage`` in-process,
    bypassing the wire round-trip that was silently dropping the
    moderation fields.
    """
    import contextlib
    import io

    from scripts.stress._controller import (
        _parse_wake_marker,
        _summarize_real_curve_points,
        _token_usage_from_marker,
    )
    from scripts.stress._real_drivers import _emit_wake_marker

    refusal_usage = TokenUsage(
        input_tokens=18,
        output_tokens=0,
        cost_usd=0.0003,
        stop_reason="refusal",
        is_error=True,
        terminal_reason="error_during_execution",
    )
    # Capture the emit via a stdout redirect rather than ``capsys`` so
    # the test can co-exist with other captures in the suite.
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        _emit_wake_marker(
            framework="claude-cli",
            fw_id="claude-1",
            seed_id="seed-refusal-e2e",
            reaction_id="reaction-refusal-e2e",
            wall_ns=2_000_000_000,
            token_usage=refusal_usage,
            provider="claude-cli",
        )
    marker_line = next(line for line in buffer.getvalue().splitlines() if line.startswith(WAKE_MARKER))
    fields = _parse_wake_marker(marker_line)
    assert fields is not None
    rehydrated = _token_usage_from_marker(fields)
    assert isinstance(rehydrated, TokenUsage)
    assert rehydrated.stop_reason == "refusal"
    assert rehydrated.is_error is True
    assert rehydrated.terminal_reason == "error_during_execution"

    reaction = _make_reaction("claude-cli", usage=rehydrated, provider="claude-cli")
    cost_unknown, invariant_failures = _summarize_real_curve_points([_make_curve_point([reaction])])
    assert invariant_failures == 1, (
        "refusal envelope crossed the wake-marker boundary intact must trip invariant_failures; "
        f"got rehydrated usage {rehydrated!r}"
    )
    assert cost_unknown == 0


def test_summarize_real_curve_points_trips_on_terminal_reason_error() -> None:
    """A ``terminal_reason="error_during_execution"`` envelope alone trips the invariant.

    The synthetic refusal fixture documents this terminal-state shape;
    a non-refusal upstream-error envelope can carry it without
    ``stop_reason="refusal"``. The aggregator must treat it as an
    invariant failure regardless of the other two flags.
    """
    from scripts.stress._controller import _summarize_real_curve_points

    error_only = _make_reaction(
        "claude-cli",
        usage=TokenUsage(
            input_tokens=10,
            output_tokens=0,
            cost_usd=0.001,
            stop_reason="end_turn",
            is_error=False,
            terminal_reason="error_during_execution",
        ),
    )
    cost_unknown, invariant_failures = _summarize_real_curve_points([_make_curve_point([error_only])])
    assert invariant_failures == 1
    assert cost_unknown == 0


def test_dispatch_shell_control_marker_carries_metadata_defaults() -> None:
    """Wake marker for shell-control carries the additive metadata fields as ``None``.

    The non-LLM driver passes ``token_usage=None`` to the emitter; the
    metadata fields therefore land as the absent-value sentinel
    ``"None"`` in the marker line. This proves the additive fields ride
    every driver's marker (forward-compat decode for unrelated
    consumers) without adding any semantic burden to the non-LLM path.
    """
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.stress._real_drivers",
            "shell-control",
            "--socket",
            "NONE",
            "--db",
            "/tmp/none.db",
            "--doorbell",
            "/tmp/none.sock",
            "--seed-scope-id",
            "stress-real-test",
            "--fw-id",
            "test-shell-3",
            "--once-then-exit",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10.0,
    )
    from scripts.stress._controller import _parse_wake_marker

    line = next(line for line in proc.stdout.splitlines() if line.startswith(WAKE_MARKER))
    fields = _parse_wake_marker(line)
    assert fields is not None
    # ``shell-control`` is a no-LLM driver: token_usage rides the
    # payload as ``null`` so the moderation / upstream-error fields
    # have no host struct to land on (their absence is the contract).
    assert fields["token_usage"] is None


# --- Greenfield model selection: key-presence drives real-vs-fake ---------


def test_openai_key_present_requires_valid_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_openai_key_present`` accepts only a shape-valid ``sk-`` key.

    A truncated or mistyped value must read as absent so the driver
    routes to the offline fallback instead of burning budget on a 401
    against the live endpoint.
    """
    from scripts.stress._real_drivers import _openai_key_present

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert _openai_key_present() is False

    monkeypatch.setenv("OPENAI_API_KEY", "")
    assert _openai_key_present() is False

    # Right prefix but too short (truncated paste).
    monkeypatch.setenv("OPENAI_API_KEY", "sk-short")
    assert _openai_key_present() is False

    # Long enough but missing the canonical prefix (typo / wrong var).
    monkeypatch.setenv("OPENAI_API_KEY", "not-an-openai-key-but-long-enough")
    assert _openai_key_present() is False

    # Shape-valid: sk- prefix and past the length floor.
    monkeypatch.setenv("OPENAI_API_KEY", "sk-not-real-but-present")
    assert _openai_key_present() is True


def test_openai_key_present_warns_on_nonempty_invalid_shape(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A non-empty but shape-invalid key leaves a WARNING breadcrumb.

    A truncated / mistyped key silently routing to the offline fake is a
    confusing failure mode (the operator thinks they enabled real mode but
    every driver runs offline). The warning makes that observable. An absent
    or empty key is the ordinary key-less path and must NOT warn.
    """
    import logging as _logging

    from scripts.stress._real_drivers import _openai_key_present

    # Non-empty but wrong shape -> rejected, with a breadcrumb.
    monkeypatch.setenv("OPENAI_API_KEY", "sk-short")
    with caplog.at_level(_logging.WARNING, logger="waitbus.stress.real_drivers"):
        assert _openai_key_present() is False
    assert any("openai_key_shape_rejected" in record.getMessage() for record in caplog.records)

    # Absent key is the ordinary key-less path -> no warning.
    caplog.clear()
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with caplog.at_level(_logging.WARNING, logger="waitbus.stress.real_drivers"):
        assert _openai_key_present() is False
    assert not any("openai_key_shape_rejected" in record.getMessage() for record in caplog.records)


def test_select_pydantic_model_offline_when_key_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without OPENAI_API_KEY, ``_select_pydantic_model`` returns TestModel.

    The absent-key fallback keeps a key-less CI runner exercising the
    driver wiring; the offline fakes still produce a wake-marker line
    so the orchestrator's collection logic is covered.
    """
    from pydantic_ai.models.test import TestModel

    from scripts.stress._real_drivers import (
        PROVIDER_OFFLINE_PYDANTIC,
        _select_pydantic_model,
    )

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    model, provider = _select_pydantic_model()
    assert isinstance(model, TestModel)
    assert provider == PROVIDER_OFFLINE_PYDANTIC


def test_select_pydantic_model_offline_fallback_emits_structured_log(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Absent-key fallback emits ``pydantic_driver_offline_fallback`` at INFO."""
    import logging as _logging

    from scripts.stress._real_drivers import _select_pydantic_model

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with caplog.at_level(_logging.INFO, logger="waitbus.stress.real_drivers"):
        _select_pydantic_model()
    assert any("pydantic_driver_offline_fallback" in record.getMessage() for record in caplog.records)


def test_select_pydantic_model_real_path_when_key_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """With OPENAI_API_KEY set: returns OpenAIModel (real) OR falls back on SDK import failure.

    Either outcome is acceptable per the fail-soft contract: if the
    operator's environment has a working ``pydantic_ai.models.openai``
    module the real path engages; if not (SDK version skew, missing
    optional dependency), the structured-warning fallback to TestModel
    keeps the harness running.
    """
    from pydantic_ai.models.test import TestModel

    from scripts.stress._real_drivers import (
        PROVIDER_OFFLINE_PYDANTIC,
        PROVIDER_OPENAI_GPT_4_1_NANO,
        _select_pydantic_model,
    )

    monkeypatch.setenv("OPENAI_API_KEY", "sk-not-real-but-present")
    model, provider = _select_pydantic_model()
    if provider == PROVIDER_OPENAI_GPT_4_1_NANO:
        # Real-OpenAI path is available on this host.
        from pydantic_ai.models.openai import OpenAIModel

        assert isinstance(model, OpenAIModel)
    else:
        # SDK import failed; fail-soft fallback engaged.
        assert provider == PROVIDER_OFFLINE_PYDANTIC
        assert isinstance(model, TestModel)


def test_extract_pydantic_token_usage_maps_canonical_usage_fields() -> None:
    """``_extract_pydantic_token_usage`` reads pydantic-ai 1.x ``result.usage``.

    Confirms the helper resolves ``input_tokens`` / ``output_tokens`` off
    the SDK's ``RunUsage`` property and constructs a ``TokenUsage`` with
    the provider id, the canonical token counts, AND the cost computed
    via the shared rate card. The pydantic-ai usage struct does not
    surface a per-call billing figure, so the driver computes cost from
    the captured tokens at the same layer that knows the provider --
    the verdict's per-driver token totals stay truthful end-to-end.
    """
    from scripts.stress._real_drivers import _extract_pydantic_token_usage

    class _FakeUsage:
        input_tokens = 42
        output_tokens = 7

    class _FakeResult:
        usage = _FakeUsage()

    token_usage = _extract_pydantic_token_usage(_FakeResult(), provider="openai-gpt-4o-mini")
    assert token_usage is not None
    assert token_usage.input_tokens == 42
    assert token_usage.output_tokens == 7
    # gpt-4o-mini rate card: $0.15/M input + $0.60/M output.
    # 42 * 0.15 / 1_000_000 + 7 * 0.60 / 1_000_000 = 6.3e-6 + 4.2e-6 = 1.05e-5
    assert token_usage.cost_usd == pytest.approx(1.05e-5)
    assert token_usage.model == "openai-gpt-4o-mini"


def test_extract_pydantic_token_usage_returns_none_when_usage_absent() -> None:
    """An ``AgentRunResult`` lacking usage rolls up to a ``None`` token envelope.

    A stubbed model, a streaming-only path, or an SDK version skew may
    leave ``result.usage`` as ``None`` or absent; the helper returns
    ``None`` and the orchestrator's invariant gate consumes it cleanly
    so the verdict does not silently record fabricated zeros.
    """
    from scripts.stress._real_drivers import _extract_pydantic_token_usage

    class _ResultNoUsage:
        usage = None

    assert _extract_pydantic_token_usage(_ResultNoUsage(), provider="openai-gpt-4o-mini") is None


def test_extract_langgraph_token_usage_maps_usage_metadata_dict() -> None:
    """``_extract_langgraph_token_usage`` reads langchain ``BaseMessage.usage_metadata``.

    Pins the canonical langchain schema: ``usage_metadata`` is a dict
    with ``input_tokens`` / ``output_tokens`` keys. Constructs a
    ``TokenUsage`` carrying the provider id, the token counts, AND
    the cost computed via the shared rate card.
    """
    from scripts.stress._real_drivers import _extract_langgraph_token_usage

    class _FakeReply:
        def __init__(self) -> None:
            self.usage_metadata: dict[str, int] = {
                "input_tokens": 13,
                "output_tokens": 4,
                "total_tokens": 17,
            }

    token_usage = _extract_langgraph_token_usage(_FakeReply(), provider="openai-gpt-4o-mini")
    assert token_usage is not None
    assert token_usage.input_tokens == 13
    assert token_usage.output_tokens == 4
    # gpt-4o-mini rate card: 13 * 0.15 / 1M + 4 * 0.60 / 1M = 1.95e-6 + 2.4e-6 = 4.35e-6
    assert token_usage.cost_usd == pytest.approx(4.35e-6)
    assert token_usage.model == "openai-gpt-4o-mini"


def test_extract_langgraph_token_usage_returns_none_when_metadata_absent() -> None:
    """A langchain reply with no ``usage_metadata`` rolls up to ``None``.

    ``langchain_openai.ChatOpenAI`` drops ``usage_metadata`` on some
    configurations (``with_structured_output``, partial-stream paths);
    the bench treats the orchestrator-side direct-SDK capture as the
    source of truth and lets the driver-side ``None`` flow through.
    """
    from scripts.stress._real_drivers import _extract_langgraph_token_usage

    class _ReplyNoMetadata:
        content = "ack"

    assert _extract_langgraph_token_usage(_ReplyNoMetadata(), provider="openai-gpt-4o-mini") is None


def test_extract_langgraph_token_usage_returns_none_for_non_dict_metadata() -> None:
    """A reply whose ``usage_metadata`` is not dict-shaped resolves to ``None``.

    A future SDK rev that returns a typed-object usage envelope (not a
    dict) reaches the helper's defensive branch instead of raising on
    ``.get``; the orchestrator's invariant gate consumes the ``None``
    without crashing the iteration.
    """
    from scripts.stress._real_drivers import _extract_langgraph_token_usage

    class _ReplyOpaque:
        usage_metadata = object()  # not a dict

    assert _extract_langgraph_token_usage(_ReplyOpaque(), provider="openai-gpt-4o-mini") is None


def test_emit_early_wake_marker_prints_canonical_line(capsys: pytest.CaptureFixture[str]) -> None:
    """``_emit_early_wake_marker`` writes one ``WAKE_RECEIVED`` line with every timing field.

    Pins the wire contract the orchestrator depends on: a JSON-bodied
    payload after the ``WAKE_RECEIVED`` prefix, carrying the three
    cross-process monotonic anchors the bench's bus-latency and
    delivery-mode classifications consume downstream.
    """
    from scripts.stress._controller import _parse_wake_marker
    from scripts.stress._real_drivers import EARLY_WAKE_MARKER, _emit_early_wake_marker

    _emit_early_wake_marker(
        framework="pydantic",
        fw_id="pydantic-1",
        seed_id="stress-real-seed:abc-123",
        wake_monotonic_ns=1_000_000_500,
        t_sub_monotonic_ns=999_999_000,
        t_import_done_monotonic_ns=900_000_000,
    )
    out = capsys.readouterr().out
    line = next(line for line in out.splitlines() if line.startswith(EARLY_WAKE_MARKER))
    fields = _parse_wake_marker(line, prefix=EARLY_WAKE_MARKER)
    assert fields is not None
    assert fields["framework"] == "pydantic"
    assert fields["fw_id"] == "pydantic-1"
    assert fields["seed"] == "stress-real-seed:abc-123"
    assert fields["wake_monotonic_ns"] == 1_000_000_500
    assert fields["t_sub_monotonic_ns"] == 999_999_000
    assert fields["t_import_done_monotonic_ns"] == 900_000_000


def test_select_langgraph_chat_model_offline_when_key_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without OPENAI_API_KEY, ``_select_langgraph_chat_model`` returns FakeListChatModel."""
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    from scripts.stress._real_drivers import (
        PROVIDER_OFFLINE_LANGGRAPH,
        _select_langgraph_chat_model,
    )

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    model, provider = _select_langgraph_chat_model()
    assert isinstance(model, FakeListChatModel)
    assert provider == PROVIDER_OFFLINE_LANGGRAPH


def test_select_langgraph_chat_model_offline_fallback_emits_structured_log(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Absent-key fallback emits ``langgraph_driver_offline_fallback`` at INFO."""
    import logging as _logging

    from scripts.stress._real_drivers import _select_langgraph_chat_model

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with caplog.at_level(_logging.INFO, logger="waitbus.stress.real_drivers"):
        _select_langgraph_chat_model()
    assert any("langgraph_driver_offline_fallback" in record.getMessage() for record in caplog.records)


def test_select_langgraph_chat_model_real_path_when_key_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """With OPENAI_API_KEY set: returns ChatOpenAI (real) OR falls back on SDK import failure."""
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    from scripts.stress._real_drivers import (
        PROVIDER_OFFLINE_LANGGRAPH,
        PROVIDER_OPENAI_GPT_4_1_NANO,
        _select_langgraph_chat_model,
    )

    monkeypatch.setenv("OPENAI_API_KEY", "sk-not-real-but-present")
    model, provider = _select_langgraph_chat_model()
    if provider == PROVIDER_OPENAI_GPT_4_1_NANO:
        from langchain_openai import ChatOpenAI

        assert isinstance(model, ChatOpenAI)
    else:
        assert provider == PROVIDER_OFFLINE_LANGGRAPH
        assert isinstance(model, FakeListChatModel)


# ---------------------------------------------------------------------------
# Silent real-LLM fallback gate (non-vacuity tests)
#
# Under REAL mode an absent / shape-invalid OPENAI_API_KEY must HARD-FAIL the
# OpenAI-backed selectors and the auth-smoke preflight, never silently downgrade
# to an offline fake that still counts toward the proof. These tests exercise
# the real selectors / real auth_smoke_check / real env signal -- no hand-set
# booleans standing in for the production decision.
# ---------------------------------------------------------------------------


def test_select_pydantic_model_real_mode_raises_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real mode + absent OPENAI_API_KEY: ``_select_pydantic_model`` RAISES, no TestModel."""
    from scripts.stress._real_drivers import _select_pydantic_model

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        _select_pydantic_model(real_mode=True)


def test_select_pydantic_model_real_mode_passes_with_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real mode + shape-valid key: ``_select_pydantic_model`` does NOT raise.

    The selector either returns the real ``OpenAIModel`` or (only on an SDK
    ImportError, a separate degradation) the offline ``TestModel``; the gate
    under test is solely that a present key does not trip the hard-fail.
    """
    from scripts.stress._real_drivers import _select_pydantic_model

    monkeypatch.setenv("OPENAI_API_KEY", "sk-not-real-but-present-and-long-enough")
    model, provider = _select_pydantic_model(real_mode=True)
    assert model is not None
    assert isinstance(provider, str) and provider


def test_select_langgraph_chat_model_real_mode_raises_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real mode + absent OPENAI_API_KEY: ``_select_langgraph_chat_model`` RAISES, no fake."""
    from scripts.stress._real_drivers import _select_langgraph_chat_model

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        _select_langgraph_chat_model(real_mode=True)


def test_select_langgraph_chat_model_real_mode_passes_with_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real mode + shape-valid key: ``_select_langgraph_chat_model`` does NOT raise."""
    from scripts.stress._real_drivers import _select_langgraph_chat_model

    monkeypatch.setenv("OPENAI_API_KEY", "sk-not-real-but-present-and-long-enough")
    model, provider = _select_langgraph_chat_model(real_mode=True)
    assert model is not None
    assert isinstance(provider, str) and provider


def test_real_mode_active_reads_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_real_mode_active`` reflects the REAL_MODE_ENV_VAR the controller sets."""
    from scripts.stress._real_drivers import REAL_MODE_ENV_VAR, _real_mode_active

    monkeypatch.delenv(REAL_MODE_ENV_VAR, raising=False)
    assert _real_mode_active() is False
    monkeypatch.setenv(REAL_MODE_ENV_VAR, "1")
    assert _real_mode_active() is True
    monkeypatch.setenv(REAL_MODE_ENV_VAR, "0")
    assert _real_mode_active() is False


def test_auth_smoke_check_requires_openai_key_for_openai_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    """A spec with pydantic/langgraph + absent key: ``auth_smoke_check`` RAISES naming OPENAI_API_KEY.

    Exercises the real preflight (the CLI binaries resolve in this env via the
    skip-guard) so the gate is the production code path, not a mocked stub.
    """
    if shutil.which("claude") is None or shutil.which("gemini") is None or shutil.which("waitbus") is None:
        pytest.skip("auth_smoke_check requires claude, gemini, waitbus on PATH")
    from scripts.stress._real_drivers import auth_smoke_check

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        auth_smoke_check(frameworks={"pydantic", "langgraph"})


def test_auth_smoke_check_passes_for_openai_spec_with_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """A spec with pydantic + shape-valid key: ``auth_smoke_check`` passes and records presence."""
    if shutil.which("claude") is None or shutil.which("gemini") is None or shutil.which("waitbus") is None:
        pytest.skip("auth_smoke_check requires claude, gemini, waitbus on PATH")
    from scripts.stress._real_drivers import auth_smoke_check

    monkeypatch.setenv("OPENAI_API_KEY", "sk-not-real-but-present-and-long-enough")
    provenance = auth_smoke_check(frameworks={"pydantic"})
    assert provenance["openai_api_key"] == "present"


def test_auth_smoke_check_no_openai_requirement_for_cli_only_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    """A claude/gemini/shell-only spec does NOT require OPENAI_API_KEY (no raise, no key recorded)."""
    if shutil.which("claude") is None or shutil.which("gemini") is None or shutil.which("waitbus") is None:
        pytest.skip("auth_smoke_check requires claude, gemini, waitbus on PATH")
    from scripts.stress._real_drivers import auth_smoke_check

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    provenance = auth_smoke_check(frameworks={"claude-cli", "gemini-cli", "shell-control"})
    assert "openai_api_key" not in provenance
    assert set(provenance.keys()) == {"claude", "gemini", "waitbus"}


def test_dispatch_shell_control_marker_carries_provider_field() -> None:
    """Wake marker for shell-control carries ``provider=offline`` (additive field).

    The orchestrator's parser tolerates unknown fields per its docstring;
    this asserts the new field is emitted but adds no constraint on its
    presence in the existing marker-line consumers.
    """
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.stress._real_drivers",
            "shell-control",
            "--socket",
            "NONE",
            "--db",
            "/tmp/none.db",
            "--doorbell",
            "/tmp/none.sock",
            "--seed-scope-id",
            "stress-real-test",
            "--fw-id",
            "test-shell-2",
            "--once-then-exit",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10.0,
    )
    from scripts.stress._controller import _parse_wake_marker

    line = next(line for line in proc.stdout.splitlines() if line.startswith(WAKE_MARKER))
    fields = _parse_wake_marker(line)
    assert fields is not None
    # provider field is present and carries the offline identifier.
    assert fields.get("provider") == "offline"


# --- Provider rehydrate + verdict-level distribution ----------------------


def test_provider_from_marker_extracts_field() -> None:
    """``_provider_from_marker`` returns the provider id from the fields dict."""
    from scripts.stress._controller import _provider_from_marker

    assert _provider_from_marker({"provider": "openai-gpt-4o-mini"}) == "openai-gpt-4o-mini"
    assert _provider_from_marker({"provider": "claude-cli"}) == "claude-cli"


def test_provider_from_marker_defaults_to_unknown_when_field_missing() -> None:
    """Absent provider field rehydrates as ``"unknown"`` (forward-compat decode)."""
    from scripts.stress._controller import _provider_from_marker

    assert _provider_from_marker({}) == "unknown"
    assert _provider_from_marker({"framework": "pydantic"}) == "unknown"


def test_observed_reaction_default_provider_is_unknown() -> None:
    """``ObservedReaction`` with no provider kwarg defaults to ``"unknown"``."""
    reaction = ObservedReaction(
        framework="pydantic",
        fw_id="pydantic-1",
        seed_delivery_id="seed-A",
        reaction_delivery_id="reaction-A",
        received_wall_ns=1_000_000_000,
        reaction_latency_ms=5.0,
        token_usage=None,
    )
    assert reaction.provider == "unknown"


def test_provider_distribution_counts_per_reaction_path() -> None:
    """``_provider_distribution`` rolls per-reaction providers into a histogram."""
    from scripts.stress._controller import _provider_distribution

    r1 = _make_reaction("pydantic", usage=None, provider="openai-gpt-4o-mini")
    r2 = _make_reaction("langgraph", usage=None, provider="openai-gpt-4o-mini")
    r3 = _make_reaction("claude-cli", usage=None, provider="claude-cli")
    r4 = _make_reaction("shell-control", usage=None, provider="offline")
    dist = _provider_distribution([_make_curve_point([r1, r2, r3, r4])])
    assert dist == {"openai-gpt-4o-mini": 2, "claude-cli": 1, "offline": 1}


def test_provider_distribution_empty_for_no_points() -> None:
    """No curve points = empty distribution (offline-only run shape)."""
    from scripts.stress._controller import _provider_distribution

    assert _provider_distribution([]) == {}


# --- Source-mix propagation (stress controller seed emit) ----------------


def test_emit_seed_event_accepts_picked_source_pair() -> None:
    """``_emit_seed_event`` accepts the per-iter ``(source, event_type)`` override.

    Documents the wired-in shape: the stress sweep mints the pair via
    ``pick_source_for_iter`` and threads it through to the seed-emit
    site. The default kwarg values fall back to the historical pair so
    a unit-shaped caller that omits them keeps byte-identical
    behaviour. This is a signature assertion, not a daemon-emit test.
    """
    import inspect

    from scripts.stress._controller import _emit_seed_event
    from scripts.stress._real_drivers import SEED_EVENT_TYPE, SEED_SOURCE

    sig = inspect.signature(_emit_seed_event)
    assert "source" in sig.parameters
    assert "event_type" in sig.parameters
    # Default values preserve the historical pair so any direct caller
    # without overrides retains the prior shape.
    assert sig.parameters["source"].default == SEED_SOURCE
    assert sig.parameters["event_type"].default == SEED_EVENT_TYPE


def test_run_real_window_accepts_picked_source_pair() -> None:
    """``run_real_window`` accepts the per-iter ``(picked_source, picked_event_type)`` override.

    The sweep wrapper draws the pair from
    ``pick_source_for_iter(iter_id)`` and passes it through; absent
    overrides, the historical default ``(agent, agent_message)`` pair
    is used. This is a signature assertion that the override channel
    exists; the e2e test exercises the actual daemon path.
    """
    import inspect

    from scripts.stress._controller import run_real_window
    from scripts.stress._real_drivers import SEED_EVENT_TYPE, SEED_SOURCE

    sig = inspect.signature(run_real_window)
    assert "picked_source" in sig.parameters
    assert "picked_event_type" in sig.parameters
    assert sig.parameters["picked_source"].default == SEED_SOURCE
    assert sig.parameters["picked_event_type"].default == SEED_EVENT_TYPE


def test_run_real_mode_sweep_returns_source_histogram() -> None:
    """``_run_real_mode_sweep`` returns a per-iter source-distribution histogram.

    Signature assertion that the new return tuple shape is in place;
    the sweep walks ``pick_source_for_iter(iter_id)`` per window and
    aggregates the source-name draws into the third tuple slot the
    controller's ``main`` threads into the verdict.
    """
    import inspect

    from scripts.stress._controller import _run_real_mode_sweep

    sig = inspect.signature(_run_real_mode_sweep)
    return_annotation = sig.return_annotation
    # The annotation is a 3-tuple: (curve_points, failures, source_distribution).
    assert "dict[str, int]" in str(return_annotation), (
        f"expected dict[str, int] in return annotation, got {return_annotation!r}"
    )


def test_picked_source_pair_round_trips_through_picker() -> None:
    """For a known ``iter_id``, the picker returns a registered taxonomy pair.

    Documents the contract the sweep wrapper relies on: every pair the
    picker returns is in ``SOAK_SOURCE_REGISTRY``, so the daemon's
    ``_fan_out`` (which only routes registered event_types) will
    successfully deliver every per-window seed event to the drivers.
    """
    from benchmarks._bench_source_mix import pick_source_for_iter
    from benchmarks._source_taxonomy import SOAK_SOURCE_REGISTRY

    registered_pairs = {(spec.name, spec.event_type) for spec in SOAK_SOURCE_REGISTRY}
    for iter_id in (0, 1, 2, 5, 10):
        pair = pick_source_for_iter(iter_id)
        assert pair in registered_pairs, f"iter_id={iter_id} drew unregistered pair {pair!r}"


# --- Owner-only wait predicate (source-mix taxonomy fan-out) --------------


def test_wait_for_seed_via_subscribe_uses_owner_only_predicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI-driver SDK-wait helper subscribes with owner-only match + source=None.

    The bench's source-mix picker sweeps the seed across the full
    registered taxonomy; the driver-side predicate must not pin
    ``source`` / ``event_type`` or it would only wake on the
    historical ``(agent, agent_message)`` slice and fail every other
    pick.
    """
    from scripts.stress import _real_drivers

    captured: dict[str, Any] = {}

    def fake_wait_for(
        match: list[str],
        *,
        source: str | None,
        timeout: float,
        socket_path: str,
        since: str | None = None,
    ) -> None:
        captured["match"] = list(match)
        captured["source"] = source
        captured["timeout"] = timeout
        captured["socket_path"] = socket_path
        captured["since"] = since
        return None

    monkeypatch.setattr(_real_drivers, "wait_for", fake_wait_for)
    _real_drivers._wait_for_seed_via_subscribe(
        socket_path="NONE",
        db_path=Path("/tmp/none.db"),
        seed_scope_id="stress-real-test-owner-only",
        timeout_sec=1.0,
        framework="claude-cli",
        fw_id="claude-cli-test",
    )
    # Predicate has exactly one clause: owner. No source / event_type pin.
    assert captured["match"] == ['fields.owner="stress-real-test-owner-only"']
    assert captured["source"] is None
    # Default replay cursor is None: subscribe-from-live preserved when the
    # caller omits the new ``since`` parameter.
    assert captured["since"] is None


def test_wait_for_seed_via_subscribe_threads_since_cursor_through_to_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-None ``since`` cursor reaches the ``wait_for`` SDK call.

    Pins the contract that ``_wait_for_seed_via_subscribe`` forwards the
    replay cursor through to ``waitbus.wait_for`` instead of silently
    dropping it: the bench / stress orchestrator threads the anchor
    event_id through this helper for the CLI-driver roles, and any
    drop would re-introduce the subscribe-register race the cursor
    closes.
    """
    from scripts.stress import _real_drivers

    captured: dict[str, Any] = {}
    anchor_event_id = "01H8XYZA01H8XYZA01H8XYZA01"

    def fake_wait_for(
        match: list[str],
        *,
        source: str | None,
        timeout: float,
        socket_path: str,
        since: str | None = None,
    ) -> None:
        captured["since"] = since
        return None

    monkeypatch.setattr(_real_drivers, "wait_for", fake_wait_for)
    _real_drivers._wait_for_seed_via_subscribe(
        socket_path="NONE",
        db_path=Path("/tmp/none.db"),
        seed_scope_id="stress-real-test-since",
        timeout_sec=1.0,
        since=anchor_event_id,
        framework="gemini-cli",
        fw_id="gemini-cli-test",
    )
    assert captured["since"] == anchor_event_id


def test_run_real_mode_sweep_isolates_a_window_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """A raising window records a loud failure and the sweep continues (fault isolation).

    ``_run_real_mode_sweep`` must not let one window's ``run_real_window``
    exception (e.g. a daemon-bind timeout) crash the whole sweep and lose
    the verdict. The failing window contributes exactly one
    ``real_window_error`` ``StressSignalFailure`` (which flips
    ``overall_passed`` to False) while the remaining windows still run and
    produce their curve points; the error is also surfaced to progress.
    """
    import io

    from scripts.stress import _controller

    seen: list[int] = []

    def _fake_run_real_window(n: int, **_kwargs: Any) -> tuple[RealCurvePoint, dict[str, int]]:
        seen.append(n)
        if n == 5:
            raise RuntimeError("daemon failed to bind within 10.0s")
        # The surviving window is a clean PROVEN point so the only failure
        # recorded by the sweep is the isolated n=5 real_window_error.
        reactions = [_make_reaction(fw, usage=None) for fw in FRAMEWORK_ORDER]
        point = RealCurvePoint(
            n=n,
            framework_mix={fw: n // len(FRAMEWORK_ORDER) for fw in FRAMEWORK_ORDER},
            seed_delivery_id="seed-1",
            observed_reactions=tuple(reactions),
            cross_broadcast_proven=True,
            unique_frameworks_observed=len(FRAMEWORK_ORDER),
            reactions_received=n,
            reactions_expected=n,
            median_reaction_latency_ms=10.0,
            p99_reaction_latency_ms=10.0,
            total_token_usage=TokenUsage(input_tokens=0, output_tokens=0, cost_usd=None),
            duration_window_sec=10.0,
            auth_provenance={},
        )
        return point, {}

    monkeypatch.setattr(_controller, "run_real_window", _fake_run_real_window)
    progress = io.StringIO()
    points, failures, _source_dist = _controller._run_real_mode_sweep(
        sweep=(5, 10),
        env={},
        duration_sec=1.0,
        progress_fh=progress,
        waitbus_path="waitbus",
        auth_provenance={},
    )

    # Both windows were attempted: the n=5 exception did NOT abort the sweep.
    assert seen == [5, 10]
    # The failing window produced exactly one real_window_error failure.
    assert [f.signal for f in failures] == ["real_window_error"]
    assert "daemon failed to bind" in failures[0].detail
    # The surviving window still produced its curve point.
    assert len(points) == 1
    # The error was surfaced to the progress log.
    assert "real_window_error" in progress.getvalue()
