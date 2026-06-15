"""Unit tests for ``benchmarks._bench_shared``.

Every public helper has at least one positive test. The OpenAI SDK
wrapper is exercised against a synthetic ``OpenAI`` client (no network
call, no API key) — the bench's end-to-end coverage of the real path
lives in the per-experiment scripts. These tests pin the helper
contracts so a future bench iteration cannot silently re-shape the
recorded fields.

Linux-only by construction; tests that would be meaningful on non-Linux
hosts are guarded by an explicit skip.
"""

from __future__ import annotations

import contextlib
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import msgspec
import pytest

from benchmarks._bench_shared import (
    COLD_CACHE_PREFIX_LEN,
    ClaudeEnvelope,
    CostBudgetTracker,
    ExternalStateReport,
    GeminiEnvelope,
    IterationRow,
    OpenAIEnvelope,
    _hashseed_or_default,
    _parse_physical_cpu_count,
    capture_daemon_pragmas,
    capture_external_state,
    claude_envelope_from_token_usage,
    count_cache_contaminated_rows,
    detect_ntp_daemon,
    drain_children_concurrently,
    force_cold_cache_prefix,
    gemini_envelope_from_token_usage,
    merge_distribution,
    merge_observed_models,
    openai_envelope_to_usd,
    read_daemon_cpu_ns,
)
from scripts.stress._context import (
    DEFAULT_SCAN_BALANCED_JSON_MAX_BYTES,
    OPENAI_PROVIDER_GPT_4_1_NANO,
    OPENAI_PROVIDER_GPT_4O_MINI,
    TokenUsage,
    openai_tokens_to_usd,
    scan_balanced_json,
)

# ---------------------------------------------------------------------
# ExternalStateReport round-trip.
# ---------------------------------------------------------------------


def test_external_state_report_msgspec_roundtrip() -> None:
    """Encode + decode via msgspec.json preserves every declared field."""
    report = ExternalStateReport(
        claude_cli_version="1.0.0",
        gemini_cli_version=None,
        pydantic_ai_version="1.104.0",
        langgraph_version="1.2.2",
        langchain_core_version="1.4.0",
        langchain_openai_version="1.1.9",
        openai_sdk_version="1.109.1",
        anthropic_sdk_version=None,
        msgspec_version="0.21.1",
        hdrhistogram_version="0.10.3",
        tiktoken_version="0.13.0",
        anthropic_response_model_set=["claude-haiku-4-5-20251001"],
        openai_response_model_set=["gpt-4o-mini-2024-07-18"],
        gemini_response_model_set=["gemini-2.5-flash"],
        agent_tool_call_count_per_iter=[1, 1, 0],
        agent_turn_count_per_iter=[1, 1, 1],
        waitbus_daemon_synchronous="NORMAL",
        waitbus_daemon_journal_mode="wal",
        waitbus_daemon_page_size=4096,
        waitbus_daemon_broadcast_pool_size=4,
        waitbus_daemon_doorbell_socket_buffer=65536,
        waitbus_daemon_pragmas={"journal_mode": "wal", "synchronous": "1"},
        waitbus_env_vars={"WAITBUS_LOG_LEVEL": "INFO"},
        pythonhashseed="0",
        pythonmalloc=None,
        ntp_active=True,
        ntp_source="timedatectl",
        boot_time_ns=1_700_000_000_000_000_000,
        cpu_count_physical=16,
        cpu_count_logical=32,
        moderation_event_count=0,
        stop_reason_distribution={"end_turn": 42},
        api_error_status_distribution={},
        openai_key_present=True,
    )
    encoded = msgspec.json.encode(report)
    decoded = msgspec.json.decode(encoded, type=ExternalStateReport)
    assert decoded == report


def test_capture_external_state_populates_python_and_proc_fields() -> None:
    """Probe the real host: every Python-distribution field whose package
    is installed by the dev sync resolves; ``openai_key_present`` mirrors
    the parameter (the helper does not lookup the keyring itself); the
    per-iteration list fields start empty."""
    report = capture_external_state(openai_api_key_present=True)
    assert report.openai_key_present is True
    assert report.msgspec_version is not None
    assert report.openai_sdk_version is not None
    # langchain-openai is in the bench extra and the dev sync installs it.
    assert report.langchain_openai_version is not None
    # Per-iteration accumulators start empty so the bench's iteration
    # loop drives them deterministically.
    assert report.anthropic_response_model_set == []
    assert report.openai_response_model_set == []
    assert report.agent_tool_call_count_per_iter == []
    assert report.agent_turn_count_per_iter == []
    assert report.stop_reason_distribution == {}
    assert report.moderation_event_count == 0
    # On Linux hosts boot_time_ns and cpu counts populate.
    if sys.platform.startswith("linux"):
        assert report.boot_time_ns is not None
        assert report.cpu_count_logical is not None


def test_capture_external_state_records_only_bool_openai_key_presence() -> None:
    """``openai_api_key_present=False`` must propagate; no key value
    is read or stored anywhere in the report struct."""
    report = capture_external_state(openai_api_key_present=False)
    assert report.openai_key_present is False
    # No field with a string-typed key shape exists on the report.
    encoded = msgspec.json.encode(report).decode("utf-8")
    assert "sk-" not in encoded


# ---------------------------------------------------------------------
# Physical-core probe (/proc/cpuinfo parser).
# ---------------------------------------------------------------------


def test_parse_physical_cpu_count_collapses_smt_siblings() -> None:
    """Two logical processors sharing one (physical id, core id) count as one core."""
    cpuinfo = "processor\t: 0\nphysical id\t: 0\ncore id\t\t: 0\n\nprocessor\t: 1\nphysical id\t: 0\ncore id\t\t: 0\n\n"
    # Two logical procs (hyperthreads) on the same physical core => 1.
    assert _parse_physical_cpu_count(cpuinfo) == 1


def test_parse_physical_cpu_count_distinct_cores() -> None:
    """Two distinct (physical id, core id) pairs count as two physical cores."""
    cpuinfo = "processor\t: 0\nphysical id\t: 0\ncore id\t\t: 0\n\nprocessor\t: 1\nphysical id\t: 0\ncore id\t\t: 1\n\n"
    assert _parse_physical_cpu_count(cpuinfo) == 2


def test_parse_physical_cpu_count_handles_missing_trailing_blank() -> None:
    """A final block not terminated by a blank line is still committed."""
    cpuinfo = "processor\t: 0\nphysical id\t: 1\ncore id\t\t: 0\n\nprocessor\t: 1\nphysical id\t: 1\ncore id\t\t: 0"
    # Both blocks share (1, 0) so the trailing block (no blank line) still
    # gets committed and collapses onto the same core => 1.
    assert _parse_physical_cpu_count(cpuinfo) == 1


def test_parse_physical_cpu_count_none_when_keys_absent() -> None:
    """A cpuinfo with no topology keys yields None, not a fabricated count."""
    cpuinfo = "processor\t: 0\nmodel name\t: Synthetic CPU\n\nprocessor\t: 1\n\n"
    assert _parse_physical_cpu_count(cpuinfo) is None


def test_parse_physical_cpu_count_none_on_empty() -> None:
    """Empty input yields None (probe could not observe the topology)."""
    assert _parse_physical_cpu_count("") is None


# ---------------------------------------------------------------------
# _hashseed_or_default.
# ---------------------------------------------------------------------


def test_hashseed_or_default_parses_numeric(monkeypatch: pytest.MonkeyPatch) -> None:
    """A numeric ``PYTHONHASHSEED`` parses to the matching int."""
    monkeypatch.setenv("PYTHONHASHSEED", "7")
    assert _hashseed_or_default() == 7


def test_hashseed_or_default_random_sentinel_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """The CPython ``"random"`` sentinel maps to the default, not a ValueError."""
    monkeypatch.setenv("PYTHONHASHSEED", "random")
    assert _hashseed_or_default() == 0


def test_hashseed_or_default_garbage_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unparseable value maps to the default rather than aborting."""
    monkeypatch.setenv("PYTHONHASHSEED", "not-a-number")
    assert _hashseed_or_default() == 0


def test_hashseed_or_default_unset_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unset variable maps to the default."""
    monkeypatch.delenv("PYTHONHASHSEED", raising=False)
    assert _hashseed_or_default() == 0


def test_hashseed_or_default_clamps_out_of_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """A negative or oversized PYTHONHASHSEED clamps to the valid 0..2**32-1 band."""
    monkeypatch.setenv("PYTHONHASHSEED", "-5")
    assert _hashseed_or_default() == 0
    monkeypatch.setenv("PYTHONHASHSEED", str(2**33))
    assert _hashseed_or_default() == 2**32 - 1


# ---------------------------------------------------------------------
# force_cold_cache_prefix.
# ---------------------------------------------------------------------


def test_force_cold_cache_prefix_deterministic_within_run() -> None:
    """Same run_salt + same iter_id returns same prefix; byte-identical re-runs."""
    a = force_cold_cache_prefix("run-salt-a", iter_id=42)
    b = force_cold_cache_prefix("run-salt-a", iter_id=42)
    assert a == b
    assert len(a) == COLD_CACHE_PREFIX_LEN


def test_force_cold_cache_prefix_differs_across_runs() -> None:
    """Different run_salt produces a different prefix for the same iter_id.

    This is the cross-run cache isolation guarantee: the prompt cache
    is content-addressed per-API-key (NOT per-process), so a fresh salt
    each orchestrator run is what makes a separate process MISS the
    prior run's cached prefix.
    """
    a = force_cold_cache_prefix("run-salt-a", iter_id=0)
    b = force_cold_cache_prefix("run-salt-b", iter_id=0)
    assert a != b


def test_force_cold_cache_prefix_differs_across_iter_ids() -> None:
    """Different iter_ids produce different prefixes within a run — the whole point."""
    prefixes = {force_cold_cache_prefix("run-salt-a", iter_id=i) for i in range(8)}
    assert len(prefixes) == 8


def test_force_cold_cache_prefix_target_length_filled() -> None:
    """Prefix is padded to the documented ~200-char target via SHA-256 repetition."""
    out = force_cold_cache_prefix("run-salt-a", iter_id=1)
    assert len(out) == COLD_CACHE_PREFIX_LEN
    # All characters land in the hex alphabet (SHA-256 hex digest output).
    assert set(out).issubset(set("0123456789abcdef"))


# ---------------------------------------------------------------------
# scan_balanced_json.
# ---------------------------------------------------------------------


def test_scan_balanced_json_extracts_envelope_past_preamble() -> None:
    """Gemini-style MCP-discovery preamble does not confuse the scanner."""
    blob = (
        "Error during discovery for MCP server 'quint-code': spawn quint-code ENOENT\n"
        '{"stats": {"models": {"gemini-2.5-flash": {"tokens": {"prompt": 10, "candidates": 20}}}}, '
        '"stop_reason": "end_turn"}'
    )
    parsed = scan_balanced_json(blob)
    assert parsed is not None
    assert parsed["stop_reason"] == "end_turn"


def test_scan_balanced_json_respects_string_literals_with_braces() -> None:
    """A ``{`` inside a JSON string literal must not confuse the depth counter."""
    blob = 'noise {"x": "{not json}", "y": 1} trailing'
    parsed = scan_balanced_json(blob)
    assert parsed == {"x": "{not json}", "y": 1}


def test_scan_balanced_json_oversized_input_raises() -> None:
    """A 2 MiB blob aborts at the length cap."""
    blob = "x" * (2 * 1024 * 1024)
    with pytest.raises(ValueError, match="max_bytes"):
        scan_balanced_json(blob, max_bytes=DEFAULT_SCAN_BALANCED_JSON_MAX_BYTES)


def test_scan_balanced_json_returns_none_on_no_object() -> None:
    """No braces in input -> ``None`` (caller distinguishes from raise)."""
    assert scan_balanced_json("no braces here") is None


def test_scan_balanced_json_falls_through_bad_first_candidate() -> None:
    """A first ``{...}`` that fails JSON decode falls through to the next one."""
    blob = '{not really json} then {"real": true}'
    parsed = scan_balanced_json(blob)
    assert parsed == {"real": True}


# ---------------------------------------------------------------------
# read_daemon_cpu_ns.
# ---------------------------------------------------------------------


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux-only /proc layout")
def test_read_daemon_cpu_ns_reads_real_process() -> None:
    """Reading the current process's /proc/<pid>/stat yields non-negative CPU ns."""
    utime_ns, stime_ns = read_daemon_cpu_ns(pid=__import__("os").getpid())
    assert utime_ns >= 0
    assert stime_ns >= 0


def test_read_daemon_cpu_ns_parses_comm_with_parens(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A synthetic /proc/<pid>/stat with ``comm`` containing ``)`` parses correctly.

    A daemon named via ``prctl(PR_SET_NAME)`` with parens in the name
    would break a naive ``line.split()``. The fixture line follows
    ``man 5 proc`` exactly with utime=100, stime=50 in jiffies.
    """
    # Simulate a daemon named ``(waitbus-daemon (worker))`` — two ``) ``
    # occurrences inside ``comm``; the parser must anchor on the LAST.
    fake_pid = 99999
    fake_stat_dir = tmp_path / "proc" / str(fake_pid)
    fake_stat_dir.mkdir(parents=True)
    fake_stat_file = fake_stat_dir / "stat"
    # Fields after the comm close: state ppid pgrp session tty_nr
    # tpgid flags minflt cminflt majflt cmajflt utime stime ...
    # utime=100, stime=50.
    fake_stat_file.write_text(
        f"{fake_pid} (waitbus-daemon (worker)) S 1 1 1 0 -1 0 0 0 0 0 100 50 0 0 20 0 1 0 0 0 0 0 0 0\n"
    )
    real_open = open

    def fake_open(path: Any, *args: Any, **kwargs: Any) -> Any:
        if str(path) == f"/proc/{fake_pid}/stat":
            return real_open(fake_stat_file, *args, **kwargs)
        return real_open(path, *args, **kwargs)

    with patch("builtins.open", fake_open):
        utime_ns, stime_ns = read_daemon_cpu_ns(pid=fake_pid)
    # On a 100 Hz kernel, 100 jiffies = 1s = 1e9 ns and 50 jiffies = 5e8 ns.
    # CLK_TCK can also be 1000 (rare); the assertion uses the ratio.
    assert utime_ns > 0
    assert stime_ns > 0
    assert utime_ns == 2 * stime_ns  # ratio of utime:stime jiffies


def test_read_daemon_cpu_ns_missing_pid_raises(tmp_path: Path) -> None:
    """A non-existent pid path raises OSError (the bench's preflight catches it)."""
    nonexistent_pid = 2**30  # almost certainly not allocated
    with pytest.raises((FileNotFoundError, ProcessLookupError, OSError)):
        read_daemon_cpu_ns(pid=nonexistent_pid)


# ---------------------------------------------------------------------
# detect_ntp_daemon — daemon-agnostic.
# ---------------------------------------------------------------------


def test_detect_ntp_daemon_returns_tuple() -> None:
    """The probe always returns a ``(bool|None, str|None)`` tuple.

    The bench records both fields; no assumption is made about whether
    the test host has timedatectl/chronyc installed — only that the
    helper's shape is stable.
    """
    active, source = detect_ntp_daemon()
    assert active is None or isinstance(active, bool)
    assert source is None or isinstance(source, str)


# ---------------------------------------------------------------------
# OpenAIEnvelope struct.
# ---------------------------------------------------------------------


def test_openai_envelope_msgspec_roundtrip() -> None:
    """OpenAIEnvelope round-trips via msgspec.json with every field declared."""
    cap = OpenAIEnvelope(
        model="gpt-4o-mini-2024-07-18",
        input_tokens=42,
        output_tokens=13,
        cached_tokens=0,
        finish_reason="stop",
        stop_reason=None,
        is_error=False,
        api_error_status=None,
        terminal_reason=None,
    )
    decoded = msgspec.json.decode(msgspec.json.encode(cap), type=OpenAIEnvelope)
    assert decoded == cap


def test_openai_envelope_clean_completion_defaults_are_neutral() -> None:
    """Constructing a capture for a clean completion leaves the moderation
    fields at their neutral defaults.

    Pins the cross-provider invariant gate contract: a successful call
    never trips the ``is_error`` / ``stop_reason`` checks because the
    construction defaults are explicitly non-error.
    """
    cap = OpenAIEnvelope(
        model="gpt-4o-mini-2024-07-18",
        input_tokens=10,
        output_tokens=5,
        cached_tokens=0,
        finish_reason="stop",
    )
    assert cap.stop_reason is None
    assert cap.is_error is False
    assert cap.api_error_status is None
    assert cap.terminal_reason is None


def test_openai_envelope_records_error_fields_from_exception() -> None:
    """A caller-synthesised error capture preserves error fields through roundtrip.

    Mirrors ``ClaudeEnvelope`` / ``GeminiEnvelope`` semantics: a caught API
    exception is converted to a capture with ``is_error=True``, the
    HTTP-style status, and the formatted exception string so the invariant
    gate has one cross-provider error pivot.
    """
    cap = OpenAIEnvelope(
        model="gpt-4o-mini-2024-07-18",
        input_tokens=0,
        output_tokens=0,
        cached_tokens=0,
        finish_reason=None,
        stop_reason="error_during_execution",
        is_error=True,
        api_error_status="429",
        terminal_reason="RateLimitError: rate_limit_exceeded",
    )
    decoded = msgspec.json.decode(msgspec.json.encode(cap), type=OpenAIEnvelope)
    assert decoded == cap
    assert decoded.is_error is True
    assert decoded.api_error_status == "429"


# ---------------------------------------------------------------------
# Aggregation helpers.
# ---------------------------------------------------------------------


def test_merge_observed_models_preserves_order_and_dedups() -> None:
    """First-observation order is preserved; repeats are dropped."""
    out = merge_observed_models([], "gpt-4o-mini-2024-07-18")
    assert out == ["gpt-4o-mini-2024-07-18"]
    out = merge_observed_models(out, "gpt-4o-mini-2024-07-18")
    assert out == ["gpt-4o-mini-2024-07-18"]
    out = merge_observed_models(out, "gpt-4o-mini-2024-09-12")
    assert out == ["gpt-4o-mini-2024-07-18", "gpt-4o-mini-2024-09-12"]


def test_merge_observed_models_drops_none() -> None:
    """``None`` observations are silently dropped (no implicit bucket)."""
    assert merge_observed_models([], None) == []
    assert merge_observed_models(["x"], None) == ["x"]


def test_merge_distribution_increments_and_returns_new_dict() -> None:
    """The helper returns a new dict (no in-place mutation of the input)."""
    original: dict[str, int] = {"end_turn": 1}
    out = merge_distribution(original, "end_turn")
    assert out == {"end_turn": 2}
    assert original == {"end_turn": 1}  # input untouched


def test_merge_distribution_silently_drops_none_key() -> None:
    """``key=None`` does not create a bucket; the distribution is unchanged."""
    out = merge_distribution({"end_turn": 5}, None)
    assert out == {"end_turn": 5}


# ---------------------------------------------------------------------
# Envelope substructs (Claude / Gemini / OpenAI).
# ---------------------------------------------------------------------


def test_claude_envelope_from_token_usage_roundtrip() -> None:
    """Every field on TokenUsage maps onto the typed ClaudeEnvelope."""
    tu = TokenUsage(
        input_tokens=10,
        output_tokens=20,
        cost_usd=0.01,
        cache_creation_input_tokens=30,
        cache_read_input_tokens=40,
        billed_input_tokens=80,
        model="claude-haiku-4-5-20251001",
        stop_reason="end_turn",
        is_error=False,
        api_error_status=None,
        terminal_reason="completed",
        num_turns=1,
        service_tier="standard",
    )
    env = claude_envelope_from_token_usage(tu)
    assert env.input_tokens_visible == 10
    assert env.cache_creation_input_tokens == 30
    assert env.cache_read_input_tokens == 40
    assert env.output_tokens == 20
    assert env.billed_input_tokens == 80
    assert env.total_billed_tokens == 100
    assert env.cost_usd == pytest.approx(0.01)
    assert env.model == "claude-haiku-4-5-20251001"
    assert env.stop_reason == "end_turn"
    assert env.is_error is False
    assert env.terminal_reason == "completed"
    assert env.num_turns == 1


def test_gemini_envelope_from_token_usage_roundtrip() -> None:
    """Every field on TokenUsage maps onto the typed GeminiEnvelope."""
    tu = TokenUsage(
        input_tokens=100,
        output_tokens=50,
        cost_usd=None,
        thoughts_tokens=200,
        cached_tokens=10,
        tool_tokens=5,
        total_tokens_reported=365,
        total_tokens_recomputed=355,
        model="gemini-2.5-flash",
        stop_reason=None,
        is_error=False,
        api_error_status=None,
        terminal_reason=None,
        num_turns=1,
    )
    env = gemini_envelope_from_token_usage(tu)
    assert env.prompt_tokens == 100
    assert env.candidates_tokens == 50
    assert env.thoughts_tokens == 200
    assert env.cached_tokens == 10
    assert env.tool_tokens == 5
    assert env.total_tokens_reported == 365
    assert env.total_tokens_recomputed == 355
    assert env.cost_usd is None
    assert env.model == "gemini-2.5-flash"


def test_capture_daemon_pragmas_reads_real_db(tmp_path: Path) -> None:
    """The probe reads PRAGMA values from a real SQLite file via read-only URI.

    Persistent PRAGMAs (``journal_mode=WAL``) survive a connection
    close because they are written into the DB header; per-connection
    PRAGMAs (``synchronous``, ``cache_size``, ``mmap_size``) reset to
    their compiled defaults on each open and so the probe records the
    probe-connection's default values. Both shapes appear in the result
    dict so the verdict's downstream reader sees the snapshot at probe
    time.
    """
    import sqlite3

    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE t(x INTEGER)")
        conn.commit()
    finally:
        conn.close()
    pragmas = capture_daemon_pragmas(db_path)
    # journal_mode is persistent: the probe sees the WAL setting.
    assert pragmas.get("journal_mode", "").lower() == "wal"
    # Per-connection PRAGMAs are populated to their probe-connection
    # defaults; the keys are present so the verdict's reader sees the
    # snapshot is non-empty.
    assert "synchronous" in pragmas
    assert "cache_size" in pragmas
    assert "mmap_size" in pragmas


def test_capture_daemon_pragmas_returns_empty_on_missing_db(tmp_path: Path) -> None:
    """A non-existent DB path returns ``{}`` rather than raising."""
    pragmas = capture_daemon_pragmas(tmp_path / "does-not-exist.db")
    assert pragmas == {}


# ---------------------------------------------------------------------
# Cost-budget tracker.
# ---------------------------------------------------------------------


def _openai_envelope(*, input_tokens: int, output_tokens: int) -> OpenAIEnvelope:
    return OpenAIEnvelope(
        model="gpt-4o-mini-2024-07-18",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=0,
        finish_reason="stop",
    )


def test_openai_envelope_to_usd_scales_with_rate_card() -> None:
    """1M input tokens at the published per-1M-token rate gives the expected USD.

    The rate card lives in ``scripts.stress._context._OPENAI_PRICING_USD_PER_1M``
    keyed by canonical provider id; ``openai_envelope_to_usd`` delegates
    to ``openai_tokens_to_usd`` (which normalizes OpenAI's dated
    model ids onto the canonical provider) so bench and driver share
    one source of truth. Test pins the gpt-4o-mini input rate
    (0.15 USD / 1M tokens) via the dated id ``gpt-4o-mini-2024-07-18``
    the fixture stamps.
    """
    capture = _openai_envelope(input_tokens=1_000_000, output_tokens=0)
    assert openai_envelope_to_usd(capture) == pytest.approx(0.15)


def test_openai_tokens_to_usd_bills_cached_at_per_model_rate() -> None:
    """Cached prompt reads bill at the per-model cached rate, disjoint from input.

    gpt-4o-mini: input 0.15, cached 0.075 (0.5x) per 1M. 1M visible
    input + 1M cached read + 0 output = 0.15 + 0.075 = 0.225.
    gpt-4.1-nano: input 0.10, cached 0.025 (0.25x). Same token split
    = 0.10 + 0.025 = 0.125.
    """
    mini = openai_tokens_to_usd(1_000_000, 0, provider=OPENAI_PROVIDER_GPT_4O_MINI, cached_tokens=1_000_000)
    assert mini == pytest.approx(0.225)
    nano = openai_tokens_to_usd(1_000_000, 0, provider=OPENAI_PROVIDER_GPT_4_1_NANO, cached_tokens=1_000_000)
    assert nano == pytest.approx(0.125)


def test_openai_tokens_to_usd_cached_defaults_to_zero() -> None:
    """Omitting cached_tokens leaves the bill identical to the input+output sum."""
    no_cache = openai_tokens_to_usd(1_000_000, 0, provider=OPENAI_PROVIDER_GPT_4O_MINI)
    explicit_zero = openai_tokens_to_usd(1_000_000, 0, provider=OPENAI_PROVIDER_GPT_4O_MINI, cached_tokens=0)
    assert no_cache == pytest.approx(0.15)
    assert explicit_zero == pytest.approx(0.15)


def test_openai_tokens_to_usd_unknown_provider_is_none_even_with_cached() -> None:
    """An unmapped provider returns None regardless of the cached count."""
    assert openai_tokens_to_usd(1_000, 1_000, provider="anthropic-haiku", cached_tokens=500) is None


def test_openai_envelope_to_usd_passes_cached_tokens_through() -> None:
    """openai_envelope_to_usd bills the envelope's cached_tokens disjointly.

    Envelope carries 1M visible input + 2M cached reads + 0 output on
    gpt-4o-mini: 0.15 (input) + 2 * 0.075 (cached) = 0.30.
    """
    env = OpenAIEnvelope(
        model="gpt-4o-mini-2024-07-18",
        input_tokens=1_000_000,
        output_tokens=0,
        cached_tokens=2_000_000,
        finish_reason="stop",
    )
    assert openai_envelope_to_usd(env) == pytest.approx(0.30)


def test_cost_budget_tracker_advances_on_openai_call() -> None:
    """Recording an OpenAI capture advances observed_usd by the rate-card cost."""
    tracker = CostBudgetTracker(max_usd=10.0)
    tracker.record_openai(_openai_envelope(input_tokens=1_000_000, output_tokens=0))
    assert tracker.observed_usd == pytest.approx(0.15)


def test_cost_budget_tracker_treats_none_cost_as_unknown() -> None:
    """A claude envelope with cost_usd=None increments unknown_count, not observed_usd."""
    tracker = CostBudgetTracker(max_usd=5.0)
    tracker.record_claude(None)
    assert tracker.unknown_usd_call_count == 1
    assert tracker.observed_usd == 0.0


def test_cost_budget_tracker_gemini_call_is_unknown() -> None:
    """A gemini call is always unknown — Gemini does not surface per-call cost."""
    tracker = CostBudgetTracker(max_usd=5.0)
    tracker.record_gemini()
    assert tracker.unknown_usd_call_count == 1
    assert tracker.observed_usd == 0.0


def test_cost_budget_tracker_claude_cost_is_notional_not_metered() -> None:
    """A claude subscription cost advances notional_subscription_usd, not observed_usd.

    claude runs via the subscription CLI (``claude -p``, no API key), so its
    reported ``cost_usd`` is notional -- surfaced for transparency but never
    folded into the metered budget the gate caps.
    """
    tracker = CostBudgetTracker(max_usd=5.0)
    tracker.record_claude(0.084)
    assert tracker.notional_subscription_usd == pytest.approx(0.084)
    assert tracker.observed_usd == 0.0
    assert tracker.unknown_usd_call_count == 0


def test_cost_budget_tracker_notional_claude_cost_never_aborts() -> None:
    """Regression: notional subscription cost must NEVER trip the budget gate.

    The original bug: opus-priced ``claude -p`` envelopes reported ~$0.084/call;
    folding that notional figure into a $5 real-dollar budget aborted the run at
    iteration 31 of 50 -- killing the entire subscribe arm -- on money a
    subscriber never paid. The gate must consider only genuinely-metered
    (OpenAI) spend. Here: 100 expensive claude iterations against a $1 budget
    must not abort, because none of it is metered.
    """
    tracker = CostBudgetTracker(max_usd=1.0)
    for _ in range(100):
        tracker.begin_iteration()
        tracker.record_claude(0.084)  # opus-priced notional cost
        assert tracker.should_abort() is False
    assert tracker.notional_subscription_usd == pytest.approx(8.4)
    assert tracker.observed_usd == 0.0


def test_cost_budget_tracker_first_iteration_never_aborts() -> None:
    """begin_iteration(); should_abort() must return False on the first iteration."""
    tracker = CostBudgetTracker(max_usd=1.0)
    tracker.begin_iteration()
    assert tracker.should_abort() is False


def test_cost_budget_tracker_should_abort_when_mean_extrapolation_breaches_budget() -> None:
    """Two iterations at $0.50 each, $1.00 budget: the third iteration should abort."""
    tracker = CostBudgetTracker(max_usd=1.0)
    # Two recorded iterations of $0.50 each (3.33M output tokens at 0.6/1M).
    fake_capture = OpenAIEnvelope(
        model="gpt-4o-mini",
        input_tokens=0,
        output_tokens=3_333_333 * 2 // 2,  # $0.50/iter total
        cached_tokens=0,
        finish_reason="stop",
    )
    # Two iterations, then check the gate before a third.
    tracker.begin_iteration()
    tracker.record_openai(fake_capture)
    tracker.begin_iteration()
    tracker.record_openai(fake_capture)
    tracker.begin_iteration()
    assert tracker.should_abort() is True


def test_cost_budget_tracker_single_expensive_iteration_trips_gate() -> None:
    """One high-variance iteration trips the gate where the mean would not.

    Budget $1.00. Five cheap iterations (~$0.02 each) then one expensive
    iteration (~$0.70). The running MEAN per iteration stays small, so a
    mean-projection (observed + mean) reads ~$0.93 and would NOT abort.
    The MAX-projection (observed + the largest single-iteration delta,
    ~$0.70) reads ~$1.50 and DOES abort -- the load-bearing distinction.
    gpt-4o-mini output bills at 0.6 USD / 1M tokens.
    """
    tracker = CostBudgetTracker(max_usd=1.0)
    cheap = _openai_envelope(input_tokens=0, output_tokens=33_333)  # ~$0.02/call
    spike = _openai_envelope(input_tokens=0, output_tokens=1_166_666)  # ~$0.70/call

    # Five cheap iterations: observed ~= $0.10, mean ~= $0.02.
    for _ in range(5):
        tracker.begin_iteration()
        tracker.record_openai(cheap)
    # Mean-projection (observed + mean) ~= 0.10 + 0.02 = 0.12 -- nowhere
    # near the $1.00 budget. The max so far is also ~$0.02.
    tracker.begin_iteration()  # closes the 5th cheap iteration's delta
    assert tracker.should_abort() is False

    # One expensive iteration: observed ~= $0.80, but its delta is ~$0.70.
    tracker.record_openai(spike)
    tracker.begin_iteration()  # closes the spike iteration, max_iter_cost ~= 0.70
    # Mean-projection would be observed(0.80) + mean(0.80/6 ~= 0.13) = 0.93
    # < 1.00 and would NOT abort. Max-projection is observed(0.80) +
    # max(0.70) = 1.50 >= 1.00 and DOES abort.
    observed = tracker.observed_usd
    mean = observed / 6
    assert observed + mean < tracker.max_usd  # the OLD mean gate would not trip
    assert tracker.should_abort() is True  # the MAX gate does trip


def test_cost_budget_tracker_max_iter_cost_ignores_zero_width_first_window() -> None:
    """The first begin_iteration's zero-width prior window contributes no cost.

    A single recorded iteration leaves ``_iters == 1`` so ``should_abort``
    short-circuits to False regardless of cost, and the spurious 0.0
    delta from the first ``begin_iteration`` never inflates the max.
    """
    tracker = CostBudgetTracker(max_usd=0.01)
    tracker.begin_iteration()
    tracker.record_openai(_openai_envelope(input_tokens=0, output_tokens=1_000_000))
    assert tracker.should_abort() is False  # _iters == 1 guard holds


def test_cost_budget_tracker_max_iter_cost_folds_unclosed_final_window() -> None:
    """The final iteration's delta is reflected even without a trailing begin_iteration.

    The bench loops check the gate at the TOP of each iteration (right after
    ``begin_iteration``) and simply end after the last one, so the final
    iteration's delta is never folded into ``_max_iter_cost`` by a subsequent
    ``begin_iteration``. ``max_iter_cost`` folds the still-open window on read,
    keeping the value correct after the loop's last iteration and guarding any
    post-loop consumer against a stale max.
    """
    tracker = CostBudgetTracker(max_usd=10.0)
    tracker.begin_iteration()
    tracker.record_openai(_openai_envelope(input_tokens=0, output_tokens=33_333))  # ~$0.02
    tracker.begin_iteration()  # closes the first iteration (~$0.02)
    tracker.record_openai(_openai_envelope(input_tokens=0, output_tokens=1_166_666))  # ~$0.70
    # The costly second iteration is left UNCLOSED (no trailing begin_iteration),
    # mirroring the bench loop ending after its last window. The raw field still
    # holds only the first iteration's ~$0.02; the fold-on-read property sees the
    # open ~$0.70 window.
    assert tracker.max_iter_cost == pytest.approx(0.70, rel=1e-3)


# ---------------------------------------------------------------------
# Cold-cache contamination counter.
# ---------------------------------------------------------------------


def _claude_envelope(*, cache_read: int) -> ClaudeEnvelope:
    return ClaudeEnvelope(
        input_tokens_visible=10,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=cache_read,
        output_tokens=5,
        billed_input_tokens=10 + cache_read,
        total_billed_tokens=15 + cache_read,
        cost_usd=None,
        model="claude-haiku-4-5-20251001",
        stop_reason="end_turn",
        is_error=False,
        api_error_status=None,
        terminal_reason="completed",
        num_turns=1,
    )


def _iteration_row(
    *,
    driver: str,
    openai_env: OpenAIEnvelope | None = None,
    claude_env: ClaudeEnvelope | None = None,
) -> IterationRow:
    return IterationRow(
        iter_id=0,
        arm="loaded",
        driver=driver,
        sentinel="s",
        t_send_ns=0,
        t_observe_ns=0,
        latency_ns=0,
        cache_state="NA",
        claude_env=claude_env,
        gemini_env=None,
        openai_env=openai_env,
        invariant_failed=False,
        invariant_failure_field=None,
    )


def test_count_cache_contaminated_rows_clean_run_is_zero() -> None:
    """Cold-cache rows (no cached reads) yield a contamination count of 0."""
    rows = [
        _iteration_row(driver="pydantic", openai_env=_openai_envelope(input_tokens=100, output_tokens=5)),
        _iteration_row(driver="claude-cli", claude_env=_claude_envelope(cache_read=0)),
    ]
    assert count_cache_contaminated_rows(rows) == 0


def test_count_cache_contaminated_rows_counts_openai_cache_read() -> None:
    """An OpenAI row with cached_tokens > 0 counts as one contaminated row."""
    contaminated = OpenAIEnvelope(
        model="gpt-4o-mini-2024-07-18",
        input_tokens=100,
        output_tokens=5,
        cached_tokens=64,
        finish_reason="stop",
    )
    rows = [_iteration_row(driver="pydantic", openai_env=contaminated)]
    assert count_cache_contaminated_rows(rows) == 1


def test_count_cache_contaminated_rows_counts_claude_cache_read() -> None:
    """A Claude row with cache_read_input_tokens > 0 counts as contaminated."""
    rows = [_iteration_row(driver="claude-cli", claude_env=_claude_envelope(cache_read=512))]
    assert count_cache_contaminated_rows(rows) == 1


def test_count_cache_contaminated_rows_sums_across_drivers() -> None:
    """Both an OpenAI and a Claude contaminated row in the same run sum to 2."""
    openai_warm = OpenAIEnvelope(
        model="gpt-4o-mini-2024-07-18",
        input_tokens=100,
        output_tokens=5,
        cached_tokens=32,
        finish_reason="stop",
    )
    rows = [
        _iteration_row(driver="pydantic", openai_env=openai_warm),
        _iteration_row(driver="claude-cli", claude_env=_claude_envelope(cache_read=128)),
        _iteration_row(driver="langgraph", openai_env=_openai_envelope(input_tokens=50, output_tokens=2)),
    ]
    assert count_cache_contaminated_rows(rows) == 2


def test_count_cache_contaminated_rows_ignores_non_llm_rows() -> None:
    """A row with no LLM envelope (all None) does not count."""
    rows = [_iteration_row(driver="missing")]
    assert count_cache_contaminated_rows(rows) == 0


def test_envelope_substructs_are_frozen() -> None:
    """The substructs are immutable so a verdict row cannot be mutated post-hoc."""
    env = ClaudeEnvelope(
        input_tokens_visible=1,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
        output_tokens=1,
        billed_input_tokens=1,
        total_billed_tokens=2,
        cost_usd=None,
        model="claude-haiku-4-5-20251001",
        stop_reason="end_turn",
        is_error=False,
        api_error_status=None,
        terminal_reason="completed",
        num_turns=1,
    )
    with pytest.raises(AttributeError):
        env.cost_usd = 0.5  # type: ignore[misc]
    g_env = GeminiEnvelope(
        prompt_tokens=1,
        candidates_tokens=1,
        thoughts_tokens=0,
        cached_tokens=0,
        tool_tokens=0,
        total_tokens_reported=2,
        total_tokens_recomputed=2,
        cost_usd=None,
        model="gemini-2.5-flash",
        stop_reason=None,
        is_error=False,
        api_error_status=None,
        terminal_reason=None,
        num_turns=1,
    )
    with pytest.raises(AttributeError):
        g_env.prompt_tokens = 100  # type: ignore[misc]


# --- drain_children_concurrently (shared concurrent-stdout drainer) -----------


class _PopenChild:
    """Minimal ``DrainableChild`` wrapping a real subprocess for the drain tests."""

    def __init__(self, proc: subprocess.Popen[bytes]) -> None:
        self.proc = proc

    def terminate(self) -> None:
        with contextlib.suppress(ProcessLookupError):
            self.proc.terminate()


def _spawn_printer(text: str, *, sleep_after: float = 0.0) -> _PopenChild:
    """Spawn a child that writes ``text`` (flushed) then optionally sleeps before exit."""
    code = f"import sys, time; sys.stdout.write({text!r}); sys.stdout.flush(); time.sleep({sleep_after})"
    return _PopenChild(subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE))


def test_drain_children_concurrently_harvests_each_childs_stdout() -> None:
    """Every child's stdout is returned keyed by its index, with a monotonic stamp."""
    children = [_spawn_printer(f"child-{i}\n") for i in range(4)]
    deadline = time.monotonic() + 10.0
    drained = drain_children_concurrently(
        children, deadline_monotonic=deadline, min_remaining_sec=0.1, term_grace_sec=2.0
    )
    assert set(drained) == {0, 1, 2, 3}
    for i in range(4):
        out, t_observe_ns = drained[i]
        assert out.decode().strip() == f"child-{i}"
        assert t_observe_ns > 0


def test_drain_children_concurrently_fast_child_not_blocked_by_slow_sibling() -> None:
    """A slow child must not delay a fast sibling's harvest -- the concurrency contract.

    A serial drain blocks on the slow child's full exit before reading the
    fast one, so the fast child would be stamped at the slow child's EOF.
    Concurrent draining stamps each child at its OWN EOF: the fast child's
    ``t_observe_ns`` strictly precedes the slow child's.
    """
    slow = _spawn_printer("slow\n", sleep_after=2.0)  # index 0, holds its process open ~2s
    fast = _spawn_printer("fast\n", sleep_after=0.0)  # index 1, exits immediately
    deadline = time.monotonic() + 10.0
    drained = drain_children_concurrently(
        [slow, fast], deadline_monotonic=deadline, min_remaining_sec=0.1, term_grace_sec=2.0
    )
    assert drained[1][0].decode().strip() == "fast"
    assert drained[0][0].decode().strip() == "slow"
    assert drained[1][1] < drained[0][1]


def test_drain_children_concurrently_terminates_a_child_past_the_deadline() -> None:
    """A child outliving the deadline is terminated; already-buffered output survives.

    The pre-sleep flushed write lands in the pipe before the long sleep, so
    the post-terminate grace drain still recovers it -- the drainer neither
    hangs to the 30 s sleep nor loses the already-emitted bytes.
    """
    slow = _spawn_printer("started\n", sleep_after=30.0)
    deadline = time.monotonic() + 0.5
    started = time.monotonic()
    drained = drain_children_concurrently(
        [slow], deadline_monotonic=deadline, min_remaining_sec=0.1, term_grace_sec=2.0
    )
    elapsed = time.monotonic() - started
    assert 0 in drained
    assert b"started" in drained[0][0]
    assert elapsed < 10.0
