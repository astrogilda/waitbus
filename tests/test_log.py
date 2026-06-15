"""Contract tests for `waitbus._log.structured()`.

The helper is shared by every daemon module; the deferral of
structlog hinges on the helper producing journald-friendly compact
JSON with a numeric `ts` field and `default=str` coercion for
non-JSON-native payloads. These tests pin the contract so a future
refactor (e.g., the eventual structlog re-evaluation when OTel
adoption lands) cannot silently drift the wire shape.
"""

from __future__ import annotations

import io
import json
import logging
import time
from typing import Any

import pytest

from waitbus._log import structured


@pytest.fixture
def captured_logger() -> tuple[logging.Logger, io.StringIO]:
    """Return a fresh logger whose only handler writes to an in-memory buffer.

    Each test gets an isolated logger to avoid cross-test handler pollution.
    """
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    # Bare format: the test asserts on the raw JSON payload produced by
    # ``structured``, not on logging's own formatter wrapping.
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger = logging.getLogger(f"test_log.{id(buf)}")
    logger.setLevel(logging.DEBUG)
    logger.handlers = [handler]
    logger.propagate = False
    return logger, buf


def _last_record(buf: io.StringIO) -> dict[str, Any]:
    raw = buf.getvalue().strip().splitlines()[-1]
    record: dict[str, Any] = json.loads(raw)
    return record


def test_structured_emits_one_compact_json_line_per_call(captured_logger: tuple[logging.Logger, io.StringIO]) -> None:
    logger, buf = captured_logger
    structured(logger, logging.INFO, "hello", n=1)
    lines = buf.getvalue().strip().splitlines()
    assert len(lines) == 1
    # Compact separators: no space after comma or colon.
    assert ", " not in lines[0]
    assert ": " not in lines[0]


def test_structured_includes_ts_and_event_fields(captured_logger: tuple[logging.Logger, io.StringIO]) -> None:
    logger, buf = captured_logger
    before = time.time()
    structured(logger, logging.INFO, "trace", request_id="abc")
    after = time.time()
    record = _last_record(buf)
    assert record["event"] == "trace"
    assert record["request_id"] == "abc"
    assert before <= record["ts"] <= after


def test_structured_ts_is_numeric_not_iso_string(captured_logger: tuple[logging.Logger, io.StringIO]) -> None:
    """The log framing pins `ts` as float epoch seconds for jq/grep
    pipelines. A future drift to ISO string would break those pipelines.
    """
    logger, buf = captured_logger
    structured(logger, logging.INFO, "trace")
    record = _last_record(buf)
    assert isinstance(record["ts"], (int, float))
    assert record["ts"] > 1_000_000_000  # plausible 2001+ epoch second


def test_structured_default_str_coerces_non_json_payloads(captured_logger: tuple[logging.Logger, io.StringIO]) -> None:
    """``default=str`` lets callers log arbitrary payload values (exception
    objects, byte strings, frame dicts) without a custom encoder.
    """
    logger, buf = captured_logger
    err = RuntimeError("boom")
    structured(logger, logging.ERROR, "fail", error=err, raw_bytes=b"\x01\x02")
    record = _last_record(buf)
    assert record["event"] == "fail"
    # Both arbitrary payloads survive serialisation as strings.
    assert "boom" in record["error"]
    assert "\\x01\\x02" in record["raw_bytes"] or "01" in record["raw_bytes"]


def test_structured_kwargs_become_top_level_keys(captured_logger: tuple[logging.Logger, io.StringIO]) -> None:
    logger, buf = captured_logger
    structured(logger, logging.INFO, "wire", method="notifications/claude/channel", attempt=3)
    record = _last_record(buf)
    assert record["method"] == "notifications/claude/channel"
    assert record["attempt"] == 3


def test_structured_respects_logger_level(captured_logger: tuple[logging.Logger, io.StringIO]) -> None:
    logger, buf = captured_logger
    logger.setLevel(logging.WARNING)
    # DEBUG below the threshold: nothing emitted.
    structured(logger, logging.DEBUG, "noisy")
    assert buf.getvalue() == ""
    # WARNING at the threshold: emitted.
    structured(logger, logging.WARNING, "loud")
    record = _last_record(buf)
    assert record["event"] == "loud"


def test_structured_event_is_formal_parameter_cannot_be_shadowed_via_kwarg(
    captured_logger: tuple[logging.Logger, io.StringIO],
) -> None:
    """`event` is a positional-or-keyword formal parameter, so a caller
    passing it BOTH positionally and as a kwarg gets a loud TypeError
    instead of a silent shadow. This protects against typo-induced
    log-field corruption.
    """
    logger, _buf = captured_logger
    with pytest.raises(TypeError, match="event"):
        structured(logger, logging.INFO, "shadowed", event="from_kwarg")  # type: ignore[misc]


def test_structured_ts_can_be_shadowed_via_kwarg(captured_logger: tuple[logging.Logger, io.StringIO]) -> None:
    """`ts` is NOT a formal parameter; it is injected by the helper after
    the **fields unpack. A caller-supplied ``ts=<value>`` kwarg therefore
    OVERWRITES the helper's injected timestamp in the resulting record.

    This is occasionally useful (e.g., to backfill a replayed event with
    its original timestamp), but documents the shadow path explicitly so
    a future contributor does not introduce a typo'd ``ts`` kwarg by
    accident expecting it to be ignored.
    """
    logger, buf = captured_logger
    structured(logger, logging.INFO, "replayed", ts=42.0)
    record = _last_record(buf)
    assert record["event"] == "replayed"
    assert record["ts"] == 42.0  # caller's kwarg wins
