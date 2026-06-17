"""Consistency test: §2a frame catalogue in CONSUMER_API.md vs _frame.py.

Assertions:

1. Every Struct's ``kind`` default value appears as a ``###`` heading in
   §2a of CONSUMER_API.md.

2. Every field name on each Struct (excluding ``kind`` itself) appears
   somewhere in the JSON example block under that kind's ``###`` heading.

3. Every entry in ``ALL_FRAME_KINDS`` has a corresponding ``###``
   heading in §2a.

4. The two reject-frame constants in broadcast.py
   (``_SUBSCRIBE_REJECT_VERSION_FRAME``,
   ``_SUBSCRIBE_REJECT_LAG_LIMIT_FRAME``) decode via msgspec to
   ``SubscribeRejectedFrame`` instances whose ``reason`` values match
   what §3 of CONSUMER_API.md documents.
"""

from __future__ import annotations

import re
import struct
from pathlib import Path

import msgspec

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DOC_PATH = _REPO_ROOT / "docs" / "CONSUMER_API.md"

# Import the frame module directly to reference real Struct types.
import importlib

from waitbus._frame import (
    ALL_FRAME_KINDS,
    EventFrame,
    HeartbeatFrame,
    SubscribeAckFrame,
    SubscribeRejectedFrame,
    TruncatedFrame,
)

# The five Structs in the frame catalogue.
_CATALOGUE_STRUCTS: tuple[type[msgspec.Struct], ...] = (
    EventFrame,
    TruncatedFrame,
    HeartbeatFrame,
    SubscribeAckFrame,
    SubscribeRejectedFrame,
)

# The reject-frame constants live in broadcast.py and are imported
# lazily here to avoid a costly full-module import in fast tests.
_BROADCAST_REJECT_NAMES = (
    "_SUBSCRIBE_REJECT_VERSION_FRAME",
    "_SUBSCRIBE_REJECT_LAG_LIMIT_FRAME",
)

# §3 documents exactly these reason values for the framed rejects.
_DOCUMENTED_REASONS = {"version", "lag_limit_exceeded"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_doc() -> str:
    return _DOC_PATH.read_text(encoding="utf-8")


def _section_2a(text: str) -> str:
    """Extract the text from the '## 2a.' heading to the next '## ' heading."""
    m = re.search(r"^## 2a\.", text, re.MULTILINE)
    if not m:
        raise AssertionError("Could not find '## 2a.' heading in CONSUMER_API.md")
    start = m.start()
    # Find the next top-level '## ' heading after 2a.
    rest = text[start + 1 :]
    end_m = re.search(r"^## ", rest, re.MULTILINE)
    end = start + 1 + end_m.start() if end_m else len(text)
    return text[start:end]


def _h3_headings(section: str) -> set[str]:
    """Return all text tokens following '### ' on their own line."""
    return {m.group(1).strip() for m in re.finditer(r"^### (.+)$", section, re.MULTILINE)}


def _json_block_for_kind(section: str, kind: str) -> str:
    """Return the first ```json ... ``` block following the ### kind heading."""
    # Find the heading line.
    pattern = re.compile(
        r"^### `?" + re.escape(kind) + r"`?.*?$\s+(```json\s+.*?```)",
        re.MULTILINE | re.DOTALL,
    )
    m = pattern.search(section)
    if not m:
        return ""
    return m.group(1)


def _kind_default(struct_cls: type[msgspec.Struct]) -> str:
    """Return the default value of the 'kind' field on the Struct."""
    for field in msgspec.structs.fields(struct_cls):
        if field.name == "kind":
            val = field.default
            if val is msgspec.NODEFAULT:
                # All five wire Structs pin ``kind`` to a scalar literal default;
                # a default_factory would defeat the freeze-pinned discriminator.
                # Fail loud rather than silently computing a kind if that changes.
                raise AssertionError(
                    f"{struct_cls.__name__}.kind has no scalar default; the wire discriminator must be a literal"
                )
            return str(val)
    raise AssertionError(f"{struct_cls.__name__} has no 'kind' field")


def _field_names(struct_cls: type[msgspec.Struct]) -> list[str]:
    """Return field names excluding 'kind'."""
    return [f.name for f in msgspec.structs.fields(struct_cls) if f.name != "kind"]


def _decode_framed(raw: bytes) -> SubscribeRejectedFrame:
    """Strip the 4-byte length prefix and decode to SubscribeRejectedFrame."""
    if len(raw) < 4:
        raise ValueError(f"frame too short: {len(raw)} bytes")
    (length,) = struct.unpack(">I", raw[:4])
    payload = raw[4 : 4 + length]
    return msgspec.json.decode(payload, type=SubscribeRejectedFrame)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_struct_kind_has_h3_heading() -> None:
    """Each Struct's kind default must appear as a ### heading in §2a."""
    text = _parse_doc()
    section = _section_2a(text)
    headings = _h3_headings(section)
    missing: list[str] = []
    for cls in _CATALOGUE_STRUCTS:
        kind = _kind_default(cls)
        # Headings look like  ### `event` frame (data)  — match by kind substring.
        found = any(kind in h for h in headings)
        if not found:
            missing.append(f"  {cls.__name__}: kind='{kind}' not found in §2a headings {headings!r}")
    if missing:
        raise AssertionError("Frame catalogue heading(s) missing from §2a:\n" + "\n".join(missing))


def test_struct_fields_in_json_example() -> None:
    """Every field name (except 'kind') must appear in the JSON example block."""
    text = _parse_doc()
    section = _section_2a(text)
    violations: list[str] = []
    for cls in _CATALOGUE_STRUCTS:
        kind = _kind_default(cls)
        block = _json_block_for_kind(section, kind)
        if not block:
            # Heading is missing — that will be caught by test_struct_kind_has_h3_heading.
            continue
        for field in _field_names(cls):
            if f'"{field}"' not in block:
                violations.append(f"  {cls.__name__}.{field}: not found in JSON example for '{kind}'")
    if violations:
        raise AssertionError("Field(s) missing from §2a JSON examples:\n" + "\n".join(violations))


def test_all_frame_kinds_have_heading() -> None:
    """Every value in ALL_FRAME_KINDS must have a ### heading in §2a."""
    text = _parse_doc()
    section = _section_2a(text)
    headings = _h3_headings(section)
    missing: list[str] = []
    for kind in sorted(ALL_FRAME_KINDS):
        found = any(kind in h for h in headings)
        if not found:
            missing.append(f"  '{kind}' — no §2a heading found (headings: {sorted(headings)!r})")
    if missing:
        raise AssertionError("ALL_FRAME_KINDS entry/entries without §2a heading:\n" + "\n".join(missing))


def test_reject_frame_constants_decode_to_documented_reasons() -> None:
    """The three reject-frame byte constants decode to SubscribeRejectedFrame
    instances whose 'reason' values are all covered by §3's documented set."""

    broadcast = importlib.import_module("waitbus.broadcast")

    frames_and_names: list[tuple[str, bytes]] = [(name, getattr(broadcast, name)) for name in _BROADCAST_REJECT_NAMES]
    reasons_seen: set[str] = set()
    for name, raw in frames_and_names:
        frame = _decode_framed(raw)
        assert isinstance(frame, SubscribeRejectedFrame), (
            f"{name} did not decode to SubscribeRejectedFrame; got {type(frame)}"
        )
        assert frame.reason, f"{name}: decoded frame has empty reason"
        reasons_seen.add(frame.reason)

    undocumented = reasons_seen - _DOCUMENTED_REASONS
    assert not undocumented, (
        f"Reject frames use reason(s) not documented in §3: {undocumented!r}. "
        f"Add them to CONSUMER_API.md §3 or update _DOCUMENTED_REASONS in this test."
    )
