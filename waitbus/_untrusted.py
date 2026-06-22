"""Neutralise webhook-derived free text before it reaches an agent.

Every string waitbus surfaces to an MCP client originates in an external,
attacker-influenceable payload: a GitHub ``display_title``/commit
message, a ``workflow_name``/``job_name`` from a fork PR's workflow
YAML, an Alertmanager ``alertname``, or the raw webhook ``payload_json``.
None of it is trusted instruction text. Without an explicit boundary an
LLM consuming a ``notifications/claude/channel`` summary cannot
distinguish "the commit title is X" from an injected "ignore previous
instructions" smuggled into that title.

Two defences, applied at the emission seam (not at ingest -- the stored
row stays byte-faithful for forensics):

* ``strip_control`` removes characters that carry no legitimate signal
  in a one-line status summary but are the usual injection carriers:
  C0 controls (except tab/newline), DEL, the C1 block, zero-width and
  bidi-override codepoints, and ANSI CSI/OSC escape sequences.
* ``fence`` wraps the cleaned text in a single, fixed, self-describing
  delimiter and neutralises any occurrence of that delimiter inside the
  text, so the boundary cannot be spoofed by the payload itself.

The fence tag is intentionally fixed (not a per-process nonce): a
stable contract is greppable, testable, and documentable for downstream
consumers, and delimiter-injection is already defeated by the
inner-occurrence neutralisation.
"""

from __future__ import annotations

import re
from typing import Any, Final

#: Opening / closing fence. Fixed and public: downstream consumers may
#: rely on this exact contract. Collisions inside the payload are
#: neutralised by :func:`fence`, so a fixed tag is not a spoofing risk.
_FENCE_OPEN: Final = "<waitbus:untrusted>"
_FENCE_CLOSE: Final = "</waitbus:untrusted>"

#: Tool-response delimiter for attacker-controllable event fields (PR
#: titles, commit messages, log/body text) returned inside a structured
#: tool result. A consuming LLM treats text between these tags as inert
#: external data, never as instructions. Distinct from the channel-summary
#: :data:`_FENCE_OPEN`/:data:`_FENCE_CLOSE` pair so a consumer can tell a
#: one-line channel summary apart from a wrapped structured field.
_EXTERNAL_OPEN: Final = "<external_event_data>"
_EXTERNAL_CLOSE: Final = "</external_event_data>"

# C0 controls except TAB (0x09) and LF (0x0A); DEL (0x7F); C1 (0x80-0x9F);
# zero-width + bidi-override codepoints commonly used to hide payloads;
# U+2028 LINE SEPARATOR and U+2029 PARAGRAPH SEPARATOR (some renderers
# treat them as line breaks an attacker could use to separate an
# injected instruction from the fenced label).
_CONTROL_RE: Final = re.compile(
    "[\x00-\x08\x0b-\x1f\x7f-\x9f"
    "\u200b-\u200f"  # ZWSP, ZWNJ, ZWJ, LRM, RLM
    "\u2028\u2029"  # LINE SEPARATOR, PARAGRAPH SEPARATOR
    "\u202a-\u202e"  # bidi formatting (LRE, RLE, PDF, LRO, RLO)
    "\u2060"  # WORD JOINER
    "\u2066-\u2069"  # isolate controls (LRI, RLI, FSI, PDI)
    "\ufeff"  # ZWNBSP / BOM
    "]"
)
# ANSI escape sequences: CSI (ESC [ ... final) and OSC (ESC ] ... BEL/ST).
_ANSI_RE: Final = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")
# A literal close-fence anywhere in the payload, case-insensitively, with
# optional internal whitespace -- the only way to forge the boundary.
_CLOSE_SPOOF_RE: Final = re.compile(r"<\s*/\s*waitbus\s*:\s*untrusted\s*>", re.IGNORECASE)
# A literal external-data close tag anywhere in the payload, case-
# insensitively, with optional internal whitespace -- the only way to forge
# the wrap_external boundary.
_EXTERNAL_CLOSE_SPOOF_RE: Final = re.compile(r"<\s*/\s*external_event_data\s*>", re.IGNORECASE)
# Provenance-label allowlist: only these characters survive into the
# opening tag's attribute, so the label can never break out of it.
_LABEL_RE: Final = re.compile(r"[^A-Za-z0-9._-]")


def strip_control(text: str) -> str:
    """Drop ANSI escapes and control/zero-width/bidi codepoints.

    Tab and newline survive (legitimate in multi-field summaries);
    everything else in the control/format ranges is removed rather than
    escaped -- a status line has no use for it and rendering it is the
    risk.
    """
    return _CONTROL_RE.sub("", _ANSI_RE.sub("", text))


def clean_opt(value: Any) -> Any:
    """``strip_control`` a value iff it is a ``str``; pass through otherwise.

    Projection helpers carry a mix of attacker free text (``workflow_name``,
    ``job_name``, ``alert_name`` ...) and structured non-strings (ids,
    ``None``). This preserves ``None``/non-string semantics (clients
    distinguish null from "") while neutralising hidden control/format
    codepoints in any string that did originate in a payload.
    """
    return strip_control(value) if isinstance(value, str) else value


def fence(text: str, *, label: str) -> str:
    """Return ``text`` cleaned and wrapped in the untrusted delimiter.

    ``label`` is a short, waitbus-controlled provenance tag (e.g.
    ``"event-summary"``) recorded on the open fence so a consumer/operator
    can see what kind of external data this is. The label is itself
    control-stripped defensively. Any attempt to embed the close fence
    inside ``text`` is rewritten so the boundary is unforgeable.

    Security-load-bearing element: **only the close fence**. A literal
    open tag inside ``text`` would just nest visually -- the consumer
    discriminates on the (single, server-controlled) close, not on
    counting opens -- so the open-fence spoof is inert. The
    :data:`_CLOSE_SPOOF_RE` rewrite is the actual escape-prevention
    invariant; the open tag's label allowlist is defence-in-depth.
    """
    clean = _CLOSE_SPOOF_RE.sub("(waitbus:untrusted)", strip_control(text))
    # The label is waitbus-controlled provenance, but constrain it to a
    # strict allowlist so a future caller cannot break out of the
    # attribute (no quotes/angle-brackets/spaces can ever reach the tag).
    safe_label = _LABEL_RE.sub("", label) or "external"
    return f'{_FENCE_OPEN[:-1]} label="{safe_label}">{clean}{_FENCE_CLOSE}'


def wrap_external(text: str) -> str:
    """Wrap an attacker-controllable tool-response field in the external delimiter.

    Companion to :func:`fence` for STRUCTURED tool results (get_event /
    tail_events / query_ci), where an event field carries free text an
    attacker can influence (a PR title, a commit message, a job/workflow
    name). The cleaned text is bracketed by
    ``<external_event_data>...</external_event_data>`` so a consuming LLM
    compartmentalises it from instructions. As with :func:`fence`, the
    security-load-bearing element is the close tag: any literal close tag
    inside ``text`` is rewritten so the boundary is unforgeable. The text
    is also control-stripped (same hygiene as :func:`clean_opt`), so a
    caller may pass either a raw or an already-stripped value.
    """
    clean = _EXTERNAL_CLOSE_SPOOF_RE.sub("(external_event_data)", strip_control(text))
    return f"{_EXTERNAL_OPEN}{clean}{_EXTERNAL_CLOSE}"


def wrap_external_opt(value: Any) -> Any:
    """:func:`wrap_external` a value iff it is a non-empty ``str``; else pass through.

    Preserves ``None`` and non-string semantics (clients distinguish null
    from a wrapped empty string) and leaves an empty string unwrapped --
    wrapping an empty value adds delimiter noise with no untrusted span to
    isolate.
    """
    return wrap_external(value) if isinstance(value, str) and value else value
