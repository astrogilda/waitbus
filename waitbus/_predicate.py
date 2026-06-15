"""Source-agnostic event-match predicate engine.

Layer 1 of the wait-CLI predicate grammar (named-condition registry +
AND-combined ``--match dotted.key=json_literal``). Stdlib-only,
import-pure, no dependency on typer, sockets, sqlite3 or anyio --
the module is unit-testable with plain dict frames.

The engine seam this slots into is :func:`waitbus._broadcast_sub.await_predicate`,
which takes a ``decide: Callable[[dict[str, Any]], FrameDecision]`` callback.
The three existing sibling closures are ``wait._decide``,
``coalesce._accumulate``, and ``_emit_predicate._decide``;
:class:`Predicate` produced here is the *bool* half (does this frame
match?), and the wait CLI's own ``_decide`` wraps that bool in the
:class:`FrameDecision` ladder (CONTINUE / MATCHED + caller side-effects
like exit-code capture). Keeping the predicate engine engine-free is
the load-bearing separation: a future ``broadcast tap --match`` or
``replay --match`` consumer composes the same Predicate without
inheriting wait's exit-code semantics.

Grammar:

    --match  ::= <dotted_key> '=' <json_literal>

Multiple ``--match`` flags AND across distinct keys; repeating the
same key ORs within that key (Docker ``--filter`` precedent;
NOT the kubectl ``--for=jsonpath`` shape -- that grammar's
flag parser splits on ``=`` and has five open issues at
github.com/kubernetes/kubectl directly attributable to the model).
``<json_literal>`` is anything ``json.loads`` accepts, so types are
precise (``fields.run_id=12345`` is int 12345, not the string
``"12345"``; ``fields.merged=true`` is bool ``True``).

Layer 2 (CEL / JMESPath / future expressive DSLs) attaches via
:func:`register_evaluator`. The wait CLI's ``--match-cel`` and
``--match-jmespath`` flags route to this seam; without a registered
evaluator they raise :class:`EvaluatorUnavailableError` with a hint
naming the extras-package the operator should ``pip install``. No
``[project.optional-dependencies]`` or entry-points group is declared
currently -- those are registered when the first plugin package is installed. The
seam alone is enough to prevent the wait surface from later changing
shape to accept a Layer-2 expression.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

__all__ = [
    "EvaluatorUnavailableError",
    "Predicate",
    "compose",
    "compose_any",
    "parse_match",
    "register_condition",
    "register_evaluator",
    "resolve_condition",
    "resolve_evaluator",
]

# Cap on the textual length of a single ``--match k=v`` spec OR a single
# Layer-2 expression. 256 bytes covers every realistic key=json_literal
# plus a CEL/JMESPath expression with room to spare; rejects pathological
# LLM-emitted predicates (the agent-emitted-tool-argument injection class
# (OWASP LLM01:2025 Prompt Injection); RFC 9535 §4 notes safe predicate
# evaluation in adversarial contexts requires implementer-supplied
# length/time limits, which this cap enforces).
# Independently re-enforced by the wait CLI before the predicate reaches
# this module. The cap is per-spec, not on the join — multiple in-bound
# ``--match`` flags can legitimately AND across to a joined string >256
# bytes, and the natural unit an operator types is one flag.
_MAX_PREDICATE_SOURCE_LEN: Final[int] = 256


class EvaluatorUnavailableError(RuntimeError):
    """Raised when a Layer-2 ``--match-<lang>`` flag is used without the
    corresponding evaluator plugin registered.

    The message is shaped exactly:
        "to use --match-<lang>, install waitbus[<lang>]"

    so a typer ``except`` block can surface it verbatim to the operator
    without re-formatting. The wait CLI catches this and re-raises as
    a startup-time exit-2 with the same message (one canonical error
    path, no double-wrapping).
    """


# Sentinel distinct from JSON ``null`` (which is a valid match target:
# the user MAY write --match fields.head_sha=null to match an absent
# head_sha that the schema represents as JSON null). Must NOT be exposed
# in the public API; comparisons use ``is`` not ``==``.
_MISSING: Any = object()


@dataclass(frozen=True, slots=True)
class Predicate:
    """A pure boolean function from a broadcast frame, plus its source text.

    ``evaluate(frame)`` is invoked once per non-heartbeat frame inside
    the engine seam. It MUST NOT mutate ``frame``, raise, sleep, or do
    I/O. Engine-side timeouts are bounded by the calling decide-closure
    (the wait deadline), not here.

    ``source`` is the original CLI text (a joined --match flag set, a
    --cond name, or a Layer-2 expression). Stored on the predicate so
    the wait CLI can log it verbatim on a timeout, and so a future
    forensic-traceback path (e.g. waitbus emit on a wait timeout) can
    record which predicate failed. Documented as the threat-model
    defense for LLM-emitted predicates: a malicious indirect prompt
    injection that produces a runaway --match is recoverable because
    the predicate text is in the local event store BEFORE evaluation.
    """

    evaluate: Callable[[dict[str, Any]], bool]
    source: str

    def __call__(self, frame: dict[str, Any]) -> bool:
        """Convenience: ``pred(frame)`` is ``pred.evaluate(frame)``."""
        return self.evaluate(frame)


# --- named-condition + evaluator registries ----------------------------------

_CONDITIONS: dict[str, Callable[[], Predicate]] = {}
_EVALUATORS: dict[str, Callable[[str], Predicate]] = {}


def register_condition(name: str, factory: Callable[[], Predicate]) -> None:
    """Register a named, zero-argument predicate factory under ``name``.

    The factory is invoked at ``resolve_condition`` time, NOT at
    registration -- defers any heavy construction (e.g. compiling a
    cached AST) to first use. Re-registration with the same name
    raises ``ValueError``; silent overwrite is the plugin-collision
    footgun. Names must match ``[a-z][a-z0-9_-]{0,31}`` so they are
    safe in CLI flag values and structured log fields.
    """
    if not name or not all(c.isalnum() or c in "_-" for c in name) or not name[0].isalpha():
        raise ValueError(f"condition name must match [a-z][a-z0-9_-]+, got {name!r}")
    if len(name) > 32:
        raise ValueError(f"condition name longer than 32 chars: {name!r}")
    if name in _CONDITIONS:
        raise ValueError(f"condition {name!r} already registered")
    _CONDITIONS[name] = factory


def resolve_condition(name: str) -> Predicate:
    """Look up a registered condition by name.

    Raises ``KeyError`` whose message names the registered conditions
    so a typo at the CLI surfaces as a startup error with a usable
    hint, not a silent never-match.
    """
    if name not in _CONDITIONS:
        known = ", ".join(sorted(_CONDITIONS)) or "(none registered)"
        raise KeyError(f"unknown condition {name!r}; registered: {known}")
    return _CONDITIONS[name]()


def register_evaluator(name: str, factory: Callable[[str], Predicate]) -> None:
    """Register a Layer-2 expression evaluator under ``name``.

    The factory takes the raw expression text and returns a
    :class:`Predicate`. Invoked at ``resolve_evaluator`` time --
    typically per ``waitbus wait`` invocation, so the compile cost is
    paid once per wait, not at every frame.

    Intended caller: a plugin distribution (e.g. ``waitbus-cel``,
    ``waitbus-jmespath``) that imports ``waitbus`` and calls
    ``register_evaluator("cel", cel_factory)`` at its own import time
    (typically from an ``__init__.py`` or an explicit
    ``waitbus_cel.install()`` entry point). No ``importlib.metadata``
    entry-point group is declared in core -- the registry is the
    promotion path; entry-points land when the first plugin author
    asks for them.
    """
    if not name or not all(c.isalnum() or c == "-" for c in name):
        raise ValueError(f"evaluator name must match [a-z0-9-]+, got {name!r}")
    if name in _EVALUATORS:
        raise ValueError(f"evaluator {name!r} already registered")
    _EVALUATORS[name] = factory


def resolve_evaluator(name: str, expr: str) -> Predicate:
    """Compile a Layer-2 expression via the named evaluator.

    Raises :class:`EvaluatorUnavailableError` (NOT KeyError) when the
    evaluator is not registered -- the operator gets a hint to
    ``pip install waitbus[<name>]`` rather than a stack trace.
    """
    if len(expr) > _MAX_PREDICATE_SOURCE_LEN:
        raise ValueError(f"--match-{name} expression exceeds {_MAX_PREDICATE_SOURCE_LEN}-byte cap")
    if name not in _EVALUATORS:
        raise EvaluatorUnavailableError(f"to use --match-{name}, install waitbus[{name}]")
    return _EVALUATORS[name](expr)


# --- Layer-1 parser + dotted-key resolver ------------------------------------


def _dig(payload: Any, dotted_key: str) -> Any:
    """Traverse a dotted path over nested ``dict`` / ``list`` structure.

    Dict keys match by string equality; list indices accept signed
    integer segments (``items.0`` and ``items.-1`` both work). Any
    missing step (key absent, list-index out of range, mid-traversal
    scalar) returns :data:`_MISSING`, a module-private sentinel
    distinct from ``None`` -- callers compare with ``is``, not ``==``,
    so a JSON ``null`` value remains a legitimate match target.

    Does NOT raise on shape mismatch: the predicate's job is "equals",
    not "well-formed-frame"; an ill-shaped frame correctly fails to
    match rather than crashing the wait loop.
    """
    cur: Any = payload
    for part in dotted_key.split("."):
        if isinstance(cur, Mapping):
            if part not in cur:
                return _MISSING
            cur = cur[part]
        elif isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError:
                return _MISSING
            if not -len(cur) <= idx < len(cur):
                return _MISSING
            cur = cur[idx]
        else:
            return _MISSING
    return cur


def _parse_one_match(spec: str) -> tuple[str, Any]:
    """Parse a single ``key=json_literal`` spec.

    The split is on the FIRST ``=`` only, so JSON literals containing
    ``=`` (e.g. a base64 string) parse correctly. Raises ``ValueError``
    naming the offending spec verbatim for forensic traceback.
    """
    if "=" not in spec:
        raise ValueError(f"--match must be key=json_literal, got {spec!r}")
    key, _, raw = spec.partition("=")
    key = key.strip()
    if not key:
        raise ValueError(f"--match key must be non-empty, got {spec!r}")
    if any(seg == "" for seg in key.split(".")):
        raise ValueError(f"--match key has an empty dotted segment, got {spec!r}")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f'--match RHS must be a JSON literal (true / 42 / "text" / null), got {raw!r} in {spec!r}: {exc.msg}'
        ) from exc
    return key, value


def parse_match(specs: Sequence[str]) -> Predicate:
    """Parse zero or more ``--match k=v`` specs into one AND Predicate.

    Returns a Predicate whose ``evaluate`` is True iff EVERY distinct
    key matches at least ONE of its associated values (AND across
    distinct keys; OR within a repeated key). Empty input returns a
    tautology Predicate (always True) so callers can call this
    unconditionally without a None branch.

    Raises ``ValueError`` on a malformed spec (no ``=``, empty key,
    unparseable JSON RHS, OR an individual spec longer than the
    256-byte cap). The cap is applied PER-SPEC, not on the joined
    string -- a legitimate AND of small predicates can exceed 256
    bytes when joined, and the natural unit an operator types is one
    flag. The caller (wait CLI) maps ``ValueError`` to startup exit-2
    with the verbatim message.
    """
    by_key: dict[str, list[Any]] = {}
    for spec in specs:
        if len(spec) > _MAX_PREDICATE_SOURCE_LEN:
            raise ValueError(
                f"--match spec exceeds {_MAX_PREDICATE_SOURCE_LEN}-byte cap (got {len(spec)} bytes): {spec[:80]!r}..."
            )
        key, value = _parse_one_match(spec)
        by_key.setdefault(key, []).append(value)

    joined = " ".join(specs)

    # Tautology is allowed: callers compose this with other predicates
    # via compose() and an always-True identity element keeps composition
    # uniform (no None branches at call sites).
    items: tuple[tuple[str, tuple[Any, ...]], ...] = tuple((k, tuple(vs)) for k, vs in by_key.items())

    def _evaluate(frame: dict[str, Any]) -> bool:
        for key, allowed in items:
            got = _dig(frame, key)
            if got is _MISSING:
                return False
            if got not in allowed:
                return False
        return True

    return Predicate(evaluate=_evaluate, source=joined)


# GitHub stores the full 40-char ``head_sha`` (straight from the webhook /
# ``gh``), but humans type and read the 7-char abbreviation everywhere
# (commit URLs, ``git log --oneline``, the GitHub UI). ``waitbus wait --sha``
# therefore matches a git-style prefix rather than the exact full SHA the
# generic ``--match fields.head_sha=`` path requires. 7 hex chars (28 bits)
# keeps a prefix collision within one repo's wait window negligible; 4 (git's
# floor) would not on a live stream where uniqueness can't be pre-verified.
_MIN_SHA_PREFIX_LEN = 7
_SHA_HEX_ALPHABET = frozenset("0123456789abcdef")


def head_sha_prefix(prefix: str) -> Predicate:
    """Build a case-insensitive git-style ``fields.head_sha`` prefix Predicate.

    ``prefix`` must be hex and at least :data:`_MIN_SHA_PREFIX_LEN` chars
    (and no longer than a 40-char SHA-1); otherwise ``ValueError``, which
    the wait CLI maps to a startup exit-2 so a malformed ``--sha`` is never
    a silent never-match. Matching is case-insensitive because SHAs are
    lowercase hex but a pasted value may arrive upper-cased.

    Streaming semantics: ``waitbus wait`` resolves on the FIRST frame whose
    ``head_sha`` carries this prefix. Unlike ``git``, an online event stream
    has no static object set to verify prefix uniqueness against, so a
    (vanishingly unlikely at >=7 hex chars) collision resolves to whichever
    matching frame arrives first. Only the ``--sha`` affordance is
    prefix-matched; the generic ``--match fields.head_sha=`` path stays exact.
    """
    norm = prefix.strip().lower()
    if len(norm) < _MIN_SHA_PREFIX_LEN:
        raise ValueError(f"--sha must be at least {_MIN_SHA_PREFIX_LEN} hex chars (GitHub short-SHA), got {prefix!r}")
    if len(norm) > 40:
        raise ValueError(f"--sha is longer than a 40-char SHA-1, got {prefix!r}")
    if not all(c in _SHA_HEX_ALPHABET for c in norm):
        raise ValueError(f"--sha must be hexadecimal, got {prefix!r}")

    def _evaluate(frame: dict[str, Any]) -> bool:
        got = _dig(frame, "fields.head_sha")
        return isinstance(got, str) and got.lower().startswith(norm)

    return Predicate(evaluate=_evaluate, source=f'fields.head_sha^="{norm}"')


def compose(*predicates: Predicate) -> Predicate:
    """AND-combine N Predicates into one, short-circuiting on first False.

    ``compose()`` (no args) returns the tautology Predicate (always
    True), so callers can pass arbitrary lists without special-casing
    the empty case. Source text concatenates with `` & `` for
    forensic-log readability.
    """
    if not predicates:
        return Predicate(evaluate=lambda _frame: True, source="")
    if len(predicates) == 1:
        return predicates[0]

    parts = tuple(predicates)
    source = " & ".join(p.source for p in parts if p.source)

    def _evaluate(frame: dict[str, Any]) -> bool:
        return all(p.evaluate(frame) for p in parts)

    return Predicate(evaluate=_evaluate, source=source)


def compose_any(*predicates: Predicate) -> Predicate:
    """OR-combine N Predicates into one, short-circuiting on first True.

    The OR dual of :func:`compose`. ``compose_any()`` (no args) returns
    the always-False Predicate -- the OR identity element -- so the
    combinator stays total (callers composing a clause list never pass
    zero in practice). Source text concatenates with `` | `` for
    forensic-log readability, mirroring ``compose``'s `` & `` join.
    """
    if not predicates:
        return Predicate(evaluate=lambda _frame: False, source="")
    if len(predicates) == 1:
        return predicates[0]

    parts = tuple(predicates)
    source = " | ".join(p.source for p in parts if p.source)

    def _evaluate(frame: dict[str, Any]) -> bool:
        return any(p.evaluate(frame) for p in parts)

    return Predicate(evaluate=_evaluate, source=source)
