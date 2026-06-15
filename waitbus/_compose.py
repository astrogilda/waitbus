"""Cross-source clause composition for the wait surface.

Stdlib-only and import-pure like :mod:`waitbus._predicate` -- unit-testable
with plain dict frames, no typer / socket / sqlite3 dependency.

Clause grammar (shared by ``waitbus wait --all-of`` / ``--first-of`` and the
SDK ``wait_for(all_of=...)`` / ``first_of=...``)::

    <clause> ::= <source> ':' <spec>
    <source> ::= [a-z][a-z0-9_-]{0,31}          (registered-condition charset)
    <spec>   ::= <dotted.key> '=' <json_literal> (the existing Layer-1 grammar)

The split is on the FIRST ``:`` only. A well-formed clause's first ``:``
always precedes its first ``=``, so a JSON string value containing ``:``
parses correctly, and a forgotten source prefix (e.g. ``fields.x="a:b"``)
fails source-charset validation with the clause named verbatim rather than
mis-parsing silently.

A clause lowers (via :func:`clause_predicate`) to
``parse_match(["fields.source=<json source>", "<spec>"])`` -- byte-identical
to the spec shape the single-source wait path emits for ``--source``, so a
clause IS the existing per-source predicate scoped to its source, not a new
grammar. The lowered predicate carries the operator's VERBATIM clause text
as its ``source`` (not the lowered join), so timeout output names
outstanding clauses exactly as typed. One inherited corner: a spec whose key is itself ``fields.source``
OR-merges with the injected source predicate (``parse_match`` ORs within a
repeated key) instead of ANDing -- identical to the single-source path's
behaviour for ``--source X --match fields.source=...``.

:class:`AllOfTracker` carries the sticky-satisfaction state the conjunction
form needs: each clause may be satisfied by a DIFFERENT event over the
wait's lifetime, and a clause once satisfied stays satisfied. The wait
returns when the last outstanding clause matches. (A single-event
conjunction would be unsatisfiable across sources by construction -- one
frame carries exactly one ``fields.source``.)
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any, Final, NamedTuple

from ._predicate import Predicate, parse_match

__all__ = ["AllOfTracker", "Clause", "clause_predicate", "parse_clause"]

#: Source-name shape: the same charset rule registered-condition names use,
#: so a forgotten source prefix fails loudly instead of mis-parsing.
_SOURCE_NAME: Final = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")


class Clause(NamedTuple):
    """One parsed ``source:spec`` clause plus the verbatim text as typed.

    ``text`` is the operator's original clause string. It is carried
    through lowering (see :func:`clause_predicate`) so timeout output can
    name an outstanding clause exactly as the operator typed it, never the
    lowered ``fields.source=... key=lit`` predicate text.
    """

    source: str
    spec: str
    text: str


def parse_clause(text: str) -> Clause:
    """Split one ``source:spec`` clause on the FIRST ``:``.

    Returns a :class:`Clause` carrying the split halves and the verbatim
    clause text. Raises ``ValueError`` naming the clause verbatim when
    there is no ``:``, the source half does not match the source-name
    charset, or the spec half is empty. The spec half's own grammar
    (key=json_literal, the 256-byte per-spec cap) is enforced by
    ``parse_match`` inside :func:`clause_predicate`, not re-implemented here.
    """
    if ":" not in text:
        raise ValueError(f"clause must be source:key=json_literal, got {text!r}")
    source, _, spec = text.partition(":")
    if not _SOURCE_NAME.match(source):
        raise ValueError(f"clause source must match [a-z][a-z0-9_-]{{0,31}}, got {source!r} in clause {text!r}")
    if not spec:
        raise ValueError(f"clause expression must be non-empty, got {text!r}")
    return Clause(source=source, spec=spec, text=text)


def clause_predicate(clause: Clause) -> Predicate:
    """Lower one clause to the existing per-source predicate.

    Exactly the spec shape the single-source wait path emits for
    ``--source``: a ``fields.source=`` predicate AND-merged with the
    clause's own spec by ``parse_match``. The returned predicate's
    ``source`` text is the clause's VERBATIM ``source:spec`` form (not
    the lowered join), so timeout output names outstanding clauses as
    typed. Raises ``ValueError`` on a malformed spec (propagated from
    ``parse_match``).
    """
    lowered = parse_match([f"fields.source={json.dumps(clause.source)}", clause.spec])
    return Predicate(evaluate=lowered.evaluate, source=clause.text)


class AllOfTracker:
    """Sticky-satisfaction state over a clause list (the conjunction form).

    :meth:`update` evaluates each NOT-yet-satisfied clause against a frame
    and flips its flag on a match -- a clause once satisfied stays satisfied
    for the tracker's lifetime, and one frame may flip several clauses at
    once. It returns ``True`` iff every clause is now satisfied, i.e. on the
    frame that completes the conjunction.
    """

    __slots__ = ("_clauses", "_satisfied")

    def __init__(self, clauses: Sequence[Predicate]) -> None:
        self._clauses: tuple[Predicate, ...] = tuple(clauses)
        self._satisfied: list[bool] = [False] * len(self._clauses)

    def update(self, frame: dict[str, Any]) -> bool:
        """Fold one frame into the satisfaction state; True iff all satisfied."""
        for index, clause in enumerate(self._clauses):
            if not self._satisfied[index] and clause.evaluate(frame):
                self._satisfied[index] = True
        return all(self._satisfied)

    @property
    def outstanding(self) -> tuple[str, ...]:
        """Verbatim clause texts not yet satisfied (for timeout output)."""
        return tuple(clause.source for index, clause in enumerate(self._clauses) if not self._satisfied[index])
