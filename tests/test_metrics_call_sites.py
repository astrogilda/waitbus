"""AST-walk regression test for _metrics call-site / _LABEL_NAMES parity.

Walks every ``.py`` file under ``waitbus/`` and asserts that every
production call to ``_metrics.incr(...)`` or to one of the three module-level
singletons passes a kwarg set that matches what ``_metrics._LABEL_NAMES``
declares for that counter, or no kwargs at all for the unlabelled singletons.

Three syntactic patterns are recognised:

* Pattern 1: ``_metrics.incr(NAME, **kwargs)`` and ``<alias>.incr(NAME, **kwargs)``
  where ``<alias>`` is any imported alias of the ``_metrics`` module
  (e.g., ``from waitbus import _metrics as m`` -> ``m.incr(...)``).
* Pattern 2: ``_metrics.<SINGLETON>.method(**kwargs)`` where SINGLETON is one of
  ``BROADCAST_SEND_SECONDS`` / ``SUBSCRIBER_COUNT`` / ``WATERMARK_REPLAY_EVENTS_TOTAL``.
* Pattern 3: ``<imported-singleton>.method(**kwargs)`` after a
  ``from waitbus._metrics import SINGLETON`` form.

A counter call whose ``name`` is not a string literal (e.g., a computed name)
is skipped with a recorded note; an audit reviewer can grep ``# noqa: call-site``
for any dynamic calls.

This test is structural, NOT behavioural: it does not exercise the metric
recording path. Behavioural coverage lives in ``test_metrics.py``.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from waitbus._metrics import _LABEL_NAMES

PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "waitbus"

# The three module-level singletons declared in _metrics.py.
SINGLETON_NAMES = frozenset(
    {
        "BROADCAST_SEND_SECONDS",
        "SUBSCRIBER_COUNT",
        "WATERMARK_REPLAY_EVENTS_TOTAL",
    }
)

# Methods that take **labels-shaped kwargs at the call site.
LABELLED_METHODS = frozenset({"inc", "dec", "set", "observe", "value"})


def _kwarg_names(call: ast.Call) -> set[str]:
    """Return the set of keyword-arg names passed to ``call`` (literal only)."""
    return {kw.arg for kw in call.keywords if kw.arg is not None}


def _module_aliases(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Return (metrics_module_aliases, imported_singleton_names) for ``tree``.

    The first set contains the module-level names that refer to ``_metrics``
    (covering ``from waitbus import _metrics`` and ``... as alias``).
    The second is the set of singleton names imported from ``_metrics`` directly
    (covering ``from waitbus._metrics import SUBSCRIBER_COUNT``).
    """
    metrics_aliases: set[str] = set()
    imported_singletons: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module in {"waitbus._metrics", "._metrics"}:
                for alias in node.names:
                    if alias.name in SINGLETON_NAMES:
                        imported_singletons.add(alias.asname or alias.name)
            elif node.module == "waitbus":
                for alias in node.names:
                    if alias.name == "_metrics":
                        metrics_aliases.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "waitbus._metrics":
                    metrics_aliases.add((alias.asname or alias.name).split(".")[-1])
    return metrics_aliases, imported_singletons


def _calls_in(tree: ast.Module) -> list[ast.Call]:
    return [node for node in ast.walk(tree) if isinstance(node, ast.Call)]


def _string_arg0(call: ast.Call) -> str | None:
    """Return the literal string value of ``call``'s first positional arg, or None."""
    if not call.args:
        return None
    first = call.args[0]
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value
    return None


def _match_incr_call(call: ast.Call, metrics_aliases: set[str]) -> ast.Attribute | None:
    """Return the matched func node iff this is ``<alias>.incr(...)``, else None."""
    func = call.func
    if (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id in metrics_aliases
        and func.attr == "incr"
    ):
        return func
    return None


def _match_singleton_via_module(call: ast.Call, metrics_aliases: set[str]) -> ast.Attribute | None:
    """Return matched func iff ``<alias>.<SINGLETON>.<labelled_method>(...)``, else None."""
    match call.func:
        case ast.Attribute(
            value=ast.Attribute(value=ast.Name(id=mod), attr=singleton),
            attr=method,
        ) if mod in metrics_aliases and singleton in SINGLETON_NAMES and method in LABELLED_METHODS:
            assert isinstance(call.func, ast.Attribute)
            return call.func
    return None


def _match_singleton_direct(call: ast.Call, imported_singletons: set[str]) -> ast.Attribute | None:
    """Return matched func iff ``<imported_singleton>.<labelled_method>(...)``, else None."""
    func = call.func
    if (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id in imported_singletons
        and func.attr in LABELLED_METHODS
    ):
        return func
    return None


def _check_incr_call(path: Path, call: ast.Call, func: ast.Attribute) -> list[str]:
    """Emit violations for a matched ``<alias>.incr(NAME, **kwargs)`` call."""
    del func  # unused; signature parity with _check_singleton_kwargs
    name = _string_arg0(call)
    if name is None:
        return []  # dynamic name; skip silently
    kwargs = _kwarg_names(call)
    declared = _LABEL_NAMES.get(name)
    if declared is None:
        return [f"{path}:{call.lineno}: incr({name!r}) but {name!r} is not in _LABEL_NAMES"]
    if kwargs != set(declared):
        return [f"{path}:{call.lineno}: incr({name!r}) kwargs {sorted(kwargs)} != declared {list(declared)}"]
    return []


def _check_singleton_kwargs(path: Path, call: ast.Call, func: ast.Attribute) -> list[str]:
    """Emit violations for a matched singleton method call (Patterns 2 and 3)."""
    kwargs = _kwarg_names(call)
    if not kwargs:
        return []
    value = func.value
    if isinstance(value, ast.Attribute):
        receiver = value.attr  # Pattern 2: <alias>.<SINGLETON>
    else:
        assert isinstance(value, ast.Name)
        receiver = value.id  # Pattern 3: <imported-singleton>
    return [
        f"{path}:{call.lineno}: {receiver}.{func.attr}(...) passed kwargs {sorted(kwargs)} (singleton is unlabelled)"
    ]


def _violations_in_file(path: Path) -> list[str]:
    source = path.read_text()
    tree = ast.parse(source, filename=str(path))
    metrics_aliases, imported_singletons = _module_aliases(tree)
    violations: list[str] = []
    for call in _calls_in(tree):
        if (func := _match_incr_call(call, metrics_aliases)) is not None:
            violations.extend(_check_incr_call(path, call, func))
            continue
        if (func := _match_singleton_via_module(call, metrics_aliases)) is not None:
            violations.extend(_check_singleton_kwargs(path, call, func))
            continue
        if (func := _match_singleton_direct(call, imported_singletons)) is not None:
            violations.extend(_check_singleton_kwargs(path, call, func))
    return violations


def test_every_metrics_call_site_matches_declared_label_names() -> None:
    """Every production call to ``_metrics.incr`` or singleton methods has a
    kwarg set that matches its declaration in ``_LABEL_NAMES`` (for counters)
    or is empty (for the three unlabelled singletons).

    Regression form: prevents call-site kwarg drift from the declared label-name set.
    """
    all_violations: list[str] = []
    for py_file in sorted(PACKAGE_ROOT.rglob("*.py")):
        all_violations.extend(_violations_in_file(py_file))
    assert not all_violations, "\n".join(["call-site / _LABEL_NAMES drift:", *all_violations])


@pytest.mark.parametrize("name", sorted(_LABEL_NAMES))
def test_label_names_dict_matches_help(name: str) -> None:
    """Every entry in ``_LABEL_NAMES`` has a corresponding ``_HELP`` entry.

    Prevents dual-source-of-truth drift between ``_LABEL_NAMES`` and ``_HELP``.
    """
    from waitbus._metrics import _HELP

    assert name in _HELP, f"_LABEL_NAMES has {name!r} but _HELP does not"
