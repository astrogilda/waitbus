"""Lint: the committed ``waitbus/__init__.pyi`` stub must stay in lockstep
with the runtime public surface (``__all__`` + the lazy ``_LAZY_EXPORTS`` map).

The package root re-exports a curated public API: eager predicate hooks plus
lazily-resolved producer/consumer symbols (``emit`` / ``subscribe`` / ...) whose
implementations live in PRIVATE modules (``_emit`` / ``_subscribe``). Type
checkers read the ``.pyi`` stub, the interpreter runs ``__getattr__`` over
``_LAZY_EXPORTS`` -- two hand-maintained surfaces. If they drift, downstream
``mypy --strict`` silently rots (a stub export with no runtime symbol, or a
runtime export the stub never declares). This test makes drift a hard failure.
"""

from __future__ import annotations

import ast
from pathlib import Path

import waitbus

_PKG = Path(waitbus.__file__).resolve().parent
_INIT_PY = _PKG / "__init__.py"
_INIT_PYI = _PKG / "__init__.pyi"


def _pyi_reexports_and_all() -> tuple[set[str], set[str]]:
    """Return (aliased re-export names, ``__all__`` set) declared in the stub."""
    tree = ast.parse(_INIT_PYI.read_text(encoding="utf-8"))
    reexports: set[str] = set()
    dunder_all: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                # PEP 484 re-export requires ``import X as X``; only those count.
                if alias.asname == alias.name:
                    reexports.add(alias.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    dunder_all = {
                        el.value
                        for el in ast.walk(node.value)
                        if isinstance(el, ast.Constant) and isinstance(el.value, str)
                    }
    return reexports, dunder_all


def _runtime_lazy_keys() -> set[str]:
    return set(waitbus._LAZY_EXPORTS)


def test_stub_exists() -> None:
    assert _INIT_PYI.is_file(), "committed waitbus/__init__.pyi is missing"
    assert (_PKG / "py.typed").is_file(), "PEP 561 py.typed marker is missing"


def test_stub_matches_runtime_all() -> None:
    reexports, pyi_all = _pyi_reexports_and_all()
    runtime_all = set(waitbus.__all__)
    assert pyi_all == runtime_all, (
        f"__init__.pyi __all__ != runtime __all__: "
        f"stub-only={pyi_all - runtime_all}, runtime-only={runtime_all - pyi_all}"
    )
    assert reexports == runtime_all, (
        f"__init__.pyi re-exports (import X as X) != runtime __all__: "
        f"stub-only={reexports - runtime_all}, runtime-only={runtime_all - reexports}"
    )


def test_every_public_name_resolves_at_runtime() -> None:
    # Eager + lazy: getattr must succeed for every advertised public name.
    for name in waitbus.__all__:
        assert getattr(waitbus, name) is not None


def test_lazy_keys_are_a_subset_of_public_surface() -> None:
    runtime_all = set(waitbus.__all__)
    lazy = _runtime_lazy_keys()
    assert lazy <= runtime_all, f"_LAZY_EXPORTS keys not in __all__: {lazy - runtime_all}"
    # Every lazy target must point at a private impl module (the convention).
    for name, target in waitbus._LAZY_EXPORTS.items():
        leaf = target.rsplit(".", 1)[-1]
        assert leaf.startswith("_") or "._" in target, (
            f"lazy export {name!r} -> {target!r} does not resolve to a private module"
        )
