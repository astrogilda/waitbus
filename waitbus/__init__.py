"""waitbus: report GitHub Actions CI status from locally-cached webhook events.

Package providing the listener, ETag-poll fallback, query CLI, watchdog
absence-detector, and the broadcast daemon. Each module is invocable as
`python -m waitbus.<module>` or via its installed console-script
(`waitbus-listener`, `waitbus-broadcast`, etc.). The systemd units
shipped under `share/systemd/user/` dispatch to the console-script paths.

Library-mode usage: `import waitbus.<x>` is import-side-effect-free.
The `ensure_state_dirs()` helper in `waitbus._paths` is invoked only
from entry-point `main()` functions, never at import.
"""

from __future__ import annotations

import logging
from typing import Any

# Re-export the predicate-engine plugin hooks so Layer-2 extras packages
# (e.g. waitbus-cel, waitbus-jmespath) can register from a single, stable
# import path that the rest of the predicate machinery uses. The full module
# is private (_predicate); only the hook surface is re-exported here.
# These are cheap to import eagerly.
from ._predicate import (
    EvaluatorUnavailableError,
    Predicate,
    register_condition,
    register_evaluator,
)

# PEP 282 best practice: library/package root logger gets a NullHandler
# so consumers who don't configure logging don't see leaked records.
# Entry-point `main()` functions (listener, broadcast, etc.) call
# `logging.basicConfig(...)` themselves to install a real handler.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# --- Public Python API: producer (emit) + consumer (subscribe/wait_for) ---
# `emit` / `register_source` (producer side) and `subscribe` / `wait_for` /
# `asubscribe` (consumer side) + the frozen wire `EventFrame` form one
# coherent, symmetric public surface. The subscriber engine and `emit()` form
# the public API contract. Their implementations live in PRIVATE modules (`_emit`,
# `_subscribe`) -- matching the `_predicate` / `_broadcast_sub` / `_frame`
# convention -- and the curated public symbols are re-exported HERE at the
# package root. The canonical public import is `from waitbus import emit`.
# Resolution is LAZY (PEP 562 `__getattr__`) so the bare `import waitbus`
# stays side-effect-free and cheap: it pulls only the light predicate hooks
# above, NOT msgspec or the broadcast/registry machinery, which load only when
# a consumer first touches `emit` / `subscribe`. Static type-checkers read the
# committed `__init__.pyi` stub (which mirrors this map), so the public function
# names do not collide with any submodule.
_LAZY_EXPORTS: dict[str, str] = {
    "subscribe": "waitbus._subscribe",
    "wait_for": "waitbus._subscribe",
    "asubscribe": "waitbus._subscribe",
    "EventFrame": "waitbus._subscribe",
    "emit": "waitbus._emit",
    "register_source": "waitbus.sources._registry",
    "request": "waitbus._messaging",
    "respond": "waitbus._messaging",
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute resolution for the public API surface."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(target), name)


def __dir__() -> list[str]:
    return [*globals(), *_LAZY_EXPORTS]


# Eager: predicate-engine plugin hooks. Lazy (via __getattr__): the producer
# API (emit, register_source) + consumer SDK (subscribe, wait_for, asubscribe,
# EventFrame). __all__ is sorted (RUF022); the grouping is documented above.
__all__ = (
    "EvaluatorUnavailableError",
    "EventFrame",
    "Predicate",
    "asubscribe",
    "emit",
    "register_condition",
    "register_evaluator",
    "register_source",
    "request",
    "respond",
    "subscribe",
    "wait_for",
)
