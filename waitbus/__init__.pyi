# Type stub for the waitbus package root. Single source of truth for the public
# API that static type-checkers see (mypy consults this stub instead of the
# runtime ``__init__.py`` ``__getattr__``). It mirrors ``_LAZY_EXPORTS`` + the
# eager predicate hooks; the drift-guard test ``tests/test_init_pyi_drift.py``
# pins the two in lockstep. Public functions live in PRIVATE impl modules
# (``_emit`` / ``_subscribe``) re-exported here, so no public name collides with
# a submodule. ``as`` aliasing is required for re-export under
# ``--no-implicit-reexport`` (PEP 484).
from ._emit import emit as emit
from ._messaging import request as request
from ._messaging import respond as respond
from ._predicate import EvaluatorUnavailableError as EvaluatorUnavailableError
from ._predicate import Predicate as Predicate
from ._predicate import register_condition as register_condition
from ._predicate import register_evaluator as register_evaluator
from ._subscribe import EventFrame as EventFrame
from ._subscribe import asubscribe as asubscribe
from ._subscribe import subscribe as subscribe
from ._subscribe import wait_for as wait_for
from .sources._registry import register_source as register_source

# Package version (PEP 396); not part of the curated ``__all__`` public surface.
__version__: str

# Runtime-only internals the test suite / drift-guard reference (not public API).
_LAZY_EXPORTS: dict[str, str]

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
