"""``scripts.soak`` - 24-hour mixed-source soak orchestrator package.

Submodules (acyclic DAG; ``_context`` is stdlib + msgspec only, each
deeper module imports from siblings above it):

- ``_context``  - frozen ``_SoakContext`` + mutable ``_SoakAccumulators`` +
  scalar ``_SoakState`` + ``SuspendCycle`` + sample-interval cadences.
- ``_emit``     - synthetic + corpus-replay event emit helpers.
- ``_verdict``  - verdict computation, integrity check, sample writers.
- ``_suspend``  - SIGSTOP/SIGCONT suspend cycles + isolated-env context manager.
- ``_main``     - orchestrator entry-point, subscriber thread, main loop,
  arg parsing, and ``main()``.

Run via ``python -m scripts.soak [...]`` (see ``__main__.py``).

Layout split from a 1351-LOC ``scripts/soak.py``: the package stays in
``scripts/`` and is NOT promoted to ``waitbus.soak`` or ``waitbus
soak``.  Consumers import from specific submodules; no top-level
re-exports per the no-aliases policy.
"""
