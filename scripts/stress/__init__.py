"""``scripts.stress`` -- cross-harness fan-out stress / break harness package.

Submodules form an acyclic DAG; ``_context`` is stdlib + msgspec only,
each deeper module imports from siblings above it:

- ``_context``     -- frozen ``_StressContext`` + mutable ``_StressAccumulators``
  + scalar ``_StressState`` + ``StressSignalFailure`` + ``_VerdictDoc``.
- ``_verdict``     -- ``_write_verdict`` (atomic tmp+rename), ``_append_progress``
  (long-lived FD + flush per record), verdict-aggregation helpers.
- ``_sources``, ``_ledger``, ``_scrape``, ``_usl``, ``_controller``
  -- the emitter pool, durability ledger, metrics scrape, USL fit, and
  orchestrator that build on the two above.

The user-facing ``waitbus stress`` subcommand
(``waitbus.cli.stress``) shares the controller and verdict shape.

The package lives in ``scripts/`` because the controller spawns and
supervises subprocesses (a harness concern, not a library one) and
shares primitives directly with ``scripts.soak`` and
``benchmarks._harness`` -- ``HdrRecorder``, ``OpenLoopScheduler``,
``wilson_rank_ci``, ``capture_t0`` / ``consume_t0``, ``daemon_context``,
``spawn_waitbus_daemon``, ``_emit_one``, ``_isolated_waitbus_dirs``,
``FaultInjectionOutcome``, the soak fault-injection probes -- by
direct import. Nothing here duplicates a soak or benchmarks primitive.
"""

from __future__ import annotations
