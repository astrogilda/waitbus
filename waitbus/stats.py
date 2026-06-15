"""Per-source counterfactual ROI model for the local event store.

Powers ``waitbus stats``: an operator gets a tripartite report —
**MEASURED** facts (per-source event counts, read straight off the
durable events table), **ESTIMATED** per-source polling costs (every
assumption inline; configurable per source via env vars), and
**COMPUTED** per-source modelled savings (the deterministic product
of the two halves).

Design posture, in order of importance:

1. **Per-source measurement, never a single hero number.** The four
   sources waitbus ingests (github, pytest, docker, fs) have very
   different polling-response shapes and very different per-poll
   token costs. A single ``--per-poll-tokens`` value cannot honestly
   model all four. The CLI carries one cost per source.

2. **Three banners, never mixed.** MEASURED is what the events table
   says. ESTIMATED is the per-source cost assumptions printed inline.
   COMPUTED is the deterministic product (events x cost). A reader can
   tell which line is which without parsing prose.

3. **No fabricated headline.** There is no bare ``you saved $X`` line.
   Per-source modelled savings only appear beneath the per-source
   cost that produced them. The aggregate sum prints too, but only
   after every per-source line, never as a standalone summary.

4. **What is *not* a single counter.** "Events delivered"
   and "subscription uptime" are not first-class counters; the
   caveats block documents this verbatim so the operator is never
   shown a zero that looks measured.

5. **Read-only, no schema change.** The events DB is opened
   ``readonly=True``; the report adds no column and no metric.

The per-source costs are derived empirically by
``scripts/derive_poll_costs.py`` (tiktoken cl100k_base against
representative synthetic polling-response payloads) and committed to
``benchmarks/poll_cost_derivation.json``. The defaults here are the
script's output for a typical-session weighted average; operators
override per source via ``$WAITBUS_POLL_COST_{GITHUB,PYTEST,DOCKER,FS}``.
The defaults are average-per-poll over a typical session (mostly
small "not done yet" polls + one terminal payload), NOT the
terminal-payload cost — a workload with many failing tests carrying
large tracebacks should set ``$WAITBUS_POLL_COST_PYTEST`` higher.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from . import _db, _paths
from ._types import NS_PER_SECOND

# Default assumed agent poll cadence, in seconds. A coding agent that
# polls a CI status endpoint (``gh run watch``-style, or a bespoke
# ``sleep N; check`` loop) typically does so on the order of every 15 s.
# Printed inline with every estimate; operator-overridable via
# ``--poll-interval``.
DEFAULT_POLL_INTERVAL_SECONDS: Final[float] = 15.0

# Per-source default token cost of one avoided poll. Each value is the
# weighted average of small-poll cost (``status=in_progress``-shaped
# responses) and terminal-poll cost (final payload) across a typical
# session. Empirically derived by ``scripts/derive_poll_costs.py`` and
# committed to ``benchmarks/poll_cost_derivation.json``; verified in CI
# by that script's ``--against`` mode pointing at this module. Override
# per source via ``$WAITBUS_POLL_COST_{GITHUB,PYTEST,DOCKER,FS}``.
#
# github: ``gh run watch`` polls a workflow_run JSON body (5 fields:
# status, conclusion, run_number, name, updated_at = ~45 tokens
# small-poll). Terminal payload is a full workflow_run object
# (~977 tokens). Weighted over 300 small polls + 1 terminal during a
# typical 15-min CI run lands at 48 tokens/poll.
DEFAULT_POLL_COST_GITHUB: Final[int] = 48
# pytest: tail/parse ``report.xml``; small ~10 tokens (no new content),
# terminal ~419 tokens (a typical XML report). 20 small polls + 1
# terminal during a 100-second test run weights to 29. Workloads with
# many failing tests + large tracebacks should override upward.
DEFAULT_POLL_COST_PYTEST: Final[int] = 29
# docker: ``docker ps -a --filter id=<id>`` (header + one data row
# tokenises to ~53 small-poll, ~58 terminal). Tight distribution
# because the container-state shape is small either way; small poll
# dominates the average.
DEFAULT_POLL_COST_DOCKER: Final[int] = 53
# fs: ``os.stat`` formatted as ``st_mtime=... st_size=... st_ino=...``
# (the key=value form an agent uses to detect change) tokenises to ~21
# small-poll, ~25 terminal. Small poll dominates.
DEFAULT_POLL_COST_FS: Final[int] = 21

# Stop condition the counterfactual assumes for the polling agent. Held
# as a constant so the wording the report prints stays in one place.
_STOP_CONDITION: Final[str] = (
    "the polling agent stops once it observes the terminal event it is "
    "waiting on (one poll per observed event, not continuous polling)"
)

# Canonical source ordering for the report. Pinned to the four
# token-cost-derived sources (github / pytest / docker / fs) because each
# one has a measured, empirically derived
# ``$WAITBUS_POLL_COST_<SOURCE>`` baseline. Other sources (alertmanager,
# operator-registered plugin sources) appear in ``MeasuredFacts.by_source``
# but are excluded from the estimate block until a derivation lands
# per source.
_REPORT_SOURCES: Final[tuple[str, ...]] = (
    "github",
    "pytest",
    "docker",
    "fs",
)


@dataclass(frozen=True)
class StatsRequest:
    """Resolved parameters for one ``waitbus stats`` invocation.

    Built by the CLI layer from typer args + env-var lookup; consumed
    by ``run_stats``. Frozen so the request is safe to pass around
    without worrying about post-resolution mutation.

    Attributes:
        db_path: Path to the events SQLite DB (read-only).
        poll_interval_seconds: Assumed agent poll cadence. Printed
            inline with every estimate.
        per_source_token_costs: Per-source cost-per-poll assumptions.
            Keys are the four canonical source names with measured
            poll-cost derivations (``github``, ``pytest``, ``docker``,
            ``fs``). Each entry is the stated cost-per-poll for that
            source. The CLI layer reads ``$WAITBUS_POLL_COST_<SOURCE>``
            for each, with the per-source defaults as fallback.
        as_json: When True emit the report as a single JSON object
            with ``measured`` / ``estimated`` / ``computed``
            sub-objects; when False emit the banner/key:value text
            form.
    """

    db_path: Path
    poll_interval_seconds: float
    per_source_token_costs: dict[str, int]
    as_json: bool


@dataclass(frozen=True)
class MeasuredFacts:
    """Facts read directly off the durable events table (read-only).

    Every field here is observed, never modelled. ``span_seconds`` is
    ``None`` when there are fewer than two events (no span to measure).
    """

    total_events: int
    by_source: dict[str, int]
    by_event_type: dict[str, int]
    earliest_received_at_ns: int | None
    latest_received_at_ns: int | None
    span_seconds: float | None


@dataclass(frozen=True)
class PerSourceEstimate:
    """The modelled savings for one source — assumptions are first-class.

    The CLI renders each ``PerSourceEstimate`` as its own block so the
    per-source cost-per-poll always appears next to the per-source
    modelled savings. A source with zero observed events still gets a
    row (with ``modelled_savings_tokens = 0``) so the operator can see
    every source the model knows about.
    """

    source: str
    events_observed: int
    per_poll_tokens: int
    polls_avoided: int
    modelled_savings_tokens: int


@dataclass(frozen=True)
class Estimate:
    """The full estimate block: per-source rows + aggregate sum.

    The aggregate is the deterministic sum of the per-source modelled
    savings; it appears AFTER every per-source line in the report so
    the per-source detail always reads first.
    """

    poll_interval_seconds: float
    per_source: tuple[PerSourceEstimate, ...]
    stop_condition: str
    aggregate_polls_avoided: int
    aggregate_modelled_savings_tokens: int


def _measure(conn: sqlite3.Connection) -> MeasuredFacts:
    """Read the measured facts off an already-open read-only connection.

    One pass each for the scalar aggregates and the two group-bys. The
    connection is opened ``readonly=True`` by the caller, so none of
    these can mutate the store even though they are plain SELECTs.
    """
    total: int = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    by_source: dict[str, int] = {
        str(src): int(n)
        for src, n in conn.execute("SELECT source, COUNT(*) FROM events GROUP BY source ORDER BY COUNT(*) DESC, source")
    }
    by_event_type: dict[str, int] = {
        str(et): int(n)
        for et, n in conn.execute(
            "SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY COUNT(*) DESC, event_type"
        )
    }

    # A GROUP-BY-less aggregate always returns exactly one row -- on an
    # empty table that row is (None, None) -- so .fetchone() is never
    # None here.
    span_row = conn.execute("SELECT MIN(received_at), MAX(received_at) FROM events").fetchone()
    earliest: int | None = span_row[0]
    latest: int | None = span_row[1]

    span_seconds: float | None = None
    if earliest is not None and latest is not None and latest > earliest:
        span_seconds = (latest - earliest) / NS_PER_SECOND

    return MeasuredFacts(
        total_events=total,
        by_source=by_source,
        by_event_type=by_event_type,
        earliest_received_at_ns=earliest,
        latest_received_at_ns=latest,
        span_seconds=span_seconds,
    )


def _estimate(facts: MeasuredFacts, req: StatsRequest) -> Estimate:
    """Build the per-source labelled counterfactual.

    For each known source: ``polls_avoided = events_observed`` and
    ``modelled_savings_tokens = polls_avoided x per_poll_tokens`` for
    that source. Sources outside the canonical built-in set
    (custom-registered sources from the ``waitbus.sources.v1`` entry-point
    registry) are surfaced in ``MeasuredFacts.by_source`` but excluded
    from the estimate block because they have no declared per-poll cost
    in this request.
    """
    per_source: list[PerSourceEstimate] = []
    aggregate_polls = 0
    aggregate_tokens = 0
    for source in _REPORT_SOURCES:
        events_observed = facts.by_source.get(source, 0)
        per_poll_tokens = req.per_source_token_costs[source]
        polls_avoided = events_observed
        savings = polls_avoided * per_poll_tokens
        per_source.append(
            PerSourceEstimate(
                source=source,
                events_observed=events_observed,
                per_poll_tokens=per_poll_tokens,
                polls_avoided=polls_avoided,
                modelled_savings_tokens=savings,
            )
        )
        aggregate_polls += polls_avoided
        aggregate_tokens += savings

    return Estimate(
        poll_interval_seconds=req.poll_interval_seconds,
        per_source=tuple(per_source),
        stop_condition=_STOP_CONDITION,
        aggregate_polls_avoided=aggregate_polls,
        aggregate_modelled_savings_tokens=aggregate_tokens,
    )


# The standing caveat block. "Delivered" and "uptime" are not single
# counters, and the daemon-side Prometheus counters are unreachable
# from this CLI process. Stated verbatim so the operator is never shown
# a zero that looks measured.
_COUNTER_CAVEATS: Final[tuple[str, ...]] = (
    "events_delivered is NOT a single counter: daemon-side it is proxied "
    "by waitbus_watermark_replay_events_total plus the waitbus_broadcast_send_"
    "seconds histogram.",
    "subscription_uptime is NOT a counter: it is a Grafana-side rate "
    "derivation off the waitbus_subscriber_count / waitbus_broadcast_*_count "
    "gauges.",
    "waitbus_db_inserted_total's DB-side equivalent is measured.total_events "
    "(COUNT(*)); the live counter itself lives in the daemon process.",
    "waitbus_db_dedup_ignored_total is NOT recoverable from the events table "
    "(a deduped insert lands no row); read it from the listener's /metrics "
    "scrape, not here.",
    "Live Prometheus counters are in-process to the daemons; scrape the "
    "listener's /metrics endpoint for their current values.",
)


def _emit_text(facts: MeasuredFacts, est: Estimate) -> None:
    """Render the report in three-banner key:value text form.

    Order is MEASURED → ESTIMATED → COMPUTED. Per-source rows appear
    under each banner so a reader scanning the output sees the
    measurement, the assumption, and the deterministic product side
    by side per source. The aggregate sum prints last and only after
    every per-source line.
    """
    print("=== MEASURED (observed from the events table; read-only) ===")
    print(f"total_events: {facts.total_events}")

    print("by_source:")
    for src, n in facts.by_source.items():
        print(f"  {src}: {n}")

    print("by_event_type:")
    for et, n in facts.by_event_type.items():
        print(f"  {et}: {n}")

    if facts.earliest_received_at_ns is None:
        print("time_span: no_events")
    else:
        print(f"earliest_received_at_ns: {facts.earliest_received_at_ns}")
        print(f"latest_received_at_ns: {facts.latest_received_at_ns}")
        if facts.span_seconds is None:
            print("span_seconds: single_event (no span)")
        else:
            print(f"span_seconds: {facts.span_seconds:.3f}")

    print("caveats:")
    for caveat in _COUNTER_CAVEATS:
        print(f"  - {caveat}")

    print()
    print("=== ESTIMATED (per-source polling costs; configurable) ===")
    print(f"assumed_poll_interval_seconds: {est.poll_interval_seconds}")
    print("per-source poll-cost assumptions (override via $WAITBUS_POLL_COST_<SOURCE>):")
    for row in est.per_source:
        print(f"  {row.source}: per_poll_tokens={row.per_poll_tokens}")
    print(f"stop_condition: {est.stop_condition}")
    print("defaults derived by scripts/derive_poll_costs.py; the listed values")
    print("are weighted averages of small-poll and terminal-poll costs over a")
    print("typical session -- NOT the terminal-payload cost. Workloads with")
    print("heavy traceback or large terminal payloads should override.")

    print()
    print("=== COMPUTED (per-source events_observed x per_poll_tokens) ===")
    print("per-source modelled_savings_tokens:")
    for row in est.per_source:
        print(
            f"  {row.source}: "
            f"events_observed={row.events_observed} "
            f"x per_poll_tokens={row.per_poll_tokens} "
            f"= modelled_savings_tokens={row.modelled_savings_tokens}"
        )
    print("aggregate (deterministic sum of per-source):")
    print(f"  aggregate_polls_avoided: {est.aggregate_polls_avoided}")
    print(f"  aggregate_modelled_savings_tokens: {est.aggregate_modelled_savings_tokens}")


def _emit_json(facts: MeasuredFacts, est: Estimate) -> None:
    """Render the report as a single JSON object.

    ``measured`` / ``estimated`` / ``computed`` are distinct top-level
    keys so the boundary is explicit in machine-consumed output. Each
    per-source row is a sibling of its assumption (per_poll_tokens)
    and its derived value (modelled_savings_tokens), keeping the
    structure parallel to the text output.
    """
    per_source_estimated = [{"source": r.source, "per_poll_tokens": r.per_poll_tokens} for r in est.per_source]
    per_source_computed = [
        {
            "source": r.source,
            "events_observed": r.events_observed,
            "per_poll_tokens": r.per_poll_tokens,
            "polls_avoided": r.polls_avoided,
            "modelled_savings_tokens": r.modelled_savings_tokens,
        }
        for r in est.per_source
    ]
    payload = {
        "measured": {
            "total_events": facts.total_events,
            "by_source": facts.by_source,
            "by_event_type": facts.by_event_type,
            "earliest_received_at_ns": facts.earliest_received_at_ns,
            "latest_received_at_ns": facts.latest_received_at_ns,
            "span_seconds": facts.span_seconds,
            "caveats": list(_COUNTER_CAVEATS),
        },
        "estimated": {
            "_note": (
                "per-source poll-cost assumptions, configurable via "
                "$WAITBUS_POLL_COST_<SOURCE>; defaults are weighted-average "
                "tokens-per-poll over a typical session, derived by "
                "scripts/derive_poll_costs.py"
            ),
            "assumed_poll_interval_seconds": est.poll_interval_seconds,
            "per_source": per_source_estimated,
            "stop_condition": est.stop_condition,
        },
        "computed": {
            "_note": (
                "deterministic product of measured.by_source x "
                "estimated.per_source[].per_poll_tokens; "
                "the aggregate is the exact sum of the per-source rows"
            ),
            "model": (
                "for each source s: polls_avoided[s] = events_observed[s]; "
                "modelled_savings_tokens[s] = polls_avoided[s] * "
                "per_poll_tokens[s]; aggregate = sum over s"
            ),
            "per_source": per_source_computed,
            "aggregate_polls_avoided": est.aggregate_polls_avoided,
            "aggregate_modelled_savings_tokens": est.aggregate_modelled_savings_tokens,
        },
    }
    print(json.dumps(payload, indent=2, default=str))


def run_stats(req: StatsRequest) -> int:
    """Execute the request and emit the report; return the exit code.

    Returns 0 on success (including an empty store), 2 when the events
    DB file is absent (with a ``waitbus init`` remediation hint) or a
    SQLite error surfaces. The connection is opened read-only inside
    the ``_db.connect`` context manager so a failure never leaks it
    and no write can land regardless of what runs.
    """
    if not req.db_path.exists():
        print(
            f"waitbus stats: events DB not found at {req.db_path}. Run `waitbus init` first.",
            file=sys.stderr,
        )
        return 2

    try:
        with _db.connect(req.db_path, readonly=True) as conn:
            facts = _measure(conn)
    except (sqlite3.DatabaseError, sqlite3.Warning) as exc:
        # ``sqlite3.DatabaseError`` is the base class for ``OperationalError``
        # (locked / corrupt / schema-mismatch), ``DataError``,
        # ``IntegrityError``, etc. Catching the base produces a friendly
        # "sqlite error" exit for every DB-shape failure rather than
        # leaking a Python traceback to the operator. A non-SQLite file
        # at ``db_path`` raises bare ``DatabaseError`` ("file is not a
        # database") from the WAL-PRAGMA in ``_db.open_conn``; the
        # narrower ``OperationalError`` catch missed that case.
        print(f"waitbus stats: sqlite error: {exc}", file=sys.stderr)
        return 2

    est = _estimate(facts, req)

    if req.as_json:
        _emit_json(facts, est)
    else:
        _emit_text(facts, est)
    return 0


def cli_entry(
    *,
    poll_interval_seconds: float,
    per_source_token_costs: dict[str, int],
    as_json: bool,
    db_path: Path | None,
) -> int:
    """Thin adapter from the typer command to ``run_stats``.

    Lives here (rather than in the CLI module) so the read-only
    connection lifecycle and the counterfactual model stay in one
    place and can be tested without spinning up the full typer app.
    The ``per_source_token_costs`` dict must carry an entry for every
    member of ``_REPORT_SOURCES``; the CLI layer guarantees this by
    reading env vars with the defaults from this module as fallback.
    """
    effective_db = _paths.resolve_db_path(db_path)
    missing = [s for s in _REPORT_SOURCES if s not in per_source_token_costs]
    if missing:
        msg = (
            f"waitbus stats: per_source_token_costs missing required keys: {missing}. "
            "The CLI layer is responsible for resolving every source's cost "
            "(via $WAITBUS_POLL_COST_<SOURCE> or the per-source default)."
        )
        raise ValueError(msg)
    req = StatsRequest(
        db_path=effective_db,
        poll_interval_seconds=poll_interval_seconds,
        per_source_token_costs=per_source_token_costs,
        as_json=as_json,
    )
    return run_stats(req)
