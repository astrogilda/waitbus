"""Shared terminal-state classifier and per-source entity key.

This module is the single neutral home for two seams that several
consumers (the blocking ``waitbus wait`` verb, the new coalesced replay
delivery mode, and the future MCP Tasks adapter) all need:

* :data:`SUCCESS_CONCLUSION` / :data:`FAILURE_CONCLUSIONS` /
  :data:`NON_TERMINAL_CONCLUSIONS` -- the canonical GitHub-conclusion
  buckets that previously lived in :mod:`waitbus.pr_monitor`.
  ``pr_monitor.AGG_SQL`` keeps its byte-identical text because it
  interpolates the *values* of the frozensets; only the import site
  changes (greenfield: every consumer imports from this module
  directly, with no re-export alias in ``pr_monitor``).

* :func:`entity_key` / :func:`is_terminal` -- a per-source classifier
  that maps a broadcast wire frame (the shape
  :func:`waitbus.broadcast._row_to_frame` produces) to (i) the
  stable per-entity key the coalesced delivery mode collapses on and
  (ii) the terminal-state predicate that decides whether a snapshot
  entry is final. Sources without a stable upstream entity key or
  terminal-state semantics (the local watchers: ``pytest`` / ``docker`` /
  ``fs``; the Alertmanager watchdog liveness signal) return ``None``
  from :func:`entity_key` and are delivered verbatim by the coalesced
  consumer (pass-through, never collapsed).

The contract is intentionally narrow: this module does **not** raise,
emit CLI errors, or know about ``typer`` -- it is engine-side and is
imported by both the MIT-core surface (``coalesce``, ``wait``,
``pr_monitor``) and the test suite.
"""

from __future__ import annotations

from typing import Any

SUCCESS_CONCLUSION: str = "success"
#: GitHub ``conclusion`` values that terminate a wait with a non-zero
#: exit (the run reached a terminal failure state).
FAILURE_CONCLUSIONS: frozenset[str] = frozenset({"failure", "cancelled", "timed_out"})
#: GitHub conclusions that do NOT terminate a wait. ``skipped`` /
#: ``neutral`` are benign no-run outcomes; ``action_required`` is a
#: human gate; ``stale`` is a re-run-pending state. Consumers keep
#: waiting (until timeout) rather than declaring a premature verdict.
NON_TERMINAL_CONCLUSIONS: frozenset[str] = frozenset({"skipped", "neutral", "action_required", "stale"})

#: A stable per-entity key the coalesced delivery mode collapses on.
#: ``None`` -- intentionally so for sources without a stable upstream
#: identity -- means "deliver verbatim; never collapse".
EntityKey = tuple[str, ...]


def entity_key(frame: dict[str, Any]) -> EntityKey | None:
    """Return a stable per-source collapse key, or ``None`` to pass through.

    ``frame`` is a broadcast wire frame
    (:func:`waitbus.broadcast._row_to_frame` shape): the event
    columns live under ``frame["fields"]``; the source identifier is
    ``frame["fields"]["source"]``.

    Per-source mapping:

    * ``github`` + ``workflow_run`` (with ``run_id``) →
      ``("github", "run", run_id)``
    * ``github`` + ``workflow_job`` (with ``job_id``) →
      ``("github", "job", job_id)``
    * ``alertmanager`` + ``prometheus_alert`` (with
      ``alert_fingerprint``) → ``("alertmanager", "alert", fingerprint)``
    * everything else (``prometheus_watchdog`` liveness, the local
      watcher sources ``pytest`` / ``docker`` / ``fs``, and any
      GitHub/Alertmanager row missing its identity column) → ``None``
      (pass-through).
    """
    fields = frame.get("fields")
    if not isinstance(fields, dict):
        return None
    source = fields.get("source")
    event_type = frame.get("event_type")

    if source == "github":
        # Narrow to positive ints only: GitHub's API contract guarantees
        # workflow run / job ids are positive int64. type(v) is int (not
        # isinstance) excludes bool, which is a subclass of int and would
        # otherwise pass through as the entity-key string "True"/"False".
        # Defence-in-depth pair with listener._event_from_webhook_payload
        # ingress narrowing; any non-conforming value short-circuits to
        # pass-through.
        if event_type == "workflow_job":
            job_id = fields.get("job_id")
            if type(job_id) is int and job_id > 0:
                return ("github", "job", str(job_id))
        elif event_type == "workflow_run":
            run_id = fields.get("run_id")
            if type(run_id) is int and run_id > 0:
                return ("github", "run", str(run_id))
        return None
    if source == "alertmanager":
        fingerprint = fields.get("alert_fingerprint")
        if isinstance(fingerprint, str) and fingerprint:
            return ("alertmanager", "alert", fingerprint)
        return None
    return None


def is_terminal(frame: dict[str, Any]) -> bool:
    """Return True iff the frame represents a terminal state for its entity.

    Drives "the coalesced snapshot entry for this entity is final"
    decisions; a non-terminal frame on a known entity is still kept (as
    the latest-version-seen), it simply does not declare the entity
    done.

    Per-source semantics:

    * ``github``: ``conclusion`` is the terminal axis. ``SUCCESS_CONCLUSION``
      or any value in :data:`FAILURE_CONCLUSIONS` is terminal; everything
      else (``None``, ``in_progress``, ``queued``, anything in
      :data:`NON_TERMINAL_CONCLUSIONS`) is not.
    * ``alertmanager`` + ``prometheus_alert``: ``status == "resolved"``
      is terminal. ``prometheus_watchdog`` (the liveness heartbeat) is
      never terminal.
    * Other sources: not terminal (``False``) -- they have no terminal-
      state semantics today and the coalesced consumer pass-through
      handles them via the ``entity_key(...) is None`` path.
    """
    fields = frame.get("fields")
    if not isinstance(fields, dict):
        return False
    source = fields.get("source")
    if source == "github":
        conclusion = fields.get("conclusion")
        return bool(conclusion) and (conclusion == SUCCESS_CONCLUSION or conclusion in FAILURE_CONCLUSIONS)
    if source == "alertmanager":
        return frame.get("event_type") == "prometheus_alert" and fields.get("status") == "resolved"
    return False
