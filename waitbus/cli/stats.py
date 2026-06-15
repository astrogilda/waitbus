"""`stats` top-level command — per-source counterfactual ROI report.

Thin typer wrapper over ``waitbus.stats``. The model, the
read-only connection lifecycle, and the printed-assumptions discipline
all live in that module; this file only wires typer args + the four
``$WAITBUS_POLL_COST_<SOURCE>`` env vars onto ``stats.cli_entry`` and
maps its return value onto the process exit code.

The per-source token-cost surface is FOUR env vars (one per source),
each with a sensible default from the model module:

- ``$WAITBUS_POLL_COST_GITHUB``  default: ``DEFAULT_POLL_COST_GITHUB``  (48)
- ``$WAITBUS_POLL_COST_PYTEST``  default: ``DEFAULT_POLL_COST_PYTEST``  (29)
- ``$WAITBUS_POLL_COST_DOCKER``  default: ``DEFAULT_POLL_COST_DOCKER``  (53)
- ``$WAITBUS_POLL_COST_FS``      default: ``DEFAULT_POLL_COST_FS``      (21)

These defaults are derived empirically by ``scripts/derive_poll_costs.py``
(tiktoken cl100k_base against representative synthetic polling-response
payloads, weighted across a typical session) and committed to
``benchmarks/poll_cost_derivation.json``. The script's ``--against``
mode points at ``waitbus/stats.py`` (where the constants live)
and exits non-zero if the committed JSON drifts from the module
constants.

Why env vars not CLI flags: an operator's per-source assumptions are
deployment-stable (not invocation-stable), so they belong in the
shell rc / systemd unit's ``Environment=`` directives, not in every
``waitbus stats`` invocation. The env-var-with-default pattern matches
how waitbus already exposes ``WAITBUS_HEARTBEAT_SEC``,
``WAITBUS_STATE_DIR``, etc.
"""

from __future__ import annotations

import os
from pathlib import Path

import typer


def _resolve_per_source_costs() -> dict[str, int]:
    """Resolve the four per-source token costs from env, falling back to defaults.

    Returns a dict with exactly the four keys ``stats.cli_entry`` requires
    (``github`` / ``pytest`` / ``docker`` / ``fs``). A malformed env var
    (non-int or negative) raises ``typer.BadParameter`` with a
    remediation hint naming the offending variable; silent fallback to
    default would let a typo'd ``$WAITBUS_POLL_COST_PYTEST=fifty`` produce
    a misleading report with no signal to the operator.
    """
    # Imported inline so import-time side effects of the stats module
    # are deferred until the user actually invokes ``waitbus stats``.
    import waitbus.stats as mod

    source_default_pairs = (
        ("github", mod.DEFAULT_POLL_COST_GITHUB),
        ("pytest", mod.DEFAULT_POLL_COST_PYTEST),
        ("docker", mod.DEFAULT_POLL_COST_DOCKER),
        ("fs", mod.DEFAULT_POLL_COST_FS),
    )
    resolved: dict[str, int] = {}
    for source, default in source_default_pairs:
        env_name = f"WAITBUS_POLL_COST_{source.upper()}"
        raw = os.environ.get(env_name)
        if raw is None:
            resolved[source] = default
            continue
        try:
            value = int(raw)
        except ValueError as exc:
            raise typer.BadParameter(
                f"${env_name}={raw!r} is not an integer; set it to a "
                "non-negative number of tokens per poll or unset to use "
                f"the default ({default}).",
            ) from exc
        if value < 0:
            raise typer.BadParameter(
                f"${env_name}={raw!r} is negative; per-poll token cost must be >= 0.",
            )
        resolved[source] = value
    return resolved


def stats(
    poll_interval: float = typer.Option(
        15.0,
        "--poll-interval",
        help="ASSUMED agent poll cadence (seconds) used by the "
        "counterfactual. Printed verbatim next to the estimate; "
        "substitute your own to re-base the model. Default 15.",
    ),
    as_json: bool = typer.Option(
        False,
        "--json/--text",
        help="Output format. --text (default) prints separated "
        "MEASURED / ESTIMATED / COMPUTED banners; --json prints one "
        "object with distinct `measured` / `estimated` / `computed` "
        "keys.",
    ),
    db: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None,
        "--db",
        help="Path to the events SQLite DB. Defaults to the "
        "platformdirs-resolved location (typically "
        "~/.local/state/waitbus/github.db on Linux).",
        exists=False,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
) -> None:
    """Report MEASURED event-store facts + ESTIMATED + COMPUTED per-source savings.

    Three banners (MEASURED / ESTIMATED / COMPUTED) print in order.
    Per-source poll-cost assumptions are configured via four env vars
    ($WAITBUS_POLL_COST_GITHUB / _PYTEST / _DOCKER / _FS); defaults come
    from the model module and are documented as weighted-average
    tokens-per-poll over a typical session. The aggregate sum prints
    last, after every per-source row, never as a hero number. The
    events DB is opened read-only; no metric or column is added.
    """
    import waitbus.stats as mod

    raise typer.Exit(
        mod.cli_entry(
            poll_interval_seconds=poll_interval,
            per_source_token_costs=_resolve_per_source_costs(),
            as_json=as_json,
            db_path=db,
        )
    )
