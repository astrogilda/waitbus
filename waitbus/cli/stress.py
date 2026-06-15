"""``waitbus stress`` top-level command -- user-facing concurrency-knee probe.

Thin Typer wrapper that delegates to ``scripts.stress._controller.main``.
The controller is the canonical entry point shared with the CI gate
(``python -m scripts.stress``) and the user-facing ``waitbus stress``;
the wrapper exists so the public binary has a discoverable command
and so a downstream operator does not need the maintainer-only
``scripts.stress`` import path.

scipy is the canonical dep for the Universal Scalability Law fit and
is imported lazily by the controller. ``waitbus stress`` is the
canonical ``waitbus[stress]`` extra; an operator that did not install
the extra sees a clear error rather than a stale ImportError.
"""

from __future__ import annotations

import typer


def stress_cmd(ctx: typer.Context) -> None:
    """Probe the bus concurrency knee + run the zero-poll signal.

    Sweeps a per-N subscriber-count grid (default 1, 2, 4, 8, 16, 32,
    64), measures per-N throughput for the configured window, fits
    Gunther's three-parameter USL across the sweep, and writes a
    verdict JSON carrying the per-N curve, the fit parameters, and
    the derived knee. Flags:

        --sweep "1,2,4,8,16,32,64"   subscriber-count sweep
        --duration 60s               per-N measurement window
        --signals curve,zero_poll
        --real                        Real-mode (real claude -p / gemini -p / OpenAI drivers)
        --output ./stress-verdict.json

    Returns the controller's exit code: 0 on overall_passed=True,
    1 on any failure recorded in the verdict.
    """
    try:
        from scripts.stress._controller import main
    except ImportError as exc:
        if "scipy" in str(exc):
            raise typer.BadParameter(
                "waitbus stress requires the [stress] extra. "
                "Install with `pip install waitbus[stress]` or `uv tool install waitbus[stress]`.",
            ) from exc
        raise

    raise typer.Exit(main(list(ctx.args)))
