"""Universal Scalability Law fit + CUSUM steady-state changepoint.

Two small numerical primitives the stress controller needs once the
per-N curve has been collected:

- ``fit_usl`` runs ``scipy.optimize.curve_fit`` against Gunther's
  three-parameter USL form ``X(N) = gamma * N / (1 + alpha * (N - 1) + beta * N * (N - 1))``,
  returning (alpha, beta, gamma) plus the parameter-covariance
  matrix. ``knee`` then derives the maximum-throughput concurrency
  ``N* = sqrt((1 - alpha) / beta)`` from the textbook canonical form
  (VL-10).

- ``cusum_changepoint`` is the steady-state-onset detector for the
  per-N measurement window: each per-tick throughput sample is fed
  through a one-sided CUSUM, and the first index whose accumulator
  exceeds the threshold is the changepoint at which the run is taken
  to be in steady state. Reserves the warmup-leak assertion that
  Barrett / Tratt / Bolz-Tereick et al. called out as the
  load-bearing prerequisite for any percentile reporting.

scipy is imported lazily (and only inside ``fit_usl``) so the harness
package itself imports cleanly without ``[stress]`` extras installed
-- the CLI subcommand surfaces an informative error when scipy is
absent.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

# Minimum number of (N, throughput) data points ``fit_usl`` requires before
# attempting the 3-parameter curve fit.  ``scipy.optimize.curve_fit`` refuses
# an under-determined system; raising this sentinel from the call site produces
# a cleaner ``ValueError`` than the opaque scipy exception that would otherwise
# surface.
_USL_MIN_POINTS: Final[int] = 3

# Initial parameter guesses for ``curve_fit``.  The textbook starting point
# (Gunther §7.2) corresponds to a perfectly-linear bus before any contention
# is visible: ``alpha = 0.1`` (10 % serialisation penalty) and
# ``beta = 0.01`` (1 % coherency penalty).  ``gamma`` is initialised from the
# first non-zero throughput sample because it anchors the curve to the
# observed scale and dramatically shrinks the optimisation search space.
_USL_INITIAL_ALPHA: Final[float] = 0.1
_USL_INITIAL_BETA: Final[float] = 0.01

# Maximum number of function evaluations ``curve_fit`` is allowed before it
# gives up and raises ``RuntimeError``.  10 000 is the scipy default; named
# here so the harness can tighten or loosen the budget without hunting for a
# bare literal.
_USL_MAXFEV: Final[int] = 10_000


def usl_throughput(n: float, alpha: float, beta: float, gamma: float) -> float:
    """Evaluate Gunther's three-parameter USL ``X(N)`` at one ``N``.

    Returns ``gamma * N / (1 + alpha * (N - 1) + beta * N * (N - 1))``. The denominator is the
    serialization (alpha) and coherency (beta) penalty. The closed-form is
    safe for ``n >= 1`` and any non-negative parameters; the harness
    only ever evaluates it on the sweep grid, which is well-defined.
    """
    return gamma * n / (1.0 + alpha * (n - 1.0) + beta * n * (n - 1.0))


@dataclass(slots=True, frozen=True)
class USLFit:
    """Fit result from ``fit_usl``.

    ``alpha`` / ``beta`` / ``gamma`` are the regressed parameters;
    ``covariance`` is the parameter-covariance matrix flattened into a
    3x3 tuple-of-tuples so the verdict JSON can carry it verbatim
    without a numpy round-trip. ``residuals_rss`` is the sum-of-squared
    residuals from the fit -- a sanity signal that the data was
    well-described by the USL form. A ``None`` ``covariance`` indicates
    scipy failed to estimate it (typically when the sweep had fewer
    than three independent points; the harness logs the failure).
    """

    alpha: float
    beta: float
    gamma: float
    covariance: tuple[tuple[float, float, float], ...] | None
    residuals_rss: float


def fit_usl(n_values: Sequence[float], throughput_values: Sequence[float]) -> USLFit:
    """Fit the three-parameter USL to ``(n, throughput)`` pairs.

    Requires at least ``_USL_MIN_POINTS`` points (scipy refuses a
    3-parameter fit against fewer). The initial parameter guess is the
    textbook starting point: ``alpha = _USL_INITIAL_ALPHA``,
    ``beta = _USL_INITIAL_BETA``, ``gamma = throughput[0]`` -- which
    corresponds to a perfectly-linear bus until contention shows up. The fit is bounded to keep the
    physical interpretation valid (``alpha >= 0``, ``beta >= 0``,
    ``gamma > 0``) so the regression cannot wander into negative-
    contention territory that would never make sense for a real bus.

    Raises ``ValueError`` when the inputs cannot be fit (fewer than
    three points, mismatched lengths, all-zero throughput).
    """
    if len(n_values) != len(throughput_values):
        raise ValueError(
            f"fit_usl requires len(n_values) == len(throughput_values); got {len(n_values)} vs {len(throughput_values)}"
        )
    if len(n_values) < _USL_MIN_POINTS:
        raise ValueError(
            f"fit_usl requires at least {_USL_MIN_POINTS} points for the 3-parameter form; got {len(n_values)}"
        )

    import numpy as np  # lazy import to keep the package import light
    from scipy.optimize import curve_fit

    n_arr = np.asarray(n_values, dtype=float)
    x_arr = np.asarray(throughput_values, dtype=float)
    if not np.any(x_arr > 0):
        raise ValueError("fit_usl received all-zero throughput; the sweep produced no observable signal")

    initial_gamma = float(x_arr[0]) if x_arr[0] > 0 else float(x_arr[x_arr > 0][0])
    initial_guess = [_USL_INITIAL_ALPHA, _USL_INITIAL_BETA, initial_gamma]
    bounds = ([0.0, 0.0, 1e-9], [1.0, 1.0, np.inf])

    def _model(n: np.ndarray, alpha: float, beta: float, gamma: float) -> np.ndarray:
        return gamma * n / (1.0 + alpha * (n - 1.0) + beta * n * (n - 1.0))

    popt, pcov = curve_fit(_model, n_arr, x_arr, p0=initial_guess, bounds=bounds, maxfev=_USL_MAXFEV)

    residuals = x_arr - _model(n_arr, *popt)
    residuals_rss = float(np.sum(residuals * residuals))

    covariance: tuple[tuple[float, float, float], ...] | None
    if pcov is None or not np.all(np.isfinite(pcov)):
        covariance = None
    else:
        covariance = tuple((float(row[0]), float(row[1]), float(row[2])) for row in pcov)

    alpha, beta, gamma = (float(v) for v in popt)
    return USLFit(alpha=alpha, beta=beta, gamma=gamma, covariance=covariance, residuals_rss=residuals_rss)


def knee(alpha: float, beta: float) -> float | None:
    """Compute Gunther's canonical knee ``N* = sqrt((1 - alpha) / beta)``.

    Returns ``None`` when no real-valued knee exists (``alpha >= 1`` or
    ``beta <= 0`` -- the model has no maximum and the curve is
    monotonically rising or saturating at the Amdahl asymptote
    ``1/alpha`` instead).
    """
    if alpha >= 1.0 or beta <= 0.0:
        return None
    return math.sqrt((1.0 - alpha) / beta)


# --- CUSUM steady-state detector --------------------------------------------


def cusum_changepoint(
    series: Sequence[float],
    *,
    threshold: float,
    sensitivity: float = 1.0,
) -> int | None:
    """One-sided CUSUM detector for steady-state onset.

    Returns the first index ``i`` whose running CUSUM statistic
    exceeds ``threshold`` and stays above the centered mean of the
    samples seen up to that point. ``sensitivity`` rescales the
    departure value (default 1.0); raising it makes the detector
    less reactive.

    Returns ``None`` when the series never converges (no index
    crosses the threshold). The harness in that case emits a
    ``NO_STEADY_STATE`` failure into the verdict rather than
    silently report percentiles from a non-stationary window.

    Implementation note: this is the canonical Page (1954) one-sided
    CUSUM with online-updated reference value. We deliberately stay
    away from ``ruptures`` / PELT here: those introduce a third-party
    dep just for a 20-line detector, and the BarrettTratt / Bolz-
    Tereick papers themselves consider CUSUM the canonical online
    detector for steady-state onset (more advanced methods buy
    statistical efficiency, not structural correctness).
    """
    if not series:
        return None
    cusum_pos = 0.0
    running_sum = 0.0
    for index, value in enumerate(series):
        running_count = index + 1
        running_sum += float(value)
        running_mean = running_sum / running_count
        departure = (float(value) - running_mean) / max(sensitivity, 1e-12)
        cusum_pos = max(0.0, cusum_pos + departure)
        if cusum_pos > threshold:
            return index
    return None
