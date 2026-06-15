"""Tests for the USL fit + waitbus stress CLI wiring.

The USL numerics are tested against synthetic data drawn from the
canonical Gunther curve so the fit's recovered (alpha, beta, gamma)
must round-trip within a tight tolerance. The CLI test is the wiring
gate: the ``stress`` subcommand must be registered on the root Typer
and produce a usable ``--help`` view.
"""

from __future__ import annotations

import re

import pytest

scipy_optimize = pytest.importorskip("scipy.optimize")

from typer.testing import CliRunner

from scripts.stress._usl import (
    USLFit,
    cusum_changepoint,
    fit_usl,
    knee,
    usl_throughput,
)
from waitbus import cli

# --- USL functional shape ----------------------------------------------------


def test_usl_throughput_is_linear_when_alpha_and_beta_are_zero() -> None:
    """``X(N) = gamma * N`` is the ideal-parallel case (no contention, no coherency)."""
    for n in (1.0, 2.0, 4.0, 8.0):
        assert usl_throughput(n, alpha=0.0, beta=0.0, gamma=100.0) == pytest.approx(100.0 * n)


def test_usl_throughput_saturates_with_alpha_only() -> None:
    """With beta = 0 the curve saturates at the Amdahl asymptote ``gamma / alpha``."""
    n_high = 10_000.0
    actual = usl_throughput(n_high, alpha=0.05, beta=0.0, gamma=100.0)
    assert actual == pytest.approx(100.0 / 0.05, rel=0.01)


def test_knee_is_sqrt_of_one_minus_alpha_over_beta() -> None:
    """The canonical knee formula round-trips."""
    n_star = knee(alpha=0.05, beta=0.001)
    assert n_star is not None
    # sqrt((1 - 0.05) / 0.001) = sqrt(950) ~ 30.82
    assert n_star == pytest.approx((0.95 / 0.001) ** 0.5)


def test_knee_returns_none_when_no_real_max_exists() -> None:
    """A degenerate curve with no real knee (beta=0 or alpha>=1) returns None."""
    assert knee(alpha=0.05, beta=0.0) is None
    assert knee(alpha=1.0, beta=0.001) is None


# --- USL nonlinear regression ------------------------------------------------


def test_fit_usl_round_trips_known_alpha_beta_gamma() -> None:
    """Generating data from a known (alpha, beta, gamma) and fitting must recover them."""
    true_alpha, true_beta, true_gamma = 0.04, 0.0008, 120.0
    n_values = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0]
    throughput_values = [usl_throughput(n, true_alpha, true_beta, true_gamma) for n in n_values]

    fit = fit_usl(n_values, throughput_values)

    assert isinstance(fit, USLFit)
    assert fit.alpha == pytest.approx(true_alpha, abs=1e-6)
    assert fit.beta == pytest.approx(true_beta, abs=1e-6)
    assert fit.gamma == pytest.approx(true_gamma, abs=1e-4)
    assert fit.residuals_rss == pytest.approx(0.0, abs=1e-6)


def test_fit_usl_refuses_fewer_than_three_points() -> None:
    """The three-parameter form requires at least three points."""
    with pytest.raises(ValueError):
        fit_usl([1.0, 2.0], [10.0, 20.0])


def test_fit_usl_refuses_length_mismatch() -> None:
    """A length-mismatched (N, throughput) pair is a setup-side bug."""
    with pytest.raises(ValueError):
        fit_usl([1.0, 2.0, 4.0], [10.0, 20.0])


def test_fit_usl_refuses_all_zero_throughput() -> None:
    """If the sweep produced no observable signal, the fit refuses to lie about it."""
    with pytest.raises(ValueError):
        fit_usl([1.0, 2.0, 4.0], [0.0, 0.0, 0.0])


# --- CUSUM steady-state detector --------------------------------------------


def test_cusum_changepoint_returns_none_for_a_stable_series() -> None:
    """A constant-mean series never crosses the CUSUM threshold."""
    assert cusum_changepoint([10.0] * 20, threshold=5.0) is None


def test_cusum_changepoint_flags_a_step_change() -> None:
    """A step-change halfway through the series crosses the CUSUM threshold."""
    series = [10.0] * 10 + [50.0] * 10
    changepoint = cusum_changepoint(series, threshold=10.0)

    assert changepoint is not None
    assert changepoint >= 10  # the changepoint sits at or after the step


def test_cusum_changepoint_handles_empty_series() -> None:
    """An empty series cannot produce a changepoint."""
    assert cusum_changepoint([], threshold=5.0) is None


# --- waitbus stress CLI wiring -------------------------------------------------


def test_waitbus_stress_subcommand_is_registered() -> None:
    """The root Typer app exposes a ``stress`` subcommand."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["--help"])

    assert result.exit_code == 0
    assert "stress" in result.output, result.output


def test_waitbus_stress_help_exposes_the_user_facing_flags() -> None:
    """``waitbus stress --help`` reaches the controller and prints its argparse usage."""
    runner = CliRunner()
    # Pin a wide width so rich does not wrap, and strip ANSI before the token
    # checks. Typer renders --help through rich; when color is forced (the CI
    # runners export a color-forcing env), rich's help highlighter styles each
    # dash of a flag in its own SGR span, e.g. "\x1b[..m-\x1b[0m\x1b[..m-sweep",
    # so the raw output has no contiguous "--sweep" substring. Locally color is
    # off and the raw text already contains the flags; stripping ANSI is a
    # no-op there and the correct normalisation under forced color.
    result = runner.invoke(cli.app, ["stress", "--help"], env={"COLUMNS": "200"})

    # argparse exits 0 on --help; the output should mention the controller's flags.
    assert result.exit_code == 0
    clean = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
    for token in ("--sweep", "--duration", "--signals", "--real", "--output"):
        assert token in clean, f"missing flag {token!r} in --help output:\n{clean}"
