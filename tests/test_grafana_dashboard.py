"""Validation tests for the waitbus Grafana dashboard JSON.

Each test is self-contained and does not require a running Grafana instance.
The dashboard file is loaded once at module import via the session-scoped
fixture; individual tests assert structural and semantic properties.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from waitbus._metrics import _HELP

# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

_DASHBOARD_PATH = Path(__file__).parent.parent / "monitoring" / "grafana" / "waitbus-backpressure.json"


@pytest.fixture(scope="session")
def dashboard() -> dict:  # type: ignore[type-arg]
    """Load and parse the Grafana dashboard JSON once per test session."""
    return json.loads(_DASHBOARD_PATH.read_text())  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_METRIC_NAME_RE = re.compile(r"\bci_status_\w+")


def _extract_metric_names(expr: str) -> list[str]:
    """Return every ci_status_* token found in a PromQL expression string.

    This is a conservative lexical scan — it does not parse PromQL grammar.
    It catches every bare metric name and every name inside a selector or
    function call.
    """
    return _METRIC_NAME_RE.findall(expr)


def _all_panels(dashboard: dict) -> list[dict]:  # type: ignore[type-arg]
    """Yield all non-row panels from the top-level panels list."""
    result = []
    for panel in dashboard.get("panels", []):
        if panel.get("type") == "row":
            continue
        result.append(panel)
    return result


def _all_exprs(dashboard: dict) -> list[str]:  # type: ignore[type-arg]
    """Return every PromQL expression string across all panel targets."""
    exprs = []
    for panel in _all_panels(dashboard):
        for target in panel.get("targets", []):
            expr = target.get("expr", "")
            if expr:
                exprs.append(expr)
    return exprs


# ---------------------------------------------------------------------------
# Structural tests
# ---------------------------------------------------------------------------


def test_dashboard_parses_as_dict(dashboard: dict) -> None:  # type: ignore[type-arg]
    """The dashboard JSON must parse into a dict (not a list or primitive)."""
    assert isinstance(dashboard, dict)


def test_dashboard_uid(dashboard: dict) -> None:  # type: ignore[type-arg]
    """The dashboard uid must be the canonical waitbus-backpressure value."""
    assert dashboard.get("uid") == "waitbus-backpressure"


def test_dashboard_has_panels(dashboard: dict) -> None:  # type: ignore[type-arg]
    """The dashboard must declare at least one non-row panel."""
    panels = _all_panels(dashboard)
    assert len(panels) > 0, "expected at least one non-row panel"


def test_all_panels_have_non_empty_title(dashboard: dict) -> None:  # type: ignore[type-arg]
    """Every non-row panel must have a non-empty string title."""
    for panel in _all_panels(dashboard):
        title = panel.get("title", "")
        assert isinstance(title, str) and title.strip(), f"panel id={panel.get('id')} has a missing or empty title"


def test_all_panels_have_at_least_one_target_expr(dashboard: dict) -> None:  # type: ignore[type-arg]
    """Every non-row panel must have at least one target with a non-empty expr."""
    for panel in _all_panels(dashboard):
        targets = panel.get("targets", [])
        exprs = [t.get("expr", "") for t in targets if t.get("expr", "")]
        assert exprs, f"panel '{panel.get('title')}' (id={panel.get('id')}) has no target with an expr"


# ---------------------------------------------------------------------------
# Semantic tests: metric names must exist in _HELP
# ---------------------------------------------------------------------------

# Strip the prometheus_client-appended histogram suffixes so the names map
# back to the keys in _HELP (e.g. waitbus_broadcast_send_seconds_bucket
# -> waitbus_broadcast_send_seconds).
_HISTOGRAM_SUFFIX_RE = re.compile(r"_(bucket|count|sum)$")

# The gauge and histogram singletons are declared under their base names in
# _HELP; the _total suffix is on counters only.
_KNOWN_METRIC_NAMES: frozenset[str] = frozenset(_HELP.keys())


def _canonical(name: str) -> str:
    """Recover the _HELP key from a raw metric name that may carry a suffix.

    prometheus_client appends ``_bucket``, ``_count``, or ``_sum`` to
    histogram series names when scraping. We strip those suffixes only when
    the raw name is not itself a known key — this avoids incorrectly stripping
    ``_count`` from ``waitbus_subscriber_count`` (a Gauge whose base name
    ends in ``_count``).
    """
    if name in _KNOWN_METRIC_NAMES:
        return name
    return _HISTOGRAM_SUFFIX_RE.sub("", name)


def test_all_promql_metric_names_exist_in_help(dashboard: dict) -> None:  # type: ignore[type-arg]
    """Every ci_status_* metric referenced in a PromQL expression must appear in _HELP.

    This guards against typos in metric names and ensures the dashboard stays
    in sync with the declared metric surface.
    """
    unknown: list[tuple[str, str]] = []
    for expr in _all_exprs(dashboard):
        for raw_name in _extract_metric_names(expr):
            canonical = _canonical(raw_name)
            if canonical not in _KNOWN_METRIC_NAMES:
                unknown.append((raw_name, expr))

    if unknown:
        lines = "\n".join(f"  {name!r} in {expr!r}" for name, expr in unknown)
        pytest.fail(f"PromQL expressions reference unknown metric names:\n{lines}")


# ---------------------------------------------------------------------------
# Negative test: dropped daemon label must not appear
# ---------------------------------------------------------------------------


def test_no_daemon_label_in_exprs(dashboard: dict) -> None:  # type: ignore[type-arg]
    """No PromQL expression must reference the 'daemon' label (dropped in MID-CS-007)."""
    for expr in _all_exprs(dashboard):
        assert 'daemon="' not in expr and "daemon='" not in expr, f"expr references dropped daemon label: {expr!r}"


def test_no_daemon_label_in_legend_formats(dashboard: dict) -> None:  # type: ignore[type-arg]
    """No legendFormat template must reference the 'daemon' label variable."""
    for panel in _all_panels(dashboard):
        for target in panel.get("targets", []):
            fmt = target.get("legendFormat", "")
            assert "{{daemon}}" not in fmt and "{{ daemon }}" not in fmt, (
                f"legendFormat references dropped daemon label: {fmt!r}"
            )
