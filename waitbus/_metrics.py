"""Prometheus metrics facade backed by prometheus_client.

The listener's HTTP handler exposes a snapshot on ``/metrics``. Writers
call ``incr(...)``, ``Histogram.observe(...)``, or ``Gauge.set/inc/dec(...)``.
Serialisation delegates to ``prometheus_client.generate_latest()``.

Counters use the ``_total`` suffix; histograms and gauges do not.
``prometheus_client`` appends ``_total`` at render time, so counters are
registered under ``name.removesuffix("_total")``.

``disable_created_metrics()`` suppresses the ``*_created`` companion gauges
that ``prometheus_client`` emits by default.
"""

from __future__ import annotations

from typing import Final, TypedDict

import prometheus_client as pc
from prometheus_client.parser import text_string_to_metric_families


class MetricSample(TypedDict):
    """One row in a ``snapshot()`` family list.

    ``name`` is the Prometheus per-sample name (e.g. ``waitbus_broadcast_send_seconds_bucket``).
    ``labels`` is the label-key/value dict for that sample (empty for unlabelled metrics).
    ``value`` is the numeric value at the time of the snapshot call.
    """

    name: str
    labels: dict[str, str]
    value: float


pc.disable_created_metrics()  # type: ignore[no-untyped-call]

# HELP strings — serialised as ``# HELP`` lines in Prometheus text format.
_HELP: dict[str, str] = {
    "waitbus_webhook_received_total": "Webhook deliveries received by the listener, by HTTP path.",
    "waitbus_webhook_hmac_rejected_total": "Webhook deliveries rejected for invalid or missing HMAC signature.",
    "waitbus_webhook_bad_json_total": "Webhook deliveries rejected for malformed JSON.",
    "waitbus_webhook_bad_length_total": "Webhook deliveries rejected for Content-Length out of range.",
    "waitbus_webhook_ignored_total": "Webhook deliveries acknowledged but not persisted (unsupported X-GitHub-Event).",
    "waitbus_webhook_read_timeout_total": "Webhook deliveries aborted because the body read timed out.",
    "waitbus_db_inserted_total": "Event rows actually inserted into the events table.",
    "waitbus_db_dedup_ignored_total": "Events dropped by INSERT OR IGNORE because delivery_id already existed.",
    "waitbus_db_error_total": "Event inserts that raised sqlite3.Error.",
    "waitbus_etag_poll_runs_total": "Number of times the ETag poller has run, by outcome.",
    "waitbus_etag_poll_requests_total": "GitHub API requests issued by the poller, by HTTP status code.",
    "waitbus_watermark_replay_events_total": "Events delivered via watermark replay rather than live broadcast.",
    "waitbus_subscriber_count": "Active subscribers connected to the broadcast daemon.",
    "waitbus_broadcast_send_seconds": "End-to-end producer-side broadcast send latency.",
    "waitbus_broadcast_subscription_count": (
        "Subscribers the broadcast daemon attempted to send the last fan-out frame to."
    ),
    "waitbus_broadcast_emission_latency_seconds": (
        "Slowest single-subscriber send latency observed in the last fan-out pass."
    ),
    "waitbus_broadcast_stale_subscription_count": (
        "Subscribers carrying a non-zero EAGAIN lag counter (lagging but not yet dropped)."
    ),
    "waitbus_subscriber_rejected_total": (
        "Subscribe attempts that received a subscribe_rejected wire frame, by consumer-facing reason."
    ),
    "waitbus_subscriber_evicted_total": (
        "Subscribers removed from the broadcast daemon's subscriber map, by internal close reason."
    ),
    "waitbus_subscriber_opened_total": (
        "Subscriber connections that completed the subscribe handshake and were registered."
    ),
    "waitbus_subscriber_closed_total": (
        "Subscribers removed from the daemon map for any reason (mirror decrement of opened)."
    ),
    "waitbus_broadcast_events_emitted_total": (
        "Event rows swept above the cursor and fanned out by the broadcast daemon."
    ),
    "waitbus_broadcast_events_delivered_total": (
        "Per-subscriber EVENT frames delivered in full, counted at "
        "kernel-accept: synchronously sent frames count at send time, "
        "EAGAIN-queued frames at that frame's flush completion. Control "
        "frames (heartbeat, subscribe_ack, subscribe_rejected) never count."
    ),
}

# Label names per metric — single source of truth, declared at import.
# Empty tuple means the metric is unlabelled.
_LABEL_NAMES: dict[str, tuple[str, ...]] = {
    "waitbus_webhook_received_total": ("path",),
    "waitbus_webhook_hmac_rejected_total": ("path", "reason"),
    "waitbus_webhook_bad_json_total": ("path",),
    "waitbus_webhook_bad_length_total": ("path",),
    "waitbus_webhook_ignored_total": ("path", "event_type"),
    "waitbus_webhook_read_timeout_total": ("path",),
    "waitbus_db_inserted_total": ("event_type", "source", "ingest_method"),
    "waitbus_db_dedup_ignored_total": ("event_type", "source", "ingest_method"),
    "waitbus_db_error_total": ("path", "source"),
    "waitbus_etag_poll_runs_total": ("outcome",),
    "waitbus_etag_poll_requests_total": ("endpoint", "status"),
    "waitbus_watermark_replay_events_total": (),
    "waitbus_subscriber_rejected_total": ("reason",),
    "waitbus_subscriber_evicted_total": ("reason",),
    "waitbus_subscriber_opened_total": (),
    "waitbus_subscriber_closed_total": (),
    "waitbus_broadcast_events_emitted_total": (),
    "waitbus_broadcast_events_delivered_total": (),
}

# Module-level registry — replaced wholesale on reset().
_REGISTRY: pc.CollectorRegistry = pc.CollectorRegistry()
_COUNTERS: dict[str, pc.Counter] = {}  # name (with _total) -> pc.Counter
_HISTOGRAMS: dict[str, Histogram] = {}
_GAUGES: dict[str, Gauge] = {}


def _init_registry() -> None:
    """Pre-register every declared Counter in ``_REGISTRY``.

    Single-threaded import-time registration walks ``_HELP`` keyed by
    ``_LABEL_NAMES`` so every counter is declared before any writer can
    race. Histograms and Gauges register themselves in their ``__init__``
    when the module-level singletons are constructed below.
    """
    for name, label_names in _LABEL_NAMES.items():
        if name in _COUNTERS:
            continue
        _COUNTERS[name] = pc.Counter(
            name.removesuffix("_total"),
            _HELP.get(name, ""),
            list(label_names),
            registry=_REGISTRY,
        )


def incr(name: str, value: int = 1, **labels: str) -> None:
    """Increment counter ``name`` by ``value`` with the given label set.

    The counter is pre-registered at module import by ``_init_registry()``
    with the label-names declared in ``_LABEL_NAMES``; this function only
    looks it up and calls ``.labels(...).inc()`` or ``.inc()``. Thread-safe
    by construction: no registration happens on the hot path, so the
    prometheus_client internal lock is the only synchronisation needed.
    """
    counter = _COUNTERS[name]
    if labels:
        counter.labels(**labels).inc(value)
    else:
        counter.inc(value)


def get(name: str, **labels: str) -> int:
    """Return the current integer value of ``name`` for the given label set."""
    target = name.removesuffix("_total")
    for family in text_string_to_metric_families(pc.generate_latest(_REGISTRY).decode()):
        if family.name != target:
            continue
        for sample in family.samples:
            if sample.name.endswith("_total") and dict(sample.labels) == labels:
                return int(sample.value)
    return 0


def reset() -> None:
    """Test-only: zero every counter, histogram, and gauge.

    Replaces ``_REGISTRY`` with a fresh ``CollectorRegistry``, then
    re-runs ``_init_registry()`` so every declared counter is registered
    again, and rebinds the histogram/gauge backings to the new registry.
    """
    global _REGISTRY
    _REGISTRY = pc.CollectorRegistry()
    _COUNTERS.clear()
    _init_registry()
    for h in _HISTOGRAMS.values():
        h._reset_backing(_REGISTRY)
    for g in _GAUGES.values():
        g._reset_backing(_REGISTRY)


def render() -> bytes:
    """Serialise all metrics to Prometheus text-format via generate_latest()."""
    return pc.generate_latest(_REGISTRY)


def _render_block(name: str) -> list[str]:
    """Return all Prometheus text lines for the metric named ``name``."""
    lines: list[str] = []
    in_block = False
    for line in pc.generate_latest(_REGISTRY).decode().splitlines():
        if line.startswith(f"# HELP {name}"):
            in_block = True
        elif in_block and line.startswith("# HELP "):
            break
        if in_block:
            lines.append(line)
    return lines


class Counter:
    """Typed wrapper around a counter pre-registered by ``_init_registry``.

    Construction does not register; it just binds a name to the wrapper API.
    The actual ``pc.Counter`` lives in ``_COUNTERS[name]`` and was registered
    at module import. Help text comes from the ``_HELP`` dict, which is the
    single source of truth.
    """

    def __init__(self, name: str) -> None:
        self._name = name

    def inc(self, value: int = 1, **labels: str) -> None:
        """Increment this counter by ``value`` (default 1)."""
        incr(self._name, value, **labels)

    def value(self, **labels: str) -> int:
        """Return the current count for the given label combination."""
        return get(self._name, **labels)


class Histogram:
    """Cumulative histogram delegating observations to prometheus_client.

    Label names are declared at construction; observations must pass the same
    label-name set or prometheus_client raises ValueError. There is no lazy
    relabel path.
    """

    def __init__(
        self,
        name: str,
        buckets: tuple[float, ...],
        *,
        labelnames: tuple[str, ...] = (),
        help_text: str = "",
    ) -> None:
        self._name = name
        self._buckets = tuple(sorted(buckets))
        self._labelnames = labelnames
        if help_text:
            _HELP[name] = help_text
        self._backing: pc.Histogram = pc.Histogram(
            name,
            _HELP.get(name, ""),
            list(labelnames),
            buckets=self._buckets,
            registry=_REGISTRY,
        )
        _HISTOGRAMS[name] = self

    def _reset_backing(self, registry: pc.CollectorRegistry) -> None:
        """Replace the backing pc.Histogram in a fresh registry (called by reset())."""
        self._backing = pc.Histogram(
            self._name,
            _HELP.get(self._name, ""),
            list(self._labelnames),
            buckets=self._buckets,
            registry=registry,
        )

    def observe(self, value: float, **labels: str) -> None:
        """Record one observation of ``value``."""
        if labels:
            self._backing.labels(**labels).observe(value)
        else:
            self._backing.observe(value)

    def render(self) -> list[str]:
        """Return Prometheus text-format lines for this histogram (instance view)."""
        return _render_block(self._name)


class Gauge:
    """Point-in-time gauge delegating to prometheus_client.

    Label names are declared at construction; mutations must pass the same
    label-name set or prometheus_client raises ValueError.
    """

    def __init__(
        self,
        name: str,
        *,
        labelnames: tuple[str, ...] = (),
        help_text: str = "",
    ) -> None:
        self._name = name
        self._labelnames = labelnames
        if help_text:
            _HELP[name] = help_text
        self._backing: pc.Gauge = pc.Gauge(
            name,
            _HELP.get(name, ""),
            list(labelnames),
            registry=_REGISTRY,
        )
        _GAUGES[name] = self

    def _reset_backing(self, registry: pc.CollectorRegistry) -> None:
        """Replace the backing pc.Gauge in a fresh registry (called by reset())."""
        self._backing = pc.Gauge(
            self._name,
            _HELP.get(self._name, ""),
            list(self._labelnames),
            registry=registry,
        )

    def set(self, value: float, **labels: str) -> None:
        """Replace the current gauge value."""
        (self._backing.labels(**labels) if labels else self._backing).set(value)

    def inc(self, amount: float = 1, **labels: str) -> None:
        """Add ``amount`` to the current gauge value."""
        (self._backing.labels(**labels) if labels else self._backing).inc(amount)

    def dec(self, amount: float = 1, **labels: str) -> None:
        """Subtract ``amount`` from the current gauge value."""
        (self._backing.labels(**labels) if labels else self._backing).dec(amount)

    def value(self, **labels: str) -> float:
        """Return the current value for the given label combination."""
        for family in text_string_to_metric_families(pc.generate_latest(_REGISTRY).decode()):
            if family.name != self._name:
                continue
            for sample in family.samples:
                if sample.name == self._name and dict(sample.labels) == labels:
                    return float(sample.value)
        return 0.0

    def render(self) -> list[str]:
        """Return Prometheus text-format lines for this gauge (instance view)."""
        return _render_block(self._name)


SUBSCRIBER_COUNT: Final[Gauge] = Gauge("waitbus_subscriber_count")

BROADCAST_SEND_BUCKETS: Final[tuple[float, ...]] = (0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1)
"""Histogram bucket boundaries for ``waitbus_broadcast_send_seconds``.

Spans sub-millisecond (100 µs) through 100 ms; the expected p99 for a
local AF_UNIX send is well under 1 ms, so the tight lower buckets give
useful latency resolution while the upper buckets catch pathological
slow sends without overflowing.
"""

BROADCAST_SEND_SECONDS: Final[Histogram] = Histogram(
    "waitbus_broadcast_send_seconds",
    buckets=BROADCAST_SEND_BUCKETS,
)

# Per-fan-out subscription-health gauges. Driven from the broadcast
# daemon's _fan_out pass off the existing Subscriber.lag_count field —
# no new per-subscriber bookkeeping. SUBSCRIPTION_COUNT is the matched
# fan-out target count; EMISSION_LATENCY_SECONDS is the slowest single
# send in the pass; STALE_SUBSCRIPTION_COUNT is how many subscribers are
# currently lagging (lag_count > 0) but still under the drop threshold.
BROADCAST_SUBSCRIPTION_COUNT: Final[Gauge] = Gauge("waitbus_broadcast_subscription_count")

BROADCAST_EMISSION_LATENCY_SECONDS: Final[Gauge] = Gauge("waitbus_broadcast_emission_latency_seconds")

BROADCAST_STALE_SUBSCRIPTION_COUNT: Final[Gauge] = Gauge("waitbus_broadcast_stale_subscription_count")

# Backlog-drain visibility gauges, refreshed per fan-out pass and per
# heartbeat tick. Aggregates only: per-subscriber labels (fd- or
# peer-keyed) would be unbounded cardinality, the canonical Prometheus
# anti-pattern, so the surface is the worst lag count and the total
# buffered byte count across all subscribers.
SUBSCRIBER_LAG_MAX: Final[Gauge] = Gauge(
    "waitbus_subscriber_lag_max",
    help_text="Highest per-subscriber consecutive-EAGAIN lag count observed in the last gauge refresh.",
)

SUBSCRIBER_TX_BUFFER_BYTES: Final[Gauge] = Gauge(
    "waitbus_subscriber_tx_buffer_bytes",
    help_text="Total bytes pending in user-space per-subscriber transmit buffers.",
)

WATERMARK_REPLAY_EVENTS_TOTAL: Final[Counter] = Counter("waitbus_watermark_replay_events_total")

# Pre-register every counter at module import. Single-threaded import
# semantics make this race-free without an explicit lock.
_init_registry()


# ---------------------------------------------------------------------------
# JSON snapshot for off-process scrapers (stress / soak harnesses)
# ---------------------------------------------------------------------------


def snapshot() -> dict[str, list[MetricSample]]:
    """Return the current values of every registered metric as JSON-friendly data.

    Shape: ``{family_name: [MetricSample, ...]}``. The family name keys
    (e.g. ``waitbus_subscriber_evicted``) follow Prometheus's own
    family-grouping convention -- counters drop their ``_total`` suffix,
    histograms add ``_bucket`` / ``_count`` / ``_sum`` per-sample names, and
    gauges appear unchanged. The output is bit-stable: the ordering follows
    the registry-walk order from ``generate_latest``.

    This is the channel the in-tree harnesses use to scrape per-tick metric
    snapshots from a subprocess daemon. The broadcast daemon also serves an
    opt-in, loopback-only HTTP ``/metrics`` endpoint (see
    :mod:`waitbus._metrics_http`), but the JSON snapshot remains the harness
    channel: the line lands in the same stream ``_log.structured`` already
    writes to, so harnesses need no HTTP port or socket.
    """
    out: dict[str, list[MetricSample]] = {}
    for family in text_string_to_metric_families(pc.generate_latest(_REGISTRY).decode()):
        family_samples: list[MetricSample] = []
        for sample in family.samples:
            family_samples.append(
                MetricSample(
                    name=sample.name,
                    labels=dict(sample.labels),
                    value=float(sample.value),
                )
            )
        out[family.name] = family_samples
    return out
