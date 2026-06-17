"""Tests for the _metrics primitives (Counter, Histogram, Gauge) and the
listener's /metrics endpoint.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable, Generator

import pytest
from conftest import ServerHandle
from prometheus_client.parser import text_string_to_metric_families

from waitbus import _metrics


@pytest.fixture(autouse=True)
def _reset_counters() -> Generator[None, None, None]:
    """Every test starts with clean counter/histogram/gauge snapshots."""
    _metrics.reset()
    yield
    _metrics.reset()


# ---------------------------------------------------------------------------
# Helper: parse render() output into (family_name, label_dict, value) tuples
# ---------------------------------------------------------------------------


def _parse_samples(output: bytes) -> list[tuple[str, dict[str, str], float]]:
    """Return a flat list of (family_name, labels_dict, value) from Prometheus text."""
    results = []
    for family in text_string_to_metric_families(output.decode()):
        for sample in family.samples:
            results.append((sample.name, dict(sample.labels), sample.value))
    return results


# --- _metrics module --------------------------------------------------------


def test_incr_get_roundtrip_with_labels() -> None:
    _metrics.incr("waitbus_db_inserted_total", event_type="workflow_run", source="github", ingest_method="webhook")
    _metrics.incr("waitbus_db_inserted_total", event_type="workflow_run", source="github", ingest_method="webhook")
    _metrics.incr("waitbus_db_inserted_total", event_type="workflow_job", source="github", ingest_method="webhook")
    assert (
        _metrics.get(
            "waitbus_db_inserted_total",
            event_type="workflow_run",
            source="github",
            ingest_method="webhook",
        )
        == 2
    )
    assert (
        _metrics.get(
            "waitbus_db_inserted_total",
            event_type="workflow_job",
            source="github",
            ingest_method="webhook",
        )
        == 1
    )


def test_get_returns_zero_for_unset_label_combo() -> None:
    assert _metrics.get("waitbus_db_inserted_total", event_type="nope") == 0


def test_render_emits_help_and_type_lines_after_incr() -> None:
    """After incr(), HELP + TYPE lines appear for the incremented counter."""
    _metrics.incr("waitbus_webhook_received_total", path="/webhook")
    body = _metrics.render().decode("utf-8")
    assert "# HELP waitbus_webhook_received_total" in body
    assert "# TYPE waitbus_webhook_received_total counter" in body


def test_render_declares_counters_at_import_with_zero_samples() -> None:
    """Every counter declared in _LABEL_NAMES has its HELP/TYPE lines emitted
    at import. Unlabelled counters render a single zero-valued sample; labelled
    counters render no sample rows until first observation.
    """
    body = _metrics.render().decode("utf-8")
    # HELP/TYPE lines must be present for declared metrics.
    assert "# HELP waitbus_db_inserted_total" in body
    assert "# TYPE waitbus_db_inserted_total counter" in body
    # Labelled counter: no sample rows yet (only HELP/TYPE).
    assert "waitbus_db_inserted_total{" not in body


def test_render_includes_both_label_values() -> None:
    """Both label variants must appear with correct counts after incr()."""
    _metrics.incr("waitbus_webhook_received_total", path="/webhook")
    _metrics.incr("waitbus_webhook_received_total", path="/alertmanager")
    samples = _parse_samples(_metrics.render())
    alertmanager = [
        v
        for name, labels, v in samples
        if name == "waitbus_webhook_received_total" and labels.get("path") == "/alertmanager"
    ]
    webhook = [
        v
        for name, labels, v in samples
        if name == "waitbus_webhook_received_total" and labels.get("path") == "/webhook"
    ]
    assert alertmanager == [1.0], f"alertmanager count: {alertmanager}"
    assert webhook == [1.0], f"webhook count: {webhook}"


def test_render_escapes_label_value_special_chars() -> None:
    _metrics.incr("waitbus_webhook_received_total", path='/with"quote')
    body = _metrics.render().decode("utf-8")
    assert 'path="/with\\"quote"' in body


def test_subscriber_rejected_counter_records_reason_label() -> None:
    """``waitbus_subscriber_rejected_total`` is labelled by the consumer-facing wire reason."""
    _metrics.incr("waitbus_subscriber_rejected_total", reason="version")
    _metrics.incr("waitbus_subscriber_rejected_total", reason="lag_limit_exceeded")
    _metrics.incr("waitbus_subscriber_rejected_total", reason="lag_limit_exceeded")
    assert _metrics.get("waitbus_subscriber_rejected_total", reason="version") == 1
    assert _metrics.get("waitbus_subscriber_rejected_total", reason="lag_limit_exceeded") == 2


def test_subscriber_evicted_counter_records_internal_reason_label() -> None:
    """``waitbus_subscriber_evicted_total`` is labelled by the daemon-internal close reason."""
    _metrics.incr("waitbus_subscriber_evicted_total", reason="lag_limit_exceeded")
    _metrics.incr("waitbus_subscriber_evicted_total", reason="heartbeat_lag")
    _metrics.incr("waitbus_subscriber_evicted_total", reason="replay_lag_limit_exceeded")
    _metrics.incr("waitbus_subscriber_evicted_total", reason="replay_db_error")
    _metrics.incr("waitbus_subscriber_evicted_total", reason="subscribe_ack_send_failed")
    _metrics.incr("waitbus_subscriber_evicted_total", reason="shutdown")
    for reason in (
        "lag_limit_exceeded",
        "heartbeat_lag",
        "replay_lag_limit_exceeded",
        "replay_db_error",
        "subscribe_ack_send_failed",
        "shutdown",
    ):
        assert _metrics.get("waitbus_subscriber_evicted_total", reason=reason) == 1


def test_db_error_counter_accepts_broadcast_replay_path() -> None:
    """``waitbus_db_error_total`` accepts ``path='broadcast_replay'`` for the replay sqlite3.Error arm."""
    _metrics.incr("waitbus_db_error_total", path="broadcast_replay", source="broadcast")
    _metrics.incr("waitbus_db_error_total", path="broadcast_replay", source="broadcast")
    assert _metrics.get("waitbus_db_error_total", path="broadcast_replay", source="broadcast") == 2


# --- /metrics HTTP endpoint -------------------------------------------------


def test_metrics_endpoint_returns_text_plain(server_fixture: ServerHandle) -> None:
    with urllib.request.urlopen(server_fixture.url("/metrics"), timeout=2) as resp:
        assert resp.status == 200
        ct = resp.getheader("Content-Type") or ""
        assert ct.startswith("text/plain")
        assert "version=0.0.4" in ct
        body = resp.read().decode("utf-8")
    # Module-level Gauge and Histogram instances are always registered.
    assert "waitbus_subscriber_count" in body
    assert "waitbus_broadcast_send_seconds" in body


def test_metrics_endpoint_reflects_webhook_traffic(
    server_fixture: ServerHandle, gh_secret: bytes, hmac_sig: Callable[[bytes, bytes], str]
) -> None:
    """A successful webhook POST increments received_total + db_inserted_total."""
    payload = {
        "repository": {"name": "demo-repo", "owner": {"login": "demo-owner"}},
        "workflow_run": {
            "id": 1,
            "name": "Tests",
            "head_branch": "main",
            "head_sha": "a",
            "status": "completed",
            "conclusion": "success",
        },
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        server_fixture.url("/webhook"),
        data=body,
        method="POST",
        headers={
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(gh_secret, body),
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": "delivery-metric-1",
        },
    )
    with urllib.request.urlopen(req, timeout=2) as resp:
        assert resp.status == 200

    with urllib.request.urlopen(server_fixture.url("/metrics"), timeout=2) as _metrics_resp:
        metrics = _metrics_resp.read().decode("utf-8")
    assert 'waitbus_webhook_received_total{path="/webhook"} 1' in metrics
    assert ('waitbus_db_inserted_total{event_type="workflow_run",ingest_method="webhook",source="github"} 1') in metrics


def test_metrics_endpoint_records_dedup_collision(
    server_fixture: ServerHandle, gh_secret: bytes, hmac_sig: Callable[[bytes, bytes], str]
) -> None:
    """Replaying the same delivery_id increments dedup_ignored_total but
    NOT db_inserted_total a second time.
    """
    payload = {"repository": {"name": "r", "owner": {"login": "o"}}, "workflow_run": {"id": 1}}
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Length": str(len(body)),
        "X-Hub-Signature-256": hmac_sig(gh_secret, body),
        "X-GitHub-Event": "workflow_run",
        "X-GitHub-Delivery": "metric-dedup",
    }
    for _ in range(3):
        req = urllib.request.Request(
            server_fixture.url("/webhook"),
            data=body,
            method="POST",
            headers=headers,
        )
        with urllib.request.urlopen(req, timeout=2):
            pass

    with urllib.request.urlopen(server_fixture.url("/metrics"), timeout=2) as _metrics_resp:
        metrics = _metrics_resp.read().decode("utf-8")
    # Exactly one insert, two dedup-ignored.
    assert ('waitbus_db_inserted_total{event_type="workflow_run",ingest_method="webhook",source="github"} 1') in metrics
    assert (
        'waitbus_db_dedup_ignored_total{event_type="workflow_run",ingest_method="webhook",source="github"} 2'
    ) in metrics


def test_metrics_endpoint_records_hmac_rejection(
    server_fixture: ServerHandle, gh_secret: bytes, hmac_sig: Callable[[bytes, bytes], str]
) -> None:
    body = b'{"x":1}'
    # Sign with a wrong secret to produce a mismatch.
    req = urllib.request.Request(
        server_fixture.url("/webhook"),
        data=body,
        method="POST",
        headers={
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(b"wrong-secret", body),
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": "d-hmac",
        },
    )
    try:
        urllib.request.urlopen(req, timeout=2)
    except urllib.request.HTTPError as exc:
        # HTTPError holds a SpooledTemporaryFile internally on Python 3.14+;
        # closing it eagerly prevents the ResourceWarning that fires when
        # the tempfile finalizer runs at GC time.
        with exc:
            assert exc.code == 401

    with urllib.request.urlopen(server_fixture.url("/metrics"), timeout=2) as _metrics_resp:
        metrics = _metrics_resp.read().decode("utf-8")
    assert ('waitbus_webhook_hmac_rejected_total{path="/webhook",reason="mismatch"} 1') in metrics


# ---------------------------------------------------------------------------
# Histogram primitives
# ---------------------------------------------------------------------------


def test_histogram_buckets_monotonically_increase() -> None:
    """Each cumulative bucket count must be >= the previous bucket count."""
    h = _metrics.Histogram(
        "test_hist_mono",
        buckets=(0.1, 0.5, 1.0, 5.0),
        help_text="monotonicity test",
    )
    for v in (0.05, 0.2, 0.7, 2.0):
        h.observe(v)
    # Render and extract bucket lines.
    lines = h.render()
    bucket_lines = [ln for ln in lines if "_bucket" in ln and "le=" in ln]
    counts = []
    for ln in bucket_lines:
        count = int(float(ln.split(" ")[-1]))
        counts.append(count)
    for i in range(1, len(counts)):
        assert counts[i] >= counts[i - 1], f"bucket[{i}]={counts[i]} < bucket[{i - 1}]={counts[i - 1]}"


def test_histogram_sum_matches_observations() -> None:
    """_sum must equal the arithmetic sum of all observed values."""
    h = _metrics.Histogram(
        "test_hist_sum",
        buckets=(1.0, 2.0, 5.0),
        help_text="sum test",
    )
    h.observe(1.0)
    h.observe(2.0)
    h.observe(3.5)
    lines = h.render()
    sum_line = next(ln for ln in lines if ln.startswith("test_hist_sum_sum"))
    actual_sum = float(sum_line.split(" ")[-1])
    assert abs(actual_sum - 6.5) < 1e-9


def test_histogram_count_matches_observation_count() -> None:
    """_count must equal the number of observe() calls."""
    h = _metrics.Histogram(
        "test_hist_count",
        buckets=(1.0,),
        help_text="count test",
    )
    for _ in range(7):
        h.observe(0.5)
    lines = h.render()
    count_line = next(ln for ln in lines if ln.startswith("test_hist_count_count"))
    assert int(float(count_line.split(" ")[-1])) == 7


def test_histogram_renders_prometheus_format() -> None:
    """render() output must include the standard Prometheus histogram lines."""
    h = _metrics.Histogram(
        "test_hist_fmt",
        buckets=(0.01, 0.1, 1.0),
        help_text="format test",
    )
    h.observe(0.05)
    output = "\n".join(h.render())
    assert "# TYPE test_hist_fmt histogram" in output
    assert 'test_hist_fmt_bucket{le="0.01"}' in output
    assert 'test_hist_fmt_bucket{le="+Inf"}' in output
    assert "test_hist_fmt_sum" in output
    assert "test_hist_fmt_count" in output


def test_histogram_labelnames_at_construction_observe_with_labels() -> None:
    """Histogram declared with labelnames at construction observes per-label-value
    samples; each labelled observation is segregated by the label tuple.
    """
    h = _metrics.Histogram(
        "test_hist_labelled",
        buckets=(0.1, 1.0),
        labelnames=("endpoint",),
        help_text="labelled histogram",
    )
    h.observe(0.05, endpoint="webhook")
    h.observe(0.2, endpoint="webhook")
    h.observe(0.5, endpoint="poll")
    output = _metrics.render().decode()
    # endpoint="webhook" count == 2; endpoint="poll" count == 1.
    assert 'test_hist_labelled_count{endpoint="webhook"} 2.0' in output
    assert 'test_hist_labelled_count{endpoint="poll"} 1.0' in output
    # Sum is per-label-set too.
    assert 'test_hist_labelled_sum{endpoint="webhook"} 0.25' in output


def test_histogram_labelled_observation_rejects_unknown_label_keys() -> None:
    """observe() with a kwarg not in the declared labelnames raises ValueError.

    Regression-fence: the prior unregister-and-rebuild path silently absorbed
    unknown label keys instead of raising; this test ensures the strict
    labelnames-at-construction path rejects them.
    """
    h = _metrics.Histogram(
        "test_hist_strict",
        buckets=(1.0,),
        labelnames=("path",),
        help_text="strict label test",
    )
    with pytest.raises(ValueError):
        h.observe(0.5, wrong_label="x")


def test_histogram_unlabelled_construction_rejects_label_kwargs() -> None:
    """An unlabelled Histogram (labelnames=()) raises when given any kwarg."""
    h = _metrics.Histogram(
        "test_hist_unlabelled",
        buckets=(1.0,),
        help_text="unlabelled histogram",
    )
    with pytest.raises(ValueError):
        h.observe(0.5, anything="x")


# ---------------------------------------------------------------------------
# Gauge primitives
# ---------------------------------------------------------------------------


def test_gauge_set_replaces_value() -> None:
    """set() must overwrite any previous value."""
    g = _metrics.Gauge("test_gauge_set", help_text="set test")
    g.set(5)
    g.set(7)
    assert g.value() == 7.0


def test_gauge_inc_adds_to_value() -> None:
    """Successive inc() calls must accumulate."""
    g = _metrics.Gauge("test_gauge_inc", help_text="inc test")
    g.set(0)
    g.inc(2)
    g.inc(3)
    assert g.value() == 5.0


def test_gauge_dec_subtracts_from_value() -> None:
    """dec() must reduce the gauge value."""
    g = _metrics.Gauge("test_gauge_dec", help_text="dec test")
    g.set(10)
    g.dec(3)
    assert g.value() == 7.0


def test_gauge_renders_prometheus_format() -> None:
    """render() output must include a '# TYPE ... gauge' line."""
    g = _metrics.Gauge("test_gauge_fmt", help_text="format test")
    g.set(42)
    output = "\n".join(g.render())
    assert "# TYPE test_gauge_fmt gauge" in output
    # Parse to verify value semantically (native prometheus_client emits 42.0).
    samples = _parse_samples(_metrics.render())
    gauge_samples = [v for name, labels, v in samples if name == "test_gauge_fmt"]
    assert gauge_samples == [42.0], f"unexpected gauge samples: {gauge_samples}"


def test_gauge_labelnames_at_construction_set_inc_dec_with_labels() -> None:
    """Gauge declared with labelnames at construction segregates set/inc/dec
    by label tuple; each label-value-set tracks its own current value.
    """
    g = _metrics.Gauge(
        "test_gauge_labelled",
        labelnames=("zone",),
        help_text="labelled gauge",
    )
    g.set(3, zone="a")
    g.set(7, zone="b")
    g.inc(2, zone="a")  # zone=a now 5
    g.dec(1, zone="b")  # zone=b now 6
    output = _metrics.render().decode()
    assert 'test_gauge_labelled{zone="a"} 5.0' in output
    assert 'test_gauge_labelled{zone="b"} 6.0' in output


def test_gauge_labelled_mutation_rejects_unknown_label_keys() -> None:
    """set/inc/dec with a kwarg not in declared labelnames raises ValueError.

    Regression-fence: mirrors the Histogram test above for the Gauge path.
    """
    g = _metrics.Gauge(
        "test_gauge_strict",
        labelnames=("zone",),
        help_text="strict label gauge",
    )
    with pytest.raises(ValueError):
        g.set(1, wrong_label="x")


def test_gauge_unlabelled_construction_rejects_label_kwargs() -> None:
    """An unlabelled Gauge (labelnames=()) raises when given any kwarg."""
    g = _metrics.Gauge("test_gauge_unlabelled", help_text="unlabelled gauge")
    with pytest.raises(ValueError):
        g.set(1, anything="x")


# ---------------------------------------------------------------------------
# Thread-safety regression: concurrent incr() against pre-registered counters
# ---------------------------------------------------------------------------


def test_incr_under_20_threads_does_not_race() -> None:
    """Twenty threads incrementing the same counter concurrently produce the
    exact expected total and never raise ``Duplicated timeseries`` or related
    registration races.

    Regression-fence for the concurrent-registration race. The prior lazy-registration
    hot path looked up ``_COUNTERS[name]`` and ran ``pc.Counter(...)`` if absent;
    concurrent first incrementers could both miss the lookup and the second
    registration would raise ``ValueError: Duplicated timeseries in CollectorRegistry``.
    Under declare-at-import every counter is pre-registered at module load, so the
    hot path is a pure dict read and there is no race window. This test pins
    that invariant.
    """
    import threading

    _metrics.reset()
    barrier = threading.Barrier(20)
    errors: list[BaseException] = []

    def worker(per_thread: int = 50) -> None:
        try:
            barrier.wait(timeout=5.0)
            for _ in range(per_thread):
                _metrics.incr("waitbus_webhook_received_total", path="/webhook")
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors, f"concurrent incr raised: {errors[:3]!r}"
    # 20 threads x 50 increments = 1000 total.
    assert _metrics.get("waitbus_webhook_received_total", path="/webhook") == 1000


def test_subscriber_count_gauge_is_registered() -> None:
    """SUBSCRIBER_COUNT must be present at module level with the correct metadata."""
    assert hasattr(_metrics, "SUBSCRIBER_COUNT"), "SUBSCRIBER_COUNT not found in waitbus._metrics"
    g = _metrics.SUBSCRIBER_COUNT
    assert isinstance(g, _metrics.Gauge), f"expected Gauge instance, got {type(g)!r}"
    assert g._name == "waitbus_subscriber_count", f"unexpected metric name: {g._name!r}"
    help_text = _metrics._HELP[g._name]
    assert "subscriber" in help_text.lower(), f"help text does not mention subscribers: {help_text!r}"


# ---------------------------------------------------------------------------
# New module-level metric instances (wire rewrite)
# ---------------------------------------------------------------------------


def test_broadcast_send_seconds_is_registered() -> None:
    """BROADCAST_SEND_SECONDS must be present at module level with the expected
    name and bucket boundaries.
    """
    assert hasattr(_metrics, "BROADCAST_SEND_SECONDS"), "BROADCAST_SEND_SECONDS not found in waitbus._metrics"
    h = _metrics.BROADCAST_SEND_SECONDS
    assert isinstance(h, _metrics.Histogram), f"expected Histogram instance, got {type(h)!r}"
    assert h._name == "waitbus_broadcast_send_seconds", f"unexpected metric name: {h._name!r}"
    expected_buckets = (0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1)
    assert h._buckets == expected_buckets, f"unexpected bucket boundaries: {h._buckets!r}"
    help_text = _metrics._HELP[h._name]
    assert "broadcast" in help_text.lower() or "latency" in help_text.lower(), (
        f"help text does not describe broadcast send latency: {help_text!r}"
    )


def test_watermark_replay_events_total_is_registered() -> None:
    """WATERMARK_REPLAY_EVENTS_TOTAL must be present at module level with the
    expected name and help text.
    """
    assert hasattr(_metrics, "WATERMARK_REPLAY_EVENTS_TOTAL"), (
        "WATERMARK_REPLAY_EVENTS_TOTAL not found in waitbus._metrics"
    )
    c = _metrics.WATERMARK_REPLAY_EVENTS_TOTAL
    assert isinstance(c, _metrics.Counter), f"expected Counter instance, got {type(c)!r}"
    assert c._name == "waitbus_watermark_replay_events_total", f"unexpected metric name: {c._name!r}"
    assert "replay" in _metrics._HELP.get(c._name, "").lower(), (
        f"help text does not mention replay: {_metrics._HELP.get(c._name)!r}"
    )


# ---------------------------------------------------------------------------
# Per-subscriber lag / emission-latency gauges
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("attr", "name", "help_substr"),
    [
        (
            "BROADCAST_SUBSCRIPTION_COUNT",
            "waitbus_broadcast_subscription_count",
            "subscriber",
        ),
        (
            "BROADCAST_EMISSION_LATENCY_SECONDS",
            "waitbus_broadcast_emission_latency_seconds",
            "latency",
        ),
        (
            "BROADCAST_STALE_SUBSCRIPTION_COUNT",
            "waitbus_broadcast_stale_subscription_count",
            "lag",
        ),
    ],
)
def test_per_subscriber_lag_gauge_is_registered(attr: str, name: str, help_substr: str) -> None:
    """The three subscription-health gauges must be module-level Gauges
    with the expected names and descriptive help text.
    """
    assert hasattr(_metrics, attr), f"{attr} not found in waitbus._metrics"
    g = getattr(_metrics, attr)
    assert isinstance(g, _metrics.Gauge), f"expected Gauge instance for {attr}, got {type(g)!r}"
    assert g._name == name, f"unexpected metric name for {attr}: {g._name!r}"
    help_text = _metrics._HELP[g._name]
    assert help_substr in help_text.lower(), f"help text for {attr} missing {help_substr!r}: {help_text!r}"


def test_per_subscriber_lag_gauges_set_and_read_back() -> None:
    """The new gauges accept set() and round-trip through the registry."""
    _metrics.BROADCAST_SUBSCRIPTION_COUNT.set(3)
    _metrics.BROADCAST_EMISSION_LATENCY_SECONDS.set(0.012)
    _metrics.BROADCAST_STALE_SUBSCRIPTION_COUNT.set(1)
    assert _metrics.BROADCAST_SUBSCRIPTION_COUNT.value() == 3.0
    assert _metrics.BROADCAST_EMISSION_LATENCY_SECONDS.value() == pytest.approx(0.012)
    assert _metrics.BROADCAST_STALE_SUBSCRIPTION_COUNT.value() == 1.0


@pytest.mark.parametrize(
    "name",
    [
        "waitbus_subscriber_opened_total",
        "waitbus_subscriber_closed_total",
        "waitbus_broadcast_events_emitted_total",
        "waitbus_broadcast_events_delivered_total",
    ],
)
def test_lifecycle_counter_is_declared_unlabelled(name: str) -> None:
    """The subscription-lifecycle counters are declared unlabelled with help text."""
    assert _metrics._LABEL_NAMES[name] == ()
    assert len(_metrics._HELP[name]) > 10
    body = _metrics.render().decode("utf-8")
    assert f"# TYPE {name} counter" in body


@pytest.mark.parametrize(
    "name",
    [
        "waitbus_subscriber_opened_total",
        "waitbus_subscriber_closed_total",
        "waitbus_broadcast_events_emitted_total",
        "waitbus_broadcast_events_delivered_total",
    ],
)
def test_lifecycle_counter_incr_get_roundtrip(name: str) -> None:
    """incr() / get() round-trips for each unlabelled lifecycle counter."""
    assert _metrics.get(name) == 0
    _metrics.incr(name)
    _metrics.incr(name, 2)
    assert _metrics.get(name) == 3


@pytest.mark.parametrize(
    ("attr", "name", "help_substr"),
    [
        ("SUBSCRIBER_LAG_MAX", "waitbus_subscriber_lag_max", "lag"),
        ("SUBSCRIBER_TX_BUFFER_BYTES", "waitbus_subscriber_tx_buffer_bytes", "buffer"),
    ],
)
def test_backlog_gauge_is_registered(attr: str, name: str, help_substr: str) -> None:
    """The aggregate backlog gauges are module-level Gauges with help text."""
    g = getattr(_metrics, attr)
    assert isinstance(g, _metrics.Gauge)
    assert g._name == name
    assert help_substr in _metrics._HELP[name].lower()


def test_backlog_gauges_set_and_read_back() -> None:
    """The backlog gauges accept set() and round-trip through the registry."""
    _metrics.SUBSCRIBER_LAG_MAX.set(4)
    _metrics.SUBSCRIBER_TX_BUFFER_BYTES.set(1024)
    assert _metrics.SUBSCRIBER_LAG_MAX.value() == 4.0
    assert _metrics.SUBSCRIBER_TX_BUFFER_BYTES.value() == 1024.0
