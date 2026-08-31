"""Unit tests for the shared Python Temporal cronjob harness's run metrics.

Mirrors internal/adapters/outbound/temporal/metrics_test.go: the alert rules
in alerts/vector-cronjobs.yaml key on these exact instrument names and label
values, so this pins them.
"""

from collections.abc import Iterator
from typing import cast

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import DataPointT, HistogramDataPoint, InMemoryMetricReader, NumberDataPoint
from opentelemetry.sdk.resources import Resource

from app.adapters.temporal.metrics import _SECONDS_DURATION_BUCKETS, CronjobMetrics


def _points(reader: InMemoryMetricReader, name: str) -> list[DataPointT]:
    data = reader.get_metrics_data()
    assert data is not None
    return [
        point
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
        if metric.name == name
        for point in metric.data.data_points
    ]


def _counts_by_status(reader: InMemoryMetricReader) -> dict[str, float]:
    out: dict[str, float] = {}
    for point in _points(reader, "cronjob.runs.total"):
        assert isinstance(point, NumberDataPoint)
        status = point.attributes["status"] if point.attributes else None
        assert isinstance(status, str)
        out[status] = point.value
    return out


@pytest.fixture
def probe() -> Iterator[tuple[InMemoryMetricReader, CronjobMetrics]]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    yield reader, CronjobMetrics(meter_provider=provider)
    provider.shutdown()


def test_construction_seeds_every_status_series_at_zero(probe):
    # Without this, a series does not exist until its first occurrence, so
    # increase()/rate() cannot see its 0->1 appearance -- see
    # CronjobMetrics._seed_status_series's docstring for the alert this
    # protects (VectorCronjobAllRunsFailing).
    reader, _ = probe

    assert _counts_by_status(reader) == {"success": 0, "error": 0, "canceled": 0}


@pytest.mark.parametrize(
    ("status", "other_statuses"),
    [
        ("success", ("error", "canceled")),
        ("error", ("success", "canceled")),
        ("canceled", ("success", "error")),
    ],
)
def test_record_run_increments_only_its_own_status_series(probe, status, other_statuses):
    reader, metrics = probe

    metrics.record_run(1.0, status)

    got = _counts_by_status(reader)
    assert got[status] == 1
    for other in other_statuses:
        assert got[other] == 0


def test_record_run_records_the_duration_histogram(probe):
    reader, metrics = probe

    metrics.record_run(2.5, "success")

    histogram_points = _points(reader, "cronjob.run.duration_seconds")
    assert len(histogram_points) == 1
    point = histogram_points[0]
    assert isinstance(point, HistogramDataPoint)
    assert point.sum == 2.5
    assert point.attributes is not None
    assert point.attributes["status"] == "success"


def test_duration_histogram_uses_the_go_harness_bucket_boundaries(probe):
    # The boundaries are hand-copied from telemetry.SecondsDurationBuckets;
    # without the advisory the SDK falls back to millisecond-sized defaults
    # and every cronjob duration collapses into the (0, 5] bucket.
    reader, metrics = probe

    metrics.record_run(2.5, "success")

    (point,) = _points(reader, "cronjob.run.duration_seconds")
    assert isinstance(point, HistogramDataPoint)
    assert tuple(point.explicit_bounds) == _SECONDS_DURATION_BUCKETS


def test_init_metrics_provider_is_a_noop_without_an_otlp_endpoint(monkeypatch):
    from app.adapters.temporal import metrics as metrics_module

    monkeypatch.delenv(metrics_module.OTEL_EXPORTER_OTLP_ENDPOINT_ENV, raising=False)
    shutdown = metrics_module.init_metrics_provider("my-cronjob")
    shutdown()  # must not raise


def test_init_metrics_provider_sets_service_name_on_the_resource(monkeypatch):
    # service_name is what every alert in alerts/vector-cronjobs.yaml groups
    # by -- confirm it actually reaches the Resource the exported series
    # inherit it from, not just that construction succeeds.
    from app.adapters.temporal import metrics as metrics_module

    monkeypatch.setenv(metrics_module.OTEL_EXPORTER_OTLP_ENDPOINT_ENV, "http://localhost:1")
    captured: dict[str, object] = {}
    original_init = MeterProvider.__init__

    def _capturing_init(self, *args, **kwargs):
        captured["resource"] = kwargs.get("resource")
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(MeterProvider, "__init__", _capturing_init)

    shutdown = metrics_module.init_metrics_provider("my-cronjob")
    try:
        resource = cast(Resource, captured["resource"])
        assert resource.attributes["service.name"] == "my-cronjob"
    finally:
        shutdown()


def test_shutdown_does_not_raise_when_the_provider_fails_to_flush(monkeypatch):
    # provider.shutdown() raises if a reader's own shutdown fails (e.g. the
    # collector is unreachable at pod-termination time). The returned
    # shutdown must swallow that -- best-effort, matching the Go harness's
    # InitOTEL -- rather than replacing whatever the caller's own `finally`
    # was already unwinding from.
    from app.adapters.temporal import metrics as metrics_module

    monkeypatch.setenv(metrics_module.OTEL_EXPORTER_OTLP_ENDPOINT_ENV, "http://localhost:1")
    monkeypatch.setattr(
        MeterProvider,
        "shutdown",
        lambda self, timeout_millis=30000: (_ for _ in ()).throw(RuntimeError("collector unreachable")),
    )

    shutdown = metrics_module.init_metrics_provider("my-cronjob")

    shutdown()  # must not raise
