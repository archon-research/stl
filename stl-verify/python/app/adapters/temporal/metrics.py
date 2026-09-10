"""OTel run metrics for the shared Python Temporal cronjob harness.

Mirrors the Go harness's cronjob.runs.total / cronjob.run.duration_seconds
instruments (internal/adapters/outbound/temporal/metrics.go) so the same
alerts (stl/alerts/vector-cronjobs.yaml) cover a Python cronjob the moment it
ships, with no per-job wiring: `service_name=<cronjob name>` is set once, on
the OTel Resource at MeterProvider construction, and every series inherits
it.
"""

import logging
import os
from collections.abc import Callable

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

logger = logging.getLogger(__name__)

OTEL_EXPORTER_OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"

_INSTRUMENTATION_NAME = "app.adapters.temporal.cronjob"
_EXPORT_INTERVAL_MILLIS = 15_000
# Bounds the final flush the way the Go harness's InitOTEL does (10s) so an
# unreachable collector at pod-termination time cannot ride the SDK's default
# 30s timeout past the k8s terminationGracePeriodSeconds budget.
_SHUTDOWN_TIMEOUT_MILLIS = 10_000

# The terminal statuses this harness records, matching the Go harness's
# runStatusValues (metrics.go) -- kept in one place so CronjobMetrics's
# zero-seeding (__init__) stays in step with what interceptor.py classifies.
_RUN_STATUS_VALUES = ("success", "error", "canceled")

# Same boundaries as the Go harness's telemetry.SecondsDurationBuckets
# (internal/pkg/telemetry/meter.go): sub-second resolution where a cronjob
# tick normally lands, coarse buckets out to 300s for the slower ones. Kept
# in step by hand -- there is no shared source between the two languages.
_SECONDS_DURATION_BUCKETS = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1,
    2.5,
    5,
    10,
    30,
    60,
    120,
    300,
)


def init_metrics_provider(service_name: str) -> Callable[[], None]:
    """Install a MeterProvider resourced to service_name; return its shutdown.

    Without OTEL_EXPORTER_OTLP_ENDPOINT set (unit tests, a local run with no
    collector) this leaves the OTel API's default no-op MeterProvider in
    place -- CronjobMetrics still constructs and records against it, just to
    nowhere -- matching the Go harness's InitMetrics no-op path. Must run
    before CronjobMetrics() is constructed: instrument creation binds to
    whichever MeterProvider is global at that moment.
    """
    endpoint = os.environ.get(OTEL_EXPORTER_OTLP_ENDPOINT_ENV, "")
    if not endpoint:
        logger.warning("%s is not set; cronjob run metrics are NOT exported anywhere", OTEL_EXPORTER_OTLP_ENDPOINT_ENV)
        return lambda: None

    resource = Resource.create({"service.name": service_name})
    exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=_EXPORT_INTERVAL_MILLIS)
    # shutdown_on_exit=False: the caller owns shutdown explicitly (see the
    # returned function below), matching run_cronjob's signal-driven
    # lifecycle -- an atexit hook would fire independently of it, in
    # unspecified order relative to the worker's own graceful stop.
    provider = MeterProvider(resource=resource, metric_readers=[reader], shutdown_on_exit=False)
    metrics.set_meter_provider(provider)
    logger.info("cronjob run metrics initialized service_name=%s endpoint=%s", service_name, endpoint)

    def shutdown() -> None:
        # provider.shutdown() raises if a reader's own shutdown fails (e.g.
        # the collector is unreachable during pod termination) -- best-effort
        # here, matching the Go harness's InitOTEL shutdown, so a flush
        # failure logs instead of replacing whatever exception sent the
        # caller's own `finally` down this path (see interceptor.py's
        # matching guard on record_run for why that replacement is a bug).
        try:
            provider.shutdown(timeout_millis=_SHUTDOWN_TIMEOUT_MILLIS)
        except Exception:
            logger.warning("failed to shut down cronjob metrics provider", exc_info=True)

    return shutdown


class CronjobMetrics:
    """Records the outcome of every cronjob activity execution.

    Built against meter_provider, defaulting to whatever MeterProvider is
    global at construction time -- call init_metrics_provider(service_name)
    first so the default binds to a resource carrying service_name rather
    than the no-op provider. Tests pass their own provider directly: the
    OTel API only accepts one process-wide global (a second
    set_meter_provider call is a silent no-op with a warning), so a real
    provider under test cannot go through the global.
    """

    def __init__(self, meter_provider: metrics.MeterProvider | None = None) -> None:
        provider = meter_provider if meter_provider is not None else metrics.get_meter_provider()
        meter = provider.get_meter(_INSTRUMENTATION_NAME)
        self._runs_total = meter.create_counter(
            "cronjob.runs.total",
            description='Total cronjob runs, labelled by terminal status ("success"|"error"|"canceled")',
        )
        self._run_duration = meter.create_histogram(
            "cronjob.run.duration_seconds",
            unit="s",
            description="Duration of a cronjob run in seconds",
            explicit_bucket_boundaries_advisory=_SECONDS_DURATION_BUCKETS,
        )
        self._seed_status_series()

    def _seed_status_series(self) -> None:
        """Export every status series at 0 before any run lands.

        Mirrors the Go harness's seedStatusSeries (metrics.go): without this,
        e.g. {status="success"} does not exist as a series until the first
        success ever happens, and increase()/rate() cannot see a series's
        0->1 appearance -- only a later delta between two samples of an
        already-existing series. VectorCronjobAllRunsFailing (the page) reads
        `sum(...status="success"...) == 0`, which an absent series also fails
        to match, so a cronjob that errors on every run since its very first
        tick would never page without this seed.
        """
        for status in _RUN_STATUS_VALUES:
            self._runs_total.add(0, {"status": status})

    def record_run(self, duration_seconds: float, status: str) -> None:
        """Record one run's terminal status and duration.

        status must be one of _RUN_STATUS_VALUES; interceptor.py's
        RunMetricsInterceptor is the only caller and owns the classification.
        """
        attributes = {"status": status}
        self._runs_total.add(1, attributes)
        self._run_duration.record(duration_seconds, attributes)
