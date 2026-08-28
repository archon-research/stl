"""OpenTelemetry tracing and metrics configuration."""

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app.config import Settings


def setup_telemetry(app, settings: Settings) -> tuple[TracerProvider, MeterProvider] | None:
    """Configure OTel tracing and metrics if enabled."""
    if not settings.otel_enabled:
        return None

    resource = Resource.create({"service.name": settings.otel_service_name})
    endpoint = settings.otel_exporter_otlp_endpoint

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)

    reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=endpoint))
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meter_provider)

    FastAPIInstrumentor.instrument_app(app)
    return provider, meter_provider


def shutdown_telemetry(providers: tuple[TracerProvider, MeterProvider] | None) -> None:
    """Flush and shut down telemetry providers. No-op for None."""
    if providers is None:
        return
    tracer_provider, meter_provider = providers
    meter_provider.shutdown()
    tracer_provider.shutdown()


def instrument_sqlalchemy_engine(engine) -> None:
    """Instrument a SQLAlchemy engine for tracing.

    Call this after the engine is created (e.g. in lifespan).
    Only instruments if OTel tracing is active (i.e. a TracerProvider has been set).
    """
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        return
    sync_engine = getattr(engine, "sync_engine", engine)
    SQLAlchemyInstrumentor().instrument(engine=sync_engine)
