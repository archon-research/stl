"""OpenTelemetry tracing configuration."""

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app.config import Settings


def setup_telemetry(app, settings: Settings) -> TracerProvider | None:
    """Configure OTel tracing if enabled. Returns the provider for shutdown."""
    if not settings.otel_enabled:
        return None

    resource = Resource.create({"service.name": settings.otel_service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)
    return provider


def shutdown_telemetry() -> None:
    """Flush and shut down the tracer provider if active."""
    provider = trace.get_tracer_provider()
    if isinstance(provider, TracerProvider):
        provider.shutdown()


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
