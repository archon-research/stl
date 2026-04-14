from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

from app.middleware.request_id import RequestIdMiddleware, get_request_id
from app.telemetry import instrument_sqlalchemy_engine, setup_telemetry


class _CollectingExporter(SpanExporter):
    """Test exporter that collects finished spans in a list."""

    def __init__(self):
        self.spans = []

    def export(self, spans):
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass


@pytest.fixture(autouse=True)
def _reset_tracer_provider():
    """Ensure OTel global state is clean between tests."""
    original_provider = trace._TRACER_PROVIDER
    original_done = trace._TRACER_PROVIDER_SET_ONCE._done
    yield
    trace._TRACER_PROVIDER = original_provider
    trace._TRACER_PROVIDER_SET_ONCE._done = original_done


def _make_settings(**overrides):
    from app.config import Settings

    defaults = {
        "otel_enabled": False,
        "otel_exporter_otlp_endpoint": "http://localhost:4317",
        "otel_service_name": "test-service",
    }
    defaults.update(overrides)
    return Settings(**defaults)


def test_setup_telemetry_noop_when_disabled():
    app = FastAPI()
    settings = _make_settings(otel_enabled=False)
    setup_telemetry(app, settings)
    provider = trace.get_tracer_provider()
    assert not isinstance(provider, TracerProvider)


def test_setup_telemetry_configures_provider_when_enabled():
    app = FastAPI()
    settings = _make_settings(otel_enabled=True)
    setup_telemetry(app, settings)
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    provider.shutdown()


def test_instrument_sqlalchemy_engine_noop_when_no_provider():
    engine = MagicMock()
    instrument_sqlalchemy_engine(engine)


@pytest.mark.asyncio
async def test_request_id_appears_on_span():
    """End-to-end: middleware sets request_id on the active OTel span."""
    exporter = _CollectingExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    app = FastAPI()
    app.add_middleware(RequestIdMiddleware)

    @app.get("/probe")
    async def probe():
        return {"request_id": get_request_id()}

    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/probe")

    assert response.status_code == 200
    header_rid = response.headers.get("x-request-id")
    assert header_rid is not None

    provider.force_flush()
    spans = exporter.spans
    server_spans = [s for s in spans if s.name == "GET /probe"]
    assert len(server_spans) == 1
    assert server_spans[0].attributes.get("request_id") == header_rid

    FastAPIInstrumentor().uninstrument_app(app)
    provider.shutdown()
