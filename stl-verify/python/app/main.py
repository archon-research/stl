from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.api.v1 import allocations, risk, status
from app.config import Settings, get_settings
from app.logging import setup_logging
from app.middleware.request_id import RequestIdMiddleware
from app.telemetry import instrument_sqlalchemy_engine, setup_telemetry, shutdown_telemetry


def create_app(settings: Settings) -> FastAPI:
    setup_logging(log_level=settings.log_level, log_format=settings.log_format)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.async_database_url, pool_pre_ping=True)
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        app.state.engine = engine
        instrument_sqlalchemy_engine(engine)
        async with httpx.AsyncClient() as http_client:
            app.state.http_client = http_client
            yield
        try:
            await engine.dispose()
        finally:
            shutdown_telemetry(app.state.tracer_provider)

    application = FastAPI(title="stl-verify", lifespan=lifespan)
    application.add_middleware(RequestIdMiddleware)
    application.state.tracer_provider = setup_telemetry(application, settings)
    application.include_router(status.router, prefix="/v1")
    application.include_router(allocations.router, prefix="/v1")
    application.include_router(risk.router, prefix="/v1")
    return application


app = create_app(get_settings())
