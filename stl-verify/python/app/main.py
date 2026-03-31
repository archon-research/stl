from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.api.v1 import allocations, risk, status
from app.config import Settings, get_settings


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.database_url.get_secret_value(), pool_pre_ping=True)
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        app.state.engine = engine
        async with httpx.AsyncClient() as http_client:
            app.state.http_client = http_client
            yield
        await engine.dispose()

    application = FastAPI(title="stl-verify", lifespan=lifespan)
    application.include_router(status.router, prefix="/v1")
    application.include_router(allocations.router, prefix="/v1")
    application.include_router(risk.router, prefix="/v1")
    return application


app = create_app(get_settings())
