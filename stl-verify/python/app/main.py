from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.api.v1 import allocations, status
from app.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    engine = create_async_engine(settings.database_url.get_secret_value(), pool_pre_ping=True)
    # Verify the database connection before starting the app
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    app.state.engine = engine
    yield
    await engine.dispose()


app = FastAPI(title="stl-verify", lifespan=lifespan)

app.include_router(status.router, prefix="/v1")
app.include_router(allocations.router, prefix="/v1")
