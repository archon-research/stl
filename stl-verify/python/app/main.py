from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.adapters.postgres.engine import get_engine
from app.api.v1 import allocations, status
from app.config import get_settings


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    yield
    # Dispose the database engine on shutdown
    settings = get_settings()
    engine = get_engine(settings.database_url.get_secret_value())
    await engine.dispose()


app = FastAPI(title="stl-verify", lifespan=lifespan)

app.include_router(status.router, prefix="/v1")
app.include_router(allocations.router, prefix="/v1")
