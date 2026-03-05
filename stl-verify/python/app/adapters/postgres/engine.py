import functools

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import Settings


@functools.lru_cache(maxsize=1)
def get_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(settings.database_url, pool_pre_ping=True)

@functools.lru_cache
def get_engine(database_url: str) -> AsyncEngine:
    """Create or return a cached async SQLAlchemy engine for the given URL."""
    return create_async_engine(database_url)
