import functools

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import Settings


@functools.lru_cache(maxsize=1)
def get_engine(settings: Settings) -> AsyncEngine:
    """Create or return a cached async SQLAlchemy engine for the given settings."""
    return create_async_engine(settings.database_url.get_secret_value(), pool_pre_ping=True)
