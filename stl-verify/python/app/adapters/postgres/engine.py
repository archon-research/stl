import functools

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@functools.lru_cache
def get_engine(database_url: str) -> AsyncEngine:
    """Create or return a cached async SQLAlchemy engine for the given URL."""
    return create_async_engine(database_url)
