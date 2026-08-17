from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import Settings


def create_db_engine(settings: Settings) -> AsyncEngine:
    """Create an async SQLAlchemy engine with the configured connection pool.

    The caller owns the engine's lifecycle (the FastAPI lifespan disposes it on
    shutdown), so this deliberately returns a fresh engine per call rather than
    caching one for the process.
    """
    return create_async_engine(
        settings.async_database_url,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
    )
