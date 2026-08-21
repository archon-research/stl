from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import Settings, async_database_url


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


def create_worker_db_engine(database_url: str) -> AsyncEngine:
    """Engine for workers configured by a bare DATABASE_URL instead of Settings.

    pool_pre_ping because worker ticks can be hours apart — far past the
    pooler's idle timeout, so a cached connection is likely dead by the next
    tick. The caller owns the engine's lifecycle.
    """
    return create_async_engine(async_database_url(database_url), pool_pre_ping=True)
