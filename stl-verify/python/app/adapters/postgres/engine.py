from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_db_engine(
    url: str,
    *,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    pool_timeout: float | None = None,
) -> AsyncEngine:
    """The one engine factory for every process, keyed by an explicit URL.

    The API's composition root passes ``settings.async_database_url`` plus the
    Settings-sized pool bounds; workers pass the URL straight from their entry
    point's environment (a missing var must fail loudly, where Settings would
    silently fall back to .env.default's localhost URL) and keep SQLAlchemy's
    pool defaults — a tick holds few connections. pool_pre_ping is
    unconditional: worker ticks can be hours apart, far past the pooler's idle
    timeout. The caller owns the engine's lifecycle.
    """
    pool_kwargs: dict = {}
    if pool_size is not None:
        pool_kwargs["pool_size"] = pool_size
    if max_overflow is not None:
        pool_kwargs["max_overflow"] = max_overflow
    if pool_timeout is not None:
        pool_kwargs["pool_timeout"] = pool_timeout
    return create_async_engine(url, pool_pre_ping=True, **pool_kwargs)
