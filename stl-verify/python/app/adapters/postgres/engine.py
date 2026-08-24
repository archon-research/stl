import logging

import asyncpg
from sqlalchemy import event
from sqlalchemy.engine.interfaces import ExceptionContext
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import Settings

logger = logging.getLogger(__name__)

# The SQLSTATE class-25 errors that mean the server-side backend is gone
# (pooler shed it mid-transaction, or the server timed it out) rather than the
# application misusing a healthy transaction. Deliberately excludes 25P02
# (aborted transaction: an app error on a healthy connection) and 25006
# (read-only: a failover state) — classifying those as disconnects would turn
# an ordinary error, or every write during a failover, into pool-wide churn.
_STALE_TRANSACTION_STATE_ERRORS = (
    asyncpg.exceptions.NoActiveSQLTransactionError,  # 25P01
    asyncpg.exceptions.IdleInTransactionSessionTimeoutError,  # 25P03
    asyncpg.exceptions.TransactionTimeoutError,  # 25P04
)


def create_db_engine(settings: Settings) -> AsyncEngine:
    """Create an async SQLAlchemy engine with the configured connection pool.

    The caller owns the engine's lifecycle (the FastAPI lifespan disposes it on
    shutdown), so this deliberately returns a fresh engine per call rather than
    caching one for the process.
    """
    engine = create_async_engine(
        settings.async_database_url,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle_seconds,
        # One setting feeds both caches; see Settings.db_statement_cache_size.
        connect_args={
            "statement_cache_size": settings.db_statement_cache_size,
            "prepared_statement_cache_size": settings.db_statement_cache_size,
        },
    )
    event.listen(engine.sync_engine, "handle_error", mark_stale_transaction_state_as_disconnect)
    return engine


def mark_stale_transaction_state_as_disconnect(context: ExceptionContext) -> None:
    """Retire a connection whose transaction state desynced from the server.

    When a transaction-mode pooler sheds a backend mid-transaction, the asyncpg
    client still believes its transaction is open and issues SAVEPOINT on the
    next statement, which the replacement backend rejects (25P01). Such a
    connection fails every real query yet still answers the pre-ping, so
    without this it returns to the pool and poisons request after request.
    Marking the error a disconnect makes SQLAlchemy invalidate the connection —
    and, via its disconnect handling, recycle pool members older than the
    failure — so the next checkout starts from a fresh connection. The error
    itself still propagates to the caller unchanged.
    """
    if context.is_disconnect:
        return
    exception: BaseException | None = context.original_exception
    # __cause__ only: the dialect raises `from error`; walking __context__
    # would over-match errors merely raised while handling a class-25 one.
    while exception is not None:
        if isinstance(exception, _STALE_TRANSACTION_STATE_ERRORS):
            logger.warning(
                "Invalidating DB connection with stale transaction state (sqlstate=%s): %s",
                getattr(exception, "sqlstate", "unknown"),
                exception,
            )
            context.is_disconnect = True
            return
        exception = exception.__cause__
