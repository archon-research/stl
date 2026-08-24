import logging

import asyncpg
from sqlalchemy import event
from sqlalchemy.engine.interfaces import ExceptionContext
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

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


def create_db_engine(
    url: str,
    *,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    pool_timeout: float | None = None,
    pool_recycle: int | None = None,
    statement_cache_size: int = 0,
) -> AsyncEngine:
    """The one engine factory for every process, keyed by an explicit URL.

    The API's composition root passes ``settings.async_database_url`` plus the
    Settings-sized pool bounds; workers pass the URL straight from their entry
    point's environment (a missing var must fail loudly, where Settings would
    silently fall back to .env.default's localhost URL) and keep SQLAlchemy's
    pool defaults — a tick holds few connections. pool_pre_ping is
    unconditional: worker ticks can be hours apart, far past the pooler's idle
    timeout. Prepared-statement caching is off unless a caller opts in: every
    staging/prod DATABASE_URL is the transaction-mode TigerData pooler, where
    a cached statement can execute on a backend that never prepared it (why:
    ``Settings.db_statement_cache_size``). The caller owns the engine's
    lifecycle.
    """
    pool_kwargs: dict = {}
    if pool_size is not None:
        pool_kwargs["pool_size"] = pool_size
    if max_overflow is not None:
        pool_kwargs["max_overflow"] = max_overflow
    if pool_timeout is not None:
        pool_kwargs["pool_timeout"] = pool_timeout
    if pool_recycle is not None:
        pool_kwargs["pool_recycle"] = pool_recycle
    engine = create_async_engine(
        url,
        pool_pre_ping=True,
        # One value feeds both caches; see Settings.db_statement_cache_size.
        connect_args={
            "statement_cache_size": statement_cache_size,
            "prepared_statement_cache_size": statement_cache_size,
        },
        **pool_kwargs,
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
