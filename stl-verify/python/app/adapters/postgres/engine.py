import asyncio
import logging

import asyncpg
from sqlalchemy import event, text
from sqlalchemy.engine.interfaces import ExceptionContext
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

logger = logging.getLogger(__name__)

# Backoff shape for the first connect. Starts sub-second so a resolver that is
# ready a moment after the container costs no measurable startup time, and caps
# well inside a deadline so the last attempt still lands near it rather than
# sleeping past it. Unlike the deadline these need no per-environment tuning:
# the deadline is what has to fit a given startup probe's budget.
_INITIAL_CONNECT_BACKOFF_SECONDS = 0.5
_MAX_CONNECT_BACKOFF_SECONDS = 5.0

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
    statement_cache_size: int | None = None,
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
    if pool_recycle is not None:
        pool_kwargs["pool_recycle"] = pool_recycle
    if statement_cache_size is not None:
        # One value feeds both caches; see Settings.db_statement_cache_size.
        pool_kwargs["connect_args"] = {
            "statement_cache_size": statement_cache_size,
            "prepared_statement_cache_size": statement_cache_size,
        }
    # hide_parameters: a StatementError renders its bind parameters into its own
    # string, and on a prime-filtered query those are the caller's whole vault
    # allow-list, which the error path then logs. Use _loggable_params instead.
    engine = create_async_engine(url, pool_pre_ping=True, hide_parameters=True, **pool_kwargs)
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


async def wait_for_database(
    engine: AsyncEngine,
    *,
    deadline_seconds: float,
    initial_backoff_seconds: float = _INITIAL_CONNECT_BACKOFF_SECONDS,
    max_backoff_seconds: float = _MAX_CONNECT_BACKOFF_SECONDS,
) -> None:
    """Open the process's first connection, retrying while the host is unreachable.

    A pod's DNS is not always usable the moment its container is: cluster
    resolution can fail for minutes after a node joins, and asyncpg surfaces
    that as EAI_AGAIN out of the very first connect. That single failure ends
    startup, so the container exits and the pod enters CrashLoopBackOff — whose
    backoff grows past the outage it is waiting out, which is what leaves a
    rollout stalled long after resolution recovers. Retrying inside one
    container holds it across the outage instead.

    The deadline belongs inside the startup probe's budget (see
    ``k8s/base/python-api/deployment.yaml``) so a pod that cannot reach its
    database is killed on the probe's verdict, with this function's own error in
    the log, rather than mid-retry. Failures above the socket layer — a rejected
    password, a database that does not exist — are configuration and raise on
    the first attempt.
    """
    loop = asyncio.get_running_loop()
    give_up_at = loop.time() + deadline_seconds
    backoff = initial_backoff_seconds
    attempt = 1
    while True:
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception as error:
            if not _is_unreachable_database(error):
                raise
            remaining = give_up_at - loop.time()
            if remaining <= 0:
                logger.error(
                    "database unreachable after %d attempt(s) over %.1fs, giving up: %s",
                    attempt,
                    deadline_seconds,
                    error,
                )
                raise
            # Never sleep past the deadline: the remaining budget is worth one
            # more shorter attempt, and overshooting would hand the probe a
            # container still sleeping rather than one that has reported why.
            delay = min(backoff, max_backoff_seconds, remaining)
            logger.warning(
                "database unreachable on attempt %d, retrying in %.1fs: %s",
                attempt,
                delay,
                error,
            )
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, max_backoff_seconds)
            attempt += 1
        else:
            if attempt > 1:
                logger.info("database reachable on attempt %d", attempt)
            return


def _is_unreachable_database(error: BaseException) -> bool:
    """Whether the connect failed below the protocol: DNS, routing, refusal, timeout.

    Every such failure reaches here as an ``OSError`` — ``socket.gaierror`` for
    resolution, ``ConnectionRefusedError`` for a server not yet listening,
    ``TimeoutError`` for a black-holed route. asyncpg lets those out of the
    dialect unwrapped, so the first connect usually raises one directly; the
    chain is walked as well because a wrapped one arrives as a ``DBAPIError``
    raised ``from`` it, the same chaining
    ``mark_stale_transaction_state_as_disconnect`` reads.
    """
    cause: BaseException | None = error
    while cause is not None:
        if isinstance(cause, OSError):
            return True
        cause = cause.__cause__
    return False
