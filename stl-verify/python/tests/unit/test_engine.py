from typing import cast

import asyncpg
import pytest
from sqlalchemy import event
from sqlalchemy.engine.interfaces import ExceptionContext
from sqlalchemy.pool import QueuePool

from app.adapters.postgres.engine import create_db_engine, mark_stale_transaction_state_as_disconnect
from app.config import Settings


def test_create_db_engine_sizes_the_connection_pool_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """The pool ceiling decides how far a replica gets before callers queue.

    A prime-scoped risk-capital request opens connections concurrently in
    proportion to positions × ALM proxies (see ``Settings.db_pool_size``), so on
    SQLAlchemy's unset defaults (5 + 10 overflow) a replica saturates well inside
    one such request and further callers queue until ``pool_timeout`` turns into a
    500. The ceiling has to be a deliberate number, which means it has to be wired
    through from settings rather than left unset.
    """
    monkeypatch.setenv("DB_POOL_SIZE", "7")
    monkeypatch.setenv("DB_MAX_OVERFLOW", "13")
    settings = Settings.model_validate({})

    engine = create_db_engine(settings)

    # engine.pool is typed as the Pool base class; the sizing accessors live on
    # QueuePool, which the async engine's AsyncAdaptedQueuePool subclasses.
    pool = engine.pool
    assert isinstance(pool, QueuePool)
    assert pool.size() == 7
    # No public accessor for the overflow ceiling; asserted so dropping the kwarg fails.
    assert pool._max_overflow == 13


def test_create_db_engine_bounds_how_long_a_caller_queues_for_a_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhaustion has to fail fast rather than stall a worker for 30s.

    The pool ceiling above decides when a replica saturates; this decides what
    happens next. On SQLAlchemy's unset 30s each queued caller holds a worker for
    half a minute, so one burst on the risk-capital fan-out degrades endpoints
    that never touch this pool. Wired through from settings so the wait is a
    deliberate number and can be tuned per environment without a rebuild.
    """
    monkeypatch.setenv("DB_POOL_TIMEOUT", "3")
    settings = Settings.model_validate({})

    engine = create_db_engine(settings)

    pool = engine.pool
    assert isinstance(pool, QueuePool)
    # No public accessor for the queue wait; asserted so dropping the kwarg fails
    # back to SQLAlchemy's 30s silently.
    assert pool._timeout == 3


def test_create_db_engine_recycles_connections_on_the_configured_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A poisoned connection that keeps passing the pre-ping must still die.

    After a pooler incident a connection can fail every real query while still
    answering the pre-ping (see ``mark_stale_transaction_state_as_disconnect``).
    The recycle interval is the backstop that retires such a connection even
    when the disconnect handling never sees it, so it has to be wired through
    rather than left on SQLAlchemy's -1 (never recycle).
    """
    monkeypatch.setenv("DB_POOL_RECYCLE_SECONDS", "120")
    settings = Settings.model_validate({})

    engine = create_db_engine(settings)

    pool = engine.pool
    assert isinstance(pool, QueuePool)
    # No public accessor; asserted because dropping the kwarg would silently
    # fall back to "never".
    assert pool._recycle == 120


def test_create_db_engine_registers_the_stale_transaction_disconnect_listener() -> None:
    """A tuned engine helper proves nothing unless engines actually go through
    it, so the listener has to be asserted on the factory's output, not merely
    defined next to it.
    """
    settings = Settings.model_validate({})

    engine = create_db_engine(settings)

    assert event.contains(engine.sync_engine, "handle_error", mark_stale_transaction_state_as_disconnect)


class _FakeExceptionContext:
    """Just the two ExceptionContext members the listener reads and writes."""

    def __init__(self, exception: BaseException) -> None:
        self.original_exception = exception
        self.is_disconnect = False


def _shim_wrapped(cause: BaseException) -> BaseException:
    """Mimic the dialect's ``raise translated_error from error`` chaining."""
    wrapper = Exception("<class 'asyncpg.exceptions...'>")
    wrapper.__cause__ = cause
    return wrapper


@pytest.mark.parametrize(
    ("exception", "expected"),
    [
        pytest.param(
            asyncpg.exceptions.NoActiveSQLTransactionError("SAVEPOINT can only be used in transaction blocks"),
            True,
            id="stale-state-error-raised-directly",
        ),
        pytest.param(
            _shim_wrapped(asyncpg.exceptions.NoActiveSQLTransactionError("SAVEPOINT ...")),
            True,
            id="stale-state-error-behind-the-dialect-shim",
        ),
        pytest.param(
            _shim_wrapped(asyncpg.exceptions.IdleInTransactionSessionTimeoutError("terminating connection")),
            True,
            id="server-timed-out-backend-is-a-disconnect",
        ),
        pytest.param(
            _shim_wrapped(asyncpg.exceptions.InFailedSQLTransactionError("current transaction is aborted")),
            False,
            id="aborted-transaction-is-an-app-error-on-a-healthy-connection",
        ),
        pytest.param(
            _shim_wrapped(asyncpg.exceptions.ReadOnlySQLTransactionError("cannot execute UPDATE")),
            False,
            id="read-only-failover-state-must-not-thrash-the-pool",
        ),
        pytest.param(
            _shim_wrapped(asyncpg.exceptions.UniqueViolationError("duplicate key")),
            False,
            id="ordinary-query-errors-stay-non-disconnect",
        ),
    ],
)
def test_stale_transaction_state_is_classified_as_disconnect(exception: BaseException, expected: bool) -> None:
    """Only the class-25 errors that mean the backend is gone may invalidate.

    A desynced or server-terminated connection is unusable and must be retired,
    while errors that arise on a healthy connection (aborted transaction,
    read-only replica, constraint violations) must never tear down the pool.
    """
    context = _FakeExceptionContext(exception)

    mark_stale_transaction_state_as_disconnect(cast(ExceptionContext, context))

    assert context.is_disconnect is expected


def test_an_error_already_classified_as_disconnect_is_left_alone() -> None:
    """The listener may only ever add a disconnect classification, never remove
    one — pinned against a refactor that assigns the match result directly."""
    context = _FakeExceptionContext(Exception("connection is closed"))
    context.is_disconnect = True

    mark_stale_transaction_state_as_disconnect(cast(ExceptionContext, context))

    assert context.is_disconnect is True
