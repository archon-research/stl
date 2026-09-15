import socket
from typing import cast

import asyncpg
import pytest
from sqlalchemy import event
from sqlalchemy.engine.interfaces import ExceptionContext
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import QueuePool

from app.adapters.postgres import engine as engine_module
from app.adapters.postgres.engine import (
    create_db_engine,
    mark_stale_transaction_state_as_disconnect,
    wait_for_database,
)
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

    engine = create_db_engine(
        settings.async_database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
    )

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

    engine = create_db_engine(settings.async_database_url, pool_timeout=settings.db_pool_timeout)

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

    engine = create_db_engine(settings.async_database_url, pool_recycle=settings.db_pool_recycle_seconds)

    pool = engine.pool
    assert isinstance(pool, QueuePool)
    # No public accessor; asserted because dropping the kwarg would silently
    # fall back to "never".
    assert pool._recycle == 120


def test_create_db_engine_keeps_bind_parameters_out_of_error_strings() -> None:
    """A StatementError renders its bind parameters into its own string, and on a
    prime-filtered query those are the caller's whole vault allow-list — which
    the repositories then log. What SQLAlchemy renders once the kwarg is set is
    pinned against a real error in ``tests/integration/test_db_error_redaction``;
    this only guards the kwarg, so dropping it fails here first.
    """
    settings = Settings.model_validate({})

    engine = create_db_engine(settings.async_database_url)

    assert engine.sync_engine.hide_parameters is True


def test_create_db_engine_registers_the_stale_transaction_disconnect_listener() -> None:
    """A tuned engine helper proves nothing unless engines actually go through
    it, so the listener has to be asserted on the factory's output, not merely
    defined next to it.
    """
    settings = Settings.model_validate({})

    engine = create_db_engine(settings.async_database_url)

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


class _FakeConnectEngine:
    """An engine whose ``connect()`` replays a scripted list of outcomes.

    Each entry is either an exception to raise on that attempt or ``None`` for a
    connect that succeeds; ``SELECT 1`` on the yielded connection is a no-op, so
    a test scripts only what ``wait_for_database`` branches on.
    """

    def __init__(self, outcomes: list[BaseException | None]) -> None:
        self._outcomes = list(outcomes)
        self.attempts = 0

    def connect(self) -> "_FakeConnectEngine":
        return self

    async def __aenter__(self) -> "_FakeConnectEngine":
        self.attempts += 1
        outcome = self._outcomes.pop(0) if self._outcomes else None
        if outcome is not None:
            raise outcome
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute(self, _statement: object) -> None:
        return None


def _dns_failure() -> OSError:
    """What asyncpg lets out of the dialect when cluster resolution is not ready."""
    return socket.gaierror(-3, "Temporary failure in name resolution")


@pytest.fixture
def instant_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the backoff delays and return from each sleep immediately."""
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(engine_module.asyncio, "sleep", fake_sleep)
    return delays


async def test_wait_for_database_retries_until_name_resolution_recovers(instant_sleep: list[float]) -> None:
    """The single failure that ends startup is the one this exists to absorb.

    A pod's resolver can be unusable for a stretch after its node joins, and
    asyncpg raises EAI_AGAIN out of the very first connect. Unretried, that ends
    startup and the pod enters CrashLoopBackOff whose backoff outlives the
    outage, which is what leaves a rollout stalled.
    """
    fake = _FakeConnectEngine([_dns_failure(), _dns_failure(), None])

    await wait_for_database(cast(AsyncEngine, fake), deadline_seconds=120)

    assert fake.attempts == 3


async def test_wait_for_database_connects_once_when_the_database_is_reachable(instant_sleep: list[float]) -> None:
    """A healthy start must cost no extra connect and no sleep, so the retry
    cannot quietly become part of every pod's startup time."""
    fake = _FakeConnectEngine([None])

    await wait_for_database(cast(AsyncEngine, fake), deadline_seconds=120)

    assert fake.attempts == 1
    assert instant_sleep == []


async def test_wait_for_database_backs_off_exponentially_up_to_the_cap(instant_sleep: list[float]) -> None:
    """Sub-second first, then doubling to a cap: a resolver ready a moment later
    costs no measurable startup time, and a minutes-long outage is not hammered
    once per loop iteration."""
    fake = _FakeConnectEngine([_dns_failure()] * 6 + [None])

    await wait_for_database(
        cast(AsyncEngine, fake),
        deadline_seconds=120,
        initial_backoff_seconds=0.5,
        max_backoff_seconds=5.0,
    )

    assert instant_sleep == [0.5, 1.0, 2.0, 4.0, 5.0, 5.0]


async def test_wait_for_database_raises_the_connect_error_once_the_deadline_passes() -> None:
    """The retry is bounded so a database that never answers still ends startup,
    with the connect error in the log rather than a container that outlives its
    startup probe mid-retry."""
    fake = _FakeConnectEngine([_dns_failure()] * 50)

    with pytest.raises(OSError, match="Temporary failure in name resolution"):
        await wait_for_database(cast(AsyncEngine, fake), deadline_seconds=0)

    assert fake.attempts == 1


async def test_wait_for_database_never_sleeps_past_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """The last attempt has to land inside the budget the startup probe allows,
    so the final wait is clipped to what remains rather than the full backoff."""
    delays: list[float] = []
    clock = 0.0

    async def fake_sleep(delay: float) -> None:
        nonlocal clock
        delays.append(delay)
        clock += delay

    class _FakeLoop:
        def time(self) -> float:
            return clock

    monkeypatch.setattr(engine_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(engine_module.asyncio, "get_running_loop", lambda: _FakeLoop())
    fake = _FakeConnectEngine([_dns_failure()] * 50)

    with pytest.raises(OSError):
        await wait_for_database(
            cast(AsyncEngine, fake),
            deadline_seconds=3.0,
            initial_backoff_seconds=2.0,
            max_backoff_seconds=2.0,
        )

    assert delays == [2.0, 1.0]
    assert sum(delays) == 3.0


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            asyncpg.exceptions.InvalidPasswordError("password authentication failed"),
            id="rejected-password",
        ),
        pytest.param(
            asyncpg.exceptions.InvalidCatalogNameError('database "nope" does not exist'),
            id="missing-database",
        ),
    ],
)
async def test_wait_for_database_does_not_retry_a_misconfigured_connection(
    error: BaseException, instant_sleep: list[float]
) -> None:
    """Anything above the socket layer is configuration, and retrying it only
    spends the whole deadline before failing with the error it already had."""
    fake = _FakeConnectEngine([error])

    with pytest.raises(type(error)):
        await wait_for_database(cast(AsyncEngine, fake), deadline_seconds=120)

    assert fake.attempts == 1
    assert instant_sleep == []


async def test_wait_for_database_retries_a_socket_error_the_dialect_wrapped(instant_sleep: list[float]) -> None:
    """SQLAlchemy raises a DBAPIError ``from`` the socket error it wrapped, so
    the reachability test walks the cause chain the way the disconnect listener
    does — a wrapped EAI_AGAIN is the same outage as a bare one."""
    fake = _FakeConnectEngine([_shim_wrapped(_dns_failure()), None])

    await wait_for_database(cast(AsyncEngine, fake), deadline_seconds=120)

    assert fake.attempts == 2
