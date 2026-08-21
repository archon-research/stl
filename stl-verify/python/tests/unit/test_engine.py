import pytest
from sqlalchemy.pool import QueuePool

from app.adapters.postgres.engine import create_db_engine
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
