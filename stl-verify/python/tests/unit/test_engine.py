import pytest
from sqlalchemy.pool import QueuePool

from app.adapters.postgres.engine import get_engine
from app.config import Settings


def test_get_engine_sizes_the_connection_pool_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
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

    engine = get_engine(settings)

    # engine.pool is typed as the Pool base class; the sizing accessors live on
    # QueuePool, which the async engine's AsyncAdaptedQueuePool subclasses.
    pool = engine.pool
    assert isinstance(pool, QueuePool)
    assert pool.size() == 7
    # No public accessor for the overflow ceiling; asserted so dropping the kwarg fails.
    assert pool._max_overflow == 13
