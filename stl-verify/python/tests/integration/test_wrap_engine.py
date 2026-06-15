"""Pin non-obvious ``wrap_engine`` invariants that no product test exercises directly."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


@pytest.mark.asyncio(loop_scope="session")
async def test_set_local_statement_timeout_inside_wrap_engine(wrap_engine: AsyncEngine) -> None:
    """``SET LOCAL`` requires an active transaction. ``wrap_conn`` opens an outer
    txn + nested savepoint before yielding, so production code that issues
    ``SET LOCAL statement_timeout`` (``allocation_share_repository``,
    ``crypto_lending_reader``) keeps working under the wrap.
    """
    async with wrap_engine.connect() as conn:
        await conn.exec_driver_sql("SET LOCAL statement_timeout = '5s'")
        value = (await conn.execute(text("SHOW statement_timeout"))).scalar_one()
        assert value == "5s"
