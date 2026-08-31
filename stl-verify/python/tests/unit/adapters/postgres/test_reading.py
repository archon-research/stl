"""The connection-lending read helper."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres._reading import reading


def _engine(error: Exception | None = None):
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    engine = MagicMock()
    engine.connect.side_effect = error if error else None
    engine.connect.return_value = conn
    return engine


@pytest.mark.asyncio
async def test_names_what_was_being_read_in_the_failure() -> None:
    # The message is what sends an operator to the right query, so it carries
    # the caller's own description rather than the driver's.
    engine = _engine(RuntimeError("connection reset"))

    with pytest.raises(ValueError, match="reading the reference positions for 'spark'"):
        async with reading(engine, what="reading the reference positions for 'spark'"):
            pass


@pytest.mark.asyncio
async def test_returning_from_the_block_is_not_a_failure() -> None:
    # The readers return early when a prime has no rows, from inside the block.
    async def read() -> str:
        async with reading(_engine(), what="reading something"):
            return "answered"

    assert await read() == "answered"
