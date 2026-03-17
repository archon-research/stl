from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

# Fetch the single most-recent offchain price per token.
# Uses DISTINCT ON for efficient single-pass ordering.
_SQL = """
SELECT DISTINCT ON (token_id)
    token_id,
    price_usd
FROM offchain_token_price
WHERE token_id = ANY(:token_ids)
ORDER BY token_id, timestamp DESC
"""


class OffchainTokenPriceRepository:
    """Returns the latest CoinGecko (offchain) price for each requested token."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_prices(self, token_ids: list[int]) -> dict[int, Decimal]:
        if not token_ids:
            return {}

        async with self._engine.connect() as conn:
            result = await conn.execute(text(_SQL), {"token_ids": token_ids})
            rows = result.fetchall()

        return {row.token_id: Decimal(str(row.price_usd)) for row in rows}
