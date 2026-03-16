from decimal import Decimal
from typing import Protocol


class TokenPriceRepository(Protocol):
    """Return the latest USD price for a set of token IDs."""

    async def get_prices(self, token_ids: list[int]) -> dict[int, Decimal]:
        """Return a mapping of token_id → USD price.

        Missing token IDs (no price data) are absent from the returned dict.
        """
        ...
