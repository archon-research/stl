from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.token_catalog import TokenMetadata, TokenPriceQuote


class TokenCatalogRepository(Protocol):
    async def list_tokens(
        self,
        *,
        chain_id: int | None = None,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[TokenMetadata]:
        """Return token metadata rows with optional chain/symbol filtering."""
        ...

    async def get_token(self, token_id: int) -> TokenMetadata | None:
        """Return token metadata by id, or None when not found."""
        ...

    async def get_latest_price(self, token_id: int) -> TokenPriceQuote | None:
        """Return the latest price quote with source metadata for a token."""
        ...

    async def get_token_by_chain_and_address(self, chain_id: int, address: EthAddress) -> TokenMetadata | None:
        """Return token metadata for ``(chain_id, address)``, or None when not found."""
        ...

    async def get_latest_price_by_chain_and_address(self, chain_id: int, address: EthAddress) -> TokenPriceQuote | None:
        """Return the latest price quote for the token at ``(chain_id, address)``."""
        ...
