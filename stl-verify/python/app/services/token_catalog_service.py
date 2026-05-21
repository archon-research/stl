from app.domain.entities.allocation import EthAddress
from app.domain.entities.token_catalog import TokenMetadata, TokenPriceQuote
from app.ports.token_catalog_repository import TokenCatalogRepository


class TokenCatalogService:
    def __init__(self, repository: TokenCatalogRepository) -> None:
        self._repository = repository

    async def list_tokens(
        self,
        *,
        chain_id: int | None = None,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[TokenMetadata]:
        return await self._repository.list_tokens(chain_id=chain_id, symbol=symbol, limit=limit)

    async def get_token(self, token_id: int) -> TokenMetadata | None:
        return await self._repository.get_token(token_id)

    async def get_latest_price(self, token_id: int) -> TokenPriceQuote | None:
        return await self._repository.get_latest_price(token_id)

    async def get_token_by_chain_and_address(self, chain_id: int, address: EthAddress) -> TokenMetadata | None:
        return await self._repository.get_token_by_chain_and_address(chain_id, address)
