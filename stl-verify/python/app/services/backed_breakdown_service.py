from app.domain.entities.backed_breakdown import BackedBreakdown
from app.ports.backed_breakdown_repository import BackedBreakdownRepository
from app.ports.morpho_repository import MorphoRepository


class BackedBreakdownService:
    """Use case: compute the collateral breakdown backing a debt token.

    Routes to the Morpho repository when the debt_token_id corresponds to a
    Morpho vault, otherwise falls back to the default (SparkLend) repository.
    """

    def __init__(
        self,
        repository: BackedBreakdownRepository,
        morpho_repository: BackedBreakdownRepository,
        morpho: MorphoRepository,
    ) -> None:
        self._repository = repository
        self._morpho_repository = morpho_repository
        self._morpho = morpho

    async def get_backed_breakdown(self, protocol_id: int, debt_token_id: int) -> BackedBreakdown:
        """Determine the correct repository and delegate."""
        if await self._morpho.is_morpho_vault(debt_token_id):
            return await self._morpho_repository.get_backed_breakdown(
                protocol_id=protocol_id, debt_token_id=debt_token_id
            )
        return await self._repository.get_backed_breakdown(protocol_id=protocol_id, debt_token_id=debt_token_id)
