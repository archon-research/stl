from app.domain.entities.backed_breakdown import BackedBreakdown
from app.ports.backed_breakdown_repository_resolver import (
    BackedBreakdownRepositoryResolver,
)


class BackedBreakdownService:
    """Resolve the protocol-scoped repository and delegate the query."""

    def __init__(
        self,
        repository_resolver: BackedBreakdownRepositoryResolver,
    ) -> None:
        self._repository_resolver = repository_resolver

    async def get_backed_breakdown(self, protocol_id: int, backed_asset_id: int) -> BackedBreakdown:
        """Resolve the repository from protocol_id and delegate."""
        repository = await self._repository_resolver.resolve(protocol_id)
        return await repository.get_backed_breakdown(backed_asset_id)
