from app.ports.liquidation_params_repository import LiquidationParamsRepository
from app.ports.protocol_metadata_repository import ProtocolMetadataRepository

_AAVE_LIKE_PROTOCOL_TYPES = frozenset({"sparklend", "aave_v2", "aave_v3", "aave_v3_lido", "aave_v3_rwa"})
_MORPHO_PROTOCOL_TYPES = frozenset({"morpho_blue"})


class LiquidationParamsRepositoryResolver:
    """Resolve the correct LiquidationParamsRepository for a given protocol."""

    def __init__(
        self,
        protocol_metadata_repository: ProtocolMetadataRepository,
        aave_like_repository: LiquidationParamsRepository,
        morpho_repository: LiquidationParamsRepository,
    ) -> None:
        self._protocol_metadata_repository = protocol_metadata_repository
        self._aave_like_repository = aave_like_repository
        self._morpho_repository = morpho_repository

    async def resolve(self, protocol_id: int) -> LiquidationParamsRepository:
        """Return the repository that matches the protocol metadata."""
        protocol_type = await self._protocol_metadata_repository.get_protocol_type(protocol_id)
        return self._repository_for_protocol_type(protocol_id, protocol_type)

    def _repository_for_protocol_type(self, protocol_id: int, protocol_type: str | None) -> LiquidationParamsRepository:
        """Map a stored protocol type to the matching repository."""
        if protocol_type is None:
            raise ValueError(f"unsupported protocol: {protocol_id}")

        normalized_protocol_type = protocol_type.casefold().replace(" ", "_")

        if normalized_protocol_type in _MORPHO_PROTOCOL_TYPES:
            return self._morpho_repository

        if normalized_protocol_type in _AAVE_LIKE_PROTOCOL_TYPES:
            return self._aave_like_repository

        raise ValueError(f"unsupported protocol: {protocol_id}")
