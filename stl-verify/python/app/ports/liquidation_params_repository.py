from typing import Protocol

from app.risk_engine.entities import LiquidationParams


class LiquidationParamsRepository(Protocol):
    """Return normalised liquidation parameters for a set of collateral token IDs.

    backed_asset_id is passed through so Morpho adapters can scope the query
    to the vault's active markets (for SparkLend/Aave adapters it is unused).
    """

    async def get_params(
        self, backed_asset_id: int, token_ids: list[int]
    ) -> dict[int, LiquidationParams]:
        """Return a mapping of token_id → LiquidationParams.

        Missing token IDs are absent from the returned dict (not an error).
        All returned values are already normalised to [0,1] / >1.0 ranges.
        """
        ...
