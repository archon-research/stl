from collections.abc import Mapping
from decimal import Decimal

from app.domain.entities.risk import AssetId, PrimeId, RrcResult, SurafRrcDetails


class FakeRiskModel:
    async def applies_to(self, asset_id: AssetId, prime_id: PrimeId) -> bool:
        return asset_id == 1

    async def compute(
        self,
        asset_id: AssetId,
        prime_id: PrimeId,
        overrides: Mapping[str, Decimal] | None = None,
    ) -> RrcResult:
        usd_exposure = Decimal("10") if overrides is None else overrides.get("usd_exposure", Decimal("10"))
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=Decimal("5"),
            details=SurafRrcDetails(
                chain_id=1,
                protocol_name="Aave V3",
                symbol="aUSDC",
                underlying_symbol="USDC",
                receipt_token_address="0x" + "a" * 40,
                underlying_token_address="0x" + "b" * 40,
                usd_exposure=usd_exposure,
                usd_exposure_source="override" if overrides else "position",
                rating_id="sample_rating",
                rating_version="v1",
                crr_pct=Decimal("50"),
                source_commit_sha="abc123",
            ),
        )
