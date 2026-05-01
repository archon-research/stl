from decimal import Decimal
from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import LiquidationParams


class CryptoLendingReader(Protocol):
    """Facade for loading all crypto-lending risk inputs."""

    async def list_supported_asset_ids(self) -> set[int]:
        """Return every receipt_token_id supported by the crypto-lending model."""
        ...

    async def get_receipt_token(self, receipt_token_id: int) -> ReceiptTokenInfo | None:
        """Return receipt-token routing metadata, or ``None`` if unknown."""
        ...

    async def get_breakdown(self, info: ReceiptTokenInfo) -> BackedBreakdown:
        """Return the resolved backed breakdown for the receipt token's protocol."""
        ...

    async def get_liquidation_params(
        self,
        info: ReceiptTokenInfo,
        backed_asset_id: int,
        token_ids: list[int],
    ) -> dict[int, LiquidationParams]:
        """Return liquidation params for the receipt token's active collateral tokens."""
        ...

    async def get_share(self, info: ReceiptTokenInfo, prime_id: EthAddress) -> Decimal:
        """Return the prime's share of the receipt-token supply."""
        ...

    async def get_legacy_share(self, info: ReceiptTokenInfo) -> Decimal:
        """Return the legacy share used by old endpoints.

        Temporary compatibility method for endpoints that do not provide a
        ``prime_id``. Remove in VEC-183.
        """
        ...
