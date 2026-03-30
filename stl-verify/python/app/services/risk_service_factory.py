import logging
import re
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.onchain.allocation_share_client import (
    FixedAllocationShare,
    OnchainAllocationShareClient,
)
from app.adapters.postgres.aave_like_backed_breakdown_repository import AaveLikeBackedBreakdownRepository
from app.adapters.postgres.aave_like_liquidation_params_repository import AaveLikeLiquidationParamsRepository
from app.adapters.postgres.backed_breakdown_repository_morpho import MorphoBackedBreakdownRepository
from app.adapters.postgres.morpho_liquidation_params_repository import MorphoLiquidationParamsRepository
from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository
from app.ports.allocation_share_port import AllocationSharePort
from app.ports.backed_breakdown_repository import BackedBreakdownRepository
from app.ports.liquidation_params_repository import LiquidationParamsRepository
from app.services.risk_calculation_service import RiskCalculationService

logger = logging.getLogger(__name__)

_AAVE_LIKE = frozenset({"sparklend", "aave_v2", "aave_v3", "aave_v3_lido", "aave_v3_rwa"})
_MORPHO    = frozenset({"morpho_blue"})

_WALLET_LOOKUP_SQL = """
SELECT ap.proxy_address
FROM allocation_position ap
JOIN token t ON t.id = ap.token_id
LEFT JOIN receipt_token rt ON rt.underlying_token_id = t.id
                          AND rt.receipt_token_address = :receipt_token_address
WHERE (t.address = :receipt_token_address OR rt.id IS NOT NULL)
  AND ap.chain_id = :chain_id
  AND ap.balance > 0
ORDER BY ap.balance DESC, ap.block_number DESC
LIMIT 1
"""
# Matches wallets that hold either the receipt token directly (e.g. spPYUSD) or its
# underlying token (e.g. PYUSD) — allocation_position may index either depending on
# which Transfer event was captured. If multiple proxy addresses match, picks the one
# with the largest current balance. Treat violations as data quality issues.


class RiskServiceFactory:
    """Build a RiskCalculationService from a receipt_token_id."""

    def __init__(self, engine: AsyncEngine, alchemy_url: str) -> None:
        self._engine = engine
        self._alchemy_url = alchemy_url
        self._receipt_token_repo = ReceiptTokenRepository(engine)

    async def create(self, receipt_token_id: int) -> tuple[RiskCalculationService, int] | None:
        """Return (service, backed_asset_id) or None if the receipt token is unknown."""
        info = await self._receipt_token_repo.get(receipt_token_id)
        if info is None:
            return None

        # Normalize: strip whitespace, lowercase, replace spaces/hyphens with underscores,
        # then collapse any runs of underscores.
        normalized = re.sub(r"_+", "_", info.protocol_name.strip().casefold().replace(" ", "_").replace("-", "_"))
        breakdown_repo: BackedBreakdownRepository
        liq_repo: LiquidationParamsRepository
        share_port: AllocationSharePort

        if normalized in _AAVE_LIKE:
            breakdown_repo = AaveLikeBackedBreakdownRepository(self._engine, info.protocol_id)
            liq_repo       = AaveLikeLiquidationParamsRepository(self._engine, info.protocol_id)
            asset_id       = info.underlying_token_id
            wallet         = await self._lookup_wallet(info.receipt_token_address, info.chain_id)
            share_port     = OnchainAllocationShareClient(
                receipt_token_address=info.receipt_token_address,
                wallet_address=wallet,
                alchemy_url=self._alchemy_url,
            )

        elif normalized in _MORPHO:
            morpho_repo = MorphoBackedBreakdownRepository(self._engine, info.protocol_id)
            vault_id    = await morpho_repo.resolve_vault_id(info.receipt_token_address, info.chain_id)
            if vault_id is None:
                raise ValueError(f"morpho vault not found for receipt token {receipt_token_id}")
            breakdown_repo = morpho_repo
            liq_repo       = MorphoLiquidationParamsRepository(self._engine)
            asset_id       = vault_id
            share_port     = FixedAllocationShare(Decimal("1"))  # Morpho breakdown is already vault-scoped

        else:
            raise ValueError(
                f"unsupported protocol: {info.protocol_name!r} (normalized: {normalized!r})"
            )

        service = RiskCalculationService(
            breakdown_repo=breakdown_repo,
            liq_params_repo=liq_repo,
            share_port=share_port,
        )
        return service, asset_id

    async def _lookup_wallet(self, receipt_token_address: bytes, chain_id: int) -> bytes:
        """Find the allocator proxy address that currently holds this receipt token."""
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(_WALLET_LOOKUP_SQL),
                {"receipt_token_address": receipt_token_address, "chain_id": chain_id},
            )
            row = result.fetchone()
        if row is None:
            raise ValueError(f"no active allocation position found for receipt token {receipt_token_address.hex()}")
        return bytes(row.proxy_address)
