import logging
import re
from decimal import Decimal

import httpx
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
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
_MORPHO = frozenset({"morpho_blue"})

# Protocol names in the DB may use spaces, hyphens, or mixed case (e.g. "Aave V3", "morpho-blue").
# Normalise to lowercase underscore-separated before matching against the sets above.
_NORMALIZE_RE = re.compile(r"[\s\-_]+")

_WALLET_LOOKUP_SQL = """
WITH latest_receipt AS (
    -- Most-recent balance snapshot per wallet for the receipt token itself.
    -- DISTINCT ON ensures we read the current state, not a historical peak.
    SELECT DISTINCT ON (ap.proxy_address)
        ap.proxy_address,
        ap.balance
    FROM allocation_position ap
    JOIN token t ON t.id = ap.token_id AND t.address = :receipt_token_address
    WHERE ap.chain_id = :chain_id
    ORDER BY ap.proxy_address, ap.block_number DESC, ap.block_version DESC, ap.log_index DESC
),
latest_underlying AS (
    -- Most-recent balance snapshot per wallet for the underlying token.
    SELECT DISTINCT ON (ap.proxy_address)
        ap.proxy_address,
        ap.balance
    FROM allocation_position ap
    JOIN token t ON t.id = ap.token_id
    JOIN receipt_token rt ON rt.underlying_token_id = t.id
                         AND rt.receipt_token_address = :receipt_token_address
    WHERE ap.chain_id = :chain_id
    ORDER BY ap.proxy_address, ap.block_number DESC, ap.block_version DESC, ap.log_index DESC
)
SELECT proxy_address, balance
FROM (
    SELECT proxy_address, balance FROM latest_receipt
    UNION ALL
    SELECT proxy_address, balance FROM latest_underlying
) combined
WHERE balance > 0
ORDER BY balance DESC
LIMIT 1
"""


class RiskServiceFactory:
    """Build a RiskCalculationService from a receipt_token_id."""

    def __init__(self, engine: AsyncEngine, alchemy_url: str, http_client: httpx.AsyncClient) -> None:
        self._engine = engine
        self._alchemy_url = alchemy_url
        self._http_client = http_client
        self._receipt_token_repo = ReceiptTokenRepository(engine)

    async def create(self, receipt_token_id: int) -> tuple[RiskCalculationService, int] | None:
        """Return (service, backed_asset_id) or None if the receipt token is unknown."""
        info = await self._receipt_token_repo.get(receipt_token_id)
        if info is None:
            return None

        normalized = _NORMALIZE_RE.sub("_", info.protocol_name.strip().casefold())
        breakdown_repo: BackedBreakdownRepository
        liq_repo: LiquidationParamsRepository
        share_port: AllocationSharePort

        if normalized in _AAVE_LIKE:
            breakdown_repo = AaveLikeBackedBreakdownRepository(self._engine, info.protocol_id)
            liq_repo = AaveLikeLiquidationParamsRepository(self._engine, info.protocol_id)
            asset_id = info.underlying_token_id
            wallet = await self._lookup_wallet(info.receipt_token_address, info.chain_id)
            share_port = OnchainAllocationShareClient(
                receipt_token_address=info.receipt_token_address,
                wallet_address=wallet,
                alchemy_url=self._alchemy_url,
                http_client=self._http_client,
            )

        elif normalized in _MORPHO:
            morpho_repo = MorphoBackedBreakdownRepository(self._engine, info.protocol_id)
            vault_id = await morpho_repo.resolve_vault_id(info.receipt_token_address, info.chain_id)
            if vault_id is None:
                raise ValueError(f"morpho vault not found for receipt token {receipt_token_id}")
            breakdown_repo = morpho_repo
            liq_repo = MorphoLiquidationParamsRepository(self._engine)
            asset_id = vault_id
            share_port = FixedAllocationShare(Decimal("1"))  # Morpho breakdown is already vault-scoped

        else:
            raise ValueError(f"unsupported protocol: {info.protocol_name!r} (normalized: {normalized!r})")

        service = RiskCalculationService(
            breakdown_repo=breakdown_repo,
            liq_params_repo=liq_repo,
            share_port=share_port,
        )
        return service, asset_id

    async def _lookup_wallet(self, receipt_token_address: bytes, chain_id: int) -> bytes:
        """Find the allocator proxy address that currently holds this receipt token."""
        token_hex = receipt_token_address.hex()
        try:
            async with self._engine.connect() as conn:
                await conn.exec_driver_sql("SET LOCAL statement_timeout = '5s'")
                result = await conn.execute(
                    text(_WALLET_LOOKUP_SQL),
                    {"receipt_token_address": receipt_token_address, "chain_id": chain_id},
                )
                row = result.fetchone()
        except SQLAlchemyError:
            logger.exception(
                "risk_service_factory: DB error looking up wallet for receipt_token=%s chain_id=%d",
                token_hex,
                chain_id,
            )
            raise
        if row is None:
            raise ValueError(f"no active allocation position found for receipt token {token_hex}")
        return bytes(row.proxy_address)
