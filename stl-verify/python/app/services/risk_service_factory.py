from dataclasses import dataclass
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
from app.domain.entities.risk import ResolvedRiskPosition
from app.domain.risk_protocols import is_aave_like_protocol, is_morpho_protocol, normalize_protocol_name
from app.logging import get_logger
from app.services.risk_calculation_service import RiskCalculationService

logger = get_logger(__name__)

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
    ORDER BY ap.proxy_address, ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC
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
    ORDER BY ap.proxy_address, ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC
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


@dataclass(frozen=True)
class GapSweepConstruction:
    service: RiskCalculationService
    backed_asset_id: int


class RiskServiceFactory:
    """Build gap-sweep calculation services for compatibility and prime-based paths."""

    def __init__(
        self,
        engine: AsyncEngine,
        alchemy_url: str,
        http_client: httpx.AsyncClient,
        default_gap_pct: Decimal,
    ) -> None:
        self._engine = engine
        self._alchemy_url = alchemy_url
        self._http_client = http_client
        self._default_gap_pct = default_gap_pct
        self._receipt_token_repo = ReceiptTokenRepository(engine)

    async def create(self, receipt_token_id: int) -> tuple[RiskCalculationService, int] | None:
        """Compatibility path: build from a receipt_token_id using wallet lookup."""
        info = await self._receipt_token_repo.get(receipt_token_id)
        if info is None:
            return None

        if is_aave_like_protocol(info.protocol_name):
            wallet = await self._lookup_wallet(info.receipt_token_address, info.chain_id)
            construction = self._build_aave_like(
                protocol_id=info.protocol_id,
                underlying_token_id=info.underlying_token_id,
                receipt_token_address=info.receipt_token_address,
                wallet_address=wallet,
            )
            return construction.service, construction.backed_asset_id

        if is_morpho_protocol(info.protocol_name):
            construction = await self._build_morpho(
                protocol_id=info.protocol_id,
                receipt_token_address=info.receipt_token_address,
                chain_id=info.chain_id,
                receipt_token_context=str(receipt_token_id),
            )
            return construction.service, construction.backed_asset_id

        normalized = normalize_protocol_name(info.protocol_name)
        raise ValueError(f"unsupported protocol: {info.protocol_name!r} (normalized: {normalized!r})")

    async def create_for_position(self, position: ResolvedRiskPosition) -> GapSweepConstruction:
        """Prime-based path: build from a fully resolved position."""
        receipt_token_address = bytes.fromhex(position.receipt_token_address.removeprefix("0x"))

        if is_aave_like_protocol(position.protocol_name):
            return self._build_aave_like(
                protocol_id=position.protocol_id,
                underlying_token_id=position.underlying_token_id,
                receipt_token_address=receipt_token_address,
                wallet_address=position.prime_id.bytes,
            )

        if is_morpho_protocol(position.protocol_name):
            return await self._build_morpho(
                protocol_id=position.protocol_id,
                receipt_token_address=receipt_token_address,
                chain_id=position.chain_id,
                receipt_token_context=str(position.receipt_token_id),
            )

        normalized = normalize_protocol_name(position.protocol_name)
        raise ValueError(f"unsupported protocol: {position.protocol_name!r} (normalized: {normalized!r})")

    def _build_aave_like(
        self,
        *,
        protocol_id: int,
        underlying_token_id: int,
        receipt_token_address: bytes,
        wallet_address: bytes,
    ) -> GapSweepConstruction:
        breakdown_repo = AaveLikeBackedBreakdownRepository(self._engine, protocol_id)
        liq_repo = AaveLikeLiquidationParamsRepository(self._engine, protocol_id)
        share_port = OnchainAllocationShareClient(
            receipt_token_address=receipt_token_address,
            wallet_address=wallet_address,
            alchemy_url=self._alchemy_url,
            http_client=self._http_client,
        )
        service = RiskCalculationService(
            breakdown_repo=breakdown_repo,
            liq_params_repo=liq_repo,
            share_port=share_port,
            default_gap_pct=self._default_gap_pct,
        )
        return GapSweepConstruction(service=service, backed_asset_id=underlying_token_id)

    async def _build_morpho(
        self,
        *,
        protocol_id: int,
        receipt_token_address: bytes,
        chain_id: int,
        receipt_token_context: str,
    ) -> GapSweepConstruction:
        morpho_repo = MorphoBackedBreakdownRepository(self._engine, protocol_id)
        vault_id = await morpho_repo.resolve_vault_id(receipt_token_address, chain_id)
        if vault_id is None:
            raise ValueError(f"morpho vault not found for receipt token {receipt_token_context}")

        service = RiskCalculationService(
            breakdown_repo=morpho_repo,
            liq_params_repo=MorphoLiquidationParamsRepository(self._engine),
            share_port=FixedAllocationShare(Decimal("1")),
            default_gap_pct=self._default_gap_pct,
        )
        return GapSweepConstruction(service=service, backed_asset_id=vault_id)

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
