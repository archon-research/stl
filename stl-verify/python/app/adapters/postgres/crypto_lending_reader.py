import re
from collections.abc import Mapping, Sequence
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.aave_like_backed_breakdown_repository import AaveLikeBackedBreakdownRepository
from app.adapters.postgres.aave_like_liquidation_params_repository import AaveLikeLiquidationParamsRepository
from app.adapters.postgres.allocation_share_repository import (
    _ShareRequest,
    batch_fetch_shares,
    fetch_share,
)
from app.adapters.postgres.backed_breakdown_repository_maple import MapleBackedBreakdownRepository
from app.adapters.postgres.backed_breakdown_repository_morpho import MorphoBackedBreakdownRepository
from app.adapters.postgres.morpho_liquidation_params_repository import MorphoLiquidationParamsRepository
from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository
from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import LiquidationParams
from app.domain.exceptions import MissingShareError
from app.logging import get_logger

logger = get_logger(__name__)

_AAVE_LIKE = frozenset({"sparklend", "aave_v2", "aave_v3", "aave_v3_lido", "aave_v3_rwa"})
_MORPHO = frozenset({"morpho_blue"})
_MAPLE = frozenset({"maple"})
# Protocols eligible for the gap-sweep RRC model (feeds ``list_supported_asset_ids`` →
# ``CryptoLendingRiskService.applies_to``). Maple is deliberately excluded: it has no
# quantitative risk model yet, so RRC degrades to "no model applies" (a 404), while its
# backed-breakdown still resolves via the dedicated ``_MAPLE`` dispatch branch below.
_SUPPORTED_PROTOCOLS = _AAVE_LIKE | _MORPHO
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
),
candidates AS (
    -- Prefer wallets that explicitly hold the receipt token. Fall back to a
    -- wallet holding the underlying only when no receipt-token holder exists.
    SELECT proxy_address, balance, 0 AS source_rank
    FROM latest_receipt
    WHERE balance > 0

    UNION ALL

    SELECT proxy_address, balance, 1 AS source_rank
    FROM latest_underlying
    WHERE balance > 0
)
SELECT proxy_address, balance
FROM candidates
ORDER BY source_rank ASC, balance DESC
LIMIT 1
"""


class PostgresCryptoLendingReader:
    """Postgres-backed facade for loading crypto-lending risk inputs."""

    def __init__(
        self,
        receipt_token_repo: ReceiptTokenRepository,
        aave_breakdown_repo: AaveLikeBackedBreakdownRepository,
        morpho_breakdown_repo: MorphoBackedBreakdownRepository,
        maple_breakdown_repo: MapleBackedBreakdownRepository,
        aave_liq_repo: AaveLikeLiquidationParamsRepository,
        morpho_liq_repo: MorphoLiquidationParamsRepository,
        engine: AsyncEngine,
        allocation_share_max_stale_seconds: int = 1800,
    ) -> None:
        self._receipt_token_repo = receipt_token_repo
        self._aave_breakdown_repo = aave_breakdown_repo
        self._morpho_breakdown_repo = morpho_breakdown_repo
        self._maple_breakdown_repo = maple_breakdown_repo
        self._aave_liq_repo = aave_liq_repo
        self._morpho_liq_repo = morpho_liq_repo
        self._engine = engine
        self._allocation_share_max_stale_seconds = allocation_share_max_stale_seconds

    async def list_supported_asset_ids(self) -> set[int]:
        """Return every receipt_token_id whose protocol is supported by crypto lending."""
        rows = await self._receipt_token_repo.list_protocol_pairs()
        return {
            row.receipt_token_id for row in rows if _normalize_protocol_name(row.protocol_name) in _SUPPORTED_PROTOCOLS
        }

    async def get_receipt_token(self, receipt_token_id: int) -> ReceiptTokenInfo | None:
        return await self._receipt_token_repo.get(receipt_token_id)

    def requires_liquidation_enrichment(self, info: ReceiptTokenInfo) -> bool:
        # Whether this protocol's breakdown needs per-asset liquidation params, which
        # exist only for protocols with a quantitative risk model (``_SUPPORTED_PROTOCOLS``).
        # Maple has none: its pool-level, pre-priced breakdown carries no liq params.
        # Prime-share scaling is orthogonal — both branches scale by ``get_share`` when a
        # prime_id is supplied — so this gates liquidation enrichment only, not shares.
        return _normalize_protocol_name(info.protocol_name) in _SUPPORTED_PROTOCOLS

    async def get_breakdown(self, info: ReceiptTokenInfo) -> BackedBreakdown:
        normalized = _normalize_protocol_name(info.protocol_name)

        if normalized in _AAVE_LIKE:
            return await self._aave_breakdown_repo.get_backed_breakdown(info.protocol_id, info.underlying_token_id)

        if normalized in _MORPHO:
            backed_asset_id = await self._morpho_breakdown_repo.resolve_vault_id(
                info.receipt_token_address, info.chain_id
            )
            if backed_asset_id is None:
                raise ValueError(f"morpho vault not found for receipt token {info.receipt_token_id}")
            return await self._morpho_breakdown_repo.get_backed_breakdown(backed_asset_id)

        if normalized in _MAPLE:
            return await self._maple_breakdown_repo.get_backed_breakdown(info.receipt_token_address, info.chain_id)

        raise ValueError(f"unsupported protocol: {info.protocol_name!r} (normalized: {normalized!r})")

    async def get_liquidation_params(
        self,
        info: ReceiptTokenInfo,
        backed_asset_id: int,
        token_ids: list[int],
    ) -> dict[int, LiquidationParams]:
        normalized = _normalize_protocol_name(info.protocol_name)

        if normalized in _AAVE_LIKE:
            return await self._aave_liq_repo.get_params(info.protocol_id, token_ids)

        if normalized in _MORPHO:
            return await self._morpho_liq_repo.get_params(backed_asset_id, token_ids)

        if normalized in _MAPLE:
            # Maple has no per-asset liquidation params. Returning {} (rather than
            # raising) keeps this method total over every protocol get_breakdown
            # supports: a caller that does pass Maple here degrades visibly
            # (_build_enriched_items drops every param-less item and logs) instead
            # of hitting the unsupported-protocol raise below.
            return {}

        raise ValueError(f"unsupported protocol: {info.protocol_name!r} (normalized: {normalized!r})")

    async def get_share(self, info: ReceiptTokenInfo, prime_id: EthAddress) -> Decimal:
        normalized = _normalize_protocol_name(info.protocol_name)

        if normalized not in _AAVE_LIKE and normalized not in _MORPHO and normalized not in _MAPLE:
            raise ValueError(f"unsupported protocol: {info.protocol_name!r} (normalized: {normalized!r})")

        # Maple's prime share is the same balance/totalSupply ratio as the enriched
        # protocols: the prime holds the syrup receipt token, so its pool share is its
        # syrup balance over the vault's total shares (pari-passu, pro-rata attribution).
        token_id = self._require_receipt_token_token_id(info)
        return await self._load_share(
            chain_id=info.chain_id,
            token_id=token_id,
            wallet_address=prime_id.to_bytes(),
        )

    async def batch_get_shares(
        self,
        infos: Sequence[ReceiptTokenInfo],
        prime_id: EthAddress,
    ) -> Mapping[int, Decimal | Exception]:
        """Resolve shares for many receipt tokens in a single round-trip.

        See ``CryptoLendingReader.batch_get_shares`` for the contract. Per-asset
        validation errors (unsupported protocol, missing ``receipt_token_token_id``)
        are returned as values; the DB call itself is a single ``LATERAL JOIN``
        query that touches each ``(chain_id, token_id)`` pair via its segmentby
        index (see ``_BATCH_SHARE_LOOKUP_SQL`` in
        ``allocation_share_repository``).
        """
        if not infos:
            return {}

        results: dict[int, Decimal | Exception] = {}
        # Map (chain_id, token_id) -> [receipt_token_id, ...]; the same physical
        # share row can back multiple receipt tokens in principle, and we want
        # all of them to share the single DB lookup result.
        pair_to_receipts: dict[tuple[int, int], list[int]] = {}
        requests: list[_ShareRequest] = []

        for info in infos:
            normalized = _normalize_protocol_name(info.protocol_name)
            # Mirror get_share: Maple resolves through the same
            # (chain_id, receipt_token_token_id) lookup, so it must be accepted here
            # too — otherwise the batched path would raise for an asset the
            # un-batched path resolves fine, once Maple gains a risk model.
            if normalized not in _AAVE_LIKE and normalized not in _MORPHO and normalized not in _MAPLE:
                results[info.receipt_token_id] = ValueError(
                    f"unsupported protocol: {info.protocol_name!r} (normalized: {normalized!r})"
                )
                continue
            if info.receipt_token_token_id is None:
                results[info.receipt_token_id] = MissingShareError(
                    f"receipt-token address not indexed yet for receipt_token_id={info.receipt_token_id}"
                )
                continue

            key = (info.chain_id, info.receipt_token_token_id)
            if key not in pair_to_receipts:
                pair_to_receipts[key] = []
                requests.append(_ShareRequest(chain_id=info.chain_id, token_id=info.receipt_token_token_id))
            pair_to_receipts[key].append(info.receipt_token_id)

        if requests:
            by_pair = await batch_fetch_shares(
                engine=self._engine,
                requests=requests,
                wallet_address=prime_id.to_bytes(),
                max_stale_seconds=self._allocation_share_max_stale_seconds,
            )
            for key, receipt_ids in pair_to_receipts.items():
                value = by_pair.get(
                    key,
                    MissingShareError(f"no consistent balance+supply pair for chain_id={key[0]} token_id={key[1]}"),
                )
                for receipt_id in receipt_ids:
                    results[receipt_id] = value

        return results

    async def get_legacy_share(self, info: ReceiptTokenInfo) -> Decimal:
        normalized = _normalize_protocol_name(info.protocol_name)

        if normalized in _MORPHO:
            # Legacy endpoints are asset-level (no prime_id), so preserve the
            # pre-existing vault-level Morpho semantics until VEC-183 deletes
            # them. The prime-aware RiskModel path uses ``get_share()`` above.
            return Decimal("1")
        if normalized not in _AAVE_LIKE:
            raise ValueError(f"unsupported protocol: {info.protocol_name!r} (normalized: {normalized!r})")

        token_id = self._require_receipt_token_token_id(info)
        try:
            wallet_address = await self._lookup_wallet(info.receipt_token_address, info.chain_id)
        except ValueError as exc:
            raise MissingShareError(str(exc)) from exc
        return await self._load_share(
            chain_id=info.chain_id,
            token_id=token_id,
            wallet_address=wallet_address,
        )

    def _require_receipt_token_token_id(self, info: ReceiptTokenInfo) -> int:
        if info.receipt_token_token_id is None:
            raise MissingShareError(
                f"receipt-token address not indexed yet for receipt_token_id={info.receipt_token_id}"
            )
        return info.receipt_token_token_id

    async def _load_share(self, chain_id: int, token_id: int, wallet_address: bytes) -> Decimal:
        return await fetch_share(
            engine=self._engine,
            chain_id=chain_id,
            token_id=token_id,
            wallet_address=wallet_address,
            max_stale_seconds=self._allocation_share_max_stale_seconds,
        )

    async def _lookup_wallet(self, receipt_token_address: bytes, chain_id: int) -> bytes:
        """Find the wallet used for legacy share resolution.

        Legacy endpoints do not provide a ``prime_id``, so for Aave-like
        assets we infer a wallet by preferring the largest current holder of
        the receipt token itself and falling back to the underlying token only
        if no receipt-token holder exists.
        """
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
                "crypto_lending_reader: DB error looking up wallet for receipt_token=%s chain_id=%d",
                token_hex,
                chain_id,
            )
            raise
        if row is None:
            raise ValueError(f"no active allocation position found for receipt token {token_hex}")
        return bytes(row.proxy_address)


def _normalize_protocol_name(protocol_name: str) -> str:
    return _NORMALIZE_RE.sub("_", protocol_name.strip().casefold())
