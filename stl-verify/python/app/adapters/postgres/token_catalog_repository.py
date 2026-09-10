import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.reference_as_of import (
    ORACLE_ASSET_AS_OF,
    ReferenceAsOf,
    ReferenceEffectiveAtProvider,
)
from app.domain.entities.allocation import EthAddress
from app.domain.entities.token_catalog import TokenMetadata, TokenPriceQuote

logger = logging.getLogger(__name__)


def _escape_like_pattern(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _safe_decimal(value: Any, field_name: str, row_identifier: Any = None) -> Decimal:
    try:
        if value is None:
            raise ValueError("missing numeric value")

        parsed = Decimal(str(value))
        if not parsed.is_finite():
            raise ValueError("non-finite numeric value")

        return parsed
    except (ValueError, InvalidOperation, TypeError) as exc:
        logger.error(
            "Invalid decimal value in token catalog query",
            extra={
                "field_name": field_name,
                "row_identifier": str(row_identifier) if row_identifier else None,
                "value": str(value),
            },
        )
        raise ValueError(
            f"Database contains invalid numeric value for {field_name} (row={row_identifier}, value={value}): {exc}"
        ) from exc


def _normalize_metadata(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    return None


def _normalize_symbol(value: str | None) -> str | None:
    """Map the catalog's empty/whitespace symbols to the domain's "absent".

    The token table stores ``''`` for rows whose symbol is unknown, but the
    domain models an unknown symbol as ``None`` (a present symbol must be
    non-empty). Translating here keeps that invariant intact and prevents a
    single blank-symbol row from failing the whole listing.
    """
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


class TokenCatalogRepository:
    def __init__(self, engine: AsyncEngine, reference_effective_at: ReferenceEffectiveAtProvider) -> None:
        self._engine = engine
        self._reference = ReferenceAsOf(reference_effective_at)

    @staticmethod
    def _row_to_metadata(row: Row[Any]) -> TokenMetadata:
        return TokenMetadata(
            id=row.id,
            chain_id=row.chain_id,
            address="0x" + row.address,
            symbol=_normalize_symbol(row.symbol),
            decimals=row.decimals,
            updated_at=row.updated_at,
            metadata=_normalize_metadata(row.metadata),
        )

    async def list_tokens(
        self,
        *,
        chain_id: int | None = None,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[TokenMetadata]:
        params = {
            "chain_id": chain_id,
            "symbol": (
                f"%{_escape_like_pattern(symbol.strip())}%" if symbol is not None and symbol.strip() != "" else None
            ),
            "limit": min(max(limit, 1), 500),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(_LIST_TOKENS_SQL, params)
                rows = result.fetchall()

            return [self._row_to_metadata(row) for row in rows]
        except Exception as exc:
            logger.error(
                "Failed to fetch tokens from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "chain_id": chain_id,
                    "symbol": symbol,
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching tokens: {exc}") from exc

    async def get_token(self, token_id: int) -> TokenMetadata | None:
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(_GET_TOKEN_SQL, {"token_id": token_id})).fetchone()

            return self._row_to_metadata(row) if row else None
        except Exception as exc:
            logger.error(
                "Failed to fetch token from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "token_id": token_id,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching token {token_id}: {exc}") from exc

    async def get_token_by_chain_and_address(self, chain_id: int, address: EthAddress) -> TokenMetadata | None:
        params = {"chain_id": chain_id, "address": address.to_bytes()}
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(_GET_TOKEN_BY_CHAIN_ADDRESS_SQL, params)).fetchone()

            return self._row_to_metadata(row) if row else None
        except Exception as exc:
            logger.error(
                "Failed to fetch token by chain+address from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "chain_id": chain_id,
                    "address": str(address),
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching token for chain_id={chain_id}, address={address}: {exc}"
            ) from exc

    async def get_latest_price(self, token_id: int) -> TokenPriceQuote | None:
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(_LATEST_PRICE_SQL, self._reference.params(token_id=token_id))).fetchone()

            if row is None:
                return None

            return TokenPriceQuote(
                token_id=row.token_id,
                source_type=row.source_type,
                source_id=row.source_id,
                source_name=row.source_name,
                source_display_name=row.source_display_name,
                price_usd=_safe_decimal(row.price_usd, "price_usd", token_id),
                timestamp=row.timestamp,
                staleness_seconds=max(int(row.staleness_seconds or 0), 0),
            )
        except Exception as exc:
            logger.error(
                "Failed to fetch token price from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "token_id": token_id,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching price for token {token_id}: {exc}") from exc


_LIST_TOKENS_SQL = text(
    """
    SELECT
        t.id,
        t.chain_id,
        encode(t.address, 'hex') AS address,
        t.symbol,
        t.decimals,
        t.updated_at,
        t.metadata
    FROM token t
    WHERE (CAST(:chain_id AS INT) IS NULL OR t.chain_id = CAST(:chain_id AS INT))
    AND (CAST(:symbol AS TEXT) IS NULL OR t.symbol ILIKE CAST(:symbol AS TEXT) ESCAPE '\\')
    ORDER BY t.chain_id ASC, t.symbol ASC NULLS LAST, t.id ASC
    LIMIT CAST(:limit AS INT)
    """
)


_GET_TOKEN_SQL = text(
    """
    SELECT
        t.id,
        t.chain_id,
        encode(t.address, 'hex') AS address,
        t.symbol,
        t.decimals,
        t.updated_at,
        t.metadata
    FROM token t
    WHERE t.id = :token_id
    """
)


_GET_TOKEN_BY_CHAIN_ADDRESS_SQL = text(
    """
    SELECT
        t.id,
        t.chain_id,
        encode(t.address, 'hex') AS address,
        t.symbol,
        t.decimals,
        t.updated_at,
        t.metadata
    FROM token t
    WHERE t.chain_id = :chain_id AND t.address = :address
    """
)


_LATEST_PRICE_SQL = text(
    f"""
    -- A latest-row read with no time predicate cannot exclude a chunk, so on the
    -- price histories this statement planned over every chunk (VEC-672). The two
    -- halves differ in what fixes that: on-chain rows are written only when a
    -- price changes, so a window would drop a stable that has not moved in months
    -- and the read comes from token_price_current instead; off-chain snapshots are
    -- written every poll, so a window on the history is enough there.
    WITH latest_onchain AS (
        SELECT
            tpc.token_id,
            'onchain'::TEXT AS source_type,
            tpc.oracle_id::BIGINT AS source_id,
            o.name AS source_name,
            o.display_name AS source_display_name,
            tpc.price_usd,
            tpc.block_timestamp AS "timestamp",
            EXTRACT(EPOCH FROM (NOW() - tpc.block_timestamp))::BIGINT AS staleness_seconds
        FROM token_price_current tpc
        JOIN oracle o ON o.id = tpc.oracle_id
        WHERE tpc.token_id = :token_id
          -- An undated cache row (20260910_120050 could not read its history
          -- row) is absent here, exactly as it was absent from the history read.
          AND tpc.block_timestamp IS NOT NULL
          -- enabled-mapping filter + oracle_id tiebreak (canonical rationale, incl.
          -- the append-on-change read path, on _DIRECT_ASSET_HOLDINGS_SQL in
          -- allocation_position_repository.py). A source retired as of
          -- :reference_effective_at is excluded; same-block rows from two oracles
          -- also share the block timestamp, so ties reach this read too.
          AND EXISTS (
              SELECT 1 FROM {ORACLE_ASSET_AS_OF} oa
              WHERE oa.oracle_id = tpc.oracle_id
                AND oa.token_id = tpc.token_id
                AND oa.enabled
          )
        ORDER BY tpc.block_timestamp DESC, tpc.block_number DESC, tpc.block_version DESC,
                 tpc.processing_version DESC, tpc.oracle_id DESC
        LIMIT 1
    ),
    latest_offchain AS (
        SELECT
            otp.token_id,
            'offchain'::TEXT AS source_type,
            otp.source_id::BIGINT AS source_id,
            ops.name AS source_name,
            ops.display_name AS source_display_name,
            otp.price_usd,
            otp."timestamp",
            EXTRACT(EPOCH FROM (NOW() - otp."timestamp"))::BIGINT AS staleness_seconds
        FROM offchain_token_price otp
        JOIN offchain_price_source ops ON ops.id = otp.source_id
        WHERE otp.token_id = :token_id
          -- A SQL literal, never a bind parameter: `now() - $n` is not constified,
          -- so a bound interval plans every chunk (db/migrations/AGENTS.md). Seven
          -- days is many polls of a feed that writes every interval; a feed silent
          -- for longer has no current quote, as the CORE liveness check treats it.
          AND otp."timestamp" > now() - interval '7 days'
        ORDER BY otp."timestamp" DESC, otp.processing_version DESC
        LIMIT 1
    )
    SELECT
        token_id,
        source_type,
        source_id,
        source_name,
        source_display_name,
        price_usd,
        timestamp,
        staleness_seconds
    FROM (
        SELECT * FROM latest_onchain
        UNION ALL
        SELECT * FROM latest_offchain
    ) priced
    ORDER BY timestamp DESC
    LIMIT 1
    """
)
