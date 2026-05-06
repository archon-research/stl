import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

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


class PostgresTokenCatalogRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @staticmethod
    def _row_to_metadata(row: object) -> TokenMetadata:
        return TokenMetadata(
            id=row.id,
            chain_id=row.chain_id,
            address="0x" + row.address,
            symbol=row.symbol,
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

        async with self._engine.connect() as conn:
            result = await conn.execute(_LIST_TOKENS_SQL, params)
            rows = result.fetchall()

        return [self._row_to_metadata(row) for row in rows]

    async def get_token(self, token_id: int) -> TokenMetadata | None:
        async with self._engine.connect() as conn:
            row = (await conn.execute(_GET_TOKEN_SQL, {"token_id": token_id})).fetchone()

        if row is None:
            return None

        return self._row_to_metadata(row)

    async def get_latest_price(self, token_id: int) -> TokenPriceQuote | None:
        async with self._engine.connect() as conn:
            row = (await conn.execute(_LATEST_PRICE_SQL, {"token_id": token_id})).fetchone()

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


_LATEST_PRICE_SQL = text(
    """
    WITH latest_onchain AS (
        SELECT
            otp.token_id,
            'onchain'::TEXT AS source_type,
            otp.oracle_id::BIGINT AS source_id,
            o.name AS source_name,
            o.display_name AS source_display_name,
            otp.price_usd,
            otp.timestamp,
            EXTRACT(EPOCH FROM (NOW() - otp.timestamp))::BIGINT AS staleness_seconds
        FROM onchain_token_price otp
        JOIN oracle o ON o.id = otp.oracle_id
        WHERE otp.token_id = :token_id
        ORDER BY otp.timestamp DESC, otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
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
            otp.timestamp,
            EXTRACT(EPOCH FROM (NOW() - otp.timestamp))::BIGINT AS staleness_seconds
        FROM offchain_token_price otp
        JOIN offchain_price_source ops ON ops.id = otp.source_id
        WHERE otp.token_id = :token_id
        ORDER BY otp.timestamp DESC, otp.processing_version DESC
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
