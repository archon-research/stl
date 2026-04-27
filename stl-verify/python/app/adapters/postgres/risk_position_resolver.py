from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.risk import AssetId, PrimeId, ResolvedRiskPosition, RiskPositionNotFoundError
from app.domain.risk_protocols import is_aave_like_protocol
from app.logging import get_logger

logger = get_logger(__name__)

_RESOLVE_POSITION_SQL = text(
    """
    WITH latest_positions AS (
        SELECT DISTINCT ON (ap.token_id, ap.proxy_address)
            ap.chain_id,
            ap.token_id,
            ap.balance
        FROM allocation_position ap
        WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
        ORDER BY ap.token_id, ap.proxy_address,
                 ap.block_number DESC, ap.block_version DESC, ap.processing_version DESC, ap.log_index DESC
    ),
    candidate_positions AS (
        SELECT
            lp.chain_id,
            rt.id                                   AS receipt_token_id,
            rt.protocol_id                          AS protocol_id,
            encode(rt.receipt_token_address, 'hex') AS receipt_token_address,
            ut.id                                   AS underlying_token_id,
            encode(ut.address, 'hex')               AS underlying_token_address,
            rt.symbol                               AS symbol,
            ut.symbol                               AS underlying_symbol,
            pr.name                                 AS protocol_name,
            lp.balance                              AS balance
        FROM latest_positions lp
        JOIN token t ON t.id = lp.token_id
        JOIN receipt_token rt ON rt.underlying_token_id = t.id AND rt.id = :asset_id
        JOIN token ut ON ut.id = rt.underlying_token_id
        JOIN protocol pr ON pr.id = rt.protocol_id
        WHERE lp.balance > 0

        UNION ALL

        SELECT
            lp.chain_id,
            rt.id                                   AS receipt_token_id,
            rt.protocol_id                          AS protocol_id,
            encode(rt.receipt_token_address, 'hex') AS receipt_token_address,
            ut.id                                   AS underlying_token_id,
            encode(ut.address, 'hex')               AS underlying_token_address,
            rt.symbol                               AS symbol,
            ut.symbol                               AS underlying_symbol,
            pr.name                                 AS protocol_name,
            lp.balance                              AS balance
        FROM latest_positions lp
        JOIN token t ON t.id = lp.token_id
        JOIN receipt_token rt ON rt.receipt_token_address = t.address
                             AND rt.chain_id = lp.chain_id
                             AND rt.id = :asset_id
        JOIN token ut ON ut.id = rt.underlying_token_id
        JOIN protocol pr ON pr.id = rt.protocol_id
        WHERE lp.balance > 0
    ),
    active_oracle AS (
        SELECT DISTINCT ON (po.protocol_id)
            po.protocol_id,
            po.oracle_id
        FROM protocol_oracle po
        ORDER BY po.protocol_id, po.from_block DESC, po.id DESC
    ),
    latest_prices AS (
        SELECT DISTINCT ON (otp.oracle_id, otp.token_id)
            otp.oracle_id,
            otp.token_id,
            otp.price_usd
        FROM onchain_token_price otp
        ORDER BY otp.oracle_id, otp.token_id,
                 otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
    )
    SELECT
        cp.chain_id,
        cp.receipt_token_id,
        cp.protocol_id,
        cp.receipt_token_address,
        cp.underlying_token_id,
        cp.underlying_token_address,
        cp.symbol,
        cp.underlying_symbol,
        cp.protocol_name,
        cp.balance,
        lp.price_usd AS underlying_price_usd
    FROM candidate_positions cp
    LEFT JOIN active_oracle ao ON ao.protocol_id = cp.protocol_id
    LEFT JOIN latest_prices lp ON lp.oracle_id = ao.oracle_id AND lp.token_id = cp.underlying_token_id
    ORDER BY cp.balance DESC
    LIMIT 1
    """
)


class PostgresRiskPositionResolver:
    """Resolve a single current position from the Postgres allocation state."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def resolve(self, asset_id: AssetId, prime_id: PrimeId) -> ResolvedRiskPosition:
        try:
            async with self._engine.connect() as conn:
                await conn.exec_driver_sql("SET LOCAL statement_timeout = '5s'")
                result = await conn.execute(
                    _RESOLVE_POSITION_SQL,
                    {"asset_id": asset_id, "proxy_hex": prime_id.hex},
                )
                row = result.fetchone()
        except SQLAlchemyError:
            logger.exception(
                "risk_position_resolver: DB error resolving asset_id=%d prime_id=%s",
                asset_id,
                prime_id,
            )
            raise

        if row is None:
            raise RiskPositionNotFoundError(asset_id, prime_id)

        balance = Decimal(str(row.balance))
        underlying_price = (
            Decimal(str(row.underlying_price_usd)) if row.underlying_price_usd is not None else None
        )
        usd_exposure = None
        if underlying_price is not None and is_aave_like_protocol(row.protocol_name):
            usd_exposure = balance * underlying_price

        return ResolvedRiskPosition(
            chain_id=row.chain_id,
            prime_id=prime_id,
            receipt_token_id=row.receipt_token_id,
            receipt_token_address="0x" + row.receipt_token_address,
            underlying_token_id=row.underlying_token_id,
            underlying_token_address="0x" + row.underlying_token_address,
            protocol_id=row.protocol_id,
            symbol=row.symbol,
            underlying_symbol=row.underlying_symbol,
            protocol_name=row.protocol_name,
            balance=balance,
            usd_exposure=usd_exposure,
        )
