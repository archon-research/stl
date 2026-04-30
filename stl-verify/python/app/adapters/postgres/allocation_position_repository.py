from datetime import datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.allocation import EthAddress, Prime, ReceiptTokenPosition
from app.domain.entities.allocation_activity import AllocationActivityEvent


class PostgresAllocationRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def list_primes(self) -> list[Prime]:
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT DISTINCT ON (proxy_address)
                        p.name,
                        encode(proxy_address, 'hex') AS address
                    FROM allocation_position ap
                    JOIN prime p ON p.id = ap.prime_id
                    ORDER BY proxy_address, block_number DESC
                    """
                )
            )
            return [Prime(id="0x" + row.address, name=row.name, address="0x" + row.address) for row in result]

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        async with self._engine.connect() as conn:
            result = await conn.execute(
                _RECEIPT_TOKEN_POSITIONS_SQL,
                {"proxy_hex": prime_id.hex},
            )
            return [
                ReceiptTokenPosition(
                    chain_id=row.chain_id,
                    receipt_token_id=row.receipt_token_id,
                    receipt_token_address="0x" + row.receipt_token_address,
                    underlying_token_id=row.underlying_token_id,
                    underlying_token_address="0x" + row.underlying_token_address,
                    symbol=row.symbol,
                    underlying_symbol=row.underlying_symbol,
                    protocol_name=row.protocol_name,
                    balance=Decimal(str(row.balance)),
                    amount_usd=Decimal(str(row.amount_usd)) if row.amount_usd is not None else None,
                    latest_activity_at=row.latest_activity_at,
                )
                for row in result
            ]

    async def get_usd_exposure(self, receipt_token_id: int, prime_id: EthAddress) -> Decimal:
        """Return ``balance × price_usd`` for the prime's holding of a receipt token."""
        async with self._engine.connect() as conn:
            result = await conn.execute(
                _USD_EXPOSURE_SQL,
                {"receipt_token_id": receipt_token_id, "proxy_hex": prime_id.hex},
            )
            row = result.fetchone()

        if row is None:
            raise ValueError(f"no position or price found for receipt_token_id={receipt_token_id} prime_id={prime_id}")

        balance = Decimal(str(row.balance))
        price_usd = Decimal(str(row.price_usd))
        return balance * price_usd

    async def get_total_usd_exposure(self, prime_id: EthAddress) -> Decimal:
        """Return total priced USD exposure across all current receipt-token positions."""
        async with self._engine.connect() as conn:
            result = await conn.execute(_TOTAL_USD_EXPOSURE_SQL, {"proxy_hex": prime_id.hex})
            row = result.fetchone()

        if row is None or row.total_usd_exposure is None:
            return Decimal("0")

        return Decimal(str(row.total_usd_exposure))

    async def list_allocation_activity(
        self,
        *,
        prime_id: EthAddress | None = None,
        chain_id: int | None = None,
        protocol_name: str | None = None,
        action_type: str | None = None,
        token_symbol: str | None = None,
        tx_hash: str | None = None,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[AllocationActivityEvent]:
        params = {
            "prime_hex": prime_id.hex if prime_id else None,
            "chain_id": chain_id,
            "protocol_name": protocol_name,
            "action_type": action_type,
            "token_symbol": token_symbol,
            "tx_hash": tx_hash.removeprefix("0x") if tx_hash else None,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "limit": min(max(limit, 1), 1000),
        }

        async with self._engine.connect() as conn:
            result = await conn.execute(_ALLOCATION_ACTIVITY_SQL, params)

        return [
            AllocationActivityEvent(
                chain_id=row.chain_id,
                prime_id="0x" + row.prime_address,
                prime_name=row.prime_name,
                protocol_name=row.protocol_name,
                token_id=row.token_id,
                token_symbol=row.token_symbol,
                action_type=row.action_type,
                tx_amount=Decimal(str(row.tx_amount)),
                balance=Decimal(str(row.balance)),
                tx_hash=("0x" + row.tx_hash) if row.tx_hash else "0x",
                log_index=row.log_index,
                block_number=row.block_number,
                block_version=row.block_version,
                created_at=row.created_at,
            )
            for row in result
        ]


# The allocation_position rows are event-oriented, so we first take the
# latest row per (token_id, proxy_address) in ``latest_positions``. Each
# remaining allocation token is then matched against receipt_token two
# ways: by underlying_token_id (position recorded in the underlying) and
# by receipt_token_address (position recorded in the receipt token
# itself).
_RECEIPT_TOKEN_POSITIONS_SQL = text("""
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
    latest_prices AS (
        SELECT DISTINCT ON (otp.token_id)
            otp.token_id,
            otp.price_usd
        FROM onchain_token_price otp
        ORDER BY otp.token_id, otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
    ),
    latest_activity AS (
        SELECT DISTINCT ON (receipt_token_id)
            receipt_token_id,
            created_at AS latest_activity_at
        FROM (
            SELECT
                rt.id AS receipt_token_id,
                ap.created_at
            FROM allocation_position ap
            JOIN token t ON t.id = ap.token_id
            JOIN receipt_token rt ON rt.underlying_token_id = t.id
            JOIN protocol pr ON pr.id = rt.protocol_id AND pr.chain_id = ap.chain_id
            WHERE ap.proxy_address = decode(:proxy_hex, 'hex')

            UNION ALL

            SELECT
                rt.id AS receipt_token_id,
                ap.created_at
            FROM allocation_position ap
            JOIN token t ON t.id = ap.token_id
            JOIN receipt_token rt ON rt.receipt_token_address = t.address
            JOIN protocol pr ON pr.id = rt.protocol_id AND pr.chain_id = ap.chain_id
            WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
        ) activity_events
        ORDER BY receipt_token_id, created_at DESC
    )
    SELECT DISTINCT ON (receipt_token_id)
        combined.chain_id,
        combined.receipt_token_id,
        combined.receipt_token_address,
        combined.underlying_token_id,
        combined.underlying_token_address,
        combined.symbol,
        combined.underlying_symbol,
        combined.protocol_name,
        combined.balance,
        (combined.balance * lp.price_usd) AS amount_usd,
        la.latest_activity_at
    FROM (
        SELECT
            lp.chain_id                                  AS chain_id,
            rt.id                                        AS receipt_token_id,
            encode(rt.receipt_token_address, 'hex')      AS receipt_token_address,
            ut.id                                        AS underlying_token_id,
            encode(ut.address, 'hex')                    AS underlying_token_address,
            rt.symbol                                    AS symbol,
            ut.symbol                                    AS underlying_symbol,
            pr.name                                      AS protocol_name,
            lp.balance
        FROM latest_positions lp
        JOIN token t ON t.id = lp.token_id
        JOIN receipt_token rt ON rt.underlying_token_id = t.id
        JOIN token ut ON ut.id = rt.underlying_token_id
        JOIN protocol pr ON pr.id = rt.protocol_id
        WHERE lp.balance > 0

        UNION ALL

        SELECT
            lp.chain_id                                  AS chain_id,
            rt.id                                        AS receipt_token_id,
            encode(rt.receipt_token_address, 'hex')      AS receipt_token_address,
            ut.id                                        AS underlying_token_id,
            encode(ut.address, 'hex')                    AS underlying_token_address,
            rt.symbol                                    AS symbol,
            ut.symbol                                    AS underlying_symbol,
            pr.name                                      AS protocol_name,
            lp.balance
        FROM latest_positions lp
        JOIN token t ON t.id = lp.token_id
        JOIN receipt_token rt ON rt.receipt_token_address = t.address
        JOIN token ut ON ut.id = rt.underlying_token_id
        JOIN protocol pr ON pr.id = rt.protocol_id
        WHERE lp.balance > 0
    ) combined
    LEFT JOIN latest_prices lp ON lp.token_id = combined.underlying_token_id
    LEFT JOIN latest_activity la ON la.receipt_token_id = combined.receipt_token_id
    ORDER BY receipt_token_id, balance DESC
""")


_USD_EXPOSURE_SQL = text("""
WITH latest_balance AS (
    SELECT balance
    FROM (
        SELECT DISTINCT ON (ap.token_id)
            ap.balance
        FROM allocation_position ap
        JOIN receipt_token rt ON rt.id = :receipt_token_id
        JOIN token t ON t.id = ap.token_id
            AND (t.id = rt.underlying_token_id OR t.address = rt.receipt_token_address)
        JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = ap.chain_id
        WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
          AND ap.balance > 0
        ORDER BY ap.token_id,
                 ap.block_number DESC, ap.block_version DESC,
                 ap.processing_version DESC, ap.log_index DESC
    ) sub
    ORDER BY balance DESC
    LIMIT 1
),
latest_price AS (
    SELECT DISTINCT ON (otp.token_id)
        otp.price_usd
    FROM onchain_token_price otp
    JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
    JOIN receipt_token rt ON rt.protocol_id = po.protocol_id AND rt.id = :receipt_token_id
    WHERE otp.token_id = rt.underlying_token_id
    ORDER BY otp.token_id, otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
)
SELECT lb.balance, lp.price_usd
FROM latest_balance lb
CROSS JOIN latest_price lp
""")


_TOTAL_USD_EXPOSURE_SQL = text("""
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
receipt_positions AS (
    SELECT DISTINCT ON (receipt_token_id)
        receipt_token_id,
        underlying_token_id,
        balance
    FROM (
        SELECT
            rt.id AS receipt_token_id,
            rt.underlying_token_id,
            lp.balance
        FROM latest_positions lp
        JOIN token t ON t.id = lp.token_id
        JOIN receipt_token rt ON rt.underlying_token_id = t.id
        JOIN protocol pr ON pr.id = rt.protocol_id AND pr.chain_id = lp.chain_id
        WHERE lp.balance > 0

        UNION ALL

        SELECT
            rt.id AS receipt_token_id,
            rt.underlying_token_id,
            lp.balance
        FROM latest_positions lp
        JOIN token t ON t.id = lp.token_id
        JOIN receipt_token rt ON rt.receipt_token_address = t.address
        JOIN protocol pr ON pr.id = rt.protocol_id AND pr.chain_id = lp.chain_id
        WHERE lp.balance > 0
    ) combined
    ORDER BY receipt_token_id, balance DESC
),
latest_prices AS (
    SELECT DISTINCT ON (otp.token_id)
        otp.token_id,
        otp.price_usd
    FROM onchain_token_price otp
    ORDER BY otp.token_id, otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
)
SELECT COALESCE(SUM(rp.balance * lp.price_usd), 0) AS total_usd_exposure
FROM receipt_positions rp
LEFT JOIN latest_prices lp ON lp.token_id = rp.underlying_token_id
""")


_ALLOCATION_ACTIVITY_SQL = text("""
SELECT
    ap.chain_id,
    encode(ap.proxy_address, 'hex') AS prime_address,
    p.name AS prime_name,
    protocol_match.protocol_name,
    ap.token_id,
    t.symbol AS token_symbol,
    ap.direction AS action_type,
    ap.tx_amount,
    ap.balance,
    encode(ap.tx_hash, 'hex') AS tx_hash,
    ap.log_index,
    ap.block_number,
    ap.block_version,
    ap.created_at
FROM allocation_position ap
JOIN prime p ON p.id = ap.prime_id
JOIN token t ON t.id = ap.token_id
LEFT JOIN LATERAL (
    SELECT pr.name AS protocol_name, 1 AS match_priority
    FROM receipt_token rt
    JOIN protocol pr ON pr.id = rt.protocol_id
    WHERE pr.chain_id = ap.chain_id
      AND rt.receipt_token_address = t.address

    UNION ALL

    SELECT pr.name AS protocol_name, 2 AS match_priority
    FROM receipt_token rt
    JOIN protocol pr ON pr.id = rt.protocol_id
    WHERE pr.chain_id = ap.chain_id
      AND rt.underlying_token_id = t.id
    ORDER BY match_priority
    LIMIT 1
) AS protocol_match ON TRUE
WHERE (CAST(:prime_hex AS TEXT) IS NULL OR ap.proxy_address = decode(CAST(:prime_hex AS TEXT), 'hex'))
    AND ap.direction IS NOT NULL
    AND ap.tx_amount IS NOT NULL
    AND ap.balance IS NOT NULL
    AND ap.log_index IS NOT NULL
    AND ap.block_number IS NOT NULL
    AND ap.block_version IS NOT NULL
    AND ap.created_at IS NOT NULL
    AND (CAST(:chain_id AS INTEGER) IS NULL OR ap.chain_id = CAST(:chain_id AS INTEGER))
    AND (CAST(:protocol_name AS TEXT) IS NULL OR LOWER(COALESCE(protocol_match.protocol_name, '')) LIKE '%' || LOWER(CAST(:protocol_name AS TEXT)) || '%')
    AND (CAST(:action_type AS TEXT) IS NULL OR LOWER(COALESCE(ap.direction::text, '')) = LOWER(CAST(:action_type AS TEXT)))
    AND (CAST(:token_symbol AS TEXT) IS NULL OR LOWER(COALESCE(t.symbol, '')) LIKE '%' || LOWER(CAST(:token_symbol AS TEXT)) || '%')
    AND (CAST(:tx_hash AS TEXT) IS NULL OR encode(ap.tx_hash, 'hex') = LOWER(CAST(:tx_hash AS TEXT)))
    AND (CAST(:from_timestamp AS TIMESTAMP) IS NULL OR ap.created_at >= CAST(:from_timestamp AS TIMESTAMP))
    AND (CAST(:to_timestamp AS TIMESTAMP) IS NULL OR ap.created_at <= CAST(:to_timestamp AS TIMESTAMP))
ORDER BY ap.created_at DESC, ap.block_number DESC, ap.block_version DESC, ap.log_index DESC
LIMIT :limit
""")
