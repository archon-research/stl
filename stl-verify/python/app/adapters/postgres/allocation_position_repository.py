from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.allocation import EthAddress, Prime, ReceiptTokenPosition


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
    )
    SELECT DISTINCT ON (receipt_token_id)
        chain_id,
        receipt_token_id,
        receipt_token_address,
        underlying_token_id,
        underlying_token_address,
        symbol,
        underlying_symbol,
        protocol_name,
        balance
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
