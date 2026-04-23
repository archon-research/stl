from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.domain.entities.allocation import EthAddress, Prime, ReceiptTokenPosition


class PostgresAllocationRepository:
    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    async def list_primes(self) -> list[Prime]:
        result = await self._conn.execute(
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
        result = await self._conn.execute(
            self._RECEIPT_TOKEN_POSITIONS_SQL,
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
