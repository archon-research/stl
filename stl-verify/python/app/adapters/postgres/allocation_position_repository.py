from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.domain.entities.allocation import AllocationPosition, EthAddress, ReceiptTokenPosition, Star


class PostgresAllocationRepository:
    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    async def list_stars(self) -> list[Star]:
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
        return [Star(id="0x" + row.address, name=row.name, address="0x" + row.address) for row in result]

    async def get_star(self, address: EthAddress) -> Star | None:
        result = await self._conn.execute(
            text("""
                SELECT p.name, encode(ap.proxy_address, 'hex') AS address
                FROM allocation_position ap
                JOIN prime p ON p.id = ap.prime_id
                WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
                ORDER BY ap.block_number DESC
                LIMIT 1
            """),
            {"proxy_hex": address.hex},
        )
        row = result.fetchone()
        if row is None:
            return None
        return Star(id="0x" + row.address, name=row.name, address="0x" + row.address)

    async def list_receipt_token_positions(self, star_id: EthAddress) -> list[ReceiptTokenPosition]:
        proxy_hex = star_id.hex
        result = await self._conn.execute(
            text("""
                WITH latest_positions AS (
                    SELECT DISTINCT ON (ap.token_id, ap.proxy_address)
                        ap.token_id,
                        ap.balance
                    FROM allocation_position ap
                    JOIN prime p ON p.id = ap.prime_id
                    WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
                    ORDER BY ap.token_id, ap.proxy_address,
                             ap.block_number DESC, ap.block_version DESC, ap.log_index DESC
                )
                SELECT DISTINCT ON (rt.id)
                    rt.id                                        AS receipt_token_id,
                    rt.symbol                                    AS symbol,
                    ut.symbol                                    AS underlying_symbol,
                    pr.name                                      AS protocol_name,
                    lp.balance,
                    encode(rt.receipt_token_address, 'hex')      AS token_address
                FROM latest_positions lp
                JOIN token t ON t.id = lp.token_id
                JOIN receipt_token rt ON rt.underlying_token_id = t.id
                                      OR rt.receipt_token_address = t.address
                JOIN token ut ON ut.id = rt.underlying_token_id
                JOIN protocol pr ON pr.id = rt.protocol_id
                WHERE lp.balance > 0
                ORDER BY rt.id, lp.balance DESC
            """),
            {"proxy_hex": proxy_hex},
        )
        return [
            ReceiptTokenPosition(
                receipt_token_id=row.receipt_token_id,
                symbol=row.symbol,
                underlying_symbol=row.underlying_symbol,
                protocol_name=row.protocol_name,
                balance=Decimal(str(row.balance)),
                token_address="0x" + row.token_address if row.token_address else None,
            )
            for row in result
        ]

    _ALLOCATIONS_SQL = text("""
        SELECT DISTINCT ON
        (ap.chain_id, ap.token_id, ap.proxy_address, ap.block_number, ap.tx_hash, ap.log_index, ap.direction)
            ap.id,
            ap.chain_id,
            p.name,
            encode(ap.proxy_address, 'hex') AS proxy_address,
            encode(t.address, 'hex')        AS token_address,
            t.symbol                        AS token_symbol,
            t.decimals                      AS token_decimals,
            ap.balance,
            ap.scaled_balance,
            ap.block_number,
            ap.block_version,
            encode(ap.tx_hash, 'hex')       AS tx_hash,
            ap.log_index,
            ap.tx_amount,
            ap.direction,
            ap.created_at
        FROM allocation_position ap
        JOIN token t ON t.id = ap.token_id
        JOIN prime p ON p.id = ap.prime_id
        WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
          AND ap.block_number = COALESCE(
              :block_number,
              (SELECT MAX(block_number) FROM allocation_position
               WHERE proxy_address = decode(:proxy_hex, 'hex'))
          )
        ORDER BY ap.chain_id,
                 ap.token_id,
                 ap.proxy_address,
                 ap.block_number,
                 ap.tx_hash,
                 ap.log_index,
                 ap.direction,
                 ap.block_version DESC
    """)

    async def list_allocations_by_star(
        self, star_id: EthAddress, block_number: int | None = None
    ) -> list[AllocationPosition]:
        result = await self._conn.execute(
            self._ALLOCATIONS_SQL,
            {"proxy_hex": star_id.hex, "block_number": block_number},
        )
        return [
            AllocationPosition(
                id=row.id,
                chain_id=row.chain_id,
                name=row.name,
                proxy_address="0x" + row.proxy_address,
                token_address="0x" + row.token_address,
                token_symbol=row.token_symbol,
                token_decimals=row.token_decimals,
                balance=row.balance,
                scaled_balance=row.scaled_balance,
                block_number=row.block_number,
                block_version=row.block_version,
                tx_hash="0x" + row.tx_hash,
                log_index=row.log_index,
                tx_amount=row.tx_amount,
                direction=row.direction,
                created_at=row.created_at,
            )
            for row in result
        ]
