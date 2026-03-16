from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.domain.entities.allocation import AllocationPosition, EthAddress, Star


class PostgresAllocationRepository:
    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    async def list_stars(self) -> list[Star]:
        result = await self._conn.execute(
            text(
                """
                SELECT DISTINCT ON (proxy_address)
                    star,
                    encode(proxy_address, 'hex') AS address
                FROM allocation_position
                ORDER BY proxy_address, block_number DESC
                """
            )
        )
        return [Star(id="0x" + row.address, name=row.star, address="0x" + row.address) for row in result]

    async def list_allocations_by_star(
        self, star_id: EthAddress, block_number: int | None = None
    ) -> list[AllocationPosition]:
        proxy_hex = star_id.hex
        if block_number is None:
            block_filter = "ap.block_number = (SELECT MAX(block_number) FROM allocation_position WHERE proxy_address = decode(:proxy_hex, 'hex'))"  # noqa: E501
            params: dict = {"proxy_hex": proxy_hex}
        else:
            block_filter = "ap.block_number = :block_number"
            params = {"proxy_hex": proxy_hex, "block_number": block_number}

        result = await self._conn.execute(
            text(
                f"""
                SELECT DISTINCT ON
                (ap.chain_id, ap.token_id, ap.proxy_address, ap.block_number, ap.tx_hash, ap.log_index, ap.direction)
                    ap.id,
                    ap.chain_id,
                    ap.star,
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
                WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
                  AND {block_filter}
                ORDER BY ap.chain_id,
                         ap.token_id,
                         ap.proxy_address,
                         ap.block_number,
                         ap.tx_hash,
                         ap.log_index,
                         ap.direction,
                         ap.block_version DESC
                """
            ),
            params,
        )
        return [
            AllocationPosition(
                id=row.id,
                chain_id=row.chain_id,
                star=row.star,
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
