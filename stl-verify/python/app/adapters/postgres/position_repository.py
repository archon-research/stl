from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.entities.positions import AssetAmount, UserLatestPositions
from app.ports.position_repository import PositionRepository


class PostgresPositionRepository:
    """PostgreSQL implementation of PositionRepository.
    
    Queries the latest non-orphaned debt and collateral positions from the database.
    """

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]):
        self.sessionmaker = sessionmaker

    async def list_latest_user_positions(
        self, protocol_id: int, limit: int
    ) -> list[UserLatestPositions]:
        """Retrieve latest positions for users in a protocol.
        
        Args:
            protocol_id: The protocol ID to query positions for
            limit: Maximum number of users to return (0 = unlimited)
            
        Returns:
            List of user positions with debt and collateral, sorted by user address
        """
        async with self.sessionmaker() as session:
            # Query latest debt positions
            debt_rows = await self._query_latest_debt(session, protocol_id)
            
            # Query latest collateral positions
            collateral_rows = await self._query_latest_collateral(session, protocol_id)
            
            # Aggregate by user address
            positions_map: dict[str, UserLatestPositions] = {}
            
            for row in debt_rows:
                user_addr = row["user_address"]
                if user_addr not in positions_map:
                    positions_map[user_addr] = UserLatestPositions(
                        user_address=user_addr, debt=[], collateral=[]
                    )
                positions_map[user_addr].debt.append(
                    AssetAmount(
                        token_address=row["token_address"],
                        symbol=row["symbol"],
                        amount=int(row["amount"]),
                    )
                )
            
            for row in collateral_rows:
                user_addr = row["user_address"]
                if user_addr not in positions_map:
                    positions_map[user_addr] = UserLatestPositions(
                        user_address=user_addr, debt=[], collateral=[]
                    )
                positions_map[user_addr].collateral.append(
                    AssetAmount(
                        token_address=row["token_address"],
                        symbol=row["symbol"],
                        amount=int(row["amount"]),
                    )
                )
            
            # Filter out users with no positions
            result = [
                pos for pos in positions_map.values()
                if pos.debt or pos.collateral
            ]
            
            # Sort by user address
            result.sort(key=lambda x: x.user_address)
            
            # Apply limit
            if limit > 0:
                result = result[:limit]
            
            return result

    async def _query_latest_debt(
        self, session: AsyncSession, protocol_id: int
    ) -> list[dict]:
        """Query latest debt positions from non-orphaned blocks."""
        query = text("""
            WITH latest AS (
                SELECT DISTINCT ON (b.user_id, b.token_id)
                    b.user_id,
                    b.token_id,
                    b.amount,
                    b.block_number,
                    b.block_version
                FROM borrower b
                JOIN block_states bs ON bs.number = b.block_number AND bs.version = b.block_version
                WHERE b.protocol_id = :protocol_id
                    AND bs.is_orphaned = false
                ORDER BY b.user_id, b.token_id, b.block_number DESC, b.block_version DESC
            )
            SELECT
                encode(u.address, 'hex') as user_address,
                encode(t.address, 'hex') as token_address,
                t.symbol,
                latest.amount
            FROM latest
            JOIN "user" u ON u.id = latest.user_id
            JOIN token t ON t.id = latest.token_id
            WHERE latest.amount <> '0'
        """)
        
        result = await session.execute(query, {"protocol_id": protocol_id})
        return [dict(row._mapping) for row in result]

    async def _query_latest_collateral(
        self, session: AsyncSession, protocol_id: int
    ) -> list[dict]:
        """Query latest collateral positions from non-orphaned blocks."""
        query = text("""
            WITH latest AS (
                SELECT DISTINCT ON (c.user_id, c.token_id)
                    c.user_id,
                    c.token_id,
                    c.amount,
                    c.collateral_enabled,
                    c.block_number,
                    c.block_version
                FROM borrower_collateral c
                JOIN block_states bs ON bs.number = c.block_number AND bs.version = c.block_version
                WHERE c.protocol_id = :protocol_id
                    AND bs.is_orphaned = false
                ORDER BY c.user_id, c.token_id, c.block_number DESC, c.block_version DESC
            )
            SELECT
                encode(u.address, 'hex') as user_address,
                encode(t.address, 'hex') as token_address,
                t.symbol,
                latest.amount
            FROM latest
            JOIN "user" u ON u.id = latest.user_id
            JOIN token t ON t.id = latest.token_id
            WHERE latest.amount <> '0'
                AND latest.collateral_enabled = true
        """)
        
        result = await session.execute(query, {"protocol_id": protocol_id})
        return [dict(row._mapping) for row in result]
