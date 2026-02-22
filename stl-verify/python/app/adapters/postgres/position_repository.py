from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from web3 import Web3

from app.domain.entities.positions import AssetAmount, UserLatestPositions


class PostgresPositionRepository:
    """PostgreSQL implementation of PositionRepository.

    Queries the latest non-orphaned debt and collateral positions from the database.
    """

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]):
        self.sessionmaker = sessionmaker

    async def list_latest_user_positions(
        self, protocol_id: int, chain_id: int, limit: int
    ) -> list[UserLatestPositions]:
        """Retrieve latest positions for users in a protocol on a specific chain.

        Args:
            protocol_id: The protocol ID to query positions for
            chain_id: The chain ID to scope the query to
            limit: Maximum number of users to return (0 = unlimited)

        Returns:
            List of user positions with debt and collateral, sorted by user address
        """
        async with self.sessionmaker() as session:
            # Get the limited set of user IDs first to avoid fetching all positions
            user_ids = await self._query_limited_user_ids(session, protocol_id, chain_id, limit)
            if not user_ids:
                return []

            # Query latest debt positions for those users
            debt_rows = await self._query_latest_debt(session, protocol_id, chain_id, user_ids)

            # Query latest collateral positions for those users
            collateral_rows = await self._query_latest_collateral(session, protocol_id, chain_id, user_ids)

            # Aggregate by user address
            positions_map: dict[str, UserLatestPositions] = {}

            for row in debt_rows:
                user_addr = Web3.to_checksum_address("0x" + row["user_address"])
                if user_addr not in positions_map:
                    positions_map[user_addr] = UserLatestPositions(
                        user_address=user_addr,
                        chain_id=row["chain_id"],
                        debt=[],
                        collateral=[],
                    )
                positions_map[user_addr].debt.append(
                    AssetAmount(
                        token_address=Web3.to_checksum_address("0x" + row["token_address"]),
                        symbol=row["symbol"],
                        amount=int(row["amount"]),
                        decimals=row["decimals"],
                        chain_id=row["chain_id"],
                    )
                )

            for row in collateral_rows:
                user_addr = Web3.to_checksum_address("0x" + row["user_address"])
                if user_addr not in positions_map:
                    positions_map[user_addr] = UserLatestPositions(
                        user_address=user_addr,
                        chain_id=row["chain_id"],
                        debt=[],
                        collateral=[],
                    )
                positions_map[user_addr].collateral.append(
                    AssetAmount(
                        token_address=Web3.to_checksum_address("0x" + row["token_address"]),
                        symbol=row["symbol"],
                        amount=int(row["amount"]),
                        decimals=row["decimals"],
                        chain_id=row["chain_id"],
                    )
                )

            # Filter out users with no positions
            result = [pos for pos in positions_map.values() if pos.debt or pos.collateral]

            # Sort by user address
            result.sort(key=lambda x: x.user_address)

            return result

    async def _query_limited_user_ids(
        self, session: AsyncSession, protocol_id: int, chain_id: int, limit: int
    ) -> list[int]:
        """Return the IDs of users that have any position in the given protocol/chain.

        When limit > 0 only the first ``limit`` users (ordered by id) are returned.
        """
        query = text("""
            SELECT DISTINCT u.id
            FROM "user" u
            JOIN borrower b ON b.user_id = u.id
            JOIN block_states bs ON bs.number = b.block_number AND bs.version = b.block_version AND bs.chain_id = u.chain_id
            WHERE b.protocol_id = :protocol_id
                AND u.chain_id = :chain_id
                AND bs.is_orphaned = false
            UNION
            SELECT DISTINCT u.id
            FROM "user" u
            JOIN borrower_collateral c ON c.user_id = u.id
            JOIN block_states bs ON bs.number = c.block_number AND bs.version = c.block_version AND bs.chain_id = u.chain_id
            WHERE c.protocol_id = :protocol_id
                AND u.chain_id = :chain_id
                AND bs.is_orphaned = false
            ORDER BY id
        """)

        params: dict = {"protocol_id": protocol_id, "chain_id": chain_id}
        if limit > 0:
            query = text("""
                SELECT DISTINCT u.id
                FROM "user" u
                JOIN borrower b ON b.user_id = u.id
                JOIN block_states bs ON bs.number = b.block_number AND bs.version = b.block_version AND bs.chain_id = u.chain_id
                WHERE b.protocol_id = :protocol_id
                    AND u.chain_id = :chain_id
                    AND bs.is_orphaned = false
                UNION
                SELECT DISTINCT u.id
                FROM "user" u
                JOIN borrower_collateral c ON c.user_id = u.id
                JOIN block_states bs ON bs.number = c.block_number AND bs.version = c.block_version AND bs.chain_id = u.chain_id
                WHERE c.protocol_id = :protocol_id
                    AND u.chain_id = :chain_id
                    AND bs.is_orphaned = false
                ORDER BY id
                LIMIT :limit
            """)
            params["limit"] = limit

        result = await session.execute(query, params)
        return [row[0] for row in result]

    async def _query_latest_debt(
        self, session: AsyncSession, protocol_id: int, chain_id: int, user_ids: list[int]
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
                JOIN block_states bs ON bs.number = b.block_number AND bs.version = b.block_version AND bs.chain_id = :chain_id
                WHERE b.protocol_id = :protocol_id
                    AND b.user_id = ANY(:user_ids)
                    AND bs.is_orphaned = false
                ORDER BY b.user_id, b.token_id, b.block_number DESC, b.block_version DESC
            )
            SELECT
                encode(u.address, 'hex') as user_address,
                u.chain_id,
                encode(t.address, 'hex') as token_address,
                t.symbol,
                t.decimals,
                latest.amount
            FROM latest
            JOIN "user" u ON u.id = latest.user_id
            JOIN token t ON t.id = latest.token_id
            WHERE latest.amount <> '0'
        """)

        result = await session.execute(query, {"protocol_id": protocol_id, "chain_id": chain_id, "user_ids": user_ids})
        return [dict(row._mapping) for row in result]

    async def _query_latest_collateral(
        self, session: AsyncSession, protocol_id: int, chain_id: int, user_ids: list[int]
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
                JOIN block_states bs ON bs.number = c.block_number AND bs.version = c.block_version AND bs.chain_id = :chain_id
                WHERE c.protocol_id = :protocol_id
                    AND c.user_id = ANY(:user_ids)
                    AND bs.is_orphaned = false
                ORDER BY c.user_id, c.token_id, c.block_number DESC, c.block_version DESC
            )
            SELECT
                encode(u.address, 'hex') as user_address,
                u.chain_id,
                encode(t.address, 'hex') as token_address,
                t.symbol,
                t.decimals,
                latest.amount
            FROM latest
            JOIN "user" u ON u.id = latest.user_id
            JOIN token t ON t.id = latest.token_id
            WHERE latest.amount <> '0'
                AND latest.collateral_enabled = true
        """)

        result = await session.execute(query, {"protocol_id": protocol_id, "chain_id": chain_id, "user_ids": user_ids})
        return [dict(row._mapping) for row in result]
