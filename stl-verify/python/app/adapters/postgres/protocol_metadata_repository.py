from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.ports.protocol_metadata_repository import (
    ProtocolMetadataRepository as ProtocolMetadataRepositoryPort,
)


class ProtocolMetadataRepository(ProtocolMetadataRepositoryPort):
    """Postgres implementation for protocol metadata lookups."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_protocol_type(self, protocol_id: int) -> str | None:
        """Return the protocol_type for the requested protocol, if present."""
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text("SELECT protocol_type FROM protocol WHERE id = :protocol_id LIMIT 1"),
                {"protocol_id": protocol_id},
            )
            row = result.fetchone()

        if row is None:
            return None

        return row.protocol_type
