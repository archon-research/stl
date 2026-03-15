from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.ports.morpho_repository import MorphoRepository as MorphoRepositoryPort


class MorphoRepository(MorphoRepositoryPort):
    """Postgres implementation of the Morpho repository."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def is_morpho_vault(self, vault_id: int) -> bool:
        """Return True if *vault_id* exists in the morpho_vault table."""
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text("SELECT 1 FROM morpho_vault WHERE id = :vault_id LIMIT 1"),
                {"vault_id": vault_id},
            )
            return result.fetchone() is not None
