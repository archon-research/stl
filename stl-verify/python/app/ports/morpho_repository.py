from typing import Protocol


class MorphoRepository(Protocol):
    """Port for querying Morpho-specific data."""

    async def is_morpho_vault(self, vault_id: int) -> bool:
        """Return True if *vault_id* refers to an existing Morpho vault."""
        ...
