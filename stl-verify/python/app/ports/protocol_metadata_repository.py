from typing import Protocol


class ProtocolMetadataRepository(Protocol):
    """Port for looking up protocol metadata needed for routing."""

    async def get_protocol_type(self, protocol_id: int) -> str | None:
        """Return the stored protocol_type for the given protocol ID, if any."""
        ...
