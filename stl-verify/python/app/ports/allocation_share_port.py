from decimal import Decimal
from typing import Protocol


class AllocationSharePort(Protocol):
    """Return the fraction of a pool/vault that an allocation holds.

    Implementations vary by protocol:
    - Aave/Spark: spToken.balanceOf(wallet) / spToken.totalSupply() via RPC
    - Morpho: from indexed DB data (future)
    """

    async def get_share(self) -> Decimal:
        """Return a value in [0, 1] representing the allocator's share."""
        ...
