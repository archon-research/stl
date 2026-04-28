from decimal import Decimal


class FixedAllocationShare:
    """Returns a pre-configured share value.

    Used for protocols where the share is already accounted for in the
    breakdown (e.g. Morpho, where the backed breakdown is vault-scoped).
    """

    def __init__(self, share: Decimal) -> None:
        self._share = share

    async def get_share(self) -> Decimal:
        return self._share
