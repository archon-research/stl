"""Reference (upstream Star monitor) risk capital for a prime.

The counterpart to :class:`~app.services.prime_risk_capital_service.PrimeRiskCapitalService`:
same question, upstream's answer. It resolves the queried ALM proxy to a star
name via the in-memory axis-synome registry — no on-chain work and no model
run — then enriches upstream's breakdown with STL's own receipt-token ids so
the two provenances can be joined row by row.
"""

import asyncio
import dataclasses
import logging

from app.domain.chain_names import CHAIN_ID_TO_NAME, UPSTREAM_NETWORK_TO_CHAIN_ID
from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_risk_capital import (
    ReferenceAllocation,
    ReferencePrimeRiskCapital,
)
from app.domain.prime_registry import prime_name_for
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.reference_risk_capital import ReferenceRiskCapitalProvider

logger = logging.getLogger(__name__)


class ReferenceRiskCapitalService:
    """Fetches and resolves a prime's upstream risk-capital snapshot."""

    def __init__(self, provider: ReferenceRiskCapitalProvider, receipt_tokens: ReceiptTokenLookup) -> None:
        self._provider = provider
        self._receipt_tokens = receipt_tokens

    async def get(self, proxy_address: EthAddress) -> ReferencePrimeRiskCapital | None:
        """Return the upstream snapshot for the prime owning ``proxy_address``.

        ``None`` means no reference figures exist for this prime — either the
        axis-synome contract does not place the proxy under a star, or the
        monitor does not track that star. Both are real answers about coverage,
        not failures, and neither may be served as zeros.
        """
        star = prime_name_for(proxy_address)
        if star is None:
            logger.info(
                "Proxy is absent from the axis-synome contract; no star to ask the monitor for",
                extra={"proxy_address": str(proxy_address)},
            )
            return None

        snapshot = await self._provider.get_prime(star)
        if snapshot is None:
            return None
        return await self._resolve_allocations(snapshot)

    async def _resolve_allocations(self, snapshot: ReferencePrimeRiskCapital) -> ReferencePrimeRiskCapital:
        resolved = await asyncio.gather(*(self._resolve(row) for row in snapshot.per_allocation))
        return dataclasses.replace(snapshot, per_allocation=tuple(resolved))

    async def _resolve(self, row: ReferenceAllocation) -> ReferenceAllocation:
        """Attach STL's receipt-token id and internal chain name to an upstream row."""
        chain_id = UPSTREAM_NETWORK_TO_CHAIN_ID.get(row.network)
        if chain_id is None:
            return row

        chain = CHAIN_ID_TO_NAME.get(chain_id)
        address = _as_address(row.token_address)
        if address is None:
            # A Uniswap V4 position identifies itself by 32-byte pool id in the
            # address field, so there is nothing to look up. Gated here rather
            # than left to fail in the repository, which would read as a missing
            # token instead of a value that was never an address.
            return dataclasses.replace(row, chain=chain)

        info = await self._receipt_tokens.get_by_chain_and_address(chain_id, address)
        return dataclasses.replace(row, chain=chain, receipt_token_id=info.receipt_token_id if info else None)


def _as_address(value: str) -> EthAddress | None:
    """Return ``value`` as an address, or ``None`` if it is not one."""
    try:
        return EthAddress(value)
    except ValueError:
        return None
