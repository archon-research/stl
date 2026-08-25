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

from app.domain.entities.allocation import EthAddress, as_address
from app.domain.entities.reference_risk_capital import (
    ReferenceAllocation,
    ReferencePrimeRiskCapital,
)
from app.ports.prime_directory import PrimeDirectory
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.reference_risk_capital import ReferenceRiskCapitalProvider
from app.services.star_resolution import star_for

logger = logging.getLogger(__name__)


class ReferenceRiskCapitalService:
    """Fetches and resolves a prime's upstream risk-capital snapshot."""

    def __init__(
        self,
        provider: ReferenceRiskCapitalProvider,
        receipt_tokens: ReceiptTokenLookup,
        primes: PrimeDirectory,
    ) -> None:
        self._provider = provider
        self._receipt_tokens = receipt_tokens
        self._primes = primes

    async def get(self, proxy_address: EthAddress) -> ReferencePrimeRiskCapital | None:
        """Return the upstream snapshot for the prime owning ``proxy_address``.

        ``None`` means no reference figures exist for this prime — either the
        address names no prime STL knows, or the monitor does not track that
        prime. Both are real answers about coverage, not failures, and neither
        may be served as zeros.
        """
        star = await star_for(proxy_address, self._primes)
        if star is None:
            return None

        snapshot = await self._provider.get_prime(star)
        if snapshot is None:
            return None
        return await self._resolve_allocations(snapshot)

    async def tracked_stars(self) -> frozenset[str]:
        """Every prime the monitor covers, lowercased."""
        return await self._provider.tracked_stars()

    async def _resolve_allocations(self, snapshot: ReferencePrimeRiskCapital) -> ReferencePrimeRiskCapital:
        resolved = await asyncio.gather(*(self._resolve(row) for row in snapshot.per_allocation))
        return dataclasses.replace(snapshot, per_allocation=tuple(resolved))

    async def _resolve(self, row: ReferenceAllocation) -> ReferenceAllocation:
        """Attach STL's receipt-token id to an upstream row.

        The chain is already resolved at the adapter boundary; only the registry
        join is left, and it is skipped structurally where it cannot succeed
        rather than issued and allowed to miss.
        """
        address = as_address(row.token_address)
        if row.chain_id is None or address is None:
            return row

        info = await self._receipt_tokens.get_by_chain_and_address(row.chain_id, address)
        return dataclasses.replace(row, receipt_token_id=info.receipt_token_id if info else None)
