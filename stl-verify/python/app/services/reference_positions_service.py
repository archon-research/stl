"""Reference (upstream) balance-sheet positions for a prime.

The allocation-list counterpart to
:class:`~app.services.reference_risk_capital_service.ReferenceRiskCapitalService`.
That service answers "what does Sky say this prime's *risk capital* is"; this
one answers "what does Sky say this prime *holds*". They read different hosts
and different quantities — see
:mod:`app.domain.entities.reference_position`.

Coverage is still decided by the Star monitor's tracked set, not by this feed.
The internal feed answers an unknown star with ``200`` and an empty list, so it
cannot distinguish "not covered" from "holds nothing"; the monitor's list can,
and it is already the thing that decides whether a prime has reference figures
at all. Keeping one answer to that question stops the allocation list and the
risk-capital card disagreeing about whether a prime is covered.
"""

import asyncio
import dataclasses
import logging

from app.domain.entities.allocation import EthAddress, as_address
from app.domain.entities.reference_position import ReferencePosition
from app.domain.prime_registry import prime_name_for
from app.ports.prime_directory import PrimeDirectory
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.reference_positions import ReferencePositionProvider
from app.ports.reference_risk_capital import ReferenceRiskCapitalProvider

logger = logging.getLogger(__name__)


class ReferencePositionsService:
    """Fetches and resolves a prime's upstream balance sheet."""

    def __init__(
        self,
        positions: ReferencePositionProvider,
        coverage: ReferenceRiskCapitalProvider,
        receipt_tokens: ReceiptTokenLookup,
        primes: PrimeDirectory,
    ) -> None:
        self._positions = positions
        self._coverage = coverage
        self._receipt_tokens = receipt_tokens
        self._primes = primes

    async def get(self, proxy_address: EthAddress) -> tuple[ReferencePosition, ...] | None:
        """Return the upstream positions for the prime owning ``proxy_address``.

        ``None`` means no reference data exists for this prime — either the
        address names no prime STL knows, or the monitor does not cover that
        prime. Both are real answers about coverage, not failures, and neither
        may be served as an empty list: an empty list is a prime that holds
        nothing, which is a different claim.
        """
        star = await self._star_for(proxy_address)
        if star is None:
            return None

        if star.strip().lower() not in await self._coverage.tracked_stars():
            logger.info(
                "Prime is not tracked by the upstream Star monitor; no reference positions",
                extra={"star": star},
            )
            return None

        positions = await self._positions.get_positions(star)
        return await self._resolve(positions)

    async def _star_for(self, proxy_address: EthAddress) -> str | None:
        """Name the prime ``proxy_address`` belongs to, or ``None`` if it names none.

        The contract answers first: it is the tracked set and costs no I/O. An
        address it does not index can still identify a prime — a vault address,
        or an ALM proxy during a chain onboarding, which holds positions before
        the contract is told about it. Self mode serves both, so reference mode
        must too: the same URL differs only in which figures it returns, never
        in which addresses it accepts.
        """
        star = prime_name_for(proxy_address)
        if star is not None:
            return star

        for prime in await self._primes.list_primes():
            if proxy_address in (prime.address, prime.prime_vault_address):
                return prime.name

        logger.info(
            "Address names no prime STL knows; no star to ask upstream for",
            extra={"proxy_address": str(proxy_address)},
        )
        return None

    async def _resolve(self, positions: tuple[ReferencePosition, ...]) -> tuple[ReferencePosition, ...]:
        return tuple(await asyncio.gather(*(self._resolve_one(row) for row in positions)))

    async def _resolve_one(self, row: ReferencePosition) -> ReferencePosition:
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
