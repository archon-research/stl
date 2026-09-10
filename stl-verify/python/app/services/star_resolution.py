"""Resolve an address to the prime it names.

Its own module because both reference services need it and neither owns it. The
two reference reads are served from one URL and differ only in which figures
they return, so they must not drift on which addresses name a prime — one
implementation is what guarantees that, and importing it from a sibling service
would have made this read as that sibling's rule.
"""

import logging

from app.domain.entities.allocation import EthAddress
from app.domain.prime_registry import prime_name_for
from app.ports.prime_directory import PrimeDirectory

logger = logging.getLogger(__name__)


async def star_for(proxy_address: EthAddress, primes: PrimeDirectory) -> str | None:
    """Name the prime ``proxy_address`` belongs to, or ``None`` if it names none.

    The contract answers first: it is the tracked set and costs no I/O. An
    address it does not index can still identify a prime — a vault address, or an
    ALM proxy during a chain onboarding, which holds positions before the
    contract is told about it. Self mode serves both, so reference mode must too:
    the same URL differs only in which figures it returns, never in which
    addresses it accepts.
    """
    star = prime_name_for(proxy_address)
    if star is not None:
        return star

    for prime in await primes.list_primes():
        if proxy_address in (prime.address, prime.prime_vault_address):
            return prime.name

    logger.info(
        "Address names no prime STL knows; no star to ask upstream for",
        extra={"proxy_address": str(proxy_address)},
    )
    return None
