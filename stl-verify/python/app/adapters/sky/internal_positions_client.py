"""HTTP adapter for Sky's internal balance-sheet feed.

One upstream route backs this client, current-snapshot only:

- ``/internal/allocations/?prime={star}`` — every position the star holds.

A different host from the Star monitor
(:mod:`app.adapters.sky.reference_risk_capital_client`) and a different
question. That one reports the risk-capital breakdown — 11 priced positions for
spark, summing to its ``total_exposure`` of 2.17bn. This one reports the balance
sheet — 59 positions summing to 3.31bn, matching the prime's own ``assets``.
The two are not interchangeable; see
:mod:`app.domain.entities.reference_position`.

**Unknown stars answer ``200`` with an empty list**, not ``404``, so an empty
result here cannot be told apart from a prime that genuinely holds nothing.
Coverage must be established before calling: the service gates on the Star
monitor's tracked set, which is what decides whether reference data exists at
all.

The envelope, the transport and the figure parsing are shared with the Star
monitor's client; see :mod:`app.adapters.sky.envelope`.

``wallet_address`` and ``allocation_type`` are served by this route and
deliberately not read; :class:`~app.domain.entities.reference_position.ReferencePosition`
records why, and each is one ``required_text``/``text`` call to reinstate.
"""

import httpx

from app.adapters.sky.envelope import SkyEnvelope
from app.domain.chain_names import CHAIN_ID_TO_NAME
from app.domain.entities.reference_position import ReferencePosition

# Prefixes every failure this client raises, so an outage names the host that
# produced it rather than "Sky".
_SOURCE = "Sky internal feed"

# Upstream paginates at 20 by default — spark alone holds 59 positions, so an
# unset limit silently serves a third of them. Asked for explicitly, and the
# reported total is checked against what arrives.
_PAGE_LIMIT = 1000

# This host spells networks its own way — "ethereum" where the axis-synome
# contract and the allocation trackers say "mainnet". The same mapping the Star
# monitor client applies, repeated rather than shared: they are two vendors'
# vocabularies that happen to agree today, and a change to one must not silently
# move the other.
_NETWORK_TO_CHAIN_ID: dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "unichain": 130,
    "base": 8453,
    "arbitrum": 42161,
    "avalanche": 43114,
}


class SkyInternalPositionsClient:
    """Reads a star's upstream balance sheet."""

    def __init__(self, base_url: str, client: httpx.AsyncClient | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._upstream = SkyEnvelope(source=_SOURCE, page_limit=_PAGE_LIMIT, client=client)

    async def get_positions(self, star: str) -> tuple[ReferencePosition, ...]:
        url = f"{self._base_url}/allocations/?prime={star}&limit={_PAGE_LIMIT}"
        data = await self._upstream.get_data(url)
        rows = self._upstream.require_results(data, url=url)
        self._upstream.require_full_page(data, len(rows), url=url)
        return tuple(
            _position(row, star=star, upstream=self._upstream)
            for row in _by_assets_desc(rows, star=star, upstream=self._upstream)
        )


def _by_assets_desc(rows: list, *, star: str, upstream: SkyEnvelope) -> list:
    """Sort largest-holding first, matching how every other allocation list is served."""
    return sorted(rows, key=lambda row: upstream.decimal(row, "assets", star=star), reverse=True)


def _position(row: dict, *, star: str, upstream: SkyEnvelope) -> ReferencePosition:
    network = upstream.required_text(row, "network", star=star)
    chain_id = _NETWORK_TO_CHAIN_ID.get(network)
    return ReferencePosition(
        protocol_name=upstream.required_text(row, "protocol", star=star),
        network=network,
        symbol=upstream.required_text(row, "token_symbol", star=star),
        name=upstream.text(row, "token_name"),
        token_address=upstream.required_text(row, "address", star=star),
        assets_usd=upstream.decimal(row, "assets", star=star),
        allocated_assets_usd=upstream.optional_decimal(row, "allocated_assets", star=star),
        idle_assets_usd=upstream.optional_decimal(row, "idle_assets", star=star),
        chain_id=chain_id,
        chain=CHAIN_ID_TO_NAME.get(chain_id) if chain_id is not None else None,
    )
