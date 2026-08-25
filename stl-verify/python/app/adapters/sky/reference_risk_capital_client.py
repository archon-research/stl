"""HTTP adapter for Sky's Star Agents Risk Capital & Requirements Monitor.

Three upstream routes back this client, all current-snapshot only:

- ``/star-monitoring/risk-capital/primes/`` — every tracked prime, one call.
- ``/star-monitoring/risk-capital/primes/{star}/`` — one prime's totals.
- ``/star-monitoring/risk-capital/primes/{star}/allocations/`` — its breakdown.

The list route is fetched first to decide whether a prime is tracked at all.
That is not redundant: the detail route answers an unknown star with ``500``,
which is indistinguishable from a genuine upstream fault, so asking it directly
would conflate "prime not covered" with "monitor is down".

Upstream query parameters are not trustworthy. ``?days_ago=``, ``?date=`` and
``?order=`` are all accepted and silently ignored — verified by byte-identical
responses across values — so ordering is imposed here rather than requested,
and no date filter is ever sent.

The envelope, the transport and the figure parsing are shared with the internal
balance-sheet feed; see :mod:`app.adapters.sky.envelope`.
"""

import asyncio
import logging
from decimal import Decimal

import httpx

from app.adapters.sky.envelope import SkyEnvelope
from app.domain.chain_names import CHAIN_ID_TO_NAME
from app.domain.entities.reference_risk_capital import (
    ReferenceAllocation,
    ReferencePrimeRiskCapital,
)

logger = logging.getLogger(__name__)

# Prefixes every failure this client raises, so an outage names the host that
# produced it rather than "Sky".
_SOURCE = "Star monitor"

# Upstream paginates at 20 by default. Asked for explicitly, and the reported
# total is checked against what arrives, so a set outgrowing the page fails
# rather than silently losing rows.
_PAGE_LIMIT = 500

# Upstream reports the comparable capital-risk ratio as a 0-1 fraction
# (confirmed: crr == rrc / exposure exactly). Every consumer here reads a 0-100
# percentage, so the rescale happens once, at this boundary.
_FRACTION_TO_PCT = Decimal("100")

# The monitor spells networks its own way — "ethereum" where the axis-synome
# contract and the allocation trackers say "mainnet". Translated here with the
# other upstream encodings so no consumer has to know the vendor's vocabulary.
_NETWORK_TO_CHAIN_ID: dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "unichain": 130,
    "base": 8453,
    "arbitrum": 42161,
    "avalanche": 43114,
}


class SkyReferenceRiskCapitalClient:
    """Reads prime risk-capital snapshots from the upstream Star monitor."""

    def __init__(self, base_url: str, client: httpx.AsyncClient | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._upstream = SkyEnvelope(source=_SOURCE, page_limit=_PAGE_LIMIT, client=client)

    async def get_prime(self, star: str) -> ReferencePrimeRiskCapital | None:
        """Return ``star``'s upstream snapshot, or ``None`` if the monitor does not track it."""
        if star.strip().lower() not in await self.tracked_stars():
            logger.info(
                "Prime is not tracked by the upstream Star monitor; no reference data",
                extra={"star": star, "upstream_url": self._base_url},
            )
            return None

        # Totals and breakdown are two separately-computed live snapshots.
        # Issued concurrently to narrow the instant between them; they still
        # reconcile only to ~1e-6 relative, so neither is derived from the other.
        detail, allocations = await asyncio.gather(
            self._upstream.get_data(f"{self._base_url}/primes/{star}/"),
            self._upstream.get_data(f"{self._base_url}/primes/{star}/allocations/?limit={_PAGE_LIMIT}"),
        )
        return _build_snapshot(star, detail, allocations, upstream=self._upstream)

    async def tracked_stars(self) -> frozenset[str]:
        url = f"{self._base_url}/primes/?limit={_PAGE_LIMIT}"
        data = await self._upstream.get_data(url)
        results = self._upstream.require_results(data, url=url)
        if not results:
            # Every prime would read as untracked, and each would be served as a
            # 404 "not covered" — an outage wearing the shape of a real answer.
            raise self._upstream.failure(f"listed no primes at all: {url}")
        self._upstream.require_full_page(data, len(results), url=url)

        stars = set()
        for index, row in enumerate(results):
            star = str(row.get("star") or "").strip().lower()
            if not star:
                raise self._upstream.failure(f"listed a prime with no name at row {index}: {url}")
            stars.add(star)
        return frozenset(stars)


def _build_snapshot(star: str, detail: dict, allocations: dict, *, upstream: SkyEnvelope) -> ReferencePrimeRiskCapital:
    url = f"allocations/{star}"
    rows = upstream.require_results(allocations, url=url)
    upstream.require_full_page(allocations, len(rows), url=url)

    exposure = upstream.decimal(detail, "total_exposure", star=star)
    if not rows and exposure != 0:
        # The two routes are separate snapshots, so an empty breakdown beside a
        # live total is upstream disagreeing with itself — and serving it would
        # publish "this prime holds nothing" against real exposure.
        raise upstream.failure(f"reported exposure {exposure} for prime '{star}' but an empty breakdown")

    return ReferencePrimeRiskCapital(
        star=star,
        exposure_usd=exposure,
        required_risk_capital_usd=upstream.decimal(detail, "total_rrc", star=star),
        total_risk_capital_usd=upstream.decimal(detail, "total_rc", star=star),
        encumbrance_ratio=upstream.optional_decimal(detail, "encumbrance_ratio", star=star),
        exposure_share=upstream.decimal(detail, "total_exposure_share", star=star),
        junior_risk_capital_usd=upstream.decimal(detail, "total_jrc", star=star),
        senior_risk_capital_usd=upstream.decimal(detail, "total_src", star=star),
        internal_junior_risk_capital_usd=upstream.decimal(detail, "internal_jrc", star=star),
        external_junior_risk_capital_usd=upstream.decimal(detail, "external_jrc", star=star),
        tokenized_junior_risk_capital_usd=upstream.decimal(detail, "tokenized_jrc", star=star),
        internal_senior_risk_capital_usd=upstream.decimal(detail, "internal_src", star=star),
        external_senior_risk_capital_usd=upstream.decimal(detail, "external_src", star=star),
        epi_utilization=upstream.decimal(detail, "epi_utilization", star=star),
        spj_utilization=upstream.decimal(detail, "spj_utilization", star=star),
        per_allocation=tuple(
            _allocation(row, star=star, upstream=upstream)
            for row in _by_exposure_desc(rows, star=star, upstream=upstream)
        ),
    )


def _by_exposure_desc(rows: list, *, star: str, upstream: SkyEnvelope) -> list:
    """Sort the breakdown largest-exposure first.

    Imposed here because upstream accepts ``?order=`` and ignores it, returning
    ``200`` with unsorted rows — so requesting an order would look honoured.
    """
    return sorted(rows, key=lambda row: upstream.decimal(row, "exposure", star=star), reverse=True)


def _allocation(row: dict, *, star: str, upstream: SkyEnvelope) -> ReferenceAllocation:
    network = upstream.required_text(row, "network", star=star)
    chain_id = _NETWORK_TO_CHAIN_ID.get(network)
    return ReferenceAllocation(
        protocol_name=upstream.required_text(row, "protocol", star=star),
        network=network,
        symbol=upstream.required_text(row, "symbol", star=star),
        name=upstream.text(row, "name"),
        token_address=upstream.required_text(row, "token_address", star=star),
        loan_token_address=upstream.text(row, "loan_token_address"),
        loan_token_symbol=upstream.text(row, "loan_token_symbol"),
        exposure_usd=upstream.decimal(row, "exposure", star=star),
        required_risk_capital_usd=upstream.decimal(row, "rrc", star=star),
        crr_pct=upstream.decimal(row, "crr", star=star) * _FRACTION_TO_PCT,
        chain_id=chain_id,
        chain=CHAIN_ID_TO_NAME.get(chain_id) if chain_id is not None else None,
    )
