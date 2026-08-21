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
"""

import asyncio
import logging
from decimal import Decimal, InvalidOperation

import httpx

from app.domain.chain_names import CHAIN_ID_TO_NAME
from app.domain.entities.reference_risk_capital import (
    ReferenceAllocation,
    ReferencePrimeRiskCapital,
)
from app.domain.exceptions import ReferenceDataUnavailableError

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)

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
        self._client = client

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
            self._get_data(f"{self._base_url}/primes/{star}/"),
            self._get_data(f"{self._base_url}/primes/{star}/allocations/?limit={_PAGE_LIMIT}"),
        )
        return _build_snapshot(star, detail, allocations)

    async def tracked_stars(self) -> frozenset[str]:
        url = f"{self._base_url}/primes/?limit={_PAGE_LIMIT}"
        data = await self._get_data(url)
        results = _require_results(data, url=url)
        if not results:
            # Every prime would read as untracked, and each would be served as a
            # 404 "not covered" — an outage wearing the shape of a real answer.
            raise ReferenceDataUnavailableError(f"Star monitor listed no primes at all: {url}")
        _require_full_page(data, len(results), url=url)

        stars = set()
        for index, row in enumerate(results):
            star = str(row.get("star") or "").strip().lower()
            if not star:
                raise ReferenceDataUnavailableError(f"Star monitor listed a prime with no name at row {index}: {url}")
            stars.add(star)
        return frozenset(stars)

    async def _get_data(self, url: str) -> dict:
        """GET ``url`` and return its ``data`` object, or raise ``ReferenceDataUnavailableError``."""
        response = await self._request(url)

        if not response.is_success:
            logger.error(
                "Star monitor returned non-success status",
                extra={"upstream_url": url, "status_code": response.status_code},
            )
            raise ReferenceDataUnavailableError(f"Star monitor returned status {response.status_code}: {url}")

        try:
            payload = response.json()
        except ValueError as exc:
            logger.exception("Star monitor returned invalid JSON", extra={"upstream_url": url})
            raise ReferenceDataUnavailableError(f"Star monitor returned invalid JSON: {url}") from exc

        if not isinstance(payload, dict) or payload.get("success") is False:
            raise ReferenceDataUnavailableError(f"Star monitor reported failure: {url}")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise ReferenceDataUnavailableError(f"Star monitor response had no data object: {url}")
        return data

    async def _request(self, url: str) -> httpx.Response:
        try:
            if self._client is not None:
                return await self._client.get(url, timeout=_TIMEOUT)
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                return await client.get(url)
        except httpx.HTTPError as exc:
            logger.exception("Star monitor request failed", extra={"upstream_url": url})
            raise ReferenceDataUnavailableError(f"Star monitor request failed: {url}") from exc


def _require_results(data: dict, *, url: str) -> list[dict]:
    results = data.get("results")
    if not isinstance(results, list):
        raise ReferenceDataUnavailableError(f"Star monitor response had no results array: {url}")
    for index, row in enumerate(results):
        # Callers read every row with .get(); a primitive here would surface as
        # an AttributeError (500) instead of a bad-upstream-payload 502.
        if not isinstance(row, dict):
            raise ReferenceDataUnavailableError(f"Star monitor returned a non-object row at index {index}: {url}")
    return results


def _require_full_page(data: dict, received: int, *, url: str) -> None:
    """Reject a truncated page, which would read as rows that do not exist.

    Upstream paginates and reports the true count; an explicit limit is sent, so
    a short page means the set outgrew it rather than that the extra rows are
    absent.
    """
    pagination = data.get("pagination")
    total = pagination.get("total") if isinstance(pagination, dict) else None
    if isinstance(total, int):
        if total > received:
            raise ReferenceDataUnavailableError(
                f"Star monitor reported {total} rows but returned {received}; the page limit is too low: {url}"
            )
        return

    # No usable count to check against. A page shorter than the limit we asked
    # for is complete by construction, but a full one may have been truncated
    # and we cannot tell — so refuse rather than serve a silent partial set.
    if received >= _PAGE_LIMIT:
        raise ReferenceDataUnavailableError(
            f"Star monitor returned a full page of {received} rows with no usable total; "
            f"the set may be truncated: {url}"
        )


def _build_snapshot(star: str, detail: dict, allocations: dict) -> ReferencePrimeRiskCapital:
    url = f"allocations/{star}"
    rows = _require_results(allocations, url=url)
    _require_full_page(allocations, len(rows), url=url)

    exposure = _decimal(detail, "total_exposure", star=star)
    if not rows and exposure != 0:
        # The two routes are separate snapshots, so an empty breakdown beside a
        # live total is upstream disagreeing with itself — and serving it would
        # publish "this prime holds nothing" against real exposure.
        raise ReferenceDataUnavailableError(
            f"Star monitor reported exposure {exposure} for prime '{star}' but an empty breakdown"
        )

    return ReferencePrimeRiskCapital(
        star=star,
        exposure_usd=exposure,
        required_risk_capital_usd=_decimal(detail, "total_rrc", star=star),
        total_risk_capital_usd=_decimal(detail, "total_rc", star=star),
        encumbrance_ratio=_optional_decimal(detail, "encumbrance_ratio", star=star),
        exposure_share=_decimal(detail, "total_exposure_share", star=star),
        junior_risk_capital_usd=_decimal(detail, "total_jrc", star=star),
        senior_risk_capital_usd=_decimal(detail, "total_src", star=star),
        internal_junior_risk_capital_usd=_decimal(detail, "internal_jrc", star=star),
        external_junior_risk_capital_usd=_decimal(detail, "external_jrc", star=star),
        tokenized_junior_risk_capital_usd=_decimal(detail, "tokenized_jrc", star=star),
        internal_senior_risk_capital_usd=_decimal(detail, "internal_src", star=star),
        external_senior_risk_capital_usd=_decimal(detail, "external_src", star=star),
        epi_utilization=_decimal(detail, "epi_utilization", star=star),
        spj_utilization=_decimal(detail, "spj_utilization", star=star),
        per_allocation=tuple(_allocation(row, star=star) for row in _by_exposure_desc(rows, star=star)),
    )


def _by_exposure_desc(rows: list, *, star: str) -> list:
    """Sort the breakdown largest-exposure first.

    Imposed here because upstream accepts ``?order=`` and ignores it, returning
    ``200`` with unsorted rows — so requesting an order would look honoured.
    """
    return sorted(rows, key=lambda row: _decimal(row, "exposure", star=star), reverse=True)


def _allocation(row: dict, *, star: str) -> ReferenceAllocation:
    network = _required_text(row, "network", star=star)
    chain_id = _NETWORK_TO_CHAIN_ID.get(network)
    return ReferenceAllocation(
        protocol_name=_required_text(row, "protocol", star=star),
        network=network,
        symbol=_required_text(row, "symbol", star=star),
        name=_text(row, "name"),
        token_address=_required_text(row, "token_address", star=star),
        loan_token_address=_text(row, "loan_token_address"),
        loan_token_symbol=_text(row, "loan_token_symbol"),
        exposure_usd=_decimal(row, "exposure", star=star),
        required_risk_capital_usd=_decimal(row, "rrc", star=star),
        crr_pct=_decimal(row, "crr", star=star) * _FRACTION_TO_PCT,
        chain_id=chain_id,
        chain=CHAIN_ID_TO_NAME.get(chain_id) if chain_id is not None else None,
    )


def _required_text(row: dict, field: str, *, star: str) -> str:
    """Read a field that identifies the position, rejecting an absent one.

    Defaulting these to "" would not surface: an absent ``network`` reads as a
    network STL cannot map, and an absent ``symbol`` is served as a real symbol.
    Both look like ordinary answers rather than a feed that changed shape.
    """
    value = _text(row, field)
    if not value:
        raise ReferenceDataUnavailableError(f"Star monitor omitted required field '{field}' for prime '{star}'")
    return value


def _text(row: dict, field: str) -> str:
    """Read a descriptive field, which upstream may legitimately leave empty."""
    value = row.get(field)
    return "" if value is None else str(value)


def _decimal(row: dict, field: str, *, star: str) -> Decimal:
    value = _optional_decimal(row, field, star=star)
    if value is None:
        raise ReferenceDataUnavailableError(f"Star monitor omitted required field '{field}' for prime '{star}'")
    return value


def _optional_decimal(row: dict, field: str, *, star: str) -> Decimal | None:
    raw = row.get(field)
    if raw is None:
        return None
    try:
        # Upstream mixes plain decimal strings with E-notation (e.g. a crr of
        # "4.646E-15"); Decimal accepts both, float would lose the precision the
        # 18-decimal figures carry.
        value = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ReferenceDataUnavailableError(
            f"Star monitor returned a non-numeric '{field}' for prime '{star}': {raw!r}"
        ) from exc

    # Decimal accepts "NaN" and "Infinity" without complaint. Left through, a
    # NaN silently poisons every total it reaches and makes sorting the
    # breakdown raise, so it is rejected at the parse rather than downstream.
    if not value.is_finite():
        raise ReferenceDataUnavailableError(f"Star monitor returned a non-finite '{field}' for prime '{star}': {raw!r}")
    return value
