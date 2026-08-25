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
"""

import logging
from decimal import Decimal, InvalidOperation

import httpx

from app.domain.chain_names import CHAIN_ID_TO_NAME
from app.domain.entities.reference_position import ReferencePosition
from app.domain.exceptions import ReferenceDataUnavailableError

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)

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
        self._client = client

    async def get_positions(self, star: str) -> tuple[ReferencePosition, ...]:
        url = f"{self._base_url}/allocations/?prime={star}&limit={_PAGE_LIMIT}"
        data = await self._get_data(url)
        rows = _require_results(data, url=url)
        _require_full_page(data, len(rows), url=url)
        return tuple(_position(row, star=star) for row in _by_assets_desc(rows, star=star))

    async def _get_data(self, url: str) -> dict:
        """GET ``url`` and return its ``data`` object, or raise ``ReferenceDataUnavailableError``."""
        response = await self._request(url)

        if not response.is_success:
            logger.error(
                "Sky internal feed returned non-success status",
                extra={"upstream_url": url, "status_code": response.status_code},
            )
            raise ReferenceDataUnavailableError(f"Sky internal feed returned status {response.status_code}: {url}")

        try:
            payload = response.json()
        except ValueError as exc:
            logger.exception("Sky internal feed returned invalid JSON", extra={"upstream_url": url})
            raise ReferenceDataUnavailableError(f"Sky internal feed returned invalid JSON: {url}") from exc

        if not isinstance(payload, dict) or payload.get("success") is False:
            raise ReferenceDataUnavailableError(f"Sky internal feed reported failure: {url}")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise ReferenceDataUnavailableError(f"Sky internal feed response had no data object: {url}")
        return data

    async def _request(self, url: str) -> httpx.Response:
        try:
            if self._client is not None:
                return await self._client.get(url, timeout=_TIMEOUT)
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                return await client.get(url)
        except httpx.HTTPError as exc:
            logger.exception("Sky internal feed request failed", extra={"upstream_url": url})
            raise ReferenceDataUnavailableError(f"Sky internal feed request failed: {url}") from exc


def _require_results(data: dict, *, url: str) -> list[dict]:
    results = data.get("results")
    if not isinstance(results, list):
        raise ReferenceDataUnavailableError(f"Sky internal feed response had no results array: {url}")
    for index, row in enumerate(results):
        # Callers read every row with .get(); a primitive here would surface as
        # an AttributeError (500) instead of a bad-upstream-payload 502.
        if not isinstance(row, dict):
            raise ReferenceDataUnavailableError(f"Sky internal feed returned a non-object row at index {index}: {url}")
    return results


def _require_full_page(data: dict, received: int, *, url: str) -> None:
    """Reject a truncated page, which would read as positions the prime does not hold."""
    pagination = data.get("pagination")
    total = pagination.get("total") if isinstance(pagination, dict) else None
    if isinstance(total, int):
        if total > received:
            raise ReferenceDataUnavailableError(
                f"Sky internal feed reported {total} rows but returned {received}; the page limit is too low: {url}"
            )
        return

    # No usable count to check against. A page shorter than the limit we asked
    # for is complete by construction, but a full one may have been truncated
    # and we cannot tell — so refuse rather than serve a silent partial set.
    if received >= _PAGE_LIMIT:
        raise ReferenceDataUnavailableError(
            f"Sky internal feed returned a full page of {received} rows with no usable total; "
            f"the set may be truncated: {url}"
        )


def _by_assets_desc(rows: list, *, star: str) -> list:
    """Sort largest-holding first, matching how every other allocation list is served."""
    return sorted(rows, key=lambda row: _decimal(row, "assets", star=star), reverse=True)


def _position(row: dict, *, star: str) -> ReferencePosition:
    network = _required_text(row, "network", star=star)
    chain_id = _NETWORK_TO_CHAIN_ID.get(network)
    return ReferencePosition(
        protocol_name=_required_text(row, "protocol", star=star),
        network=network,
        symbol=_required_text(row, "token_symbol", star=star),
        name=_text(row, "token_name"),
        token_address=_required_text(row, "address", star=star),
        wallet_address=_required_text(row, "wallet_address", star=star),
        assets_usd=_decimal(row, "assets", star=star),
        allocated_assets_usd=_optional_decimal(row, "allocated_assets", star=star),
        idle_assets_usd=_optional_decimal(row, "idle_assets", star=star),
        allocation_type=_text(row, "allocation_type"),
        chain_id=chain_id,
        chain=CHAIN_ID_TO_NAME.get(chain_id) if chain_id is not None else None,
    )


def _required_text(row: dict, field: str, *, star: str) -> str:
    """Read a field that identifies the position, rejecting an absent one.

    Defaulting these to "" would not surface: an absent ``network`` reads as a
    network STL cannot map, and an absent ``token_symbol`` is served as a real
    symbol. Both look like ordinary answers rather than a feed that changed shape.
    """
    value = _text(row, field)
    if not value:
        raise ReferenceDataUnavailableError(f"Sky internal feed omitted required field '{field}' for prime '{star}'")
    return value


def _text(row: dict, field: str) -> str:
    """Read a descriptive field, which upstream may legitimately leave empty."""
    value = row.get(field)
    return "" if value is None else str(value)


def _decimal(row: dict, field: str, *, star: str) -> Decimal:
    value = _optional_decimal(row, field, star=star)
    if value is None:
        raise ReferenceDataUnavailableError(f"Sky internal feed omitted required field '{field}' for prime '{star}'")
    return value


def _optional_decimal(row: dict, field: str, *, star: str) -> Decimal | None:
    raw = row.get(field)
    if raw is None:
        return None
    try:
        # Upstream mixes plain decimal strings with E-notation; Decimal accepts
        # both, float would lose the precision the 18-decimal figures carry.
        value = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ReferenceDataUnavailableError(
            f"Sky internal feed returned a non-numeric '{field}' for prime '{star}': {raw!r}"
        ) from exc

    # Decimal accepts "NaN" and "Infinity" without complaint. Left through, a
    # NaN silently poisons every total it reaches and makes sorting the list
    # raise, so it is rejected at the parse rather than downstream.
    if not value.is_finite():
        raise ReferenceDataUnavailableError(
            f"Sky internal feed returned a non-finite '{field}' for prime '{star}': {raw!r}"
        )
    return value
