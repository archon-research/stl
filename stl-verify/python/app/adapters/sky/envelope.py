"""The response envelope both Sky hosts answer with.

The Star monitor (:mod:`app.adapters.sky.reference_risk_capital_client`) and the
internal balance-sheet feed (:mod:`app.adapters.sky.internal_positions_client`)
are different hosts answering different questions, but they wrap every answer in
the same ``{success, status, data: {results, pagination}}`` shape and encode
their figures the same way, so that shape is read in one place.

Each client binds its own ``source`` name, which prefixes every message and log
line this module produces. Which of the two hosts failed is the operational
fact — "Star monitor returned status 503" and "Sky internal feed returned status
503" send an operator to different places — so it is never generalised away.
The page limit is bound per client too: the two serve sets an order of magnitude
apart, and each owns its own limit.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import httpx

from app.domain.exceptions import ReferenceDataUnavailableError

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)


@dataclass(frozen=True)
class SkyEnvelope:
    """Reads one Sky host's paginated envelope, naming that host in every failure."""

    source: str
    page_limit: int
    client: httpx.AsyncClient | None = None

    async def get_data(self, url: str) -> dict:
        """GET ``url`` and return its ``data`` object, or raise ``ReferenceDataUnavailableError``."""
        response = await self._request(url)

        if not response.is_success:
            logger.error(
                "%s returned non-success status",
                self.source,
                extra={"upstream_url": url, "status_code": response.status_code},
            )
            raise self.failure(f"returned status {response.status_code}: {url}")

        try:
            payload = response.json()
        except ValueError as exc:
            logger.exception("%s returned invalid JSON", self.source, extra={"upstream_url": url})
            raise self.failure(f"returned invalid JSON: {url}") from exc

        if not isinstance(payload, dict) or payload.get("success") is False:
            raise self.failure(f"reported failure: {url}")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise self.failure(f"response had no data object: {url}")
        return data

    def require_results(self, data: dict, *, url: str) -> list[dict]:
        """Return the ``results`` array, rejecting a payload that cannot be read row by row."""
        results = data.get("results")
        if not isinstance(results, list):
            raise self.failure(f"response had no results array: {url}")
        for index, row in enumerate(results):
            # Callers read every row with .get(); a primitive here would surface as
            # an AttributeError (500) instead of a bad-upstream-payload 502.
            if not isinstance(row, dict):
                raise self.failure(f"returned a non-object row at index {index}: {url}")
        return results

    def require_full_page(self, data: dict, received: int, *, url: str) -> None:
        """Reject a page that may be truncated, which would read as rows that do not exist.

        Both hosts paginate and report the true count, and an explicit limit is
        sent, so a short page means the set outgrew the limit rather than that
        the extra rows are absent. Without a usable count a page at the limit
        cannot be told from a cut-off one, so it is refused rather than served as
        a silent partial set.
        """
        pagination = data.get("pagination")
        total = pagination.get("total") if isinstance(pagination, dict) else None
        if isinstance(total, int):
            if total > received:
                raise self.failure(f"reported {total} rows but returned {received}; the page limit is too low: {url}")
            return

        if received >= self.page_limit:
            raise self.failure(
                f"returned a full page of {received} rows with no usable total; the set may be truncated: {url}"
            )

    def required_text(self, row: dict, field: str, *, star: str) -> str:
        """Read a field that identifies the position, rejecting an absent one.

        Defaulting these to "" would not surface: an absent ``network`` reads as
        a network STL cannot map, and an absent symbol is served as a real
        symbol. Both look like ordinary answers rather than a feed that changed
        shape.
        """
        value = self.text(row, field)
        if not value:
            raise self.failure(f"omitted required field '{field}' for prime '{star}'")
        return value

    def text(self, row: dict, field: str) -> str:
        """Read a descriptive field, which upstream may legitimately leave empty."""
        value = row.get(field)
        return "" if value is None else str(value)

    def decimal(self, row: dict, field: str, *, star: str) -> Decimal:
        """Read a figure the caller cannot do without."""
        value = self.optional_decimal(row, field, star=star)
        if value is None:
            raise self.failure(f"omitted required field '{field}' for prime '{star}'")
        return value

    def optional_decimal(self, row: dict, field: str, *, star: str) -> Decimal | None:
        """Read a figure upstream may legitimately omit, rejecting one that cannot be totalled.

        Both hosts mix plain decimal strings with E-notation (a crr of
        ``"4.646E-15"``); ``Decimal`` takes both, where float would lose the
        precision their 18-decimal figures carry. ``Decimal`` also takes ``NaN``
        and ``Infinity`` without complaint — left through, a NaN poisons every
        total it reaches and makes sorting the rows raise, so it is rejected here
        rather than downstream.
        """
        raw = row.get(field)
        if raw is None:
            return None
        try:
            value = Decimal(str(raw))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise self.failure(f"returned a non-numeric '{field}' for prime '{star}': {raw!r}") from exc

        if not value.is_finite():
            raise self.failure(f"returned a non-finite '{field}' for prime '{star}': {raw!r}")
        return value

    async def _request(self, url: str) -> httpx.Response:
        try:
            if self.client is not None:
                return await self.client.get(url, timeout=_TIMEOUT)
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                return await client.get(url)
        except httpx.HTTPError as exc:
            logger.exception("%s request failed", self.source, extra={"upstream_url": url})
            raise self.failure(f"request failed: {url}") from exc

    def failure(self, message: str) -> ReferenceDataUnavailableError:
        """Build a host-named failure, for the checks only one client makes."""
        return ReferenceDataUnavailableError(f"{self.source} {message}")
