"""Outbound port for stored reference risk-capital snapshots."""

from datetime import datetime
from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_risk_capital import ReferenceCapitalBucket


class ReferenceCapitalRepository(Protocol):
    """Read the reference snapshots the capital-stack syncer accumulates."""

    async def list_reference_capital_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[ReferenceCapitalBucket]:
        """Return the last upstream observation per time bucket (LOCF gap-filled).

        The series starts when the syncer first ran, not when the prime did:
        the upstream monitor publishes no history, so nothing before that can
        exist. A window reaching further back yields leading ``None`` buckets,
        which mean "not yet observed" and never "zero".
        """
        ...
