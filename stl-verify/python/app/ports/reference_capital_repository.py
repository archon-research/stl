"""Outbound port for stored reference risk-capital snapshots."""

from datetime import datetime
from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_risk_capital import ReferenceCapitalBucket


class ReferenceCapitalRepository(Protocol):
    """Read the reference snapshots the capital-stack syncer accumulates.

    Reference *figures*, but not a reference *read*: these come from STL's own
    Postgres, so callers must not treat a failure here the way they treat one
    from the live Star monitor. The allocation and risk-capital endpoints serve
    their indexed half when the monitor 404s or 502s, because a third party
    being unreachable is not STL failing; a failed read of this store is the
    same database the indexed half was just read from, and answering with half a
    series would be the partial success the repo forbids. The `source=both`
    branches let it raise.
    """

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
