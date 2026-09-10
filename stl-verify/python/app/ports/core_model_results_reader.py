"""CoreModelResultsReader port — reads pre-computed CRR rows from the DB.

Used by CoreModelRiskService at request time. The cronjob writes to the
same table; the service only reads.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class CoreModelResult:
    market_key: str
    crr_el_pct: Decimal
    crr_es_pct: Decimal
    crr_var_pct: Decimal
    hhi: Decimal | None
    protocol: str
    forecast_step: int
    n_mc: int
    copula_type: str
    computed_at: datetime


class CoreModelResultsReader(Protocol):
    async def get_latest(self, market_key: str) -> CoreModelResult | None:
        """Return the most recently computed result for ``market_key``, or None."""
        ...
