"""Pydantic result type returned by the SURAF loader.

Floats from the vendored scorer are converted to ``Decimal`` at this
boundary — downstream code treats these as monetary/percent values and
must not operate on floats.
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class SurafResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    rating_id: str
    crr_pct: Decimal
    unadjusted_crr_pct: Decimal
    penalty_pp: Decimal
    avg_score: Decimal
    source_commit_sha: str
    loaded_at: datetime
