"""Pydantic result type returned by the SURAF loader.

Floats from the vendored scorer are converted to ``Decimal`` at this
boundary — downstream code treats these as monetary/percent values and
must not operate on floats.
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class SurafResult(BaseModel):
    """SURAF scoring result for a single rating package.

    CRR fields are percentages on a 0–100 scale (e.g. ``Decimal("33.7")``
    means 33.7%, not 0.337). ``crr_pct`` may differ from
    ``unadjusted_crr_pct + penalty_pp`` by up to ~1e-10 because the
    scorer computes ``adjusted = min(unadj + pen, 100)`` in float before
    the Decimal conversion at this boundary.
    """

    model_config = ConfigDict(frozen=True)

    rating_id: str
    version: str
    crr_pct: Decimal
    unadjusted_crr_pct: Decimal
    penalty_pp: Decimal
    avg_score: Decimal
    source_commit_sha: str
    loaded_at: datetime
