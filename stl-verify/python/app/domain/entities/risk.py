from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from app.domain.entities.allocation import EthAddress

# ---------------------------------------------------------------------------
# Discriminated details for RrcResult
# ---------------------------------------------------------------------------

ModelName = Literal["suraf", "gap_sweep"]


@dataclass(frozen=True)
class SurafDetails:
    """SURAF model-specific output embedded in an RrcResult."""

    rating_id: str
    rating_version: str
    crr_pct: Decimal
    source_commit_sha: str


@dataclass(frozen=True)
class GapSweepDetails:
    """Gap-sweep model-specific output embedded in an RrcResult."""

    gap_pct: Decimal
    bad_debt_usd: Decimal


RrcDetails = SurafDetails | GapSweepDetails
"""Discriminated union of model-specific detail payloads."""

_MODEL_TO_DETAILS: dict[str, type] = {
    "suraf": SurafDetails,
    "gap_sweep": GapSweepDetails,
}


class RrcResult(BaseModel):
    """Shared result type returned by every RiskModel implementation.

    ``model`` identifies which risk model produced the result (e.g.
    ``"suraf"``, ``"gap_sweep"``).  ``details`` carries model-specific
    output — use ``isinstance`` to narrow.

    Construction validates that ``model`` and ``details`` agree:
    ``"suraf"`` requires ``SurafDetails``, ``"gap_sweep"`` requires
    ``GapSweepDetails``.
    """

    model_config = ConfigDict(frozen=True)

    asset_id: int
    prime_id: str
    rrc_usd: Decimal
    model: ModelName
    details: RrcDetails

    @field_validator("prime_id", mode="before")
    @classmethod
    def _coerce_prime_id(cls, v: object) -> str:
        """Accept an EthAddress or a 0x-prefixed hex string."""
        if isinstance(v, EthAddress):
            return str(v)
        if isinstance(v, str):
            EthAddress(v)  # validate format; discard instance
            return v
        raise ValueError(f"expected EthAddress or hex string, got {type(v).__name__}")

    @model_validator(mode="after")
    def _check_model_details_pairing(self) -> "RrcResult":
        expected = _MODEL_TO_DETAILS[self.model]
        if not isinstance(self.details, expected):
            msg = f"model={self.model!r} requires {expected.__name__}, got {type(self.details).__name__}"
            raise ValueError(msg)
        return self


# ---------------------------------------------------------------------------
# Gap-sweep-specific entities for risk breakdown and liquidation params
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LiquidationParams:
    """Normalised liquidation parameters for a single collateral token.

    Both fields are in [0, ∞) range after protocol-specific normalisation:
    - liquidation_threshold: e.g. 0.825 for 82.5%
    - liquidation_bonus:     e.g. 1.05 for a 5% bonus (always > 1.0)
    """

    token_id: int
    liquidation_threshold: Decimal
    liquidation_bonus: Decimal


@dataclass(frozen=True)
class RiskEnrichedCollateral:
    """A single collateral contribution enriched with USD value and liquidation params."""

    token_id: int
    symbol: str
    amount: Decimal  # human-readable token units
    backing_pct: Decimal  # 0..100
    amount_usd: Decimal  # amount × price_usd
    price_usd: Decimal  # USD spot price used
    liquidation_threshold: Decimal
    liquidation_bonus: Decimal


@dataclass(frozen=True)
class RiskBreakdown:
    """Full enriched breakdown for a backed asset — input to gap_sweep."""

    backed_asset_id: int
    items: tuple[RiskEnrichedCollateral, ...]
