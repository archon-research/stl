from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.entities.allocation import EthAddress

# ---------------------------------------------------------------------------
# Discriminated details for RrcResult
# ---------------------------------------------------------------------------

ModelName = Literal["suraf", "gap_sweep"]


class SurafDetails(BaseModel):
    """SURAF model-specific output embedded in an RrcResult.

    ``crr_pct`` is the adjusted CRR (i.e. ``unadjusted_crr_pct + penalty_pp``,
    capped at 100). All three are percentages on a 0–100 scale (e.g.
    ``Decimal("33.7")`` means 33.7%, not 0.337). The relation
    ``crr_pct == unadjusted_crr_pct + penalty_pp`` holds up to ~1e-10 only —
    the underlying scorer caps to 100 in float before Decimal conversion;
    consumers should always trust ``crr_pct`` over a recomputation from
    parts.
    """

    model_config = ConfigDict(frozen=True)

    risk_model: Literal["suraf"] = "suraf"
    rating_id: str
    rating_version: str
    crr_pct: Decimal
    unadjusted_crr_pct: Decimal
    penalty_pp: Decimal
    source_commit_sha: str


class GapSweepDetails(BaseModel):
    """Gap-sweep model-specific output embedded in an RrcResult.

    ``gap_pct`` is a *fraction* in ``[0, 1]`` (e.g. ``Decimal("0.15")``
    means a 15% collateral price drop) — note this is a different scale
    from ``SurafDetails.crr_pct`` which is on 0–100. ``loss_usd`` is the
    engine-native expected loss in USD under that price drop; equal in
    magnitude to the envelope-level ``rrc_usd`` for this model. Both are
    rounded to USD cents.
    """

    model_config = ConfigDict(frozen=True)

    risk_model: Literal["gap_sweep"] = "gap_sweep"
    gap_pct: Decimal
    loss_usd: Decimal


RrcDetails = Annotated[Union[SurafDetails, GapSweepDetails], Field(discriminator="risk_model")]
"""Discriminated union of model-specific detail payloads keyed on ``risk_model``."""

_RISK_MODEL_TO_DETAILS: dict[str, type] = {
    "suraf": SurafDetails,
    "gap_sweep": GapSweepDetails,
}


class RrcResult(BaseModel):
    """Shared result type returned by every RiskModel implementation.

    ``risk_model`` identifies which model produced the result (e.g.
    ``"suraf"``, ``"gap_sweep"``).  ``details`` carries model-specific
    output — use ``isinstance`` to narrow. The same value also appears
    on ``details.risk_model`` and serves as the OpenAPI discriminator.
    """

    model_config = ConfigDict(frozen=True)

    asset_id: int
    prime_id: str
    rrc_usd: Decimal
    risk_model: ModelName
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
    def _check_risk_model_details_pairing(self) -> "RrcResult":
        expected = _RISK_MODEL_TO_DETAILS[self.risk_model]
        if not isinstance(self.details, expected):
            msg = f"risk_model={self.risk_model!r} requires {expected.__name__}, got {type(self.details).__name__}"
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
