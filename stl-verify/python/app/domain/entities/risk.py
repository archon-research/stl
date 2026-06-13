from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Literal, Union, get_args

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.entities.allocation import EthAddress

# ---------------------------------------------------------------------------
# Discriminated details for RrcResult
# ---------------------------------------------------------------------------

ModelName = Literal["suraf", "gap_sweep", "core_model"]


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

    model_config = ConfigDict(frozen=True, extra="forbid")

    risk_model: Literal["suraf"]
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
    magnitude to the envelope-level ``rrc_usd`` for this model and rounded
    to USD cents. The cross-model comparable capital ratio is exposed on
    ``RrcResult.comparable_crr_pct`` using the receipt-token USD exposure
    basis, not the collateral backing basis.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    risk_model: Literal["gap_sweep"]
    gap_pct: Decimal
    loss_usd: Decimal


class CoreModelDetails(BaseModel):
    """CORE model-specific output embedded in an RrcResult.

    ``crr_el_pct`` is the expected-loss CRR used as the primary capital
    charge (0-100 scale, e.g. ``Decimal("12.5")`` means 12.5%).
    ``hhi`` is the Herfindahl-Hirschman Index of borrower concentration
    expressed as a percentage; ``None`` when liquidation analysis was
    not run or the market had fewer than two borrowers.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    risk_model: Literal["core_model"]
    crr_el_pct: Decimal
    crr_es_pct: Decimal
    crr_var_pct: Decimal
    hhi: Decimal | None
    protocol: str
    forecast_step: int
    n_mc: int
    copula_type: str


RrcDetails = Annotated[Union[SurafDetails, GapSweepDetails, CoreModelDetails], Field(discriminator="risk_model")]
"""Discriminated union of model-specific detail payloads keyed on ``risk_model``."""

_RISK_MODEL_TO_DETAILS: dict[str, type] = {
    "suraf": SurafDetails,
    "gap_sweep": GapSweepDetails,
    "core_model": CoreModelDetails,
}

# Catch drift at import time: adding a literal to ``ModelName`` without
# mapping it here would silently break ``_check_risk_model_details_pairing``
# with a KeyError on the first request that uses the new variant.
# Use ``raise`` rather than ``assert`` so ``python -O`` doesn't strip the
# guard from production builds.
_missing = set(get_args(ModelName)) - set(_RISK_MODEL_TO_DETAILS)
_extra = set(_RISK_MODEL_TO_DETAILS) - set(get_args(ModelName))
if _missing or _extra:
    raise RuntimeError(
        f"_RISK_MODEL_TO_DETAILS keys must match ModelName literals (missing: {_missing}, extra: {_extra})"
    )
del _missing, _extra


class RrcResult(BaseModel):
    """Shared result type returned by every RiskModel implementation.

    ``risk_model`` identifies which model produced the result (e.g.
    ``"suraf"``, ``"gap_sweep"``).  ``details`` carries model-specific
    output — use ``isinstance`` to narrow. The same value also appears
    on ``details.risk_model`` and serves as the OpenAPI discriminator.
    ``comparable_crr_pct`` is the model's effective capital ratio on the
    shared receipt-token USD exposure basis, expressed on a 0–100 scale.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    asset_id: int
    prime_id: EthAddress
    rrc_usd: Decimal
    comparable_crr_pct: Decimal
    risk_model: ModelName
    details: RrcDetails

    @field_validator("prime_id", mode="before")
    @classmethod
    def _coerce_prime_id(cls, v: object) -> EthAddress:
        """Accept an EthAddress or a 0x-prefixed hex string."""
        if isinstance(v, EthAddress):
            return v
        if isinstance(v, str):
            return EthAddress(v)
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
