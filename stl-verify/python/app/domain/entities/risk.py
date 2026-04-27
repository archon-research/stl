from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

from app.domain.entities.allocation import EthAddress

AssetId: TypeAlias = int
PrimeId: TypeAlias = EthAddress


class RiskModelError(Exception):
    """Base class for shared risk-model errors."""


class RiskPositionNotFoundError(RiskModelError):
    """Raised when no current position exists for ``(asset_id, prime_id)``."""

    def __init__(self, asset_id: AssetId, prime_id: PrimeId) -> None:
        self.asset_id = asset_id
        self.prime_id = prime_id
        super().__init__(f"risk position not found for asset_id={asset_id} prime_id={prime_id}")


class RiskModelNotApplicableError(RiskModelError):
    """Raised when a position exists but the selected model does not apply."""

    def __init__(self, model_name: str, asset_id: AssetId, prime_id: PrimeId) -> None:
        self.model_name = model_name
        self.asset_id = asset_id
        self.prime_id = prime_id
        super().__init__(f"risk model {model_name!r} does not apply to asset_id={asset_id} prime_id={prime_id}")


class InvalidRiskModelOverridesError(RiskModelError):
    """Raised when a model receives unknown or invalid override inputs."""

    def __init__(self, model_name: str, detail: str) -> None:
        self.model_name = model_name
        self.detail = detail
        super().__init__(f"invalid overrides for risk model {model_name!r}: {detail}")


class RiskModelInputsUnavailableError(RiskModelError):
    """Raised when a model applies but required derived inputs are unavailable."""

    def __init__(self, model_name: str, asset_id: AssetId, prime_id: PrimeId, detail: str) -> None:
        self.model_name = model_name
        self.asset_id = asset_id
        self.prime_id = prime_id
        self.detail = detail
        super().__init__(
            f"risk model {model_name!r} cannot compute asset_id={asset_id} prime_id={prime_id}: {detail}"
        )


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


@dataclass(frozen=True)
class ResolvedRiskPosition:
    """Single current position resolved from the external ``(asset_id, prime_id)`` pair."""

    chain_id: int
    prime_id: PrimeId
    receipt_token_id: AssetId
    receipt_token_address: str
    underlying_token_id: int
    underlying_token_address: str
    protocol_id: int
    symbol: str
    underlying_symbol: str
    protocol_name: str
    balance: Decimal
    usd_exposure: Decimal | None


class SurafRrcDetails(BaseModel):
    model_config = ConfigDict(frozen=True)

    model: Literal["suraf"] = "suraf"
    chain_id: int
    protocol_name: str
    symbol: str
    underlying_symbol: str
    receipt_token_address: str
    underlying_token_address: str
    usd_exposure: Decimal
    usd_exposure_source: Literal["position", "override"]
    rating_id: str
    rating_version: str
    crr_pct: Decimal
    source_commit_sha: str


class GapSweepRrcDetails(BaseModel):
    model_config = ConfigDict(frozen=True)

    model: Literal["gap_sweep"] = "gap_sweep"
    chain_id: int
    protocol_name: str
    symbol: str
    underlying_symbol: str
    receipt_token_address: str
    underlying_token_address: str
    effective_gap_pct: Decimal
    backed_asset_id: int
    collateral_item_count: int


RrcDetails: TypeAlias = Annotated[SurafRrcDetails | GapSweepRrcDetails, Field(discriminator="model")]


class RrcResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset_id: AssetId
    prime_id: PrimeId
    rrc_usd: Decimal
    details: RrcDetails
