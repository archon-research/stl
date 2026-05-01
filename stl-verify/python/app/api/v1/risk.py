from decimal import Decimal
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api._validators import EthAddressParam
from app.api.deps import (
    get_crypto_lending_risk_service,
    get_model_registry,
    get_receipt_token_lookup,
    get_suraf_rrc_service,
)
from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import RrcResult
from app.domain.exceptions import (
    AllocationShareError,
    InvalidOverrideError,
    MissingShareError,
    StaleShareError,
)
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry
from app.services.suraf_rrc_service import SurafRrcService

router = APIRouter()

_ZERO = Decimal("0")
_ONE = Decimal("1")


class BadDebtResponse(BaseModel):
    receipt_token_id: int
    gap_pct: Decimal
    bad_debt_usd: Decimal


class RiskBreakdownItemResponse(BaseModel):
    token_id: int
    symbol: str
    amount: Decimal
    backing_pct: Decimal
    amount_usd: Decimal
    price_usd: Decimal
    liquidation_threshold: Decimal
    liquidation_bonus: Decimal


class RiskBreakdownResponse(BaseModel):
    receipt_token_id: int
    items: list[RiskBreakdownItemResponse]


class ScenarioRrcRequest(BaseModel):
    receipt_token_id: int
    usd_exposure: Decimal


class ScenarioRrcResponse(BaseModel):
    receipt_token_id: int
    usd_exposure: Decimal
    rating_id: str
    rating_version: str
    crr_pct: Decimal
    rrc_usd: Decimal
    source_commit_sha: str


def _share_error_503(exc: AllocationShareError) -> HTTPException:
    """Translate an AllocationShareError subtype into a 503 with a distinct code."""
    if isinstance(exc, StaleShareError):
        code = "share_data_stale"
    elif isinstance(exc, MissingShareError):
        code = "share_data_missing"
    else:
        code = "share_data_unavailable"
    return HTTPException(status_code=503, detail={"code": code, "message": str(exc)})


@router.get("/risk/{receipt_token_id}/bad-debt", response_model=BadDebtResponse)
async def get_bad_debt(
    receipt_token_id: int,
    gap_pct: Decimal,
    service: CryptoLendingRiskService = Depends(get_crypto_lending_risk_service),
) -> BadDebtResponse:
    """Estimate bad debt for a receipt token position at the given collateral price gap."""
    if not (_ZERO <= gap_pct <= _ONE):
        raise HTTPException(status_code=422, detail="gap_pct must be between 0 and 1")

    try:
        bad_debt = await service.get_bad_debt_legacy(receipt_token_id, gap_pct)
    except AllocationShareError as exc:
        raise _share_error_503(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if bad_debt is None:
        raise HTTPException(404, "receipt token not found")
    return BadDebtResponse(
        receipt_token_id=receipt_token_id,
        gap_pct=gap_pct,
        bad_debt_usd=bad_debt,
    )


@router.get("/risk/{receipt_token_id}/breakdown", response_model=RiskBreakdownResponse)
async def get_risk_breakdown(
    receipt_token_id: int,
    service: CryptoLendingRiskService = Depends(get_crypto_lending_risk_service),
) -> RiskBreakdownResponse:
    """Return the full risk-enriched collateral breakdown for a receipt token position."""
    try:
        breakdown = await service.get_risk_breakdown_legacy(receipt_token_id)
    except AllocationShareError as exc:
        raise _share_error_503(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if breakdown is None:
        raise HTTPException(404, "receipt token not found")
    return RiskBreakdownResponse(
        receipt_token_id=receipt_token_id,
        items=[
            RiskBreakdownItemResponse(
                token_id=item.token_id,
                symbol=item.symbol,
                amount=item.amount,
                backing_pct=item.backing_pct,
                amount_usd=item.amount_usd,
                price_usd=item.price_usd,
                liquidation_threshold=item.liquidation_threshold,
                liquidation_bonus=item.liquidation_bonus,
            )
            for item in breakdown.items
        ],
    )


@router.post("/risk/rrc/scenario", response_model=ScenarioRrcResponse)
async def post_rrc_scenario(
    body: ScenarioRrcRequest,
    service: SurafRrcService = Depends(get_suraf_rrc_service),
) -> ScenarioRrcResponse:
    """Return SURAF RRC for a hypothetical ``(receipt_token_id, usd_exposure)`` pair.

    ``RRC = usd_exposure * CRR``, where CRR is the pre-computed SURAF rating
    for the receipt token. Pure scenario calculation — no position state.
    """
    if body.usd_exposure <= _ZERO:
        raise HTTPException(status_code=422, detail="usd_exposure must be positive")

    result = service.compute_legacy(body.receipt_token_id, body.usd_exposure)
    if result is None:
        raise HTTPException(status_code=404, detail=f"no rating mapped for receipt_token_id: {body.receipt_token_id}")
    return ScenarioRrcResponse(**result.model_dump())


# ---------------------------------------------------------------------------
# Unified /v1/risk/rrc — registry-dispatched, multi-model
# ---------------------------------------------------------------------------


class RrcRequest(BaseModel):
    """POST /v1/risk/rrc body — overrides keyed by model name."""

    asset_id: int = Field(ge=1)
    prime_id: EthAddressParam
    overrides: dict[str, dict[str, Any]] = Field(default_factory=dict)


class RrcEnvelope(BaseModel):
    """Response wrapper carrying one RrcResult per applicable model.

    On-chain identifiers (``chain_id``, ``receipt_token_address``) are echoed
    so consumers can resolve the surrogate ``asset_id`` without a second call.

    **Important:** ``results`` may contain multiple entries (when more than
    one model applies). The values are *not* additive — SURAF capital and
    gap-sweep expected-loss overlap economically. Pick the model relevant to
    the consumer's question; do not sum.
    """

    asset_id: int
    chain_id: int
    receipt_token_address: str
    prime_id: str
    results: list[RrcResult]


@router.get("/risk/rrc", response_model=RrcEnvelope)
async def get_rrc(
    asset_id: Annotated[int, Query(ge=1)],
    prime_id: EthAddressParam,
    registry: ModelRegistry = Depends(get_model_registry),
    receipt_token_lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> RrcEnvelope:
    """Compute RRC at default stress for every model that applies to ``(asset_id, prime_id)``.

    Errors:
    - 404 if ``asset_id`` is not a known receipt token, or no models apply.
    - 422 if ``prime_id`` is malformed or ``asset_id`` < 1.
    - 503 with ``share_data_missing`` / ``share_data_stale`` codes if the
      share-data lookup fails.
    """
    return await _compute_envelope(
        asset_id, EthAddress(prime_id), overrides={}, registry=registry, lookup=receipt_token_lookup
    )


@router.post("/risk/rrc", response_model=RrcEnvelope)
async def post_rrc(
    body: RrcRequest,
    registry: ModelRegistry = Depends(get_model_registry),
    receipt_token_lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> RrcEnvelope:
    """Compute RRC with per-model scenario overrides for every applicable model.

    Errors:
    - 404 if ``asset_id`` is not a known receipt token, or no models apply.
    - 422 if ``prime_id``/``asset_id`` invalid, an unknown override model key
      is present, or any model rejects its overrides.
    - 503 with ``share_data_missing`` / ``share_data_stale`` codes if the
      share-data lookup fails.
    """
    return await _compute_envelope(
        body.asset_id,
        EthAddress(body.prime_id),
        overrides=body.overrides,
        registry=registry,
        lookup=receipt_token_lookup,
    )


async def _compute_envelope(
    asset_id: int,
    prime_id: EthAddress,
    overrides: dict[str, dict[str, Any]],
    registry: ModelRegistry,
    lookup: ReceiptTokenLookup,
) -> RrcEnvelope:
    unknown_models = set(overrides) - registry.risk_model_names
    if unknown_models:
        raise HTTPException(
            status_code=422,
            detail=f"unknown override model keys: {sorted(unknown_models)}",
        )

    info = await lookup.get(asset_id)
    if info is None:
        raise HTTPException(status_code=404, detail=f"unknown asset_id={asset_id}")

    applicable = registry.applicable(asset_id, prime_id)
    if not applicable:
        raise HTTPException(
            status_code=404,
            detail=f"no risk models apply for asset_id={asset_id}, prime_id={prime_id}",
        )

    results: list[RrcResult] = []
    for m in applicable:
        try:
            result = await m.compute(asset_id, prime_id, overrides.get(m.risk_model, {}))
        except AllocationShareError as exc:
            raise _share_error_503(exc) from exc
        except InvalidOverrideError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        # Bare ValueError (invariant breach — e.g. registry returned an
        # asset the service rejects) is intentionally NOT caught: it
        # surfaces as 500 so the bug is loud rather than masked as 422.
        results.append(result)

    return RrcEnvelope(
        asset_id=asset_id,
        chain_id=info.chain_id,
        receipt_token_address=info.receipt_token_address_hex,
        prime_id=str(prime_id),
        results=results,
    )
