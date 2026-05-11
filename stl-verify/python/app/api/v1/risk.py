from decimal import Decimal
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api._validators import EthAddressParam
from app.api.deps import (
    get_crypto_lending_risk_service,
    get_model_registry,
    get_receipt_token_lookup,
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

router = APIRouter(tags=["risk"])

_ZERO = Decimal("0")
_ONE = Decimal("1")


class BadDebtResponse(BaseModel):
    """Estimated bad debt for a receipt-token position at a given collateral gap."""

    receipt_token_id: int = Field(description="Surrogate id of the receipt token.", examples=[42])
    gap_pct: Decimal = Field(
        description="Collateral price gap as a fraction in `[0, 1]`. Decimal serialized as a JSON string.",
        examples=["0.10"],
    )
    bad_debt_usd: Decimal = Field(
        description="Estimated USD bad debt at the given gap. Decimal serialized as a JSON string.",
        examples=["1234567.89"],
    )

    model_config = {
        "json_schema_extra": {
            "example": {"receipt_token_id": 42, "gap_pct": "0.10", "bad_debt_usd": "1234567.89"},
        }
    }


class RiskBreakdownItemResponse(BaseModel):
    """One backing-token row in a receipt-token's risk-enriched breakdown."""

    token_id: int = Field(description="Surrogate token id of the backing token.", examples=[101])
    symbol: str = Field(description="Backing-token symbol.", examples=["WETH"])
    amount: Decimal = Field(
        description="Backing-token amount, expressed in token units. Decimal serialized as a JSON string.",
        examples=["12.345678"],
    )
    backing_pct: Decimal = Field(
        description="Share of the receipt token backed by this row, as a 0–100 percentage.",
        examples=["42.0"],
    )
    amount_usd: Decimal = Field(
        description="USD value of the backing-token row.",
        examples=["41234.56"],
    )
    price_usd: Decimal = Field(description="Latest USD price for the backing token.", examples=["3340.55"])
    liquidation_threshold: Decimal = Field(
        description="Lender's liquidation threshold (LTV ratio) for the backing token, in `[0, 1]`.",
        examples=["0.83"],
    )
    liquidation_bonus: Decimal = Field(
        description=(
            "Liquidation bonus expressed as a multiplier (e.g. `1.05` for a 5% bonus). "
            "Stored as basis points upstream and normalised by dividing by 10000."
        ),
        examples=["1.05"],
    )


class RiskBreakdownResponse(BaseModel):
    """Risk-enriched breakdown of a receipt token's backing collateral."""

    receipt_token_id: int = Field(description="Surrogate id of the receipt token.", examples=[42])
    items: list[RiskBreakdownItemResponse] = Field(description="One entry per backing-token row.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "receipt_token_id": 42,
                "items": [
                    {
                        "token_id": 101,
                        "symbol": "WETH",
                        "amount": "12.345678",
                        "backing_pct": "42.0",
                        "amount_usd": "41234.56",
                        "price_usd": "3340.55",
                        "liquidation_threshold": "0.83",
                        "liquidation_bonus": "1.05",
                    }
                ],
            }
        }
    }


def _share_error_503(exc: AllocationShareError) -> HTTPException:
    """Translate an AllocationShareError subtype into a 503 with a distinct code."""
    if isinstance(exc, StaleShareError):
        code = "share_data_stale"
    elif isinstance(exc, MissingShareError):
        code = "share_data_missing"
    else:
        code = "share_data_unavailable"
    return HTTPException(status_code=503, detail={"code": code, "message": str(exc)})


@router.get(
    "/risk/{receipt_token_id}/bad-debt",
    response_model=BadDebtResponse,
    summary="Estimate bad debt at a collateral gap",
    include_in_schema=False,
    description=(
        "Estimate USD bad debt for a receipt-token position when collateral prices "
        "fall by `gap_pct` (a fraction in `[0, 1]`).\n\n"
        "Errors:\n"
        "- `404` if the receipt token is not found.\n"
        "- `422` if `gap_pct` is outside `[0, 1]`.\n"
        "- `503` (`share_data_*`) if the allocation-share lookup fails."
    ),
)
async def get_bad_debt(
    receipt_token_id: int,
    gap_pct: Decimal = Query(description="Collateral gap fraction in [0, 1].", examples=["0.10"]),
    service: CryptoLendingRiskService = Depends(get_crypto_lending_risk_service),
) -> BadDebtResponse:
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


@router.get(
    "/risk/{receipt_token_id}/breakdown",
    response_model=RiskBreakdownResponse,
    summary="Risk-enriched collateral breakdown",
    description=(
        "Return the full risk-enriched collateral breakdown for a receipt-token position: "
        "one row per backing token with amount, USD value, price, liquidation threshold, and bonus.\n\n"
        "Errors:\n"
        "- `404` if the receipt token is not found.\n"
        "- `503` (`share_data_*`) if the allocation-share lookup fails."
    ),
)
async def get_risk_breakdown(
    receipt_token_id: int,
    service: CryptoLendingRiskService = Depends(get_crypto_lending_risk_service),
) -> RiskBreakdownResponse:
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


# ---------------------------------------------------------------------------
# Unified /v1/risk/rrc{,/scenario} — registry-dispatched, multi-model
# ---------------------------------------------------------------------------


class RrcRequest(BaseModel):
    """POST /v1/risk/rrc/scenario body — overrides keyed by model name."""

    asset_id: int = Field(ge=1, description="Surrogate receipt-token id.", examples=[42])
    prime_id: EthAddressParam = Field(
        description="Prime's 0x-prefixed Ethereum address.",
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    overrides: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description=(
            "Per-model scenario overrides. Outer keys are registered risk-model names "
            "(`suraf`, `gap_sweep`); inner objects are model-specific. For example, "
            "`gap_sweep` accepts `gap_pct` (a price-drop fraction in `[0, 1]`) and "
            "`suraf` accepts `usd_exposure`. Unknown outer keys are rejected with `422`."
        ),
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "asset_id": 42,
                "prime_id": "0x1234567890abcdef1234567890abcdef12345678",
                "overrides": {"gap_sweep": {"gap_pct": "0.15"}},
            }
        }
    }


class RrcEnvelope(BaseModel):
    """Response wrapper carrying one RrcResult per applicable model.

    On-chain identifiers (``chain_id``, ``receipt_token_address``) are echoed
    so consumers can resolve the surrogate ``asset_id`` without a second call.

    ``max_rrc_usd`` and ``max_crr_pct`` collapse every applicable model to a
    single conservative number. Use these when you need one capital figure
    rather than a per-model breakdown: ``max_rrc_usd`` is the largest USD
    figure across results; ``max_crr_pct`` is the largest
    ``results[].comparable_crr_pct``. Each model computes that value on the
    same receipt-token USD exposure basis, so it is safe to compare across
    models.

    Per-model values in ``results`` are *not* additive — SURAF capital and
    gap-sweep expected-loss overlap economically. Pick a single result, or
    use the ``max_*`` fields; do not sum across results. The two ``max_*``
    fields may come from different models.
    """

    asset_id: int = Field(description="Echo of the requested asset id.", examples=[42])
    chain_id: int = Field(description="EVM chain id of the receipt token.", examples=[1])
    receipt_token_address: str = Field(
        description="0x-prefixed contract address of the receipt token.",
        examples=["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
    )
    prime_id: str = Field(
        description="Echo of the requested prime address.",
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    results: list[RrcResult] = Field(description="One entry per applicable risk model.")
    max_rrc_usd: Decimal = Field(
        description="Largest `rrc_usd` across `results`. Decimal serialized as a JSON string.",
        examples=["12300"],
    )
    max_crr_pct: Decimal = Field(
        description="Largest `comparable_crr_pct` across `results`, as a 0–100 percentage.",
        examples=["33.7"],
    )


@router.get(
    "/risk/rrc",
    response_model=RrcEnvelope,
    summary="Risk capital (RRC) at default stress",
    description=(
        "Compute RRC at default stress for every model that applies to "
        "`(asset_id, prime_id)`. See `RrcEnvelope` for how to interpret per-model "
        "`results` versus the `max_*` summary fields.\n\n"
        "Errors:\n"
        "- `404` if `asset_id` is not a known receipt token, or no models apply.\n"
        "- `422` if `prime_id` is malformed or `asset_id < 1`.\n"
        "- `503` (`share_data_missing` / `share_data_stale`) if share-data lookup fails."
    ),
)
async def get_rrc(
    asset_id: Annotated[int, Query(ge=1, description="Surrogate receipt-token id.", examples=[42])],
    prime_id: EthAddressParam,
    registry: ModelRegistry = Depends(get_model_registry),
    receipt_token_lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> RrcEnvelope:
    return await _compute_envelope(
        asset_id, EthAddress(prime_id), overrides={}, registry=registry, lookup=receipt_token_lookup
    )


@router.post(
    "/risk/rrc/scenario",
    response_model=RrcEnvelope,
    summary="Risk capital (RRC) with scenario overrides",
    description=(
        "Compute RRC with per-model scenario overrides for every applicable model. "
        "Outer override keys must be valid model names; unknown keys reject the request "
        "with `422`. See `RrcEnvelope` for how to interpret per-model `results` versus the "
        "`max_*` summary fields.\n\n"
        "Errors:\n"
        "- `404` if `asset_id` is not a known receipt token, or no models apply.\n"
        "- `422` if `prime_id`/`asset_id` are invalid, an unknown override model key is "
        "present, or any model rejects its overrides.\n"
        "- `503` (`share_data_missing` / `share_data_stale`) if share-data lookup fails."
    ),
)
async def post_rrc_scenario(
    body: RrcRequest,
    registry: ModelRegistry = Depends(get_model_registry),
    receipt_token_lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> RrcEnvelope:
    return await _compute_envelope(
        body.asset_id,
        EthAddress(body.prime_id),
        overrides=body.overrides,
        registry=registry,
        lookup=receipt_token_lookup,
    )


@router.post(
    "/risk/rrc",
    response_model=RrcEnvelope,
    include_in_schema=False,
    deprecated=True,
)
async def post_rrc(
    body: RrcRequest,
    registry: ModelRegistry = Depends(get_model_registry),
    receipt_token_lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> RrcEnvelope:
    return await post_rrc_scenario(body, registry, receipt_token_lookup)


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
            detail=f"no risk models apply for asset_id={asset_id}",
        )

    results: list[RrcResult] = []
    for m in applicable:
        try:
            result = await m.compute(asset_id, prime_id, overrides.get(m.risk_model, {}))
        except AllocationShareError as exc:
            raise _share_error_503(exc) from exc
        except InvalidOverrideError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        results.append(result)

    return RrcEnvelope(
        asset_id=asset_id,
        chain_id=info.chain_id,
        receipt_token_address=info.receipt_token_address_hex,
        prime_id=str(prime_id),
        results=results,
        max_rrc_usd=max(r.rrc_usd for r in results),
        max_crr_pct=max(r.comparable_crr_pct for r in results),
    )
