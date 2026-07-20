from decimal import Decimal
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api._share_errors import share_error_503
from app.api._validators import (
    ChainIdPath,
    EthAddressParam,
    OptionalEthAddressParam,
    TokenAddressPath,
)
from app.api.deps import (
    get_crypto_lending_risk_service,
    get_model_registry,
    get_receipt_token_lookup,
)
from app.api.v1._resolvers import (
    AssetById,
    AssetIdentity,
    parse_asset_identity,
    resolve_receipt_token,
)
from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import RrcResult
from app.domain.exceptions import (
    AllocationShareError,
    InvalidOverrideError,
)
from app.domain.serialization import PlainDecimal
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry

router = APIRouter(tags=["risk"])

_ZERO = Decimal("0")
_ONE = Decimal("1")


def _parse_optional_prime(prime_id: str | None) -> EthAddress | None:
    """Build an ``EthAddress`` from a validated optional prime query param."""
    return EthAddress(prime_id) if prime_id is not None else None


class BadDebtResponse(BaseModel):
    """Estimated bad debt for a receipt-token position at a given collateral gap."""

    receipt_token_id: int = Field(description="Surrogate id of the receipt token.", examples=[42])
    gap_pct: PlainDecimal = Field(
        description="Collateral price gap as a fraction in `[0, 1]`. Decimal serialized as a JSON string.",
        examples=["0.10"],
    )
    bad_debt_usd: PlainDecimal = Field(
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

    token_id: int | None = Field(
        default=None,
        description=(
            "Surrogate token id of the backing token. Null for symbol-keyed collateral (e.g. Maple custody assets)."
        ),
        examples=[101],
    )
    symbol: str = Field(description="Backing-token symbol.", examples=["WETH"])
    amount: PlainDecimal = Field(
        description="Backing-token amount, expressed in token units. Decimal serialized as a JSON string.",
        examples=["12.345678"],
    )
    backing_pct: PlainDecimal = Field(
        description="Share of the receipt token backed by this row, as a 0–100 percentage.",
        examples=["42.0"],
    )
    amount_usd: PlainDecimal = Field(
        description="USD value of the backing-token row.",
        examples=["41234.56"],
    )
    price_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Latest USD price for the backing token. Null when the price is unavailable "
            "(e.g. a Maple custody asset whose attested price is missing); in that case "
            "`amount` is 0 while `amount_usd` is still the attested USD value."
        ),
        examples=["3340.55"],
    )
    liquidation_threshold: PlainDecimal | None = Field(
        default=None,
        description=(
            "Lender's liquidation threshold (LTV ratio) for the backing token, in `[0, 1]`. "
            "Null when the protocol has no per-asset threshold (e.g. Maple)."
        ),
        examples=["0.83"],
    )
    liquidation_bonus: PlainDecimal | None = Field(
        default=None,
        description=(
            "Liquidation bonus expressed as a multiplier (e.g. `1.05` for a 5% bonus). "
            "Stored as basis points upstream and normalised by dividing by 10000. "
            "Null when the protocol has no per-asset bonus (e.g. Maple)."
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


async def _compute_bad_debt(
    receipt_token_id: int,
    gap_pct: Decimal,
    service: CryptoLendingRiskService,
) -> BadDebtResponse:
    if not (_ZERO <= gap_pct <= _ONE):
        raise HTTPException(status_code=422, detail="gap_pct must be between 0 and 1")

    try:
        bad_debt = await service.get_bad_debt_legacy(receipt_token_id, gap_pct)
    except AllocationShareError as exc:
        raise share_error_503(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if bad_debt is None:
        raise HTTPException(404, "receipt token not found")
    return BadDebtResponse(
        receipt_token_id=receipt_token_id,
        gap_pct=gap_pct,
        bad_debt_usd=bad_debt,
    )


async def _compute_risk_breakdown(
    receipt_token_id: int,
    service: CryptoLendingRiskService,
    prime_id: EthAddress | None = None,
) -> RiskBreakdownResponse:
    try:
        breakdown = await service.get_risk_breakdown(receipt_token_id, prime_id)
    except AllocationShareError as exc:
        raise share_error_503(exc) from exc
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


@router.get(
    "/risk/{receipt_token_id}/bad-debt",
    response_model=BadDebtResponse,
    summary="Estimate bad debt at a collateral gap (deprecated)",
    tags=["internal"],
    deprecated=True,
    description=(
        "Estimate USD bad debt for a receipt-token position when collateral prices "
        "fall by `gap_pct` (a fraction in `[0, 1]`).\n\n"
        "**Deprecated.** Prefer `/v1/risk/{chain_id}/{token_address}/bad-debt`.\n\n"
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
    return await _compute_bad_debt(receipt_token_id, gap_pct, service)


@router.get(
    "/risk/{receipt_token_id}/breakdown",
    response_model=RiskBreakdownResponse,
    summary="Risk-enriched collateral breakdown (deprecated)",
    deprecated=True,
    description=(
        "Return the full risk-enriched collateral breakdown for a receipt-token position: "
        "one row per backing token with amount, USD value, price, liquidation threshold, and bonus.\n\n"
        "Pass an optional `prime_id` to scale the breakdown to that prime's position "
        "(per-prime, pro-rata by pool share); omit it for the pool-level breakdown.\n\n"
        "**Deprecated.** Prefer `/v1/risk/{chain_id}/{token_address}/breakdown`.\n\n"
        "Errors:\n"
        "- `404` if the receipt token is not found.\n"
        "- `422` if `prime_id` is malformed.\n"
        "- `503` (`share_data_*`) if the allocation-share lookup fails."
    ),
)
async def get_risk_breakdown(
    receipt_token_id: int,
    prime_id: Annotated[
        OptionalEthAddressParam,
        Query(
            description="Optional prime address; scales the breakdown to that prime's pro-rata pool share.",
            examples=["0x1234567890abcdef1234567890abcdef12345678"],
        ),
    ] = None,
    service: CryptoLendingRiskService = Depends(get_crypto_lending_risk_service),
) -> RiskBreakdownResponse:
    return await _compute_risk_breakdown(receipt_token_id, service, _parse_optional_prime(prime_id))


@router.get(
    "/risk/{chain_id}/{token_address}/bad-debt",
    response_model=BadDebtResponse,
    summary="Estimate bad debt at a collateral gap (by chain id and receipt-token address)",
    description=(
        "Estimate USD bad debt for the receipt-token position at "
        "`(chain_id, token_address)` when collateral prices fall by `gap_pct` "
        "(a fraction in `[0, 1]`).\n\n"
        "`token_address` is the **receipt-token** address (e.g. `aUSDC`, `spWETH`), "
        "not the underlying ERC-20 address. Passing an underlying address yields a "
        "`404` whose body suggests matching receipt tokens.\n\n"
        "Errors:\n"
        "- `404` if the receipt token is not found.\n"
        "- `422` if `chain_id` < 1, `token_address` is malformed, or `gap_pct` is "
        "outside `[0, 1]`.\n"
        "- `503` (`share_data_*`) if the allocation-share lookup fails."
    ),
)
async def get_bad_debt_by_address(
    chain_id: ChainIdPath,
    token_address: TokenAddressPath,
    gap_pct: Decimal = Query(description="Collateral gap fraction in [0, 1].", examples=["0.10"]),
    service: CryptoLendingRiskService = Depends(get_crypto_lending_risk_service),
    lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> BadDebtResponse:
    info = await resolve_receipt_token(chain_id, token_address, lookup)
    return await _compute_bad_debt(info.receipt_token_id, gap_pct, service)


@router.get(
    "/risk/{chain_id}/{token_address}/breakdown",
    response_model=RiskBreakdownResponse,
    summary="Risk-enriched collateral breakdown (by chain id and receipt-token address)",
    description=(
        "Return the full risk-enriched collateral breakdown for the receipt-token "
        "position at `(chain_id, token_address)`.\n\n"
        "`token_address` is the **receipt-token** address (e.g. `aUSDC`, `spWETH`), "
        "not the underlying ERC-20 address. Passing an underlying address yields a "
        "`404` whose body suggests matching receipt tokens.\n\n"
        "Pass an optional `prime_id` to scale the breakdown to that prime's position "
        "(per-prime, pro-rata by pool share); omit it for the pool-level breakdown.\n\n"
        "Errors:\n"
        "- `404` if the receipt token is not found.\n"
        "- `422` if `chain_id` < 1, `token_address` is malformed, or `prime_id` is malformed.\n"
        "- `503` (`share_data_*`) if the allocation-share lookup fails."
    ),
)
async def get_risk_breakdown_by_address(
    chain_id: ChainIdPath,
    token_address: TokenAddressPath,
    prime_id: Annotated[
        OptionalEthAddressParam,
        Query(
            description="Optional prime address; scales the breakdown to that prime's pro-rata pool share.",
            examples=["0x1234567890abcdef1234567890abcdef12345678"],
        ),
    ] = None,
    service: CryptoLendingRiskService = Depends(get_crypto_lending_risk_service),
    lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> RiskBreakdownResponse:
    info = await resolve_receipt_token(chain_id, token_address, lookup)
    return await _compute_risk_breakdown(info.receipt_token_id, service, _parse_optional_prime(prime_id))


# ---------------------------------------------------------------------------
# Unified /v1/risk/rrc{,/scenario} — registry-dispatched, multi-model
# ---------------------------------------------------------------------------


class RrcRequest(BaseModel):
    """POST /v1/risk/rrc/scenario body — overrides keyed by model name.

    Asset identity must be supplied as **exactly one** of:

    * ``asset_id`` (deprecated surrogate id), or
    * ``(chain_id, token_address)`` — receipt-token address on the given chain.

    Both forms or neither raises 422.
    """

    asset_id: int | None = Field(
        default=None,
        ge=1,
        description="Surrogate receipt-token id. **Deprecated** — pass `chain_id` + `token_address` instead.",
        examples=[42],
        deprecated=True,
    )
    chain_id: int | None = Field(
        default=None,
        ge=1,
        description="EVM chain id of the receipt token.",
        examples=[1],
    )
    token_address: OptionalEthAddressParam = Field(
        default=None,
        description="0x-prefixed receipt-token contract address.",
        examples=["0xbcca60bb61934080951369a648fb03df4f96263c"],
    )
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
                "chain_id": 1,
                "token_address": "0xbcca60bb61934080951369a648fb03df4f96263c",
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
    max_rrc_usd: PlainDecimal = Field(
        description="Largest `rrc_usd` across `results`. Decimal serialized as a JSON string.",
        examples=["12300"],
    )
    max_crr_pct: PlainDecimal = Field(
        description="Largest `comparable_crr_pct` across `results`, as a 0–100 percentage.",
        examples=["33.7"],
    )


@router.get(
    "/risk/rrc",
    response_model=RrcEnvelope,
    summary="Risk capital (RRC) at default stress",
    description=(
        "Compute RRC at default stress for every model that applies to the asset. "
        "Identify the asset by **exactly one** of:\n\n"
        "- `asset_id` (deprecated surrogate id), or\n"
        "- `chain_id` + `token_address` (receipt-token address on the given chain).\n\n"
        "Passing both forms or neither yields a `422`.\n\n"
        "See `RrcEnvelope` for how to interpret per-model `results` versus the "
        "`max_*` summary fields.\n\n"
        "Errors:\n"
        "- `404` if the asset is not a known receipt token, or no models apply.\n"
        "- `422` if `prime_id` is malformed, identifiers are invalid, or the "
        "asset-identity mix is wrong (both forms / neither / partial pair).\n"
        "- `503` (`share_data_missing` / `share_data_stale`) if share-data lookup fails."
    ),
)
async def get_rrc(
    prime_id: EthAddressParam,
    asset_id: Annotated[
        int | None,
        Query(
            ge=1,
            description="Surrogate receipt-token id. **Deprecated** — prefer `chain_id` + `token_address`.",
            examples=[42],
            deprecated=True,
        ),
    ] = None,
    chain_id: Annotated[
        int | None,
        Query(ge=1, description="EVM chain id of the receipt token.", examples=[1]),
    ] = None,
    token_address: Annotated[
        OptionalEthAddressParam,
        Query(
            description="0x-prefixed receipt-token contract address.",
            examples=["0xbcca60bb61934080951369a648fb03df4f96263c"],
        ),
    ] = None,
    registry: ModelRegistry = Depends(get_model_registry),
    receipt_token_lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> RrcEnvelope:
    identity = parse_asset_identity(asset_id, chain_id, token_address)
    info = await _resolve_asset(identity, receipt_token_lookup)
    return await _compute_envelope(
        info=info,
        prime_id=EthAddress(prime_id),
        overrides={},
        registry=registry,
    )


@router.post(
    "/risk/rrc/scenario",
    response_model=RrcEnvelope,
    summary="Risk capital (RRC) with scenario overrides",
    description=(
        "Compute RRC with per-model scenario overrides for every applicable model. "
        "Identify the asset by **exactly one** of `asset_id` (deprecated) or "
        "`chain_id` + `token_address`. Outer override keys must be valid model names; "
        "unknown keys reject the request with `422`. See `RrcEnvelope` for how to "
        "interpret per-model `results` versus the `max_*` summary fields.\n\n"
        "Errors:\n"
        "- `404` if the asset is not a known receipt token, or no models apply.\n"
        "- `422` if identifiers are invalid, asset-identity mix is wrong, an unknown "
        "override model key is present, or any model rejects its overrides.\n"
        "- `503` (`share_data_missing` / `share_data_stale`) if share-data lookup fails."
    ),
)
async def post_rrc_scenario(
    body: RrcRequest,
    registry: ModelRegistry = Depends(get_model_registry),
    receipt_token_lookup: ReceiptTokenLookup = Depends(get_receipt_token_lookup),
) -> RrcEnvelope:
    identity = parse_asset_identity(body.asset_id, body.chain_id, body.token_address)
    info = await _resolve_asset(identity, receipt_token_lookup)
    return await _compute_envelope(
        info=info,
        prime_id=EthAddress(body.prime_id),
        overrides=body.overrides,
        registry=registry,
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


async def _resolve_asset(
    identity: AssetIdentity,
    lookup: ReceiptTokenLookup,
) -> ReceiptTokenInfo:
    """Resolve a typed asset identity to a ``ReceiptTokenInfo`` or raise 404."""
    if isinstance(identity, AssetById):
        info = await lookup.get(identity.asset_id)
        if info is None:
            raise HTTPException(status_code=404, detail=f"unknown asset_id={identity.asset_id}")
        return info
    return await resolve_receipt_token(identity.chain_id, identity.token_address, lookup)


async def _compute_envelope(
    *,
    info: ReceiptTokenInfo,
    prime_id: EthAddress,
    overrides: dict[str, dict[str, Any]],
    registry: ModelRegistry,
) -> RrcEnvelope:
    unknown_models = set(overrides) - registry.risk_model_names
    if unknown_models:
        raise HTTPException(
            status_code=422,
            detail=f"unknown override model keys: {sorted(unknown_models)}",
        )

    asset_id = info.receipt_token_id
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
            raise share_error_503(exc) from exc
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
