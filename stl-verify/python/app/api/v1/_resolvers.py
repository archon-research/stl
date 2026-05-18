"""Resolution helpers for the address-based ``/v1/...`` endpoints.

These helpers translate the on-chain identity ``(chain_id, token_address)``
into the internal surrogate-ID entities that the service layer still
consumes. Centralised here so the 404 contract (including the "did you
mean" hint for receipt-token vs underlying-token mix-ups) stays uniform
across endpoints.
"""

from typing import Protocol

from fastapi import HTTPException

from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.token_catalog import TokenMetadata
from app.ports.receipt_token_lookup import ReceiptTokenLookup


class _TokenByAddressLookup(Protocol):
    """Narrow read protocol satisfied by both the repository and the service.

    The handler holds a ``TokenCatalogService``; the unit test holds a mock.
    Both expose ``get_token_by_chain_and_address`` — that is all the
    resolver needs, so we depend on the smaller interface.
    """

    async def get_token_by_chain_and_address(self, chain_id: int, address: EthAddress) -> TokenMetadata | None: ...


async def resolve_token(
    chain_id: int,
    token_address: str,
    repo: _TokenByAddressLookup,
) -> TokenMetadata:
    """Return the token row at ``(chain_id, token_address)`` or raise 404.

    ``token_address`` is the underlying ERC-20 contract address.
    """
    address = EthAddress(token_address)
    meta = await repo.get_token_by_chain_and_address(chain_id, address)
    if meta is None:
        raise HTTPException(status_code=404, detail="Token not found")
    return meta


async def resolve_receipt_token(
    chain_id: int,
    token_address: str,
    lookup: ReceiptTokenLookup,
) -> ReceiptTokenInfo:
    """Return the receipt-token row at ``(chain_id, token_address)`` or raise 404.

    On miss, attempts a second cheap lookup against the underlying-token
    table. If the address matches a registered underlying ERC-20, the 404
    body includes a hint listing up to three receipt tokens that wrap it.
    """
    address = EthAddress(token_address)
    info = await lookup.get_by_chain_and_address(chain_id, address)
    if info is not None:
        return info

    hint_candidates = await lookup.list_receipt_tokens_for_underlying(chain_id, address)
    if not hint_candidates:
        raise HTTPException(status_code=404, detail="Receipt token not found")

    suggestions = [
        {
            "chain_id": ref.chain_id,
            "receipt_token_address": ref.receipt_token_address_hex,
            "symbol": ref.symbol,
        }
        for ref in hint_candidates[:3]
    ]
    raise HTTPException(
        status_code=404,
        detail={
            "message": (
                "Receipt token not found. The address matches an underlying "
                "ERC-20; pass a receipt-token address instead."
            ),
            "suggestions": suggestions,
        },
    )


def check_exactly_one_asset_identity(
    asset_id: int | None,
    chain_id: int | None,
    token_address: str | None,
) -> None:
    """Raise ``ValueError`` unless exactly one of ``asset_id`` or ``(chain_id, token_address)`` supplied.

    ``chain_id`` and ``token_address`` count as a single identity pair:
    both must be provided together, or both omitted. Pydantic
    ``model_validator`` surfaces the ``ValueError`` as a normal 422.
    Handlers that call this directly should catch ``ValueError`` and
    re-raise as ``HTTPException(status_code=422, detail=...)``.
    """
    has_surrogate = asset_id is not None
    pair_provided = chain_id is not None or token_address is not None
    pair_complete = chain_id is not None and token_address is not None

    if pair_provided and not pair_complete:
        raise ValueError("chain_id and token_address must be supplied together")
    if has_surrogate and pair_complete:
        raise ValueError("provide exactly one of asset_id or (chain_id, token_address); got both")
    if not has_surrogate and not pair_complete:
        raise ValueError("provide exactly one of asset_id or (chain_id, token_address); got neither")
