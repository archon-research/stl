"""Resolution helpers for the address-based ``/v1/...`` endpoints.

These helpers translate the on-chain identity ``(chain_id, token_address)``
into the internal surrogate-ID entities that the service layer still
consumes. Centralised here so the 404 contract stays uniform across
endpoints.
"""

from fastapi import HTTPException

from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.token_catalog import TokenMetadata
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.token_catalog_repository import TokenCatalogRepository


async def resolve_token(
    chain_id: int,
    token_address: str,
    repo: TokenCatalogRepository,
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
    """Return the receipt-token row at ``(chain_id, token_address)`` or raise 404."""
    address = EthAddress(token_address)
    info = await lookup.get_by_chain_and_address(chain_id, address)
    if info is None:
        raise HTTPException(status_code=404, detail="Receipt token not found")
    return info


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
