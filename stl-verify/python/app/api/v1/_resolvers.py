"""Resolution helpers for the address-based ``/v1/...`` endpoints.

These helpers translate the on-chain identity ``(chain_id, token_address)``
into the internal surrogate-ID entities that the service layer still
consumes. Centralised here so the 404 contract stays uniform across
endpoints.

The ``AssetIdentity`` discriminated union and ``parse_asset_identity``
factory also live here so route handlers can lift the loose "one of
``asset_id`` or ``(chain_id, token_address)``" trio into a single typed
value at the API boundary, eliminating the need for trust-based asserts
downstream.
"""

from dataclasses import dataclass

from fastapi import HTTPException

from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.token_catalog import TokenMetadata
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.token_catalog_repository import TokenCatalogRepositoryPort


async def resolve_token(
    chain_id: int,
    token_address: str,
    repo: TokenCatalogRepositoryPort,
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


@dataclass(frozen=True, slots=True)
class AssetById:
    asset_id: int


@dataclass(frozen=True, slots=True)
class AssetByAddress:
    chain_id: int
    token_address: str


AssetIdentity = AssetById | AssetByAddress


def parse_asset_identity(
    asset_id: int | None,
    chain_id: int | None,
    token_address: str | None,
) -> AssetIdentity:
    """Lift loose identity inputs into a typed ``AssetIdentity`` or raise 422.

    Exactly one of ``asset_id`` or ``(chain_id, token_address)`` must be
    supplied. ``chain_id`` and ``token_address`` count as a single pair —
    both together, or both omitted. Once this returns, downstream code
    pattern-matches on the variant and never needs to inspect ``None``.
    """
    if asset_id is not None and chain_id is None and token_address is None:
        return AssetById(asset_id)
    if asset_id is None and chain_id is not None and token_address is not None:
        return AssetByAddress(chain_id, token_address)

    if (chain_id is None) != (token_address is None):
        detail = "chain_id and token_address must be supplied together"
    elif asset_id is not None:
        detail = "provide exactly one of asset_id or (chain_id, token_address); got both"
    else:
        detail = "provide exactly one of asset_id or (chain_id, token_address); got neither"
    raise HTTPException(status_code=422, detail=detail)
