# ruff: noqa: E501
"""Shared helpers for the Morpho backed-breakdown repositories (V1/V1.1 and VaultV2).

Both repositories resolve a vault the same way and map a priced breakdown row to a
``CollateralContribution`` with identical USD-scaling, so that logic lives here once.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.backed_breakdown import CollateralContribution


@dataclass(frozen=True)
class MorphoVaultRef:
    """A resolved Morpho vault: its internal id plus its ``vault_version``.

    ``vault_version`` (morpho_vault.vault_version) selects the backed-breakdown walk:
    1 = MetaMorpho V1, 2 = MetaMorpho V1.1 (both direct Morpho-Blue allocation), 3 =
    Morpho VaultV2 (adapter-based). Returned by ``resolve_morpho_vault`` so the reader
    can dispatch on version without embedding SQL.
    """

    id: int
    vault_version: int


# Resolve a vault's internal id AND version in one round trip, from its natural key.
_VAULT_RESOLVE_SQL = """
SELECT id, vault_version FROM morpho_vault WHERE address = :addr AND chain_id = :chain_id
"""


async def resolve_morpho_vault(engine: AsyncEngine, address: bytes, chain_id: int) -> MorphoVaultRef | None:
    """Resolve a Morpho vault's internal id and ``vault_version`` from its onchain address."""
    async with engine.connect() as conn:
        result = await conn.execute(text(_VAULT_RESOLVE_SQL), {"addr": address, "chain_id": chain_id})
        row = result.fetchone()
    return MorphoVaultRef(id=row.id, vault_version=row.vault_version) if row is not None else None


def to_contribution(row: Any) -> CollateralContribution:
    """Map a priced backed-breakdown row to a ``CollateralContribution``.

    ``backed_amount`` is in loan-token units; scaling by the loan-token price yields a
    USD ``backing_value`` (what enrichment reads as amount_usd), correct even when the
    loan token is not ~$1. ``price_usd`` is each row token's OWN price so amount and
    price stay denominated in the row's symbol; it is None for a collateral token the
    oracle does not price, and that row drops at enrichment.

    If the vault's loan token itself has no price, no backing amount can be converted
    to USD: keep raw loan-token units and force ``price_usd`` None on every row. The
    risk service treats an all-unpriced breakdown as price_data_missing.
    """
    backed_amount = Decimal(str(row.backed_amount))
    loan_token_price = Decimal(str(row.loan_token_price)) if row.loan_token_price is not None else None
    if loan_token_price is None:
        return CollateralContribution(
            token_id=row.token_id,
            symbol=row.symbol,
            backing_value=backed_amount,
            backing_pct=Decimal(str(row.backing_pct)),
            price_usd=None,
        )
    token_price_usd = Decimal(str(row.token_price_usd)) if row.token_price_usd is not None else None
    return CollateralContribution(
        token_id=row.token_id,
        symbol=row.symbol,
        backing_value=backed_amount * loan_token_price,
        backing_pct=Decimal(str(row.backing_pct)),
        price_usd=token_price_usd,
    )
