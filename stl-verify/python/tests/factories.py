"""Domain-entity factories for the unit test suite."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.domain.entities.allocation import (
    AnchorageCustodyHolding,
    DirectAssetHolding,
    Psm3Position,
    ReceiptTokenPosition,
)

# The live feed is frozen at this snapshot_time (upstream outage since
# 2026-06-16). The staleness test asserts it surfaces verbatim.
ANCHORAGE_FROZEN_AS_OF = datetime(2026, 6, 16, 19, 45, 17, tzinfo=UTC)


def make_receipt_token_position(**overrides: Any) -> ReceiptTokenPosition:
    defaults: dict[str, Any] = dict(
        chain_id=1,
        receipt_token_id=1,
        receipt_token_address="0x" + "a" * 40,
        underlying_token_id=10,
        underlying_token_address="0x" + "b" * 40,
        symbol="aUSDC",
        underlying_symbol="USDC",
        protocol_name="aave_v3",
        balance=Decimal("100.0"),
    )
    defaults.update(overrides)
    return ReceiptTokenPosition(**defaults)


def make_direct_asset_holding(**overrides: Any) -> DirectAssetHolding:
    defaults: dict[str, Any] = dict(
        chain_id=1,
        token_id=99,
        token_address="0x" + "c" * 40,
        symbol="PYUSD",
        balance=Decimal("250.0"),
    )
    defaults.update(overrides)
    return DirectAssetHolding(**defaults)


def make_anchorage_custody_holding(**overrides: Any) -> AnchorageCustodyHolding:
    """Defaults mirror the last-poll BTC/AnchorageCustody cohort: $250M loan
    (exposure), $309.67M collateral (package value), 4722.61 BTC.
    """
    defaults: dict[str, Any] = dict(
        symbol="BTC",
        custody_type="AnchorageCustody",
        balance=Decimal("4722.61"),
        amount_usd=Decimal("250000000"),
        collateral_usd=Decimal("309672229"),
        as_of=ANCHORAGE_FROZEN_AS_OF,
    )
    defaults.update(overrides)
    return AnchorageCustodyHolding(**defaults)


def make_psm3_position(**overrides: Any) -> Psm3Position:
    """Defaults mirror a PSM3 LP stake: optimism PSM3, ~$1M par, 1M shares."""
    defaults: dict[str, Any] = dict(
        chain_id=10,
        psm3_address="0x" + "e0" * 20,
        alm_address="0x" + "87" * 20,
        shares=Decimal("1000000"),
        asset_value=Decimal("1000000"),
        block_number=12345678,
        block_timestamp=datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
    )
    defaults.update(overrides)
    return Psm3Position(**defaults)
