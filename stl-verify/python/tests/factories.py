"""Domain-entity factories for the unit test suite."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.domain.entities.allocation import (
    AnchorageCustodyHolding,
    DirectAssetHolding,
    EthAddress,
    ReceiptTokenPosition,
)
from app.domain.entities.prime import PrimeIdentity, PrimeScope, ProxyWallet
from app.domain.prime_registry import ProxyKind

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


def make_prime_identity(**overrides: Any) -> PrimeIdentity:
    defaults: dict[str, Any] = dict(
        id=1,
        name="spark",
        external_id="4bd9ee3c-58df-4587-9c04-63b928f1a169",
        vault_address=EthAddress("0x" + "ab" * 20),
    )
    defaults.update(overrides)
    return PrimeIdentity(**defaults)


def make_proxy_wallet(**overrides: Any) -> ProxyWallet:
    defaults: dict[str, Any] = dict(
        address=EthAddress("0x" + "11" * 20),
        chain_id=1,
        kind=ProxyKind.ALM,
    )
    defaults.update(overrides)
    return ProxyWallet(**defaults)


def make_prime_scope(**overrides: Any) -> PrimeScope:
    """A resolved scope with one ALM proxy and one SubProxy, as a real prime has."""
    defaults: dict[str, Any] = dict(
        identity=make_prime_identity(),
        wallets=(
            make_proxy_wallet(address=EthAddress("0x" + "11" * 20), kind=ProxyKind.ALM),
            make_proxy_wallet(address=EthAddress("0x" + "22" * 20), kind=ProxyKind.SUB_PROXY),
        ),
        unserved_chains=(),
    )
    defaults.update(overrides)
    return PrimeScope(**defaults)
