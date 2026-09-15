from decimal import Decimal

from app.domain.entities.allocation import DirectAssetHolding
from app.services.self_backed_breakdown import build_self_backed_breakdown


def test_self_backed_breakdown_basic() -> None:
    holding = DirectAssetHolding(
        chain_id=1,
        token_id=99,
        token_address="0x" + "aa" * 20,
        symbol="RLUSD",
        balance=Decimal("1000.50"),
        amount_usd=Decimal("1001.25"),
    )

    result = build_self_backed_breakdown(holding)

    assert result.backed_asset_id == 99
    assert len(result.items) == 1
    item = result.items[0]
    assert item.token_id == 99
    assert item.symbol == "RLUSD"
    assert item.amount == Decimal("1000.50")
    assert item.backing_pct == Decimal("100")
    assert item.amount_usd == Decimal("1001.25")
    assert item.price_usd == Decimal("1001.25") / Decimal("1000.50")
    assert item.liquidation_threshold is None
    assert item.liquidation_bonus is None


def test_self_backed_breakdown_zero_balance() -> None:
    holding = DirectAssetHolding(
        chain_id=1,
        token_id=50,
        token_address="0x" + "bb" * 20,
        symbol="PYUSD",
        balance=Decimal("0"),
        amount_usd=None,
    )

    result = build_self_backed_breakdown(holding)

    assert result.backed_asset_id == 50
    item = result.items[0]
    assert item.amount == Decimal("0")
    assert item.amount_usd == Decimal("0")
    assert item.price_usd is None


def test_self_backed_breakdown_no_price() -> None:
    holding = DirectAssetHolding(
        chain_id=1,
        token_id=77,
        token_address="0x" + "cc" * 20,
        symbol="UNKNOWN",
        balance=Decimal("500"),
        amount_usd=None,
    )

    result = build_self_backed_breakdown(holding)

    item = result.items[0]
    assert item.amount == Decimal("500")
    assert item.amount_usd == Decimal("0")
    assert item.price_usd is None
