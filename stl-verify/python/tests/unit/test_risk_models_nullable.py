from decimal import Decimal

from app.api.v1.risk import RiskBreakdownItemResponse
from app.domain.entities.backed_breakdown import CollateralContribution
from app.domain.entities.risk import RiskEnrichedCollateral


def test_collateral_contribution_allows_null_token_id():
    c = CollateralContribution(
        token_id=None,
        symbol="BTC",
        backing_value=Decimal("1"),
        backing_pct=Decimal("100"),
        price_usd=Decimal("65000"),
    )
    assert c.token_id is None


def test_risk_enriched_collateral_allows_null_maple_fields():
    c = RiskEnrichedCollateral(
        token_id=None,
        symbol="BTC",
        amount=Decimal("1"),
        backing_pct=Decimal("100"),
        amount_usd=Decimal("65000"),
        price_usd=Decimal("65000"),
        liquidation_threshold=None,
        liquidation_bonus=None,
    )
    assert c.token_id is None
    assert c.liquidation_threshold is None
    assert c.liquidation_bonus is None


def test_risk_breakdown_item_allows_null_maple_fields():
    item = RiskBreakdownItemResponse(
        token_id=None,
        symbol="BTC",
        amount=Decimal("1"),
        backing_pct=Decimal("100"),
        amount_usd=Decimal("65000"),
        price_usd=Decimal("65000"),
        liquidation_threshold=None,
        liquidation_bonus=None,
    )
    assert item.token_id is None
    assert item.liquidation_threshold is None
    assert item.liquidation_bonus is None
