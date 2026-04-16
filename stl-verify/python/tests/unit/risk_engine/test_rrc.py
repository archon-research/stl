from datetime import datetime, timezone
from decimal import Decimal

from app.domain.entities.risk import RiskBreakdown, RiskEnrichedCollateral
from app.risk_engine.rrc import compute_portfolio_rrc, compute_scenario_rrc
from app.risk_engine.suraf.result import SurafResult


def _item(
    token_id: int,
    symbol: str,
    amount_usd: str = "100",
) -> RiskEnrichedCollateral:
    return RiskEnrichedCollateral(
        token_id=token_id,
        symbol=symbol,
        amount=Decimal("1"),
        backing_pct=Decimal("100"),
        amount_usd=Decimal(amount_usd),
        price_usd=Decimal(amount_usd),
        liquidation_threshold=Decimal("0.85"),
        liquidation_bonus=Decimal("1.05"),
    )


def _breakdown(*items: RiskEnrichedCollateral) -> RiskBreakdown:
    return RiskBreakdown(backed_asset_id=42, items=tuple(items))


def _rating(rating_id: str, crr_pct: str, sha: str = "abc123") -> SurafResult:
    return SurafResult(
        rating_id=rating_id,
        crr_pct=Decimal(crr_pct),
        unadjusted_crr_pct=Decimal(crr_pct),
        penalty_pp=Decimal("0"),
        avg_score=Decimal("3"),
        source_commit_sha=sha,
        loaded_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


class TestComputePortfolioRrc:
    def test_single_mapped_item(self) -> None:
        breakdown = _breakdown(_item(1, "aUSDC", "1000"))
        mapping = {"aUSDC": "aave_ausdc"}
        ratings = {"aave_ausdc": _rating("aave_ausdc", "33.7")}

        result = compute_portfolio_rrc(breakdown, mapping, ratings)

        assert result.total_exposure_usd == Decimal("1000")
        assert result.modeled_exposure_usd == Decimal("1000")
        assert result.final_crr_pct == Decimal("33.7")
        assert result.final_rrc_usd == Decimal("337.0")

        assert len(result.models) == 1
        m = result.models[0]
        assert m.name == "suraf"
        assert m.source_commit_sha == "abc123"
        assert m.modeled_exposure_usd == Decimal("1000")
        assert m.crr_pct == Decimal("33.7")
        assert m.rrc_usd == Decimal("337.0")

        assert len(result.items) == 1
        it = result.items[0]
        assert it.token_id == 1
        assert it.symbol == "aUSDC"
        assert it.amount_usd == Decimal("1000")
        assert it.rating_id == "aave_ausdc"
        assert it.crr_pct == Decimal("33.7")
        assert it.rrc_usd == Decimal("337.0")

    def test_multi_item_all_mapped_weighted_avg(self) -> None:
        breakdown = _breakdown(
            _item(1, "aUSDC", "600"),
            _item(2, "spUSDC", "400"),
        )
        mapping = {"aUSDC": "aave_ausdc", "spUSDC": "sparklend_spusdc"}
        ratings = {
            "aave_ausdc": _rating("aave_ausdc", "30"),
            "sparklend_spusdc": _rating("sparklend_spusdc", "50"),
        }

        result = compute_portfolio_rrc(breakdown, mapping, ratings)

        # 600 * 30% + 400 * 50% = 180 + 200 = 380; modeled = 1000;
        # SURAF's weighted-avg CRR over its modeled exposure = 38%
        assert result.total_exposure_usd == Decimal("1000")
        assert result.modeled_exposure_usd == Decimal("1000")
        assert result.final_crr_pct == Decimal("38")
        assert result.final_rrc_usd == Decimal("380")

    def test_unmapped_item_appears_but_excluded_from_aggregate(self) -> None:
        """Coverage gap must not dilute the final CRR.

        60% mapped at 50% CRR + 40% unmapped. Exposure-over-total would be
        30% (dangerous: hides missing mapping as lower risk). Exposure-over-
        modeled keeps final_crr_pct at 50% and surfaces the gap via the
        difference between ``total_exposure_usd`` and ``modeled_exposure_usd``.
        """
        breakdown = _breakdown(
            _item(1, "aUSDC", "600"),
            _item(2, "WBTC", "400"),  # not in mapping
        )
        mapping = {"aUSDC": "aave_ausdc"}
        ratings = {"aave_ausdc": _rating("aave_ausdc", "50")}

        result = compute_portfolio_rrc(breakdown, mapping, ratings)

        assert result.total_exposure_usd == Decimal("1000")
        assert result.modeled_exposure_usd == Decimal("600")
        assert result.final_crr_pct == Decimal("50")
        assert result.final_rrc_usd == Decimal("300")

        assert len(result.items) == 2
        mapped = next(i for i in result.items if i.symbol == "aUSDC")
        unmapped = next(i for i in result.items if i.symbol == "WBTC")
        assert mapped.rating_id == "aave_ausdc"
        assert mapped.rrc_usd == Decimal("300")
        assert unmapped.rating_id is None
        assert unmapped.crr_pct is None
        assert unmapped.rrc_usd is None

    def test_no_items_mapped(self) -> None:
        breakdown = _breakdown(_item(1, "WBTC", "1000"))
        mapping: dict[str, str] = {}
        ratings: dict[str, SurafResult] = {}

        result = compute_portfolio_rrc(breakdown, mapping, ratings)

        assert result.total_exposure_usd == Decimal("1000")
        assert result.modeled_exposure_usd == Decimal("0")
        assert result.final_crr_pct is None
        assert result.final_rrc_usd is None
        assert result.models == []
        assert len(result.items) == 1
        assert result.items[0].rating_id is None

    def test_empty_breakdown(self) -> None:
        result = compute_portfolio_rrc(_breakdown(), {"aUSDC": "aave_ausdc"}, {})

        assert result.total_exposure_usd == Decimal("0")
        assert result.modeled_exposure_usd == Decimal("0")
        assert result.final_crr_pct is None
        assert result.final_rrc_usd is None
        assert result.models == []
        assert result.items == []


class TestComputeScenarioRrc:
    def test_mapped_asset(self) -> None:
        mapping = {"aUSDC": "aave_ausdc"}
        ratings = {"aave_ausdc": _rating("aave_ausdc", "33.7")}

        result = compute_scenario_rrc("aUSDC", Decimal("1000"), mapping, ratings)

        assert result is not None
        assert result.asset == "aUSDC"
        assert result.usd_exposure == Decimal("1000")
        assert result.final_crr_pct == Decimal("33.7")
        assert result.final_rrc_usd == Decimal("337.0")
        assert len(result.models) == 1
        m = result.models[0]
        assert m.name == "suraf"
        assert m.source_commit_sha == "abc123"
        assert m.modeled_exposure_usd == Decimal("1000")

    def test_unmapped_asset_returns_none(self) -> None:
        result = compute_scenario_rrc("WBTC", Decimal("1000"), {}, {})
        assert result is None

    def test_scenario_is_pure_no_db(self) -> None:
        """Sanity: scenario takes only primitives; a second call with the
        same args produces the same result (except loaded_at on SurafResult)."""
        mapping = {"aUSDC": "aave_ausdc"}
        ratings = {"aave_ausdc": _rating("aave_ausdc", "25")}

        r1 = compute_scenario_rrc("aUSDC", Decimal("500"), mapping, ratings)
        r2 = compute_scenario_rrc("aUSDC", Decimal("500"), mapping, ratings)

        assert r1 is not None and r2 is not None
        assert r1.final_rrc_usd == r2.final_rrc_usd == Decimal("125.00")
