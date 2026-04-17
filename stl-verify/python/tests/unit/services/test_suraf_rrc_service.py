from datetime import datetime, timezone
from decimal import Decimal

from app.risk_engine.suraf.result import SurafResult
from app.services.suraf_rrc_service import SurafRrcService


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


class TestSurafRrcService:
    def test_mapped_asset(self) -> None:
        service = SurafRrcService(
            asset_to_rating={"ausdc": "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "33.7")},
        )

        result = service.compute("aUSDC", Decimal("1000"))

        assert result is not None
        assert result.asset == "aUSDC"
        assert result.usd_exposure == Decimal("1000")
        assert result.rating_id == "aave_ausdc"
        assert result.crr_pct == Decimal("33.7")
        assert result.rrc_usd == Decimal("337.0")
        assert result.source_commit_sha == "abc123"

    def test_unmapped_asset_returns_none(self) -> None:
        service = SurafRrcService(asset_to_rating={}, suraf_ratings={})
        assert service.compute("WBTC", Decimal("1000")) is None

    def test_zero_crr(self) -> None:
        service = SurafRrcService(
            asset_to_rating={"ausdc": "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "0")},
        )

        result = service.compute("aUSDC", Decimal("1000"))

        assert result is not None
        assert result.rrc_usd == Decimal("0")

    def test_is_pure(self) -> None:
        """Repeated calls with the same inputs produce identical results."""
        service = SurafRrcService(
            asset_to_rating={"ausdc": "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "25")},
        )

        assert service.compute("aUSDC", Decimal("500")) == service.compute("aUSDC", Decimal("500"))

    def test_case_insensitive_asset_lookup(self) -> None:
        service = SurafRrcService(
            asset_to_rating={"ausdc": "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "50")},
        )

        for variant in ("aUSDC", "AUSDC", "ausdc", "aUsDc"):
            result = service.compute(variant, Decimal("100"))
            assert result is not None
            assert result.rating_id == "aave_ausdc"
