from datetime import datetime, timezone
from decimal import Decimal

from app.risk_engine.rrc import compute_rrc
from app.risk_engine.suraf.result import SurafResult


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


class TestComputeRrc:
    def test_mapped_asset(self) -> None:
        mapping = {"ausdc": "aave_ausdc"}
        ratings = {"aave_ausdc": _rating("aave_ausdc", "33.7")}

        result = compute_rrc("aUSDC", Decimal("1000"), mapping, ratings)

        assert result is not None
        assert result.asset == "aUSDC"
        assert result.usd_exposure == Decimal("1000")
        assert result.rating_id == "aave_ausdc"
        assert result.crr_pct == Decimal("33.7")
        assert result.rrc_usd == Decimal("337.0")
        assert result.source_commit_sha == "abc123"

    def test_unmapped_asset_returns_none(self) -> None:
        assert compute_rrc("WBTC", Decimal("1000"), {}, {}) is None

    def test_zero_crr(self) -> None:
        mapping = {"ausdc": "aave_ausdc"}
        ratings = {"aave_ausdc": _rating("aave_ausdc", "0")}

        result = compute_rrc("aUSDC", Decimal("1000"), mapping, ratings)

        assert result is not None
        assert result.rrc_usd == Decimal("0")

    def test_is_pure(self) -> None:
        """Repeated calls with the same inputs produce identical results."""
        mapping = {"ausdc": "aave_ausdc"}
        ratings = {"aave_ausdc": _rating("aave_ausdc", "25")}

        r1 = compute_rrc("aUSDC", Decimal("500"), mapping, ratings)
        r2 = compute_rrc("aUSDC", Decimal("500"), mapping, ratings)

        assert r1 == r2

    def test_case_insensitive_asset_lookup(self) -> None:
        mapping = {"ausdc": "aave_ausdc"}
        ratings = {"aave_ausdc": _rating("aave_ausdc", "50")}

        for variant in ("aUSDC", "AUSDC", "ausdc", "aUsDc"):
            result = compute_rrc(variant, Decimal("100"), mapping, ratings)
            assert result is not None
            assert result.rating_id == "aave_ausdc"
