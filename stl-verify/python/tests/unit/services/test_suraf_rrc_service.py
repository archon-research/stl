from datetime import datetime, timezone
from decimal import Decimal

from app.risk_engine.suraf.result import SurafResult
from app.services.suraf_rrc_service import SurafRrcService


def _rating(rating_id: str, crr_pct: str, sha: str = "abc123", version: str = "v1") -> SurafResult:
    return SurafResult(
        rating_id=rating_id,
        version=version,
        crr_pct=Decimal(crr_pct),
        unadjusted_crr_pct=Decimal(crr_pct),
        penalty_pp=Decimal("0"),
        avg_score=Decimal("3"),
        source_commit_sha=sha,
        loaded_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


class TestSurafRrcService:
    def test_mapped_receipt_token(self) -> None:
        service = SurafRrcService(
            asset_to_rating={1: "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "33.7", version="v7")},
        )

        result = service.compute(1, Decimal("1000"))

        assert result is not None
        assert result.receipt_token_id == 1
        assert result.usd_exposure == Decimal("1000")
        assert result.rating_id == "aave_ausdc"
        assert result.rating_version == "v7"
        assert result.crr_pct == Decimal("33.7")
        assert result.rrc_usd == Decimal("337.0")
        assert result.source_commit_sha == "abc123"

    def test_unmapped_receipt_token_returns_none(self) -> None:
        service = SurafRrcService(asset_to_rating={}, suraf_ratings={})
        assert service.compute(999, Decimal("1000")) is None

    def test_zero_crr(self) -> None:
        service = SurafRrcService(
            asset_to_rating={1: "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "0")},
        )

        result = service.compute(1, Decimal("1000"))

        assert result is not None
        assert result.rrc_usd == Decimal("0")

    def test_is_pure(self) -> None:
        """Repeated calls with the same inputs produce identical results."""
        service = SurafRrcService(
            asset_to_rating={1: "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "25")},
        )

        assert service.compute(1, Decimal("500")) == service.compute(1, Decimal("500"))
