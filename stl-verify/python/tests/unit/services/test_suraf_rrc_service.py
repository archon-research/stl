from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import RrcResult, SurafDetails
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


def _mock_allocation_repo(return_value: Decimal = Decimal("1000")) -> AsyncMock:
    mock = AsyncMock()
    mock.get_usd_exposure.return_value = return_value
    return mock


class TestSurafRrcServiceLegacy:
    """Legacy compute_legacy() API — will be removed in VEC-183."""

    def test_mapped_receipt_token(self) -> None:
        service = SurafRrcService(
            asset_to_rating={1: "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "33.7", version="v7")},
            allocation_repo=_mock_allocation_repo(),
        )

        result = service.compute_legacy(1, Decimal("1000"))

        assert result is not None
        assert result.receipt_token_id == 1
        assert result.usd_exposure == Decimal("1000")
        assert result.rating_id == "aave_ausdc"
        assert result.rating_version == "v7"
        assert result.crr_pct == Decimal("33.7")
        assert result.rrc_usd == Decimal("337.0")
        assert result.source_commit_sha == "abc123"

    def test_unmapped_receipt_token_returns_none(self) -> None:
        service = SurafRrcService(
            asset_to_rating={},
            suraf_ratings={},
            allocation_repo=_mock_allocation_repo(),
        )
        assert service.compute_legacy(999, Decimal("1000")) is None

    def test_zero_crr(self) -> None:
        service = SurafRrcService(
            asset_to_rating={1: "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "0")},
            allocation_repo=_mock_allocation_repo(),
        )

        result = service.compute_legacy(1, Decimal("1000"))

        assert result is not None
        assert result.rrc_usd == Decimal("0")

    def test_is_pure(self) -> None:
        """Repeated calls with the same inputs produce identical results."""
        service = SurafRrcService(
            asset_to_rating={1: "aave_ausdc"},
            suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "25")},
            allocation_repo=_mock_allocation_repo(),
        )

        assert service.compute_legacy(1, Decimal("500")) == service.compute_legacy(1, Decimal("500"))


# ---------------------------------------------------------------------------
# RiskModel protocol tests (VEC-179)
# ---------------------------------------------------------------------------

DUMMY_PRIME = EthAddress("0x" + "ab" * 20)


def _service(
    asset_to_rating: dict[int, str] | None = None,
    suraf_ratings: dict[str, SurafResult] | None = None,
    allocation_repo: AsyncMock | None = None,
) -> SurafRrcService:
    if asset_to_rating is None:
        asset_to_rating = {1: "aave_ausdc"}
    if suraf_ratings is None:
        suraf_ratings = {"aave_ausdc": _rating("aave_ausdc", "33.7", version="v7")}
    if allocation_repo is None:
        allocation_repo = _mock_allocation_repo()
    return SurafRrcService(
        asset_to_rating=asset_to_rating,
        suraf_ratings=suraf_ratings,
        allocation_repo=allocation_repo,
    )


class TestModelAttribute:
    def test_model_is_suraf(self) -> None:
        assert _service().model == "suraf"


class TestAppliesTo:
    @pytest.mark.parametrize(
        "asset_id, expected",
        [
            (1, True),
            (999, False),
        ],
        ids=["mapped-asset", "unmapped-asset"],
    )
    def test_applies_to(self, asset_id: int, expected: bool) -> None:
        assert _service().applies_to(asset_id, DUMMY_PRIME) is expected


class TestRiskModelCompute:
    async def test_compute_with_usd_exposure_override(self) -> None:
        svc = _service()
        result = await svc.compute(1, DUMMY_PRIME, overrides={"usd_exposure": Decimal("1000")})

        assert isinstance(result, RrcResult)
        assert result.asset_id == 1
        assert result.prime_id == str(DUMMY_PRIME)
        assert result.model == "suraf"
        assert result.rrc_usd == Decimal("337.0")
        assert isinstance(result.details, SurafDetails)
        assert result.details.rating_id == "aave_ausdc"
        assert result.details.rating_version == "v7"
        assert result.details.crr_pct == Decimal("33.7")
        assert result.details.source_commit_sha == "abc123"

    async def test_compute_zero_crr_returns_zero_rrc(self) -> None:
        svc = _service(suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "0")})
        result = await svc.compute(1, DUMMY_PRIME, overrides={"usd_exposure": Decimal("1000")})

        assert result.rrc_usd == Decimal("0")

    @pytest.mark.parametrize(
        "overrides",
        [
            {"unknown_key": 42},
            {"usd_exposure": Decimal("100"), "extra": "bad"},
            {"foo": "bar"},
        ],
        ids=["single-unknown", "valid-plus-unknown", "arbitrary-key"],
    )
    async def test_compute_rejects_unknown_override_keys(self, overrides: dict) -> None:
        with pytest.raises(ValueError, match="unknown override"):
            await _service().compute(1, DUMMY_PRIME, overrides=overrides)

    @pytest.mark.parametrize(
        "bad_exposure",
        [Decimal("0"), Decimal("-1"), Decimal("-0.01")],
        ids=["zero", "negative", "slightly-negative"],
    )
    async def test_compute_rejects_non_positive_usd_exposure(self, bad_exposure: Decimal) -> None:
        with pytest.raises(ValueError, match="usd_exposure"):
            await _service().compute(1, DUMMY_PRIME, overrides={"usd_exposure": bad_exposure})

    async def test_compute_unmapped_asset_raises(self) -> None:
        with pytest.raises(ValueError, match="no rating mapped"):
            await _service().compute(999, DUMMY_PRIME, overrides={"usd_exposure": Decimal("1000")})

    async def test_compute_derives_exposure_from_port(self) -> None:
        """When usd_exposure is not in overrides, derive it from the exposure port."""
        mock_port = _mock_allocation_repo(Decimal("5000"))
        svc = _service(allocation_repo=mock_port)

        result = await svc.compute(1, DUMMY_PRIME, overrides={})

        mock_port.get_usd_exposure.assert_awaited_once_with(1, DUMMY_PRIME)
        assert result.rrc_usd == Decimal("5000") * Decimal("33.7") / Decimal("100")

    async def test_compute_override_takes_precedence_over_port(self) -> None:
        """Explicit usd_exposure override is used even if the port would return something else."""
        mock_port = _mock_allocation_repo(Decimal("9999"))
        svc = _service(allocation_repo=mock_port)

        result = await svc.compute(1, DUMMY_PRIME, overrides={"usd_exposure": Decimal("1000")})

        mock_port.get_usd_exposure.assert_not_awaited()
        assert result.rrc_usd == Decimal("337.0")

    async def test_compute_port_raises_propagates(self) -> None:
        """If the exposure port raises, the error propagates."""
        mock_port = AsyncMock()
        mock_port.get_usd_exposure.side_effect = ValueError("no position found")
        svc = _service(allocation_repo=mock_port)

        with pytest.raises(ValueError, match="no position found"):
            await svc.compute(1, DUMMY_PRIME, overrides={})
