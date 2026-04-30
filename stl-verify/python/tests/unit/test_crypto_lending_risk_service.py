from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.allocation_share_repository import MissingShareError
from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import GapSweepDetails, LiquidationParams, RiskBreakdown, RrcResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService

DUMMY_PRIME = EthAddress("0x" + "ab" * 20)
RECEIPT_TOKEN_ID = 99
UNDERLYING_TOKEN_ID = 42
MORPHO_VAULT_ID = 55


def _breakdown(items: tuple[CollateralContribution, ...], backed_asset_id: int) -> BackedBreakdown:
    return BackedBreakdown(backed_asset_id=backed_asset_id, items=items)


def _contrib(token_id: int, symbol: str, backing_value: str, price_usd: str | None = "2000") -> CollateralContribution:
    return CollateralContribution(
        token_id=token_id,
        symbol=symbol,
        backing_value=Decimal(backing_value),
        backing_pct=Decimal("100"),
        price_usd=Decimal(price_usd) if price_usd is not None else None,
    )


def _params(token_id: int, lt: str, lb: str) -> LiquidationParams:
    return LiquidationParams(
        token_id=token_id,
        liquidation_threshold=Decimal(lt),
        liquidation_bonus=Decimal(lb),
    )


def _aave_like_info(protocol_name: str = "Aave V3") -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=RECEIPT_TOKEN_ID,
        protocol_id=1,
        underlying_token_id=UNDERLYING_TOKEN_ID,
        receipt_token_address=bytes.fromhex("e7df13b8e3d6740fe17cbe928c7334243d86c92f"),
        chain_id=1,
        protocol_name=protocol_name,
        receipt_token_token_id=777,
    )


def _morpho_info() -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=RECEIPT_TOKEN_ID,
        protocol_id=2,
        underlying_token_id=UNDERLYING_TOKEN_ID,
        receipt_token_address=bytes.fromhex("a7df13b8e3d6740fe17cbe928c7334243d86c92f"),
        chain_id=1,
        protocol_name="Morpho Blue",
        receipt_token_token_id=None,
    )


@pytest.fixture
def reader() -> MagicMock:
    reader = MagicMock()
    reader.get_receipt_token = AsyncMock(return_value=_aave_like_info())
    reader.get_breakdown = AsyncMock(
        return_value=_breakdown((_contrib(10, "WETH", "10000", "2000"),), backed_asset_id=UNDERLYING_TOKEN_ID)
    )
    reader.get_liquidation_params = AsyncMock(return_value={10: _params(10, "0.825", "1.05")})
    reader.get_share = AsyncMock(return_value=Decimal("1"))
    reader.get_legacy_share = AsyncMock(return_value=Decimal("1"))
    return reader


@pytest.fixture
def service(reader: MagicMock) -> CryptoLendingRiskService:
    return CryptoLendingRiskService(
        reader=reader,
        default_gap_pct=Decimal("0.15"),
        supported_asset_ids={RECEIPT_TOKEN_ID},
    )


class TestModelAttribute:
    def test_model_attribute_is_gap_sweep(self, service: CryptoLendingRiskService) -> None:
        assert service.model == "gap_sweep"


class TestAppliesTo:
    @pytest.mark.parametrize(
        ("asset_id", "expected"),
        [
            (RECEIPT_TOKEN_ID, True),
            (RECEIPT_TOKEN_ID + 1, False),
        ],
        ids=["supported-asset", "unsupported-asset"],
    )
    def test_applies_to_uses_supported_asset_ids(
        self,
        service: CryptoLendingRiskService,
        asset_id: int,
        expected: bool,
    ) -> None:
        assert service.applies_to(asset_id, DUMMY_PRIME) is expected


class TestCompute:
    @pytest.mark.asyncio
    async def test_compute_aave_like_with_default_gap_pct(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        info = _aave_like_info()

        result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

        assert isinstance(result, RrcResult)
        assert result.asset_id == RECEIPT_TOKEN_ID
        assert result.prime_id == str(DUMMY_PRIME)
        assert result.model == "gap_sweep"
        assert result.rrc_usd >= Decimal("0")
        assert isinstance(result.details, GapSweepDetails)
        assert result.details.gap_pct == Decimal("0.15")
        assert result.details.bad_debt_usd == result.rrc_usd
        reader.get_receipt_token.assert_awaited_once_with(RECEIPT_TOKEN_ID)
        reader.get_breakdown.assert_awaited_once_with(info)
        reader.get_liquidation_params.assert_awaited_once_with(info, UNDERLYING_TOKEN_ID, [10])
        reader.get_share.assert_awaited_once_with(info, DUMMY_PRIME)
        reader.get_legacy_share.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_compute_uses_gap_pct_override(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        override_result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={"gap_pct": Decimal("0.30")})
        default_result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

        assert override_result.details.gap_pct == Decimal("0.30")
        assert override_result.rrc_usd >= default_result.rrc_usd
        assert reader.get_share.await_count == 2

    @pytest.mark.asyncio
    async def test_compute_uses_breakdown_backed_asset_id_for_liquidation_lookup(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        info = _morpho_info()
        reader.get_receipt_token.return_value = info
        reader.get_breakdown.return_value = _breakdown(
            (_contrib(11, "USDC", "5000", "1"),),
            backed_asset_id=MORPHO_VAULT_ID,
        )
        reader.get_liquidation_params.return_value = {11: _params(11, "0.86", "1.03")}

        result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

        assert isinstance(result, RrcResult)
        assert result.asset_id == RECEIPT_TOKEN_ID
        reader.get_breakdown.assert_awaited_once_with(info)
        reader.get_liquidation_params.assert_awaited_once_with(info, MORPHO_VAULT_ID, [11])
        reader.get_share.assert_awaited_once_with(info, DUMMY_PRIME)

    @pytest.mark.asyncio
    async def test_compute_rejects_unsupported_asset(self, service: CryptoLendingRiskService) -> None:
        with pytest.raises(ValueError, match="unsupported asset_id"):
            await service.compute(RECEIPT_TOKEN_ID + 1, DUMMY_PRIME, overrides={})

    @pytest.mark.asyncio
    async def test_compute_raises_when_receipt_token_is_missing(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        reader.get_receipt_token.return_value = None

        with pytest.raises(ValueError, match="receipt token not found"):
            await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "overrides",
        [
            {"unknown_key": 42},
            {"gap_pct": Decimal("0.10"), "extra": "bad"},
            {"foo": "bar"},
        ],
        ids=["single-unknown", "valid-plus-unknown", "arbitrary-key"],
    )
    async def test_compute_rejects_unknown_override_keys(
        self,
        service: CryptoLendingRiskService,
        overrides: dict,
    ) -> None:
        with pytest.raises(ValueError, match="unknown override"):
            await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides=overrides)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_gap_pct",
        [Decimal("-0.01"), Decimal("-1"), Decimal("1.01"), Decimal("2")],
        ids=["slightly-negative", "very-negative", "slightly-over-1", "way-over-1"],
    )
    async def test_compute_rejects_gap_pct_out_of_range(
        self,
        service: CryptoLendingRiskService,
        bad_gap_pct: Decimal,
    ) -> None:
        with pytest.raises(ValueError, match="gap_pct"):
            await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={"gap_pct": bad_gap_pct})

    @pytest.mark.asyncio
    async def test_compute_missing_receipt_token_share_propagates(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        reader.get_share.side_effect = MissingShareError("share missing")

        with pytest.raises(MissingShareError, match="share missing"):
            await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

    @pytest.mark.asyncio
    async def test_compute_items_without_prices_are_skipped(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        reader.get_breakdown.return_value = _breakdown(
            (_contrib(10, "WETH", "10000", None),),
            backed_asset_id=UNDERLYING_TOKEN_ID,
        )
        reader.get_liquidation_params.return_value = {10: _params(10, "0.825", "1.05")}

        result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

        assert result.rrc_usd == Decimal("0")


class TestLegacyMethods:
    @pytest.mark.asyncio
    async def test_get_bad_debt_legacy_returns_none_for_unknown_receipt_token(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        reader.get_receipt_token.return_value = None

        assert await service.get_bad_debt_legacy(RECEIPT_TOKEN_ID, Decimal("0.15")) is None

    @pytest.mark.asyncio
    async def test_get_bad_debt_legacy_uses_legacy_share(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        info = _aave_like_info()

        result = await service.get_bad_debt_legacy(RECEIPT_TOKEN_ID, Decimal("0.15"))

        assert isinstance(result, Decimal)
        assert result >= Decimal("0")
        reader.get_breakdown.assert_awaited_once_with(info)
        reader.get_legacy_share.assert_awaited_once_with(info)
        reader.get_share.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_bad_debt_legacy_propagates_missing_share_error(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        reader.get_legacy_share.side_effect = MissingShareError("no active allocation position")

        with pytest.raises(MissingShareError, match="no active allocation position"):
            await service.get_bad_debt_legacy(RECEIPT_TOKEN_ID, Decimal("0.15"))

    @pytest.mark.asyncio
    async def test_get_bad_debt_legacy_validates_share_before_empty_breakdown(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        reader.get_breakdown.return_value = _breakdown((), backed_asset_id=UNDERLYING_TOKEN_ID)
        reader.get_legacy_share.side_effect = MissingShareError("share missing")

        with pytest.raises(MissingShareError, match="share missing"):
            await service.get_bad_debt_legacy(RECEIPT_TOKEN_ID, Decimal("0.15"))

        reader.get_breakdown.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_risk_breakdown_legacy_returns_enriched_items(
        self,
        service: CryptoLendingRiskService,
    ) -> None:
        result = await service.get_risk_breakdown_legacy(RECEIPT_TOKEN_ID)

        assert isinstance(result, RiskBreakdown)
        assert result.backed_asset_id == UNDERLYING_TOKEN_ID
        assert len(result.items) == 1
        assert result.items[0].amount_usd == Decimal("10000")
        assert result.items[0].amount == Decimal("5")

    @pytest.mark.asyncio
    async def test_get_risk_breakdown_legacy_returns_none_for_unknown_receipt_token(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        reader.get_receipt_token.return_value = None

        assert await service.get_risk_breakdown_legacy(RECEIPT_TOKEN_ID) is None
