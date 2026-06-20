from decimal import ROUND_HALF_EVEN, Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import GapSweepDetails, LiquidationParams, RiskBreakdown, RrcResult
from app.domain.exceptions import MissingShareError
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
    reader.is_maple = MagicMock(return_value=False)
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
        assert service.risk_model == "gap_sweep"


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
        assert result.risk_model == "gap_sweep"
        assert result.rrc_usd >= Decimal("0")
        # Comparable CRR uses the protocol's own collateral basis: the sum of
        # backing_value * share across breakdown items (default fixture: a
        # single item with backing_value=10000, share=1 -> 10000 USD).
        assert result.comparable_crr_pct == (result.rrc_usd / Decimal("10000") * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN
        )
        assert isinstance(result.details, GapSweepDetails)
        assert result.details.gap_pct == Decimal("0.15")
        assert result.details.loss_usd == result.rrc_usd
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

        assert isinstance(override_result.details, GapSweepDetails)
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
    async def test_compute_morpho_prime_share_scales_rrc(
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
        reader.get_share.return_value = Decimal("1")
        full_share_result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

        reader.get_share.reset_mock()
        reader.get_share.return_value = Decimal("0.25")
        quarter_share_result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

        reader.get_share.assert_awaited_once_with(info, DUMMY_PRIME)
        assert quarter_share_result.rrc_usd < full_share_result.rrc_usd

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

        # Item dropped (no price) -> empty enriched list -> rrc=0 and the
        # collateral basis is also 0, so comparable CRR is 0 (not div-by-zero).
        assert result.rrc_usd == Decimal("0")
        assert result.comparable_crr_pct == Decimal("0.00")

    @pytest.mark.asyncio
    async def test_compute_comparable_crr_uses_collateral_sum(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        # Two items totalling 7000 USD; quantization should hit fractional cents.
        reader.get_breakdown.return_value = _breakdown(
            (
                _contrib(10, "USDC", "5000", "1"),
                _contrib(11, "DAI", "2000", "1"),
            ),
            backed_asset_id=UNDERLYING_TOKEN_ID,
        )
        reader.get_liquidation_params.return_value = {
            10: _params(10, "0.825", "1.05"),
            11: _params(11, "0.825", "1.05"),
        }

        result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

        expected_basis = Decimal("7000")
        assert result.comparable_crr_pct == (result.rrc_usd / expected_basis * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN
        )

    @pytest.mark.asyncio
    async def test_compute_empty_breakdown_returns_zero_crr_without_dividing(
        self,
        service: CryptoLendingRiskService,
        reader: MagicMock,
    ) -> None:
        # No collateral items -> total amount_usd = 0; must NOT divide by zero,
        # must NOT raise. Both rrc_usd and comparable_crr_pct collapse to 0.
        reader.get_breakdown.return_value = _breakdown((), backed_asset_id=UNDERLYING_TOKEN_ID)
        reader.get_liquidation_params.return_value = {}

        result = await service.compute(RECEIPT_TOKEN_ID, DUMMY_PRIME, overrides={})

        assert result.rrc_usd == Decimal("0")
        assert result.comparable_crr_pct == Decimal("0.00")


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


def _maple_info() -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=RECEIPT_TOKEN_ID,
        protocol_id=3,
        underlying_token_id=UNDERLYING_TOKEN_ID,
        receipt_token_address=bytes.fromhex("80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
        chain_id=1,
        protocol_name="maple",
        receipt_token_token_id=None,
    )


class TestMaplePath:
    @pytest.fixture
    def maple_reader(self, reader: MagicMock) -> MagicMock:
        reader.is_maple = MagicMock(return_value=True)
        reader.get_receipt_token = AsyncMock(return_value=_maple_info())
        reader.get_breakdown = AsyncMock(
            return_value=_breakdown(
                (
                    CollateralContribution(
                        token_id=None,
                        symbol="BTC",
                        backing_value=Decimal("130000"),
                        backing_pct=Decimal("67"),
                        price_usd=Decimal("65000"),
                    ),
                    CollateralContribution(
                        token_id=None,
                        symbol="USDC",
                        backing_value=Decimal("1000000"),
                        backing_pct=Decimal("33"),
                        price_usd=Decimal("1"),
                    ),
                ),
                backed_asset_id=7,
            )
        )
        return reader

    @pytest.fixture
    def maple_service(self, maple_reader: MagicMock) -> CryptoLendingRiskService:
        return CryptoLendingRiskService(
            reader=maple_reader,
            default_gap_pct=Decimal("0.1"),
            supported_asset_ids={RECEIPT_TOKEN_ID},
        )

    @pytest.mark.asyncio
    async def test_breakdown_enriches_without_liq_params_or_share(
        self,
        maple_service: CryptoLendingRiskService,
        maple_reader: MagicMock,
    ) -> None:
        result = await maple_service.get_risk_breakdown_legacy(RECEIPT_TOKEN_ID)

        assert isinstance(result, RiskBreakdown)
        assert result.backed_asset_id == 7
        by_symbol = {i.symbol: i for i in result.items}
        assert by_symbol["BTC"].token_id is None
        assert by_symbol["BTC"].liquidation_threshold is None
        assert by_symbol["BTC"].liquidation_bonus is None
        assert by_symbol["BTC"].amount_usd == Decimal("130000")
        assert by_symbol["BTC"].amount == Decimal("2")  # 130000 / 65000
        # Maple skips both prime-share scaling and liquidation-param enrichment.
        maple_reader.get_share.assert_not_awaited()
        maple_reader.get_legacy_share.assert_not_awaited()
        maple_reader.get_liquidation_params.assert_not_awaited()

    # Both a None price and a literal zero price hit distinct ternary predicates
    # in _build_maple_items (``if price`` vs ``price is not None``); cover both.
    @pytest.mark.parametrize("price", [None, Decimal("0")])
    @pytest.mark.asyncio
    async def test_zero_or_missing_price_yields_zero_amount(
        self,
        maple_service: CryptoLendingRiskService,
        maple_reader: MagicMock,
        price: Decimal | None,
    ) -> None:
        maple_reader.get_breakdown = AsyncMock(
            return_value=_breakdown(
                (
                    CollateralContribution(
                        token_id=None,
                        symbol="MYSTERY",
                        backing_value=Decimal("500"),
                        backing_pct=Decimal("100"),
                        price_usd=price,
                    ),
                ),
                backed_asset_id=7,
            )
        )

        result = await maple_service.get_risk_breakdown_legacy(RECEIPT_TOKEN_ID)

        assert result is not None
        assert result.items[0].amount == Decimal("0")
        # Missing/zero price surfaces as null (machine-detectable "unpriced"),
        # not a misleading 0 that would imply amount × price == amount_usd.
        assert result.items[0].price_usd is None
        assert result.items[0].amount_usd == Decimal("500")
