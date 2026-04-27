from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.adapters.onchain.allocation_share_client import FixedAllocationShare, OnchainAllocationShareClient
from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import ResolvedRiskPosition
from app.services.risk_service_factory import RiskServiceFactory


def _make_receipt_token_info(protocol_name: str) -> MagicMock:
    info = MagicMock()
    info.protocol_name = protocol_name
    info.protocol_id = 1
    info.underlying_token_id = 10
    info.receipt_token_address = bytes.fromhex("e7df13b8e3d6740fe17cbe928c7334243d86c92f")
    info.chain_id = 1
    return info


def _make_position(protocol_name: str) -> ResolvedRiskPosition:
    return ResolvedRiskPosition(
        chain_id=1,
        prime_id=EthAddress("0x1601843c5e9bc251a3272907010afa41fa18347e"),
        receipt_token_id=99,
        receipt_token_address="0xe7df13b8e3d6740fe17cbe928c7334243d86c92f",
        underlying_token_id=10,
        underlying_token_address="0x" + "a" * 40,
        protocol_id=1,
        symbol="aUSDC",
        underlying_symbol="USDC",
        protocol_name=protocol_name,
        balance=Decimal("100"),
        usd_exposure=Decimal("100"),
    )


@pytest.mark.asyncio
async def test_aave_like_creates_onchain_share_client() -> None:
    """Aave-like protocols get an OnchainAllocationShareClient injected."""
    engine = MagicMock()
    default_gap_pct = Decimal("0.17")

    with (
        patch("app.services.risk_service_factory.ReceiptTokenRepository") as mock_rt_repo_cls,
        patch("app.services.risk_service_factory.AaveLikeBackedBreakdownRepository"),
        patch("app.services.risk_service_factory.AaveLikeLiquidationParamsRepository"),
        patch("app.services.risk_service_factory.RiskCalculationService") as mock_svc_cls,
    ):
        mock_rt_repo = AsyncMock()
        mock_rt_repo.get.return_value = _make_receipt_token_info("SparkLend")
        mock_rt_repo_cls.return_value = mock_rt_repo

        factory = RiskServiceFactory(
            engine,
            alchemy_url="https://fake-alchemy-url",
            http_client=MagicMock(),
            default_gap_pct=default_gap_pct,
        )
        # Patch the wallet lookup to avoid needing a real DB
        factory._lookup_wallet = AsyncMock(return_value=bytes.fromhex("1601843c5e9bc251a3272907010afa41fa18347e"))

        await factory.create(receipt_token_id=99)

    _, kwargs = mock_svc_cls.call_args
    assert isinstance(kwargs["share_port"], OnchainAllocationShareClient)
    assert kwargs["default_gap_pct"] == default_gap_pct


@pytest.mark.asyncio
async def test_morpho_creates_fixed_share_of_one() -> None:
    """Morpho protocols get FixedAllocationShare(1) because the breakdown is already vault-scoped."""
    engine = MagicMock()
    default_gap_pct = Decimal("0.23")

    with (
        patch("app.services.risk_service_factory.ReceiptTokenRepository") as mock_rt_repo_cls,
        patch("app.services.risk_service_factory.MorphoBackedBreakdownRepository") as mock_morpho_cls,
        patch("app.services.risk_service_factory.MorphoLiquidationParamsRepository"),
        patch("app.services.risk_service_factory.RiskCalculationService") as mock_svc_cls,
    ):
        mock_rt_repo = AsyncMock()
        mock_rt_repo.get.return_value = _make_receipt_token_info("morpho_blue")
        mock_rt_repo_cls.return_value = mock_rt_repo

        mock_morpho_repo = AsyncMock()
        mock_morpho_repo.resolve_vault_id = AsyncMock(return_value=55)
        mock_morpho_cls.return_value = mock_morpho_repo

        factory = RiskServiceFactory(
            engine,
            alchemy_url="https://fake-alchemy-url",
            http_client=MagicMock(),
            default_gap_pct=default_gap_pct,
        )
        await factory.create(receipt_token_id=99)

    _, kwargs = mock_svc_cls.call_args
    share_port = kwargs["share_port"]
    assert isinstance(share_port, FixedAllocationShare)
    assert await share_port.get_share() == Decimal("1")
    assert kwargs["default_gap_pct"] == default_gap_pct


@pytest.mark.asyncio
async def test_lookup_wallet_raises_when_no_position_found() -> None:
    """_lookup_wallet raises ValueError when no active allocation position exists."""
    engine = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=MagicMock(fetchone=MagicMock(return_value=None)))
    engine.connect.return_value = mock_conn

    factory = RiskServiceFactory(
        engine,
        alchemy_url="https://fake-alchemy-url",
        http_client=MagicMock(),
        default_gap_pct=Decimal("0.15"),
    )
    with pytest.raises(ValueError, match="no active allocation position"):
        await factory._lookup_wallet(bytes(20), 1)


@pytest.mark.asyncio
async def test_unknown_protocol_raises_value_error() -> None:
    """Unknown protocols raise ValueError immediately."""
    engine = MagicMock()

    with (
        patch("app.services.risk_service_factory.ReceiptTokenRepository") as mock_rt_repo_cls,
    ):
        mock_rt_repo = AsyncMock()
        mock_rt_repo.get.return_value = _make_receipt_token_info("unknown_protocol_xyz")
        mock_rt_repo_cls.return_value = mock_rt_repo

        factory = RiskServiceFactory(
            engine,
            alchemy_url="https://fake-alchemy-url",
            http_client=MagicMock(),
            default_gap_pct=Decimal("0.15"),
        )
        with pytest.raises(ValueError, match="unsupported protocol"):
            await factory.create(receipt_token_id=99)


@pytest.mark.asyncio
async def test_create_for_position_uses_supplied_prime_id_as_wallet_for_aave_like() -> None:
    engine = MagicMock()

    with (
        patch("app.services.risk_service_factory.AaveLikeBackedBreakdownRepository"),
        patch("app.services.risk_service_factory.AaveLikeLiquidationParamsRepository"),
        patch("app.services.risk_service_factory.RiskCalculationService") as mock_svc_cls,
    ):
        factory = RiskServiceFactory(
            engine,
            alchemy_url="https://fake-alchemy-url",
            http_client=MagicMock(),
            default_gap_pct=Decimal("0.15"),
        )
        position = _make_position("Aave V3")

        await factory.create_for_position(position)

    _, kwargs = mock_svc_cls.call_args
    share_port = kwargs["share_port"]
    assert isinstance(share_port, OnchainAllocationShareClient)
    assert share_port._wallet_addr == str(position.prime_id)


@pytest.mark.asyncio
async def test_create_for_position_keeps_morpho_fixed_share_of_one() -> None:
    engine = MagicMock()

    with (
        patch("app.services.risk_service_factory.MorphoBackedBreakdownRepository") as mock_morpho_cls,
        patch("app.services.risk_service_factory.MorphoLiquidationParamsRepository"),
        patch("app.services.risk_service_factory.RiskCalculationService") as mock_svc_cls,
    ):
        mock_morpho_repo = AsyncMock()
        mock_morpho_repo.resolve_vault_id = AsyncMock(return_value=55)
        mock_morpho_cls.return_value = mock_morpho_repo

        factory = RiskServiceFactory(
            engine,
            alchemy_url="https://fake-alchemy-url",
            http_client=MagicMock(),
            default_gap_pct=Decimal("0.15"),
        )
        await factory.create_for_position(_make_position("Morpho Blue"))

    _, kwargs = mock_svc_cls.call_args
    share_port = kwargs["share_port"]
    assert isinstance(share_port, FixedAllocationShare)
    assert await share_port.get_share() == Decimal("1")
