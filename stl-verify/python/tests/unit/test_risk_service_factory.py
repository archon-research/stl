from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.adapters.onchain.allocation_share_client import FixedAllocationShare
from app.adapters.postgres.allocation_share_repository import MissingShareError, PostgresAllocationShare
from app.services.risk_service_factory import RiskServiceFactory


def _make_receipt_token_info(protocol_name: str) -> MagicMock:
    info = MagicMock()
    info.protocol_name = protocol_name
    info.protocol_id = 1
    info.underlying_token_id = 10
    info.receipt_token_address = bytes.fromhex("e7df13b8e3d6740fe17cbe928c7334243d86c92f")
    info.chain_id = 1
    info.receipt_token_token_id = 777
    return info


@pytest.mark.asyncio
async def test_aave_like_creates_postgres_allocation_share() -> None:
    """Aave-like protocols get a PostgresAllocationShare, not an onchain client."""
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
            allocation_share_max_stale_seconds=600,
            default_gap_pct=default_gap_pct,
        )
        factory._lookup_wallet = AsyncMock(return_value=bytes.fromhex("1601843c5e9bc251a3272907010afa41fa18347e"))

        await factory.create(receipt_token_id=99)

    _, kwargs = mock_svc_cls.call_args
    share_port = kwargs["share_port"]
    assert isinstance(share_port, PostgresAllocationShare)
    assert share_port._token_id == 777
    assert share_port._max_stale_seconds == 600
    assert kwargs["default_gap_pct"] == default_gap_pct


@pytest.mark.asyncio
async def test_aave_like_missing_wallet_raises_missing_share_error() -> None:
    """Wallet-not-found during factory.create surfaces as MissingShareError (→ HTTP 503)."""
    engine = MagicMock()

    with (
        patch("app.services.risk_service_factory.ReceiptTokenRepository") as mock_rt_repo_cls,
        patch("app.services.risk_service_factory.AaveLikeBackedBreakdownRepository"),
        patch("app.services.risk_service_factory.AaveLikeLiquidationParamsRepository"),
    ):
        mock_rt_repo = AsyncMock()
        mock_rt_repo.get.return_value = _make_receipt_token_info("SparkLend")
        mock_rt_repo_cls.return_value = mock_rt_repo

        factory = RiskServiceFactory(engine)
        factory._lookup_wallet = AsyncMock(side_effect=ValueError("no active allocation position"))

        with pytest.raises(MissingShareError):
            await factory.create(receipt_token_id=99)


@pytest.mark.asyncio
async def test_aave_like_missing_receipt_token_token_id_raises_missing_share_error() -> None:
    """Warm-up: receipt_token exists but its address `token` row hasn't been created yet."""
    engine = MagicMock()

    with (
        patch("app.services.risk_service_factory.ReceiptTokenRepository") as mock_rt_repo_cls,
        patch("app.services.risk_service_factory.AaveLikeBackedBreakdownRepository"),
        patch("app.services.risk_service_factory.AaveLikeLiquidationParamsRepository"),
    ):
        info = _make_receipt_token_info("SparkLend")
        info.receipt_token_token_id = None

        mock_rt_repo = AsyncMock()
        mock_rt_repo.get.return_value = info
        mock_rt_repo_cls.return_value = mock_rt_repo

        factory = RiskServiceFactory(engine)
        with pytest.raises(MissingShareError):
            await factory.create(receipt_token_id=99)


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

        factory = RiskServiceFactory(engine, default_gap_pct=default_gap_pct)
        await factory.create(receipt_token_id=99)

    _, kwargs = mock_svc_cls.call_args
    share_port = kwargs["share_port"]
    assert isinstance(share_port, FixedAllocationShare)
    assert await share_port.get_share() == Decimal("1")
    assert kwargs["default_gap_pct"] == default_gap_pct


@pytest.mark.asyncio
async def test_morpho_resolves_without_receipt_token_token_id() -> None:
    """Morpho doesn't use receipt_token_token_id, so its absence must not block resolution."""
    engine = MagicMock()

    with (
        patch("app.services.risk_service_factory.ReceiptTokenRepository") as mock_rt_repo_cls,
        patch("app.services.risk_service_factory.MorphoBackedBreakdownRepository") as mock_morpho_cls,
        patch("app.services.risk_service_factory.MorphoLiquidationParamsRepository"),
        patch("app.services.risk_service_factory.RiskCalculationService"),
    ):
        info = _make_receipt_token_info("morpho_blue")
        info.receipt_token_token_id = None

        mock_rt_repo = AsyncMock()
        mock_rt_repo.get.return_value = info
        mock_rt_repo_cls.return_value = mock_rt_repo

        mock_morpho_repo = AsyncMock()
        mock_morpho_repo.resolve_vault_id = AsyncMock(return_value=55)
        mock_morpho_cls.return_value = mock_morpho_repo

        factory = RiskServiceFactory(engine)
        result = await factory.create(receipt_token_id=99)

    assert result is not None
    _, asset_id = result
    assert asset_id == 55


@pytest.mark.asyncio
async def test_lookup_wallet_raises_when_no_position_found() -> None:
    """_lookup_wallet raises ValueError when no active allocation position exists."""
    engine = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=MagicMock(fetchone=MagicMock(return_value=None)))
    engine.connect.return_value = mock_conn

    factory = RiskServiceFactory(engine)
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

        factory = RiskServiceFactory(engine)
        with pytest.raises(ValueError, match="unsupported protocol"):
            await factory.create(receipt_token_id=99)
