from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.adapters.postgres.crypto_lending_reader import PostgresCryptoLendingReader, _normalize_protocol_name
from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.domain.entities.receipt_token import ReceiptTokenInfo, ReceiptTokenProtocolPair
from app.domain.entities.risk import LiquidationParams
from app.domain.exceptions import MissingShareError

DUMMY_PRIME = EthAddress("0x" + "ab" * 20)


def _aave_like_info(protocol_name: str = "Aave V3") -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=99,
        protocol_id=1,
        underlying_token_id=42,
        receipt_token_address=bytes.fromhex("e7df13b8e3d6740fe17cbe928c7334243d86c92f"),
        chain_id=1,
        protocol_name=protocol_name,
        receipt_token_token_id=777,
    )


def _morpho_info() -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=99,
        protocol_id=2,
        underlying_token_id=42,
        receipt_token_address=bytes.fromhex("a7df13b8e3d6740fe17cbe928c7334243d86c92f"),
        chain_id=1,
        protocol_name="Morpho Blue",
        receipt_token_token_id=None,
    )


def _maple_info() -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=99,
        protocol_id=3,
        underlying_token_id=42,
        receipt_token_address=bytes.fromhex("80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
        chain_id=1,
        protocol_name="maple",
        receipt_token_token_id=None,
    )


@pytest.fixture
def engine() -> MagicMock:
    return MagicMock()


@pytest.fixture
def receipt_token_repo() -> MagicMock:
    repo = MagicMock()
    repo.get = AsyncMock(return_value=_aave_like_info())
    return repo


@pytest.fixture
def aave_breakdown_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_backed_breakdown = AsyncMock(return_value=BackedBreakdown(backed_asset_id=42, items=()))
    return repo


@pytest.fixture
def morpho_breakdown_repo() -> MagicMock:
    repo = MagicMock()
    repo.resolve_vault_id = AsyncMock(return_value=55)
    repo.get_backed_breakdown = AsyncMock(return_value=BackedBreakdown(backed_asset_id=55, items=()))
    return repo


@pytest.fixture
def maple_breakdown_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_backed_breakdown = AsyncMock(
        return_value=BackedBreakdown(
            backed_asset_id=7,
            items=(
                CollateralContribution(
                    token_id=None,
                    symbol="BTC",
                    backing_value=Decimal("130000"),
                    backing_pct=Decimal("67"),
                    price_usd=Decimal("65000"),
                ),
            ),
        )
    )
    return repo


@pytest.fixture
def aave_liq_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_params = AsyncMock(return_value={1: LiquidationParams(1, Decimal("0.8"), Decimal("1.05"))})
    return repo


@pytest.fixture
def morpho_liq_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_params = AsyncMock(return_value={1: LiquidationParams(1, Decimal("0.8"), Decimal("1.05"))})
    return repo


@pytest.fixture
def reader(
    engine: MagicMock,
    receipt_token_repo: MagicMock,
    aave_breakdown_repo: MagicMock,
    morpho_breakdown_repo: MagicMock,
    maple_breakdown_repo: MagicMock,
    aave_liq_repo: MagicMock,
    morpho_liq_repo: MagicMock,
) -> PostgresCryptoLendingReader:
    return PostgresCryptoLendingReader(
        receipt_token_repo=receipt_token_repo,
        aave_breakdown_repo=aave_breakdown_repo,
        morpho_breakdown_repo=morpho_breakdown_repo,
        maple_breakdown_repo=maple_breakdown_repo,
        aave_liq_repo=aave_liq_repo,
        morpho_liq_repo=morpho_liq_repo,
        engine=engine,
        allocation_share_max_stale_seconds=600,
    )


@pytest.mark.asyncio
async def test_list_supported_asset_ids_filters_supported_protocols(
    reader: PostgresCryptoLendingReader,
    receipt_token_repo: MagicMock,
) -> None:
    receipt_token_repo.list_protocol_pairs = AsyncMock(
        return_value=[
            ReceiptTokenProtocolPair(receipt_token_id=1, protocol_name="Aave V3"),
            ReceiptTokenProtocolPair(receipt_token_id=2, protocol_name="morpho-blue"),
            ReceiptTokenProtocolPair(receipt_token_id=3, protocol_name="Unsupported"),
        ]
    )

    assert await reader.list_supported_asset_ids() == {1, 2}
    receipt_token_repo.list_protocol_pairs.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_get_receipt_token_delegates_to_repo(
    reader: PostgresCryptoLendingReader,
    receipt_token_repo: MagicMock,
) -> None:
    result = await reader.get_receipt_token(99)

    receipt_token_repo.get.assert_awaited_once_with(99)
    assert result == _aave_like_info()


@pytest.mark.asyncio
async def test_get_breakdown_uses_aave_like_repository(
    reader: PostgresCryptoLendingReader,
    aave_breakdown_repo: MagicMock,
) -> None:
    info = _aave_like_info()

    result = await reader.get_breakdown(info)

    aave_breakdown_repo.get_backed_breakdown.assert_awaited_once_with(info.protocol_id, info.underlying_token_id)
    assert result.backed_asset_id == info.underlying_token_id


@pytest.mark.asyncio
async def test_get_breakdown_uses_morpho_repository(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
) -> None:
    info = _morpho_info()

    result = await reader.get_breakdown(info)

    morpho_breakdown_repo.resolve_vault_id.assert_awaited_once_with(info.receipt_token_address, info.chain_id)
    morpho_breakdown_repo.get_backed_breakdown.assert_awaited_once_with(55)
    assert result.backed_asset_id == 55


@pytest.mark.asyncio
async def test_get_breakdown_raises_when_morpho_vault_missing(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
) -> None:
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault_id.return_value = None

    with pytest.raises(ValueError, match="morpho vault not found"):
        await reader.get_breakdown(info)


@pytest.mark.asyncio
async def test_get_breakdown_raises_on_unknown_protocol(reader: PostgresCryptoLendingReader) -> None:
    with pytest.raises(ValueError, match="unsupported protocol"):
        await reader.get_breakdown(_aave_like_info(protocol_name="unknown_protocol_xyz"))


@pytest.mark.asyncio
async def test_get_liquidation_params_uses_aave_like_repository(
    reader: PostgresCryptoLendingReader,
    aave_liq_repo: MagicMock,
) -> None:
    info = _aave_like_info()

    result = await reader.get_liquidation_params(info, backed_asset_id=42, token_ids=[1, 2])

    aave_liq_repo.get_params.assert_awaited_once_with(info.protocol_id, [1, 2])
    assert result == aave_liq_repo.get_params.return_value


@pytest.mark.asyncio
async def test_get_liquidation_params_uses_morpho_repository(
    reader: PostgresCryptoLendingReader,
    morpho_liq_repo: MagicMock,
) -> None:
    info = _morpho_info()

    result = await reader.get_liquidation_params(info, backed_asset_id=55, token_ids=[1, 2])

    morpho_liq_repo.get_params.assert_awaited_once_with(55, [1, 2])
    assert result == morpho_liq_repo.get_params.return_value


@pytest.mark.asyncio
async def test_get_share_uses_prime_wallet_for_supply_share(
    reader: PostgresCryptoLendingReader,
    engine: MagicMock,
) -> None:
    info = _aave_like_info()

    with patch("app.adapters.postgres.crypto_lending_reader.PostgresAllocationShare") as mock_share_cls:
        mock_share = AsyncMock()
        mock_share.get_share.return_value = Decimal("0.25")
        mock_share_cls.return_value = mock_share

        result = await reader.get_share(info, DUMMY_PRIME)

    mock_share_cls.assert_called_once_with(
        engine=engine,
        chain_id=1,
        token_id=777,
        wallet_address=bytes.fromhex(DUMMY_PRIME.hex),
        max_stale_seconds=600,
    )
    mock_share.get_share.assert_awaited_once_with()
    assert result == Decimal("0.25")


@pytest.mark.asyncio
async def test_get_share_uses_prime_wallet_for_morpho_supply_share(
    reader: PostgresCryptoLendingReader,
    engine: MagicMock,
) -> None:
    info = replace(_morpho_info(), receipt_token_token_id=888)

    with patch("app.adapters.postgres.crypto_lending_reader.PostgresAllocationShare") as mock_share_cls:
        mock_share = AsyncMock()
        mock_share.get_share.return_value = Decimal("0.4")
        mock_share_cls.return_value = mock_share

        result = await reader.get_share(info, DUMMY_PRIME)

    mock_share_cls.assert_called_once_with(
        engine=engine,
        chain_id=1,
        token_id=888,
        wallet_address=bytes.fromhex(DUMMY_PRIME.hex),
        max_stale_seconds=600,
    )
    mock_share.get_share.assert_awaited_once_with()
    assert result == Decimal("0.4")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "info",
    [
        replace(_aave_like_info(), receipt_token_token_id=None),
        _morpho_info(),
    ],
    ids=["aave-like-missing-token-id", "morpho-missing-token-id"],
)
async def test_get_share_raises_when_receipt_token_token_id_missing(
    reader: PostgresCryptoLendingReader,
    info: ReceiptTokenInfo,
) -> None:
    with pytest.raises(MissingShareError, match="not indexed yet"):
        await reader.get_share(info, DUMMY_PRIME)


@pytest.mark.asyncio
async def test_get_legacy_share_returns_one_for_morpho(reader: PostgresCryptoLendingReader) -> None:
    with (
        patch.object(reader, "_lookup_wallet", AsyncMock()) as mock_lookup,
        patch("app.adapters.postgres.crypto_lending_reader.PostgresAllocationShare") as mock_share_cls,
    ):
        result = await reader.get_legacy_share(_morpho_info())

    mock_lookup.assert_not_awaited()
    mock_share_cls.assert_not_called()
    assert result == Decimal("1")


@pytest.mark.asyncio
async def test_get_legacy_share_looks_up_wallet_then_loads_share(
    reader: PostgresCryptoLendingReader,
    engine: MagicMock,
) -> None:
    info = _aave_like_info()

    with (
        patch.object(reader, "_lookup_wallet", AsyncMock(return_value=bytes(20))) as mock_lookup,
        patch("app.adapters.postgres.crypto_lending_reader.PostgresAllocationShare") as mock_share_cls,
    ):
        mock_share = AsyncMock()
        mock_share.get_share.return_value = Decimal("0.5")
        mock_share_cls.return_value = mock_share

        result = await reader.get_legacy_share(info)

    mock_lookup.assert_awaited_once_with(info.receipt_token_address, info.chain_id)
    mock_share_cls.assert_called_once_with(
        engine=engine,
        chain_id=1,
        token_id=777,
        wallet_address=bytes(20),
        max_stale_seconds=600,
    )
    mock_share.get_share.assert_awaited_once_with()
    assert result == Decimal("0.5")


def test_normalize_maple() -> None:
    assert _normalize_protocol_name("maple") == "maple"


def test_requires_liquidation_enrichment_false_for_maple(reader: PostgresCryptoLendingReader) -> None:
    assert reader.requires_liquidation_enrichment(_maple_info()) is False


def test_requires_liquidation_enrichment_true_for_aave(reader: PostgresCryptoLendingReader) -> None:
    assert reader.requires_liquidation_enrichment(_aave_like_info()) is True


@pytest.mark.asyncio
async def test_get_breakdown_uses_maple_repository(
    reader: PostgresCryptoLendingReader,
    maple_breakdown_repo: MagicMock,
) -> None:
    info = _maple_info()

    result = await reader.get_breakdown(info)

    maple_breakdown_repo.get_backed_breakdown.assert_awaited_once_with(info.receipt_token_address, info.chain_id)
    assert result.items[0].symbol == "BTC"
    assert result.items[0].token_id is None


@pytest.mark.asyncio
async def test_get_liquidation_params_empty_for_maple(reader: PostgresCryptoLendingReader) -> None:
    assert await reader.get_liquidation_params(_maple_info(), 7, []) == {}


@pytest.mark.asyncio
async def test_list_supported_asset_ids_excludes_maple(
    reader: PostgresCryptoLendingReader,
    receipt_token_repo: MagicMock,
) -> None:
    receipt_token_repo.list_protocol_pairs = AsyncMock(
        return_value=[
            ReceiptTokenProtocolPair(receipt_token_id=1, protocol_name="Aave V3"),
            ReceiptTokenProtocolPair(receipt_token_id=2, protocol_name="maple"),
        ]
    )

    # Maple has no RRC model yet, so it must not be RRC-eligible.
    assert await reader.list_supported_asset_ids() == {1}
