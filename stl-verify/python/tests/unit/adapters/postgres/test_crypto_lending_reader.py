from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.adapters.postgres.backed_breakdown_repository_morpho import MorphoVaultRef
from app.adapters.postgres.crypto_lending_reader import PostgresCryptoLendingReader, _normalize_protocol_name
from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.domain.entities.receipt_token import ReceiptTokenInfo, ReceiptTokenProtocolPair
from app.domain.entities.risk import LiquidationParams
from app.domain.exceptions import AdapterDataMissingError, MissingShareError

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
    repo.get_backed_breakdowns = AsyncMock(return_value={})
    return repo


@pytest.fixture
def morpho_breakdown_repo() -> MagicMock:
    repo = MagicMock()
    # Default: a MetaMorpho V1 vault (vault_version=1) resolving to internal id 55.
    repo.resolve_vault = AsyncMock(return_value=MorphoVaultRef(id=55, vault_version=1))
    repo.get_backed_breakdown = AsyncMock(return_value=BackedBreakdown(backed_asset_id=55, items=()))
    return repo


@pytest.fixture
def morpho_v2_breakdown_repo() -> MagicMock:
    repo = MagicMock()
    # Distinct backed_asset_id (77) so a test can tell the V2 repo apart from V1 (55).
    repo.get_backed_breakdown = AsyncMock(return_value=BackedBreakdown(backed_asset_id=77, items=()))
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
def morpho_v2_liq_repo() -> MagicMock:
    repo = MagicMock()
    # Distinct params dict so a test can tell the V2 liq repo apart from V1.
    repo.get_params = AsyncMock(return_value={2: LiquidationParams(2, Decimal("0.9"), Decimal("1.02"))})
    repo.has_active_adapters = AsyncMock(return_value=True)
    return repo


@pytest.fixture
def reader(
    engine: MagicMock,
    receipt_token_repo: MagicMock,
    aave_breakdown_repo: MagicMock,
    morpho_breakdown_repo: MagicMock,
    morpho_v2_breakdown_repo: MagicMock,
    maple_breakdown_repo: MagicMock,
    aave_liq_repo: MagicMock,
    morpho_liq_repo: MagicMock,
    morpho_v2_liq_repo: MagicMock,
) -> PostgresCryptoLendingReader:
    return PostgresCryptoLendingReader(
        receipt_token_repo=receipt_token_repo,
        aave_breakdown_repo=aave_breakdown_repo,
        morpho_breakdown_repo=morpho_breakdown_repo,
        morpho_v2_breakdown_repo=morpho_v2_breakdown_repo,
        maple_breakdown_repo=maple_breakdown_repo,
        aave_liq_repo=aave_liq_repo,
        morpho_liq_repo=morpho_liq_repo,
        morpho_v2_liq_repo=morpho_v2_liq_repo,
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
    morpho_v2_breakdown_repo: MagicMock,
) -> None:
    info = _morpho_info()  # resolve_vault → MorphoVaultRef(id=55, vault_version=1)

    result = await reader.get_breakdown(info)

    morpho_breakdown_repo.resolve_vault.assert_awaited_once_with(info.receipt_token_address, info.chain_id)
    morpho_breakdown_repo.get_backed_breakdown.assert_awaited_once_with(55)
    morpho_v2_breakdown_repo.get_backed_breakdown.assert_not_awaited()
    assert result.backed_asset_id == 55


@pytest.mark.asyncio
async def test_get_breakdown_morpho_v1_1_uses_v1_repository(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
    morpho_v2_breakdown_repo: MagicMock,
) -> None:
    """vault_version=2 (MetaMorpho V1.1) shares the V1 direct-allocation walk."""
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = MorphoVaultRef(id=55, vault_version=2)

    result = await reader.get_breakdown(info)

    morpho_breakdown_repo.get_backed_breakdown.assert_awaited_once_with(55)
    morpho_v2_breakdown_repo.get_backed_breakdown.assert_not_awaited()
    assert result.backed_asset_id == 55


@pytest.mark.asyncio
async def test_get_breakdown_morpho_v2_uses_v2_repository(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
    morpho_v2_breakdown_repo: MagicMock,
) -> None:
    """vault_version=3 (Morpho VaultV2) routes to the adapter-based V2 repository."""
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = MorphoVaultRef(id=66, vault_version=3)

    result = await reader.get_breakdown(info)

    morpho_v2_breakdown_repo.get_backed_breakdown.assert_awaited_once_with(66)
    morpho_breakdown_repo.get_backed_breakdown.assert_not_awaited()
    assert result.backed_asset_id == 77


@pytest.mark.asyncio
async def test_get_breakdown_morpho_unexpected_version_raises(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
    morpho_v2_breakdown_repo: MagicMock,
) -> None:
    """An unknown vault_version must fail loudly (enum drift guard), never silently
    fall through to a wrong repository."""
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = MorphoVaultRef(id=55, vault_version=4)

    with pytest.raises(ValueError, match="vault_version"):
        await reader.get_breakdown(info)

    morpho_breakdown_repo.get_backed_breakdown.assert_not_awaited()
    morpho_v2_breakdown_repo.get_backed_breakdown.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_breakdown_raises_when_morpho_vault_missing(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
) -> None:
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = None

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
    morpho_v2_liq_repo: MagicMock,
) -> None:
    """vault_version=1 (MetaMorpho V1) uses the V1 liquidation-params repo."""
    info = _morpho_info()  # resolve_vault → MorphoVaultRef(id=55, vault_version=1)

    result = await reader.get_liquidation_params(info, backed_asset_id=55, token_ids=[1, 2])

    morpho_liq_repo.get_params.assert_awaited_once_with(55, [1, 2])
    morpho_v2_liq_repo.get_params.assert_not_awaited()
    assert result == morpho_liq_repo.get_params.return_value


@pytest.mark.asyncio
async def test_get_liquidation_params_morpho_v1_1_uses_v1_repository(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
    morpho_liq_repo: MagicMock,
    morpho_v2_liq_repo: MagicMock,
) -> None:
    """vault_version=2 (MetaMorpho V1.1) shares the V1 liquidation-params walk."""
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = MorphoVaultRef(id=55, vault_version=2)

    result = await reader.get_liquidation_params(info, backed_asset_id=55, token_ids=[1, 2])

    morpho_liq_repo.get_params.assert_awaited_once_with(55, [1, 2])
    morpho_v2_liq_repo.get_params.assert_not_awaited()
    assert result == morpho_liq_repo.get_params.return_value


@pytest.mark.asyncio
async def test_get_liquidation_params_morpho_v2_uses_v2_repository(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
    morpho_liq_repo: MagicMock,
    morpho_v2_liq_repo: MagicMock,
) -> None:
    """vault_version=3 routes to the adapter-graph V2 liq repo; non-empty params
    short-circuit the adapter probe."""
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = MorphoVaultRef(id=66, vault_version=3)

    result = await reader.get_liquidation_params(info, backed_asset_id=66, token_ids=[1, 2])

    morpho_v2_liq_repo.get_params.assert_awaited_once_with(66, [1, 2])
    morpho_v2_liq_repo.has_active_adapters.assert_not_awaited()
    morpho_liq_repo.get_params.assert_not_awaited()
    assert result == morpho_v2_liq_repo.get_params.return_value


@pytest.mark.asyncio
async def test_get_liquidation_params_morpho_v2_no_adapters_raises(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
    morpho_v2_liq_repo: MagicMock,
) -> None:
    """A v3 vault with empty params AND no active adapters means the composition is
    not indexed yet — it must degrade (AdapterDataMissingError), never silently drop
    every item and report a confident rrc=0."""
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = MorphoVaultRef(id=66, vault_version=3)
    morpho_v2_liq_repo.get_params.return_value = {}
    morpho_v2_liq_repo.has_active_adapters.return_value = False

    with pytest.raises(AdapterDataMissingError, match="adapter"):
        await reader.get_liquidation_params(info, backed_asset_id=66, token_ids=[1, 2])

    morpho_v2_liq_repo.has_active_adapters.assert_awaited_once_with(66)


@pytest.mark.asyncio
async def test_get_liquidation_params_morpho_v2_empty_params_with_adapters_is_not_degraded(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
    morpho_v2_liq_repo: MagicMock,
) -> None:
    """A v3 vault with active adapters but no collateral markets (genuinely idle)
    returns empty params — a real rrc=0, NOT a degradation."""
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = MorphoVaultRef(id=66, vault_version=3)
    morpho_v2_liq_repo.get_params.return_value = {}
    morpho_v2_liq_repo.has_active_adapters.return_value = True

    result = await reader.get_liquidation_params(info, backed_asset_id=66, token_ids=[1, 2])

    assert result == {}
    morpho_v2_liq_repo.has_active_adapters.assert_awaited_once_with(66)


@pytest.mark.asyncio
async def test_get_liquidation_params_morpho_unexpected_version_raises(
    reader: PostgresCryptoLendingReader,
    morpho_breakdown_repo: MagicMock,
) -> None:
    info = _morpho_info()
    morpho_breakdown_repo.resolve_vault.return_value = MorphoVaultRef(id=55, vault_version=4)

    with pytest.raises(ValueError, match="vault_version"):
        await reader.get_liquidation_params(info, backed_asset_id=55, token_ids=[1, 2])


@pytest.mark.asyncio
async def test_get_share_uses_prime_wallet_for_supply_share(
    reader: PostgresCryptoLendingReader,
    engine: MagicMock,
) -> None:
    info = _aave_like_info()

    with patch(
        "app.adapters.postgres.crypto_lending_reader.fetch_share",
        AsyncMock(return_value=Decimal("0.25")),
    ) as mock_fetch:
        result = await reader.get_share(info, DUMMY_PRIME)

    mock_fetch.assert_awaited_once_with(
        engine=engine,
        chain_id=1,
        token_id=777,
        wallet_address=bytes.fromhex(DUMMY_PRIME.hex),
        max_stale_seconds=600,
    )
    assert result == Decimal("0.25")


@pytest.mark.asyncio
async def test_get_share_uses_prime_wallet_for_morpho_supply_share(
    reader: PostgresCryptoLendingReader,
    engine: MagicMock,
) -> None:
    info = replace(_morpho_info(), receipt_token_token_id=888)

    with patch(
        "app.adapters.postgres.crypto_lending_reader.fetch_share",
        AsyncMock(return_value=Decimal("0.4")),
    ) as mock_fetch:
        result = await reader.get_share(info, DUMMY_PRIME)

    mock_fetch.assert_awaited_once_with(
        engine=engine,
        chain_id=1,
        token_id=888,
        wallet_address=bytes.fromhex(DUMMY_PRIME.hex),
        max_stale_seconds=600,
    )
    assert result == Decimal("0.4")


@pytest.mark.asyncio
async def test_get_share_uses_prime_wallet_for_maple_supply_share(
    reader: PostgresCryptoLendingReader,
    engine: MagicMock,
) -> None:
    info = replace(_maple_info(), receipt_token_token_id=555)

    with patch(
        "app.adapters.postgres.crypto_lending_reader.fetch_share",
        AsyncMock(return_value=Decimal("0.1")),
    ) as mock_fetch:
        result = await reader.get_share(info, DUMMY_PRIME)

    mock_fetch.assert_awaited_once_with(
        engine=engine,
        chain_id=1,
        token_id=555,
        wallet_address=bytes.fromhex(DUMMY_PRIME.hex),
        max_stale_seconds=600,
    )
    assert result == Decimal("0.1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "info",
    [
        replace(_aave_like_info(), receipt_token_token_id=None),
        _morpho_info(),
        _maple_info(),
    ],
    ids=["aave-like-missing-token-id", "morpho-missing-token-id", "maple-missing-token-id"],
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
        patch("app.adapters.postgres.crypto_lending_reader.fetch_share", AsyncMock()) as mock_fetch,
    ):
        result = await reader.get_legacy_share(_morpho_info())

    mock_lookup.assert_not_awaited()
    mock_fetch.assert_not_awaited()
    assert result == Decimal("1")


@pytest.mark.asyncio
async def test_get_legacy_share_looks_up_wallet_then_loads_share(
    reader: PostgresCryptoLendingReader,
    engine: MagicMock,
) -> None:
    info = _aave_like_info()

    with (
        patch.object(reader, "_lookup_wallet", AsyncMock(return_value=bytes(20))) as mock_lookup,
        patch(
            "app.adapters.postgres.crypto_lending_reader.fetch_share",
            AsyncMock(return_value=Decimal("0.5")),
        ) as mock_fetch,
    ):
        result = await reader.get_legacy_share(info)

    mock_lookup.assert_awaited_once_with(info.receipt_token_address, info.chain_id)
    mock_fetch.assert_awaited_once_with(
        engine=engine,
        chain_id=1,
        token_id=777,
        wallet_address=bytes(20),
        max_stale_seconds=600,
    )
    assert result == Decimal("0.5")


# ----------------------------------------------------------------------
# batch_get_shares
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_get_shares_delegates_to_batch_fetch_with_one_call(
    reader: PostgresCryptoLendingReader,
) -> None:
    """The adapter must collapse N receipt-token shares into one DB call.

    Per-allocation ``get_share`` was the dominant cost on ``/risk-capital``;
    if this regresses (e.g. someone re-introduces an in-place ``get_share``
    loop) the whole fan-out elimination is silently undone.
    """
    info_a = _aave_like_info()
    info_b = replace(info_a, receipt_token_id=100, receipt_token_token_id=888)

    with patch(
        "app.adapters.postgres.crypto_lending_reader.batch_fetch_shares",
        AsyncMock(return_value={(1, 777): Decimal("0.25"), (1, 888): Decimal("0.5")}),
    ) as mock_batch:
        out = await reader.batch_get_shares([info_a, info_b], DUMMY_PRIME)

    assert mock_batch.await_count == 1
    assert out == {99: Decimal("0.25"), 100: Decimal("0.5")}


@pytest.mark.asyncio
async def test_batch_get_shares_returns_error_values_for_invalid_inputs(
    reader: PostgresCryptoLendingReader,
) -> None:
    """Validation failures must surface as per-asset *values*, not exceptions.

    A single missing ``receipt_token_token_id`` (warm-up window) or unsupported
    protocol must not abort the whole batch.
    """
    info_morpho_unindexed = replace(_morpho_info(), receipt_token_id=200)  # receipt_token_token_id=None
    info_unsupported = replace(_aave_like_info(), receipt_token_id=300, protocol_name="curve_stableswap")

    with patch(
        "app.adapters.postgres.crypto_lending_reader.batch_fetch_shares",
        AsyncMock(return_value={}),
    ) as mock_batch:
        out = await reader.batch_get_shares([info_morpho_unindexed, info_unsupported], DUMMY_PRIME)

    # Neither input could be turned into a (chain_id, token_id) lookup, so the
    # DB layer should never have been called.
    mock_batch.assert_not_awaited()
    assert isinstance(out[200], MissingShareError)
    assert isinstance(out[300], ValueError)


@pytest.mark.asyncio
async def test_batch_get_shares_empty_input_short_circuits(
    reader: PostgresCryptoLendingReader,
) -> None:
    with patch(
        "app.adapters.postgres.crypto_lending_reader.batch_fetch_shares",
        AsyncMock(return_value={}),
    ) as mock_batch:
        out = await reader.batch_get_shares([], DUMMY_PRIME)
    mock_batch.assert_not_awaited()
    assert out == {}


@pytest.mark.asyncio
async def test_batch_get_shares_collapses_duplicate_pairs(
    reader: PostgresCryptoLendingReader,
) -> None:
    """Two receipt tokens that map to the same (chain_id, token_id) share one DB row.

    Defends the dedup logic: if both copies were re-issued as separate DB
    queries the batch would silently re-introduce per-asset fan-out.
    """
    info_a = _aave_like_info()
    info_b = replace(info_a, receipt_token_id=100)  # same chain_id+token_id

    captured = {}

    async def fake_batch(*, engine, requests, wallet_address, max_stale_seconds):  # noqa: ARG001
        captured["n_requests"] = len(list(requests))
        return {(1, 777): Decimal("0.4")}

    with patch(
        "app.adapters.postgres.crypto_lending_reader.batch_fetch_shares",
        side_effect=fake_batch,
    ):
        out = await reader.batch_get_shares([info_a, info_b], DUMMY_PRIME)

    assert captured["n_requests"] == 1
    assert out[99] == Decimal("0.4")
    assert out[100] == Decimal("0.4")


@pytest.mark.asyncio
async def test_batch_get_shares_accepts_maple(
    reader: PostgresCryptoLendingReader,
) -> None:
    """Maple resolves through the same lookup as ``get_share``; the batch must not
    reject it as an unsupported protocol, or the batched path would 500 for an
    asset the un-batched path prices fine once Maple gains a risk model."""
    info = replace(_maple_info(), receipt_token_token_id=555)

    with patch(
        "app.adapters.postgres.crypto_lending_reader.batch_fetch_shares",
        AsyncMock(return_value={(1, 555): Decimal("0.3")}),
    ):
        out = await reader.batch_get_shares([info], DUMMY_PRIME)

    assert out[99] == Decimal("0.3")


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


@pytest.mark.asyncio
async def test_batch_get_breakdowns_groups_aave_by_protocol(
    reader: PostgresCryptoLendingReader,
    aave_breakdown_repo: MagicMock,
    morpho_breakdown_repo: MagicMock,
) -> None:
    """Aave-like infos of one protocol resolve in a single get_backed_breakdowns
    call (protocol-wide CTEs run once); non-aave protocols fall back to per-token
    lookups. Results are keyed by receipt_token_id."""
    info_a = _aave_like_info()  # receipt_token_id=99, protocol_id=1, underlying=42
    info_b = replace(_aave_like_info(), receipt_token_id=100, underlying_token_id=43)
    morpho = replace(_morpho_info(), receipt_token_id=101)

    aave_breakdown_repo.get_backed_breakdowns = AsyncMock(
        return_value={
            42: BackedBreakdown(backed_asset_id=42, items=()),
            43: BackedBreakdown(backed_asset_id=43, items=()),
        }
    )

    out = await reader.batch_get_breakdowns([info_a, info_b, morpho])

    aave_breakdown_repo.get_backed_breakdowns.assert_awaited_once_with(1, [42, 43])
    aave_breakdown_repo.get_backed_breakdown.assert_not_awaited()
    assert out[99].backed_asset_id == 42
    assert out[100].backed_asset_id == 43
    # Morpho routed through its own (per-token) resolution.
    assert out[101].backed_asset_id == 55
