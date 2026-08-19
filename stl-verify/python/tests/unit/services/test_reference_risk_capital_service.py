from decimal import Decimal
from unittest.mock import AsyncMock

from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.reference_risk_capital import ReferenceAllocation, ReferencePrimeRiskCapital
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService

# A real axis-synome spark ALM proxy: the service resolves the star name through
# the contract, so a placeholder address would resolve to no prime at all.
_SPARK_ALM = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_UNKNOWN_PROXY = "0x" + "ab" * 20
_TOKEN = "0x" + "cd" * 20
# Uniswap V4 identifies a position by 32-byte pool id, which is not an address.
_V4_POOL_ID = "0x" + "ef" * 32


def _allocation(*, network: str = "ethereum", token_address: str = _TOKEN) -> ReferenceAllocation:
    return ReferenceAllocation(
        protocol_name="sparklend",
        network=network,
        symbol="spUSDT",
        name="Spark USDT",
        token_address=token_address,
        loan_token_address="0x" + "12" * 20,
        loan_token_symbol="USDS",
        exposure_usd=Decimal("100"),
        required_risk_capital_usd=Decimal("1"),
        crr_pct=Decimal("1"),
    )


def _snapshot(*allocations: ReferenceAllocation) -> ReferencePrimeRiskCapital:
    zero = Decimal("0")
    return ReferencePrimeRiskCapital(
        star="spark",
        exposure_usd=Decimal("100"),
        required_risk_capital_usd=Decimal("1"),
        total_risk_capital_usd=Decimal("10"),
        encumbrance_ratio=Decimal("0.1"),
        exposure_share=zero,
        junior_risk_capital_usd=zero,
        senior_risk_capital_usd=zero,
        internal_junior_risk_capital_usd=zero,
        external_junior_risk_capital_usd=zero,
        tokenized_junior_risk_capital_usd=zero,
        internal_senior_risk_capital_usd=zero,
        external_senior_risk_capital_usd=zero,
        epi_utilization=zero,
        spj_utilization=zero,
        per_allocation=allocations,
    )


def _token_info(receipt_token_id: int) -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=receipt_token_id,
        protocol_id=1,
        underlying_token_id=2,
        receipt_token_address=b"\xcd" * 20,
        chain_id=1,
        protocol_name="sparklend",
        receipt_token_token_id=None,
    )


def _service(snapshot: ReferencePrimeRiskCapital | None, lookup: AsyncMock | None = None):
    provider = AsyncMock()
    provider.get_prime.return_value = snapshot
    receipt_tokens = lookup or AsyncMock()
    return ReferenceRiskCapitalService(provider, receipt_tokens), provider, receipt_tokens


async def test_get_resolves_an_upstream_row_to_stls_receipt_token_id():
    lookup = AsyncMock()
    lookup.get_by_chain_and_address.return_value = _token_info(41)
    service, _, _ = _service(_snapshot(_allocation()), lookup)

    result = await service.get(EthAddress(_SPARK_ALM))

    assert result is not None
    assert result.per_allocation[0].receipt_token_id == 41
    assert result.per_allocation[0].chain == "mainnet"


async def test_get_leaves_a_uniswap_v4_pool_id_unresolved_without_querying_the_registry():
    lookup = AsyncMock()
    service, _, _ = _service(_snapshot(_allocation(token_address=_V4_POOL_ID)), lookup)

    result = await service.get(EthAddress(_SPARK_ALM))

    assert result is not None
    assert result.per_allocation[0].receipt_token_id is None
    assert result.per_allocation[0].chain == "mainnet"
    lookup.get_by_chain_and_address.assert_not_awaited()


async def test_get_leaves_a_row_on_an_unmapped_network_unresolved():
    lookup = AsyncMock()
    service, _, _ = _service(_snapshot(_allocation(network="solana")), lookup)

    result = await service.get(EthAddress(_SPARK_ALM))

    assert result is not None
    assert result.per_allocation[0].receipt_token_id is None
    assert result.per_allocation[0].chain is None
    lookup.get_by_chain_and_address.assert_not_awaited()


async def test_get_leaves_a_token_stl_does_not_index_unresolved():
    lookup = AsyncMock()
    lookup.get_by_chain_and_address.return_value = None
    service, _, _ = _service(_snapshot(_allocation()), lookup)

    result = await service.get(EthAddress(_SPARK_ALM))

    assert result is not None
    assert result.per_allocation[0].receipt_token_id is None


async def test_get_returns_none_for_a_proxy_absent_from_the_axis_synome_contract():
    service, provider, _ = _service(_snapshot())

    assert await service.get(EthAddress(_UNKNOWN_PROXY)) is None
    provider.get_prime.assert_not_awaited()


async def test_get_returns_none_when_the_monitor_does_not_track_the_prime():
    service, _, _ = _service(None)

    assert await service.get(EthAddress(_SPARK_ALM)) is None
