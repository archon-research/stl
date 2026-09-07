from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_risk_capital import ReferenceAllocation, ReferencePrimeRiskCapital
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService

# A real axis-synome spark ALM proxy: the service resolves the star name through
# the contract, so a placeholder address would resolve to no prime at all.
_SPARK_ALM = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_UNKNOWN_PROXY = "0x" + "ab" * 20
_TOKEN = "0x" + "cd" * 20
_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


def _allocation(*, receipt_token_id: int | None = 41) -> ReferenceAllocation:
    return ReferenceAllocation(
        protocol_name="sparklend",
        network="ethereum",
        symbol="spUSDT",
        name="Spark USDT",
        token_address=_TOKEN,
        loan_token_address="0x" + "12" * 20,
        loan_token_symbol="USDS",
        exposure_usd=Decimal("100"),
        required_risk_capital_usd=Decimal("1"),
        crr_pct=Decimal("1"),
        receipt_token_id=receipt_token_id,
        chain_id=1,
        chain="mainnet",
    )


def _snapshot(*allocations: ReferenceAllocation) -> ReferencePrimeRiskCapital:
    zero = Decimal("0")
    return ReferencePrimeRiskCapital(
        star="spark",
        synced_at=_SYNCED_AT,
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


def _service(snapshot: ReferencePrimeRiskCapital | None):
    provider = AsyncMock()
    provider.get_prime.return_value = snapshot
    directory = AsyncMock()
    directory.list_primes.return_value = []
    return ReferenceRiskCapitalService(provider, directory), provider


async def test_get_serves_the_stored_snapshot_with_its_resolved_registry_ids():
    # The registry join is the reader's SQL now, so the service must pass rows
    # through untouched rather than re-resolving them per row.
    service, _ = _service(_snapshot(_allocation()))

    result = await service.get(EthAddress(_SPARK_ALM))

    assert result is not None
    assert result.synced_at == _SYNCED_AT
    assert result.per_allocation[0].receipt_token_id == 41
    assert result.per_allocation[0].chain == "mainnet"


async def test_get_keeps_a_row_stl_does_not_index():
    # Most of the breakdown can be positions STL has no registry entry for; an
    # unresolved id must not drop the row.
    service, _ = _service(_snapshot(_allocation(receipt_token_id=None)))

    result = await service.get(EthAddress(_SPARK_ALM))

    assert result is not None
    assert result.per_allocation[0].receipt_token_id is None
    assert result.per_allocation[0].symbol == "spUSDT"


async def test_get_returns_none_for_an_address_that_names_no_prime():
    service, provider = _service(_snapshot())

    assert await service.get(EthAddress(_UNKNOWN_PROXY)) is None
    provider.get_prime.assert_not_awaited()


async def test_get_asks_the_reader_for_the_resolved_star():
    service, provider = _service(_snapshot())

    await service.get(EthAddress(_SPARK_ALM))

    provider.get_prime.assert_awaited_once_with("spark")


async def test_get_returns_none_when_no_cycle_has_reported_on_the_prime():
    service, _ = _service(None)

    assert await service.get(EthAddress(_SPARK_ALM)) is None


async def test_covered_stars_is_the_readers_answer():
    service, provider = _service(_snapshot())
    provider.covered_stars.return_value = frozenset({"spark"})

    assert await service.covered_stars() == frozenset({"spark"})
