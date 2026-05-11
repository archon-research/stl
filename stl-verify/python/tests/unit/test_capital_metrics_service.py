from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress, Prime
from app.services.capital_metrics_service import CapitalMetricsService

_VALID_ADDR = EthAddress("0x" + "ab" * 20)


@pytest.mark.asyncio
async def test_get_capital_metrics_returns_none_for_unknown_prime() -> None:
    repo = AsyncMock()
    repo.list_primes.return_value = [Prime(id="0x" + "cd" * 20, name="other", address="0x" + "cd" * 20)]
    service = CapitalMetricsService(repo)

    result = await service.get_capital_metrics(_VALID_ADDR)

    assert result is None
    repo.get_total_usd_exposure.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_capital_metrics_sets_ratio_none_when_total_capital_zero() -> None:
    repo = AsyncMock()
    repo.list_primes.return_value = [Prime(id=str(_VALID_ADDR), name="spark", address=str(_VALID_ADDR))]
    repo.get_total_usd_exposure.return_value = Decimal("250.50")
    service = CapitalMetricsService(repo)

    result = await service.get_capital_metrics(_VALID_ADDR)

    assert result is not None
    assert result.prime_id == str(_VALID_ADDR)
    assert result.risk_capital == Decimal("250.50")
    assert result.total_capital == Decimal("0")
    assert result.risk_to_capital_ratio is None
    assert result.benchmark_source == "onchain:allocation_position+onchain_token_price"


@pytest.mark.asyncio
async def test_list_all_capital_metrics_builds_rows_for_each_prime() -> None:
    repo = AsyncMock()
    first = "0x" + "ab" * 20
    second = "0x" + "cd" * 20
    repo.list_primes.return_value = [
        Prime(id=first, name="spark", address=first),
        Prime(id=second, name="grove", address=second),
    ]
    repo.get_total_usd_exposure.side_effect = [Decimal("100"), Decimal("200")]
    service = CapitalMetricsService(repo)

    result = await service.list_all_capital_metrics()

    assert len(result) == 2
    assert result[0].risk_capital == Decimal("100")
    assert result[1].risk_capital == Decimal("200")
    assert result[0].risk_to_capital_ratio is None
    assert repo.get_total_usd_exposure.await_count == 2


@pytest.mark.asyncio
async def test_list_all_capital_metrics_propagates_repository_error() -> None:
    repo = AsyncMock()
    repo.list_primes.side_effect = RuntimeError("boom")
    service = CapitalMetricsService(repo)

    with pytest.raises(RuntimeError, match="boom"):
        await service.list_all_capital_metrics()
