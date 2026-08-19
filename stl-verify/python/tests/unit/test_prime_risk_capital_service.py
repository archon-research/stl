from decimal import Decimal
from types import SimpleNamespace
from typing import Protocol, cast
from unittest.mock import AsyncMock, patch

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import LiquidationParams
from app.domain.exceptions import AdapterDataMissingError, MissingShareError, StaleShareError
from app.ports.allocation_repository import AllocationRepositoryPort
from app.services.model_registry import ModelRegistry
from app.services.prime_risk_capital_service import PrimeRiskCapitalService
from tests.factories import make_receipt_token_position


class _AppliesTo(Protocol):
    """Structural type for anything the fake registry can dispatch on.

    Both ``_FakeModel`` and the concrete ``CryptoLendingRiskService`` expose
    ``applies_to``; widening the parameter lets tests mix them without a cast.
    """

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool: ...


_PRIME = EthAddress("0x" + "ab" * 20)


class _FakeModel:
    def __init__(self, name: str, applies_ids: set[int], rrc: Decimal, crr: Decimal) -> None:
        self.risk_model = name
        self._ids = applies_ids
        self._rrc = rrc
        self._crr = crr
        self.computed_ids: list[int] = []

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:
        return asset_id in self._ids

    async def compute(self, asset_id, prime_id, overrides):
        self.computed_ids.append(asset_id)
        return SimpleNamespace(rrc_usd=self._rrc, comparable_crr_pct=self._crr, risk_model=self.risk_model)


class _FakeRegistry:
    def __init__(self, models: list[_AppliesTo]) -> None:
        self._models = models

    def applicable(self, asset_id: int, prime_id: EthAddress):
        return [m for m in self._models if m.applies_to(asset_id, prime_id)]


def _repo(positions, total_rc):
    repo = AsyncMock(spec=AllocationRepositoryPort)
    repo.list_receipt_token_positions.return_value = positions
    repo.get_latest_total_capital_usd.return_value = total_rc
    return repo


def _service(repo: AllocationRepositoryPort, registry: _FakeRegistry) -> PrimeRiskCapitalService:
    # _FakeRegistry / _FakeModel are structural stand-ins for the concrete
    # ModelRegistry / RiskModel; the service only reads registry.applicable().
    return PrimeRiskCapitalService(repo, cast(ModelRegistry, registry))


_SPARK_MAINNET_ALM = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_SPARK_AVALANCHE_ALM = "0xece6b0e8a54c2f44e066fbb9234e7157b15b7fec"
_SPARK_MAINNET_ALM_MIXED_CASE = "0x1601843C5E9BC251A3272907010AFA41FA18347E"
_SPARK_BASE_ALM = "0x2917956eff0b5eaf030abdb4ef4296df775009ca"
# On a chain no allocation tracker serves (acknowledgedUnservedByTrackerChains).
_SPARK_ARBITRUM_ALM = "0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709"


def _repo_by_proxy(positions_by_proxy: dict[str, list], total_rc: Decimal | None):
    """Repository stub that answers per queried proxy address.

    Keys are normalised to lowercase strings because ``EthAddress`` compares
    case-sensitively against a plain ``str`` despite hashing case-insensitively.
    """
    normalised = {address.lower(): positions for address, positions in positions_by_proxy.items()}

    async def _positions(proxy):
        return normalised.get(str(proxy).lower(), [])

    repo = AsyncMock(spec=AllocationRepositoryPort)
    repo.list_receipt_token_positions.side_effect = _positions
    repo.get_latest_total_capital_usd.return_value = total_rc
    return repo


def _two_chain_spark_repo():
    """Spark with one priced position on mainnet and one on avalanche.

    Mainnet's asset prices to RRC 40, avalanche's to RRC 2, against a prime-wide
    treasury of 100 — so the correct prime ratio is 42/100 while the mainnet
    proxy's own (deprecated) ratio stays 40/100.
    """
    return _repo_by_proxy(
        {
            _SPARK_MAINNET_ALM: [make_receipt_token_position(receipt_token_id=1, amount_usd=Decimal("400"))],
            _SPARK_AVALANCHE_ALM: [make_receipt_token_position(receipt_token_id=2, amount_usd=Decimal("20"))],
        },
        Decimal("100"),
    )


def _two_asset_registry() -> _FakeRegistry:
    """Two gap_sweep models with disjoint asset sets, so each asset gets its own RRC."""
    return _FakeRegistry(
        [
            _FakeModel("gap_sweep", {1}, rrc=Decimal("40"), crr=Decimal("10")),
            _FakeModel("gap_sweep", {2}, rrc=Decimal("2"), crr=Decimal("10")),
        ]
    )


def _partly_modeled_two_chain_spark_repo():
    """Spark holding an unmodeled position, so modeled exposure differs from exposure.

    Mainnet holds 300 modeled plus 100 unmodeled; avalanche holds 100 modeled. The
    mainnet proxy is therefore 300/400 modeled while the prime is 400/500, so the
    proxy-scoped and prime-scoped modeled figures take different values.
    ``_two_chain_spark_repo`` prices every position, which makes both 1.0000 and
    cannot tell a prime-scoped modeled field from a proxy-scoped one.
    """
    return _repo_by_proxy(
        {
            _SPARK_MAINNET_ALM: [
                make_receipt_token_position(receipt_token_id=1, amount_usd=Decimal("300")),
                make_receipt_token_position(receipt_token_id=2, amount_usd=Decimal("100")),
            ],
            _SPARK_AVALANCHE_ALM: [make_receipt_token_position(receipt_token_id=3, amount_usd=Decimal("100"))],
        },
        Decimal("100"),
    )


def _registry_leaving_asset_two_unmodeled() -> _FakeRegistry:
    """A gap_sweep model for assets 1 and 3 only, so asset 2 stays unmodeled."""
    return _FakeRegistry([_FakeModel("gap_sweep", {1, 3}, rrc=Decimal("40"), crr=Decimal("10"))])


@pytest.mark.asyncio
async def test_compute_leaves_the_proxy_scoped_required_risk_capital_unchanged():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.required_risk_capital_usd == Decimal("40")


@pytest.mark.asyncio
async def test_compute_leaves_the_proxy_scoped_exposure_unchanged():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.exposure_usd == Decimal("400")


@pytest.mark.asyncio
async def test_compute_leaves_the_deprecated_encumbrance_ratio_unchanged():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.encumbrance_ratio == Decimal("0.4000")


@pytest.mark.asyncio
async def test_compute_leaves_per_allocation_scoped_to_the_queried_proxy():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_AVALANCHE_ALM))

    assert {alloc.receipt_token_id for alloc in result.per_allocation} == {2}


@pytest.mark.asyncio
async def test_compute_sums_prime_required_risk_capital_across_the_alm_proxies():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.prime_required_risk_capital_usd == Decimal("42")


@pytest.mark.asyncio
async def test_compute_sums_prime_exposure_across_the_alm_proxies():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.prime_exposure_usd == Decimal("420")


@pytest.mark.asyncio
async def test_compute_sums_prime_modeled_exposure_across_the_alm_proxies():
    service = _service(_partly_modeled_two_chain_spark_repo(), _registry_leaving_asset_two_unmodeled())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.prime_modeled_exposure_usd == Decimal("400")


@pytest.mark.asyncio
async def test_compute_divides_prime_modeled_exposure_by_prime_exposure():
    service = _service(_partly_modeled_two_chain_spark_repo(), _registry_leaving_asset_two_unmodeled())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.prime_modeled_pct == Decimal("0.8000")


@pytest.mark.asyncio
async def test_compute_leaves_the_proxy_scoped_modeled_figures_unchanged():
    service = _service(_partly_modeled_two_chain_spark_repo(), _registry_leaving_asset_two_unmodeled())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.modeled_exposure_usd == Decimal("300")
    assert result.modeled_pct == Decimal("0.7500")


@pytest.mark.asyncio
async def test_compute_reports_the_same_prime_modeled_figures_from_every_proxy():
    service = _service(_partly_modeled_two_chain_spark_repo(), _registry_leaving_asset_two_unmodeled())

    from_mainnet = await service.compute(EthAddress(_SPARK_MAINNET_ALM))
    from_avalanche = await service.compute(EthAddress(_SPARK_AVALANCHE_ALM))

    assert from_avalanche.prime_modeled_exposure_usd == from_mainnet.prime_modeled_exposure_usd
    assert from_avalanche.prime_modeled_pct == from_mainnet.prime_modeled_pct


@pytest.mark.asyncio
async def test_compute_divides_the_summed_rrc_by_the_prime_wide_treasury():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.prime_encumbrance_ratio == Decimal("0.4200")


@pytest.mark.asyncio
async def test_compute_takes_total_risk_capital_once_rather_than_per_proxy():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.total_risk_capital_usd == Decimal("100")


@pytest.mark.asyncio
async def test_compute_reports_the_same_prime_figures_from_every_proxy():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    from_mainnet = await service.compute(EthAddress(_SPARK_MAINNET_ALM))
    from_avalanche = await service.compute(EthAddress(_SPARK_AVALANCHE_ALM))

    assert from_avalanche.prime_required_risk_capital_usd == from_mainnet.prime_required_risk_capital_usd
    assert from_avalanche.prime_encumbrance_ratio == from_mainnet.prime_encumbrance_ratio


@pytest.mark.asyncio
async def test_compute_reports_a_per_chain_breakdown_of_the_aggregation():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    by_chain = {row.chain: row.required_risk_capital_usd for row in result.prime_per_chain}
    assert by_chain["mainnet"] == Decimal("40")
    assert by_chain["avalanche-c"] == Decimal("2")


@pytest.mark.asyncio
async def test_compute_reports_null_not_zero_for_a_chain_no_tracker_serves():
    """The distinction the prime-wide totals rest on.

    Spark's arbitrum, optimism and unichain proxies have no
    ``allocation_position`` rows because no tracker indexes those chains. Reported
    as ``0`` they would claim the prime holds nothing there, which understates
    encumbrance in the direction that looks safe.
    """
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    unserved = [row for row in result.prime_per_chain if row.chain == "arbitrum"]
    assert len(unserved) == 1
    assert unserved[0].exposure_usd is None
    assert unserved[0].required_risk_capital_usd is None
    assert unserved[0].allocation_count is None


@pytest.mark.asyncio
async def test_compute_names_the_chains_its_totals_exclude():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.prime_unserved_chains == ("arbitrum", "optimism", "unichain")


@pytest.mark.asyncio
async def test_compute_does_not_query_a_proxy_on_an_unserved_chain():
    """Each skipped proxy is a pooled connection not taken; see Settings.db_pool_size."""
    repo = _two_chain_spark_repo()
    service = _service(repo, _two_asset_registry())

    await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    queried = {str(call.args[0]).lower() for call in repo.list_receipt_token_positions.await_args_list}
    assert queried == {_SPARK_MAINNET_ALM, _SPARK_AVALANCHE_ALM, _SPARK_BASE_ALM}


@pytest.mark.asyncio
async def test_compute_covers_every_proxy_of_the_prime_in_the_per_chain_breakdown():
    """Served or not, a proxy is present: absence would read as "no proxy there"."""
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert len(result.prime_per_chain) == 6
    assert tuple(row.proxy_address for row in result.prime_per_chain) == result.prime_proxies


@pytest.mark.asyncio
async def test_compute_totals_equal_the_sum_of_the_per_chain_rows_that_carry_figures():
    """`prime_per_chain` is sold as making the total auditable, so the sum must tie.

    The totals are summed from ``per_proxy`` while the rows are built from the
    prime's proxy list, so this is two derivations of one figure meeting.
    """
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_AVALANCHE_ALM))

    assert sum(row.exposure_usd for row in result.prime_per_chain if row.exposure_usd is not None) == (
        result.prime_exposure_usd
    )
    assert (
        sum(
            row.required_risk_capital_usd for row in result.prime_per_chain if row.required_risk_capital_usd is not None
        )
        == result.prime_required_risk_capital_usd
    )


@pytest.mark.asyncio
async def test_compute_warns_when_a_proxy_holds_positions_on_a_chain_declared_unserved():
    """A stale SERVED_TRACKER_CHAINS silently nulls real per-chain figures."""
    repo = _repo_by_proxy(
        {_SPARK_ARBITRUM_ALM: [make_receipt_token_position(receipt_token_id=1, amount_usd=Decimal("400"))]},
        Decimal("100"),
    )
    service = _service(repo, _two_asset_registry())

    with patch("app.services.prime_risk_capital_service.logger") as mock_logger:
        await service.compute(EthAddress(_SPARK_ARBITRUM_ALM))

    assert "unserved chain" in mock_logger.warning.call_args.args[0]


@pytest.mark.asyncio
async def test_compute_orders_prime_proxies_identically_regardless_of_which_was_queried():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    from_mainnet = await service.compute(EthAddress(_SPARK_MAINNET_ALM))
    from_avalanche = await service.compute(EthAddress(_SPARK_AVALANCHE_ALM))

    assert from_avalanche.prime_proxies == from_mainnet.prime_proxies


@pytest.mark.asyncio
async def test_compute_names_the_prime_it_aggregated_over():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert result.prime_name == "spark"


@pytest.mark.asyncio
async def test_compute_lists_every_proxy_it_aggregated_over():
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    lowered = {address.lower() for address in result.prime_proxies}
    assert _SPARK_MAINNET_ALM in lowered
    assert _SPARK_AVALANCHE_ALM in lowered


@pytest.mark.asyncio
async def test_compute_falls_back_to_the_queried_proxy_when_it_is_not_in_the_contract():
    unknown = "0x" + "cd" * 20
    repo = _repo_by_proxy(
        {unknown: [make_receipt_token_position(receipt_token_id=1, amount_usd=Decimal("400"))]},
        Decimal("100"),
    )
    service = _service(repo, _two_asset_registry())

    result = await service.compute(EthAddress(unknown))

    assert result.prime_required_risk_capital_usd == Decimal("40")
    assert result.prime_name is None
    assert result.prime_proxies == (unknown,)


@pytest.mark.asyncio
async def test_compute_resolves_siblings_for_a_mixed_case_queried_address():
    assert _SPARK_MAINNET_ALM_MIXED_CASE.lower() == _SPARK_MAINNET_ALM
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    result = await service.compute(EthAddress(_SPARK_MAINNET_ALM_MIXED_CASE))

    assert result.prime_name == "spark"
    assert result.prime_required_risk_capital_usd == Decimal("42")


@pytest.mark.asyncio
async def test_compute_normalises_prime_scoped_addresses_for_a_mixed_case_queried_address():
    """The prime-scoped lists are reconciliation keys, so they must be byte-identical.

    ``EthAddress`` preserves the caller's casing and siblings come from the
    contract lowercased, so a checksummed query would otherwise emit one
    mixed-case element among lowercase ones — leaving a consumer that dedupes by
    comparing or hashing these lists seeing two different primes.
    """
    service = _service(_two_chain_spark_repo(), _two_asset_registry())

    from_mixed_case = await service.compute(EthAddress(_SPARK_MAINNET_ALM_MIXED_CASE))
    from_lowercase = await service.compute(EthAddress(_SPARK_MAINNET_ALM))

    assert from_mixed_case.prime_proxies == from_lowercase.prime_proxies
    assert from_mixed_case.prime_per_chain == from_lowercase.prime_per_chain


@pytest.mark.asyncio
async def test_compute_mixes_modeled_and_unmodeled_allocations():
    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="spUSDT", amount_usd=Decimal("600")),
        make_receipt_token_position(receipt_token_id=2, symbol="spDAI", amount_usd=Decimal("400")),
    ]
    # gap_sweep applies only to asset 1.
    registry = _FakeRegistry([_FakeModel("gap_sweep", {1}, rrc=Decimal("30"), crr=Decimal("5"))])
    service = _service(_repo(positions, Decimal("100")), registry)

    result = await service.compute(_PRIME)

    assert result.model == "gap_sweep"
    assert result.exposure_usd == Decimal("1000")
    assert result.total_risk_capital_usd == Decimal("100")
    assert result.required_risk_capital_usd == Decimal("30")
    assert result.modeled_exposure_usd == Decimal("600")
    assert result.modeled_pct == Decimal("0.6000")
    assert result.encumbrance_ratio == Decimal("0.3000")  # 30 / 100

    by_id = {a.receipt_token_id: a for a in result.per_allocation}
    assert by_id[1].applied is True
    assert by_id[1].required_risk_capital_usd == Decimal("30")
    assert by_id[1].crr_pct == Decimal("5")
    assert by_id[1].model == "gap_sweep"
    assert by_id[2].applied is False
    assert by_id[2].required_risk_capital_usd is None
    assert by_id[2].crr_pct is None
    assert by_id[2].model is None
    assert by_id[2].unpriced_reason == "no_model"


@pytest.mark.asyncio
async def test_compute_encumbrance_none_when_no_total_risk_capital():
    positions = [make_receipt_token_position(receipt_token_id=1, symbol="spUSDT", amount_usd=Decimal("600"))]
    registry = _FakeRegistry([_FakeModel("gap_sweep", {1}, rrc=Decimal("30"), crr=Decimal("5"))])
    service = _service(_repo(positions, None), registry)

    result = await service.compute(_PRIME)

    assert result.total_risk_capital_usd is None
    assert result.encumbrance_ratio is None
    assert result.required_risk_capital_usd == Decimal("30")


@pytest.mark.asyncio
async def test_compute_empty_positions_yields_zeroes_and_null_ratios():
    registry = _FakeRegistry([_FakeModel("gap_sweep", set(), rrc=Decimal("0"), crr=Decimal("0"))])
    service = _service(_repo([], Decimal("100")), registry)

    result = await service.compute(_PRIME)

    assert result.exposure_usd == Decimal("0")
    assert result.required_risk_capital_usd == Decimal("0")
    assert result.modeled_pct is None
    assert result.encumbrance_ratio == Decimal("0")  # 0 / 100
    assert result.per_allocation == []


@pytest.mark.asyncio
async def test_compute_skips_zero_exposure_positions():
    # Asset 1 has zero balance: the model applies, but we must not run a compute
    # for it (it contributes nothing) and it is reported as not modeled.
    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="aEthUSDT", amount_usd=Decimal("0")),
        make_receipt_token_position(receipt_token_id=2, symbol="spUSDT", amount_usd=Decimal("600")),
    ]
    model = _FakeModel("gap_sweep", {1, 2}, rrc=Decimal("30"), crr=Decimal("5"))
    service = _service(_repo(positions, Decimal("100")), _FakeRegistry([model]))

    result = await service.compute(_PRIME)

    # The zero-exposure position must be reported as not modeled and, crucially,
    # must never trigger a (costly) model compute.
    assert model.computed_ids == [2]
    by_id = {a.receipt_token_id: a for a in result.per_allocation}
    assert by_id[1].applied is False
    assert by_id[1].unpriced_reason == "no_model"
    assert by_id[2].applied is True
    assert result.required_risk_capital_usd == Decimal("30")
    assert result.modeled_exposure_usd == Decimal("600")


@pytest.mark.asyncio
async def test_compute_ignores_non_default_models():
    positions = [make_receipt_token_position(receipt_token_id=1, symbol="spUSDT", amount_usd=Decimal("600"))]
    # Only a non-default model applies; the default (gap_sweep) does not.
    registry = _FakeRegistry([_FakeModel("suraf", {1}, rrc=Decimal("99"), crr=Decimal("9"))])
    service = _service(_repo(positions, Decimal("100")), registry)

    result = await service.compute(_PRIME)

    assert result.required_risk_capital_usd == Decimal("0")
    assert result.per_allocation[0].applied is False
    assert result.modeled_exposure_usd == Decimal("0")


# ----------------------------------------------------------------------
# Share fan-out elimination
# ----------------------------------------------------------------------

from app.adapters.postgres.crypto_lending_reader import PostgresCryptoLendingReader  # noqa: E402
from app.domain.entities.backed_breakdown import BackedBreakdown  # noqa: E402
from app.domain.entities.receipt_token import ReceiptTokenInfo  # noqa: E402
from app.services.crypto_lending_risk_service import CryptoLendingRiskService  # noqa: E402


def _info(receipt_token_id: int, receipt_token_token_id: int = 777) -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=receipt_token_id,
        protocol_id=1,
        underlying_token_id=42,
        receipt_token_address=bytes.fromhex("e7df13b8e3d6740fe17cbe928c7334243d86c92f"),
        chain_id=1,
        protocol_name="Aave V3",
        receipt_token_token_id=receipt_token_token_id,
    )


def _crypto_lending_service(reader) -> CryptoLendingRiskService:
    return CryptoLendingRiskService(
        reader=reader,
        default_gap_pct=Decimal("0.15"),
        supported_asset_ids={1, 2, 3},
    )


@pytest.mark.asyncio
async def test_prime_compute_uses_batch_get_shares_and_skips_per_asset_get_share():
    """The prime service must collapse per-allocation share lookups into one DB call.

    Regression check: if someone re-introduces ``reader.get_share`` inside the
    ``asyncio.gather`` loop, this test catches it — ``get_share`` must remain
    un-awaited and ``batch_get_shares`` must be called exactly once with all
    crypto-lending asset infos.
    """
    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="aWETH", amount_usd=Decimal("100")),
        make_receipt_token_position(receipt_token_id=2, symbol="aDAI", amount_usd=Decimal("200")),
    ]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    infos = {1: _info(1, 777), 2: _info(2, 888)}
    # Map each asset_id to its info for the prefetch's concurrent lookups.
    reader.get_receipt_token.side_effect = lambda aid: infos[aid]
    reader.batch_get_shares.return_value = {1: Decimal("0.4"), 2: Decimal("0.25")}
    empty_breakdown = BackedBreakdown(backed_asset_id=42, items=())
    reader.batch_get_breakdowns.return_value = {1: empty_breakdown, 2: empty_breakdown}

    model = _crypto_lending_service(reader)
    registry = _FakeRegistry([model])
    service = _service(_repo(positions, Decimal("1000")), registry)

    result = await service.compute(_PRIME)

    # batch_get_shares hit once, get_share never hit.
    reader.batch_get_shares.assert_awaited_once()
    reader.get_share.assert_not_awaited()
    # Each asset's receipt-token info is fetched once during prefetch and reused
    # by compute_with_share, not re-fetched — N lookups, not 2N.
    assert reader.get_receipt_token.await_count == 2
    # The result still surfaces the per-allocation entries even though the
    # gap-sweep RRC happens to be zero (no breakdown items in the test fixture).
    assert len(result.per_allocation) == 2


@pytest.mark.asyncio
async def test_prime_compute_batches_breakdowns_and_skips_per_asset_fetch():
    """Backed breakdowns are prefetched in one batch and reused, not fetched once
    per allocation — the protocol-wide breakdown query must not fan out."""
    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="aWETH", amount_usd=Decimal("100")),
        make_receipt_token_position(receipt_token_id=2, symbol="aDAI", amount_usd=Decimal("200")),
    ]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    reader.get_receipt_token.side_effect = lambda aid: {1: _info(1, 777), 2: _info(2, 888)}[aid]
    reader.batch_get_shares.return_value = {1: Decimal("0.4"), 2: Decimal("0.25")}
    empty = BackedBreakdown(backed_asset_id=42, items=())
    reader.batch_get_breakdowns.return_value = {1: empty, 2: empty}
    service = _service(_repo(positions, Decimal("1000")), _FakeRegistry([_crypto_lending_service(reader)]))

    await service.compute(_PRIME)

    reader.batch_get_breakdowns.assert_awaited_once()
    reader.get_breakdown.assert_not_awaited()


@pytest.mark.asyncio
async def test_prime_compute_logs_missing_receipt_token():
    """A crypto-lending position whose receipt-token record is missing is a data
    gap: it must be logged (not silently dropped from the batch), and — since no
    model can price it — still surface as an error rather than a fake zero RRC."""
    positions = [make_receipt_token_position(receipt_token_id=1, symbol="aWETH", amount_usd=Decimal("100"))]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    reader.get_receipt_token.return_value = None  # receipt-token record cannot be resolved
    model = _crypto_lending_service(reader)
    service = _service(_repo(positions, Decimal("1000")), _FakeRegistry([model]))

    with (
        patch("app.services.prime_risk_capital_service.logger") as mock_logger,
        pytest.raises(ValueError, match="receipt token not found"),
    ):
        await service.compute(_PRIME)

    reader.batch_get_shares.assert_not_awaited()
    mock_logger.warning.assert_called_once()
    fmt, arg = mock_logger.warning.call_args[0][0], mock_logger.warning.call_args[0][1]
    assert "no receipt-token record" in fmt and arg == 1


@pytest.mark.parametrize(
    "share_error, expected_reason",
    [
        (MissingShareError("no consistent balance+supply pair"), "share_data_missing"),
        (StaleShareError("supply row too old"), "share_data_stale"),
    ],
    ids=["missing", "stale"],
)
@pytest.mark.asyncio
async def test_prime_compute_degrades_share_error_to_unpriced(share_error, expected_reason):
    """A per-allocation share-lookup failure degrades just that allocation to
    unpriced (carrying the reason) and prices the rest — it must not fail the
    whole prime. Only a non-empty breakdown surfaces the error; empty breakdowns
    never consult the share (covered by the test below).
    """
    from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution

    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="aWETH", amount_usd=Decimal("100")),
        make_receipt_token_position(receipt_token_id=2, symbol="aDAI", amount_usd=Decimal("200")),
    ]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    reader.get_receipt_token.side_effect = lambda aid: {1: _info(1, 777), 2: _info(2, 888)}[aid]
    reader.batch_get_shares.return_value = {1: share_error, 2: Decimal("0.5")}
    nonempty_breakdown = BackedBreakdown(
        backed_asset_id=42,
        items=(
            CollateralContribution(
                token_id=99,
                symbol="WETH",
                backing_value=Decimal("1"),
                backing_pct=Decimal("1"),
                price_usd=Decimal("2000"),
            ),
        ),
    )
    reader.batch_get_breakdowns.return_value = {1: nonempty_breakdown, 2: nonempty_breakdown}
    service = _service(_repo(positions, Decimal("1000")), _FakeRegistry([_crypto_lending_service(reader)]))

    with patch("app.services.prime_risk_capital_service.logger") as mock_logger:
        result = await service.compute(_PRIME)  # must not raise

    by_id = {a.receipt_token_id: a for a in result.per_allocation}
    assert by_id[1].applied is False
    assert by_id[1].required_risk_capital_usd is None
    assert by_id[1].unpriced_reason == expected_reason
    assert by_id[2].applied is True
    assert by_id[2].unpriced_reason is None
    # The unpriced allocation still counts toward exposure, but not modeled exposure.
    assert result.exposure_usd == Decimal("300")
    assert result.modeled_exposure_usd == Decimal("200")
    # The data gap is logged, not silently masked.
    mock_logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_prime_compute_swallows_share_error_for_empty_breakdown():
    """Empty-breakdown assets must return 200, not 503, even when share lookup failed.

    Regression check for parity with the un-batched ``compute`` path: that
    path returned early on empty breakdowns and never called ``get_share``,
    so an asset with no backed-breakdown rows and a missing supply row
    contributed zero items without ever surfacing the share-lookup failure.
    The batched dispatcher must preserve that semantics.
    """
    from app.domain.entities.backed_breakdown import BackedBreakdown

    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="aWETH", amount_usd=Decimal("100")),
    ]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    reader.get_receipt_token.return_value = _info(1, 777)
    reader.batch_get_shares.return_value = {1: MissingShareError("warm-up")}
    reader.batch_get_breakdowns.return_value = {1: BackedBreakdown(backed_asset_id=42, items=())}

    model = _crypto_lending_service(reader)
    registry = _FakeRegistry([model])
    service = _service(_repo(positions, Decimal("1000")), registry)

    result = await service.compute(_PRIME)

    assert result.required_risk_capital_usd == Decimal("0")
    # The position is still reported (with zero RRC), matching the un-batched
    # behaviour for empty breakdowns. Crucially it is priced-to-zero, NOT
    # degraded to unpriced: the swallowed share error must not surface as an
    # unpriced_reason, or this test would pass even under the regression it guards.
    assert len(result.per_allocation) == 1
    alloc = result.per_allocation[0]
    assert alloc.applied is True
    assert alloc.unpriced_reason is None


@pytest.mark.asyncio
async def test_prime_compute_degrades_price_data_missing_to_unpriced():
    """A vault whose loan token has no USD price degrades to unpriced.

    An all-unpriced breakdown (every ``price_usd`` None) means the loan token
    itself is unpriced, so backing_value is raw loan-token units, not USD. The
    allocation must be reported as unpriced (``price_data_missing``) rather than a
    misleading fully-covered ``rrc=0``, while the rest of the prime still prices.
    """
    from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution

    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="mUNPX", amount_usd=Decimal("100")),
        make_receipt_token_position(receipt_token_id=2, symbol="aDAI", amount_usd=Decimal("200")),
    ]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    reader.get_receipt_token.side_effect = lambda aid: {1: _info(1, 777), 2: _info(2, 888)}[aid]
    reader.batch_get_shares.return_value = {1: Decimal("1"), 2: Decimal("1")}
    unpriced_breakdown = BackedBreakdown(
        backed_asset_id=1,
        items=(
            CollateralContribution(
                token_id=99,
                symbol="UNPX",
                backing_value=Decimal("100"),
                backing_pct=Decimal("100"),
                price_usd=None,
            ),
        ),
    )
    priced_breakdown = BackedBreakdown(
        backed_asset_id=2,
        items=(
            CollateralContribution(
                token_id=88,
                symbol="WETH",
                backing_value=Decimal("1"),
                backing_pct=Decimal("100"),
                price_usd=Decimal("2000"),
            ),
        ),
    )
    reader.batch_get_breakdowns.return_value = {1: unpriced_breakdown, 2: priced_breakdown}
    service = _service(_repo(positions, Decimal("1000")), _FakeRegistry([_crypto_lending_service(reader)]))

    with patch("app.services.prime_risk_capital_service.logger") as mock_logger:
        result = await service.compute(_PRIME)  # must not raise

    by_id = {a.receipt_token_id: a for a in result.per_allocation}
    assert by_id[1].applied is False
    assert by_id[1].required_risk_capital_usd is None
    assert by_id[1].unpriced_reason == "price_data_missing"
    assert by_id[2].applied is True
    # The unpriced allocation still counts toward exposure, but not modeled exposure.
    assert result.exposure_usd == Decimal("300")
    assert result.modeled_exposure_usd == Decimal("200")
    # The data gap is logged, not silently masked.
    mock_logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_prime_compute_degrades_adapter_data_missing_to_unpriced():
    """A Morpho VaultV2 whose adapter graph is not indexed degrades to unpriced.

    ``get_liquidation_params`` raises ``AdapterDataMissingError`` (a priced but
    liquidation-unresolvable v3 allocation). Rather than dropping every collateral
    item and reporting a confident ``rrc=0`` with applied=True, that one allocation
    must render as unpriced (``adapter_data_missing``) while the rest still prices.
    """
    from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution

    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="grove-bbqUSDC", amount_usd=Decimal("100")),
        make_receipt_token_position(receipt_token_id=2, symbol="aDAI", amount_usd=Decimal("200")),
    ]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    reader.get_receipt_token.side_effect = lambda aid: {1: _info(1, 777), 2: _info(2, 888)}[aid]
    reader.batch_get_shares.return_value = {1: Decimal("1"), 2: Decimal("1")}

    def _priced_breakdown(backed_asset_id: int) -> BackedBreakdown:
        return BackedBreakdown(
            backed_asset_id=backed_asset_id,
            items=(
                CollateralContribution(
                    token_id=99,
                    symbol="WETH",
                    backing_value=Decimal("100"),
                    backing_pct=Decimal("100"),
                    price_usd=Decimal("2000"),
                ),
            ),
        )

    reader.batch_get_breakdowns.return_value = {1: _priced_breakdown(1), 2: _priced_breakdown(2)}

    async def _liq(info, backed_asset_id, token_ids):  # noqa: ARG001
        if backed_asset_id == 1:
            raise AdapterDataMissingError("morpho VaultV2 id=1 has no active adapters indexed yet")
        return {99: LiquidationParams(99, Decimal("0.8"), Decimal("1.05"))}

    reader.get_liquidation_params.side_effect = _liq
    service = _service(_repo(positions, Decimal("1000")), _FakeRegistry([_crypto_lending_service(reader)]))

    with patch("app.services.prime_risk_capital_service.logger") as mock_logger:
        result = await service.compute(_PRIME)  # must not raise

    by_id = {a.receipt_token_id: a for a in result.per_allocation}
    assert by_id[1].applied is False
    assert by_id[1].required_risk_capital_usd is None
    assert by_id[1].unpriced_reason == "adapter_data_missing"
    assert by_id[2].applied is True
    # The unpriced allocation still counts toward exposure, but not modeled exposure.
    assert result.exposure_usd == Decimal("300")
    assert result.modeled_exposure_usd == Decimal("200")
    mock_logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_prime_compute_unaffected_for_non_crypto_lending_models():
    """The legacy ``model.compute`` path must remain unchanged for non-crypto-lending models.

    SURAF/CORE are not crypto-lending and will route through the unbatched
    dispatch. The ``isinstance`` check must not poison their flow.
    """
    positions = [make_receipt_token_position(receipt_token_id=1, symbol="X", amount_usd=Decimal("100"))]
    fake = _FakeModel("gap_sweep", {1}, rrc=Decimal("7"), crr=Decimal("1"))
    service = _service(_repo(positions, Decimal("100")), _FakeRegistry([fake]))

    result = await service.compute(_PRIME)

    assert fake.computed_ids == [1]
    assert result.required_risk_capital_usd == Decimal("7")
