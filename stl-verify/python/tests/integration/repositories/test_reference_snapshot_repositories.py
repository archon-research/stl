"""The stored reference readers against a real TimescaleDB.

The risk these cover is entirely in the SQL — the latest-cycle selection across
processing versions, the ``decode``-guarded registry join, and the coverage row
that separates "holds nothing" from "never reported on". None of it is exercised
by mocking a connection.

Both readers share a module so they share one migrated database: they read the
same coverage table and the same registry, and a per-reader module would apply
the migration set twice to assert the same seeds.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.reference_position_repository import ReferencePositionRepository
from app.adapters.postgres.reference_risk_capital_repository import ReferenceRiskCapitalRepository
from tests.integration.seed import insert_receipt_token_row, insert_token

_STAR = "spark"
_CYCLE = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)
_EARLIER_CYCLE = _CYCLE - timedelta(minutes=15)
# An address the test registers as a receipt token, so the join has something to
# hit; mixed case because upstream does not promise one and hex decodes either way.
_INDEXED_TOKEN = "0xCDcdCDcdCDcdCDcdCDcdCDcdCDcdCDcdCDcdCDcd"
_UNINDEXED_TOKEN = "0x" + "ab" * 20
# Uniswap V4 identifies a position by 32-byte pool id, which `decode` would
# accept as 32 bytes and the registry can never match — and a value that is not
# hex at all, which `decode` raises on.
_V4_POOL_ID = "0x" + "ef" * 32
_NOT_AN_ADDRESS = "uniswap-v4-position"
# The receipt token's underlying, inserted with a known symbol rather than
# picked with `LIMIT 1`: `token.symbol` is nullable, and an arbitrary
# chain_id=1 row could carry one.
_UNDERLYING_TOKEN = "0x" + "11" * 20
_UNDERLYING_SYMBOL = "USDT"


async def _insert_stack(conn: asyncpg.Connection, prime_id: int, synced_at: datetime, **overrides) -> None:
    figures = {
        "exposure_usd": Decimal("2098090654.81"),
        "required_risk_capital_usd": Decimal("17837860.43"),
        "total_risk_capital_usd": Decimal("48142491.08"),
        "encumbrance_ratio": Decimal("0.3705"),
        "build_id": 1,
        **overrides,
    }
    await conn.execute(
        """
        INSERT INTO prime_capital_stack (
            prime_id, synced_at, exposure_usd, required_risk_capital_usd, total_risk_capital_usd,
            junior_risk_capital_usd, senior_risk_capital_usd,
            internal_junior_risk_capital_usd, external_junior_risk_capital_usd,
            tokenized_junior_risk_capital_usd,
            internal_senior_risk_capital_usd, external_senior_risk_capital_usd,
            encumbrance_ratio, exposure_share, epi_utilization, spj_utilization, source, build_id
        ) VALUES ($1, $2, $3, $4, $5, '1', '0', '1', '0', '0', '0', '0', $6, '0.008', '0', '0',
                  'skyeco:star-monitoring:risk-capital', $7)
        """,
        prime_id,
        synced_at,
        figures["exposure_usd"],
        figures["required_risk_capital_usd"],
        figures["total_risk_capital_usd"],
        figures["encumbrance_ratio"],
        figures["build_id"],
    )


async def _insert_allocation(
    conn: asyncpg.Connection,
    prime_id: int,
    synced_at: datetime,
    *,
    token_address: str,
    chain_id: int | None = 1,
    network: str = "ethereum",
    exposure: str = "344187505.66",
    crr: str = "0.0028764051",
    build_id: int = 1,
) -> None:
    await conn.execute(
        """
        INSERT INTO prime_capital_stack_allocation (
            prime_id, synced_at, network, chain_id, protocol_name, symbol, name, token_address,
            loan_token_address, loan_token_symbol, exposure_usd, required_risk_capital_usd, crr,
            source, build_id
        ) VALUES ($1, $2, $3, $4, 'sparklend', 'spUSDT', 'Spark USDT', $5,
                  NULL, NULL, $6, '990048.94', $7, 'skyeco:star-monitoring:allocations', $8)
        """,
        prime_id,
        synced_at,
        network,
        chain_id,
        token_address,
        Decimal(exposure),
        Decimal(crr),
        build_id,
    )


async def _insert_position(
    conn: asyncpg.Connection,
    prime_id: int,
    synced_at: datetime,
    *,
    token_address: str,
    chain_id: int | None = 1,
    network: str = "ethereum",
    assets: str = "787379142.91",
    build_id: int = 1,
    wallet_address: str = "0x1111111111111111111111111111111111111111",
) -> None:
    await conn.execute(
        """
        INSERT INTO prime_reference_position (
            prime_id, synced_at, network, chain_id, protocol_name, token_symbol, token_name,
            token_address, wallet_address, assets_usd, allocated_assets_usd, idle_assets_usd, source, build_id
        ) VALUES ($1, $2, $3, $4, 'sparklend', 'spUSDS', 'Spark USDS', $5, $6, $7, NULL, NULL,
                  'skyeco:internal:allocations', $8)
        """,
        prime_id,
        synced_at,
        network,
        chain_id,
        token_address,
        wallet_address,
        Decimal(assets),
        build_id,
    )


@pytest_asyncio.fixture(loop_scope="module")
async def seeded(db_url: str):
    """A prime with a registered receipt token and no reference rows."""
    conn = await asyncpg.connect(db_url)
    try:
        prime_id = cast(int, await conn.fetchval("SELECT id FROM prime WHERE name = $1", _STAR))
        protocol_id = cast(int, await conn.fetchval("SELECT id FROM protocol WHERE chain_id = 1 LIMIT 1"))
        underlying_id = await insert_token(conn, _UNDERLYING_SYMBOL, 6, bytes.fromhex(_UNDERLYING_TOKEN[2:]))
        receipt_token_id = await conn.fetchval(
            "SELECT id FROM receipt_token WHERE chain_id = 1 AND receipt_token_address = $1",
            bytes.fromhex(_INDEXED_TOKEN[2:]),
        )
        if receipt_token_id is None:
            await insert_receipt_token_row(
                conn,
                protocol_id=protocol_id,
                underlying_token_id=underlying_id,
                address=bytes.fromhex(_INDEXED_TOKEN[2:]),
                symbol="spUSDT",
            )
            receipt_token_id = cast(
                int,
                await conn.fetchval(
                    "SELECT id FROM receipt_token WHERE chain_id = 1 AND receipt_token_address = $1",
                    bytes.fromhex(_INDEXED_TOKEN[2:]),
                ),
            )

        await _clear(conn, prime_id)
        yield conn, prime_id, receipt_token_id
        await _clear(conn, prime_id)
    finally:
        await conn.close()


async def _clear(conn: asyncpg.Connection, prime_id: int) -> None:
    for table in ("prime_capital_stack_allocation", "prime_reference_position", "prime_capital_stack"):
        await conn.execute(f"DELETE FROM {table} WHERE prime_id = $1", prime_id)


async def _risk_capital(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        return await ReferenceRiskCapitalRepository(engine).get_prime(_STAR)
    finally:
        await engine.dispose()


async def _covered(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        return await ReferenceRiskCapitalRepository(engine).covered_stars()
    finally:
        await engine.dispose()


async def _positions(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        return await ReferencePositionRepository(engine).get_positions(_STAR)
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_a_prime_with_no_cycle_has_no_risk_capital_snapshot(seeded, async_db_url: str):
    assert await _risk_capital(async_db_url) is None


@pytest.mark.asyncio(loop_scope="module")
async def test_a_prime_with_no_cycle_has_no_positions_snapshot(seeded, async_db_url: str):
    assert await _positions(async_db_url) is None


@pytest.mark.asyncio(loop_scope="module")
async def test_covered_stars_lists_a_prime_only_once_a_cycle_has_landed(seeded, async_db_url: str):
    conn, prime_id, _ = seeded

    assert _STAR not in await _covered(async_db_url)

    await _insert_stack(conn, prime_id, _CYCLE)

    assert _STAR in await _covered(async_db_url)


@pytest.mark.asyncio(loop_scope="module")
async def test_a_covered_prime_whose_positions_never_landed_has_no_snapshot(seeded, async_db_url: str):
    # The window this PR's readers had to get right: prime_capital_stack
    # predates the positions table, so coverage rows exist for cycles that have
    # no positions. Serving those as an empty list would publish "Sky reports
    # this prime holds nothing".
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)

    assert await _positions(async_db_url) is None


@pytest.mark.asyncio(loop_scope="module")
async def test_positions_without_a_coverage_row_are_not_served(seeded, async_db_url: str):
    # Coverage is the risk-capital table's answer for both endpoints, so the
    # allocation list cannot claim a prime the risk-capital card calls uncovered.
    conn, prime_id, _ = seeded
    await _insert_position(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN)

    assert await _positions(async_db_url) is None


@pytest.mark.asyncio(loop_scope="module")
async def test_a_cycle_reporting_exposure_with_no_breakdown_is_skipped(seeded, async_db_url: str):
    # Same window on the risk-capital side: the totals are readable and real,
    # but with no other cycle to fall back to this reads as "never reported on"
    # rather than a permanent 500 for a prime the monitor has stopped covering.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE, exposure_usd=Decimal("2098090654.81"))

    assert await _risk_capital(async_db_url) is None


@pytest.mark.asyncio(loop_scope="module")
async def test_an_incomplete_newest_cycle_falls_back_to_the_last_complete_one(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _EARLIER_CYCLE, exposure_usd=Decimal("1"))
    await _insert_allocation(conn, prime_id, _EARLIER_CYCLE, token_address=_UNINDEXED_TOKEN, exposure="1")
    await _insert_stack(conn, prime_id, _CYCLE, exposure_usd=Decimal("2098090654.81"))

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert snapshot.synced_at == _EARLIER_CYCLE
    assert snapshot.exposure_usd == Decimal("1")


@pytest.mark.asyncio(loop_scope="module")
async def test_a_cycle_with_no_exposure_and_no_breakdown_is_served(seeded, async_db_url: str):
    # The permitted half: at zero exposure no row is owed, and the indexer
    # writes exactly this for a prime the monitor covers but prices nothing for.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE, exposure_usd=Decimal("0"))

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert snapshot.per_allocation == ()


@pytest.mark.parametrize("queried", ["SPARK", "Spark"])
@pytest.mark.asyncio(loop_scope="module")
async def test_a_star_resolves_whatever_case_it_arrives_in(seeded, async_db_url: str, queried: str):
    # The registry does not promise a case; `prime.name` does. Without the
    # fold, every reference request for a capitalised star 404s while its rows
    # sit in the table.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN)

    engine = create_async_engine(async_db_url)
    try:
        assert await ReferenceRiskCapitalRepository(engine).get_prime(queried) is not None
        assert await ReferencePositionRepository(engine).get_positions(queried) is not None
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_a_star_that_names_no_prime_reads_as_uncovered(seeded, async_db_url: str):
    # The `target` CTE matches nothing, so `prime_id = NULL` is never true.
    # Pinning this stops a broadened predicate silently picking some other row.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)

    engine = create_async_engine(async_db_url)
    try:
        assert await ReferenceRiskCapitalRepository(engine).get_prime("zzznotastar") is None
        assert await ReferencePositionRepository(engine).get_positions("zzznotastar") is None
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Latest-cycle selection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_risk_capital_prefers_a_correction_over_the_original_it_supersedes(seeded, async_db_url: str):
    # Two build_ids at one synced_at means processing_version 0 and 1; ordering
    # by build_id would pick an arbitrary one.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE, total_risk_capital_usd=Decimal("1"), build_id=1)
    await _insert_stack(conn, prime_id, _CYCLE, total_risk_capital_usd=Decimal("999"), build_id=2)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN)

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert snapshot.total_risk_capital_usd == Decimal("999")


@pytest.mark.asyncio(loop_scope="module")
async def test_risk_capital_serves_the_newest_cycle_not_an_earlier_one(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _EARLIER_CYCLE, exposure_usd=Decimal("1"))
    await _insert_stack(conn, prime_id, _CYCLE, exposure_usd=Decimal("2"))
    await _insert_allocation(conn, prime_id, _EARLIER_CYCLE, token_address=_UNINDEXED_TOKEN)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN)

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert snapshot.synced_at == _CYCLE
    assert snapshot.exposure_usd == Decimal("2")


@pytest.mark.asyncio(loop_scope="module")
async def test_the_breakdown_comes_from_the_totals_own_cycle(seeded, async_db_url: str):
    # Pairing one cycle's totals with another's rows would publish a breakdown
    # that does not decompose the published total.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _EARLIER_CYCLE)
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_allocation(conn, prime_id, _EARLIER_CYCLE, token_address=_UNINDEXED_TOKEN, exposure="1")
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, exposure="2")

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert [row.exposure_usd for row in snapshot.per_allocation] == [Decimal("2")]


@pytest.mark.asyncio(loop_scope="module")
async def test_the_breakdown_prefers_a_correction_over_the_original_it_supersedes(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, exposure="1", build_id=1)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, exposure="999", build_id=2)

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert [row.exposure_usd for row in snapshot.per_allocation] == [Decimal("999")]


@pytest.mark.asyncio(loop_scope="module")
async def test_positions_serve_the_newest_cycle_only(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _EARLIER_CYCLE, token_address=_UNINDEXED_TOKEN, assets="1")
    await _insert_position(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, assets="2")

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert snapshot.synced_at == _CYCLE
    assert [row.assets_usd for row in snapshot.positions] == [Decimal("2")]


@pytest.mark.asyncio(loop_scope="module")
async def test_positions_serve_both_wallets_for_the_same_token(seeded, async_db_url: str):
    # Grove's positions feed legitimately reports the same (network,
    # token_address) under two proxy wallets, with materially different
    # balances on each (verified live: ~$1.02M vs ~$29.0M on the same Uni V3
    # LP pair). wallet_address is part of row identity, so both must serve
    # rather than one silently overwriting the other.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(
        conn,
        prime_id,
        _CYCLE,
        token_address=_UNINDEXED_TOKEN,
        assets="1020000",
        wallet_address="0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
    )
    await _insert_position(
        conn,
        prime_id,
        _CYCLE,
        token_address=_UNINDEXED_TOKEN,
        assets="29000000",
        wallet_address="0x000000005ce4e5e4e5e4e5e4e5e4e5e4e5e4e5e4",
    )

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert sorted(row.assets_usd for row in snapshot.positions) == [Decimal("1020000"), Decimal("29000000")]


@pytest.mark.asyncio(loop_scope="module")
async def test_positions_prefer_a_correction_over_the_original_it_supersedes(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, assets="1", build_id=1)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, assets="999", build_id=2)

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert [row.assets_usd for row in snapshot.positions] == [Decimal("999")]


# ---------------------------------------------------------------------------
# Ordering and scale
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_the_breakdown_is_ordered_largest_exposure_first(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, exposure="1")
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_INDEXED_TOKEN, exposure="900")

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert [row.exposure_usd for row in snapshot.per_allocation] == [Decimal("900"), Decimal("1")]


@pytest.mark.asyncio(loop_scope="module")
async def test_positions_are_ordered_largest_holding_first(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, assets="1")
    await _insert_position(conn, prime_id, _CYCLE, token_address=_INDEXED_TOKEN, assets="900")

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert [row.assets_usd for row in snapshot.positions] == [Decimal("900"), Decimal("1")]


@pytest.mark.asyncio(loop_scope="module")
async def test_the_stored_fraction_is_served_as_a_percentage(seeded, async_db_url: str):
    # The column is upstream's own 0-1 crr and every consumer reads 0-100, so a
    # missed rescale publishes a ratio 100x too small.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN, crr="0.0028764051")

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert snapshot.per_allocation[0].crr_pct == Decimal("0.28764051")


# ---------------------------------------------------------------------------
# The registry join
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_the_breakdown_resolves_a_token_stl_indexes(seeded, async_db_url: str):
    conn, prime_id, receipt_token_id = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_INDEXED_TOKEN)

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert snapshot.per_allocation[0].receipt_token_id == receipt_token_id


@pytest.mark.asyncio(loop_scope="module")
async def test_positions_resolve_a_token_stl_indexes(seeded, async_db_url: str):
    conn, prime_id, receipt_token_id = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_INDEXED_TOKEN)

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert snapshot.positions[0].receipt_token_id == receipt_token_id


@pytest.mark.asyncio(loop_scope="module")
async def test_positions_resolve_the_registrys_underlying_when_the_receipt_token_matches(
    seeded, async_db_url: str
) -> None:
    # This feed never names an underlying itself; a resolved receipt token
    # contributes one from STL's registry, via the same join that resolves
    # `receipt_token_id`.
    conn, prime_id, receipt_token_id = seeded
    underlying = await conn.fetchrow(
        "SELECT rt.underlying_token_id, t.symbol, encode(t.address, 'hex') AS address "
        "FROM receipt_token rt JOIN token t ON t.id = rt.underlying_token_id WHERE rt.id = $1",
        receipt_token_id,
    )
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_INDEXED_TOKEN)

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    (position,) = snapshot.positions
    assert position.underlying_token_id == underlying["underlying_token_id"]
    assert position.underlying_token_address == "0x" + underlying["address"]
    assert position.underlying_symbol == underlying["symbol"]


@pytest.mark.asyncio(loop_scope="module")
async def test_positions_leave_the_underlying_unresolved_when_the_registry_join_misses(
    seeded, async_db_url: str
) -> None:
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN)

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    (position,) = snapshot.positions
    assert position.underlying_token_id is None
    assert position.underlying_token_address is None
    assert position.underlying_symbol == ""


@pytest.mark.asyncio(loop_scope="module")
async def test_the_join_misses_without_dropping_the_row(seeded, async_db_url: str):
    # Most of the balance sheet is positions STL has no registry entry for.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_UNINDEXED_TOKEN)

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert len(snapshot.positions) == 1
    assert snapshot.positions[0].receipt_token_id is None


@pytest.mark.asyncio(loop_scope="module")
async def test_the_join_misses_on_the_right_chain(seeded, async_db_url: str):
    # The registry is keyed on (chain_id, address); joining on the address alone
    # would attach a mainnet id to a position held on another chain.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_INDEXED_TOKEN, chain_id=8453, network="base")

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert snapshot.positions[0].receipt_token_id is None


@pytest.mark.asyncio(loop_scope="module")
async def test_a_position_on_an_unmapped_network_is_served_without_a_chain(seeded, async_db_url: str):
    # Upstream adds chains before STL has an id for them, and the join has no
    # chain to try; failing the row would cost the prime everything else it holds.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=_INDEXED_TOKEN, chain_id=None, network="plume")

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert snapshot.positions[0].chain_id is None
    assert snapshot.positions[0].chain is None
    assert snapshot.positions[0].network == "plume"


@pytest.mark.parametrize("token_address", [_V4_POOL_ID, _NOT_AN_ADDRESS])
@pytest.mark.asyncio(loop_scope="module")
async def test_a_position_identifier_that_is_not_an_address_is_served_unresolved(
    seeded, async_db_url: str, token_address: str
):
    # `decode` raises on a value that is not hex, so the join key is guarded
    # rather than attempted — and a pool id is hex but can never match.
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_position(conn, prime_id, _CYCLE, token_address=token_address)

    snapshot = await _positions(async_db_url)

    assert snapshot is not None
    assert snapshot.positions[0].token_address == token_address
    assert snapshot.positions[0].receipt_token_id is None


@pytest.mark.asyncio(loop_scope="module")
async def test_the_breakdown_serves_a_pool_id_unresolved(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_stack(conn, prime_id, _CYCLE)
    await _insert_allocation(conn, prime_id, _CYCLE, token_address=_V4_POOL_ID)

    snapshot = await _risk_capital(async_db_url)

    assert snapshot is not None
    assert snapshot.per_allocation[0].receipt_token_id is None
