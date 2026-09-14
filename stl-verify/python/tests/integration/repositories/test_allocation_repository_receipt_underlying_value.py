"""Integration tests for redeemable-value pricing of receipt-token positions.

Every position-valuation read (``list_receipt_token_positions``,
``get_usd_exposure``, ``get_total_usd_exposure``, ``list_exposure_buckets``)
values a receipt position at ``COALESCE(underlying_value, balance) x underlying
price``: ``underlying_value`` is the on-chain redeemable value (convertToAssets),
so non-1:1 vault shares (syrupUSDC-like) stop being priced as if one share were
one underlying unit. NULL ``underlying_value`` (rows written before the column
existed) falls back to the balance basis; 1:1 aTokens are unchanged because
their ``underlying_value`` equals ``balance`` by construction. A position whose
own ``underlying_token_id`` diverges from the registry's is refused a price
(NULL ``amount_usd``); that and the balance-basis fallback are surfaced via
telemetry.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_receipt_underlying_value_positions``.
"""

import asyncio
import datetime as dt
import logging
import re
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import (
    _EXPOSURE_BUCKETS_SQL,
    AllocationRepository,
)
from app.adapters.postgres.reference_as_of import ReferenceAsOf, utc_now
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    RUV_ATOKEN_BALANCE,
    RUV_CONTEST_BALANCE,
    RUV_CONTEST_HIGH_ORACLE_PRICE,
    RUV_CONTEST_LOW_ORACLE_STALE_PRICE,
    RUV_CONTEST_PROXY_HEX,
    RUV_CONTEST_WINNING_PRICE,
    RUV_DIVERGENT_BALANCE,
    RUV_LEGACY_BALANCE,
    RUV_LOCF_BASE_TS,
    RUV_LOCF_LATER_VALUE,
    RUV_LOCF_NEWER_VALUE,
    RUV_LOCF_PROXY_HEX,
    RUV_MORPHO_PROXY_HEX,
    RUV_MORPHO_SHARE_BALANCE,
    RUV_MORPHO_UNDERLYING_PRICE,
    RUV_MORPHO_UNDERLYING_VALUE,
    RUV_PROXY_HEX,
    RUV_SYRUP_LIKE_BALANCE,
    RUV_SYRUP_LIKE_UNDERLYING_VALUE,
    RUV_UNDERLYING_PRICE,
    RUV_ZERO_REDEEMED_BALANCE,
    RUV_ZERO_REDEEMED_UNDERLYING_VALUE,
    seed_receipt_underlying_value_positions,
)

_PRIME = EthAddress(f"0x{RUV_PROXY_HEX}")

_REPO_LOGGER = "app.adapters.postgres.allocation_position_repository"

# The divergent position is refused a price and contributes nothing; the
# fully-redeemed position contributes exactly 0, not its share balance.
_EXPECTED_TOTAL = (
    RUV_SYRUP_LIKE_UNDERLYING_VALUE + RUV_LEGACY_BALANCE + RUV_ATOKEN_BALANCE + RUV_ZERO_REDEEMED_UNDERLYING_VALUE
) * RUV_UNDERLYING_PRICE

# One row per valuation basis: (symbol, expected share balance, expected USD).
_VALUATION_CASES = [
    # Non-1:1 share ratio: redeemable value drives the USD figure, balance stays shares.
    ("syrupLike", RUV_SYRUP_LIKE_BALANCE, RUV_SYRUP_LIKE_UNDERLYING_VALUE * RUV_UNDERLYING_PRICE),
    # NULL underlying_value (legacy row): balance-based fallback.
    ("legacyReceipt", RUV_LEGACY_BALANCE, RUV_LEGACY_BALANCE * RUV_UNDERLYING_PRICE),
    # 1:1 aToken (underlying_value == balance): value unchanged.
    ("aOneToOne", RUV_ATOKEN_BALANCE, RUV_ATOKEN_BALANCE * RUV_UNDERLYING_PRICE),
    # Fully-redeemed vault (underlying_value = 0, balance > 0): worth exactly 0.
    # COALESCE must fall back on NULL only, never on 0 (guards an ``or``-style refactor).
    ("fullyRedeemed", RUV_ZERO_REDEEMED_BALANCE, RUV_ZERO_REDEEMED_UNDERLYING_VALUE * RUV_UNDERLYING_PRICE),
]


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the receipt redeemable-value scenarios and yield the async URL."""
    asyncio.run(seed_receipt_underlying_value_positions(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct-method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine, utc_now)
    finally:
        await engine.dispose()


@pytest_asyncio.fixture()
async def conn(async_db_url: str):
    """A connection for running the cache and history queries side by side."""
    engine = create_async_engine(async_db_url)
    try:
        async with engine.connect() as connection:
            yield connection
    finally:
        await engine.dispose()


async def _fetch_receipt_token_ids(db_url: str) -> dict[str, int]:
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("SELECT id, symbol FROM receipt_token WHERE chain_id = 1")
        return {row["symbol"]: row["id"] for row in rows}
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def receipt_token_ids(async_db_url: str, db_url: str) -> dict[str, int]:
    """Map seeded receipt-token symbols to their receipt_token ids.

    ``async_db_url`` is requested only for its seeding side effect; the read
    itself goes through the plain ``db_url`` from ``conftest``.
    """
    return asyncio.run(_fetch_receipt_token_ids(db_url))


async def _position(repo: AllocationRepository, symbol: str, prime: EthAddress = _PRIME):
    positions = await repo.list_receipt_token_positions(prime)
    return {p.symbol: p for p in positions}.get(symbol)


@pytest.mark.parametrize(("symbol", "expected_balance", "expected_amount_usd"), _VALUATION_CASES)
@pytest.mark.asyncio
async def test_receipt_position_amount_usd_uses_redeemable_value(
    repo, symbol, expected_balance, expected_amount_usd
) -> None:
    """list_receipt_token_positions values each basis correctly and keeps balance as shares."""
    position = await _position(repo, symbol)
    assert position is not None
    assert position.balance == expected_balance
    assert position.amount_usd == expected_amount_usd


@pytest.mark.parametrize(("symbol", "_expected_balance", "expected_amount_usd"), _VALUATION_CASES)
@pytest.mark.asyncio
async def test_usd_exposure_uses_redeemable_value(
    repo, receipt_token_ids, symbol, _expected_balance, expected_amount_usd
) -> None:
    """get_usd_exposure applies the same redeemable-value basis per receipt token."""
    exposure = await repo.get_usd_exposure(receipt_token_ids[symbol], _PRIME)
    assert exposure == expected_amount_usd


@pytest.mark.asyncio
async def test_total_usd_exposure_sums_redeemable_values(repo) -> None:
    """get_total_usd_exposure sums the redeemable-value USD figure across positions."""
    total = await repo.get_total_usd_exposure(_PRIME)
    assert total == _EXPECTED_TOTAL


@pytest.mark.asyncio
async def test_exposure_buckets_value_positions_at_redeemable_value(repo) -> None:
    """list_exposure_buckets carries the redeemable value forward, not the share balance."""
    now = dt.datetime.now(dt.UTC)
    buckets = await repo.list_exposure_buckets(
        [_PRIME],
        from_timestamp=now - dt.timedelta(hours=1),
        to_timestamp=now + dt.timedelta(hours=1),
        bucket_seconds=3600.0,
        limit=10,
    )
    non_null = [b.exposure_usd for b in buckets if b.exposure_usd is not None]
    assert non_null, "expected at least one priced bucket in the seeded window"
    assert all(value == _EXPECTED_TOTAL for value in non_null)


@pytest.mark.asyncio
async def test_divergent_underlying_position_is_refused_a_price(repo) -> None:
    """A position whose own underlying_token_id diverges from the registry's gets NULL amount_usd.

    Pricing joins ``receipt_token.underlying_token_id`` (the curated registry).
    Verified against the production warehouse on 2026-07-09 (5484 receipt rows
    carrying an ``underlying_token_id``, 0 divergent), so a divergence indicates
    an ingest bug: the registry price would multiply a position value
    denominated in a different unit, so the read refuses to price it (surfaced
    as unpriced via telemetry) rather than returning a plausible wrong number.
    """
    position = await _position(repo, "divergentReceipt")
    assert position is not None
    assert position.balance == RUV_DIVERGENT_BALANCE
    assert position.amount_usd is None


@pytest.mark.asyncio
async def test_usd_exposure_raises_for_divergent_underlying(repo, receipt_token_ids) -> None:
    """get_usd_exposure refuses to price a divergent position instead of returning a wrong figure."""
    with pytest.raises(ValueError, match="diverges"):
        await repo.get_usd_exposure(receipt_token_ids["divergentReceipt"], _PRIME)


@pytest.mark.asyncio
async def test_morpho_vault_receipt_like_spark_usdc_bc_priced_by_redeemable_value(repo) -> None:
    """A Morpho-vault share registered as a receipt token prices at underlying_value x underlying price.

    sparkUSDCbc semantics: the registry migration registers sparkUSDCbc as a
    ``receipt_token`` (Morpho Blue, USDC underlying), so it is priced by the
    receipt path at its convertToAssets-derived redeemable value (a non-1:1
    share ratio) rather than by a direct-holdings allowlist. The whole binding
    (protocol, oracle, protocol_oracle, registry row, underlying price) is
    seeded by this module, independent of migration-seeded registry rows.
    """
    position = await _position(repo, "sparkUSDCbcLike", EthAddress(f"0x{RUV_MORPHO_PROXY_HEX}"))
    assert position is not None
    assert position.balance == RUV_MORPHO_SHARE_BALANCE
    assert position.amount_usd == RUV_MORPHO_UNDERLYING_VALUE * RUV_MORPHO_UNDERLYING_PRICE


# ---------------------------------------------------------------------------
# Exposure-bucket LOCF behavior, observed mid-series via explicit created_at
# timestamps: two observations inside the first hourly bucket, an empty bucket,
# then a third observation two buckets in.
# ---------------------------------------------------------------------------


async def _exposure_by_bucket(repo: AllocationRepository, proxy_hex: str) -> dict[dt.datetime, Decimal | None]:
    buckets = await repo.list_exposure_buckets(
        [EthAddress(f"0x{proxy_hex}")],
        from_timestamp=RUV_LOCF_BASE_TS,
        to_timestamp=RUV_LOCF_BASE_TS + dt.timedelta(hours=3),
        bucket_seconds=3600.0,
        limit=10,
    )
    return {b.bucket_start: b.exposure_usd for b in buckets}


async def _locf_exposure_by_bucket(repo: AllocationRepository) -> dict[dt.datetime, Decimal | None]:
    return await _exposure_by_bucket(repo, RUV_LOCF_PROXY_HEX)


@pytest.mark.asyncio
async def test_exposure_bucket_takes_newest_position_value_within_bucket(repo) -> None:
    """With two observations in one bucket, last() picks the newer redeemable value, not the backdated one."""
    by_start = await _locf_exposure_by_bucket(repo)
    assert by_start[RUV_LOCF_BASE_TS] == RUV_LOCF_NEWER_VALUE * RUV_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_exposure_bucket_carries_value_until_next_observation(repo) -> None:
    """LOCF carries the last redeemable value through an empty bucket, then steps at the next observation."""
    by_start = await _locf_exposure_by_bucket(repo)
    assert by_start[RUV_LOCF_BASE_TS + dt.timedelta(hours=1)] == RUV_LOCF_NEWER_VALUE * RUV_UNDERLYING_PRICE
    assert by_start[RUV_LOCF_BASE_TS + dt.timedelta(hours=2)] == RUV_LOCF_LATER_VALUE * RUV_UNDERLYING_PRICE


# ---------------------------------------------------------------------------
# Valuation-gap telemetry: the receipt path mirrors the direct-holdings
# unpriced signal: a NULL amount_usd and a balance-basis fallback each get a
# warning so coverage regressions don't have to be discovered by a user.
# ---------------------------------------------------------------------------


class _CapturingHandler(logging.Handler):
    """Collects emitted records; attached directly to the target logger so
    capture does not depend on propagation (the app disables it), which would
    otherwise make this test order-dependent."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def _warning_records(repo: AllocationRepository) -> list[logging.LogRecord]:
    """Return the warnings emitted by a list_receipt_token_positions read of the main prime."""
    handler = _CapturingHandler()
    target = logging.getLogger(_REPO_LOGGER)
    previous_level = target.level
    target.addHandler(handler)
    target.setLevel(logging.WARNING)
    try:
        await repo.list_receipt_token_positions(_PRIME)
    finally:
        target.removeHandler(handler)
        target.setLevel(previous_level)
    return handler.records


@pytest.mark.asyncio
async def test_unpriced_receipt_position_is_surfaced(repo) -> None:
    """A receipt position resolving to no USD value is warned about, not silently unpriced."""
    records = await _warning_records(repo)
    flagged = [r for r in records if "divergentReceipt" in getattr(r, "unpriced_symbols", [])]
    assert flagged, "expected a warning flagging the receipt position that resolved to no USD value"


@pytest.mark.asyncio
async def test_balance_basis_receipt_position_is_surfaced(repo) -> None:
    """A receipt position valued on the share-balance fallback is warned about (methodology signal)."""
    records = await _warning_records(repo)
    flagged = [r for r in records if "legacyReceipt" in getattr(r, "balance_basis_symbols", [])]
    assert flagged, "expected a warning flagging the receipt position valued on the balance fallback"


# ---------------------------------------------------------------------------
# VEC-712: the bucketed read resolves its latest prices from the
# trigger-maintained ``token_price_current`` cache instead of LATERAL-ing into
# the ``onchain_token_price`` hypertable behind it. The two must agree row for
# row — the cache holds the newest row per (oracle, token) under the same
# newer-wins comparison, so all that changed is which relation is scanned.
# The columns are identically named, so the pre-swap query is the live one with
# the relation substituted back; the guards below fail loudly if a later edit
# makes that substitution a no-op, which would leave this test comparing a
# query against itself.
# ---------------------------------------------------------------------------

# Matched by pattern rather than by literal: alias-agnostic, so a second cache
# read added later cannot survive the substitution under a different alias, and
# anchored to the keyword that introduces a relation, so prose naming the table
# is not mistaken for a read of it. JOIN is covered as well as FROM — a cache
# read reached by a join, not a lateral, is the form a literal FROM match misses.
_CACHE_RELATION = re.compile(r"\b(FROM|JOIN)\s+token_price_current\b")

_PRE_SWAP_EXPOSURE_BUCKETS_SQL = text(_CACHE_RELATION.sub(r"\1 onchain_token_price", str(_EXPOSURE_BUCKETS_SQL)))


@pytest.mark.parametrize("proxy_hex", [RUV_LOCF_PROXY_HEX, RUV_CONTEST_PROXY_HEX])
@pytest.mark.asyncio
async def test_exposure_buckets_match_the_pre_swap_history_read(repo, conn, proxy_hex: str) -> None:
    """Reading token_price_current yields the same buckets as LATERAL-ing into the history."""
    assert _CACHE_RELATION.search(str(_EXPOSURE_BUCKETS_SQL)), (
        "the bucketed read no longer resolves prices from the cache (VEC-712)"
    )
    assert not _CACHE_RELATION.search(str(_PRE_SWAP_EXPOSURE_BUCKETS_SQL)), (
        "the pre-swap substitution missed a cache read; this test would compare the query against itself"
    )

    params = {
        "proxy_addrs": [EthAddress(f"0x{proxy_hex}").to_bytes()],
        "from_timestamp": RUV_LOCF_BASE_TS,
        "to_timestamp": RUV_LOCF_BASE_TS + dt.timedelta(hours=3),
        "bucket_seconds": 3600.0,
        "limit": 10,
    }
    reference = ReferenceAsOf(utc_now)

    before = (await conn.execute(_PRE_SWAP_EXPOSURE_BUCKETS_SQL, reference.params(**params))).fetchall()

    after = await _exposure_by_bucket(repo, proxy_hex)

    assert any(row.exposure_usd for row in before), (
        "expected a priced bucket; two unpriced reads would both COALESCE to 0 and match vacuously"
    )
    assert {row.bucket_start: row.exposure_usd for row in before} == after


@pytest.mark.asyncio
async def test_exposure_bucket_prices_at_the_newest_revision_of_the_winning_oracle(repo) -> None:
    """With two enabled oracles and a revised price, the bucket carries the newest revision.

    Equality against the pre-swap query cannot show this on its own: a fixture
    with one eligible candidate agrees either way. Here the low-id oracle's
    revision (block 2100) must beat both its own superseded row (2000) and the
    high-id oracle's row (2050) — so a cache that failed to retain the revision,
    or an outer sort that ranked ``oracle_id`` above recency, lands on a
    different, asserted-against price.
    """
    priced = [v for v in (await _exposure_by_bucket(repo, RUV_CONTEST_PROXY_HEX)).values() if v is not None]

    assert priced, "expected at least one priced bucket for the contested-price position"
    assert all(v == RUV_CONTEST_BALANCE * RUV_CONTEST_WINNING_PRICE for v in priced)
    assert RUV_CONTEST_WINNING_PRICE not in (RUV_CONTEST_LOW_ORACLE_STALE_PRICE, RUV_CONTEST_HIGH_ORACLE_PRICE), (
        "the losing prices must differ from the winner, or this test cannot fail"
    )
