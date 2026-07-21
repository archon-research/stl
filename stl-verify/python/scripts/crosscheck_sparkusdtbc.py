"""Cross-check our Morpho VaultV2 backed breakdown for sparkUSDTbc against data.spark.fi.

Run:

    uv run python scripts/crosscheck_sparkusdtbc.py            # uses $DATABASE_URL
    uv run python scripts/crosscheck_sparkusdtbc.py --database-url postgresql://... \
        --wallet 0x1601843c5e9bc251a3272907010afa41fa18347e --tolerance-blocks 2

What it does:
  1. GET the wallet's position in the sparkUSDTbc VaultV2 from data.spark.fi.
  2. Read our side: the wallet's latest ``morpho_vault_position`` (redeemable
     assets) and the vault's VaultV2 backed breakdown via
     ``MorphoV2BackedBreakdownRepository``, scaled to the wallet's share of the
     vault. Both sides are normalised to a ``Snapshot`` (a redeemable-asset
     balance + per-underlying amounts, all ``Decimal``).
  3. Compare the balance and every per-asset amount within a relative tolerance,
     print a table, and exit 0 (pass) or 1 (mismatch). A run that could not
     execute (network/DB error, wallet not indexed) exits 2.

±2-block skew tolerance
  Our indexer's latest snapshot and Spark's reading are almost never at the exact
  same block, and a Morpho vault's share price and each market's utilisation drift
  a little every block as interest accrues. ``--tolerance-blocks`` (default 2)
  bounds that skew: it is converted to a relative tolerance of
  ``tolerance_blocks × 0.1%`` per field (``_REL_DRIFT_PER_BLOCK``), i.e. 0.2% by
  default. A field passes when ``|spark − ours| ≤ tolerance × max(|spark|, |ours|)``.

Response schema (IMPORTANT)
  ``data.spark.fi`` is a single-page app whose backend is BlockAnalitica. From the
  build sandbox the SPA host returns only an HTML shell and the BlockAnalitica
  asset endpoint returned 404 for this vault/wallet, so the exact JSON payload
  could not be pinned live. ``parse_spark_response`` therefore parses the schema
  DERIVED FROM THE TICKET — a top-level ``balance`` plus an ``assets`` array of
  ``{symbol, amount}`` underlying rows (see the recorded sample in
  ``tests/unit/scripts/fixtures/spark_sparkusdtbc_response.json``). The parser is
  deliberately lenient about field names; adjust it once a live payload is seen.

The pure comparison logic (``parse_spark_response`` / ``compare_snapshots`` /
``rel_tolerance_from_blocks``) is unit-tested with injected fixtures — no network,
no DB. The live end-to-end pass against production data is expected to run only
AFTER VEC-218 (the VaultV2 adapter indexer + schema) has deployed and backfilled,
so until then this script is validated by its unit tests, not a live green run.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

import httpx
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.adapters.postgres.backed_breakdown_repository_morpho_v2 import MorphoV2BackedBreakdownRepository

_SPARK_BASE_URL = "https://data.spark.fi"
_SPARKUSDTBC_VAULT = "0xc7cdcfdefc64631ed6799c95e3b110cd42f2bd22"
_DEFAULT_WALLET = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_DEFAULT_NETWORK = "ethereum"
_DEFAULT_CHAIN_ID = 1

# Per-block relative drift budget (see the module docstring). A Morpho vault's
# share price and market utilisation move only marginally per block, so ~0.1% of
# divergence per block of snapshot skew is generous; tolerance_blocks scales it.
_REL_DRIFT_PER_BLOCK = Decimal("0.001")


# ---------------------------------------------------------------------------
# Domain of the comparison (pure, injectable — the unit-tested core).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Snapshot:
    """One side of the comparison: a redeemable-asset balance + per-underlying amounts."""

    balance: Decimal
    underlying: dict[str, Decimal]


@dataclass(frozen=True)
class FieldComparison:
    """One compared field (``balance`` or an underlying symbol) and its verdict."""

    name: str
    spark_value: Decimal
    our_value: Decimal
    within_tolerance: bool


@dataclass(frozen=True)
class ComparisonResult:
    fields: tuple[FieldComparison, ...]

    @property
    def passed(self) -> bool:
        return all(f.within_tolerance for f in self.fields)


def rel_tolerance_from_blocks(tolerance_blocks: int) -> Decimal:
    """Convert an allowed block skew into a relative amount tolerance."""
    return _REL_DRIFT_PER_BLOCK * Decimal(tolerance_blocks)


def _within_tolerance(a: Decimal, b: Decimal, rel_tolerance: Decimal) -> bool:
    """Relative closeness: |a − b| ≤ tolerance × max(|a|, |b|). 0 vs 0 passes."""
    return abs(a - b) <= rel_tolerance * max(abs(a), abs(b))


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError):
        return None


def parse_spark_response(payload: Mapping[str, Any]) -> Snapshot:
    """Normalise a data.spark.fi asset+wallet payload into a ``Snapshot``.

    Assumed schema (see module docstring): a top-level ``balance`` and an
    ``assets`` array of ``{symbol, amount}`` rows. Alternative field names
    (``underlying``/``token``/``balance``) are accepted defensively.
    """
    balance = _to_decimal(payload.get("balance"))
    if balance is None:
        raise ValueError("spark response missing a numeric 'balance' field")

    raw_rows = payload.get("assets") or payload.get("underlying") or []
    rows: list[Any] = raw_rows if isinstance(raw_rows, list) else []
    underlying: dict[str, Decimal] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        row = cast("Mapping[str, Any]", raw)
        symbol = row.get("symbol") or row.get("token")
        amount = _to_decimal(row.get("amount") if row.get("amount") is not None else row.get("balance"))
        if not isinstance(symbol, str) or amount is None:
            continue
        underlying[symbol] = underlying.get(symbol, Decimal(0)) + amount

    return Snapshot(balance=balance, underlying=underlying)


def compare_snapshots(spark: Snapshot, ours: Snapshot, *, rel_tolerance: Decimal) -> ComparisonResult:
    """Compare balance + every underlying symbol (union) within ``rel_tolerance``.

    A symbol present on only one side is compared against 0 on the other, so a
    missing or extra asset is surfaced as a failure rather than silently ignored.
    """
    fields: list[FieldComparison] = [
        FieldComparison(
            name="balance",
            spark_value=spark.balance,
            our_value=ours.balance,
            within_tolerance=_within_tolerance(spark.balance, ours.balance, rel_tolerance),
        )
    ]
    for symbol in sorted(set(spark.underlying) | set(ours.underlying)):
        spark_value = spark.underlying.get(symbol, Decimal(0))
        our_value = ours.underlying.get(symbol, Decimal(0))
        fields.append(
            FieldComparison(
                name=symbol,
                spark_value=spark_value,
                our_value=our_value,
                within_tolerance=_within_tolerance(spark_value, our_value, rel_tolerance),
            )
        )
    return ComparisonResult(fields=tuple(fields))


def render_table(result: ComparisonResult, rel_tolerance: Decimal) -> str:
    """Render the comparison as a fixed-width table with a per-row verdict."""
    header = f"{'field':<12} {'spark':>22} {'ours':>22} {'verdict':>8}"
    lines = [header, "-" * len(header)]
    for f in result.fields:
        verdict = "ok" if f.within_tolerance else "MISMATCH"
        lines.append(f"{f.name:<12} {_fmt(f.spark_value):>22} {_fmt(f.our_value):>22} {verdict:>8}")
    lines.append("-" * len(header))
    status = "PASS" if result.passed else "FAIL"
    lines.append(f"result: {status}  (relative tolerance {rel_tolerance * 100:.2f}%)")
    return "\n".join(lines)


def _fmt(value: Decimal) -> str:
    return f"{value:,.6f}"


# ---------------------------------------------------------------------------
# I/O layers (best-effort; exercised live only after VEC-218 deploys).
# ---------------------------------------------------------------------------


async def fetch_spark_snapshot(client: httpx.AsyncClient, *, vault: str, wallet: str, network: str) -> Snapshot:
    """Fetch the wallet's position in ``vault`` from data.spark.fi."""
    url = f"{_SPARK_BASE_URL}/spark-liquidity-layer/assets/{vault}"
    response = await client.get(url, params={"wallet_address": wallet, "network": network})
    response.raise_for_status()
    return parse_spark_response(response.json())


_WALLET_POSITION_SQL = """
WITH v AS (
    SELECT mv.id AS vault_id, t.decimals AS asset_decimals
    FROM morpho_vault mv
    JOIN token t ON t.id = mv.asset_token_id
    WHERE mv.id = :vault_id
),
wallet_user AS (
    SELECT id FROM "user" WHERE address = :wallet AND chain_id = :chain_id
),
pos AS (
    SELECT vp.assets
    FROM morpho_vault_position vp, v
    WHERE vp.morpho_vault_id = v.vault_id
      AND vp.user_id = (SELECT id FROM wallet_user)
    ORDER BY vp.block_number DESC, vp.block_version DESC, vp.processing_version DESC
    LIMIT 1
),
vs AS (
    SELECT vst.total_assets
    FROM morpho_vault_state vst, v
    WHERE vst.morpho_vault_id = v.vault_id
    ORDER BY vst.block_number DESC, vst.block_version DESC, vst.processing_version DESC
    LIMIT 1
)
SELECT (SELECT assets FROM pos)         AS wallet_assets,
       (SELECT total_assets FROM vs)    AS vault_total_assets,
       (SELECT asset_decimals FROM v)   AS asset_decimals
"""


async def load_our_snapshot(engine: AsyncEngine, *, vault_hex: str, wallet_hex: str, chain_id: int) -> Snapshot:
    """Read our VaultV2 breakdown for the wallet's share of the sparkUSDTbc vault.

    ``balance`` is the wallet's redeemable assets (``morpho_vault_position.assets``
    scaled to human units); ``underlying`` is the whole-vault backed breakdown scaled
    by the wallet's share of the vault (wallet_assets / vault_total_assets). Since
    VEC-511, ``CollateralContribution.backing_value`` is a USD value (loan-token amount
    × loan-token price), so per-asset ``underlying`` here is USD; for the stable
    sparkUSDTbc vault that is ≈ 1:1 with the underlying-token ``balance``. The exact
    Spark-side unit is pinned when the endpoint is reachable (see module docstring).
    """
    repo = MorphoV2BackedBreakdownRepository(engine)
    vault_bytes = bytes.fromhex(vault_hex.removeprefix("0x"))
    wallet_bytes = bytes.fromhex(wallet_hex.removeprefix("0x"))

    ref = await repo.resolve_vault(vault_bytes, chain_id)
    if ref is None:
        raise ValueError(f"sparkUSDTbc vault {vault_hex} not indexed on chain {chain_id}")
    breakdown = await repo.get_backed_breakdown(ref.id)

    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(_WALLET_POSITION_SQL),
                {"vault_id": ref.id, "wallet": wallet_bytes, "chain_id": chain_id},
            )
        ).fetchone()

    if row is None or row.asset_decimals is None:
        raise ValueError(f"vault {vault_hex} has no state rows on chain {chain_id}")

    wallet_assets = Decimal(str(row.wallet_assets)) if row.wallet_assets is not None else Decimal(0)
    vault_total = Decimal(str(row.vault_total_assets)) if row.vault_total_assets is not None else Decimal(0)
    scale = Decimal(10) ** int(row.asset_decimals)

    balance = wallet_assets / scale
    fraction = (wallet_assets / vault_total) if vault_total > 0 else Decimal(0)
    underlying = {item.symbol: item.backing_value * fraction for item in breakdown.items}
    return Snapshot(balance=balance, underlying=underlying)


def _async_database_url(database_url: str) -> str:
    """Normalise a plain postgres URL to the asyncpg dialect (mirrors app.config)."""
    url = make_url(database_url).set(drivername="postgresql+asyncpg")
    query = dict(url.query)
    query.pop("sslmode", None)
    return url.set(query=query).render_as_string(hide_password=False)


async def run(args: argparse.Namespace) -> int:
    async with httpx.AsyncClient(timeout=30) as client:
        spark = await fetch_spark_snapshot(client, vault=args.vault, wallet=args.wallet, network=args.network)

    engine = create_async_engine(_async_database_url(args.database_url))
    try:
        ours = await load_our_snapshot(engine, vault_hex=args.vault, wallet_hex=args.wallet, chain_id=args.chain_id)
    finally:
        await engine.dispose()

    rel_tolerance = rel_tolerance_from_blocks(args.tolerance_blocks)
    result = compare_snapshots(spark, ours, rel_tolerance=rel_tolerance)
    print(render_table(result, rel_tolerance))
    return 0 if result.passed else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cross-check the sparkUSDTbc VaultV2 backed breakdown against data.spark.fi"
    )
    parser.add_argument(
        "--database-url", default=os.environ.get("DATABASE_URL"), help="Postgres URL (default: $DATABASE_URL)"
    )
    parser.add_argument("--wallet", default=_DEFAULT_WALLET, help="Wallet address to check")
    parser.add_argument("--vault", default=_SPARKUSDTBC_VAULT, help="sparkUSDTbc VaultV2 address")
    parser.add_argument("--network", default=_DEFAULT_NETWORK, help="data.spark.fi network key")
    parser.add_argument("--chain-id", type=int, default=_DEFAULT_CHAIN_ID, help="Our chain_id for the vault")
    parser.add_argument("--tolerance-blocks", type=int, default=2, help="Allowed block skew (→ relative tolerance)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.database_url:
        print("error: provide --database-url or set DATABASE_URL", file=sys.stderr)
        return 2
    try:
        return asyncio.run(run(args))
    except (httpx.HTTPError, OSError, ValueError) as exc:
        print(f"cross-check could not run: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
