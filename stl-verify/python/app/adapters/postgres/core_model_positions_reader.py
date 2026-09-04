"""SparkLend borrower positions for the CORE model, from the live tables.

Replaces ``users_sparklend_*.parquet`` / ``market_sparklend_*.parquet``. The
model consumes a wide per-user frame (per-asset ``<sym>_supply``,
``<sym>_supply_usd``, ``<sym>_borrow``, ``<sym>_borrow_usd`` plus aggregate
columns) and a market frame of oracle prices for the simulated collaterals;
this adapter reproduces both shapes from ``borrower_current`` /
``borrower_collateral_current`` / ``sparklend_reserve_data`` /
``token_price_current``.

Positions are valued with the protocol's own oracle (``_PROTOCOL_ORACLE``,
checked against ``protocol_oracle``), joined by token id — never by symbol.
Freshness is a property of the feed, not of a token: the oracle worker writes
a row only when a price changes, so a fixed $1 feed legitimately stays silent
for weeks while the feed as a whole ticks every block.

Aggregate semantics were reverse-engineered from BA's parquet rows and
reproduce them exactly:

- ``total_collateral_usd``  = Σ supply_usd over *eligible* assets (LT > 0 and
  enabled as collateral); USDC/USDS-style supply still shows in its own
  ``<sym>_supply_usd`` column but is not collateral
- ``lltv``                  = Σ supply_usd × LT / total_collateral_usd
- ``ltv``                   = total_borrow_usd / total_collateral_usd
- ``health_factor``         = Σ supply_usd × LT / total_borrow_usd
- ``liquidation_incentive`` = Σ supply_usd × bonus / Σ supply_usd over *every*
  supplied asset — BA's rows dilute the bonus by non-eligible supply (a real
  row: WETH 18,231 + WBTC 10,715 + USDC 40,079 → total 28,946, incentive
  0.4434), so this is reproduced, not corrected
- a borrower with no eligible collateral has no row (BA's files contain none)

Deliberate deviations, documented in DATA_GAPS.md:

- e-mode categories are not indexed, so reserve-level LT/bonus are used for
  every user and ``emode_category`` is 0. For e-mode users this understates
  LT, hence understates HF — the conservative direction.
- Only SparkLend on Ethereum is implemented; other protocols keep parquet.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.adapters.postgres.core_model_orderbook_reader import BTC_GROUP, ETH_GROUP

logger = logging.getLogger(__name__)

# Collaterals whose price paths the model simulates. Everything else a user
# supplies is carried at constant USD (the model's "unmodeled" bucket).
MODELED_COLLATERALS = frozenset(ETH_GROUP | BTC_GROUP | {"XRP", "SOL", "JITOSOL", "HYPE"})

# The oracle each protocol's positions are valued with. Explicit on purpose:
# protocol_oracle binds SparkLend to two oracles, and "newest row across every
# oracle" made the price source of each run arbitrary.
_PROTOCOL_ORACLE: dict[str, tuple[str, str]] = {
    "SPARKLEND": ("SparkLend", "sparklend"),
    # Blue markets carry per-market oracle contracts we do not index; the
    # registered Chainlink feeds stand in (DATA_GAPS.md §3).
    "MORPHO": ("Morpho Blue", "chainlink"),
}

_ORACLE_ID = text("""
    SELECT o.id
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id
    JOIN oracle o ON o.id = po.oracle_id
    WHERE p.chain_id = :chain_id AND p.name = :protocol_name
      AND o.name = :oracle_name AND o.chain_id = :chain_id
    ORDER BY po.from_block DESC
    LIMIT 1
""")

# A feed that wrote nothing at all in the window is the dead-indexer case; a
# single token's old row is not (rows are written only when a price changes).
_FEED_ALIVE = text("""
    SELECT 1
    FROM onchain_token_price
    WHERE oracle_id = :oracle_id AND "timestamp" > now() - CAST(:max_age AS interval)
    LIMIT 1
""")

# Newest state per (user, token) per side from the trigger-fed *_current caches,
# not DISTINCT ON over the histories: those hypertables tier chunks older than a
# year to S3, where a plain session cannot see them, so a borrower idle for a year
# would silently drop out or reappear with an old amount. Priced by token id from
# the oracle's current-price cache; NULL = unpriced.
_POSITIONS = text("""
    SELECT 'borrow' AS side, u.address AS user_address, t.id AS token_id, t.symbol, t.decimals,
           b.amount, true AS collateral_enabled, pr.price_usd
    FROM borrower_current b
    JOIN protocol p ON p.id = b.protocol_id
    JOIN "user" u ON u.id = b.user_id
    JOIN token t ON t.id = b.token_id
    LEFT JOIN token_price_current pr ON pr.oracle_id = :oracle_id AND pr.token_id = b.token_id
    WHERE p.chain_id = :chain_id AND p.name = :protocol_name AND b.amount > 0
    UNION ALL
    SELECT 'supply', u.address, t.id, t.symbol, t.decimals, c.amount, c.collateral_enabled, pr.price_usd
    FROM borrower_collateral_current c
    JOIN protocol p ON p.id = c.protocol_id
    JOIN "user" u ON u.id = c.user_id
    JOIN token t ON t.id = c.token_id
    LEFT JOIN token_price_current pr ON pr.oracle_id = :oracle_id AND pr.token_id = c.token_id
    WHERE p.chain_id = :chain_id AND p.name = :protocol_name AND c.amount > 0
""")

# Morpho has no *_current cache yet, so its newest-per-key read must be able to
# see S3-tiered chunks. The GUC exists only where tiering does (Timescale Cloud);
# without it there is no tiered history to miss.
_TIERED_READS_GUC = text("SELECT 1 FROM pg_settings WHERE name = 'timescaledb.enable_tiered_reads'")
_ENABLE_TIERED_READS = text("SET LOCAL timescaledb.enable_tiered_reads = 'on'")

# sparklend_reserve_data is partitioned by block_number and has no tiering
# policy, and every reserve is rewritten constantly, so its newest row is local.
_RESERVE_PARAMS = text("""
    SELECT DISTINCT ON (srd.token_id)
           t.symbol,
           srd.liquidation_threshold / 10000::numeric AS liquidation_threshold,
           srd.liquidation_bonus / 10000::numeric     AS liquidation_bonus
    FROM sparklend_reserve_data srd
    JOIN protocol p ON p.id = srd.protocol_id
    JOIN token t ON t.id = srd.token_id
    WHERE p.chain_id = :chain_id AND p.name = :protocol_name
      -- The only writer always sets it (decode fails on a missing field); this
      -- filter only skips historical rows written before that path existed.
      AND srd.liquidation_threshold IS NOT NULL
    ORDER BY srd.token_id, srd.block_number DESC, srd.block_version DESC, srd.processing_version DESC
""")


@dataclass(frozen=True)
class PositionRow:
    side: str  # "borrow" | "supply"
    user_address: str
    token_id: int
    symbol: str
    amount: float  # token units, decimals already applied
    collateral_enabled: bool
    price: float | None  # USD per token from the protocol oracle; None = unpriced


def supply_prices(positions: Sequence[PositionRow]) -> dict[str, float]:
    """``{SYMBOL: price}`` of the priced supplied tokens — the market frame's input."""
    return {p.symbol.upper(): p.price for p in positions if p.side == "supply" and p.price is not None}


def build_market_frame(supplied_symbols: set[str], prices: dict[str, float]) -> pd.DataFrame:
    """Oracle prices for the simulated collaterals present in this market."""
    modeled = sorted(s for s in supplied_symbols if s in MODELED_COLLATERALS)
    missing = [s for s in modeled if s not in prices]
    if missing:
        raise ValueError(f"no oracle price for modeled collateral(s) {missing}; refusing a partial market frame")
    return pd.DataFrame({"token_symbol": modeled, "oracle_price": [prices[s] for s in modeled]})


def build_users_frame(
    positions: list[PositionRow],
    reserve_params: dict[str, tuple[float, float]],  # symbol -> (LT, bonus), both fractions
    loan_token: str,
) -> pd.DataFrame:
    """Assemble the wide per-user frame the model consumes.

    Users qualify by borrowing the market's loan token (``ALL`` = any borrow),
    exactly as BA's per-market parquet files were pre-filtered.
    """
    users: dict[str, dict[str, float]] = {}
    enabled: dict[tuple[str, str], bool] = {}
    prices: dict[str, float | None] = {}
    tokens_by_symbol: dict[str, set[int]] = {}
    for row in positions:
        cols = users.setdefault(row.user_address, {})
        sym = row.symbol.lower()
        prices[row.symbol.upper()] = row.price
        tokens_by_symbol.setdefault(row.symbol.upper(), set()).add(row.token_id)
        if row.side == "borrow":
            cols[f"{sym}_borrow"] = cols.get(f"{sym}_borrow", 0.0) + row.amount
        else:
            cols[f"{sym}_supply"] = cols.get(f"{sym}_supply", 0.0) + row.amount
            enabled[(row.user_address, row.symbol.upper())] = row.collateral_enabled

    # The wide frame keys columns by symbol, so two distinct tokens sharing one
    # cannot be represented — refuse rather than merge a spoof into the real one.
    ambiguous = sorted(s for s, ids in tokens_by_symbol.items() if len(ids) > 1)
    if ambiguous:
        raise ValueError(
            f"symbol(s) {ambiguous} are held as more than one distinct token (ids "
            f"{[sorted(tokens_by_symbol[s]) for s in ambiguous]}); refusing a symbol-keyed frame"
        )

    unpriced: set[str] = set()
    dropped_no_collateral: list[tuple[str, float]] = []
    records: list[dict] = []
    for address, cols in users.items():
        record: dict = {"wallet_address": address, "emode_category": 0}
        total_supply = total_collateral = total_lt = total_bonus = total_borrow = 0.0
        borrows_loan_token = False
        for col, qty in cols.items():
            sym, side = col.rsplit("_", 1)
            upper = sym.upper()
            price = prices.get(upper)
            if price is None:
                unpriced.add(upper)
                continue
            usd = qty * price
            record[col] = qty
            record[f"{col}_usd"] = usd
            if side == "supply":
                total_supply += usd
                lt, bonus = reserve_params.get(upper, (0.0, 0.0))
                # Disabled collateral is not collateral, same as an LT=0 reserve;
                # the supply itself still shows and still dilutes the incentive.
                if enabled.get((address, upper), True) and lt > 0:
                    total_collateral += usd
                    total_lt += usd * lt
                    total_bonus += usd * bonus
            else:
                total_borrow += usd
                if loan_token.upper() == "ALL" or upper == loan_token.upper():
                    borrows_loan_token = True
        if not borrows_loan_token or total_borrow <= 0:
            continue
        if total_collateral <= 0:
            # A borrower with no eligible collateral is already bad debt, not a
            # future liquidation the simulation can process — every downstream
            # ratio divides by collateral and would poison the CRR with NaN.
            # Excluded loudly below, never silently.
            dropped_no_collateral.append((address, total_borrow))
            continue
        record["total_collateral_usd"] = total_collateral
        record["total_borrow_usd"] = total_borrow
        record["lltv"] = total_lt / total_collateral
        record["ltv"] = total_borrow / total_collateral
        record["health_factor"] = total_lt / total_borrow
        record["liquidation_incentive"] = total_bonus / total_supply
        records.append(record)

    if unpriced:
        raise ValueError(
            f"no protocol-oracle price for supplied/borrowed token(s) {sorted(unpriced)}; "
            "refusing to build users with silent USD holes"
        )
    if dropped_no_collateral:
        logger.warning(
            "excluded %d borrower(s) with zero enabled collateral (existing bad debt, "
            "not simulatable): $%.2f total borrow dropped",
            len(dropped_no_collateral),
            sum(usd for _, usd in dropped_no_collateral),
        )
    if not records:
        raise ValueError(f"no active borrowers found for loan_token={loan_token!r}")
    return pd.DataFrame.from_records(records)


# Morpho Blue's liquidation incentive factor is a pure function of the
# market's LLTV: LIF = min(M, 1 / (beta * LLTV + (1 - beta))). M and beta are
# `constant`s in the non-upgradeable Blue singleton (ConstantsLib.sol:
# MAX_LIQUIDATION_INCENTIVE_FACTOR, LIQUIDATION_CURSOR), so they cannot change
# for this deployment and are hardcoded rather than configured. To check them
# against reality: on indexed `Liquidate` events, seizedAssets × collateral
# price / (repaidAssets × loan price) equals this formula for the market's LLTV
# (staging, 26 Aug 2026: LLTVs 0.77–0.945 all within oracle noise). BA's
# parquet carries the same value for LLTV 0.86.
_MORPHO_LIF_CAP = 1.15
_MORPHO_BETA = 0.3

# All Blue markets for one (collateral, loan) pair, with the latest position
# per (user, market). The model's market key spans the pair, not one LLTV
# tranche, so every tranche's borrowers are included.
_MORPHO_POSITIONS = text("""
    WITH markets AS (
        SELECT mm.id, mm.lltv / 1e18 AS lltv,
               ct.symbol AS collateral_symbol, ct.decimals AS collateral_decimals,
               ct.address AS collateral_address, cp.price_usd AS collateral_price,
               lt.symbol AS loan_symbol, lt.decimals AS loan_decimals,
               lt.address AS loan_address, lp.price_usd AS loan_price
        FROM morpho_market mm
        JOIN token ct ON ct.id = mm.collateral_token_id
        JOIN token lt ON lt.id = mm.loan_token_id
        LEFT JOIN token_price_current cp ON cp.oracle_id = :oracle_id AND cp.token_id = mm.collateral_token_id
        LEFT JOIN token_price_current lp ON lp.oracle_id = :oracle_id AND lp.token_id = mm.loan_token_id
        WHERE mm.chain_id = :chain_id
          AND upper(ct.symbol) = :collateral AND upper(lt.symbol) = :loan
    ),
    latest AS (
        SELECT DISTINCT ON (mp.user_id, mp.morpho_market_id)
               mp.user_id, mp.morpho_market_id, mp.collateral, mp.borrow_assets
        FROM morpho_market_position mp
        JOIN markets m ON m.id = mp.morpho_market_id
        ORDER BY mp.user_id, mp.morpho_market_id,
                 mp.block_number DESC, mp.block_version DESC, mp.processing_version DESC
    )
    SELECT u.address AS user_address, m.lltv,
           m.collateral_symbol, m.collateral_decimals, m.collateral_address, m.collateral_price,
           m.loan_symbol, m.loan_decimals, m.loan_address, m.loan_price,
           l.collateral, l.borrow_assets
    FROM latest l
    JOIN markets m ON m.id = l.morpho_market_id
    JOIN "user" u ON u.id = l.user_id
    WHERE l.borrow_assets > 0
""")


def morpho_liquidation_incentive(lltv: float) -> float:
    return min(_MORPHO_LIF_CAP, 1.0 / (_MORPHO_BETA * lltv + (1.0 - _MORPHO_BETA)))


def build_morpho_users_frame(rows: Sequence[Any]) -> pd.DataFrame:
    """Assemble the Morpho users frame: one row per borrower of the pair.

    A wallet borrowing across several LLTV tranches of the same pair collapses
    to one row (the model keys rows by wallet); its lltv is the
    collateral-USD-weighted average across tranches. BA's snapshot had a single
    tranche, so this only deviates when a second tranche has real borrowers.
    """
    per_user: dict[str, dict] = {}
    unpriced: set[str] = set()
    dropped_no_collateral: list[tuple[str, float]] = []
    for r in rows:
        collat_sym, loan_sym = r.collateral_symbol.upper(), r.loan_symbol.upper()
        if r.collateral_price is None or r.loan_price is None:
            unpriced.update(s for s, p in ((collat_sym, r.collateral_price), (loan_sym, r.loan_price)) if p is None)
            continue
        address = "0x" + bytes(r.user_address).hex()
        collateral_qty = float(Decimal(str(r.collateral)) / (Decimal(10) ** int(r.collateral_decimals)))
        borrow_qty = float(Decimal(str(r.borrow_assets)) / (Decimal(10) ** int(r.loan_decimals)))
        agg = per_user.setdefault(
            address,
            {"collateral_qty": 0.0, "collateral_usd": 0.0, "borrow_qty": 0.0, "borrow_usd": 0.0, "lltv_weighted": 0.0},
        )
        collateral_usd = collateral_qty * float(r.collateral_price)
        agg["collateral_qty"] += collateral_qty
        agg["collateral_usd"] += collateral_usd
        agg["borrow_qty"] += borrow_qty
        agg["borrow_usd"] += borrow_qty * float(r.loan_price)
        agg["lltv_weighted"] += float(r.lltv) * collateral_usd
        agg["symbols"] = (collat_sym, loan_sym)

    if unpriced:
        raise ValueError(
            f"no protocol-oracle price for token(s) {sorted(unpriced)}; refusing to build users with silent USD holes"
        )

    records = []
    for address, agg in per_user.items():
        if agg["collateral_usd"] <= 0:
            dropped_no_collateral.append((address, agg["borrow_usd"]))
            continue
        collat_sym, loan_sym = agg["symbols"]
        lltv = agg["lltv_weighted"] / agg["collateral_usd"]
        records.append(
            {
                "wallet_address": address,
                "lltv": lltv,
                "ltv": agg["borrow_usd"] / agg["collateral_usd"],
                "health_factor": lltv * agg["collateral_usd"] / agg["borrow_usd"],
                "liquidation_incentive": morpho_liquidation_incentive(lltv),
                f"{collat_sym.lower()}_supply": agg["collateral_qty"],
                f"{collat_sym.lower()}_supply_usd": agg["collateral_usd"],
                f"{loan_sym.lower()}_borrow": agg["borrow_qty"],
                f"{loan_sym.lower()}_borrow_usd": agg["borrow_usd"],
                "total_collateral_usd": agg["collateral_usd"],
                "total_borrow_usd": agg["borrow_usd"],
            }
        )
    if dropped_no_collateral:
        logger.warning(
            "excluded %d morpho borrower(s) with zero collateral (existing bad debt, "
            "not simulatable): $%.2f total borrow dropped",
            len(dropped_no_collateral),
            sum(usd for _, usd in dropped_no_collateral),
        )
    if not records:
        raise ValueError("no active morpho borrowers found for this market pair")
    return pd.DataFrame.from_records(records)


class PostgresPositionsReader:
    """``get_protocol_data`` from the live tables. SparkLend and Morpho on Ethereum.

    ``max_feed_age`` bounds how long the protocol's oracle feed may have been
    silent as a whole; single tokens carry no age bound (see module docstring).
    """

    def __init__(self, engine: AsyncEngine, chain_id: int = 1, max_feed_age: timedelta = timedelta(days=2)) -> None:
        self._engine = engine
        self._chain_id = chain_id
        self._max_feed_age = max_feed_age

    async def _live_oracle_id(self, conn: AsyncConnection, protocol_key: str) -> int:
        """Id of the protocol's valuation oracle, refusing a binding that is missing or a feed that is silent."""
        protocol_name, oracle_name = _PROTOCOL_ORACLE[protocol_key]
        params = {"chain_id": self._chain_id, "protocol_name": protocol_name, "oracle_name": oracle_name}
        oracle_id = (await conn.execute(_ORACLE_ID, params)).scalar_one_or_none()
        if oracle_id is None:
            raise ValueError(
                f"oracle {oracle_name!r} is not bound to protocol {protocol_name!r} on chain {self._chain_id} "
                "in protocol_oracle; refusing to value positions with an unregistered oracle"
            )
        alive = await conn.execute(_FEED_ALIVE, {"oracle_id": oracle_id, "max_age": self._max_feed_age})
        if alive.scalar_one_or_none() is None:
            raise ValueError(
                f"oracle feed {oracle_name!r} wrote no price in the last {self._max_feed_age}; "
                "refusing to value positions on a dead feed — is oracle-price-worker running?"
            )
        return int(oracle_id)

    async def get_protocol_data(
        self,
        protocol: str,
        network: str,
        morpho_market: str,
        loan_token: str,
        galaxy_type: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        if network.upper() != "ETHEREUM":
            raise ValueError(f"live positions are Ethereum-only, got {network}")
        if protocol.upper() == "MORPHO":
            return await self._get_morpho_data(morpho_market.upper(), loan_token.upper())
        if protocol.upper() != "SPARKLEND":
            raise ValueError(
                f"live positions are only implemented for SPARKLEND and MORPHO, got {protocol}. "
                "See app/risk_engine/core_model/DATA_GAPS.md."
            )
        protocol_name, _ = _PROTOCOL_ORACLE["SPARKLEND"]
        params = {"chain_id": self._chain_id, "protocol_name": protocol_name}
        async with self._engine.connect() as conn:
            oracle_id = await self._live_oracle_id(conn, "SPARKLEND")
            position_rows = (await conn.execute(_POSITIONS, {**params, "oracle_id": oracle_id})).fetchall()
            reserve_rows = (await conn.execute(_RESERVE_PARAMS, params)).fetchall()

        positions = [
            PositionRow(
                side=r.side,
                user_address="0x" + bytes(r.user_address).hex(),
                token_id=int(r.token_id),
                symbol=r.symbol,
                amount=float(Decimal(str(r.amount)) / (Decimal(10) ** int(r.decimals))),
                collateral_enabled=bool(r.collateral_enabled),
                price=None if r.price_usd is None else float(r.price_usd),
            )
            for r in position_rows
        ]
        reserve_params = {
            r.symbol.upper(): (float(r.liquidation_threshold), float(r.liquidation_bonus)) for r in reserve_rows
        }

        users_df = build_users_frame(positions, reserve_params, loan_token)
        supplied = {c.rsplit("_", 1)[0].upper() for c in users_df.columns if c.endswith("_supply")}
        market_df = build_market_frame(supplied, supply_prices(positions))
        logger.info(
            "positions loaded from live tables: %d borrowers, %d modeled collaterals (loan_token=%s)",
            len(users_df),
            len(market_df),
            loan_token,
        )
        return users_df, market_df

    async def _get_morpho_data(self, collateral: str, loan_token: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        async with self._engine.begin() as conn:
            if (await conn.execute(_TIERED_READS_GUC)).scalar_one_or_none() is not None:
                await conn.execute(_ENABLE_TIERED_READS)
            oracle_id = await self._live_oracle_id(conn, "MORPHO")
            rows = (
                await conn.execute(
                    _MORPHO_POSITIONS,
                    {"chain_id": self._chain_id, "collateral": collateral, "loan": loan_token, "oracle_id": oracle_id},
                )
            ).fetchall()
        if not rows:
            raise ValueError(
                f"no morpho_market rows (or no borrowers) for {collateral}/{loan_token} on chain "
                f"{self._chain_id} — is the morpho indexer running?"
            )
        # Blue is permissionless and markets are matched by display symbol: refuse
        # unless every matched market (only those with borrowers) agrees on addresses.
        for side, addresses in (
            ("collateral", {bytes(r.collateral_address) for r in rows}),
            ("loan", {bytes(r.loan_address) for r in rows}),
        ):
            if len(addresses) > 1:
                raise ValueError(
                    f"ambiguous {side} token for {collateral}/{loan_token}: symbol resolves to "
                    f"{len(addresses)} distinct addresses ({sorted('0x' + a.hex() for a in addresses)}); "
                    "refusing a symbol-keyed market selection"
                )
        users_df = build_morpho_users_frame(rows)
        market_df = build_market_frame(
            {collateral}, {collateral: float(r.collateral_price) for r in rows if r.collateral_price is not None}
        )
        logger.info(
            "morpho positions loaded from live tables: %d borrowers of %s/%s across %d tranche row(s)",
            len(users_df),
            collateral,
            loan_token,
            len(rows),
        )
        return users_df, market_df
