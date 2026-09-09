"""Borrower positions for the CORE model, from the live tables.

Replaces the ``users_*.parquet`` / ``market_*.parquet`` snapshots. The
model consumes a wide per-user frame (per-asset ``<sym>_supply``,
``<sym>_supply_usd``, ``<sym>_borrow``, ``<sym>_borrow_usd`` plus aggregate
columns) and a market frame of oracle prices for the simulated collaterals;
this adapter reproduces both shapes — SparkLend from ``borrower_current`` /
``borrower_collateral_current`` / ``sparklend_reserve_data`` /
``token_price_current``, Morpho from ``morpho_market_position``, and Syrup
(Maple) from ``maple_loan_current`` / ``maple_loan_state`` /
``maple_loan_collateral`` / ``maple_pool_state``.

Positions are valued with the protocol's own oracle (``_PROTOCOL_ORACLE``,
checked against ``protocol_oracle``), joined by token id — never by symbol.
Freshness is a property of the feed, not of a token: the oracle worker writes
a row only when a price changes, so a fixed $1 feed legitimately stays silent
for weeks while the feed as a whole ticks every block. Syrup has no on-chain
oracle; its positions use Maple's attested per-unit valuations instead, and
its freshness bound is the pool's sync-cycle age.

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
- Syrup loans whose liquidation trigger sits at or above par coverage
  (stable-on-stable loans, LT >= 1) are excluded: the protocol does not rely
  on collateral price to protect them, so the model's price-shock liquidation
  mechanism does not apply — and BA's parquet frames contain no such rows.
  Their debt would add to exposure while contributing ~zero simulated loss,
  so excluding them biases CRR up, not down.
- Syrup skips BA's ``interest_rate`` / ``loan_token_symbol`` /
  ``collateral_token_symbol`` parquet columns — nothing in the model reads
  them.
- Only Ethereum is implemented; other protocols keep parquet.
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

# All Blue markets for one (collateral, loan) pair, each borrower's newest state
# from the trigger-fed morpho_market_position_current cache (VEC-753) — not
# DISTINCT ON over the history: that scanned every chunk (1.7 s at 1.23M rows on
# staging, growing with history) and needed tiered reads past the 1-year S3
# horizon. The model's market key spans the pair, not one LLTV tranche, so every
# tranche's borrowers are included.
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
    )
    SELECT u.address AS user_address, m.lltv,
           m.collateral_symbol, m.collateral_decimals, m.collateral_address, m.collateral_price,
           m.loan_symbol, m.loan_decimals, m.loan_address, m.loan_price,
           cur.collateral, cur.borrow_assets
    FROM morpho_market_position_current cur
    JOIN markets m ON m.id = cur.morpho_market_id
    JOIN "user" u ON u.id = cur.user_id
    WHERE cur.borrow_assets > 0
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


# Maple publishes no per-loan liquidation incentive; BA's parquet carries a
# flat 2% for every loan in both syrup markets, reproduced here.
_SYRUP_LIQUIDATION_INCENTIVE = 1.02

# Debt is valued at $1/unit, which is only correct for the stablecoin
# underlyings Maple ships today (same reasoning as the backed-breakdown
# repository's allowlist) — refuse a pool outside it rather than mis-value it.
_SYRUP_STABLE_LOAN_TOKENS = frozenset({"USDC", "USDT", "USDG"})

# maple_loan_collateral.liquidation_level is the margin-call coverage trigger
# x1e6 (collateral/debt ratio at which the loan is called). The model's LT is
# its inverse: level 1204800 -> LT 0.830013, the parquet's own 0.83001.
_SYRUP_PAR_COVERAGE_LEVEL = 1_000_000

_SYRUP_POOL = text("""
    SELECT mp.id, ut.decimals AS underlying_decimals
    FROM maple_pool_current mp
    JOIN token ut ON ut.id = mp.asset_token_id
    WHERE mp.chain_id = :chain_id AND mp.is_syrup AND upper(ut.symbol) = :loan
""")

# External Active loans at the pool's current sync cycle, with the collateral
# row of the SAME (synced_at, processing_version) snapshot. Both bounds are
# lifted from backed_breakdown_repository_maple and are load-bearing there:
# the indexer emits no tombstones, so a repaid loan's last Active state
# lingers in older cycles forever, and collateral must never mix with a
# fresher principal.
_SYRUP_POSITIONS = text("""
    WITH pool_cycle AS (
        SELECT max(synced_at) AS synced_at
        FROM maple_pool_state
        WHERE maple_pool_id = :pool_id
    )
    SELECT u.address AS borrower_address,
           ls.principal_owed, ls.acm_ratio, ls.synced_at,
           c.asset_symbol, c.asset_amount, c.asset_decimals,
           c.asset_value_usd, c.liquidation_level
    FROM maple_loan_current l
    JOIN "user" u ON u.id = l.borrower_user_id
    JOIN LATERAL (
        SELECT synced_at, processing_version, state, principal_owed, acm_ratio
        FROM maple_loan_state s
        WHERE s.maple_loan_id = l.id
        ORDER BY s.synced_at DESC, s.processing_version DESC
        LIMIT 1
    ) ls ON true
    LEFT JOIN maple_loan_collateral c
      ON c.maple_loan_id      = l.id
     AND c.synced_at          = ls.synced_at
     AND c.processing_version = ls.processing_version
    WHERE l.maple_pool_id = :pool_id
      AND NOT l.is_internal
      AND ls.state = 'Active'
      AND ls.principal_owed > 0
      AND ls.synced_at = (SELECT synced_at FROM pool_cycle)
""")

# Loans of one cycle share one Maple-attested price per symbol; anything else
# is a data defect, not a spread to average over.
_SYRUP_PRICE_TOLERANCE = 1e-6

# Computed coverage (collateral USD / principal) and Maple's own acm_ratio are
# derived from the same attested prices, so they should agree to rounding;
# a wider gap means the units or the snapshot join drifted.
_SYRUP_ACM_WARN_TOLERANCE = 0.01


def syrup_lltv(liquidation_level: float) -> float:
    """LT from Maple's margin-call coverage trigger (x1e6): its inverse."""
    return float(_SYRUP_PAR_COVERAGE_LEVEL) / float(liquidation_level)


def syrup_attested_prices(rows: Sequence[Any]) -> dict[str, float]:
    """``{SYMBOL: price}`` from Maple's attested per-unit valuations — the market frame's input.

    Scoped to the loans that can enter the frame (above-par trigger): a price
    disagreement on an excluded stable must not fail the whole market.
    """
    prices: dict[str, float] = {}
    for r in rows:
        if r.asset_symbol is None or r.asset_symbol == "" or r.asset_value_usd is None:
            continue
        if r.liquidation_level is None or float(r.liquidation_level) <= _SYRUP_PAR_COVERAGE_LEVEL:
            continue
        symbol = r.asset_symbol.upper()
        price = float(Decimal(str(r.asset_value_usd)) / Decimal(10) ** 8)
        known = prices.setdefault(symbol, price)
        if abs(known - price) > _SYRUP_PRICE_TOLERANCE * max(abs(known), abs(price)):
            raise ValueError(
                f"Maple attested two prices for {symbol} in one cycle ({known} vs {price}); "
                "refusing an ambiguous collateral valuation"
            )
    return prices


def build_syrup_users_frame(rows: Sequence[Any], loan_token: str, underlying_decimals: int) -> pd.DataFrame:
    """Assemble the Syrup users frame: one row per external Active loan.

    BA's parquet is per-loan too (wallet_address repeats; nothing downstream
    groups by it), each loan carrying exactly one collateral asset and its own
    LT from the loan's margin-call trigger.
    """
    borrow_col = loan_token.lower()
    prices = syrup_attested_prices(rows)
    dropped_no_collateral: list[tuple[str, float]] = []
    dropped_par_trigger: list[tuple[str, float]] = []
    acm_deviations: list[float] = []
    records: list[dict] = []
    for r in rows:
        address = "0x" + bytes(r.borrower_address).hex()
        principal = float(Decimal(str(r.principal_owed)) / (Decimal(10) ** underlying_decimals))
        unusable = (
            r.asset_symbol is None
            or r.asset_symbol == ""
            or r.asset_amount is None
            or r.asset_value_usd is None
            or r.liquidation_level is None
            or float(r.liquidation_level) <= 0
        )
        if unusable:
            dropped_no_collateral.append((address, principal))
            continue
        if float(r.liquidation_level) <= _SYRUP_PAR_COVERAGE_LEVEL:
            # LT >= 1: the protocol margin-calls at/above par coverage, so
            # collateral price is not what protects this loan (stable-on-stable
            # terms). No simulatable price-liquidation mechanism — and LT >= 1
            # would also break the liquidator's -1 + LT*(1+bonus) < 0 guard.
            dropped_par_trigger.append((address, principal))
            continue
        symbol = r.asset_symbol.upper()
        qty = float(Decimal(str(r.asset_amount)) / (Decimal(10) ** int(r.asset_decimals)))
        collateral_usd = qty * prices[symbol]
        if collateral_usd <= 0:
            dropped_no_collateral.append((address, principal))
            continue
        if r.acm_ratio is not None:
            acm = float(Decimal(str(r.acm_ratio)) / Decimal(10) ** 6)
            if acm > 0:
                acm_deviations.append(abs(collateral_usd / principal - acm) / acm)
        lltv = syrup_lltv(float(r.liquidation_level))
        ltv = principal / collateral_usd
        records.append(
            {
                "wallet_address": address,
                "lltv": lltv,
                "ltv": ltv,
                "health_factor": lltv / ltv,
                "liquidation_incentive": _SYRUP_LIQUIDATION_INCENTIVE,
                f"{symbol.lower()}_supply": qty,
                f"{symbol.lower()}_supply_usd": collateral_usd,
                f"{borrow_col}_borrow": principal,
                f"{borrow_col}_borrow_usd": principal,
                "total_collateral_usd": collateral_usd,
                "total_borrow_usd": principal,
            }
        )

    for dropped, why in (
        (dropped_no_collateral, "no usable collateral row (existing bad debt or pending deposit, not simulatable)"),
        (dropped_par_trigger, "margin-call trigger at/above par coverage (stable-on-stable, no price risk)"),
    ):
        if dropped:
            logger.warning(
                "excluded %d syrup loan(s): %s; $%.2f total principal dropped",
                len(dropped),
                why,
                sum(usd for _, usd in dropped),
            )
    if acm_deviations and max(acm_deviations) > _SYRUP_ACM_WARN_TOLERANCE:
        logger.warning(
            "syrup computed coverage disagrees with Maple's acm_ratio by up to %.2f%% across %d loan(s); "
            "expected < %.0f%% — check collateral units and the snapshot join",
            max(acm_deviations) * 100,
            len(acm_deviations),
            _SYRUP_ACM_WARN_TOLERANCE * 100,
        )
    if not records:
        raise ValueError(f"no simulatable external Active syrup loans for loan_token={loan_token!r}")
    return pd.DataFrame.from_records(records)


class PostgresPositionsReader:
    """``get_protocol_data`` from the live tables. SparkLend, Morpho and Syrup on Ethereum.

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
        if protocol.upper() == "SYRUP":
            return await self._get_syrup_data(loan_token.upper())
        if protocol.upper() != "SPARKLEND":
            raise ValueError(
                f"live positions are only implemented for SPARKLEND, MORPHO and SYRUP, got {protocol}. "
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

    async def _get_syrup_data(self, loan_token: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        if loan_token not in _SYRUP_STABLE_LOAN_TOKENS:
            raise ValueError(
                f"syrup loan token {loan_token!r} is outside the $1-valuation allowlist "
                f"{sorted(_SYRUP_STABLE_LOAN_TOKENS)}; price the debt via an oracle before adding it"
            )
        async with self._engine.connect() as conn:
            pools = (await conn.execute(_SYRUP_POOL, {"chain_id": self._chain_id, "loan": loan_token})).fetchall()
            if len(pools) != 1:
                raise ValueError(
                    f"expected exactly one syrup pool with underlying {loan_token} on chain "
                    f"{self._chain_id}, found {len(pools)} — is maple-graphql-indexer running?"
                )
            pool = pools[0]
            rows = (await conn.execute(_SYRUP_POSITIONS, {"pool_id": pool.id})).fetchall()
        if not rows:
            raise ValueError(
                f"no external Active syrup loans for {loan_token} on chain {self._chain_id} "
                "— is maple-graphql-indexer running?"
            )
        # The pool cycle is Maple's feed: positions AND valuations date from it,
        # so a stale cycle is the syrup analog of a silent oracle.
        cycle_age = pd.Timestamp.now(tz="UTC") - pd.Timestamp(rows[0].synced_at)
        if cycle_age > self._max_feed_age:
            raise ValueError(
                f"the syrup pool's newest sync cycle is {cycle_age} old (bound {self._max_feed_age}); "
                "refusing to value positions on a stale snapshot — is maple-graphql-indexer running?"
            )
        users_df = build_syrup_users_frame(rows, loan_token, int(pool.underlying_decimals))
        supplied = {c.rsplit("_", 1)[0].upper() for c in users_df.columns if c.endswith("_supply")}
        market_df = build_market_frame(supplied, syrup_attested_prices(rows))
        logger.info(
            "syrup positions loaded from live tables: %d loan(s) of %s, %d modeled collateral(s)",
            len(users_df),
            loan_token,
            len(market_df),
        )
        return users_df, market_df

    async def _get_morpho_data(self, collateral: str, loan_token: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        async with self._engine.connect() as conn:
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
