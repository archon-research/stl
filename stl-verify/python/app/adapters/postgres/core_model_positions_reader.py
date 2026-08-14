"""SparkLend borrower positions for the CORE model, from the live tables.

Replaces ``users_sparklend_*.parquet`` / ``market_sparklend_*.parquet``. The
model consumes a wide per-user frame (per-asset ``<sym>_supply``,
``<sym>_supply_usd``, ``<sym>_borrow``, ``<sym>_borrow_usd`` plus aggregate
columns) and a market frame of oracle prices for the simulated collaterals;
this adapter reproduces both shapes from ``borrower`` /
``borrower_collateral`` / ``sparklend_reserve_data`` / ``onchain_token_price``.

Aggregate semantics were reverse-engineered from BA's parquet rows and
reproduce them exactly:

- ``total_collateral_usd``  = Σ supply_usd over every supplied asset
- ``lltv``                  = Σ supply_usd × LT / total_collateral_usd
- ``health_factor``         = Σ supply_usd × LT / total_borrow_usd
- ``liquidation_incentive`` = Σ supply_usd × bonus / total_collateral_usd
  (assets with LT = 0 contribute supply to the denominator but nothing to the
  LT / bonus numerators — matching how BA's rows treat USDS-style collateral)

Deliberate deviations, documented in DATA_GAPS.md:

- e-mode categories are not indexed, so reserve-level LT/bonus are used for
  every user and ``emode_category`` is 0. For e-mode users this understates
  LT, hence understates HF — the conservative direction.
- Only SparkLend on Ethereum is implemented; other protocols keep parquet.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.core_model_orderbook_reader import _BTC_GROUP, _ETH_GROUP

logger = logging.getLogger(__name__)

# Collaterals whose price paths the model simulates. Everything else a user
# supplies is carried at constant USD (the model's "unmodeled" bucket).
MODELED_COLLATERALS = frozenset(_ETH_GROUP | _BTC_GROUP | {"XRP", "SOL", "JITOSOL", "HYPE"})

# Latest state per (user, token) per side, snapshot-read per db/migrations
# AGENTS: order by the snapshot-time key then processing_version, never build_id.
_POSITIONS = text("""
    WITH latest_borrow AS (
        SELECT DISTINCT ON (b.user_id, b.token_id) b.user_id, b.token_id, b.amount
        FROM borrower b
        JOIN protocol p ON p.id = b.protocol_id
        WHERE p.chain_id = :chain_id AND p.name = :protocol_name
        ORDER BY b.user_id, b.token_id, b.block_number DESC, b.block_version DESC, b.processing_version DESC
    ),
    latest_supply AS (
        SELECT DISTINCT ON (c.user_id, c.token_id) c.user_id, c.token_id, c.amount, c.collateral_enabled
        FROM borrower_collateral c
        JOIN protocol p ON p.id = c.protocol_id
        WHERE p.chain_id = :chain_id AND p.name = :protocol_name
        ORDER BY c.user_id, c.token_id, c.block_number DESC, c.block_version DESC, c.processing_version DESC
    )
    SELECT 'borrow' AS side, u.address AS user_address, t.symbol, t.decimals, lb.amount, true AS collateral_enabled
    FROM latest_borrow lb
    JOIN "user" u ON u.id = lb.user_id
    JOIN token t ON t.id = lb.token_id
    WHERE lb.amount > 0
    UNION ALL
    SELECT 'supply', u.address, t.symbol, t.decimals, ls.amount, ls.collateral_enabled
    FROM latest_supply ls
    JOIN "user" u ON u.id = ls.user_id
    JOIN token t ON t.id = ls.token_id
    WHERE ls.amount > 0
""")

_RESERVE_PARAMS = text("""
    SELECT DISTINCT ON (srd.token_id)
           t.symbol,
           srd.liquidation_threshold / 10000::numeric AS liquidation_threshold,
           srd.liquidation_bonus / 10000::numeric     AS liquidation_bonus
    FROM sparklend_reserve_data srd
    JOIN protocol p ON p.id = srd.protocol_id
    JOIN token t ON t.id = srd.token_id
    WHERE p.chain_id = :chain_id AND p.name = :protocol_name
      AND srd.liquidation_threshold IS NOT NULL
    ORDER BY srd.token_id, srd.block_number DESC, srd.block_version DESC, srd.processing_version DESC
""")

_ORACLE_PRICES = text("""
    SELECT DISTINCT ON (otp.token_id) t.symbol, otp.price_usd
    FROM onchain_token_price otp
    JOIN token t ON t.id = otp.token_id
    WHERE t.chain_id = :chain_id
    ORDER BY otp.token_id, otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
""")


@dataclass(frozen=True)
class PositionRow:
    side: str  # "borrow" | "supply"
    user_address: str
    symbol: str
    amount: float  # token units, decimals already applied
    collateral_enabled: bool


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
    prices: dict[str, float],
    loan_token: str,
) -> pd.DataFrame:
    """Assemble the wide per-user frame the model consumes.

    Users qualify by borrowing the market's loan token (``ALL`` = any borrow),
    exactly as BA's per-market parquet files were pre-filtered.
    """
    users: dict[str, dict[str, float]] = {}
    enabled: dict[tuple[str, str], bool] = {}
    for row in positions:
        cols = users.setdefault(row.user_address, {})
        sym = row.symbol.lower()
        if row.side == "borrow":
            cols[f"{sym}_borrow"] = cols.get(f"{sym}_borrow", 0.0) + row.amount
        else:
            cols[f"{sym}_supply"] = cols.get(f"{sym}_supply", 0.0) + row.amount
            enabled[(row.user_address, row.symbol.upper())] = row.collateral_enabled

    unpriced: set[str] = set()
    dropped_no_collateral: list[tuple[str, float]] = []
    records: list[dict] = []
    for address, cols in users.items():
        record: dict = {"wallet_address": address, "emode_category": 0}
        total_collateral = total_lt = total_bonus = total_borrow = 0.0
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
                total_collateral += usd
                lt, bonus = reserve_params.get(upper, (0.0, 0.0))
                # Collateral the user disabled protects nothing and pays no
                # bonus, same as an LT=0 reserve; the supply itself still shows.
                if enabled.get((address, upper), True) and lt > 0:
                    total_lt += usd * lt
                    total_bonus += usd * bonus
            else:
                total_borrow += usd
                if loan_token.upper() == "ALL" or upper == loan_token.upper():
                    borrows_loan_token = True
        if not borrows_loan_token or total_borrow <= 0:
            continue
        if total_collateral <= 0:
            # A borrower with no live collateral is already bad debt, not a
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
        record["liquidation_incentive"] = total_bonus / total_collateral
        records.append(record)

    if unpriced:
        raise ValueError(
            f"no oracle price for supplied/borrowed token(s) {sorted(unpriced)}; "
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


class PostgresPositionsReader:
    """``get_protocol_data`` from the live tables. SparkLend/Ethereum only."""

    def __init__(self, engine: AsyncEngine, chain_id: int = 1, protocol_name: str = "SparkLend") -> None:
        self._engine = engine
        self._chain_id = chain_id
        self._protocol_name = protocol_name

    async def get_protocol_data(
        self,
        protocol: str,
        network: str,
        morpho_market: str,
        loan_token: str,
        galaxy_type: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        if protocol.upper() != "SPARKLEND" or network.upper() != "ETHEREUM":
            raise ValueError(
                f"live positions are only implemented for SPARKLEND on ETHEREUM, got {protocol}/{network}. "
                "See app/risk_engine/core_model/DATA_GAPS.md."
            )
        params = {"chain_id": self._chain_id, "protocol_name": self._protocol_name}
        async with self._engine.connect() as conn:
            position_rows = (await conn.execute(_POSITIONS, params)).fetchall()
            reserve_rows = (await conn.execute(_RESERVE_PARAMS, params)).fetchall()
            price_rows = (await conn.execute(_ORACLE_PRICES, {"chain_id": self._chain_id})).fetchall()

        positions = [
            PositionRow(
                side=r.side,
                user_address="0x" + bytes(r.user_address).hex(),
                symbol=r.symbol,
                amount=float(Decimal(str(r.amount)) / (Decimal(10) ** int(r.decimals))),
                collateral_enabled=bool(r.collateral_enabled),
            )
            for r in position_rows
        ]
        reserve_params = {
            r.symbol.upper(): (float(r.liquidation_threshold), float(r.liquidation_bonus)) for r in reserve_rows
        }
        prices = {r.symbol.upper(): float(r.price_usd) for r in price_rows}

        users_df = build_users_frame(positions, reserve_params, prices, loan_token)
        supplied = {c.rsplit("_", 1)[0].upper() for c in users_df.columns if c.endswith("_supply")}
        market_df = build_market_frame(supplied, prices)
        logger.info(
            "positions loaded from live tables: %d borrowers, %d modeled collaterals (loan_token=%s)",
            len(users_df),
            len(market_df),
            loan_token,
        )
        return users_df, market_df
