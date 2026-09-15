"""Daily close prices for the CORE model, from onchain_token_price.

Replaces ``prices_df.parquet``. The model consumes a frame of daily closes
indexed by day with one column per collateral symbol; the close is the last
oracle price observed each day.

On-chain oracle prices, not the offchain (CoinGecko) feed, on purpose: BA's
original used Yahoo Finance purely for convenience, while the model's own
liquidation mechanics run on oracle prices — calibrating the return series on
the same source the liquidations trigger on is more self-consistent. It is
also the repo's preferred data lineage, and the on-chain series has deeper,
gap-free history (the offchain feed has outage holes; see
``app/risk_engine/core_model/DATA_GAPS.md``).

Coverage is validated up front: GARCH calibration needs ``min_days`` of
contiguous daily history per symbol, and a series with holes would silently
distort the return series, so shortfalls and gaps fail the run instead.

One oracle feeds the whole series. Competing oracles hold separate rows for
the same token, and "newest block of the day" would hop between them from
one day to the next; the return series must not carry that noise. The
default is the SparkLend oracle because it is the only feed with a full
year of gap-free daily history for every modeled collateral on staging
(Chainlink is indexed since Feb 2026 only).

Two symbol classes have no on-chain oracle series and resolve differently:

- **Token-less assets** (XRP, HYPE — natives of ecosystems we do not index;
  ``offchain_price_asset.tokenless``) read the CoinGecko series in
  ``asset_price`` instead. Off-chain is acceptable here precisely because
  there is no oracle to be self-consistent with, and Maple's own liquidation
  process is a custodian margin call, not an on-chain oracle trigger.
- **Native BTC and ETH** proxy the WBTC/WETH oracle series
  (``_ORACLE_PROXIES``): the wrapped token trades 1:1 and its oracle history
  is deep and gap-free (approved in the 17 Aug #at_stl thread). A market
  holding both BTC and WBTC gets two identical columns; the copula's
  eigenvalue flooring (aggregator.py) factors that deliberately.
"""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

# Last oracle price per token per day. Symbols matched case-insensitively:
# the token registry stores display casing (cbBTC), the model uses upper.
# Within a day, the newest block wins, then processing_version per the
# snapshot-read rules.
_DAILY_CLOSES = text("""
    SELECT DISTINCT ON (upper(t.symbol), date_trunc('day', p."timestamp"))
           upper(t.symbol)                        AS symbol,
           date_trunc('day', p."timestamp")::date AS day,
           p.price_usd                            AS close
    FROM onchain_token_price p
    JOIN token t ON t.id = p.token_id
    WHERE t.chain_id = :chain_id AND p.oracle_id = :oracle_id AND upper(t.symbol) = ANY(:symbols)
    ORDER BY upper(t.symbol), date_trunc('day', p."timestamp"),
             p.block_number DESC, p.block_version DESC, p.processing_version DESC
""")

# Symbols are display labels, not identifiers: two priced tokens sharing one
# would interleave into a single series above, so that is refused up front.
_AMBIGUOUS_SYMBOLS = text("""
    SELECT upper(t.symbol) AS symbol, count(*) AS token_count
    FROM token t
    WHERE t.chain_id = :chain_id AND upper(t.symbol) = ANY(:symbols)
      -- "has this oracle ever priced the token" — token_price_current holds exactly
      -- one row per (oracle, token) that has, so it answers without walking the
      -- onchain_token_price history (VEC-672).
      AND EXISTS (SELECT 1 FROM token_price_current p WHERE p.token_id = t.id AND p.oracle_id = :oracle_id)
    GROUP BY upper(t.symbol)
    HAVING count(*) > 1
""")

_ORACLE_BY_NAME = text("SELECT id FROM oracle WHERE name = :oracle_name AND chain_id = :chain_id")

# Native symbols with no ERC-20 (hence no token row and no oracle series),
# served by the wrapped token's oracle under the requested label.
_ORACLE_PROXIES: dict[str, str] = {"BTC": "WBTC", "ETH": "WETH"}

# Which of the requested symbols are token-less off-chain assets. One catalog
# row per symbol, or the two sources' series would interleave into one column.
_TOKENLESS_SYMBOLS = text("""
    SELECT upper(a.symbol) AS symbol, count(*) AS asset_count
    FROM offchain_price_asset a
    WHERE a.tokenless AND upper(a.symbol) = ANY(:symbols)
    GROUP BY upper(a.symbol)
""")

# Last CoinGecko price per token-less asset per day; within a day the newest
# snapshot wins, then processing_version per the snapshot-read rules.
_TOKENLESS_DAILY_CLOSES = text("""
    SELECT DISTINCT ON (upper(a.symbol), date_trunc('day', p."timestamp"))
           upper(a.symbol)                        AS symbol,
           date_trunc('day', p."timestamp")::date AS day,
           p.price_usd                            AS close
    FROM asset_price p
    JOIN offchain_price_asset a ON a.id = p.asset_id
    WHERE a.tokenless AND upper(a.symbol) = ANY(:symbols)
    ORDER BY upper(a.symbol), date_trunc('day', p."timestamp"),
             p."timestamp" DESC, p.processing_version DESC
""")


def _pivot(rows) -> pd.DataFrame:
    """(symbol, day, close) rows into a day-indexed frame, one column per symbol."""
    frame = pd.DataFrame(rows, columns=["symbol", "day", "close"])
    if frame.empty:
        return pd.DataFrame()
    frame["close"] = frame["close"].astype(float)
    return frame.pivot(index="day", columns="symbol", values="close")


class PostgresPriceReader:
    def __init__(
        self, engine: AsyncEngine, min_days: int = 180, chain_id: int = 1, oracle_name: str = "sparklend"
    ) -> None:
        self._engine = engine
        self._min_days = min_days
        self._chain_id = chain_id
        self._oracle_name = oracle_name

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        symbols = sorted({token.upper() for token in collateral_list})
        async with self._engine.connect() as conn:
            tokenless = await self._tokenless_symbols(conn, symbols)
            oracle_requested = [s for s in symbols if s not in tokenless]
            frames = []
            if oracle_requested:
                frames.append(await self._oracle_closes(conn, oracle_requested))
            if tokenless:
                rows = (await conn.execute(_TOKENLESS_DAILY_CLOSES, {"symbols": sorted(tokenless)})).fetchall()
                frames.append(_pivot(rows))

        # concat unions the two day indexes WITHOUT sorting; statsmodels
        # refuses a non-monotonic date index at forecast time.
        prices = pd.concat(frames, axis=1).sort_index()
        self._validate(prices, symbols, tokenless)

        prices.index = pd.to_datetime(prices.index)
        prices.index.name = None
        prices.columns.name = None
        logger.info(
            "prices loaded: %d days x %d symbols (%s .. %s); %s via onchain_token_price (%s), %s via asset_price",
            len(prices),
            len(prices.columns),
            prices.index.min().date(),
            prices.index.max().date(),
            oracle_requested,
            self._oracle_name,
            sorted(tokenless),
        )
        # The port promises columns named exactly as the caller's labels.
        out = prices[[token.upper() for token in collateral_list]].copy()
        out.columns = list(collateral_list)
        return out

    async def _tokenless_symbols(self, conn, symbols: list[str]) -> set[str]:
        rows = (await conn.execute(_TOKENLESS_SYMBOLS, {"symbols": symbols})).fetchall()
        ambiguous = [r for r in rows if r.asset_count > 1]
        if ambiguous:
            raise ValueError(
                f"ambiguous token-less symbol(s) {[f'{r.symbol} ({r.asset_count} catalog rows)' for r in ambiguous]}: "
                "several offchain_price_asset rows share the symbol; refusing a mixed price series"
            )
        return {r.symbol for r in rows}

    async def _oracle_closes(self, conn, requested: list[str]) -> pd.DataFrame:
        """Daily oracle closes for ``requested``, native BTC/ETH served by their wrapped proxy's series."""
        oracle_id = (
            await conn.execute(_ORACLE_BY_NAME, {"oracle_name": self._oracle_name, "chain_id": self._chain_id})
        ).scalar_one_or_none()
        if oracle_id is None:
            raise ValueError(f"oracle {self._oracle_name!r} is not registered on chain {self._chain_id}")
        oracle_symbols = sorted({_ORACLE_PROXIES.get(s, s) for s in requested})
        params = {"symbols": oracle_symbols, "chain_id": self._chain_id, "oracle_id": oracle_id}
        ambiguous = (await conn.execute(_AMBIGUOUS_SYMBOLS, params)).fetchall()
        if ambiguous:
            raise ValueError(
                "ambiguous token symbol(s) "
                f"{[f'{r.symbol} ({r.token_count} tokens)' for r in ambiguous]}: multiple token rows "
                "on this chain carry oracle prices under the same symbol; refusing a mixed price series"
            )
        onchain = _pivot((await conn.execute(_DAILY_CLOSES, params)).fetchall())
        # Relabel to the requested symbols; BTC and WBTC in one market share the
        # WBTC series on purpose (module docstring).
        columns = {
            symbol: onchain[_ORACLE_PROXIES.get(symbol, symbol)]
            for symbol in requested
            if _ORACLE_PROXIES.get(symbol, symbol) in onchain.columns
        }
        return pd.DataFrame(columns)

    def _validate(self, prices: pd.DataFrame, symbols: list[str], tokenless: set[str]) -> None:
        problems: list[str] = []
        for symbol in symbols:
            if symbol not in prices.columns:
                if symbol in tokenless:
                    problems.append(f"{symbol}: no asset_price series for the token-less catalog row")
                elif symbol in _ORACLE_PROXIES:
                    problems.append(f"{symbol}: no oracle prices for its proxy {_ORACLE_PROXIES[symbol]}")
                else:
                    problems.append(f"{symbol}: no on-chain oracle prices (no token row or no oracle feed)")
                continue
            series = prices[symbol].dropna()
            # The window ends yesterday: today's close does not exist yet, so a
            # run early in the day must not fail on a day that is not over.
            end = (pd.Timestamp.now("UTC") - pd.Timedelta(days=1)).date()
            window = [ts.date() for ts in pd.date_range(end=end, periods=self._min_days, freq="D")]
            missing = [d for d in window if d not in set(series.index)]
            if missing:
                problems.append(
                    f"{symbol}: {len(missing)} of the last {self._min_days} days missing "
                    f"(first: {missing[0]}, last: {missing[-1]})"
                )
        if problems:
            raise ValueError(
                "price history insufficient for GARCH calibration:\n  "
                + "\n  ".join(problems)
                + "\nBackfill with oracle-pricing-backfill (Erigon) for oracle series, or "
                "offchain-price-backfill (docs/backfilling-offchain-prices.md) for token-less assets; "
                "see app/risk_engine/core_model/DATA_GAPS.md."
            )
