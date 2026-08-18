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
    WHERE t.chain_id = :chain_id AND upper(t.symbol) = ANY(:symbols)
    ORDER BY upper(t.symbol), date_trunc('day', p."timestamp"),
             p.block_number DESC, p.block_version DESC, p.processing_version DESC
""")


class PostgresPriceReader:
    def __init__(self, engine: AsyncEngine, min_days: int = 180, chain_id: int = 1) -> None:
        self._engine = engine
        self._min_days = min_days
        self._chain_id = chain_id

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        symbols = sorted({token.upper() for token in collateral_list})
        async with self._engine.connect() as conn:
            result = await conn.execute(_DAILY_CLOSES, {"symbols": symbols, "chain_id": self._chain_id})
            rows = result.fetchall()

        frame = pd.DataFrame(rows, columns=["symbol", "day", "close"])
        if not frame.empty:
            frame["close"] = frame["close"].astype(float)
        prices = frame.pivot(index="day", columns="symbol", values="close") if not frame.empty else pd.DataFrame()
        self._validate(prices, symbols)

        prices.index = pd.to_datetime(prices.index)
        prices.index.name = None
        prices.columns.name = None
        logger.info(
            "prices loaded from onchain_token_price: %d days x %d symbols (%s .. %s)",
            len(prices),
            len(prices.columns),
            prices.index.min().date(),
            prices.index.max().date(),
        )
        # The model addresses columns by the original collateral spelling.
        return prices.rename(columns={s: s for s in symbols})[[token.upper() for token in collateral_list]]

    def _validate(self, prices: pd.DataFrame, symbols: list[str]) -> None:
        problems: list[str] = []
        for symbol in symbols:
            if symbol not in prices.columns:
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
                + "\nBackfill with oracle-pricing-backfill (Erigon); see app/risk_engine/core_model/DATA_GAPS.md."
            )
