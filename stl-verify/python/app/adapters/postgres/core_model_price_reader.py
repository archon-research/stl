"""Daily close prices for the CORE model, from offchain_token_price.

Replaces ``prices_df.parquet``. The model consumes a frame of daily closes
indexed by day with one column per collateral symbol; the close is the last
price observed each day (the indexer ticks every ~5 minutes).

Coverage is validated up front: GARCH calibration needs ``min_days`` of
contiguous daily history per symbol, and a series with holes would silently
distort the return series, so shortfalls and gaps fail the run instead. The
known shortfalls and the backfill procedure live in
``app/risk_engine/core_model/DATA_GAPS.md``.
"""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

# Last price per asset per day. Symbols are matched case-insensitively:
# offchain_price_asset stores display casing (cbBTC), the model uses upper.
_DAILY_CLOSES = text("""
    SELECT DISTINCT ON (upper(a.symbol), date_trunc('day', p."timestamp"))
           upper(a.symbol)                        AS symbol,
           date_trunc('day', p."timestamp")::date AS day,
           p.price_usd                            AS close
    FROM offchain_price_asset a
    JOIN offchain_token_price p ON p.token_id = a.token_id AND p.source_id = a.source_id
    WHERE upper(a.symbol) = ANY(:symbols)
    ORDER BY upper(a.symbol), date_trunc('day', p."timestamp"), p."timestamp" DESC
""")


class PostgresPriceReader:
    def __init__(self, engine: AsyncEngine, min_days: int = 180) -> None:
        self._engine = engine
        self._min_days = min_days

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        symbols = sorted({token.upper() for token in collateral_list})
        async with self._engine.connect() as conn:
            result = await conn.execute(_DAILY_CLOSES, {"symbols": symbols})
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
            "prices loaded from offchain_token_price: %d days x %d symbols (%s .. %s)",
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
                problems.append(f"{symbol}: no offchain_price_asset row / no prices at all")
                continue
            series = prices[symbol].dropna()
            # The window ends yesterday: today's close does not exist yet, so a
            # run early in the day must not fail on a day that is not over.
            end = pd.Timestamp.now("UTC").date() - pd.Timedelta(days=1)
            window = pd.date_range(end=end, periods=self._min_days, freq="D").date
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
                + "\nBackfill with offchain-price-backfill; see app/risk_engine/core_model/DATA_GAPS.md."
            )
