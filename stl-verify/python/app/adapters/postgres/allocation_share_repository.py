from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.logging import get_logger

logger = get_logger(__name__)


class AllocationShareError(Exception):
    """Base class for allocation share lookup failures."""


class StaleShareError(AllocationShareError):
    """Raised when the most-recent supply row is older than the configured staleness window."""


class MissingShareError(AllocationShareError):
    """Raised when no balance or supply row is available for the (chain, token, wallet) triple."""


# Single-round-trip query: pull the latest balance row for the wallet and the
# latest supply row for the token, filtered to the same (chain, token).
# Both sub-queries order by (block_number, block_version, processing_version)
# DESC so they deterministically pick the newest version of each row.
# Pair the latest supply with a balance from the same (block_number,
# block_version) or any strictly earlier block. 
_SHARE_LOOKUP_SQL = """
WITH latest_supply AS (
    SELECT total_supply, scaled_total_supply,
           block_number, block_version,
           block_timestamp
    FROM token_total_supply
    WHERE chain_id = :chain_id
      AND token_id = :token_id
    ORDER BY block_number DESC, block_version DESC,
             processing_version DESC
    LIMIT 1
),
pinned_balance AS (
    SELECT ap.balance, ap.scaled_balance
    FROM allocation_position ap, latest_supply ls
    WHERE ap.chain_id = :chain_id
      AND ap.token_id = :token_id
      AND ap.proxy_address = :wallet
      AND (ap.block_number < ls.block_number
           OR (ap.block_number = ls.block_number
               AND ap.block_version = ls.block_version))
    ORDER BY ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC
    LIMIT 1
)
SELECT pb.balance, pb.scaled_balance,
       ls.total_supply, ls.scaled_total_supply,
       ls.block_timestamp AS supply_ts
FROM pinned_balance pb, latest_supply ls
"""


class PostgresAllocationShare:
    """Compute share = balance / totalSupply from indexed DB rows.

    Prefers the scaled pair when both ``scaled_balance`` and ``scaled_total_supply``
    are present (rebase-immune); falls back to the unscaled pair otherwise.

    Raises :class:`MissingShareError` if either side returns zero rows, or if no
    balance row exists at or before the latest supply row's block.
    Raises :class:`StaleShareError` if the most-recent supply row's
    ``block_timestamp`` is older than ``max_stale_seconds`` relative to ``now()``.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        chain_id: int,
        token_id: int,
        wallet_address: bytes,
        max_stale_seconds: int = 1800,
    ) -> None:
        if len(wallet_address) != 20:
            raise ValueError(f"wallet_address must be 20 bytes, got {len(wallet_address)}")
        self._engine = engine
        self._chain_id = chain_id
        self._token_id = token_id
        self._wallet = wallet_address
        self._max_stale_seconds = max_stale_seconds

    async def get_share(self) -> Decimal:
        try:
            async with self._engine.connect() as conn:
                await conn.exec_driver_sql("SET LOCAL statement_timeout = '5s'")
                result = await conn.execute(
                    text(_SHARE_LOOKUP_SQL),
                    {
                        "chain_id": self._chain_id,
                        "token_id": self._token_id,
                        "wallet": self._wallet,
                    },
                )
                row = result.fetchone()
        except SQLAlchemyError:
            logger.exception(
                "allocation_share: DB error for chain_id=%d token_id=%d",
                self._chain_id,
                self._token_id,
            )
            raise

        if row is None:
            # Either no supply row at all, or no balance row at-or-before the
            # latest supply's block.
            raise MissingShareError(
                f"no consistent balance+supply pair for chain_id={self._chain_id} token_id={self._token_id}"
            )

        supply_ts = row.supply_ts
        if supply_ts is None:
            raise MissingShareError(f"supply row missing for chain_id={self._chain_id} token_id={self._token_id}")

        now = datetime.now(timezone.utc)
        # `supply_ts` may be naive if the DB driver strips tzinfo; normalise.
        if supply_ts.tzinfo is None:
            supply_ts = supply_ts.replace(tzinfo=timezone.utc)
        age = (now - supply_ts).total_seconds()
        if age > self._max_stale_seconds:
            raise StaleShareError(
                f"supply row age {int(age)}s > {self._max_stale_seconds}s for "
                f"chain_id={self._chain_id} token_id={self._token_id}"
            )

        # Prefer the scaled pair when both sides have scaled values.
        if row.scaled_balance is not None and row.scaled_total_supply is not None:
            return self._ratio(row.scaled_balance, row.scaled_total_supply)
        return self._ratio(row.balance, row.total_supply)

    def _ratio(self, numerator: Decimal | int | float, denominator: Decimal | int | float) -> Decimal:
        num = Decimal(numerator)
        den = Decimal(denominator)
        if den == 0:
            logger.warning(
                "allocation_share: zero denominator for chain_id=%d token_id=%d; returning share=0",
                self._chain_id,
                self._token_id,
            )
            return Decimal("0")
        return num / den
