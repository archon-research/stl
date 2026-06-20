from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.exceptions import MissingShareError, StaleShareError
from app.logging import get_logger

logger = get_logger(__name__)


# Single-round-trip query: pull the latest balance row for the wallet and the
# latest supply row for the token, filtered to the same (chain, token).
# Both sub-queries order by (block_number, block_version, processing_version)
# DESC so they deterministically pick the newest version of each row.
# Pair the latest supply with a balance from the same (block_number,
# block_version) or any strictly earlier block.
#
# The `created_at >= NOW() - INTERVAL '14 days'` predicate on `pinned_balance`
# is a perf guardrail, not a correctness predicate. allocation_position is a
# columnstore hypertable with no segmentby on proxy_address (see migration
# 20260620_120000), so an unbounded `LIMIT 1` scans every chunk for the wallet.
# NOW() (not ls.block_timestamp) is required for plan-time chunk exclusion:
# TimescaleDB only prunes chunks for stable expressions of NOW(); CTE column
# refs are runtime-only and force a full scan. Safe because the caller rejects
# with StaleShareError when ls is older than max_stale_seconds (~minutes), so
# any row newer than ls is also within NOW() - 14d by a wide margin.
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
      -- See module-level comment for the NOW()-based chunk-pruning rationale.
      AND ap.created_at >= NOW() - INTERVAL '14 days'
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


# Same pinning semantics as ``_SHARE_LOOKUP_SQL`` — see that query's comment for
# the per-input rationale. The LATERAL joins force one per-(chain_id, token_id)
# segmentby-index lookup per input pair, but in a single round-trip; this turns
# the per-allocation ``get_share`` fan-out into a single network hop.
_BATCH_SHARE_LOOKUP_SQL = """
WITH inputs AS (
    SELECT unnest(CAST(:chain_ids AS int[]))   AS chain_id,
           unnest(CAST(:token_ids AS bigint[])) AS token_id
)
SELECT
    i.chain_id,
    i.token_id,
    pb.balance,
    pb.scaled_balance,
    ls.total_supply,
    ls.scaled_total_supply,
    ls.block_timestamp AS supply_ts
FROM inputs i
LEFT JOIN LATERAL (
    SELECT total_supply, scaled_total_supply,
           block_number, block_version,
           block_timestamp
    FROM token_total_supply
    WHERE chain_id = i.chain_id
      AND token_id = i.token_id
    ORDER BY block_number DESC, block_version DESC,
             processing_version DESC
    LIMIT 1
) ls ON true
LEFT JOIN LATERAL (
    SELECT ap.balance, ap.scaled_balance
    FROM allocation_position ap
    WHERE ap.chain_id = i.chain_id
      AND ap.token_id = i.token_id
      AND ap.proxy_address = :wallet
      AND ap.created_at >= NOW() - INTERVAL '14 days'
      AND (ap.block_number < ls.block_number
           OR (ap.block_number = ls.block_number
               AND ap.block_version = ls.block_version))
    ORDER BY ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC
    LIMIT 1
) pb ON true
"""


@dataclass(frozen=True)
class _ShareRequest:
    chain_id: int
    token_id: int


async def batch_fetch_shares(
    engine: AsyncEngine,
    requests: Iterable[_ShareRequest],
    wallet_address: bytes,
    max_stale_seconds: int = 1800,
) -> Mapping[tuple[int, int], Decimal | Exception]:
    """Resolve shares for many (chain_id, token_id) pairs in a single round-trip.

    Returns a mapping keyed by ``(chain_id, token_id)``. Per-pair failures are
    returned as ``MissingShareError`` / ``StaleShareError`` *values* rather than
    raised, so a single missing supply row does not poison the whole batch.
    Driver-level exceptions still propagate.

    The single query reproduces the per-pair semantics of
    ``PostgresAllocationShare.get_share`` (latest supply, balance pinned to
    that supply's block, 14d chunk-pruning bound, scaled-pair preference).
    """
    if len(wallet_address) != 20:
        raise ValueError(f"wallet_address must be 20 bytes, got {len(wallet_address)}")

    req_list = list(requests)
    if not req_list:
        return {}

    # Deduplicate on (chain_id, token_id) so a caller asking for the same pair
    # twice doesn't pay for it twice. Preserves the first occurrence.
    seen: set[tuple[int, int]] = set()
    unique: list[_ShareRequest] = []
    for r in req_list:
        key = (r.chain_id, r.token_id)
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)

    chain_ids = [r.chain_id for r in unique]
    token_ids = [r.token_id for r in unique]

    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("SET LOCAL statement_timeout = '5s'")
            result = await conn.execute(
                text(_BATCH_SHARE_LOOKUP_SQL),
                {"chain_ids": chain_ids, "token_ids": token_ids, "wallet": wallet_address},
            )
            rows = result.fetchall()
    except SQLAlchemyError:
        logger.exception(
            "allocation_share: DB error in batch lookup (n_pairs=%d)",
            len(unique),
        )
        raise

    now = datetime.now(timezone.utc)
    by_key: dict[tuple[int, int], Decimal | Exception] = {}
    seen_rows: set[tuple[int, int]] = set()
    for row in rows:
        key = (row.chain_id, row.token_id)
        seen_rows.add(key)
        by_key[key] = _resolve_row(row, now, max_stale_seconds)

    # Any input pair whose LEFT JOIN produced no row at all surfaces the same
    # way the per-instance path would: MissingShareError.
    for key in seen - seen_rows:
        by_key[key] = MissingShareError(f"no consistent balance+supply pair for chain_id={key[0]} token_id={key[1]}")
    return by_key


def _resolve_row(row, now: datetime, max_stale_seconds: int) -> Decimal | Exception:
    """Apply the same validity rules as ``PostgresAllocationShare.get_share``."""
    # `pb` half of the LEFT JOIN can be NULL if no pinned balance exists.
    if row.total_supply is None or row.supply_ts is None:
        return MissingShareError(
            f"no consistent balance+supply pair for chain_id={row.chain_id} token_id={row.token_id}"
        )
    if row.balance is None:
        return MissingShareError(
            f"no consistent balance+supply pair for chain_id={row.chain_id} token_id={row.token_id}"
        )

    supply_ts = row.supply_ts
    if supply_ts.tzinfo is None:
        supply_ts = supply_ts.replace(tzinfo=timezone.utc)
    age = (now - supply_ts).total_seconds()
    if age > max_stale_seconds:
        return StaleShareError(
            f"supply row age {int(age)}s > {max_stale_seconds}s for chain_id={row.chain_id} token_id={row.token_id}"
        )

    if row.scaled_balance is not None and row.scaled_total_supply is not None:
        return _safe_ratio(row.scaled_balance, row.scaled_total_supply, row.chain_id, row.token_id)
    return _safe_ratio(row.balance, row.total_supply, row.chain_id, row.token_id)


def _safe_ratio(
    numerator: Decimal | int | float,
    denominator: Decimal | int | float,
    chain_id: int,
    token_id: int,
) -> Decimal:
    num = Decimal(numerator)
    den = Decimal(denominator)
    if den == 0:
        logger.warning(
            "allocation_share: zero denominator for chain_id=%d token_id=%d; returning share=0",
            chain_id,
            token_id,
        )
        return Decimal("0")
    return num / den
