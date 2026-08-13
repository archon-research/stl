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


# Canonical share-lookup query. Resolves ``share = balance / total_supply`` for
# one or more ``(chain_id, token_id)`` pairs against a single wallet, in one
# round-trip. Single-pair callers go through :func:`fetch_share` (which wraps
# this with N=1 and unwraps per-pair errors into raises).
#
# Semantics — kept stable because both the batch and single-pair callers rely
# on them:
#   * Latest supply: most-recent ``token_total_supply`` row per pair, broken
#     by ``(block_number, block_version, processing_version) DESC``.
#   * Pinned balance: most-recent ``allocation_position`` row at or strictly
#     before the supply row's ``(block_number, block_version)`` — never *after*
#     — so a balance from a later block is never paired with an earlier supply
#     when ``totalSupply()`` failed on the newer block.
#   * 14d ``created_at`` bound: perf guardrail, not correctness.
#     ``allocation_position`` is a TimescaleDB columnstore hypertable and
#     ``LIMIT 1`` over the segmentby-indexed ``(chain_id, token_id,
#     proxy_address)`` triple still needs a chunk-prune to avoid scanning
#     every chunk for the wallet. TimescaleDB only prunes for stable
#     expressions of ``NOW()`` (CTE column refs like ``ls.block_timestamp``
#     are runtime-only and force a full scan). Safe because the caller rejects
#     with :class:`StaleShareError` when the supply row is older than
#     ``max_stale_seconds`` (~minutes), so any row newer than the supply is
#     within ``NOW() - 14d`` by a wide margin.
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
    """Resolve shares for many ``(chain_id, token_id)`` pairs in one round-trip.

    Returns a mapping keyed by ``(chain_id, token_id)``. Per-pair failures are
    returned as :class:`MissingShareError` / :class:`StaleShareError` *values*
    rather than raised, so a single missing supply row does not poison the
    whole batch. Driver-level exceptions still propagate.
    """
    if len(wallet_address) != 20:
        raise ValueError(f"wallet_address must be 20 bytes, got {len(wallet_address)}")

    seen: set[tuple[int, int]] = set()
    unique: list[_ShareRequest] = []
    for r in requests:
        key = (r.chain_id, r.token_id)
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)

    if not unique:
        return {}

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

    for key in seen - seen_rows:
        by_key[key] = MissingShareError(f"no consistent balance+supply pair for chain_id={key[0]} token_id={key[1]}")
    return by_key


async def fetch_share(
    engine: AsyncEngine,
    chain_id: int,
    token_id: int,
    wallet_address: bytes,
    max_stale_seconds: int = 1800,
) -> Decimal:
    """Resolve a single share, raising on lookup failure.

    Thin wrapper around :func:`batch_fetch_shares` so the single-pair path
    shares the same SQL, the same balance-pinning predicates, and the same
    validity rules. Per-pair error values are re-raised here to match the
    "raises on failure" contract callers expect.
    """
    by_pair = await batch_fetch_shares(
        engine=engine,
        requests=[_ShareRequest(chain_id=chain_id, token_id=token_id)],
        wallet_address=wallet_address,
        max_stale_seconds=max_stale_seconds,
    )
    value = by_pair[(chain_id, token_id)]
    if isinstance(value, Exception):
        raise value
    return value


def _resolve_row(row, now: datetime, max_stale_seconds: int) -> Decimal | Exception:
    """Apply the per-pair validity rules to a single result row."""
    if row.total_supply is None or row.supply_ts is None or row.balance is None:
        # LEFT JOIN can null either half: no latest_supply, or no pinned_balance.
        return MissingShareError(
            f"no consistent balance+supply pair for chain_id={row.chain_id} token_id={row.token_id}"
        )

    supply_ts = row.supply_ts
    if supply_ts.tzinfo is None:
        # Some drivers strip tzinfo; treat naive timestamps as UTC so the age
        # comparison below stays well-defined.
        supply_ts = supply_ts.replace(tzinfo=timezone.utc)
    age = (now - supply_ts).total_seconds()
    if age > max_stale_seconds:
        return StaleShareError(
            f"supply row age {int(age)}s > {max_stale_seconds}s for chain_id={row.chain_id} token_id={row.token_id}"
        )

    if row.scaled_balance is not None and row.scaled_total_supply is not None:
        # Prefer the scaled pair when both sides are present: rebase-immune.
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
