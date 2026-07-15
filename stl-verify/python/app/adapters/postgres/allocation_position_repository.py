import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from opentelemetry import trace
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._time_window import (
    clamp_limit,
    required_time_window_clause,
    time_bucket_expr,
)
from app.domain.entities.allocation import (
    ChainMetadata,
    DirectAssetHolding,
    EthAddress,
    Prime,
    ProtocolMetadata,
    ReceiptTokenPosition,
)
from app.domain.entities.allocation_activity import AllocationActivityEvent
from app.domain.entities.time_series_bucket import (
    AllocationActivityBucket,
    ExposureBucket,
    TotalCapitalBucket,
)
from app.domain.proxy_kind import ProxyKind, classify_proxy, subproxy_addresses

# USDS (mainnet). A prime's treasury USDS held in its SubProxy wallet is its
# total capital; this isolates that token from any other SubProxy holding.
_USDS_ADDRESS_HEX = "dc035d45d973e3ec169d2276ddab16f1e407384f"

# Tokens priced from allocation_position.underlying_value x the underlying's
# oracle price, rather than the legacy balance x own-oracle price that leaves
# them unpriced. Two member classes:
#   * Vault share tokens (underlying_value = on-chain redeemable value, e.g.
#     convertToAssets). These graduate out of the allowlist by being registered
#     in receipt_token, which routes them through the receipt path's
#     redeemable-value pricing instead of this direct-holdings branch.
#   * Non-ERC20 pool positions (underlying_value = tracker-computed full
#     position value). The row's address is the pool contract, which can never
#     be a receipt_token or have its own oracle, so these are permanent
#     members.
# A deliberately curated set (VEC-450): the general widening to every vault,
# and syrupUSDC, are owned separately. Add addresses here to widen.
_UNDERLYING_VALUE_TOKEN_HEXES = frozenset(
    {
        "38464507e02c983f20428a6e8566693fe9e422a9",  # sparkPrimeUSDC1
        # AUSD/USDC Uni V3 pool position: not an ERC20, valued by the
        # tracker-computed underlying_value in USDC units.
        "bafead7c60ea473758ed6c6021505e8bbd7e8e5d",
    }
)
_UNDERLYING_VALUE_TOKEN_ADDRS = [bytes.fromhex(h) for h in _UNDERLYING_VALUE_TOKEN_HEXES]

logger = logging.getLogger(__name__)

_ALLOCATION_ACTIVITY_LIMIT = 1000


def _escape_like_pattern(value: str) -> str:
    r"""Escape LIKE metacharacters to prevent pattern injection.

    LIKE patterns support wildcards: % (any chars), _ (single char), \ (escape).
    User input must be escaped to prevent unintended wildcard matching.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _strip_hex_prefix(tx_hash: str | None) -> str | None:
    """Strip a leading ``0x``/``0X`` prefix so the hex can be decoded by Postgres.

    Defense in depth: API validators already canonicalize uppercase ``0X`` to
    lowercase ``0x``, but this repository is also reachable from non-HTTP
    callers (jobs, ad-hoc scripts) that may not normalize first.
    """
    if tx_hash is None:
        return None
    if tx_hash.startswith(("0x", "0X")):
        return tx_hash[2:]
    return tx_hash


def _safe_decimal(value: Any, field_name: str, row_identifier: Any = None) -> Decimal:
    """Convert value to Decimal with error context for debugging.

    Raises ValueError with context if conversion fails, helping identify
    which field and row caused the issue in production.
    """
    try:
        if value is None:
            return Decimal("0")
        return Decimal(str(value))
    except (ValueError, InvalidOperation, TypeError) as exc:
        logger.error(
            f"Invalid decimal value in database field {field_name}",
            extra={
                "field_name": field_name,
                "row_identifier": str(row_identifier) if row_identifier else None,
                "value": str(value),
                "value_type": type(value).__name__,
            },
        )
        raise ValueError(f"Database contains invalid numeric value for {field_name}: {value}") from exc


class AllocationRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def list_chains(self) -> list[ChainMetadata]:
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT chain_id, name
                        FROM chain
                        ORDER BY chain_id ASC
                        """
                    )
                )
                rows = result.fetchall()

            return [ChainMetadata(chain_id=row.chain_id, name=row.name) for row in rows]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch chains from database",
                extra={"error_type": type(exc).__name__, "error_message": str(exc)},
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching chains: {exc}") from exc

    async def list_protocols(self) -> list[ProtocolMetadata]:
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT id, chain_id, encode(address, 'hex') AS encode, name
                        FROM protocol
                        WHERE name IS NOT NULL
                        ORDER BY chain_id ASC, name ASC
                        """
                    )
                )
                rows = result.fetchall()

            return [
                ProtocolMetadata(
                    id=row.id,
                    chain_id=row.chain_id,
                    encode=row.encode,
                    name=row.name,
                )
                for row in rows
            ]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch protocols from database",
                extra={"error_type": type(exc).__name__, "error_message": str(exc)},
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching protocols: {exc}") from exc

    async def list_primes(self) -> list[Prime]:
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT DISTINCT ON (proxy_address)
                            p.name,
                            encode(proxy_address, 'hex') AS address
                        FROM allocation_position ap
                        JOIN prime p ON p.id = ap.prime_id
                        ORDER BY proxy_address, block_number DESC
                        """
                    )
                )
                primes: list[Prime] = []
                for row in result:
                    address = "0x" + row.address
                    # SubProxy wallets share a prime_id with the ALM proxy; surfacing
                    # them here would duplicate each prime in /v1/primes.
                    if classify_proxy(address) is not ProxyKind.ALM:
                        continue
                    primes.append(Prime(id=address, name=row.name, address=address))
                return primes
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch primes from database",
                extra={"error_type": type(exc).__name__, "error_message": str(exc)},
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching primes: {exc}") from exc

    async def prime_exists(self, prime_address: EthAddress) -> bool:
        # Match what list_receipt_token_positions / get_*_usd_exposure can actually
        # answer: presence in allocation_position.proxy_address. /v1/primes also
        # defines "prime" as "has any allocation_position row", so this is the
        # same identity the public API exposes.
        query = text(
            """
            SELECT 1
            FROM allocation_position
            WHERE proxy_address = decode(:address_hex, 'hex')
            LIMIT 1
            """
        )

        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(query, {"address_hex": prime_address.hex})).fetchone()
            return row is not None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to check prime existence in database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while checking if prime {prime_address} exists: {exc}") from exc

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(
                    _RECEIPT_TOKEN_POSITIONS_SQL,
                    {"proxy_hex": prime_id.hex},
                )
                rows = result.fetchall()
            positions = [
                ReceiptTokenPosition(
                    chain_id=row.chain_id,
                    receipt_token_id=row.receipt_token_id,
                    receipt_token_address="0x" + row.receipt_token_address,
                    underlying_token_id=row.underlying_token_id,
                    underlying_token_address="0x" + row.underlying_token_address,
                    symbol=row.symbol,
                    underlying_symbol=row.underlying_symbol,
                    protocol_name=row.protocol_name,
                    balance=_safe_decimal(row.balance, "balance", row.receipt_token_id),
                    amount_usd=(
                        _safe_decimal(row.amount_usd, "amount_usd", row.receipt_token_id)
                        if row.amount_usd is not None
                        else None
                    ),
                    latest_activity_at=row.latest_activity_at,
                    latest_activity_action=row.latest_activity_action,
                    latest_activity_amount=(
                        _safe_decimal(
                            row.latest_activity_amount,
                            "latest_activity_amount",
                            row.receipt_token_id,
                        )
                        if row.latest_activity_amount is not None
                        else None
                    ),
                )
                for row in rows
            ]
            self._record_receipt_valuation_gaps(prime_id, rows)
            return positions
        except asyncio.CancelledError:
            raise
        except ValueError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch receipt token positions from database",
                extra={
                    "prime_id": str(prime_id),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching receipt token positions: {exc}") from exc

    async def list_direct_asset_holdings(self, prime_id: EthAddress) -> list[DirectAssetHolding]:
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(
                    _DIRECT_ASSET_HOLDINGS_SQL,
                    {"proxy_hex": prime_id.hex, "uv_token_addrs": _UNDERLYING_VALUE_TOKEN_ADDRS},
                )
                holdings = [
                    DirectAssetHolding(
                        chain_id=row.chain_id,
                        token_id=row.token_id,
                        token_address="0x" + row.token_address,
                        symbol=row.symbol,
                        balance=_safe_decimal(row.balance, "balance", row.token_id),
                        amount_usd=(
                            _safe_decimal(row.amount_usd, "amount_usd", row.token_id)
                            if row.amount_usd is not None
                            else None
                        ),
                        latest_activity_at=row.latest_activity_at,
                        latest_activity_action=row.latest_activity_action,
                        latest_activity_amount=(
                            _safe_decimal(
                                row.latest_activity_amount,
                                "latest_activity_amount",
                                row.token_id,
                            )
                            if row.latest_activity_amount is not None
                            else None
                        ),
                        underlying_token_id=row.underlying_token_id,
                        underlying_token_address=(
                            "0x" + row.underlying_token_address if row.underlying_token_address is not None else None
                        ),
                        underlying_symbol=row.underlying_symbol,
                    )
                    for row in result
                ]
            self._record_unpriced_holdings(prime_id, holdings)
            return holdings
        except asyncio.CancelledError:
            raise
        except ValueError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch direct asset holdings from database",
                extra={
                    "prime_id": str(prime_id),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching direct asset holdings: {exc}") from exc

    @staticmethod
    def _record_receipt_valuation_gaps(prime_id: EthAddress, rows: Sequence[Any]) -> None:
        """Surface receipt positions whose valuation degraded.

        Mirrors ``_record_unpriced_holdings`` for the receipt path, with two
        distinct signals:

        * ``unpriced``: ``amount_usd`` resolved to NULL, i.e. a missing underlying
          oracle price, or a position/registry underlying divergence refused by
          the valuation CASE. Receipt positions price through the curated
          registry and are expected to price, so a null is a coverage
          regression worth alerting on.
        * ``balance_basis``: ``underlying_value`` is NULL, so the read fell
          back to the share-balance basis, a silent methodology fallback
          (expected only for rows written before the column existed) that gets
          its own signal so it cannot linger unnoticed.
        """
        unpriced = [row.symbol for row in rows if row.amount_usd is None]
        if unpriced:
            trace.get_current_span().set_attribute("allocations.receipt_positions.unpriced", len(unpriced))
            logger.warning(
                "Receipt-token positions resolved to no USD value",
                extra={
                    "prime_id": str(prime_id),
                    "unpriced_count": len(unpriced),
                    "total_count": len(rows),
                    "unpriced_symbols": unpriced,
                },
            )
        balance_basis = [row.symbol for row in rows if row.underlying_value is None]
        if balance_basis:
            trace.get_current_span().set_attribute("allocations.receipt_positions.balance_basis", len(balance_basis))
            logger.warning(
                "Receipt-token positions valued on the share-balance fallback (underlying_value missing)",
                extra={
                    "prime_id": str(prime_id),
                    "balance_basis_count": len(balance_basis),
                    "total_count": len(rows),
                    "balance_basis_symbols": balance_basis,
                },
            )

    @staticmethod
    def _record_unpriced_holdings(prime_id: EthAddress, holdings: list[DirectAssetHolding]) -> None:
        """Surface direct holdings that resolved to no oracle price.

        A null ``amount_usd`` is legitimate for assets with no oracle feed (LP/
        curve shares), but it is indistinguishable at the row level from a
        coverage regression — a token that should price but silently stopped
        (oracle disabled, reorg, backfill gap). Recording the unpriced count as
        a span attribute lets that be alerted on in the tracing backend instead
        of being discovered by a user noticing a missing USD value.

        An allowlisted vault (``_UNDERLYING_VALUE_TOKEN_HEXES``) is priced only by
        its ``underlying_value`` and is expected to price, so a null there is not
        routine — it means the underlying oracle dropped out. It is surfaced
        separately (warning + own span attribute) rather than folded into the
        LP/curve nulls, so it can be alerted on distinctly.
        """
        unpriced = [h for h in holdings if h.amount_usd is None]
        if not unpriced:
            return
        trace.get_current_span().set_attribute("allocations.direct_holdings.unpriced", len(unpriced))
        logger.debug(
            "Direct asset holdings without an oracle price",
            extra={
                "prime_id": str(prime_id),
                "unpriced_count": len(unpriced),
                "total_count": len(holdings),
                "unpriced_symbols": [h.symbol for h in unpriced],
            },
        )
        allowlisted_unpriced = [h for h in unpriced if h.token_address[2:].lower() in _UNDERLYING_VALUE_TOKEN_HEXES]
        if not allowlisted_unpriced:
            return
        trace.get_current_span().set_attribute(
            "allocations.direct_holdings.allowlisted_unpriced", len(allowlisted_unpriced)
        )
        logger.warning(
            "Allowlisted token resolved to no USD value (underlying_value or underlying oracle price missing)",
            extra={
                "prime_id": str(prime_id),
                "allowlisted_unpriced_symbols": [h.symbol for h in allowlisted_unpriced],
            },
        )

    @staticmethod
    def _record_empty_total_capital(prime_address: EthAddress, buckets: list[TotalCapitalBucket]) -> None:
        """Surface a total-capital series that gapfilled to all-``None``.

        A prime passes the ``prime_exists`` check on its ALM ``proxy_address``,
        but total capital is read from a *different* row set — the SubProxy
        treasury USDS scoped by ``_USDS_ADDRESS_HEX``. If that set is empty (no
        SubProxy configured for the prime, treasury not yet indexed, or the USDS
        address drifting out of the ``token`` registry) the gapfill still returns
        a full window of buckets, every one ``None``. That is a valid 200 for a
        brand-new prime but is indistinguishable from a coverage regression, so
        record it for alerting rather than letting it surface as a blank chart.
        """
        if not buckets or any(b.total_capital_usd is not None for b in buckets):
            return
        trace.get_current_span().set_attribute("allocations.total_capital.all_null", True)
        logger.warning(
            "Total capital series is entirely empty for a known prime",
            extra={
                "prime_address": str(prime_address),
                "bucket_count": len(buckets),
            },
        )

    async def get_usd_exposure(self, receipt_token_id: int, prime_id: EthAddress) -> Decimal:
        """Return the redeemable-value USD exposure of the prime's receipt-token holding.

        ``COALESCE(underlying_value, balance) × underlying price``, multiplied
        in SQL (NUMERIC) like every other valuation read; rationale on
        ``_RECEIPT_TOKEN_POSITIONS_SQL``.
        """
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(
                    _USD_EXPOSURE_SQL,
                    {"receipt_token_id": receipt_token_id, "proxy_hex": prime_id.hex},
                )
                row = result.fetchone()

            if row is None:
                raise ValueError(
                    f"no position or price found for receipt_token_id={receipt_token_id} prime_id={prime_id}"
                )
            if row.usd_exposure is None:
                raise ValueError(
                    f"position underlying diverges from the registry underlying for "
                    f"receipt_token_id={receipt_token_id} prime_id={prime_id}; refusing to price"
                )

            return _safe_decimal(row.usd_exposure, "usd_exposure", f"receipt_token_id={receipt_token_id}")
        except asyncio.CancelledError:
            raise
        except ValueError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch USD exposure from database",
                extra={
                    "receipt_token_id": receipt_token_id,
                    "prime_id": str(prime_id),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching USD exposure: {exc}") from exc

    async def get_total_usd_exposure(self, prime_id: EthAddress) -> Decimal:
        """Return total priced USD exposure across all current receipt-token positions."""
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(_TOTAL_USD_EXPOSURE_SQL, {"proxy_hex": prime_id.hex})
                row = result.fetchone()

            if row is None or row.total_usd_exposure is None:
                return Decimal("0")

            return _safe_decimal(row.total_usd_exposure, "total_usd_exposure", f"prime_id={prime_id}")
        except asyncio.CancelledError:
            raise
        except ValueError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch total USD exposure from database",
                extra={
                    "prime_id": str(prime_id),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching total USD exposure: {exc}") from exc

    async def list_allocation_activity(
        self,
        *,
        prime_id: EthAddress | None = None,
        chain_id: int | None = None,
        protocol_name: str | None = None,
        action_type: str | None = None,
        token_symbol: str | None = None,
        tx_hash: str | None = None,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[AllocationActivityEvent]:
        # Escape LIKE metacharacters to prevent pattern injection
        params = {
            "prime_hex": prime_id.hex if prime_id else None,
            "chain_id": chain_id,
            "protocol_name": _escape_like_pattern(protocol_name) if protocol_name else None,
            "action_type": action_type,
            "token_symbol": _escape_like_pattern(token_symbol) if token_symbol else None,
            "tx_hash": _strip_hex_prefix(tx_hash),
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "limit": clamp_limit(limit, _ALLOCATION_ACTIVITY_LIMIT),
        }

        logger.debug(
            "Executing allocation activity query",
            extra={
                "prime_id": str(prime_id) if prime_id else None,
                "chain_id": chain_id,
                "limit": params["limit"],
                "has_time_filter": from_timestamp is not None,
            },
        )

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(_ALLOCATION_ACTIVITY_SQL, params)
                rows = result.fetchall()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Allocation activity query failed",
                extra={
                    "params": {k: str(v) if v is not None else None for k, v in params.items()},
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching allocation activity: {exc}") from exc

        return [
            AllocationActivityEvent(
                chain_id=row.chain_id,
                prime_address="0x" + row.prime_address,
                prime_name=row.prime_name,
                protocol_name=row.protocol_name,
                token_id=row.token_id,
                token_symbol=row.token_symbol,
                action_type=row.action_type,
                tx_amount=_safe_decimal(row.tx_amount, "tx_amount", f"block={row.block_number}"),
                balance=_safe_decimal(row.balance, "balance", f"block={row.block_number}"),
                tx_hash=("0x" + row.tx_hash) if row.tx_hash else None,
                log_index=row.log_index,
                block_number=row.block_number,
                block_version=row.block_version,
                created_at=row.created_at,
            )
            for row in rows
        ]

    async def list_activity_buckets(
        self,
        *,
        prime_id: EthAddress | None = None,
        chain_id: int | None = None,
        protocol_name: str | None = None,
        action_type: str | None = None,
        token_symbol: str | None = None,
        tx_hash: str | None = None,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[AllocationActivityBucket]:
        """Return allocation activity counts and tx-amount sums per time bucket."""
        params = {
            "prime_hex": prime_id.hex if prime_id else None,
            "chain_id": chain_id,
            "protocol_name": _escape_like_pattern(protocol_name) if protocol_name else None,
            "action_type": action_type,
            "token_symbol": _escape_like_pattern(token_symbol) if token_symbol else None,
            "tx_hash": _strip_hex_prefix(tx_hash),
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "bucket_seconds": bucket_seconds,
            "limit": clamp_limit(limit, _ALLOCATION_ACTIVITY_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(_ALLOCATION_ACTIVITY_BUCKETS_SQL, params)
                rows = result.fetchall()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Allocation activity bucket query failed",
                extra={
                    "params": {k: str(v) if v is not None else None for k, v in params.items()},
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while aggregating allocation activity: {exc}") from exc

        return [
            AllocationActivityBucket(
                bucket_start=row.bucket_start,
                event_count=row.event_count,
                total_tx_amount=_safe_decimal(row.total_tx_amount, "total_tx_amount", "aggregate"),
                net_flow_usd=_safe_decimal(row.net_flow_usd, "net_flow_usd", "aggregate"),
            )
            for row in rows
        ]

    async def list_total_capital_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[TotalCapitalBucket]:
        """Return the last observed treasury USDS balance per time bucket (LOCF).

        A prime's total capital is the USDS held in its SubProxy wallet, which
        shares the prime's ``prime_id`` but a distinct ``proxy_address``. The
        prime is identified by its ALM ``proxy_address``; the matching SubProxy
        is the one sharing that ``prime_id``. USDS is dollar-pegged, so the raw
        balance is the USD figure. Buckets with no observation carry the prior
        value forward; leading buckets before the first observation are ``None``.
        """
        subproxies = [bytes.fromhex(address[2:]) for address in subproxy_addresses()]
        query = text(
            """
            WITH target AS (
                SELECT prime_id
                FROM allocation_position
                WHERE proxy_address = decode(:address_hex, 'hex')
                LIMIT 1
            )
            SELECT
                time_bucket_gapfill(
                    make_interval(secs => :bucket_seconds),
                    ap.created_at,
                    CAST(:from_timestamp AS TIMESTAMPTZ),
                    CAST(:to_timestamp AS TIMESTAMPTZ)
                ) AS bucket_start,
                locf(last(ap.balance, ap.created_at)) AS total_capital_usd
            FROM allocation_position ap
            JOIN token t ON t.id = ap.token_id
            WHERE ap.prime_id = (SELECT prime_id FROM target)
              AND ap.proxy_address IN :subproxy_addrs
              AND t.address = decode(:usds_hex, 'hex')
            """
            + required_time_window_clause("ap.created_at")
            + """
            GROUP BY bucket_start
            ORDER BY bucket_start DESC
            LIMIT :limit
            """
        ).bindparams(bindparam("subproxy_addrs", expanding=True))

        params = {
            "address_hex": prime_address.hex,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "bucket_seconds": bucket_seconds,
            "subproxy_addrs": subproxies,
            "usds_hex": _USDS_ADDRESS_HEX,
            "limit": clamp_limit(limit, _ALLOCATION_ACTIVITY_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch total capital buckets from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching total capital buckets for prime {prime_address}: {exc}"
            ) from exc

        buckets = [
            TotalCapitalBucket(
                bucket_start=row.bucket_start,
                total_capital_usd=(
                    _safe_decimal(row.total_capital_usd, "total_capital_usd", "aggregate")
                    if row.total_capital_usd is not None
                    else None
                ),
            )
            for row in rows
        ]
        self._record_empty_total_capital(prime_address, buckets)
        return buckets

    async def get_latest_total_capital_usd(self, prime_address: EthAddress) -> Decimal | None:
        """Return the prime's latest treasury USDS balance (Total Risk Capital), or None.

        The treasury is the USDS held in the prime's SubProxy wallet (shares the
        prime's ``prime_id``, distinct ``proxy_address``). USDS is dollar-pegged,
        so the balance is the USD figure. Returns ``None`` when the prime has no
        SubProxy treasury position.
        """
        subproxies = [bytes.fromhex(address[2:]) for address in subproxy_addresses()]
        query = text(
            """
            SELECT ap.balance
            FROM allocation_position ap
            JOIN token t ON t.id = ap.token_id
            WHERE ap.prime_id = (
                SELECT prime_id FROM allocation_position
                WHERE proxy_address = decode(:address_hex, 'hex')
                LIMIT 1
            )
              AND ap.proxy_address IN :subproxy_addrs
              AND t.address = decode(:usds_hex, 'hex')
            ORDER BY ap.block_number DESC, ap.block_version DESC,
                     ap.processing_version DESC, ap.log_index DESC
            LIMIT 1
            """
        ).bindparams(bindparam("subproxy_addrs", expanding=True))
        params = {
            "address_hex": prime_address.hex,
            "subproxy_addrs": subproxies,
            "usds_hex": _USDS_ADDRESS_HEX,
        }

        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(query, params)).fetchone()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch latest total capital from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching latest total capital for prime {prime_address}: {exc}"
            ) from exc

        if row is None or row.balance is None:
            return None
        return _safe_decimal(row.balance, "balance", f"prime_id={prime_address}")

    async def list_exposure_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[ExposureBucket]:
        """Return priced receipt-token exposure per time bucket (LOCF gap-filled).

        Per bucket and receipt-token position, the last observed redeemable
        value (``COALESCE(underlying_value, balance)``; rationale on
        ``_RECEIPT_TOKEN_POSITIONS_SQL``) is carried forward and valued at the
        *latest* underlying oracle price (via the protocol-bound oracle), then
        summed across positions. The position size is the historical driver;
        the price is held at its latest value because ``onchain_token_price``
        is change-only, so a bucketed price-LOCF would drop stable assets whose
        last price change predates the window. This is exact for the
        dollar-pegged positions that dominate the book; for volatile
        underlyings (e.g. WETH) historical buckets use the current price (a
        bounded approximation). Leading buckets before the first observation
        are ``None``. Direct holdings (no receipt token) are excluded, matching
        the exposure basis of the risk-capital endpoint.

        Windows spanning the ``underlying_value`` rollout boundary show a
        valuation-basis step: buckets fed by pre-rollout rows carry the share
        balance, later ones the redeemable value, which is frozen at the last
        position event until the next one.
        """
        query = text(
            """
            WITH position_buckets AS (
                SELECT
                    rt.id AS receipt_token_id,
                    rt.underlying_token_id,
                    rt.protocol_id,
                    time_bucket_gapfill(
                        make_interval(secs => :bucket_seconds),
                        ap.created_at,
                        CAST(:from_timestamp AS TIMESTAMPTZ),
                        CAST(:to_timestamp AS TIMESTAMPTZ)
                    ) AS bucket,
                    locf(last(
                        CASE
                            WHEN ap.underlying_token_id IS NOT NULL
                             AND ap.underlying_token_id <> rt.underlying_token_id
                            THEN NULL
                            ELSE COALESCE(ap.underlying_value, ap.balance)
                        END,
                        ap.created_at)) AS valuation_units
                FROM allocation_position ap
                JOIN token t ON t.id = ap.token_id
                JOIN receipt_token rt
                    ON rt.receipt_token_address = t.address AND rt.chain_id = ap.chain_id
                WHERE ap.proxy_address = decode(:address_hex, 'hex')
                  AND ap.created_at >= CAST(:from_timestamp AS TIMESTAMPTZ)
                  AND ap.created_at <= CAST(:to_timestamp AS TIMESTAMPTZ)
                GROUP BY rt.id, rt.underlying_token_id, rt.protocol_id, bucket
            )
            SELECT
                b.bucket AS bucket_start,
                SUM(b.valuation_units * COALESCE(px.price_usd, 0)) AS exposure_usd
            FROM position_buckets b
            LEFT JOIN LATERAL (
                SELECT otp.price_usd
                FROM onchain_token_price otp
                JOIN protocol_oracle po
                    ON po.oracle_id = otp.oracle_id AND po.protocol_id = b.protocol_id
                WHERE otp.token_id = b.underlying_token_id
                -- enabled-mapping filter (rationale on _DIRECT_ASSET_HOLDINGS_SQL):
                -- a retired source's tail must not serve any bucket after
                -- retirement (nor, given the no-history simplification, before).
                  AND EXISTS (
                      SELECT 1 FROM oracle_asset oa
                      WHERE oa.oracle_id = otp.oracle_id
                        AND oa.token_id = otp.token_id
                        AND oa.enabled
                  )
                ORDER BY otp.block_number DESC, otp.block_version DESC,
                         otp.processing_version DESC, otp.oracle_id DESC
                LIMIT 1
            ) px ON TRUE
            GROUP BY b.bucket
            ORDER BY b.bucket DESC
            LIMIT :limit
            """
        )
        params = {
            "address_hex": prime_address.hex,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "bucket_seconds": bucket_seconds,
            "limit": clamp_limit(limit, _ALLOCATION_ACTIVITY_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch exposure buckets from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching exposure buckets for prime {prime_address}: {exc}"
            ) from exc

        return [
            ExposureBucket(
                bucket_start=row.bucket_start,
                exposure_usd=(
                    _safe_decimal(row.exposure_usd, "exposure_usd", "aggregate")
                    if row.exposure_usd is not None
                    else None
                ),
            )
            for row in rows
        ]


# Match positions to receipt tokens only by receipt_token_address: a prime's
# direct holding of an underlying asset (e.g. raw USDT in the proxy wallet)
# is not a position in any receipt token that wraps it, and attributing it to
# every such receipt token double-counts and inflates per-token balances.
#
# Valuation basis (shared by every receipt position-valuation read: this query,
# ``_USD_EXPOSURE_SQL``, ``_TOTAL_USD_EXPOSURE_SQL``, and the exposure-buckets
# query): ``COALESCE(underlying_value, balance) x underlying price``.
# ``underlying_value`` is the on-chain redeemable value (convertToAssets) in
# underlying units, so non-1:1 vault shares (syrupUSDC-like) are no longer
# priced as if one share redeemed one underlying unit; NULL (rows written
# before the column existed) falls back to the balance basis, and 1:1 aTokens
# are unchanged (their underlying_value equals balance by construction).
# Flow-level reads (``net_flow_usd``) convert each flow at its row's share
# ratio, borrowing the nearest same-token row's when the row lacks one; see
# ``_ALLOCATION_ACTIVITY_BUCKETS_SQL``.
#
# The underlying is priced via the registry's ``receipt_token.underlying_token_id``,
# not the position's own ``underlying_token_id`` (verified identical on every
# receipt row carrying one; warehouse, 2026-07-09: 5484 rows, 0 divergent). A
# row whose own underlying diverges from the registry's indicates an ingest
# bug: the registry price would multiply a position value denominated in a
# different unit, so every valuation read refuses to price it (NULL) rather
# than producing a plausible wrong number. The positions list surfaces the
# refusal via ``_record_receipt_valuation_gaps``; the exposure-buckets read
# nulls the divergent observation before ``last()``, so ``locf`` carries the
# last pre-divergence value (stale but unit-correct) without telemetry.
_RECEIPT_TOKEN_POSITIONS_SQL = text("""
    WITH latest_receipt_positions AS (
        SELECT DISTINCT ON (rt.id)
            rt.id                                    AS receipt_token_id,
            rt.symbol                                AS symbol,
            encode(rt.receipt_token_address, 'hex')  AS receipt_token_address,
            ut.id                                    AS underlying_token_id,
            ut.symbol                                AS underlying_symbol,
            encode(ut.address, 'hex')                AS underlying_token_address,
            pr.id                                    AS protocol_id,
            pr.name                                  AS protocol_name,
            ap.chain_id                              AS chain_id,
            ap.balance                               AS balance,
            ap.underlying_value                      AS underlying_value,
            ap.underlying_token_id                   AS position_underlying_token_id,
            ap.created_at                            AS latest_activity_at,
            ap.direction                             AS latest_activity_action,
            ap.tx_amount                             AS latest_activity_amount
        FROM allocation_position ap
        JOIN token t          ON t.id = ap.token_id
        JOIN receipt_token rt ON rt.receipt_token_address = t.address AND rt.chain_id = ap.chain_id
        JOIN token ut         ON ut.id = rt.underlying_token_id
        JOIN protocol pr      ON pr.id = rt.protocol_id AND pr.chain_id = ap.chain_id
        WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
        ORDER BY rt.id,
                 ap.block_number DESC, ap.block_version DESC,
                 ap.processing_version DESC, ap.log_index DESC
    )
    SELECT
        p.chain_id,
        p.receipt_token_id,
        p.receipt_token_address,
        p.underlying_token_id,
        p.underlying_token_address,
        p.symbol,
        p.underlying_symbol,
        p.protocol_name,
        p.balance,
        p.underlying_value,
        CASE
            WHEN p.position_underlying_token_id IS NOT NULL
             AND p.position_underlying_token_id <> p.underlying_token_id
            THEN NULL
            ELSE COALESCE(p.underlying_value, p.balance) * lp.price_usd
        END AS amount_usd,
        p.latest_activity_at,
        p.latest_activity_action,
        p.latest_activity_amount
    FROM latest_receipt_positions p
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
            AND po.protocol_id = p.protocol_id
        WHERE otp.token_id = p.underlying_token_id
        -- enabled-mapping filter + oracle_id tiebreak (rationale on _DIRECT_ASSET_HOLDINGS_SQL).
          AND EXISTS (
              SELECT 1 FROM oracle_asset oa
              WHERE oa.oracle_id = otp.oracle_id
                AND oa.token_id = otp.token_id
                AND oa.enabled
          )
        ORDER BY otp.block_number DESC, otp.block_version DESC,
                 otp.processing_version DESC, otp.oracle_id DESC
        LIMIT 1
    ) lp ON TRUE
    WHERE p.balance > 0
    ORDER BY p.balance DESC
""")


# A "direct asset holding" is a position whose token has no row in the
# receipt_token table — i.e. the prime holds the token itself rather than
# a registered protocol wrapper for it. Receipt-token positions are returned
# by ``_RECEIPT_TOKEN_POSITIONS_SQL``; this query is the complementary set.
#
# Pricing has no protocol context here (the token is held bare), so the latest
# oracle price for the token across any oracle is used rather than the
# protocol-bound oracle that ``_RECEIPT_TOKEN_POSITIONS_SQL`` resolves through
# ``protocol_oracle``. Tokens with no oracle price (e.g. LP/curve shares) yield
# a null ``amount_usd`` rather than being dropped.
# An allowlisted vault share token (``_UNDERLYING_VALUE_TOKEN_HEXES``) is priced
# ONLY by its redeemable ``underlying_value`` x the UNDERLYING asset's oracle
# price (``up``); if either is absent the result is NULL (a surfaced coverage
# gap, see ``_record_unpriced_holdings``), never the legacy ``balance x own
# price``. For these tokens ``balance`` is a share count, so the legacy basis
# would be a silent methodology flip to a wrong value. Every non-allowlisted
# token keeps the legacy ``balance x px`` valuation, byte-identical to before.
# Allowlisted rows also project the underlying's identity (underlying_token_id
# / address / symbol) when the row carries one: ``amount_usd`` derives from the
# underlying's oracle price and consumers key drilldowns off ``underlying_*``,
# so the identity must travel with the price basis. All other cases emit NULL
# and the endpoint falls back to the held token itself. The projection is
# atomic by construction (allowlist gate + symbol presence live on the ``ut``
# join): either all three columns emit or none do, because ``token.symbol`` is
# nullable and a partial identity would let the endpoint compose a hybrid of
# underlying id/address with the held token's symbol.
_DIRECT_ASSET_HOLDINGS_SQL = text("""
    WITH latest_positions AS (
        SELECT DISTINCT ON (ap.token_id)
            ap.chain_id,
            ap.token_id,
            ap.balance,
            ap.underlying_value,
            ap.underlying_token_id,
            ap.created_at AS latest_activity_at,
            ap.direction AS latest_activity_action,
            ap.tx_amount AS latest_activity_amount
        FROM allocation_position ap
        WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
        ORDER BY ap.token_id,
                 ap.block_number DESC, ap.block_version DESC,
                 ap.processing_version DESC, ap.log_index DESC
    )
    SELECT
        lp.chain_id,
        lp.token_id,
        encode(t.address, 'hex') AS token_address,
        t.symbol                 AS symbol,
        lp.balance,
        CASE
            WHEN t.address IN :uv_token_addrs
            THEN lp.underlying_value * up.price_usd
            ELSE lp.balance * px.price_usd
        END AS amount_usd,
        ut.id                     AS underlying_token_id,
        encode(ut.address, 'hex') AS underlying_token_address,
        ut.symbol                 AS underlying_symbol,
        lp.latest_activity_at,
        lp.latest_activity_action,
        lp.latest_activity_amount
    FROM latest_positions lp
    JOIN token t ON t.id = lp.token_id
    LEFT JOIN token ut
        ON ut.id = lp.underlying_token_id
        AND t.address IN :uv_token_addrs
        AND ut.symbol IS NOT NULL
    LEFT JOIN receipt_token rt
        ON rt.receipt_token_address = t.address AND rt.chain_id = lp.chain_id
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        WHERE otp.token_id = lp.token_id
        -- Enabled-mapping filter (CANONICAL rationale; every current/latest
        -- onchain_token_price read across the API repositories carries this
        -- EXISTS and points here). A price row is eligible only while its
        -- (oracle_id, token_id) still has an ENABLED oracle_asset mapping.
        -- Retiring a source (oracle_asset.enabled = false) drops it from every
        -- latest-price read immediately at read time, not merely from future
        -- collection. The snapshot-key ordering and the oracle_id tiebreak
        -- below cannot rescue correctness on their own: a reorg can republish a
        -- frozen/retired source's row at a FRESH (max) block while a
        -- change-suppressed live feed writes no newer row, so the retired
        -- source would otherwise beat the live one indefinitely.
        -- Tradeoff: oracle_asset.enabled carries no history, so a retired
        -- source vanishes from ALL price reads including the historical/LOCF
        -- time-series buckets — even buckets before its retirement that
        -- legitimately used it. Accepted simplification; per-block temporal
        -- enablement tracking is out of scope.
          AND EXISTS (
              SELECT 1 FROM oracle_asset oa
              WHERE oa.oracle_id = otp.oracle_id
                AND oa.token_id = otp.token_id
                AND oa.enabled
          )
        -- oracle_id breaks ties when multiple oracles price the same token at
        -- identical (block_number, block_version, processing_version), e.g. a
        -- frozen source re-emitted by a republished block next to a live one.
        -- Deterministic, and the higher id is the later-registered oracle; the
        -- real ordering signal stays the snapshot keys, because a retired
        -- source stops producing new rows and loses on recency from then on.
        -- Every ordered onchain_token_price read carries this tiebreaker.
        ORDER BY otp.block_number DESC, otp.block_version DESC,
                 otp.processing_version DESC, otp.oracle_id DESC
        LIMIT 1
    ) px ON TRUE
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        WHERE otp.token_id = lp.underlying_token_id
        -- enabled-mapping filter + oracle_id tiebreak (rationale on the px LATERAL above).
          AND EXISTS (
              SELECT 1 FROM oracle_asset oa
              WHERE oa.oracle_id = otp.oracle_id
                AND oa.token_id = otp.token_id
                AND oa.enabled
          )
        ORDER BY otp.block_number DESC, otp.block_version DESC,
                 otp.processing_version DESC, otp.oracle_id DESC
        LIMIT 1
    ) up ON TRUE
    WHERE rt.id IS NULL AND lp.balance > 0
    ORDER BY lp.balance DESC
""").bindparams(bindparam("uv_token_addrs", expanding=True))


# Redeemable-value basis; rationale (incl. the divergence refusal) on
# _RECEIPT_TOKEN_POSITIONS_SQL. The open-position filter stays on the raw share
# balance, and the units x price multiplication happens here in NUMERIC
# arithmetic for consistency with the other valuation reads.
_USD_EXPOSURE_SQL = text("""
WITH latest_position AS (
    SELECT
        ap.balance,
        COALESCE(ap.underlying_value, ap.balance) AS valuation_units,
        ap.underlying_token_id AS position_underlying_token_id,
        rt.underlying_token_id AS registry_underlying_token_id
    FROM allocation_position ap
    JOIN receipt_token rt ON rt.id = :receipt_token_id
    JOIN token t ON t.id = ap.token_id AND t.address = rt.receipt_token_address
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = ap.chain_id
    WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
    ORDER BY ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC
    LIMIT 1
),
latest_price AS (
    SELECT otp.price_usd
    FROM onchain_token_price otp
    JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
    JOIN receipt_token rt ON rt.protocol_id = po.protocol_id AND rt.id = :receipt_token_id
    WHERE otp.token_id = rt.underlying_token_id
    -- enabled-mapping filter + oracle_id tiebreak (rationale on _DIRECT_ASSET_HOLDINGS_SQL).
      AND EXISTS (
          SELECT 1 FROM oracle_asset oa
          WHERE oa.oracle_id = otp.oracle_id
            AND oa.token_id = otp.token_id
            AND oa.enabled
      )
    ORDER BY otp.block_number DESC, otp.block_version DESC,
             otp.processing_version DESC, otp.oracle_id DESC
    LIMIT 1
)
SELECT
    CASE
        WHEN lb.position_underlying_token_id IS NOT NULL
         AND lb.position_underlying_token_id <> lb.registry_underlying_token_id
        THEN NULL
        ELSE lb.valuation_units * lp.price_usd
    END AS usd_exposure
FROM latest_position lb
CROSS JOIN latest_price lp
WHERE lb.balance > 0
""")


# Redeemable-value basis; rationale (incl. the divergence refusal) on
# _RECEIPT_TOKEN_POSITIONS_SQL. A refused or unpriced position contributes
# nothing to the SUM (NULL terms are skipped), matching the positions list
# where it shows as NULL amount_usd.
_TOTAL_USD_EXPOSURE_SQL = text("""
WITH latest_receipt_positions AS (
    SELECT DISTINCT ON (rt.id)
        rt.id                  AS receipt_token_id,
        rt.underlying_token_id AS underlying_token_id,
        rt.protocol_id         AS protocol_id,
        ap.balance,
        ap.underlying_value,
        ap.underlying_token_id AS position_underlying_token_id
    FROM allocation_position ap
    JOIN token t          ON t.id = ap.token_id
    JOIN receipt_token rt ON rt.receipt_token_address = t.address AND rt.chain_id = ap.chain_id
    JOIN protocol pr      ON pr.id = rt.protocol_id AND pr.chain_id = ap.chain_id
    WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
    ORDER BY rt.id,
             ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC
)
SELECT COALESCE(SUM(
    CASE
        WHEN p.position_underlying_token_id IS NOT NULL
         AND p.position_underlying_token_id <> p.underlying_token_id
        THEN NULL
        ELSE COALESCE(p.underlying_value, p.balance) * lp.price_usd
    END
), 0) AS total_usd_exposure
FROM latest_receipt_positions p
LEFT JOIN LATERAL (
    SELECT otp.price_usd
    FROM onchain_token_price otp
    JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
        AND po.protocol_id = p.protocol_id
    WHERE otp.token_id = p.underlying_token_id
    -- enabled-mapping filter + oracle_id tiebreak (rationale on _DIRECT_ASSET_HOLDINGS_SQL).
      AND EXISTS (
          SELECT 1 FROM oracle_asset oa
          WHERE oa.oracle_id = otp.oracle_id
            AND oa.token_id = otp.token_id
            AND oa.enabled
      )
    ORDER BY otp.block_number DESC, otp.block_version DESC,
             otp.processing_version DESC, otp.oracle_id DESC
    LIMIT 1
) lp ON TRUE
WHERE p.balance > 0
""")


_ALLOCATION_ACTIVITY_SQL = text("""
SELECT
    ap.chain_id,
    encode(ap.proxy_address, 'hex') AS prime_address,
    p.name AS prime_name,
    protocol_match.protocol_name,
    ap.token_id,
    t.symbol AS token_symbol,
    ap.direction AS action_type,
    ap.tx_amount,
    ap.balance,
    encode(ap.tx_hash, 'hex') AS tx_hash,
    ap.log_index,
    ap.block_number,
    ap.block_version,
    ap.created_at
FROM allocation_position ap
JOIN prime p ON p.id = ap.prime_id
JOIN token t ON t.id = ap.token_id
LEFT JOIN LATERAL (
    SELECT pr.name AS protocol_name, 1 AS match_priority
    FROM receipt_token rt
    JOIN protocol pr ON pr.id = rt.protocol_id
    WHERE pr.chain_id = ap.chain_id
      AND rt.receipt_token_address = t.address

    UNION ALL

    SELECT pr.name AS protocol_name, 2 AS match_priority
    FROM receipt_token rt
    JOIN protocol pr ON pr.id = rt.protocol_id
    WHERE pr.chain_id = ap.chain_id
      AND rt.underlying_token_id = t.id
    ORDER BY match_priority
    LIMIT 1
) AS protocol_match ON TRUE
WHERE (CAST(:prime_hex AS TEXT) IS NULL OR ap.proxy_address = decode(CAST(:prime_hex AS TEXT), 'hex'))
    AND ap.direction IS NOT NULL
    AND ap.tx_amount IS NOT NULL
    AND ap.balance IS NOT NULL
    AND ap.log_index IS NOT NULL
    AND ap.block_number IS NOT NULL
    AND ap.block_version IS NOT NULL
    AND ap.created_at IS NOT NULL
    AND (CAST(:chain_id AS INTEGER) IS NULL OR ap.chain_id = CAST(:chain_id AS INTEGER))
    AND (CAST(:protocol_name AS TEXT) IS NULL OR LOWER(COALESCE(protocol_match.protocol_name, ''))
         LIKE '%' || LOWER(CAST(:protocol_name AS TEXT)) || '%' ESCAPE '\')
    AND (CAST(:action_type AS TEXT) IS NULL OR LOWER(COALESCE(ap.direction::text, '')) =
         LOWER(CAST(:action_type AS TEXT)))
    AND (CAST(:token_symbol AS TEXT) IS NULL OR LOWER(COALESCE(t.symbol, ''))
         LIKE '%' || LOWER(CAST(:token_symbol AS TEXT)) || '%' ESCAPE '\')
    AND (CAST(:tx_hash AS TEXT) IS NULL OR encode(ap.tx_hash, 'hex') = LOWER(CAST(:tx_hash AS TEXT)))
    AND (CAST(:from_timestamp AS TIMESTAMPTZ) IS NULL OR ap.created_at >= CAST(:from_timestamp AS TIMESTAMPTZ))
    AND (CAST(:to_timestamp AS TIMESTAMPTZ) IS NULL OR ap.created_at <= CAST(:to_timestamp AS TIMESTAMPTZ))
ORDER BY ap.created_at DESC, ap.block_number DESC, ap.block_version DESC, ap.log_index DESC
LIMIT :limit
""")


# Aggregated counterpart of _ALLOCATION_ACTIVITY_SQL: same filters, bucketed by
# time. Reuses the shared window/bucket SQL helpers. Bounds are required so the
# JOIN/filter set matches the raw query exactly. ``tx_amount`` is an unsigned
# magnitude (direction carries the sign), so ``SUM`` is already ``>= 0``; the
# ``GREATEST(..., 0)`` clamp on ``total_tx_amount`` is defensive only, guarding
# the non-negativity invariant on ``AllocationActivityBucket`` against any stray
# negative row rather than letting it surface as a 500.
#
# ``net_flow_usd`` is the SIGNED net flow valued in USD. The sign comes from
# ``direction`` (tx_amount is a magnitude): inflows add, outflows subtract, and
# sweeps are internal position moves that net to zero and are excluded. It lets
# the UI reconstruct a total-allocation balance series by anchoring at the
# current total and cumulating net flows backwards.
#
# Only RECEIPT-TOKEN flows are valued. Directly-held underlying tokens (treasury
# USDS/USDC/DAI/...) are excluded on purpose: their outflows are recorded mostly
# as ``sweep`` (excluded above) while their inflows are ``in``, so pricing them
# yields gross inflow throughput with the offsetting legs dropped — net-positive
# every bucket and orders of magnitude larger than the true balance change. That
# made the reconstructed curve ramp up from zero instead of tracking the roughly
# flat real balance. The anchor still sums BOTH receipt and direct holdings, so
# the series sits at the full current total and moves only with receipt-token
# flows; direct-holding moves are not reflected until their in/out/sweep
# classification is trustworthy. Flows with no receipt-token oracle price
# contribute 0.
#
# Receipt-token flows are valued at a share ratio resolved in three tiers.
# Every allocation_position row is per-tx (tx_hash, log_index, direction) and
# carries ``balance`` and ``underlying_value`` read at its own pinned block:
#
#   1. Own row usable (``underlying_value`` present, ``balance > 0``):
#      ``underlying_value / balance`` is the vault's share ratio at that tx's
#      block, so ``tx_amount x ratio`` converts the share-denominated flow
#      into underlying units before pricing (a syrupUSDC-like deposit is no
#      longer understated by its ~1.17 ratio). ``underlying_value = 0`` with
#      ``balance > 0`` (drained vault) is a real ratio of 0, matching the
#      position basis.
#   2. Own row unusable (NULL ``underlying_value`` on rows written before the
#      column existed, or ``balance = 0`` on a full exit's own row): borrow
#      the ratio from the nearest same-token row that has a usable one. The
#      share ratio is a property of the vault, not of any position, so any
#      proxy's row pins it; sweep snapshots give dense coverage. Nearest by
#      block distance (the at-or-before row wins ties), then the usual
#      block_version / processing_version / log_index DESC tiebreaks for
#      determinism. The borrowed ratio is an approximation whose error is
#      bounded by the vault's ratio drift across the block gap to the donor
#      row (basis points per day for a yield vault), though the gap can be
#      large for flows before the token's first valued row.
#   3. No usable same-token row at all: fall back to the raw ``tx_amount``
#      (ratio 1). This genuinely means "never valued" (e.g. a token whose
#      rows all predate the column), not "this row happened to lack a value".
#
# A row whose own ``underlying_token_id`` diverges from the registry's is
# refused before any tier applies (contributes nothing): its
# ``underlying_value`` is denominated in a different asset than the registry
# price multiplies, per the refusal policy on ``_RECEIPT_TOKEN_POSITIONS_SQL``
# and matching the SUM behavior of ``_TOTAL_USD_EXPOSURE_SQL``. Divergent rows
# are excluded from tier-2 candidacy for the same reason.
#
# Remaining approximation: even a tier-1 ratio is the post-tx position ratio
# at the flow's block, not a per-leg execution price. Acceptable because a
# yield vault's share ratio moves slowly, so the same-block position ratio is
# indistinguishable from the execution price at this read's resolution.
_ALLOCATION_ACTIVITY_BUCKETS_SQL = text(f"""
WITH receipt_token_price AS (
    -- Latest underlying oracle price per receipt token, computed ONCE per token
    -- (a few dozen rows) rather than once per activity event (~100k+). The
    -- main query then hash-joins this by token address.
    SELECT
        rt.chain_id,
        rt.receipt_token_address,
        rt.underlying_token_id,
        (
            SELECT otp.price_usd
            FROM onchain_token_price otp
            JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
            WHERE po.protocol_id = rt.protocol_id
              AND otp.token_id = rt.underlying_token_id
            -- enabled-mapping filter + oracle_id tiebreak (rationale on _DIRECT_ASSET_HOLDINGS_SQL).
              AND EXISTS (
                  SELECT 1 FROM oracle_asset oa
                  WHERE oa.oracle_id = otp.oracle_id
                    AND oa.token_id = otp.token_id
                    AND oa.enabled
              )
            ORDER BY otp.block_number DESC, otp.block_version DESC,
                     otp.processing_version DESC, otp.oracle_id DESC
            LIMIT 1
        ) AS price_usd
    FROM receipt_token rt
),
share_ratio_stream AS (
    -- One pass over each receipt token's FULL history (deliberately not
    -- filtered by prime or time window: the nearest valued row may belong to
    -- any position and sit outside the queried window), keeping donor rows
    -- (usable, unit-consistent ratio) and rows that need to borrow one.
    -- donor_key is [block_number, block_version, processing_version,
    -- log_index, ratio]: lexicographic MAX/MIN picks the nearest donor with
    -- the usual version tiebreaks, and the ratio rides along as the last
    -- element. Per-row probes into allocation_position are not an option
    -- here: it is a columnar-compressed hypertable with no token segmentby,
    -- so a per-flow LATERAL cannot prune batches and re-scans the token's
    -- block range per flow row; this stream plus the window pass below
    -- resolves every needed ratio in one scan.
    SELECT
        ap2.token_id,
        ap2.chain_id,
        ap2.block_number,
        CASE
            WHEN ap2.underlying_value IS NOT NULL
             AND ap2.balance > 0
             AND ap2.underlying_token_id = rt2.underlying_token_id
            THEN ARRAY[
                ap2.block_number, ap2.block_version,
                ap2.processing_version, ap2.log_index,
                ap2.underlying_value / ap2.balance
            ]
        END AS donor_key
    FROM allocation_position ap2
    JOIN token t2 ON t2.id = ap2.token_id
    JOIN receipt_token rt2
        ON rt2.receipt_token_address = t2.address AND rt2.chain_id = ap2.chain_id
    WHERE (
            ap2.underlying_value IS NOT NULL
        AND ap2.balance > 0
        AND ap2.underlying_token_id = rt2.underlying_token_id
        )
       OR (
            ap2.direction IN ('in', 'out')
        AND (ap2.underlying_value IS NULL OR ap2.balance = 0)
        )
),
nearest_share_ratio AS MATERIALIZED (
    -- Tier-2 nearest donor ratio per (token, chain, block). The RANGE frames
    -- make every row of a block see the same prev/next donor (same-block
    -- donors count at distance 0 on both sides), so DISTINCT collapses the
    -- stream to one row per block and the equi-join below cannot fan out.
    -- prev wins distance ties: the at-or-before row.
    --
    -- MATERIALIZED is load-bearing: inlined, the planner merge-joins this map
    -- on (chain_id, token_id) alone with block_number as a join filter,
    -- sorting the entire activity scan and rescanning each token's map rows
    -- per activity row (minutes on the warehouse). Fenced, it builds the map
    -- once (~tens of thousands of rows) and hash-joins on all three keys.
    SELECT DISTINCT
        token_id,
        chain_id,
        block_number,
        CASE
            WHEN prev_donor IS NULL THEN next_donor[5]
            WHEN next_donor IS NULL THEN prev_donor[5]
            WHEN block_number - prev_donor[1] <= next_donor[1] - block_number
                THEN prev_donor[5]
            ELSE next_donor[5]
        END AS ratio
    FROM (
        SELECT
            token_id,
            chain_id,
            block_number,
            MAX(donor_key) OVER (
                PARTITION BY token_id, chain_id ORDER BY block_number
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS prev_donor,
            -- ORDER BY DESC + UNBOUNDED PRECEDING accumulates from the
            -- partition's end, covering the same rows (block >= current) as
            -- CURRENT ROW .. UNBOUNDED FOLLOWING over ASC would; the latter
            -- is a shrinking frame Postgres recomputes from scratch per row,
            -- quadratic per partition (measured 64s on the warehouse).
            MIN(CASE WHEN donor_key IS NOT NULL THEN ARRAY[
                donor_key[1], -donor_key[2], -donor_key[3], -donor_key[4],
                donor_key[5]
            ] END) OVER (
                PARTITION BY token_id, chain_id ORDER BY block_number DESC
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS next_donor
        FROM share_ratio_stream
    ) donors
)
SELECT
    {time_bucket_expr("ap.created_at")} AS bucket_start,
    COUNT(*) AS event_count,
    GREATEST(COALESCE(SUM(ap.tx_amount), 0), 0) AS total_tx_amount,
    COALESCE(SUM(
        CASE ap.direction
            WHEN 'in' THEN ap.tx_amount
            WHEN 'out' THEN -ap.tx_amount
            ELSE 0
        END
        * CASE
            WHEN ap.underlying_token_id IS NOT NULL
             AND ap.underlying_token_id <> price.underlying_token_id THEN 0
            WHEN ap.underlying_value IS NOT NULL AND ap.balance > 0
                THEN ap.underlying_value / ap.balance
            ELSE COALESCE(nearest_ratio.ratio, 1)
        END
        * COALESCE(price.price_usd, 0)
    ), 0) AS net_flow_usd
FROM allocation_position ap
JOIN prime p ON p.id = ap.prime_id
JOIN token t ON t.id = ap.token_id
LEFT JOIN LATERAL (
    SELECT pr.name AS protocol_name, 1 AS match_priority
    FROM receipt_token rt
    JOIN protocol pr ON pr.id = rt.protocol_id
    WHERE pr.chain_id = ap.chain_id
      AND rt.receipt_token_address = t.address

    UNION ALL

    SELECT pr.name AS protocol_name, 2 AS match_priority
    FROM receipt_token rt
    JOIN protocol pr ON pr.id = rt.protocol_id
    WHERE pr.chain_id = ap.chain_id
      AND rt.underlying_token_id = t.id
    ORDER BY match_priority
    LIMIT 1
) AS protocol_match ON TRUE
LEFT JOIN receipt_token_price price
    ON price.receipt_token_address = t.address
    AND price.chain_id = ap.chain_id
LEFT JOIN nearest_share_ratio nearest_ratio
    ON nearest_ratio.token_id = ap.token_id
    AND nearest_ratio.chain_id = ap.chain_id
    AND nearest_ratio.block_number = ap.block_number
WHERE (CAST(:prime_hex AS TEXT) IS NULL OR ap.proxy_address = decode(CAST(:prime_hex AS TEXT), 'hex'))
    AND ap.direction IS NOT NULL
    AND ap.tx_amount IS NOT NULL
    AND ap.created_at IS NOT NULL
    AND (CAST(:chain_id AS INTEGER) IS NULL OR ap.chain_id = CAST(:chain_id AS INTEGER))
    AND (CAST(:protocol_name AS TEXT) IS NULL OR LOWER(COALESCE(protocol_match.protocol_name, ''))
         LIKE '%' || LOWER(CAST(:protocol_name AS TEXT)) || '%' ESCAPE '\\')
    AND (CAST(:action_type AS TEXT) IS NULL OR LOWER(COALESCE(ap.direction::text, '')) =
         LOWER(CAST(:action_type AS TEXT)))
    AND (CAST(:token_symbol AS TEXT) IS NULL OR LOWER(COALESCE(t.symbol, ''))
         LIKE '%' || LOWER(CAST(:token_symbol AS TEXT)) || '%' ESCAPE '\\')
    AND (CAST(:tx_hash AS TEXT) IS NULL OR encode(ap.tx_hash, 'hex') = LOWER(CAST(:tx_hash AS TEXT)))
    {required_time_window_clause("ap.created_at")}
GROUP BY bucket_start
ORDER BY bucket_start DESC
LIMIT :limit
""")
