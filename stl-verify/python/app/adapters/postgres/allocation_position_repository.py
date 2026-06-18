import asyncio
import logging
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
from app.domain.entities.time_series_bucket import AllocationActivityBucket, TotalCapitalBucket
from app.domain.proxy_kind import ProxyKind, classify_proxy, subproxy_addresses

# USDS (mainnet). A prime's treasury USDS held in its SubProxy wallet is its
# total capital; this isolates that token from any other SubProxy holding.
_USDS_ADDRESS_HEX = "dc035d45d973e3ec169d2276ddab16f1e407384f"

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
                return [
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
                    )
                    for row in result
                ]
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
                    {"proxy_hex": prime_id.hex},
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
    def _record_unpriced_holdings(prime_id: EthAddress, holdings: list[DirectAssetHolding]) -> None:
        """Surface direct holdings that resolved to no oracle price.

        A null ``amount_usd`` is legitimate for assets with no oracle feed (LP/
        curve shares), but it is indistinguishable at the row level from a
        coverage regression — a token that should price but silently stopped
        (oracle disabled, reorg, backfill gap). Recording the unpriced count as
        a span attribute lets that be alerted on in the tracing backend instead
        of being discovered by a user noticing a missing USD value.
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
        """Return ``balance × price_usd`` for the prime's holding of a receipt token."""
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

            balance = _safe_decimal(row.balance, "balance", f"receipt_token_id={receipt_token_id}")
            price_usd = _safe_decimal(row.price_usd, "price_usd", f"receipt_token_id={receipt_token_id}")
            return balance * price_usd
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


# Match positions to receipt tokens only by receipt_token_address: a prime's
# direct holding of an underlying asset (e.g. raw USDT in the proxy wallet)
# is not a position in any receipt token that wraps it, and attributing it to
# every such receipt token double-counts and inflates per-token balances.
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
            ap.created_at                            AS latest_activity_at
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
        (p.balance * lp.price_usd) AS amount_usd,
        p.latest_activity_at
    FROM latest_receipt_positions p
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
            AND po.protocol_id = p.protocol_id
        WHERE otp.token_id = p.underlying_token_id
        ORDER BY otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
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
_DIRECT_ASSET_HOLDINGS_SQL = text("""
    WITH latest_positions AS (
        SELECT DISTINCT ON (ap.token_id)
            ap.chain_id,
            ap.token_id,
            ap.balance,
            ap.created_at AS latest_activity_at
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
        (lp.balance * px.price_usd) AS amount_usd,
        lp.latest_activity_at
    FROM latest_positions lp
    JOIN token t ON t.id = lp.token_id
    LEFT JOIN receipt_token rt
        ON rt.receipt_token_address = t.address AND rt.chain_id = lp.chain_id
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        WHERE otp.token_id = lp.token_id
        -- oracle_id breaks ties when multiple oracles price the same token at the
        -- same block, keeping the chosen price deterministic across calls.
        ORDER BY otp.block_number DESC, otp.block_version DESC,
                 otp.processing_version DESC, otp.oracle_id DESC
        LIMIT 1
    ) px ON TRUE
    WHERE rt.id IS NULL AND lp.balance > 0
    ORDER BY lp.balance DESC
""")


_USD_EXPOSURE_SQL = text("""
WITH latest_balance AS (
    SELECT ap.balance
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
    ORDER BY otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
    LIMIT 1
)
SELECT lb.balance, lp.price_usd
FROM latest_balance lb
CROSS JOIN latest_price lp
WHERE lb.balance > 0
""")


_TOTAL_USD_EXPOSURE_SQL = text("""
WITH latest_receipt_positions AS (
    SELECT DISTINCT ON (rt.id)
        rt.id                  AS receipt_token_id,
        rt.underlying_token_id AS underlying_token_id,
        rt.protocol_id         AS protocol_id,
        ap.balance
    FROM allocation_position ap
    JOIN token t          ON t.id = ap.token_id
    JOIN receipt_token rt ON rt.receipt_token_address = t.address AND rt.chain_id = ap.chain_id
    JOIN protocol pr      ON pr.id = rt.protocol_id AND pr.chain_id = ap.chain_id
    WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
    ORDER BY rt.id,
             ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC
)
SELECT COALESCE(SUM(p.balance * lp.price_usd), 0) AS total_usd_exposure
FROM latest_receipt_positions p
LEFT JOIN LATERAL (
    SELECT otp.price_usd
    FROM onchain_token_price otp
    JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
        AND po.protocol_id = p.protocol_id
    WHERE otp.token_id = p.underlying_token_id
    ORDER BY otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
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
# current total and cumulating net flows backwards — so the valuation basis must
# match the anchor, which sums BOTH receipt-token positions and direct holdings.
# Each flow is therefore priced by its receipt token's underlying oracle price
# when wrapped, falling back to the token's own latest oracle price when held
# directly (no receipt token). Flows whose token has no oracle price at all
# (e.g. LP/curve shares) still contribute 0.
_ALLOCATION_ACTIVITY_BUCKETS_SQL = text(f"""
WITH receipt_token_price AS (
    -- Latest underlying oracle price per receipt token, computed ONCE per token
    -- (a few dozen rows) rather than once per activity event (~100k+). The
    -- main query then hash-joins this by token address.
    SELECT
        rt.chain_id,
        rt.receipt_token_address,
        (
            SELECT otp.price_usd
            FROM onchain_token_price otp
            JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
            WHERE po.protocol_id = rt.protocol_id
              AND otp.token_id = rt.underlying_token_id
            ORDER BY otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
            LIMIT 1
        ) AS price_usd
    FROM receipt_token rt
),
direct_token_price AS (
    -- Latest oracle price per token actually involved in these flows, used to
    -- value direct-asset flows (no receipt-token wrapper). Bounded to the flow
    -- token set and computed ONCE per token, mirroring receipt_token_price.
    -- No protocol context here (the token is held bare), so the latest price
    -- across any oracle is used, with oracle_id breaking ties deterministically.
    -- The token set mirrors the outer query's prime/chain/time-window filters so
    -- an all-primes ("no prime_hex") query does not scan every token ever held.
    SELECT
        ft.token_id,
        (
            SELECT otp.price_usd
            FROM onchain_token_price otp
            WHERE otp.token_id = ft.token_id
            ORDER BY otp.block_number DESC, otp.block_version DESC,
                     otp.processing_version DESC, otp.oracle_id DESC
            LIMIT 1
        ) AS price_usd
    FROM (
        SELECT DISTINCT ap.token_id
        FROM allocation_position ap
        WHERE (CAST(:prime_hex AS TEXT) IS NULL
               OR ap.proxy_address = decode(CAST(:prime_hex AS TEXT), 'hex'))
          AND (CAST(:chain_id AS INTEGER) IS NULL OR ap.chain_id = CAST(:chain_id AS INTEGER))
          AND ap.token_id IS NOT NULL
          AND ap.created_at IS NOT NULL
          {required_time_window_clause("ap.created_at")}
    ) AS ft
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
        END * COALESCE(price.price_usd, direct_price.price_usd, 0)
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
LEFT JOIN direct_token_price direct_price
    ON direct_price.token_id = ap.token_id
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
