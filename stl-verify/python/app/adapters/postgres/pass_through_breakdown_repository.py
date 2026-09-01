from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from sqlalchemy import TextClause, bindparam, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.allocation import EthAddress
from app.domain.entities.pass_through_breakdown import PassThroughHolding
from app.domain.prime_registry import subproxy_addresses

# Latest eligible USD price for one token: enabled-mapping filter + oracle_id
# tiebreak (canonical rationale on _DIRECT_ASSET_HOLDINGS_SQL in
# allocation_position_repository.py).
_LATEST_PRICE_LATERAL = """
        SELECT tpc.price_usd
        FROM token_price_current tpc
        WHERE tpc.token_id = {token_id_expr}
          AND EXISTS (
              SELECT 1 FROM oracle_asset oa
              WHERE oa.oracle_id = tpc.oracle_id
                AND oa.token_id = tpc.token_id
                AND oa.enabled
          )
        ORDER BY tpc.block_number DESC, tpc.block_version DESC,
                 tpc.processing_version DESC, tpc.oracle_id DESC
        LIMIT 1
"""


def _holding_sql(with_proxy_filter: bool) -> TextClause:
    proxy_filter = "AND ap.proxy_address IN :proxy_addrs" if with_proxy_filter else ""
    # SubProxy treasury wallets hold a prime's total capital, not allocations
    # (rationale on list_prime_proxy_addresses), so they never contribute.
    sql = f"""
    WITH position AS (
        SELECT
            ap.token_id,
            SUM(ap.balance) AS balance,
            MAX(ap.underlying_token_id) AS underlying_token_id,
            SUM(ap.underlying_value) AS underlying_value,
            COUNT(*) = COUNT(ap.underlying_value) AS fully_valued
        FROM allocation_position_current ap
        JOIN token t ON t.id = ap.token_id
        WHERE ap.chain_id = :chain_id
          AND t.chain_id = :chain_id
          AND t.address = decode(:token_hex, 'hex')
          AND ap.proxy_address NOT IN :subproxy_addrs
          {proxy_filter}
        GROUP BY ap.token_id
    )
    SELECT
        p.token_id,
        t.symbol,
        p.balance,
        p.underlying_token_id,
        ut.symbol AS underlying_symbol,
        p.underlying_value,
        p.fully_valued,
        self_px.price_usd  AS token_price_usd,
        under_px.price_usd AS underlying_price_usd
    FROM position p
    JOIN token t ON t.id = p.token_id
    LEFT JOIN token ut ON ut.id = p.underlying_token_id
    LEFT JOIN LATERAL ({_LATEST_PRICE_LATERAL.format(token_id_expr="p.token_id")}
    ) self_px ON TRUE
    LEFT JOIN LATERAL ({_LATEST_PRICE_LATERAL.format(token_id_expr="p.underlying_token_id")}
    ) under_px ON TRUE
    """
    stmt = text(sql).bindparams(bindparam("subproxy_addrs", expanding=True))
    if with_proxy_filter:
        stmt = stmt.bindparams(bindparam("proxy_addrs", expanding=True))
    return stmt


_HOLDING_SQL = _holding_sql(with_proxy_filter=False)
_HOLDING_SQL_BY_PROXIES = _holding_sql(with_proxy_filter=True)


class PassThroughBreakdownRepository:
    """Reads the single-token breakdown of a directly-held allocated asset.

    Serves the risk-breakdown fallback for allocated tokens that are not
    registered receipt tokens: one aggregate over their
    ``allocation_position_current`` rows, collapsed to the token (or its
    tracked underlying) that backs the position one-to-one.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_holding(
        self,
        chain_id: int,
        token_address: EthAddress,
        proxy_addresses: Sequence[EthAddress] | None = None,
    ) -> PassThroughHolding | None:
        """Aggregate the token's allocation positions into one holding.

        ``proxy_addresses`` narrows the aggregate to one prime's proxies; None
        spans every proxy. Returns None when the token has no allocation
        position at all.
        """
        params: dict[str, Any] = {
            "chain_id": chain_id,
            "token_hex": token_address.hex,
            "subproxy_addrs": [bytes.fromhex(address[2:]) for address in subproxy_addresses()],
        }
        stmt = _HOLDING_SQL
        if proxy_addresses is not None:
            stmt = _HOLDING_SQL_BY_PROXIES
            params["proxy_addrs"] = [address.to_bytes() for address in proxy_addresses]

        async with self._engine.connect() as conn:
            row = (await conn.execute(stmt, params)).fetchone()
        if row is None:
            return None

        # Collapse to the underlying only when EVERY contributing row carries a
        # redeemable value: a partial SUM(underlying_value) would silently
        # understate exposure (NULL is never zero exposure). The NULL-symbol
        # gate mirrors the ut join guard in _DIRECT_ASSET_HOLDINGS_SQL.
        underlying_differs = (
            row.fully_valued
            and row.underlying_token_id is not None
            and row.underlying_token_id != row.token_id
            and row.underlying_symbol is not None
        )
        if underlying_differs:
            return PassThroughHolding(
                token_id=row.underlying_token_id,
                symbol=row.underlying_symbol,
                amount=Decimal(str(row.underlying_value)),
                price_usd=Decimal(str(row.underlying_price_usd)) if row.underlying_price_usd is not None else None,
            )
        return PassThroughHolding(
            token_id=row.token_id,
            symbol=row.symbol,
            amount=Decimal(str(row.balance)),
            price_usd=Decimal(str(row.token_price_usd)) if row.token_price_usd is not None else None,
        )
