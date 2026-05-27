"""Reflection-driven insert helpers for DB-backed tests.

All helpers take an ``AsyncConnection`` (the per-test ``wrap_conn`` fixture)
and insert one row using SQLAlchemy Core + reflected ``MetaData``. Inserts
inherit the test's outer transaction + nested savepoint, so writes are
rolled back at teardown — no manual cleanup required.

Helpers covering tables with unique constraints already populated by
migration seeds (``token``, ``user``, ``protocol``, ``prime``,
``receipt_token``) use ``ON CONFLICT DO UPDATE`` so tests that reseed
fixture rows are idempotent. Append-only tables (positions, states,
events, prices) use plain inserts.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.db import get_session_metadata

_ZERO_TX_HASH = b"\x00" * 32
_ZERO_ADDRESS = b"\x00" * 20


def _table(name: str) -> Table:
    metadata = get_session_metadata()
    try:
        return metadata.tables[name]
    except KeyError:
        known = sorted(metadata.tables)
        raise KeyError(
            f"table {name!r} not in reflected MetaData — did the migration adding it run? known tables: {known}"
        ) from None


async def _insert_returning_id(conn: AsyncConnection, table: Table, values: dict[str, Any]) -> int:
    result = await conn.execute(table.insert().values(**values).returning(table.c.id))
    return cast(int, result.scalar_one())


async def _upsert_returning_id(
    conn: AsyncConnection,
    table: Table,
    values: dict[str, Any],
    *,
    index_elements: list[str] | None = None,
    constraint: str | None = None,
    update_columns: list[str] | None = None,
) -> int:
    """Upsert via ``INSERT ... ON CONFLICT DO UPDATE ... RETURNING id``.

    DO UPDATE (not DO NOTHING) so RETURNING fires on both insert and conflict.
    """
    stmt = pg_insert(table).values(**values)
    set_cols = update_columns or list(values)
    set_ = {col: stmt.excluded[col] for col in set_cols}
    if constraint is not None:
        stmt = stmt.on_conflict_do_update(constraint=constraint, set_=set_)
    else:
        stmt = stmt.on_conflict_do_update(index_elements=index_elements, set_=set_)
    result = await conn.execute(stmt.returning(table.c.id))
    return cast(int, result.scalar_one())


async def insert_token(
    conn: AsyncConnection,
    *,
    address: bytes,
    symbol: str,
    decimals: int,
    chain_id: int = 1,
) -> int:
    """Upsert a ``token`` row on ``(chain_id, address)``."""
    return await _upsert_returning_id(
        conn,
        _table("token"),
        {"chain_id": chain_id, "address": address, "symbol": symbol, "decimals": decimals},
        index_elements=["chain_id", "address"],
        update_columns=["symbol"],
    )


async def insert_user(
    conn: AsyncConnection,
    *,
    address: bytes,
    chain_id: int = 1,
) -> int:
    """Upsert a ``user`` row on ``(chain_id, address)``.

    ``updated_at`` is passed explicitly so the upsert path bumps it instead of
    relying on the schema's ``DEFAULT NOW()``, which would silently no-op on
    conflict.
    """
    return await _upsert_returning_id(
        conn,
        _table("user"),
        {
            "chain_id": chain_id,
            "address": address,
            "updated_at": datetime.now(timezone.utc),
        },
        index_elements=["chain_id", "address"],
        update_columns=["updated_at"],
    )


async def insert_protocol(
    conn: AsyncConnection,
    *,
    name: str,
    address: bytes = _ZERO_ADDRESS,
    protocol_type: str = "aave_v3",
    created_at_block: int = 1,
    chain_id: int = 1,
) -> int:
    """Upsert a ``protocol`` row on ``(chain_id, address)``."""
    return await _upsert_returning_id(
        conn,
        _table("protocol"),
        {
            "chain_id": chain_id,
            "address": address,
            "name": name,
            "protocol_type": protocol_type,
            "created_at_block": created_at_block,
            "updated_at": datetime.now(timezone.utc),
        },
        index_elements=["chain_id", "address"],
        update_columns=["name"],
    )


async def insert_prime(
    conn: AsyncConnection,
    *,
    name: str,
    vault_address: bytes,
) -> int:
    """Upsert a ``prime`` row on ``name``."""
    return await _upsert_returning_id(
        conn,
        _table("prime"),
        {"name": name, "vault_address": vault_address},
        index_elements=["name"],
        update_columns=["vault_address"],
    )


async def insert_receipt_token(
    conn: AsyncConnection,
    *,
    protocol_id: int,
    underlying_token_id: int,
    address: bytes,
    symbol: str,
    created_at_block: int = 1,
    chain_id: int = 1,
    index_receipt_token_address: bool = False,
) -> int:
    """Upsert a ``receipt_token`` row on the chain_id+address unique constraint.

    When ``index_receipt_token_address=True`` a matching ``token`` row is also
    inserted for the receipt-token address, mirroring what the prime-allocation
    indexer does in production. Tests that exercise the ``receipt_token_token_id``
    JOIN need this; warm-up tests (token row missing) leave it ``False``.
    """
    if index_receipt_token_address:
        await insert_token(
            conn,
            chain_id=chain_id,
            address=address,
            symbol=symbol,
            decimals=18,
        )
    return await _upsert_returning_id(
        conn,
        _table("receipt_token"),
        {
            "chain_id": chain_id,
            "protocol_id": protocol_id,
            "underlying_token_id": underlying_token_id,
            "receipt_token_address": address,
            "symbol": symbol,
            "created_at_block": created_at_block,
        },
        constraint="receipt_token_chain_address_unique",
        update_columns=["symbol"],
    )


async def insert_allocation_position(
    conn: AsyncConnection,
    *,
    token_id: int,
    prime_id: int,
    proxy_address: bytes,
    balance: int | Decimal,
    block_number: int,
    block_version: int = 0,
    tx_hash: bytes = _ZERO_TX_HASH,
    log_index: int = 0,
    direction: str = "in",
    chain_id: int = 1,
    scaled_balance: int | Decimal | None = None,
) -> None:
    """Insert an ``allocation_position`` event row (append-only)."""
    values: dict[str, Any] = {
        "chain_id": chain_id,
        "token_id": token_id,
        "prime_id": prime_id,
        "proxy_address": proxy_address,
        "balance": balance,
        "block_number": block_number,
        "block_version": block_version,
        "tx_hash": tx_hash,
        "log_index": log_index,
        "tx_amount": balance,
        "direction": direction,
    }
    if scaled_balance is not None:
        values["scaled_balance"] = scaled_balance
    await conn.execute(_table("allocation_position").insert().values(**values))


async def insert_morpho_market(
    conn: AsyncConnection,
    *,
    protocol_id: int,
    market_id: bytes,
    loan_token_id: int,
    collateral_token_id: int,
    lltv: Decimal = Decimal("0.86"),
    oracle_address: bytes = _ZERO_ADDRESS,
    irm_address: bytes = _ZERO_ADDRESS,
    created_at_block: int = 1,
    chain_id: int = 1,
) -> int:
    """Upsert a ``morpho_market`` row on ``(chain_id, market_id)``."""
    return await _upsert_returning_id(
        conn,
        _table("morpho_market"),
        {
            "chain_id": chain_id,
            "protocol_id": protocol_id,
            "market_id": market_id,
            "loan_token_id": loan_token_id,
            "collateral_token_id": collateral_token_id,
            "oracle_address": oracle_address,
            "irm_address": irm_address,
            "lltv": lltv,
            "created_at_block": created_at_block,
        },
        index_elements=["chain_id", "market_id"],
        update_columns=["lltv"],
    )


async def insert_morpho_market_state(
    conn: AsyncConnection,
    *,
    morpho_market_id: int,
    block_number: int,
    total_supply_assets: int | Decimal,
    total_borrow_assets: int | Decimal,
    block_version: int = 0,
    fee: int | Decimal = 0,
) -> None:
    await conn.execute(
        _table("morpho_market_state")
        .insert()
        .values(
            morpho_market_id=morpho_market_id,
            block_number=block_number,
            block_version=block_version,
            timestamp=datetime.now(timezone.utc),
            total_supply_assets=total_supply_assets,
            total_supply_shares=total_supply_assets,
            total_borrow_assets=total_borrow_assets,
            total_borrow_shares=total_borrow_assets,
            last_update=block_number,
            fee=fee,
        )
    )


async def insert_morpho_market_position(
    conn: AsyncConnection,
    *,
    user_id: int,
    morpho_market_id: int,
    block_number: int,
    supply_assets: int | Decimal = 0,
    borrow_assets: int | Decimal = 0,
    collateral: int | Decimal = 0,
    block_version: int = 0,
) -> None:
    await conn.execute(
        _table("morpho_market_position")
        .insert()
        .values(
            user_id=user_id,
            morpho_market_id=morpho_market_id,
            block_number=block_number,
            block_version=block_version,
            timestamp=datetime.now(timezone.utc),
            supply_shares=supply_assets,
            borrow_shares=borrow_assets,
            collateral=collateral,
            supply_assets=supply_assets,
            borrow_assets=borrow_assets,
        )
    )


async def insert_morpho_vault(
    conn: AsyncConnection,
    *,
    protocol_id: int,
    address: bytes,
    asset_token_id: int,
    name: str = "Test Vault",
    symbol: str = "TV",
    vault_version: int = 1,
    created_at_block: int = 1,
    chain_id: int = 1,
) -> int:
    """Upsert a ``morpho_vault`` row on ``(chain_id, address)``."""
    return await _upsert_returning_id(
        conn,
        _table("morpho_vault"),
        {
            "chain_id": chain_id,
            "protocol_id": protocol_id,
            "address": address,
            "name": name,
            "symbol": symbol,
            "asset_token_id": asset_token_id,
            "vault_version": vault_version,
            "created_at_block": created_at_block,
        },
        index_elements=["chain_id", "address"],
        update_columns=["name", "symbol"],
    )


async def insert_morpho_vault_state(
    conn: AsyncConnection,
    *,
    morpho_vault_id: int,
    block_number: int,
    total_assets: int | Decimal,
    block_version: int = 0,
) -> None:
    await conn.execute(
        _table("morpho_vault_state")
        .insert()
        .values(
            morpho_vault_id=morpho_vault_id,
            block_number=block_number,
            block_version=block_version,
            timestamp=datetime.now(timezone.utc),
            total_assets=total_assets,
            total_shares=total_assets,
        )
    )


async def insert_sparklend_reserve(
    conn: AsyncConnection,
    *,
    protocol_id: int,
    token_id: int,
    block_number: int,
    usage_as_collateral_enabled: bool = True,
    ltv: Decimal | None = None,
    block_version: int = 0,
) -> None:
    if ltv is None:
        ltv = Decimal("8000") if usage_as_collateral_enabled else Decimal("0")
    await conn.execute(
        _table("sparklend_reserve_data")
        .insert()
        .values(
            protocol_id=protocol_id,
            token_id=token_id,
            block_number=block_number,
            block_version=block_version,
            usage_as_collateral_enabled=usage_as_collateral_enabled,
            ltv=ltv,
        )
    )


async def insert_borrower(
    conn: AsyncConnection,
    *,
    user_id: int,
    protocol_id: int,
    token_id: int,
    amount: int | Decimal,
    block_number: int,
    event_type: str = "borrow",
    tx_hash: bytes = _ZERO_TX_HASH,
    block_version: int = 0,
) -> None:
    await conn.execute(
        _table("borrower")
        .insert()
        .values(
            user_id=user_id,
            protocol_id=protocol_id,
            token_id=token_id,
            block_number=block_number,
            block_version=block_version,
            amount=amount,
            change=amount,
            event_type=event_type,
            tx_hash=tx_hash,
        )
    )


async def insert_borrower_collateral(
    conn: AsyncConnection,
    *,
    user_id: int,
    protocol_id: int,
    token_id: int,
    amount: int | Decimal,
    block_number: int,
    collateral_enabled: bool = True,
    event_type: str = "deposit",
    tx_hash: bytes = _ZERO_TX_HASH,
    block_version: int = 0,
) -> None:
    await conn.execute(
        _table("borrower_collateral")
        .insert()
        .values(
            user_id=user_id,
            protocol_id=protocol_id,
            token_id=token_id,
            block_number=block_number,
            block_version=block_version,
            amount=amount,
            change=amount,
            event_type=event_type,
            tx_hash=tx_hash,
            collateral_enabled=collateral_enabled,
        )
    )


async def insert_oracle_asset(
    conn: AsyncConnection,
    *,
    oracle_id: int,
    token_id: int,
    enabled: bool = True,
) -> None:
    """Insert an ``oracle_asset`` row.

    The unique constraint covers ``(oracle_id, token_id) WHERE feed_address IS NULL``;
    on conflict we treat the row as already present and skip.
    """
    stmt = pg_insert(_table("oracle_asset")).values(oracle_id=oracle_id, token_id=token_id, enabled=enabled)
    await conn.execute(stmt.on_conflict_do_nothing())


async def insert_onchain_token_price(
    conn: AsyncConnection,
    *,
    token_id: int,
    oracle_id: int,
    block_number: int,
    price_usd: Decimal,
    block_version: int = 0,
    timestamp: datetime | None = None,
) -> None:
    await conn.execute(
        _table("onchain_token_price")
        .insert()
        .values(
            token_id=token_id,
            oracle_id=oracle_id,
            block_number=block_number,
            block_version=block_version,
            timestamp=timestamp or datetime.now(timezone.utc),
            price_usd=price_usd,
        )
    )
