"""Test-data seeding helpers for the integration suite."""

import datetime as dt
from decimal import Decimal
from typing import cast

import asyncpg


async def insert_token(conn: asyncpg.Connection, symbol: str, decimals: int, address: bytes) -> int:
    """Insert a chain_id=1 token or return the existing ID."""
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO token (chain_id, address, symbol, decimals)
        VALUES (1, $1, $2, $3)
        ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
        RETURNING id
        """,
            address,
            symbol,
            decimals,
        ),
    )


async def insert_user(conn: asyncpg.Connection, address: bytes) -> int:
    """Insert a chain_id=1 user or return the existing ID."""
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO "user" (chain_id, address)
        VALUES (1, $1)
        ON CONFLICT (chain_id, address) DO UPDATE SET updated_at = NOW()
        RETURNING id
        """,
            address,
        ),
    )


async def insert_receipt_token(
    db_url: str,
    chain_id: int,
    address: bytes,
    symbol: str = "aUSDC",
) -> int:
    """Insert a receipt_token row (upserting on chain_id + address) and return its ID.

    Links the receipt token to a crypto-lending (``protocol_type = 'lending'``)
    protocol, ordered by id for determinism. Lending is required, not arbitrary:
    a receipt token routes to a risk model by its protocol name, and only lending
    protocols are supported — picking any protocol (e.g. a DEX one, now that DEX
    protocols are seeded on mainnet) would drop it from the crypto-lending set.
    The underlying token only satisfies an FK, so any one on the chain will do.
    """
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await conn.fetchval(
            "SELECT id FROM protocol WHERE chain_id = $1 AND protocol_type = 'lending' ORDER BY id LIMIT 1",
            chain_id,
        )
        if protocol_id is None:
            raise RuntimeError(f"no lending protocol seed found for chain_id={chain_id}")
        token_id = await conn.fetchval(
            "SELECT id FROM token WHERE chain_id = $1 ORDER BY id LIMIT 1",
            chain_id,
        )
        if token_id is None:
            raise RuntimeError(f"no token seed found for chain_id={chain_id}")
        return cast(
            int,
            await conn.fetchval(
                """
                INSERT INTO receipt_token
                    (protocol_id, underlying_token_id, receipt_token_address, symbol,
                     created_at_block, chain_id)
                VALUES ($1, $2, $3, $4, 1, $5)
                ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique
                    DO UPDATE SET symbol = EXCLUDED.symbol
                RETURNING id
                """,
                protocol_id,
                token_id,
                address,
                symbol,
                chain_id,
            ),
        )
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Maple Syrup vault seed
#
# Shared by the Maple backed-breakdown repository tests and the Maple risk-API
# tests. The ``maple`` protocol, the ``USDC`` token and the ``syrupUSDC``
# receipt_token are migration-seeded; these helpers insert the pool / loan /
# collateral / state snapshot rows a scenario needs. ``synced_at`` is passed
# explicitly so all snapshot rows can be pinned to one processing cycle — the
# breakdown query joins collateral to loan_state on (synced_at, processing_version).
#
# maple_pool / maple_loan are Data-Vault hubs: editorial attributes (name,
# is_syrup, loan_meta_type) live in the maple_pool_meta / maple_loan_meta SCD2
# satellites and are exposed through the maple_pool_current / maple_loan_current
# views. So insert_maple_pool / insert_maple_loan write a hub row AND its latest
# satellite row — without a live satellite row the entity never appears in the
# *_current view the breakdown query reads.
# ---------------------------------------------------------------------------


async def maple_seed_ids(conn: asyncpg.Connection) -> tuple[int, int]:
    """Return the migration-seeded (maple protocol_id, USDC token_id) for chain_id=1."""
    protocol_id = await conn.fetchval("SELECT id FROM protocol WHERE chain_id = 1 AND name = 'maple'")
    usdc_id = await conn.fetchval("SELECT id FROM token WHERE chain_id = 1 AND symbol = 'USDC'")
    if protocol_id is None or usdc_id is None:
        raise RuntimeError("maple protocol / USDC token not seeded by migrations")
    return cast(int, protocol_id), cast(int, usdc_id)


async def insert_maple_pool(
    conn: asyncpg.Connection,
    *,
    protocol_id: int,
    address: bytes,
    asset_token_id: int,
    synced_at: dt.datetime,
    name: str = "Syrup USDC",
    is_syrup: bool = True,
) -> int:
    """Insert a maple_pool hub row plus its current maple_pool_meta satellite row."""
    pool_id = cast(
        int,
        await conn.fetchval(
            """
            INSERT INTO maple_pool (chain_id, protocol_id, address, asset_token_id)
            VALUES (1, $1, $2, $3)
            RETURNING id
            """,
            protocol_id,
            address,
            asset_token_id,
        ),
    )
    await conn.execute(
        """
        INSERT INTO maple_pool_meta (maple_pool_id, synced_at, name, is_syrup, hashdiff)
        VALUES (
            $1, $2, $3::text, $4,
            decode(md5($3::text || E'\\x1f' || CASE WHEN $4 THEN 'true' ELSE 'false' END), 'hex')
        )
        """,
        pool_id,
        synced_at,
        name,
        is_syrup,
    )
    return pool_id


async def insert_maple_pool_state(
    conn: asyncpg.Connection,
    *,
    pool_id: int,
    synced_at: dt.datetime,
    liquid_assets: int,
    principal_out: int = 0,
) -> None:
    """Insert a maple_pool_state snapshot (nullable metrics left NULL/zero)."""
    await conn.execute(
        """
        INSERT INTO maple_pool_state
            (maple_pool_id, synced_at, liquid_assets, principal_out, utilization, monthly_apy, spot_apy)
        VALUES ($1, $2, $3, $4, 0, 0, 0)
        """,
        pool_id,
        synced_at,
        Decimal(liquid_assets),
        Decimal(principal_out),
    )


async def insert_maple_loan(
    conn: asyncpg.Connection,
    *,
    protocol_id: int,
    pool_id: int,
    borrower_user_id: int,
    address: bytes,
    synced_at: dt.datetime,
    loan_meta_type: str | None = None,
) -> int:
    """Insert a maple_loan hub row plus its current maple_loan_meta satellite row.

    ``loan_meta_type`` drives ``maple_loan_current.is_internal`` (True for
    'amm'/'strategy'), which the breakdown query filters on.
    """
    loan_id = cast(
        int,
        await conn.fetchval(
            """
            INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
            VALUES (1, $1, $2, $3, $4)
            RETURNING id
            """,
            protocol_id,
            address,
            pool_id,
            borrower_user_id,
        ),
    )
    await conn.execute(
        """
        INSERT INTO maple_loan_meta (maple_loan_id, synced_at, loan_type, loan_meta_type, hashdiff)
        VALUES ($1, $2, 'OTL', $3::text, decode(md5('OTL' || E'\\x1f' || COALESCE($3::text, E'\\x1e')), 'hex'))
        """,
        loan_id,
        synced_at,
        loan_meta_type,
    )
    return loan_id


async def insert_maple_loan_state(
    conn: asyncpg.Connection,
    *,
    loan_id: int,
    synced_at: dt.datetime,
    state: str,
    principal_owed: int,
    acm_ratio: int | None = None,
) -> None:
    """Insert a maple_loan_state snapshot."""
    await conn.execute(
        """
        INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, acm_ratio)
        VALUES ($1, $2, $3, $4, $5)
        """,
        loan_id,
        synced_at,
        state,
        Decimal(principal_owed),
        Decimal(acm_ratio) if acm_ratio is not None else None,
    )


async def insert_maple_loan_collateral(
    conn: asyncpg.Connection,
    *,
    loan_id: int,
    synced_at: dt.datetime,
    symbol: str,
    amount: int | None,
    decimals: int,
    value_usd: int | None,
    state: str = "Deposited",
) -> None:
    """Insert a maple_loan_collateral snapshot row.

    ``amount`` / ``value_usd`` accept ``None`` to model the API reporting null
    (e.g. a DepositPending asset), which the breakdown query filters out.
    """
    await conn.execute(
        """
        INSERT INTO maple_loan_collateral
            (maple_loan_id, synced_at, asset_symbol, asset_amount, asset_decimals, asset_value_usd, state)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        loan_id,
        synced_at,
        symbol,
        Decimal(amount) if amount is not None else None,
        decimals,
        Decimal(value_usd) if value_usd is not None else None,
        state,
    )


async def store_test_ids(conn: asyncpg.Connection, ids: dict[str, int]) -> None:
    """Persist seed IDs into a helper table so test fixtures can retrieve them."""
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _test_ids (
            key TEXT PRIMARY KEY,
            val BIGINT NOT NULL
        )
        """
    )
    for key, val in ids.items():
        await conn.execute(
            """
            INSERT INTO _test_ids (key, val)
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val
            """,
            key,
            val,
        )


async def insert_allocation_position(
    conn: asyncpg.Connection,
    *,
    token_id: int,
    prime_id: int,
    proxy_hex: str,
    balance: int,
    block: int,
    tx: str,
    direction: str,
    log_index: int = 0,
    block_version: int = 0,
) -> None:
    """Insert one allocation_position row (chain_id=1, tx_amount=balance)."""
    await conn.execute(
        "INSERT INTO allocation_position "
        "(chain_id, token_id, prime_id, proxy_address, balance, "
        "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
        "VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $4, $9)",
        token_id,
        prime_id,
        bytes.fromhex(proxy_hex),
        Decimal(balance),
        block,
        block_version,
        bytes.fromhex(tx),
        log_index,
        direction,
    )


# ---------------------------------------------------------------------------
# Ghost-balance regression seed
#
# Shared by the allocation API tests and the allocation repository tests.
# The original bug applied ``balance > 0`` inside the DISTINCT ON / LIMIT 1
# latest-row selection, so sweep-to-zero rows were skipped and the last
# non-zero balance resurfaced as an open position.  These rows belong to
# their own ``ghost_balance`` prime so they never masquerade as the
# migration-seeded primes (spark/grove/obex) in ``/v1/primes``.
# ---------------------------------------------------------------------------

# The proxy_kind classifier marks an address as ALM unless it matches a known
# sub-proxy.  Any address that is NOT the Spark sub-proxy will do.
GHOST_CLOSED_PROXY_HEX = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"  # all positions closed to zero
GHOST_SWEEP_PROXY_HEX = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"  # zero sweep rows newer than the non-zero row
GHOST_OPEN_PROXY_HEX = "cccccccccccccccccccccccccccccccccccccccc"  # open position with older zero rows
GHOST_MIXED_PROXY_HEX = "dddddddddddddddddddddddddddddddddddddddd"  # one swept + one open token
GHOST_TIEBREAK_PROXY_HEX = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"  # same-block rows, log_index decides

# Token addresses, not used by migrations.
_GHOST_SYRUP_USDT_HEX = "1111111111111111111111111111111111111111"
_GHOST_USDS_HEX = "2222222222222222222222222222222222222222"
GHOST_RECEIPT_SYRUP_HEX = "3333333333333333333333333333333333333333"  # receipt token wrapping syrupUSDT

# Vault address for the ghost_balance prime (20 bytes, unique in the prime table).
_GHOST_VAULT_HEX = "99" * 20

# Transaction hashes (32 bytes).
_GHOST_TXA = "aa" * 32
_GHOST_TXB = "bb" * 32
_GHOST_TXC = "cc" * 32
_GHOST_TXD = "dd" * 32
_GHOST_TXE = "ee" * 32
_GHOST_TXF = "ff" * 32


async def _ghost_seed_reference_rows(conn: asyncpg.Connection) -> tuple[int, int, int]:
    """Create the ghost_balance prime, tokens, receipt-token registration, and a syrupUSDT price of 2 USD.

    Returns (prime_id, asyrup_token_id, usds_token_id).
    """
    prime_id = await conn.fetchval(
        "INSERT INTO prime (name, vault_address) VALUES ('ghost_balance', $1) RETURNING id",
        bytes.fromhex(_GHOST_VAULT_HEX),
    )
    # protocol.name is not unique across chains; chain_id pins mainnet.
    protocol_id = await conn.fetchval("SELECT id FROM protocol WHERE name = 'Aave V3' AND chain_id = 1")
    oracle_id = await conn.fetchval("SELECT id FROM oracle WHERE name = 'aave_v3'")

    syrup_id = await insert_token(conn, "syrupUSDT", 6, bytes.fromhex(_GHOST_SYRUP_USDT_HEX))
    usds_id = await insert_token(conn, "USDS", 18, bytes.fromhex(_GHOST_USDS_HEX))
    asyrup_id = await insert_token(conn, "aSyrupUSDT", 6, bytes.fromhex(GHOST_RECEIPT_SYRUP_HEX))

    await conn.execute(
        "INSERT INTO receipt_token "
        "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
        "VALUES (1, $1, $2, $3, 'aSyrupUSDT')",
        protocol_id,
        syrup_id,
        bytes.fromhex(GHOST_RECEIPT_SYRUP_HEX),
    )
    await conn.execute(
        "INSERT INTO onchain_token_price "
        "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
        "VALUES ($1, $2, 2000, 0, NOW(), $3)",
        syrup_id,
        oracle_id,
        Decimal(2),
    )
    return prime_id, asyrup_id, usds_id


async def _ghost_seed_closed_proxy(conn: asyncpg.Connection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """Both holdings closed to zero: aSyrupUSDT 100 then 0, USDS 75894 then 0.

    The latest balance is 0 for both, so neither token may appear in any query.
    """
    for token_id, rows in [
        (asyrup_id, [(1000, 100, _GHOST_TXA, "in"), (2000, 0, _GHOST_TXB, "out")]),
        (usds_id, [(1000, 75894, _GHOST_TXC, "in"), (2000, 0, _GHOST_TXD, "out")]),
    ]:
        for block, bal, tx, direction in rows:
            await insert_allocation_position(
                conn,
                token_id=token_id,
                prime_id=prime_id,
                proxy_hex=GHOST_CLOSED_PROXY_HEX,
                balance=bal,
                block=block,
                tx=tx,
                direction=direction,
            )


async def _ghost_seed_sweep_proxy(conn: asyncpg.Connection, prime_id: int, asyrup_id: int) -> None:
    """The production sweep shape: five zero-balance sweep rows newer than the non-zero row."""
    await insert_allocation_position(
        conn,
        token_id=asyrup_id,
        prime_id=prime_id,
        proxy_hex=GHOST_SWEEP_PROXY_HEX,
        balance=68231707,
        block=2000,
        tx=_GHOST_TXE,
        direction="in",
    )
    for sweep_block in range(2001, 2006):
        await insert_allocation_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=GHOST_SWEEP_PROXY_HEX,
            balance=0,
            block=sweep_block,
            tx=_GHOST_TXF,
            direction="sweep",
        )


async def _ghost_seed_open_proxy(conn: asyncpg.Connection, prime_id: int, asyrup_id: int) -> None:
    """Open position with an older zero row; must still appear with balance=500."""
    for block, bal, tx, direction in [(999, 0, _GHOST_TXA, "out"), (1000, 500, _GHOST_TXB, "in")]:
        await insert_allocation_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=GHOST_OPEN_PROXY_HEX,
            balance=bal,
            block=block,
            tx=tx,
            direction=direction,
        )


async def _ghost_seed_mixed_proxy(conn: asyncpg.Connection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """One swept receipt token (250 then 0) and one open USDS holding (1000); only USDS may appear."""
    for block, bal, tx, direction in [(3000, 250, _GHOST_TXA, "in"), (3001, 0, _GHOST_TXB, "out")]:
        await insert_allocation_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=GHOST_MIXED_PROXY_HEX,
            balance=bal,
            block=block,
            tx=tx,
            direction=direction,
        )
    await insert_allocation_position(
        conn,
        token_id=usds_id,
        prime_id=prime_id,
        proxy_hex=GHOST_MIXED_PROXY_HEX,
        balance=1000,
        block=3000,
        tx=_GHOST_TXC,
        direction="in",
    )


async def _ghost_seed_tiebreak_proxy(conn: asyncpg.Connection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """All rows in block 4000, so log_index alone orders them.

    aSyrupUSDT is deposited then fully withdrawn within the block and must not
    appear; USDS is the mirror shape and ends the block at 400.
    """
    for log_index, bal, direction in [(0, 300, "in"), (1, 0, "out")]:
        await insert_allocation_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=GHOST_TIEBREAK_PROXY_HEX,
            balance=bal,
            block=4000,
            tx=_GHOST_TXD,
            direction=direction,
            log_index=log_index,
        )
    for log_index, bal, direction in [(0, 0, "out"), (1, 400, "in")]:
        await insert_allocation_position(
            conn,
            token_id=usds_id,
            prime_id=prime_id,
            proxy_hex=GHOST_TIEBREAK_PROXY_HEX,
            balance=bal,
            block=4000,
            tx=_GHOST_TXE,
            direction=direction,
            log_index=log_index,
        )


async def seed_ghost_balance(db_url: str) -> None:
    """Seed all ghost-balance proxy scenarios into the given database."""
    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            prime_id, asyrup_id, usds_id = await _ghost_seed_reference_rows(conn)
            await _ghost_seed_closed_proxy(conn, prime_id, asyrup_id, usds_id)
            await _ghost_seed_sweep_proxy(conn, prime_id, asyrup_id)
            await _ghost_seed_open_proxy(conn, prime_id, asyrup_id)
            await _ghost_seed_mixed_proxy(conn, prime_id, asyrup_id, usds_id)
            await _ghost_seed_tiebreak_proxy(conn, prime_id, asyrup_id, usds_id)
    finally:
        await conn.close()
