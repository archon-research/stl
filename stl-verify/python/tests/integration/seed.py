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


async def insert_receipt_token_row(
    conn: asyncpg.Connection,
    *,
    protocol_id: int,
    underlying_token_id: int,
    address: bytes,
    symbol: str,
) -> None:
    """Insert a chain_id=1 receipt_token row with an explicit protocol/underlying binding."""
    await conn.execute(
        "INSERT INTO receipt_token "
        "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
        "VALUES (1, $1, $2, $3, $4)",
        protocol_id,
        underlying_token_id,
        address,
        symbol,
    )


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
    build_id: int = 0,
) -> None:
    """Insert a maple_loan_state snapshot.

    ``processing_version`` is trigger-assigned per (loan, synced_at): a distinct
    ``build_id`` for the same key gets ``MAX(processing_version) + 1``, so pass
    successive build_ids to model a reprocess (pv 0, then pv 1) of one cycle.
    """
    await conn.execute(
        """
        INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, acm_ratio, build_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        loan_id,
        synced_at,
        state,
        Decimal(principal_owed),
        Decimal(acm_ratio) if acm_ratio is not None else None,
        build_id,
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
    build_id: int = 0,
) -> None:
    """Insert a maple_loan_collateral snapshot row.

    ``amount`` / ``value_usd`` accept ``None`` to model the API reporting null
    (e.g. a DepositPending asset), which the breakdown query filters out.
    ``build_id`` drives trigger-assigned ``processing_version`` as in
    ``insert_maple_loan_state``; pin a collateral row to the same build as its
    loan_state so they share a processing_version.
    """
    await conn.execute(
        """
        INSERT INTO maple_loan_collateral
            (maple_loan_id, synced_at, asset_symbol, asset_amount, asset_decimals, asset_value_usd, state, build_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        loan_id,
        synced_at,
        symbol,
        Decimal(amount) if amount is not None else None,
        decimals,
        Decimal(value_usd) if value_usd is not None else None,
        state,
        build_id,
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
    balance: int | Decimal,
    block: int,
    tx: str,
    direction: str,
    log_index: int = 0,
    block_version: int = 0,
    underlying_value: Decimal | None = None,
    underlying_token_id: int | None = None,
    created_at: dt.datetime | None = None,
    tx_amount: int | Decimal | None = None,
) -> None:
    """Insert one allocation_position row (chain_id=1; tx_amount defaults to balance).

    ``underlying_value``/``underlying_token_id`` default to NULL (both-or-neither,
    matching the tracker's domain invariant) so existing callers are unaffected.
    ``created_at`` defaults to the column's NOW(); pass it to place rows in
    specific buckets for the time-bucketed reads. Pass ``tx_amount`` when a flow
    scenario needs the tx magnitude decoupled from the post-tx balance.
    """
    await conn.execute(
        "INSERT INTO allocation_position "
        "(chain_id, token_id, prime_id, proxy_address, balance, "
        "block_number, block_version, tx_hash, log_index, tx_amount, direction, "
        "underlying_value, underlying_token_id, created_at) "
        "VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $13, $9, $10, $11, COALESCE($12, NOW()))",
        token_id,
        prime_id,
        bytes.fromhex(proxy_hex),
        Decimal(balance),
        block,
        block_version,
        bytes.fromhex(tx),
        log_index,
        direction,
        underlying_value,
        underlying_token_id,
        created_at,
        Decimal(tx_amount) if tx_amount is not None else Decimal(balance),
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

    await insert_receipt_token_row(
        conn,
        protocol_id=protocol_id,
        underlying_token_id=syrup_id,
        address=bytes.fromhex(GHOST_RECEIPT_SYRUP_HEX),
        symbol="aSyrupUSDT",
    )
    await conn.execute(
        "INSERT INTO onchain_token_price "
        "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
        "VALUES ($1, $2, 2000, 0, NOW(), $3)",
        syrup_id,
        oracle_id,
        Decimal(2),
    )
    await insert_oracle_asset(conn, oracle_id, syrup_id)
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


# ---------------------------------------------------------------------------
# Underlying-value direct-holdings seed (VEC-450 / B4)
#
# One prime holding the same allowlisted vault (sparkPrimeUSDC1) plus control
# tokens across several proxies, one proxy per pricing branch of
# ``_DIRECT_ASSET_HOLDINGS_SQL``. sparkPrimeUSDC1 is given its OWN oracle price
# here (it has none on mainnet) so the tests can prove allowlisted tokens ignore
# it — the underlying-value path must win, never the legacy balance x own-price.
#   * UV_PROXY_PRICED           allowlisted, underlying_value + USDC price   -> underlying_value x USDC price
#   * UV_PROXY_UNDERLYING_UNPRICED allowlisted, underlying has no oracle     -> NULL (surfaced, not legacy)
#   * UV_PROXY_NULL_VALUE       allowlisted, underlying_value NULL           -> NULL (surfaced, not legacy)
#   * UV_PROXY_NON_ALLOWLISTED  unlisted vault, same shape but not allowlisted -> legacy path (no own oracle -> NULL)
#   * UV_PROXY_PLAIN            plain USDC with own oracle                    -> legacy balance x own price
# ---------------------------------------------------------------------------

UV_PROXY_PRICED = "17" * 20
UV_PROXY_UNDERLYING_UNPRICED = "27" * 20
UV_PROXY_NULL_VALUE = "37" * 20
UV_PROXY_NON_ALLOWLISTED = "47" * 20
UV_PROXY_PLAIN = "57" * 20
UV_PROXY_UNIV3_POOL = "77" * 20
UV_PROXY_SYMBOLLESS_UNDERLYING = "87" * 20

# Real mainnet address: must match the pricing allowlist in the repository.
_UV_SPARK_PRIME_USDC1_HEX = "38464507e02c983f20428a6e8566693fe9e422a9"
# Synthetic vault, deliberately NOT in the allowlist. A synthetic (not real)
# address is used on purpose: a real non-allowlisted vault (e.g. syrupUSDC) can
# be registered as a receipt_token by a migration, which would route it through
# the receipt-token path instead of direct holdings and break this control.
_UV_UNLISTED_VAULT_HEX = "67" * 20
_UV_USDC_HEX = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
# Synthetic underlying with no oracle row, to exercise the missing-price branch.
_UV_UNPRICED_UNDERLYING_HEX = "0e" * 20
# Synthetic underlying with a NULL symbol (token.symbol is nullable), to prove
# the underlying-identity projection is atomic rather than per-field.
_UV_SYMBOLLESS_UNDERLYING_HEX = "1e" * 20
_UV_VAULT_HEX = "88" * 20
# Real mainnet AUSD/USDC Uniswap V3 pool contract (grove position): not an
# ERC20 and never has its own oracle, so it must match the pricing allowlist.
_UV_UNIV3_POOL_HEX = "bafead7c60ea473758ed6c6021505e8bbd7e8e5d"

UV_USDC_PRICE = Decimal("1.00")
# sparkPrimeUSDC1's own (share) price. Distinct from the USDC price so that
# "balance x own price" is a different number than "underlying_value x USDC
# price": the double-count guard test asserts the code never uses this.
UV_SPARK_OWN_PRICE = Decimal("0.90")
UV_SPARKPRIME_BALANCE = Decimal("19647500.616754")
UV_SPARKPRIME_UNDERLYING_VALUE = Decimal("20138132.383754")
UV_UNLISTED_UNDERLYING_VALUE = Decimal("105303042.633792")
UV_UNLISTED_BALANCE = Decimal("89822198.110408")
UV_USDC_BALANCE = Decimal("1000")
# Uni V3 rows carry the tracker-computed full position value in both columns
# (balance == underlying_value by construction).
UV_UNIV3_UNDERLYING_VALUE = Decimal("26927207.299715")


async def insert_oracle_asset(conn: asyncpg.Connection, oracle_id: int, token_id: int, *, enabled: bool = True) -> None:
    """Register a non-feed oracle_asset mapping (idempotent).

    Every seeded onchain price needs an enabled oracle_asset mapping to stay
    eligible for the latest-price reads, which exclude rows whose
    ``(oracle_id, token_id)`` has no enabled mapping (rationale on
    ``_DIRECT_ASSET_HOLDINGS_SQL``). Production never writes a price without one
    (the worker fetches only enabled assets), so the seeds mirror that; pass
    ``enabled=False`` to model a retired source whose stale rows must stop
    surfacing.
    """
    await conn.execute(
        """
        INSERT INTO oracle_asset (oracle_id, token_id, enabled)
        VALUES ($1, $2, $3)
        ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING
        """,
        oracle_id,
        token_id,
        enabled,
    )


async def _insert_price(conn: asyncpg.Connection, token_id: int, oracle_id: int, price: Decimal) -> None:
    """Insert a price row AND its enabled oracle_asset mapping (see insert_oracle_asset).

    Retired-source scenarios that need a price without an enabled mapping must
    insert the price row directly instead of calling this.
    """
    await conn.execute(
        "INSERT INTO onchain_token_price "
        "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
        "VALUES ($1, $2, 1000, 0, NOW(), $3)",
        token_id,
        oracle_id,
        price,
    )
    await insert_oracle_asset(conn, oracle_id, token_id)


async def seed_underlying_value_direct_holdings(db_url: str) -> None:
    """Seed the underlying-value direct-holdings scenarios into the given database."""
    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            prime_id = await conn.fetchval(
                "INSERT INTO prime (name, vault_address) VALUES ('uv_direct_holdings', $1) RETURNING id",
                bytes.fromhex(_UV_VAULT_HEX),
            )
            # A dedicated oracle whose mappings this seed enables freshly: reusing
            # a migration oracle (e.g. sparklend) would inherit its enabled state,
            # and the dust migration disables sparklend -> USDC, which the
            # enabled-mapping price filter would then exclude. The oracle never
            # drives an assertion (one price row per token, tie-break inert).
            oracle_id = await conn.fetchval(
                "INSERT INTO oracle (name, display_name, chain_id, address) "
                "VALUES ('uv_direct_test', 'Underlying-value direct-holdings test oracle', 1, $1) RETURNING id",
                bytes.fromhex("77" * 20),
            )

            usdc_id = await insert_token(conn, "USDC", 6, bytes.fromhex(_UV_USDC_HEX))
            spark_id = await insert_token(conn, "sparkPrimeUSDC1", 6, bytes.fromhex(_UV_SPARK_PRIME_USDC1_HEX))
            unlisted_id = await insert_token(conn, "unlistedVault", 6, bytes.fromhex(_UV_UNLISTED_VAULT_HEX))
            unpriced_id = await insert_token(conn, "NOPRICEUND", 18, bytes.fromhex(_UV_UNPRICED_UNDERLYING_HEX))

            await _insert_price(conn, usdc_id, oracle_id, UV_USDC_PRICE)
            await _insert_price(conn, spark_id, oracle_id, UV_SPARK_OWN_PRICE)

            # Allowlisted + underlying_value + priced underlying -> underlying_value x USDC price.
            # (spark also has its own price above: the result must ignore it.)
            await insert_allocation_position(
                conn,
                token_id=spark_id,
                prime_id=prime_id,
                proxy_hex=UV_PROXY_PRICED,
                balance=UV_SPARKPRIME_BALANCE,
                block=1000,
                tx="a1" * 32,
                direction="sweep",
                underlying_value=UV_SPARKPRIME_UNDERLYING_VALUE,
                underlying_token_id=usdc_id,
            )
            # Allowlisted + underlying_value present but underlying has no oracle -> NULL.
            await insert_allocation_position(
                conn,
                token_id=spark_id,
                prime_id=prime_id,
                proxy_hex=UV_PROXY_UNDERLYING_UNPRICED,
                balance=UV_SPARKPRIME_BALANCE,
                block=1000,
                tx="b1" * 32,
                direction="sweep",
                underlying_value=UV_SPARKPRIME_UNDERLYING_VALUE,
                underlying_token_id=unpriced_id,
            )
            # Allowlisted + underlying_value NULL (both-NULL) -> NULL, not balance x own price.
            await insert_allocation_position(
                conn,
                token_id=spark_id,
                prime_id=prime_id,
                proxy_hex=UV_PROXY_NULL_VALUE,
                balance=UV_SPARKPRIME_BALANCE,
                block=1000,
                tx="c1" * 32,
                direction="sweep",
            )
            # Not allowlisted, same shape as the priced case -> legacy path (no own oracle -> NULL).
            await insert_allocation_position(
                conn,
                token_id=unlisted_id,
                prime_id=prime_id,
                proxy_hex=UV_PROXY_NON_ALLOWLISTED,
                balance=UV_UNLISTED_BALANCE,
                block=1000,
                tx="d1" * 32,
                direction="sweep",
                underlying_value=UV_UNLISTED_UNDERLYING_VALUE,
                underlying_token_id=usdc_id,
            )
            # Plain USDC held directly, own oracle -> legacy balance x own price.
            await insert_allocation_position(
                conn,
                token_id=usdc_id,
                prime_id=prime_id,
                proxy_hex=UV_PROXY_PLAIN,
                balance=UV_USDC_BALANCE,
                block=1000,
                tx="e1" * 32,
                direction="sweep",
            )

            # Allowlisted + underlying whose token row has a NULL symbol: the
            # identity projection must emit all-or-nothing, never a hybrid of
            # underlying id/address with the held token's symbol.
            symbolless_id = await conn.fetchval(
                """
                INSERT INTO token (chain_id, address, symbol, decimals)
                VALUES (1, $1, NULL, 18)
                ON CONFLICT (chain_id, address) DO UPDATE SET symbol = NULL
                RETURNING id
                """,
                bytes.fromhex(_UV_SYMBOLLESS_UNDERLYING_HEX),
            )
            await insert_allocation_position(
                conn,
                token_id=spark_id,
                prime_id=prime_id,
                proxy_hex=UV_PROXY_SYMBOLLESS_UNDERLYING,
                balance=UV_SPARKPRIME_BALANCE,
                block=1000,
                tx="e2" * 32,
                direction="sweep",
                underlying_value=UV_SPARKPRIME_UNDERLYING_VALUE,
                underlying_token_id=symbolless_id,
            )

            # Allowlisted Uni V3 pool position: no own oracle possible (the
            # address is the pool contract), priced ONLY by the tracker-computed
            # underlying_value x the USDC price.
            univ3_id = await insert_token(conn, "UNIV3-LP-AUSD-USDC", 6, bytes.fromhex(_UV_UNIV3_POOL_HEX))
            await insert_allocation_position(
                conn,
                token_id=univ3_id,
                prime_id=prime_id,
                proxy_hex=UV_PROXY_UNIV3_POOL,
                balance=UV_UNIV3_UNDERLYING_VALUE,
                block=1000,
                tx="f1" * 32,
                direction="sweep",
                underlying_value=UV_UNIV3_UNDERLYING_VALUE,
                underlying_token_id=usdc_id,
            )
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Oracle price tie seed (frozen-source replant shape)
#
# Production shape this reproduces: a protocol bound to TWO oracles where a
# retired source re-emits rows on republished (reorged) blocks, so both
# oracles carry a price row for the same underlying at IDENTICAL
# (block_number, block_version, processing_version) — warehouse-verified at
# block 25524078/1/0 for the frozen sparklend stables next to chainlink.
# Ordering by those three keys alone leaves the winner to the executor, so a
# position's USD value flips between identical API calls. The reads break the
# tie on oracle_id DESC; these seeds insert the LOWER-oracle_id (stale) row
# first so an un-tiebroken top-1 sort is biased toward returning it.
# ---------------------------------------------------------------------------

TIE_PROXY_HEX = "f1" * 20
_TIE_VAULT_HEX = "f2" * 20
_TIE_PROTOCOL_HEX = "f3" * 20
_TIE_STALE_ORACLE_HEX = "f4" * 20
_TIE_FRESH_ORACLE_HEX = "f5" * 20
_TIE_UNDERLYING_HEX = "f6" * 20
_TIE_RECEIPT_HEX = "f7" * 20

# Distinct prices so the winning oracle is observable in every USD figure.
TIE_STALE_PRICE = Decimal("1.00")
TIE_FRESH_PRICE = Decimal("1.25")
TIE_BALANCE = Decimal("100")
TIE_UNDERLYING_VALUE = Decimal("117.5")
_TIE_PRICE_BLOCK = 5000


async def seed_price_tiebreak_positions(db_url: str) -> None:
    """Seed one receipt position whose underlying has two tied price rows.

    Everything is seeded locally (protocol, two oracles, both bindings,
    registry row, position) so the scenario does not lean on migration-seeded
    registry rows. The stale oracle is created first, so it has the LOWER id;
    its price row is also inserted first.
    """
    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            prime_id = await conn.fetchval(
                "INSERT INTO prime (name, vault_address) VALUES ('price_tiebreak', $1) RETURNING id",
                bytes.fromhex(_TIE_VAULT_HEX),
            )
            protocol_id = await conn.fetchval(
                "INSERT INTO protocol (chain_id, address, name, protocol_type) "
                "VALUES (1, $1, 'tieLending', 'lending') RETURNING id",
                bytes.fromhex(_TIE_PROTOCOL_HEX),
            )
            stale_oracle_id = await conn.fetchval(
                "INSERT INTO oracle (name, display_name, chain_id, address) "
                "VALUES ('tie_stale', 'Tie stale test oracle', 1, $1) RETURNING id",
                bytes.fromhex(_TIE_STALE_ORACLE_HEX),
            )
            fresh_oracle_id = await conn.fetchval(
                "INSERT INTO oracle (name, display_name, chain_id, address) "
                "VALUES ('tie_fresh', 'Tie fresh test oracle', 1, $1) RETURNING id",
                bytes.fromhex(_TIE_FRESH_ORACLE_HEX),
            )
            if not stale_oracle_id < fresh_oracle_id:
                raise RuntimeError("seed premise broken: stale oracle must have the lower id")
            for oracle_id in (stale_oracle_id, fresh_oracle_id):
                await conn.execute(
                    "INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block) VALUES ($1, $2, 1)",
                    protocol_id,
                    oracle_id,
                )

            underlying_id = await insert_token(conn, "tieUSD", 6, bytes.fromhex(_TIE_UNDERLYING_HEX))
            receipt_token_id = await insert_token(conn, "tieReceipt", 6, bytes.fromhex(_TIE_RECEIPT_HEX))
            await insert_receipt_token_row(
                conn,
                protocol_id=protocol_id,
                underlying_token_id=underlying_id,
                address=bytes.fromhex(_TIE_RECEIPT_HEX),
                symbol="tieReceipt",
            )

            # Two price rows at IDENTICAL (block_number, block_version,
            # processing_version); only oracle_id (and price) differ. Both
            # sources stay enabled so the tie is genuine (only oracle_id breaks
            # it), not resolved by the disabled-mapping filter.
            for oracle_id, price in ((stale_oracle_id, TIE_STALE_PRICE), (fresh_oracle_id, TIE_FRESH_PRICE)):
                await conn.execute(
                    "INSERT INTO onchain_token_price "
                    "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
                    "VALUES ($1, $2, $3, 1, NOW(), $4)",
                    underlying_id,
                    oracle_id,
                    _TIE_PRICE_BLOCK,
                    price,
                )
                await insert_oracle_asset(conn, oracle_id, underlying_id)

            await insert_allocation_position(
                conn,
                token_id=receipt_token_id,
                prime_id=prime_id,
                proxy_hex=TIE_PROXY_HEX,
                balance=TIE_BALANCE,
                block=_TIE_PRICE_BLOCK,
                tx="f9" * 32,
                direction="in",
                underlying_value=TIE_UNDERLYING_VALUE,
                underlying_token_id=underlying_id,
            )
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Retired-source seed (disabled oracle_asset, higher-block replant shape)
#
# A protocol bound to TWO oracles where the RETIRED source holds the HIGHER
# block (and later timestamp, and higher oracle_id) — the exact shape a reorg
# leaves when it republishes a frozen sparklend row at a fresh height next to a
# live chainlink row that change-suppression has not refreshed. Disabling the
# oracle_asset mapping must retire the source from every latest-price read
# immediately at read time, not merely from future collection: the enabled
# source's LOWER-block price must win. Every ordering signal (recency, then
# oracle_id) favours the disabled source here, so only the enabled-mapping
# filter can produce the correct answer.
# ---------------------------------------------------------------------------

DIS_RECEIPT_PROXY_HEX = "e1" * 20
DIS_DIRECT_PROXY_HEX = "e2" * 20
_DIS_VAULT_HEX = "e3" * 20
_DIS_PROTOCOL_HEX = "e4" * 20
_DIS_ENABLED_ORACLE_HEX = "e5" * 20
_DIS_DISABLED_ORACLE_HEX = "e6" * 20
_DIS_UNDERLYING_HEX = "e7" * 20
_DIS_RECEIPT_HEX = "e8" * 20
_DIS_DIRECT_HEX = "e9" * 20

DIS_ENABLED_PRICE = Decimal("1.25")  # live source, LOWER block: must win
DIS_DISABLED_PRICE = Decimal("1.00")  # retired source, HIGHER block: must lose
DIS_RECEIPT_BALANCE = Decimal("100")
DIS_RECEIPT_UNDERLYING_VALUE = Decimal("117.5")
DIS_DIRECT_BALANCE = Decimal("200")
_DIS_ENABLED_BLOCK = 6000
_DIS_DISABLED_BLOCK = 6001
_DIS_BASE_TS = dt.datetime(2026, 3, 1, tzinfo=dt.UTC)


async def _insert_disabled_source_prices(
    conn: asyncpg.Connection, *, token_id: int, enabled_oracle_id: int, disabled_oracle_id: int
) -> None:
    """Price one token from both sources; the higher-block source's mapping is disabled."""
    for oracle_id, block, offset, price, enabled in (
        (enabled_oracle_id, _DIS_ENABLED_BLOCK, dt.timedelta(0), DIS_ENABLED_PRICE, True),
        (disabled_oracle_id, _DIS_DISABLED_BLOCK, dt.timedelta(minutes=1), DIS_DISABLED_PRICE, False),
    ):
        await conn.execute(
            "INSERT INTO onchain_token_price "
            "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
            "VALUES ($1, $2, $3, 0, $4, $5)",
            token_id,
            oracle_id,
            block,
            _DIS_BASE_TS + offset,
            price,
        )
        await insert_oracle_asset(conn, oracle_id, token_id, enabled=enabled)


async def seed_disabled_source_positions(db_url: str) -> None:
    """Seed a receipt position and a bare direct holding whose retired source holds the higher block.

    One protocol bound to two oracles: the enabled source prices at the LOWER
    block, the disabled (retired) source at the HIGHER block, later timestamp
    and higher oracle_id. The receipt-token underlying and a bare directly-held
    token each get that two-source shape, so the receipt, direct-holding and
    token-catalog latest-price reads can all be checked against the same seed.
    Only excluding the disabled mapping yields the enabled source's price.
    """
    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            prime_id = await conn.fetchval(
                "INSERT INTO prime (name, vault_address) VALUES ('disabled_source', $1) RETURNING id",
                bytes.fromhex(_DIS_VAULT_HEX),
            )
            protocol_id = await conn.fetchval(
                "INSERT INTO protocol (chain_id, address, name, protocol_type) "
                "VALUES (1, $1, 'disLending', 'lending') RETURNING id",
                bytes.fromhex(_DIS_PROTOCOL_HEX),
            )
            # Enabled oracle created first -> lower id; the disabled source is
            # created second so it also holds the higher oracle_id, making the
            # tiebreak favour it too.
            enabled_oracle_id = await conn.fetchval(
                "INSERT INTO oracle (name, display_name, chain_id, address) "
                "VALUES ('dis_enabled', 'Disabled-source test: live oracle', 1, $1) RETURNING id",
                bytes.fromhex(_DIS_ENABLED_ORACLE_HEX),
            )
            disabled_oracle_id = await conn.fetchval(
                "INSERT INTO oracle (name, display_name, chain_id, address) "
                "VALUES ('dis_disabled', 'Disabled-source test: retired oracle', 1, $1) RETURNING id",
                bytes.fromhex(_DIS_DISABLED_ORACLE_HEX),
            )
            if not enabled_oracle_id < disabled_oracle_id:
                raise RuntimeError("seed premise broken: disabled oracle must have the higher id")
            for oracle_id in (enabled_oracle_id, disabled_oracle_id):
                await conn.execute(
                    "INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block) VALUES ($1, $2, 1)",
                    protocol_id,
                    oracle_id,
                )

            underlying_id = await insert_token(conn, "disUnderlying", 6, bytes.fromhex(_DIS_UNDERLYING_HEX))
            direct_id = await insert_token(conn, "disDirect", 6, bytes.fromhex(_DIS_DIRECT_HEX))
            receipt_token_id = await insert_token(conn, "disReceipt", 6, bytes.fromhex(_DIS_RECEIPT_HEX))
            await insert_receipt_token_row(
                conn,
                protocol_id=protocol_id,
                underlying_token_id=underlying_id,
                address=bytes.fromhex(_DIS_RECEIPT_HEX),
                symbol="disReceipt",
            )

            for token_id in (underlying_id, direct_id):
                await _insert_disabled_source_prices(
                    conn,
                    token_id=token_id,
                    enabled_oracle_id=enabled_oracle_id,
                    disabled_oracle_id=disabled_oracle_id,
                )

            await insert_allocation_position(
                conn,
                token_id=receipt_token_id,
                prime_id=prime_id,
                proxy_hex=DIS_RECEIPT_PROXY_HEX,
                balance=DIS_RECEIPT_BALANCE,
                block=_DIS_DISABLED_BLOCK,
                tx="ea" * 32,
                direction="in",
                underlying_value=DIS_RECEIPT_UNDERLYING_VALUE,
                underlying_token_id=underlying_id,
            )
            await insert_allocation_position(
                conn,
                token_id=direct_id,
                prime_id=prime_id,
                proxy_hex=DIS_DIRECT_PROXY_HEX,
                balance=DIS_DIRECT_BALANCE,
                block=_DIS_DISABLED_BLOCK,
                tx="eb" * 32,
                direction="in",
            )
    finally:
        await conn.close()


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


# ---------------------------------------------------------------------------
# Receipt-token redeemable-value seed (ER missing prices)
#
# One prime, three proxies:
#
# ``RUV_PROXY_HEX`` holds five receipt-token positions, one per valuation basis
# of the receipt-token reads (positions list, per-token and total USD exposure,
# exposure buckets):
#   * syrupLike        underlying_value at a non-1:1 share ratio -> priced by it
#   * legacyReceipt    NULL underlying_value (pre-2026-07-02 row) -> balance-based
#   * aOneToOne        aToken shape, underlying_value == balance -> unchanged
#   * fullyRedeemed    underlying_value = 0 with balance > 0 -> worth exactly 0
#                      (COALESCE falls back on NULL only, never on 0)
#   * divergentReceipt position's own underlying_token_id differs from the
#                      receipt_token registry's -> refused a price (NULL, surfaced)
# These wrap underlyings priced through the migration-seeded 'Aave V3' protocol +
# 'aave_v3' oracle protocol_oracle binding, which the receipt reads resolve
# prices through.
#
# ``RUV_LOCF_PROXY_HEX`` holds one receipt position observed at three explicit
# ``created_at`` timestamps (two in the first hourly bucket, one two buckets
# later) so exposure-bucket last()-within-bucket and LOCF carry are observable
# mid-series.
#
# ``RUV_MORPHO_PROXY_HEX`` holds a Morpho-vault share (sparkUSDCbc semantics)
# registered against a Morpho-Blue-like protocol/oracle binding seeded here,
# deliberately independent of migration-seeded registry rows.
# ---------------------------------------------------------------------------

RUV_PROXY_HEX = "a7" * 20
RUV_MORPHO_PROXY_HEX = "a8" * 20
RUV_LOCF_PROXY_HEX = "a9" * 20

_RUV_VAULT_HEX = "98" * 20
_RUV_UNDERLYING_HEX = "b7" * 20
_RUV_ALT_UNDERLYING_HEX = "b8" * 20
_RUV_MORPHO_UNDERLYING_HEX = "b9" * 20
_RUV_SYRUP_LIKE_RECEIPT_HEX = "c1" * 20
_RUV_LEGACY_RECEIPT_HEX = "c2" * 20
_RUV_ATOKEN_RECEIPT_HEX = "c3" * 20
_RUV_DIVERGENT_RECEIPT_HEX = "c4" * 20
_RUV_ZERO_RECEIPT_HEX = "c5" * 20
_RUV_LOCF_RECEIPT_HEX = "c6" * 20
_RUV_MORPHO_RECEIPT_HEX = "c7" * 20
_RUV_MORPHO_PROTOCOL_HEX = "d0" * 20
_RUV_MORPHO_ORACLE_HEX = "d1" * 20

# Not 1.00 on purpose: at a 1:1 price a dropped price multiplication is
# invisible, so every expected value folds in the 1.03 factor.
RUV_UNDERLYING_PRICE = Decimal("1.03")
# Priced but must never be used: the divergent position is refused a price, so
# neither the registry price nor this one may surface for it.
RUV_ALT_UNDERLYING_PRICE = Decimal("5.00")

RUV_SYRUP_LIKE_BALANCE = Decimal("100")
RUV_SYRUP_LIKE_UNDERLYING_VALUE = Decimal("117.23")
RUV_LEGACY_BALANCE = Decimal("250")
RUV_ATOKEN_BALANCE = Decimal("300")
RUV_DIVERGENT_BALANCE = Decimal("4")
RUV_DIVERGENT_UNDERLYING_VALUE = Decimal("10")
RUV_ZERO_REDEEMED_BALANCE = Decimal("40")
# Fully-redeemed vault: shares still outstanding, redeemable value exactly 0.
RUV_ZERO_REDEEMED_UNDERLYING_VALUE = Decimal("0")

# LOCF series: constant share balance, distinct from every underlying_value so
# a balance-based bucket value can never masquerade as a redeemable one.
RUV_LOCF_BASE_TS = dt.datetime(2026, 3, 1, tzinfo=dt.UTC)
RUV_LOCF_BALANCE = Decimal("90")
RUV_LOCF_STALE_VALUE = Decimal("100")  # bucket 0, backdated 10 min in
RUV_LOCF_NEWER_VALUE = Decimal("140")  # bucket 0, 20 min in; must win within the bucket
RUV_LOCF_LATER_VALUE = Decimal("180")  # bucket 2; ends the carried value

RUV_MORPHO_SHARE_BALANCE = Decimal("1000")
RUV_MORPHO_UNDERLYING_VALUE = Decimal("1023.917201")
# Distinct from RUV_UNDERLYING_PRICE so a cross-binding price mixup is visible.
RUV_MORPHO_UNDERLYING_PRICE = Decimal("1.04")


async def seed_receipt_underlying_value_positions(db_url: str) -> None:
    """Seed the receipt-token redeemable-value scenarios into the given database."""
    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            prime_id = await conn.fetchval(
                "INSERT INTO prime (name, vault_address) VALUES ('receipt_uv', $1) RETURNING id",
                bytes.fromhex(_RUV_VAULT_HEX),
            )
            protocol_id = await conn.fetchval("SELECT id FROM protocol WHERE name = 'Aave V3' AND chain_id = 1")
            oracle_id = await conn.fetchval("SELECT id FROM oracle WHERE name = 'aave_v3'")
            if protocol_id is None or oracle_id is None:
                raise RuntimeError("Aave V3 protocol / aave_v3 oracle not seeded by migrations")

            underlying_id = await insert_token(conn, "rUSDC", 6, bytes.fromhex(_RUV_UNDERLYING_HEX))
            alt_underlying_id = await insert_token(conn, "rALT", 6, bytes.fromhex(_RUV_ALT_UNDERLYING_HEX))
            await _insert_price(conn, underlying_id, oracle_id, RUV_UNDERLYING_PRICE)
            await _insert_price(conn, alt_underlying_id, oracle_id, RUV_ALT_UNDERLYING_PRICE)

            receipts = [
                ("syrupLike", _RUV_SYRUP_LIKE_RECEIPT_HEX),
                ("legacyReceipt", _RUV_LEGACY_RECEIPT_HEX),
                ("aOneToOne", _RUV_ATOKEN_RECEIPT_HEX),
                ("divergentReceipt", _RUV_DIVERGENT_RECEIPT_HEX),
                ("fullyRedeemed", _RUV_ZERO_RECEIPT_HEX),
            ]
            receipt_token_ids: dict[str, int] = {}
            for symbol, receipt_hex in receipts:
                receipt_token_ids[symbol] = await insert_token(conn, symbol, 6, bytes.fromhex(receipt_hex))
                await insert_receipt_token_row(
                    conn,
                    protocol_id=protocol_id,
                    underlying_token_id=underlying_id,
                    address=bytes.fromhex(receipt_hex),
                    symbol=symbol,
                )

            positions = [
                # Non-1:1 share ratio: priced by underlying_value, not balance.
                ("syrupLike", RUV_SYRUP_LIKE_BALANCE, RUV_SYRUP_LIKE_UNDERLYING_VALUE, underlying_id),
                # Row written before underlying_value existed: balance-based fallback.
                ("legacyReceipt", RUV_LEGACY_BALANCE, None, None),
                # aToken: underlying_value == balance by construction, value unchanged.
                ("aOneToOne", RUV_ATOKEN_BALANCE, RUV_ATOKEN_BALANCE, underlying_id),
                # Position's own underlying_token_id diverges from the registry's:
                # pricing must refuse the row (NULL amount_usd, surfaced).
                ("divergentReceipt", RUV_DIVERGENT_BALANCE, RUV_DIVERGENT_UNDERLYING_VALUE, alt_underlying_id),
                # Fully-redeemed vault: shares outstanding, redeemable value 0.
                ("fullyRedeemed", RUV_ZERO_REDEEMED_BALANCE, RUV_ZERO_REDEEMED_UNDERLYING_VALUE, underlying_id),
            ]
            for index, (symbol, balance, underlying_value, underlying_token_id) in enumerate(positions):
                await insert_allocation_position(
                    conn,
                    token_id=receipt_token_ids[symbol],
                    prime_id=prime_id,
                    proxy_hex=RUV_PROXY_HEX,
                    balance=balance,
                    block=1000,
                    tx=f"{index:02x}" * 32,
                    direction="in",
                    underlying_value=underlying_value,
                    underlying_token_id=underlying_token_id,
                )

            await _ruv_seed_locf_series(conn, prime_id=prime_id, protocol_id=protocol_id, underlying_id=underlying_id)
            await _ruv_seed_morpho_like_position(conn, prime_id=prime_id)
    finally:
        await conn.close()


async def _ruv_seed_locf_series(
    conn: asyncpg.Connection, *, prime_id: int, protocol_id: int, underlying_id: int
) -> None:
    """Seed one receipt position observed at three explicit created_at timestamps.

    Two observations land in the first hourly bucket (the backdated stale
    value, then the newer one that must win) and one lands two buckets later,
    so the exposure-bucket tests can observe last()-within-bucket and the LOCF
    carry across the empty middle bucket.
    """
    locf_token_id = await insert_token(conn, "locfReceipt", 6, bytes.fromhex(_RUV_LOCF_RECEIPT_HEX))
    await insert_receipt_token_row(
        conn,
        protocol_id=protocol_id,
        underlying_token_id=underlying_id,
        address=bytes.fromhex(_RUV_LOCF_RECEIPT_HEX),
        symbol="locfReceipt",
    )
    observations = [
        (RUV_LOCF_STALE_VALUE, dt.timedelta(minutes=10), 1100),
        (RUV_LOCF_NEWER_VALUE, dt.timedelta(minutes=20), 1101),
        (RUV_LOCF_LATER_VALUE, dt.timedelta(hours=2, minutes=10), 1102),
    ]
    for index, (underlying_value, offset, block) in enumerate(observations):
        await insert_allocation_position(
            conn,
            token_id=locf_token_id,
            prime_id=prime_id,
            proxy_hex=RUV_LOCF_PROXY_HEX,
            balance=RUV_LOCF_BALANCE,
            block=block,
            tx=f"1{index}" * 32,
            direction="in",
            underlying_value=underlying_value,
            underlying_token_id=underlying_id,
            created_at=RUV_LOCF_BASE_TS + offset,
        )


async def _ruv_seed_morpho_like_position(conn: asyncpg.Connection, *, prime_id: int) -> None:
    """Seed a Morpho-vault-share receipt position (sparkUSDCbc semantics) with its own binding.

    Everything the receipt reads price through (protocol, oracle,
    protocol_oracle binding, registry row, underlying token and its price) is
    seeded here, so the scenario does not lean on migration-seeded registry
    rows (which own the real sparkUSDCbc registration).
    """
    morpho_protocol_id = await conn.fetchval(
        "INSERT INTO protocol (chain_id, address, name, protocol_type) "
        "VALUES (1, $1, 'morphoBlueLike', 'lending') RETURNING id",
        bytes.fromhex(_RUV_MORPHO_PROTOCOL_HEX),
    )
    morpho_oracle_id = await conn.fetchval(
        "INSERT INTO oracle (name, display_name, chain_id, address) "
        "VALUES ('ruv_morpho_like', 'Morpho-Blue-like test oracle', 1, $1) RETURNING id",
        bytes.fromhex(_RUV_MORPHO_ORACLE_HEX),
    )
    await conn.execute(
        "INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block) VALUES ($1, $2, 1)",
        morpho_protocol_id,
        morpho_oracle_id,
    )
    underlying_id = await insert_token(conn, "mbUSDC", 6, bytes.fromhex(_RUV_MORPHO_UNDERLYING_HEX))
    await _insert_price(conn, underlying_id, morpho_oracle_id, RUV_MORPHO_UNDERLYING_PRICE)
    share_token_id = await insert_token(conn, "sparkUSDCbcLike", 18, bytes.fromhex(_RUV_MORPHO_RECEIPT_HEX))
    await insert_receipt_token_row(
        conn,
        protocol_id=morpho_protocol_id,
        underlying_token_id=underlying_id,
        address=bytes.fromhex(_RUV_MORPHO_RECEIPT_HEX),
        symbol="sparkUSDCbcLike",
    )
    await insert_allocation_position(
        conn,
        token_id=share_token_id,
        prime_id=prime_id,
        proxy_hex=RUV_MORPHO_PROXY_HEX,
        balance=RUV_MORPHO_SHARE_BALANCE,
        block=1000,
        tx="20" * 32,
        direction="in",
        underlying_value=RUV_MORPHO_UNDERLYING_VALUE,
        underlying_token_id=underlying_id,
    )


# ---------------------------------------------------------------------------
# Flow share-ratio seed (net_flow_usd valued at the share ratio)
#
# One prime; one receipt token PER SCENARIO (the nearest-ratio lookup is
# token-global, so sharing a token would let one scenario's rows become
# another's nearest candidates). All wrap frUSDC, priced at 1.03 (not 1.00,
# so a dropped price multiplication is visible). Donor rows, the nearest-ratio
# candidates, sit on ``_FR_PROXY_DONOR``, outside every tested proxy's
# buckets: the lookup must find them across proxies because the share ratio is
# a property of the token, not of any position.
#
#   * FR_PROXY_RATIO            deposit with a usable own ratio -> own ratio
#   * FR_PROXY_LEGACY           NULL underlying_value -> nearest donor's ratio
#   * FR_PROXY_FULL_EXIT        balance = 0 after a full exit; the token's only
#                               valued row sits AFTER the exit (the warehouse
#                               shape of the syrupUSDC exit at block 24869871)
#   * FR_PROXY_NEVER_VALUED     no usable row anywhere (0/0 rows like
#                               syrupUSDT's don't count) -> raw tx_amount
#   * FR_PROXY_ATOKEN           legacy row on a 1:1 token: the nearest ratio is
#                               exactly 1, so the value equals the raw fallback
#   * FR_PROXY_DRAINED          underlying_value = 0, balance > 0 -> own ratio 0
#   * FR_PROXY_DIVERGENT        row's own underlying_token_id diverges from the
#                               registry's -> refused, contributes nothing
#   * FR_PROXY_DONOR_DIVERGENT  the only valued same-token row is divergent ->
#                               not a candidate -> raw tx_amount
#   * FR_PROXY_DISTANCE         donors on both sides at different distances ->
#                               the closer one wins regardless of side
#   * FR_PROXY_TIE              donors equidistant -> the at-or-before one wins
#   * FR_PROXY_MIXED            ratio in + ratio out + legacy in + sweep, one
#                               bucket; the legacy row borrows the out row's
#                               ratio (nearest at-or-before)
# Every row carries the same explicit created_at so each proxy's events land
# in a single hourly bucket.
# ---------------------------------------------------------------------------

FR_PROXY_RATIO = "1e" * 20
FR_PROXY_LEGACY = "2e" * 20
FR_PROXY_FULL_EXIT = "3e" * 20
FR_PROXY_DRAINED = "4e" * 20
FR_PROXY_MIXED = "5e" * 20
FR_PROXY_DIVERGENT = "6e" * 20
FR_PROXY_NEVER_VALUED = "7e" * 20
FR_PROXY_ATOKEN = "8e" * 20
FR_PROXY_DONOR_DIVERGENT = "9e" * 20
FR_PROXY_DISTANCE = "ae" * 20
FR_PROXY_TIE = "be" * 20
_FR_PROXY_DONOR = "fe" * 20

_FR_VAULT_HEX = "97" * 20
_FR_UNDERLYING_HEX = "e5" * 20
# Priced like the RUV divergent scenario so the refusal is what keeps the
# wrong-unit value out, not a missing price.
_FR_ALT_UNDERLYING_HEX = "e7" * 20
# Synthetic protocol/oracle registration (like _ruv_seed_morpho_like_position):
# the scenarios must not lean on migration-seeded registry rows, and protocol
# display names are not natural keys.
_FR_PROTOCOL_HEX = "e8" * 20
_FR_ORACLE_HEX = "e9" * 20

FR_UNDERLYING_PRICE = Decimal("1.03")
FR_BUCKET_TS = dt.datetime(2026, 3, 5, 0, 30, tzinfo=dt.UTC)

# Ratio deposit: 100 shares in while the row's own balance/underlying_value pin
# the share ratio at 234 / 200 = 1.17.
FR_RATIO_TX_AMOUNT = Decimal("100")
FR_RATIO_BALANCE = Decimal("200")
FR_RATIO_UNDERLYING_VALUE = Decimal("234")

# Legacy row written before underlying_value existed (both columns NULL), plus
# a valued donor sweep one block earlier pinning the token's ratio at 1.17.
FR_LEGACY_TX_AMOUNT = Decimal("100")
FR_LEGACY_BALANCE = Decimal("500")
FR_LEGACY_DONOR_BALANCE = Decimal("200")
FR_LEGACY_DONOR_UNDERLYING_VALUE = Decimal("234")

# Full exit: the withdrawal leaves balance = 0 on its own row; the donor sweep
# AFTER it carries the warehouse-observed syrupUSDC ratio of 1.172241.
FR_FULL_EXIT_TX_AMOUNT = Decimal("150")
FR_FULL_EXIT_DONOR_BALANCE = Decimal("1000000")
FR_FULL_EXIT_DONOR_UNDERLYING_VALUE = Decimal("1172241")

# Never valued: a legacy flow plus a 0/0 sweep (syrupUSDT's shape:
# underlying_value present but balance = 0 is not a usable ratio).
FR_NEVER_VALUED_TX_AMOUNT = Decimal("100")
FR_NEVER_VALUED_BALANCE = Decimal("500")

# 1:1 aToken: the donor's underlying_value equals its balance, so the borrowed
# ratio is exactly 1 and the flow's value must equal the raw fallback.
FR_ATOKEN_TX_AMOUNT = Decimal("100")
FR_ATOKEN_BALANCE = Decimal("300")
FR_ATOKEN_DONOR_BALANCE = Decimal("300")

# Drained vault: shares outstanding but redeemable value exactly 0.
FR_DRAINED_TX_AMOUNT = Decimal("80")
FR_DRAINED_BALANCE = Decimal("400")

# Divergent row: its 2.5 ratio would price to a plausible wrong number if the
# refusal were dropped.
FR_DIVERGENT_TX_AMOUNT = Decimal("8")
FR_DIVERGENT_BALANCE = Decimal("4")
FR_DIVERGENT_UNDERLYING_VALUE = Decimal("10")
FR_ALT_UNDERLYING_PRICE = Decimal("5.00")

# Donor-divergent: the only valued same-token row is divergent (wrong-unit
# underlying_value), so the legacy flow must fall back raw.
FR_DONOR_DIVERGENT_TX_AMOUNT = Decimal("50")
FR_DONOR_DIVERGENT_BALANCE = Decimal("500")

# Distance: the far donor (two blocks before, ratio 1.10) must lose to the
# near donor (one block after, ratio 1.20).
FR_DISTANCE_TX_AMOUNT = Decimal("60")
FR_DISTANCE_BALANCE = Decimal("500")
FR_DISTANCE_FAR_DONOR_BALANCE = Decimal("100")
FR_DISTANCE_FAR_DONOR_UNDERLYING_VALUE = Decimal("110")
FR_DISTANCE_NEAR_DONOR_BALANCE = Decimal("100")
FR_DISTANCE_NEAR_DONOR_UNDERLYING_VALUE = Decimal("120")

# Tie: donors one block before (ratio 1.10) and one block after (ratio 1.30);
# the at-or-before donor must win.
FR_TIE_TX_AMOUNT = Decimal("70")
FR_TIE_BALANCE = Decimal("500")
FR_TIE_BEFORE_DONOR_BALANCE = Decimal("100")
FR_TIE_BEFORE_DONOR_UNDERLYING_VALUE = Decimal("110")
FR_TIE_AFTER_DONOR_BALANCE = Decimal("100")
FR_TIE_AFTER_DONOR_UNDERLYING_VALUE = Decimal("130")

# Mixed bucket: both own-ratio rows sit at the same 1.17 share ratio; the
# legacy row borrows the out row's ratio (nearest at-or-before, one block).
FR_MIXED_IN_TX_AMOUNT = Decimal("100")
FR_MIXED_IN_BALANCE = Decimal("200")
FR_MIXED_IN_UNDERLYING_VALUE = Decimal("234")
FR_MIXED_OUT_TX_AMOUNT = Decimal("40")
FR_MIXED_OUT_BALANCE = Decimal("160")
FR_MIXED_OUT_UNDERLYING_VALUE = Decimal("187.2")
FR_MIXED_LEGACY_TX_AMOUNT = Decimal("10")
FR_MIXED_LEGACY_BALANCE = Decimal("300")
FR_MIXED_SWEEP_TX_AMOUNT = Decimal("1000")


async def seed_flow_share_ratio_activity(db_url: str) -> None:
    """Seed the flow share-ratio activity scenarios into the given database."""
    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            prime_id = await conn.fetchval(
                "INSERT INTO prime (name, vault_address) VALUES ('flow_ratio', $1) RETURNING id",
                bytes.fromhex(_FR_VAULT_HEX),
            )
            protocol_id = await conn.fetchval(
                "INSERT INTO protocol (chain_id, address, name, protocol_type) "
                "VALUES (1, $1, 'flowRatioLike', 'lending') RETURNING id",
                bytes.fromhex(_FR_PROTOCOL_HEX),
            )
            oracle_id = await conn.fetchval(
                "INSERT INTO oracle (name, display_name, chain_id, address) "
                "VALUES ('fr_flow_ratio', 'Flow share-ratio test oracle', 1, $1) RETURNING id",
                bytes.fromhex(_FR_ORACLE_HEX),
            )
            await conn.execute(
                "INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block) VALUES ($1, $2, 1)",
                protocol_id,
                oracle_id,
            )

            underlying_id = await insert_token(conn, "frUSDC", 6, bytes.fromhex(_FR_UNDERLYING_HEX))
            await _insert_price(conn, underlying_id, oracle_id, FR_UNDERLYING_PRICE)
            alt_underlying_id = await insert_token(conn, "frALT", 6, bytes.fromhex(_FR_ALT_UNDERLYING_HEX))
            await _insert_price(conn, alt_underlying_id, oracle_id, FR_ALT_UNDERLYING_PRICE)

            async def receipt(address_hex: str, symbol: str) -> int:
                token_id = await insert_token(conn, symbol, 6, bytes.fromhex(address_hex))
                await insert_receipt_token_row(
                    conn,
                    protocol_id=protocol_id,
                    underlying_token_id=underlying_id,
                    address=bytes.fromhex(address_hex),
                    symbol=symbol,
                )
                return token_id

            ratio_token = await receipt("d2" * 20, "frVaultRatio")
            legacy_token = await receipt("d3" * 20, "frVaultLegacy")
            exit_token = await receipt("d4" * 20, "frVaultExit")
            never_token = await receipt("d5" * 20, "frVaultNever")
            atoken_token = await receipt("d6" * 20, "frAToken")
            drained_token = await receipt("d7" * 20, "frVaultDrained")
            divergent_token = await receipt("d8" * 20, "frVaultDivergent")
            donor_div_token = await receipt("d9" * 20, "frVaultDonorDiv")
            distance_token = await receipt("da" * 20, "frVaultDistance")
            tie_token = await receipt("db" * 20, "frVaultTie")
            mixed_token = await receipt("dc" * 20, "frVaultMixed")

            donor = _FR_PROXY_DONOR
            # (token, proxy, direction, tx_amount, balance, underlying_value,
            # underlying_token_id, block); underlying_value/underlying_token_id
            # follow the both-or-neither invariant, with the divergent rows
            # deliberately carrying the wrong-unit alt underlying.
            rows = [
                (
                    ratio_token,
                    FR_PROXY_RATIO,
                    "in",
                    FR_RATIO_TX_AMOUNT,
                    FR_RATIO_BALANCE,
                    FR_RATIO_UNDERLYING_VALUE,
                    underlying_id,
                    9000,
                ),
                (legacy_token, FR_PROXY_LEGACY, "in", FR_LEGACY_TX_AMOUNT, FR_LEGACY_BALANCE, None, None, 9101),
                (
                    legacy_token,
                    donor,
                    "sweep",
                    Decimal(0),
                    FR_LEGACY_DONOR_BALANCE,
                    FR_LEGACY_DONOR_UNDERLYING_VALUE,
                    underlying_id,
                    9100,
                ),
                (
                    exit_token,
                    FR_PROXY_FULL_EXIT,
                    "out",
                    FR_FULL_EXIT_TX_AMOUNT,
                    Decimal(0),
                    Decimal(0),
                    underlying_id,
                    9200,
                ),
                (
                    exit_token,
                    donor,
                    "sweep",
                    Decimal(0),
                    FR_FULL_EXIT_DONOR_BALANCE,
                    FR_FULL_EXIT_DONOR_UNDERLYING_VALUE,
                    underlying_id,
                    9201,
                ),
                (
                    never_token,
                    FR_PROXY_NEVER_VALUED,
                    "in",
                    FR_NEVER_VALUED_TX_AMOUNT,
                    FR_NEVER_VALUED_BALANCE,
                    None,
                    None,
                    9300,
                ),
                (never_token, FR_PROXY_NEVER_VALUED, "sweep", Decimal(0), Decimal(0), Decimal(0), underlying_id, 9301),
                (atoken_token, FR_PROXY_ATOKEN, "in", FR_ATOKEN_TX_AMOUNT, FR_ATOKEN_BALANCE, None, None, 9400),
                (
                    atoken_token,
                    donor,
                    "sweep",
                    Decimal(0),
                    FR_ATOKEN_DONOR_BALANCE,
                    FR_ATOKEN_DONOR_BALANCE,
                    underlying_id,
                    9399,
                ),
                (
                    drained_token,
                    FR_PROXY_DRAINED,
                    "in",
                    FR_DRAINED_TX_AMOUNT,
                    FR_DRAINED_BALANCE,
                    Decimal(0),
                    underlying_id,
                    9500,
                ),
                (
                    divergent_token,
                    FR_PROXY_DIVERGENT,
                    "in",
                    FR_DIVERGENT_TX_AMOUNT,
                    FR_DIVERGENT_BALANCE,
                    FR_DIVERGENT_UNDERLYING_VALUE,
                    alt_underlying_id,
                    9600,
                ),
                (
                    donor_div_token,
                    FR_PROXY_DONOR_DIVERGENT,
                    "in",
                    FR_DONOR_DIVERGENT_TX_AMOUNT,
                    FR_DONOR_DIVERGENT_BALANCE,
                    None,
                    None,
                    9700,
                ),
                (
                    donor_div_token,
                    donor,
                    "sweep",
                    Decimal(0),
                    FR_DIVERGENT_BALANCE,
                    FR_DIVERGENT_UNDERLYING_VALUE,
                    alt_underlying_id,
                    9699,
                ),
                (distance_token, FR_PROXY_DISTANCE, "in", FR_DISTANCE_TX_AMOUNT, FR_DISTANCE_BALANCE, None, None, 9800),
                (
                    distance_token,
                    donor,
                    "sweep",
                    Decimal(0),
                    FR_DISTANCE_FAR_DONOR_BALANCE,
                    FR_DISTANCE_FAR_DONOR_UNDERLYING_VALUE,
                    underlying_id,
                    9798,
                ),
                (
                    distance_token,
                    donor,
                    "sweep",
                    Decimal(0),
                    FR_DISTANCE_NEAR_DONOR_BALANCE,
                    FR_DISTANCE_NEAR_DONOR_UNDERLYING_VALUE,
                    underlying_id,
                    9801,
                ),
                (tie_token, FR_PROXY_TIE, "in", FR_TIE_TX_AMOUNT, FR_TIE_BALANCE, None, None, 9900),
                (
                    tie_token,
                    donor,
                    "sweep",
                    Decimal(0),
                    FR_TIE_BEFORE_DONOR_BALANCE,
                    FR_TIE_BEFORE_DONOR_UNDERLYING_VALUE,
                    underlying_id,
                    9899,
                ),
                (
                    tie_token,
                    donor,
                    "sweep",
                    Decimal(0),
                    FR_TIE_AFTER_DONOR_BALANCE,
                    FR_TIE_AFTER_DONOR_UNDERLYING_VALUE,
                    underlying_id,
                    9901,
                ),
                (
                    mixed_token,
                    FR_PROXY_MIXED,
                    "in",
                    FR_MIXED_IN_TX_AMOUNT,
                    FR_MIXED_IN_BALANCE,
                    FR_MIXED_IN_UNDERLYING_VALUE,
                    underlying_id,
                    9010,
                ),
                (
                    mixed_token,
                    FR_PROXY_MIXED,
                    "out",
                    FR_MIXED_OUT_TX_AMOUNT,
                    FR_MIXED_OUT_BALANCE,
                    FR_MIXED_OUT_UNDERLYING_VALUE,
                    underlying_id,
                    9011,
                ),
                (
                    mixed_token,
                    FR_PROXY_MIXED,
                    "in",
                    FR_MIXED_LEGACY_TX_AMOUNT,
                    FR_MIXED_LEGACY_BALANCE,
                    None,
                    None,
                    9012,
                ),
                (
                    mixed_token,
                    FR_PROXY_MIXED,
                    "sweep",
                    FR_MIXED_SWEEP_TX_AMOUNT,
                    FR_MIXED_IN_BALANCE,
                    FR_MIXED_IN_UNDERLYING_VALUE,
                    underlying_id,
                    9013,
                ),
            ]
            for index, (
                token_id,
                proxy_hex,
                direction,
                tx_amount,
                balance,
                underlying_value,
                underlying_token_id,
                block,
            ) in enumerate(rows):
                await insert_allocation_position(
                    conn,
                    token_id=token_id,
                    prime_id=prime_id,
                    proxy_hex=proxy_hex,
                    balance=balance,
                    block=block,
                    tx=f"{0x30 + index:02x}" * 32,
                    direction=direction,
                    underlying_value=underlying_value,
                    underlying_token_id=underlying_token_id,
                    created_at=FR_BUCKET_TS,
                    tx_amount=tx_amount,
                )
    finally:
        await conn.close()
