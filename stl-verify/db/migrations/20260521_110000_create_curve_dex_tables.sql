-- Curve DEX indexer tables (VEC-260).
-- Creates the registry (curve_pool, curve_pool_coin) and 4 hypertables
-- (curve_swap, curve_liquidity_event, curve_stableswap_state,
-- curve_cryptoswap_state) with full auditability (ADR-0002).
--
-- Prerequisites (already applied):
--   20260521_100000_create_dex_prereqs.sql  -- Curve protocol row
--   20260521_105000_seed_steth_token.sql    -- stETH token
--
-- pool_kind domain:
--   'plain_pre_ng'  -- legacy plain pools (no last_price / price_oracle)
--   'plain_ng'      -- NG plain pools (scalar last_price() / price_oracle())
--   'cryptoswap'    -- VolatileSwap / TricryptoNG (indexed price_scale/oracle)
--
-- ============================================================================
-- Interface notes for Tasks 5/9/10 (VEC-260):
-- These do NOT change the SQL below; they record the ABI truth so downstream
-- tasks do not re-discover it.
--
-- NG 2-coin plain pools (pool_kind='plain_ng'):
--   price_oracle() and last_price() are NO-ARG scalars (one price for the
--   pair). stored_rates() returns uint256[2] (fixed-size array, NOT dynamic).
--   N_COINS() REVERTS on NG pools; use n_coins from the registry instead.
--
-- Pre-NG pools (pool_kind='plain_pre_ng'):
--   NO last_price / price_oracle at all. Leave those columns NULL for them.
--
-- Cryptoswap (pool_kind='cryptoswap'):
--   price_scale(i), price_oracle(i), last_prices(i) are uint256-indexed
--   (n-1 values). Pool also exposes public D() and xcp_profit().
-- ============================================================================

-- ============================================================================
-- curve_pool: registry of Curve AMM pools (one row per deployed pool).
-- Migration-seeded and read-only at runtime: the indexer only LoadPools()s this
-- table, there is no application write path. Do not add an UPDATE/DELETE path
-- (or an ON CONFLICT DO UPDATE upsert) without a deliberate design decision.
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_pool
(
    id                BIGSERIAL PRIMARY KEY,
    chain_id          INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id       BIGINT      NOT NULL REFERENCES protocol (id),
    pool_address      BYTEA       NOT NULL,
    pool_kind         VARCHAR(20) NOT NULL CHECK (pool_kind IN ('plain_pre_ng', 'plain_ng', 'cryptoswap')),
    n_coins           SMALLINT    NOT NULL CHECK (n_coins >= 2),
    lp_token_address  BYTEA,
    deploy_block      BIGINT,
    has_a_precise     BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, pool_address)
);

-- chain_id lookups (LoadPools) are served by the UNIQUE (chain_id, pool_address)
-- index, so no separate chain_id index is needed; pool_kind is filtered in
-- application code, not SQL, so it is not indexed.
CREATE INDEX IF NOT EXISTS idx_curve_pool_protocol ON curve_pool (protocol_id);

-- ============================================================================
-- curve_pool_coin: coins within a pool, FK'd to token by (chain_id, address).
-- Join on (chain_id, address) only -- NEVER join by symbol.
-- Migration-seeded and read-only at runtime, like curve_pool.
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_pool_coin
(
    curve_pool_id BIGINT   NOT NULL REFERENCES curve_pool (id),
    coin_index    SMALLINT NOT NULL CHECK (coin_index >= 0),
    token_id      BIGINT   NOT NULL REFERENCES token (id),
    precision     NUMERIC,
    PRIMARY KEY (curve_pool_id, coin_index)
);

CREATE INDEX IF NOT EXISTS idx_curve_pool_coin_token ON curve_pool_coin (token_id);

-- ============================================================================
-- curve_swap: on-chain TokenExchange / TokenExchangeUnderlying events.
-- Hypertable partitioned on block_timestamp (1-day chunks).
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_swap
(
    curve_pool_id      BIGINT      NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    tx_hash            BYTEA       NOT NULL,
    log_index          INT         NOT NULL,
    buyer              BYTEA       NOT NULL,
    sold_id            INT         NOT NULL,
    bought_id          INT         NOT NULL,
    tokens_sold        NUMERIC     NOT NULL,
    tokens_bought      NUMERIC     NOT NULL,
    fee                NUMERIC,
    is_underlying      BOOLEAN     NOT NULL DEFAULT FALSE,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    -- block_timestamp must be in the PK: TimescaleDB requires the partition
    -- column in every unique index on a hypertable.
    PRIMARY KEY (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE curve_swap SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'curve_pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('curve_swap', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_swap', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_swap';
END $$;

-- Processing-version lookup indexes (ADR-0002 §4).
CREATE INDEX IF NOT EXISTS idx_curve_swap_pv_lookup
    ON curve_swap (curve_pool_id, block_number, block_version, log_index, build_id);

-- Build-aware processing-version trigger (ADR-0002 §3).
-- Prefix 'cs' for curve_swap.
CREATE OR REPLACE FUNCTION assign_processing_version_curve_swap()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cs|%s|%s|%s|%s', NEW.curve_pool_id, NEW.block_number, NEW.block_version, NEW.log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM curve_swap
    WHERE curve_pool_id = NEW.curve_pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND log_index     = NEW.log_index
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_swap
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND log_index     = NEW.log_index;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_swap
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_swap();

-- ============================================================================
-- curve_liquidity_event: AddLiquidity / RemoveLiquidity events.
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_liquidity_event
(
    curve_pool_id      BIGINT      NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    tx_hash            BYTEA       NOT NULL,
    log_index          INT         NOT NULL,
    provider           BYTEA       NOT NULL,
    kind               VARCHAR(20) NOT NULL CHECK (kind IN ('add','remove','remove_one','remove_imbalance')),
    token_amounts      NUMERIC[]   NOT NULL,
    coin_index         INT,
    fees               NUMERIC[],
    invariant          NUMERIC,
    token_supply       NUMERIC,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE curve_liquidity_event SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'curve_pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('curve_liquidity_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_liquidity_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_liquidity_event';
END $$;

CREATE INDEX IF NOT EXISTS idx_curve_liquidity_event_pv_lookup
    ON curve_liquidity_event (curve_pool_id, block_number, block_version, log_index, build_id);

-- Prefix 'cle' for curve_liquidity_event.
CREATE OR REPLACE FUNCTION assign_processing_version_curve_liquidity_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cle|%s|%s|%s|%s', NEW.curve_pool_id, NEW.block_number, NEW.block_version, NEW.log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM curve_liquidity_event
    WHERE curve_pool_id = NEW.curve_pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND log_index     = NEW.log_index
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_liquidity_event
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND log_index     = NEW.log_index;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_liquidity_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_liquidity_event();

-- ============================================================================
-- curve_stableswap_state: periodic snapshots of plain-pool oracle state.
--
-- Interface truth (see top-of-file notes):
--   plain_ng pools:     last_price and price_oracle are non-NULL scalars.
--   plain_pre_ng pools: last_price and price_oracle are NULL (no such call).
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_stableswap_state
(
    curve_pool_id      BIGINT      NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    balances           NUMERIC[]   NOT NULL,
    virtual_price      NUMERIC     NOT NULL,
    total_supply       NUMERIC     NOT NULL,
    a                  NUMERIC     NOT NULL,
    fee                NUMERIC     NOT NULL,
    spot_dy                  NUMERIC[]   NOT NULL,
    last_price               NUMERIC,
    price_oracle             NUMERIC,
    a_precise                NUMERIC,
    admin_balances           NUMERIC[],
    stored_rates             NUMERIC[],
    ema_price                NUMERIC,
    get_p                    NUMERIC,
    calc_token_amount        NUMERIC,
    calc_withdraw_one_coin   NUMERIC[],
    processing_version       INT         NOT NULL DEFAULT 0,
    build_id                 INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (curve_pool_id, block_timestamp, block_number, block_version, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE curve_stableswap_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'curve_pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('curve_stableswap_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_stableswap_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_stableswap_state';
END $$;

CREATE INDEX IF NOT EXISTS idx_curve_stableswap_state_pv_lookup
    ON curve_stableswap_state (curve_pool_id, block_number, block_version, build_id);

-- Prefix 'css' for curve_stableswap_state.
CREATE OR REPLACE FUNCTION assign_processing_version_curve_stableswap_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('css|%s|%s|%s', NEW.curve_pool_id, NEW.block_number, NEW.block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM curve_stableswap_state
    WHERE curve_pool_id = NEW.curve_pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_stableswap_state
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_stableswap_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_stableswap_state();

-- ============================================================================
-- curve_cryptoswap_state: periodic snapshots of cryptoswap pool oracle state.
-- price_scale / price_oracle / last_prices store the (n-1)-element vectors
-- returned by price_scale(i) / price_oracle(i) / last_prices(i) for i in [0, n-2].
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_cryptoswap_state
(
    curve_pool_id      BIGINT      NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    balances           NUMERIC[]   NOT NULL,
    virtual_price      NUMERIC     NOT NULL,
    total_supply       NUMERIC     NOT NULL,
    a                  NUMERIC     NOT NULL,
    gamma              NUMERIC     NOT NULL,
    fee                NUMERIC     NOT NULL,
    d                  NUMERIC,
    xcp_profit         NUMERIC,
    price_scale        NUMERIC[]   NOT NULL,
    price_oracle       NUMERIC[]   NOT NULL,
    last_prices        NUMERIC[]   NOT NULL,
    spot_dy                  NUMERIC[]   NOT NULL,
    admin_balances           NUMERIC[],
    lp_price                 NUMERIC,
    xcp_profit_a             NUMERIC,
    last_prices_timestamp    BIGINT,
    get_dx                   NUMERIC[],
    calc_token_amount        NUMERIC,
    calc_withdraw_one_coin   NUMERIC[],
    processing_version       INT         NOT NULL DEFAULT 0,
    build_id                 INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (curve_pool_id, block_timestamp, block_number, block_version, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE curve_cryptoswap_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'curve_pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('curve_cryptoswap_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_cryptoswap_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_cryptoswap_state';
END $$;

CREATE INDEX IF NOT EXISTS idx_curve_cryptoswap_state_pv_lookup
    ON curve_cryptoswap_state (curve_pool_id, block_number, block_version, build_id);

-- Prefix 'ccs' for curve_cryptoswap_state.
CREATE OR REPLACE FUNCTION assign_processing_version_curve_cryptoswap_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('ccs|%s|%s|%s', NEW.curve_pool_id, NEW.block_number, NEW.block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM curve_cryptoswap_state
    WHERE curve_pool_id = NEW.curve_pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_cryptoswap_state
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_cryptoswap_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_cryptoswap_state();

-- ============================================================================
-- Seed missing coin tokens (ON CONFLICT DO NOTHING = idempotent on re-apply).
-- stETH is already seeded by 20260521_105000. These are the remaining coins
-- that are not guaranteed to exist in the token registry before this migration.
-- ============================================================================

-- ETH placeholder: Curve uses 0xEeee...EEeE as a sentinel for native ETH in
-- pool.coins[]. It has no real ERC-20 contract, but the FK from curve_pool_coin
-- to token requires a row. symbol='ETH', decimals=18.
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea, 'ETH', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea, 'DAI', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, 'USDC', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, 'USDT', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea, 'WBTC', 8)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea, 'WETH', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- Seed 4 cast-verified pools (all 3 pool_kind values).
-- All addresses stored as lowercase bytea (\x...).
-- Coins joined by (chain_id, address) -- NEVER by symbol.
--
-- NOTE(VEC-260): initial seed covers 4 cast-verified pools across all 3
-- pool_kind values. Remaining top pools (FRAXBP, crvUSD/USDC, crvUSD/USDT,
-- frxETH, TricryptoUSDT, TriCRV) are added by a follow-up seed / VEC-330
-- watchlist; registry is additive.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. stETH classic (plain_pre_ng, 2 coins)
--    ETH placeholder (index 0) + stETH (index 1)
-- ---------------------------------------------------------------------------
WITH proto AS (
    SELECT id FROM protocol
    WHERE chain_id = 1 AND address = '\x6a8cbed756804b16e05e741edabd5cb544ae21bf'::bytea
    LIMIT 1
),
pool_ins AS (
    INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, lp_token_address, deploy_block, has_a_precise)
    SELECT 1, proto.id,
           '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea,
           'plain_pre_ng', 2,
           '\x06325440D014e39736583c165C2963BA99fAf14E'::bytea,
           11592551, TRUE  -- stETH classic exposes A_precise()
    FROM proto
    ON CONFLICT (chain_id, pool_address) DO NOTHING
    RETURNING id
)
INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id)
SELECT p.id, t.coin_index, t.token_id
FROM pool_ins p
CROSS JOIN LATERAL (
    SELECT 0::smallint AS coin_index, tk.id AS token_id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea
    UNION ALL
    SELECT 1::smallint, tk.id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea
) t
ON CONFLICT (curve_pool_id, coin_index) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 2. stETH-ng (plain_ng, 2 coins)
--    ETH placeholder (index 0) + stETH (index 1)
--    lp_token_address = NULL (NG pool is its own LP token)
-- ---------------------------------------------------------------------------
WITH proto AS (
    SELECT id FROM protocol
    WHERE chain_id = 1 AND address = '\x6a8cbed756804b16e05e741edabd5cb544ae21bf'::bytea
    LIMIT 1
),
pool_ins AS (
    INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, lp_token_address, deploy_block, has_a_precise)
    SELECT 1, proto.id,
           '\x21E27a5E5513D6e65C4f830167390997aA84843a'::bytea,
           'plain_ng', 2,
           NULL,
           17500000, TRUE  -- stETH-ng (NG) exposes A_precise()
    FROM proto
    ON CONFLICT (chain_id, pool_address) DO NOTHING
    RETURNING id
)
INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id)
SELECT p.id, t.coin_index, t.token_id
FROM pool_ins p
CROSS JOIN LATERAL (
    SELECT 0::smallint AS coin_index, tk.id AS token_id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea
    UNION ALL
    SELECT 1::smallint, tk.id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea
) t
ON CONFLICT (curve_pool_id, coin_index) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3. 3pool DAI/USDC/USDT (plain_pre_ng, 3 coins)
-- ---------------------------------------------------------------------------
WITH proto AS (
    SELECT id FROM protocol
    WHERE chain_id = 1 AND address = '\x6a8cbed756804b16e05e741edabd5cb544ae21bf'::bytea
    LIMIT 1
),
pool_ins AS (
    INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, lp_token_address, deploy_block, has_a_precise)
    SELECT 1, proto.id,
           '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'::bytea,
           'plain_pre_ng', 3,
           '\x6c3F90f043a72FA612cbac8115EE7e52BDe6E490'::bytea,
           10809473, FALSE  -- 3pool (oldest pre-NG) has no A_precise() getter
    FROM proto
    ON CONFLICT (chain_id, pool_address) DO NOTHING
    RETURNING id
)
INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id)
SELECT p.id, t.coin_index, t.token_id
FROM pool_ins p
CROSS JOIN LATERAL (
    SELECT 0::smallint AS coin_index, tk.id AS token_id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea  -- DAI
    UNION ALL
    SELECT 1::smallint, tk.id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea  -- USDC
    UNION ALL
    SELECT 2::smallint, tk.id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea  -- USDT
) t
ON CONFLICT (curve_pool_id, coin_index) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 4. TricryptoUSDC USDC/WBTC/WETH (cryptoswap, 3 coins)
--    lp_token_address = NULL (its own LP token)
-- ---------------------------------------------------------------------------
WITH proto AS (
    SELECT id FROM protocol
    WHERE chain_id = 1 AND address = '\x6a8cbed756804b16e05e741edabd5cb544ae21bf'::bytea
    LIMIT 1
),
pool_ins AS (
    INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, lp_token_address, deploy_block, has_a_precise)
    SELECT 1, proto.id,
           '\x7F86Bf177Dd4F3494b841a37e810A34dD56c829B'::bytea,
           'cryptoswap', 3,
           NULL,
           17072859, FALSE  -- cryptoswap never calls A_precise()
    FROM proto
    ON CONFLICT (chain_id, pool_address) DO NOTHING
    RETURNING id
)
INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id)
SELECT p.id, t.coin_index, t.token_id
FROM pool_ins p
CROSS JOIN LATERAL (
    SELECT 0::smallint AS coin_index, tk.id AS token_id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea  -- USDC
    UNION ALL
    SELECT 1::smallint, tk.id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea  -- WBTC
    UNION ALL
    SELECT 2::smallint, tk.id
    FROM token tk
    WHERE tk.chain_id = 1
      AND tk.address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
) t
ON CONFLICT (curve_pool_id, coin_index) DO NOTHING;

-- ============================================================================
-- Seed curve_pool_coin.precision = 10^(18 - token.decimals) for all coins.
-- This UPDATE is idempotent on re-apply (IS NULL guard).
-- ============================================================================
UPDATE curve_pool_coin cpc
SET    precision = power(10, 18 - t.decimals)
FROM   token t
WHERE  t.id    = cpc.token_id
  AND  cpc.precision IS NULL;

-- ============================================================================
-- curve_stableswap_config: append-on-change governance config for plain pools.
-- Regular table (low volume; NOT a hypertable). One row per (pool, block) when
-- any config field changes or on first-seen block for the pool.
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_stableswap_config
(
    curve_pool_id      BIGINT      NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    initial_a          NUMERIC     NOT NULL,
    initial_a_time     BIGINT      NOT NULL,
    future_a           NUMERIC     NOT NULL,
    future_a_time      BIGINT      NOT NULL,
    admin_fee          NUMERIC     NOT NULL,
    future_fee         NUMERIC     NOT NULL,
    future_admin_fee   NUMERIC, -- nullable: pre-NG only; NG pools have no future_admin_fee()
    ma_exp_time        BIGINT,
    oracle_method      NUMERIC,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (curve_pool_id, block_number, block_version, processing_version)
);

-- Prefix 'cssc' for curve_stableswap_config.
CREATE OR REPLACE FUNCTION assign_processing_version_curve_stableswap_config()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cssc|%s|%s|%s', NEW.curve_pool_id, NEW.block_number, NEW.block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM curve_stableswap_config
    WHERE curve_pool_id = NEW.curve_pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_stableswap_config
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_stableswap_config
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_stableswap_config();

CREATE INDEX IF NOT EXISTS idx_curve_stableswap_config_pv_lookup
    ON curve_stableswap_config (curve_pool_id, block_number, block_version, build_id);

-- ============================================================================
-- curve_cryptoswap_config: append-on-change governance config for cryptoswap.
-- Regular table (low volume; NOT a hypertable).
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_cryptoswap_config
(
    curve_pool_id          BIGINT      NOT NULL REFERENCES curve_pool (id),
    block_number           BIGINT      NOT NULL,
    block_version          INT         NOT NULL DEFAULT 0,
    block_timestamp        TIMESTAMPTZ NOT NULL,
    initial_a_gamma        NUMERIC     NOT NULL,
    future_a_gamma         NUMERIC     NOT NULL,
    initial_a_gamma_time   BIGINT      NOT NULL,
    future_a_gamma_time    BIGINT      NOT NULL,
    mid_fee                NUMERIC     NOT NULL,
    out_fee                NUMERIC     NOT NULL,
    fee_gamma              NUMERIC     NOT NULL,
    allowed_extra_profit   NUMERIC     NOT NULL,
    adjustment_step        NUMERIC     NOT NULL,
    ma_time                NUMERIC     NOT NULL,
    admin_fee              NUMERIC     NOT NULL,
    processing_version     INT         NOT NULL DEFAULT 0,
    build_id               INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (curve_pool_id, block_number, block_version, processing_version)
);

-- Prefix 'ccsc' for curve_cryptoswap_config.
CREATE OR REPLACE FUNCTION assign_processing_version_curve_cryptoswap_config()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('ccsc|%s|%s|%s', NEW.curve_pool_id, NEW.block_number, NEW.block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM curve_cryptoswap_config
    WHERE curve_pool_id = NEW.curve_pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_cryptoswap_config
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_cryptoswap_config
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_cryptoswap_config();

CREATE INDEX IF NOT EXISTS idx_curve_cryptoswap_config_pv_lookup
    ON curve_cryptoswap_config (curve_pool_id, block_number, block_version, build_id);

-- ============================================================================
-- curve_parameter_event: on-chain admin/governance parameter events.
-- Hypertable partitioned on block_timestamp (1-day chunks), same as curve_swap.
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_parameter_event
(
    curve_pool_id      BIGINT      NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    tx_hash            BYTEA       NOT NULL,
    log_index          INT         NOT NULL,
    -- No CHECK enum: the decoder's event map is the gate (only known names are
    -- written), and a CHECK would turn a future Curve event type into a
    -- block-stopping INSERT failure even though protocol_event keeps the raw copy.
    -- Documented values live in the column COMMENT.
    event_name         VARCHAR(40) NOT NULL,
    params             JSONB       NOT NULL,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE curve_parameter_event SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'curve_pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('curve_parameter_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_parameter_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_parameter_event';
END $$;

CREATE INDEX IF NOT EXISTS idx_curve_parameter_event_pv_lookup
    ON curve_parameter_event (curve_pool_id, block_number, block_version, log_index, build_id);

-- Prefix 'cpe' for curve_parameter_event.
CREATE OR REPLACE FUNCTION assign_processing_version_curve_parameter_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cpe|%s|%s|%s|%s', NEW.curve_pool_id, NEW.block_number, NEW.block_version, NEW.log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM curve_parameter_event
    WHERE curve_pool_id = NEW.curve_pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND log_index     = NEW.log_index
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_parameter_event
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND log_index     = NEW.log_index;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_parameter_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_parameter_event();

-- ============================================================================
-- curve_lp_token_event: LP token Transfer and Approval events.
-- Hypertable partitioned on block_timestamp (1-day chunks), same as curve_swap.
-- ============================================================================
CREATE TABLE IF NOT EXISTS curve_lp_token_event
(
    curve_pool_id      BIGINT      NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    tx_hash            BYTEA       NOT NULL,
    log_index          INT         NOT NULL,
    event_name         VARCHAR(10) NOT NULL, -- gated by the decoder (see curve_parameter_event.event_name); documented values in the column COMMENT
    from_address       BYTEA       NOT NULL,
    to_address         BYTEA       NOT NULL,
    value              NUMERIC     NOT NULL,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (curve_pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE curve_lp_token_event SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'curve_pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('curve_lp_token_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_lp_token_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_lp_token_event';
END $$;

CREATE INDEX IF NOT EXISTS idx_curve_lp_token_event_pv_lookup
    ON curve_lp_token_event (curve_pool_id, block_number, block_version, log_index, build_id);

-- Prefix 'clte' for curve_lp_token_event.
CREATE OR REPLACE FUNCTION assign_processing_version_curve_lp_token_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('clte|%s|%s|%s|%s', NEW.curve_pool_id, NEW.block_number, NEW.block_version, NEW.log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM curve_lp_token_event
    WHERE curve_pool_id = NEW.curve_pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND log_index     = NEW.log_index
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_lp_token_event
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND log_index     = NEW.log_index;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_lp_token_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_lp_token_event();

-- ============================================================================
-- Assertions: verify seed integrity before committing.
-- ============================================================================

DO $$
DECLARE pool_count INT;
BEGIN
    SELECT COUNT(*) INTO pool_count FROM curve_pool WHERE chain_id = 1;
    IF pool_count <> 4 THEN
        RAISE EXCEPTION 'expected 4 seeded curve_pool rows for chain_id=1, found %', pool_count;
    END IF;
END $$;

DO $$
DECLARE coin_count INT;
BEGIN
    SELECT COUNT(*) INTO coin_count
    FROM curve_pool_coin cpc
    JOIN curve_pool cp ON cp.id = cpc.curve_pool_id
    WHERE cp.chain_id = 1;
    IF coin_count <> 10 THEN
        RAISE EXCEPTION 'expected 10 curve_pool_coin rows for chain_id=1 (2+2+3+3), found %', coin_count;
    END IF;
END $$;

DO $$
DECLARE missing_count INT;
BEGIN
    -- Every coin in every seeded pool must resolve to a token row.
    SELECT COUNT(*) INTO missing_count
    FROM curve_pool_coin cpc
    WHERE NOT EXISTS (SELECT 1 FROM token t WHERE t.id = cpc.token_id);
    IF missing_count <> 0 THEN
        RAISE EXCEPTION 'found % curve_pool_coin rows with no matching token', missing_count;
    END IF;
END $$;

DO $$
DECLARE trigger_count INT;
BEGIN
    -- Assert each of the 8 curve tables has its processing_version trigger.
    -- Count DISTINCT table name, not rows: some integration harnesses apply the
    -- migrations into more than one schema of the same database, so an unscoped
    -- pg_trigger/pg_class join sees the same table name in several schemas.
    -- DISTINCT relname collapses that while still catching a missing trigger.
    -- Matching on the assign_processing_version_curve* functions excludes ts_insert_blocker.
    SELECT COUNT(DISTINCT c.relname) INTO trigger_count
    FROM pg_trigger tg
    JOIN pg_class c ON c.oid = tg.tgrelid
    JOIN pg_proc p ON p.oid = tg.tgfoid
    WHERE NOT tg.tgisinternal
      AND c.relname IN (
          'curve_swap', 'curve_liquidity_event',
          'curve_stableswap_state', 'curve_cryptoswap_state',
          'curve_stableswap_config', 'curve_cryptoswap_config',
          'curve_parameter_event', 'curve_lp_token_event'
      )
      AND p.proname LIKE 'assign_processing_version_curve%';
    IF trigger_count <> 8 THEN
        RAISE EXCEPTION 'expected processing_version triggers on all 8 curve tables, found % distinct', trigger_count;
    END IF;
END $$;

DO $$
DECLARE null_precision_count INT;
BEGIN
    -- All seeded coins must have precision populated after the UPDATE above.
    SELECT COUNT(*) INTO null_precision_count
    FROM curve_pool_coin
    WHERE precision IS NULL;
    IF null_precision_count <> 0 THEN
        RAISE EXCEPTION 'found % curve_pool_coin rows with NULL precision after seed', null_precision_count;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260521_110000_create_curve_dex_tables.sql')
ON CONFLICT (filename) DO NOTHING;
