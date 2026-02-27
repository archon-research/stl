-- Morpho Protocol tables: Morpho Blue markets + MetaMorpho vaults
-- All time-series tables use TimescaleDB hypertables partitioned on timestamp (block timestamp)
--
-- Compression strategy (consistent with onchain_token_price):
-- - Segment by entity FK columns (queries always filter by these)
-- - Order by block_number DESC, block_version DESC (time-series access pattern)
-- - Compress chunks older than 2 days (2x chunk_interval)
-- - Tier to S3 after 1 year

-- ============================================================================
-- morpho_market: Registry of Morpho Blue isolated markets
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_market
(
    id                  BIGSERIAL PRIMARY KEY,
    chain_id            INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id         BIGINT      NOT NULL REFERENCES protocol (id),
    market_id           BYTEA       NOT NULL,
    loan_token_id       BIGINT      NOT NULL REFERENCES token (id),
    collateral_token_id BIGINT      NOT NULL REFERENCES token (id),
    oracle_address      BYTEA       NOT NULL,
    irm_address         BYTEA       NOT NULL,
    lltv                NUMERIC     NOT NULL,
    created_at_block    BIGINT      NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, market_id)
);

CREATE INDEX IF NOT EXISTS idx_morpho_market_chain_id ON morpho_market (chain_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_market_id ON morpho_market (market_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_loan_token ON morpho_market (loan_token_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_collateral_token ON morpho_market (collateral_token_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_protocol_id ON morpho_market (protocol_id);

-- ============================================================================
-- morpho_market_state: Market state snapshots (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_market_state
(
    morpho_market_id     BIGINT      NOT NULL REFERENCES morpho_market (id),
    block_number         BIGINT      NOT NULL,
    block_version        INT         NOT NULL DEFAULT 0,
    timestamp            TIMESTAMPTZ NOT NULL,
    total_supply_assets  NUMERIC     NOT NULL,
    total_supply_shares  NUMERIC     NOT NULL,
    total_borrow_assets  NUMERIC     NOT NULL,
    total_borrow_shares  NUMERIC     NOT NULL,
    last_update          BIGINT      NOT NULL,
    fee                  NUMERIC     NOT NULL,
    -- AccrueInterest raw data (nullable, only set when triggered by the AccrueInterest event)
    prev_borrow_rate     NUMERIC,
    interest_accrued     NUMERIC,
    fee_shares           NUMERIC,
    PRIMARY KEY (morpho_market_id, block_number, block_version, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '1 day'
);

CREATE INDEX IF NOT EXISTS idx_morpho_market_state_market ON morpho_market_state (morpho_market_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_state_block ON morpho_market_state (block_number);

ALTER TABLE morpho_market_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'morpho_market_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC'
);

SELECT add_compression_policy('morpho_market_state', INTERVAL '2 days', if_not_exists => TRUE);

SELECT add_tiering_policy('morpho_market_state', INTERVAL '1 year');

-- ============================================================================
-- morpho_market_position: User position snapshots in Morpho Blue markets (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_market_position
(
    user_id          BIGINT      NOT NULL REFERENCES "user" (id),
    morpho_market_id BIGINT      NOT NULL REFERENCES morpho_market (id),
    block_number     BIGINT      NOT NULL,
    block_version    INT         NOT NULL DEFAULT 0,
    timestamp        TIMESTAMPTZ NOT NULL,
    supply_shares    NUMERIC     NOT NULL,
    borrow_shares    NUMERIC     NOT NULL,
    collateral       NUMERIC     NOT NULL,
    supply_assets    NUMERIC     NOT NULL,
    borrow_assets    NUMERIC     NOT NULL,
    PRIMARY KEY (user_id, morpho_market_id, block_number, block_version, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '1 day'
);

CREATE INDEX IF NOT EXISTS idx_morpho_market_position_user ON morpho_market_position (user_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_position_market ON morpho_market_position (morpho_market_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_position_block ON morpho_market_position (block_number);
CREATE INDEX IF NOT EXISTS idx_morpho_market_position_user_market ON morpho_market_position (user_id, morpho_market_id);

ALTER TABLE morpho_market_position SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'morpho_market_id, user_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC'
);

SELECT add_compression_policy('morpho_market_position', INTERVAL '2 days', if_not_exists => TRUE);

SELECT add_tiering_policy('morpho_market_position', INTERVAL '1 year');

-- ============================================================================
-- morpho_vault: MetaMorpho vault registry
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_vault
(
    id               BIGSERIAL PRIMARY KEY,
    chain_id         INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id      BIGINT      NOT NULL REFERENCES protocol (id),
    address          BYTEA       NOT NULL,
    name             VARCHAR(255),
    symbol           VARCHAR(50),
    asset_token_id   BIGINT      NOT NULL REFERENCES token (id),
    vault_version    SMALLINT    NOT NULL,
    created_at_block BIGINT      NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_chain_id ON morpho_vault (chain_id);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_address ON morpho_vault (address);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_asset_token ON morpho_vault (asset_token_id);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_protocol_id ON morpho_vault (protocol_id);

-- ============================================================================
-- morpho_vault_state: Vault state snapshots (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_vault_state
(
    morpho_vault_id  BIGINT      NOT NULL REFERENCES morpho_vault (id),
    block_number     BIGINT      NOT NULL,
    block_version    INT         NOT NULL DEFAULT 0,
    timestamp        TIMESTAMPTZ NOT NULL,
    total_assets     NUMERIC     NOT NULL,
    total_shares     NUMERIC     NOT NULL,
    -- AccrueInterest raw data (nullable, only set when triggered by the AccrueInterest event)
    fee_shares       NUMERIC,
    new_total_assets NUMERIC,
    previous_total_assets  NUMERIC,
    management_fee_shares  NUMERIC,
    PRIMARY KEY (morpho_vault_id, block_number, block_version, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '1 day'
);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_state_vault ON morpho_vault_state (morpho_vault_id);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_state_block ON morpho_vault_state (block_number);

ALTER TABLE morpho_vault_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'morpho_vault_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC'
);

SELECT add_compression_policy('morpho_vault_state', INTERVAL '2 days', if_not_exists => TRUE);

SELECT add_tiering_policy('morpho_vault_state', INTERVAL '1 year');

-- ============================================================================
-- morpho_vault_position: User position snapshots in MetaMorpho vaults (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_vault_position
(
    user_id         BIGINT      NOT NULL REFERENCES "user" (id),
    morpho_vault_id BIGINT      NOT NULL REFERENCES morpho_vault (id),
    block_number    BIGINT      NOT NULL,
    block_version   INT         NOT NULL DEFAULT 0,
    timestamp       TIMESTAMPTZ NOT NULL,
    shares          NUMERIC     NOT NULL,
    assets          NUMERIC     NOT NULL,
    PRIMARY KEY (user_id, morpho_vault_id, block_number, block_version, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '1 day'
);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_user ON morpho_vault_position (user_id);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_vault ON morpho_vault_position (morpho_vault_id);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_block ON morpho_vault_position (block_number);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_user_vault ON morpho_vault_position (user_id, morpho_vault_id);

ALTER TABLE morpho_vault_position SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'morpho_vault_id, user_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC'
);

SELECT add_compression_policy('morpho_vault_position', INTERVAL '2 days', if_not_exists => TRUE);

SELECT add_tiering_policy('morpho_vault_position', INTERVAL '1 year');

INSERT INTO migrations (filename)
VALUES ('20260224_100000_create_morpho_tables.sql')
ON CONFLICT (filename) DO NOTHING;
