-- Morpho Protocol tables: Morpho Blue markets + MetaMorpho vaults
-- All time-series tables use TimescaleDB hypertables partitioned on block_number

-- ============================================================================
-- morpho_market: Registry of Morpho Blue isolated markets
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_market
(
    id                  BIGSERIAL PRIMARY KEY,
    protocol_id         BIGINT      NOT NULL REFERENCES protocol (id),
    market_id           BYTEA       NOT NULL, -- 32-byte keccak256 hash identifying the market
    loan_token_id       BIGINT      NOT NULL REFERENCES token (id),
    collateral_token_id BIGINT      NOT NULL REFERENCES token (id),
    oracle_address      BYTEA       NOT NULL, -- 20 bytes
    irm_address         BYTEA       NOT NULL, -- 20 bytes (interest rate model)
    lltv                NUMERIC     NOT NULL, -- liquidation loan-to-value (scaled by 1e18)
    created_at_block    BIGINT      NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (protocol_id, market_id)
);

CREATE INDEX IF NOT EXISTS idx_morpho_market_market_id ON morpho_market (market_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_loan_token ON morpho_market (loan_token_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_collateral_token ON morpho_market (collateral_token_id);

-- ============================================================================
-- morpho_market_state: Market state snapshots (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_market_state
(
    id                   BIGSERIAL,
    morpho_market_id     BIGINT      NOT NULL REFERENCES morpho_market (id),
    block_number         BIGINT      NOT NULL,
    block_version        INT         NOT NULL DEFAULT 0,
    total_supply_assets  NUMERIC     NOT NULL,
    total_supply_shares  NUMERIC     NOT NULL,
    total_borrow_assets  NUMERIC     NOT NULL,
    total_borrow_shares  NUMERIC     NOT NULL,
    last_update          BIGINT      NOT NULL, -- timestamp of last interest accrual
    fee                  NUMERIC     NOT NULL, -- protocol fee (scaled by 1e18)
    -- AccrueInterest event data (nullable, only set when triggered by AccrueInterest)
    prev_borrow_rate     NUMERIC,
    interest_accrued     NUMERIC,
    fee_shares           NUMERIC,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, block_number),
    CONSTRAINT morpho_market_state_unique UNIQUE (morpho_market_id, block_number, block_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_number',
    tsdb.chunk_interval = 100000
);

CREATE INDEX IF NOT EXISTS idx_morpho_market_state_market ON morpho_market_state (morpho_market_id);
CREATE INDEX IF NOT EXISTS idx_morpho_market_state_block ON morpho_market_state (block_number);

-- ============================================================================
-- morpho_position: User position snapshots in Morpho Blue markets (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_position
(
    id               BIGSERIAL,
    user_id          BIGINT      NOT NULL REFERENCES "user" (id),
    morpho_market_id BIGINT      NOT NULL REFERENCES morpho_market (id),
    block_number     BIGINT      NOT NULL,
    block_version    INT         NOT NULL DEFAULT 0,
    supply_shares    NUMERIC     NOT NULL,
    borrow_shares    NUMERIC     NOT NULL,
    collateral       NUMERIC     NOT NULL,
    supply_assets    NUMERIC     NOT NULL, -- computed: supplyShares * totalSupplyAssets / totalSupplyShares
    borrow_assets    NUMERIC     NOT NULL, -- computed: round-up division
    event_type       TEXT        NOT NULL,
    tx_hash          BYTEA       NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, block_number),
    CONSTRAINT morpho_position_unique UNIQUE (user_id, morpho_market_id, block_number, block_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_number',
    tsdb.chunk_interval = 100000
);

CREATE INDEX IF NOT EXISTS idx_morpho_position_user ON morpho_position (user_id);
CREATE INDEX IF NOT EXISTS idx_morpho_position_market ON morpho_position (morpho_market_id);
CREATE INDEX IF NOT EXISTS idx_morpho_position_block ON morpho_position (block_number);
CREATE INDEX IF NOT EXISTS idx_morpho_position_user_market ON morpho_position (user_id, morpho_market_id);

-- ============================================================================
-- morpho_vault: MetaMorpho vault registry
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_vault
(
    id               BIGSERIAL PRIMARY KEY,
    protocol_id      BIGINT      NOT NULL REFERENCES protocol (id),
    address          BYTEA       NOT NULL, -- 20 bytes
    name             VARCHAR(255),
    symbol           VARCHAR(50),
    asset_token_id   BIGINT      NOT NULL REFERENCES token (id),
    vault_version    SMALLINT    NOT NULL, -- 1 = V1.1 (MetaMorpho), 2 = V2 (MetaMorpho V2)
    created_at_block BIGINT      NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (protocol_id, address)
);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_address ON morpho_vault (address);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_asset_token ON morpho_vault (asset_token_id);

-- ============================================================================
-- morpho_vault_state: Vault state snapshots (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_vault_state
(
    id               BIGSERIAL,
    morpho_vault_id  BIGINT      NOT NULL REFERENCES morpho_vault (id),
    block_number     BIGINT      NOT NULL,
    block_version    INT         NOT NULL DEFAULT 0,
    total_assets     NUMERIC     NOT NULL,
    total_supply     NUMERIC     NOT NULL, -- total vault shares
    -- AccrueInterest raw data (nullable, only set when triggered by AccrueInterest)
    fee_shares       NUMERIC,
    new_total_assets NUMERIC,    -- V2 only
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, block_number),
    CONSTRAINT morpho_vault_state_unique UNIQUE (morpho_vault_id, block_number, block_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_number',
    tsdb.chunk_interval = 100000
);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_state_vault ON morpho_vault_state (morpho_vault_id);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_state_block ON morpho_vault_state (block_number);

-- ============================================================================
-- morpho_vault_position: User position snapshots in MetaMorpho vaults (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_vault_position
(
    id              BIGSERIAL,
    user_id         BIGINT      NOT NULL REFERENCES "user" (id),
    morpho_vault_id BIGINT      NOT NULL REFERENCES morpho_vault (id),
    block_number    BIGINT      NOT NULL,
    block_version   INT         NOT NULL DEFAULT 0,
    shares          NUMERIC     NOT NULL,
    assets          NUMERIC     NOT NULL, -- computed: shares * totalAssets / totalSupply
    event_type      TEXT        NOT NULL,
    tx_hash         BYTEA       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, block_number),
    CONSTRAINT morpho_vault_position_unique UNIQUE (user_id, morpho_vault_id, block_number, block_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_number',
    tsdb.chunk_interval = 100000
);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_user ON morpho_vault_position (user_id);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_vault ON morpho_vault_position (morpho_vault_id);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_block ON morpho_vault_position (block_number);
CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_user_vault ON morpho_vault_position (user_id, morpho_vault_id);

-- ============================================================================
-- Seed data: Morpho Blue protocol
-- ============================================================================
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (1, '\xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb'::bytea, 'Morpho Blue', 'lending', 18883124, NOW(),
        '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260224_100000_create_morpho_tables.sql')
ON CONFLICT (filename) DO NOTHING;
