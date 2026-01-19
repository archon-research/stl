-- 001_initial_schema.sql
-- Creates the core tables for block state tracking

-- Enable TimescaleDB extension for time-series data
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Block states table tracks all blocks seen (both canonical and orphaned)
CREATE TABLE IF NOT EXISTS block_states (
    number BIGINT NOT NULL,
    hash TEXT NOT NULL UNIQUE,
    parent_hash TEXT NOT NULL,
    received_at BIGINT NOT NULL,
    is_orphaned BOOLEAN NOT NULL DEFAULT FALSE,
    version INT NOT NULL DEFAULT 0,
    -- Publish status tracking: allows crash recovery and prevents duplicate publishes
    -- Each flag is set to TRUE after the corresponding event is successfully published
    block_published BOOLEAN NOT NULL DEFAULT FALSE,
    receipts_published BOOLEAN NOT NULL DEFAULT FALSE,
    traces_published BOOLEAN NOT NULL DEFAULT FALSE,
    blobs_published BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (number, hash),
    CONSTRAINT unique_block_number_version UNIQUE (number, version),
    CONSTRAINT version_non_negative CHECK (version >= 0)
);

CREATE INDEX IF NOT EXISTS idx_block_states_hash ON block_states(hash);
CREATE INDEX IF NOT EXISTS idx_block_states_received_at ON block_states(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_block_states_canonical ON block_states(number DESC) WHERE NOT is_orphaned;
CREATE INDEX IF NOT EXISTS idx_block_states_orphaned ON block_states(is_orphaned) WHERE is_orphaned;
-- Index for finding blocks with incomplete publishes (for backfill recovery)
CREATE INDEX IF NOT EXISTS idx_block_states_incomplete_publish ON block_states(number) 
    WHERE NOT is_orphaned AND (NOT block_published OR NOT receipts_published OR NOT traces_published);

-- Function to auto-assign version on insert
-- Calculates MAX(version) + 1 for the block number, defaulting to 0
CREATE OR REPLACE FUNCTION assign_block_version()
RETURNS TRIGGER AS $$
BEGIN
    SELECT COALESCE(MAX(version), -1) + 1 INTO NEW.version
    FROM block_states
    WHERE number = NEW.number;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-assign version before insert
DROP TRIGGER IF EXISTS trigger_assign_block_version ON block_states;
CREATE TRIGGER trigger_assign_block_version
    BEFORE INSERT ON block_states
    FOR EACH ROW
    EXECUTE FUNCTION assign_block_version();

-- Reorg events table tracks detected chain reorganizations
CREATE TABLE IF NOT EXISTS reorg_events (
    id BIGSERIAL PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    block_number BIGINT NOT NULL,
    old_hash TEXT NOT NULL,
    new_hash TEXT NOT NULL,
    depth INT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_reorg_events_detected_at ON reorg_events(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_reorg_events_block_number ON reorg_events(block_number);

-- Backfill watermark tracks the highest block number that has been verified as gap-free.
-- FindGaps only needs to scan blocks ABOVE this watermark, avoiding full table scans.
CREATE TABLE IF NOT EXISTS backfill_watermark (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    watermark BIGINT NOT NULL DEFAULT 0
);

INSERT INTO backfill_watermark (id, watermark) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

-- Creates the core tables for Sentinel risk data layer

-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================

-- Chains table - blockchain networks
CREATE TABLE IF NOT EXISTS chains (
    chain_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tokens table - ERC20 tokens
CREATE TABLE IF NOT EXISTS tokens (
    id BIGSERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains(chain_id),
    address BYTEA NOT NULL,
    symbol VARCHAR(50),
    decimals SMALLINT NOT NULL DEFAULT 18,
    created_at_block BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    CONSTRAINT tokens_chain_address_unique UNIQUE (chain_id, address),
    CONSTRAINT tokens_address_length CHECK (octet_length(address) = 20)
);

CREATE INDEX IF NOT EXISTS idx_tokens_chain_id ON tokens(chain_id);
CREATE INDEX IF NOT EXISTS idx_tokens_symbol ON tokens(symbol);

-- Protocols table - DeFi protocols (e.g., SparkLend, Aave)
CREATE TABLE IF NOT EXISTS protocols (
    id BIGSERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains(chain_id),
    address BYTEA NOT NULL,
    name VARCHAR(100) NOT NULL,
    protocol_type VARCHAR(50) NOT NULL, -- 'lending', 'rwa', etc.
    created_at_block BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    CONSTRAINT protocols_chain_address_unique UNIQUE (chain_id, address),
    CONSTRAINT protocols_address_length CHECK (octet_length(address) = 20)
);

CREATE INDEX IF NOT EXISTS idx_protocols_chain_id ON protocols(chain_id);
CREATE INDEX IF NOT EXISTS idx_protocols_type ON protocols(protocol_type);

-- =============================================================================
-- TOKEN DERIVATIVE TABLES
-- =============================================================================

-- Receipt tokens (aTokens, spTokens, cTokens, etc.)
CREATE TABLE IF NOT EXISTS receipt_tokens (
    id BIGSERIAL PRIMARY KEY,
    protocol_id BIGINT NOT NULL REFERENCES protocols(id),
    underlying_token_id BIGINT NOT NULL REFERENCES tokens(id),
    receipt_token_address BYTEA NOT NULL,
    symbol VARCHAR(50),
    created_at_block BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    CONSTRAINT receipt_tokens_protocol_underlying_unique UNIQUE (protocol_id, underlying_token_id),
    CONSTRAINT receipt_tokens_address_length CHECK (octet_length(receipt_token_address) = 20)
);

CREATE INDEX IF NOT EXISTS idx_receipt_tokens_protocol_id ON receipt_tokens(protocol_id);
CREATE INDEX IF NOT EXISTS idx_receipt_tokens_underlying_token_id ON receipt_tokens(underlying_token_id);

-- Debt tokens (variable and stable debt tokens)
CREATE TABLE IF NOT EXISTS debt_tokens (
    id BIGSERIAL PRIMARY KEY,
    protocol_id BIGINT NOT NULL REFERENCES protocols(id),
    underlying_token_id BIGINT NOT NULL REFERENCES tokens(id),
    variable_debt_address BYTEA,
    stable_debt_address BYTEA, -- nullable, not all protocols have stable debt
    variable_symbol VARCHAR(50),
    stable_symbol VARCHAR(50),
    created_at_block BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    CONSTRAINT debt_tokens_protocol_underlying_unique UNIQUE (protocol_id, underlying_token_id),
    CONSTRAINT debt_tokens_variable_address_length CHECK (variable_debt_address IS NULL OR octet_length(variable_debt_address) = 20),
    CONSTRAINT debt_tokens_stable_address_length CHECK (stable_debt_address IS NULL OR octet_length(stable_debt_address) = 20)
);

CREATE INDEX IF NOT EXISTS idx_debt_tokens_protocol_id ON debt_tokens(protocol_id);
CREATE INDEX IF NOT EXISTS idx_debt_tokens_underlying_token_id ON debt_tokens(underlying_token_id);

-- =============================================================================
-- USER TABLES
-- =============================================================================

-- Users table - wallet addresses that interact with protocols
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains(chain_id),
    address BYTEA NOT NULL,
    first_seen_block BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    CONSTRAINT users_chain_address_unique UNIQUE (chain_id, address),
    CONSTRAINT users_address_length CHECK (octet_length(address) = 20)
);

CREATE INDEX IF NOT EXISTS idx_users_chain_id ON users(chain_id);

-- User protocol metadata - protocol-specific user data
CREATE TABLE IF NOT EXISTS user_protocol_metadata (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    protocol_id BIGINT NOT NULL REFERENCES protocols(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    CONSTRAINT user_protocol_metadata_user_protocol_unique UNIQUE (user_id, protocol_id)
);

CREATE INDEX IF NOT EXISTS idx_user_protocol_metadata_user_id ON user_protocol_metadata(user_id);
CREATE INDEX IF NOT EXISTS idx_user_protocol_metadata_protocol_id ON user_protocol_metadata(protocol_id);

-- =============================================================================
-- POSITION TABLES
-- =============================================================================

-- Borrowers table - tracks user debt positions over time
CREATE TABLE IF NOT EXISTS borrowers (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    protocol_id BIGINT NOT NULL REFERENCES protocols(id),
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    block_number BIGINT NOT NULL,
    block_version INTEGER NOT NULL DEFAULT 0,
    amount NUMERIC NOT NULL, -- current total debt amount
    change NUMERIC NOT NULL DEFAULT 0, -- change from previous snapshot
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT borrowers_unique_position UNIQUE (user_id, protocol_id, token_id, block_number, block_version)
);

CREATE INDEX IF NOT EXISTS idx_borrowers_user_id ON borrowers(user_id);
CREATE INDEX IF NOT EXISTS idx_borrowers_protocol_id ON borrowers(protocol_id);
CREATE INDEX IF NOT EXISTS idx_borrowers_token_id ON borrowers(token_id);
CREATE INDEX IF NOT EXISTS idx_borrowers_block_number ON borrowers(block_number DESC);
CREATE INDEX IF NOT EXISTS idx_borrowers_user_protocol ON borrowers(user_id, protocol_id);

-- Borrower collateral table - tracks user collateral positions over time
CREATE TABLE IF NOT EXISTS borrower_collateral (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    protocol_id BIGINT NOT NULL REFERENCES protocols(id),
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    block_number BIGINT NOT NULL,
    block_version INTEGER NOT NULL DEFAULT 0,
    amount NUMERIC NOT NULL, -- current total collateral amount
    change NUMERIC NOT NULL DEFAULT 0, -- change from previous snapshot
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT borrower_collateral_unique_position UNIQUE (user_id, protocol_id, token_id, block_number, block_version)
);

CREATE INDEX IF NOT EXISTS idx_borrower_collateral_user_id ON borrower_collateral(user_id);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_protocol_id ON borrower_collateral(protocol_id);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_token_id ON borrower_collateral(token_id);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_block_number ON borrower_collateral(block_number DESC);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_user_protocol ON borrower_collateral(user_id, protocol_id);

-- =============================================================================
-- PROTOCOL-SPECIFIC TABLES (TimescaleDB Hypertables)
-- =============================================================================

-- SparkLend reserve data - protocol reserve state snapshots
-- Created as a TimescaleDB hypertable for efficient time-series queries.
-- Uses block_number as the partition column with chunks of 100,000 blocks (~2 weeks at ~12s/block).
CREATE TABLE IF NOT EXISTS sparklend_reserve_data (
    id BIGSERIAL PRIMARY KEY,
    protocol_id BIGINT NOT NULL REFERENCES protocols(id),
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    block_number BIGINT NOT NULL,
    block_version INTEGER NOT NULL DEFAULT 0,
    -- Reserve state
    unbacked NUMERIC,
    accrued_to_treasury_scaled NUMERIC,
    total_a_token NUMERIC,
    total_stable_debt NUMERIC,
    total_variable_debt NUMERIC,
    -- Interest rates (ray - 27 decimals)
    liquidity_rate NUMERIC,
    variable_borrow_rate NUMERIC,
    stable_borrow_rate NUMERIC,
    average_stable_borrow_rate NUMERIC,
    -- Indexes (ray - 27 decimals)
    liquidity_index NUMERIC,
    variable_borrow_index NUMERIC,
    -- Timestamps
    last_update_timestamp BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT sparklend_reserve_data_unique UNIQUE (protocol_id, token_id, block_number, block_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_number',
    tsdb.chunk_interval = 100000
);

CREATE INDEX IF NOT EXISTS idx_sparklend_reserve_data_protocol_id ON sparklend_reserve_data(protocol_id);
CREATE INDEX IF NOT EXISTS idx_sparklend_reserve_data_token_id ON sparklend_reserve_data(token_id);
CREATE INDEX IF NOT EXISTS idx_sparklend_reserve_data_block_number ON sparklend_reserve_data(block_number DESC);
CREATE INDEX IF NOT EXISTS idx_sparklend_reserve_data_protocol_token ON sparklend_reserve_data(protocol_id, token_id);

-- =============================================================================
-- SEED DATA
-- =============================================================================

-- Insert initial chains
INSERT INTO chains (chain_id, name) VALUES
    (1, 'mainnet')
ON CONFLICT (chain_id) DO NOTHING;
