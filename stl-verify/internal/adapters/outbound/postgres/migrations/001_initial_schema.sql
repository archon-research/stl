-- 001_initial_schema.sql
-- Creates the core tables for block state tracking

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

CREATE TABLE IF NOT EXISTS chain (
                                     chain_id INT PRIMARY KEY,
                                     name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS token (
                                     id BIGSERIAL PRIMARY KEY,
                                     chain_id INT NOT NULL REFERENCES chain(chain_id),
                                     address TEXT NOT NULL,
                                     symbol VARCHAR(50),
                                     decimals SMALLINT,
                                     created_at_block BIGINT,
                                     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                     metadata JSONB,
                                     UNIQUE(chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_token_chain_address ON token(chain_id, address);

CREATE TABLE IF NOT EXISTS protocol (
                                        id BIGSERIAL PRIMARY KEY,
                                        chain_id INT NOT NULL REFERENCES chain(chain_id),
                                        address TEXT NOT NULL,
                                        name VARCHAR(255),
                                        protocol_type VARCHAR(50), -- 'lending', 'rwa', etc.
                                        created_at_block BIGINT,
                                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                        metadata JSONB,
                                        UNIQUE(chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_protocol_chain_address ON protocol(chain_id, address);

CREATE TABLE IF NOT EXISTS users (
                                     id BIGSERIAL PRIMARY KEY,
                                     chain_id INT NOT NULL REFERENCES chain(chain_id),
                                     address TEXT NOT NULL,
                                     first_seen_block BIGINT,
                                     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                     metadata JSONB,
                                     UNIQUE(chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_users_chain_address ON users(chain_id, address);

CREATE TABLE IF NOT EXISTS borrowers (
                                         id BIGSERIAL PRIMARY KEY,
                                         user_id BIGINT NOT NULL REFERENCES users(id),
                                         protocol_id BIGINT NOT NULL REFERENCES protocol(id),
                                         token_id BIGINT NOT NULL REFERENCES token(id),
                                         block_number BIGINT NOT NULL,
                                         block_version INT NOT NULL DEFAULT 0, -- maps to block_states.version
                                         amount NUMERIC NOT NULL, -- total debt at this block
                                         change NUMERIC NOT NULL, -- change in this block (borrow amount)
                                         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                         UNIQUE(user_id, protocol_id, token_id, block_number, block_version)
);

CREATE INDEX IF NOT EXISTS idx_borrowers_user ON borrowers(user_id);
CREATE INDEX IF NOT EXISTS idx_borrowers_protocol ON borrowers(protocol_id);
CREATE INDEX IF NOT EXISTS idx_borrowers_token ON borrowers(token_id);
CREATE INDEX IF NOT EXISTS idx_borrowers_block ON borrowers(block_number);
CREATE INDEX IF NOT EXISTS idx_borrowers_user_protocol ON borrowers(user_id, protocol_id);
CREATE INDEX IF NOT EXISTS idx_borrowers_block_version ON borrowers(block_number, block_version);

CREATE TABLE IF NOT EXISTS borrower_collateral (
                                                   id BIGSERIAL PRIMARY KEY,
                                                   user_id BIGINT NOT NULL REFERENCES users(id),
                                                   protocol_id BIGINT NOT NULL REFERENCES protocol(id),
                                                   token_id BIGINT NOT NULL REFERENCES token(id),
                                                   block_number BIGINT NOT NULL,
                                                   block_version INT NOT NULL DEFAULT 0, -- maps to block_states.version
                                                   amount NUMERIC NOT NULL, -- total collateral at this block
                                                   change NUMERIC NOT NULL, -- change in this block
                                                   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                                   UNIQUE(user_id, protocol_id, token_id, block_number, block_version)
);

CREATE INDEX IF NOT EXISTS idx_borrower_collateral_user ON borrower_collateral(user_id);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_protocol ON borrower_collateral(protocol_id);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_token ON borrower_collateral(token_id);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_block ON borrower_collateral(block_number);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_user_protocol ON borrower_collateral(user_id, protocol_id);
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_block_version ON borrower_collateral(block_number, block_version);

INSERT INTO chain (chain_id, name) VALUES (1, 'Ethereum Mainnet')
ON CONFLICT (chain_id) DO NOTHING;