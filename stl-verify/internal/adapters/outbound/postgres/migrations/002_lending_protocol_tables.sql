-- Migration: 002_lending_protocol_tables.sql
-- Creates lending protocol tracking tables
-- Assumes block_states table already exists from worker migration

-- Chain registry
CREATE TABLE IF NOT EXISTS chain (
                                     chain_id INT PRIMARY KEY,
                                     name VARCHAR(255) NOT NULL UNIQUE
);

-- Tokens (underlying assets)
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

-- Protocols (Aave, Spark, Morpho, etc.)
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

-- Users (borrowers/lenders)
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

-- Borrowers (debt positions)
-- Integrates with block_states.version for reorg handling
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

-- Borrower collateral (supply positions used as collateral)
-- Integrates with block_states.version for reorg handling
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

-- Add comments
COMMENT ON TABLE borrowers IS 'Tracks debt positions. block_version maps to block_states.version for reorg handling.';
COMMENT ON TABLE borrower_collateral IS 'Tracks collateral positions. block_version maps to block_states.version for reorg handling.';

-- Insert initial data

-- Default chain (only pre-populated table)
INSERT INTO chain (chain_id, name) VALUES (1, 'Ethereum Mainnet')
ON CONFLICT (chain_id) DO NOTHING;

-- Print success message
DO $$
    BEGIN
        RAISE NOTICE '✅ Lending protocol tables created successfully!';
        RAISE NOTICE '📊 Created tables: chain, token, protocol, users, borrowers, borrower_collateral';
        RAISE NOTICE '🏦 Inserted: Ethereum Mainnet chain';
        RAISE NOTICE '⚙️  Tokens and protocols will be auto-created when first seen';
        RAISE NOTICE '📝 Addresses stored as TEXT (readable hex strings)';
    END $$;