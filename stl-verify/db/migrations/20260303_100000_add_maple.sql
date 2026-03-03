-- Create Maple-specific tables for loan metadata, borrower debt, and collateral positions.
-- Maple data uses symbol-only assets (no contract address, no EVM chain ID),
-- so it cannot use the shared borrower/borrower_collateral tables that require token_id.
--
-- Schema is normalized: loan metadata is stored once in maple_loan, referenced by
-- maple_borrower and maple_collateral via loan_id.

-- Maple loan metadata - one row per loan per block snapshot
-- Contains loanMeta fields from the Maple API (type, dexName, walletAddress, etc.)
-- and the funding pool information for the loan.
CREATE TABLE maple_loan (
    id                  BIGSERIAL PRIMARY KEY,
    loan_address        BYTEA NOT NULL,                 -- openTermLoan.id (contract address)
    protocol_id         BIGINT NOT NULL REFERENCES protocol(id),
    block_number        BIGINT NOT NULL,
    block_version       INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Funding pool fields: every loan is funded by a specific pool
    pool_address        BYTEA NOT NULL,                 -- funding pool contract address
    pool_name           VARCHAR(255) NOT NULL,          -- e.g., "Syrup USDC Pool"
    pool_asset_symbol   VARCHAR(50) NOT NULL,           -- e.g., "USDC", "WBTC"
    pool_asset_decimals INT NOT NULL,                   -- decimals for pool asset
    -- loanMeta fields: populated for internal Maple positions (type = "amm" or "strategy"), NULL for external loans
    loan_type           VARCHAR(50),                    -- "amm", "strategy" for internal loans, NULL for external
    loan_asset_symbol   VARCHAR(50),                    -- underlying asset symbol for internal positions
    loan_dex_name       VARCHAR(100),                   -- e.g., "Aerodrome", "Fluid" for AMM positions
    loan_location       VARCHAR(255),                   -- location metadata
    loan_wallet_address VARCHAR(255),                   -- wallet address holding the position
    loan_wallet_type    VARCHAR(50),                    -- e.g., "EVM", "BASE" for blockchain type
    UNIQUE(loan_address, block_number, block_version)
);

CREATE INDEX idx_maple_loan_protocol ON maple_loan(protocol_id);
CREATE INDEX idx_maple_loan_block ON maple_loan(block_number);
CREATE INDEX idx_maple_loan_block_version ON maple_loan(block_number, block_version);
CREATE INDEX idx_maple_loan_type ON maple_loan(loan_type) WHERE loan_type IS NOT NULL;
CREATE INDEX idx_maple_loan_pool ON maple_loan(pool_address);

-- Maple borrower (denominated in pool assets like USDC, WBTC)
CREATE TABLE maple_borrower (
    id              BIGSERIAL PRIMARY KEY,
    loan_id         BIGINT NOT NULL REFERENCES maple_loan(id),
    user_id         BIGINT NOT NULL REFERENCES "user"(id),
    protocol_id     BIGINT NOT NULL REFERENCES protocol(id),
    pool_asset      VARCHAR(50) NOT NULL,
    pool_decimals   INT NOT NULL,
    amount          NUMERIC NOT NULL,
    block_number    BIGINT NOT NULL,
    block_version   INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(loan_id, block_number, block_version)
);

CREATE INDEX idx_maple_borrower_loan ON maple_borrower(loan_id);
CREATE INDEX idx_maple_borrower_user ON maple_borrower(user_id);
CREATE INDEX idx_maple_borrower_protocol ON maple_borrower(protocol_id);
CREATE INDEX idx_maple_borrower_block ON maple_borrower(block_number);
CREATE INDEX idx_maple_borrower_block_version ON maple_borrower(block_number, block_version);

-- Maple collateral (denominated in collateral assets like BTC, XRP)
CREATE TABLE maple_collateral (
    id                  BIGSERIAL PRIMARY KEY,
    loan_id             BIGINT NOT NULL REFERENCES maple_loan(id),
    user_id             BIGINT NOT NULL REFERENCES "user"(id),
    protocol_id         BIGINT NOT NULL REFERENCES protocol(id),
    collateral_asset    VARCHAR(50) NOT NULL,
    collateral_decimals INT NOT NULL,
    amount              NUMERIC NOT NULL,
    custodian           VARCHAR(100),
    state               VARCHAR(50),
    liquidation_level   NUMERIC,
    block_number        BIGINT NOT NULL,
    block_version       INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(loan_id, block_number, block_version)
);

CREATE INDEX idx_maple_collateral_loan ON maple_collateral(loan_id);
CREATE INDEX idx_maple_collateral_user ON maple_collateral(user_id);
CREATE INDEX idx_maple_collateral_protocol ON maple_collateral(protocol_id);
CREATE INDEX idx_maple_collateral_block ON maple_collateral(block_number);
CREATE INDEX idx_maple_collateral_block_version ON maple_collateral(block_number, block_version);

-- Grant permissions
GRANT SELECT ON maple_loan TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON maple_loan TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE maple_loan_id_seq TO stl_readwrite;

GRANT SELECT ON maple_borrower TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON maple_borrower TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE maple_borrower_id_seq TO stl_readwrite;

GRANT SELECT ON maple_collateral TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON maple_collateral TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE maple_collateral_id_seq TO stl_readwrite;

-- Seed Maple Finance protocol (MapleGlobals contract on mainnet)
-- https://github.com/maple-labs/address-registry/blob/main/MapleAddressRegistryETH.md
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
VALUES (1, '\x804a6F5F667170F545Bf14e5DDB48C70B788390C'::bytea, 'Maple Finance', 'rwa', 11964925, NOW())
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO migrations (filename) VALUES ('20260303_100000_add_maple.sql')
ON CONFLICT (filename) DO NOTHING;