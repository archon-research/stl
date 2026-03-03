-- Create Maple-specific tables for borrower debt and collateral positions.
-- Maple data uses symbol-only assets (no contract address, no EVM chain ID),
-- so it cannot use the shared borrower/borrower_collateral tables that require token_id.

-- Maple borrower (denominated in pool assets like USDC, WBTC)
-- loan_meta fields: populated for internal Maple positions (type = "amm" or "strategy"), NULL for external loans
CREATE TABLE maple_borrower (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             BIGINT NOT NULL REFERENCES "user"(id),
    protocol_id         BIGINT NOT NULL REFERENCES protocol(id),
    pool_asset          VARCHAR(50) NOT NULL,
    pool_decimals       INT NOT NULL,
    amount              NUMERIC NOT NULL,
    block_number        BIGINT NOT NULL,
    block_version       INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- loanMeta fields for internal loan tracking
    loan_type           VARCHAR(50),   -- "amm", "strategy" for internal loans, NULL for external
    loan_asset_symbol   VARCHAR(50),   -- underlying asset symbol for internal positions
    loan_dex_name       VARCHAR(100),  -- e.g., "Aerodrome" for AMM positions
    loan_location       VARCHAR(255),  -- location metadata
    loan_wallet_address VARCHAR(255),  -- wallet address holding the position
    loan_wallet_type    VARCHAR(50),   -- e.g., "BASE" for Base blockchain
    UNIQUE(user_id, protocol_id, pool_asset, block_number, block_version)
);

CREATE INDEX idx_maple_borrower_user ON maple_borrower(user_id);
CREATE INDEX idx_maple_borrower_protocol ON maple_borrower(protocol_id);
CREATE INDEX idx_maple_borrower_block ON maple_borrower(block_number);
CREATE INDEX idx_maple_borrower_block_version ON maple_borrower(block_number, block_version);

-- Maple collateral (denominated in collateral assets like BTC, XRP)
-- loan_meta fields: populated for internal Maple positions (type = "amm" or "strategy"), NULL for external loans
CREATE TABLE maple_collateral (
    id                  BIGSERIAL PRIMARY KEY,
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
    -- loanMeta fields for internal loan tracking
    loan_type           VARCHAR(50),   -- "amm", "strategy" for internal loans, NULL for external
    loan_asset_symbol   VARCHAR(50),   -- underlying asset symbol for internal positions
    loan_dex_name       VARCHAR(100),  -- e.g., "Aerodrome" for AMM positions
    loan_location       VARCHAR(255),  -- location metadata
    loan_wallet_address VARCHAR(255),  -- wallet address holding the position
    loan_wallet_type    VARCHAR(50),   -- e.g., "BASE" for Base blockchain
    UNIQUE(user_id, protocol_id, collateral_asset, block_number, block_version)
);

CREATE INDEX idx_maple_collateral_user ON maple_collateral(user_id);
CREATE INDEX idx_maple_collateral_protocol ON maple_collateral(protocol_id);
CREATE INDEX idx_maple_collateral_block ON maple_collateral(block_number);
CREATE INDEX idx_maple_collateral_block_version ON maple_collateral(block_number, block_version);

-- Grant permissions
GRANT SELECT ON maple_borrower TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON maple_borrower TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE maple_borrower_id_seq TO stl_readwrite;

GRANT SELECT ON maple_collateral TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON maple_collateral TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE maple_collateral_id_seq TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260303_100000_create_maple_tables.sql')
ON CONFLICT (filename) DO NOTHING;
