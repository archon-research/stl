-- Maple position and pool collateral snapshot tables
-- maple_position: tracks a user's lending position in a Maple pool
-- maple_pool_collateral: tracks collateral composition of each pool

CREATE TABLE IF NOT EXISTS maple_position (
    id               BIGSERIAL   PRIMARY KEY,
    user_id          BIGINT      NOT NULL REFERENCES "user"(id),
    protocol_id      BIGINT      NOT NULL REFERENCES protocol(id),
    pool_address     BYTEA       NOT NULL,
    pool_name        VARCHAR(255) NOT NULL,
    asset_symbol     VARCHAR(50) NOT NULL,
    asset_decimals   SMALLINT    NOT NULL,
    lending_balance  NUMERIC     NOT NULL,
    snapshot_block   BIGINT      NOT NULL,
    snapshot_time    TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, protocol_id, pool_address, snapshot_block)
);

CREATE INDEX IF NOT EXISTS idx_maple_position_user_id ON maple_position (user_id);
CREATE INDEX IF NOT EXISTS idx_maple_position_pool_address ON maple_position (pool_address);
CREATE INDEX IF NOT EXISTS idx_maple_position_snapshot_block ON maple_position (snapshot_block);

CREATE TABLE IF NOT EXISTS maple_pool_collateral (
    id               BIGSERIAL   PRIMARY KEY,
    pool_address     BYTEA       NOT NULL,
    pool_name        VARCHAR(255) NOT NULL,
    asset            VARCHAR(50) NOT NULL,
    asset_decimals   SMALLINT    NOT NULL,
    asset_value_usd  NUMERIC     NOT NULL,
    pool_tvl         NUMERIC     NOT NULL,
    snapshot_block   BIGINT      NOT NULL,
    snapshot_time    TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pool_address, asset, snapshot_block)
);

CREATE INDEX IF NOT EXISTS idx_maple_pool_collateral_pool_address ON maple_pool_collateral (pool_address);
CREATE INDEX IF NOT EXISTS idx_maple_pool_collateral_snapshot_block ON maple_pool_collateral (snapshot_block);

-- Seed Maple Finance protocol (MapleGlobals contract on mainnet)
-- https://github.com/maple-labs/address-registry/blob/main/MapleAddressRegistryETH.md
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
VALUES (1, '\x804a6F5F667170F545Bf14e5DDB48C70B788390C'::bytea, 'Maple Finance', 'rwa', 11964925, NOW())
ON CONFLICT (chain_id, address) DO NOTHING;
