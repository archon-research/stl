-- PSM3 share legs: total shares on the pool row, per-ALM stakes in their own table.
--
-- PSM3 is also an LP pool with an internal, non-transferable share mapping.
-- psm3_reserves says how big the pool is, so totalShares() lands there;
-- who owns how much of it is per holder, so each tracked ALM proxy gets its own
-- psm3_alm_shares row per sweep, keyed by (prime_id, alm_address). Tracking a
-- new prime's stake is a registry/config entry, never a migration.
--
-- total_shares is nullable because psm3_reserves is append-only: rows written
-- before this migration have no share readings and are never backfilled.

ALTER TABLE psm3_reserves
    ADD COLUMN total_shares NUMERIC;   -- raw 1e18, PSM3.totalShares()

COMMENT ON COLUMN psm3_reserves.total_shares IS
  'PSM3.totalShares() — total LP shares outstanding. Raw on-chain integer at 1e18: shares are minted in the contract''s own 18-decimal USD value unit (the 1 USDC genesis deposit mints 1e18 shares) and the value per share drifts up from there, so this is NOT a token amount and must not be scaled by any token.decimals. Never zero on a live deployment: the deploy script seeds a first deposit to address(0), permanently locking about 1e18 share units. Per-holder stakes are in psm3_alm_shares; divide its shares by this column for an ownership fraction. NULL on rows written before the share legs were indexed.';

-- One tracked ALM proxy's stake in one PSM3 at one block. Same sweep, same
-- pinned block hash and same cadence as the matching psm3_reserves row; the
-- repository writes both in one transaction. Auditability follows the
-- psm3_reserves pattern (build_id + assign_processing_version trigger).
CREATE TABLE IF NOT EXISTS psm3_alm_shares (
    chain_id           INT         NOT NULL REFERENCES chain (chain_id),
    address            BYTEA       NOT NULL,            -- PSM3 contract
    prime_id           BIGINT      NOT NULL REFERENCES prime (id),
    alm_address        BYTEA       NOT NULL,            -- holder the legs were read for
    shares             NUMERIC     NOT NULL,            -- raw 1e18, PSM3.shares(alm_address)
    asset_value        NUMERIC     NOT NULL,            -- raw 1e18 par, PSM3.convertToAssetValue(shares)
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    source             TEXT        NOT NULL CHECK (source = 'sweep'),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, alm_address, block_number, block_version, processing_version, block_timestamp)
);

SELECT create_hypertable('psm3_alm_shares', by_range('block_timestamp', INTERVAL '7 days'));

CREATE INDEX idx_psm3_alm_shares_current
    ON psm3_alm_shares (chain_id, alm_address, block_number DESC, block_version DESC, processing_version DESC);

COMMENT ON TABLE psm3_alm_shares IS
  '[Hypertable] One tracked ALM proxy''s LP stake in a Spark PSM3 pool at one block, one row per (chain, alm_address, block) per sweep. Written by the psm3-indexer in the same transaction and pinned to the same block hash as the matching psm3_reserves row. Which ALMs are tracked comes from indexer config cross-checked against axis-synome at startup; adding a prime is a config entry, not a schema change.';
COMMENT ON COLUMN psm3_alm_shares.chain_id IS 'Roles: PK, FK→chain.chain_id.';
COMMENT ON COLUMN psm3_alm_shares.address IS 'PSM3 contract the stake is held in. Matches psm3_reserves.address for the same (chain_id, block).';
COMMENT ON COLUMN psm3_alm_shares.prime_id IS 'Roles: FK→prime.id. The prime whose ALM proxy this stake belongs to, resolved by prime.name from the indexer config at write time.';
COMMENT ON COLUMN psm3_alm_shares.alm_address IS 'Roles: PK. The ALM proxy the share legs were read for. Config-sourced (cross-checked against axis-synome at startup), so after a proxy rotation rows on either side name their holder and a step change in shares stays distinguishable from a real divestment. Shares are an internal mapping with no share token, so the holder is not recoverable from a later state read; holder history is reconstructable only by replaying Deposit/Withdraw logs.';
COMMENT ON COLUMN psm3_alm_shares.shares IS 'PSM3.shares(alm_address) — the internal LP shares held by this ALM proxy. deposit() is permissionless, so other depositors are possible and shares / psm3_reserves.total_shares stays below 1. Raw on-chain integer at 1e18 in the contract''s own USD value unit (see psm3_reserves.total_shares), NOT a token amount — never scale by token.decimals.';
COMMENT ON COLUMN psm3_alm_shares.asset_value IS 'PSM3.convertToAssetValue(shares) — this stake valued at par. Raw on-chain integer, 18-decimal USD, the same par scale as psm3_reserves.total_assets (not market-priced). Equals floor(shares x total_assets / total_shares) exactly in integer math over the matching psm3_reserves row — derivable by construction, stored for auditability: a nonzero residual indicates a decode/ordering bug, not rounding.';
COMMENT ON COLUMN psm3_alm_shares.block_number IS 'Roles: PK. Block the read was pinned to.';
COMMENT ON COLUMN psm3_alm_shares.block_version IS 'Roles: PK. Increments on chain reorgs.';
COMMENT ON COLUMN psm3_alm_shares.block_timestamp IS 'Roles: PK, Partition. Timestamp of the pinned block.';
COMMENT ON COLUMN psm3_alm_shares.source IS 'How the row was produced; ''sweep''-only until an event-driven path lands.';
COMMENT ON COLUMN psm3_alm_shares.processing_version IS 'Roles: PK. Correction version: 0=original, N=Nth reprocess (assigned by trigger).';
COMMENT ON COLUMN psm3_alm_shares.build_id IS 'Roles: Audit. Which deployment wrote the row; never use to pick latest.';
COMMENT ON COLUMN psm3_alm_shares.created_at IS 'Roles: Audit. Insert wall-clock time.';

CREATE OR REPLACE FUNCTION assign_processing_version_psm3_alm_shares()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('psm3alm|%s|%s|%s|%s|%s',
            NEW.chain_id,
            NEW.alm_address,
            NEW.block_number,
            NEW.block_version,
            NEW.block_timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM psm3_alm_shares
    WHERE chain_id        = NEW.chain_id
      AND alm_address     = NEW.alm_address
      AND block_number    = NEW.block_number
      AND block_version   = NEW.block_version
      AND block_timestamp = NEW.block_timestamp
      AND build_id        = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM psm3_alm_shares
        WHERE chain_id        = NEW.chain_id
          AND alm_address     = NEW.alm_address
          AND block_number    = NEW.block_number
          AND block_version   = NEW.block_version
          AND block_timestamp = NEW.block_timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON psm3_alm_shares
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_psm3_alm_shares();

-- Append-only from birth: the indexer only ever INSERTs (ON CONFLICT DO NOTHING),
-- so the application role gets no update channel and a future UPDATE/DELETE or
-- DO UPDATE fails at runtime instead of rewriting history.
GRANT SELECT ON psm3_alm_shares TO stl_readonly;
GRANT SELECT, INSERT ON psm3_alm_shares TO stl_readwrite;
REVOKE UPDATE, DELETE ON psm3_alm_shares FROM stl_readwrite;

-- Columnstore/tiering deliberately skipped, matching psm3_reserves: ~600 rows/day
-- per tracked ALM across 4 chains is not worth the policy overhead.

INSERT INTO migrations (filename)
VALUES ('20260819_110000_psm3_share_legs.sql')
ON CONFLICT (filename) DO NOTHING;
