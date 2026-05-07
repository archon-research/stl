-- Per-token pool totalSupply / scaledTotalSupply indexed alongside allocation_position.
--
-- Populated by the prime-allocation-indexer's BalanceOfSource in the same multicall
-- batch that reads balanceOf / scaledBalanceOf. One row per (chain, token, block).
--
-- Auditability follows the repo+trigger pattern used by allocation_position:
--   - Repository supplies build_id at construction.
--   - assign_processing_version_token_total_supply BEFORE INSERT trigger assigns
--     processing_version so same-build replays are idempotent and cross-build
--     reprocesses append a new version.
--
-- The hypertable partition column is block_timestamp. Both application writes
-- (event path and sweep path) land in the same partition for a given block.

CREATE TABLE IF NOT EXISTS token_total_supply (
    chain_id            INT         NOT NULL REFERENCES chain (chain_id),
    token_id            BIGINT      NOT NULL REFERENCES token (id),
    total_supply        NUMERIC     NOT NULL,
    scaled_total_supply NUMERIC,
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL DEFAULT 0,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    source              TEXT        NOT NULL CHECK (source IN ('event', 'sweep')),
    processing_version  INT         NOT NULL DEFAULT 0,
    build_id            INT         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, token_id, block_number, block_version, processing_version, block_timestamp)
);

SELECT create_hypertable('token_total_supply', by_range('block_timestamp', INTERVAL '7 days'));

CREATE INDEX idx_tts_current
    ON token_total_supply (chain_id, token_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_token_total_supply()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('tts|%s|%s|%s|%s|%s',
            NEW.chain_id,
            NEW.token_id,
            NEW.block_number,
            NEW.block_version,
            NEW.block_timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM token_total_supply
    WHERE chain_id        = NEW.chain_id
      AND token_id        = NEW.token_id
      AND block_number    = NEW.block_number
      AND block_version   = NEW.block_version
      AND block_timestamp = NEW.block_timestamp
      AND build_id        = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM token_total_supply
        WHERE chain_id        = NEW.chain_id
          AND token_id        = NEW.token_id
          AND block_number    = NEW.block_number
          AND block_version   = NEW.block_version
          AND block_timestamp = NEW.block_timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON token_total_supply
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_token_total_supply();

GRANT SELECT ON token_total_supply TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON token_total_supply TO stl_readwrite;

-- Columnstore + tiering, mirroring allocation_position.

ALTER TABLE token_total_supply SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'chain_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('token_total_supply', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('token_total_supply', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for token_total_supply';
END $$;

INSERT INTO migrations (filename)
VALUES ('20260423_214929_create_token_total_supply.sql')
ON CONFLICT (filename) DO NOTHING;
