-- Add multi-chain support: chain_id columns, hypertable conversion, compression, retention.
-- REQUIRES: watcher stopped during migration (hypertable conversion rewrites block_states).

-- 1a. Seed Avalanche C-Chain
INSERT INTO chain (chain_id, name) VALUES (43114, 'Avalanche C-Chain') ON CONFLICT DO NOTHING;

-- 1b. block_states: add columns (before hypertable conversion)
ALTER TABLE block_states ADD COLUMN chain_id INT NOT NULL DEFAULT 1;
-- created_at: set to block timestamp by application code (deterministic for dedup).
-- DEFAULT NOW() is a safety net only; all INSERT paths should set created_at explicitly.
ALTER TABLE block_states ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE block_states ADD CONSTRAINT fk_block_states_chain
    FOREIGN KEY (chain_id) REFERENCES chain(chain_id);

-- 1c. block_states: restructure constraints for hypertable compatibility
-- TimescaleDB requires all unique constraints to include partition columns (created_at, chain_id).
ALTER TABLE block_states DROP CONSTRAINT block_states_pkey;
ALTER TABLE block_states DROP CONSTRAINT block_states_hash_key;
ALTER TABLE block_states DROP CONSTRAINT unique_block_number_version;

ALTER TABLE block_states ADD PRIMARY KEY (created_at, chain_id, number, hash);
ALTER TABLE block_states ADD CONSTRAINT unique_chain_block_version
    UNIQUE (created_at, chain_id, number, version);

-- 1d. block_states: convert to hypertable
SELECT create_hypertable('block_states', by_range('created_at', INTERVAL '1 day'),
    migrate_data => true);
SELECT add_dimension('block_states', by_hash('chain_id', 4));

-- 1e. block_states: indexes
DROP INDEX IF EXISTS idx_block_states_hash;
DROP INDEX IF EXISTS idx_block_states_canonical;
DROP INDEX IF EXISTS idx_block_states_incomplete_publish;

CREATE INDEX idx_block_states_hash ON block_states (hash);
CREATE INDEX idx_block_states_chain_canonical
    ON block_states (chain_id, number DESC) WHERE NOT is_orphaned;
CREATE INDEX idx_block_states_chain_incomplete_publish
    ON block_states (chain_id, number)
    WHERE NOT is_orphaned AND NOT block_published;

-- 1f. block_states: update trigger to be chain-aware
CREATE OR REPLACE FUNCTION assign_block_version() RETURNS TRIGGER AS $$
BEGIN
    SELECT COALESCE(MAX(version), -1) + 1 INTO NEW.version
    FROM block_states
    WHERE chain_id = NEW.chain_id AND number = NEW.number;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1g. block_states: compression and retention policies
ALTER TABLE block_states SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'chain_id',
    timescaledb.compress_orderby = 'number DESC'
);
SELECT add_compression_policy('block_states', INTERVAL '1 day');
SELECT add_retention_policy('block_states', INTERVAL '7 days');

-- 1h. reorg_events: add chain_id
ALTER TABLE reorg_events ADD COLUMN chain_id INT NOT NULL DEFAULT 1;
ALTER TABLE reorg_events ADD CONSTRAINT fk_reorg_events_chain
    FOREIGN KEY (chain_id) REFERENCES chain(chain_id);

DROP INDEX IF EXISTS idx_reorg_events_detected_at;
DROP INDEX IF EXISTS idx_reorg_events_block_number;
CREATE INDEX idx_reorg_events_chain_detected_at ON reorg_events (chain_id, detected_at DESC);
CREATE INDEX idx_reorg_events_chain_block_number ON reorg_events (chain_id, block_number);

-- 1i. backfill_watermark: add chain_id
ALTER TABLE backfill_watermark DROP CONSTRAINT IF EXISTS backfill_watermark_id_check;
ALTER TABLE backfill_watermark ADD COLUMN chain_id INT NOT NULL DEFAULT 1;
ALTER TABLE backfill_watermark ADD CONSTRAINT fk_backfill_watermark_chain
    FOREIGN KEY (chain_id) REFERENCES chain(chain_id);
ALTER TABLE backfill_watermark ADD CONSTRAINT unique_backfill_watermark_chain
    UNIQUE (chain_id);

INSERT INTO backfill_watermark (id, chain_id, watermark) VALUES (2, 43114, 0)
    ON CONFLICT DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260207_120000_add_chain_id_and_hypertable.sql')
ON CONFLICT (filename) DO NOTHING;
