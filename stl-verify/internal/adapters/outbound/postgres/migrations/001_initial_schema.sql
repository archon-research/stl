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
    PRIMARY KEY (number, hash),
    CONSTRAINT unique_block_number_version UNIQUE (number, version),
    CONSTRAINT version_non_negative CHECK (version >= 0)
);

CREATE INDEX IF NOT EXISTS idx_block_states_hash ON block_states(hash);
CREATE INDEX IF NOT EXISTS idx_block_states_received_at ON block_states(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_block_states_canonical ON block_states(number DESC) WHERE NOT is_orphaned;
CREATE INDEX IF NOT EXISTS idx_block_states_orphaned ON block_states(is_orphaned) WHERE is_orphaned;

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
