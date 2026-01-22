-- Creates the core tables for block state tracking

-- Block states table tracks all blocks seen (both canonical and orphaned)
CREATE TABLE IF NOT EXISTS block_states (
                                            number BIGINT NOT NULL,
                                            hash TEXT NOT NULL UNIQUE,
                                            parent_hash TEXT NOT NULL,
                                            received_at BIGINT NOT NULL,
                                            is_orphaned BOOLEAN NOT NULL DEFAULT FALSE,
                                            version INT NOT NULL DEFAULT 0,
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
CREATE INDEX IF NOT EXISTS idx_block_states_incomplete_publish ON block_states(number)
    WHERE NOT is_orphaned AND (NOT block_published OR NOT receipts_published OR NOT traces_published);

-- Function to auto-assign version on insert
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

-- Reorg events table
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

-- Backfill watermark
CREATE TABLE IF NOT EXISTS backfill_watermark (
                                                  id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                                                  watermark BIGINT NOT NULL DEFAULT 0
);

INSERT INTO backfill_watermark (id, watermark) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;
INSERT INTO migrations (filename)
VALUES ('20250122_143000_initial_schema.sql')
ON CONFLICT (filename) DO NOTHING;