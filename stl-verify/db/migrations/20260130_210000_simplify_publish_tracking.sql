-- migrate: no-transaction
-- Simplify publish tracking to only use block_published column.
-- This migration runs outside a transaction to allow CONCURRENTLY operations.

-- Drop the old index that checked multiple publish columns
DROP INDEX CONCURRENTLY IF EXISTS idx_block_states_incomplete_publish;

-- Create new index that only checks block_published
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_states_incomplete_publish ON block_states (number)
    WHERE NOT is_orphaned AND NOT block_published;

-- Remove the legacy publish tracking columns (no longer used)
ALTER TABLE block_states DROP COLUMN IF EXISTS receipts_published;
ALTER TABLE block_states DROP COLUMN IF EXISTS traces_published;
ALTER TABLE block_states DROP COLUMN IF EXISTS blobs_published;

-- Add comment explaining the design
COMMENT ON COLUMN block_states.block_published IS 'Tracks whether the single SQS publish event was successful. All block data (block, receipts, traces, blobs) is published in a single event.';

INSERT INTO migrations (filename)
VALUES ('20260130_210000_simplify_publish_tracking.sql')
ON CONFLICT (filename) DO NOTHING;
