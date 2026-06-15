-- Add indexes to support time-windowed API queries for allocation activity,
-- protocol events, and prime debt snapshots.

CREATE INDEX IF NOT EXISTS idx_alloc_pos_prime_created_at_sort
    ON allocation_position (prime_id, created_at DESC, block_number DESC, block_version DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS idx_alloc_pos_chain_created_at_sort
    ON allocation_position (chain_id, created_at DESC, block_number DESC, block_version DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS idx_protocol_event_protocol_created_at_sort
    ON protocol_event (protocol_id, created_at DESC, block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS idx_prime_debt_prime_synced_block_sort
    ON prime_debt (prime_id, synced_at DESC, block_number DESC, block_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260615_120000_add_time_series_query_indexes.sql')
ON CONFLICT (filename) DO NOTHING;
