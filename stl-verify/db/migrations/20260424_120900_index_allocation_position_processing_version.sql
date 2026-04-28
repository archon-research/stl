-- VEC-185: Cover assign_processing_version_allocation_position() lookup.
CREATE INDEX IF NOT EXISTS idx_allocation_position_pv_lookup
    ON allocation_position (chain_id, token_id, prime_id, proxy_address, block_number, block_version, tx_hash, log_index, direction, created_at, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_120900_index_allocation_position_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
