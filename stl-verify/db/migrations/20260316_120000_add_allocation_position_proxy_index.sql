CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_alloc_pos_proxy
    ON allocation_position (proxy_address, block_number DESC);

INSERT INTO migrations (filename)
VALUES ('20260316_120000_add_allocation_position_proxy_index.sql')
ON CONFLICT (filename) DO NOTHING;