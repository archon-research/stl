-- VEC-185: Cover assign_processing_version_protocol_event() lookup.
CREATE INDEX IF NOT EXISTS idx_protocol_event_pv_lookup
    ON protocol_event (chain_id, block_number, block_version, tx_hash, log_index, created_at, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_121000_index_protocol_event_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
