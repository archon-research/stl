-- VEC-185: Cover assign_processing_version_sparklend_reserve_data() lookup.
CREATE INDEX IF NOT EXISTS idx_sparklend_reserve_data_pv_lookup
    ON sparklend_reserve_data (protocol_id, token_id, block_number, block_version, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_120200_index_sparklend_reserve_data_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
