-- VEC-185: Cover assign_processing_version_anchorage_package_snapshot() lookup.
CREATE INDEX IF NOT EXISTS idx_anchorage_package_snapshot_pv_lookup
    ON anchorage_package_snapshot (prime_id, package_id, asset_type, custody_type, snapshot_time, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_121100_index_anchorage_package_snapshot_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
