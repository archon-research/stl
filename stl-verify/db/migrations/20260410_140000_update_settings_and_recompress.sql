-- Update compression/columnstore orderby settings to include processing_version,
-- then re-enable policies and recompress existing chunks.
--
-- processing_version is appended to the existing orderby so it benefits from
-- RLE/dictionary compression (almost always 0, compresses to near-zero).
--
-- See ADR-0002: Data Auditability and Processing Versioning.

-- ============================================================================
-- Helper: compress uncompressed chunks older than an interval.
-- Only targets chunks that are not already compressed, avoiding errors
-- without swallowing real failures.
-- ============================================================================

CREATE OR REPLACE FUNCTION _compress_old_chunks(p_hypertable regclass, p_older_than interval)
RETURNS void AS $$
DECLARE
    v_chunk regclass;
    v_ht_schema text;
    v_ht_name text;
    v_cutoff timestamptz := NOW() - p_older_than;
BEGIN
    SELECT nspname, relname INTO v_ht_schema, v_ht_name
    FROM pg_class JOIN pg_namespace ON pg_namespace.oid = relnamespace
    WHERE pg_class.oid = p_hypertable;

    FOR v_chunk IN
        SELECT format('%I.%I', chunk_schema, chunk_name)::regclass
        FROM timescaledb_information.chunks
        WHERE hypertable_schema = v_ht_schema
          AND hypertable_name = v_ht_name
          AND is_compressed = false
          AND range_end < v_cutoff
    LOOP
        PERFORM compress_chunk(v_chunk);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Old compression API tables — update settings and re-add policies
-- ============================================================================

-- onchain_token_price (was: block_number DESC, block_version DESC)
ALTER TABLE onchain_token_price SET (
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);
SELECT add_compression_policy('onchain_token_price', INTERVAL '2 days', if_not_exists => true);

-- morpho_market_state (was: block_number DESC, block_version DESC)
ALTER TABLE morpho_market_state SET (
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);
SELECT add_compression_policy('morpho_market_state', INTERVAL '2 days', if_not_exists => true);

-- morpho_market_position (was: block_number DESC, block_version DESC)
ALTER TABLE morpho_market_position SET (
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);
SELECT add_compression_policy('morpho_market_position', INTERVAL '2 days', if_not_exists => true);

-- morpho_vault_state (was: block_number DESC, block_version DESC)
ALTER TABLE morpho_vault_state SET (
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);
SELECT add_compression_policy('morpho_vault_state', INTERVAL '2 days', if_not_exists => true);

-- morpho_vault_position (was: block_number DESC, block_version DESC)
ALTER TABLE morpho_vault_position SET (
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);
SELECT add_compression_policy('morpho_vault_position', INTERVAL '2 days', if_not_exists => true);

-- anchorage_package_snapshot (was: snapshot_time DESC)
ALTER TABLE anchorage_package_snapshot SET (
    timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC'
);
SELECT add_compression_policy('anchorage_package_snapshot', INTERVAL '2 days', if_not_exists => true);

-- anchorage_operation (was: created_at DESC, operation_id DESC)
ALTER TABLE anchorage_operation SET (
    timescaledb.compress_orderby = 'created_at DESC, operation_id DESC, processing_version DESC'
);
SELECT add_compression_policy('anchorage_operation', INTERVAL '2 days', if_not_exists => true);

-- offchain_token_price (was: timestamp DESC)
ALTER TABLE offchain_token_price SET (
    timescaledb.compress_orderby = 'timestamp DESC, processing_version DESC'
);
SELECT add_compression_policy('offchain_token_price', INTERVAL '2 days', if_not_exists => true);

-- sparklend_reserve_data (compression was set externally; re-enable with processing_version)
-- Partitioned on block_number (integer), so compress_after uses block count, not interval.
-- 200000 blocks ≈ ~27 days at 12s/block.
ALTER TABLE sparklend_reserve_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'protocol_id, token_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);
SELECT add_compression_policy('sparklend_reserve_data', 200000, if_not_exists => true);

-- prime_debt (compression was set externally; re-enable with processing_version)
ALTER TABLE prime_debt SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'prime_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);
SELECT add_compression_policy('prime_debt', INTERVAL '2 days', if_not_exists => true);

-- Recompress chunks older than 2 days
SELECT _compress_old_chunks('onchain_token_price', INTERVAL '2 days');
SELECT _compress_old_chunks('morpho_market_state', INTERVAL '2 days');
SELECT _compress_old_chunks('morpho_market_position', INTERVAL '2 days');
SELECT _compress_old_chunks('morpho_vault_state', INTERVAL '2 days');
SELECT _compress_old_chunks('morpho_vault_position', INTERVAL '2 days');
SELECT _compress_old_chunks('anchorage_package_snapshot', INTERVAL '2 days');
SELECT _compress_old_chunks('anchorage_operation', INTERVAL '2 days');
SELECT _compress_old_chunks('offchain_token_price', INTERVAL '2 days');
SELECT _compress_old_chunks('sparklend_reserve_data', INTERVAL '2 days');
SELECT _compress_old_chunks('prime_debt', INTERVAL '2 days');

-- ============================================================================
-- Columnstore API tables — re-enable columnstore with updated orderby, restart jobs
-- ============================================================================

-- borrower (was: block_number DESC, block_version DESC)
ALTER TABLE borrower SET (
    timescaledb.columnstore = true,
    timescaledb.orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- borrower_collateral (was: block_number DESC, block_version DESC)
ALTER TABLE borrower_collateral SET (
    timescaledb.columnstore = true,
    timescaledb.orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- protocol_event (was: block_number DESC, block_version DESC)
ALTER TABLE protocol_event SET (
    timescaledb.columnstore = true,
    timescaledb.orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- allocation_position (was: block_number DESC, block_version DESC)
ALTER TABLE allocation_position SET (
    timescaledb.columnstore = true,
    timescaledb.orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- Restart paused columnstore jobs
DO $$
DECLARE
    v_job_id INT;
    v_table TEXT;
BEGIN
    FOR v_table IN VALUES ('borrower'), ('borrower_collateral'), ('protocol_event'), ('allocation_position')
    LOOP
        SELECT job_id INTO v_job_id FROM timescaledb_information.jobs
        WHERE proc_name = 'policy_compression'
          AND hypertable_name = v_table;
        IF v_job_id IS NOT NULL THEN
            PERFORM alter_job(v_job_id, scheduled => true);
        ELSE
            RAISE WARNING 'no compression job found for table %, cannot resume', v_table;
        END IF;
    END LOOP;
END $$;

-- Compress old chunks for columnstore tables
SELECT _compress_old_chunks('borrower', INTERVAL '2 days');
SELECT _compress_old_chunks('borrower_collateral', INTERVAL '2 days');
SELECT _compress_old_chunks('protocol_event', INTERVAL '2 days');
SELECT _compress_old_chunks('allocation_position', INTERVAL '2 days');

-- Cleanup helper
DROP FUNCTION _compress_old_chunks(regclass, interval);

INSERT INTO migrations (filename)
VALUES ('20260410_140000_update_settings_and_recompress.sql')
ON CONFLICT (filename) DO NOTHING;
