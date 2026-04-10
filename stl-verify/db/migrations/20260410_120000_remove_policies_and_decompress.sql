-- Remove compression/columnstore policies and decompress all affected hypertable chunks.
-- This prepares for PK/UNIQUE constraint alteration in the next migration.
--
-- Split from constraint alteration so that if decompression fails, no schema changes
-- have occurred. Constraint alteration (next file) is fast DDL on decompressed data.
--
-- Tables with no compression (sparklend_reserve_data, prime_debt) are skipped.
--
-- See ADR-0002: Data Auditability and Processing Versioning.

-- ============================================================================
-- Helper: decompress all chunks for a hypertable, skipping already-decompressed.
-- decompress_chunk() on self-hosted TimescaleDB does not support if_not_exists,
-- so we check chunk status first.
-- ============================================================================

CREATE OR REPLACE FUNCTION _decompress_all_chunks(p_hypertable regclass)
RETURNS void AS $$
DECLARE
    v_chunk regclass;
BEGIN
    FOR v_chunk IN
        SELECT show_chunks(p_hypertable)
    LOOP
        BEGIN
            PERFORM decompress_chunk(v_chunk);
        EXCEPTION WHEN OTHERS THEN
            -- Chunk was not compressed; skip.
            NULL;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Old compression API tables (timescaledb.compress)
-- ============================================================================

SELECT remove_compression_policy('onchain_token_price', if_exists => true);
SELECT remove_compression_policy('morpho_market_state', if_exists => true);
SELECT remove_compression_policy('morpho_market_position', if_exists => true);
SELECT remove_compression_policy('morpho_vault_state', if_exists => true);
SELECT remove_compression_policy('morpho_vault_position', if_exists => true);
SELECT remove_compression_policy('anchorage_package_snapshot', if_exists => true);
SELECT remove_compression_policy('anchorage_operation', if_exists => true);
SELECT remove_compression_policy('offchain_token_price', if_exists => true);

SELECT _decompress_all_chunks('onchain_token_price');
SELECT _decompress_all_chunks('morpho_market_state');
SELECT _decompress_all_chunks('morpho_market_position');
SELECT _decompress_all_chunks('morpho_vault_state');
SELECT _decompress_all_chunks('morpho_vault_position');
SELECT _decompress_all_chunks('anchorage_package_snapshot');
SELECT _decompress_all_chunks('anchorage_operation');
SELECT _decompress_all_chunks('offchain_token_price');

-- ============================================================================
-- Columnstore API tables (timescaledb.enable_columnstore)
-- Pause jobs, decompress, disable columnstore so constraints can be altered.
-- ============================================================================

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
            PERFORM alter_job(v_job_id, scheduled => false);
        END IF;
    END LOOP;
END $$;

SELECT _decompress_all_chunks('borrower');
SELECT _decompress_all_chunks('borrower_collateral');
SELECT _decompress_all_chunks('protocol_event');
SELECT _decompress_all_chunks('allocation_position');

ALTER TABLE borrower SET (timescaledb.columnstore = false);
ALTER TABLE borrower_collateral SET (timescaledb.columnstore = false);
ALTER TABLE protocol_event SET (timescaledb.columnstore = false);
ALTER TABLE allocation_position SET (timescaledb.columnstore = false);

-- Cleanup helper
DROP FUNCTION _decompress_all_chunks(regclass);

INSERT INTO migrations (filename)
VALUES ('20260410_120000_remove_policies_and_decompress.sql')
ON CONFLICT (filename) DO NOTHING;
