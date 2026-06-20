-- migrate: no-transaction
--
-- Add segmentby to the four columnstore hypertables that were previously
-- compressed with orderby-only:
--   allocation_position, borrower, borrower_collateral, protocol_event.
--
-- Why: TimescaleDB's columnstore (hypercore) builds per-segment min/max and
-- bloom-filter sparse indexes from the segmentby columns. Without segmentby,
-- every batch in every chunk must be decompressed to evaluate filters on the
-- segmentby targets, which is the dominant cost of `/risk-capital` and a
-- direct contributor to backend memory spikes/OOMs under concurrency.
--
-- Segmentby choice per table reflects the actual hot filter columns:
--   allocation_position : chain_id, token_id, proxy_address   (~4*51*1 segments/chunk)
--   borrower            : protocol_id, token_id               (~5*55 segments/chunk)
--   borrower_collateral : protocol_id, token_id               (~6*98 segments/chunk)
--   protocol_event      : protocol_id                         (~7 segments/chunk)
--
-- user_id is deliberately excluded from borrower*/segmentby: cardinality is
-- 67k/240k, which would shred compression ratio and explode batch count.
--
-- Runs outside a transaction (`-- migrate: no-transaction` above) so the
-- policy_compression pause commits before decompress/ALTER. In a single
-- transaction the worker can't observe `scheduled => false` until the
-- migration commits, which would let it race the decompress/ALTER and
-- deadlock. Each statement below is idempotent and safe to re-run if the
-- migration fails partway:
--   - alter_job() sets a stable target, not a delta;
--   - decompress_chunk() loops only over `is_compressed = true`;
--   - ALTER TABLE ... SET (timescaledb.segmentby=...) is a no-op on second
--     application;
--   - re-enable sets scheduled => true unconditionally;
--   - the final INSERT uses ON CONFLICT DO NOTHING.
--
-- We do NOT recompress synchronously: holding the per-chunk advisory lock
-- plus a ShareRowExclusiveLock races with the freshly-resumed policy worker
-- and deadlocks in load-heavy environments. The policy job picks decompressed
-- chunks up on its next scheduled run and recompresses them with the new
-- segmentby naturally.

-- ============================================================================
-- Pause the per-table policy_compression jobs. `alter_job(scheduled => false)`
-- only stops future scheduling — a currently-running job continues. We pause
-- here and then rely on the next step's `decompress_chunk` calls to fail-fast
-- if a worker is still mid-compression on the same chunk (the migration is
-- idempotent, so re-running after a transient deadlock is safe).
-- ============================================================================
DO $$
DECLARE
    v_job_id INT;
    v_table TEXT;
BEGIN
    FOR v_table IN VALUES ('allocation_position'), ('borrower'),
                          ('borrower_collateral'), ('protocol_event')
    LOOP
        SELECT job_id INTO v_job_id FROM timescaledb_information.jobs
        WHERE proc_name = 'policy_compression'
          AND hypertable_name = v_table;
        IF v_job_id IS NULL THEN
            RAISE WARNING
                'pause: no policy_compression job found for hypertable %; '
                'segmentby change will still apply but no policy will recompress',
                v_table;
        ELSE
            PERFORM alter_job(v_job_id, scheduled => false);
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- Decompress every compressed chunk so the new segmentby applies on recompress.
-- ============================================================================
DO $$
DECLARE
    v_chunk regclass;
BEGIN
    FOR v_chunk IN
        SELECT format('%I.%I', chunk_schema, chunk_name)::regclass
        FROM timescaledb_information.chunks
        WHERE hypertable_name IN (
                  'allocation_position', 'borrower',
                  'borrower_collateral', 'protocol_event'
              )
          AND is_compressed = true
    LOOP
        PERFORM decompress_chunk(v_chunk);
    END LOOP;
END $$;

-- ============================================================================
-- Apply the new segmentby. orderby is preserved from 20260410_140000.
-- Re-running these is a no-op when the options already match.
-- ============================================================================
ALTER TABLE allocation_position SET (
    timescaledb.columnstore = true,
    timescaledb.segmentby = 'chain_id, token_id, proxy_address',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

ALTER TABLE borrower SET (
    timescaledb.columnstore = true,
    timescaledb.segmentby = 'protocol_id, token_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

ALTER TABLE borrower_collateral SET (
    timescaledb.columnstore = true,
    timescaledb.segmentby = 'protocol_id, token_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

ALTER TABLE protocol_event SET (
    timescaledb.columnstore = true,
    timescaledb.segmentby = 'protocol_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- ============================================================================
-- Re-enable the paused jobs. The next scheduled run will recompress any
-- chunks decompressed above (and pick up the new segmentby).
-- ============================================================================
DO $$
DECLARE
    v_job_id INT;
    v_table TEXT;
BEGIN
    FOR v_table IN VALUES ('allocation_position'), ('borrower'),
                          ('borrower_collateral'), ('protocol_event')
    LOOP
        SELECT job_id INTO v_job_id FROM timescaledb_information.jobs
        WHERE proc_name = 'policy_compression'
          AND hypertable_name = v_table;
        IF v_job_id IS NULL THEN
            RAISE WARNING
                'resume: no policy_compression job found for hypertable %; '
                'background recompression will not run until a policy is added',
                v_table;
        ELSE
            PERFORM alter_job(v_job_id, scheduled => true);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260620_120000_add_columnstore_segmentby.sql')
ON CONFLICT (filename) DO NOTHING;
