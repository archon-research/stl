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
-- ── Execution shape (runs outside a transaction) ────────────────────────────
-- `-- migrate: no-transaction` makes the migrator run each statement below on
-- its own via the simple protocol (autocommit), so the pause commits before the
-- conversion and the per-chunk loop can COMMIT between chunks. Order matters:
--
--   1. Pause the per-table policy_compression jobs so no worker races our
--      decompress/recompress. `alter_job(scheduled => false)` only stops future
--      scheduling — a currently-running job keeps going, so step 3 sets a short
--      `lock_timeout` and a chunk that loses that race just fails and is retried
--      on the next run (see re-run semantics below).
--   2. ALTER the four hypertables to the new segmentby *before* any decompress.
--      On TimescaleDB 2.28 this is legal while chunks are still compressed: the
--      setting is per-chunk, so existing chunks keep their old orderby-only
--      layout until they are recompressed in step 3.
--   3. Convert each chunk in its own transaction: decompress → recompress →
--      COMMIT. This bounds the ACCESS EXCLUSIVE lock and the extra on-disk
--      footprint to ONE chunk at a time (~45 MB worst case) rather than holding
--      every chunk's lock and ~+7.3 GB of decompressed data (staging: 19 → 26
--      GB) until a single all-or-nothing commit. Recompressing inline — instead
--      of leaving ~568 chunks for the resumed 12 h policy — removes both the
--      up-to-12 h fully-uncompressed window and the one-shot recompression burst
--      that pushed the 8 GB instance to its memory ceiling.
--   4. Re-enable the policy_compression jobs.
--
-- ── Re-run semantics: this migration MUST converge (not merely "may re-run") ──
-- Step 1 commits on its own, so if a later step fails the policy jobs stay
-- PAUSED and uncompressed chunks accumulate unbounded — the exact bloat this
-- fixes — until the migration runs again. It self-registers last (final INSERT),
-- so an interrupted run stays pending and re-executes on the next deploy until
-- it converges, and the step-3 loop is driven off `segmentby IS NULL` so a
-- re-run only processes the chunks not yet converted. If deploys are halted
-- before convergence, recover manually by re-running this migration, or at
-- minimum re-enable the four jobs so background recompression resumes:
--     SELECT alter_job(job_id, scheduled => true)
--     FROM timescaledb_information.jobs
--     WHERE proc_name = 'policy_compression'
--       AND hypertable_name IN ('allocation_position', 'borrower',
--                               'borrower_collateral', 'protocol_event');
--
-- Each statement is idempotent: alter_job()/ALTER set stable targets (not
-- deltas), the loop skips already-converted chunks, re-enable sets
-- scheduled => true unconditionally, and the final INSERT is ON CONFLICT DO
-- NOTHING.

-- ============================================================================
-- 1. Pause the per-table policy_compression jobs.
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
-- 2. Apply the new segmentby up front. orderby is preserved from 20260410_140000.
-- Legal while chunks are still compressed on TimescaleDB 2.28 (per-chunk
-- setting); existing chunks keep the old layout until recompressed in step 3.
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
-- 3. Convert each still-orderby-only chunk in its own transaction: decompress,
-- recompress with the new segmentby, then COMMIT before moving on. The chunk
-- list is snapshotted up front (segmentby IS NULL); recompressed chunks drop out
-- of that predicate, so a re-run after a partial failure resumes on the rest.
-- COMMIT inside a top-level DO is valid on our execution path: the migrator runs
-- each statement via a no-arg Exec (simple protocol, autocommit), so the DO is
-- top-level and PG >= 11 permits transaction control inside it.
-- ============================================================================
DO $$
DECLARE
    v_chunk regclass;
    v_chunks regclass[];
BEGIN
    SELECT array_agg(chunk ORDER BY hypertable::text, chunk::text)
      INTO v_chunks
      FROM timescaledb_information.chunk_columnstore_settings
     WHERE hypertable::text IN ('allocation_position', 'borrower',
                                'borrower_collateral', 'protocol_event')
       AND segmentby IS NULL;

    IF v_chunks IS NULL THEN
        RAISE NOTICE 'add_columnstore_segmentby: no orderby-only chunks to convert';
        RETURN;
    END IF;

    FOREACH v_chunk IN ARRAY v_chunks
    LOOP
        -- Turn a race with an in-flight policy worker into a fast, retryable
        -- error instead of an indefinite wait on ACCESS EXCLUSIVE.
        SET LOCAL lock_timeout = '5s';
        PERFORM decompress_chunk(v_chunk);
        PERFORM compress_chunk(v_chunk);
        COMMIT;
    END LOOP;
END $$;

-- ============================================================================
-- 4. Re-enable the paused jobs. All existing chunks are recompressed with the
-- new segmentby above, so the policy only maintains future chunks — no burst.
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
VALUES ('20260715_120000_add_columnstore_segmentby.sql')
ON CONFLICT (filename) DO NOTHING;
