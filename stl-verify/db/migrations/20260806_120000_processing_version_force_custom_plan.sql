-- Pin offchain_token_price's processing_version trigger to custom (per-execution)
-- query plans, so its per-row lookups can prune hypertable chunks.
--
-- THE PROBLEM
-- assign_processing_version_offchain_token_price() runs two lookups per row: a
-- build_id retry check (has this exact build already written this natural key?)
-- and a MAX(processing_version) probe. PL/pgSQL caches the plans for statements
-- inside a function and, after ~5 executions of the same statement, switches to a
-- GENERIC plan whose parameters are placeholders rather than values.
--
-- A generic plan cannot prune hypertable chunks: with the partition column
-- unknown at planning time, the plan keeps every chunk and re-derives the
-- surviving one at each execution (visible as "Chunks excluded during startup: N"
-- under a Custom Scan (ChunkAppend)). Per-row cost therefore grows with chunk
-- count — on a hypertable that only ever gains chunks.
--
-- Measured on timescaledb 2.25.1-pg17 with real migrations and the production
-- UpsertPrices statement, one 721-row batch against 2,071 chunks:
--
--     production (as-is)     4,410 ms
--     triggers disabled          8-19 ms
--     force_custom_plan        148 ms   <- and flat in chunk count
--
-- ~92% of the cost was the build_id retry check, which no covering index serves.
-- Note the trigger is BEFORE INSERT, so it also fires for rows that ON CONFLICT
-- later discards: a re-run over an already-filled range pays the same floor and
-- is NOT free.
--
-- THE FIX
-- force_custom_plan makes PL/pgSQL re-plan with real parameter values every
-- execution. Chunk pruning returns, cost stops scaling with chunk count, and the
-- extra planning is paid back many times over. Semantics are untouched: identical
-- rows, identical versions, identical locking — only the plan changes.
--
-- Preferred over adding a covering index for the build_id check: an index would
-- fix one table, cost write throughput on the hot ingest path, and still leave a
-- generic plan unable to prune.
--
-- SCOPE: DELIBERATELY ONE TABLE
-- The mechanism is shared by all ~36 assign_processing_version_* functions, and
-- every versioned hypertable will eventually want this. It is applied here to
-- offchain_token_price alone — the table where the cost was measured and where a
-- historical backfill makes it acute — rather than to all of them at once, to
-- keep the blast radius of a plan-behaviour change to the one ingest path that
-- has been exercised end to end. Convert the others per table, each with its own
-- measurement, and add each to pinnedCustomPlanFunctions in
-- db/migrator/processing_version_indexes_integration_test.go so a revert is
-- caught.

ALTER FUNCTION assign_processing_version_offchain_token_price()
    SET plan_cache_mode = 'force_custom_plan';

DO $$
BEGIN
    -- Fail loudly rather than leaving the trigger on generic plans if the function
    -- was renamed out from under this migration.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname = 'assign_processing_version_offchain_token_price'
          AND 'plan_cache_mode=force_custom_plan' = ANY(p.proconfig)
    ) THEN
        RAISE EXCEPTION
            'assign_processing_version_offchain_token_price() is not pinned to '
            'force_custom_plan; without it every inserted row re-derives chunk '
            'exclusion across the whole hypertable';
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260806_120000_processing_version_force_custom_plan.sql')
ON CONFLICT (filename) DO NOTHING;
