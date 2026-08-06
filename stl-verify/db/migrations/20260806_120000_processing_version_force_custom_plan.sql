-- Pin every processing_version trigger function to custom (per-execution) query
-- plans, so its per-row lookups can prune hypertable chunks.
--
-- THE PROBLEM
-- Each assign_processing_version_<table>() trigger runs two lookups per row: a
-- build_id retry check (has this exact build already written this natural key?)
-- and a MAX(processing_version) probe. PL/pgSQL caches the plans for statements
-- inside a function and, after ~5 executions of the same statement, switches to a
-- GENERIC plan whose parameters are placeholders rather than values.
--
-- A generic plan cannot prune hypertable chunks: with the partition column
-- unknown at planning time, the plan must keep every chunk in the scan. So each
-- inserted row touches ALL chunks of the table rather than the one its timestamp
-- falls in, and per-row cost grows linearly with chunk count — on a hypertable
-- that only ever grows.
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
-- Applied to every such function rather than just the one that surfaced this,
-- because the mechanism is shared by all of them and the next append-heavy
-- backfill would rediscover it on a different table.
--
-- Prefer this over adding a covering index for the build_id check: an index would
-- fix one table, cost write throughput on the hot ingest path, and still leave a
-- generic plan unable to prune.

DO $$
DECLARE
    fn      record;
    updated int := 0;
BEGIN
    -- Driven off the catalogue, not a hand-maintained list, so a function this
    -- migration has never heard of cannot be silently skipped.
    FOR fn IN
        SELECT p.oid::regprocedure AS sig
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname LIKE 'assign\_processing\_version\_%'
    LOOP
        EXECUTE format('ALTER FUNCTION %s SET plan_cache_mode = %L',
                       fn.sig, 'force_custom_plan');
        updated := updated + 1;
    END LOOP;

    -- 36 existed when this was written. A floor rather than an equality so a
    -- later migration adding a versioned table does not break this one, while a
    -- renaming that silently matches nothing still fails loudly instead of
    -- leaving every trigger on generic plans.
    IF updated < 36 THEN
        RAISE EXCEPTION
            'expected at least 36 assign_processing_version_* functions, configured %; '
            'has the naming convention changed? every versioned hypertable needs '
            'plan_cache_mode = force_custom_plan or its per-row trigger lookups '
            'stop pruning chunks', updated;
    END IF;

    RAISE NOTICE 'set plan_cache_mode = force_custom_plan on % processing_version functions', updated;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260806_120000_processing_version_force_custom_plan.sql')
ON CONFLICT (filename) DO NOTHING;
