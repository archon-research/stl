-- Initial crystallization of position_daily_observation (VEC-636), and the schedule that keeps it
-- current. Separate from 20260824_120000 as 20260819_150100 established: the migrator runs a whole
-- file in one transaction, and this one scans the full spine.
CALL crystallize_position_daily();

-- Built after the first pass, so the rows arrive as one bulk build rather than random btree inserts.
-- Both serve reads the PK cannot: a holder's series, and the whole book on one date.
CREATE INDEX IF NOT EXISTS position_daily_observation_holder_idx
    ON public.position_daily_observation (holder_id, as_of_date);
CREATE INDEX IF NOT EXISTS position_daily_observation_as_of_date_idx
    ON public.position_daily_observation (as_of_date, position_id);

ANALYZE public.position_daily_observation;

-- Daily, through TimescaleDB's job runner, which already runs this database's policies. The procedure
-- is idempotent, so a missed or repeated run costs a scan and writes nothing.
CREATE OR REPLACE PROCEDURE crystallize_position_daily_job(job_id integer, config jsonb)
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public
AS $$
BEGIN
    CALL public.crystallize_position_daily(COALESCE((config ->> 'settle_after')::interval, interval '1 hour'));
END;
$$;

COMMENT ON PROCEDURE crystallize_position_daily_job(integer, jsonb) IS '[Operational] TimescaleDB job entry point for crystallize_position_daily (VEC-636). Takes settle_after from the job config, so the settling window is retunable with alter_job rather than a migration.';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE proc_name = 'crystallize_position_daily_job') THEN
        PERFORM add_job('crystallize_position_daily_job', INTERVAL '1 day',
                        config => '{"settle_after": "1 hour"}'::jsonb);
    END IF;
END $$;

INSERT INTO public.migrations (filename) VALUES ('20260824_120100_backfill_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
