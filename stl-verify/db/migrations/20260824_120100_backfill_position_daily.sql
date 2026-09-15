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

-- The daily schedule is the position-daily-crystallizer cronjob (cmd/cronjobs), Temporal-scheduled
-- like every other cronjob here. Its tick is one CALL, and the procedure is idempotent, so a missed
-- or retried tick costs a scan and writes nothing.

INSERT INTO public.migrations (filename) VALUES ('20260824_120100_backfill_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
