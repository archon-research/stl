-- Initial backfill of position_daily (VEC-636), and the statement an operator re-runs to converge it.
-- Separate from 20260824_120000 as 20260819_150100 established: the migrator runs a whole file in one
-- transaction, so together CREATE TRIGGER's lock on position_state was held for this full-spine scan.
-- Split, this file holds ACCESS SHARE on position_state, which conflicts with nothing ingest does, and
-- the trigger is live before it starts, so it only fills what preceded the trigger.
CALL rebuild_position_daily();

-- Built AFTER the backfill: created first, every backfilled row pays a random btree insert with its own
-- WAL instead of one bulk build. The holder index serves the filter the PK cannot, as_of_date trailing so
-- a holder's series is ordered by it; the date index answers the whole book on one date, which is the
-- query this grain exists for and the one the PK cannot serve -- chunk exclusion answered it while this
-- table was still a hypertable.
CREATE INDEX IF NOT EXISTS position_daily_holder_idx ON public.position_daily (holder_id, as_of_date);
CREATE INDEX IF NOT EXISTS position_daily_as_of_date_idx ON public.position_daily (as_of_date);

ANALYZE public.position_daily;

INSERT INTO public.migrations (filename) VALUES ('20260824_120100_backfill_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
