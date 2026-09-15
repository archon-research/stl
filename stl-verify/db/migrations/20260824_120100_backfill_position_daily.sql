-- Initial backfill of position_daily (VEC-636), and the statement an operator re-runs to repair it.
-- Separate from 20260824_120000 as 20260819_150100 established: the migrator runs a whole file in one
-- transaction, so together CREATE TRIGGER's lock on position_state was held for this full-spine scan.
CALL rebuild_position_daily();

-- Built AFTER the backfill, so the rows arrive as one bulk build rather than random btree inserts.
-- The holder index serves a holder's series, as_of_date trailing so it comes out in date order. The
-- date index serves the whole book on one date, position_id trailing so the newest-per-position pick
-- reads each position's rows together.
CREATE INDEX IF NOT EXISTS position_daily_holder_idx ON public.position_daily (holder_id, as_of_date);
CREATE INDEX IF NOT EXISTS position_daily_as_of_date_idx ON public.position_daily (as_of_date, position_id);

ANALYZE public.position_daily;

INSERT INTO public.migrations (filename) VALUES ('20260824_120100_backfill_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
