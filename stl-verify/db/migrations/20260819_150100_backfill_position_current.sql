-- Initial backfill of position_current (VEC-409). Separate from 20260819_150000 so CREATE TRIGGER's
-- lock is not held across this full-history scan; that file commits first, so the trigger is live
-- throughout and this only fills what preceded it.
CALL rebuild_position_current();

ANALYZE public.position_current;

INSERT INTO public.migrations (filename) VALUES ('20260819_150100_backfill_position_current.sql') ON CONFLICT (filename) DO NOTHING;
