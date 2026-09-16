-- Corrections for position_daily (VEC-636): the writer that appends a retraction, and the view that
-- says where the reading and the spine disagree. Separate from 20260824_120000 because that file
-- creates the table and this one is about operating it.

ALTER TABLE position_daily_observation ADD COLUMN IF NOT EXISTS retraction_ticket text;
ALTER TABLE position_daily_observation ADD COLUMN IF NOT EXISTS retraction_reason text;

COMMENT ON COLUMN position_daily_observation.retraction_ticket IS 'Roles: Audit. The ticket a retraction was written under; NULL on every row that is not a retraction. Required by retract_position_daily, so no key is withdrawn without a record of who decided to.';
COMMENT ON COLUMN position_daily_observation.retraction_reason IS 'Roles: Audit. Why a retraction was written; NULL on every row that is not a retraction.';

-- A retraction must carry its attribution and nothing else may: the pair is meaningless on a
-- crystallized row, and a tombstone without it cannot be audited.
ALTER TABLE position_daily_observation
    ADD CONSTRAINT position_daily_observation_retraction_attribution_chk
    CHECK ((is_retracted IS TRUE) = (retraction_ticket IS NOT NULL)
       AND (retraction_ticket IS NULL) = (retraction_reason IS NULL));

-- The retraction writer. Callers name a (position, date) and a ticket; the procedure copies that
-- day's winning row at its own spine coordinate and correction_seq + 1, with is_retracted TRUE.
--
-- It needs no version allocator: correction_seq belongs to this table, so unlike ARCT-470's
-- processing_version encoding there is no shared namespace to serialise against (ARCT-428).
CREATE OR REPLACE PROCEDURE retract_position_daily(
    p_position_id bytea,
    p_as_of_date  date,
    p_ticket      text,
    p_reason      text,
    INOUT appended bigint DEFAULT NULL)
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public
    SET lock_timeout = '10s'
AS $proc$
DECLARE
    winner_retracted boolean;
BEGIN
    IF p_ticket IS NULL OR btrim(p_ticket) = '' THEN
        RAISE EXCEPTION 'retract_position_daily: a ticket is required; an unattributed retraction is not auditable';
    END IF;
    IF p_reason IS NULL OR btrim(p_reason) = '' THEN
        RAISE EXCEPTION 'retract_position_daily: a reason is required';
    END IF;

    -- The day's current answer. A retraction copies the row it withdraws, and after one has been
    -- written that row IS the tombstone -- so without this guard a retry copies the tombstone at the
    -- next correction_seq and the conflict clause never fires.
    SELECT d.is_retracted INTO winner_retracted
      FROM public.position_daily_observation d
     WHERE d.position_id = p_position_id AND d.as_of_date = p_as_of_date
     ORDER BY d.block_number DESC, d.block_version DESC, d.processing_version DESC,
              d.block_timestamp DESC, d.correction_seq DESC
     LIMIT 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'retract_position_daily: no rows for position % on %', encode(p_position_id, 'hex'), p_as_of_date;
    END IF;

    IF winner_retracted IS TRUE THEN
        appended := 0;
        RETURN;
    END IF;

    INSERT INTO public.position_daily_observation
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         run_id, deal_type, is_retracted, correction_seq, retraction_ticket, retraction_reason)
    SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
           d.quantity, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
           d.projection, d.build_id, d.run_id, d.deal_type, TRUE, d.correction_seq + 1,
           p_ticket, p_reason
      FROM public.position_daily_observation d
     WHERE d.position_id = p_position_id AND d.as_of_date = p_as_of_date
     ORDER BY d.block_number DESC, d.block_version DESC, d.processing_version DESC,
              d.block_timestamp DESC, d.correction_seq DESC
     LIMIT 1
    -- A re-run of the same correction is a retry, not a second withdrawal.
    ON CONFLICT ON CONSTRAINT position_daily_observation_pkey DO NOTHING;

    GET DIAGNOSTICS appended = ROW_COUNT;
END;
$proc$;

COMMENT ON PROCEDURE retract_position_daily(bytea, date, text, text, bigint) IS '[Operational] Withdraws one (position, UTC date) from position_daily by appending a retraction (VEC-636, ADR-0006 §3): CALL retract_position_daily(position_id, as_of_date, ticket, reason). Copies that day''s winning row at its own spine coordinate and correction_seq + 1 with is_retracted TRUE, so it outranks what it withdraws and never occupies a primary key the spine can reach. Idempotent: a day whose current answer is already retracted is left alone, returning 0. Raises if the day has no rows, or if ticket or reason is blank. Needs no version allocator, because correction_seq is this table''s own axis. Use it only for a key that should never have existed; a day whose VALUE is wrong is corrected upstream and supersedes on its own.';

-- position_daily_as_of RETURNS SETOF position_daily_observation, so its row type was fixed when the
-- table had fewer columns. Both reads are rebuilt here or a consumer of the view sees a narrower
-- shape than the table (TestPositionDailySchema/view_exposes_every_table_column).
DROP VIEW IF EXISTS position_daily;

CREATE OR REPLACE FUNCTION position_daily_as_of(seen_before timestamptz)
    RETURNS SETOF position_daily_observation
    LANGUAGE sql STABLE
AS $fn$
    SELECT w.* FROM (
        SELECT DISTINCT ON (d.position_id, d.as_of_date, d.holder_id) d.*
          FROM public.position_daily_observation d
         WHERE d.created_at <= public.position_daily_as_of_bound(seen_before)
         ORDER BY d.position_id, d.as_of_date, d.holder_id,
                  d.block_number DESC, d.block_version DESC, d.processing_version DESC,
                  d.block_timestamp DESC, d.correction_seq DESC
    ) w
    WHERE w.is_retracted IS NOT TRUE;
$fn$;

CREATE VIEW position_daily AS
    SELECT * FROM public.position_daily_as_of('infinity'::timestamptz);

-- DROP took the COMMENT with it, so it is restated rather than inherited.
COMMENT ON VIEW position_daily IS '[Operational] What each position held on each observed UTC date: one row per (position, UTC date), that day''s winning observation (VEC-636). Equal to the newest position_state observation per (position, UTC date) across settled days, except where a retraction withdraws the key. Only OBSERVED dates get a row -- no carry-forward, so a query for one date may correctly return nothing, and the current UTC day is absent until it is crystallized. A retracted key is absent entirely. Not reproducible across corrections; pin a time with position_daily_as_of(T) for that. position_daily_anomaly lists where this disagrees with the spine, and why.';

GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT ON position_daily TO stl_readwrite;

-- Where the reading and the spine disagree, and why. Nothing here fires on its own: these are the
-- cases a human or a data-quality job has to decide about, and before this view each of them was a
-- paragraph in a PR rather than something you could query.
CREATE OR REPLACE VIEW position_daily_anomaly AS
WITH spine AS (
    SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
           p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date AS as_of_date,
           p.block_number, p.block_version, p.processing_version, p.projection
      FROM public.position_state p
     ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
              p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
)
-- A day the spine has moved past. The writer catches up on its next tick, so a row here that
-- survives a crystallization is the real signal.
SELECT 'stale_day'::text AS reason, d.position_id, d.as_of_date,
       format('reading at (%s,%s,%s), spine winner at (%s,%s,%s)',
              d.block_number, d.block_version, d.processing_version,
              s.block_number, s.block_version, s.processing_version) AS detail
  FROM public.position_daily d
  JOIN spine s ON s.position_id = d.position_id AND s.as_of_date = d.as_of_date
 WHERE (d.block_number, d.block_version, d.processing_version)
       IS DISTINCT FROM (s.block_number, s.block_version, s.processing_version)

UNION ALL

-- The midnight case: the same block reprocessed at a higher version, landing on another date. No
-- ordering rule reaches it, because the two rows are in different (position, date) groups.
SELECT DISTINCT 'moved_day'::text, d.position_id, d.as_of_date,
       format('block %s stranded on %s: reprocessed at version %s, now dated %s',
              d.block_number, d.as_of_date, p.processing_version,
              (p.block_timestamp AT TIME ZONE 'utc')::date)
  FROM public.position_daily d
  JOIN public.position_state p
    ON p.position_id = d.position_id AND p.block_number = d.block_number
   AND p.block_version = d.block_version AND p.processing_version > d.processing_version
   AND (p.block_timestamp AT TIME ZONE 'utc')::date <> d.as_of_date

UNION ALL

-- A key that was withdrawn and has come back, because a later observation outranked the tombstone.
-- Correct when the day genuinely moved on, and the signature of a mis-keyed projection still emitting.
SELECT DISTINCT 'resurrected'::text, d.position_id, d.as_of_date,
       'a live row now outranks a retraction on this key'
  FROM public.position_daily d
  JOIN public.position_daily_observation r
    ON r.position_id = d.position_id AND r.as_of_date = d.as_of_date AND r.is_retracted

UNION ALL

-- A spine row re-stamped in place by the superuser recovery path in 20260818_130000. The copy here
-- keeps the old projection and no writer can repair it.
SELECT 'projection_drift'::text, d.position_id, d.as_of_date,
       format('reading says %s, spine says %s', d.projection, s.projection)
  FROM public.position_daily d
  JOIN spine s ON s.position_id = d.position_id AND s.as_of_date = d.as_of_date
 WHERE (d.block_number, d.block_version, d.processing_version)
       IS NOT DISTINCT FROM (s.block_number, s.block_version, s.processing_version)
   AND d.projection IS DISTINCT FROM s.projection;

COMMENT ON VIEW position_daily_anomaly IS '[Operational] Every (position, UTC date) where position_daily does not match the position_state argmax, with a reason (VEC-636). stale_day: the spine has moved past the reading, which the next crystallization fixes -- only a row that SURVIVES a tick is a finding. moved_day: a correction crossed UTC midnight, so the reading''s block now belongs to another date and no ordering rule can withdraw it; retract_position_daily is the instrument. resurrected: a live row outranks a retraction on that key, which is correct when the day gained a real observation and is the signature of a mis-keyed projection still emitting. projection_drift: the spine row was re-stamped in place by the 20260818_130000 recovery path and this copy cannot be repaired. Scans the whole spine, so it is a data-quality read, not a hot path. Empty is the steady state.';

GRANT SELECT ON position_daily_anomaly TO stl_readonly;
GRANT SELECT ON position_daily_anomaly TO stl_readwrite;

INSERT INTO public.migrations (filename) VALUES ('20260916_120000_position_daily_corrections.sql') ON CONFLICT (filename) DO NOTHING;
