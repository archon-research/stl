-- Corrections for position_daily (VEC-636): the writer that appends a retraction, and the view that
-- says where the reading and the spine disagree.

-- Copies the day's winning row at its own spine coordinate and correction_seq + 1, is_retracted TRUE.
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

    -- The insert's coordinate is chosen from this read, which ON CONFLICT cannot guard: two callers
    -- would each copy the other's tombstone at the next correction_seq (db/migrations/AGENTS.md).
    PERFORM pg_advisory_xact_lock(
        hashtext('position_daily:' || encode(p_position_id, 'hex') || ':' || p_as_of_date::text));

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
     LIMIT 1;

    GET DIAGNOSTICS appended = ROW_COUNT;

    -- The crystallizer takes no key lock, so it can commit a higher coordinate between the read
    -- above and this insert, leaving the tombstone inert. Raise rather than report a withdrawal.
    IF EXISTS (SELECT 1 FROM public.position_daily d
                WHERE d.position_id = p_position_id AND d.as_of_date = p_as_of_date) THEN
        RAISE EXCEPTION 'retract_position_daily: % on % is still readable after the retraction; a newer observation outranks it',
            encode(p_position_id, 'hex'), p_as_of_date;
    END IF;
END;
$proc$;

COMMENT ON PROCEDURE retract_position_daily(bytea, date, text, text, bigint) IS '[Operational] Withdraws one (position, UTC date) from position_daily by appending a retraction (VEC-636, ADR-0006 §3): CALL retract_position_daily(position_id, as_of_date, ticket, reason). Copies that day''s winning row at its own spine coordinate and correction_seq + 1 with is_retracted TRUE, so it outranks what it withdraws and never occupies a primary key the spine can reach. Serialises on the key with pg_advisory_xact_lock, because the coordinate it writes is chosen from a prior read. Idempotent: a day whose current answer is already retracted is left alone, returning 0. Raises if the day is still readable afterwards, which means a newer observation landed and the tombstone is inert. Raises if the day has no rows, or if ticket or reason is blank. Needs no version allocator, because correction_seq is this table''s own axis. Use it only for a key that should never have existed; a day whose VALUE is wrong is corrected upstream and supersedes on its own.';

-- Where the reading and the spine disagree, and why. Each row is a case a human or a data-quality
-- job decides about; nothing here fires on its own.
--
-- A function rather than a view because newest-per-day over the spine's local chunks alone reads a
-- PARTIAL history, and only a function can pin enable_tiered_reads.
CREATE OR REPLACE FUNCTION position_daily_anomalies()
    RETURNS TABLE (reason text, position_id bytea, as_of_date date, detail text)
    LANGUAGE sql STABLE
    SET search_path = pg_catalog, public
    SET timescaledb.enable_tiered_reads = 'on'
    SET work_mem = '64MB'
AS $fn$
WITH spine AS (
    SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
           p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date AS as_of_date,
           p.block_number, p.block_version, p.processing_version, p.block_timestamp, p.projection
      FROM public.position_state p
     ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
              p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
)
-- A day the spine has moved past. The writer catches up on its next tick, so a row here that
-- survives a crystallization is the real signal.
SELECT 'stale_day'::text AS reason, d.position_id, d.as_of_date,
       format('reading at (%s,%s,%s,%s), spine winner at (%s,%s,%s,%s)',
              d.block_number, d.block_version, d.processing_version, d.block_timestamp,
              s.block_number, s.block_version, s.processing_version, s.block_timestamp) AS detail
  FROM public.position_daily d
  JOIN spine s ON s.position_id = d.position_id AND s.as_of_date = d.as_of_date
 WHERE (d.block_number, d.block_version, d.processing_version, d.block_timestamp)
       IS DISTINCT FROM (s.block_number, s.block_version, s.processing_version, s.block_timestamp)

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

-- A key that was withdrawn and has come back. The live row must both outrank the tombstone and
-- postdate it, or a retraction that was inert from birth would report as a resurrection.
SELECT DISTINCT 'resurrected'::text, d.position_id, d.as_of_date,
       'a live row written after a retraction now outranks it'
  FROM public.position_daily d
  JOIN public.position_daily_observation r
    ON r.position_id = d.position_id AND r.as_of_date = d.as_of_date AND r.is_retracted
   AND (d.block_number, d.block_version, d.processing_version, d.block_timestamp, d.correction_seq)
       > (r.block_number, r.block_version, r.processing_version, r.block_timestamp, r.correction_seq)
   AND d.created_at > r.created_at

UNION ALL

-- A date the reading holds and the spine has no row for at all. Nothing the writer does can clear
-- it, because the crystallizer only appends: withdrawing it is a decision, not a repair.
SELECT 'orphaned_day'::text, d.position_id, d.as_of_date,
       'the spine holds no observation on this date'
  FROM public.position_daily d
 WHERE NOT EXISTS (SELECT 1 FROM spine s
                    WHERE s.position_id = d.position_id AND s.as_of_date = d.as_of_date)

UNION ALL

-- A spine row re-stamped in place by the superuser recovery path in 20260818_130000. The copy here
-- keeps the old projection and no writer can repair it.
SELECT 'projection_drift'::text, d.position_id, d.as_of_date,
       format('reading says %s, spine says %s', d.projection, s.projection)
  FROM public.position_daily d
  JOIN spine s ON s.position_id = d.position_id AND s.as_of_date = d.as_of_date
 WHERE (d.block_number, d.block_version, d.processing_version, d.block_timestamp)
       IS NOT DISTINCT FROM (s.block_number, s.block_version, s.processing_version, s.block_timestamp)
   AND d.projection IS DISTINCT FROM s.projection;
$fn$;

CREATE OR REPLACE VIEW position_daily_anomaly AS SELECT * FROM public.position_daily_anomalies();

COMMENT ON FUNCTION position_daily_anomalies() IS '[Operational] The rows position_daily_anomaly exposes; pins enable_tiered_reads so the spine is computed over the whole history (VEC-636).';
COMMENT ON VIEW position_daily_anomaly IS '[Operational] Every (position, UTC date) where position_daily does not match the position_state argmax, with a reason (VEC-636). stale_day: the spine has moved past the reading, which the next crystallization fixes -- only a row that SURVIVES a tick is a finding. moved_day: a correction crossed UTC midnight, so the reading''s block now belongs to another date and no ordering rule can withdraw it; retract_position_daily is the instrument. resurrected: a live row outranks a retraction on that key, which is correct when the day gained a real observation and is the signature of a mis-keyed projection still emitting. orphaned_day: the spine has no observation on that date at all, so no tick can clear it and only retract_position_daily can. projection_drift: the spine row was re-stamped in place by the 20260818_130000 recovery path and this copy cannot be repaired. Scans the whole spine, so it is a data-quality read, not a hot path. Empty is the steady state.';

GRANT SELECT ON position_daily_anomaly TO stl_readonly;
GRANT SELECT ON position_daily_anomaly TO stl_readwrite;

INSERT INTO public.migrations (filename) VALUES ('20260916_120000_position_daily_corrections.sql') ON CONFLICT (filename) DO NOTHING;
