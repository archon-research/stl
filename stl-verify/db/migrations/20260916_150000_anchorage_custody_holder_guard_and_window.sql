-- VEC-819: name a malformed holder, and forward p_window, in materialize_anchorage_custody.

-- Dropped first: the two-argument signature would survive CREATE OR REPLACE and make a call that
-- omits p_window ambiguous between the two.
DROP FUNCTION IF EXISTS materialize_anchorage_custody(integer, bigint);

CREATE OR REPLACE FUNCTION materialize_anchorage_custody(p_build_id integer DEFAULT 0,
                                                         p_run_id bigint DEFAULT NULL,
                                                         p_window interval DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    -- Pinned to the materializer's own setting, or the checks below read fewer chunks than the run.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('materialize_position_projection.public.position_anchorage_custody', 0));
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        -- One scan feeds every check: as separate aggregations each planned its own set of chunk scans.
        WITH src AS MATERIALIZED (
            SELECT s.prime_id, s.package_id, s.asset_type, s.custody_type, s.processing_version, s.snapshot_time
            FROM public.anchorage_package_snapshot s
        )
        SELECT msg FROM (
            SELECT format('custody_type %L on %s snapshot(s) is not a known custodian', s.custody_type, count(*)) AS msg
            FROM src s
            LEFT JOIN public.anchorage_known_custody_type k ON k.custody_type = s.custody_type
            WHERE k.custody_type IS NULL
            GROUP BY s.custody_type
            UNION ALL
            -- block_number is the instant in whole seconds, so a pair inside one second collides however
            -- the rows differ. prime_id stands for holder_id here: prime.vault_address is UNIQUE.
            SELECT format('package %L asset %L has %s snapshots within one second (%s)', s.package_id, s.asset_type, count(*), date_trunc('second', s.snapshot_time))
            FROM src s
            GROUP BY s.prime_id, s.package_id, s.asset_type, s.processing_version, date_trunc('second', s.snapshot_time)
            HAVING count(*) > 1
            UNION ALL
            -- ':' is legal in a native instrument_key, so this guards injectivity: two distinct pairs
            -- rendering one key would give two assets one position_id, interleaving under closure.
            SELECT format('%s distinct package/asset pairs render the instrument_key %L', count(*), key)
            FROM (SELECT DISTINCT s.package_id, s.asset_type,
                         'anchorage:' || s.package_id || ':' || s.asset_type AS key
                  FROM src s) p
            GROUP BY key
            HAVING count(*) > 1
            UNION ALL
            -- position_key()'s own blank predicate, so the two cannot disagree: btrim() strips spaces
            -- only, and the 'anchorage:' prefix means a whitespace component never reaches it blank.
            SELECT format('package %L asset %L has a blank or delimiter-bearing identity on %s snapshot(s)', s.package_id, s.asset_type, count(*))
            FROM src s
            WHERE s.package_id ~ '^\s*$' OR s.asset_type ~ '^\s*$'
               OR s.package_id LIKE '%;%' OR s.asset_type LIKE '%;%'
            GROUP BY s.package_id, s.asset_type
            UNION ALL
            -- prime.vault_address carries no length CHECK, and holder_id is its hex: a short one fails
            -- the spine's 40-hex CHECK with a 23514 naming no row. Sky (20260819_140000) guards the
            -- same column the same way.
            SELECT format('prime %L has a vault address of %s bytes, which cannot render the 40-hex holder_id', pr.name, octet_length(pr.vault_address))
            FROM (SELECT DISTINCT s.prime_id FROM src s) d
            JOIN public.prime pr ON pr.id = d.prime_id
            WHERE octet_length(pr.vault_address) <> 20
        ) all_msgs
        ORDER BY msg
        LIMIT 5) worst_five;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_anchorage_custody: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_anchorage_custody'::regclass, p_build_id, p_run_id, p_window);
END
$fn$;

COMMENT ON FUNCTION materialize_anchorage_custody(integer, bigint, interval) IS '[Operational] VEC-408: materialize Anchorage custody packages into position_state via materialize_position_projection(position_anchorage_custody). Refuses to run, naming up to five offenders, when a custody_type is not a known custodian (the view drops it), when one (prime, package, asset, processing_version) carries two snapshots inside one second — including two under different custody types, since custody_type is not part of the observation key and block_number is the instant in whole seconds — when two distinct (package, asset) pairs render one instrument_key, when a package or asset id is blank or contains the '';'' key delimiter, or when a prime''s vault address is not 20 bytes and so cannot render holder_id. Takes the materializer''s own advisory lock key, so two runs of this projection serialise; it does not exclude the source''s writer, which locks a per-row natural key, so a snapshot committed after the check is read by the run unchecked — a collision then raises as the materializer''s generic double-emit rather than by name, and the next run names it. Every check reads one materialised scan of anchorage_package_snapshot. Idempotent; run out of band. Returns rows appended. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2). p_window is forwarded to the materializer, which bounds the batch it reads; against this view it filters rows without pruning chunks.';

INSERT INTO migrations (filename) VALUES ('20260916_150000_anchorage_custody_holder_guard_and_window.sql') ON CONFLICT (filename) DO NOTHING;
