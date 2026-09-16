-- VEC-408: project Anchorage custody packages onto the position spine.

CREATE OR REPLACE VIEW anchorage_known_custody_type AS
SELECT * FROM (VALUES ('AnchorageCustody')) AS m(custody_type);

COMMENT ON VIEW anchorage_known_custody_type IS '[Operational] VEC-408: the anchorage_package_snapshot.custody_type values position_anchorage_custody understands. The view joins it and materialize_anchorage_custody()''s pre-check anti-joins it, so the two cannot disagree. A new custodian is added here, in a new migration, after checking its pledge semantics match.';

CREATE OR REPLACE VIEW position_anchorage_custody AS
SELECT NULL::integer                                              AS chain_id,
       NULL::bigint                                               AS protocol_id,
       'anchorage:' || s.package_id || ':' || s.asset_type        AS instrument_key,
       encode(pr.vault_address, 'hex')                            AS holder_id,
       s.asset_quantity                                           AS quantity,
       'CUSTODY_COLLATERAL'::text                                 AS deal_type,
       floor(extract(epoch FROM s.snapshot_time))::bigint         AS block_number,
       -- No reorg axis off-chain; processing_version is the source's and is propagated.
       0                                                          AS block_version,
       s.processing_version,
       s.snapshot_time                                            AS block_timestamp
FROM anchorage_package_snapshot s
JOIN prime pr ON pr.id = s.prime_id
-- INNER: an unknown custodian emits nothing here, and the wrapper refuses the run naming it.
JOIN anchorage_known_custody_type k ON k.custody_type = s.custody_type;

COMMENT ON VIEW position_anchorage_custody IS '[Operational] VEC-408 projection: Anchorage custody packages as off-chain position rows, one per (prime, package, asset_type, custody_type, snapshot_time, processing_version) — the source''s own grain. instrument_key = ''anchorage:'' package_id '':'' asset_type: provider-prefixed because a package id is unique only within Anchorage and, with chain_id and protocol_id NULL, nothing else in the hashed id separates providers; asset-scoped because the source carries one row per asset and asset_type names the units of quantity. holder_id = the prime''s vault address, which is what separates two primes holding one package. quantity = asset_quantity as the custodian reports it: DECIMAL-NORMALISED into whole units of asset_type (BTC, not satoshi), never a raw native-decimal integer. deal_type is CUSTODY_COLLATERAL for every row: the source''s pledgor_id, secured_party_id and current_ltv are NOT NULL, so it cannot express an unencumbered package, and CUSTODY — which ref_deal_type defines as unencumbered — would report pledged collateral as free. Off-chain: chain_id and protocol_id NULL, block_number = the instant in whole epoch seconds, not a block on any chain; because it is derived from block_timestamp, a higher block cannot carry an earlier instant. active and state are deliberately not read: closure fires only on a reported asset_quantity of 0, so a package that STOPS being reported stays open at its last quantity — staleness is answered from position_projection_run. Emits the shared position_state column contract; closure is applied by materialize_position_projection().';

-- Dropped first: the one-argument signature would survive CREATE OR REPLACE and make a call that
-- omits p_run_id ambiguous between the two.
DROP FUNCTION IF EXISTS materialize_anchorage_custody(integer);

CREATE OR REPLACE FUNCTION materialize_anchorage_custody(p_build_id integer DEFAULT 0,
                                                         p_run_id bigint DEFAULT NULL) RETURNS bigint
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
        ) all_msgs
        ORDER BY msg
        LIMIT 5) worst_five;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_anchorage_custody: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_anchorage_custody'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_anchorage_custody(integer, bigint) IS '[Operational] VEC-408: materialize Anchorage custody packages into position_state via materialize_position_projection(position_anchorage_custody). Refuses to run, naming up to five offenders, when a custody_type is not a known custodian (the view drops it), when one (prime, package, asset, processing_version) carries two snapshots inside one second — including two under different custody types, since custody_type is not part of the observation key and block_number is the instant in whole seconds — when two distinct (package, asset) pairs render one instrument_key, or when a package or asset id is blank or contains the '';'' key delimiter. Takes the materializer''s own advisory lock key, so two runs of this projection serialise; it does not exclude the source''s writer, which locks a per-row natural key, so a snapshot committed after the check is read by the run unchecked — a collision then raises as the materializer''s generic double-emit rather than by name, and the next run names it. Every check reads one materialised scan of anchorage_package_snapshot. Idempotent; run out of band. Returns rows appended. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

GRANT SELECT ON anchorage_known_custody_type, position_anchorage_custody TO stl_readonly;
GRANT SELECT ON anchorage_known_custody_type, position_anchorage_custody TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260909_120000_materialize_anchorage_custody.sql') ON CONFLICT (filename) DO NOTHING;
