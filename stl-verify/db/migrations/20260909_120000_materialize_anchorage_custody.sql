-- VEC-408: project Anchorage custody packages onto the position spine. A custodied asset in a package
-- held by a prime is a position: instrument_key = 'anchorage:' package_id ':' asset_type, holder = the
-- prime's vault address, quantity = that asset's quantity as the custodian reports it.

-- 20260818_130000's scope note said a snapshot-keyed source was unsupported pending a deliberate
-- decision; 20260818_140000 made it by enforcing the encoding below, and this is the first source to
-- use it. Treat that note as superseded, not as a rule this file breaks.

-- Off-chain: chain_id and protocol_id are NULL (a protocol row needs a chain address) and block_number
-- is the snapshot instant in whole epoch seconds, which the materializer enforces. Derived from
-- block_timestamp, so a higher block cannot carry an earlier instant.

-- The custodians this projection understands. Read by the view and anti-joined by the wrapper's
-- pre-check, so a new custodian cannot be added to one and forgotten in the other.
CREATE OR REPLACE VIEW anchorage_known_custody_type AS
SELECT * FROM (VALUES ('AnchorageCustody')) AS m(custody_type);

COMMENT ON VIEW anchorage_known_custody_type IS '[Operational] VEC-408: the anchorage_package_snapshot.custody_type values position_anchorage_custody understands. The view joins it and materialize_anchorage_custody()''s pre-check anti-joins it, so the two cannot disagree. A new custodian is added here, in a new migration, after checking its pledge semantics match.';

CREATE OR REPLACE VIEW position_anchorage_custody AS
SELECT NULL::integer                                              AS chain_id,
       NULL::bigint                                               AS protocol_id,
       'anchorage:' || s.package_id || ':' || s.asset_type        AS instrument_key,
       encode(pr.vault_address, 'hex')                            AS holder_id,
       s.asset_quantity                                           AS quantity,
       -- Every package in this source is pledged: pledgor_id, secured_party_id and current_ltv are all
       -- NOT NULL, so an unencumbered package cannot be expressed and CUSTODY would misreport pledged
       -- collateral as free. An unpledged package needs a source change, not a CASE here.
       'CUSTODY_COLLATERAL'::text                                 AS deal_type,
       floor(extract(epoch FROM s.snapshot_time))::bigint         AS block_number,
       -- No reorg axis off-chain, unlike processing_version, which is the source's and is propagated.
       0                                                          AS block_version,
       s.processing_version,
       s.snapshot_time                                            AS block_timestamp
FROM anchorage_package_snapshot s
JOIN prime pr ON pr.id = s.prime_id
-- An INNER join: an unknown custodian emits nothing here and the wrapper names it, rather than being
-- projected under pledge semantics that may not hold for it.
JOIN anchorage_known_custody_type k ON k.custody_type = s.custody_type;

COMMENT ON VIEW position_anchorage_custody IS '[Operational] VEC-408 projection: Anchorage custody packages as off-chain position rows, one per (prime, package, asset_type, custody_type, snapshot_time, processing_version) — the source''s own grain. instrument_key = ''anchorage:'' package_id '':'' asset_type: provider-prefixed because a package id is unique only within Anchorage and, with chain_id and protocol_id NULL, nothing else in the hashed id separates providers; asset-scoped because the source carries one row per asset and asset_type names the units of quantity. holder_id = the prime''s vault address. quantity = asset_quantity as the custodian reports it, in that asset''s own units. deal_type is CUSTODY_COLLATERAL for every row: the source''s pledgor_id, secured_party_id and current_ltv are NOT NULL, so it cannot express an unencumbered package. Off-chain: chain_id and protocol_id NULL, block_number = the instant in whole epoch seconds, not a block on any chain. active and state are deliberately not read: closure fires only on a reported asset_quantity of 0, so a package that STOPS being reported stays open at its last quantity — staleness is answered from position_projection_run, not from this view. Emits the shared position_state column contract; closure is applied by materialize_position_projection().';

-- Names every input the view cannot place, then delegates; each case would otherwise be dropped or
-- collide silently. It takes the materializer's OWN lock key first, so a row inserted after the check
-- cannot slip past, and matches its enable_tiered_reads, or it would read fewer chunks than the view.
CREATE OR REPLACE FUNCTION materialize_anchorage_custody(p_build_id integer DEFAULT 0) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('materialize_position_projection.public.position_anchorage_custody', 0));
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT msg FROM (
            SELECT format('custody_type %L on %s snapshot(s) is not a known custodian', s.custody_type, count(*)) AS msg
            FROM public.anchorage_package_snapshot s
            LEFT JOIN public.anchorage_known_custody_type k ON k.custody_type = s.custody_type
            WHERE k.custody_type IS NULL
            GROUP BY s.custody_type
            UNION ALL
            -- block_number is the instant in whole seconds, so two snapshots of one instrument inside
            -- one second collapse onto one observation key.
            SELECT format('package %L asset %L has %s snapshots within one second (%s)', s.package_id, s.asset_type, count(*), date_trunc('second', s.snapshot_time))
            FROM public.anchorage_package_snapshot s
            GROUP BY s.prime_id, s.package_id, s.asset_type, s.custody_type, s.processing_version, date_trunc('second', s.snapshot_time)
            HAVING count(*) > 1
            UNION ALL
            -- deal_type is not part of the identity, so two custody types for one (package, asset) at
            -- one instant would collide on the observation key.
            SELECT format('package %L asset %L carries %s custody types at one snapshot', s.package_id, s.asset_type, count(DISTINCT s.custody_type))
            FROM public.anchorage_package_snapshot s
            GROUP BY s.prime_id, s.package_id, s.asset_type, s.snapshot_time, s.processing_version
            HAVING count(DISTINCT s.custody_type) > 1
            UNION ALL
            -- ':' is legal in a native instrument_key (VEC-400 names Sky's registry:ilk), so the guard
            -- is injectivity, not the character: two distinct pairs rendering one key would give two
            -- assets one position_id, their histories interleaving under closure.
            SELECT format('%s distinct package/asset pairs render the instrument_key %L', count(*), key)
            FROM (SELECT DISTINCT s.package_id, s.asset_type,
                         'anchorage:' || s.package_id || ':' || s.asset_type AS key
                  FROM public.anchorage_package_snapshot s) p
            GROUP BY key
            HAVING count(*) > 1
            UNION ALL
            -- position_key() would raise on these without naming the row they came from.
            SELECT format('package %L asset %L has a blank or delimiter-bearing identity on %s snapshot(s)', s.package_id, s.asset_type, count(*))
            FROM public.anchorage_package_snapshot s
            WHERE btrim(s.package_id) = '' OR btrim(s.asset_type) = ''
               OR s.package_id LIKE '%;%' OR s.asset_type LIKE '%;%'
            GROUP BY s.package_id, s.asset_type
        ) all_msgs
        ORDER BY msg
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_anchorage_custody: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    -- This projection never closes from absence, so a package that stops being reported stays open
    -- at its last quantity. One package going is a delisting; several going at once is the feed
    -- failing, and carrying stale pledged collateral forward is worse than stopping.
    WITH inst AS (
        SELECT prime_id, snapshot_time,
               row_number() OVER (PARTITION BY prime_id ORDER BY snapshot_time DESC) AS rn
          FROM (SELECT DISTINCT prime_id, snapshot_time FROM public.anchorage_package_snapshot) d
    )
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT format('prime %s stopped reporting %s live package(s) at %s: %s',
                      o.prime_id, count(DISTINCT s.package_id), n.snapshot_time,
                      string_agg(DISTINCT s.package_id, ',')) AS msg
          FROM inst o
          JOIN inst n ON n.prime_id = o.prime_id AND n.rn = 1
          JOIN public.anchorage_package_snapshot s
            ON s.prime_id = o.prime_id AND s.snapshot_time = o.snapshot_time AND s.asset_quantity > 0
         WHERE o.rn = 2
           AND NOT EXISTS (SELECT 1 FROM public.anchorage_package_snapshot q
                            WHERE q.prime_id = o.prime_id AND q.snapshot_time = n.snapshot_time
                              AND q.package_id = s.package_id)
         GROUP BY o.prime_id, n.snapshot_time
        HAVING count(DISTINCT s.package_id) > 1
    ) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_anchorage_custody: several live packages stopped being reported at once, which is a feed failure rather than a delisting; refusing to run: %', v_bad;
    END IF;

    RETURN public.materialize_position_projection('public.position_anchorage_custody'::regclass, p_build_id);
END
$fn$;

COMMENT ON FUNCTION materialize_anchorage_custody(integer) IS '[Operational] VEC-408: materialize Anchorage custody packages into position_state via materialize_position_projection(position_anchorage_custody). Refuses to run, naming up to five offenders, when a custody_type is not a known custodian (the view drops it), when one instrument has two snapshots inside a second or one (package, asset) carries several custody types (either collides on the observation key, since block_number is the instant in whole seconds), or when a package or asset id is blank or contains the '';'' key delimiter. Takes the materializer''s own advisory lock first, so a row inserted after the check cannot slip past it. Idempotent; run out of band. Returns rows appended.';

GRANT SELECT ON anchorage_known_custody_type, position_anchorage_custody TO stl_readonly;
GRANT SELECT ON anchorage_known_custody_type, position_anchorage_custody TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260909_120000_materialize_anchorage_custody.sql') ON CONFLICT (filename) DO NOTHING;
