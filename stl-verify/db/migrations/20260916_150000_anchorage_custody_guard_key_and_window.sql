-- VEC-819: name a malformed holder, and forward p_window, in materialize_anchorage_custody.
-- VEC-809: derive the instrument_key once, so the view and the guard cannot drift apart.
-- VEC-811: state asset_quantity's unit and scale on the source column.

-- VEC-811. The projection's COMMENT says this on the read side; a reader of the source table had
-- nothing to go on, and position_state.quantity is deliberately NOT normalised across protocols.
COMMENT ON COLUMN anchorage_package_snapshot.asset_quantity IS '[Timeseries] Roles: none. Quantity of asset_type held in the package, DECIMAL-NORMALISED into whole units of that asset (BTC, not satoshi) as the custodian reports it — never a raw native-decimal integer; staging carries values such as 2833.56758957.';

-- VEC-809. The key was spelled once in the view and again in the wrapper's injectivity guard, which
-- re-derived it from the source. Changing the view alone left the guard validating the old shape: it
-- kept passing and protected nothing, with no error. One definition, called by both.
-- No SET search_path, deliberately: a SQL function carrying a SET clause cannot be inlined, and this
-- body names no object at all -- two literals and its own parameters -- so there is nothing to
-- qualify. Measured: with the SET it plans as a per-row anchorage_instrument_key() call; without it
-- the plan carries the concatenation itself.
CREATE OR REPLACE FUNCTION anchorage_instrument_key(p_package_id text, p_asset_type text)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE AS $fn$
SELECT 'anchorage:' || p_package_id || ':' || p_asset_type
$fn$;

COMMENT ON FUNCTION anchorage_instrument_key(text, text) IS '[Operational] VEC-809: the single definition of position_anchorage_custody''s instrument_key. Provider-prefixed because a package id is unique only within Anchorage and, with chain_id and protocol_id NULL, nothing else in the hashed id separates providers; asset-scoped because the source carries one row per asset. Both the view and materialize_anchorage_custody()''s injectivity pre-check call this, so neither can validate a shape the other no longer emits. A plain SQL IMMUTABLE function, so the planner inlines it and the guard keeps its single materialised scan.';

CREATE OR REPLACE VIEW position_anchorage_custody AS
SELECT NULL::integer                                              AS chain_id,
       NULL::bigint                                               AS protocol_id,
       anchorage_instrument_key(s.package_id, s.asset_type)       AS instrument_key,
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

COMMENT ON VIEW position_anchorage_custody IS '[Operational] VEC-408 projection: Anchorage custody packages as off-chain position rows, one per (prime, package, asset_type, custody_type, snapshot_time, processing_version) — the source''s own grain. instrument_key is built by anchorage_instrument_key(package_id, asset_type), which the wrapper''s injectivity guard also calls so the two cannot drift (VEC-809). holder_id = the prime''s vault address, which is what separates two primes holding one package. quantity = asset_quantity as the custodian reports it: DECIMAL-NORMALISED into whole units of asset_type (BTC, not satoshi), never a raw native-decimal integer. deal_type is CUSTODY_COLLATERAL for every row: the source''s pledgor_id, secured_party_id and current_ltv are NOT NULL, so it cannot express an unencumbered package, and CUSTODY — which ref_deal_type defines as unencumbered — would report pledged collateral as free. Off-chain: chain_id and protocol_id NULL, block_number = the instant in whole epoch seconds, not a block on any chain; because it is derived from block_timestamp, a higher block cannot carry an earlier instant. active and state are deliberately not read: closure fires only on a reported asset_quantity of 0, so a package that STOPS being reported stays open at its last quantity — staleness is answered from position_projection_run. Emits the shared position_state column contract; closure is applied by materialize_position_projection().';

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
        -- instrument_key comes from the view's own helper, so the guard cannot check a stale shape.
        WITH src AS MATERIALIZED (
            SELECT s.prime_id, s.package_id, s.asset_type, s.custody_type, s.processing_version, s.snapshot_time,
                   public.anchorage_instrument_key(s.package_id, s.asset_type) AS instrument_key
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
            SELECT format('%s distinct package/asset pairs render the instrument_key %L', count(*), p.instrument_key)
            FROM (SELECT DISTINCT s.package_id, s.asset_type, s.instrument_key FROM src s) p
            GROUP BY p.instrument_key
            HAVING count(*) > 1
            UNION ALL
            -- position_key()'s own blank predicate, so the two cannot disagree: btrim() strips spaces
            -- only, and the helper's provider prefix means a whitespace component never reaches it blank.
            SELECT format('package %L asset %L has a blank or delimiter-bearing identity on %s snapshot(s)', s.package_id, s.asset_type, count(*))
            FROM src s
            WHERE s.package_id ~ '^\s*$' OR s.asset_type ~ '^\s*$'
               OR s.package_id LIKE '%;%' OR s.asset_type LIKE '%;%'
            GROUP BY s.package_id, s.asset_type
            UNION ALL
            -- prime.vault_address carries no length CHECK, and holder_id is its hex: a short one fails
            -- the spine's 40-hex CHECK with a 23514 naming no row.
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

COMMENT ON FUNCTION materialize_anchorage_custody(integer, bigint, interval) IS '[Operational] VEC-408: materialize Anchorage custody packages into position_state via materialize_position_projection(position_anchorage_custody). Refuses to run, naming up to five offenders, when a custody_type is not a known custodian (the view drops it), when one (prime, package, asset, processing_version) carries two snapshots inside one second — including two under different custody types, since custody_type is not part of the observation key and block_number is the instant in whole seconds — when two distinct (package, asset) pairs render one instrument_key, when a package or asset id is blank or contains the '';'' key delimiter, or when a prime''s vault address is not 20 bytes and so cannot render holder_id. The injectivity check reads anchorage_instrument_key(), the same helper the view builds instrument_key with, so it cannot validate a shape the view no longer emits (VEC-809). Takes the materializer''s own advisory lock key, so two runs of this projection serialise; it does not exclude the source''s writer, which locks a per-row natural key, so a snapshot committed after the check is read by the run unchecked — a collision then raises as the materializer''s generic double-emit rather than by name, and the next run names it. Every check reads one materialised scan of anchorage_package_snapshot. Idempotent; run out of band. Returns rows appended. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2). p_window is forwarded to the materializer, which bounds the batch it reads; against this view it filters rows without pruning chunks.';

INSERT INTO migrations (filename) VALUES ('20260916_150000_anchorage_custody_guard_key_and_window.sql') ON CONFLICT (filename) DO NOTHING;
