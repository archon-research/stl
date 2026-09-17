-- migrate: no-transaction
--
-- Convert 57 of the 61 hypertables back to plain PostgreSQL tables.
--
-- Why: measured on staging 2026-09-16 (PG 18.6 / TimescaleDB 2.29.2), 84% of the
-- 4,769 chunks serve 8% of the data, and planning is 46.3% of all database CPU
-- time (2,583 s planning vs 2,992 s executing over 1.54M calls). A 4-row read of
-- offchain_token_price -- 12,866 rows spread over 344 chunks -- plans for 224 ms
-- and allocates 1.3 GB, against 35 ms of execution. The dominant read shape is
-- "latest row per entity" ordered by block_number, which prunes no chunk at all.
-- Findings and the per-table decision: hypertable-conversion-research.md.
--
-- Four hypertables are deliberately KEPT and are absent from the list below:
--   cex_orderbook_snapshots  compression saves 30.7 GB (45.0 -> 14.3 GB)
--   protocol_event           compression saves 13.9 GB (15.2 -> 1.3 GB)
--   block_states             the schema's only drop_chunks retention (30 days)
--   morpho_vault_position    pending the VEC-800 segmentby fix and re-measure
--
-- Shape: every constraint, index, trigger, grant, comment, sequence and default is
-- preserved byte-for-byte, INCLUDING the partition column's place in each primary
-- key. Narrowing those keys would break every ON CONFLICT arbiter that names them
-- and is deliberately left to a later, per-table change. Nothing in Go or Python
-- has to change for this migration.
--
-- WHY no-transaction: one transaction over all 57 cannot hold the locks. Dropping a
-- hypertable locks every chunk, every chunk index and every TOAST relation it owns --
-- 44,890 lock objects across this set, against 76,800 shared slots that every other
-- backend draws from too. Measured on a staging fork 2026-09-17: a single-transaction
-- run died at table 30 of 57 with "out of shared memory" (SQLSTATE 53200) after 2m54s.
-- Statement-per-statement, each table commits and releases its locks, so the peak is
-- one table (borrower, the worst, needs ~5,100).
--
-- The cost is that a failure leaves the set half-converted. That is safe here: each
-- table is independently correct either way, there is no cross-table invariant, and
-- _dechunk_to_plain_table skips a table that is already plain -- so re-running the
-- migration resumes where it stopped. It also means the ArgoCD PreSync Job's
-- activeDeadlineSeconds (900) can no longer lose work: a kill costs one table.
--
-- Lock and duration: each table is rewritten under ACCESS EXCLUSIVE, ~8.5 GB
-- uncompressed in total across 21.8M rows. Needs a maintenance window; workers
-- retry through it.

CREATE OR REPLACE FUNCTION _dechunk_to_plain_table(p_schema text, p_table text)
RETURNS void
LANGUAGE plpgsql
AS $dechunk$
DECLARE
    v_rel        regclass := format('%I.%I', p_schema, p_table)::regclass;
    v_tmp        text     := left('__dechunk_' || p_table, 63);
    v_owner      text;
    v_rows_before bigint;
    v_rows_after  bigint;
    v_started    timestamptz := clock_timestamp();
    v_checks     text[] := '{}';
    v_keys       text[] := '{}';
    v_fks        text[] := '{}';
    v_indexes    text[] := '{}';
    v_triggers   text[] := '{}';
    v_grants     text[] := '{}';
    v_comments   text[] := '{}';
    v_seqs       text[] := '{}';
    v_views      text[] := '{}';
    v_stmt       text;
    r            record;
BEGIN
    -- Idempotent: this migration runs statement-per-statement (see the header), so a
    -- re-run after a partial failure must skip what the previous attempt committed.
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables
        WHERE hypertable_schema = p_schema AND hypertable_name = p_table
    ) THEN
        RAISE NOTICE '[dechunk] %.% already plain, skipping', p_schema, p_table;
        RETURN;
    END IF;

    SELECT pg_get_userbyid(relowner) INTO v_owner FROM pg_class WHERE oid = v_rel;

    -- Dependent views block DROP TABLE. Capture, drop, and recreate them around the
    -- swap. A view that is itself depended on would need ordering we do not do, so
    -- refuse rather than guess.
    FOR r IN
        SELECT DISTINCT v.oid, v.relname, n.nspname,
               pg_get_userbyid(v.relowner) AS viewowner, v.relacl
        FROM pg_depend d
        JOIN pg_rewrite rw ON rw.oid = d.objid
        JOIN pg_class v ON v.oid = rw.ev_class
        JOIN pg_namespace n ON n.oid = v.relnamespace
        WHERE d.classid = 'pg_rewrite'::regclass
          AND d.refobjid = v_rel
          AND v.relkind IN ('v', 'm')
          AND v.oid <> v_rel
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_depend d2
            JOIN pg_rewrite rw2 ON rw2.oid = d2.objid
            JOIN pg_class v2 ON v2.oid = rw2.ev_class
            WHERE d2.classid = 'pg_rewrite'::regclass
              AND d2.refobjid = r.oid AND v2.oid <> r.oid
        ) THEN
            RAISE EXCEPTION 'view %.% depends on %.% and has dependents of its own; '
                            'recreate it by hand', r.nspname, r.relname, p_schema, p_table;
        END IF;

        v_views := v_views || format('CREATE VIEW %I.%I AS %s',
                                     r.nspname, r.relname, pg_get_viewdef(r.oid));
        v_views := v_views || format('ALTER VIEW %I.%I OWNER TO %I',
                                     r.nspname, r.relname, r.viewowner);
        IF r.relacl IS NOT NULL THEN
            SELECT v_views || coalesce(array_agg(
                       format('GRANT %s ON %I.%I TO %s%s', a.privilege_type,
                              r.nspname, r.relname,
                              CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                                   ELSE quote_ident(pg_get_userbyid(a.grantee)) END,
                              CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END)),
                   '{}')
            INTO v_views FROM aclexplode(r.relacl) a;
        END IF;
        IF obj_description(r.oid, 'pg_class') IS NOT NULL THEN
            v_views := v_views || format('COMMENT ON VIEW %I.%I IS %L',
                                         r.nspname, r.relname, obj_description(r.oid, 'pg_class'));
        END IF;
        EXECUTE format('DROP VIEW %I.%I', r.nspname, r.relname);
    END LOOP;

    -- CHECK constraints, then PRIMARY KEY / UNIQUE, then foreign keys: LIKE below
    -- copies NOT NULL but none of these, which is what lets them keep their names.
    SELECT coalesce(array_agg(format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                                     p_schema, p_table, conname, pg_get_constraintdef(oid))
                              ORDER BY conname), '{}')
    INTO v_checks FROM pg_constraint WHERE conrelid = v_rel AND contype = 'c';

    SELECT coalesce(array_agg(format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                                     p_schema, p_table, conname, pg_get_constraintdef(oid))
                              ORDER BY contype DESC, conname), '{}')
    INTO v_keys FROM pg_constraint WHERE conrelid = v_rel AND contype IN ('p', 'u');

    SELECT coalesce(array_agg(format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                                     p_schema, p_table, conname, pg_get_constraintdef(oid))
                              ORDER BY conname), '{}')
    INTO v_fks FROM pg_constraint WHERE conrelid = v_rel AND contype = 'f';

    -- Indexes that are not backing a constraint (those come back with the constraint).
    SELECT coalesce(array_agg(pg_get_indexdef(i.indexrelid) ORDER BY i.indexrelid::regclass::text), '{}')
    INTO v_indexes
    FROM pg_index i
    WHERE i.indrelid = v_rel
      AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid);

    SELECT coalesce(array_agg(pg_get_triggerdef(oid) ORDER BY tgname), '{}')
    INTO v_triggers FROM pg_trigger WHERE tgrelid = v_rel AND NOT tgisinternal;

    -- The ACL is replayed verbatim rather than left to ALTER DEFAULT PRIVILEGES,
    -- which hands every migrator-owned table INSERT/UPDATE/DELETE and would
    -- silently re-grant the UPDATE and DELETE that the append-only tables
    -- (asset_price, morpho_adapter_state, psm3_alm_shares, position_state, the
    -- uniswap_v4_* set) have revoked. TestConvertedTablesAreAppendOnly covers it.
    SELECT coalesce(array_agg(
               format('GRANT %s ON %I.%I TO %s%s', a.privilege_type, p_schema, p_table,
                      CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                           ELSE quote_ident(pg_get_userbyid(a.grantee)) END,
                      CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END)), '{}')
    INTO v_grants
    FROM pg_class c, aclexplode(c.relacl) a WHERE c.oid = v_rel;

    -- LIKE INCLUDING COMMENTS covers columns but never the table comment.
    IF obj_description(v_rel, 'pg_class') IS NOT NULL THEN
        v_comments := v_comments || format('COMMENT ON TABLE %I.%I IS %L',
                                           p_schema, p_table, obj_description(v_rel, 'pg_class'));
    END IF;
    SELECT v_comments || coalesce(array_agg(
               format('COMMENT ON COLUMN %I.%I.%I IS %L', p_schema, p_table, a.attname,
                      col_description(v_rel, a.attnum)) ORDER BY a.attnum), '{}')
    INTO v_comments
    FROM pg_attribute a
    WHERE a.attrelid = v_rel AND a.attnum > 0 AND NOT a.attisdropped
      AND col_description(v_rel, a.attnum) IS NOT NULL;

    -- A sequence owned by this table (sparklend_reserve_data.id) would be dropped
    -- along with it, taking the new table's nextval default with it. Detach now,
    -- re-attach after the rename; the sequence keeps its value either way.
    FOR r IN
        SELECT s.relname AS seqname, ns.nspname AS seqschema, a.attname
        FROM pg_depend d
        JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
        JOIN pg_namespace ns ON ns.oid = s.relnamespace
        JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
        WHERE d.refobjid = v_rel AND d.deptype = 'a'
    LOOP
        EXECUTE format('ALTER SEQUENCE %I.%I OWNED BY NONE', r.seqschema, r.seqname);
        v_seqs := v_seqs || format('ALTER SEQUENCE %I.%I OWNED BY %I.%I.%I',
                                   r.seqschema, r.seqname, p_schema, p_table, r.attname);
    END LOOP;

    -- Policies would otherwise race the rewrite for chunk locks.
    PERFORM remove_retention_policy(v_rel, if_exists => true);
    PERFORM remove_compression_policy(v_rel, if_exists => true);
    BEGIN
        PERFORM remove_tiering_policy(v_rel, if_exists => true);
    EXCEPTION WHEN undefined_function THEN
        NULL;  -- no tiering outside Tiger Cloud
    END;

    EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE', p_schema, p_table);
    EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, p_table) INTO v_rows_before;

    EXECUTE format(
        'CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS INCLUDING GENERATED '
        'INCLUDING IDENTITY INCLUDING STORAGE INCLUDING STATISTICS)',
        p_schema, v_tmp, p_schema, p_table);
    EXECUTE format('INSERT INTO %I.%I SELECT * FROM %I.%I', p_schema, v_tmp, p_schema, p_table);
    GET DIAGNOSTICS v_rows_after = ROW_COUNT;

    IF v_rows_before <> v_rows_after THEN
        RAISE EXCEPTION 'copy of %.% moved % of % rows',
                        p_schema, p_table, v_rows_after, v_rows_before;
    END IF;

    EXECUTE format('DROP TABLE %I.%I', p_schema, p_table);
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_schema, v_tmp, p_table);
    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', p_schema, p_table, v_owner);

    -- Strip what ALTER DEFAULT PRIVILEGES just handed the new table, then replay
    -- the captured ACL exactly.
    EXECUTE format('REVOKE ALL ON %I.%I FROM PUBLIC', p_schema, p_table);
    FOR r IN
        SELECT DISTINCT CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                             ELSE quote_ident(pg_get_userbyid(a.grantee)) END AS grantee
        FROM pg_class c, aclexplode(c.relacl) a
        WHERE c.oid = format('%I.%I', p_schema, p_table)::regclass
    LOOP
        EXECUTE format('REVOKE ALL ON %I.%I FROM %s', p_schema, p_table, r.grantee);
    END LOOP;

    -- The grants go back BEFORE anything that needs one. The revoke above strips the
    -- owner too, and CREATE TRIGGER is checked against the TRIGGER privilege rather than
    -- ownership, so replaying triggers first fails with "permission denied" for every
    -- table that has one. Only a superuser survives that order, which is why it passes
    -- locally (migrations run as postgres) and fails as stl_migrator on a real service.
    FOREACH v_stmt IN ARRAY v_grants LOOP
        EXECUTE v_stmt;
    END LOOP;

    FOREACH v_stmt IN ARRAY v_checks || v_keys || v_indexes || v_fks || v_triggers
                           || v_comments || v_seqs || v_views
    LOOP
        EXECUTE v_stmt;
    END LOOP;

    EXECUTE format('ANALYZE %I.%I', p_schema, p_table);

    RAISE NOTICE '[dechunk] % %.% — % rows, %s, % indexes, % triggers, % grants',
                 to_char(clock_timestamp(), 'HH24:MI:SS'), p_schema, p_table,
                 to_char(v_rows_after, 'FM999,999,999'),
                 round(extract(epoch FROM clock_timestamp() - v_started)::numeric, 1),
                 cardinality(v_indexes) + cardinality(v_keys),
                 cardinality(v_triggers), cardinality(v_grants);
END;
$dechunk$;


-- ============================================================================
-- The 57 conversions, one statement each so each commits on its own. The four
-- keeps are absent by name, not by filter, so a new hypertable added later is not
-- swept up silently.
-- ============================================================================

SELECT _dechunk_to_plain_table('public', 'allocation_position');
SELECT _dechunk_to_plain_table('public', 'anchorage_operation');
SELECT _dechunk_to_plain_table('public', 'anchorage_package_snapshot');
SELECT _dechunk_to_plain_table('public', 'asset_price');
SELECT _dechunk_to_plain_table('public', 'borrower');
SELECT _dechunk_to_plain_table('public', 'borrower_collateral');
SELECT _dechunk_to_plain_table('public', 'core_model_results');
SELECT _dechunk_to_plain_table('public', 'curve_cryptoswap_state');
SELECT _dechunk_to_plain_table('public', 'curve_liquidity_event');
SELECT _dechunk_to_plain_table('public', 'curve_lp_token_event');
SELECT _dechunk_to_plain_table('public', 'curve_parameter_event');
SELECT _dechunk_to_plain_table('public', 'curve_stableswap_state');
SELECT _dechunk_to_plain_table('public', 'curve_swap');
SELECT _dechunk_to_plain_table('public', 'fluid_vault_state');
SELECT _dechunk_to_plain_table('public', 'maple_ftl_loan_state');
SELECT _dechunk_to_plain_table('public', 'maple_loan_collateral');
SELECT _dechunk_to_plain_table('public', 'maple_loan_state');
SELECT _dechunk_to_plain_table('public', 'maple_pool_state');
SELECT _dechunk_to_plain_table('public', 'maple_sky_strategy_state');
SELECT _dechunk_to_plain_table('public', 'maple_syrup_global_state');
SELECT _dechunk_to_plain_table('public', 'morpho_adapter_state');
SELECT _dechunk_to_plain_table('public', 'morpho_market_position');
SELECT _dechunk_to_plain_table('public', 'morpho_market_state');
SELECT _dechunk_to_plain_table('public', 'morpho_vault_state');
SELECT _dechunk_to_plain_table('public', 'offchain_token_price');
SELECT _dechunk_to_plain_table('public', 'onchain_token_price');
SELECT _dechunk_to_plain_table('public', 'position_state');
SELECT _dechunk_to_plain_table('public', 'prime_capital_stack');
SELECT _dechunk_to_plain_table('public', 'prime_capital_stack_allocation');
SELECT _dechunk_to_plain_table('public', 'prime_debt');
SELECT _dechunk_to_plain_table('public', 'prime_reference_balance_sheet');
SELECT _dechunk_to_plain_table('public', 'prime_reference_position');
SELECT _dechunk_to_plain_table('public', 'psm3_alm_shares');
SELECT _dechunk_to_plain_table('public', 'psm3_reserves');
SELECT _dechunk_to_plain_table('public', 'sparklend_reserve_data');
SELECT _dechunk_to_plain_table('public', 'token_total_supply');
SELECT _dechunk_to_plain_table('public', 'uniswap_v3_liquidity_event');
SELECT _dechunk_to_plain_table('public', 'uniswap_v3_pool_event');
SELECT _dechunk_to_plain_table('public', 'uniswap_v3_pool_state');
SELECT _dechunk_to_plain_table('public', 'uniswap_v3_swap');
SELECT _dechunk_to_plain_table('public', 'uniswap_v4_liquidity_event');
SELECT _dechunk_to_plain_table('public', 'uniswap_v4_pool_event');
SELECT _dechunk_to_plain_table('public', 'uniswap_v4_pool_state');
SELECT _dechunk_to_plain_table('public', 'uniswap_v4_swap');
SELECT _dechunk_to_plain_table('transformed', 'fluid_vault_state');
SELECT _dechunk_to_plain_table('transformed', 'maple_loan_collateral');
SELECT _dechunk_to_plain_table('transformed', 'maple_loan_state');
SELECT _dechunk_to_plain_table('transformed', 'maple_pool_state');
SELECT _dechunk_to_plain_table('transformed', 'maple_sky_strategy_state');
SELECT _dechunk_to_plain_table('transformed', 'maple_syrup_global_state');
SELECT _dechunk_to_plain_table('transformed', 'morpho_market_position');
SELECT _dechunk_to_plain_table('transformed', 'morpho_market_state');
SELECT _dechunk_to_plain_table('transformed', 'morpho_vault_position');
SELECT _dechunk_to_plain_table('transformed', 'morpho_vault_state');
SELECT _dechunk_to_plain_table('transformed', 'offchain_token_price');
SELECT _dechunk_to_plain_table('transformed', 'onchain_token_price');
SELECT _dechunk_to_plain_table('transformed', 'token_total_supply');

-- ============================================================================
-- Assert the end state: exactly the four keeps remain partitioned.
-- ============================================================================

DO $verify$
DECLARE
    v_left text[];
BEGIN
    SELECT coalesce(array_agg(s.name ORDER BY s.name), '{}')
    INTO v_left
    FROM (SELECT format('%s.%s', hypertable_schema, hypertable_name) AS name
          FROM timescaledb_information.hypertables) s;

    IF v_left <> ARRAY['public.block_states', 'public.cex_orderbook_snapshots',
                       'public.morpho_vault_position', 'public.protocol_event'] THEN
        RAISE EXCEPTION 'expected only the four kept hypertables to remain, found %', v_left;
    END IF;
END;
$verify$;

DROP FUNCTION IF EXISTS _dechunk_to_plain_table(text, text);

INSERT INTO migrations (filename) VALUES ('20260917_120000_dechunk_hypertables_to_plain_tables.sql') ON CONFLICT (filename) DO NOTHING;
