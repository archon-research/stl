-- VEC-404: project the Aave-family lending ledgers onto the position spine.

-- What the projection resolved silently, recorded per run instead of failing it: a reserve it could not
-- key, or an observation it arbitrated. Append-only, so each has a history. Plain table, not a
-- hypertable: it appends a handful of rows per run (db/migrations/AGENTS.md's sparse-table exception).
CREATE TABLE IF NOT EXISTS aave_projection_note (
    protocol_id  bigint      NOT NULL REFERENCES protocol (id),
    token_id     bigint      NOT NULL REFERENCES token (id),
    reason       text        NOT NULL,
    observations bigint      NOT NULL,
    build_id     integer     NOT NULL,
    run_id       bigint,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT aave_projection_note_pkey PRIMARY KEY (protocol_id, token_id, reason, created_at),
    CONSTRAINT aave_projection_note_reason_chk
        CHECK (reason IN ('no_variable_debt_token', 'no_receipt_token', 'arbitrated_observation')),
    CONSTRAINT aave_projection_note_observations_chk CHECK (observations > 0)
);

COMMENT ON TABLE aave_projection_note IS '[Operational] VEC-404: one row per materialize_aave_lending() run per reserve the projection did not project as-read. reason names the case and the ledger: no_variable_debt_token for a debt reserve (borrower) absent from debt_token or carrying a NULL variable_debt_address, no_receipt_token for a supply reserve (borrower_collateral) absent from receipt_token, arbitrated_observation for a reserve where one observation key held rows disagreeing on the value the view emits, so the earliest-created_at pick decided it. observations counts the ledger rows skipped, or the keys arbitrated. Append-only: each run appends its own view, so a case closing is visible as its disappearance from later runs rather than as a mutation. Plain table, deliberately: it appends a handful of rows per run. A skipped reserve holds real exposure that position_state does not carry; an arbitrated key means two writers disagreed and one value was dropped.';
COMMENT ON COLUMN aave_projection_note.protocol_id IS 'Roles: PK, FK->protocol.id. The lending protocol whose reserve is unmapped.';
COMMENT ON COLUMN aave_projection_note.token_id IS 'Roles: PK, FK->token.id. The reserve''s underlying token, which is what the mapping is missing for.';
COMMENT ON COLUMN aave_projection_note.reason IS 'Roles: PK, Derived. The tag, which also names the ledger: no_variable_debt_token (borrower), no_receipt_token (borrower_collateral), or arbitrated_observation (either ledger, so this one reason does not name which) where one observation key held rows disagreeing on a value the view emits: the key''s rows always differ on created_at, which the view emits as block_timestamp, and may differ on amount too.';
COMMENT ON COLUMN aave_projection_note.observations IS 'Roles: Derived. A count whose unit depends on reason: ledger rows skipped for this reserve for no_variable_debt_token and no_receipt_token, observation KEYS arbitrated for arbitrated_observation. Both as at the time of the run.';
COMMENT ON COLUMN aave_projection_note.build_id IS 'Roles: Audit. build_registry.id of the run that recorded the gap (0 = pre-tracking).';
COMMENT ON COLUMN aave_projection_note.run_id IS 'Roles: Audit. writer_run.id of the run that recorded the gap (ADR-0006 §2); NULL means it predates run tracking.';
COMMENT ON COLUMN aave_projection_note.created_at IS 'Roles: PK, Audit. When the run recorded it: one clock_timestamp() read per call, so every row a run writes shares it and the run''s rows group by it without depending on run_id, which is NULL for a defaulted call. Two runs in one transaction still read different values, so they cannot collide.';

GRANT SELECT ON aave_projection_note TO stl_readonly;
GRANT SELECT, INSERT ON aave_projection_note TO stl_readwrite;
REVOKE UPDATE, DELETE ON aave_projection_note FROM stl_readwrite;
-- Owner-side too, as sec_node/sec_edge do: nothing FKs this table, so no RI probe needs the owner's
-- UPDATE, and a later fix-migration rewriting history here fails loudly. Derived from pg_class.relowner
-- so it lands whatever the role is called, including in CI where stl_migrator does not exist.
DO $acl$
DECLARE owner_role text;
BEGIN
    SELECT pg_get_userbyid(relowner) INTO owner_role FROM pg_class WHERE oid = 'aave_projection_note'::regclass;
    EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON aave_projection_note FROM %I', owner_role);
END
$acl$;

CREATE OR REPLACE VIEW position_aave_lending AS
WITH debt AS (
    -- created_at is the block header time the tracker stamps. Two builds can resolve two times for one
    -- block, both at processing_version 0, so the earliest is the stable pick.
    SELECT DISTINCT ON (b.user_id, b.protocol_id, b.token_id, b.block_number, b.block_version, b.processing_version)
           'debt'::text AS leg,
           b.user_id, b.protocol_id, b.token_id,
           b.block_number, b.block_version, b.processing_version,
           b.created_at AS block_timestamp,
           b.amount AS quantity,
           'BORROW'::text AS deal_type,
           encode(dt.variable_debt_address, 'hex') AS instrument_key
    FROM borrower b
    LEFT JOIN debt_token dt ON dt.protocol_id = b.protocol_id AND dt.underlying_token_id = b.token_id
    ORDER BY b.user_id, b.protocol_id, b.token_id, b.block_number, b.block_version, b.processing_version,
             b.created_at
),
receipt AS (
    -- receipt_token is unique on (chain_id, address) only. A reserve mapped to two receipt tokens
    -- resolves to NULL here so a direct reader never sees the position twice.
    SELECT protocol_id, underlying_token_id,
           CASE WHEN count(*) = 1 THEN min(receipt_token_address) END AS receipt_token_address
    FROM receipt_token
    GROUP BY protocol_id, underlying_token_id
),
supply AS (
    SELECT DISTINCT ON (c.user_id, c.protocol_id, c.token_id, c.block_number, c.block_version, c.processing_version)
           'supply'::text AS leg,
           c.user_id, c.protocol_id, c.token_id,
           c.block_number, c.block_version, c.processing_version,
           c.created_at AS block_timestamp,
           c.amount AS quantity,
           CASE WHEN c.collateral_enabled THEN 'COLLATERAL' ELSE 'LOAN' END AS deal_type,
           encode(rt.receipt_token_address, 'hex') AS instrument_key
    FROM borrower_collateral c
    LEFT JOIN receipt AS rt ON rt.protocol_id = c.protocol_id AND rt.underlying_token_id = c.token_id
    ORDER BY c.user_id, c.protocol_id, c.token_id, c.block_number, c.block_version, c.processing_version,
             c.created_at
)
SELECT p.chain_id,
       s.protocol_id,
       s.instrument_key,
       encode(u.address, 'hex') AS holder_id,
       s.quantity,
       s.deal_type,
       s.block_number,
       s.block_version,
       s.processing_version,
       s.block_timestamp
FROM (SELECT * FROM debt UNION ALL SELECT * FROM supply) s
JOIN protocol p ON p.id = s.protocol_id
JOIN "user"   u ON u.id = s.user_id
-- materialize_aave_lending() records an unkeyable reserve in aave_projection_note, and the run
-- continues without it.
WHERE s.instrument_key IS NOT NULL;

COMMENT ON VIEW position_aave_lending IS '[Operational] VEC-404 projection: Aave-family lending positions (SparkLend, Aave V2/V3 and forks sharing the borrower ledgers) as native per-instrument position rows. Debt leg: borrower rows -> BORROW, instrument_key = the reserve''s variable-debt token address (debt_token). Supply leg: borrower_collateral rows -> COLLATERAL when collateral_enabled else LOAN, instrument_key = the reserve''s receipt token address (receipt_token). Quantities are the ledger''s point-in-time balances in native decimals; block_timestamp is the ledger''s created_at, which the tracker sets to the block header time. Emits the shared position_state column contract consumed by materialize_position_projection(); one row per observation; closure is applied by the materializer. A reserve with no token mapping is skipped and recorded in aave_projection_note, so the exposure it holds is visible; a reserve mapped ambiguously would key wrongly and refuses the run instead.';

-- Records the reserves it cannot key, then refuses what would key wrongly: an ambiguous mapping or a
-- cross-chain ledger row mints a colliding or wrong position_id, which the spine cannot undo.
DROP FUNCTION IF EXISTS materialize_aave_lending(integer);
DROP FUNCTION IF EXISTS materialize_aave_lending(integer, bigint);

CREATE OR REPLACE FUNCTION materialize_aave_lending(p_build_id integer DEFAULT 0,
                                                    p_run_id bigint DEFAULT NULL,
                                                    p_window interval DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    -- Superuser-context GUC: creating and calling this needs SET ON PARAMETER temp_file_limit. Measured
    -- as held by PUBLIC on staging, granted for kind in k8s/dev-infra/jobs/bootstrap-db.yaml, prod
    -- unconfirmed (VEC-812): without it the migration aborts, and it cannot be fixed in place.
    SET temp_file_limit = '4GB'
    -- Pinned to the materializer's own setting, so the branches below cannot read fewer chunks than the
    -- run does on an instance where the default is off; both ledgers tier at one year.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad  text;
    v_msgs text[];
    v_at   timestamptz := clock_timestamp();
BEGIN
    v_msgs := ARRAY[]::text[];

    v_msgs := v_msgs || ARRAY(
        SELECT * FROM (
            SELECT format('variable-debt token %s is mapped to %s reserves of protocol_id %s', encode(dt.variable_debt_address, 'hex'), count(*), dt.protocol_id) AS msg
            FROM public.debt_token dt
            WHERE dt.variable_debt_address IS NOT NULL
              AND EXISTS (SELECT 1 FROM public.borrower b WHERE b.protocol_id = dt.protocol_id AND b.token_id = dt.underlying_token_id)
            GROUP BY dt.protocol_id, dt.variable_debt_address
            HAVING count(*) > 1
        ) b1 ORDER BY 1 LIMIT 10);

    v_msgs := v_msgs || ARRAY(
        SELECT * FROM (
            SELECT format('supply reserve (protocol_id %s, token_id %s) maps to %s receipt tokens', r.protocol_id, r.token_id, count(DISTINCT rt.receipt_token_address))
            FROM (SELECT DISTINCT protocol_id, token_id FROM public.borrower_collateral) r
            JOIN public.receipt_token rt ON rt.protocol_id = r.protocol_id AND rt.underlying_token_id = r.token_id
            GROUP BY r.protocol_id, r.token_id
            HAVING count(DISTINCT rt.receipt_token_address) > 1
        ) b2 ORDER BY 1 LIMIT 10);

    -- receipt_token is unique on (chain_id, receipt_token_address), its OWN chain_id, which nothing
    -- ties to the protocol's; two reserves can share one address and merge into one position.
    v_msgs := v_msgs || ARRAY(
        SELECT * FROM (
            SELECT format('receipt token %s is mapped to %s reserves of protocol_id %s', encode(rt.receipt_token_address, 'hex'), count(DISTINCT rt.underlying_token_id), rt.protocol_id)
            FROM public.receipt_token rt
            WHERE EXISTS (SELECT 1 FROM public.borrower_collateral c
                           WHERE c.protocol_id = rt.protocol_id AND c.token_id = rt.underlying_token_id)
            GROUP BY rt.protocol_id, rt.receipt_token_address
            HAVING count(DISTINCT rt.underlying_token_id) > 1
        ) b6 ORDER BY 1 LIMIT 10);

    v_msgs := v_msgs || ARRAY(
        SELECT * FROM (
            SELECT format('address %s is registered as both a variable-debt and a receipt token of protocol_id %s', encode(dt.variable_debt_address, 'hex'), dt.protocol_id)
            FROM public.debt_token dt
            JOIN public.receipt_token rt ON rt.protocol_id = dt.protocol_id
                                        AND rt.receipt_token_address = dt.variable_debt_address
            WHERE dt.variable_debt_address IS NOT NULL
              -- Only when a ledger actually keys through it, as every other branch requires: a
              -- reference-data collision on a protocol nobody holds a position in keys nothing.
              AND (EXISTS (SELECT 1 FROM public.borrower b
                            WHERE b.protocol_id = dt.protocol_id AND b.token_id = dt.underlying_token_id)
                OR EXISTS (SELECT 1 FROM public.borrower_collateral c
                            WHERE c.protocol_id = rt.protocol_id AND c.token_id = rt.underlying_token_id))
        ) b3 ORDER BY 1 LIMIT 10);

    v_msgs := v_msgs || ARRAY(
        SELECT * FROM (
            SELECT format('holder %s is a %s-byte address, so holder_id would fail position_state''s 40-hex check', encode(u.address, 'hex'), length(u.address))
            FROM (SELECT DISTINCT user_id FROM public.borrower
                  UNION SELECT DISTINCT user_id FROM public.borrower_collateral) r
            JOIN public."user" u ON u.id = r.user_id
            WHERE length(u.address) <> 20
        ) b4 ORDER BY 1 LIMIT 10);

    v_msgs := v_msgs || ARRAY(
        SELECT * FROM (
            SELECT format('ledger row (user_id %s, protocol_id %s, token_id %s) mixes chains: holder %s, token %s, protocol %s', r.user_id, r.protocol_id, r.token_id, u.chain_id, t.chain_id, p.chain_id)
            FROM (SELECT DISTINCT user_id, protocol_id, token_id FROM public.borrower
                  UNION SELECT DISTINCT user_id, protocol_id, token_id FROM public.borrower_collateral) r
            JOIN public.protocol p ON p.id = r.protocol_id
            JOIN public."user"   u ON u.id = r.user_id
            JOIN public.token    t ON t.id = r.token_id
            WHERE u.chain_id <> p.chain_id OR t.chain_id <> p.chain_id
        ) b5 ORDER BY 1 LIMIT 10);

    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad
      FROM (SELECT unnest(v_msgs) AS msg ORDER BY 1 LIMIT 10) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_aave_lending: inputs that would key wrongly, refusing to run: %', v_bad;
    END IF;

    -- Record this run's gap. A NULL variable_debt_address is a gap, not an ambiguous mapping, and the
    -- branch above excludes it before grouping; the supply side is a LEFT JOIN miss, the column is NOT NULL.
    INSERT INTO public.aave_projection_note (protocol_id, token_id, reason, observations, build_id, run_id, created_at)
    SELECT b.protocol_id, b.token_id, 'no_variable_debt_token', count(*), p_build_id, p_run_id, v_at
    FROM public.borrower b
    LEFT JOIN public.debt_token dt ON dt.protocol_id = b.protocol_id AND dt.underlying_token_id = b.token_id
    WHERE dt.variable_debt_address IS NULL
    GROUP BY b.protocol_id, b.token_id
    UNION ALL
    SELECT c.protocol_id, c.token_id, 'no_receipt_token', count(*), p_build_id, p_run_id, v_at
    FROM public.borrower_collateral c
    LEFT JOIN public.receipt_token rt ON rt.protocol_id = c.protocol_id AND rt.underlying_token_id = c.token_id
    WHERE rt.receipt_token_address IS NULL
    GROUP BY c.protocol_id, c.token_id
    UNION ALL
    -- Both legs collapse an observation key to its earliest created_at, silently. Any key holding
    -- more than one row is arbitrated: both PKs carry created_at, so the rows must differ on it, and
    -- the view emits created_at as block_timestamp -- so a value is always dropped, not just on amount.
    SELECT d.protocol_id, d.token_id, 'arbitrated_observation', count(*), p_build_id, p_run_id, v_at
    FROM (SELECT protocol_id, token_id
            FROM public.borrower
           GROUP BY user_id, protocol_id, token_id, block_number, block_version, processing_version
          HAVING count(*) > 1
           UNION ALL
          SELECT protocol_id, token_id
            FROM public.borrower_collateral
           GROUP BY user_id, protocol_id, token_id, block_number, block_version, processing_version
          HAVING count(*) > 1) d
    GROUP BY d.protocol_id, d.token_id;

    -- A mapping that disappears OR moves strands live exposure: the view stops emitting that instrument
    -- and nothing closes the stored rows, so refuse rather than leave it reading as current. Scoped to
    -- the position's own protocol and leg, so a re-registration elsewhere does not read as still mapped.
    SELECT string_agg(DISTINCT c.instrument_key, ', ' ORDER BY c.instrument_key) INTO v_bad
    FROM public.position_current c
    WHERE c.projection = 'public.position_aave_lending'
      AND c.quantity > 0
      AND NOT EXISTS (
            SELECT 1 FROM public.debt_token dt
             WHERE c.deal_type = 'BORROW'
               AND dt.protocol_id = c.protocol_id
               AND dt.variable_debt_address IS NOT NULL
               AND encode(dt.variable_debt_address, 'hex') = c.instrument_key
             UNION ALL
            SELECT 1 FROM public.receipt_token rt
             WHERE c.deal_type <> 'BORROW'
               AND rt.protocol_id = c.protocol_id
               AND encode(rt.receipt_token_address, 'hex') = c.instrument_key);
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_aave_lending: live exposure whose instrument the view no longer emits, so a lost token mapping would strand it; refusing to run: %', v_bad;
    END IF;

    RETURN public.materialize_position_projection('public.position_aave_lending'::regclass, p_build_id, p_run_id, p_window);
END
$fn$;

COMMENT ON FUNCTION materialize_aave_lending(integer, bigint, interval) IS '[Operational] VEC-404: materialize Aave-family lending positions into position_state via materialize_position_projection(position_aave_lending). Records in aave_projection_note each reserve it cannot key (no_variable_debt_token, no_receipt_token) and each reserve whose observation key held rows disagreeing on an emitted value, which the view resolves earliest-first (arbitrated_observation), then projects the rest. Refuses to run, naming up to ten offenders: a reserve mapped to several receipt tokens, several reserves sharing one receipt token, one variable-debt token shared across reserves, an address registered as both a debt and a receipt token, a holder address that is not 20 bytes, a ledger row mixing chains, and live exposure in position_current whose instrument no longer maps under that position''s own protocol and leg. Idempotent; run out of band. Caps its temp files at 4 GB per backend process (temp_file_limit; each parallel worker holds its own): one call spills 3.0 to 4.3 GB on a clone at 90% of prod against a 200 GB server default, so a runaway aborts with SQLSTATE 53400 whichever role calls it. p_build_id (build_registry.id; 0 = pre-tracking) and p_run_id (writer_run.id) are stamped on every row appended (ADR-0006 §2). p_window is forwarded to the materializer, which bounds the batch it reads; it filters rows without pruning chunks against this view, so it reduces work done per run, not the chunks scanned. Returns position_state rows appended.';

INSERT INTO migrations (filename) VALUES ('20260908_120000_materialize_aave_lending.sql') ON CONFLICT (filename) DO NOTHING;
