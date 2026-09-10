-- VEC-404: project the Aave-family lending ledgers (borrower, borrower_collateral) onto the position
-- spine. Debt rows become BORROW observations keyed on the reserve's variable-debt token; supply rows
-- become COLLATERAL (collateral_enabled) or LOAN observations keyed on the reserve's receipt (aToken).

-- Reserves the projection cannot key, recorded per run instead of failing it: a reserve with no token
-- mapping has no native instrument, and minting a surrogate would change its position_id the day the
-- real mapping arrives, orphaning rows the spine cannot delete. Append-only, so the gap has a history.
CREATE TABLE IF NOT EXISTS aave_unmapped_reserve (
    protocol_id  bigint      NOT NULL REFERENCES protocol (id),
    token_id     bigint      NOT NULL REFERENCES token (id),
    reason       text        NOT NULL,
    observations bigint      NOT NULL,
    build_id     integer     NOT NULL,
    run_id       bigint,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT aave_unmapped_reserve_pkey PRIMARY KEY (protocol_id, token_id, reason, created_at),
    CONSTRAINT aave_unmapped_reserve_reason_chk
        CHECK (reason IN ('no_variable_debt_token', 'no_receipt_token')),
    CONSTRAINT aave_unmapped_reserve_observations_chk CHECK (observations > 0)
);

COMMENT ON TABLE aave_unmapped_reserve IS '[Operational] VEC-404: one row per materialize_aave_lending() run per reserve the projection skipped because no token mapping resolves it. The tag is reason, which also names the ledger: no_variable_debt_token for a debt reserve (borrower) absent from debt_token or carrying a NULL variable_debt_address, no_receipt_token for a supply reserve (borrower_collateral) absent from receipt_token. observations counts the ledger rows skipped. Append-only: each run appends its own view of the gap, so closing one is visible as its disappearance from later runs rather than as a mutation. A reserve here holds real exposure that position_state does not carry.';
COMMENT ON COLUMN aave_unmapped_reserve.protocol_id IS 'Roles: PK, FK->protocol.id. The lending protocol whose reserve is unmapped.';
COMMENT ON COLUMN aave_unmapped_reserve.token_id IS 'Roles: PK, FK->token.id. The reserve''s underlying token, which is what the mapping is missing for.';
COMMENT ON COLUMN aave_unmapped_reserve.reason IS 'Roles: PK, Derived. The tag, which also names the ledger: no_variable_debt_token (borrower) or no_receipt_token (borrower_collateral).';
COMMENT ON COLUMN aave_unmapped_reserve.observations IS 'Roles: Derived. Ledger rows skipped for this reserve at the time of the run.';
COMMENT ON COLUMN aave_unmapped_reserve.build_id IS 'Roles: Audit. build_registry.id of the run that recorded the gap (0 = pre-tracking).';
COMMENT ON COLUMN aave_unmapped_reserve.run_id IS 'Roles: Audit. writer_run.id of the run that recorded the gap (ADR-0006 §2); NULL means it predates run tracking.';
COMMENT ON COLUMN aave_unmapped_reserve.created_at IS 'Roles: PK, Audit. When the run recorded it. clock_timestamp(), so two runs in one transaction do not collide.';

GRANT SELECT ON aave_unmapped_reserve TO stl_readonly;
GRANT SELECT, INSERT ON aave_unmapped_reserve TO stl_readwrite;
REVOKE UPDATE, DELETE ON aave_unmapped_reserve FROM stl_readwrite;

CREATE OR REPLACE VIEW position_aave_lending AS
WITH debt AS (
    -- created_at is the block header time the tracker stamps. Two builds can resolve two times for one
    -- block, both at processing_version 0; the earliest is the stable pick, as in position_morpho_market.
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
-- A reserve with no token mapping has no native instrument; materialize_aave_lending() records it in
-- aave_unmapped_reserve so the exposure is visible, and the run continues without it.
WHERE s.instrument_key IS NOT NULL;

COMMENT ON VIEW position_aave_lending IS '[Operational] VEC-404 projection: Aave-family lending positions (SparkLend, Aave V2/V3 and forks sharing the borrower ledgers) as native per-instrument position rows. Debt leg: borrower rows -> BORROW, instrument_key = the reserve''s variable-debt token address (debt_token). Supply leg: borrower_collateral rows -> COLLATERAL when collateral_enabled else LOAN, instrument_key = the reserve''s receipt token address (receipt_token). Quantities are the ledger''s point-in-time balances in native decimals; block_timestamp is the ledger''s created_at, which the tracker sets to the block header time. Emits the shared position_state column contract consumed by materialize_position_projection(); one row per observation; closure is applied by the materializer. A reserve with no token mapping is skipped and recorded in aave_unmapped_reserve, so the exposure it holds is visible; a reserve mapped ambiguously would key wrongly and refuses the run instead.';

-- Records the reserves it cannot key, then refuses only what would key WRONGLY: an ambiguous mapping
-- or a cross-chain ledger row would mint a colliding or wrong position_id, which the spine cannot undo.
CREATE OR REPLACE FUNCTION materialize_aave_lending(p_build_id integer DEFAULT 0,
                                                    p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT AS $fn$
DECLARE
    v_bad text;
BEGIN
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT msg FROM (
            SELECT format('variable-debt token %s is mapped to %s reserves of protocol_id %s', encode(dt.variable_debt_address, 'hex'), count(*), dt.protocol_id) AS msg
            FROM public.debt_token dt
            WHERE dt.variable_debt_address IS NOT NULL
              AND EXISTS (SELECT 1 FROM public.borrower b WHERE b.protocol_id = dt.protocol_id AND b.token_id = dt.underlying_token_id)
            GROUP BY dt.protocol_id, dt.variable_debt_address
            HAVING count(*) > 1
            UNION ALL
            SELECT format('supply reserve (protocol_id %s, token_id %s) maps to %s receipt tokens', r.protocol_id, r.token_id, count(rt.receipt_token_address))
            FROM (SELECT DISTINCT protocol_id, token_id FROM public.borrower_collateral) r
            JOIN public.receipt_token rt ON rt.protocol_id = r.protocol_id AND rt.underlying_token_id = r.token_id
            GROUP BY r.protocol_id, r.token_id
            HAVING count(*) > 1
            UNION ALL
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
            UNION ALL
            SELECT format('holder %s is a %s-byte address, so holder_id would fail position_state''s 40-hex check', encode(u.address, 'hex'), length(u.address))
            FROM (SELECT DISTINCT user_id FROM public.borrower
                  UNION SELECT DISTINCT user_id FROM public.borrower_collateral) r
            JOIN public."user" u ON u.id = r.user_id
            WHERE length(u.address) <> 20
            UNION ALL
            SELECT format('ledger row (user_id %s, protocol_id %s, token_id %s) mixes chains: holder %s, token %s, protocol %s', r.user_id, r.protocol_id, r.token_id, u.chain_id, t.chain_id, p.chain_id)
            FROM (SELECT DISTINCT user_id, protocol_id, token_id FROM public.borrower
                  UNION SELECT DISTINCT user_id, protocol_id, token_id FROM public.borrower_collateral) r
            JOIN public.protocol p ON p.id = r.protocol_id
            JOIN public."user"   u ON u.id = r.user_id
            JOIN public.token    t ON t.id = r.token_id
            WHERE u.chain_id <> p.chain_id OR t.chain_id <> p.chain_id
        ) all_msgs
        ORDER BY msg
        LIMIT 10
    ) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_aave_lending: inputs that would key wrongly, refusing to run: %', v_bad;
    END IF;

    -- Record this run's gap: a debt reserve with no variable-debt token, a supply reserve with no
    -- receipt token. A NULL address is a gap, never an ambiguous mapping -- SQL groups all NULLs into
    -- one, so counting them as a repeated mapping refused the very runs this table exists to survive.
    INSERT INTO public.aave_unmapped_reserve (protocol_id, token_id, reason, observations, build_id, run_id)
    SELECT b.protocol_id, b.token_id, 'no_variable_debt_token', count(*), p_build_id, p_run_id
    FROM public.borrower b
    LEFT JOIN public.debt_token dt ON dt.protocol_id = b.protocol_id AND dt.underlying_token_id = b.token_id
    WHERE dt.variable_debt_address IS NULL
    GROUP BY b.protocol_id, b.token_id
    UNION ALL
    SELECT c.protocol_id, c.token_id, 'no_receipt_token', count(*), p_build_id, p_run_id
    FROM public.borrower_collateral c
    LEFT JOIN public.receipt_token rt ON rt.protocol_id = c.protocol_id AND rt.underlying_token_id = c.token_id
    WHERE rt.receipt_token_address IS NULL
    GROUP BY c.protocol_id, c.token_id;

    -- A mapping that disappears strands live exposure: the view stops emitting that instrument, the
    -- stored rows keep their last quantity and nothing closes them. Reference data regressing is not a
    -- data conflict, so refuse by name rather than leave the exposure reading as current.
    SELECT string_agg(DISTINCT s.instrument_key, ', ' ORDER BY s.instrument_key) INTO v_bad
    FROM (SELECT DISTINCT ON (p.position_id) p.instrument_key, p.quantity
            FROM public.position_state p
           WHERE p.projection = 'public.position_aave_lending'
           ORDER BY p.position_id, p.block_number DESC, p.block_version DESC,
                    p.processing_version DESC, p.block_timestamp DESC) s
    WHERE s.quantity > 0
      AND NOT EXISTS (SELECT 1 FROM public.position_aave_lending v
                       WHERE v.instrument_key = s.instrument_key);
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_aave_lending: live exposure whose instrument the view no longer emits, so a lost token mapping would strand it; refusing to run: %', v_bad;
    END IF;

    RETURN public.materialize_position_projection('public.position_aave_lending'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_aave_lending(integer, bigint) IS '[Operational] VEC-404: materialize Aave-family lending positions into position_state via materialize_position_projection(position_aave_lending). Records each reserve it cannot key in aave_unmapped_reserve (tagged no_variable_debt_token or no_receipt_token) and projects the rest. Refuses to run, naming up to ten offenders, only for inputs that would key WRONGLY: a reserve mapped to several receipt tokens, one variable-debt token shared across reserves, or a ledger row mixing chains. Idempotent; run out of band. p_build_id is stamped on every appended row (build_registry.id; 0 = pre-tracking). Returns position_state rows appended.';

INSERT INTO migrations (filename) VALUES ('20260908_120000_materialize_aave_lending.sql') ON CONFLICT (filename) DO NOTHING;
