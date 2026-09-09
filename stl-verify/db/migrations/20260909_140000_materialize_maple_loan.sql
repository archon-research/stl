-- VEC-405: project Maple Open Term Loan state onto the position spine. One loan is one position: the
-- borrower's outstanding principal in that loan contract, held by the borrower's address.

-- maple_loan_state carries no block, only the cron cycle's synced_at, so the observation is placed at
-- the last block at or before that instant and takes THAT block's timestamp: a block has one instant,
-- so carrying synced_at alongside another block's number would break the spine's key.

-- The first consumer of a block_meta time lookup, so it adds the index for it, per the rule in
-- 20260818_130000: reverse-lookup indexes land with the consumer that needs them, not with the table.
CREATE INDEX IF NOT EXISTS block_meta_chain_time_idx ON block_meta (chain_id, block_timestamp);

COMMENT ON INDEX block_meta_chain_time_idx IS '[Dimension] Serves date-or-instant to block-height lookups (VEC-405, ARCT-225): the PK leads with block_number, so resolving "the block at or before this instant" needs this. Added by its first consumer.';

CREATE OR REPLACE VIEW position_maple_loan AS
WITH placed AS (
    -- One row per (loan, resolved block): ~139 syncs a loan-day resolve to distinct blocks on mainnet,
    -- but any that share a block collapse here, earliest synced_at winning as the stable pick.
    SELECT DISTINCT ON (s.maple_loan_id, b.block_number, b.block_version, s.processing_version)
           l.chain_id, l.protocol_id, l.loan_address, l.borrower_user_id,
           s.principal_owed, s.processing_version,
           b.block_number, b.block_version, b.block_timestamp
    FROM maple_loan_state s
    JOIN maple_loan l ON l.id = s.maple_loan_id
    CROSS JOIN LATERAL (
        SELECT m.block_number, m.block_version, m.block_timestamp
        FROM block_meta m
        WHERE m.chain_id = l.chain_id AND m.block_timestamp <= s.synced_at
        ORDER BY m.block_timestamp DESC, m.block_number DESC, m.block_version DESC
        LIMIT 1) b
    ORDER BY s.maple_loan_id, b.block_number, b.block_version, s.processing_version, s.synced_at
)
SELECT p.chain_id,
       p.protocol_id,
       encode(p.loan_address, 'hex')  AS instrument_key,
       encode(u.address, 'hex')       AS holder_id,
       p.principal_owed               AS quantity,
       -- The holder is the BORROWER and the quantity is what they owe, so this is a BORROW. It is a
       -- LOAN only from the pool's side, which is a different holder and not this projection.
       'BORROW'::text                 AS deal_type,
       p.block_number,
       p.block_version,
       p.processing_version,
       p.block_timestamp
FROM placed p
JOIN "user" u ON u.id = p.borrower_user_id;

COMMENT ON VIEW position_maple_loan IS '[Operational] VEC-405 projection: Maple Open Term Loan state as native position rows, one per (loan, resolved block). instrument_key = the loan contract address; holder_id = the borrower''s address; quantity = principal_owed in the loan asset''s native decimals; deal_type = BORROW, because the holder is the borrower and the quantity is what they owe. The source carries no block, so each cycle is placed at the last block_meta block at or before its synced_at and takes that block''s timestamp, which is why block_meta must be populated before this projection means anything. KNOWN LIMIT: the source only ever reports state Active with a non-zero principal, so a repaid loan simply stops being reported and its position NEVER receives a closing zero — it stays open at its last principal. Closing those needs a source change or an absence-derived close; staleness meanwhile is answered from position_projection_run. Emits the shared position_state column contract; closure is applied by materialize_position_projection().';

-- Refuses when a cycle cannot be placed: block_meta has no block at or before it, so the LATERAL drops
-- the row and the loan's history would silently start late. Names the earliest such cycle per chain.
CREATE OR REPLACE FUNCTION materialize_maple_loan(p_build_id integer DEFAULT 0) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('materialize_position_projection.public.position_maple_loan', 0));
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT format('chain %s has %s Maple cycle(s) before its earliest block_meta block, from %s', l.chain_id, count(*), min(s.synced_at)) AS msg
        FROM public.maple_loan_state s
        JOIN public.maple_loan l ON l.id = s.maple_loan_id
        WHERE NOT EXISTS (SELECT 1 FROM public.block_meta m
                           WHERE m.chain_id = l.chain_id AND m.block_timestamp <= s.synced_at)
        GROUP BY l.chain_id
        ORDER BY l.chain_id
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: cycles that no block_meta block precedes, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_maple_loan'::regclass, p_build_id);
END
$fn$;

COMMENT ON FUNCTION materialize_maple_loan(integer) IS '[Operational] VEC-405: materialize Maple loan state into position_state via materialize_position_projection(position_maple_loan). Refuses to run, naming the chain and the earliest offending cycle, when block_meta holds no block at or before a cycle''s synced_at, since the projection would otherwise start that loan''s history late. Takes the materializer''s own advisory lock first. Idempotent; run out of band. Returns rows appended.';

GRANT SELECT ON position_maple_loan TO stl_readonly;
GRANT SELECT ON position_maple_loan TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260909_140000_materialize_maple_loan.sql') ON CONFLICT (filename) DO NOTHING;
