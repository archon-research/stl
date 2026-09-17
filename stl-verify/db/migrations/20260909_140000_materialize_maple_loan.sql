-- migrate: no-transaction
-- VEC-405: project Maple Open Term Loan state onto the position spine. One loan is one position: the
-- borrower's outstanding principal in that loan contract, held by the borrower's address.

CREATE OR REPLACE VIEW public.position_maple_loan AS
WITH cycle AS (
    SELECT s.maple_loan_id, s.synced_at, s.principal_owed, s.processing_version,
           l.chain_id, l.protocol_id, l.loan_address, l.borrower_user_id, l.maple_pool_id
    FROM public.maple_loan_state s
    JOIN public.maple_loan l ON l.id = s.maple_loan_id
), last_seen AS (
    SELECT maple_loan_id, chain_id, protocol_id, loan_address, borrower_user_id, maple_pool_id,
           max(synced_at) AS last_synced_at
    FROM cycle GROUP BY 1, 2, 3, 4, 5, 6
), closed AS (
    SELECT ls.maple_loan_id, c.synced_at, 0::numeric AS principal_owed, 0 AS processing_version,
           ls.chain_id, ls.protocol_id, ls.loan_address, ls.borrower_user_id, ls.maple_pool_id
    FROM last_seen ls
    -- A cycle that places at the last sighting's block would lose the close to it in the collapse below.
    CROSS JOIN LATERAL (
        SELECT m.block_timestamp FROM public.block_meta_surviving m
        WHERE m.chain_id = ls.chain_id AND m.block_timestamp > ls.last_synced_at
        ORDER BY m.block_timestamp LIMIT 1) nb
    CROSS JOIN LATERAL (
        SELECT ps.synced_at FROM public.maple_pool_state ps
        WHERE ps.maple_pool_id = ls.maple_pool_id AND ps.synced_at >= nb.block_timestamp
          AND NOT EXISTS (SELECT 1 FROM public.maple_pool_state p2
                           WHERE p2.maple_pool_id = ps.maple_pool_id AND p2.synced_at = ps.synced_at
                             AND p2.processing_version > ps.processing_version)
          AND ps.principal_out = (
              SELECT coalesce(sum(x.principal_owed), 0) FROM (
                  SELECT DISTINCT ON (s.maple_loan_id) s.principal_owed
                  FROM public.maple_loan pl
                  JOIN public.maple_loan_state s ON s.maple_loan_id = pl.id AND s.synced_at = ps.synced_at
                  WHERE pl.maple_pool_id = ps.maple_pool_id
                  ORDER BY s.maple_loan_id, s.processing_version DESC) x)
        ORDER BY ps.synced_at LIMIT 1) c
), placed AS (
    SELECT DISTINCT ON (c.maple_loan_id, b.block_number, b.block_version)
           c.chain_id, c.protocol_id, c.loan_address, c.borrower_user_id,
           c.principal_owed, c.processing_version, c.maple_loan_id,
           b.block_number, b.block_version, b.block_timestamp
    FROM (SELECT * FROM cycle UNION ALL SELECT * FROM closed) c
    -- LEFT: an unplaceable cycle reaches the materializer as a NULL block and is refused there.
    LEFT JOIN LATERAL (
        SELECT m.block_timestamp, m.block_number, m.block_version
        FROM public.block_meta_surviving m
        WHERE m.chain_id = c.chain_id AND m.block_timestamp <= c.synced_at
        ORDER BY m.block_timestamp DESC, m.block_number DESC
        LIMIT 1) b ON true
    ORDER BY c.maple_loan_id, b.block_number, b.block_version, c.synced_at, c.processing_version DESC
)
SELECT p.chain_id,
       p.protocol_id,
       encode(p.loan_address, 'hex')  AS instrument_key,
       encode(u.address, 'hex')       AS holder_id,
       p.principal_owed               AS quantity,
       'BORROW'::text                 AS deal_type,
       p.block_number,
       p.block_version,
       p.processing_version,
       p.block_timestamp
FROM placed p
JOIN public."user" u ON u.id = p.borrower_user_id;

COMMENT ON VIEW public.position_maple_loan IS '[Operational] VEC-405 projection: Maple Open Term Loan state as native position rows, one observation per (loan, resolved block_number, block_version). instrument_key is the loan contract address as hex, the bare native id its sibling projections use; holder_id is the borrower''s address; quantity is principal_owed, a raw integer in the POOL asset''s native decimals (maple_loan.maple_pool_id -> maple_pool.asset_token_id -> token.decimals); deal_type is BORROW, because the holder is the borrower and the quantity is what they owe. The source carries no block, so each cycle is placed at the last surviving (highest block_version, then processing_version) block_meta block at or before its synced_at and takes that block''s timestamp. Cycles resolving to one block collapse to the earliest synced_at, carrying that cycle''s highest processing_version, so a later reading inside the same block window appears at no block. A repaid loan is closed from its absence at a COMPLETE cycle of its pool: one whose maple_pool_state.principal_out, fetched from a separate endpoint, equals the sum of principal_owed over the loans that cycle reported for the pool. A truncated fetch that drops a loan owing anything fails that equality and closes nothing, so a close is not retracted by the loan reappearing; several loans repaying in one cycle all close. The close is placed at the first complete cycle at or after the first surviving block later than the last sighting, so it never shares that sighting''s block. Loans outside maple_loan_state that count toward principal_out would make every cycle incomplete and stop closes, not falsify them; materialize_maple_loan warns naming each loan left open that way. Emits the shared position_state column contract; closure is applied by materialize_position_projection().';

-- An older argument list beside this one makes a call that omits trailing arguments ambiguous.
DROP FUNCTION IF EXISTS public.materialize_maple_loan(integer);
DROP FUNCTION IF EXISTS public.materialize_maple_loan(integer, interval);
DROP FUNCTION IF EXISTS public.materialize_maple_loan(integer, interval, bigint);
DROP FUNCTION IF EXISTS public.materialize_maple_loan(integer, interval, bigint, interval);

CREATE OR REPLACE FUNCTION public.materialize_maple_loan(p_build_id integer DEFAULT 0,
                                                         p_run_id bigint DEFAULT NULL,
                                                         p_window interval DEFAULT NULL,
                                                         p_max_skew interval DEFAULT INTERVAL '10 minutes') RETURNS bigint
    LANGUAGE plpgsql
    SET search_path = ''
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_since timestamptz := CASE WHEN p_window IS NULL THEN '-infinity'::timestamptz ELSE now() - p_window END;
    v_bad text;
    v_chains integer;
    v_appended bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'materialize_position_projection.' || format('%I.%I', n.nspname, c.relname), 0))
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = 'public.position_maple_loan'::regclass;

    -- Adjacent pairs find every inversion, because an out-of-order sequence inverts somewhere adjacent.
    SELECT max(total), string_agg(msg, '; ' ORDER BY msg) INTO v_chains, v_bad FROM (
        SELECT msg, count(*) OVER () AS total FROM (
            SELECT format('chain %s: block %s at %s precedes block %s at %s', p.chain_id,
                          p.block_number, p.block_timestamp, p.prev_number, p.prev_timestamp) AS msg
            FROM (SELECT s.chain_id, s.block_number, s.block_timestamp,
                         lag(s.block_number)    OVER w AS prev_number,
                         lag(s.block_timestamp) OVER w AS prev_timestamp
                  FROM public.block_meta_surviving s
                  WHERE EXISTS (SELECT 1 FROM public.maple_loan l WHERE l.chain_id = s.chain_id)
                  WINDOW w AS (PARTITION BY s.chain_id ORDER BY s.block_number)) p
            WHERE p.prev_timestamp IS NOT NULL AND p.block_timestamp < p.prev_timestamp) q
        ORDER BY msg
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: block_meta header times invert against height at % pair(s), so a placement would be wrong; fix the mis-parsed rows first (first 5): %', v_chains, v_bad;
    END IF;

    -- Loan cycles place positives and pool cycles place closes, so both are checked.
    SELECT count(*), string_agg(msg, '; ' ORDER BY msg) INTO v_chains, v_bad FROM (
        SELECT format('chain %s: %s', i.chain_id, concat_ws(', and ',
                 CASE WHEN NOT EXISTS (SELECT 1 FROM public.block_meta e WHERE e.chain_id = i.chain_id)
                   THEN 'block_meta holds no blocks for this chain; load it before running'
                 WHEN count(*) FILTER (WHERE b.block_timestamp IS NULL) > 0
                   THEN format('%s cycle(s) that no surviving block precedes, earliest at %s; backfill earlier blocks',
                               count(*) FILTER (WHERE b.block_timestamp IS NULL),
                               min(i.synced_at) FILTER (WHERE b.block_timestamp IS NULL)) END,
                 CASE WHEN count(b.block_timestamp) > 0
                   THEN format('%s cycle(s) stale by up to %s, earliest at %s; densify blocks around them',
                               count(b.block_timestamp), max(i.synced_at - b.block_timestamp),
                               min(i.synced_at) FILTER (WHERE b.block_timestamp IS NOT NULL)) END)) AS msg
        FROM (SELECT l.chain_id, s.synced_at
                FROM public.maple_loan_state s JOIN public.maple_loan l ON l.id = s.maple_loan_id
               WHERE s.synced_at > v_since
              UNION
              SELECT l.chain_id, ps.synced_at
                FROM public.maple_pool_state ps JOIN public.maple_loan l ON l.maple_pool_id = ps.maple_pool_id
               WHERE ps.synced_at > v_since
                 AND EXISTS (SELECT 1 FROM public.maple_loan_state s
                              WHERE s.maple_loan_id = l.id AND s.synced_at < ps.synced_at)) i
        LEFT JOIN LATERAL (
            SELECT m.block_timestamp FROM public.block_meta_surviving m
            WHERE m.chain_id = i.chain_id AND m.block_timestamp <= i.synced_at
            ORDER BY m.block_timestamp DESC LIMIT 1) b ON true
        WHERE b.block_timestamp IS NULL OR i.synced_at - b.block_timestamp > p_max_skew
        GROUP BY i.chain_id) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: % chain(s) have cycles that cannot be placed within % of a block, so their history would start late or be back-dated: %', v_chains, p_max_skew, v_bad;
    END IF;

    -- position_state requires a 40-hex holder_id and position_key() a non-blank instrument_key.
    SELECT max(total), string_agg(msg, '; ' ORDER BY msg) INTO v_chains, v_bad FROM (
        SELECT msg, count(*) OVER () AS total FROM (
            SELECT format('loan %s (chain %s) has a %s-byte borrower address', l.id, l.chain_id, length(u.address)) AS msg
            FROM public.maple_loan l
            JOIN public."user" u ON u.id = l.borrower_user_id
            WHERE length(u.address) <> 20
            UNION ALL
            SELECT format('loan %s (chain %s) has a %s-byte loan address', l.id, l.chain_id, length(l.loan_address))
            FROM public.maple_loan l
            WHERE length(l.loan_address) <> 20
        ) all_msgs
        ORDER BY msg
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: % address(es) are not 20-byte EVM addresses, so the identities they key would be malformed (first 5): %', v_chains, v_bad;
    END IF;

    SELECT max(total), string_agg(msg, '; ' ORDER BY msg) INTO v_chains, v_bad FROM (
        SELECT format('%s x%s, earliest at %s', s.state, count(*), min(s.synced_at)) AS msg, count(*) OVER () AS total
        FROM public.maple_loan_state s
        WHERE s.state <> 'Active' AND s.synced_at > v_since
        GROUP BY s.state
        ORDER BY msg
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: maple_loan_state holds % state(s) this projection cannot classify as an open BORROW (first 5): %', v_chains, v_bad;
    END IF;

    v_appended := public.materialize_position_projection('public.position_maple_loan'::regclass, p_build_id, p_run_id, p_window);

    -- A pool whose principal_out never matches its loans closes nothing, so an open position outliving its pool is named.
    SELECT count(*), string_agg(msg, '; ' ORDER BY msg) INTO v_chains, v_bad FROM (
        SELECT format('loan %s (chain %s) open at %s, last seen %s, pool reported through %s',
                      l.id, l.chain_id, pc.quantity, ls.last_synced_at, pn.newest) AS msg
        FROM public.maple_loan l
        CROSS JOIN LATERAL (SELECT max(s.synced_at) AS last_synced_at FROM public.maple_loan_state s
                             WHERE s.maple_loan_id = l.id) ls
        CROSS JOIN LATERAL (SELECT max(ps.synced_at) AS newest FROM public.maple_pool_state ps
                             WHERE ps.maple_pool_id = l.maple_pool_id) pn
        JOIN public.position_current pc ON pc.projection = 'public.position_maple_loan'
                                       AND pc.instrument_key = encode(l.loan_address, 'hex') AND pc.quantity > 0
        WHERE pn.newest > ls.last_synced_at + INTERVAL '1 day') z;
    IF v_chains > 0 THEN
        RAISE WARNING 'materialize_maple_loan: % loan(s) are still open more than a day after their last sighting, because no later cycle of their pool is complete: %', v_chains, v_bad;
    END IF;
    RETURN v_appended;
END
$fn$;

COMMENT ON FUNCTION public.materialize_maple_loan(integer, bigint, interval, interval) IS '[Operational] VEC-405: materialize Maple loan state into position_state via materialize_position_projection(position_maple_loan). Takes that function''s advisory lock first, then refuses on four conditions: block_meta header times that invert against height, counting every inverted pair and naming up to five; loan or pool cycles that no surviving block precedes, or that sit further than p_max_skew from the block they resolve to, naming every offending chain and saying when a chain has no blocks at all; a borrower or loan address that is not 20 bytes, counting every one and naming up to five; and source states other than Active, counting every state and naming up to five. After appending, it warns naming every loan still open more than a day after its last sighting while its pool kept reporting, since such a pool has no complete cycle to close it from. Widen p_max_skew only deliberately, since it is the only bound on placement error. The cycle and state checks read only rows with synced_at after now() - p_window, which is every row that can place inside the window, so a bounded run ignores older offenders; an unbounded run still refuses on them, since maple_loan_state cannot be deleted from. COST: the inversion and address checks read all of block_meta and maple_loan on every call, and the cycle check probes block_meta once per cycle in the window. The advisory lock excludes other runs, not the Maple indexer, so a cycle committed after the checks is read by the run unchecked, and the next run names it. Idempotent for a FIXED block_meta: a block added later closer to a cycle re-places it and appends a second observation, so block_meta must be complete for a chain''s range before this is run over it. Returns rows appended. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2). p_window is forwarded to the materializer, which bounds the batch it reads.';

INSERT INTO migrations (filename) VALUES ('20260909_140000_materialize_maple_loan.sql') ON CONFLICT (filename) DO NOTHING;
