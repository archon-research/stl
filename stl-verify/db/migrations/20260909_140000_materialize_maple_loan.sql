-- migrate: no-transaction
-- VEC-405: project Maple Open Term Loan state onto the position spine. One loan is one position: the
-- borrower's outstanding principal in that loan contract, held by the borrower's address.

-- maple_loan_state carries no block, only the cron cycle's synced_at, so an observation is placed at
-- the last block at or before that instant and takes THAT block's timestamp: a block has one instant,
-- so pairing synced_at with another block's number would break the logical observation key.

-- Reverse lookup for the placement. block_meta carries only its PK, which leads with block_number.
-- Four columns so the pick is an index-only scan: the SELECT list is exactly the trailing three.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
                WHERE c.relname = 'block_meta_chain_time_idx' AND NOT i.indisvalid) THEN
        DROP INDEX block_meta_chain_time_idx;
    END IF;
END $$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS block_meta_chain_time_idx
    ON block_meta (chain_id, block_timestamp DESC, block_number DESC, block_version DESC);

COMMENT ON INDEX block_meta_chain_time_idx IS '[Dimension] Serves the date-or-instant to block-height lookup that position_maple_loan (VEC-405) performs per sync cycle; block_meta''s PK leads with block_number and cannot. Column order matches that view''s ORDER BY so the pick is an index-only scan. Following 20260818_130000''s note on position_state, which defers a reverse-lookup index to the PR of its first consumer.';

CREATE OR REPLACE VIEW position_maple_loan AS
WITH canonical AS (
    -- One row per (chain, height): the surviving block. Mixing reorg versions puts an orphan and its
    -- replacement on one timeline, whose header times run backwards against height, which wedges the
    -- spine's monotonic gate permanently -- position_state has no update channel to repair it.
    SELECT DISTINCT ON (m.chain_id, m.block_number)
           m.chain_id, m.block_number, m.block_version, m.block_timestamp
    FROM block_meta m
    ORDER BY m.chain_id, m.block_number, m.block_version DESC
), cycle AS (
    SELECT s.maple_loan_id, s.synced_at, s.principal_owed, s.processing_version,
           l.chain_id, l.protocol_id, l.loan_address, l.borrower_user_id
    FROM maple_loan_state s
    -- The hub, not maple_loan_current: that view filters is_present, so a delisted registry row would
    -- silently drop a loan's whole history. Delisting a loan does not unmake its past observations.
    JOIN maple_loan l ON l.id = s.maple_loan_id
), instant AS (
    SELECT chain_id, synced_at FROM cycle GROUP BY chain_id, synced_at
), last_seen AS (
    SELECT maple_loan_id, chain_id, protocol_id, loan_address, borrower_user_id,
           max(synced_at) AS last_synced_at
    FROM cycle GROUP BY 1, 2, 3, 4, 5
), closed AS (
    -- Absence IS the close signal and a false zero is permanent here, so the cycle this loan
    -- vanished from must still carry every peer it had; counting peers instead let an origination
    -- refill what a partial fetch emptied. Two limits below, both irreducible from this source.
    SELECT ls.maple_loan_id, i.synced_at, 0::numeric AS principal_owed, 0 AS processing_version,
           ls.chain_id, ls.protocol_id, ls.loan_address, ls.borrower_user_id
    FROM last_seen ls
    CROSS JOIN LATERAL (
        SELECT n.synced_at FROM instant n
        WHERE n.chain_id = ls.chain_id AND n.synced_at > ls.last_synced_at
        ORDER BY n.synced_at
        LIMIT 1) i
    WHERE (SELECT count(*) FROM instant n2
            WHERE n2.chain_id = ls.chain_id AND n2.synced_at > ls.last_synced_at) >= 2
      -- It had peers, so a sole loan on a chain is never closed from absence: its disappearance
      -- leaves no cycle row at all, which is indistinguishable from the fetch having stopped.
      AND EXISTS (SELECT 1 FROM cycle p
                   WHERE p.chain_id = ls.chain_id AND p.synced_at = ls.last_synced_at
                     AND p.maple_loan_id <> ls.maple_loan_id)
      -- And every one of those peers is still reported at the vanishing cycle.
      AND NOT EXISTS (
          SELECT 1 FROM cycle p
           WHERE p.chain_id = ls.chain_id AND p.synced_at = ls.last_synced_at
             AND p.maple_loan_id <> ls.maple_loan_id
             AND NOT EXISTS (SELECT 1 FROM cycle q
                              WHERE q.chain_id = ls.chain_id AND q.synced_at = i.synced_at
                                AND q.maple_loan_id = p.maple_loan_id))
), placed AS (
    -- Many cycles share a block at a 10-minute cadence, so they collapse here and the earliest
    -- synced_at is the stable pick: a later arrival must not move an already-emitted observation.
    SELECT DISTINCT ON (c.maple_loan_id, b.block_number, b.block_version, c.processing_version)
           c.chain_id, c.protocol_id, c.loan_address, c.borrower_user_id,
           c.principal_owed, c.processing_version, c.maple_loan_id,
           b.block_number, b.block_version, b.block_timestamp
    FROM (SELECT * FROM cycle UNION ALL SELECT * FROM closed) c
    -- LEFT, not CROSS: an unplaceable cycle must reach the materializer as a NULL block and be
    -- refused there, on the snapshot it materialized. Dropping it here would start a loan late.
    LEFT JOIN LATERAL (
        SELECT m.block_timestamp, m.block_number, m.block_version
        FROM canonical m
        WHERE m.chain_id = c.chain_id AND m.block_timestamp <= c.synced_at
        ORDER BY m.block_timestamp DESC, m.block_number DESC
        LIMIT 1) b ON true
    ORDER BY c.maple_loan_id, b.block_number, b.block_version, c.processing_version, c.synced_at
)
SELECT p.chain_id,
       p.protocol_id,
       -- The bare native id, matching every sibling projection. Whether a key needs chain qualifying
       -- is one decision for the instrument register (VEC-616), which owns native-key resolution and
       -- already records it; six projections must not answer it six ways.
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
-- A close landing in the block that still carries the loan's last positive needs no filter: that
-- block always holds a pv=0 positive whose synced_at is earlier, so DISTINCT ON drops the close and
-- the absence is simply not yet observable at that grain.
FROM placed p
JOIN "user" u ON u.id = p.borrower_user_id;

COMMENT ON VIEW position_maple_loan IS '[Operational] VEC-405 projection: Maple Open Term Loan state as native position rows, at the grain (loan, resolved block_number, block_version, processing_version). instrument_key is the loan contract address as hex, the bare native id its sibling projections use; chain qualification is the instrument register''s decision (VEC-616), not this view''s; holder_id is the borrower''s address; quantity is principal_owed, a raw integer in the POOL asset''s native decimals (maple_loan.maple_pool_id -> maple_pool.asset_token_id -> token.decimals), which the row itself does not carry; deal_type is BORROW, because the holder is the borrower and the quantity is what they owe. The source carries no block, so each cycle is placed at the last surviving (highest block_version) block_meta block at or before its synced_at and takes that block''s timestamp; reorg versions are collapsed first because a mixed timeline runs header time backwards against height. Cycles sharing a resolved block collapse to one observation, earliest synced_at winning, so any later reading inside that block window is DISCARDED and appears at no block -- the collapse rate is a property of block_meta density, not of this view. A repaid loan is closed from its ABSENCE, which maple_loan_state''s COMMENT defines as no longer active, but only when it had peers, every one of those peers is still reported at the cycle it vanished from, and at least two further cycles passed without it returning. The two-cycle rule is what stops a single bad response; peer retention is what stops a persistent partial fetch, which a count of peers did not, because a concurrent origination refills it. Two cases this cannot resolve, both for want of a completeness signal in the source: two or more loans vanishing in one cycle are never closed and stay open at their last principal, and a truncation that drops only this loan while retaining every peer is indistinguishable from the loan repaying, so it still closes. Emits the shared position_state column contract; closure is applied by materialize_position_projection().';

CREATE OR REPLACE FUNCTION materialize_maple_loan(p_build_id integer DEFAULT 0,
                                                  p_max_skew interval DEFAULT INTERVAL '10 minutes')
    RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
    v_chains integer;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'materialize_position_projection.' || format('%I.%I', n.nspname, c.relname), 0))
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = 'public.position_maple_loan'::regclass;

    -- Block header times rise with height by consensus, so a pair that inverts is a mis-parsed
    -- block_meta row. Unrefused it silently wins the placement, or wedges the spine's gate later.
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT format('chain %s: block %s at %s precedes block %s at %s', a.chain_id,
                      b.block_number, b.block_timestamp, a.block_number, a.block_timestamp) AS msg
        FROM public.block_meta a
        JOIN public.block_meta b ON b.chain_id = a.chain_id AND b.block_number > a.block_number
                                AND b.block_timestamp < a.block_timestamp
        WHERE EXISTS (SELECT 1 FROM public.maple_loan l WHERE l.chain_id = a.chain_id)
          AND NOT EXISTS (SELECT 1 FROM public.block_meta o WHERE o.chain_id = a.chain_id
                           AND o.block_number = a.block_number AND o.block_version > a.block_version)
          AND NOT EXISTS (SELECT 1 FROM public.block_meta o WHERE o.chain_id = b.chain_id
                           AND o.block_number = b.block_number AND o.block_version > b.block_version)
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: block_meta header times invert against height, so a placement would be wrong; fix the mis-parsed rows first (first 5): %', v_bad;
    END IF;

    -- An unplaceable cycle is a different operator action from a stale one: backfill earlier blocks
    -- versus densify around the cycles. Both would otherwise be absorbed silently, so both are named.
    SELECT count(*), string_agg(msg, '; ' ORDER BY msg) INTO v_chains, v_bad FROM (
        SELECT format('chain %s: %s cycle(s) that no block precedes, earliest at %s',
                      l.chain_id, count(*), min(s.synced_at)) AS msg
        FROM public.maple_loan_state s
        JOIN public.maple_loan l ON l.id = s.maple_loan_id
        WHERE NOT EXISTS (SELECT 1 FROM public.block_meta m
                           WHERE m.chain_id = l.chain_id AND m.block_timestamp <= s.synced_at)
        GROUP BY l.chain_id) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: % chain(s) have cycles no block_meta block precedes, so their history would start late; backfill earlier blocks: %', v_chains, v_bad;
    END IF;

    SELECT count(*), string_agg(msg, '; ' ORDER BY msg) INTO v_chains, v_bad FROM (
        SELECT format('chain %s: %s cycle(s) stale by up to %s, earliest at %s',
                      l.chain_id, count(*), max(s.synced_at - b.block_timestamp), min(s.synced_at)) AS msg
        FROM public.maple_loan_state s
        JOIN public.maple_loan l ON l.id = s.maple_loan_id
        CROSS JOIN LATERAL (
            SELECT m.block_timestamp FROM public.block_meta m
            WHERE m.chain_id = l.chain_id AND m.block_timestamp <= s.synced_at
              AND NOT EXISTS (SELECT 1 FROM public.block_meta o WHERE o.chain_id = m.chain_id
                               AND o.block_number = m.block_number AND o.block_version > m.block_version)
            ORDER BY m.block_timestamp DESC LIMIT 1) b
        WHERE s.synced_at - b.block_timestamp > p_max_skew
        GROUP BY l.chain_id) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: block_meta is too sparse near the cycles of % chain(s) (tolerance %), so an observation would be back-dated by the whole gap: %', v_chains, p_max_skew, v_bad;
    END IF;

    -- holder_id must be 40 hex chars for position_state's CHECK. "user" is written by every indexer
    -- and its address is a bare BYTEA, so one bad row would raise on a chunk constraint by name only.
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT format('loan %s (chain %s) has a %s-byte borrower address', l.id, l.chain_id, length(u.address)) AS msg
        FROM public.maple_loan l
        JOIN "user" u ON u.id = l.borrower_user_id
        WHERE length(u.address) <> 20
          AND EXISTS (SELECT 1 FROM public.maple_loan_state s WHERE s.maple_loan_id = l.id)
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: a borrower address is not a 20-byte EVM address, so holder_id would fail position_state''s format check: %', v_bad;
    END IF;

    -- The projection reads principal_owed without reading state, which is safe only while every row
    -- is Active. Refused rather than filtered: a WHERE in the view would drop the row silently.
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT format('%s x%s, earliest at %s', s.state, count(*), min(s.synced_at)) AS msg
        FROM public.maple_loan_state s
        WHERE s.state <> 'Active'
        GROUP BY s.state
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_maple_loan: maple_loan_state holds states this projection cannot classify as an open BORROW: %', v_bad;
    END IF;

    RETURN public.materialize_position_projection('public.position_maple_loan'::regclass, p_build_id);
END
$fn$;

COMMENT ON FUNCTION materialize_maple_loan(integer, interval) IS '[Operational] VEC-405: materialize Maple loan state into position_state via materialize_position_projection(position_maple_loan). Takes that function''s own advisory lock first, derived from the view''s canonical name rather than transcribed, then refuses on either of two block_meta conditions, naming every offending chain: header times that invert against height, which are mis-parsed rows that would place a cycle at the wrong block; cycles that no block precedes, whose history would start late; cycles further than p_max_skew from the block they resolve to, which would be back-dated by the whole gap; source states other than Active, which the projection would otherwise carry through as open borrowings; and a borrower address that is not 20 bytes, which would otherwise surface only as a chunk CHECK violation. Widen p_max_skew only deliberately -- it is the only bound on placement error. Idempotent for a FIXED block_meta: a later-arriving block closer to a cycle re-places it and appends a second observation for that cycle, so block_meta must be complete for a chain''s range before this is run over it. Returns rows appended.';

-- No GRANT: ALTER DEFAULT PRIVILEGES (20260122_140100) already grants SELECT on new public views to
-- both app roles, so an explicit one would be a no-op that reads as the enforcement. The view is not
-- auto-updatable (DISTINCT ON, UNION ALL, joins), so a write fails on its shape, not on a privilege.

INSERT INTO migrations (filename) VALUES ('20260909_140000_materialize_maple_loan.sql') ON CONFLICT (filename) DO NOTHING;
