-- VEC-407: project Prime ALM allocations onto the position spine. A prime's holding of one token at one
-- of its proxy contracts is a position: instrument_key = proxy address ':' token address, holder = the
-- prime's vault address, quantity = the on-chain balanceOf reading.

-- protocol_id is NULL with chain_id SET, the first projection to do so: a token held at the prime's own
-- proxy has no protocol contract between holder and instrument, so no protocol row to point at.
-- Both spine columns are independently nullable; the NULL-pairing convention covers off-chain sources.
CREATE OR REPLACE VIEW position_prime_allocation AS
WITH obs AS (
    -- balance is a post-transaction balanceOf reading, not a delta, so the LAST event in a block is
    -- that block's balance. tx_amount and direction are cash-flow detail and are deliberately not read.
    SELECT DISTINCT ON (ap.chain_id, ap.token_id, ap.prime_id, ap.proxy_address,
                        ap.block_number, ap.block_version, ap.processing_version)
           ap.chain_id, ap.token_id, ap.prime_id, ap.proxy_address,
           ap.block_number, ap.block_version, ap.processing_version,
           ap.created_at AS block_timestamp,
           ap.balance
    FROM allocation_position ap
    ORDER BY ap.chain_id, ap.token_id, ap.prime_id, ap.proxy_address,
             ap.block_number, ap.block_version, ap.processing_version,
             -- Every remaining PK column, in allocation_position_current's rank order: created_at
             -- (this view's block_timestamp) above log_index, then direction and tx_hash, which settle
             -- the sweep/event tie at log_index 0. Leaving one free made the pick ingest-order bound.
             ap.created_at DESC, ap.log_index DESC, ap.direction DESC, ap.tx_hash DESC
)
SELECT o.chain_id,
       NULL::bigint                                                        AS protocol_id,
       encode(o.proxy_address, 'hex') || ':' || encode(t.address, 'hex')   AS instrument_key,
       encode(pr.vault_address, 'hex')                                     AS holder_id,
       o.balance                                                           AS quantity,
       'ALLOCATION'::text                                                  AS deal_type,
       o.block_number,
       o.block_version,
       o.processing_version,
       o.block_timestamp
FROM obs o
JOIN token t  ON t.id = o.token_id
JOIN prime pr ON pr.id = o.prime_id;

COMMENT ON VIEW position_prime_allocation IS '[Operational] VEC-407 projection: Prime ALM allocations as native position rows, one per (prime, proxy, token, block). instrument_key = proxy address '':'' token address, because a prime holds the same token at several proxies and each is a distinct holding; holder_id = the prime''s vault address; quantity = allocation_position.balance, the post-transaction balanceOf reading, decimals-normalised by the writer rather than raw. protocol_id is NULL with chain_id set: a token at the prime''s own proxy has no protocol contract in between. deal_type is ALLOCATION; direction and tx_amount are cash-flow detail and are not read. The last event in a block supplies that block''s balance -- balanceOf at that block''s hash. A block read while only partly indexed stores a partial balance; the balance stops drifting once the writer replays with a higher processing_version, and until then materialize_position_projection records the observation_drift on the next run rather than correcting the stored row (the tie between the block''s log-index-0 sweep and log-index-0 event does not correct it either: both read balanceOf at the same block hash and carry the same balance). Emits the shared position_state column contract; closure is applied by materialize_position_projection().';

-- Thin per-projection entry point for the runner's POSITION_PROJECTIONS list; the view above holds all
-- the allocation logic. No pre-check: token_id and prime_id are FK-enforced and both token.address and
-- prime.vault_address are NOT NULL, so the view's joins cannot drop a row.
-- Ordered after the spine's own migration on purpose: this wrapper is LANGUAGE sql, so its body is
-- parsed when it is created, and the three-argument materialize_position_projection has to exist by
-- then.

-- An empty search_path, not FROM CURRENT: that captures '"$user", public', which still resolves per
-- role at call time and so keeps the shadowing hazard it looks like it removes. Both references are
-- schema-qualified and regclass resolves through implicit pg_catalog, so nothing needs a path.
-- Dropped rather than replaced: keeping the old argument list beside the new one makes a
-- call that omits the run ambiguous, as it did for the spine.
DROP FUNCTION IF EXISTS materialize_prime_allocation(integer);

CREATE OR REPLACE FUNCTION materialize_prime_allocation(p_build_id integer DEFAULT 0,
                                                        p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE sql
    SET search_path = '' AS $fn$
    SELECT public.materialize_position_projection('public.position_prime_allocation'::regclass, p_build_id, p_run_id);
$fn$;

COMMENT ON FUNCTION materialize_prime_allocation(integer, bigint) IS '[Operational] VEC-407: appends Prime ALM allocation observations into position_state via materialize_position_projection(position_prime_allocation). See that function''s comment for the run contract. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

INSERT INTO migrations (filename) VALUES ('20260910_130000_materialize_prime_allocation.sql') ON CONFLICT (filename) DO NOTHING;
