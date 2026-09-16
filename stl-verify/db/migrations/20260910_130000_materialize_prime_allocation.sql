-- VEC-407: project Prime ALM allocations onto the position spine.
CREATE OR REPLACE VIEW position_prime_allocation AS
WITH obs AS (
    -- balance is a post-transaction reading, not a delta, so the latest reading in a block is that
    -- block's balance and summing tx_amount would double-count against it.
    SELECT DISTINCT ON (ap.chain_id, ap.token_id, ap.prime_id, ap.proxy_address,
                        ap.block_number, ap.block_version, ap.processing_version)
           ap.chain_id, ap.token_id, ap.prime_id, ap.proxy_address,
           ap.block_number, ap.block_version, ap.processing_version,
           ap.created_at AS block_timestamp,
           ap.balance
    FROM allocation_position ap
    ORDER BY ap.chain_id, ap.token_id, ap.prime_id, ap.proxy_address,
             ap.block_number, ap.block_version, ap.processing_version,
             -- Every remaining PK column, in allocation_position_current's rank order, so the pick is
             -- total over the source key rather than ingest-order bound.
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

COMMENT ON VIEW position_prime_allocation IS '[Operational] VEC-407 projection: Prime ALM allocations as native position rows, one per (prime, proxy, token, block, block_version, processing_version). instrument_key = proxy address '':'' token address, because a prime holds the same token at several proxies and each is a distinct holding; holder_id = the prime''s vault address; quantity = allocation_position.balance, decimals-normalised by the writer into whole units, never raw. For erc20/atoken/erc4626/curve rows that is the proxy''s post-transaction balanceOf, denominated in the token named by instrument_key''s second half. For uni_v3_pool/uni_v3_lp rows no balanceOf exists and balance is the tracker-computed full position value denominated in the entry''s HINT asset, which is NOT that token, so a SUM across both classes mixes bases. protocol_id is NULL with chain_id set: a token at the prime''s own proxy has no protocol contract in between. deal_type is ALLOCATION; tx_amount is cash-flow detail and is not read. The latest created_at in a block, then the highest log_index, supplies that block''s balance, read at that block''s hash. A block read while only partly indexed stores a partial balance; the balance stops drifting once the writer replays with a higher processing_version, and until then materialize_position_projection records the observation_drift on the next run rather than correcting the stored row (the tie between the block''s log-index-0 sweep and log-index-0 event does not correct it either: both read balanceOf at the same block hash and carry the same balance). Emits the shared position_state column contract; closure is applied by materialize_position_projection().';

-- search_path is empty, so every reference below is schema-qualified.
DROP FUNCTION IF EXISTS materialize_prime_allocation(integer);

CREATE OR REPLACE FUNCTION materialize_prime_allocation(p_build_id integer DEFAULT 0,
                                                        p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path = ''
    -- Pinned to the materializer's own setting, so the checks below cannot read fewer chunks than the
    -- run does on an instance where the default is off.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT msg FROM (
            -- The prime is in holder_id only, never in instrument_key, so two primes on one proxy are
            -- two positions carrying one proxy's balance -- a double count the spine cannot see.
            SELECT format('chain %s proxy %s token %s is held by %s primes (%s)', ap.chain_id,
                          encode(ap.proxy_address, 'hex'), encode(t.address, 'hex'),
                          count(DISTINCT ap.prime_id),
                          string_agg(DISTINCT encode(pr.vault_address, 'hex'), ',')) AS msg
            FROM public.allocation_position ap
            JOIN public.token t  ON t.id = ap.token_id
            JOIN public.prime pr ON pr.id = ap.prime_id
            GROUP BY ap.chain_id, ap.proxy_address, t.address
            HAVING count(DISTINCT ap.prime_id) > 1
            UNION ALL
            -- Neither prime.vault_address nor token.address carries an octet_length CHECK, and a blank
            -- one aborts inside position_key() naming no row.
            SELECT format('chain %s proxy %s: holder %s is %s bytes and token %s is %s bytes', ap.chain_id,
                          encode(ap.proxy_address, 'hex'), encode(pr.vault_address, 'hex'),
                          length(pr.vault_address), encode(t.address, 'hex'), length(t.address))
            FROM public.allocation_position ap
            JOIN public.token t  ON t.id = ap.token_id
            JOIN public.prime pr ON pr.id = ap.prime_id
            WHERE length(pr.vault_address) <> 20 OR length(t.address) = 0 OR length(ap.proxy_address) = 0
            GROUP BY ap.chain_id, ap.proxy_address, pr.vault_address, t.address
            UNION ALL
            -- token is unique per (chain_id, address), so a row whose chain disagrees with its token's
            -- renders the same instrument_key as the row on the other chain and collides on one key.
            SELECT format('chain %s proxy %s token_id %s is registered on chain %s', ap.chain_id,
                          encode(ap.proxy_address, 'hex'), ap.token_id, t.chain_id)
            FROM public.allocation_position ap
            JOIN public.token t ON t.id = ap.token_id
            WHERE t.chain_id <> ap.chain_id
            GROUP BY ap.chain_id, ap.proxy_address, ap.token_id, t.chain_id
        ) all_msgs
        ORDER BY msg
        LIMIT 5) worst_five;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_prime_allocation: inputs that would key wrongly, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_prime_allocation'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_prime_allocation(integer, bigint) IS '[Operational] VEC-407: appends Prime ALM allocation observations into position_state via materialize_position_projection(position_prime_allocation). Refuses to run, naming up to five offenders, when one (chain, proxy, token) is held by several primes, since the prime is carried by holder_id alone and both rows would store the same proxy balance, when a row''s chain_id disagrees with its token''s, since both render one instrument_key, or when a vault, token or proxy address cannot render a usable identity. See that function''s comment for the run contract. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

INSERT INTO migrations (filename) VALUES ('20260910_130000_materialize_prime_allocation.sql') ON CONFLICT (filename) DO NOTHING;
