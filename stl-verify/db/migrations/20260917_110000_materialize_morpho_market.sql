-- VEC-402: materialize Morpho market positions into position_state on the native per-instrument grain.

-- A (user, market) pair the projection cannot key is withheld, not refused: its rows are recorded and every
-- other position lands, since a registry defect has no append-only repair.
ALTER TABLE position_projection_refusal DROP CONSTRAINT position_projection_refusal_reason_chk;
ALTER TABLE position_projection_refusal ADD CONSTRAINT position_projection_refusal_reason_chk
    CHECK (reason IN ('block_time_inverts_height', 'deal_type_drift', 'observation_drift',
                      'holder_address_shared', 'holder_address_malformed', 'token_on_other_chain'));

CREATE OR REPLACE VIEW morpho_market_withheld_pair AS
SELECT c.user_id, c.morpho_market_id, d.reason, d.detail
FROM (SELECT c.user_id, c.morpho_market_id,
             count(*) OVER (PARTITION BY c.morpho_market_id, u.address) AS holders_on_address
      FROM public.morpho_market_position_current c
      JOIN public."user" u ON u.id = c.user_id) c
JOIN public."user" u ON u.id = c.user_id
JOIN public.morpho_market m ON m.id = c.morpho_market_id
JOIN public.token lt ON lt.id = m.loan_token_id
JOIN public.token ct ON ct.id = m.collateral_token_id
CROSS JOIN LATERAL (VALUES
    ('holder_address_shared', format('market %s on chain %s is held by %s "user" rows sharing address %s',
        encode(m.market_id, 'hex'), m.chain_id, c.holders_on_address, encode(u.address, 'hex')),
     c.holders_on_address > 1),
    ('holder_address_malformed', format('market %s on chain %s holder %s is a %s-byte address',
        encode(m.market_id, 'hex'), m.chain_id, encode(u.address, 'hex'), length(u.address)),
     length(u.address) <> 20),
    ('token_on_other_chain', format('market %s on chain %s holder %s: loan token on chain %s, collateral token on chain %s',
        encode(m.market_id, 'hex'), m.chain_id, encode(u.address, 'hex'), lt.chain_id, ct.chain_id),
     lt.chain_id <> m.chain_id OR ct.chain_id <> m.chain_id)
) AS d(reason, detail, applies)
WHERE d.applies;

COMMENT ON VIEW morpho_market_withheld_pair IS '[Operational] VEC-402: the (user, market) pairs position_morpho_market withholds, one row per reason: holder_address_shared, where several "user" rows share one address in one market and would render one position_id; holder_address_malformed, where the holder address is not 20 bytes and cannot render the 40-hex holder_id; token_on_other_chain, where a leg''s token is not on the market''s chain and the address-keyed legs would merge two tokens. Read from morpho_market_position_current, so a pair missing from that cache is not listed; the view applies the malformed-holder and token checks to every row itself.';

CREATE OR REPLACE VIEW position_morpho_market AS
WITH obs AS (
    -- The earliest timestamp is the stable pick when one block is observed twice.
    SELECT DISTINCT ON (p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version)
           p.user_id, p.morpho_market_id,
           p.block_number, p.block_version, p.processing_version, p.timestamp AS block_timestamp,
           p.supply_assets, p.borrow_assets, p.collateral,
           ct0.address = lt0.address AS same_token
    -- newest_pv is the highest processing_version per (user, market, block, block_version, timestamp); a negative
    -- amount below it has been corrected, and abs() would store a value that was never held.
    FROM (SELECT p.*, max(p.processing_version) OVER (PARTITION BY p.user_id, p.morpho_market_id, p.block_number,
                                                                    p.block_version, p.timestamp) AS newest_pv
          FROM morpho_market_position p) p
    JOIN morpho_market m ON m.id = p.morpho_market_id
    JOIN token lt0 ON lt0.id = m.loan_token_id AND lt0.chain_id = m.chain_id
    JOIN token ct0 ON ct0.id = m.collateral_token_id AND ct0.chain_id = m.chain_id
    JOIN "user" u0 ON u0.id = p.user_id AND length(u0.address) = 20
    WHERE (least(p.supply_assets, p.borrow_assets, p.collateral) >= 0 OR p.processing_version = p.newest_pv)
      AND NOT EXISTS (SELECT 1 FROM morpho_market_withheld_pair w
                       WHERE w.user_id = p.user_id AND w.morpho_market_id = p.morpho_market_id)
    ORDER BY p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version, p.timestamp
),
-- The loan leg's SIGNED exposure. Where the collateral token IS the loan token it is the same native
-- instrument, so it nets here rather than inflating a magnitude the direction below never saw.
netted AS (
    SELECT o.*,
           o.supply_assets - o.borrow_assets
             + CASE WHEN o.same_token THEN o.collateral ELSE 0 END AS signed_net
    FROM obs o
),
-- Loan-leg direction per observation, from the sign of that net; a net-zero row inherits the last known
-- direction, so the closing row of a repaid borrow is BORROW. Closure is applied by the shared materializer.
series AS (
    SELECT n.*,
           abs(n.signed_net) AS net_loan,
           -- Direction of the latest non-zero observation, carried forward. The partition is already
           -- ordered by the array's first three elements, so the greatest array is the latest such row.
           (max(CASE WHEN n.signed_net <> 0
                     THEN ARRAY[n.block_number, n.block_version::bigint,
                                n.processing_version::bigint, (n.signed_net > 0)::int::bigint]
                END) OVER (PARTITION BY n.user_id, n.morpho_market_id
                           ORDER BY n.block_number, n.block_version, n.processing_version
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))[4] = 1 AS is_loan
    FROM netted n
),
legs AS (
    -- Where the collateral token IS the loan token both legs would key one position_id, so only the
    -- loan leg is emitted and the collateral nets into its quantity.
    SELECT s.user_id, s.morpho_market_id,
           encode(m.market_id, 'hex') || ':' || encode(lt.address, 'hex') AS instrument_key,
           s.net_loan AS quantity,
           CASE s.is_loan WHEN true THEN 'LOAN' WHEN false THEN 'BORROW' END AS deal_type,
           s.block_number, s.block_version, s.processing_version, s.block_timestamp
    FROM series s
    JOIN morpho_market m ON m.id = s.morpho_market_id
    JOIN token lt ON lt.id = m.loan_token_id
    UNION ALL
    SELECT s.user_id, s.morpho_market_id,
           encode(m.market_id, 'hex') || ':' || encode(ct.address, 'hex'),
           s.collateral, 'COLLATERAL',
           s.block_number, s.block_version, s.processing_version, s.block_timestamp
    FROM series s
    JOIN morpho_market m ON m.id = s.morpho_market_id
    JOIN token ct ON ct.id = m.collateral_token_id
    JOIN token lt2 ON lt2.id = m.loan_token_id AND lt2.address <> ct.address
)
SELECT m.chain_id, m.protocol_id, l.instrument_key, encode(u.address, 'hex') AS holder_id, l.quantity, l.deal_type,
       l.block_number, l.block_version, l.processing_version, l.block_timestamp
FROM legs l
JOIN morpho_market m ON m.id = l.morpho_market_id
JOIN "user"        u ON u.id = l.user_id;

COMMENT ON VIEW position_morpho_market IS '[Operational] VEC-402 projection: Morpho market positions as native per-instrument position rows (loan-token and collateral-token legs, composite market_id:token key). Emits the shared position_state column contract consumed by materialize_position_projection(); deal_type is COLLATERAL on the collateral leg and LOAN or BORROW on the loan leg, by the sign of supply minus borrow plus, where the collateral token is the loan token, the collateral; a net-zero row inherits the direction it closes. quantity is a raw native-decimal integer, not normalised, and the two legs carry DIFFERENT scales: the loan leg is at the loan token''s decimals and the collateral leg at the collateral token''s, each named by the token half of its own instrument_key, so a SUM across legs mixes bases. A market whose collateral token is its loan token emits the loan leg only, netting the collateral in. A negative source amount is not emitted when a higher processing_version exists for the same user, market, block_number, block_version and timestamp; one with no higher processing_version IS emitted, and on the loan leg abs() turns it into a plausible magnitude, so only materialize_morpho_market(), which refuses it, is a safe reader of an unjudged history. Pairs listed in morpho_market_withheld_pair, and any row whose holder is not 20 bytes or whose market takes a token from another chain, are not emitted.';

CREATE OR REPLACE FUNCTION materialize_morpho_market_refusals(p_since timestamptz DEFAULT NULL) RETURNS text
    LANGUAGE plpgsql
    SET search_path = ''
    -- Pinned to the materializer's own setting, or the checks read fewer chunks than the run:
    -- morpho_market_position tiers at one year and a database can set tiered reads off.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_neg text[] := '{}';
    v_corrected boolean;
    r record;
BEGIN
    -- The loan leg takes abs() of the net, so a negative amount would pass as a plausible exposure. Only an
    -- uncorrected one refuses; the window bound and each probe's timestamp are literals so they prune chunks.
    FOR r IN EXECUTE format($q$
        SELECT p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version, p.timestamp,
               format('market %%s on chain %%s user %%s at bn=%%s bv=%%s pv=%%s has a negative source amount: supply=%%s borrow=%%s collateral=%%s',
                      encode(m.market_id, 'hex'), m.chain_id, encode(u.address, 'hex'), p.block_number,
                      p.block_version, p.processing_version, p.supply_assets, p.borrow_assets, p.collateral) AS msg
        FROM public.morpho_market_position p
        JOIN public.morpho_market m ON m.id = p.morpho_market_id
        JOIN public."user" u ON u.id = p.user_id
        WHERE least(p.supply_assets, p.borrow_assets, p.collateral) < 0 %s
        ORDER BY msg$q$,
        CASE WHEN p_since IS NULL THEN '' ELSE format('AND p.timestamp > %L::timestamptz', p_since) END)
    LOOP
        EXECUTE format($q$
            SELECT EXISTS (SELECT 1 FROM public.morpho_market_position q
                            WHERE q.timestamp = %L::timestamptz AND q.user_id = $1 AND q.morpho_market_id = $2
                              AND q.block_number = $3 AND q.block_version = $4 AND q.processing_version > $5)$q$,
            r.timestamp)
            INTO v_corrected USING r.user_id, r.morpho_market_id, r.block_number, r.block_version, r.processing_version;
        IF NOT v_corrected THEN
            v_neg := v_neg || r.msg;
            EXIT WHEN cardinality(v_neg) = 5;
        END IF;
    END LOOP;
    RETURN nullif(array_to_string(v_neg, '; '), '');
END
$fn$;

COMMENT ON FUNCTION materialize_morpho_market_refusals(timestamptz) IS '[Operational] VEC-402: why materialize_morpho_market() would refuse to run, NULL when it may run, naming up to five negative source supply, borrow or collateral amounts with no higher processing_version for the same user, market, block, block_version and timestamp, judged only for observations after p_since when given. A negative collateral reaches the collateral leg raw and the spine refuses it; a negative supply or borrow, or a collateral netted into a same-token market, reaches the loan leg through abs() and would store a plausible magnitude. Registry defects are not refusals: position_morpho_market withholds those pairs (morpho_market_withheld_pair).';

DROP FUNCTION IF EXISTS materialize_morpho_market(integer);
DROP FUNCTION IF EXISTS materialize_morpho_market(integer, bigint);

CREATE OR REPLACE FUNCTION materialize_morpho_market(p_build_id integer DEFAULT 0,
                                                     p_run_id bigint DEFAULT NULL,
                                                     p_window interval DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path = '' AS $fn$
DECLARE
    v_bad      text;
    v_since    timestamptz;
    v_appended bigint;
    v_withheld bigint;
BEGIN
    IF p_window IS NOT NULL THEN
        v_since := now() - p_window;
        IF NOT isfinite(v_since) OR v_since >= now() THEN
            RAISE EXCEPTION 'materialize_morpho_market: p_window must be a finite positive interval, got %', p_window;
        END IF;
    END IF;
    v_bad := public.materialize_morpho_market_refusals(v_since);
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_morpho_market: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    v_appended := public.materialize_position_projection('public.position_morpho_market'::regclass, p_build_id, p_run_id, p_window);
    -- The check above and the spine's read are separate snapshots under READ COMMITTED. Judging again here
    -- covers everything the spine read, and raising rolls back what it appended.
    v_bad := public.materialize_morpho_market_refusals(v_since);
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_morpho_market: unresolved inputs written during the run, refusing it: %', v_bad;
    END IF;
    INSERT INTO public.position_projection_refusal
        (projection, position_id, block_number, block_version, processing_version, reason, detail, build_id, run_id)
    SELECT 'public.position_morpho_market',
           public.position_id(m.chain_id, m.protocol_id, encode(m.market_id, 'hex') || ':' || encode(lt.address, 'hex'),
                              encode(u.address, 'hex')),
           c.block_number, c.block_version, c.processing_version, w.reason, w.detail, p_build_id, p_run_id
    FROM public.morpho_market_withheld_pair w
    JOIN public.morpho_market_position_current c ON c.user_id = w.user_id AND c.morpho_market_id = w.morpho_market_id
    JOIN public.morpho_market m ON m.id = w.morpho_market_id
    JOIN public.token lt ON lt.id = m.loan_token_id
    JOIN public."user" u ON u.id = w.user_id
    ON CONFLICT DO NOTHING;
    SELECT count(DISTINCT (user_id, morpho_market_id)) INTO v_withheld FROM public.morpho_market_withheld_pair;
    IF v_withheld > 0 THEN
        RAISE WARNING 'materialize_morpho_market: withheld % (user, market) pair(s) it cannot key; see morpho_market_withheld_pair', v_withheld;
    END IF;
    RETURN v_appended;
END
$fn$;

COMMENT ON FUNCTION materialize_morpho_market(integer, bigint, interval) IS '[Operational] VEC-402: appends Morpho market position observations into position_state via materialize_position_projection(position_morpho_market). Rejects an invalid p_window first, refuses to run on a negative amount from materialize_morpho_market_refusals() judged over the window, and judges again after the append in the same transaction, raising so a negative written during the run cannot land. Pairs the view withholds for a registry defect are recorded in position_projection_refusal at their cached latest observation, and every other position lands. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2). p_window is forwarded to the materializer, which bounds the batch it reads; against this view it filters rows without pruning chunks.';

INSERT INTO migrations (filename) VALUES ('20260917_110000_materialize_morpho_market.sql') ON CONFLICT (filename) DO NOTHING;
