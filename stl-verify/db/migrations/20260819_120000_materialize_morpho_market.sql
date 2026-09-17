-- VEC-402: materialize Morpho market positions into position_state on the native per-instrument grain.
CREATE OR REPLACE VIEW position_morpho_market AS
WITH obs AS (
    -- The earliest timestamp is the stable pick when one block is observed twice.
    SELECT DISTINCT ON (p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version)
           p.user_id, p.morpho_market_id,
           p.block_number, p.block_version, p.processing_version, p.timestamp AS block_timestamp,
           p.supply_assets, p.borrow_assets, p.collateral,
           ct0.address = lt0.address AS same_token
    -- newest_pv per reprocessed observation, taken in the same read: a negative amount a reprocess has
    -- corrected is not an observation, and abs() would store a value that was never held.
    FROM (SELECT p.*, max(p.processing_version) OVER (PARTITION BY p.user_id, p.morpho_market_id, p.block_number,
                                                                    p.block_version, p.timestamp) AS newest_pv
          FROM morpho_market_position p) p
    JOIN morpho_market m ON m.id = p.morpho_market_id
    JOIN token lt0 ON lt0.id = m.loan_token_id
    JOIN token ct0 ON ct0.id = m.collateral_token_id
    WHERE least(p.supply_assets, p.borrow_assets, p.collateral) >= 0 OR p.processing_version = p.newest_pv
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

COMMENT ON VIEW position_morpho_market IS '[Operational] VEC-402 projection: Morpho market positions as native per-instrument position rows (loan-token and collateral-token legs, composite market_id:token key). Emits the shared position_state column contract consumed by materialize_position_projection(); deal_type is LOAN or BORROW on the loan leg by the sign of supply minus borrow, plus the collateral where the collateral token is the loan token (a net-zero row inherits the direction it closes) and COLLATERAL on the collateral leg. quantity is a raw native-decimal integer, not normalised, and the two legs carry DIFFERENT scales: the loan leg is at the loan token''s decimals and the collateral leg at the collateral token''s, each named by the token half of its own instrument_key, so a SUM across legs mixes bases. A market whose collateral token is its loan token emits the loan leg only, netting the collateral in. A negative source amount superseded by a higher processing_version of the same row is not emitted.';

DROP FUNCTION IF EXISTS materialize_morpho_market(integer);
DROP FUNCTION IF EXISTS materialize_morpho_market(integer, bigint);

CREATE OR REPLACE FUNCTION materialize_morpho_market(p_build_id integer DEFAULT 0,
                                                     p_run_id bigint DEFAULT NULL,
                                                     p_window interval DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    -- Pinned to the materializer's own setting, or the check below reads fewer chunks than the run:
    -- morpho_market_position tiers at one year and a database can set tiered reads off.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
    v_since timestamptz;
    v_neg text[] := '{}';
    v_corrected boolean;
    r record;
BEGIN
    IF p_window IS NOT NULL THEN
        v_since := now() - p_window;
        IF NOT isfinite(v_since) OR v_since >= now() THEN
            RAISE EXCEPTION 'materialize_morpho_market: p_window must be a finite positive interval, got %', p_window;
        END IF;
    END IF;

    -- The loan leg takes abs() of the net, so a negative amount would pass as a plausible exposure. Only an
    -- uncorrected one refuses; each bound is a literal, so the scan and each probe prune to their chunks.
    FOR r IN EXECUTE format($q$
        SELECT p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version, p.timestamp,
               format('market %%s user %%s at bn=%%s bv=%%s pv=%%s has a negative source amount: supply=%%s borrow=%%s collateral=%%s',
                      encode(m.market_id, 'hex'), encode(u.address, 'hex'), p.block_number,
                      p.block_version, p.processing_version, p.supply_assets, p.borrow_assets, p.collateral) AS msg
        FROM public.morpho_market_position p
        JOIN public.morpho_market m ON m.id = p.morpho_market_id
        JOIN public."user" u ON u.id = p.user_id
        WHERE least(p.supply_assets, p.borrow_assets, p.collateral) < 0 %s
        ORDER BY msg$q$,
        CASE WHEN p_window IS NULL THEN '' ELSE format('AND p.timestamp > %L::timestamptz', v_since) END)
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

    -- Five per class, or one class's offenders hide another's. These read the (user, market) pairs from
    -- morpho_market_position_current rather than the history.
    SELECT nullif(concat_ws('; ', array_to_string(v_neg, '; '), string_agg(msg, '; ' ORDER BY cls, msg)), '')
      INTO v_bad
    FROM (
        (-- holder_id is the address alone while chain_id comes from the market, so two "user" rows
         -- sharing an address render one position_id and interleave under closure.
         SELECT 2 AS cls, format('market %s is held by %s "user" rows sharing address %s across chains %s',
                          encode(m.market_id, 'hex'), count(DISTINCT u.id), encode(u.address, 'hex'),
                          string_agg(DISTINCT u.chain_id::text, ',' ORDER BY u.chain_id::text)) AS msg
         FROM public.morpho_market_position_current c
         JOIN public.morpho_market m ON m.id = c.morpho_market_id
         JOIN public."user" u ON u.id = c.user_id
         GROUP BY m.id, m.market_id, u.address
         HAVING count(DISTINCT u.id) > 1
         ORDER BY 2 LIMIT 5)
        UNION ALL
        (-- Only holder_id carries position_state's 40-hex check; without this the run aborts on that
         -- CHECK or inside position_key(), naming no row.
         SELECT 3, format('market %s holder %s is a %s-byte address, which cannot render the 40-hex holder_id',
                          encode(m.market_id, 'hex'), encode(u.address, 'hex'), length(u.address))
         FROM public.morpho_market_position_current c
         JOIN public.morpho_market m ON m.id = c.morpho_market_id
         JOIN public."user" u ON u.id = c.user_id
         WHERE length(u.address) <> 20
         GROUP BY m.id, m.market_id, u.address
         ORDER BY 2 LIMIT 5)
        UNION ALL
        (-- The legs split on token addresses, and two token rows share one only across chains, so a token
         -- from another chain would merge two different tokens into one quantity.
         SELECT 4, format('market %s takes its %s token from chain %s but is on chain %s',
                          encode(m.market_id, 'hex'), t.leg, t.chain_id, m.chain_id)
         FROM (SELECT DISTINCT morpho_market_id FROM public.morpho_market_position_current) c
         JOIN public.morpho_market m ON m.id = c.morpho_market_id
         JOIN public.token lt ON lt.id = m.loan_token_id
         JOIN public.token ct ON ct.id = m.collateral_token_id
         CROSS JOIN LATERAL (VALUES ('loan', lt.chain_id), ('collateral', ct.chain_id)) AS t(leg, chain_id)
         WHERE t.chain_id <> m.chain_id
         ORDER BY 2 LIMIT 5)
    ) all_msgs;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_morpho_market: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_morpho_market'::regclass, p_build_id, p_run_id, p_window);
END
$fn$;

COMMENT ON FUNCTION materialize_morpho_market(integer, bigint, interval) IS '[Operational] VEC-402: appends Morpho market position observations into position_state via materialize_position_projection(position_morpho_market). Refuses to run, naming up to five offenders per class: a negative source supply, borrow or collateral amount that is the newest processing_version of its row, which the loan leg''s abs() would launder into a plausible exposure, judged only inside p_window when one is given; one address held by several "user" rows, which renders one position_id since holder_id carries the address alone; a holder address that is not 20 bytes; and a market whose loan or collateral token is on another chain, which the address-keyed legs would merge. The identity checks read the (user, market) pairs from morpho_market_position_current. An invalid p_window is rejected before any check. See that function''s comment for the run contract. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2). p_window is forwarded to the materializer, which bounds the batch it reads; against this view it filters rows without pruning chunks.';

INSERT INTO migrations (filename) VALUES ('20260819_120000_materialize_morpho_market.sql') ON CONFLICT (filename) DO NOTHING;
