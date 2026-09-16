-- VEC-402: materialize Morpho market positions into position_state on the native per-instrument grain.
CREATE OR REPLACE VIEW position_morpho_market AS
WITH obs AS (
    -- The earliest timestamp is the stable pick when one block is observed twice.
    SELECT DISTINCT ON (p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version)
           p.user_id, p.morpho_market_id,
           p.block_number, p.block_version, p.processing_version, p.timestamp AS block_timestamp,
           p.supply_assets, p.borrow_assets, p.collateral,
           ct0.address = lt0.address AS same_token
    FROM morpho_market_position p
    JOIN morpho_market m ON m.id = p.morpho_market_id
    JOIN token lt0 ON lt0.id = m.loan_token_id
    JOIN token ct0 ON ct0.id = m.collateral_token_id
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

COMMENT ON VIEW position_morpho_market IS '[Operational] VEC-402 projection: Morpho market positions as native per-instrument position rows (loan-token and collateral-token legs, composite market_id:token key). Emits the shared position_state column contract consumed by materialize_position_projection(); deal_type is LOAN or BORROW on the loan leg by the sign of supply minus borrow (a net-zero row inherits the direction it closes) and COLLATERAL on the collateral leg. quantity is a raw native-decimal integer, not normalised, and the two legs carry DIFFERENT scales: the loan leg is at the loan token''s decimals and the collateral leg at the collateral token''s, each named by the token half of its own instrument_key, so a SUM across legs mixes bases. A market whose collateral token is its loan token emits the loan leg only, netting the collateral in.';

-- Refuses a negative source amount first: the loan leg takes abs() of the net, so a negative supply or
-- borrow is laundered into a plausible exposure the spine's negative-quantity check never sees.
DROP FUNCTION IF EXISTS materialize_morpho_market(integer);

CREATE OR REPLACE FUNCTION materialize_morpho_market(p_build_id integer DEFAULT 0,
                                                     p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    -- Pinned to the materializer's own setting, or the check below reads fewer chunks than the run:
    -- morpho_market_position tiers at one year and tiered reads default off.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT msg FROM (
            SELECT format('market %s user %s at bn=%s bv=%s pv=%s has a negative source amount: supply=%s borrow=%s collateral=%s',
                          encode(m.market_id, 'hex'), encode(u.address, 'hex'), p.block_number,
                          p.block_version, p.processing_version, p.supply_assets, p.borrow_assets, p.collateral) AS msg
            FROM public.morpho_market_position p
            JOIN public.morpho_market m ON m.id = p.morpho_market_id
            JOIN public."user" u ON u.id = p.user_id
            WHERE least(p.supply_assets, p.borrow_assets, p.collateral) < 0
            UNION ALL
            -- holder_id is the address alone while chain_id comes from the market, so two "user" rows
            -- sharing an address render one position_id and interleave under closure.
            SELECT format('market %s is held by %s "user" rows sharing address %s across chains %s',
                          encode(m.market_id, 'hex'), count(DISTINCT u.id), encode(u.address, 'hex'),
                          string_agg(DISTINCT u.chain_id::text, ',' ORDER BY u.chain_id::text))
            FROM public.morpho_market_position p
            JOIN public.morpho_market m ON m.id = p.morpho_market_id
            JOIN public."user" u ON u.id = p.user_id
            GROUP BY m.id, m.market_id, u.address
            HAVING count(DISTINCT u.id) > 1
            UNION ALL
            -- Only holder_id carries position_state's 40-hex check; without this the run aborts on that
            -- CHECK or inside position_key(), naming no row.
            SELECT format('market %s holder %s is a %s-byte address, which cannot render the 40-hex holder_id',
                          encode(m.market_id, 'hex'), encode(u.address, 'hex'), length(u.address))
            FROM public.morpho_market_position p
            JOIN public.morpho_market m ON m.id = p.morpho_market_id
            JOIN public."user" u ON u.id = p.user_id
            WHERE length(u.address) <> 20
            GROUP BY m.market_id, u.address
        ) all_msgs
        ORDER BY msg
        LIMIT 5) worst_five;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_morpho_market: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_morpho_market'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_morpho_market(integer, bigint) IS '[Operational] VEC-402: appends Morpho market position observations into position_state via materialize_position_projection(position_morpho_market). Refuses to run, naming up to five offenders: a negative source supply, borrow or collateral amount, which the loan leg''s abs() would launder into a plausible exposure; one address held by several "user" rows, which renders one position_id since holder_id carries the address alone; and a holder address that is not 20 bytes. The legs are split on the token ADDRESSES rather than the token ids, because the address is what instrument_key is built from. See that function''s comment for the run contract. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

INSERT INTO migrations (filename) VALUES ('20260819_120000_materialize_morpho_market.sql') ON CONFLICT (filename) DO NOTHING;
