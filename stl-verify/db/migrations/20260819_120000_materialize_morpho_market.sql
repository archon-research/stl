-- VEC-402: materialize Morpho market positions into position_state on the native per-instrument grain
-- (VEC-400). One raw row fans out into its loan-token leg (supply netted against borrow, and against
-- collateral when that is the same token) and its collateral-token leg, keyed market_id ':' token_address.
CREATE OR REPLACE VIEW position_morpho_market AS
WITH obs AS (
    -- One row per observation key; the earliest timestamp is the stable pick when a block is observed twice.
    SELECT DISTINCT ON (p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version)
           p.user_id, p.morpho_market_id,
           p.block_number, p.block_version, p.processing_version, p.timestamp AS block_timestamp,
           p.supply_assets, p.borrow_assets, p.collateral,
           m.collateral_token_id = m.loan_token_id AS same_token
    FROM morpho_market_position p
    JOIN morpho_market m ON m.id = p.morpho_market_id
    ORDER BY p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version, p.timestamp
),
-- The loan leg's SIGNED exposure. In a market whose collateral token is its loan token the collateral is
-- the same native instrument, so it nets here rather than being added to a magnitude the direction below
-- never saw: posting 300 against a 100 borrow is +200 LONG, not 400 SHORT.
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
           (array_remove(array_agg(CASE WHEN n.signed_net <> 0 THEN n.signed_net > 0 END)
                                   OVER (PARTITION BY n.user_id, n.morpho_market_id
                                         ORDER BY n.block_number, n.block_version, n.processing_version
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), NULL)) AS dirs
    FROM netted n
),
legs AS (
    -- Loan-token exposure. A market whose collateral token IS its loan token keys both legs on one
    -- position_id, which the shared materializer rejects as a double-emitted key; such a market is a
    -- single native instrument, so only the loan leg is emitted and its quantity nets in the collateral.
    SELECT s.user_id, s.morpho_market_id,
           encode(m.market_id, 'hex') || ':' || encode(lt.address, 'hex') AS instrument_key,
           s.net_loan AS quantity,
           CASE s.dirs[cardinality(s.dirs)] WHEN true THEN 'LOAN' WHEN false THEN 'BORROW' END AS deal_type,
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
    JOIN morpho_market m ON m.id = s.morpho_market_id AND m.collateral_token_id <> m.loan_token_id
    JOIN token ct ON ct.id = m.collateral_token_id
)
SELECT m.chain_id, m.protocol_id, l.instrument_key, encode(u.address, 'hex') AS holder_id, l.quantity, l.deal_type,
       l.block_number, l.block_version, l.processing_version, l.block_timestamp
FROM legs l
JOIN morpho_market m ON m.id = l.morpho_market_id
JOIN "user"        u ON u.id = l.user_id;

COMMENT ON VIEW position_morpho_market IS '[Operational] VEC-402 projection: Morpho market positions as native per-instrument position rows (loan-token and collateral-token legs, composite market_id:token key). Emits the shared position_state column contract consumed by materialize_position_projection(); deal_type is LOAN or BORROW on the loan leg by the sign of supply minus borrow (a net-zero row inherits the direction it closes) and COLLATERAL on the collateral leg. A market whose collateral token is its loan token emits the loan leg only, netting the collateral in.';

-- Per-projection entry point; the view above holds all the Morpho-market logic. It refuses a negative
-- source amount first: netting means abs() would launder it, and a negative borrow makes the netted
-- sum MORE positive, so neither the view nor the spine's negative-quantity check would see it.
-- Dropped rather than replaced: keeping the old argument list beside the new one makes a
-- call that omits the run ambiguous, as it did for the spine.
DROP FUNCTION IF EXISTS materialize_morpho_market(integer);

CREATE OR REPLACE FUNCTION materialize_morpho_market(p_build_id integer DEFAULT 0,
                                                     p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT AS $fn$
DECLARE
    v_bad text;
BEGIN
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT format('market %s user %s at bn=%s bv=%s pv=%s: supply=%s borrow=%s collateral=%s',
                      encode(m.market_id, 'hex'), encode(u.address, 'hex'), p.block_number,
                      p.block_version, p.processing_version, p.supply_assets, p.borrow_assets, p.collateral) AS msg
        FROM public.morpho_market_position p
        JOIN public.morpho_market m ON m.id = p.morpho_market_id
        JOIN public."user" u ON u.id = p.user_id
        WHERE least(p.supply_assets, p.borrow_assets, p.collateral) < 0
        ORDER BY p.block_number, p.block_version, p.processing_version
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_morpho_market: a negative source amount cannot be a position exposure, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_morpho_market'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_morpho_market(integer, bigint) IS '[Operational] VEC-402: appends Morpho market position observations into position_state via materialize_position_projection(position_morpho_market). See that function''s comment for the run contract. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

INSERT INTO migrations (filename) VALUES ('20260819_120000_materialize_morpho_market.sql') ON CONFLICT (filename) DO NOTHING;
