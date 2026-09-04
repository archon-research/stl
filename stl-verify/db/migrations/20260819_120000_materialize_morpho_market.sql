-- VEC-402: materialize Morpho market positions into the position_state spine on the native
-- per-instrument grain (VEC-400) -- one raw row fans out to its loan-token and collateral-token
-- positions, keyed market_id ':' token_address. Data facts, counts and rationale: #624.

-- Per-protocol projection: raw Morpho market positions -> native position rows. VEC-409 reads the
-- per-protocol outputs from position_state; this view is the materializer's source of truth.
CREATE OR REPLACE VIEW position_morpho_market AS
WITH obs AS (
    SELECT DISTINCT ON (p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version)
           m.chain_id, m.protocol_id,
           encode(m.market_id, 'hex') AS mkt,
           encode(u.address, 'hex')   AS holder_id,
           lt.address AS loan_addr,
           ct.address AS coll_addr,
           p.user_id, p.morpho_market_id,
           p.block_number, p.block_version, p.processing_version, p.timestamp AS block_timestamp,
           p.supply_assets, p.borrow_assets, p.collateral
    FROM morpho_market_position p
    JOIN morpho_market m ON m.id = p.morpho_market_id
    JOIN "user"        u ON u.id = p.user_id
    JOIN token        lt ON lt.id = m.loan_token_id
    JOIN token        ct ON ct.id = m.collateral_token_id
    ORDER BY p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.processing_version, p.timestamp DESC
),
-- Per-observation leg quantities plus the previous observation's quantity (per position), so a real
-- transition-to-zero can be told apart from a leg that was never entered. The observation axis includes
-- processing_version (a reprocessed row is a new observation, ordered after the one it corrects).
series AS (
    SELECT chain_id, protocol_id, mkt, holder_id, loan_addr, coll_addr,
           block_number, block_version, processing_version, block_timestamp,
           abs(supply_assets - borrow_assets) AS loan_qty,
           (supply_assets >= borrow_assets)   AS loan_is_supply,
           collateral                         AS coll_qty,
           LAG(abs(supply_assets - borrow_assets)) OVER w AS prev_loan_qty,
           LAG(collateral)                         OVER w AS prev_coll_qty
    FROM obs
    WINDOW w AS (PARTITION BY user_id, morpho_market_id ORDER BY block_number, block_version, processing_version)
),
legs AS (
    -- loan-token exposure: net supply vs borrow into one position; emit while open plus one closing zero-row
    SELECT chain_id, protocol_id,
           mkt || ':' || encode(loan_addr, 'hex') AS instrument_key,
           holder_id,
           loan_qty AS quantity,
           CASE WHEN loan_is_supply THEN 'LOAN' ELSE 'BORROW' END AS deal_type_code,
           block_number, block_version, processing_version, block_timestamp
    FROM series
    WHERE loan_qty > 0 OR prev_loan_qty > 0
    UNION ALL
    -- Guard: when a market's collateral token IS its loan token the two legs share one position_id,
    -- so emitting both aborts the run on a duplicate. Such a market is a single native instrument,
    -- so the loan leg alone represents it. Two exist on prod, both with zero collateral (#624).
    SELECT chain_id, protocol_id,
           mkt || ':' || encode(coll_addr, 'hex'),
           holder_id,
           coll_qty, 'COLLATERAL',
           block_number, block_version, processing_version, block_timestamp
    FROM series
    WHERE (coll_qty > 0 OR prev_coll_qty > 0)
      AND coll_addr <> loan_addr
)
SELECT position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
       chain_id, protocol_id, instrument_key, holder_id, quantity, deal_type_code,
       block_number, block_version, processing_version, block_timestamp
FROM legs;

COMMENT ON VIEW position_morpho_market IS '[Operational] VEC-402 projection: Morpho market positions as native per-instrument position rows (loan-token and collateral-token legs, composite market_id:token key). Emits the shared position_state column contract consumed by materialize_position_projection(); one row per (position_id, observation), including one closing zero-quantity row on a real transition-to-zero (VEC-409 closure).';

-- Populates via the shared materializer defined with the spine; the projection view above holds all
-- the Morpho-specific logic. Idempotent, run out of band -- a full-table INSERT..SELECT does not
-- belong in the migrator's single transaction. Returns position_state rows written.
CREATE OR REPLACE FUNCTION materialize_morpho_market(p_build_id integer DEFAULT 0) RETURNS bigint
    LANGUAGE sql AS $fn$
    SELECT materialize_position_projection('position_morpho_market'::regclass, p_build_id);
$fn$;

COMMENT ON FUNCTION materialize_morpho_market(integer) IS '[Operational] VEC-402: materialize Morpho market positions into position_state (observations only; no classification is written), via materialize_position_projection(position_morpho_market). Idempotent; run out of band. p_build_id is stamped on every appended row (build_registry.id; 0 = pre-tracking). Returns position_state rows appended.';

INSERT INTO migrations (filename) VALUES ('20260819_120000_materialize_morpho_market.sql') ON CONFLICT (filename) DO NOTHING;
