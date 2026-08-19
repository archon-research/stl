-- VEC-402: materialize Morpho market positions into the position_state spine (+ current classification).
--
-- Native, per-instrument grain (VEC-400). One morpho_market_position row fans out by NATIVE INSTRUMENT
-- only — never by a house leg/deal_type classifier — into at most two positions:
--   * the loan-token position   (instrument_key = market_id ':' loan_token_address)
--   * the collateral-token position (instrument_key = market_id ':' collateral_token_address)
-- The composite market_id:token native key keeps per-market granularity AND makes the borrow leg and the
-- collateral leg distinct instruments (so each carries a single deal_type), consistent with how Aave's
-- aToken/debtToken and every other protocol already model collateral as its own native instrument.
--
-- Identity is native only: position_id(chain_id, protocol_id, instrument_key, holder_id). holder_id is the
-- on-chain user address (lowercase hex, no 0x). deal_type (LOAN/BORROW/COLLATERAL) is a classification
-- attribute in position_classification, NOT part of the id.
--
-- Three data facts drive the projection (verified live 2026-07-24; leg counts re-verified 2026-07-28):
--   * 8 (user,market,block_number,block_version) groups carry a second row — reprocessing, not a
--     duplicate: the two rows differ by processing_version (verified: 0 same-pv duplicates on prod).
--     processing_version is part of the observation axis and the position_state PK, so both versions are
--     retained; position_current picks the latest pv. DISTINCT ON only dedups a genuine same-(...,pv)
--     collision, keeping the latest timestamp.
--   * 1,853 rows have supply_assets AND borrow_assets both > 0 (leverage loops). A single native
--     instrument (the loan token) holds one position, so these net: quantity = |supply - borrow|,
--     deal_type = LOAN when net-supplied, BORROW when net-borrowed.
--   * Positions close: 11,528 collateral legs and 36,217 loan legs are observed transitioning from a
--     positive quantity to 0. The raw table records that transition as a real row (collateral = 0 /
--     supply_assets = borrow_assets), so the projection must emit ONE closing zero-observation per real
--     transition-to-zero — otherwise position_current (VEC-409) reports a closed position as still open.
--     It must NOT emit a zero for a leg that was never entered (e.g. a pure lender has collateral = 0
--     on every row), which is the common case (769,999 of 887,923 rows have collateral = 0). The LAG
--     filter `quantity > 0 OR prev_quantity > 0` (prev per position, ordered by block) does exactly this:
--     emit while open, emit the first zero after a positive, drop leading/repeated zeros.
--
-- DDL/function only. Population runs out of band (a full-table INSERT..SELECT does not belong in the
-- migrator's single transaction), mirroring block_time and the transform _bootstrap functions.

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
    -- collateral-token exposure. Guard: when a market's collateral token IS its loan token
    -- (coll_addr = loan_addr), the two legs share instrument_key mkt:token and therefore the same
    -- position_id, so emitting both would produce a duplicate (position_id, block axis) and the
    -- materializer's ON CONFLICT DO UPDATE would abort the whole run ("cannot affect row a second time").
    -- Such a market is a single native instrument, so the loan leg alone represents it; the collateral
    -- leg is folded out. 2 such markets exist on prod today (WBTC 795, USDC 885), both with collateral = 0
    -- on every row, so this drops nothing live. If one ever carries non-zero collateral, netting
    -- collateral into the loan-token exposure must be revisited (it cannot be a separate position under
    -- native-only keying — a role classifier must never enter the instrument_key).
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

-- Populate the spine + current classification via the shared materializer (defined with position_state,
-- VEC-402 spine). The projection view above holds all the Morpho-market-specific logic; the identical
-- upsert plumbing is not duplicated here. Idempotent; run out of band; returns position_state rows written.
CREATE OR REPLACE FUNCTION materialize_morpho_market() RETURNS bigint
    LANGUAGE sql AS $fn$
    SELECT materialize_position_projection('position_morpho_market'::regclass, 'VEC-402: morpho_market materializer');
$fn$;

COMMENT ON FUNCTION materialize_morpho_market() IS '[Operational] VEC-402: materialize Morpho market positions into position_state + position_classification, via materialize_position_projection(position_morpho_market). Idempotent; run out of band. Returns position_state rows written.';

INSERT INTO migrations (filename) VALUES ('20260819_120000_materialize_morpho_market.sql') ON CONFLICT (filename) DO NOTHING;
