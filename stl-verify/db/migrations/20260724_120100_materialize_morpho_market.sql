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
-- Two data facts drive the projection (verified live 2026-07-24 over 866,833 rows):
--   * 8 (user,market,block_number,block_version) groups carry two timestamps — a same-block anomaly.
--     DISTINCT ON keeps the latest timestamp so (position_id, block_number, block_version) stays unique.
--   * 1,853 rows have supply_assets AND borrow_assets both > 0 (leverage loops). A single native
--     instrument (the loan token) holds one position, so these net: quantity = |supply - borrow|,
--     deal_type = LOAN when net-supplied, BORROW when net-borrowed.
--
-- DDL/function only. Population runs out of band (a full-table INSERT..SELECT does not belong in the
-- migrator's single transaction), mirroring block_time and the transform _bootstrap functions.

-- Per-protocol projection: raw Morpho market positions -> native position rows. VEC-409 unions the
-- per-protocol outputs from position_state; this view is the materializer's source of truth.
CREATE OR REPLACE VIEW position_morpho_market AS
WITH obs AS (
    SELECT DISTINCT ON (p.user_id, p.morpho_market_id, p.block_number, p.block_version)
           m.chain_id, m.protocol_id,
           encode(m.market_id, 'hex') AS mkt,
           encode(u.address, 'hex')   AS holder_id,
           lt.address AS loan_addr,
           ct.address AS coll_addr,
           p.block_number, p.block_version, p.timestamp AS block_timestamp,
           p.supply_assets, p.borrow_assets, p.collateral
    FROM morpho_market_position p
    JOIN morpho_market m ON m.id = p.morpho_market_id
    JOIN "user"        u ON u.id = p.user_id
    JOIN token        lt ON lt.id = m.loan_token_id
    JOIN token        ct ON ct.id = m.collateral_token_id
    ORDER BY p.user_id, p.morpho_market_id, p.block_number, p.block_version, p.timestamp DESC
),
legs AS (
    -- loan-token exposure: net supply vs borrow into one position for the loan-token instrument
    SELECT chain_id, protocol_id,
           mkt || ':' || encode(loan_addr, 'hex') AS instrument_key,
           holder_id,
           abs(supply_assets - borrow_assets) AS quantity,
           CASE WHEN supply_assets >= borrow_assets THEN 'LOAN' ELSE 'BORROW' END AS deal_type_code,
           block_number, block_version, block_timestamp
    FROM obs
    WHERE supply_assets <> borrow_assets
    UNION ALL
    -- collateral-token exposure
    SELECT chain_id, protocol_id,
           mkt || ':' || encode(coll_addr, 'hex'),
           holder_id,
           collateral, 'COLLATERAL',
           block_number, block_version, block_timestamp
    FROM obs
    WHERE collateral > 0
)
SELECT position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
       chain_id, protocol_id, instrument_key, holder_id, quantity, deal_type_code,
       block_number, block_version, block_timestamp
FROM legs;

COMMENT ON VIEW position_morpho_market IS '[Operational] VEC-402 projection: Morpho market positions as native per-instrument position rows (loan-token and collateral-token legs, composite market_id:token key). Source for materialize_morpho_market(); one row per (position_id, observation).';

-- Populate the spine + current classification. Idempotent (ON CONFLICT). Returns rows written to
-- position_state. Run out of band after deploy; safe to re-run.
CREATE OR REPLACE FUNCTION materialize_morpho_market() RETURNS bigint
    LANGUAGE plpgsql AS $fn$
DECLARE n bigint;
BEGIN
    WITH ins AS (
        INSERT INTO position_state
            (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
             block_number, block_version, processing_version, block_timestamp)
        SELECT position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, 0, block_timestamp
        FROM position_morpho_market
        ON CONFLICT (position_id, block_number, block_version, processing_version) DO UPDATE
            SET quantity        = EXCLUDED.quantity,
                block_timestamp = EXCLUDED.block_timestamp,
                chain_id        = EXCLUDED.chain_id,
                protocol_id     = EXCLUDED.protocol_id,
                instrument_key  = EXCLUDED.instrument_key,
                holder_id       = EXCLUDED.holder_id
        RETURNING 1
    )
    SELECT count(*) INTO n FROM ins;

    -- Current deal-type per position (latest observation wins); direction frozen from ref_deal_type.
    INSERT INTO position_classification (position_id, deal_type_code, direction, change_reason)
    SELECT DISTINCT ON (r.position_id)
           r.position_id, r.deal_type_code, d.direction, 'VEC-402: morpho_market materializer'
    FROM position_morpho_market r
    JOIN ref_deal_type d ON d.deal_type = r.deal_type_code
    ORDER BY r.position_id, r.block_number DESC, r.block_version DESC
    ON CONFLICT (position_id) DO UPDATE
        SET deal_type_code = EXCLUDED.deal_type_code,
            direction      = EXCLUDED.direction,
            change_reason  = EXCLUDED.change_reason;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_morpho_market() IS '[Operational] VEC-402: upsert Morpho market positions into position_state and current deal-type into position_classification, from position_morpho_market. Idempotent; run out of band. Returns position_state rows written.';

INSERT INTO migrations (filename) VALUES ('20260724_120100_materialize_morpho_market.sql') ON CONFLICT (filename) DO NOTHING;
