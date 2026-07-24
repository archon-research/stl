-- VEC-403: materialize Morpho vault positions into the position_state spine (+ current classification).
--
-- Native, per-instrument grain (VEC-400). A Morpho (MetaMorpho) vault is a SINGLE native instrument —
-- the ERC-4626 vault contract — so unlike the market case (VEC-402) there is no loan/collateral split
-- and no fan-out: one raw morpho_vault_position row is one position. instrument_key = the vault's
-- contract address (the VEC-412 bridge form for an ERC-4626 vault); holder_id = the on-chain user
-- address; quantity = assets (underlying-denominated deposit). deal_type = LOAN (a vault deposit lends
-- to the underlying markets and earns yield) — a classification attribute in position_classification,
-- not part of the id.
--
-- One data fact drives the projection (verified live 2026-07-24 over 4,410,899 rows): 164
-- (user,vault,block_number,block_version) groups carry two timestamps — the same same-block anomaly as
-- the market table. DISTINCT ON keeps the latest so (position_id, block_number, block_version) stays
-- unique. Zero-asset observations (exited/empty positions) are skipped, matching VEC-402.
--
-- DDL/function only. Population runs out of band (a 4M-row INSERT..SELECT does not belong in the
-- migrator's single transaction), mirroring block_time, the transform _bootstrap functions, and VEC-402.

-- Per-protocol projection: raw Morpho vault positions -> native position rows. VEC-409 unions the
-- per-protocol outputs from position_state; this view is the materializer's source of truth.
CREATE OR REPLACE VIEW position_morpho_vault AS
SELECT DISTINCT ON (p.user_id, p.morpho_vault_id, p.block_number, p.block_version)
       position_id(v.chain_id, v.protocol_id, encode(v.address, 'hex'), encode(u.address, 'hex')) AS position_id,
       v.chain_id,
       v.protocol_id,
       encode(v.address, 'hex') AS instrument_key,
       encode(u.address, 'hex') AS holder_id,
       p.assets                 AS quantity,
       'LOAN'::text             AS deal_type_code,
       p.block_number,
       p.block_version,
       p.timestamp              AS block_timestamp
FROM morpho_vault_position p
JOIN morpho_vault v ON v.id = p.morpho_vault_id
JOIN "user"       u ON u.id = p.user_id
WHERE p.assets > 0
ORDER BY p.user_id, p.morpho_vault_id, p.block_number, p.block_version, p.timestamp DESC;

COMMENT ON VIEW position_morpho_vault IS '[Operational] VEC-403 projection: Morpho vault positions as native position rows (one per vault deposit; instrument_key = vault contract address). Source for materialize_morpho_vault(); one row per (position_id, observation).';

-- Populate the spine + current classification. Idempotent (ON CONFLICT). Returns rows written to
-- position_state. Run out of band after deploy; safe to re-run.
CREATE OR REPLACE FUNCTION materialize_morpho_vault() RETURNS bigint
    LANGUAGE plpgsql AS $fn$
DECLARE n bigint;
BEGIN
    WITH ins AS (
        INSERT INTO position_state
            (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
             block_number, block_version, processing_version, block_timestamp)
        SELECT position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, 0, block_timestamp
        FROM position_morpho_vault
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
           r.position_id, r.deal_type_code, d.direction, 'VEC-403: morpho_vault materializer'
    FROM position_morpho_vault r
    JOIN ref_deal_type d ON d.deal_type = r.deal_type_code
    ORDER BY r.position_id, r.block_number DESC, r.block_version DESC
    ON CONFLICT (position_id) DO UPDATE
        SET deal_type_code = EXCLUDED.deal_type_code,
            direction      = EXCLUDED.direction,
            change_reason  = EXCLUDED.change_reason;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_morpho_vault() IS '[Operational] VEC-403: upsert Morpho vault positions into position_state and current deal-type into position_classification, from position_morpho_vault. Idempotent; run out of band. Returns position_state rows written.';

INSERT INTO migrations (filename) VALUES ('20260724_130000_materialize_morpho_vault.sql') ON CONFLICT (filename) DO NOTHING;
