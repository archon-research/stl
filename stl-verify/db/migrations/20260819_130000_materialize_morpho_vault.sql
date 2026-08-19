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
-- Two data facts drive the projection (verified live 2026-07-24; closure re-verified 2026-07-28):
--   * 164 (user,vault,block_number,block_version) groups carry a second row — reprocessing, not a
--     duplicate: the rows differ by processing_version (verified: 0 same-pv duplicates on prod).
--     processing_version is part of the observation axis and position_state PK, so both versions are
--     retained; position_current picks the latest pv. DISTINCT ON only dedups a genuine same-(...,pv)
--     collision, keeping the latest timestamp.
--   * Positions close: 81,200 vault deposits are observed transitioning from a positive quantity to 0
--     (of 94,813 zero-asset rows), and morpho_vault_position records that exit as a real row. The
--     projection must emit ONE closing zero-observation per real transition-to-zero — otherwise
--     position_current (VEC-409) reports an exited deposit as still open — while dropping leading and
--     repeated zeros. The LAG filter `quantity > 0 OR prev_quantity > 0` (prev per position, ordered by
--     block) does exactly this, replacing the earlier blanket `WHERE assets > 0`.
--
-- DDL/function only. Population runs out of band (a 4M-row INSERT..SELECT does not belong in the
-- migrator's single transaction), mirroring block_time, the transform _bootstrap functions, and VEC-402.

-- Per-protocol projection: raw Morpho vault positions -> native position rows. VEC-409 reads the
-- per-protocol outputs from position_state; this view is the materializer's source of truth.
CREATE OR REPLACE VIEW position_morpho_vault AS
WITH obs AS (
    SELECT DISTINCT ON (p.user_id, p.morpho_vault_id, p.block_number, p.block_version, p.processing_version)
           v.chain_id, v.protocol_id,
           encode(v.address, 'hex') AS instrument_key,
           encode(u.address, 'hex') AS holder_id,
           p.assets                 AS quantity,
           p.user_id, p.morpho_vault_id,
           p.block_number, p.block_version, p.processing_version, p.timestamp AS block_timestamp
    FROM morpho_vault_position p
    JOIN morpho_vault v ON v.id = p.morpho_vault_id
    JOIN "user"       u ON u.id = p.user_id
    ORDER BY p.user_id, p.morpho_vault_id, p.block_number, p.block_version, p.processing_version, p.timestamp DESC
),
-- Previous observation's quantity per position, so a real exit (positive->0) is told apart from a
-- position never entered. The observation axis includes processing_version (a reprocessed row is a new
-- observation, ordered after the one it corrects).
series AS (
    SELECT chain_id, protocol_id, instrument_key, holder_id, quantity,
           block_number, block_version, processing_version, block_timestamp,
           LAG(quantity) OVER (PARTITION BY user_id, morpho_vault_id
                               ORDER BY block_number, block_version, processing_version) AS prev_qty
    FROM obs
)
SELECT position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
       chain_id, protocol_id, instrument_key, holder_id, quantity,
       'LOAN'::text AS deal_type_code,
       block_number, block_version, processing_version, block_timestamp
FROM series
WHERE quantity > 0 OR prev_qty > 0;

COMMENT ON VIEW position_morpho_vault IS '[Operational] VEC-403 projection: Morpho vault positions as native position rows (one per vault deposit; instrument_key = vault contract address). Emits the shared position_state column contract consumed by materialize_position_projection(); one row per (position_id, observation), including one closing zero-quantity row on a real exit (VEC-409 closure).';

-- Populate the spine + current classification via the shared materializer (defined with position_state,
-- VEC-402 spine). The projection view above holds all the Morpho-vault-specific logic; the identical
-- upsert plumbing is not duplicated here. Idempotent; run out of band; returns position_state rows written.
CREATE OR REPLACE FUNCTION materialize_morpho_vault() RETURNS bigint
    LANGUAGE sql AS $fn$
    SELECT materialize_position_projection('position_morpho_vault'::regclass, 'VEC-403: morpho_vault materializer');
$fn$;

COMMENT ON FUNCTION materialize_morpho_vault() IS '[Operational] VEC-403: materialize Morpho vault positions into position_state + position_classification, via materialize_position_projection(position_morpho_vault). Idempotent; run out of band. Returns position_state rows written.';

INSERT INTO migrations (filename) VALUES ('20260819_130000_materialize_morpho_vault.sql') ON CONFLICT (filename) DO NOTHING;
