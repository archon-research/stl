-- VEC-403: materialize Morpho vault positions into the position_state spine on the native
-- per-instrument grain (VEC-400). A MetaMorpho vault is a SINGLE native instrument, so unlike the
-- market case there is no fan-out: one raw row is one position, keyed by the vault address. #626.

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

COMMENT ON VIEW position_morpho_vault IS '[Operational] VEC-403 projection: Morpho vault positions as native position rows (one per vault deposit; instrument_key = vault contract address). Emits the shared position_state column contract consumed by materialize_position_projection(); one row per (position_id, observation), including one closing zero-quantity row on a real exit (VEC-409 closure). deal_type_code is part of the contract and is validated, but the spine does not store it.';

-- Appends via the shared materializer defined with the spine; the projection view above holds all the
-- Morpho-vault-specific logic. p_build_id is the ADR-0002 provenance record stamped on every appended
-- row (0 = pre-tracking). Idempotent, run out of band; returns position_state rows appended.
CREATE OR REPLACE FUNCTION materialize_morpho_vault(p_build_id integer DEFAULT 0) RETURNS bigint
    LANGUAGE sql AS $fn$
    SELECT materialize_position_projection('position_morpho_vault'::regclass, p_build_id);
$fn$;

COMMENT ON FUNCTION materialize_morpho_vault(integer) IS '[Operational] VEC-403: append Morpho vault position observations into position_state, via materialize_position_projection(position_morpho_vault). Observations only -- no classification is written; deal_type is an attribute of the instrument and is resolved instrument-side. p_build_id is stamped on every appended row (build_registry.id; 0 = pre-tracking). Idempotent; run out of band. Returns position_state rows appended.';

INSERT INTO migrations (filename) VALUES ('20260819_130000_materialize_morpho_vault.sql') ON CONFLICT (filename) DO NOTHING;
