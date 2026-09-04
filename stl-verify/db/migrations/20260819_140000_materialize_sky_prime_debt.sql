-- VEC-406: materialize Sky prime debt into the position_state spine on the native per-instrument
-- grain (VEC-400). One prime_debt row is one position: the prime's debt in a Sky ilk, held by the
-- prime's vault address. Closure and data verification: #627.

-- instrument_key = ilk_name, NOT the bridge comment's "ilk-registry address ':' ilk": the registry
-- address is not on prime_debt and not a code constant, and a remembered address must never be baked
-- into a hashed position_id. VEC-419 must key the Sky bridge rows on this same form.

-- chain_id is the Sky mainnet constant 1 (prime_debt has no chain_id); protocol_id is NULL, since Sky
-- prime debt is not protocol-scoped. prime_debt carries its own processing_version, which flows
-- through to the spine's.

-- Per-protocol projection: raw Sky prime debt -> native position rows. VEC-409 reads the per-protocol
-- outputs from position_state; this view is the materializer's source of truth.
CREATE OR REPLACE VIEW position_sky_prime_debt AS
WITH obs AS (
    SELECT DISTINCT ON (pd.prime_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version)
           pd.ilk_name              AS instrument_key,
           encode(pr.vault_address, 'hex') AS holder_id,
           pd.debt_wad              AS quantity,
           pd.prime_id,
           pd.block_number,
           pd.block_version,
           pd.processing_version,
           pd.synced_at             AS block_timestamp
    FROM prime_debt pd
    JOIN prime pr ON pr.id = pd.prime_id
    ORDER BY pd.prime_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version, pd.synced_at DESC
),
-- Previous observation's debt per position, so a repayment (positive->0) is told apart from a
-- prime x ilk that never carried debt.
series AS (
    SELECT instrument_key, holder_id, quantity,
           block_number, block_version, processing_version, block_timestamp,
           LAG(quantity) OVER (PARTITION BY prime_id, instrument_key
                               ORDER BY block_number, block_version, processing_version) AS prev_qty
    FROM obs
)
SELECT position_id(1, NULL, instrument_key, holder_id) AS position_id,
       1::integer   AS chain_id,
       NULL::bigint AS protocol_id,
       instrument_key,
       holder_id,
       quantity,
       'BORROW'::text AS deal_type_code,
       block_number,
       block_version,
       processing_version,
       block_timestamp
FROM series
WHERE quantity > 0 OR prev_qty > 0;

COMMENT ON VIEW position_sky_prime_debt IS '[Operational] VEC-406 projection: Sky prime debt as native position rows (one per prime x ilk; instrument_key = native ilk_name; holder = prime vault address; deal_type BORROW). Emits the shared position_state column contract consumed by materialize_position_projection(); one closing zero-quantity row on a real repayment-to-zero (VEC-409 closure; none observed on live data yet).';

-- Populate the spine + current classification via the shared materializer (defined with position_state,
-- VEC-402 spine). The projection view above holds all the Sky-prime-debt-specific logic; the identical
-- upsert plumbing is not duplicated here. Idempotent; run out of band; returns position_state rows written.
CREATE OR REPLACE FUNCTION materialize_sky_prime_debt(p_build_id integer DEFAULT 0) RETURNS bigint
    LANGUAGE sql AS $fn$
    SELECT materialize_position_projection('position_sky_prime_debt'::regclass, p_build_id);
$fn$;

COMMENT ON FUNCTION materialize_sky_prime_debt(integer) IS '[Operational] VEC-406: materialize Sky prime debt into position_state (observations only; no classification is written), via materialize_position_projection(position_sky_prime_debt). Idempotent; run out of band. p_build_id is stamped on every appended row (build_registry.id; 0 = pre-tracking). Returns position_state rows appended.';

INSERT INTO migrations (filename) VALUES ('20260819_140000_materialize_sky_prime_debt.sql') ON CONFLICT (filename) DO NOTHING;
