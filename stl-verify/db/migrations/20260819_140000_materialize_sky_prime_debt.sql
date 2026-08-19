-- VEC-406: materialize Sky prime debt into the position_state spine (+ current classification).
--
-- Native, per-instrument grain (VEC-400). One prime_debt row is one position: the prime's debt in a
-- given Sky ilk. holder_id = the prime's on-chain vault address; quantity = debt_wad; deal_type = BORROW
-- (Sky debt is a borrow). event-time = synced_at (native; no block_time join needed).
--
-- instrument_key = ilk_name (the native MakerDAO/Sky ilk identifier, e.g. ALLOCATOR-SPARK-A; the string
-- form of the on-chain bytes32 ilk). NOTE: the bridge comment (VEC-412) describes the Sky native key as
-- "ilk-registry address ':' ilk". The ilk-registry address is not carried on prime_debt and is not a
-- code/config constant, so it is not available to hash here — and a remembered address must never be
-- baked into a hashed position_id. ilk_name alone is the native, globally-unique ilk id within Sky's
-- single VAT, so it is used as the instrument_key. VEC-419 must key the Sky bridge rows on the SAME
-- form; if a registry prefix is wanted later, the registry address needs a real source first.
--
-- chain_id is the Sky mainnet constant 1 (prime_debt carries no chain_id column); protocol_id is NULL
-- (Sky prime debt is not protocol-scoped — see schema_master required_keys exempt). Unlike Morpho,
-- prime_debt carries its own processing_version, so it flows into the spine's processing_version.
--
-- Closure (VEC-409): the projection emits ONE closing zero-observation per real transition-to-zero
-- (debt repaid), via the LAG filter `quantity > 0 OR prev_quantity > 0` — uniform with VEC-402/403 —
-- rather than the earlier blanket `WHERE debt_wad > 0`. On live data today this is behaviour-identical:
-- verified 2026-07-28 there are no negative debt_wad rows and ZERO positive->0 transitions (Sky prime
-- debt is not observed repaying to 0 in prime_debt; the 3 zero rows are standalone and dropped either
-- way). The filter is in place so that if a prime ever repays to 0, position_current closes it
-- automatically instead of reporting stale debt.
--
-- DDL/function only. Population runs out of band, mirroring block_time, the transform _bootstrap
-- functions, and VEC-402/403.

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
CREATE OR REPLACE FUNCTION materialize_sky_prime_debt() RETURNS bigint
    LANGUAGE sql AS $fn$
    SELECT materialize_position_projection('position_sky_prime_debt'::regclass, 'VEC-406: sky_prime_debt materializer');
$fn$;

COMMENT ON FUNCTION materialize_sky_prime_debt() IS '[Operational] VEC-406: materialize Sky prime debt into position_state + position_classification, via materialize_position_projection(position_sky_prime_debt). Idempotent; run out of band. Returns position_state rows written.';

INSERT INTO migrations (filename) VALUES ('20260819_140000_materialize_sky_prime_debt.sql') ON CONFLICT (filename) DO NOTHING;
