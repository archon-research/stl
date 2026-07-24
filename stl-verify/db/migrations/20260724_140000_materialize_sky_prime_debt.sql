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
-- Zero/negative debt rows (repaid) are skipped.
--
-- DDL/function only. Population runs out of band, mirroring block_time, the transform _bootstrap
-- functions, and VEC-402/403.

-- Per-protocol projection: raw Sky prime debt -> native position rows. VEC-409 unions the per-protocol
-- outputs from position_state; this view is the materializer's source of truth.
CREATE OR REPLACE VIEW position_sky_prime_debt AS
SELECT DISTINCT ON (pd.prime_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version)
       position_id(1, NULL, pd.ilk_name, encode(pr.vault_address, 'hex')) AS position_id,
       1::integer               AS chain_id,
       NULL::bigint             AS protocol_id,
       pd.ilk_name              AS instrument_key,
       encode(pr.vault_address, 'hex') AS holder_id,
       pd.debt_wad              AS quantity,
       'BORROW'::text           AS deal_type_code,
       pd.block_number,
       pd.block_version,
       pd.processing_version,
       pd.synced_at             AS block_timestamp
FROM prime_debt pd
JOIN prime pr ON pr.id = pd.prime_id
WHERE pd.debt_wad > 0
ORDER BY pd.prime_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version, pd.synced_at DESC;

COMMENT ON VIEW position_sky_prime_debt IS '[Operational] VEC-406 projection: Sky prime debt as native position rows (one per prime x ilk; instrument_key = native ilk_name; holder = prime vault address; deal_type BORROW). Source for materialize_sky_prime_debt().';

-- Populate the spine + current classification. Idempotent (ON CONFLICT). Returns rows written to
-- position_state. Run out of band after deploy; safe to re-run.
CREATE OR REPLACE FUNCTION materialize_sky_prime_debt() RETURNS bigint
    LANGUAGE plpgsql AS $fn$
DECLARE n bigint;
BEGIN
    WITH ins AS (
        INSERT INTO position_state
            (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
             block_number, block_version, processing_version, block_timestamp)
        SELECT position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, processing_version, block_timestamp
        FROM position_sky_prime_debt
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

    INSERT INTO position_classification (position_id, deal_type_code, direction, change_reason)
    SELECT DISTINCT ON (r.position_id)
           r.position_id, r.deal_type_code, d.direction, 'VEC-406: sky_prime_debt materializer'
    FROM position_sky_prime_debt r
    JOIN ref_deal_type d ON d.deal_type = r.deal_type_code
    ORDER BY r.position_id, r.block_number DESC, r.block_version DESC, r.processing_version DESC
    ON CONFLICT (position_id) DO UPDATE
        SET deal_type_code = EXCLUDED.deal_type_code,
            direction      = EXCLUDED.direction,
            change_reason  = EXCLUDED.change_reason;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_sky_prime_debt() IS '[Operational] VEC-406: upsert Sky prime debt into position_state and current deal-type into position_classification, from position_sky_prime_debt. Idempotent; run out of band. Returns position_state rows written.';

INSERT INTO migrations (filename) VALUES ('20260724_140000_materialize_sky_prime_debt.sql') ON CONFLICT (filename) DO NOTHING;
