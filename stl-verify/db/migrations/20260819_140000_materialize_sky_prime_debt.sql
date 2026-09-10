-- VEC-406: project Sky prime debt onto the position spine. One prime_debt row is one observation of a
-- prime's debt in an ilk, held by the prime's vault address; instrument_key = ilk_name (unique within
-- the single Vat); protocol_id = the Vat row the indexer stamps on each prime_debt row.

-- prime_debt carries the Vat the indexer read. A bare nullable column on a compressed hypertable (the
-- 20260410_110000 retrofit shape); the writer sets it and materialize_sky_prime_debt() refuses NULLs.
ALTER TABLE prime_debt ADD COLUMN IF NOT EXISTS protocol_id bigint;
COMMENT ON COLUMN prime_debt.protocol_id IS 'protocol.id of the Vat contract this snapshot was read from; set by the prime-debt indexer. Rows written before the column existed were backfilled to the MCD Vat row.';

-- The MCD Vat, the indexer's VAT_ADDRESS default: the row every pre-existing snapshot was read from.
-- Named for the contract, not for Sky, and protocol_type is left NULL: the column is free text with
-- no vocabulary behind it, and Sky is not a lending protocol.
INSERT INTO protocol (chain_id, address, name, protocol_type)
VALUES (1, '\x35d1b3f3d7966a1dfe207aa4514c12a259a0492b', 'mcd-vat', NULL)
ON CONFLICT (chain_id, address) DO NOTHING;

UPDATE prime_debt
   SET protocol_id = (SELECT id FROM protocol WHERE chain_id = 1 AND address = '\x35d1b3f3d7966a1dfe207aa4514c12a259a0492b')
 WHERE protocol_id IS NULL;

CREATE OR REPLACE VIEW position_sky_prime_debt AS
WITH obs AS (
    -- One row per observation key; the earliest synced_at is the stable pick (a retry can only add a later one).
    SELECT DISTINCT ON (pd.prime_id, pd.protocol_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version)
           pd.prime_id, pd.protocol_id, pd.ilk_name, pd.debt_wad,
           pd.block_number, pd.block_version, pd.processing_version, pd.synced_at
    FROM prime_debt pd
    ORDER BY pd.prime_id, pd.protocol_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version,
             pd.synced_at
)
SELECT p.chain_id,
       o.protocol_id,
       o.ilk_name                      AS instrument_key,
       encode(pr.vault_address, 'hex') AS holder_id,
       o.debt_wad                      AS quantity,
       'BORROW'::text                  AS deal_type,
       o.block_number,
       o.block_version,
       o.processing_version,
       o.synced_at                     AS block_timestamp
FROM obs o
JOIN protocol p  ON p.id  = o.protocol_id
JOIN prime    pr ON pr.id = o.prime_id;

COMMENT ON VIEW position_sky_prime_debt IS '[Operational] VEC-406 projection: Sky prime debt as native position rows, one per (prime, Vat, ilk); instrument_key = native ilk_name, holder_id = the prime vault address, protocol_id = the Vat row stamped on the snapshot, deal_type BORROW. Emits the shared position_state column contract consumed by materialize_position_projection(); closure is applied there.';

-- Names every snapshot the view cannot resolve, then delegates to the shared materializer.
-- Dropped rather than replaced: keeping the old argument list beside the new one makes a
-- call that omits the run ambiguous, as it did for the spine.
DROP FUNCTION IF EXISTS materialize_sky_prime_debt(integer);

CREATE OR REPLACE FUNCTION materialize_sky_prime_debt(p_build_id integer DEFAULT 0,
                                                      p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT AS $fn$
DECLARE
    v_bad text;
BEGIN
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT format('prime_debt (prime_id %s, ilk %s, block %s) has protocol_id %s with no protocol row',
                      pd.prime_id, pd.ilk_name, pd.block_number, coalesce(pd.protocol_id::text, 'NULL')) AS msg
        FROM public.prime_debt pd
        LEFT JOIN public.protocol p ON p.id = pd.protocol_id
        WHERE p.id IS NULL
        ORDER BY pd.prime_id, pd.ilk_name, pd.block_number
        LIMIT 10) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_sky_prime_debt: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_sky_prime_debt'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_sky_prime_debt(integer, bigint) IS '[Operational] VEC-406: materialize Sky prime debt into position_state via materialize_position_projection(position_sky_prime_debt), refusing by name a snapshot whose protocol_id has no protocol row. Returns rows appended. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

INSERT INTO migrations (filename) VALUES ('20260819_140000_materialize_sky_prime_debt.sql') ON CONFLICT (filename) DO NOTHING;
