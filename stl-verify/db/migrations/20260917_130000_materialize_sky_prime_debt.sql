-- VEC-406: project Sky prime debt onto the position spine.

CREATE OR REPLACE VIEW position_sky_prime_debt AS
WITH obs AS (
    -- One row per observation key; the earliest synced_at is the stable pick (a retry can only add a later one).
    SELECT DISTINCT ON (pd.prime_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version)
           pd.prime_id, pd.ilk_name, pd.debt_wad,
           pd.block_number, pd.block_version, pd.processing_version
    FROM prime_debt pd
    ORDER BY pd.prime_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version, pd.synced_at
)
-- Sky is the issuer, not a protocol: the key is the native Vat address, never a protocol.id, which is a
-- BIGSERIAL and would make position_id differ between environments.
SELECT 1::integer                                                AS chain_id,
       NULL::bigint                                              AS protocol_id,
       '35d1b3f3d7966a1dfe207aa4514c12a259a0492b:' || o.ilk_name AS instrument_key,
       encode(pr.vault_address, 'hex')                           AS holder_id,
       o.debt_wad                                                AS quantity,
       'BORROW'::text                                            AS deal_type,
       o.block_number,
       o.block_version,
       o.processing_version,
       bm.block_timestamp
FROM obs o
JOIN prime pr ON pr.id = o.prime_id
-- The block header time; readers take the highest processing_version. A block the loader has not
-- reached yet emits nothing, and a later run appends it.
CROSS JOIN LATERAL (
    SELECT m.block_timestamp FROM block_meta m
     WHERE m.chain_id = 1 AND m.block_number = o.block_number AND m.block_version = o.block_version
     ORDER BY m.processing_version DESC
     LIMIT 1) bm;

COMMENT ON VIEW position_sky_prime_debt IS '[Operational] VEC-406 projection: Sky prime debt as native position rows, one position per (prime, ilk) and one row per observation. Sky is the issuer and the Vat its ledger, not a protocol, so protocol_id is NULL, as for Anchorage. instrument_key = vat_address:ilk_name, the native pair (an ilk name is unique only within its Vat), with the MCD Vat in lowercase hex and no 0x; chain_id 1. holder_id = the prime vault address. quantity = debt_wad, wad-scaled, not normalised. deal_type BORROW. block_timestamp is the block header time from block_meta; an observation whose block block_meta does not hold yet is not emitted until it does. prime_debt does not record the Vat, so the indexer refuses any Vat but the MCD Vat. prime_debt''s unique constraint omits ilk_name; the indexer resolves one ilk per prime vault, so it never writes a second ilk for a prime at one block. Closure is applied by materialize_position_projection().';

-- Names every snapshot the view cannot resolve, then delegates to the shared materializer.
DROP FUNCTION IF EXISTS materialize_sky_prime_debt(integer);
DROP FUNCTION IF EXISTS materialize_sky_prime_debt(integer, bigint);

CREATE OR REPLACE FUNCTION materialize_sky_prime_debt(p_build_id integer DEFAULT 0,
                                                      p_run_id bigint DEFAULT NULL,
                                                      p_window interval DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    -- Pinned to the materializer's own setting, so the check cannot read fewer chunks than the run.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    -- position_key() and the spine's hex CHECK reject these ilk and holder shapes, but neither names the
    -- row it came from, so they are named here. processing_version is set by prime_debt's insert trigger.
    SELECT string_agg(msg, '; ') INTO v_bad FROM (
        SELECT format('prime_debt (prime %L, ilk %L, block %s) has vault_address %L',
                      pr.name, pd.ilk_name, pd.block_number, encode(pr.vault_address, 'hex')) AS msg
        FROM public.prime_debt pd
        JOIN public.prime pr ON pr.id = pd.prime_id
        WHERE pd.ilk_name ~ '^\s*$'
           OR strpos(pd.ilk_name, ';') > 0
           OR pd.ilk_name ~ '(^\s|\s$)'
           OR octet_length(pr.vault_address) <> 20
        ORDER BY pd.prime_id, pd.ilk_name, pd.block_number
        LIMIT 10) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_sky_prime_debt: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_sky_prime_debt'::regclass, p_build_id, p_run_id, p_window);
END
$fn$;

COMMENT ON FUNCTION materialize_sky_prime_debt(integer, bigint, interval) IS '[Operational] VEC-406: materialize Sky prime debt into position_state via materialize_position_projection(position_sky_prime_debt), refusing by name a snapshot whose ilk_name is blank, padded or carries the '';'' key delimiter, or whose prime has a vault address that is not 20 bytes. Returns rows appended. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2). p_window is forwarded to the materializer, which bounds the batch it reads; against this view it filters rows without pruning chunks.';

INSERT INTO migrations (filename) VALUES ('20260917_130000_materialize_sky_prime_debt.sql') ON CONFLICT (filename) DO NOTHING;
