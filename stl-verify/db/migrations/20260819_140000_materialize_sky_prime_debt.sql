-- VEC-406: project Sky prime debt onto the position spine. One prime_debt row is one observation of a
-- prime's debt in an ilk, held by the prime's vault address. instrument_key = vat_address:ilk_name, the
-- native pair (an ilk name is unique only within its Vat); protocol_id is NULL: the Vat is an address in the key, not a protocol row.

CREATE OR REPLACE VIEW position_sky_prime_debt AS
WITH obs AS (
    -- One row per observation key; the earliest synced_at is the stable pick (a retry can only add a later one).
    SELECT DISTINCT ON (pd.prime_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version)
           pd.prime_id, pd.ilk_name, pd.debt_wad,
           pd.block_number, pd.block_version, pd.processing_version, pd.synced_at
    FROM prime_debt pd
    ORDER BY pd.prime_id, pd.ilk_name, pd.block_number, pd.block_version, pd.processing_version, pd.synced_at
)
-- The indexer reads one Vat (its VAT_ADDRESS default), so the address is a literal here. A protocol.id
-- (BIGSERIAL, environment-local) must never enter the hash.
SELECT 1::integer                                                  AS chain_id,
       NULL::bigint                                                AS protocol_id,
       '35d1b3f3d7966a1dfe207aa4514c12a259a0492b:' || o.ilk_name   AS instrument_key,
       encode(pr.vault_address, 'hex')                             AS holder_id,
       o.debt_wad                                                  AS quantity,
       'BORROW'::text                                              AS deal_type,
       o.block_number,
       o.block_version,
       o.processing_version,
       o.synced_at                                                 AS block_timestamp
FROM obs o
JOIN prime pr ON pr.id = o.prime_id;

COMMENT ON VIEW position_sky_prime_debt IS '[Operational] VEC-406 projection: Sky prime debt as native position rows, one position per (prime, ilk), one row per observation. instrument_key = vat_address:ilk_name (the MCD Vat, lowercase hex, no 0x), holder_id = the prime vault address, chain_id 1, protocol_id NULL, deal_type BORROW. block_timestamp is prime_debt.synced_at, the indexer''s receipt time, since prime_debt carries no block time. Emits the shared position_state column contract consumed by materialize_position_projection(); closure is applied there.';

-- The only check on these inputs: a blank or padded ilk_name still yields a well-formed key once the Vat
-- is prefixed, and a malformed vault address fails only inside the shared materializer, naming no row.
CREATE OR REPLACE FUNCTION materialize_sky_prime_debt(p_build_id integer DEFAULT 0,
                                                      p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT AS $fn$
DECLARE
    v_bad text;
BEGIN
    SELECT string_agg(msg, '; ') INTO v_bad FROM (
        SELECT format('prime_debt (prime %L, block %s) has ilk_name %L or vault_address %L that cannot key a position',
                      pr.name, pd.block_number, pd.ilk_name, encode(pr.vault_address, 'hex')) AS msg
        FROM public.prime_debt pd
        JOIN public.prime pr ON pr.id = pd.prime_id
        WHERE pd.ilk_name ~ '^\s*$' OR pd.ilk_name ~ '(^\s|\s$)' OR strpos(pd.ilk_name, ';') > 0
           OR octet_length(pr.vault_address) <> 20
        ORDER BY pd.prime_id, pd.block_number
        LIMIT 10) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_sky_prime_debt: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_sky_prime_debt'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_sky_prime_debt(integer, bigint) IS '[Operational] VEC-406: materialize Sky prime debt into position_state via materialize_position_projection(position_sky_prime_debt), refusing by name a snapshot whose ilk_name is blank, padded or '';''-bearing, or whose prime has a vault address that is not 20 bytes. Returns rows appended. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

INSERT INTO migrations (filename) VALUES ('20260819_140000_materialize_sky_prime_debt.sql') ON CONFLICT (filename) DO NOTHING;
