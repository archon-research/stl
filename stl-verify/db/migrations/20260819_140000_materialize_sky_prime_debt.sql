-- VEC-406: project Sky prime debt onto the position spine.

-- Bounds the wait for the ADD COLUMN's lock on prime_debt, which the indexer writes to. The migrator
-- runs a file in one transaction, so this covers the DO block's EXECUTE too.
SET LOCAL lock_timeout = '10s';

-- The MCD Vat, the indexer's VAT_ADDRESS default: the row every pre-existing snapshot was read from.
-- protocol_type is free text with no vocabulary behind it, so it stays NULL.
INSERT INTO protocol (chain_id, address, name, protocol_type)
VALUES (1, '\x35d1b3f3d7966a1dfe207aa4514c12a259a0492b', 'mcd-vat', NULL)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ADD COLUMN with a constant DEFAULT is catalogue-only, so it stamps every pre-existing row without
-- decompressing a chunk. The DEFAULT stays: rows the pre-rollout pod writes would be NULL forever.
DO $mig$
DECLARE v_vat bigint;
BEGIN
    SELECT id INTO STRICT v_vat FROM protocol
     WHERE chain_id = 1 AND address = '\x35d1b3f3d7966a1dfe207aa4514c12a259a0492b';
    EXECUTE format('ALTER TABLE prime_debt ADD COLUMN IF NOT EXISTS protocol_id bigint DEFAULT %s', v_vat);
END
$mig$;

COMMENT ON COLUMN prime_debt.protocol_id IS 'protocol.id of the Vat contract this snapshot was read from; set by the prime-debt indexer. Rows written before the column existed, and any written by a pre-rollout pod that does not name it, take the MCD Vat row from the column DEFAULT.';

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

COMMENT ON VIEW position_sky_prime_debt IS '[Operational] VEC-406 projection: Sky prime debt as native position rows, one position per (prime, Vat, ilk) and one row per observation; instrument_key = native ilk_name, holder_id = the prime vault address, quantity = debt_wad, wad-scaled (the raw integer divided by 1e18), not normalised across projections, protocol_id = the Vat row stamped on the snapshot, chain_id taken from that same Vat row because prime carries no chain of its own, deal_type BORROW. block_timestamp is prime_debt.synced_at, the indexer''s receipt time, since prime_debt carries no block time. GRAIN LIMIT: this view keys finer than prime_debt can store — its unique constraint is (prime_id, block_number, block_version, processing_version, synced_at), with neither protocol_id nor ilk_name, so a second Vat or a second ilk per prime at one block and synced_at is dropped at INSERT by ON CONFLICT DO NOTHING and never reaches this view. Single-Vat, single-ilk-per-prime is an assumption here, not an invariant the table enforces; widening that constraint is the fix when either arrives. Emits the shared position_state column contract consumed by materialize_position_projection(); closure is applied there.';

-- Names every snapshot the view cannot resolve, then delegates to the shared materializer.
DROP FUNCTION IF EXISTS materialize_sky_prime_debt(integer);

CREATE OR REPLACE FUNCTION materialize_sky_prime_debt(p_build_id integer DEFAULT 0,
                                                      p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    -- Pinned to the materializer's own setting, so the check cannot read fewer chunks than the run.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    -- position_key() and the spine's hex CHECK reject these, but neither names the row it came from.
    -- Every shape they reject is named here instead.
    SELECT string_agg(msg, '; ') INTO v_bad FROM (
        SELECT format('prime_debt (prime %L, ilk %L, block %s) has protocol_id %s, vault_address %L',
                      pr.name, pd.ilk_name, pd.block_number, coalesce(pd.protocol_id::text, 'NULL'),
                      encode(pr.vault_address, 'hex')) AS msg
        FROM public.prime_debt pd
        JOIN public.prime pr ON pr.id = pd.prime_id
        LEFT JOIN public.protocol p ON p.id = pd.protocol_id
        WHERE p.id IS NULL
           OR pd.ilk_name ~ '^\s*$'
           OR strpos(pd.ilk_name, ';') > 0
           OR pd.ilk_name ~ '(^\s|\s$)'
           OR octet_length(pr.vault_address) <> 20
        ORDER BY pd.prime_id, pd.ilk_name, pd.block_number
        LIMIT 10) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_sky_prime_debt: unresolved inputs, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_sky_prime_debt'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_sky_prime_debt(integer, bigint) IS '[Operational] VEC-406: materialize Sky prime debt into position_state via materialize_position_projection(position_sky_prime_debt), refusing by name a snapshot whose protocol_id has no protocol row, whose ilk_name is blank, padded or carries the '';'' key delimiter, or whose prime has a vault address that is not 20 bytes. Returns rows appended. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

INSERT INTO migrations (filename) VALUES ('20260819_140000_materialize_sky_prime_debt.sql') ON CONFLICT (filename) DO NOTHING;
