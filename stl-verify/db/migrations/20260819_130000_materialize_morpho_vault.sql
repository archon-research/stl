-- VEC-403: materialize Morpho vault positions into position_state on the native per-instrument grain
-- (VEC-400). A MetaMorpho vault is a single native instrument, so one raw row is one position, keyed
-- by the vault address.
CREATE OR REPLACE VIEW position_morpho_vault AS
WITH obs AS (
    -- One row per observation key; the earliest timestamp is the stable pick when a block is observed twice.
    SELECT DISTINCT ON (p.user_id, p.morpho_vault_id, p.block_number, p.block_version, p.processing_version)
           p.user_id, p.morpho_vault_id, p.assets AS quantity,
           p.block_number, p.block_version, p.processing_version, p.timestamp AS block_timestamp
    FROM morpho_vault_position p
    ORDER BY p.user_id, p.morpho_vault_id, p.block_number, p.block_version, p.processing_version, p.timestamp
)
SELECT v.chain_id, v.protocol_id,
       encode(v.address, 'hex') AS instrument_key,
       encode(u.address, 'hex') AS holder_id,
       o.quantity,
       'LOAN'::text AS deal_type,
       o.block_number, o.block_version, o.processing_version, o.block_timestamp
FROM obs o
JOIN morpho_vault v ON v.id = o.morpho_vault_id
JOIN "user"       u ON u.id = o.user_id;

COMMENT ON VIEW position_morpho_vault IS '[Operational] VEC-403 projection: Morpho vault positions as native position rows, one per vault deposit; instrument_key = vault contract address, holder_id = depositor address, quantity = assets in the vault asset''s native decimals, deal_type LOAN. Emits the position_state column contract; closure is applied by materialize_position_projection().';

-- Wrapper over the shared materializer: the projection view above holds all the Morpho-vault-specific
-- logic. It refuses first, because holder_id is the depositor's address alone while chain_id comes from
-- the vault, and morpho_vault_position constrains neither against the other.
-- Dropped rather than replaced: keeping the old argument list beside the new one makes a
-- call that omits the run ambiguous, as it did for the spine.
DROP FUNCTION IF EXISTS materialize_morpho_vault(integer);

CREATE OR REPLACE FUNCTION materialize_morpho_vault(p_build_id integer DEFAULT 0,
                                                    p_run_id bigint DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT AS $fn$
DECLARE
    v_bad text;
BEGIN
    -- Two "user" rows sharing an address on different chains, depositing into one vault, render one
    -- position_id: at one block that is a double-emit, at different blocks their histories silently
    -- interleave under closure. Verified: 2 rows, 1 position_id, no error.
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM (
        SELECT format('vault %s holds deposits from %s "user" rows sharing address %s across chains %s',
                      encode(v.address, 'hex'), count(DISTINCT u.id), encode(u.address, 'hex'),
                      string_agg(DISTINCT u.chain_id::text, ',' ORDER BY u.chain_id::text)) AS msg
        FROM public.morpho_vault_position p
        JOIN public.morpho_vault v ON v.id = p.morpho_vault_id
        JOIN public."user" u ON u.id = p.user_id
        GROUP BY v.id, v.address, u.address
        HAVING count(DISTINCT u.id) > 1
        LIMIT 5) z;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_morpho_vault: one address on several chains would collapse into one position, refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_morpho_vault'::regclass, p_build_id, p_run_id);
END
$fn$;

COMMENT ON FUNCTION materialize_morpho_vault(integer, bigint) IS '[Operational] VEC-403: appends Morpho vault position observations into position_state via materialize_position_projection(position_morpho_vault). See that function''s comment for the run contract. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2).';

INSERT INTO migrations (filename) VALUES ('20260819_130000_materialize_morpho_vault.sql') ON CONFLICT (filename) DO NOTHING;
