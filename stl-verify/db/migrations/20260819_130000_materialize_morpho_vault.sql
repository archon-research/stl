-- VEC-403: materialize Morpho vault positions into position_state on the native per-instrument grain
-- (VEC-400). A MetaMorpho vault is a single native instrument.
CREATE OR REPLACE VIEW position_morpho_vault AS
WITH obs AS (
    -- The earliest timestamp is the stable pick when one block is observed twice.
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

COMMENT ON VIEW position_morpho_vault IS '[Operational] VEC-403 projection: Morpho vault positions as native position rows, one per vault deposit; instrument_key = vault contract address, holder_id = depositor address, quantity = assets in the vault asset''s native decimals, deal_type LOAN. chain_id and protocol_id are taken from the VAULT, never from the depositor: the position lives on the vault''s chain, and that is the fixed NULL-ness and provenance convention position_key() requires of each projection. Emits the position_state column contract; closure is applied by materialize_position_projection().';

-- MATERIALIZED is explicit: NOT MATERIALIZED would make each branch re-read the source.
CREATE OR REPLACE FUNCTION materialize_morpho_vault_refusals() RETURNS SETOF text
    LANGUAGE sql STABLE AS $fn$
WITH depositors AS MATERIALIZED (
    SELECT v.id AS vault_id, v.address AS vault_address, u.id AS user_id, u.address, u.chain_id
    FROM (SELECT DISTINCT p.user_id, p.morpho_vault_id FROM public.morpho_vault_position p) x
    JOIN public.morpho_vault v ON v.id = x.morpho_vault_id
    JOIN public."user" u ON u.id = x.user_id
)
SELECT msg FROM (
    -- holder_id is the depositor's address alone while chain_id comes from the vault, so two
    -- "user" rows sharing an address render one position_id and interleave under closure.
    SELECT format('vault %s holds deposits from %s "user" rows sharing address %s across chains %s',
                  encode(vault_address, 'hex'), count(DISTINCT user_id), encode(address, 'hex'),
                  string_agg(DISTINCT chain_id::text, ',' ORDER BY chain_id::text)) AS msg
    FROM depositors
    GROUP BY vault_id, vault_address, address
    HAVING count(DISTINCT user_id) > 1
    UNION ALL
    -- Only holder_id carries position_state's 40-hex check; instrument_key is a native key of
    -- any width. Without this the run aborts inside position_key(), naming no row.
    SELECT format('vault %s holder %s: a %s-byte holder address cannot render the 40-hex holder_id',
                  encode(vault_address, 'hex'), encode(address, 'hex'), length(address))
    FROM depositors
    WHERE length(address) <> 20
    GROUP BY vault_address, address
) all_msgs
ORDER BY msg
LIMIT 5;
$fn$;

COMMENT ON FUNCTION materialize_morpho_vault_refusals() IS '[Operational] VEC-403: why materialize_morpho_vault() would refuse to run, at most five messages, none when it may run. Two refusals: one address held by "user" rows on several chains depositing in one vault, which would collapse into one position_id, and a holder address that is not 20 bytes, which cannot render the 40-hex holder_id. Reads morpho_vault_position once, for its distinct (user_id, morpho_vault_id) pairs. Reads tiered chunks only under the caller''s timescaledb.enable_tiered_reads; materialize_morpho_vault() sets it on.';

-- Dropped first: the one-argument signature would survive CREATE OR REPLACE and make a call that
-- omits p_run_id ambiguous between the two.
DROP FUNCTION IF EXISTS materialize_morpho_vault(integer);
DROP FUNCTION IF EXISTS materialize_morpho_vault(integer, bigint);

CREATE OR REPLACE FUNCTION materialize_morpho_vault(p_build_id integer DEFAULT 0,
                                                    p_run_id bigint DEFAULT NULL,
                                                    p_window interval DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    -- Pinned to the materializer's own setting, or the refusals read fewer chunks than the run:
    -- morpho_vault_position tiers at one year and a database can set tiered reads off.
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    SELECT string_agg(msg, '; ' ORDER BY msg) INTO v_bad FROM materialize_morpho_vault_refusals() AS r(msg);
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'materialize_morpho_vault: refusing to run: %', v_bad;
    END IF;
    RETURN public.materialize_position_projection('public.position_morpho_vault'::regclass, p_build_id, p_run_id, p_window);
END
$fn$;

COMMENT ON FUNCTION materialize_morpho_vault(integer, bigint, interval) IS '[Operational] VEC-403: appends Morpho vault position observations into position_state via materialize_position_projection(position_morpho_vault). Refuses to run, naming up to five offenders, when one address on several chains would collapse two depositors into one position_id, or when a holder address is not 20 bytes and so cannot render the 40-hex holder_id position_state requires. See that function''s comment for the run contract. p_build_id and p_run_id are stamped on every row appended (ADR-0006 §2). p_window is forwarded to the materializer, which bounds the batch it reads; against this view it filters rows without pruning chunks.';

INSERT INTO migrations (filename) VALUES ('20260819_130000_materialize_morpho_vault.sql') ON CONFLICT (filename) DO NOTHING;
