-- VEC-417: holder_entity resolver (Gap B). Maps a position's holder to a legal entity_id.
--
-- The spec resolved issuer/custodian but never the holder. Two paths:
--   * USER  (wallet holder): user.id -> user.address matched against a catalogued address code in
--     entity_ref_codes_current -> entity_master_current.
--   * PRIME (prime holder):  prime.id -> entity_master_current.pipeline_prime_id (the internal SPV).
--
-- Resolves to the natural key entity_id. (The ticket's entity_sk / counterparty_scope /
-- counterparty_type_code are the pre-shipping draft -- none exist on the shipped tables; resolution is
-- by entity_id via the _current views, no surrogate.)
--
-- Only catalogued holders resolve: the vast majority of the ~348k users are anonymous EOAs with no
-- entity_ref_codes entry, so they return no row here -- a position-side LEFT JOIN yields NULL, which
-- VEC-524's unmapped-holder check surfaces. Nothing is invented for an unknown holder.
--
-- The USER join is written u.address = decode(code_value,'hex') (not encode(u.address) = code_value)
-- so the planner can index-probe user by address from the small code set, rather than compute
-- encode() across every user row. Address code_values are even-length lowercase hex (the
-- entity_ref_codes address CHECK), so decode is safe.

CREATE OR REPLACE VIEW holder_entity AS
SELECT
    'USER'::text AS holder_kind,
    u.id         AS holder_id,
    em.entity_id
FROM "user" u
JOIN entity_ref_codes_current c
    ON c.code_type IN ('BLOCKCHAIN_ADDRESS', 'CONTRACT_ADDRESS')
   AND u.address = decode(c.code_value, 'hex')
JOIN entity_master_current em ON em.entity_id = c.entity_id
UNION ALL
SELECT
    'PRIME'::text AS holder_kind,
    p.id          AS holder_id,
    em.entity_id
FROM prime p
JOIN entity_master_current em ON em.pipeline_prime_id = p.id;

COMMENT ON VIEW holder_entity IS '[Resolver] Position holder -> legal entity_id (VEC-417, Gap B). holder_kind USER: user.id via its address code in entity_ref_codes_current; holder_kind PRIME: prime.id via entity_master_current.pipeline_prime_id. Unknown holders return no row (resolve to NULL on a LEFT JOIN); coverage is checked by VEC-524.';

GRANT SELECT ON holder_entity TO stl_readonly;
GRANT SELECT ON holder_entity TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260722_180000_create_holder_entity_resolver.sql') ON CONFLICT (filename) DO NOTHING;
