-- VEC-418: seed entity_master + entity_ref_codes from the prime/protocol registry.
--
-- Generated NATIVELY from chain/protocol data at apply time -- every value is read from the `prime`
-- and `protocol` base tables via INSERT ... SELECT (addresses via encode(...,'hex')). Nothing is
-- transcribed by hand (so no silent typo can slip past the hex CHECK) and nothing is a classification
-- guess. One entity per registry row (no dedup) so the mapping is 1:1 and reviewable; deduping
-- deployments (e.g. the Aave rows) is a later, reviewable decision, not baked in here.
--
-- Field provenance:
--   * entity_id            authored key: 'em-prime-'||id / 'em-protocol-'||id (stable on the registry id)
--   * short_name           prime.name / protocol.name, verbatim
--   * pipeline_prime_id /
--     pipeline_protocol_id  the registry id, verbatim
--   * code_value           encode(vault_address|address,'hex') -- the on-chain address, no transcription
--   * is_internal          rule: from the prime registry => true; from the protocol registry => false
--   * counterparty_role    rule: a protocol registry row is a PROTOCOL_OPERATOR (primes: none)
--   * code_type            rule: a registry address is a deployed contract => CONTRACT_ADDRESS
--   * source_system        rule: origin table ('prime' / 'protocol')
--   * entity_type          NO native source (legal form is off-chain) => 'UNKNOWN'. NOT a guess.
--                          Classification is a separate, sourced/reviewed pass; VEC-524 surfaces every
--                          UNKNOWN so it cannot sit silently. domicile/country_of_risk/sector/lei stay NULL
--                          for the same reason.
--
-- Excluded by construction: issuers and the Anchorage custodian have no prime/protocol registry row,
-- so they are not seeded here (they need a curated, sourced entry -- tracked on VEC-418).

INSERT INTO entity_master (entity_id, short_name, entity_type, is_internal, source_system, pipeline_prime_id, change_reason)
SELECT 'em-prime-' || id, name, 'UNKNOWN', true, 'prime', id, 'Seed (VEC-418): prime registry'
FROM prime
ON CONFLICT (entity_id, processing_version) DO NOTHING;

INSERT INTO entity_master (entity_id, short_name, entity_type, counterparty_role, is_internal, source_system, pipeline_protocol_id, change_reason)
SELECT 'em-protocol-' || id, name, 'UNKNOWN', 'PROTOCOL_OPERATOR', false, 'protocol', id, 'Seed (VEC-418): protocol registry'
FROM protocol
ON CONFLICT (entity_id, processing_version) DO NOTHING;

INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason)
SELECT 'CONTRACT_ADDRESS', encode(vault_address, 'hex'), 'em-prime-' || id, 'Seed (VEC-418): prime vault address'
FROM prime
ON CONFLICT (code_type, code_value, processing_version) DO NOTHING;

INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason)
SELECT 'CONTRACT_ADDRESS', encode(address, 'hex'), 'em-protocol-' || id, 'Seed (VEC-418): protocol contract address'
FROM protocol
ON CONFLICT (code_type, code_value, processing_version) DO NOTHING;

INSERT INTO migrations (filename) VALUES ('20260722_150000_seed_entity_master_operators.sql') ON CONFLICT (filename) DO NOTHING;
