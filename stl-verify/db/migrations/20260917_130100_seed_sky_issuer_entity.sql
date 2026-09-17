-- VEC-406: Sky as the issuer entity behind position_sky_prime_debt, with the MCD Vat as its contract.
-- A DAO issuing the currency the prime products are denominated in; internal to the Sky/Prime group.
-- No LEI or domicile is sourced, so those stay NULL rather than guessed.

INSERT INTO entity_master (entity_id, short_name, entity_type, counterparty_role, is_internal, source_system, change_reason) VALUES
    ('em-issuer-sky', 'Sky', 'DAO', 'ISSUER', true, 'curated', 'Curated (VEC-406): Sky, issuer of the currency; the MCD Vat is its ledger')
ON CONFLICT (entity_id, processing_version) DO NOTHING;

INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason) VALUES
    ('CONTRACT_ADDRESS', '35d1b3f3d7966a1dfe207aa4514c12a259a0492b', 'em-issuer-sky', 'Curated (VEC-406): MCD Vat contract')
ON CONFLICT (code_type, code_value, processing_version) DO NOTHING;

INSERT INTO migrations (filename) VALUES ('20260917_130100_seed_sky_issuer_entity.sql') ON CONFLICT (filename) DO NOTHING;
