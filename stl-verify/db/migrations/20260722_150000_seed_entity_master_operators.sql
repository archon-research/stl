-- VEC-418: initial entity_master + entity_ref_codes seed -- derivable internal + operator entities.
--
-- Scope of THIS seed: only entities and codes that are fully derivable from live protocol/prime data
-- (no external sourcing, no house classification beyond the vocab's natural bucket). Deliberately
-- EXCLUDED here (need a decision or a source, tracked on VEC-418):
--   * Issuers (Circle, Lido, ...) -- addresses are intrinsic but domicile/sector/LEI have no protocol
--     source; seed them once sourced, per the Part C decision.
--   * DEX operators (Curve/Uniswap/Balancer) -- include only if we hold positions there.
--   * Anchorage custodian -- not a prime/protocol row; a curated entry.
-- All sourcing FK fields (domicile_country, country_of_risk, sector, origination_type, lei, ...) are
-- left NULL here on purpose. entity_master is SCD2 (PK entity_id, processing_version), so a later
-- processing_version can enrich these without rewriting the seed.
--
-- Flagged defaults for review (Simon/Peter) -- adjustable before merge:
--   * entity_type: primes = SPV (factual: a prime IS an SPV); protocol operators = DAO (the vocab's
--     natural bucket for Aave/Morpho/Maple/SparkLend/Fluid).
--   * Aave dedup: one em-aave entity for all 5 mainnet+Avalanche deployments. entity_master.
--     pipeline_protocol_id is singular, so it points at the V3 mainnet id (3); the other four
--     deployment addresses are carried as CONTRACT_ADDRESS ref codes below. If per-deployment
--     entities are preferred instead, split em-aave into five.
--
-- Addresses are stored hex-encoded, lowercase, no 0x prefix (the encode(addr,'hex') form the
-- entity_ref_codes address CHECK and VEC-417's holder join expect).

-- Internal entities: the 3 primes (SPVs holding positions).
INSERT INTO entity_master (entity_id, change_reason, short_name, entity_type, is_internal, pipeline_prime_id, source_system) VALUES
    ('em-spark', 'Initial seed (VEC-418): prime SPV', 'spark', 'SPV', true, 1, 'prime'),
    ('em-grove', 'Initial seed (VEC-418): prime SPV', 'grove', 'SPV', true, 2, 'prime'),
    ('em-obex',  'Initial seed (VEC-418): prime SPV', 'obex',  'SPV', true, 3, 'prime');

-- Operator entities: protocol operators (single-deployment protocols + the deduped Aave).
INSERT INTO entity_master (entity_id, change_reason, short_name, entity_type, counterparty_role, is_internal, pipeline_protocol_id, source_system) VALUES
    ('em-sparklend',   'Initial seed (VEC-418): protocol operator', 'SparkLend',   'DAO', 'PROTOCOL_OPERATOR', false, 1,       'protocol'),
    ('em-morpho-blue', 'Initial seed (VEC-418): protocol operator', 'Morpho Blue', 'DAO', 'PROTOCOL_OPERATOR', false, 6,       'protocol'),
    ('em-maple',       'Initial seed (VEC-418): protocol operator', 'Maple',       'DAO', 'PROTOCOL_OPERATOR', false, 6330502, 'protocol'),
    ('em-fluid',       'Initial seed (VEC-418): protocol operator', 'Fluid',       'DAO', 'PROTOCOL_OPERATOR', false, 7909060, 'protocol'),
    ('em-aave',        'Initial seed (VEC-418): protocol operator (deduped across 5 deployments)', 'Aave', 'DAO', 'PROTOCOL_OPERATOR', false, 3, 'protocol');

-- Prime vault addresses (BLOCKCHAIN_ADDRESS -> entity_id).
INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason) VALUES
    ('BLOCKCHAIN_ADDRESS', '691a6c29e9e96dd897718305427ad5d534db16ba', 'em-spark', 'Initial seed (VEC-418): prime vault address'),
    ('BLOCKCHAIN_ADDRESS', '26512a41c8406800f21094a7a7a0f980f6e25d43', 'em-grove', 'Initial seed (VEC-418): prime vault address'),
    ('BLOCKCHAIN_ADDRESS', 'f275110dfe7b80df66a762f968f59b70babe2b29', 'em-obex',  'Initial seed (VEC-418): prime vault address');

-- Protocol contract addresses (CONTRACT_ADDRESS -> entity_id). Aave carries all 5 deployments.
INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason) VALUES
    ('CONTRACT_ADDRESS', 'c13e21b648a5ee794902342038ff3adab66be987', 'em-sparklend',   'Initial seed (VEC-418): SparkLend'),
    ('CONTRACT_ADDRESS', 'bbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb', 'em-morpho-blue', 'Initial seed (VEC-418): Morpho Blue'),
    ('CONTRACT_ADDRESS', '804a6f5f667170f545bf14e5ddb48c70b788390c', 'em-maple',       'Initial seed (VEC-418): Maple'),
    ('CONTRACT_ADDRESS', '52aa899454998be5b000ad077a46bbe360f4e497', 'em-fluid',       'Initial seed (VEC-418): Fluid'),
    ('CONTRACT_ADDRESS', '7d2768de32b0b80b7a3454c06bdac94a69ddc7a9', 'em-aave',        'Initial seed (VEC-418): Aave V2'),
    ('CONTRACT_ADDRESS', '87870bca3f3fd6335c3f4ce8392d69350b4fa4e2', 'em-aave',        'Initial seed (VEC-418): Aave V3'),
    ('CONTRACT_ADDRESS', '4e033931ad43597d96d6bcc25c280717730b58b1', 'em-aave',        'Initial seed (VEC-418): Aave V3 Lido'),
    ('CONTRACT_ADDRESS', 'ae05cd22df81871bc7cc2a04becfb516bfe332c8', 'em-aave',        'Initial seed (VEC-418): Aave V3 RWA'),
    ('CONTRACT_ADDRESS', '794a61358d6845594f94dc1db02a252b5b4814ad', 'em-aave',        'Initial seed (VEC-418): Aave V3 Avalanche');

INSERT INTO migrations (filename) VALUES ('20260722_150000_seed_entity_master_operators.sql') ON CONFLICT (filename) DO NOTHING;
