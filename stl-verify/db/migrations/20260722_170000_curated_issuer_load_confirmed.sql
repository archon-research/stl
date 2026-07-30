-- VEC-525: curated issuer load -- confirmed/sourced subset only.
--
-- Creates the issuer entities that are NOT in the prime/protocol registry (so #611's native seed never
-- creates them), for the five issuers whose legal entity is GLEIF-sourced and whose issuer pick is
-- settled. Each gets an entity_master row + its token CONTRACT_ADDRESS in entity_ref_codes (the
-- resolution key: a position in the token resolves through the address to the issuer entity).
--
-- Every non-native value is GLEIF-sourced and cited in change_reason (provenance is first-class).
-- entity_type = the GLEIF legal form; domicile/country_of_risk = the GLEIF registered country; lei =
-- the GLEIF LEI. sector is left NULL -- GLEIF carries no GICS, so it is not yet sourced (never guessed).
-- counterparty_role = ISSUER (structural: these issue the token). is_internal = false.
--
-- HELD BACK (not confirmed, deliberately excluded): Tether (issuing entity + SV/BVI jurisdiction
-- unsettled), Aave (Aave Group SEZC vs the Avara rebrand), Maple (Maple BLC Limited vs a fund vehicle),
-- and every entity still at XX/UNKNOWN. They stay UNKNOWN in the native seed until sourced/resolved.
--
-- Addresses are the token contracts (hex, lowercase, no 0x -- the entity_ref_codes CHECK form),
-- verified against the token table.

INSERT INTO entity_master (entity_id, legal_name, short_name, entity_type, counterparty_role, is_internal, domicile_country, country_of_risk, lei, source_system, change_reason) VALUES
    ('em-issuer-circle',   'Circle Internet Financial, LLC', 'Circle',   'LLC',         'ISSUER', false, 'US', 'US', '549300UHJLR6LBGAFV55', 'curated', 'Curated (VEC-525): GLEIF Circle Internet Financial, LLC 549300UHJLR6LBGAFV55'),
    ('em-issuer-paxos',    'Paxos Trust Company, LLC',       'Paxos',    'LLC',         'ISSUER', false, 'US', 'US', '549300EQW4J4RXDLD359', 'curated', 'Curated (VEC-525): GLEIF Paxos Trust Company, LLC 549300EQW4J4RXDLD359 (NY trust co organised as LLC)'),
    ('em-issuer-coinbase', 'Coinbase, Inc.',                 'Coinbase', 'CORPORATION', 'ISSUER', false, 'US', 'US', '549300QHD76EP6ZKTT48', 'curated', 'Curated (VEC-525): GLEIF Coinbase, Inc. 549300QHD76EP6ZKTT48'),
    ('em-issuer-bitgo',    'BitGo Trust Company, Inc.',      'BitGo',    'CORPORATION', 'ISSUER', false, 'US', 'US', '254900QXDWGM1T0HGF47', 'curated', 'Curated (VEC-525): GLEIF BitGo Trust Company, Inc. 254900QXDWGM1T0HGF47'),
    ('em-issuer-ethena',   'Ethena (BVI) Limited',           'Ethena',   'CORPORATION', 'ISSUER', false, 'VG', 'VG', '984500V3A60AFE7F7859', 'curated', 'Curated (VEC-525): GLEIF Ethena (BVI) Limited 984500V3A60AFE7F7859')
ON CONFLICT (entity_id, processing_version) DO NOTHING;

-- Token contract -> issuer (CONTRACT_ADDRESS resolution key). Primary token per issuer; more tokens
-- can be appended later.
INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason) VALUES
    ('CONTRACT_ADDRESS', 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'em-issuer-circle',   'Curated (VEC-525): USDC token contract'),
    ('CONTRACT_ADDRESS', '6c3ea9036406852006290770bedfcaba0e23a0e8', 'em-issuer-paxos',    'Curated (VEC-525): PYUSD token contract'),
    ('CONTRACT_ADDRESS', 'cbb7c0000ab88b473b1f5afd9ef808440eed33bf', 'em-issuer-coinbase', 'Curated (VEC-525): cbBTC token contract'),
    ('CONTRACT_ADDRESS', '2260fac5e5542a773aa44fbcfedf7c193bc2c599', 'em-issuer-bitgo',    'Curated (VEC-525): WBTC token contract'),
    ('CONTRACT_ADDRESS', '4c9edd5852cd905f086c759e8383e09bff1e68b3', 'em-issuer-ethena',   'Curated (VEC-525): USDe token contract')
ON CONFLICT (code_type, code_value, processing_version) DO NOTHING;

INSERT INTO migrations (filename) VALUES ('20260722_170000_curated_issuer_load_confirmed.sql') ON CONFLICT (filename) DO NOTHING;
