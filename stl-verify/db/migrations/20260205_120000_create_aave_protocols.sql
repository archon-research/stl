-- Aave V2
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
VALUES (1, '\x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'::bytea, 'Aave V2', 'lending', 11362579, NOW());

-- Aave V3
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
VALUES (1, '\x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2'::bytea, 'Aave V3', 'lending', 16776401, NOW());

-- Aave V3 Lido
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
VALUES (1, '\x4e033931ad43597d96d6bcc25c280717730b58b1'::bytea, 'Aave V3 Lido', 'lending', 18720000, NOW());

-- Aave V3 RWA
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
VALUES (1, '\xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8'::bytea, 'Aave V3 RWA', 'lending', 19320000, NOW());