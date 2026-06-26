-- VEC-79: seed the stETH token before the Curve schema migration.
--
-- The Curve pool seed in 20260521_110000_create_curve_dex_tables.sql (VEC-260)
-- maps the stETH-classic / stETH-ng pool coins to this token. It MUST resolve
-- stETH by its (chain_id, address) natural key — never by a literal token id —
-- so this seed only has to guarantee the row exists; it does not pin a BIGSERIAL
-- id. (The Curve reward tokens in 20260521_100000 are seeded the same way: by
-- address, no id.)
--
-- ON CONFLICT (chain_id, address) DO NOTHING is idempotent: a fresh DB inserts
-- stETH at the next serial id, and staging (where stETH already exists) is a
-- no-op. The address is the EIP-55 checksummed mainnet stETH address.

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea, 'stETH', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260521_105000_seed_steth_token.sql')
ON CONFLICT (filename) DO NOTHING;
