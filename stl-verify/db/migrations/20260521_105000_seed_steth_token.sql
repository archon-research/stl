-- VEC-79: seed stETH at id=227 before the Curve schema migration.
--
-- The curve_pool seed in 20260521_110000_create_curve_dex_tables.sql encodes
-- coin_token_ids = [4 (WETH), 227 (stETH)] for the stETH-classic and stETH-ng
-- pools, with a post-seed assertion that joins by token.id and verifies the
-- coin_addresses[i] matches token.address. The ID 227 was "verified live on
-- 2026-05-20" against staging, where the token table had accumulated rows
-- ad-hoc beyond what the migration set seeds. Fresh containers don't have
-- stETH at all, so the assertion fires before any integration test can run.
--
-- Insert stETH explicitly at id=227 and advance the BIGSERIAL so subsequent
-- inserts don't collide. ON CONFLICT DO NOTHING covers the staging case
-- where id=227 already happens to be stETH.

INSERT INTO token (id, chain_id, address, symbol, decimals)
VALUES (227, 1, '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea, 'stETH', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- Advance the sequence past 227 so future BIGSERIAL inserts don't collide.
SELECT setval(
    pg_get_serial_sequence('token', 'id'),
    GREATEST((SELECT COALESCE(MAX(id), 0) FROM token), 227),
    true
);

INSERT INTO migrations (filename)
VALUES ('20260521_105000_seed_steth_token.sql')
ON CONFLICT (filename) DO NOTHING;
