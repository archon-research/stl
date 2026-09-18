-- VEC-616: the held book, seeded into the instrument register — 21 native keys resolving to
-- 14 securities, generated 2026-08-26 from the live token table (addresses lowercase hex, no
-- 0x). security_id values follow the security classification worksheet's sec-* ids and are soft
-- references, so this applies before the SECURITY nodes exist and resolves once they load
-- (VEC-625/627). The full-population load — receipt tokens, debt legs, Morpho market ids, Sky
-- ilks, Anchorage packages, holder addresses — is one slice per namespace and hands out
-- separately; each slice needs its namespace's security ids, which is what it waits on.
--
-- A plain INSERT, not ON CONFLICT DO NOTHING: the migration self-registers, so it runs once, and
-- DO NOTHING would swallow a collision the guards raise on.

INSERT INTO instrument_register (instrument_key, key_namespace, security_id, chain_id, attrs, valid_from, actor, change_reason_code, change_reason, source_system) VALUES
 ('00000000efe302beaa2b3e6e1b18d08d69a9012a','token_address','sec-ausd',1,'{"symbol":"AUSD","address":"00000000efe302beaa2b3e6e1b18d08d69a9012a","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('6a9da2d710bb9b700acde7cb81f10f1ff8c89041','token_address','sec-buidl',1,'{"symbol":"BUIDL-I","address":"6a9da2d710bb9b700acde7cb81f10f1ff8c89041","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('6b175474e89094c44da98b954eedeac495271d0f','token_address','sec-dai',1,'{"symbol":"DAI","address":"6b175474e89094c44da98b954eedeac495271d0f","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('2c0adff8e114f3ca106051144353ac703d24b901','token_address','sec-gaclo1',43114,'{"symbol":"GACLO-1","address":"2c0adff8e114f3ca106051144353ac703d24b901","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('6c3ea9036406852006290770bedfcaba0e23a0e8','token_address','sec-pyusd',1,'{"symbol":"PYUSD","address":"6c3ea9036406852006290770bedfcaba0e23a0e8","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('8292bb45bf1ee4d140127049757c2e0ff06317ed','token_address','sec-rlusd',1,'{"symbol":"RLUSD","address":"8292bb45bf1ee4d140127049757c2e0ff06317ed","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('51c2d74017390cbbd30550179a16a1c28f7210fc','token_address','sec-stac',1,'{"symbol":"STAC","address":"51c2d74017390cbbd30550179a16a1c28f7210fc","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48','token_address','sec-usdc',1,'{"symbol":"USDC","address":"a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('0b2c639c533813f4aa9d7837caf62653d097ff85','token_address','sec-usdc',10,'{"symbol":"USDC","address":"0b2c639c533813f4aa9d7837caf62653d097ff85","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('078d782b760474a361dda0af3839290b0ef57ad6','token_address','sec-usdc',130,'{"symbol":"USDC","address":"078d782b760474a361dda0af3839290b0ef57ad6","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('833589fcd6edb6e08f4c7c32d4f71b54bda02913','token_address','sec-usdc',8453,'{"symbol":"USDC","address":"833589fcd6edb6e08f4c7c32d4f71b54bda02913","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('af88d065e77c8cc2239327c5edb3a432268e5831','token_address','sec-usdc',42161,'{"symbol":"USDC","address":"af88d065e77c8cc2239327c5edb3a432268e5831","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('b97ef9ef8734c71904d8002f8b6bc66dd9c48a6e','token_address','sec-usdc',43114,'{"symbol":"USDC","address":"b97ef9ef8734c71904d8002f8b6bc66dd9c48a6e","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('dc035d45d973e3ec169d2276ddab16f1e407384f','token_address','sec-usds',1,'{"symbol":"USDS","address":"dc035d45d973e3ec169d2276ddab16f1e407384f","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('6491c05a82219b8d1479057361ff1654749b876b','token_address','sec-usds',42161,'{"symbol":"USDS","address":"6491c05a82219b8d1479057361ff1654749b876b","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('dac17f958d2ee523a2206206994597c13d831ec7','token_address','sec-usdt',1,'{"symbol":"USDT","address":"dac17f958d2ee523a2206206994597c13d831ec7","decimals":6}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('4c9edd5852cd905f086c759e8383e09bff1e68b3','token_address','sec-usde',1,'{"symbol":"USDe","address":"4c9edd5852cd905f086c759e8383e09bff1e68b3","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','token_address','sec-weth',1,'{"symbol":"WETH","address":"c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('a3931d71877c0e7a3148cb7eb4463524fec27fbd','token_address','sec-susds',1,'{"symbol":"sUSDS","address":"a3931d71877c0e7a3148cb7eb4463524fec27fbd","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('5875eee11cf8398102fdad704c9e96607675467a','token_address','sec-susds',8453,'{"symbol":"sUSDS","address":"5875eee11cf8398102fdad704c9e96607675467a","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token'),
 ('9d39a5de30e57443bff2a8307a4256c8797a3497','token_address','sec-susde',1,'{"symbol":"sUSDe","address":"9d39a5de30e57443bff2a8307a4256c8797a3497","decimals":18}'::jsonb,'2026-08-26','seed','SEED_LOAD','Seed: held book from token table','token');

INSERT INTO migrations (filename) VALUES ('20260915_120200_secstore_instrument_register_held_book_seed.sql') ON CONFLICT (filename) DO NOTHING;
