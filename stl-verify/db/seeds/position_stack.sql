-- Dev seed for the position_id stack: source rows for the three materializers, then
-- position_state and position_current populated from them. Run: make dev-seed-positions.
-- NOT a migration -- it sits outside db/migrations/ so the migrator never sees it.

-- Idempotent: every insert is ON CONFLICT DO NOTHING on a natural key and every id is
-- looked up rather than captured from RETURNING, so a re-run is a no-op. The
-- materializers are idempotent in their own right.

-- Fixture: holder aa supplies twice to a market (two observations, one position); holder
-- bb borrows and posts collateral (ONE raw row, TWO positions); holder aa enters then
-- exits a vault (a closing zero-row); seed-prime borrows in a Sky ilk.

BEGIN;

INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;

INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xf5eed0', 'seed-morpho')
  ON CONFLICT (chain_id, address) DO NOTHING;
INSERT INTO token (chain_id, address, symbol, decimals) VALUES
    (1, '\xf5eedda1', 'SEED-USD', 6),
    (1, '\xf5eedbee', 'SEED-ETH', 18)
  ON CONFLICT (chain_id, address) DO NOTHING;
INSERT INTO "user" (chain_id, address) VALUES
    (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
    (1, '\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')
  ON CONFLICT (chain_id, address) DO NOTHING;

-- Morpho market (VEC-402). oracle_address, irm_address and lltv are NOT NULL with no
-- default, which is the seed's easiest thing to get wrong: the whole block rolls back and
-- every materializer then reports 0 for want of source rows, not for want of logic.
INSERT INTO morpho_market (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
                           oracle_address, irm_address, lltv, created_at_block)
SELECT 1, p.id, '\xf5eed123', lt.id, ct.id, '\xf5eed00a', '\xf5eed00b', 860000000000000000, 1
FROM protocol p,
     token lt, token ct
WHERE p.chain_id = 1 AND p.address = '\xf5eed0'
  AND lt.chain_id = 1 AND lt.address = '\xf5eedda1'
  AND ct.chain_id = 1 AND ct.address = '\xf5eedbee'
ON CONFLICT (chain_id, market_id) DO NOTHING;

INSERT INTO morpho_market_position
    (user_id, morpho_market_id, block_number, block_version, timestamp,
     supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
SELECT u.id, m.id, v.blk, 0, v.ts, v.ss, v.bs, v.coll, v.sa, v.ba
FROM morpho_market m
CROSS JOIN (VALUES
    ('\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::bytea, 100, '2026-01-01T00:00:00Z'::timestamptz,  90, 0, 0, 100,  0),
    ('\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::bytea, 200, '2026-01-02T00:00:00Z'::timestamptz, 140, 0, 0, 150,  0),
    ('\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'::bytea, 100, '2026-01-01T00:00:00Z'::timestamptz,   0, 25, 5,   0, 30)
  ) AS v(holder, blk, ts, ss, bs, coll, sa, ba)
JOIN "user" u ON u.chain_id = 1 AND u.address = v.holder
WHERE m.chain_id = 1 AND m.market_id = '\xf5eed123'
ON CONFLICT DO NOTHING;

-- Morpho vault (VEC-403): a single native instrument, so one raw row is one position.
INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
SELECT 1, p.id, '\xf5eedabc', 'seedVault', t.id, 1, 1
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.address = '\xf5eed0'
  AND t.chain_id = 1 AND t.address = '\xf5eedda1'
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
SELECT u.id, vt.id, v.blk, 0, v.ts, v.sh, v.assets
FROM morpho_vault vt
JOIN "user" u ON u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
CROSS JOIN (VALUES
    (100, '2026-01-01T00:00:00Z'::timestamptz, 90, 100),
    (200, '2026-01-02T00:00:00Z'::timestamptz,  0,   0)
  ) AS v(blk, ts, sh, assets)
WHERE vt.chain_id = 1 AND vt.address = '\xf5eedabc'
ON CONFLICT DO NOTHING;

-- Sky prime debt (VEC-406): holder is the prime's vault address, instrument is the ilk name.
INSERT INTO prime (name, vault_address) VALUES ('seed-prime', '\xf5eedccc00000000000000000000000000000000')
  ON CONFLICT (name) DO NOTHING;
INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at,
                        processing_version, build_id)
SELECT p.id, 'ALLOCATOR-SEED-A', 1000, 100, 0, '2026-01-01T00:00:00Z', 0, 0
FROM prime p WHERE p.name = 'seed-prime'
ON CONFLICT DO NOTHING;

COMMIT;

-- Materialize whatever is installed. Each materializer lives in its own PR (VEC-402/403/406),
-- so on a database without one this reports it as missing rather than silently seeding nothing.
DO $$
DECLARE fn text; n bigint;
BEGIN
  FOREACH fn IN ARRAY ARRAY['materialize_morpho_market','materialize_morpho_vault','materialize_sky_prime_debt'] LOOP
    IF to_regprocedure(fn || '(integer)') IS NULL THEN
      RAISE WARNING '% is not installed; its migration is not applied here', fn;
    ELSE
      EXECUTE format('SELECT %I(0)', fn) INTO n;
      RAISE NOTICE '% appended % position_state row(s)', fn, n;
    END IF;
  END LOOP;
END $$;
