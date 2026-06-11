-- Seed the Anchorage custody escrow as a receipt token (Ethereum mainnet,
-- chain_id = 1) so the allocations API can surface Spark's Anchorage custody
-- position with the non-null underlying_token_id / receipt_token_id that
-- AllocationResponse requires.
--
-- Background: Anchorage custody data is ingested by the anchorage-indexer
-- cronjob into anchorage_package_snapshot, and the allocation tracker
-- explicitly skips token_type == "anchorage"
-- (internal/services/allocation_tracker/source_stubs.go: NewSkipSource).
-- Because of that skip, no allocation_position row is ever written for the
-- escrow, so the token / protocol / receipt_token rows the position handler
-- would otherwise upsert never get created. The /allocations endpoint merges
-- a synthetic row from the snapshot table (see get_anchorage_position), but it
-- still needs these catalog rows to populate the surrogate ids and addresses.
--
-- Double-counting is not a risk: the anchorage-skip source guarantees no
-- allocation_position row will ever match this receipt token, so the
-- snapshot-sourced row is the only one that can reference it.
--
-- 0x49506c3aa028693458d6ee816b2ec28522946872 is the Anchorage Escrow contract
-- (docs/prices_oracles_reference.md). USDC is the underlying. Looks up the
-- USDC token so the FK stays correct across environments (ids differ per env),
-- inserts nothing if USDC is absent, and no-ops if the rows already exist.

-- 1. Escrow token row (the "receipt token" side, symbol ANCHORAGE).
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\x49506c3aa028693458d6ee816b2ec28522946872'::bytea, 'ANCHORAGE', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

-- 2. Anchorage protocol row. protocol.address is NOT NULL; the escrow address
--    doubles as the protocol identity (there is no separate on-chain anchorage
--    protocol contract STL tracks). The category service maps protocol_name
--    "anchorage" to category "allocation" via its fallback.
INSERT INTO protocol (chain_id, address, name, protocol_type)
VALUES (1, '\x49506c3aa028693458d6ee816b2ec28522946872'::bytea, 'anchorage', 'custody')
ON CONFLICT (chain_id, address) DO NOTHING;

-- 3. Receipt token linking the escrow -> USDC under the anchorage protocol.
INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol)
SELECT 1, p.id, t.id, '\x49506c3aa028693458d6ee816b2ec28522946872'::bytea, 'ANCHORAGE'
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.name = 'anchorage'
  AND t.chain_id = 1 AND t.address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260611_120000_seed_anchorage_receipt_token.sql')
ON CONFLICT (filename) DO NOTHING;
