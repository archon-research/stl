-- VEC-817: the chain a prime's vault is deployed on.
--
-- prime_debt has no chain column, so schema_master asserted chain 1 as a literal. Nothing enforced it:
-- the indexer takes its chain from an env var, and a second one elsewhere would write rows a chain-1
-- loader run resolves against Ethereum's archive. The chain belongs on the prime the snapshots join.
ALTER TABLE prime
    ADD COLUMN IF NOT EXISTS chain_id INT REFERENCES chain (chain_id);

UPDATE prime SET chain_id = 1 WHERE chain_id IS NULL;

ALTER TABLE prime
    ALTER COLUMN chain_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_prime_chain ON prime (chain_id);

COMMENT ON COLUMN prime.chain_id IS 'The chain the vault at vault_address is deployed on. Roles: Foreign key (chain). Backfilled to 1 for the primes that predate this column, all of them Ethereum vaults. prime_debt resolves its own chain through this column rather than a constant, so a prime on another chain is enumerated by that chain''s block-meta run and by no other. vault_address stays globally UNIQUE: the same address on two chains is a distinct prime this constraint would refuse, and widening it changes how the API resolves a prime by address.';

INSERT INTO migrations (filename)
VALUES ('20260916_120000_add_chain_id_to_prime.sql')
ON CONFLICT (filename) DO NOTHING;
