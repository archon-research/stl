-- VEC-817: the chain a prime's vault is deployed on.
--
-- prime_debt has no chain column, so schema_master asserted chain 1 as a literal; nothing enforced it.
ALTER TABLE prime
    ADD COLUMN IF NOT EXISTS chain_id INT REFERENCES chain (chain_id);

UPDATE prime SET chain_id = 1 WHERE chain_id IS NULL;

ALTER TABLE prime
    ALTER COLUMN chain_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_prime_chain ON prime (chain_id);

COMMENT ON COLUMN prime.chain_id IS 'Roles: FK→chain.chain_id. The chain the vault at vault_address is deployed on; prime_debt resolves its own chain through this column rather than a constant.';

INSERT INTO migrations (filename)
VALUES ('20260916_120000_add_chain_id_to_prime.sql')
ON CONFLICT (filename) DO NOTHING;
