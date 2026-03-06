-- Migration: replace star on allocation_position with prime_id FK

-- 1. Add prime_id column (nullable initially for backfill)
ALTER TABLE allocation_position
    ADD COLUMN prime_id BIGINT REFERENCES prime(id);

-- 2. Backfill prime_id from existing star data
UPDATE allocation_position ap
SET    prime_id = p.id
FROM   prime p
WHERE  ap.star = p.name;

-- 3. Make prime_id NOT NULL after backfill
ALTER TABLE allocation_position
    ALTER COLUMN prime_id SET NOT NULL;

-- 4. Drop star column
ALTER TABLE allocation_position
    DROP COLUMN star;

-- 5. Drop old indexes that referenced star
DROP INDEX IF EXISTS idx_alloc_pos_current;
DROP INDEX IF EXISTS idx_alloc_pos_star;

-- 6. Recreate indexes using prime_id
CREATE INDEX idx_alloc_pos_current ON allocation_position (chain_id, token_id, prime_id, proxy_address, block_number DESC);

CREATE INDEX idx_alloc_pos_prime ON allocation_position (prime_id, chain_id, block_number DESC);

-- 7. Update unique constraint
ALTER TABLE allocation_position
    DROP CONSTRAINT allocation_position_chain_id_token_id_proxy_address_block_n_key;

ALTER TABLE allocation_position
    ADD CONSTRAINT allocation_position_chain_token_prime_proxy_block_ver_tx_key
        UNIQUE (chain_id, token_id, prime_id, proxy_address, block_number, block_version, tx_hash, log_index, direction);

INSERT INTO migrations (filename)
VALUES ('20260306_120000_replace_star_with_prime_id.sql')
ON CONFLICT (filename) DO NOTHING;