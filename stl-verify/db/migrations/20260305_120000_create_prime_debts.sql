-- Migration: prime agents registry and debt snapshots

CREATE TABLE IF NOT EXISTS primes (
                                      id            BIGSERIAL    PRIMARY KEY,
                                      name          TEXT         NOT NULL UNIQUE,
                                      vault_address BYTEA        NOT NULL UNIQUE,
                                      created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

INSERT INTO primes (name, vault_address) VALUES
                                             ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba'),
                                             ('grove',  '\x26512a41c8406800f21094a7a7a0f980f6e25d43'),
                                             ('obex',   '\xf275110dfe7b80df66a762f968f59b70babe2b29')
ON CONFLICT DO NOTHING;

-- prime_debts stores append-only debt snapshots.
-- debt_wad is NUMERIC — stores the exact wad-scaled big.Int (art * rate / 1e27).
-- block_number records which Ethereum block the debt was read at.
CREATE TABLE IF NOT EXISTS prime_debts (
                                           id            BIGSERIAL    PRIMARY KEY,
                                           prime_id      BIGINT       NOT NULL REFERENCES primes(id),
                                           prime_name    TEXT         NOT NULL,
                                           vault_address BYTEA        NOT NULL,
                                           ilk_name      TEXT         NOT NULL,
                                           debt_wad      NUMERIC      NOT NULL,
                                           block_number  BIGINT       NOT NULL,
                                           synced_at     TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_prime_debts_prime_synced
    ON prime_debts (prime_id, synced_at DESC);

CREATE INDEX IF NOT EXISTS idx_prime_debts_synced_at
    ON prime_debts (synced_at DESC);

CREATE INDEX IF NOT EXISTS idx_prime_debts_block
    ON prime_debts (prime_id, block_number DESC);

INSERT INTO migrations (filename)
VALUES ('20260305_120000_create_prime_debts.sql')
ON CONFLICT (filename) DO NOTHING;