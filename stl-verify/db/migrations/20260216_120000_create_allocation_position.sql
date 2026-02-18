CREATE TABLE IF NOT EXISTS allocation_position (
                                                   id               BIGSERIAL    PRIMARY KEY,
                                                   chain_id         INT          NOT NULL REFERENCES chain(chain_id),
                                                   token_id         BIGINT       NOT NULL REFERENCES token(id),
                                                   star             TEXT         NOT NULL CHECK (star IN ('spark', 'grove')),
                                                   proxy_address    BYTEA        NOT NULL,
                                                   balance          NUMERIC      NOT NULL,
                                                   scaled_balance   NUMERIC,
                                                   block_number     BIGINT       NOT NULL,
                                                   block_version    INT          NOT NULL DEFAULT 0,
                                                   tx_hash          BYTEA        NOT NULL,
                                                   log_index        INT          NOT NULL,
                                                   tx_amount        NUMERIC      NOT NULL,
                                                   direction        TEXT         NOT NULL CHECK (direction IN ('in', 'out')),
                                                   created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                                                   UNIQUE (chain_id, token_id, proxy_address, block_number, block_version, tx_hash, log_index, direction)
);

-- Current positions: latest snapshot per position
CREATE INDEX idx_alloc_pos_current ON allocation_position (chain_id, token_id, star, proxy_address, block_number DESC);

-- History by star
CREATE INDEX idx_alloc_pos_star ON allocation_position (star, chain_id, block_number DESC);

-- History by token
CREATE INDEX idx_alloc_pos_token ON allocation_position (token_id, chain_id, block_number DESC);

GRANT SELECT ON allocation_position TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON allocation_position TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE allocation_position_id_seq TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260216_120000_create_allocation_position.sql')
ON CONFLICT (filename) DO NOTHING;