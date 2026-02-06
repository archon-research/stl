-- Create protocol_event table for storing decoded protocol events
-- Stores all events across all protocols in a single table with JSONB event data

CREATE TABLE IF NOT EXISTS protocol_event (
    id               BIGSERIAL PRIMARY KEY,
    chain_id         INT       NOT NULL REFERENCES chain(chain_id),
    protocol_id      BIGINT    NOT NULL REFERENCES protocol(id),
    block_number     BIGINT    NOT NULL,
    block_version    INT       NOT NULL DEFAULT 0,
    tx_hash          BYTEA     NOT NULL,
    log_index        INT       NOT NULL,
    contract_address BYTEA     NOT NULL,
    event_name       TEXT      NOT NULL,
    event_data       JSONB     NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, block_number, block_version, tx_hash, log_index)
);

-- Indexes for analytics query patterns
CREATE INDEX idx_protocol_event_block ON protocol_event (chain_id, block_number);
CREATE INDEX idx_protocol_event_name ON protocol_event (event_name);

-- Grant permissions to application roles
GRANT SELECT ON protocol_event TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON protocol_event TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE protocol_event_id_seq TO stl_readwrite;

-- Track this migration
INSERT INTO migrations (filename) VALUES ('20260206_150000_create_protocol_event.sql')
ON CONFLICT (filename) DO NOTHING;
