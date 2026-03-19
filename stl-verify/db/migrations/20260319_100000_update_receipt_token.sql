ALTER TABLE receipt_token ADD COLUMN IF NOT EXISTS chain_id INT;

ALTER TABLE receipt_token ALTER COLUMN chain_id SET NOT NULL;

ALTER TABLE receipt_token DROP CONSTRAINT IF EXISTS receipt_token_chain_fk;
ALTER TABLE receipt_token ADD CONSTRAINT receipt_token_chain_fk
    FOREIGN KEY (chain_id) REFERENCES chain(chain_id);

ALTER TABLE receipt_token DROP CONSTRAINT IF EXISTS receipt_token_protocol_underlying_unique;

ALTER TABLE receipt_token DROP CONSTRAINT IF EXISTS receipt_token_chain_address_unique;
ALTER TABLE receipt_token ADD CONSTRAINT receipt_token_chain_address_unique
    UNIQUE (chain_id, receipt_token_address);

CREATE INDEX IF NOT EXISTS idx_receipt_token_protocol ON receipt_token (protocol_id);
CREATE INDEX IF NOT EXISTS idx_receipt_token_underlying ON receipt_token (underlying_token_id);
