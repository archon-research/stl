-- token_price_current.block_timestamp (VEC-672): the winning row's observation
-- time, so /v1/tokens/{id}/price and the CORE feed-liveness check can read the
-- cache instead of planning over every chunk of onchain_token_price.
--
-- Catalog-only, as VEC-661 does for sparklend_reserve_data_current: ALTER on the
-- cache and the trigger function replaced in place (no CREATE TRIGGER, so no lock
-- on the hypertable). The backfill is the SEPARATE next migration,
-- 20260910_120050: ADD COLUMN holds ACCESS EXCLUSIVE on the cache until commit,
-- and every price insert's trigger would queue behind a history scan run in the
-- same transaction.
--
-- Nullable: the backfill cannot date a row whose winning history row is no longer
-- readable in a plain session (tiered), and reads filter IS NOT NULL. Named
-- block_timestamp, the register's canonical name for a copy of this column.
SET LOCAL lock_timeout = '10s';

ALTER TABLE token_price_current ADD COLUMN IF NOT EXISTS block_timestamp TIMESTAMPTZ;

COMMENT ON COLUMN token_price_current.block_timestamp IS 'Derived (copy of onchain_token_price.timestamp). On-chain block time of the winning row; not a term of the newer-wins comparison. NULL for a row the 20260910_120050 backfill could not date (its winning history row is tiered); reads treat such a row as absent.';

-- Same body as 20260820_120000 plus the column. Still not SECURITY DEFINER: the
-- VEC-577 caches keep their grant form until VEC-684 aligns them.
CREATE OR REPLACE FUNCTION upsert_token_price_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO token_price_current AS cur
        (oracle_id, token_id, price_usd,
         block_number, block_version, processing_version, block_timestamp)
    VALUES
        (NEW.oracle_id, NEW.token_id, NEW.price_usd,
         NEW.block_number, NEW.block_version, NEW.processing_version, NEW."timestamp")
    ON CONFLICT (oracle_id, token_id) DO UPDATE SET
        price_usd = EXCLUDED.price_usd,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        block_timestamp = EXCLUDED.block_timestamp
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

INSERT INTO migrations (filename)
VALUES ('20260910_120000_add_block_timestamp_to_token_price_current.sql')
ON CONFLICT (filename) DO NOTHING;
