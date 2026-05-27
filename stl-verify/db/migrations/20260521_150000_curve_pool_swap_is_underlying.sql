-- Add is_underlying so consumers can distinguish NG meta-pool TokenExchange
-- (wrapped indices into pool.coins) from TokenExchangeUnderlying (underlying
-- indices). Both share the same (sold_id, bought_id) SMALLINT columns;
-- without this flag a `WHERE sold_id=0` query mixes wrapped and underlying
-- swaps and downstream volume aggregates double-count or mis-route.

ALTER TABLE curve_pool_swap
    ADD COLUMN IF NOT EXISTS is_underlying BOOLEAN NOT NULL DEFAULT false;

INSERT INTO migrations (filename)
VALUES ('20260521_150000_curve_pool_swap_is_underlying.sql')
ON CONFLICT (filename) DO NOTHING;
