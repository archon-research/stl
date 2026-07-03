-- Change the uniswap_v3 event_name enum columns from VARCHAR(N) to TEXT.
-- OSM object-storage tiering cannot mirror a VARCHAR CHECK onto a tiered chunk
-- (it errors/stalls once a chunk ages past the tiering boundary); a TEXT CHECK
-- attaches cleanly. curve_liquidity_event.kind already uses TEXT for exactly
-- this reason. The existing CHECK (event_name IN (...)) and NOT NULL are
-- preserved by the type change. New migration because migrations are immutable
-- once applied; the tables were created VARCHAR in
-- 20260701_100000_create_uniswap_v3_tables.sql.

ALTER TABLE uniswap_v3_liquidity_event ALTER COLUMN event_name TYPE TEXT;
ALTER TABLE uniswap_v3_pool_event ALTER COLUMN event_name TYPE TEXT;
