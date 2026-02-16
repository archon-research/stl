-- Fix integer_now function to use schema-qualified table name.
--
-- TimescaleDB background workers (compression/columnstore jobs) may run with
-- a search_path that does not include 'public'. The unqualified reference to
-- sparklend_reserve_data in the integer_now function causes:
--   42P01: relation "sparklend_reserve_data" does not exist
--
-- Fix: schema-qualify the table reference and pin search_path on the function.

CREATE OR REPLACE FUNCTION sparklend_reserve_data_integer_now()
RETURNS BIGINT LANGUAGE SQL STABLE
SET search_path = public
AS
$$
    SELECT COALESCE(MAX(block_number), 0) FROM public.sparklend_reserve_data
$$;

INSERT INTO migrations (filename)
VALUES ('20260213_100000_fix_integer_now_search_path.sql')
ON CONFLICT (filename) DO NOTHING;
