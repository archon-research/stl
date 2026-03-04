-- Set integer_now function for sparklend_reserve_data.
--
-- TimescaleDB requires an integer_now function for hypertables partitioned by
-- an integer column (block_number). Without it, background policies like
-- compression/columnstore fail with "integer_now function not set".

CREATE OR REPLACE FUNCTION sparklend_reserve_data_integer_now()
RETURNS BIGINT LANGUAGE SQL STABLE AS
$$
    SELECT COALESCE(MAX(block_number), 0) FROM sparklend_reserve_data
$$;

SELECT set_integer_now_func('sparklend_reserve_data', 'sparklend_reserve_data_integer_now');

INSERT INTO migrations (filename)
VALUES ('20260212_100000_set_integer_now_func.sql')
ON CONFLICT (filename) DO NOTHING;
