-- Declared prime→proxy mapping. Transcribed from the axis-synome contract.
-- Onboard a new proxy by adding a new migration (this file is checksummed).
-- Grove's plasma/plume/monad ALM proxies omitted: no chain_id in this repo yet.

CREATE TABLE IF NOT EXISTS prime_proxy (
    chain_id      INT    NOT NULL REFERENCES chain (chain_id),
    proxy_address BYTEA  NOT NULL,
    prime_id      BIGINT NOT NULL REFERENCES prime (id),
    PRIMARY KEY (chain_id, proxy_address)
);

COMMENT ON TABLE prime_proxy IS '[Dimension] Which prime owns each allocation proxy wallet. Static reference data from the axis-synome contract.';
COMMENT ON COLUMN prime_proxy.chain_id IS 'PK. FK→chain.chain_id. The chain the proxy is deployed on.';
COMMENT ON COLUMN prime_proxy.proxy_address IS 'PK. The prime''s ALM or SubProxy wallet address, raw 20 bytes (not hex-encoded).';
COMMENT ON COLUMN prime_proxy.prime_id IS 'FK→prime.id. The owning prime.';

-- Callers resolve by proxy_address alone, so it must be globally unique.
ALTER TABLE prime_proxy ADD CONSTRAINT prime_proxy_address_key UNIQUE (proxy_address);

GRANT SELECT ON prime_proxy TO stl_readonly;
GRANT SELECT ON prime_proxy TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON prime_proxy FROM stl_readwrite;

INSERT INTO prime_proxy (chain_id, proxy_address, prime_id)
SELECT v.chain_id, decode(v.proxy_hex, 'hex'), p.id
FROM (VALUES
    -- spark
    (1,     '1601843c5e9bc251a3272907010afa41fa18347e', 'spark'),  -- mainnet ALM
    (1,     '3300f198988e4c9c63f75df86de36421f06af8c4', 'spark'),  -- mainnet SubProxy
    (10,    '876664f0c9ff24d1aa355ce9f1680ae1a5bf36fb', 'spark'),  -- optimism ALM
    (130,   '345e368fccd62266b3f5f37c9a131fd1c39f5869', 'spark'),  -- unichain ALM
    (8453,  '2917956eff0b5eaf030abdb4ef4296df775009ca', 'spark'),  -- base ALM
    (42161, '92afd6f2385a90e44da3a8b60fe36f6cbe1d8709', 'spark'),  -- arbitrum ALM
    (43114, 'ece6b0e8a54c2f44e066fbb9234e7157b15b7fec', 'spark'),  -- avalanche-c ALM
    -- grove
    (1,     '491edfb0b8b608044e227225c715981a30f3a44e', 'grove'),  -- mainnet ALM
    (1,     '1369f7b2b38c76b6478c0f0e66d94923421891ba', 'grove'),  -- mainnet SubProxy
    (8453,  '9b746dbc5269e1df6e4193bcb441c0fbbf1cecee', 'grove'),  -- base ALM
    (43114, '7107dd8f56642327945294a18a4280c78e153644', 'grove')   -- avalanche-c ALM
) AS v(chain_id, proxy_hex, prime_name)
JOIN prime p ON p.name = v.prime_name;

DO $$
    BEGIN
        IF (SELECT count(*) FROM prime_proxy) <> 11 THEN
            RAISE EXCEPTION
                'Migration aborted: prime_proxy holds % rows, expected 11. A prime name in the list above does not match prime.name.',
                (SELECT count(*) FROM prime_proxy);
        END IF;
    END $$;

INSERT INTO migrations (filename)
VALUES ('20260825_120000_create_prime_proxy.sql')
ON CONFLICT (filename) DO NOTHING;
