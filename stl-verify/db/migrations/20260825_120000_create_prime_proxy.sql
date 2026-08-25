-- prime_proxy: which prime owns an allocation proxy wallet.
--
-- A static reference table. The rows below are the declared proxy universe,
-- transcribed from the axis-synome contract
-- (stl-verify/contracts/axis-synome/axis_synome_entities.json), which is where a
-- proxy is declared in the first place and is PR-gated upstream.
--
-- Onboarding a proxy means a NEW migration adding its row — this file is
-- immutable once applied (the migrator checksums it), so it cannot be edited.
-- Ship that migration in the same PR that updates the axis-synome contract; the
-- proxy is invisible to the API until it is deployed.
--
-- This is declared data, not observed data: a proxy appears here from the moment
-- it is onboarded, before any allocation_position row exists for it. The reads
-- that resolve a prime therefore answer from what is declared, and an endpoint
-- may legitimately return an empty series for a proxy no tracker has indexed yet.
-- That is the deliberate trade: /v1/primes and every prime-scoped endpoint agree
-- with the contract rather than with ingest.
--
-- Written once by this migration and never again, so every runtime write channel
-- is revoked below rather than merely left ungranted. No exception to the
-- append-only rule in AGENTS.md in this directory. The keys are what enforce one
-- prime per proxy — a hardcoded list cannot contradict itself, so there is
-- nothing to reconcile at write time.
--
-- prime_id is resolved by name rather than hardcoded: prime.id is a surrogate key
-- from a sequence, and pinning integers here would silently bind these rows to
-- whatever ids one environment happened to assign.
--
-- Three declared proxies are deliberately absent: grove's ALM proxies on plasma,
-- plume and monad. Those chains have no EVM chain id anywhere in this repo
-- (chain.chain_id, entity.ChainIDToName and app.domain.chain_names all carry the
-- same six), so the rows are not expressible, and no allocation tracker is
-- deployed for them — see SERVED_TRACKER_CHAINS in app/domain/chain_names.py. Add
-- them here when their chain ids do.

CREATE TABLE IF NOT EXISTS prime_proxy (
    chain_id      INT    NOT NULL REFERENCES chain (chain_id),
    proxy_address BYTEA  NOT NULL,
    prime_id      BIGINT NOT NULL REFERENCES prime (id),
    PRIMARY KEY (chain_id, proxy_address)
);

COMMENT ON TABLE prime_proxy IS '[Dimension] Which prime owns each allocation proxy wallet, one row per (chain, proxy address). Static reference data transcribed from the axis-synome contract, not derived from ingest: a proxy is listed from onboarding, which may be before any allocation_position row exists for it.';
COMMENT ON COLUMN prime_proxy.chain_id IS 'PK. FK→chain.chain_id. The chain the proxy is deployed on.';
COMMENT ON COLUMN prime_proxy.proxy_address IS 'PK. The prime''s ALM or SubProxy wallet address, raw 20 bytes (not hex-encoded).';
COMMENT ON COLUMN prime_proxy.prime_id IS 'FK→prime.id. The owning prime.';

-- UNIQUE, not a plain index. Callers resolve a proxy without naming a chain, so
-- `WHERE proxy_address = ... LIMIT 1` has to be provably one row: with only the
-- (chain_id, proxy_address) key, one address declared on two chains would let
-- that lookup return either prime, and the API would serve another prime's
-- capital or custody data. This is the same rule prime_registry._index_proxies
-- already enforces on the contract side, where a duplicate address raises at
-- import — the constraint makes the database agree rather than assume.
--
-- It costs the ability to declare one address on two chains (a CREATE2
-- deployment at the same address). Nothing does that today, and the contract
-- could not express it either, so it must be a deliberate decision here first.
ALTER TABLE prime_proxy ADD CONSTRAINT prime_proxy_address_key UNIQUE (proxy_address);

-- No index on prime_id: at eleven rows a sequential scan of the single page wins
-- and the planner picks it (confirmed on a staging clone), so an index would be
-- maintenance cost for nothing.

-- The roles migration's ALTER DEFAULT PRIVILEGES already grants stl_readwrite
-- SELECT, INSERT, UPDATE and DELETE on every table stl_migrator creates
-- (20260122_140100_create_app_roles_and_privileges.sql), so a narrow GRANT here
-- would add nothing and remove nothing — only the REVOKE closes the channel.
-- Same reasoning, and the same precedent, as
-- 20260818_130000_create_position_state.sql. Nothing writes this table at
-- runtime, so it keeps no write privilege at all.
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

-- Every listed prime name must exist, or a proxy silently drops out of the table
-- and every endpoint for it returns empty.
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
