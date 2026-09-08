-- VEC-535: allocation_position_key_retirement — the register of (chain, token) keys
-- the allocation tracker no longer writes, plus the gate that keeps
-- allocation_position_current from carrying them.
--
-- WHY A REGISTER AND NOT A DELETE. The tracker used to key an ERC-7540 centrifuge
-- position on the vault address. A vault is not a token — it has no price — so a
-- ~$260M JAAA holding valued at zero. The Go change in this PR moves those positions
-- onto the share token the wallet actually holds, which leaves the vault-keyed cache
-- rows behind. Three things make a plain DELETE the wrong repair:
--
--   1. DEPLOY ORDER. The migrate Job is an ArgoCD PreSync hook, so this file applies
--      BEFORE the new tracker image rolls. Old pods keep appending vault-keyed rows to
--      allocation_position for the minutes the rollout takes, and
--      trigger_upsert_allocation_position_current would put every one of them straight
--      back into the cache.
--   2. RECOVERY RE-RUN. 20260825_120100 is the documented operator statement for
--      converging the cache, and it is a forward-only merge over allocation_position.
--      That history is append-only and keeps the vault-keyed rows forever, so the next
--      re-run resurrects exactly the keys a DELETE removed. 20260908_120100 supersedes
--      it for that reason; 20260825_120100 must no longer be re-run.
--   3. DOUBLE COUNT. While both survive, the vault row and the share row are current
--      for the SAME holding, so every consumer summing the cache counts the position
--      twice — once at its real price and once at whatever the vault row carries.
--
-- The retirement is therefore DATA, read by the trigger below and by 20260908_120100.
-- Purging the cache rows it retires is that file's job, not this one's: it is the
-- re-runnable statement, so retiring a NEW key later converges the cache by re-running
-- it rather than by writing another one-shot migration.
--
-- Between this file (PreSync) and the new image's first sweep the four keys are simply
-- ABSENT from the cache — the rollout plus up to 75 blocks. Absent, not $0: a consumer
-- summing the cache under-reports for that window rather than reporting a wrong number.
--
-- The four retired keys are grove's ERC-7540 vaults: JAAA on mainnet and on Avalanche,
-- and JTRSY, which shares one address across both chains. Spark's mainnet JTRSY entry
-- is a direct share token (its share() reverts), already keyed on the token the wallet
-- holds, and is deliberately NOT retired.
--
-- APPEND-ON-CHANGE, like oracle_asset enable/disable (20260901_120000): putting a key
-- back in service is a new row with retired = false and a later valid_from, never an
-- UPDATE. UPDATE, DELETE and TRUNCATE are revoked from stl_readwrite — the half
-- TestConvertedTablesAreAppendOnly asserts — and from the owner, which no test run
-- exercises because stl_migrator does not exist under the harness (as 20260818_130000
-- records of its own owner-side revoke).
--
-- stl_readwrite keeps SELECT and INSERT rather than SELECT alone, matching oracle_asset,
-- the sibling governance register: nothing in the application appends today, but the
-- append-only converted set is defined by "keeps INSERT, holds no UPDATE/DELETE", and
-- splitting this one table out of it would buy less than the consistency costs.

-- Fail fast rather than convoy ingestion: CREATE OR REPLACE FUNCTION below waits behind
-- every in-flight execution of the trigger function. Same value as the sibling
-- migrations; re-run in a quieter window. Outside a transaction block SET LOCAL only
-- warns, so this file must never be marked `-- migrate: no-transaction`.
SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS allocation_position_key_retirement (
    chain_id           INT         NOT NULL REFERENCES chain (chain_id),
    token_id           BIGINT      NOT NULL REFERENCES token (id),
    retired            BOOLEAN     NOT NULL,
    valid_from         TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version INT         NOT NULL DEFAULT 0,
    reason             TEXT        NOT NULL,
    ticket             TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, token_id, valid_from, processing_version),
    CONSTRAINT allocation_position_key_retirement_pv_chk CHECK (processing_version >= 0),
    -- An empty string is not a reason, and an empty ticket traces to nothing.
    CONSTRAINT allocation_position_key_retirement_reason_chk CHECK (btrim(reason) <> ''),
    CONSTRAINT allocation_position_key_retirement_ticket_chk CHECK (btrim(ticket) <> '')
);

COMMENT ON TABLE allocation_position_key_retirement IS '[Configuration] Register of the (chain_id, token_id) keys the allocation tracker no longer writes positions on, and which allocation_position_current must therefore not cache. Append-on-change: putting a key back in service is a new row with retired = false and a later valid_from, never an UPDATE — UPDATE/DELETE/TRUNCATE are revoked, the owner included. Read it through allocation_position_key_retirement_current, never raw: the raw table matches a superseded version as readily as the live one. A plain table, not a hypertable: a row lands only when a tracker keying decision changes, on the order of rows per month, so chunking, compression and tiering buy nothing (db/migrations/AGENTS.md sparse-table exception). build_id is absent because no build writes this table — it is seeded by migrations and by an operator, never by an indexer.';
COMMENT ON COLUMN allocation_position_key_retirement.chain_id IS 'PK. FK→chain.chain_id. Chain of the retired key.';
COMMENT ON COLUMN allocation_position_key_retirement.token_id IS 'PK. FK→token.id. The token row the tracker no longer writes positions on.';
COMMENT ON COLUMN allocation_position_key_retirement.retired IS 'true retires the key, false puts it back in service. The latest version per (chain_id, token_id) is the one in force; resolve it through allocation_position_key_retirement_current rather than reading this column raw.';
COMMENT ON COLUMN allocation_position_key_retirement.valid_from IS 'PK. Instant this version became effective (timestamptz, so it cannot shift with a session TimeZone). There is no valid_to: a version runs until the next valid_from for the same key. A future-dated row is stored but not yet in force — allocation_position_key_retirement_current bounds on now().';
COMMENT ON COLUMN allocation_position_key_retirement.processing_version IS 'PK. Correction version of this (chain_id, token_id, valid_from); monotonic from 0. Load-bearing because valid_from DEFAULTs to now(), which is transaction time: two changes to one key in one transaction share it, and only this column separates them. An appending writer reads the current maximum and adds one, so concurrent appenders on one key must serialize (pg_advisory_xact_lock, ADR-0002).';
COMMENT ON COLUMN allocation_position_key_retirement.reason IS 'Audit. Mandatory: why this version exists. erc7540_vault = the address is an ERC-7540 vault rather than a token, and the position now keys on the share the wallet holds.';
COMMENT ON COLUMN allocation_position_key_retirement.ticket IS 'Audit. Mandatory: the ticket that decided this version, so a retirement can be traced to the change that made it true.';
COMMENT ON COLUMN allocation_position_key_retirement.created_at IS 'Audit. When the row was written. Not the instant the version took effect (valid_from), which a backdated correction can set earlier.';

GRANT SELECT ON allocation_position_key_retirement TO stl_readonly;
-- Append-only from birth: a correction is a new version, so nothing needs UPDATE or
-- DELETE. 20260122_140100's ALTER DEFAULT PRIVILEGES hands stl_readwrite full DML on
-- every migrator-owned table at creation, so the REVOKE below — not the narrow GRANT
-- here — is what takes them back.
GRANT SELECT, INSERT ON allocation_position_key_retirement TO stl_readwrite;
-- Guarded by role existence, mirroring position_state (20260818_130000). The guard is
-- load-bearing for stl_migrator only: the infra bootstrap creates that role and no
-- migration does, so it is absent under the test harness, which migrates as its own
-- bootstrap superuser. Revoking the OWNER's UPDATE/DELETE is safe because nothing FKs
-- this table, so no RI probe needs FOR KEY SHARE on it (the trap 20260714_160000 fixed);
-- a deliberate history fix costs a visible re-GRANT in a migration.
DO $$
DECLARE role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['stl_readwrite', 'stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON allocation_position_key_retirement FROM %I', role_name);
        END IF;
    END LOOP;
END $$;

-- The version in force per key. DISTINCT ON is what makes the view non-updatable, so
-- the REVOKE below is belt to that braces rather than the only barrier.
CREATE OR REPLACE VIEW allocation_position_key_retirement_current AS
SELECT DISTINCT ON (chain_id, token_id) *
FROM allocation_position_key_retirement
WHERE valid_from <= now()
ORDER BY chain_id, token_id, valid_from DESC, processing_version DESC;

COMMENT ON VIEW allocation_position_key_retirement_current IS '[Configuration] The retirement version in force per (chain_id, token_id): the newest valid_from. The read every writer of allocation_position_current consults — the trigger below and 20260908_120100.';

GRANT SELECT ON allocation_position_key_retirement_current TO stl_readonly;
GRANT SELECT ON allocation_position_key_retirement_current TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON allocation_position_key_retirement_current FROM stl_readwrite;

-- Replaced, not re-triggered: CREATE TRIGGER takes SHARE ROW EXCLUSIVE on the
-- allocation_position hypertable and on every one of its chunks, which conflicts with
-- the ROW EXCLUSIVE each ingest INSERT holds; replacing the function takes neither.
--
-- The body is 20260825_120000's verbatim, plus the retirement gate at the top. Copied
-- rather than extended because CREATE OR REPLACE FUNCTION redeclares the whole thing:
-- SECURITY DEFINER and the pinned search_path do not carry over, and losing either
-- would break every ingest INSERT (no login role can write the cache) or leave a
-- caller free to bind these unqualified names to objects of its own.
CREATE OR REPLACE FUNCTION upsert_allocation_position_current()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    -- A retired key is not a position any more, only history. Caching it would double
    -- count against the row that replaced it, and the old tracker image keeps appending
    -- to it for the length of the rollout (see this file's header). The history row is
    -- untouched either way: this is an AFTER trigger, so only the cache write is skipped.
    IF EXISTS (
        SELECT 1
        FROM allocation_position_key_retirement_current r
        WHERE r.chain_id = NEW.chain_id AND r.token_id = NEW.token_id AND r.retired
    ) THEN
        RETURN NULL;
    END IF;

    INSERT INTO allocation_position_current AS cur
        (proxy_address, chain_id, token_id, balance, underlying_value, underlying_token_id,
         tx_amount, direction, tx_hash, block_timestamp,
         block_number, block_version, log_index, processing_version)
    VALUES
        (NEW.proxy_address, NEW.chain_id, NEW.token_id, NEW.balance, NEW.underlying_value,
         NEW.underlying_token_id, NEW.tx_amount, NEW.direction, NEW.tx_hash, NEW.created_at,
         NEW.block_number, NEW.block_version, NEW.log_index, NEW.processing_version)
    ON CONFLICT (proxy_address, chain_id, token_id) DO UPDATE SET
        balance = EXCLUDED.balance,
        underlying_value = EXCLUDED.underlying_value,
        underlying_token_id = EXCLUDED.underlying_token_id,
        tx_amount = EXCLUDED.tx_amount,
        direction = EXCLUDED.direction,
        tx_hash = EXCLUDED.tx_hash,
        block_timestamp = EXCLUDED.block_timestamp,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        log_index = EXCLUDED.log_index,
        processing_version = EXCLUDED.processing_version,
        created_at = now()
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.block_timestamp,
           EXCLUDED.log_index, EXCLUDED.direction, EXCLUDED.tx_hash, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.block_timestamp,
           cur.log_index, cur.direction, cur.tx_hash, cur.processing_version);
    RETURN NULL;
END;
$$;

-- Seeded by NATURAL KEY, never by id: token.id is a per-environment surrogate, so a
-- hardcoded id would retire an unrelated token on staging or prod. On a fresh database
-- no token row matches and this inserts nothing, which is correct — a database that
-- never saw the vault-keyed writes has nothing to retire.
INSERT INTO allocation_position_key_retirement (chain_id, token_id, retired, reason, ticket)
SELECT t.chain_id, t.id, true, 'erc7540_vault', 'VEC-535'
FROM token t
WHERE (t.chain_id, t.address) IN (
    (1, '\x4880799ee5200fc58da299e965df644fbf46780b'::bytea),
    (43114, '\x1121f4e21ed8b9bc1bb9a2952cdd8639ac897784'::bytea),
    (1, '\xfe6920eb6c421f1179ca8c8d4170530cdbdfd77a'::bytea),
    (43114, '\xfe6920eb6c421f1179ca8c8d4170530cdbdfd77a'::bytea)
);

-- An ABSOLUTE count, not a comparison against the same literal list: deriving both
-- sides from one list in one transaction proves only that the list equals itself, so a
-- typo'd address would pass. Four keys exist, so any database the tracker ever wrote
-- them on holds four token rows; 0 is the fresh database that never did. Anything
-- between means an address or a chain_id here disagrees with what token holds.
DO $$
DECLARE seeded int;
BEGIN
    SELECT count(*) INTO seeded
    FROM allocation_position_key_retirement WHERE ticket = 'VEC-535';
    IF seeded NOT IN (0, 4) THEN
        RAISE EXCEPTION 'VEC-535 expected to retire 4 keys (or 0 on a fresh database), retired %', seeded;
    END IF;
    RAISE NOTICE 'VEC-535 retired % ERC-7540 vault keys', seeded;
END $$;

COMMENT ON TABLE allocation_position_current IS '[Operational] Newest allocation_position row per (ALM proxy, chain, token). Derived cache of the allocation_position history; converged at any time by re-running 20260908_120100, which purges the keys allocation_position_key_retirement_current marks retired and then merges the rest — both directions, so retiring a new key needs no migration. 20260825_120100 is superseded and must NOT be re-run: it merges every key the history holds, so it puts the retired ones straight back. Never read this table as a history — it holds no "as of block N" answer. Keyed without prime_id, which is a function of proxy_address, so a proxy that changed prime holds one row: its newest.';

INSERT INTO migrations (filename)
VALUES ('20260908_120000_create_allocation_position_key_retirement.sql')
ON CONFLICT (filename) DO NOTHING;
