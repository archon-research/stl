-- VEC-402..409: position_state — the shared spine every per-protocol position materializer writes into.
--
-- One row per (native position identity, observation). Position identity is NATIVE ONLY (VEC-400):
--   position_id = position_id(chain_id, protocol_id, instrument_key, holder_id)
-- No mapped / interpreted value enters the id. instrument_key and holder_id are the source's own
-- native ids (contract address / market id / registry:ilk / provider:package, and the on-chain holder
-- address or prime vault address), never one of our classifications. Classifications (deal_type,
-- direction) live in position_classification (VEC-401), keyed by position_id; they are attributes, not
-- part of identity.
--
-- quantity is a single canonical amount: the holder's balance in the instrument's own native units.
-- Per-protocol amount breakdowns (Morpho supply/borrow/collateral shares, Aave scaled balances, ...)
-- stay in the raw source; this spine carries the one number every consumer needs plus the resolution
-- keys. The per-protocol materializers (VEC-402..408) fan a raw row out by NATIVE INSTRUMENT only
-- (e.g. a Morpho market row -> its loan-token position and its collateral-token position), never by a
-- house leg/deal_type classifier.
--
-- Observation axis: (block_number, block_version, processing_version). processing_version defaults 0
-- for sources that don't carry one (Morpho). The current state per position is position_current
-- (VEC-409, DISTINCT ON position_id) — built once all materializers land.
--
-- Plain table, not a hypertable: it is a curated/derived spine populated out of band by the
-- materializer functions below (mirroring block_time and the transform _bootstrap pattern), not a
-- high-ingest raw pipeline. Add hypertable + tiering in a follow-up if volume warrants.

CREATE TABLE IF NOT EXISTS position_state (
    position_id        bytea       NOT NULL,
    chain_id           integer,                       -- native; nullable per position_id's structural-field convention
    protocol_id        bigint,                        -- native; nullable per convention
    instrument_key     text        NOT NULL,          -- native instrument id (VEC-412 bridge key); resolves security via security_instrument_bridge_current
    holder_id          text        NOT NULL,          -- native on-chain holder (wallet / prime vault address, lowercase hex, no 0x); resolves entity downstream (VEC-417)
    quantity           numeric     NOT NULL,          -- holder's balance in the instrument's native units
    block_number       bigint      NOT NULL,
    block_version      integer     NOT NULL DEFAULT 0,
    processing_version integer     NOT NULL DEFAULT 0,
    block_timestamp    timestamptz NOT NULL,          -- on-chain observation time
    created_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT position_state_pkey PRIMARY KEY (position_id, block_number, block_version, processing_version),
    -- position_id is sha256() output: enforce the 32-byte width (bytea is unlength-modified), matching
    -- position_classification (Simon review on #572).
    CONSTRAINT position_state_id_len_chk CHECK (octet_length(position_id) = 32)
);

COMMENT ON TABLE position_state IS '[Operational] Shared spine for materialized positions (VEC-402..408). One row per (native position_id, observation): the native resolution keys (instrument_key -> security via the bridge; holder_id -> entity via VEC-417) plus a single canonical quantity. Identity is native-only (VEC-400); classifications live in position_classification. Current state per position is position_current (VEC-409).';
COMMENT ON COLUMN position_state.position_id IS 'bytea(32) native identity from position_id() (VEC-400): hash(chain_id, protocol_id, instrument_key, holder_id). No mapped value in the hash.';
COMMENT ON COLUMN position_state.chain_id IS 'Native chain id. Nullable per the position_id structural-field convention; each materializer uses a fixed NULL-ness convention.';
COMMENT ON COLUMN position_state.protocol_id IS 'Native protocol id. Nullable per convention.';
COMMENT ON COLUMN position_state.instrument_key IS 'Native, globally-unique instrument id (the security_instrument_bridge key). Resolves to a security via security_instrument_bridge_current -> security_master_current (VEC-420).';
COMMENT ON COLUMN position_state.holder_id IS 'Native on-chain holder id (wallet or prime vault address, lowercase hex without 0x). Resolves to an entity downstream (VEC-417); not resolved here so the id stays computable without a master lookup.';
COMMENT ON COLUMN position_state.quantity IS 'Holder balance in the instrument''s native units. Per-protocol amount breakdowns stay in the raw source.';
COMMENT ON COLUMN position_state.block_number IS 'PK. Block height of the observation.';
COMMENT ON COLUMN position_state.block_version IS 'PK. Reorg version of the block.';
COMMENT ON COLUMN position_state.processing_version IS 'PK. Pipeline processing version; 0 for sources without one (e.g. Morpho).';
COMMENT ON COLUMN position_state.block_timestamp IS 'On-chain observation time (UTC).';
COMMENT ON COLUMN position_state.created_at IS 'Audit. Row insert time.';

-- Current-state lookup (VEC-409 ORDER BY): latest observation per position_id.
CREATE INDEX IF NOT EXISTS position_state_current_idx ON position_state (position_id, block_number DESC, block_version DESC, processing_version DESC);
-- Reverse lookups for resolution/aggregation.
CREATE INDEX IF NOT EXISTS position_state_instrument_idx ON position_state (instrument_key);
CREATE INDEX IF NOT EXISTS position_state_holder_idx ON position_state (holder_id);

GRANT SELECT ON position_state TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON position_state TO stl_readwrite;

-- Shared materializer body for every per-protocol projection (VEC-402..408). Each projection view
-- (position_morpho_market, position_morpho_vault, position_sky_prime_debt, ...) holds its own bespoke
-- projection logic but emits the identical position_state column contract: (position_id, chain_id,
-- protocol_id, instrument_key, holder_id, quantity, deal_type_code, block_number, block_version,
-- processing_version, block_timestamp). This function is the identical plumbing shared by all of them
-- (CLAUDE.md: consolidate duplicated code) — it upserts the observations into the spine and the current
-- (latest NON-ZERO) deal-type into position_classification (VEC-401). A closed position keeps the
-- deal_type of its last real observation, not the ambiguous direction of a closing zero-row.
--
-- p_view is a regclass, so it must name an existing relation — no SQL injection via the dynamic FROM.
-- Idempotent (ON CONFLICT); run out of band (a full-table INSERT..SELECT does not belong in the
-- migrator's single transaction). Returns the number of position_state rows written.
CREATE OR REPLACE FUNCTION materialize_position_projection(p_view regclass, p_reason text)
    RETURNS bigint
    LANGUAGE plpgsql AS $fn$
DECLARE n bigint;
BEGIN
    EXECUTE format($q$
        WITH ins AS (
            INSERT INTO position_state
                (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
                 block_number, block_version, processing_version, block_timestamp)
            SELECT position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
                   block_number, block_version, processing_version, block_timestamp
            FROM %s
            ON CONFLICT (position_id, block_number, block_version, processing_version) DO UPDATE
                SET quantity        = EXCLUDED.quantity,
                    block_timestamp = EXCLUDED.block_timestamp,
                    chain_id        = EXCLUDED.chain_id,
                    protocol_id     = EXCLUDED.protocol_id,
                    instrument_key  = EXCLUDED.instrument_key,
                    holder_id       = EXCLUDED.holder_id
            RETURNING 1
        )
        SELECT count(*) FROM ins
    $q$, p_view) INTO n;

    EXECUTE format($q$
        INSERT INTO position_classification (position_id, deal_type_code, direction, change_reason)
        SELECT DISTINCT ON (r.position_id)
               r.position_id, r.deal_type_code, d.direction, %L
        FROM %s r
        JOIN ref_deal_type d ON d.deal_type = r.deal_type_code
        WHERE r.quantity > 0
        ORDER BY r.position_id, r.block_number DESC, r.block_version DESC, r.processing_version DESC
        ON CONFLICT (position_id) DO UPDATE
            SET deal_type_code = EXCLUDED.deal_type_code,
                direction      = EXCLUDED.direction,
                change_reason  = EXCLUDED.change_reason
    $q$, p_reason, p_view);

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, text) IS '[Operational] VEC-402..408 shared materializer: upsert a per-protocol projection view (which must emit the position_state column contract) into position_state, and its current latest-non-zero deal-type into position_classification. Idempotent; run out of band. Returns position_state rows written.';

INSERT INTO migrations (filename) VALUES ('20260724_120000_create_position_state.sql') ON CONFLICT (filename) DO NOTHING;
