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
    CONSTRAINT position_state_id_len_chk CHECK (octet_length(position_id) = 32),
    -- quantity is a non-negative native magnitude; direction (BORROW/short vs supply/long) lives in
    -- position_classification, never as a sign here. Every materializer emits abs()/balance/debt >= 0
    -- (verified: 0 negative rows across morpho/vault/sky on prod). Fail hard if a bad source amount
    -- would write a negative "exposure" that exposure queries (quantity <> 0) would silently surface.
    -- Exclude 'NaN' explicitly: in Postgres NaN sorts above every numeric, so it clears quantity >= 0
    -- and the quantity > 0 classification filter, then poisons every downstream SUM(quantity).
    CONSTRAINT position_state_qty_nonneg_chk CHECK (quantity >= 0 AND quantity <> 'NaN'::numeric)
);

COMMENT ON TABLE position_state IS '[Operational] Shared spine for materialized positions (VEC-402..408). One row per (native position_id, observation): the native resolution keys (instrument_key -> security via the bridge; holder_id -> entity via VEC-417) plus a single canonical quantity. Identity is native-only (VEC-400); classifications live in position_classification. Current state per position is position_current (VEC-409).';
COMMENT ON COLUMN position_state.position_id IS 'PK. bytea(32) native identity from position_id() (VEC-400): hash(chain_id, protocol_id, instrument_key, holder_id). No mapped value in the hash.';
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
-- Append-only: corrections arrive as new (block_version, processing_version) rows, so no DELETE.
-- (stl_readwrite still holds DML via the roles migration's ALTER DEFAULT PRIVILEGES; a dedicated
-- narrow materializer role is the real least-privilege fix, tracked as a VEC-402..408 follow-up.)
GRANT SELECT, INSERT, UPDATE ON position_state TO stl_readwrite;

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
-- migrator's single transaction). Returns the number of position_state rows actually inserted or
-- changed — the guarded upsert skips no-op reruns, so the count measures real work, not view size.
--
-- Precondition: the projection emits at most one row per (position_id, block_number, block_version,
-- processing_version). A view that double-emits a PK (e.g. two legs collapsing onto one instrument_key
-- at one observation) aborts the run with SQLSTATE 21000 ('cannot affect row a second time') — that is
-- a projection bug to fix at source (cf. PR #624's coll_addr <> loan_addr guard), not to paper over
-- with a silent dedup that would hide conflicting quantities.
CREATE OR REPLACE FUNCTION materialize_position_projection(p_view regclass, p_reason text)
    RETURNS bigint
    LANGUAGE plpgsql AS $fn$
DECLARE n bigint;
BEGIN
    -- Serialize concurrent runs of the SAME projection (cron overlap, scheduled + manual backfill):
    -- both drive the same large upsert and would otherwise take row locks in plan-dependent order and
    -- deadlock. Keyed on the view's oid so different projections still run concurrently; held to the
    -- end of the (out-of-band) transaction.
    PERFORM pg_advisory_xact_lock(p_view::oid::bigint);

    -- One evaluation feeds both writes. The projection (full raw scan + joins + LAG + per-row sha256)
    -- is the dominant cost; src AS MATERIALIZED guarantees it is scanned once, and running both INSERTs
    -- as data-modifying CTEs of a single statement gives them one snapshot (under READ COMMITTED two
    -- separate statements would each take their own, and could disagree). The classification CTE is a
    -- data-modifying WITH: Postgres runs it to completion even though the top-level query reads only ins.
    --
    -- position_id is recomputed here from the native identity fields via the canonical IMMUTABLE
    -- position_id(), not trusted from the projection's own column, so a view that mis-hashes (NULL-ness
    -- or address-casing divergence) can never mint an id that stores but joins to nothing recomputed.
    EXECUTE format($q$
        WITH src AS MATERIALIZED (
            SELECT position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
                   chain_id, protocol_id, instrument_key, holder_id, quantity, deal_type_code,
                   block_number, block_version, processing_version, block_timestamp
            FROM %1$s
        ),
        ins AS (
            INSERT INTO position_state
                (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
                 block_number, block_version, processing_version, block_timestamp)
            SELECT position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
                   block_number, block_version, processing_version, block_timestamp
            FROM src
            ON CONFLICT (position_id, block_number, block_version, processing_version) DO UPDATE
                SET quantity        = EXCLUDED.quantity,
                    block_timestamp = EXCLUDED.block_timestamp
                -- Skip unchanged rows: no dead tuples / WAL / 3-index churn on idempotent reruns, and
                -- the returned count then reflects real change. Identity columns are intentionally not
                -- reassigned — they are fixed by position_id, so EXCLUDED already equals the stored row.
                WHERE position_state.quantity        IS DISTINCT FROM EXCLUDED.quantity
                   OR position_state.block_timestamp IS DISTINCT FROM EXCLUDED.block_timestamp
            RETURNING 1
        ),
        -- Canonical version at each observed block: a reorg/reprocess correction (higher block_version
        -- or processing_version) supersedes the row it replaces. Collapse to the winning version per
        -- block BEFORE the non-zero filter, so a superseded positive that was zeroed out at the same
        -- block cannot resurrect a stale classification.
        canonical AS (
            SELECT DISTINCT ON (position_id, block_number)
                   position_id, block_number, quantity, deal_type_code
            FROM src
            ORDER BY position_id, block_number, block_version DESC, processing_version DESC
        ),
        -- Current deal-type = the most recent NON-ZERO canonical observation. A closed position keeps
        -- the deal_type of its last real observation, not the direction of a closing zero.
        latest AS (
            SELECT DISTINCT ON (position_id)
                   position_id, deal_type_code
            FROM canonical
            WHERE quantity > 0
            ORDER BY position_id, block_number DESC
        ),
        cls AS (
            INSERT INTO position_classification (position_id, deal_type_code, direction, change_reason)
            -- LEFT JOIN + raw deal_type_code: an unseeded / typo'd code must hit the deal_type_code FK
            -- (23503) and fail the run, not be silently dropped by an inner join (CLAUDE.md: fail hard).
            SELECT l.position_id, l.deal_type_code, d.direction, %2$L
            FROM latest l
            LEFT JOIN ref_deal_type d ON d.deal_type = l.deal_type_code
            ON CONFLICT (position_id) DO UPDATE
                SET deal_type_code = EXCLUDED.deal_type_code,
                    direction      = EXCLUDED.direction,
                    change_reason  = EXCLUDED.change_reason,
                    -- valid_from records when THIS classification became effective, so re-stamp it (and
                    -- change_reason) only when the classification actually changes — guarded below.
                    valid_from     = (now() AT TIME ZONE 'utc')::date
                WHERE position_classification.deal_type_code IS DISTINCT FROM EXCLUDED.deal_type_code
                   OR position_classification.direction      IS DISTINCT FROM EXCLUDED.direction
        )
        SELECT count(*) FROM ins
    $q$, p_view, p_reason) INTO n;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, text) IS '[Operational] VEC-402..408 shared materializer: in one snapshot, upsert a per-protocol projection view (which must emit the position_state column contract, at most one row per PK) into position_state, and its current latest-non-zero deal-type into position_classification. position_id is recomputed via position_id(); runs are serialized per view by an advisory lock; guarded upserts skip no-op reruns. Idempotent; run out of band. Returns position_state rows inserted or changed.';

INSERT INTO migrations (filename) VALUES ('20260724_120000_create_position_state.sql') ON CONFLICT (filename) DO NOTHING;
