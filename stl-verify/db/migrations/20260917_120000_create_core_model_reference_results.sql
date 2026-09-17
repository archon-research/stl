-- VEC-828: per-cycle snapshots of the results the upstream CORE risk model
-- publishes on its dashboard, one table for markets and one for vaults.
-- Source: the dashboard's /core/overview/ route (CORE_MODEL_REFERENCE_URL).
--
-- Why these tables exist: the dashboard publishes one figure per calendar day
-- and answers `?date=` by ignoring it, so a past day's result can only be
-- observed while it is the current one. These rows are reference data -- what
-- the upstream model said -- never STL's own model output, which lives in
-- core_model_results and is keyed and scaled differently (market_key, percent).
--
-- Row identity is (network, protocol_name, market_uid, synced_at): the feed
-- keys a market by (network, protocol, market_uid) and two protocols share the
-- zero-ish uid on ethereum (anchorage and galaxy), so protocol is part of the
-- identity. market_uid is TEXT, not BYTEA: a Morpho Blue market id is 32 bytes,
-- a SparkLend spToken is 20, and the off-chain markets carry a synthetic label
-- ("0x…anchorageusdc"), so it is upstream's opaque handle recorded verbatim.
--
-- Identity fields are upstream's claims recorded verbatim, deliberately NOT
-- foreign keys into STL's registries: the feed covers networks and markets STL
-- does not index (Base, Robinhood Chain, 17 Morpho Blue markets), and a
-- reference row must stay traceable to what the feed said. Registry resolution
-- happens at read time.
--
-- Encoding: every *_usd amount is an already-normalized USD decimal, NOT a raw
-- native-decimal integer. crr_* and prob_no_bad_debt are plain 0-1 fractions,
-- upstream's vocabulary throughout; crr_floor is stored on its own and NEVER
-- pre-added to crr_el -- the dashboard's "display CRR" is crr_el + crr_floor and
-- is a read-time derivation. Consumers that need percent rescale at their boundary.
--
-- Plain tables, per db/migrations/AGENTS.md: ~32 market rows and ~10 vault rows
-- per 30-minute cycle is ~2k rows/day, far below where partitioning pays off. The
-- VectorCoreModelReferenceResultGrowthHigh tripwire watches the rate; the conversion
-- path (partition on synced_at, segment by network + protocol_name) is in
-- docs/runbooks/vector-cronjobs.md.
--
-- Append-only from birth (ADR-0002): processing_version + build_id + run_id,
-- PK = natural key + processing_version, a build-aware advisory-locked BEFORE
-- INSERT trigger, and UPDATE/DELETE/TRUNCATE revoked from the app role.

CREATE TABLE IF NOT EXISTS core_model_reference_market_result
(
    network                TEXT        NOT NULL,
    chain_id               INTEGER,
    protocol_name          TEXT        NOT NULL,
    market_uid             TEXT        NOT NULL,
    market_symbol          TEXT        NOT NULL,
    loan_token_symbol      TEXT        NOT NULL,
    loan_token_address     TEXT        NOT NULL,
    model_date             DATE        NOT NULL,
    synced_at              TIMESTAMPTZ NOT NULL,
    n_scenarios            INTEGER     NOT NULL CHECK (n_scenarios >= 0),
    horizon_days           INTEGER     NOT NULL CHECK (horizon_days >= 0),
    effective_horizon_days INTEGER     NOT NULL CHECK (effective_horizon_days >= 0),
    total_supply_usd       NUMERIC     NOT NULL,
    prob_no_bad_debt       NUMERIC     NOT NULL CHECK (prob_no_bad_debt >= 0 AND prob_no_bad_debt <= 1),
    crr_el                 NUMERIC     NOT NULL CHECK (crr_el >= 0),
    crr_var                NUMERIC     NOT NULL CHECK (crr_var >= 0),
    crr_es                 NUMERIC     NOT NULL CHECK (crr_es >= 0),
    crr_el_se              NUMERIC     NOT NULL CHECK (crr_el_se >= 0),
    crr_var_se             NUMERIC     NOT NULL CHECK (crr_var_se >= 0),
    crr_es_se              NUMERIC     NOT NULL CHECK (crr_es_se >= 0),
    crr_floor              NUMERIC     NOT NULL CHECK (crr_floor >= 0),
    external_flow_enabled  BOOLEAN     NOT NULL,
    source                 TEXT        NOT NULL,
    processing_version     INT         NOT NULL DEFAULT 0,
    build_id               INT         NOT NULL DEFAULT 0,
    run_id                 BIGINT,
    PRIMARY KEY (network, protocol_name, market_uid, synced_at, processing_version)
);

CREATE OR REPLACE FUNCTION assign_processing_version_core_model_reference_market_result()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('rcmr|%s|%s|%s|%s',
               NEW.network, NEW.protocol_name, NEW.market_uid, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM core_model_reference_market_result
    WHERE network       = NEW.network
      AND protocol_name = NEW.protocol_name
      AND market_uid    = NEW.market_uid
      AND synced_at     = NEW.synced_at
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM core_model_reference_market_result
        WHERE network       = NEW.network
          AND protocol_name = NEW.protocol_name
          AND market_uid    = NEW.market_uid
          AND synced_at     = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON core_model_reference_market_result
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_core_model_reference_market_result();

COMMENT ON TABLE core_model_reference_market_result IS
  '[Timeseries] Per-cycle snapshot of one market''s result from the upstream CORE risk model dashboard (/core/overview/). Reference data: what the upstream model reported, never STL''s own model output (that is core_model_results, keyed by market_key and scaled to percent). One row per (network, protocol_name, market_uid) per sync cycle; the feed publishes one result per calendar day (model_date) and ignores ?date=, so rows can only be accumulated forward. Identity fields are upstream''s claims verbatim, not registry FKs -- the feed covers networks and markets STL does not index. Deliberately a plain table (db/migrations/AGENTS.md create-plain rule): ~1.5k rows/day at the 30-minute cadence. Conversion path -- hypertable on synced_at, segment by (network, protocol_name), order by synced_at DESC, compress after 14 days -- is in docs/runbooks/vector-cronjobs.md under VectorCoreModelReferenceResultGrowthHigh, the alert that would prompt it. Append-only via the processing_version trigger; UPDATE/DELETE revoked from stl_readwrite.';
COMMENT ON COLUMN core_model_reference_market_result.network IS 'PK. Upstream''s network label verbatim (''ethereum'', ''base'', ''robinhood''), where STL says ''mainnet'' for the first.';
COMMENT ON COLUMN core_model_reference_market_result.chain_id IS 'EVM chain id mapped from network at write time. NULL for a network STL has no id for, which is a fact about the mapping, not missing data; the indexer counts such rows (unmapped_network_rows) and VectorCoreModelReferenceIndexerUnmappedNetwork asks for the map to be extended.';
COMMENT ON COLUMN core_model_reference_market_result.protocol_name IS 'PK. Upstream protocol label verbatim (''sparklend'', ''morpho'', ''maple'', ''anchorage'', ''galaxy''), not an FK into protocol. Part of the identity because anchorage and galaxy share a placeholder market_uid on ethereum.';
COMMENT ON COLUMN core_model_reference_market_result.market_uid IS 'PK. Upstream''s opaque market handle verbatim: the spToken address for SparkLend, the 32-byte Blue market id for Morpho, the pool address for Maple, a synthetic label (''0x…anchorageusdc'') for the off-chain markets. TEXT on purpose: three shapes, one column, no decoding. Stored as upstream spells it (lowercase hex today) and compared case-sensitively, so a spelling change upstream would open a second identity; the writer rejects two spellings within one fetch.';
COMMENT ON COLUMN core_model_reference_market_result.market_symbol IS 'Display symbol as upstream reports it (''spUSDS'', ''cbBTC/USDC 86%'').';
COMMENT ON COLUMN core_model_reference_market_result.loan_token_symbol IS 'Symbol of the market''s loan (supplied) token as upstream reports it.';
COMMENT ON COLUMN core_model_reference_market_result.loan_token_address IS '0x-prefixed address of the loan token as upstream reports it, on the row''s network. TEXT verbatim, not a token FK: the same token on Base or Robinhood Chain has no token row here.';
COMMENT ON COLUMN core_model_reference_market_result.model_date IS 'Calendar date (UTC) of the upstream model run this row reports. The feed publishes one result per market per day; a row whose model_date lags synced_at by more than a day is a stale upstream run, not STL''s doing.';
COMMENT ON COLUMN core_model_reference_market_result.synced_at IS 'PK. Cron-cycle timestamp (UTC), shared by every market and vault row of one cycle. Order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN core_model_reference_market_result.n_scenarios IS 'Monte Carlo scenarios behind the row''s figures (10000 on live markets; 0 on a market the model does not simulate, whose CRRs are then 0 by construction).';
COMMENT ON COLUMN core_model_reference_market_result.horizon_days IS 'Liquidation horizon of the run, in calendar days (upstream reports 15 where its path engine steps 14; recorded as reported).';
COMMENT ON COLUMN core_model_reference_market_result.effective_horizon_days IS 'Days the upstream kernel actually needed to clear the liquidation flow under its participation cap; 1 means everything cleared on day one.';
COMMENT ON COLUMN core_model_reference_market_result.total_supply_usd IS 'Normalized USD decimal. Upstream tot_supply_usd: the market''s total supply the CRRs are a fraction of.';
COMMENT ON COLUMN core_model_reference_market_result.prob_no_bad_debt IS 'Plain 0-1 fraction. Share of scenarios with zero bad debt; 1 - this is the loss frequency.';
COMMENT ON COLUMN core_model_reference_market_result.crr_el IS 'Plain 0-1 fraction of total_supply_usd (NOT percent, NOT floored). Expected-loss capital-risk ratio. The dashboard displays crr_el + crr_floor; derive that at read time, never store it here.';
COMMENT ON COLUMN core_model_reference_market_result.crr_var IS 'Plain 0-1 fraction of total_supply_usd. Value-at-risk CRR at upstream''s confidence level (0.975 today; not carried by the feed).';
COMMENT ON COLUMN core_model_reference_market_result.crr_es IS 'Plain 0-1 fraction of total_supply_usd. Expected-shortfall CRR at upstream''s confidence level.';
COMMENT ON COLUMN core_model_reference_market_result.crr_el_se IS 'Plain 0-1 fraction. Monte Carlo standard error of crr_el, same scale as crr_el.';
COMMENT ON COLUMN core_model_reference_market_result.crr_var_se IS 'Plain 0-1 fraction. Monte Carlo standard error of crr_var.';
COMMENT ON COLUMN core_model_reference_market_result.crr_es_se IS 'Plain 0-1 fraction. Monte Carlo standard error of crr_es.';
COMMENT ON COLUMN core_model_reference_market_result.crr_floor IS 'Plain 0-1 fraction. Additive floor upstream adds to crr_el for its displayed CRR (0.02 on collateralised markets, 0 where there is no collateral). Stored separately so the model output and the display policy stay distinguishable; whether the floor is governance or model is an open question on VEC-777.';
COMMENT ON COLUMN core_model_reference_market_result.external_flow_enabled IS 'Whether upstream''s cross-protocol liquidation wall (external flow) was switched on for this run.';
COMMENT ON COLUMN core_model_reference_market_result.source IS 'Provenance slug of the upstream route that produced the row, so a figure can be traced to the feed that reported it.';
COMMENT ON COLUMN core_model_reference_market_result.processing_version IS 'PK, Audit. Correction version: 0=original, N=Nth reprocess under a later build (ADR-0002). Order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN core_model_reference_market_result.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';
COMMENT ON COLUMN core_model_reference_market_result.run_id IS 'Audit. writer_run.id of the process start that wrote this row (ADR-0006 §2): resolves to the artefact (build_registry) and the reference snapshot/effective instant the writer ran with. Not an FK, like build_id; never used for ordering or to pick the latest row.';

REVOKE UPDATE, DELETE, TRUNCATE ON core_model_reference_market_result FROM stl_readwrite;

CREATE TABLE IF NOT EXISTS core_model_reference_vault_result
(
    network            TEXT        NOT NULL,
    chain_id           INTEGER,
    protocol_name      TEXT        NOT NULL,
    vault_address      TEXT        NOT NULL,
    vault_symbol       TEXT        NOT NULL,
    vault_name         TEXT        NOT NULL,
    version_label      TEXT        NOT NULL,
    loan_token_symbol  TEXT        NOT NULL,
    loan_token_address TEXT        NOT NULL,
    method             TEXT        NOT NULL,
    model_date         DATE        NOT NULL,
    synced_at          TIMESTAMPTZ NOT NULL,
    n_markets          INTEGER     NOT NULL CHECK (n_markets >= 0),
    total_assets_usd   NUMERIC     NOT NULL,
    idle_assets_usd    NUMERIC     NOT NULL,
    crr_el             NUMERIC     NOT NULL CHECK (crr_el >= 0),
    crr_el_se          NUMERIC     CHECK (crr_el_se >= 0),
    crr_es             NUMERIC     CHECK (crr_es >= 0),
    source             TEXT        NOT NULL,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    run_id             BIGINT,
    -- An override vault carries a flat governance value with no Monte Carlo
    -- behind it, so it has no standard error and no expected shortfall (verified
    -- live: groveUSDG); a modelled vault always has both. Enforced in both
    -- directions, so "crr_el_se IS NULL" and "method = 'override'" stay the same
    -- statement and a reader may trust either one.
    CHECK (CASE WHEN method = 'override'
                THEN crr_el_se IS NULL AND crr_es IS NULL
                ELSE crr_el_se IS NOT NULL AND crr_es IS NOT NULL END),
    PRIMARY KEY (network, protocol_name, vault_address, synced_at, processing_version)
);

CREATE OR REPLACE FUNCTION assign_processing_version_core_model_reference_vault_result()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('rcvr|%s|%s|%s|%s',
               NEW.network, NEW.protocol_name, NEW.vault_address, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM core_model_reference_vault_result
    WHERE network       = NEW.network
      AND protocol_name = NEW.protocol_name
      AND vault_address = NEW.vault_address
      AND synced_at     = NEW.synced_at
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM core_model_reference_vault_result
        WHERE network       = NEW.network
          AND protocol_name = NEW.protocol_name
          AND vault_address = NEW.vault_address
          AND synced_at     = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON core_model_reference_vault_result
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_core_model_reference_vault_result();

COMMENT ON TABLE core_model_reference_vault_result IS
  '[Timeseries] Per-cycle snapshot of one vault''s result from the upstream CORE risk model dashboard (/core/overview/), the vault-level aggregate of core_model_reference_market_result written by the same cycle with the same synced_at. Reference data, not STL''s model output. One row per (network, protocol_name, vault_address) per sync cycle; forward-only for the same reason as the market table. Identity fields are upstream''s claims verbatim, not registry FKs. Deliberately a plain table (~500 rows/day); shares the market table''s tripwire and conversion path.';
COMMENT ON COLUMN core_model_reference_vault_result.network IS 'PK. Upstream''s network label verbatim.';
COMMENT ON COLUMN core_model_reference_vault_result.chain_id IS 'EVM chain id mapped from network at write time. NULL for a network STL has no id for.';
COMMENT ON COLUMN core_model_reference_vault_result.protocol_name IS 'PK. Upstream protocol label verbatim (''morpho'' today), not an FK into protocol.';
COMMENT ON COLUMN core_model_reference_vault_result.vault_address IS 'PK. 0x-prefixed vault address as upstream reports it. TEXT verbatim, not the canonical BYTEA: a reference row stays traceable to the feed''s spelling and the vault may sit on a network STL does not index. Compared case-sensitively (lowercase hex today); a spelling change upstream would open a second identity, and the writer rejects two spellings within one fetch.';
COMMENT ON COLUMN core_model_reference_vault_result.vault_symbol IS 'Vault share symbol as upstream reports it (''steakUSDC'').';
COMMENT ON COLUMN core_model_reference_vault_result.vault_name IS 'Vault display name as upstream reports it.';
COMMENT ON COLUMN core_model_reference_vault_result.version_label IS 'Upstream''s vault version label verbatim (''v1'', ''v2''): a Morpho vault generation, not an STL schema version.';
COMMENT ON COLUMN core_model_reference_vault_result.loan_token_symbol IS 'Symbol of the vault''s asset token as upstream reports it.';
COMMENT ON COLUMN core_model_reference_vault_result.loan_token_address IS '0x-prefixed address of the vault''s asset token as upstream reports it, on the row''s network. TEXT verbatim, not a token FK.';
COMMENT ON COLUMN core_model_reference_vault_result.method IS 'How upstream produced the vault figure: ''model'' = allocation-weighted sum over its markets'' results, ''override'' = a flat governance-set value (groveUSDG today).';
COMMENT ON COLUMN core_model_reference_vault_result.model_date IS 'Calendar date (UTC) of the upstream model run this row reports.';
COMMENT ON COLUMN core_model_reference_vault_result.synced_at IS 'PK. Cron-cycle timestamp (UTC), equal to the cycle''s core_model_reference_market_result.synced_at so the two join exactly.';
COMMENT ON COLUMN core_model_reference_vault_result.n_markets IS 'Number of markets the vault allocates to that entered the aggregate; 0 under method=override.';
COMMENT ON COLUMN core_model_reference_vault_result.total_assets_usd IS 'Normalized USD decimal. Upstream total_assets_usd: the vault''s total assets the CRRs are a fraction of.';
COMMENT ON COLUMN core_model_reference_vault_result.idle_assets_usd IS 'Normalized USD decimal. Upstream idle_usd: assets not allocated to any market; dilutes the supply-weighted CRR.';
COMMENT ON COLUMN core_model_reference_vault_result.crr_el IS 'Plain 0-1 fraction of total_assets_usd (NOT percent). Expected-loss CRR; the vault rows carry no crr_floor.';
COMMENT ON COLUMN core_model_reference_vault_result.crr_el_se IS 'Plain 0-1 fraction. Monte Carlo standard error of crr_el. NULL if and only if method=override, which has no simulation behind it; the CHECK enforces both directions.';
COMMENT ON COLUMN core_model_reference_vault_result.crr_es IS 'Plain 0-1 fraction of total_assets_usd. Expected-shortfall CRR. NULL if and only if method=override (a flat value has no tail); the CHECK enforces both directions.';
COMMENT ON COLUMN core_model_reference_vault_result.source IS 'Provenance slug of the upstream route that produced the row.';
COMMENT ON COLUMN core_model_reference_vault_result.processing_version IS 'PK, Audit. Correction version: 0=original, N=Nth reprocess under a later build (ADR-0002).';
COMMENT ON COLUMN core_model_reference_vault_result.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';
COMMENT ON COLUMN core_model_reference_vault_result.run_id IS 'Audit. writer_run.id of the process start that wrote this row (ADR-0006 §2). Not an FK, like build_id; never used for ordering or to pick the latest row.';

REVOKE UPDATE, DELETE, TRUNCATE ON core_model_reference_vault_result FROM stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260917_120000_create_core_model_reference_results.sql')
ON CONFLICT (filename) DO NOTHING;
