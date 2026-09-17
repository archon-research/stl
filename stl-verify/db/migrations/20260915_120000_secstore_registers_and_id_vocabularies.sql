-- VEC-616: identifier resolution for the combined master — the instrument register (native
-- instrument_key -> SECURITY node) and the alias register (public identifier -> any node), with
-- the two governed vocabularies that scope them. Deferred out of wave 1 (VEC-617, #875); the
-- model is ADR-0007 §1–§2, the audit spine and supersession order ADR-0006 §2/§5.
--
-- These are the two seams §9 gives the position stream: a position carries its native
-- instrument_key (hashed into position_id) and its holder address, and resolves both here at
-- READ time. Nothing is stamped onto position_state, so a reclassification never rewrites a
-- position. They replace the frozen security_instrument_bridge and entity_ref_codes paths.

-- ---------------------------------------------------------------------------
-- Vocabularies: governed lists at the ref_* bar, seeded once (as ADR-0007 §5's).
-- ---------------------------------------------------------------------------

CREATE TABLE key_namespace_vocabulary (
    key_namespace text PRIMARY KEY,
    description   text NOT NULL,
    change_reason text NOT NULL DEFAULT 'SEED_LOAD',
    run_id        bigint REFERENCES writer_run(id)
);
COMMENT ON TABLE key_namespace_vocabulary IS '[Configuration] The forms an instrument_register.instrument_key can take (ADR-0007 §1.2). A new source of positions is one row here, not a schema change — which is also how a non-EVM asset with no contract gets a key. Plain table: seed-once, extended by reviewed migration.';
COMMENT ON COLUMN key_namespace_vocabulary.key_namespace IS 'Roles: PK. Namespace name, lower_snake. It describes the FORM of the native key, never what the instrument is: a house classifier in a key would end up in position_id, which hashes instrument_key (VEC-400).';
COMMENT ON COLUMN key_namespace_vocabulary.description IS 'Which source assigns the key and how it is composed. Composite keys join their components with '':'', each component '':''-free.';
COMMENT ON COLUMN key_namespace_vocabulary.change_reason IS 'Roles: Audit. Why the row exists (vocabulary rows carry the slim spine; full provenance lives on the register rows).';
COMMENT ON COLUMN key_namespace_vocabulary.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2). NULL means written before run tracking — which is what the rows seeded here are.';

CREATE TABLE id_scheme_vocabulary (
    id_scheme      text PRIMARY KEY,
    applies_to     text[] NOT NULL CHECK (
                     applies_to <> '{}'
                     AND applies_to <@ ARRAY['ENTITY','SECURITY','CONCEPT','SOURCE','ACCOUNT']),
    value_form     text NOT NULL,
    unique_current boolean NOT NULL,
    description    text NOT NULL,
    change_reason  text NOT NULL DEFAULT 'SEED_LOAD',
    run_id         bigint REFERENCES writer_run(id)
);
COMMENT ON TABLE id_scheme_vocabulary IS '[Configuration] The identifier schemes alias_register can carry (ADR-0007 §1.3). Public identifiers are lookups, never node ids: every one of them is eventually reused, corrected or reassigned, and an id derived from one breaks with it. Plain table: seed-once, extended by reviewed migration.';
COMMENT ON COLUMN id_scheme_vocabulary.id_scheme IS 'Roles: PK. Scheme name, UPPER_SNAKE.';
COMMENT ON COLUMN id_scheme_vocabulary.applies_to IS 'Node kinds this scheme can alias (sec_node.record_type values). CHECKed non-empty and against the closed kind set, so a typo fails at seed time rather than yielding a scheme nothing can use.';
COMMENT ON COLUMN id_scheme_vocabulary.value_form IS 'Human-readable check rule for id_value (length, alphabet, casing). Prose rather than a regex: the register holds one value column for every scheme, so per-scheme form is validator-enforced, not a column CHECK.';
COMMENT ON COLUMN id_scheme_vocabulary.unique_current IS 'true: one current node per value — a DQ check over alias_register_current, not a write constraint, because an open alias always time-overlaps its own re-point (the ADR-0007 §5 cardinality rule). false: many-to-many over time, as TICKER is.';
COMMENT ON COLUMN id_scheme_vocabulary.description IS 'What the scheme identifies and who assigns it.';
COMMENT ON COLUMN id_scheme_vocabulary.change_reason IS 'Roles: Audit. Why the row exists.';
COMMENT ON COLUMN id_scheme_vocabulary.run_id IS 'Roles: FK→writer_run.id, Audit. See key_namespace_vocabulary.run_id.';

-- ---------------------------------------------------------------------------
-- Seam 1: native instrument key -> security.
-- ---------------------------------------------------------------------------

CREATE TABLE instrument_register (
    instrument_key      text NOT NULL,
    key_namespace       text NOT NULL REFERENCES key_namespace_vocabulary(key_namespace),
    security_id         text NOT NULL,
    chain_id            int4,
    chain_scope         int4 GENERATED ALWAYS AS (coalesce(chain_id, 0)) STORED,
    attrs               jsonb NOT NULL DEFAULT '{}'::jsonb,
    valid_from          date NOT NULL,
    valid_to            date NOT NULL DEFAULT 'infinity',
    record_id           bigint GENERATED ALWAYS AS IDENTITY,
    processing_version  integer NOT NULL DEFAULT 0 CHECK (processing_version >= 0),
    ingest_xid          xid8 NOT NULL DEFAULT pg_current_xact_id(),
    ingested_at         timestamptz NOT NULL DEFAULT now(),
    run_id              bigint REFERENCES writer_run(id),
    actor               text NOT NULL,
    change_reason_code  text NOT NULL REFERENCES change_reason_vocabulary(code),
    change_reason       text NOT NULL,
    approved_by         text,
    supersedes_record_id bigint,
    source_system       text NOT NULL,
    content_hash        bytea NOT NULL,
    PRIMARY KEY (instrument_key, chain_scope, valid_from, valid_to, processing_version),
    CONSTRAINT instrument_register_record_id_key UNIQUE (record_id),
    CONSTRAINT instrument_register_valid_chk CHECK (valid_from <= valid_to),
    CONSTRAINT instrument_register_security_id_prefix_chk CHECK (security_id LIKE 'sec-%'),
    -- No surrounding whitespace, not merely non-blank: position_id hashes this text, so a
    -- leading space forks the identity of every position on the key.
    CONSTRAINT instrument_register_key_nonblank_chk CHECK (instrument_key <> '' AND instrument_key = btrim(instrument_key)),
    -- position_key() renders the key between ';' delimiters, so a ';' inside one collapses two
    -- identities onto one position_id. A key this register accepts but position_id forks is worse
    -- than a rejected load.
    CONSTRAINT instrument_register_key_delim_chk CHECK (strpos(instrument_key, ';') = 0),
    CONSTRAINT instrument_register_chain_pos_chk CHECK (chain_id IS NULL OR chain_id > 0),
    CONSTRAINT instrument_register_valid_from_finite_chk CHECK (valid_from <> 'infinity' AND valid_from <> '-infinity')
);
COMMENT ON TABLE instrument_register IS '[Dimension] Native instrument key -> SECURITY node (ADR-0007 §2): the instrument is a KEY, not a node, so resolution is a lookup and no RESOLVES_TO edge type exists. Many keys resolve to one security — six USDC deployments are six rows pointing at one sec-usdc — which is why this is a register rather than a node kind. Append-only (full ACL revoke incl. owner; nothing FKs it), bitemporal. Successor of the frozen security_instrument_bridge. The hottest join in the metric path: position_state.instrument_key resolves here (seam 1, ADR-0007 §9.1) and nothing is stamped back onto the position. Identity is (instrument_key, chain_scope), NOT instrument_key alone as ADR-0007 §10''s dim_instrument contract still says — that correction is VEC-616''s to record. Plain table: governance-rate writes.';
COMMENT ON COLUMN instrument_register.instrument_key IS 'Roles: PK (with chain_scope, valid_from, valid_to, processing_version). The instrument''s NATIVE, globally-unique id, assigned by the chain or the source and never by us: contract address (lowercase hex, no 0x), protocol-emitted market id, the Sky ilk, provider:package. position_id hashes this value (VEC-400), so no house classifier may appear in it and its text form is frozen once positions exist. key_namespace is deliberately NOT part of the key: a position row carries no namespace, so if one key could sit under two namespaces the read could not choose between them. The key alone cannot enforce that — two rows differing in valid_from do not collide — so instrument_register_namespace_guard refuses the second namespace.';
COMMENT ON COLUMN instrument_register.key_namespace IS 'Roles: FK→key_namespace_vocabulary.key_namespace. Which form the key takes. Descriptive, not part of identity (see instrument_key).';
COMMENT ON COLUMN instrument_register.security_id IS 'Roles: FK→sec_node.id (soft; SCD2 ids are non-unique, so resolve through sec_node_current). The SECURITY node this key means. NO foreign key BY DESIGN, and not merely because the target is non-unique: a register row must be writable before its security node exists, which is what lets the register load ahead of the security load (VEC-625/627). An unresolvable security_id is a DQ finding, not a write error.';
COMMENT ON COLUMN instrument_register.chain_id IS 'Roles: FK→chain.chain_id (soft). Native chain id, raw integer — READ THIS COLUMN FOR THE CHAIN, never chain_scope. NULL where the namespace has no chain BY CONSTRUCTION, not where one was missed: Anchorage packages are off-chain custody and Sky ilks come from a source that carries no chain column. Hardcoding 1 for those is the remembered-constant antipattern VEC-400''s helper warns against.';
COMMENT ON COLUMN instrument_register.chain_scope IS 'Roles: PK component, Derived. EXISTS FOR THE KEY AND IS NOT THE CHAIN — read chain_id for that. coalesce(chain_id, 0), because the same contract address is deployed on several chains (13 addresses today, 3 of them held) and a key without chain rejects the second one; chain_id itself cannot go in the key because a PRIMARY KEY forces NOT NULL, which would make the two chainless namespaces unregisterable unless they carried a sentinel in real data. 0 is safe: EVM chain ids start at 1 and the chain_id CHECK refuses anything lower. Reads join on instrument_key = $1 AND chain_scope = coalesce($2, 0), on columns the position stream already carries. Excluded from content_hash, which covers chain_id instead — a STORED generated column is computed AFTER the BEFORE INSERT guard, so hashing it would hash a NULL and never reproduce from the stored row.';
COMMENT ON COLUMN instrument_register.attrs IS 'Instrument-level attributes that are not identity: symbol, decimals, venue, first-seen block. Decimals here are the token''s own scale, never applied to a quantity by this table.';
COMMENT ON COLUMN instrument_register.valid_from IS 'Roles: PK component. Valid-time window start, UTC date, half-open [valid_from, valid_to). A re-point is a new row with a later valid_from, never an UPDATE: MKR -> SKY leaves the MKR row in place and the SKY row becomes current. Grain is a day, as sec_node.valid_from.';
COMMENT ON COLUMN instrument_register.valid_to IS 'Roles: PK component. Valid-time window end, exclusive; ''infinity'' = open, never NULL. In the key so ending a mapping is an ordinary append at processing_version 0 rather than an UPDATE the append-only grants refuse — a key whose instrument is retired with no successor security has to stop resolving somehow. A zero-length window is a retraction tombstone, as sec_node.valid_to — and it withdraws ONE WINDOW, so retiring an open mapping means tombstoning the window it opened, not appending a later one. Note the converse too: giving a re-point a finite valid_to does not end the key, it resurrects whatever the re-point superseded. Because this column is in the key but NOT in what the reads group on, a second row on one window would otherwise land silently and displace the first; sec_register_supersession_guard requires such an append to name the row it closes.';
COMMENT ON COLUMN instrument_register.record_id IS 'Roles: Audit, UNIQUE. Per-append surrogate; what supersedes_record_id and a reproduction manifest point at (PR-2.1).';
COMMENT ON COLUMN instrument_register.processing_version IS 'Roles: Audit, PK component. Correction version, caller-assigned per ADR-0006 §3: 0 live, N per correction run. A re-point is a VALID-TIME change and stays at 0 — which is exactly what the drafted key (instrument_key, processing_version) could not hold, since the second row collided. See sec_node.processing_version for what a correction at N then does to later loads.';
COMMENT ON COLUMN instrument_register.ingest_xid IS 'Roles: Audit. Knowledge-time visibility key (ADR-0006 §5) and the supersession tiebreak inside a valid window. Never writer-supplied; enforced by the append guard.';
COMMENT ON COLUMN instrument_register.ingested_at IS 'Roles: Audit. Wall-clock label only; never the audit key.';
COMMENT ON COLUMN instrument_register.run_id IS 'Roles: FK→writer_run.id, Audit. See sec_node.run_id.';
COMMENT ON COLUMN instrument_register.actor IS 'Roles: Audit. Real, non-shared principal that appended the row. Required.';
COMMENT ON COLUMN instrument_register.change_reason_code IS 'Roles: FK→change_reason_vocabulary.code, Audit.';
COMMENT ON COLUMN instrument_register.change_reason IS 'Roles: Audit. Free-text reason for the append.';
COMMENT ON COLUMN instrument_register.approved_by IS 'Roles: Audit. Approver, distinct from actor, where the reason code requires approval.';
COMMENT ON COLUMN instrument_register.supersedes_record_id IS 'Roles: FK-shaped→instrument_register.record_id (enforced by the append guard, which needs the predecessor''s content_hash to chain this row''s), Audit. Not consulted by the resolved reads.';
COMMENT ON COLUMN instrument_register.source_system IS 'Roles: Audit. Which table or feed the mapping came from.';
COMMENT ON COLUMN instrument_register.content_hash IS 'Roles: Audit, Derived. sha256 over the canonical stored form, computed by sec_store_append_guard on every insert so the chain runs from the first append (AR-1.2). A supplied value is verified, never trusted. See sec_node.content_hash for the exact pre-image and its portability limits.';
-- Resolution index: the reads sort (instrument_key, chain_scope, valid_from) ASC then
-- processing_version DESC, ingest_xid DESC, record_id DESC. Columns AND directions must match the
-- whole key or the DISTINCT ON degrades to a full scan plus sort on every resolution (as
-- sec_node_resolve_idx).
-- A row is superseded at most once. The supersession guard only checks that the named row is on
-- the window being landed on, so A-open, B-closes-A, C-also-closes-A all pass it and B becomes
-- unreachable — the shadow row again, one level up. Declarative rather than another trigger
-- branch: a fork is a uniqueness violation, not a policy decision (VEC-823 covers the same hole
-- on sec_node / sec_edge, which are untouched here).
CREATE UNIQUE INDEX instrument_register_supersedes_once_idx
    ON instrument_register (supersedes_record_id) WHERE supersedes_record_id IS NOT NULL;
CREATE INDEX instrument_register_resolve_idx ON instrument_register (instrument_key, chain_scope, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC);
-- Reverse lookup (which keys mean this security) and the DQ sweeps, both of which read the base
-- table; the resolved views scan and filter, so this index does not serve them.
CREATE INDEX instrument_register_security_idx ON instrument_register (security_id);

-- ---------------------------------------------------------------------------
-- Seam 2: public identifier / pipeline link / holder address -> any node.
-- ---------------------------------------------------------------------------

CREATE TABLE alias_register (
    id_scheme           text NOT NULL REFERENCES id_scheme_vocabulary(id_scheme),
    id_value            text NOT NULL,
    node_id             text NOT NULL,
    valid_from          date NOT NULL,
    valid_to            date NOT NULL DEFAULT 'infinity',
    record_id           bigint GENERATED ALWAYS AS IDENTITY,
    processing_version  integer NOT NULL DEFAULT 0 CHECK (processing_version >= 0),
    ingest_xid          xid8 NOT NULL DEFAULT pg_current_xact_id(),
    ingested_at         timestamptz NOT NULL DEFAULT now(),
    run_id              bigint REFERENCES writer_run(id),
    actor               text NOT NULL,
    change_reason_code  text NOT NULL REFERENCES change_reason_vocabulary(code),
    change_reason       text NOT NULL,
    approved_by         text,
    supersedes_record_id bigint,
    source_system       text NOT NULL,
    content_hash        bytea NOT NULL,
    PRIMARY KEY (id_scheme, id_value, valid_from, valid_to, processing_version),
    CONSTRAINT alias_register_record_id_key UNIQUE (record_id),
    -- Same prefix contract sec_edge's endpoints carry: sec_node_id_prefix_chk makes record_type a
    -- deterministic function of the prefix, so an alias pointing at no possible node is
    -- single-row checkable. Endpoint EXISTENCE is cross-row and stays with the validator.
    CONSTRAINT alias_register_node_prefix_chk CHECK (
        node_id LIKE 'em-%' OR node_id LIKE 'sec-%' OR node_id LIKE 'concept-%'
        OR node_id LIKE 'src-%' OR node_id LIKE 'acct-%'
    ),
    -- Addresses are stored in the same form position_state.holder_id uses, so seam 2 is a plain
    -- equality join: lowercase hex, no 0x. Any other casing forks the holder's identity.
    -- 40 exactly, matching position_state_holder_hex_chk. A shorter value is well-formed hex
    -- that seam 2 can never join, so it would register and silently resolve nothing.
    CONSTRAINT alias_register_hex_chk CHECK (
        id_scheme NOT IN ('CONTRACT_ADDRESS','BLOCKCHAIN_ADDRESS')
        OR id_value ~ '^[0-9a-f]{40}$'
    ),
    CONSTRAINT alias_register_value_nonblank_chk CHECK (id_value <> '' AND id_value = btrim(id_value)),
    CONSTRAINT alias_register_valid_chk CHECK (valid_from <= valid_to),
    CONSTRAINT alias_register_valid_from_finite_chk CHECK (valid_from <> 'infinity' AND valid_from <> '-infinity')
);
COMMENT ON TABLE alias_register IS '[Dimension] Public identifier, pipeline link or holder address -> node (ADR-0007 §1.3). One book with a scheme column: LEI, ISIN, TICKER, contract and wallet addresses, prime and protocol ids all live here, told apart by id_scheme. Aliases are LOOKUPS, never ids and never hashed into a key, so a node stays reachable by any of its aliases without any one of them being load-bearing. Append-only (full ACL revoke incl. owner; nothing FKs it), bitemporal. Successor of entity_ref_codes and the holder_entity resolver, extended to securities; it is the holder-resolution path for positions (seam 2, ADR-0007 §9.2). Plain table: governance-rate writes.';
COMMENT ON COLUMN alias_register.id_scheme IS 'Roles: FK→id_scheme_vocabulary.id_scheme, PK component. Which identifier namespace id_value is drawn from.';
COMMENT ON COLUMN alias_register.id_value IS 'Roles: PK component. The identifier within the scheme, verbatim from its assigner — except addresses, which are normalised to lowercase hex with no 0x (CHECK) so this joins position_state.holder_id directly.';
COMMENT ON COLUMN alias_register.node_id IS 'Roles: FK→sec_node.id (soft; resolve through sec_node_current). Any node kind — the scheme''s applies_to says which are legal. No foreign key, for the reason instrument_register.security_id gives.';
COMMENT ON COLUMN alias_register.valid_from IS 'Roles: PK component. Valid-time window start, UTC date, half-open [valid_from, valid_to). An alias re-pointing is a versioned append, as an instrument re-point is.';
COMMENT ON COLUMN alias_register.valid_to IS 'Roles: PK component. Valid-time window end, exclusive; ''infinity'' = open, never NULL. In the key so closing an alias — a lapsed LEI, a wallet that changed hands — is an append at processing_version 0, which must name the row it closes (sec_register_supersession_guard). It is exactly what a nullable valid_to outside the key could not express: the closing row would collide with the open one it closes, leaving an UPDATE as the only route and the append-only grants refusing it. A zero-length window is a retraction tombstone.';
COMMENT ON COLUMN alias_register.record_id IS 'Roles: Audit, UNIQUE. See instrument_register.record_id.';
COMMENT ON COLUMN alias_register.processing_version IS 'Roles: Audit, PK component. Correction version, 0 live (ADR-0006 §3).';
COMMENT ON COLUMN alias_register.ingest_xid IS 'Roles: Audit. Knowledge-time visibility key and supersession tiebreak; never writer-supplied.';
COMMENT ON COLUMN alias_register.ingested_at IS 'Roles: Audit. Wall-clock label only.';
COMMENT ON COLUMN alias_register.run_id IS 'Roles: FK→writer_run.id, Audit. See sec_node.run_id.';
COMMENT ON COLUMN alias_register.actor IS 'Roles: Audit. Appending principal. Required.';
COMMENT ON COLUMN alias_register.change_reason_code IS 'Roles: FK→change_reason_vocabulary.code, Audit.';
COMMENT ON COLUMN alias_register.change_reason IS 'Roles: Audit. Free-text reason for the append.';
COMMENT ON COLUMN alias_register.approved_by IS 'Roles: Audit. Approver where the reason code requires one.';
COMMENT ON COLUMN alias_register.supersedes_record_id IS 'Roles: FK-shaped→alias_register.record_id (enforced by the append guard), Audit. Not consulted by the resolved reads.';
COMMENT ON COLUMN alias_register.source_system IS 'Roles: Audit. Where the alias came from.';
COMMENT ON COLUMN alias_register.content_hash IS 'Roles: Audit, Derived. See instrument_register.content_hash.';
-- See instrument_register_supersedes_once_idx.
CREATE UNIQUE INDEX alias_register_supersedes_once_idx
    ON alias_register (supersedes_record_id) WHERE supersedes_record_id IS NOT NULL;
CREATE INDEX alias_register_resolve_idx ON alias_register (id_scheme, id_value, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC);
CREATE INDEX alias_register_node_idx ON alias_register (node_id);

-- ---------------------------------------------------------------------------
-- Reads: latest append per (logical record, valid_from) first, then the valid window.
-- The other order resurrects a superseded row (ADR-0006 §5; the wave-1 rule).
-- ---------------------------------------------------------------------------

-- Step one of the two-step read, shared by the reads that can take it. The knowledge-time
-- overload CANNOT: its snapshot filter has to run BEFORE this resolution, or a correction
-- invisible in the snapshot wins its group and then vanishes, so it inlines the CTE instead.
CREATE VIEW instrument_register_latest AS
SELECT DISTINCT ON (instrument_key, chain_scope, valid_from) *
FROM instrument_register
ORDER BY instrument_key, chain_scope, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC;
COMMENT ON VIEW instrument_register_latest IS 'Latest append per (instrument_key, chain_scope, valid_from) — step one of the two-step read (ADR-0006 §5), with no valid-time filter applied. Not a consumer surface: reading it alone returns closed and tombstoned windows. instrument_register_current and instrument_register_as_of(date) apply the window to it; instrument_register_as_of(date, pg_snapshot) must not use it, because the snapshot filter has to precede this resolution.';

CREATE VIEW instrument_register_current AS
SELECT DISTINCT ON (instrument_key, chain_scope) *
FROM instrument_register_latest
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
  AND (now() AT TIME ZONE 'utc')::date < valid_to
ORDER BY instrument_key, chain_scope, valid_from DESC;
COMMENT ON VIEW instrument_register_current IS 'One current mapping per (key, chain_scope) — the unique-current guarantee dim_instrument carries and the metric path''s hottest join needs. A closed or tombstoned window is absent here. Operational reads only: a calculation uses instrument_register_as_of(effective_at), and a replay instrument_register_as_of(effective_at, known_at), because resolving the register at now() while scanning last quarter''s positions applies today''s mapping to yesterday''s holding, silently (ADR-0007 §9.3). Two OVERLAPPING windows for one key are not rejected here or by the PK — an open mapping always time-overlaps its own re-point, so that stays a DQ check over current state, the same position wave 1 took for single-valued edge cardinality. What IS refused at write is a second key_namespace for one key, which no key shape could catch (instrument_register_namespace_guard).';

CREATE FUNCTION instrument_register_as_of(effective_at date)
RETURNS SETOF instrument_register LANGUAGE sql STABLE AS $$
    SELECT DISTINCT ON (instrument_key, chain_scope) *
    FROM instrument_register_latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY instrument_key, chain_scope, valid_from DESC
$$;
COMMENT ON FUNCTION instrument_register_as_of(date) IS 'As-of instrument resolution; effective_at is an explicit recorded parameter, never now() (ADR-0006 §4). Positions are block-keyed and this is calendar-dated, so the caller passes the block''s UTC date from block_meta — the only correct join (ADR-0007 §9.3).';

-- Knowledge-time read: the snapshot filter runs BEFORE version resolution, or a correction
-- invisible in the snapshot wins its group and then vanishes (see sec_node_as_of).
CREATE FUNCTION instrument_register_as_of(effective_at date, known_at pg_snapshot)
RETURNS SETOF instrument_register LANGUAGE sql STABLE AS $$
    WITH known AS (
        SELECT * FROM instrument_register
        WHERE pg_visible_in_snapshot(ingest_xid, known_at)
    ), latest AS (
        SELECT DISTINCT ON (instrument_key, chain_scope, valid_from) *
        FROM known
        ORDER BY instrument_key, chain_scope, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (instrument_key, chain_scope) *
    FROM latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY instrument_key, chain_scope, valid_from DESC
$$;
COMMENT ON FUNCTION instrument_register_as_of(date, pg_snapshot) IS 'Bitemporal instrument resolution: latest append visible in known_at per (key, valid_from), then the valid window over effective_at (RP-4.1, CR-3.6). Serves the exact replay a recorded snapshot pins.';

CREATE VIEW alias_register_latest AS
SELECT DISTINCT ON (id_scheme, id_value, valid_from) *
FROM alias_register
ORDER BY id_scheme, id_value, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC;
COMMENT ON VIEW alias_register_latest IS 'Latest append per (id_scheme, id_value, valid_from) — step one of the two-step read; see instrument_register_latest for why the knowledge-time overload does not use it.';

CREATE VIEW alias_register_current AS
SELECT DISTINCT ON (id_scheme, id_value) *
FROM alias_register_latest
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
  AND (now() AT TIME ZONE 'utc')::date < valid_to
ORDER BY id_scheme, id_value, valid_from DESC;
COMMENT ON VIEW alias_register_current IS 'Current alias -> node resolution, two-step as instrument_register_current. Operational reads only; calculations use alias_register_as_of(effective_at).';

CREATE FUNCTION alias_register_as_of(effective_at date)
RETURNS SETOF alias_register LANGUAGE sql STABLE AS $$
    SELECT DISTINCT ON (id_scheme, id_value) *
    FROM alias_register_latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY id_scheme, id_value, valid_from DESC
$$;
COMMENT ON FUNCTION alias_register_as_of(date) IS 'As-of alias resolution; effective_at is an explicit recorded parameter, never now() (ADR-0006 §4).';

CREATE FUNCTION alias_register_as_of(effective_at date, known_at pg_snapshot)
RETURNS SETOF alias_register LANGUAGE sql STABLE AS $$
    WITH known AS (
        SELECT * FROM alias_register
        WHERE pg_visible_in_snapshot(ingest_xid, known_at)
    ), latest AS (
        SELECT DISTINCT ON (id_scheme, id_value, valid_from) *
        FROM known
        ORDER BY id_scheme, id_value, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (id_scheme, id_value) *
    FROM latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY id_scheme, id_value, valid_from DESC
$$;
COMMENT ON FUNCTION alias_register_as_of(date, pg_snapshot) IS 'Bitemporal alias resolution; see instrument_register_as_of(date, pg_snapshot).';

-- ---------------------------------------------------------------------------
-- Vocabulary seeds: the four key namespaces and the eleven schemes ADR-0007 ratified.
-- ---------------------------------------------------------------------------

INSERT INTO key_namespace_vocabulary (key_namespace, description) VALUES
 ('token_address','token / aToken / debtToken / ERC-4626 vault: the on-chain contract address, lowercase hex, no 0x'),
 ('morpho_market','the protocol-emitted market id (bytes32), as Morpho emits it'),
 ('sky_ilk','the Sky ilk: a collateral/vault type in the Vat, e.g. ALLOCATOR-SPARK-A'),
 ('provider_package','provider '':'' package_id, the source-native custody id (Anchorage)')
ON CONFLICT (key_namespace) DO NOTHING;

INSERT INTO id_scheme_vocabulary (id_scheme, applies_to, value_form, unique_current, description) VALUES
 ('LEI','{ENTITY}','20-char ISO 17442, uppercase alphanumeric', true,'Legal Entity Identifier, assigned by a GLEIF-accredited LOU'),
 ('ISIN','{SECURITY}','12-char ISO 6166', true,'International Securities Identification Number'),
 ('CUSIP','{SECURITY}','9-char', true,'CUSIP, North American securities'),
 ('SEDOL','{SECURITY}','7-char', true,'SEDOL, London Stock Exchange'),
 ('FIGI','{SECURITY}','12-char OpenFIGI', true,'Financial Instrument Global Identifier'),
 ('TICKER','{SECURITY}','venue ticker; many-to-many over time', false,'Exchange ticker symbol, and the ONE home for it: a ticker is an alias row, never a node attribute, so a rename is a new row rather than an overwrite and dim_security.ticker reads the current alias. Not unique_current: tickers are reassigned, and one security carries different tickers per venue'),
 ('CONTRACT_ADDRESS','{SECURITY,ENTITY}','hex lowercase, no 0x', true,'On-chain contract address'),
 ('BLOCKCHAIN_ADDRESS','{ENTITY,ACCOUNT}','hex lowercase, no 0x', true,'On-chain wallet / EOA address; the position holder-resolution path (seam 2)'),
 ('PIPELINE_PRIME_ID','{ENTITY,ACCOUNT}','prime.id, verbatim', true,'Our own prime registry id — the path the frozen holder_entity view took for PRIME holders'),
 ('PIPELINE_PROTOCOL_ID','{ENTITY}','protocol.id, verbatim', true,'Our own protocol registry id'),
 ('INTERNAL','{ENTITY,ACCOUNT,SOURCE}','house-internal code', true,'House-internal code with no external assigner')
ON CONFLICT (id_scheme) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Write boundary and append-only, on the wave-1 terms (VEC-617).
-- ---------------------------------------------------------------------------

-- Replaced for one reason: a STORED generated column is computed AFTER a BEFORE INSERT trigger,
-- so the guard hashed a NULL chain_scope, which no verification from the stored row reproduces.
CREATE OR REPLACE FUNCTION sec_store_append_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
DECLARE
    computed     bytea;
    pre_image    jsonb;
    parent_hash  bytea;
    declared_key text[];
    declared_src text[];
    declared_dst text[];
    derived_disc text;
    parent_disc  text;
    vocab_found  boolean;
    generated_col text;
    needs_approval boolean;
BEGIN
    IF NEW.ingest_xid IS DISTINCT FROM pg_current_xact_id() THEN
        RAISE EXCEPTION 'ingest_xid is platform-assigned on %.% and must never be writer-supplied (ADR-0007 §4, ADR-0006 §5); omit the column and let the default stand',
            TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;

    -- Derived before the pre-image is taken, so content_hash covers the discriminator it ends up
    -- keyed by. Keyed on the column rather than the table name, which a rename would defeat.
    IF to_jsonb(NEW) ? 'edge_disc' THEN
        SELECT v.cluster_key, v.src_kinds, v.dst_kinds, true
          INTO declared_key, declared_src, declared_dst, vocab_found
          FROM rel_type_vocabulary v WHERE v.rel_type = NEW.rel_type;
        IF coalesce(vocab_found, false) THEN
            -- GQ-11's triple, on the row already read for cluster_key. src_kinds and dst_kinds are
            -- independent sets, so this is the cross product; a type legal only over matched pairs
            -- (SAME_AS, SUPERSEDES) needs the legal-pairs shape VEC-622 owns.
            IF NOT (NEW.src_kind = ANY (declared_src) AND NEW.dst_kind = ANY (declared_dst)) THEN
                RAISE EXCEPTION '% is declared % -> %, so (%, %) is not a legal endpoint pair for it (ADR-0007 §5, GQ-11)',
                    NEW.rel_type, declared_src, declared_dst, NEW.src_kind, NEW.dst_kind;
            END IF;

            derived_disc := sec_edge_discriminator(NEW.payload, declared_key);
            IF derived_disc IS NULL THEN
                RAISE EXCEPTION 'payload carries none of the cluster keys % that % declares, so this edge has no identity to take; the discriminator is derived from them (ADR-0007 §3)',
                    declared_key, NEW.rel_type;
            END IF;
            IF NEW.edge_disc IS NOT NULL AND NEW.edge_disc <> derived_disc THEN
                RAISE EXCEPTION 'edge_disc mismatch on %.%: supplied %, derived % — the discriminator is a function of rel_type_vocabulary.cluster_key over the payload, verified and never trusted (ADR-0007 §3)',
                    TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.edge_disc, derived_disc;
            END IF;
            NEW.edge_disc := derived_disc;

            -- A correction or a retraction names the row it amends. Its cluster key is that row's
            -- identity, so a superseding append that derives a different one is amending nothing:
            -- without this the tombstone lands on a second edge and leaves its target current.
            IF NEW.supersedes_record_id IS NOT NULL THEN
                EXECUTE format('SELECT edge_disc FROM %I.%I WHERE record_id = $1', TG_TABLE_SCHEMA, TG_TABLE_NAME)
                    INTO parent_disc USING NEW.supersedes_record_id;
                IF parent_disc IS NOT NULL AND parent_disc <> NEW.edge_disc THEN
                    RAISE EXCEPTION 'this append supersedes record % whose edge_disc is %, but derives % from its own payload — a changed cluster key is a different edge, so retract that one and append the new (ADR-0007 §3)',
                        NEW.supersedes_record_id, parent_disc, NEW.edge_disc;
                END IF;
            END IF;
        ELSE
            -- No vocabulary row: the rel_type FK owns this (23503, GQ-01). A placeholder only so
            -- the NOT NULL check, which runs first, cannot pre-empt it with 23502.
            NEW.edge_disc := coalesce(NEW.edge_disc, 'base');
        END IF;
    END IF;

    pre_image := to_jsonb(NEW)
                   - 'record_id' - 'ingest_xid' - 'ingested_at' - 'content_hash'
                   - 'supersedes_record_id';

    -- Every STORED generated column, dropped by catalogue rather than by name: Postgres computes
    -- them AFTER this trigger, so NEW carries NULL for each and a hash over that NULL would never
    -- reproduce from the stored row. Their inputs are ordinary columns and stay in the pre-image,
    -- so nothing is lost. This generalises the literal '- edge_id' it replaces — sec_edge.edge_id
    -- is the one such column on the wave-1 stores, so their hashes are unchanged — and covers
    -- instrument_register.chain_scope (VEC-616) and whatever a later store generates.
    FOR generated_col IN
        SELECT attname FROM pg_attribute
         WHERE attrelid = TG_RELID AND attgenerated <> '' AND NOT attisdropped
    LOOP
        pre_image := pre_image - generated_col;
    END LOOP;

    IF NEW.supersedes_record_id IS NOT NULL THEN
        EXECUTE format('SELECT content_hash FROM %I.%I WHERE record_id = $1', TG_TABLE_SCHEMA, TG_TABLE_NAME)
            INTO parent_hash USING NEW.supersedes_record_id;
        IF parent_hash IS NULL THEN
            RAISE EXCEPTION 'supersedes_record_id % names no stored row in %.%; a correction chains on the content it supersedes, so the predecessor must already be appended (AR-1.2)',
                NEW.supersedes_record_id, TG_TABLE_SCHEMA, TG_TABLE_NAME;
        END IF;
        pre_image := pre_image || jsonb_build_object('supersedes_content_hash', encode(parent_hash, 'hex'));
    END IF;

    -- CR-3.3 / four-eyes (NFR-2), VEC-793: the vocabulary has said which codes need an approver
    -- since wave 1 seeded it, and nothing read it. Placed after the supersedes resolution so an
    -- unresolvable pointer still reports itself rather than being pre-empted by a missing approver.
    SELECT v.requires_approval INTO needs_approval
      FROM change_reason_vocabulary v WHERE v.code = NEW.change_reason_code;
    IF coalesce(needs_approval, false) THEN
        IF NEW.approved_by IS NULL THEN
            RAISE EXCEPTION 'change_reason_code % requires an approver on %.%, and approved_by is null (ADR-0007 §4, CR-3.3)',
                NEW.change_reason_code, TG_TABLE_SCHEMA, TG_TABLE_NAME;
        END IF;
        IF NEW.approved_by = NEW.actor THEN
            RAISE EXCEPTION 'approved_by must differ from actor on %.%: % approved their own append, which is not four-eyes (NFR-2)',
                TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.actor;
        END IF;
    END IF;

    computed := sha256(convert_to(pre_image::text, 'UTF8'));

    IF NEW.content_hash IS NOT NULL AND NEW.content_hash <> computed THEN
        RAISE EXCEPTION 'content_hash mismatch on %.%: supplied %, computed % — a supplied hash is verified, never trusted (AR-1.2)',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, encode(NEW.content_hash,'hex'), encode(computed,'hex');
    END IF;

    NEW.content_hash := computed;
    RETURN NEW;
END $$;
COMMENT ON FUNCTION sec_store_append_guard() IS 'BEFORE INSERT guard for sec_node / sec_edge and the VEC-616 registers: rejects a writer-supplied ingest_xid; on a table carrying edge_disc, derives it from the type''s declared cluster key before the pre-image is taken so the hash covers it, refuses a payload carrying none of the declared keys, and refuses an append whose supersedes_record_id names a row of a different identity; computes content_hash over to_jsonb(row) minus the platform-assigned fields, minus every STORED generated column (computed after this trigger, so NEW holds NULL for them — their inputs are hashed instead), with supersedes_record_id replaced by the predecessor''s content_hash so the digest chains and survives a re-import that reassigns record_ids; rejects a supersedes_record_id naming no stored row; verifies rather than trusts a supplied hash or discriminator (AR-1.2, NFR-5). Reads the store it guards by record_id and rel_type_vocabulary by rel_type (equality on unique indexes), plus change_reason_vocabulary by code and a range scan of pg_attribute for the relation''s generated columns. None is the per-row hypertable lookup AGENTS.md''s plan_cache_mode rule is scoped to — these are plain tables at governance write rates.';

-- One key belongs to one namespace, enforced here because the primary key cannot: two rows
-- differing in valid_from do not collide, so a token address and a Maple loan address that happen
-- to share a string would both land and the later one would silently shadow the earlier in every
-- resolved read. The read side cannot arbitrate — a position row carries no namespace — so the
-- collision has to be refused at write. The advisory lock is the AGENTS.md read-then-write rule:
-- the decision is made from a SELECT, which ON CONFLICT cannot guard (ADR-0002 §3).
CREATE FUNCTION instrument_register_namespace_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
DECLARE
    other_namespace text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('instrument_register:' || NEW.instrument_key || ':' || coalesce(NEW.chain_id, 0)::text));

    SELECT r.key_namespace INTO other_namespace
      FROM instrument_register r
     WHERE r.instrument_key = NEW.instrument_key
       AND r.chain_scope = coalesce(NEW.chain_id, 0)
       AND r.key_namespace <> NEW.key_namespace
     LIMIT 1;

    IF other_namespace IS NOT NULL THEN
        RAISE EXCEPTION 'instrument_key % (chain_scope %) is already registered under namespace %, and this append claims %; one key means one instrument, and a position row carries no namespace to tell them apart',
            NEW.instrument_key, coalesce(NEW.chain_id, 0), other_namespace, NEW.key_namespace;
    END IF;
    RETURN NEW;
END $$;
COMMENT ON FUNCTION instrument_register_namespace_guard() IS 'BEFORE INSERT on instrument_register: refuses a key already registered under a different key_namespace. Reads chain_id rather than chain_scope because a STORED generated column is computed after this trigger. Serialised per natural key with pg_advisory_xact_lock, since two concurrent appends would otherwise both see no conflicting row and both land.';

-- The scheme vocabulary declares which node kinds it may alias; until now nothing read it, so an
-- LEI could point at a security. The kind comes from the id prefix, which sec_node_id_prefix_chk
-- makes a deterministic function of the id — the same trick sec_edge's endpoint CHECKs use.
CREATE FUNCTION alias_register_scheme_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
DECLARE
    node_kind text;
    legal     text[];
BEGIN
    node_kind := CASE
        WHEN NEW.node_id LIKE 'em-%'      THEN 'ENTITY'
        WHEN NEW.node_id LIKE 'sec-%'     THEN 'SECURITY'
        WHEN NEW.node_id LIKE 'concept-%' THEN 'CONCEPT'
        WHEN NEW.node_id LIKE 'src-%'     THEN 'SOURCE'
        WHEN NEW.node_id LIKE 'acct-%'    THEN 'ACCOUNT'
    END;

    SELECT v.applies_to INTO legal FROM id_scheme_vocabulary v WHERE v.id_scheme = NEW.id_scheme;

    -- node_kind NULL: an unknown prefix, which alias_register_node_prefix_chk owns.
    -- legal NULL: an unknown scheme, which the id_scheme foreign key owns (23503).
    IF node_kind IS NOT NULL AND legal IS NOT NULL AND NOT (node_kind = ANY (legal)) THEN
        RAISE EXCEPTION 'scheme % applies to %, so it cannot alias % (a % node) (ADR-0007 §1.3)',
            NEW.id_scheme, legal, NEW.node_id, node_kind;
    END IF;
    RETURN NEW;
END $$;
COMMENT ON FUNCTION alias_register_scheme_guard() IS 'BEFORE INSERT on alias_register: enforces id_scheme_vocabulary.applies_to against the node kind implied by node_id''s prefix. value_form stays prose and validator-owned — one value column serves every scheme, so its form is not a column CHECK.';

-- A second row on a window that already exists must say which row it closes. Without this the
-- two are indistinguishable from a race: they differ only in valid_to, so the key does not
-- collide, both land, and step one of the read — which groups on valid_from and ignores
-- valid_to — keeps whichever was inserted last. The displaced row never reaches the valid-time
-- filter, so once the survivor's window ends the key resolves to NOTHING while an open mapping
-- sits in the table. No DQ check can find it afterwards either: the _current views distinct on
-- the identity with security_id/node_id OUTSIDE that key, so they return one row by
-- construction and the discarded one leaves no trace (sec_edge_current keeps dst_id IN its
-- distinct key, which is what lets wave 1's equivalent rule fire).
--
-- AFTER INSERT because chain_scope is generated and is still NULL in a BEFORE trigger. The
-- advisory lock is the read-then-write rule (ADR-0002 §3): two concurrent appends would
-- otherwise each miss the other's uncommitted row.
--
-- NOT attached to sec_node or sec_edge, which have the same gap: wave 1 shipped the opposite
-- convention, where a close names nothing, and three of its tests assert it. Changing a merged
-- write contract is its own ticket, not this one's.
CREATE FUNCTION sec_register_supersession_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
DECLARE
    identity  jsonb;
    predicate text := '';
    col       text;
    coltype   text;
    clash     bigint;
    named     bigint;
BEGIN
    identity := (SELECT jsonb_object_agg(k, to_jsonb(NEW) -> k) FROM unnest(TG_ARGV) AS k);
    PERFORM pg_advisory_xact_lock(hashtext(TG_TABLE_NAME || ':' || identity::text));

    -- Cast the parameter to each column's own type rather than the column to text, so the
    -- equality stays indexable on the resolve index whose leading columns these are.
    FOREACH col IN ARRAY TG_ARGV LOOP
        SELECT atttypid::regtype::text INTO coltype
          FROM pg_attribute WHERE attrelid = TG_RELID AND attname = col;
        predicate := predicate || format(' AND t.%I = ($2->>%L)::%s', col, col, coltype);
    END LOOP;

    EXECUTE format('SELECT t.record_id FROM %I.%I t WHERE t.record_id <> $1%s LIMIT 1',
                   TG_TABLE_SCHEMA, TG_TABLE_NAME, predicate)
       INTO clash USING NEW.record_id, identity;

    IF clash IS NULL THEN
        RETURN NULL;
    END IF;

    IF NEW.supersedes_record_id IS NULL THEN
        RAISE EXCEPTION 'append to %.% lands on a window that record % already holds (%); a second row on one window must name the row it closes in supersedes_record_id, or the two differ only by insert order and the displaced one becomes unreachable',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, clash, identity;
    END IF;

    EXECUTE format('SELECT t.record_id FROM %I.%I t WHERE t.record_id = $1%s',
                   TG_TABLE_SCHEMA, TG_TABLE_NAME, predicate)
       INTO named USING NEW.supersedes_record_id, identity;

    IF named IS NULL THEN
        RAISE EXCEPTION 'append to %.% supersedes record %, which is not on the window it lands on (%); a close names the row it closes, not one on another window',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.supersedes_record_id, identity;
    END IF;
    RETURN NULL;
END $$;
COMMENT ON FUNCTION sec_register_supersession_guard() IS 'AFTER INSERT on the registers: a second append on one valid-time window must name the row it closes in supersedes_record_id, and that row must be on the same window. Identity columns come from TG_ARGV, so one function serves both registers. Without it two rows differing only in valid_to both land, the read keeps whichever was inserted last, and once its window ends the key resolves to nothing while an open mapping remains in the table — invisible to any check over the _current views, which return one row per identity by construction.';

CREATE TRIGGER instrument_register_append_guard BEFORE INSERT ON instrument_register
    FOR EACH ROW EXECUTE FUNCTION sec_store_append_guard();
CREATE TRIGGER instrument_register_namespace_guard BEFORE INSERT ON instrument_register
    FOR EACH ROW EXECUTE FUNCTION instrument_register_namespace_guard();
CREATE TRIGGER alias_register_append_guard BEFORE INSERT ON alias_register
    FOR EACH ROW EXECUTE FUNCTION sec_store_append_guard();
CREATE TRIGGER alias_register_scheme_guard BEFORE INSERT ON alias_register
    FOR EACH ROW EXECUTE FUNCTION alias_register_scheme_guard();

CREATE TRIGGER instrument_register_supersession_guard AFTER INSERT ON instrument_register
    FOR EACH ROW EXECUTE FUNCTION sec_register_supersession_guard('instrument_key', 'chain_scope', 'valid_from');
CREATE TRIGGER alias_register_supersession_guard AFTER INSERT ON alias_register
    FOR EACH ROW EXECUTE FUNCTION sec_register_supersession_guard('id_scheme', 'id_value', 'valid_from');

DO $$
DECLARE
    t text;
    owner_role text;
    owner_is_super boolean;
BEGIN
    -- Registers: full revoke, owner included (nothing FKs them), as sec_node / sec_edge.
    FOREACH t IN ARRAY ARRAY['instrument_register','alias_register'] LOOP
        SELECT pg_get_userbyid(c.relowner) INTO owner_role FROM pg_class c WHERE c.oid = t::regclass;
        SELECT rolsuper INTO owner_is_super FROM pg_roles WHERE rolname = owner_role;
        EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM %I', t, owner_role);
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM stl_readwrite', t);
        END IF;
        IF NOT owner_is_super AND has_table_privilege(owner_role, t, 'UPDATE') THEN
            RAISE EXCEPTION 'append-only not enforced: owner % still holds UPDATE on % after the revoke', owner_role, t;
        END IF;
    END LOOP;
    -- Vocabularies: app role fully revoked, owner keeps UPDATE because the FK integrity probe
    -- runs as the parent's owner (20260714_160000, #574); the trigger blocks real mutation.
    FOREACH t IN ARRAY ARRAY['key_namespace_vocabulary','id_scheme_vocabulary'] LOOP
        SELECT pg_get_userbyid(c.relowner) INTO owner_role FROM pg_class c WHERE c.oid = t::regclass;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM stl_readwrite', t);
        END IF;
        EXECUTE format('REVOKE DELETE, TRUNCATE ON %I FROM %I', t, owner_role);
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reference_table_immutable()',
                       t || '_immutable', t);
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260915_120000_secstore_registers_and_id_vocabularies.sql') ON CONFLICT (filename) DO NOTHING;
