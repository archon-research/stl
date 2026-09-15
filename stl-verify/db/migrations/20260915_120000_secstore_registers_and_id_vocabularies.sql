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
    attrs               jsonb NOT NULL DEFAULT '{}'::jsonb,
    valid_from          date NOT NULL,
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
    PRIMARY KEY (instrument_key, processing_version),
    CONSTRAINT instrument_register_record_id_key UNIQUE (record_id),
    CONSTRAINT instrument_register_security_id_prefix_chk CHECK (security_id LIKE 'sec-%'),
    CONSTRAINT instrument_register_key_nonblank_chk CHECK (instrument_key !~ '^\s*$'),
    -- position_key() renders the key between ';' delimiters, so a ';' inside one collapses two
    -- identities onto one position_id. A key this register accepts but position_id forks is worse
    -- than a rejected load.
    CONSTRAINT instrument_register_key_delim_chk CHECK (strpos(instrument_key, ';') = 0),
    CONSTRAINT instrument_register_chain_pos_chk CHECK (chain_id IS NULL OR chain_id > 0),
    CONSTRAINT instrument_register_valid_from_finite_chk CHECK (valid_from <> 'infinity' AND valid_from <> '-infinity')
);
COMMENT ON TABLE instrument_register IS '[Dimension] Native instrument key -> SECURITY node (ADR-0007 §2): the instrument is a KEY, not a node, so resolution is a lookup and no RESOLVES_TO edge type exists. Many keys resolve to one security — six USDC deployments are six rows pointing at one sec-usdc — which is why this is a register rather than a node kind. Append-only (full ACL revoke incl. owner; nothing FKs it), bitemporal. Successor of the frozen security_instrument_bridge. The hottest join in the metric path: position_state.instrument_key resolves here (seam 1, ADR-0007 §9.1) and nothing is stamped back onto the position. Plain table: governance-rate writes.';
COMMENT ON COLUMN instrument_register.instrument_key IS 'Roles: PK (with processing_version). The instrument''s NATIVE, globally-unique id, assigned by the chain or the source and never by us: contract address (lowercase hex, no 0x), protocol-emitted market id, registry:ilk, provider:package. position_id hashes this value (VEC-400), so no house classifier may appear in it and its text form is frozen once positions exist. key_namespace is deliberately NOT part of the key: a position row carries no namespace, so if one key could sit under two namespaces the read could not choose between them — leaving it out turns that collision into a PK violation at write instead.';
COMMENT ON COLUMN instrument_register.key_namespace IS 'Roles: FK→key_namespace_vocabulary.key_namespace. Which form the key takes. Descriptive, not part of identity (see instrument_key).';
COMMENT ON COLUMN instrument_register.security_id IS 'Roles: FK→sec_node.id (soft; SCD2 ids are non-unique, so resolve through sec_node_current). The SECURITY node this key means. NO foreign key BY DESIGN, and not merely because the target is non-unique: a register row must be writable before its security node exists, which is what lets the register load ahead of the security load (VEC-625/627). An unresolvable security_id is a DQ finding, not a write error.';
COMMENT ON COLUMN instrument_register.chain_id IS 'Roles: FK→chain.chain_id (soft). Native chain id, raw integer. NULL where the namespace has no chain BY CONSTRUCTION, not where one was missed: Anchorage packages are off-chain custody and Sky ilks come from a source that carries no chain column. Hardcoding 1 for those is the remembered-constant antipattern VEC-400''s helper warns against.';
COMMENT ON COLUMN instrument_register.attrs IS 'Instrument-level attributes that are not identity: symbol, decimals, venue, first-seen block. Decimals here are the token''s own scale, never applied to a quantity by this table.';
COMMENT ON COLUMN instrument_register.valid_from IS 'Valid-time window start, UTC date. A re-point is a new row with a later valid_from, never an UPDATE: MKR -> SKY leaves the MKR row in place and the SKY row becomes current. Grain is a day, as sec_node.valid_from.';
COMMENT ON COLUMN instrument_register.record_id IS 'Roles: Audit, UNIQUE. Per-append surrogate; what supersedes_record_id and a reproduction manifest point at (PR-2.1).';
COMMENT ON COLUMN instrument_register.processing_version IS 'Roles: Audit, PK component. Correction version, caller-assigned per ADR-0006 §3: 0 live, N per correction run. A re-point is a VALID-TIME change and stays at 0 — see sec_node.processing_version for what a correction at N then does to later loads.';
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
-- Resolution index: the reads sort (instrument_key, valid_from) ASC then processing_version DESC,
-- ingest_xid DESC, record_id DESC. Columns AND directions must match the whole key or the
-- DISTINCT ON degrades to a full scan plus sort on every resolution (as sec_node_resolve_idx).
CREATE INDEX instrument_register_resolve_idx ON instrument_register (instrument_key, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC);
CREATE INDEX instrument_register_security_idx ON instrument_register (security_id);

-- ---------------------------------------------------------------------------
-- Seam 2: public identifier / pipeline link / holder address -> any node.
-- ---------------------------------------------------------------------------

CREATE TABLE alias_register (
    id_scheme           text NOT NULL REFERENCES id_scheme_vocabulary(id_scheme),
    id_value            text NOT NULL,
    node_id             text NOT NULL,
    valid_from          date NOT NULL,
    valid_to            date,
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
    PRIMARY KEY (id_scheme, id_value, processing_version, valid_from),
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
    CONSTRAINT alias_register_hex_chk CHECK (
        id_scheme NOT IN ('CONTRACT_ADDRESS','BLOCKCHAIN_ADDRESS')
        OR id_value ~ '^[0-9a-f]+$'
    ),
    CONSTRAINT alias_register_value_nonblank_chk CHECK (id_value !~ '^\s*$'),
    CONSTRAINT alias_register_valid_chk CHECK (valid_to IS NULL OR valid_from <= valid_to),
    CONSTRAINT alias_register_valid_from_finite_chk CHECK (valid_from <> 'infinity' AND valid_from <> '-infinity')
);
COMMENT ON TABLE alias_register IS '[Dimension] Public identifier, pipeline link or holder address -> node (ADR-0007 §1.3). One book with a scheme column: LEI, ISIN, TICKER, contract and wallet addresses, prime and protocol ids all live here, told apart by id_scheme. Aliases are LOOKUPS, never ids and never hashed into a key, so a node stays reachable by any of its aliases without any one of them being load-bearing. Append-only (full ACL revoke incl. owner; nothing FKs it), bitemporal. Successor of entity_ref_codes and the holder_entity resolver, extended to securities; it is the holder-resolution path for positions (seam 2, ADR-0007 §9.2). Plain table: governance-rate writes.';
COMMENT ON COLUMN alias_register.id_scheme IS 'Roles: FK→id_scheme_vocabulary.id_scheme, PK component. Which identifier namespace id_value is drawn from.';
COMMENT ON COLUMN alias_register.id_value IS 'Roles: PK component. The identifier within the scheme, verbatim from its assigner — except addresses, which are normalised to lowercase hex with no 0x (CHECK) so this joins position_state.holder_id directly.';
COMMENT ON COLUMN alias_register.node_id IS 'Roles: FK→sec_node.id (soft; resolve through sec_node_current). Any node kind — the scheme''s applies_to says which are legal. No foreign key, for the reason instrument_register.security_id gives.';
COMMENT ON COLUMN alias_register.valid_from IS 'Roles: PK component. Valid-time window start, UTC date. An alias re-pointing is a versioned append, as an instrument re-point is.';
COMMENT ON COLUMN alias_register.valid_to IS 'Valid-time window end, exclusive; NULL = open.';
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
CREATE INDEX alias_register_resolve_idx ON alias_register (id_scheme, id_value, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC);
CREATE INDEX alias_register_node_idx ON alias_register (node_id);

-- ---------------------------------------------------------------------------
-- Reads: latest append per (logical record, valid_from) first, then the valid window.
-- The other order resurrects a superseded row (ADR-0006 §5; the wave-1 rule).
-- ---------------------------------------------------------------------------

CREATE VIEW instrument_register_current AS
WITH latest AS (
    SELECT DISTINCT ON (instrument_key, valid_from) *
    FROM instrument_register
    ORDER BY instrument_key, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
)
SELECT DISTINCT ON (instrument_key) *
FROM latest
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
ORDER BY instrument_key, valid_from DESC;
COMMENT ON VIEW instrument_register_current IS 'One current mapping per key — the unique-current guarantee dim_instrument carries and the metric path''s hottest join needs. Operational reads only: a calculation uses instrument_register_as_of(effective_at), and a replay instrument_register_as_of(effective_at, known_at), because resolving the register at now() while scanning last quarter''s positions applies today''s mapping to yesterday''s holding, silently (ADR-0007 §9.3).';

CREATE FUNCTION instrument_register_as_of(effective_at date)
RETURNS SETOF instrument_register LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (instrument_key, valid_from) *
        FROM instrument_register
        ORDER BY instrument_key, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (instrument_key) *
    FROM latest
    WHERE valid_from <= effective_at
    ORDER BY instrument_key, valid_from DESC
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
        SELECT DISTINCT ON (instrument_key, valid_from) *
        FROM known
        ORDER BY instrument_key, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (instrument_key) *
    FROM latest
    WHERE valid_from <= effective_at
    ORDER BY instrument_key, valid_from DESC
$$;
COMMENT ON FUNCTION instrument_register_as_of(date, pg_snapshot) IS 'Bitemporal instrument resolution: latest append visible in known_at per (key, valid_from), then the valid window over effective_at (RP-4.1, CR-3.6). Serves the exact replay a recorded snapshot pins.';

CREATE VIEW alias_register_current AS
WITH latest AS (
    SELECT DISTINCT ON (id_scheme, id_value, valid_from) *
    FROM alias_register
    ORDER BY id_scheme, id_value, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
)
SELECT DISTINCT ON (id_scheme, id_value) *
FROM latest
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
  AND (valid_to IS NULL OR (now() AT TIME ZONE 'utc')::date < valid_to)
ORDER BY id_scheme, id_value, valid_from DESC;
COMMENT ON VIEW alias_register_current IS 'Current alias -> node resolution, two-step as instrument_register_current. Operational reads only; calculations use alias_register_as_of(effective_at).';

CREATE FUNCTION alias_register_as_of(effective_at date)
RETURNS SETOF alias_register LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (id_scheme, id_value, valid_from) *
        FROM alias_register
        ORDER BY id_scheme, id_value, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (id_scheme, id_value) *
    FROM latest
    WHERE valid_from <= effective_at
      AND (valid_to IS NULL OR effective_at < valid_to)
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
      AND (valid_to IS NULL OR effective_at < valid_to)
    ORDER BY id_scheme, id_value, valid_from DESC
$$;
COMMENT ON FUNCTION alias_register_as_of(date, pg_snapshot) IS 'Bitemporal alias resolution; see instrument_register_as_of(date, pg_snapshot).';

-- ---------------------------------------------------------------------------
-- Vocabulary seeds: the four key namespaces and the eleven schemes ADR-0007 ratified.
-- ---------------------------------------------------------------------------

INSERT INTO key_namespace_vocabulary (key_namespace, description) VALUES
 ('token_address','token / aToken / debtToken / ERC-4626 vault / loan: the on-chain contract address, lowercase hex, no 0x'),
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
 ('TICKER','{SECURITY}','venue ticker; many-to-many over time', false,'Exchange ticker symbol. Not unique_current: a ticker is reassigned, and one security carries different tickers per venue'),
 ('CONTRACT_ADDRESS','{SECURITY,ENTITY}','hex lowercase, no 0x', true,'On-chain contract address'),
 ('BLOCKCHAIN_ADDRESS','{ENTITY,ACCOUNT}','hex lowercase, no 0x', true,'On-chain wallet / EOA address; the position holder-resolution path (seam 2)'),
 ('PIPELINE_PRIME_ID','{ENTITY,ACCOUNT}','prime.id, verbatim', true,'Our own prime registry id — the path the frozen holder_entity view took for PRIME holders'),
 ('PIPELINE_PROTOCOL_ID','{ENTITY}','protocol.id, verbatim', true,'Our own protocol registry id'),
 ('INTERNAL','{ENTITY,ACCOUNT,SOURCE}','house-internal code', true,'House-internal code with no external assigner')
ON CONFLICT (id_scheme) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Write boundary and append-only, on the wave-1 terms (VEC-617).
-- ---------------------------------------------------------------------------

-- sec_store_append_guard is reused rather than reimplemented: its edge_disc branch is keyed on
-- the column, which the registers do not carry, so what runs here is exactly the part that
-- applies — reject a writer-supplied ingest_xid, chain content_hash through supersedes_record_id,
-- verify a supplied hash rather than trust it.
CREATE TRIGGER instrument_register_append_guard BEFORE INSERT ON instrument_register
    FOR EACH ROW EXECUTE FUNCTION sec_store_append_guard();
CREATE TRIGGER alias_register_append_guard BEFORE INSERT ON alias_register
    FOR EACH ROW EXECUTE FUNCTION sec_store_append_guard();

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
