-- VEC-617: the combined master, wave 1 — the node store, the edge store, and their governed
-- vocabularies (ADR-0005, #652). Deliberately narrow: this wave ships only what the reference
-- taxonomy (20260904_120100) needs and what is stable enough to freeze in an immutable
-- migration. Deferred to their own waves, with the tickets that own them: the registers
-- (VEC-616), the shape rows and guard concepts (VEC-622, landing with the validator that
-- reads them), the curated entity/security load (VEC-625/627), the pivot read models
-- (VEC-619). Draft-maturity relationship types land when they ratify; this file seeds the
-- ratified set only.
--
-- Conventions carried from the frozen masters (VEC-410/411 migrations): append-only, soft
-- references between versioned stores (SCD2 ids are non-unique; resolve via the _current
-- views), no session-level SET search_path (VEC-411). Spine columns per ADR-0006:
-- processing_version is caller-assigned (0 live, N per correction run), ingest_xid is never
-- writer-supplied, ingested_at is a label only. run_id stays a bare bigint until the
-- writer_run FK lands with ADR-0006 §2 (#689 / VEC-598).
--
-- Append-only enforcement follows the two house patterns deliberately, per table class:
--   * sec_node / sec_edge: full ACL revoke including the owner (position_state pattern).
--     Nothing FKs these tables, so the owner-side revoke cannot break an RI probe.
--   * the vocabulary tables: they are FK parents, and the FK integrity probe
--     (SELECT ... FOR KEY SHARE, executed as the parent's OWNER) requires UPDATE on the
--     parent — so the owner keeps UPDATE and append-only is enforced by the
--     reference_table_immutable() trigger instead (20260714_160000, Simon's #574 finding).
--     Revoking owner UPDATE here would make every INSERT into sec_node/sec_edge fail with
--     "permission denied" under the prod roles while passing superuser CI.
--
-- Plain tables, not hypertables: every table here writes at governance rate (rows per day
-- at most), the sparse-table exception in db/migrations AGENTS.md. Each table COMMENT
-- restates the decision. The one type that would break that premise — a block-stamped
-- projection such as ALLOCATES — is draft and excluded from this wave; if it ratifies, it
-- lands in its own store with hypertable treatment, not in sec_edge.

-- ---------------------------------------------------------------------------
-- Vocabulary tables (governed lists; the ref_* bar: standards-anchored or
-- decided in ADR-0005, nothing debatable, seed-once)
-- ---------------------------------------------------------------------------

CREATE TABLE rel_type_vocabulary (
    rel_type            text PRIMARY KEY,
    family              text NOT NULL CHECK (family IN
                          ('composition','issuance_ownership_control','holding_allocation',
                           'classification_governance','identity_resolution','lifecycle')),
    src_kinds           text[] NOT NULL,
    dst_kinds           text[] NOT NULL,
    cardinality         text NOT NULL CHECK (cardinality IN ('1','n','1_per_class','1_per_parent')),
    weight_basis        text,
    derived_only        boolean NOT NULL DEFAULT false,
    maturity            text NOT NULL CHECK (maturity IN ('ratified','draft')),
    description         text NOT NULL,
    change_reason       text NOT NULL DEFAULT 'SEED_LOAD'
);
COMMENT ON TABLE rel_type_vocabulary IS '[Configuration] Governed relationship vocabulary (ADR-0005 §5). Adding or ratifying a type is a reviewed migration. Endpoint legality is the (rel_type, src_kind, dst_kind) triple, enforced by the loader/validator (cross-row; an FK cannot see the endpoint row). Plain table: governance-rate writes.';
COMMENT ON COLUMN rel_type_vocabulary.rel_type IS 'Roles: PK. The edge type name, UPPER_SNAKE.';
COMMENT ON COLUMN rel_type_vocabulary.family IS 'One of the six ADR-0005 §5 families.';
COMMENT ON COLUMN rel_type_vocabulary.src_kinds IS 'Legal source node kinds (sec_node.record_type values).';
COMMENT ON COLUMN rel_type_vocabulary.dst_kinds IS 'Legal destination node kinds.';
COMMENT ON COLUMN rel_type_vocabulary.cardinality IS 'Expected current-state cardinality; a DQ check over current state, never a write trigger (an open edge always time-overlaps its re-point).';
COMMENT ON COLUMN rel_type_vocabulary.weight_basis IS 'Roles: FK→weight_basis_vocabulary.basis (soft; declared basis for weighted types). NULL = unweighted type.';
COMMENT ON COLUMN rel_type_vocabulary.derived_only IS 'true: rows of this type are projections written by a loader with lineage, never curated by hand.';
COMMENT ON COLUMN rel_type_vocabulary.maturity IS 'ratified: decided and stable. draft types are not seeded; they land by migration when ratified.';
COMMENT ON COLUMN rel_type_vocabulary.description IS 'What the type means; the reviewed definition.';
COMMENT ON COLUMN rel_type_vocabulary.change_reason IS 'Roles: Audit. Why the row exists (vocabulary rows carry the slim spine; full provenance lives on nodes/edges).';

CREATE TABLE weight_basis_vocabulary (
    basis        text PRIMARY KEY,
    description  text NOT NULL
);
COMMENT ON TABLE weight_basis_vocabulary IS '[Configuration] Legal weight bases (ADR-0005 §3). Weights of unlike bases must never be summed; conversion ratios are edge payload, not weights. Plain table: seed-once.';
COMMENT ON COLUMN weight_basis_vocabulary.basis IS 'Roles: PK. Basis code (VALUE / NOTIONAL / UNITS / OWNERSHIP_PCT).';
COMMENT ON COLUMN weight_basis_vocabulary.description IS 'What the basis measures and where it is used.';

CREATE TABLE change_reason_vocabulary (
    code               text PRIMARY KEY,
    description        text NOT NULL,
    requires_approval  boolean NOT NULL DEFAULT false
);
COMMENT ON TABLE change_reason_vocabulary IS '[Configuration] Structured change_reason_code set (ADR-0005 §4, CR-3.3). Every node/edge append cites one. Plain table: seed-once, extended by reviewed migration.';
COMMENT ON COLUMN change_reason_vocabulary.code IS 'Roles: PK. Reason code, UPPER_SNAKE.';
COMMENT ON COLUMN change_reason_vocabulary.description IS 'When to use the code.';
COMMENT ON COLUMN change_reason_vocabulary.requires_approval IS 'true: an append citing this code must carry approved_by (validator-enforced; approval identity distinct from the appender).';

CREATE TABLE concept_class_vocabulary (
    concept_class text PRIMARY KEY,
    maturity      text NOT NULL CHECK (maturity IN ('ratified','draft')),
    seed_source   text,
    description   text NOT NULL
);
COMMENT ON TABLE concept_class_vocabulary IS '[Configuration] Concept classes: which kind of category a CONCEPT node is (sec_node.attrs.concept_class). Plain table: seed-once. The guard class ships with the shape system (VEC-622).';
COMMENT ON COLUMN concept_class_vocabulary.concept_class IS 'Roles: PK. Class name, lower_snake.';
COMMENT ON COLUMN concept_class_vocabulary.maturity IS 'ratified: carries shapes and governed memberships. draft: taxonomy exists, rules pending.';
COMMENT ON COLUMN concept_class_vocabulary.seed_source IS 'Which ref_* vocabulary seeds the class, where one does (the promotion path of 20260904_120100).';
COMMENT ON COLUMN concept_class_vocabulary.description IS 'What the class categorises.';

CREATE TABLE node_status_vocabulary (
    record_type  text NOT NULL,
    status       text NOT NULL,
    is_terminal  boolean NOT NULL,
    pairs_with   text,
    description  text NOT NULL,
    PRIMARY KEY (record_type, status)
);
COMMENT ON TABLE node_status_vocabulary IS '[Configuration] Per-kind node status vocabulary (ADR-0005 §2). A status change is a node version, never a mutation; terminal statuses retire nothing — history, edges and register rows remain readable. Plain table: seed-once.';
COMMENT ON COLUMN node_status_vocabulary.record_type IS 'Roles: PK (with status). The node kind the status applies to.';
COMMENT ON COLUMN node_status_vocabulary.status IS 'Roles: PK (with record_type). Status value, UPPER_SNAKE.';
COMMENT ON COLUMN node_status_vocabulary.is_terminal IS 'true: no further lifecycle expected; excluded from the active universe, history intact.';
COMMENT ON COLUMN node_status_vocabulary.pairs_with IS 'Roles: FK→rel_type_vocabulary.rel_type (soft). The edge type a transition into this status pairs with, where one is required (e.g. MERGED pairs with SUCCEEDED_BY).';
COMMENT ON COLUMN node_status_vocabulary.description IS 'When the status applies.';

-- ---------------------------------------------------------------------------
-- The combined master: nodes
-- ---------------------------------------------------------------------------

CREATE TABLE sec_node (
    id                  text NOT NULL,
    record_type         text NOT NULL CHECK (record_type IN ('ENTITY','SECURITY','CONCEPT','SOURCE','ACCOUNT')),
    chain_id            int4,
    status              text NOT NULL DEFAULT 'ACTIVE',
    attrs               jsonb NOT NULL DEFAULT '{}'::jsonb,
    valid_from          date NOT NULL,
    valid_to            date,
    record_id           bigint GENERATED ALWAYS AS IDENTITY,
    processing_version  integer NOT NULL DEFAULT 0 CHECK (processing_version >= 0),
    ingest_xid          xid8 NOT NULL DEFAULT pg_current_xact_id(),
    ingested_at         timestamptz NOT NULL DEFAULT now(),
    run_id              bigint,
    actor               text NOT NULL,
    change_reason_code  text NOT NULL REFERENCES change_reason_vocabulary(code),
    change_reason       text NOT NULL,
    approved_by         text,
    supersedes_record_id bigint,
    source_system       text NOT NULL,
    content_hash        bytea,
    PRIMARY KEY (id, processing_version, valid_from),
    CONSTRAINT sec_node_id_prefix_chk CHECK (
        (record_type = 'ENTITY'   AND id LIKE 'em-%')      OR
        (record_type = 'SECURITY' AND id LIKE 'sec-%')     OR
        (record_type = 'CONCEPT'  AND id LIKE 'concept-%') OR
        (record_type = 'SOURCE'   AND id LIKE 'src-%')     OR
        (record_type = 'ACCOUNT'  AND id LIKE 'acct-%')
    ),
    CONSTRAINT sec_node_valid_chk CHECK (valid_to IS NULL OR valid_from < valid_to)
);
COMMENT ON TABLE sec_node IS '[Dimension] Combined SECs master (ADR-0005 §2): one node per real-world thing, discriminated by record_type. Append-only (full ACL revoke incl. owner — nothing FKs this table), bitemporal (valid window + ingest_xid). The instrument is NOT a node kind: native keys resolve via the instrument register (VEC-616). Individuals carry a pseudonymous surrogate only; PII lives in a separate store (DP-1). Plain table: governance-rate writes, per the sparse-table exception.';
COMMENT ON COLUMN sec_node.id IS 'Roles: PK (with processing_version, valid_from). Opaque, kind-prefixed (em-/sec-/concept-/src-/acct-), house-assigned once, never derived from a public identifier or symbol, and never hashed into position_id. Seeded em-* ids stand unchanged.';
COMMENT ON COLUMN sec_node.record_type IS 'Node kind. ENTITY / SECURITY / CONCEPT / SOURCE live; ACCOUNT staged (ADR-0005 §2).';
COMMENT ON COLUMN sec_node.chain_id IS 'Roles: FK→chain.chain_id (soft). NULL for off-chain things.';
COMMENT ON COLUMN sec_node.status IS 'Roles: FK→node_status_vocabulary (composite with record_type). A status change is a new version.';
COMMENT ON COLUMN sec_node.attrs IS 'Kind-specific attributes as jsonb; the shape system (VEC-622) decides required-ness per type. Hot attributes promote to typed columns only on VEC-633 evidence.';
COMMENT ON COLUMN sec_node.valid_from IS 'Valid-time window start, UTC date, half-open [valid_from, valid_to).';
COMMENT ON COLUMN sec_node.valid_to IS 'Valid-time window end; NULL = open/current. Close-and-open on change.';
COMMENT ON COLUMN sec_node.record_id IS 'Roles: Audit. Per-append surrogate; what supersedes_record_id and lineage point at.';
COMMENT ON COLUMN sec_node.processing_version IS 'Roles: Audit. Correction version, caller-assigned per ADR-0006: 0 live, N per correction run via processing_version_log. Never a valid-time change.';
COMMENT ON COLUMN sec_node.ingest_xid IS 'Roles: Audit. Knowledge-time visibility key (ADR-0006 §5, pg_visible_in_snapshot). Never writer-supplied.';
COMMENT ON COLUMN sec_node.ingested_at IS 'Roles: Audit. Wall-clock label only; never the audit key (a row stamps at transaction start but becomes visible at commit).';
COMMENT ON COLUMN sec_node.run_id IS 'Roles: Audit. Writer run; FK to writer_run lands with ADR-0006 §2 (VEC-598).';
COMMENT ON COLUMN sec_node.actor IS 'Roles: Audit. Real, non-shared principal (human or service) that appended the row. Required.';
COMMENT ON COLUMN sec_node.change_reason_code IS 'Roles: FK→change_reason_vocabulary.code, Audit. Structured reason for the append.';
COMMENT ON COLUMN sec_node.change_reason IS 'Roles: Audit. Free-text reason; cites the source where change_reason_code = CURATED_SOURCE.';
COMMENT ON COLUMN sec_node.approved_by IS 'Roles: Audit. Approver, distinct from actor, where the reason code requires approval.';
COMMENT ON COLUMN sec_node.supersedes_record_id IS 'Roles: Audit. record_id this append corrects or retracts; the correction chain is walkable through it.';
COMMENT ON COLUMN sec_node.source_system IS 'Roles: Audit. Where the fact came from (registry, worksheet, port, loader).';
COMMENT ON COLUMN sec_node.content_hash IS 'Roles: Audit. Per-record content hash over the canonical stored form (AR-1.2); population is wired with the validator work (VEC-622), the column exists from row one so no migration is needed then.';
CREATE INDEX sec_node_type_idx ON sec_node (record_type, id, valid_from DESC, processing_version DESC);

-- ---------------------------------------------------------------------------
-- The relationship store: edges
-- ---------------------------------------------------------------------------

CREATE TABLE sec_edge (
    edge_id             text GENERATED ALWAYS AS
                          ('rel:' || rel_type || ':' || src_id || ':' || dst_id || ':' || edge_seq::text) STORED,
    edge_seq            integer NOT NULL DEFAULT 0 CHECK (edge_seq >= 0),
    src_id              text NOT NULL,
    src_kind            text NOT NULL,
    dst_id              text NOT NULL,
    dst_kind            text NOT NULL,
    rel_type            text NOT NULL REFERENCES rel_type_vocabulary(rel_type),
    rel_weight          numeric(30,18),
    weight_basis        text REFERENCES weight_basis_vocabulary(basis),
    weight_asof_block   bigint,
    payload             jsonb NOT NULL DEFAULT '{}'::jsonb,
    valid_from          date NOT NULL,
    valid_to            date,
    record_id           bigint GENERATED ALWAYS AS IDENTITY,
    processing_version  integer NOT NULL DEFAULT 0 CHECK (processing_version >= 0),
    ingest_xid          xid8 NOT NULL DEFAULT pg_current_xact_id(),
    ingested_at         timestamptz NOT NULL DEFAULT now(),
    run_id              bigint,
    actor               text NOT NULL,
    change_reason_code  text NOT NULL REFERENCES change_reason_vocabulary(code),
    change_reason       text NOT NULL,
    approved_by         text,
    supersedes_record_id bigint,
    source_system       text NOT NULL,
    content_hash        bytea,
    input_lineage       jsonb,
    PRIMARY KEY (rel_type, src_id, dst_id, edge_seq, processing_version, valid_from),
    CONSTRAINT sec_edge_weight_basis_chk CHECK (rel_weight IS NULL OR weight_basis IS NOT NULL),
    CONSTRAINT sec_edge_valid_chk CHECK (valid_to IS NULL OR valid_from < valid_to)
);
COMMENT ON TABLE sec_edge IS '[Dimension] Directed, typed, weighted relationship store (ADR-0005 §3/§5). Append-only (full ACL revoke incl. owner — nothing FKs this table); close-and-open; retraction is a tombstone append. Endpoint-kind legality vs rel_type_vocabulary is loader/validator-enforced (cross-row); single-valued cardinality is a DQ check over current state, never a write trigger. Inverses and closures are derived, never stored. Plain table: governance-rate writes — block-stamped projection types (ALLOCATES) are excluded by design and would need their own hypertable store if ratified.';
COMMENT ON COLUMN sec_edge.edge_id IS 'Roles: Derived. Generated human-readable identity; the PK is the (rel_type, src, dst, edge_seq, processing_version, valid_from) tuple.';
COMMENT ON COLUMN sec_edge.edge_seq IS 'Roles: PK component. DM-6 discriminator: deliberately duplicated edges (multi-typing, per-edge attribute clusters) coexist instead of superseding their twin.';
COMMENT ON COLUMN sec_edge.src_id IS 'Roles: FK→sec_node.id (soft; SCD2 ids non-unique — resolve via the current view). Edge source.';
COMMENT ON COLUMN sec_edge.src_kind IS 'Denormalised source kind; must agree with the source node''s record_type (validator check GQ-11).';
COMMENT ON COLUMN sec_edge.dst_id IS 'Roles: FK→sec_node.id (soft). Edge destination.';
COMMENT ON COLUMN sec_edge.dst_kind IS 'Denormalised destination kind; must agree with the destination node''s record_type.';
COMMENT ON COLUMN sec_edge.rel_type IS 'Roles: FK→rel_type_vocabulary.rel_type, PK component. The governed type.';
COMMENT ON COLUMN sec_edge.rel_weight IS 'Exact decimal numeric(30,18), never float (RP-4.4). Look-through = sum over paths of weight products within one basis. NULL on unweighted types; a NULL weight on a weighted walk is an error, never treated as 1.0.';
COMMENT ON COLUMN sec_edge.weight_basis IS 'Roles: FK→weight_basis_vocabulary.basis. Mandatory when rel_weight is present (CHECK).';
COMMENT ON COLUMN sec_edge.weight_asof_block IS 'Block number a market-derived weight was computed at. Raw chain block height. NULL for curated weights.';
COMMENT ON COLUMN sec_edge.payload IS 'Type-specific attribute cluster (ratio+event_date, agency+rating+outlook, lien seniority, role).';
COMMENT ON COLUMN sec_edge.valid_from IS 'Valid-time window start, UTC date, half-open.';
COMMENT ON COLUMN sec_edge.valid_to IS 'Valid-time window end; NULL = open. A re-point closes the current row and opens a new one in one write.';
COMMENT ON COLUMN sec_edge.record_id IS 'Roles: Audit. Per-append surrogate.';
COMMENT ON COLUMN sec_edge.processing_version IS 'Roles: Audit. Correction version, caller-assigned (ADR-0006); 0 live.';
COMMENT ON COLUMN sec_edge.ingest_xid IS 'Roles: Audit. Knowledge-time visibility key (ADR-0006 §5). Never writer-supplied.';
COMMENT ON COLUMN sec_edge.ingested_at IS 'Roles: Audit. Wall-clock label only.';
COMMENT ON COLUMN sec_edge.run_id IS 'Roles: Audit. Writer run; FK lands with ADR-0006 §2 (VEC-598).';
COMMENT ON COLUMN sec_edge.actor IS 'Roles: Audit. Appending principal. Required.';
COMMENT ON COLUMN sec_edge.change_reason_code IS 'Roles: FK→change_reason_vocabulary.code, Audit.';
COMMENT ON COLUMN sec_edge.change_reason IS 'Roles: Audit. Free-text reason.';
COMMENT ON COLUMN sec_edge.approved_by IS 'Roles: Audit. Approver where the reason code requires one.';
COMMENT ON COLUMN sec_edge.supersedes_record_id IS 'Roles: Audit. record_id this append corrects, re-points or retracts.';
COMMENT ON COLUMN sec_edge.source_system IS 'Roles: Audit. Where the edge came from.';
COMMENT ON COLUMN sec_edge.content_hash IS 'Roles: Audit. Content hash over canonical form; population wired with VEC-622.';
COMMENT ON COLUMN sec_edge.input_lineage IS 'Roles: Audit. For derived edges: source record ids (PR-2.3). NULL on curated edges.';
CREATE INDEX sec_edge_src_idx ON sec_edge (src_id, rel_type, valid_from DESC, processing_version DESC);
CREATE INDEX sec_edge_dst_idx ON sec_edge (dst_id, rel_type);

-- ---------------------------------------------------------------------------
-- Current views and as-of reads: two-step, always — latest version per logical
-- record FIRST, then the valid window (the other order resurrects superseded
-- rows). _current is for operational reads only; anything feeding a calculation
-- uses _as_of(effective_at) with an explicit recorded parameter (ADR-0006 §4).
-- ---------------------------------------------------------------------------

CREATE VIEW sec_node_current AS
WITH latest AS (
    SELECT DISTINCT ON (id, valid_from) *
    FROM sec_node
    ORDER BY id, valid_from, processing_version DESC
)
SELECT DISTINCT ON (id) *
FROM latest
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
  AND (valid_to IS NULL OR (now() AT TIME ZONE 'utc')::date < valid_to)
ORDER BY id, valid_from DESC;
COMMENT ON VIEW sec_node_current IS 'Operational reads only (two-step: latest processing_version first, valid window second). Calculations use sec_node_as_of(effective_at).';

CREATE FUNCTION sec_node_as_of(effective_at date)
RETURNS SETOF sec_node LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (id, valid_from) *
        FROM sec_node
        ORDER BY id, valid_from, processing_version DESC
    )
    SELECT DISTINCT ON (id) *
    FROM latest
    WHERE valid_from <= effective_at
      AND (valid_to IS NULL OR effective_at < valid_to)
    ORDER BY id, valid_from DESC
$$;
COMMENT ON FUNCTION sec_node_as_of(date) IS 'As-of node read; effective_at is an explicit recorded parameter, never now() (ADR-0006 §4).';

CREATE VIEW sec_edge_current AS
WITH latest AS (
    SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq, valid_from) *
    FROM sec_edge
    ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from, processing_version DESC
)
SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq) *
FROM latest
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
  AND (valid_to IS NULL OR (now() AT TIME ZONE 'utc')::date < valid_to)
ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from DESC;
COMMENT ON VIEW sec_edge_current IS 'Operational reads only. Calculations use sec_edge_as_of(effective_at).';

CREATE FUNCTION sec_edge_as_of(effective_at date)
RETURNS SETOF sec_edge LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq, valid_from) *
        FROM sec_edge
        ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from, processing_version DESC
    )
    SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq) *
    FROM latest
    WHERE valid_from <= effective_at
      AND (valid_to IS NULL OR effective_at < valid_to)
    ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from DESC
$$;
COMMENT ON FUNCTION sec_edge_as_of(date) IS 'As-of edge read; effective_at is an explicit recorded parameter, never now() (ADR-0006 §4).';

-- ---------------------------------------------------------------------------
-- Vocabulary seeds (the decided, stable content only)
-- ---------------------------------------------------------------------------

INSERT INTO weight_basis_vocabulary (basis, description) VALUES
 ('VALUE','share by USD value: look-through composition, allocations'),
 ('NOTIONAL','share by notional: index/benchmark membership'),
 ('UNITS','unit ratio'),
 ('OWNERSHIP_PCT','ownership fraction: corporate structure')
ON CONFLICT (basis) DO NOTHING;

INSERT INTO change_reason_vocabulary (code, description, requires_approval) VALUES
 ('SEED_LOAD','initial vocabulary/schema/data seed', false),
 ('PORT_FROM_STANDALONE','row ported from the frozen standalone masters', false),
 ('RULE_DERIVED','loader rule where the shape is known (e.g. receipt_token => receipt token)', false),
 ('CURATED_SOURCE','sourced judgment; the source is cited in change_reason', false),
 ('RECLASSIFICATION','a classification moved', true),
 ('REPOINT','an edge or register mapping re-pointed', true),
 ('VALID_TIME_AMEND','late-arriving or amended source data; valid window corrected', false),
 ('RESTATEMENT','an earlier record was wrong; supersedes_record_id set', true),
 ('RETRACTION','tombstone: the record should never have existed', true),
 ('CORPORATE_ACTION','status version + succession edge', false),
 ('DEDUP_SUPERSEDE','SAME_AS / SUPERSEDES outcome', true)
ON CONFLICT (code) DO NOTHING;

INSERT INTO concept_class_vocabulary (concept_class, maturity, seed_source, description) VALUES
 ('instrument_type','ratified','ref asset_class / security_type','top-level instrument classification; carries shapes'),
 ('instrument_subtype','ratified','ref security_subtype','subtype under instrument_type via NARROWER_THAN'),
 ('entity_type','draft','ref entity_type','legal form'),
 ('counterparty_role','draft','ref counterparty_role','role vocabulary'),
 ('sector','draft','ref sector (GICS)','issuer sector'),
 ('credit_rating','draft','ref credit_rating','RATED_BY targets'),
 ('jurisdiction','draft','ref country (ISO 3166)','DOMICILED_IN targets'),
 ('currency','draft','ref currency (ISO 4217)','DENOMINATED_IN / PEGGED_TO targets')
ON CONFLICT (concept_class) DO NOTHING;

-- The ratified relationship types only. Draft types (COLLATERALISED_BY, TRANCHE_OF,
-- REFERENCES, MANAGED_BY, RATED_BY, PEGGED_TO, SAME_AS, ...) land by migration when they
-- ratify, each with its first consumer — nothing speculative is frozen here.
INSERT INTO rel_type_vocabulary
 (rel_type, family, src_kinds, dst_kinds, cardinality, weight_basis, derived_only, maturity, description) VALUES
 ('HAS_UNDERLYING','composition','{SECURITY}','{SECURITY}','n','VALUE',false,'ratified','what a token or wrapper is built on; the look-through spine'),
 ('ISSUED_BY','issuance_ownership_control','{SECURITY}','{ENTITY}','1',NULL,false,'ratified','the issuer; replaces issuer_entity_id as authority'),
 ('SUBSIDIARY_OF','issuance_ownership_control','{ENTITY}','{ENTITY}','1_per_parent','OWNERSHIP_PCT',false,'ratified','legal parent; ultimate parent derived by walking, never stored'),
 ('AFFILIATE_OF','issuance_ownership_control','{ENTITY}','{ENTITY}','n',NULL,false,'ratified','related, not owned'),
 ('HELD_BY','holding_allocation','{SECURITY}','{ENTITY}','n',NULL,false,'ratified','holder of record where holding is a reference fact; balances stay in the timeseries'),
 ('BELONGS_TO','classification_governance','{SECURITY,ENTITY,ACCOUNT}','{CONCEPT}','1_per_class',NULL,false,'ratified','category membership'),
 ('NARROWER_THAN','classification_governance','{CONCEPT}','{CONCEPT}','1',NULL,false,'ratified','taxonomy hierarchy; shape inheritance path'),
 ('GOVERNED_BY','classification_governance','{ENTITY,ACCOUNT}','{CONCEPT}','n',NULL,false,'ratified','which rule set applies'),
 ('SCORED_BY','classification_governance','{CONCEPT}','{CONCEPT}','n',NULL,false,'ratified','concept-to-concept pivot: asset class -> risk model'),
 ('OWNED_BY','classification_governance','{CONCEPT}','{ENTITY}','1',NULL,false,'ratified','stewardship of a rule set'),
 ('SOURCED_FROM','classification_governance','{SECURITY,ENTITY,CONCEPT,SOURCE,ACCOUNT}','{SOURCE}','n',NULL,false,'ratified','feed provenance where lineage points at a source'),
 ('SUCCEEDED_BY','lifecycle','{SECURITY}','{SECURITY}','1',NULL,false,'ratified','merger / redenomination; old node -> MERGED; register re-points'),
 ('SPLIT_FROM','lifecycle','{SECURITY}','{SECURITY}','1',NULL,false,'ratified','split / reverse split; payload: ratio, ex_date')
ON CONFLICT (rel_type) DO NOTHING;

INSERT INTO node_status_vocabulary (record_type, status, is_terminal, pairs_with, description) VALUES
 ('SECURITY','ACTIVE',    false, NULL,           'live instrument-of-record'),
 ('SECURITY','SUSPENDED', false, NULL,           'trading halted / contract paused; expected to resume or resolve'),
 ('SECURITY','DELISTED',  false, NULL,           'no longer listed on its venue; may persist off-venue'),
 ('SECURITY','DEFAULTED', false, NULL,           'issuer default; may restructure, so not terminal'),
 ('SECURITY','MATURED',   true,  NULL,           'term instrument reached maturity'),
 ('SECURITY','REDEEMED',  true,  NULL,           'redeemed or called; payload carries the call details'),
 ('SECURITY','CONVERTED', true,  'CONVERTS_TO',  'converted into another security'),
 ('SECURITY','MERGED',    true,  'SUCCEEDED_BY', 'merged / redenominated; the register re-points to the successor'),
 ('SECURITY','EXPIRED',   true,  NULL,           'derivative or right lapsed unexercised'),
 ('SECURITY','RETIRED',   true,  NULL,           'wound down with no successor'),
 ('ENTITY','ACTIVE',         false, NULL,         'operating legal person / operator'),
 ('ENTITY','INACTIVE',       false, NULL,         'dormant per registry (GLEIF entity status INACTIVE)'),
 ('ENTITY','IN_LIQUIDATION', false, NULL,         'winding up in progress'),
 ('ENTITY','DISSOLVED',      true,  NULL,         'legally dissolved'),
 ('ENTITY','MERGED',         true,  NULL,         'absorbed into another entity; corporate-structure edges record where'),
 ('ENTITY','SUPERSEDED',     true,  'SUPERSEDES', 'deduplicated; the surviving node is the SUPERSEDES source'),
 ('CONCEPT','ACTIVE',     false, NULL,         'in the governed vocabulary; memberships allowed'),
 ('CONCEPT','DEPRECATED', false, NULL,         'no new memberships; existing ones stand'),
 ('CONCEPT','RETIRED',    true,  NULL,         'memberships must move; validator flags remaining ones'),
 ('CONCEPT','SUPERSEDED', true,  'SUPERSEDES', 'replaced by another concept'),
 ('SOURCE','ACTIVE',         false, NULL,         'licensed and feeding'),
 ('SOURCE','SUSPENDED',      false, NULL,         'paused, e.g. licence lapsed; exposability off'),
 ('SOURCE','DECOMMISSIONED', true,  NULL,         'feed ended; provenance references remain valid'),
 ('SOURCE','SUPERSEDED',     true,  'SUPERSEDES', 'replaced by another source'),
 ('ACCOUNT','ACTIVE', false, NULL, 'open book'),
 ('ACCOUNT','FROZEN', false, NULL, 'no movements permitted; still reportable'),
 ('ACCOUNT','CLOSED', true,  NULL, 'closed book; history remains')
ON CONFLICT (record_type, status) DO NOTHING;

-- pairs_with names two draft types (CONVERTS_TO, SUPERSEDES) not yet in the vocabulary:
-- deliberately a soft reference — the status rows are the stable record of the pairing,
-- and the edge types land when they ratify.

ALTER TABLE sec_node ADD CONSTRAINT sec_node_status_fkey
    FOREIGN KEY (record_type, status) REFERENCES node_status_vocabulary (record_type, status);

-- ---------------------------------------------------------------------------
-- Append-only enforcement, by table class (see header).
-- sec_node / sec_edge: full revoke including the owner (position_state pattern;
-- nothing FKs them). Vocabulary tables: FK parents — the RI probe runs as the
-- owner and needs UPDATE (20260714_160000), so the owner keeps UPDATE and the
-- reference_table_immutable() trigger enforces append-only instead.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION reference_table_immutable() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'reference table %.% is append-only; % is not allowed (add rows via a migration)',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END $$;
COMMENT ON FUNCTION reference_table_immutable() IS 'Raises on UPDATE/DELETE of a controlled-vocabulary reference table. Paired with owner UPDATE restored so the FK RI row-lock probe still works (see 20260714_160000_fix_reference_table_fk_inserts.sql).';

DO $$
DECLARE r text; t text;
BEGIN
    -- Stores: full revoke, owner included.
    FOREACH r IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            FOREACH t IN ARRAY ARRAY['sec_node','sec_edge'] LOOP
                EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM %I', t, r);
            END LOOP;
        END IF;
    END LOOP;
    -- Vocabulary tables: app role fully revoked; owner keeps UPDATE for the RI probe,
    -- DELETE/TRUNCATE revoked; the trigger below blocks real mutation.
    FOREACH t IN ARRAY ARRAY['rel_type_vocabulary','weight_basis_vocabulary',
                             'change_reason_vocabulary','concept_class_vocabulary',
                             'node_status_vocabulary'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM stl_readwrite', t);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_migrator') THEN
            EXECUTE format('REVOKE DELETE, TRUNCATE ON %I FROM stl_migrator', t);
        END IF;
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reference_table_immutable()',
                       t || '_immutable', t);
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260904_120000_secstore_node_edge_stores_and_vocabularies.sql') ON CONFLICT (filename) DO NOTHING;
