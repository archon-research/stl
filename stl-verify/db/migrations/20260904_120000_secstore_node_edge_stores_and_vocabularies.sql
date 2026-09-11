-- VEC-617: combined master wave 1 — sec_node, sec_edge and their governed vocabularies
-- (ADR-0007/#652; spine columns and supersession order are ADR-0006 §2/§5). Per-table decisions
-- sit in each table's COMMENT; scope, deferrals and the append-only/ACL rationale are in the PR.

-- ---------------------------------------------------------------------------
-- Vocabulary tables: governed lists at the ref_* bar (ADR-0007), seeded once.
-- ---------------------------------------------------------------------------

CREATE TABLE weight_basis_vocabulary (
    basis        text PRIMARY KEY,
    description  text NOT NULL,
    run_id       bigint REFERENCES writer_run(id)
);
COMMENT ON TABLE weight_basis_vocabulary IS '[Configuration] Legal weight bases (ADR-0007 §3): three, each a share of a whole. Weights of unlike bases must never be summed; a conversion ratio is edge payload, not a weight (see the basis column). Plain table: seed-once, extended by reviewed migration.';
COMMENT ON COLUMN weight_basis_vocabulary.basis IS 'Roles: PK. Basis code (VALUE / NOTIONAL / OWNERSHIP_PCT). Each names a SHARE OF A WHOLE, which is what makes weights along a path multiplicable and weights under one basis summable. A conversion ratio is not a share: ADR-0007 §3 puts ratios in the edge payload.';
COMMENT ON COLUMN weight_basis_vocabulary.description IS 'What the basis measures and where it is used.';
COMMENT ON COLUMN weight_basis_vocabulary.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2); resolves to the build artefact through writer_run.build_id. NULL means written before run tracking — which is what the rows seeded here are.';

-- Declared before rel_type_vocabulary so its weight_basis is a real FK, not a soft one.
CREATE TABLE rel_type_vocabulary (
    rel_type            text PRIMARY KEY,
    family              text NOT NULL CHECK (family IN
                          ('composition','issuance_ownership_control','holding_allocation',
                           'classification_governance','identity_resolution','lifecycle')),
    src_kinds           text[] NOT NULL,
    dst_kinds           text[] NOT NULL,
    cardinality         text NOT NULL CHECK (cardinality IN ('1','n','1_per_class','1_per_parent')),
    weight_basis        text REFERENCES weight_basis_vocabulary(basis),
    derived_only        boolean NOT NULL DEFAULT false,
    maturity            text NOT NULL CHECK (maturity IN ('ratified','draft')),
    description         text NOT NULL,
    change_reason       text NOT NULL DEFAULT 'SEED_LOAD',
    run_id              bigint REFERENCES writer_run(id)
);
COMMENT ON TABLE rel_type_vocabulary IS '[Configuration] Governed relationship vocabulary (ADR-0007 §5). Adding or ratifying a type is a reviewed migration. Endpoint legality is the (rel_type, src_kind, dst_kind) triple, enforced by the loader/validator (cross-row; an FK cannot see the endpoint row). Plain table: governance-rate writes.';
COMMENT ON COLUMN rel_type_vocabulary.rel_type IS 'Roles: PK. The edge type name, UPPER_SNAKE.';
COMMENT ON COLUMN rel_type_vocabulary.family IS 'One of the six ADR-0007 §5 families.';
COMMENT ON COLUMN rel_type_vocabulary.src_kinds IS 'Legal source node kinds (sec_node.record_type values).';
COMMENT ON COLUMN rel_type_vocabulary.dst_kinds IS 'Legal destination node kinds.';
COMMENT ON COLUMN rel_type_vocabulary.cardinality IS 'Expected current-state cardinality; a DQ check over current state, never a write trigger (an open edge always time-overlaps its re-point).';
COMMENT ON COLUMN rel_type_vocabulary.weight_basis IS 'Roles: FK→weight_basis_vocabulary.basis. The declared basis for weighted types; NULL = unweighted type.';
COMMENT ON COLUMN rel_type_vocabulary.derived_only IS 'true: rows of this type are projections written by a loader with lineage, never curated by hand.';
COMMENT ON COLUMN rel_type_vocabulary.maturity IS 'ratified: decided and stable. draft types are not seeded; they land by migration when ratified.';
COMMENT ON COLUMN rel_type_vocabulary.description IS 'What the type means; the reviewed definition.';
COMMENT ON COLUMN rel_type_vocabulary.change_reason IS 'Roles: Audit. Why the row exists (vocabulary rows carry the slim spine; full provenance lives on nodes/edges).';
COMMENT ON COLUMN rel_type_vocabulary.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2); resolves to the build artefact through writer_run.build_id. NULL means written before run tracking — which is what the rows seeded here are.';

CREATE TABLE change_reason_vocabulary (
    code               text PRIMARY KEY,
    description        text NOT NULL,
    requires_approval  boolean NOT NULL DEFAULT false,
    run_id             bigint REFERENCES writer_run(id)
);
COMMENT ON TABLE change_reason_vocabulary IS '[Configuration] Structured change_reason_code set (ADR-0007 §4, CR-3.3). Every node/edge append cites one. Plain table: seed-once, extended by reviewed migration.';
COMMENT ON COLUMN change_reason_vocabulary.code IS 'Roles: PK. Reason code, UPPER_SNAKE.';
COMMENT ON COLUMN change_reason_vocabulary.description IS 'When to use the code.';
COMMENT ON COLUMN change_reason_vocabulary.requires_approval IS 'true: an append citing this code must carry approved_by (validator-enforced; approval identity distinct from the appender).';
COMMENT ON COLUMN change_reason_vocabulary.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2); resolves to the build artefact through writer_run.build_id. NULL means written before run tracking — which is what the rows seeded here are.';

CREATE TABLE concept_class_vocabulary (
    concept_class text PRIMARY KEY,
    maturity      text NOT NULL CHECK (maturity IN ('ratified','draft')),
    seed_source   text,
    description   text NOT NULL,
    run_id        bigint REFERENCES writer_run(id)
);
COMMENT ON TABLE concept_class_vocabulary IS '[Configuration] Concept classes: which kind of category a CONCEPT node is (sec_node.attrs.concept_class). Plain table: seed-once. The guard class ships with the shape system (VEC-622).';
COMMENT ON COLUMN concept_class_vocabulary.concept_class IS 'Roles: PK. Class name, lower_snake.';
COMMENT ON COLUMN concept_class_vocabulary.maturity IS 'ratified: carries shapes and governed memberships. draft: taxonomy exists, rules pending.';
COMMENT ON COLUMN concept_class_vocabulary.seed_source IS 'Which ref_* vocabulary seeds the class, where one does (the promotion path of 20260904_120100).';
COMMENT ON COLUMN concept_class_vocabulary.description IS 'What the class categorises.';
COMMENT ON COLUMN concept_class_vocabulary.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2); resolves to the build artefact through writer_run.build_id. NULL means written before run tracking — which is what the rows seeded here are.';

CREATE TABLE node_status_vocabulary (
    record_type  text NOT NULL,
    status       text NOT NULL,
    is_terminal  boolean NOT NULL,
    pairs_with   text,
    description  text NOT NULL,
    run_id       bigint REFERENCES writer_run(id),
    PRIMARY KEY (record_type, status)
);
COMMENT ON TABLE node_status_vocabulary IS '[Configuration] Per-kind node status vocabulary (ADR-0007 §2). A status change is a node version, never a mutation; terminal statuses retire nothing — history, edges and register rows remain readable. Plain table: seed-once.';
COMMENT ON COLUMN node_status_vocabulary.record_type IS 'Roles: PK (with status). The node kind the status applies to.';
COMMENT ON COLUMN node_status_vocabulary.status IS 'Roles: PK (with record_type). Status value, UPPER_SNAKE.';
COMMENT ON COLUMN node_status_vocabulary.is_terminal IS 'true: no further lifecycle expected; excluded from the active universe, history intact.';
COMMENT ON COLUMN node_status_vocabulary.pairs_with IS 'Roles: FK→rel_type_vocabulary.rel_type (soft). The edge type a transition into this status pairs with, where one is required (e.g. MERGED pairs with SUCCEEDED_BY).';
COMMENT ON COLUMN node_status_vocabulary.description IS 'When the status applies.';
COMMENT ON COLUMN node_status_vocabulary.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2); resolves to the build artefact through writer_run.build_id. NULL means written before run tracking — which is what the rows seeded here are.';

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
    PRIMARY KEY (id, processing_version, valid_from, valid_to),
    CONSTRAINT sec_node_record_id_key UNIQUE (record_id),
    -- The prefix is a GOVERNED part of the id contract: this CHECK, sec_edge's endpoint-kind
    -- CHECKs and the kind-scoped read's pushdown all derive record_type from it, so a new kind
    -- needs a prefix unique across all three (VEC-632 carries the ADR-0007 §1.1 correction).
    CONSTRAINT sec_node_id_prefix_chk CHECK (
        (record_type = 'ENTITY'   AND id LIKE 'em-%')      OR
        (record_type = 'SECURITY' AND id LIKE 'sec-%')     OR
        (record_type = 'CONCEPT'  AND id LIKE 'concept-%') OR
        (record_type = 'SOURCE'   AND id LIKE 'src-%')     OR
        (record_type = 'ACCOUNT'  AND id LIKE 'acct-%')
    ),
    CONSTRAINT sec_node_valid_chk CHECK (valid_from <= valid_to),
    -- 'infinity' is the open-END sentinel only. Every read tests valid_from <= effective_at, so
    -- a window starting at either infinity is unreachable by any as-of date.
    CONSTRAINT sec_node_valid_from_finite_chk CHECK (valid_from <> 'infinity' AND valid_from <> '-infinity')
);
COMMENT ON TABLE sec_node IS '[Dimension] Combined SECs master (ADR-0007 §2): one node per real-world thing, discriminated by record_type. Append-only (full ACL revoke incl. owner — nothing FKs this table), bitemporal (valid window + ingest_xid). valid_to is NOT NULL (''infinity'' when open) and in the PK, so close-and-open is an append at processing_version 0; a zero-length window is a retraction tombstone. The instrument is NOT a node kind: native keys resolve via the instrument register (VEC-616). Individuals carry a pseudonymous surrogate only; PII lives in a separate store (DP-1). Plain table: governance-rate writes, per the sparse-table exception.';
COMMENT ON COLUMN sec_node.id IS 'Roles: PK (with processing_version, valid_from). Opaque, kind-prefixed (em-/sec-/concept-/src-/acct-), house-assigned once, never derived from a public identifier or symbol, and never hashed into position_id. Seeded em-* ids stand unchanged.';
COMMENT ON COLUMN sec_node.record_type IS 'Node kind. ENTITY / SECURITY / CONCEPT / SOURCE live; ACCOUNT staged (ADR-0007 §2).';
COMMENT ON COLUMN sec_node.chain_id IS 'Roles: FK→chain.chain_id (soft). NULL for off-chain things.';
COMMENT ON COLUMN sec_node.status IS 'Roles: FK→node_status_vocabulary (composite with record_type). A status change is a new version.';
COMMENT ON COLUMN sec_node.attrs IS 'Kind-specific attributes as jsonb; the shape system (VEC-622) decides required-ness per type. Hot attributes promote to typed columns only on VEC-633 evidence.';
COMMENT ON COLUMN sec_node.valid_from IS 'Roles: PK (with id, processing_version, valid_to). Valid-time window start, UTC date, half-open [valid_from, valid_to). GRAIN IS A DAY, so two changes to one record on the same day are not both representable: both windows are [D, D+1), resolution picks one by processing_version then ingest_xid, and the other is unreachable by any as-of date even though it was true for part of D. Accepted for curated data at governance cadence; VEC-632 records the constraint in ADR-0007 §3.';
COMMENT ON COLUMN sec_node.valid_to IS 'Roles: PK (with id, processing_version, valid_from). Valid-time window end, exclusive; ''infinity'' = open/current, never NULL. In the key so close-and-open is an ordinary append at processing_version 0. A ZERO-LENGTH window (valid_to = valid_from) is a TOMBSTONE: it matches no as-of date, so THAT WINDOW drops out of the resolved reads with its history intact (ADR-0007 §3 retraction; pair it with change_reason_code RETRACTION and supersedes_record_id). It withdraws one window, not the logical record — a closed-and-reopened record takes one tombstone per window, and single-append record withdrawal is VEC-622''s.';
COMMENT ON COLUMN sec_node.record_id IS 'Roles: Audit, UNIQUE. Per-append surrogate; what supersedes_record_id, a retraction and a reproduction manifest point at (PR-2.1). Unique per store, not globally: a manifest cites (table, record_id).';
COMMENT ON COLUMN sec_node.processing_version IS 'Roles: Audit, PK component. Correction version, caller-assigned per ADR-0006 §3: 0 live, N per correction run via processing_version_log. A valid-time change (close-and-open, an ended window, a tombstone) is NOT a correction and stays at 0 — valid_to carries it. Un-retracting a tombstoned record IS a correction run at N. CONSEQUENCE of the resolution order (processing_version before ingest_xid): once a window has been corrected at N, a later ordinary append at 0 for that same (id, valid_from) never wins its group, whatever its ingest_xid, and with no error — a curator''s correction is not silently undone by the next pipeline run, and moving that window again takes another correction run. If a load appears to do nothing, this is why.';
COMMENT ON COLUMN sec_node.ingest_xid IS 'Roles: Audit. Knowledge-time visibility key (ADR-0006 §5, pg_visible_in_snapshot) and the supersession tiebreak inside a valid window. Never writer-supplied: the sec_node_append_guard trigger rejects an insert that sets it to anything but the current transaction id. xid8 is 64-bit, so no wraparound — but the values are CLUSTER-LOCAL: pg_dump/restore, logical replication and a major-version upgrade do not preserve them, so a snapshot a manifest recorded stops resolving against the restored cluster. The model-level contract is a total, commit-consistent, writer-unforgeable ordering key; xid8 + pg_visible_in_snapshot is the Postgres realization of it (VEC-632 records that distinction, and what a restore does to existing manifests).';
COMMENT ON COLUMN sec_node.ingested_at IS 'Roles: Audit. Wall-clock label only; never the audit key (a row stamps at transaction start but becomes visible at commit).';
COMMENT ON COLUMN sec_node.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2); resolves to the build artefact through writer_run.build_id, and to the reference data the writer saw through writer_run.reference_snapshot / reference_effective_at. NULL means written before run tracking — which is what the seeded rows are.';
COMMENT ON COLUMN sec_node.actor IS 'Roles: Audit. Real, non-shared principal (human or service) that appended the row. Required.';
COMMENT ON COLUMN sec_node.change_reason_code IS 'Roles: FK→change_reason_vocabulary.code, Audit. Structured reason for the append.';
COMMENT ON COLUMN sec_node.change_reason IS 'Roles: Audit. Free-text reason; cites the source where change_reason_code = CURATED_SOURCE.';
COMMENT ON COLUMN sec_node.approved_by IS 'Roles: Audit. Approver, distinct from actor, where the reason code requires approval.';
COMMENT ON COLUMN sec_node.supersedes_record_id IS 'Roles: FK-shaped→sec_node.record_id (enforced by sec_node_append_guard, which needs the predecessor''s content_hash to chain this row''s hash; an unresolvable pointer is rejected), Audit. record_id this append corrects or retracts; the correction chain is walkable through it, and content_hash binds this row to the exact content it supersedes. The resolved reads do not consult it — supersession within a window is decided by processing_version then ingest_xid, and withdrawal by the zero-length tombstone window.';
COMMENT ON COLUMN sec_node.source_system IS 'Roles: Audit. Where the fact came from (registry, worksheet, port, loader).';
COMMENT ON COLUMN sec_node.content_hash IS 'Roles: Audit, Derived. sha256 over the canonical stored form — to_jsonb(row) minus record_id, ingest_xid, ingested_at and content_hash, with supersedes_record_id replaced by the predecessor''s content_hash — computed by the sec_node_append_guard trigger on every insert, so the chain runs from the first append (AR-1.2, NFR-5). Chaining on content rather than on a row number keeps it reproducible from a Postgres export, including one that reassigns record_ids. It is NOT portable across realizations: to_jsonb is a Postgres serialization (numeric trailing zeros, jsonb key order, date and xid8 rendering), so a canonical form the model defines is what the ADR''s round-trip criterion actually needs — VEC-632. A supplied value is verified against the computed one and rejected if it differs.';
-- Resolution index: the reads below sort (id, valid_from) ASC then processing_version DESC,
-- ingest_xid DESC, record_id DESC. Columns AND directions have to match the whole key or the
-- DISTINCT ON degrades to a full scan plus sort on every current read (VEC-633 measures this).
CREATE INDEX sec_node_resolve_idx ON sec_node (id, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC);
-- Same rule as sec_node_resolve_idx: the kind-scoped read sorts (id, valid_from) ASC then
-- processing_version DESC, ingest_xid DESC, record_id DESC, so this key must match through its
-- whole length or the read pays an inner sort (measurements in the PR).
CREATE INDEX sec_node_type_idx ON sec_node (record_type, id, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC);

-- ---------------------------------------------------------------------------
-- The relationship store: edges
-- ---------------------------------------------------------------------------

CREATE TABLE sec_edge (
    edge_id             text GENERATED ALWAYS AS
                          ('rel:' || rel_type || ':' || src_id || ':' || dst_id || ':' || edge_seq::text) STORED,
    edge_seq            integer NOT NULL DEFAULT 1 CHECK (edge_seq >= 1),
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
    input_lineage       jsonb,
    PRIMARY KEY (rel_type, src_id, dst_id, edge_seq, processing_version, valid_from, valid_to),
    CONSTRAINT sec_edge_record_id_key UNIQUE (record_id),
    CONSTRAINT sec_edge_weight_basis_chk CHECK (rel_weight IS NULL OR weight_basis IS NOT NULL),
    -- The cheap half of GQ-11 at the engine boundary: sec_node_id_prefix_chk makes record_type a
    -- deterministic function of the id prefix, so a declared kind contradicting its own endpoint id
    -- is single-row checkable. Endpoint EXISTENCE is cross-row and stays with the validator.
    CONSTRAINT sec_edge_src_kind_chk CHECK (
        (src_kind = 'ENTITY'   AND src_id LIKE 'em-%')      OR
        (src_kind = 'SECURITY' AND src_id LIKE 'sec-%')     OR
        (src_kind = 'CONCEPT'  AND src_id LIKE 'concept-%') OR
        (src_kind = 'SOURCE'   AND src_id LIKE 'src-%')     OR
        (src_kind = 'ACCOUNT'  AND src_id LIKE 'acct-%')
    ),
    CONSTRAINT sec_edge_dst_kind_chk CHECK (
        (dst_kind = 'ENTITY'   AND dst_id LIKE 'em-%')      OR
        (dst_kind = 'SECURITY' AND dst_id LIKE 'sec-%')     OR
        (dst_kind = 'CONCEPT'  AND dst_id LIKE 'concept-%') OR
        (dst_kind = 'SOURCE'   AND dst_id LIKE 'src-%')     OR
        (dst_kind = 'ACCOUNT'  AND dst_id LIKE 'acct-%')
    ),
    CONSTRAINT sec_edge_valid_chk CHECK (valid_from <= valid_to),
    CONSTRAINT sec_edge_valid_from_finite_chk CHECK (valid_from <> 'infinity' AND valid_from <> '-infinity')
);
COMMENT ON TABLE sec_edge IS '[Dimension] Directed, typed, weighted relationship store (ADR-0007 §3/§5). Append-only (full ACL revoke incl. owner — nothing FKs this table); close-and-open at processing_version 0 (valid_to is NOT NULL, ''infinity'' when open, and in the PK); retraction is a tombstone append with a zero-length window. Endpoint-kind legality vs rel_type_vocabulary is loader/validator-enforced (cross-row); single-valued cardinality is a DQ check over current state, never a write trigger. Inverses and closures are derived, never stored. Plain table: governance-rate writes — block-stamped projection types (ALLOCATES) are excluded by design and would need their own hypertable store if ratified.';
COMMENT ON COLUMN sec_edge.edge_id IS 'Roles: Derived. Generated human-readable identity of the LOGICAL edge; the PK is the seven-column (rel_type, src_id, dst_id, edge_seq, processing_version, valid_from, valid_to) tuple, so one edge_id spans every version and window of that edge.';
COMMENT ON COLUMN sec_edge.edge_seq IS 'Roles: PK component. DM-6 discriminator: deliberately duplicated edges (multi-typing, per-edge attribute clusters) coexist instead of superseding their twin. Base is 1 per ADR-0007 §3, so a twin is 2; 0 is rejected rather than left as a second spelling of the base edge, since edge_seq is rendered into the stored edge_id. The seq sits inside the row''s identity and is rendered into edge_id, so allocation is a read-then-write on current state: a writer takes pg_advisory_xact_lock on (rel_type, src_id, dst_id) per the read-then-write rule in db/migrations AGENTS.md, and a replay CARRIES the seq from its source, since a recomputed seq changes edge_id and every content_hash chained from it. VEC-622 owns the loader that allocates it.';
COMMENT ON COLUMN sec_edge.src_id IS 'Roles: FK→sec_node.id (soft; SCD2 ids non-unique — resolve via the current view). Edge source.';
COMMENT ON COLUMN sec_edge.src_kind IS 'Denormalised source kind, CHECKed to agree with src_id''s own prefix (so ''em-…'' cannot be declared SECURITY). That the endpoint EXISTS as a current node is cross-row and stays validator-enforced (GQ-11).';
COMMENT ON COLUMN sec_edge.dst_id IS 'Roles: FK→sec_node.id (soft). Edge destination.';
COMMENT ON COLUMN sec_edge.dst_kind IS 'Denormalised destination kind, CHECKed to agree with dst_id''s own prefix. Endpoint existence stays validator-enforced (GQ-11).';
COMMENT ON COLUMN sec_edge.rel_type IS 'Roles: FK→rel_type_vocabulary.rel_type, PK component. The governed type.';
COMMENT ON COLUMN sec_edge.rel_weight IS 'Exact decimal numeric(30,18), never float (RP-4.4). Look-through = sum over paths of weight products within one basis. NULL on unweighted types; a NULL weight on a weighted walk is an error, never treated as 1.0.';
COMMENT ON COLUMN sec_edge.weight_basis IS 'Roles: FK→weight_basis_vocabulary.basis. Mandatory when rel_weight is present (CHECK).';
COMMENT ON COLUMN sec_edge.weight_asof_block IS 'Block number a market-derived weight was computed at. Raw chain block height. NULL for curated weights.';
COMMENT ON COLUMN sec_edge.payload IS 'Type-specific attribute cluster (ratio+event_date, agency+rating+outlook, lien seniority, role).';
COMMENT ON COLUMN sec_edge.valid_from IS 'Roles: PK component. Valid-time window start, UTC date, half-open.';
COMMENT ON COLUMN sec_edge.valid_to IS 'Roles: PK component. Valid-time window end, exclusive; ''infinity'' = open, never NULL. In the key so a re-point closes the current row and opens the new one in one write, both at processing_version 0. A ZERO-LENGTH window (valid_to = valid_from) is a TOMBSTONE — the only way to retract an edge, since an edge has no status to retire it — and it withdraws that WINDOW, so a re-pointed edge takes one tombstone per window (see sec_node.valid_to).';
COMMENT ON COLUMN sec_edge.record_id IS 'Roles: Audit, UNIQUE. Per-append surrogate; the target of supersedes_record_id, retractions and manifests (PR-2.1).';
COMMENT ON COLUMN sec_edge.processing_version IS 'Roles: Audit, PK component. Correction version, caller-assigned (ADR-0006 §3); 0 live. Close-and-open, an ended link and a tombstone all stay at 0.';
COMMENT ON COLUMN sec_edge.ingest_xid IS 'Roles: Audit. Knowledge-time visibility key (ADR-0006 §5) and the supersession tiebreak inside a valid window. Never writer-supplied; enforced by sec_edge_append_guard.';
COMMENT ON COLUMN sec_edge.ingested_at IS 'Roles: Audit. Wall-clock label only.';
COMMENT ON COLUMN sec_edge.run_id IS 'Roles: FK→writer_run.id, Audit. See sec_node.run_id.';
COMMENT ON COLUMN sec_edge.actor IS 'Roles: Audit. Appending principal. Required.';
COMMENT ON COLUMN sec_edge.change_reason_code IS 'Roles: FK→change_reason_vocabulary.code, Audit.';
COMMENT ON COLUMN sec_edge.change_reason IS 'Roles: Audit. Free-text reason.';
COMMENT ON COLUMN sec_edge.approved_by IS 'Roles: Audit. Approver where the reason code requires one.';
COMMENT ON COLUMN sec_edge.supersedes_record_id IS 'Roles: FK-shaped→sec_edge.record_id (enforced by sec_edge_append_guard, see sec_node.supersedes_record_id), Audit. record_id this append corrects, re-points or retracts. Not consulted by the resolved reads.';
COMMENT ON COLUMN sec_edge.source_system IS 'Roles: Audit. Where the edge came from.';
COMMENT ON COLUMN sec_edge.content_hash IS 'Roles: Audit, Derived. sha256 over the canonical stored form — to_jsonb(row) minus record_id, ingest_xid, ingested_at, content_hash and the generated edge_id, with supersedes_record_id replaced by the predecessor''s content_hash — computed by sec_edge_append_guard on every insert (AR-1.2). A supplied value is verified, never trusted.';
COMMENT ON COLUMN sec_edge.input_lineage IS 'Roles: Audit. For derived edges: source record ids (PR-2.3). NULL on curated edges.';
-- Resolution index (see sec_node_resolve_idx); sec_edge_src_idx stays for src traversal.
CREATE INDEX sec_edge_resolve_idx ON sec_edge (rel_type, src_id, dst_id, edge_seq, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC);
CREATE INDEX sec_edge_src_idx ON sec_edge (src_id, rel_type, valid_from DESC, processing_version DESC);
CREATE INDEX sec_edge_dst_idx ON sec_edge (dst_id, rel_type);

-- ---------------------------------------------------------------------------
-- Reads: latest append per (logical record, valid_from) first, then the valid window (ADR-0006 §5).
-- ---------------------------------------------------------------------------

CREATE VIEW sec_node_current AS
WITH latest AS (
    SELECT DISTINCT ON (id, valid_from) *
    FROM sec_node
    ORDER BY id, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
)
SELECT DISTINCT ON (id) *
FROM latest
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
  AND (now() AT TIME ZONE 'utc')::date < valid_to
ORDER BY id, valid_from DESC;
COMMENT ON VIEW sec_node_current IS 'Operational reads only (two-step: latest append per (id, valid_from) first — processing_version, then ingest_xid — valid window second). A tombstoned WINDOW is absent here and in sec_node_as_of, one tombstone per window; its history stays in the base table. Calculations use sec_node_as_of(effective_at), and replays sec_node_as_of(effective_at, known_at).';

CREATE FUNCTION sec_node_as_of(effective_at date)
RETURNS SETOF sec_node LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (id, valid_from) *
        FROM sec_node
        ORDER BY id, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (id) *
    FROM latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY id, valid_from DESC
$$;
COMMENT ON FUNCTION sec_node_as_of(date) IS 'As-of node read; effective_at is an explicit recorded parameter, never now() (ADR-0006 §4). Filtering the RESULT of this function by record_type scans and sorts the whole store — use sec_node_as_of_kind(effective_at, record_kind) instead.';

-- Kind-scoped as-of read, not a convenience: a record_type predicate on the one-argument function's
-- RESULT cannot push through the DISTINCT ON, so the whole store is scanned and sorted. Filtering
-- inside the CTE is sound because sec_node_id_prefix_chk fixes a kind for the life of an id.
CREATE FUNCTION sec_node_as_of_kind(effective_at date, record_kind text)
RETURNS SETOF sec_node LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (id, valid_from) *
        FROM sec_node
        WHERE record_type = record_kind
        ORDER BY id, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (id) *
    FROM latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY id, valid_from DESC
$$;
COMMENT ON FUNCTION sec_node_as_of_kind(date, text) IS 'As-of node read scoped to one record_type, pushed into the version resolution instead of applied to its result (see the note above the definition). Same two-step semantics as sec_node_as_of(date).';

-- Knowledge-time read: valid time says when a fact was true, knowledge time when we had learned it
-- (RP-4.1, CR-3.5/3.6). The snapshot filter runs BEFORE version resolution, or a correction invisible
-- in the snapshot wins its group; known_at is pg_snapshot because ingest_xid is what orders commits.
CREATE FUNCTION sec_node_as_of(effective_at date, known_at pg_snapshot)
RETURNS SETOF sec_node LANGUAGE sql STABLE AS $$
    WITH known AS (
        SELECT * FROM sec_node
        WHERE pg_visible_in_snapshot(ingest_xid, known_at)
    ), latest AS (
        SELECT DISTINCT ON (id, valid_from) *
        FROM known
        ORDER BY id, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (id) *
    FROM latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY id, valid_from DESC
$$;
COMMENT ON FUNCTION sec_node_as_of(date, pg_snapshot) IS 'Bitemporal node read: latest append VISIBLE IN known_at per (id, valid_from), then the valid window over effective_at (RP-4.1, CR-3.6). The snapshot filter precedes version resolution so an invisible correction cannot win its group. Serves the exact replay a recorded snapshot pins; an arbitrary wall-clock T is a nearest-prior-record lookup, not this function.';

CREATE VIEW sec_edge_current AS
WITH latest AS (
    SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq, valid_from) *
    FROM sec_edge
    ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
)
SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq) *
FROM latest
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
  AND (now() AT TIME ZONE 'utc')::date < valid_to
ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from DESC;
COMMENT ON VIEW sec_edge_current IS 'Operational reads only (two-step, as sec_node_current). A closed edge, or a window carrying a tombstone, is absent here. Calculations use sec_edge_as_of(effective_at), and replays sec_edge_as_of(effective_at, known_at).';

CREATE FUNCTION sec_edge_as_of(effective_at date)
RETURNS SETOF sec_edge LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq, valid_from) *
        FROM sec_edge
        ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq) *
    FROM latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from DESC
$$;
COMMENT ON FUNCTION sec_edge_as_of(date) IS 'As-of edge read; effective_at is an explicit recorded parameter, never now() (ADR-0006 §4).';

-- Knowledge-time edge read; semantics and the reason the snapshot filter precedes version
-- resolution are on sec_node_as_of(date, pg_snapshot) above.
CREATE FUNCTION sec_edge_as_of(effective_at date, known_at pg_snapshot)
RETURNS SETOF sec_edge LANGUAGE sql STABLE AS $$
    WITH known AS (
        SELECT * FROM sec_edge
        WHERE pg_visible_in_snapshot(ingest_xid, known_at)
    ), latest AS (
        SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq, valid_from) *
        FROM known
        ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
    )
    SELECT DISTINCT ON (rel_type, src_id, dst_id, edge_seq) *
    FROM latest
    WHERE valid_from <= effective_at
      AND effective_at < valid_to
    ORDER BY rel_type, src_id, dst_id, edge_seq, valid_from DESC
$$;
COMMENT ON FUNCTION sec_edge_as_of(date, pg_snapshot) IS 'Bitemporal edge read; see sec_node_as_of(date, pg_snapshot).';

-- ---------------------------------------------------------------------------
-- Vocabulary seeds (the decided, stable content only)
-- ---------------------------------------------------------------------------

INSERT INTO weight_basis_vocabulary (basis, description) VALUES
 ('VALUE','share by USD value: look-through composition, allocations'),
 ('NOTIONAL','share by notional: index/benchmark membership'),
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
-- Write boundary: ingest_xid platform-assigned, content_hash engine-computed (AR-1.2, NFR-5).
-- ---------------------------------------------------------------------------

CREATE FUNCTION sec_store_append_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
DECLARE
    computed    bytea;
    pre_image   jsonb;
    parent_hash bytea;
BEGIN
    IF NEW.ingest_xid IS DISTINCT FROM pg_current_xact_id() THEN
        RAISE EXCEPTION 'ingest_xid is platform-assigned on %.% and must never be writer-supplied (ADR-0007 §4, ADR-0006 §5); omit the column and let the default stand',
            TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;

    pre_image := to_jsonb(NEW)
                   - 'record_id' - 'ingest_xid' - 'ingested_at' - 'content_hash'
                   - 'edge_id' - 'supersedes_record_id';

    IF NEW.supersedes_record_id IS NOT NULL THEN
        EXECUTE format('SELECT content_hash FROM %I.%I WHERE record_id = $1', TG_TABLE_SCHEMA, TG_TABLE_NAME)
            INTO parent_hash USING NEW.supersedes_record_id;
        IF parent_hash IS NULL THEN
            RAISE EXCEPTION 'supersedes_record_id % names no stored row in %.%; a correction chains on the content it supersedes, so the predecessor must already be appended (AR-1.2)',
                NEW.supersedes_record_id, TG_TABLE_SCHEMA, TG_TABLE_NAME;
        END IF;
        pre_image := pre_image || jsonb_build_object('supersedes_content_hash', encode(parent_hash, 'hex'));
    END IF;

    computed := sha256(convert_to(pre_image::text, 'UTF8'));

    IF NEW.content_hash IS NOT NULL AND NEW.content_hash <> computed THEN
        RAISE EXCEPTION 'content_hash mismatch on %.%: supplied %, computed % — a supplied hash is verified, never trusted (AR-1.2)',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, encode(NEW.content_hash,'hex'), encode(computed,'hex');
    END IF;

    NEW.content_hash := computed;
    RETURN NEW;
END $$;
COMMENT ON FUNCTION sec_store_append_guard() IS 'BEFORE INSERT guard for sec_node / sec_edge: rejects a writer-supplied ingest_xid; computes content_hash over to_jsonb(row) minus the platform-assigned and derived fields, with supersedes_record_id replaced by the predecessor''s content_hash so the digest chains and survives a re-import that reassigns record_ids; rejects a supersedes_record_id naming no stored row; verifies rather than trusts a supplied hash (AR-1.2, NFR-5). Reads only the store it guards, by record_id.';

CREATE TRIGGER sec_node_append_guard BEFORE INSERT ON sec_node
    FOR EACH ROW EXECUTE FUNCTION sec_store_append_guard();
CREATE TRIGGER sec_edge_append_guard BEFORE INSERT ON sec_edge
    FOR EACH ROW EXECUTE FUNCTION sec_store_append_guard();

-- ---------------------------------------------------------------------------
-- Append-only by table class: full revoke on the stores, immutability trigger on the FK parents.
-- ---------------------------------------------------------------------------

-- The revoke derives the owner from pg_class.relowner, so it lands whatever the role is called.
-- Where the owner is a SUPERUSER (the test harness) the ACL is recorded but not enforced — the
-- documented position_state gap — so the assertion below checks the privilege only for a non-superuser.
DO $$
DECLARE
    t text;
    owner_role text;
    owner_is_super boolean;
BEGIN
    -- Stores: full revoke, owner included (nothing FKs them, so no RI probe needs UPDATE).
    FOREACH t IN ARRAY ARRAY['sec_node','sec_edge'] LOOP
        SELECT pg_get_userbyid(c.relowner) INTO owner_role FROM pg_class c WHERE c.oid = t::regclass;
        SELECT rolsuper INTO owner_is_super FROM pg_roles WHERE rolname = owner_role;
        EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM %I', t, owner_role);
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM stl_readwrite', t);
        END IF;
        IF NOT owner_is_super AND has_table_privilege(owner_role, t, 'UPDATE') THEN
            -- RAISE takes % only; %I belongs to format().
            RAISE EXCEPTION 'append-only not enforced: owner % still holds UPDATE on % after the revoke', owner_role, t;
        END IF;
    END LOOP;
    -- Vocabulary tables: app role fully revoked; the OWNER KEEPS UPDATE because the FK integrity
    -- probe (SELECT ... FOR KEY SHARE) runs as the parent's owner and needs it (20260714_160000,
    -- #574); DELETE/TRUNCATE revoked, and reference_table_immutable() blocks real mutation.
    FOREACH t IN ARRAY ARRAY['rel_type_vocabulary','weight_basis_vocabulary',
                             'change_reason_vocabulary','concept_class_vocabulary',
                             'node_status_vocabulary'] LOOP
        SELECT pg_get_userbyid(c.relowner) INTO owner_role FROM pg_class c WHERE c.oid = t::regclass;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM stl_readwrite', t);
        END IF;
        EXECUTE format('REVOKE DELETE, TRUNCATE ON %I FROM %I', t, owner_role);
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reference_table_immutable()',
                       t || '_immutable', t);
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260904_120000_secstore_node_edge_stores_and_vocabularies.sql') ON CONFLICT (filename) DO NOTHING;
