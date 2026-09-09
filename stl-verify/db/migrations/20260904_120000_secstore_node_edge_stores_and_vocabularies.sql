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
-- Two engine-enforced write-boundary guarantees on the stores, both by BEFORE INSERT trigger
-- (sec_store_append_guard, defined below with its full rationale): ingest_xid can only be the
-- current transaction id, and content_hash is computed by the engine from the first append —
-- neither is writer-trusted.
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
-- Valid time is TOTAL and stored, not derived: valid_to is NOT NULL with an 'infinity' sentinel
-- for an open window, and it sits IN THE PRIMARY KEY. Three things follow, and they are the
-- reason for the shape (review of the first draft, which had a nullable valid_to outside the PK):
--   * Close-and-open is an ordinary append at processing_version 0. Closing a window means
--     appending the same (id, valid_from) with a real valid_to; without valid_to in the key that
--     row collides with the open one, which would force every curated edit to allocate a
--     correction version through processing_version_log (ADR-0006 §3 reserves those for
--     correction RUNS, one per ticket) — an issuer re-point is not a correction run.
--   * A retraction is expressible: a TOMBSTONE is an append with a ZERO-LENGTH window
--     (valid_to = valid_from), which is why the window CHECK is <= and not <. It matches no
--     as-of date, so the WINDOW it names drops out of the resolved reads while every version of
--     it stays readable — ADR-0005 §3's "tombstone append that supersedes the retracted row",
--     with supersedes_record_id naming the retracted record (UNIQUE (record_id) makes that
--     pointer resolvable). Un-retracting is a correction run at N, not a re-append at 0: the
--     re-asserted row would otherwise collide with the original and be dropped by
--     ON CONFLICT DO NOTHING, leaving the tombstone winning forever.
--
--     A tombstone withdraws ONE WINDOW, not the logical record, because supersession resolves
--     per (logical record, valid_from) and a zero-length append only wins its own group. A
--     record that has been closed and reopened therefore takes one tombstone per window: given
--     Jan-open, Jan-closed-to-Jun and Jun-open, a tombstone on the Jan group leaves the Jun
--     window live and current. That is the honest scope of the mechanism, and it is asserted
--     that way in TestSecStoreClosingRowSupersedesRatherThanResurrects rather than only in the
--     single-window case, which passes either way (review finding: the claim above originally
--     said "the logical record drops out", which is true only of a one-window record).
--     Withdrawing a logical record in ONE append, and the DQ rule that flags a half-retracted
--     record, are VEC-622's — they need the validator that would police them.
--   * Knowledge time appears twice and the two are not the same: as the SUPERSESSION ORDER
--     inside a valid window (next bullet), and as a READ PARAMETER — both _as_of functions
--     take a pg_snapshot overload answering "what did we know then", which the ordering alone
--     does not provide. The first draft shipped only the effective-date read while this header
--     read as though resolution covered both clocks (review finding).
--   * Resolution is by knowledge time within a window: latest append per (logical record,
--     valid_from) is processing_version DESC, then ingest_xid DESC (ADR-0006 §5's ordering key —
--     never writer-supplied, so a writer cannot reorder its own supersession), then record_id
--     DESC to break a same-transaction tie. Only then does the valid-time window filter run.
--     Filtering on the window first resurrects superseded rows; ordering on ingested_at would
--     rest supersession on a wall clock.
-- What this does NOT do: it makes the retraction and the amendment REPRESENTABLE, it does not
-- make them mandatory. A valid-time amendment that appends the corrected window and neglects to
-- tombstone the wrong one leaves both standing; catching that is the validator's (VEC-622) and
-- GQ-2x's job, not the schema's.
--
-- The frozen masters (entity_master/security_master, VEC-410/411) stored valid_from only and
-- derived valid_to_exclusive with lead(); that pattern cannot express an ended edge, which has no
-- status column to retire it, and ADR-0005 §3 stores the pair. Divergence is deliberate.
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

CREATE TABLE weight_basis_vocabulary (
    basis        text PRIMARY KEY,
    description  text NOT NULL
);
COMMENT ON TABLE weight_basis_vocabulary IS '[Configuration] Legal weight bases (ADR-0005 §3). Weights of unlike bases must never be summed; conversion ratios are edge payload, not weights. Plain table: seed-once.';
COMMENT ON COLUMN weight_basis_vocabulary.basis IS 'Roles: PK. Basis code (VALUE / NOTIONAL / UNITS / OWNERSHIP_PCT).';
COMMENT ON COLUMN weight_basis_vocabulary.description IS 'What the basis measures and where it is used.';

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
    change_reason       text NOT NULL DEFAULT 'SEED_LOAD'
);
COMMENT ON TABLE rel_type_vocabulary IS '[Configuration] Governed relationship vocabulary (ADR-0005 §5). Adding or ratifying a type is a reviewed migration. Endpoint legality is the (rel_type, src_kind, dst_kind) triple, enforced by the loader/validator (cross-row; an FK cannot see the endpoint row). Plain table: governance-rate writes.';
COMMENT ON COLUMN rel_type_vocabulary.rel_type IS 'Roles: PK. The edge type name, UPPER_SNAKE.';
COMMENT ON COLUMN rel_type_vocabulary.family IS 'One of the six ADR-0005 §5 families.';
COMMENT ON COLUMN rel_type_vocabulary.src_kinds IS 'Legal source node kinds (sec_node.record_type values).';
COMMENT ON COLUMN rel_type_vocabulary.dst_kinds IS 'Legal destination node kinds.';
COMMENT ON COLUMN rel_type_vocabulary.cardinality IS 'Expected current-state cardinality; a DQ check over current state, never a write trigger (an open edge always time-overlaps its re-point).';
COMMENT ON COLUMN rel_type_vocabulary.weight_basis IS 'Roles: FK→weight_basis_vocabulary.basis. The declared basis for weighted types; NULL = unweighted type.';
COMMENT ON COLUMN rel_type_vocabulary.derived_only IS 'true: rows of this type are projections written by a loader with lineage, never curated by hand.';
COMMENT ON COLUMN rel_type_vocabulary.maturity IS 'ratified: decided and stable. draft types are not seeded; they land by migration when ratified.';
COMMENT ON COLUMN rel_type_vocabulary.description IS 'What the type means; the reviewed definition.';
COMMENT ON COLUMN rel_type_vocabulary.change_reason IS 'Roles: Audit. Why the row exists (vocabulary rows carry the slim spine; full provenance lives on nodes/edges).';

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
    valid_to            date NOT NULL DEFAULT 'infinity',
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
    content_hash        bytea NOT NULL,
    PRIMARY KEY (id, processing_version, valid_from, valid_to),
    CONSTRAINT sec_node_record_id_key UNIQUE (record_id),
    CONSTRAINT sec_node_id_prefix_chk CHECK (
        (record_type = 'ENTITY'   AND id LIKE 'em-%')      OR
        (record_type = 'SECURITY' AND id LIKE 'sec-%')     OR
        (record_type = 'CONCEPT'  AND id LIKE 'concept-%') OR
        (record_type = 'SOURCE'   AND id LIKE 'src-%')     OR
        (record_type = 'ACCOUNT'  AND id LIKE 'acct-%')
    ),
    CONSTRAINT sec_node_valid_chk CHECK (valid_from <= valid_to),
    -- A row whose window STARTS at infinity satisfies valid_from <= valid_to and then matches no
    -- read ever, because every read tests valid_from <= effective_at: it lands, takes a PK slot
    -- and a content_hash, and is invisible forever with no error anywhere. 'infinity' is the
    -- open-END sentinel and nothing else (review finding; '-infinity' is refused for the same
    -- reason — a start nobody can name is not a start).
    CONSTRAINT sec_node_valid_from_finite_chk CHECK (valid_from <> 'infinity' AND valid_from <> '-infinity')
);
COMMENT ON TABLE sec_node IS '[Dimension] Combined SECs master (ADR-0005 §2): one node per real-world thing, discriminated by record_type. Append-only (full ACL revoke incl. owner — nothing FKs this table), bitemporal (valid window + ingest_xid). valid_to is NOT NULL (''infinity'' when open) and in the PK, so close-and-open is an append at processing_version 0; a zero-length window is a retraction tombstone. The instrument is NOT a node kind: native keys resolve via the instrument register (VEC-616). Individuals carry a pseudonymous surrogate only; PII lives in a separate store (DP-1). Plain table: governance-rate writes, per the sparse-table exception.';
COMMENT ON COLUMN sec_node.id IS 'Roles: PK (with processing_version, valid_from). Opaque, kind-prefixed (em-/sec-/concept-/src-/acct-), house-assigned once, never derived from a public identifier or symbol, and never hashed into position_id. Seeded em-* ids stand unchanged.';
COMMENT ON COLUMN sec_node.record_type IS 'Node kind. ENTITY / SECURITY / CONCEPT / SOURCE live; ACCOUNT staged (ADR-0005 §2).';
COMMENT ON COLUMN sec_node.chain_id IS 'Roles: FK→chain.chain_id (soft). NULL for off-chain things.';
COMMENT ON COLUMN sec_node.status IS 'Roles: FK→node_status_vocabulary (composite with record_type). A status change is a new version.';
COMMENT ON COLUMN sec_node.attrs IS 'Kind-specific attributes as jsonb; the shape system (VEC-622) decides required-ness per type. Hot attributes promote to typed columns only on VEC-633 evidence.';
COMMENT ON COLUMN sec_node.valid_from IS 'Roles: PK (with id, processing_version, valid_to). Valid-time window start, UTC date, half-open [valid_from, valid_to).';
COMMENT ON COLUMN sec_node.valid_to IS 'Roles: PK (with id, processing_version, valid_from). Valid-time window end, exclusive; ''infinity'' = open/current, never NULL. In the key so close-and-open is an ordinary append at processing_version 0. A ZERO-LENGTH window (valid_to = valid_from) is a TOMBSTONE: it matches no as-of date, so THAT WINDOW drops out of the resolved reads with its history intact (ADR-0005 §3 retraction; pair it with change_reason_code RETRACTION and supersedes_record_id). It withdraws one window, not the logical record — a closed-and-reopened record takes one tombstone per window, and single-append record withdrawal is VEC-622''s.';
COMMENT ON COLUMN sec_node.record_id IS 'Roles: Audit, UNIQUE. Per-append surrogate; what supersedes_record_id, a retraction and a reproduction manifest point at (PR-2.1). Unique per store, not globally: a manifest cites (table, record_id).';
COMMENT ON COLUMN sec_node.processing_version IS 'Roles: Audit, PK component. Correction version, caller-assigned per ADR-0006 §3: 0 live, N per correction run via processing_version_log. A valid-time change (close-and-open, an ended window, a tombstone) is NOT a correction and stays at 0 — valid_to carries it. Un-retracting a tombstoned record IS a correction run at N.';
COMMENT ON COLUMN sec_node.ingest_xid IS 'Roles: Audit. Knowledge-time visibility key (ADR-0006 §5, pg_visible_in_snapshot) and the supersession tiebreak inside a valid window. Never writer-supplied: the sec_node_append_guard trigger rejects an insert that sets it to anything but the current transaction id.';
COMMENT ON COLUMN sec_node.ingested_at IS 'Roles: Audit. Wall-clock label only; never the audit key (a row stamps at transaction start but becomes visible at commit).';
COMMENT ON COLUMN sec_node.run_id IS 'Roles: Audit. Writer run; FK to writer_run lands with ADR-0006 §2 (VEC-598).';
COMMENT ON COLUMN sec_node.actor IS 'Roles: Audit. Real, non-shared principal (human or service) that appended the row. Required.';
COMMENT ON COLUMN sec_node.change_reason_code IS 'Roles: FK→change_reason_vocabulary.code, Audit. Structured reason for the append.';
COMMENT ON COLUMN sec_node.change_reason IS 'Roles: Audit. Free-text reason; cites the source where change_reason_code = CURATED_SOURCE.';
COMMENT ON COLUMN sec_node.approved_by IS 'Roles: Audit. Approver, distinct from actor, where the reason code requires approval.';
COMMENT ON COLUMN sec_node.supersedes_record_id IS 'Roles: FK-shaped→sec_node.record_id (enforced by sec_node_append_guard, which needs the predecessor''s content_hash to chain this row''s hash; an unresolvable pointer is rejected), Audit. record_id this append corrects or retracts; the correction chain is walkable through it, and content_hash binds this row to the exact content it supersedes. The resolved reads do not consult it — supersession within a window is decided by processing_version then ingest_xid, and withdrawal by the zero-length tombstone window.';
COMMENT ON COLUMN sec_node.source_system IS 'Roles: Audit. Where the fact came from (registry, worksheet, port, loader).';
COMMENT ON COLUMN sec_node.content_hash IS 'Roles: Audit, Derived. sha256 over the canonical stored form — to_jsonb(row) minus record_id, ingest_xid, ingested_at and content_hash, with supersedes_record_id replaced by the predecessor''s content_hash — computed by the sec_node_append_guard trigger on every insert, so the chain runs from the first append (AR-1.2, NFR-5). Chaining on content rather than on a row number keeps it reproducible from an export and stable across a re-realization that reassigns record_ids. A supplied value is verified against the computed one and rejected if it differs.';
-- Resolution index: the reads below sort (id, valid_from) ASC then processing_version DESC,
-- ingest_xid DESC, record_id DESC. Columns AND directions have to match the whole key or the
-- DISTINCT ON degrades to a full scan plus sort on every current read (VEC-633 measures this).
CREATE INDEX sec_node_resolve_idx ON sec_node (id, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC);
-- Same rule as sec_node_resolve_idx, and it applies here too: the kind-scoped read below sorts
-- (id, valid_from) ASC then processing_version DESC, ingest_xid DESC, record_id DESC, so this key
-- must match through its whole length. It was (…, valid_from DESC, processing_version DESC) —
-- wrong direction on valid_from and two columns short, so it could not supply the sort at all and
-- the planner fell back to sec_node_resolve_idx, which has no leading record_type. Measured at
-- 200k nodes: 102 ms with the mismatched key against 64 ms with this one, the difference being an
-- inner sort node that disappears (review of the first draft, which shipped the mismatch under a
-- comment stating the rule correctly).
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
    run_id              bigint,
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
    -- The cheap half of GQ-11 at the engine boundary. Endpoint EXISTENCE is cross-row and stays
    -- with the validator, but the kind is NOT only a domain question: sec_node_id_prefix_chk makes
    -- record_type a deterministic function of the id prefix, so a declared kind that contradicts
    -- its own endpoint id is single-row checkable — the same fact the kind-scoped read below
    -- relies on for its pushdown. The first draft checked only that the kind was one of the five,
    -- which accepted ('em-90001','SECURITY') and an empty dst_id (review finding). These CHECKs
    -- are sec_node_id_prefix_chk applied to each endpoint, and they subsume the domain check.
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
COMMENT ON TABLE sec_edge IS '[Dimension] Directed, typed, weighted relationship store (ADR-0005 §3/§5). Append-only (full ACL revoke incl. owner — nothing FKs this table); close-and-open at processing_version 0 (valid_to is NOT NULL, ''infinity'' when open, and in the PK); retraction is a tombstone append with a zero-length window. Endpoint-kind legality vs rel_type_vocabulary is loader/validator-enforced (cross-row); single-valued cardinality is a DQ check over current state, never a write trigger. Inverses and closures are derived, never stored. Plain table: governance-rate writes — block-stamped projection types (ALLOCATES) are excluded by design and would need their own hypertable store if ratified.';
COMMENT ON COLUMN sec_edge.edge_id IS 'Roles: Derived. Generated human-readable identity of the LOGICAL edge; the PK is the seven-column (rel_type, src_id, dst_id, edge_seq, processing_version, valid_from, valid_to) tuple, so one edge_id spans every version and window of that edge.';
COMMENT ON COLUMN sec_edge.edge_seq IS 'Roles: PK component. DM-6 discriminator: deliberately duplicated edges (multi-typing, per-edge attribute clusters) coexist instead of superseding their twin. Base is 1 per ADR-0005 §3, so a twin is 2; 0 is rejected rather than left as a second spelling of the base edge, since edge_seq is rendered into the stored edge_id.';
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
COMMENT ON COLUMN sec_edge.run_id IS 'Roles: Audit. Writer run; FK lands with ADR-0006 §2 (VEC-598).';
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
-- Current views and as-of reads: two-step, always — latest APPEND per (logical
-- record, valid_from) FIRST, then the valid window (the other order resurrects
-- superseded rows). Within a window the winner is processing_version DESC, then
-- ingest_xid DESC (ADR-0006 §5's ordering key; never a wall clock, never
-- writer-supplied), then record_id DESC for a same-transaction tie — so a close
-- beats the open row it closes, and a tombstone (zero-length window) beats the
-- fact it withdraws and then matches no date, dropping the record from the read.
-- _current is for operational reads only; anything feeding a calculation uses
-- _as_of(effective_at) with an explicit recorded parameter (ADR-0006 §4).
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

-- Kind-scoped as-of read, under its own name rather than as an overload (see the pg_snapshot
-- form below for why). Not a convenience: a predicate on record_type applied to the
-- one-argument function's RESULT cannot be pushed through the DISTINCT ON, because
-- record_type is not part of its key, so the whole store is scanned and sorted — measured at
-- 65 ms with a 4.4 MB external merge over 200k rows, against 0.088 ms for the same function
-- filtered on id, which IS in the key and does push down (review measurement on pg18). The
-- pivot's dim_security and dim_entity (VEC-619) are exactly that shape.
--
-- Filtering INSIDE the CTE is sound rather than an approximation: record_type is fixed for
-- the life of a node id by sec_node_id_prefix_chk, so no version of an id can carry a
-- different kind, and restricting the input therefore cannot change which version wins.
-- sec_node_type_idx is what this reads, and its key matches this sort through its whole length.
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
COMMENT ON FUNCTION sec_node_as_of_kind(date, text) IS 'As-of node read scoped to one record_type, pushed into the version resolution instead of applied to its result (see the note above the definition). Same two-step semantics as sec_node_as_of(date). Deliberately NOT an overload of sec_node_as_of: see the note on the pg_snapshot form.';

-- Knowledge-time read: what a reader holding this snapshot would have seen as true on
-- effective_at. The two clocks are independent parameters, which is the whole point — valid time
-- says when a fact was true in the world, knowledge time when we had learned it, and without the
-- second a backdated late discovery reads as though we had always known it (RP-4.1, CR-3.5/3.6).
--
-- The snapshot filter runs BEFORE version resolution, not after: a correction that is invisible
-- in the snapshot must not be able to win its group, or the replay returns today's answer with
-- yesterday's date on it. Consumers that pin a number record the snapshot alongside the
-- effective date (ADR-0006 §5) and replay through here; an arbitrary wall-clock T is served by
-- nearest-prior-record lookup, which is a read over ingested_at and not this function's job.
--
-- known_at is pg_snapshot rather than a timestamp deliberately: ingest_xid is the exact
-- visibility key and wall clock cannot order commits (a row stamps ingested_at at transaction
-- start and becomes visible at commit).
--
-- This is why the kind-scoped read is sec_node_as_of_KIND and not a second two-argument
-- overload. With both present, an unknown-typed second argument resolves to text (the preferred
-- type in its category), and most drivers have no pg_snapshot type and bind it as a string — so
-- a knowledge-time replay would silently become a kind-scoped read for a record_type that does
-- not exist: zero rows, no error, indistinguishable from "the record did not exist then". The
-- first draft documented that hazard instead of removing it (review finding). A distinct name
-- means a mis-bound snapshot is a type error at the call, which is what it should be.
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
-- Write-boundary guard on the two stores: ingest_xid is platform-assigned, and
-- content_hash is computed by the engine from the first append (AR-1.2, NFR-5).
--
-- Both were writer-trusted in the first draft and neither survives review that way:
--   * ingest_xid had only a DEFAULT, so a writer could supply any xid8. It is the
--     knowledge-time visibility and ordering key (ADR-0006 §5,
--     pg_visible_in_snapshot) and now also decides supersession inside a valid
--     window, so a forged value silently corrupts replay and lets a writer reorder
--     its own corrections. ADR-0005 §4 says never writer-supplied; this enforces it.
--     The guard RAISES rather than overwriting: a writer that sets it has a bug, and
--     a bug that repairs itself is a bug you ship. Omitting the column (the normal
--     path) leaves the DEFAULT, which equals pg_current_xact_id() in the same
--     transaction, so the check is a no-op there.
--   * content_hash was left NULL "to be wired with the validator" (VEC-622). AR-1.2
--     requires the chain to run from the FIRST append, and ADR-0005 §4 banks on the
--     store being empty as the reason that is free — deferring it spends exactly that,
--     and the 501 rows of 20260904_120100 would have been permanently outside the
--     chain. Computing it here costs nothing and covers every writer, not just the
--     seed.
--
-- The hashed canonical form is to_jsonb(NEW) minus the platform-assigned and derived
-- fields: record_id (an identity sequence), ingest_xid / ingested_at (assigned here),
-- content_hash (the output), and edge_id (generated from columns already hashed). What
-- remains is exactly what the writer determined — identity, attributes, valid window,
-- and the provenance block — so the hash is reproducible from an export and survives a
-- re-realization that assigns new record_ids (Realization §2's round-trip requirement).
--
-- supersedes_record_id is excluded for the same reason and REPLACED by the predecessor's
-- content_hash, under the key supersedes_content_hash. It is itself a record_id, so leaving
-- it in the pre-image would have broken exactly the round-trip the exclusion of record_id
-- exists to protect: re-importing an export reassigns the identity sequence, the pointer
-- changes, and every correction and tombstone in the file fails verification (review of the
-- first draft, which did leave it in). Substituting the predecessor's hash also turns this
-- from a per-row digest into a real chain — a correction is bound to the exact content it
-- supersedes, not to a row number — which is what AR-1.2's chaining language asks for, and
-- costs one lookup by record_id on a plain table at governance rate.
--
-- That lookup makes supersedes_record_id resolvable-or-nothing at the write boundary: an
-- append naming a record_id that is not in the same store is rejected. The first draft called
-- the reference deliberately soft so a correction could precede its target inside one batch;
-- that case does not exist — a correction corrects a row that is already stored — and the
-- chain cannot be computed without the predecessor.
-- jsonb gives the canonicalisation for free: keys sorted, whitespace normalised, dates
-- and timestamps rendered ISO 8601 independent of DateStyle, numerics at their stored
-- scale. Adding a column later changes the hash of rows appended after it, not of
-- existing rows — state it in the ADR when it happens rather than rehashing history.
--
-- Supplying content_hash is allowed only if it MATCHES what the engine computes: that
-- makes re-importing an exported row a verification rather than a leap of faith, and a
-- mismatch fails the insert. content_hash is declared NOT NULL on both stores: NOT NULL is
-- checked after BEFORE triggers, so the guard always satisfies it, and the declaration turns a
-- disabled trigger into a failed insert instead of a silently unhashed row.
-- The predecessor lookup does not need the plan_cache_mode treatment that AGENTS.md requires of
-- BEFORE INSERT triggers, and the reason is not "no table is read" — the guard reads the store it
-- guards (an earlier draft of this comment said otherwise): that rule is scoped to per-row
-- HYPERTABLE lookups, where a generic plan fans out over every chunk. These are plain tables, the
-- lookup is an equality on a unique index, and it goes through EXECUTE, which plpgsql never
-- plan-caches at all.
-- ---------------------------------------------------------------------------

CREATE FUNCTION sec_store_append_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
DECLARE
    computed    bytea;
    pre_image   jsonb;
    parent_hash bytea;
BEGIN
    IF NEW.ingest_xid IS DISTINCT FROM pg_current_xact_id() THEN
        RAISE EXCEPTION 'ingest_xid is platform-assigned on %.% and must never be writer-supplied (ADR-0005 §4, ADR-0006 §5); omit the column and let the default stand',
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
-- Append-only enforcement, by table class (see header).
-- sec_node / sec_edge: full revoke including the owner (position_state pattern;
-- nothing FKs them). Vocabulary tables: FK parents — the RI probe runs as the
-- owner and needs UPDATE (20260714_160000), so the owner keeps UPDATE and the
-- reference_table_immutable() trigger enforces append-only instead.
-- ---------------------------------------------------------------------------

-- reference_table_immutable() is NOT redeclared here: it is owned by
-- 20260714_160000_fix_reference_table_fk_inserts.sql and already carries every ref_* trigger.
-- Re-CREATE OR REPLACEing it from a second migration would let either file silently redefine the
-- other's behaviour (and would reset any function-level SET, per db/migrations AGENTS.md). The
-- triggers below just point at it.

-- The owner-side revoke is derived from pg_class.relowner, not from a hardcoded role name. The
-- first draft looped over ARRAY['stl_readwrite','stl_migrator'] under IF EXISTS, so in any
-- environment whose tables are owned by a differently named role the store-side revoke — the
-- whole append-only guarantee here — silently did nothing. Deriving the owner means the REVOKE
-- always executes and the ACL is always recorded, whatever the role is called.
--
-- Where the owner is a SUPERUSER (the test harness migrates as its own bootstrap role) the ACL is
-- recorded but not enforced, because superusers bypass privilege checks. That is the documented
-- position_state gap and it is not fixable from SQL; the assertion below therefore checks the
-- privilege only for a non-superuser owner, which is the prod shape, and raises rather than
-- trusting that the revoke landed.
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
            -- RAISE takes % only; %I is a format() specifier and renders as the value with a
            -- literal I glued on, which is not what you want in the one message that fires when
            -- append-only has already failed.
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
