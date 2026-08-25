# ADR-0005: Combined Master (Entity, Security, Concept) and a Relationship Store

**Status**: Accepted (data model) — storage representation and engine are follow-up decisions
**Proposed**: @peter.simon, @yasanji.ratnaike
**Date**: 2026-07-30 (proposed) · 2026-08-21 (accepted)
**Deciders**: @yasanji.ratnaike, @peter.simon, @simon.bojeoutzen

> **What is decided here is the data model, and only the data model.** One combined master of
> nodes and one relationship store of edges, with the contracts, vocabulary, and time semantics
> below. The model is deliberately technology-agnostic: any realization — fixed relational
> columns, JSON documents in Postgres, a graph store, a synlang atomspace — must implement these
> same contracts, and swapping the realization must not change the model. How the store is
> represented and which engine runs it are separate decisions, recorded under Realization below
> and resolved on their own evidence.
>
> The model conforms to the **STL Auditability & Reproducibility PRD** (v0.2, 18 Aug 2026).
> Requirement ids below (`AR-*`, `PR-*`, `CR-*`, `RP-*`, `DP-*`, `NFR-*`) reference that
> document, and the Auditability conformance section maps the model to it item by item. Where
> this ADR and that PRD conflict, the PRD wins and this ADR is wrong.

## In brief

- One node table (the **SECs master**) holds entities, securities, and concepts, discriminated by
  `record_type`. Nodes carry attributes; the identity contract is `id`, `record_type`, `chain_id`.
- A separate **relationship store** holds every link between nodes as a typed, directed,
  nullable-weighted edge with its own validity window, version, and provenance. Relationships are
  never columns on a node.
- **Concepts** are a third node kind: shared categories defined once and attributed to many nodes,
  pointing to rules or a list through ordinary edges. Categoricals that need relationships of
  their own become concepts, not lookup rows.
- Both stores are **append-only and bitemporal**: every row carries valid time (when the fact is
  true in the world) and system time (when the platform recorded it), so the model answers both
  "what was true" and "what did we know" for any date (AR-1.1, RP-4.1, CR-3.5).
- Corrections are new appends that **reference what they supersede** by record id, with a
  structured reason and approval, forming an ordered supersession chain (CR-3.1..3.4).
- The standalone masters (`entity_master`, `security_master`, `security_instrument_bridge`,
  `entity_ref_codes`, `position_entity_link`) are **superseded and frozen** — see Deprecations.

## Context

We are building the master data that feeds enriched positions in `archon-research/stl`. The model
has to hold entities (issuers, protocol operators, primes), securities (the tokens we hold),
concepts (shared, fungible categories such as an asset class or an entity type), and the
relationships between all of them — issuer, underlying, holder, parent/subsidiary, category
membership, succession through corporate actions, and the resolution of a native on-chain id to a
security.

Several requirements shaped the model:

- **One-to-many and many-to-many relationships.** syrupUSDC is issued by Maple *and* built on
  USDC; an LP token has two or more underlyings; an entity can be both an issuer and a holder.
  A single "parent" slot on a row cannot represent these.
- **Security-to-security relationships.** `HAS_UNDERLYING` links a security to another security,
  chained for token-of-token depth, so look-through is a walk over weighted edges.
- **Multiple distinct relationships between the same two nodes.** An issuer link and an underlying
  link can join one pair; each is its own edge.
- **Type-specific attributes.** Different kinds of entity (fund, person, bank) and security carry
  different fields, and the set will grow.
- **Classification.** Fields with no on-chain source (`asset_class`, `security_type`, `currency`,
  `entity_type` / `counterparty_role` moving from `UNKNOWN` to a curated value) are assigned by
  our own curated layer, not read from a raw table.
- **Auditability and reproducibility.** The Auditability & Reproducibility PRD requires
  append-only immutable storage, full provenance on every append, corrections that reference what
  they supersede, and point-in-time reproducibility. Reference data is an input to every
  published number, so this store must meet those requirements in its own right, consistent with
  ADR-0002.

The model was stress-tested on paper against the common change scenarios (renames, reorgs, proxy
upgrades, token-of-token nesting, corporate actions, cross-chain deployments, rebasing) in the
design artifacts, and the edge store and its resolution reads were verified against an ephemeral
Postgres over the live held book. On that basis the combined master was accepted as the
direction; the standalone two-master build is superseded.

## Decision: the data model

### 1. The node contract

Entities, securities, and concepts live in a single node store (the SECs master), discriminated
by `record_type ∈ {ENTITY, SECURITY, CONCEPT}`.

- **Identity is the stable contract**: `id`, `record_type`, and `chain_id`. These are what the
  relationship store and the timeseries data join on, and they must not churn. `id` is prefixed
  by kind (`sec:`, `ent:`, `concept:`) and built from native identifiers (a contract address, a
  registry id), never from a house classification. Every stored row additionally carries a
  unique, stable **record id** (the version-level identity a correction or a reproduction
  manifest references — PR-2.1, RP-4.6).
- **Attributes belong to the node** and differ by kind: names, `asset_class`, `security_type`,
  `currency`, `legal_name`, `lei`, `entity_type`, and so on. The attribute set is expected to
  evolve; how attributes are physically stored (typed columns, a document payload, node
  properties) is a realization choice, not part of this model.
- **Versioning is by row, append-only** (AR-1.1). A change to a node is a new version; nothing
  is ever updated or deleted in place. Each version carries the full provenance block defined in
  §3 below.
- **Natural persons carry no direct identifiers here** (DP-1). Where an ENTITY node represents an
  individual, the node holds only a pseudonymous surrogate key; the identifying attributes live
  in a dedicated PII store, referenced by that key, so the append-only master holds no personal
  data that an erasure request could touch. See the GDPR rule in §7.
- **Promotion rule.** A simple attribute lives on its node. It is promoted to a node of its own,
  joined by an edge, when it carries attributes itself or when other things need to relate to it.
  This is the test that turns a categorical into a concept.

### 2. The relationship contract

Every link between nodes lives in one relationship store, one row per edge. Relationships are
never columns on a node — the earlier `issuer_entity_id`, `parent_entity_id`, and
`ultimate_parent_id` columns are replaced by edges.

| field | meaning |
|---|---|
| `id` | deterministic composite `rel:rel_type:src_id:dst_id` — the logical edge; `rel_type` is included because one pair of nodes can share several link types |
| `record_id` | unique, stable id of this stored row (this version of the edge) — what corrections and manifests reference (PR-2.1) |
| `src_id`, `dst_id` | the two node ids the edge joins (soft references to the master: SCD2 ids are non-unique, so resolution goes through the current view, not a row-level FK) |
| `src_kind`, `dst_kind` | the endpoint kinds, so vocabulary rules are checkable |
| `rel_type` | the kind of link, from the governed vocabulary below |
| `valid_from` / `valid_to` | valid time: when the link is true in the world (UTC dates, half-open); open `valid_to` means current |
| `rel_weight` | nullable: composition weight, ownership fraction, or a conversion ratio; null when the link carries no number |
| provenance block | §3: system time, actor, software version, lineage, correction fields |

Semantics, independent of realization:

- **Directed.** Each edge is `src_id → dst_id`. Inverses (`ISSUES` from `ISSUED_BY`,
  `UNDERLYING_OF` from `HAS_UNDERLYING`) are derived at read time, never stored.
- **Weighted for look-through.** Exposure to a leaf is the sum over paths of the product of
  weights along each path; a 50/50 LP token is 50 % of each underlying.
- **Multi-typing by duplication.** Several relationships on one pair are several edges; an edge
  type that needs its own attribute cluster carries it on the edge.
- **Append-only, close-and-open** (AR-1.1, AR-1.4). A change closes the current row (a new
  version with `valid_to` set) and opens a new one. A re-point, a type change (`SUBSIDIARY_OF` →
  `AFFILIATE_OF`), an ended link, and a backdated late-learned link are all data changes. A
  retraction ("this edge should never have existed") is a tombstone append that supersedes the
  retracted row — never a removal.
- **Current state is a two-step read**: resolve the latest version per logical edge
  (`src_id`, `rel_type`, `dst_id`) first, then apply the valid-time window. The order matters —
  filtering on validity first drops the closing row and resurrects the superseded edge.
- **Cardinality is validated over current state, not at write.** A re-point always time-overlaps
  the edge it supersedes, so single-valued types (for example `ISSUED_BY`) are checked by a
  data-quality view over the resolved current state rather than a write-time exclusion.
- **Endpoint-kind and vocabulary rules are enforced on write** by the loader/validator, since
  they are cross-row: an `ISSUED_BY` must run security → entity.

### 3. Provenance and corrections (both stores)

Every append — node version or edge row — carries the same immutable provenance block, per the
Auditability & Reproducibility PRD §5.2–5.3 and ADR-0002:

| field | meaning | PRD |
|---|---|---|
| `record_id` | unique, stable id of this stored row | PR-2.1 |
| `recorded_at` | system time (transaction time): when the platform recorded it, UTC, from a synchronised clock, assigned by the platform and monotonic per logical record | PR-2.1, PR-2.7 |
| `actor` | the authenticated principal (human or non-shared service account) that caused the append | PR-2.1, PR-2.5 |
| `build_id` / software version | the code that produced it: source commit, build/artifact version, image digest (per ADR-0002's `build_id` convention) | PR-2.2 |
| `source_system` + trigger | the triggering event or source; for automated loads, the pipeline/job run id and configuration version | PR-2.1, PR-2.4 |
| input lineage | for derived rows (a derived weight, a projected edge): the record ids or source dataset + version it was computed from | PR-2.3 |
| `supersedes_record_id` | for a correction: the record id(s) this append supersedes, forming an ordered supersession chain | CR-3.1, CR-3.2 |
| `change_reason_code` + `change_reason` | structured reason code plus free-text justification — every append, mandatory | CR-3.3 |
| `approved_by` | approval identity where the change class requires one (4-eyes); segregation of duties per NFR-2 | CR-3.3 |
| `processing_version` | ordering of versions per logical record, per ADR-0002 | — |
| content hash | per-record tamper evidence (hash over the record's content, chained or anchored per the realization) | AR-1.2, NFR-5 |

Correction semantics the model distinguishes (CR-3.5), using the two clocks:

- A **valid-time change** — late-arriving or amended source data: the world changed, or we
  learned when it changed. New append with the corrected valid window; `recorded_at` says when we
  learned it.
- A **restatement** — an earlier record was wrong: new append with `supersedes_record_id`
  pointing at the wrong row, same valid window, a reason code that marks it a restatement.

Both leave the superseded record intact and retrievable, and the full chain — original, each
correction, who, when, why — is exposed for any record (CR-3.2, CR-3.4).

### 4. Time: two clocks, bitemporal by contract

The model is bitemporal. Every row carries both clocks, and the read contract exposes both axes
(RP-4.1, CR-3.5, CR-3.6):

- **Valid time** (`valid_from` / `valid_to`): when the fact is true in the modelled world. UTC
  dates, half-open windows, close-and-open on change.
- **System time** (`recorded_at`, ordered by `processing_version`): when the platform recorded
  it. Assigned by the platform, UTC, synchronised clock (PR-2.7, NFR-4). Never edited.

Read contract, for any past date:

| question | read |
|---|---|
| What is true now? | current view: latest version per logical record, then the valid window over today |
| What was true on date D? | as-of-valid: latest version per logical record, valid window over D |
| What did we know at system time T? | as-of-system (time-travel): versions with `recorded_at ≤ T`, then the valid window (RP-4.1) |
| What did we originally record vs. what do we now believe? | as-of-system at the original time vs. current — both derivable, per CR-3.6 |

Every consumer that pins a number (a reproduction manifest, an enriched position) references the
graph by **as-of times and record ids**, so the exact input state is reconstructable (RP-4.2's
"input data as-of time(s)" and RP-4.6's linkage are satisfiable against this store).

### 5. The governed relationship vocabulary

`rel_type` values are a governed, closed vocabulary. Adding a type is a reviewed vocabulary
change (one line in the realization's enforcement artifact), not a schema change (NFR-3: the
vocabulary lives in version control and its changes are peer-reviewed). The initial vocabulary,
by family:

**Issuance and structure**

| rel_type | src → dst | weight | cardinality | meaning |
|---|---|---|---|---|
| `ISSUED_BY` | SECURITY → ENTITY | — | 1 current | issuer of a security |
| `HAS_UNDERLYING` | SECURITY → SECURITY | composition share | n | what a token is built on; chained for token-of-token depth |
| `RESOLVES_TO` | native instrument key → SECURITY | — | many keys → 1 security | maps a native on-chain id (contract address, market id, `registry:ilk`, `provider:package`) to one security |

**Holding and counterparties**

| rel_type | src → dst | weight | cardinality | meaning |
|---|---|---|---|---|
| `HELD_BY` | SECURITY → ENTITY | — | n | a holder of record, where holding is a reference fact; balances stay in the timeseries layer |

**Corporate structure**

| rel_type | src → dst | weight | cardinality | meaning |
|---|---|---|---|---|
| `SUBSIDIARY_OF` | ENTITY → ENTITY | ownership fraction | 1 current per parent | corporate parent; the ultimate parent is the top of the chain, reached by walking, never stored |
| `AFFILIATE_OF` | ENTITY → ENTITY | — | n | affiliation short of control |

**Classification and governance**

| rel_type | src → dst | weight | cardinality | meaning |
|---|---|---|---|---|
| `BELONGS_TO` | SECURITY / ENTITY → CONCEPT | — | 1 per concept kind | category membership: asset class, entity type, security type |
| `GOVERNED_BY` | ENTITY → CONCEPT | — | n | a rule set that applies to the entity |
| `SCORED_BY` | CONCEPT → CONCEPT | — | n | concept-to-concept pivot: an asset class to its risk model |
| `OWNED_BY` | CONCEPT → ENTITY | — | 1 | stewardship of a rule set |

**Succession (corporate actions)**

| rel_type | src → dst | weight | cardinality | meaning |
|---|---|---|---|---|
| `SUCCEEDED_BY` | SECURITY → SECURITY | conversion ratio | 1 | merger, redenomination (MKR → SKY at 1 : 24000); the old node takes a status version |
| `SPLIT_FROM` | SECURITY → SECURITY | split ratio | 1 | stock split / reverse split |
| `SPUN_OFF_FROM` | SECURITY → SECURITY | ratio | 1 | spin-off |
| `DISTRIBUTED_FROM` | SECURITY → SECURITY | ratio | n | rights issue, airdrop |
| `CONVERTS_TO` | SECURITY → SECURITY | conversion ratio | 1 | convertible conversion |
| `FORKED_FROM` | SECURITY → SECURITY | — | 1 | chain/token fork |

Boundary rules that keep the graph clean:

- **What a security is, its status, and how it succeeds another live in the graph.** Recurring
  cash events (dividends, coupons) and moving balances/ratios (rebasing) are quantities for the
  timeseries layer, referenced from the graph, never modelled as edge versions.
- **Derived weights are projections, not curated facts.** A market-determined mix (a prime's
  allocation) is derived from the position data and stamped with its block and its input lineage
  (PR-2.3), never hand-curated into the store.

### 6. Concepts

A concept is the fungible, shared part of the model: defined once, attributed to many nodes. It
stores almost nothing itself — a `kind` (asset class, entity type, capability, benchmark, …) and
a name. Its list or its rules are not stored on it; they are edges to where those live, so an
asset-class concept points to a risk model (`SCORED_BY`) and every security attributed to it
(`BELONGS_TO`) resolves the same one. Re-point the concept once and every member follows.

The existing reference vocabularies (`ref_*`) are the seed content: values that need
relationships of their own become concept nodes; values that remain terminal labels stay node
attributes. The `ref_*` tables remain the governed source until that port happens.

### 7. Classification, enrichment, and personal data

Classified fields are written onto a node when the record is created or versioned. The loader
uses rules where the shape is known (a token in `receipt_token` is a `RECEIPT_TOKEN`) and a
curated, sourced mapping where it is a judgment (`BUIDL-I` is a tokenised money-market fund).
Classification is never guessed: an unsourced value stays `UNKNOWN` and is surfaced, following
the pattern already established on the entity seed. Every curated value cites its source in the
provenance block.

**Personal data (GDPR).** The SECstore scope includes individual entities, and append-only
storage is in direct tension with erasure and rectification rights. The model resolves it the
way the Auditability PRD §6.1 prescribes: the master never holds direct identifiers for a
natural person. An individual is an ENTITY node keyed by a pseudonymous surrogate; the
identifying attributes live in a dedicated PII store under per-subject destroyable keys
(DP-1, DP-2), rectified there through the same correction mechanism (DP-4), and erased by key
destruction while the pseudonymised node and its edges are retained under the legal-obligation
exemption (DP-3). Access to the PII store is logged in the append-only audit log (DP-9). The PII
store's design is a follow-up; the model-level rule — no direct identifiers in the master — is
decided here.

## Auditability conformance

How the model answers the Auditability & Reproducibility PRD, item by item. "Model" means the
contracts above guarantee it in any realization; "realization" means the model provides the hook
and the chosen technology must enforce it; "platform" means it lives outside this store.

| PRD | requirement | where it lands |
|---|---|---|
| AR-1.1 | append-only, no in-place update/delete | model: both stores are append-only by contract |
| AR-1.2 | immutable records, tamper-evident | model requires a per-record content hash; chaining/anchoring mechanism is a realization choice |
| AR-1.3 | storage-level deletion prevention | realization: privilege revocation (as on the frozen masters) or storage immutability, independent of application logic |
| AR-1.4 | retraction as tombstone append | model: tombstone supersession, §2 |
| AR-1.5 | retention per data class | platform/realization: retention config; the model never deletes |
| AR-1.7 | exceptional hard deletion | platform: out-of-band, dual-authorised; interacts with DP-2 crypto-shredding for PII |
| PR-2.1 | record id, system timestamp, actor, trigger | model: provenance block, §3 |
| PR-2.2 | software version per append | model: `build_id` block, §3, per ADR-0002 |
| PR-2.3 | input lineage for derived values | model: lineage field on derived rows, §3 |
| PR-2.4 | run id / config version for automated appends | model: `source_system` + trigger, §3 |
| PR-2.5 | attributable principals | platform (identity system); the model stores the resolved actor |
| PR-2.6 | provenance as immutable and queryable as the data | model: provenance is part of the appended row |
| PR-2.7 | UTC, synchronised time | model: all timestamps UTC; synchronisation is platform (NFR-4) |
| CR-3.1 | correction references superseded record ids | model: `supersedes_record_id`, §3 |
| CR-3.2 | superseded records intact; ordered chain | model: append-only + supersession chain |
| CR-3.3 | reason code, corrector identity, approval | model: `change_reason_code` + `change_reason`, `actor`, `approved_by`, §3 |
| CR-3.4 | full correction history exposed | model: read contract over the chain |
| CR-3.5 | restatement vs valid-time change | model: two clocks + reason code, §3–4 |
| CR-3.6 | original or latest value for any past time | model: as-of-system vs current reads, §4 |
| RP-4.1 | as-of (time-travel) queries | model: as-of-system read contract, §4 |
| RP-4.2–4.8 | reproduction manifests, artifact retention, re-execution | platform: manifests reference this store by as-of times and record ids, which the model guarantees are stable |
| NFR-1..8 | audit log, least privilege, change control, clocks, hashing, availability, evidence export, performance | platform/realization; the model contributes stable ids, provenance, and the read contract the evidence package (NFR-7) is built from |
| DP-1..10 | personal data separation, crypto-shredding, rectification, access logging | model: no direct identifiers in the master (§7); the PII store design is a follow-up |

## Deprecations

The standalone-master build is superseded by this model:

- **Frozen (no further loads):** `security_master`, `security_instrument_bridge` (both shipped
  empty), `entity_master`, `entity_ref_codes`, `position_entity_link`, and the resolvers built on
  them (`holder_entity_resolver`, #614). Their migrations are immutable and stay in place;
  deprecation is stopped loads and new migrations, never edits to applied ones.
- **Ported:** the entity rows already seeded (the prime/protocol registry seed, #611, and the
  curated GLEIF issuers, VEC-525) become the first ENTITY nodes of the combined master. Their
  append-only, provenance, and UTC conventions carry over unchanged; the port fills the new
  provenance fields (actor, build, reason code) that the standalone masters lacked.
- **Tickets:** VEC-418, VEC-419, VEC-420, and VEC-524 were canceled against the frozen tables;
  their loading, resolution, and data-quality needs are re-scoped against this model.
- **Position resolution** moves from the bridge lookup to the model (`RESOLVES_TO`), with the
  same rule the bridge established: the resolution key is the instrument's native, globally
  unique id, never a house classifier, because `position_id` hashes it.

## Realization (explicitly not decided here)

The model above is the contract. The realization decisions below are open, to be made separately
and on evidence, and none of them may change the model:

1. **Node attribute representation.** Fixed typed columns versus a JSON document per node (join
   keys as real columns, attributes in a payload). The trade, recorded as input to that decision:

   | | JSON node store | Fixed columns |
   |---|---|---|
   | Change a node's shape | new payload key, no migration | a migration (`ALTER`) |
   | Integrity (`NOT NULL` / FK / `CHECK`) | loader-enforced | database-enforced |
   | Typing | untyped payload values | typed columns |
   | Versioning (SCD2) | by row; payload diff is manual | by row; column diff is direct |
   | Query on an attribute | JSON/GIN index, or promote to a column | plain B-tree index |

   Fixed columns win on integrity, typing, and query; the JSON form wins on shape-change cost for
   a small, churning, curated dataset. Whichever is chosen, the identity contract stays typed
   columns, and a payload field that becomes a join/filter key is promoted to a column.

2. **Engine.** Postgres (relational or JSON), a dedicated graph store, or a synlang runtime over
   projected atoms. The edge store and its resolution reads (issuer, look-through, concept
   resolution, inverses) have been verified on Postgres; a synlang expression of the same reads
   is the planned trial. The engine choice must preserve portability: the model serialises to
   JSON and translates between realizations. It must also implement the conformance hooks above —
   append-only enforcement independent of application logic (AR-1.3), tamper evidence (AR-1.2),
   and the as-of-system read (RP-4.1) — and those are selection criteria, not afterthoughts.

3. **Schema enforcement mechanism.** The standalone masters enforced typing through typed columns
   and FKs into the `ref_*` vocabularies. This model needs an equivalent per-node-type mechanism
   (required fields, field types, required edges) whatever the representation; concepts carrying
   a shape that constrains their members is the candidate design. Until it lands, the loader
   validates what the database no longer can — and that gap is a reason not to load at scale
   before the enforcement decision is made.

4. **Consumer surface.** Downstream joins (timeseries enrichment, connectors, UI) read resolved
   tabular views of the graph — current state per node, resolved look-through, and the as-of
   variants — rather than walking edges themselves. The view contract survives an engine change.

## Alternatives Considered

**Two separate masters (`entity_master` + `security_master`).** The previous direction, now
superseded. Set aside in favour of one node store and one id space, so relationships and
downstream joins reference a single master and the curation layer has one write surface.

**A single parent slot on the combined master (`parent_ref` + `parent_relationship_type`).** Set
aside: one slot forces a choice between an issuer link and an underlying link (syrupUSDC has
both), and cannot represent an LP token's two underlyings at all. This is the failure that
motivated the relationship store.

**Relationships embedded as `record_type = RELATIONSHIP` rows in the master.** Set aside: node
and relationship rows would share one table, each leaving the other's columns blank, and every
read would lean on `record_type` filters. Edges are their own store.

**Relationships as a dictionary on the node.** Set aside: a dict keyed by target holds one link
per target; there is nowhere clean for per-edge validity, weight, or provenance; ids in a blob
carry no constraints; reverse and graph reads scan every node.

**Valid time only, system time later.** The earlier draft carried valid time and deferred the
second clock. Set aside: the Auditability PRD makes as-of-system reads and the
restatement/valid-time distinction mandatory (RP-4.1, CR-3.5), retrofitting a clock onto loaded
data is exactly the kind of silent history rewrite the PRD forbids, and the store is empty today
— the cheapest moment this will ever have.

## Consequences

**Positive:**
- One node store and id space with a stable identity contract; the relationship store models
  one-to-many, many-to-many, security-to-security, multi-typing, and succession uniformly, with
  per-edge history, weight, and provenance.
- The model is auditable by construction: every append carries who, what, when, why, and with
  which software; corrections chain by reference; both clocks are queryable. Conformance to the
  Auditability PRD is a property of the contract, not of the engine.
- The model is engine-portable by construction: contracts and vocabulary are data, realizations
  are swappable, and the consumer surface is resolved views.
- Change lands as data, not schema: a new relationship type is a vocabulary change; a re-point,
  a category move, or a corporate action is rows.

**Negative / trade-offs (accepted):**
- Cross-row rules (endpoint kinds, single-valued cardinality) are loader- and DQ-enforced, not
  database-enforced; a missed check fails silently until the DQ view catches it.
- Per-type schema enforcement is weaker than the FK-validated standalone masters until the
  enforcement mechanism (Realization §3) is decided — accepted for now because the dataset is
  small and curated, and flagged as a gate before loading at scale.
- The provenance block is wider than the standalone masters carried (actor, build, lineage,
  reason code, hash): every loader pays that cost on every append, and the loader is the
  enforcement point for most of it.
- Bitemporal reads make the current-state views more involved (two-step resolution on two
  clocks); consumers must use the views, never the base rows.
- Every consumer that planned to join the standalone masters re-targets the combined master's
  views; the canceled tickets are re-scoped rather than resumed.

## Evolvability: how changes land

| Change | How it lands | Cost |
|---|---|---|
| New node attribute | on the node (mechanics per realization) | data, or one small migration |
| New relationship type | vocabulary change + endpoint rule | one reviewed line |
| New or changed relationship | insert an edge / close-and-open | data only |
| A recorded value was wrong | restatement append, `supersedes_record_id` + reason code | data only |
| Late-arriving fact | valid-time append, backdated window, honest `recorded_at` | data only |
| Corporate action | status version on the node + a succession edge | data only |
| New node kind | new `record_type` value + its shape | data + review |
| Categorical needs relationships | promote it to a CONCEPT node | data only |
| Engine or representation change | re-realize the same contracts; consumers keep the views | bounded by design |

## Follow-ups / Open Questions

- **Representation and engine decision** (Realization §1–2), informed by the synlang trial of the
  resolution reads and the recorded trade table; conformance hooks (AR-1.2, AR-1.3, RP-4.1) are
  selection criteria.
- **Per-node-type schema enforcement** (Realization §3): required fields, field types, required
  edges per kind; concepts-as-shapes is the candidate mechanism. Decide before loading at scale.
- **Tamper-evidence mechanism** (AR-1.2, NFR-5): internal hash chaining versus external
  anchoring — the Auditability PRD leaves this open; the model only requires the per-record hash.
- **Reason-code vocabulary** (CR-3.3): the structured `change_reason_code` set, governed the same
  way as the relationship vocabulary.
- **PII store design** (DP-1..DP-4): the store, key scheme, and erasure process for individual
  entities; the model-level rule (pseudonymous surrogate in the master) is decided.
- **Instrument key representation**: `RESOLVES_TO` keeps the native key as an edge endpoint;
  the alternative is promoting the instrument to a node kind of its own if it accrues attributes
  and inbound references. The native-key rule (no house classifier in the key) holds either way.
- **Weight semantics**: whether `rel_weight` needs an explicit basis label so unlike weights
  (composition share, ownership fraction, conversion ratio) are never summed together.
- **Re-scoped loads**: new tickets for the entity port, the security/instrument load, position
  resolution, and the coverage DQ, replacing VEC-418/419/420/524.
- **Concept vocabulary**: the preliminary list of concept kinds and which `ref_*` values promote
  to concept nodes.
- **Retention classes** (AR-1.5): per-data-class retention for the master and PII stores, set
  with Legal and Compliance.
