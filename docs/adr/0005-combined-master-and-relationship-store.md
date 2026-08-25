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

## In brief

- One node table (the **SECs master**) holds entities, securities, and concepts, discriminated by
  `record_type`. Nodes carry attributes; the identity contract is `id`, `record_type`, `chain_id`.
- A separate **relationship store** holds every link between nodes as a typed, directed,
  nullable-weighted edge with its own validity window, version, and provenance. Relationships are
  never columns on a node.
- **Concepts** are a third node kind: shared categories defined once and attributed to many nodes,
  pointing to rules or a list through ordinary edges. Categoricals that need relationships of
  their own become concepts, not lookup rows.
- Both stores are **append-only and versioned** per ADR-0002. Corrections and re-points are new
  rows; history is never mutated.
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
- **Versioning and provenance.** Nodes and relationships are append-only and versioned,
  consistent with ADR-0002.

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
  registry id), never from a house classification.
- **Attributes belong to the node** and differ by kind: names, `asset_class`, `security_type`,
  `currency`, `legal_name`, `lei`, `entity_type`, and so on. The attribute set is expected to
  evolve; how attributes are physically stored (typed columns, a document payload, node
  properties) is a realization choice, not part of this model.
- **Versioning is by row, append-only.** A change to a node is a new version carrying
  `processing_version`, a mandatory `change_reason`, and `source_system` provenance, per
  ADR-0002 and the conventions already shipped on the standalone masters. Validity dates are UTC.
- **Promotion rule.** A simple attribute lives on its node. It is promoted to a node of its own,
  joined by an edge, when it carries attributes itself or when other things need to relate to it.
  This is the test that turns a categorical into a concept.

### 2. The relationship contract

Every link between nodes lives in one relationship store, one row per edge. Relationships are
never columns on a node — the earlier `issuer_entity_id`, `parent_entity_id`, and
`ultimate_parent_id` columns are replaced by edges.

| field | meaning |
|---|---|
| `id` | deterministic composite `rel:rel_type:src_id:dst_id` — a referenceable handle; `rel_type` is included because one pair of nodes can share several link types |
| `src_id`, `dst_id` | the two node ids the edge joins (soft references to the master: SCD2 ids are non-unique, so resolution goes through the current view, not a row-level FK) |
| `src_kind`, `dst_kind` | the endpoint kinds, so vocabulary rules are checkable |
| `rel_type` | the kind of link, from the governed vocabulary below |
| `valid_from` / `valid_to` | when the link is true in the world (UTC dates, half-open); open `valid_to` means current |
| `rel_weight` | nullable: composition weight, ownership fraction, or a conversion ratio; null when the link carries no number |
| `processing_version`, `change_reason`, `source_system`, `created_at` | version and provenance, per ADR-0002 |

Semantics, independent of realization:

- **Directed.** Each edge is `src_id → dst_id`. Inverses (`ISSUES` from `ISSUED_BY`,
  `UNDERLYING_OF` from `HAS_UNDERLYING`) are derived at read time, never stored.
- **Weighted for look-through.** Exposure to a leaf is the sum over paths of the product of
  weights along each path; a 50/50 LP token is 50 % of each underlying.
- **Multi-typing by duplication.** Several relationships on one pair are several edges; an edge
  type that needs its own attribute cluster carries it on the edge.
- **Append-only, close-and-open.** A change closes the current row (a new version with
  `valid_to` set) and opens a new one. A re-point, a type change (`SUBSIDIARY_OF` →
  `AFFILIATE_OF`), an ended link, and a backdated late-learned link are all data changes.
- **Current state is a two-step read**: resolve the latest version per logical edge
  (`src_id`, `rel_type`, `dst_id`) first, then apply the valid-time window. The order matters —
  filtering on validity first drops the closing row and resurrects the superseded edge.
- **Cardinality is validated over current state, not at write.** A re-point always time-overlaps
  the edge it supersedes, so single-valued types (for example `ISSUED_BY`) are checked by a
  data-quality view over the resolved current state rather than a write-time exclusion.
- **Endpoint-kind and vocabulary rules are enforced on write** by the loader/validator, since
  they are cross-row: an `ISSUED_BY` must run security → entity.

### 3. The governed relationship vocabulary

`rel_type` values are a governed, closed vocabulary. Adding a type is a reviewed vocabulary
change (one line in the realization's enforcement artifact), not a schema change. The initial
vocabulary, by family:

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
  allocation) is derived from the position data and stamped with its block, never hand-curated
  into the store.

### 4. Concepts

A concept is the fungible, shared part of the model: defined once, attributed to many nodes. It
stores almost nothing itself — a `kind` (asset class, entity type, capability, benchmark, …) and
a name. Its list or its rules are not stored on it; they are edges to where those live, so an
asset-class concept points to a risk model (`SCORED_BY`) and every security attributed to it
(`BELONGS_TO`) resolves the same one. Re-point the concept once and every member follows.

The existing reference vocabularies (`ref_*`) are the seed content: values that need
relationships of their own become concept nodes; values that remain terminal labels stay node
attributes. The `ref_*` tables remain the governed source until that port happens.

### 5. Classification and enrichment happen in the load

Classified fields are written onto a node when the record is created or versioned. The loader
uses rules where the shape is known (a token in `receipt_token` is a `RECEIPT_TOKEN`) and a
curated, sourced mapping where it is a judgment (`BUIDL-I` is a tokenised money-market fund).
Classification is never guessed: an unsourced value stays `UNKNOWN` and is surfaced, following
the pattern already established on the entity seed.

### 6. Time

- **Valid time is part of the model now**: every node version and every edge carries when the
  fact is true in the world, as UTC dates, half-open windows, close-and-open on change.
- **System ordering** exists via `processing_version` and `created_at`.
- **Full bitemporal reads — "what did we believe on 1 June" — are not yet part of the model.**
  The write side records enough to add them (nothing is overwritten), but there is no
  as-of-recorded-time read contract. This is the known main gap and a named follow-up, required
  by the auditability direction (point-in-time reproducibility).

## Deprecations

The standalone-master build is superseded by this model:

- **Frozen (no further loads):** `security_master`, `security_instrument_bridge` (both shipped
  empty), `entity_master`, `entity_ref_codes`, `position_entity_link`, and the resolvers built on
  them (`holder_entity_resolver`, #614). Their migrations are immutable and stay in place;
  deprecation is stopped loads and new migrations, never edits to applied ones.
- **Ported:** the entity rows already seeded (the prime/protocol registry seed, #611, and the
  curated GLEIF issuers, VEC-525) become the first ENTITY nodes of the combined master. Their
  append-only, provenance, and UTC conventions carry over unchanged.
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
   JSON and translates between realizations.

3. **Schema enforcement mechanism.** The standalone masters enforced typing through typed columns
   and FKs into the `ref_*` vocabularies. This model needs an equivalent per-node-type mechanism
   (required fields, field types, required edges) whatever the representation; concepts carrying
   a shape that constrains their members is the candidate design. Until it lands, the loader
   validates what the database no longer can — and that gap is a reason not to load at scale
   before the enforcement decision is made.

4. **Consumer surface.** Downstream joins (timeseries enrichment, connectors, UI) read resolved
   tabular views of the graph — current state per node, resolved look-through — rather than
   walking edges themselves. The view contract survives an engine change.

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

## Consequences

**Positive:**
- One node store and id space with a stable identity contract; the relationship store models
  one-to-many, many-to-many, security-to-security, multi-typing, and succession uniformly, with
  per-edge history, weight, and provenance.
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
- As-of-recorded-time reads are not yet specified (Time §6); auditability conformance is
  incomplete until they are.
- Every consumer that planned to join the standalone masters re-targets the combined master's
  views; the canceled tickets are re-scoped rather than resumed.

## Evolvability: how changes land

| Change | How it lands | Cost |
|---|---|---|
| New node attribute | on the node (mechanics per realization) | data, or one small migration |
| New relationship type | vocabulary change + endpoint rule | one reviewed line |
| New or changed relationship | insert an edge / close-and-open | data only |
| Corporate action | status version on the node + a succession edge | data only |
| New node kind | new `record_type` value + its shape | data + review |
| Categorical needs relationships | promote it to a CONCEPT node | data only |
| Engine or representation change | re-realize the same contracts; consumers keep the views | bounded by design |

## Follow-ups / Open Questions

- **Representation and engine decision** (Realization §1–2), informed by the synlang trial of the
  resolution reads and the recorded trade table.
- **Per-node-type schema enforcement** (Realization §3): required fields, field types, required
  edges per kind; concepts-as-shapes is the candidate mechanism. Decide before loading at scale.
- **Instrument key representation**: `RESOLVES_TO` keeps the native key as an edge endpoint;
  the alternative is promoting the instrument to a node kind of its own if it accrues attributes
  and inbound references. The native-key rule (no house classifier in the key) holds either way.
- **As-of-recorded-time reads** (Time §6): specify the second clock and its read contract.
- **Weight semantics**: whether `rel_weight` needs an explicit basis label so unlike weights
  (composition share, ownership fraction, conversion ratio) are never summed together.
- **Re-scoped loads**: new tickets for the entity port, the security/instrument load, position
  resolution, and the coverage DQ, replacing VEC-418/419/420/524.
- **Concept vocabulary**: the preliminary list of concept kinds and which `ref_*` values promote
  to concept nodes.
