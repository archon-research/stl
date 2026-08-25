# ADR-0005: Combined Master (Entity, Security, Concept) as JSON Nodes, with a Separate Relationship Table

**Status**: Proposed (provisional; to be validated by building it in synlang before adoption)  
**Proposed**: @peter.simon, @yasanji.ratnaike  
**Date**: 2026-07-30  
**Deciders**: @yasanji.ratnaike, @peter.simon, @simon.bojeoutzen

> **Not a final decision.** This records a proposed direction, not a settled one. Before we adopt
> it we will build the master and the associations in synlang and try it end to end. The choice
> below is confirmed or revised on the strength of that trial, so read "decision" here as "current
> proposal pending that prototype."

## In brief

- One node table (the **SECs master**) holds entities, securities, and concepts, each as a JSON
  document: `id`, `record_type`, `chain_id` as real columns, the rest in a `payload`.
- A separate, relational **associations** table holds the links between nodes as directed,
  weighted edges, because links are one-to-many and many-to-many.
- **Concepts** are a third node kind: shared categories that point to rules or a list (an asset
  class points to a risk model), attached to nodes through associations.
- Provisional, to be tried in synlang before adoption.

## Context

We are building the master data that feeds enriched positions in `archon-research/stl`. The model
has to hold entities (issuers, protocol operators, primes), securities (the tokens we hold),
concepts (shared, fungible categories that point to rules or a list, such as an asset class or an
entity type), and the relationships between all of them — issuer, underlying, holder,
parent/subsidiary, category membership, and the resolution of a native on-chain id to a security.

Several requirements shaped the discussion:

- **One-to-many and many-to-many relationships.** syrupUSDC is issued by Maple *and* built on
  USDC; an LP token has two or more underlyings; an entity can be both an issuer and a holder.
  A single "parent" slot on a row cannot represent these.
- **Security-to-security relationships.** `HAS_UNDERLYING` links a security to another security,
  chained for token-of-token depth.
- **Type-specific attributes.** Different kinds of entity (fund, person, bank) and security carry
  different fields, and the set will grow.
- **Classification.** Fields with no on-chain source (`asset_class`, `security_type`, `currency`,
  and `entity_type`/`counterparty_role` moving from `UNKNOWN` to a curated value) have to be
  assigned by our own curated layer, not read from a raw table.
- **Versioning and provenance.** Nodes and relationships are append-only and versioned,
  consistent with ADR-0002 (`processing_version`, `build_id`).

We also take it as given that this schema will change often. The masters are a small, curated
dataset, so restructuring them is cheap **provided the join interface stays stable**: the keys
that timeseries data joins on are the contract that must not churn, while the attributes around
them are expected to evolve. That expectation is what drives the storage choice below.

The model has already been stress-tested on paper against the common change scenarios (renames,
reorgs, proxy upgrades, token-of-token nesting, corporate actions, cross-chain, rebasing) in the
design artifact. Building it in synlang is the next validation.

## Proposed framework (to validate in synlang)

The following is the framework we propose to try, not a settled choice.

### 1. One combined master for nodes (entity, security, concept), stored as JSON documents

Entities, securities, and concepts live in a single node table (the SECs master), discriminated
by `record_type ∈ {ENTITY, SECURITY, CONCEPT}`. Each node is stored as a **JSON document**:

- The **join keys are real, typed columns** — `id`, `record_type`, and `chain_id` — because they
  are what the associations table and the timeseries data join and index on. They must stay a
  stable, queryable contract.
- **Everything else lives in a JSON `payload`**: names, `asset_class`, `security_type`,
  `currency`, `legal_name`, `lei`, `entity_type`, and so on. Adding an attribute is a new key in
  the payload, so a shape change needs no migration. That is the property we are optimising for,
  given the schema will churn.

A **concept** is the fungible, shared part of the model: defined once and attributed to many rows
(an `asset_class`, an `entity_type`, a benchmark). It does not store its rules or its list; those
are pointers, held as associations to where they live, so an asset class points to a risk model
and a capability to a policy. Its payload holds only its own light fields (`kind`, name), and the
existing reference vocabularies become concept nodes rather than plain lookup rows. Node
versioning is by row: a change is a new row with its own payload.

### 2. A separate relationship table (edges): a directed, weighted graph

Relationships live in their own **relational** table beside the master, one row per edge. This
table stays tabular precisely because links are one-to-many and many-to-many, which a JSON dict
on the node cannot hold cleanly.

| column | meaning |
|---|---|
| `id` | `rel:`-prefixed composite of `rel_type` + `src_id` + `dst_id`, a referenceable handle derived from the row |
| `src_id`, `dst_id` | the two node ids the edge joins (soft references to the master; no FK, since SCD2 ids are non-unique — same pattern as the existing bridge and `entity_ref_codes`) |
| `rel_type` | the kind of link: `ISSUED_BY`, `HAS_UNDERLYING`, `SUBSIDIARY_OF`, `RESOLVES_TO`, `HELD_BY`, `BELONGS_TO`, `GOVERNED_BY`, `SCORED_BY`, … |
| `valid_from` / `valid_to` | when the link is true; `valid_to` blank means current. A re-point is a new version row, never an in-place update. |
| `rel_weight` | `FLOAT`, nullable: composition weight, ownership fraction, or a cost for graph algorithms |
| `processing_version`, `build_id`, `source_system` | version and provenance, per ADR-0002 |

- **Directed.** Each row is `src_id → dst_id`; a reverse link is its own row.
- **Weighted for look-through.** `rel_weight` (nullable) carries a composition weight or ownership
  fraction, so exposure is the product of weights along a path: a 50/50 LP is 50% of each
  underlying. None are loaded yet.
- **`rel_type` controlled by a `CHECK`, not a separate reference table.** The allowed values live
  in a `CHECK` on the relationship table. The endpoint kinds (e.g. `ISSUED_BY`: security → entity)
  are a cross-row rule enforced on write. Adding a type is a one-line change to the `CHECK`.
- **Relationship id.** A deterministic composite, `rel:rel_type:src_id:dst_id`. `rel_type` is in
  it because the same two nodes can share more than one link type, so `src_id:dst_id` alone is not
  unique.
- Each link being its own row is what gives one-to-many, many-to-many, and security-to-security
  relationships for free.
- **Concepts attach through the same table.** A security or entity is linked to a concept by an
  association (`BELONGS_TO`, `GOVERNED_BY`), and a concept links out to other concepts
  (`SCORED_BY`), so `CONCEPT` is a valid endpoint kind alongside `ENTITY` and `SECURITY`.
- **Hierarchies are edges, not columns.** A parent link is one `SUBSIDIARY_OF` / `PARENT_OF` edge;
  the ultimate parent is the top of that chain, reached by walking the edges.
- **Append-only, versioned.** The table is INSERT-only (`UPDATE` / `DELETE` / `TRUNCATE` revoked, the
  same as `security_master`, `entity_master`, and `security_instrument_bridge`). A correction or
  re-point is a new row with the next `processing_version`; nothing is mutated. The current edge is
  derived by a view that takes the latest version per `(src_id, rel_type, dst_id)` and then applies
  the valid-time window, so a closing version supersedes the open row it replaces. Single-valued
  links (e.g. `ISSUED_BY`, `REPRESENTS`) are checked by a data-quality view over the current state,
  not a write-time constraint, because an append-only re-point always overlaps the edge it supersedes.

### 3. Classification/enrichment happens in the load, into the payload

The classified fields are written into a node's `payload` when the record is created or versioned,
the same way `name` is filled from the token symbol. The loader uses rules where the shape is
known (a token in `receipt_token` is a `RECEIPT_TOKEN`) and the reference vocabularies plus a
curated mapping where it is a judgment (`BUIDL-I` is a tokenised money-market fund). It is not a
separate table or a downstream pass.

### 4. JSON for node attributes, relational for keys and edges

The flexible part (per-node attributes) is a JSON `payload`; the parts that are joined, indexed,
or many-to-many (the node join keys and the whole associations table) stay relational. A payload
field that later needs to be filtered or joined on is **promoted to a real column**, so the
"variable in JSON, fixed contract in columns" line moves as the query patterns firm up.

## Alternatives Considered

**Fixed tabular columns for the whole node.** The earlier direction, now not taken. Its benefits
are real and were the main argument against this choice: type-specific columns can be `NOT NULL`
with composite FKs into the reference vocabulary, values are typed, queries use plain B-tree
indexes, and SCD2 diffs and "as of" reads are straightforward. Set aside because the schema is
expected to change often and a fixed schema needs a migration for every shape change; the dataset
is small and curated, so a flexible payload is preferred, with the join keys kept as columns so
joins and indexes still work.

**Whole node as one JSON blob, keys included.** Set aside in favour of keeping `id`,
`record_type`, and `chain_id` as real columns. Burying the join keys in the blob makes every join
and lookup a JSON path extraction, loses clean indexes and the join contract, and is the failure
mode that makes document stores painful to query.

**Two separate masters (`entity_master` + `security_master`).** Considered; set aside in favour of
one node table and one id space, so relationships and downstream joins reference a single master
and the curation layer has one write surface.

**Relationships embedded as `record_type = RELATIONSHIP` rows in the master.** Set aside: node and
relationship rows would share one table, each leaving the other's columns blank, and every read
would lean on `record_type` filters. A separate edge table keeps them apart.

**Relationships as a JSON dict on the node.** Set aside: a dict keyed by target holds one link per
target, so one-to-many and many-to-many need arrays of types per target; there is nowhere clean
for per-edge fields (`valid_from`/`valid_to`, `rel_weight`); ids in the blob cannot carry a foreign
key or `CHECK`; reverse and graph reads scan every document. Edges must be their own table.

## Consequences

**Positive:**
- Schema changes to node attributes are free: a new field is a new payload key, no migration, no
  view rebuild. This is the churn property we wanted.
- One node table and id space, with the join keys as real columns, so the associations table and
  the timeseries joins work with normal indexes and a stable contract.
- The separate edge table still models one-to-many, many-to-many, and security-to-security
  cleanly, is a directed weighted graph, and carries per-edge history and provenance.
- Type-specific fields (fund, person, bank) need no new tables or columns; they are just payload
  keys on the nodes that have them.

**Negative / Trade-offs (accepted; these were the case for fixed columns):**
- Payload fields are **not database-enforced**: no `NOT NULL`, `FK`, or `CHECK` on them, so the
  loader must validate what a fixed schema would guarantee, and a missed check fails silently.
- Payload values are **untyped** (JSON text/number), so dates and decimals rely on loader
  discipline rather than column types.
- **Versioning within the payload is manual**: SCD2 still versions by row, but "what changed
  between versions" and "as of a date" on a payload field are application logic, not a column
  diff.
- **Querying a payload field** needs a JSON/GIN index or the field promoted to a column; it is not
  a plain B-tree lookup until promoted.
- Mitigations: keep every join/filter key as a real column, validate the payload in the loader,
  version by row, and promote hot fields to columns as query patterns settle.

## Evolvability: how changes land

| Change | How it lands | Cost |
|---|---|---|
| New node attribute | Add a key to the node's JSON `payload` | Data only, no migration |
| A payload field becomes a query/join key | Promote it to a real column | One migration, once |
| New relationship type | Add the value (and its endpoint rule) to the `CHECK` | One line |
| New or changed relationship | Insert an edge, or append a new version to re-point it | Data only |
| Corporate action (delist, split, merger) | Status version on the node; a succession edge | Data only |
| New node type | New `record_type` value | Data only |
| Wholesale restructure | Rebuild the small, curated dataset | Cheap **as long as the join keys stay stable** |

The trade behind this — a JSON node store versus fixed columns — is:

| | JSON node store (this framework) | Fixed columns (the alternative) |
|---|---|---|
| Change a node's shape | write a new payload key, no migration | a migration (`ALTER`) |
| Integrity (`NOT NULL` / `FK` / `CHECK`) | loader-enforced | database-enforced |
| Typing | untyped payload values | typed columns |
| Versioning (SCD2) | by row; payload diff is manual | by row; column diff is direct |
| Query on an attribute | JSON/GIN index, or promote to a column | plain B-tree index |

Fixed columns win on integrity, typing, versioning, and query; the JSON store wins on the one
thing that matters most for a churning, small dataset: shape changes cost nothing. This framework
accepts the loader-side integrity work for that.

## Follow-ups / Open Questions

- **Build it in synlang and try it first.** Express the SECs master and the associations in
  synlang, run a few real reads (issuer, underlyings, look-through, a concept attribution), and
  only then confirm or revise this ADR. Nothing here is adopted until that trial holds up.
- Which payload fields to promote to real columns first (the ones we filter or join on beyond the
  keys), and the validation/schema the loader enforces on the payload (e.g. a JSON schema).
- Exact `rel_type` vocabulary and endpoint-rule enforcement (a `CHECK` for allowed values, a
  trigger or the loader for the cross-row endpoint kinds).
- Whether native-id → security resolution is modelled as a `RESOLVES_TO` edge in this table or
  retained as the existing `security_instrument_bridge`.
- Confirm the stable join interface — `id`, `record_type`, `chain_id` — and treat it as the
  contract that must not churn while the payload evolves.
