# ADR-0005: Combined Master (Entity, Security, Concept) and a Relationship Store

**Status**: Accepted (data model) — storage representation and engine are follow-up decisions
**Proposed**: @peter.simon, @yasanji.ratnaike
**Date**: 2026-07-30 (proposed) · 2026-08-21 (accepted) · 2026-08-25 (model completed)
**Deciders**: @yasanji.ratnaike, @peter.simon, @simon.bojeoutzen

> **What is decided here is the data model, and only the data model.** One combined master of
> nodes and one relationship store of edges, with the contracts, vocabulary, shapes, and time
> semantics below. The model is deliberately technology-agnostic: any realization — fixed
> relational columns, JSON documents in Postgres, a graph store, a synlang atomspace — must
> implement these same contracts, and swapping the realization must not change the model. How
> the store is represented and which engine runs it are separate decisions, recorded under
> Realization below and resolved on their own evidence.
>
> The model conforms to the **STL Auditability & Reproducibility PRD** (v0.2, 18 Aug 2026).
> Requirement ids below (`AR-*`, `PR-*`, `CR-*`, `RP-*`, `DP-*`, `NFR-*`) reference that
> document; ids of the form `DM-*`, `SE-*`, `IN-*`, `CH-*` reference the **SECstore PRD**
> (v0.1, 18 Aug 2026). The Auditability conformance section maps the model to the former item
> by item. Where this ADR and either PRD conflict, the PRD wins and this ADR is wrong.

## In brief

- One node store (the **SECs master**) holds the graph's nodes, discriminated by `record_type`.
  Core kinds: `ENTITY`, `SECURITY`, `CONCEPT`, `INSTRUMENT`, `SOURCE`; `ACCOUNT` proposed,
  `EVENT` deferred. Node ids are opaque and stable; native keys and public identifiers are
  lookups, never ids.
- A separate **relationship store** holds every link as a typed, directed, weighted edge with
  its own validity window, version, and provenance. Edge identity includes an `edge_seq` so
  deliberate twins coexist. Weights carry a mandatory basis and are exact decimals.
- The **relationship vocabulary** is itself governed reference data: a closed set in six
  families, each type with endpoint kinds, cardinality, and a maturity tier.
- **Concepts carry shapes**: per-type schemas (required fields, required edges, permitted
  targets, severity, maturity) that members inherit along `NARROWER_THAN`. Structural
  violations are rejected at write; semantic gaps are recorded in `node_validity` and excluded
  from metrics.
- Both stores are **append-only and bitemporal**: valid time plus system time on every row, so
  the model answers "what was true" and "what did we know" for any date (AR-1.1, RP-4.1,
  CR-3.5). Corrections reference what they supersede, with structured reason and approval
  (CR-3.1..3.4). Content hashing runs from the first append (AR-1.2).
- Consumers never walk the graph: they read the **pivot** — generated tabular views shaped
  like the old masters (`dim_security`, `dim_entity`, `dim_instrument`, look-through closure),
  as-of-capable and joinable against the timeseries at block time.
- The standalone masters (`entity_master`, `security_master`, `security_instrument_bridge`,
  `entity_ref_codes`, `position_entity_link`) are **superseded and frozen** — see Deprecations.

## Context

We are building the master data that feeds enriched positions in `archon-research/stl`. The model
has to hold entities (issuers, protocol operators, primes), securities (the tokens we hold),
concepts (shared, fungible categories such as an asset class or an entity type), the instruments
that resolve positions to securities, the sources data comes from, and the relationships between
all of them — issuer, underlying, holder, parent/subsidiary, category membership, succession
through corporate actions, and native-id resolution.

Several requirements shaped the model:

- **One-to-many and many-to-many relationships.** syrupUSDC is issued by Maple *and* built on
  USDC; an LP token has two or more underlyings; an entity can be both an issuer and a holder.
  A single "parent" slot on a row cannot represent these.
- **Security-to-security relationships.** `HAS_UNDERLYING` links a security to another security,
  chained for token-of-token depth, so look-through is a walk over weighted edges.
- **Multiple distinct relationships between the same two nodes** (DM-7), and edge duplication
  for multi-typing and attribute clusters (DM-6) — which forces edge identity beyond the triple.
- **Type-specific attributes and schema enforcement** (SE-1..SE-5): different kinds of entity
  and security carry different required fields and edges, loose where forming and strict where
  settled.
- **Classification.** Fields with no on-chain source are assigned by our own curated layer,
  never guessed, always sourced.
- **Auditability and reproducibility.** Reference data is an input to every published number,
  so this store must meet the Auditability PRD in its own right, consistent with ADR-0002.
- **Consumers are tabular.** Downstream joins (position enrichment, Time-Series API, UI panels)
  need dimension-shaped tables against TigerData, not graph traversal (IN-1..IN-3).

The model was stress-tested on paper against the common change scenarios (renames, reorgs, proxy
upgrades, token-of-token nesting, corporate actions, cross-chain deployments, rebasing), and the
edge store with its resolution reads was verified against an ephemeral Postgres over the live
held book. On that basis the combined master was accepted; the standalone two-master build is
superseded.

## Decision: the data model

### 1. Identity: three identifier classes, never conflated

Every thing in the model is addressed by exactly one of three identifier classes, and they are
never mixed:

1. **Node ids** — `id` on every node: **opaque, stable, house-assigned, assigned once**. A node
   id is never derived from a public identifier, a symbol, or a name, because every public
   identifier eventually gets reused, corrected, or reassigned, and an id derived from one
   breaks with it. Ids are kind-prefixed for legibility (`em-`, `sec-`, `concept-`, `inst-`,
   `src-`), and a mnemonic fragment is allowed (`em-prime-1` derives from the registry id,
   `em-issuer-circle` is readable) but carries no meaning the model relies on. The entity ids
   already seeded (#611, VEC-525) conform and stand unchanged.
2. **Native instrument keys** — `instrument_key`: the instrument's native, globally unique,
   namespaced identifier (contract address, protocol-emitted market id, `registry:ilk`,
   `provider:package`). Never a house classifier — `position_id` hashes this key, so nothing
   we control may appear in it. The native key is how the position stream addresses the graph.
3. **Public identifiers** — ISIN, FIGI, CUSIP, LEI, ticker: **alias lookups, never ids**. They
   live in an **alias register** — `(id_scheme, id_value, node_id, validity, provenance)` —
   the successor of `entity_ref_codes`' job, extended to securities. An alias re-pointing is a
   versioned append like any other; a node is reachable by any of its aliases without any alias
   being load-bearing.

Every stored row additionally carries a unique **`record_id`** — the version-level identity a
correction or a reproduction manifest references (PR-2.1, RP-4.6).

### 2. The node contract

Nodes live in a single store, discriminated by `record_type`. The kinds, each admitted by the
promotion test — *does it carry attributes of its own, and do other things point at it?* (DM-2):

| kind | status | why it is a node |
|---|---|---|
| `SECURITY` | core | the curated instrument-of-record: classification, currency, issuer, status |
| `ENTITY` | core | legal persons and operators: type, domicile, LEI, roles |
| `CONCEPT` | core | shared categories and rule pointers; carries edges and a shape of its own |
| `INSTRUMENT` | core | the native on-chain key made addressable: it carries attributes (address, chain, decimals, venue, first-seen block) and two products point at it (positions and the Time-Series API), so by the promotion test it promotes. Keyed by `instrument_key`. |
| `SOURCE` | core | a feed or dataset: reliability tier, licence, redistribution terms — required so per-series provenance and exposability are auditable |
| `ACCOUNT` | proposed | a book that can hold things (prime, vault, custody account); what holdings-by-dimension groups on, and the source endpoint of `ALLOCATES`. Ratify when the first account-grained consumer lands. |
| `EVENT` | deferred | dated corporate actions stay in an events table the graph references; an event promotes only when one event spans several securities and carries a ratio |

- **Identity is the stable contract**: `id`, `record_type`, `chain_id` (plus `instrument_key`
  on INSTRUMENT). These are what the relationship store and the timeseries join on.
- **Attributes belong to the node** and differ by kind; which attributes a kind requires is
  declared by its shape (§6). How attributes are physically stored (typed columns, a document
  payload, node properties) is a realization choice.
- **Nodes are editable.** Any attribute of any node may be changed at any time by an authorised
  curator — and, in future, through the UI (UI-5) — gated by the type-scoped `owner_role` of the
  governing shape (AC-3). Editability and immutability coexist because an edit *lands* as a new
  version: the row is appended, the history is never mutated, and the pivot picks the change up
  on its next resolution.
- **Versioning is by row, append-only** (AR-1.1), carrying the provenance block of §4.
- **Natural persons carry no direct identifiers here** (DP-1): an individual is an ENTITY node
  keyed by a pseudonymous surrogate; identifying attributes live in a dedicated PII store under
  destroyable keys. See §8.

### 3. The relationship contract

Every link between nodes lives in one relationship store, one row per edge. Relationships are
never columns on a node — the earlier `issuer_entity_id`, `parent_entity_id`, and
`ultimate_parent_id` columns are replaced by edges.

| field | meaning |
|---|---|
| `id` | deterministic composite `rel:rel_type:src_id:dst_id:edge_seq` — the logical edge |
| `edge_seq` | discriminator (default 1) so deliberately duplicated edges — DM-6 multi-typing and per-edge attribute clusters — **coexist** instead of silently superseding their twin |
| `record_id` | unique, stable id of this stored row (this version of the edge) — what corrections and manifests reference (PR-2.1) |
| `src_id`, `dst_id` | the two node ids the edge joins (soft references: SCD2 ids are non-unique, so resolution goes through the current view, not a row-level FK) |
| `src_kind`, `dst_kind` | the endpoint kinds, so vocabulary rules are checkable |
| `rel_type` | the kind of link, from the governed vocabulary (§5) |
| `valid_from` / `valid_to` | valid time: when the link is true in the world (UTC dates, half-open); open `valid_to` means current |
| `rel_weight` | nullable **exact decimal** (reference realization `numeric(30,18)`, never a binary float — a float weight cannot reproduce look-through bit-for-bit, RP-4.4) |
| `weight_basis` | **mandatory whenever `rel_weight` is set**: `composition_share`, `ownership_fraction`, `conversion_ratio`, … — an unlabelled 0.6 is not addable, and unlike bases must never be summed (DM-5) |
| provenance block | §4: system time, actor, software version, lineage, correction fields |

Semantics, independent of realization:

- **Directed.** Each edge is `src_id → dst_id`. Inverses (`ISSUES` from `ISSUED_BY`,
  `UNDERLYING_OF` from `HAS_UNDERLYING`) and closures (ultimate parent, look-through) are
  **derived, never stored** — every derived answer has exactly one source of truth.
- **Weighted for look-through.** Exposure to a leaf is the sum over paths of the product of
  weights along each path, defined only within one `weight_basis`.
- **Append-only, close-and-open** (AR-1.1, AR-1.4). A change closes the current row and opens a
  new one; a re-point, a type change, an ended link, and a backdated late-learned link are all
  data changes. A retraction is a tombstone append that supersedes the retracted row.
- **Current state is a two-step read**: latest version per logical edge
  (`src_id`, `rel_type`, `dst_id`, `edge_seq`) first, then the valid-time window. The order
  matters — filtering on validity first resurrects superseded edges.
- **Cardinality is validated over current state, not at write** (a re-point always
  time-overlaps the edge it supersedes); single-valued types are checked by a data-quality
  rule over the resolved current state. Per D-5's separation, such data-quality validation
  rules live in OpenMetadata with pointers from the store (AU-4): schemata (§6) govern at the
  write boundary and live with the store; DQ rules observe the resolved state and live in
  OpenMetadata.
- **Endpoint-kind and vocabulary rules are enforced on write** by the loader/validator, since
  they are cross-row.

### 4. Provenance and corrections (both stores)

Every append — node version, edge row, alias row, shape version — carries the same immutable
provenance block, per the Auditability & Reproducibility PRD §5.2–5.3 and ADR-0002:

| field | meaning | PRD |
|---|---|---|
| `record_id` | unique, stable id of this stored row | PR-2.1 |
| `recorded_at` | system time (transaction time): when the platform recorded it, UTC, synchronised clock, platform-assigned, monotonic per logical record | PR-2.1, PR-2.7 |
| `actor` | the authenticated principal (human or non-shared service account) that caused the append | PR-2.1, PR-2.5 |
| `build_id` / software version | source commit, build/artifact version, image digest (per ADR-0002) | PR-2.2 |
| `source_system` + trigger | the triggering event or source; for automated loads, the pipeline/job run id and configuration version | PR-2.1, PR-2.4 |
| input lineage | for derived rows: the record ids or source dataset + version it was computed from | PR-2.3 |
| `supersedes_record_id` | for a correction: the record id(s) this append supersedes, forming an ordered supersession chain | CR-3.1, CR-3.2 |
| `change_reason_code` + `change_reason` | structured reason code plus free-text justification — every append, mandatory | CR-3.3 |
| `approved_by` | approval identity where the change class requires one (4-eyes, NFR-2) | CR-3.3 |
| `processing_version` | ordering of versions per logical record, per ADR-0002 | — |
| content hash | per-record tamper evidence, **running from the first append** — a chain started later only proves integrity from activation onward, which is when auditors stop caring. The store is empty today; this is the cheapest moment it will ever have. | AR-1.2, NFR-5 |

Correction semantics the model distinguishes (CR-3.5), using the two clocks:

- A **valid-time change** — late-arriving or amended source data: new append with the corrected
  valid window; `recorded_at` says when we learned it.
- A **restatement** — an earlier record was wrong: new append with `supersedes_record_id`
  pointing at the wrong row, same valid window, a reason code that marks it a restatement.

Both leave the superseded record intact and retrievable, and the full chain — original, each
correction, who, when, why — is exposed for any record (CR-3.2, CR-3.4).

**Time: two clocks, bitemporal by contract** (RP-4.1, CR-3.5/3.6). Every row carries valid time
(`valid_from`/`valid_to`) and system time (`recorded_at`, ordered by `processing_version`). The
read contract, for any past date:

| question | read |
|---|---|
| What is true now? | current view: latest version per logical record, valid window over today |
| What was true on date D? | as-of-valid: latest version, valid window over D |
| What did we know at system time T? | as-of-system: versions with `recorded_at ≤ T`, then the valid window (RP-4.1) |
| Original vs corrected value? | as-of-system at the original time vs current (CR-3.6) |

Every consumer that pins a number references the graph by as-of times and record ids, so the
exact input state is reconstructable (RP-4.2, RP-4.6).

### 5. The governed relationship vocabulary

The `rel_type` vocabulary is **itself reference data**, governed to the same bar as the `ref_*`
lists: a single authoritative, versioned artifact (seeded from the table below), anchored where
possible to external practice rather than house judgment, changed only by a reviewed migration
(NFR-3). The ADR table is the ratification snapshot; the governed list is the source of truth.
Each type carries endpoint kinds, cardinality, weight basis, and a maturity tier — `ratified`
(enforced now) or `draft` (named, awaiting its first consumer). Types are `SCREAMING_SNAKE`;
concept ids are `snake_case` with display labels as attributes, so near-duplicates cannot creep
into a governed set.

**Issuance and composition**

| rel_type | src → dst | weight basis | card. | maturity | meaning |
|---|---|---|---|---|---|
| `ISSUED_BY` | SEC → ENT | — | 1 current | ratified | issuer of a security |
| `HAS_UNDERLYING` | SEC → SEC | composition_share | n | ratified | what a token is built on; chained for token-of-token depth |
| `RESOLVES_TO` | INST → SEC | — | many INST → 1 SEC | ratified | native key resolution: the position stream's entry point into the graph |
| `BRIDGED_BY` | SEC → ENT | — | n | draft | which bridge a wrapped asset came through — the "uses a given bridge" look-through |

**Holding and allocation**

| rel_type | src → dst | weight basis | card. | maturity | meaning |
|---|---|---|---|---|---|
| `HELD_BY` | SEC → ENT | — | n | ratified | a holder of record, where holding is a reference fact; balances stay in the timeseries layer |
| `ALLOCATES` | ACCT → SEC | value_share | n | draft, **derived-only** | block-stamped projection of `allocation_position`; never curated, never appended by hand (see boundary rules) |
| `COUNTERPARTY_OF` | ACCT → ENT | — | n | draft | standing bilateral relationship; role in the edge's attribute cluster |
| `OPERATED_BY` | ACCT → ENT | — | 1 | draft | whose book this is |

**Corporate structure**

| rel_type | src → dst | weight basis | card. | maturity | meaning |
|---|---|---|---|---|---|
| `SUBSIDIARY_OF` | ENT → ENT | ownership_fraction | 1 current per parent | ratified | corporate parent; the ultimate parent is the top of the chain, derived by walking, never stored |
| `AFFILIATE_OF` | ENT → ENT | — | n | ratified | affiliation short of control |

**Classification and governance**

| rel_type | src → dst | weight basis | card. | maturity | meaning |
|---|---|---|---|---|---|
| `BELONGS_TO` | SEC / ENT / ACCT → CONCEPT | — | 1 per concept class | ratified | category membership: asset class, entity type, security type |
| `NARROWER_THAN` | CONCEPT → CONCEPT | — | 1 | ratified | taxonomy hierarchy; also how a concept **inherits its parent's shape** instead of re-declaring it (§6) |
| `GOVERNED_BY` | ENT / ACCT → CONCEPT | — | n | ratified | which rule set applies |
| `SCORED_BY` | CONCEPT → CONCEPT | — | n | ratified | concept-to-concept pivot: an asset class to its risk model |
| `OWNED_BY` | CONCEPT → ENT | — | 1 | ratified | stewardship of a rule set |
| `RATED_BY` | SEC / ENT → CONCEPT | — | n | draft | an external rating attribution |
| `SOURCED_FROM` | any → SOURCE | — | n | ratified | which feed/dataset a curated fact came from, where lineage points at a source rather than records |

**Succession (corporate actions)**

| rel_type | src → dst | weight basis | card. | maturity | meaning |
|---|---|---|---|---|---|
| `SUCCEEDED_BY` | SEC → SEC | conversion_ratio | 1 | ratified | merger, redenomination (MKR → SKY at 1 : 24000); the old node takes a status version |
| `SPLIT_FROM` | SEC → SEC | conversion_ratio | 1 | ratified | stock split / reverse split |
| `SPUN_OFF_FROM` | SEC → SEC | conversion_ratio | 1 | draft | spin-off |
| `DISTRIBUTED_FROM` | SEC → SEC | conversion_ratio | n | draft | rights issue, airdrop |
| `CONVERTS_TO` | SEC → SEC | conversion_ratio | 1 | draft | convertible conversion |
| `FORKED_FROM` | SEC → SEC | — | 1 | draft | chain/token fork |

Boundary rules that keep the graph clean:

- **What a security is, its status, and how it succeeds another live in the graph.** Recurring
  cash events (dividends, coupons) and moving balances/ratios (rebasing) are quantities for the
  timeseries layer, referenced from the graph, never modelled as edge versions.
- **Derived weights are projections, not curated facts.** `ALLOCATES` — a market-determined
  mix — exists only as a block-stamped projection of the position data, carrying its input
  lineage (PR-2.3). This is the split CH-3 (as revised) asks for: the graph provides the
  *links*, while frequent(ish) weight movements from price and composition changes are
  reflected externally in the operational store and read through the projection — never as a
  hand-curated append.
- **Inverses are read-side names**, defined in the resolution layer, never in the vocabulary.

### 6. Concepts, shapes, and rules

**Concepts.** A concept is the fungible, shared part of the model: defined once, attributed to
many nodes through `BELONGS_TO`, arranged in a taxonomy by `NARROWER_THAN`. It stores almost
nothing itself — a `concept_class` (asset class, entity type, capability, benchmark, …), an id,
a label. The existing `ref_*` vocabularies are the seed content: values that need relationships
of their own become concept nodes; values that remain terminal labels stay node attributes. The
`ref_*` tables remain the governed source until that port happens.

**Shapes: concepts are the schema** (SE-1..SE-5). A concept may carry a **shape** that
constrains its members — this is the model's per-node-type schema enforcement, and it is data,
not engine configuration:

| shape field | meaning |
|---|---|
| `applies_to` | a node kind, or membership of this concept (`BELONGS_TO` activates it) |
| `required_fields` / `field_types` | attributes that must be populated, and their datatypes — a token cannot exist without address and chain (SE-1, SE-3) |
| `required_edges` | `(rel_type, direction, min, max, target_kind)` — "exactly one `BELONGS_TO` → instrument subtype" |
| `permitted_targets` | an enumerated concept list, or *anything `NARROWER_THAN` concept X* — the form that survives new members without an edit |
| `severity` | `REQUIRED` rejects the write · `EXPECTED` records a validity gap and blocks metric publication · `ADVISORY` reports only (SE-2) |
| `maturity_tier` | `DRAFT` (nothing enforced) · `GOVERNED` (enforced at declared severity) · `FROZEN` (changes need an ADR) — "loose where forming, strict where settled" (SE-4) |
| `owner_role` | which role may edit this shape and the types it governs — the hook that makes access control type-scoped (AC-3) |

Five rules bound the cascade: declarations are **local** (a concept never restates its
parent's rules); inheritance is by **walking `NARROWER_THAN`**, never by copying; closed target
sets are declared as a **subtree, not a list**, wherever possible; shape resolution has a
**depth ceiling** and the full effective shape per type is a **generated, reviewable artifact**
in the schema register, so the cascade is read in a diff, not discovered when a load breaks;
and severity separates *cannot store* from *not yet complete*.

**The SE-2 split — reject structural, record semantic.** A structurally invalid record (a token
with no address or chain) is rejected at write. A semantically incomplete one (a real holding
whose subtype nobody has curated yet) is **stored, flagged in `node_validity`, and excluded
from metrics** — losing the holding is worse than holding it incompletely, and the chain is a
primary source that does not ask permission before emitting new instruments (CH-1).
`node_validity` — per node: which shape requirements are unmet — is a first-class read artifact
of the model, not a side report.

**Rules: what a concept's pointer resolves to.** A concept that stands for a rule set (a risk
model, a policy, a mandate) does not store the rules; per D-5 they live in the rules system
(Synome / Synlang / code), "with pointers to them in SECstore" — while the schemata that govern
the store itself (shapes, the vocabulary) live in or adjacent to the store, per the same
decision. The model's contract is that the pointer
terminates somewhere real: a rule-class concept **must carry an external reference** —
`(ref_system, ref_address, ref_version)` — enforced by the rule-concept shape, so resolving
"syrupUSDC's risk model" yields the concept, its owner (`OWNED_BY`), *and* a versioned address
where the executable content lives. A rule change is a new concept version with a new
`ref_version` — auditable like everything else. Shapes themselves are versioned, append-only
data in the engine-neutral manifest; the validator is generated per realization.

### 7. Classification and enrichment happen in the load

Classified fields are written onto a node when the record is created or versioned. The loader
uses rules where the shape is known (a token in `receipt_token` is a `RECEIPT_TOKEN`) and a
curated, sourced mapping where it is a judgment (`BUIDL-I` is a tokenised money-market fund).
Classification is never guessed: an unsourced value stays `UNKNOWN`, is surfaced through
`node_validity`, and every curated value cites its source in the provenance block (or via
`SOURCED_FROM` where the source is a governed SOURCE node).

### 8. Personal data (GDPR)

The scope includes individual entities, and append-only storage is in direct tension with
erasure and rectification rights. The model resolves it as the Auditability PRD §6.1
prescribes: the master never holds direct identifiers for a natural person. An individual is an
ENTITY node keyed by a pseudonymous surrogate — **enforced by the person-entity shape** — with
identifying attributes in a dedicated PII store under per-subject destroyable keys (DP-1,
DP-2), rectified there through the same correction mechanism (DP-4), and erased by key
destruction while the pseudonymised node and its edges are retained under the legal-obligation
exemption (DP-3). Content hashes are computed over stored ciphertext where PII-adjacent, so key
destruction never breaks a hash chain. Access to the PII store is logged in the append-only
audit log (DP-9).

### 9. Interface to the position stream

The position work (`position_id`, the `position_state` spine, the per-protocol materializers)
is **not absorbed by this model and does not change**. Positions are quantities; this store is
reference. They meet at three seams, and only these:

1. **Instrument.** A position carries `instrument_key` (hashed into `position_id`, so native
   and classifier-free by construction). Enrichment resolves it through the INSTRUMENT node's
   `RESOLVES_TO` edge to a security, then reads classification, issuer, and look-through from
   the graph. Nothing is stamped onto the position row; resolution is live against the pivot,
   so a reclassification never rewrites positions.
2. **Holder.** A position's holder (wallet address or prime id) resolves to an ENTITY through
   the alias register (§1.3) — the successor of the frozen `entity_ref_codes` path.
3. **Time.** Positions are block-height keyed; graph validity is calendar-dated. The only
   correct join is through the block-time dimension (`block_meta`: block → UTC timestamp), so
   graph state is read as-of the block's UTC date — and as-of system time where reproducibility
   requires "what we knew then". A read that resolves the graph at `now()` while scanning last
   quarter's positions applies today's issuer to yesterday's holding, silently.

### 10. The pivot: the consumer read contract

Consumers never traverse the graph. SECstore does not exist in isolation — its purpose is to
feed operational engines, rule stores, and business logic (D-6), and the operational side links
back to it by reference (D-5). The model's read surface is a set of **generated tabular
views** — the pivot (IN-1) — resolved once per graph change, not per query, and shaped so
downstream SQL treats reference data as ordinary dimensions (deliberately: they look like the
old separate masters again, as regenerable read models rather than write surfaces):

| view | grain | what it serves |
|---|---|---|
| `dim_security` | one row per security, current + as-of variants | classification, issuer, status — the old `security_master_current` shape |
| `dim_entity` | one row per entity, current + as-of | the old `entity_master_current` shape, with resolved corporate-structure fields (derived, not stored) |
| `dim_instrument` | one row per instrument key → security | the resolution join for positions and the Time-Series API; carries the unique-current guarantee the old bridge held |
| `dim_concept` | one row per concept, with taxonomy path | cluster/grouping dimensions (UI-3/UI-4) |
| `edges_current` (+ as-of) | one row per resolved current edge | the general relationship read |
| `fact_lookthrough` | security → leaf, with resolved weight product, per graph version | look-through exposure without recursive SQL downstream |
| `node_validity` | node × unmet shape requirement | the SE-2 semantic-gap surface; what metrics exclude |
| alias lookup | `(id_scheme, id_value)` → node | public-identifier and holder-address resolution |

Contract properties: every view is keyed on the identity contract (§1), every view has an as-of
form (valid time, and system time where auditability requires it), all views are **regenerable
projections** — dropping and rebuilding them loses nothing — and view rows carry the graph
version / record ids they were resolved from, so a manifest can pin them (RP-4.2, RP-4.6). The
views join TigerData hypertables on stable keys at the block's UTC date (§9.3). Materialization
mechanics (tables vs views, refresh, distribution across nodes) are realization; the view set,
grains, keys, and as-of semantics are model.

## Auditability conformance

How the model answers the Auditability & Reproducibility PRD. "Model" means the contracts above
guarantee it in any realization; "realization" means the model provides the hook and the chosen
technology must enforce it; "platform" means it lives outside this store.

| PRD | requirement | where it lands |
|---|---|---|
| AR-1.1 | append-only, no in-place update/delete | model: both stores append-only by contract |
| AR-1.2 | immutable records, tamper-evident | model: per-record content hash **from the first append**; chaining/anchoring mechanism is a realization choice |
| AR-1.3 | storage-level deletion prevention | realization: privilege revocation or storage immutability, independent of application logic |
| AR-1.4 | retraction as tombstone append | model: tombstone supersession, §3 |
| AR-1.5 | retention per data class | platform/realization; the model never deletes; derived pivot rows are regenerable and need only live while manifests cite them |
| AR-1.7 | exceptional hard deletion | platform: out-of-band, dual-authorised; interacts with DP-2 crypto-shredding |
| PR-2.1 | record id, system timestamp, actor, trigger | model: provenance block, §4 |
| PR-2.2 | software version per append | model: `build_id` block, §4, per ADR-0002 |
| PR-2.3 | input lineage for derived values | model: lineage on derived rows (`ALLOCATES`, pivot outputs), §4–5 |
| PR-2.4 | run id / config version for automated appends | model: `source_system` + trigger, §4 |
| PR-2.5 | attributable principals | platform (identity system); the model stores the resolved actor |
| PR-2.6 | provenance as immutable and queryable as the data | model: provenance is part of the appended row |
| PR-2.7 | UTC, synchronised time | model: all timestamps UTC; synchronisation is platform (NFR-4) |
| CR-3.1 | correction references superseded record ids | model: `supersedes_record_id`, §4 |
| CR-3.2 | superseded records intact; ordered chain | model: append-only + supersession chain |
| CR-3.3 | reason code, corrector identity, approval | model: `change_reason_code` + text, `actor`, `approved_by`, §4 |
| CR-3.4 | full correction history exposed | model: read contract over the chain |
| CR-3.5 | restatement vs valid-time change | model: two clocks + reason code, §4 |
| CR-3.6 | original or latest value for any past time | model: as-of-system vs current reads, §4 |
| RP-4.1 | as-of (time-travel) queries | model: as-of-system read contract, §4; pivot as-of views, §10 |
| RP-4.2–4.8 | reproduction manifests, artifact retention, re-execution | platform: manifests reference this store by as-of times and record ids; §3's exact-decimal weights keep graph closures bit-for-bit reproducible (RP-4.4) |
| NFR-1..8 | audit log, least privilege, change control, clocks, hashing, availability, evidence export, performance | platform/realization; the model contributes stable ids, provenance, type-scoped `owner_role` (AC-3 hook), and the pivot the evidence package (NFR-7) reads |
| DP-1..10 | personal data separation, crypto-shredding, rectification, access logging | model: no direct identifiers in the master, shape-enforced; ciphertext hashing preserves chains (§8); the PII store design is a follow-up |

## Deprecations

The standalone-master build is superseded by this model:

- **Frozen (no further loads):** `security_master`, `security_instrument_bridge` (both shipped
  empty), `entity_master`, `entity_ref_codes`, `position_entity_link`, and the resolvers built
  on them (`holder_entity_resolver`, #614). Their migrations are immutable and stay in place;
  deprecation is stopped loads and new migrations, never edits to applied ones.
- **Ported:** the entity rows already seeded (the prime/protocol registry seed, #611, and the
  curated GLEIF issuers, VEC-525) become the first ENTITY nodes — their ids conform to §1 and
  stand unchanged. `entity_ref_codes`' resolution job moves to the alias register; the bridge's
  job moves to INSTRUMENT + `RESOLVES_TO`, keeping its native-key rule and its unique-current
  guarantee (now carried by `dim_instrument`).
- **Reference vocabularies:** the 13 `ref_*` lists stay as governed sources; values promote to
  CONCEPT nodes per §6 as they need relationships.
- **Tickets:** VEC-418, VEC-419, VEC-420, and VEC-524 were canceled against the frozen tables;
  their loading, resolution, and data-quality needs are re-scoped against this model.

## Realization (explicitly not decided here)

The model above is the contract. The realization decisions below are open, to be made
separately and on evidence, and none of them may change the model:

1. **Node attribute representation.** Fixed typed columns versus a JSON document per node (join
   keys as real columns, attributes in a payload). The trade, recorded as input:

   | | JSON node store | Fixed columns |
   |---|---|---|
   | Change a node's shape | new payload key, no migration | a migration (`ALTER`) |
   | Integrity (`NOT NULL` / FK / `CHECK`) | loader/shape-enforced | database-enforced |
   | Typing | untyped payload values | typed columns |
   | Versioning (SCD2) | by row; payload diff is manual | by row; column diff is direct |
   | Query on an attribute | JSON/GIN index, or promote to a column | plain B-tree index |

   The shape mechanism (§6) narrows this trade: required fields and datatypes are enforced by
   shapes in either representation, so the residual difference is typing depth and query
   ergonomics. Whichever is chosen, the identity contract stays typed columns, and an attribute
   that becomes a join/filter key is promoted to a column.

2. **Engine.** Postgres (relational or JSON), a dedicated graph store, or a synlang runtime
   over projected atoms. The edge store and its resolution reads have been verified on
   Postgres; a synlang expression of the same reads is the planned trial. Selection criteria,
   not afterthoughts: append-only enforcement independent of application logic (AR-1.3), tamper
   evidence (AR-1.2), the as-of-system read (RP-4.1), shape validation (§6), type-scoped access
   control (AC-3), and round-trip export — the model serialises to JSON and must translate
   between realizations losslessly.

3. **Validator generation.** Shapes are data (§6); each realization generates its enforcement —
   SHACL for an RDF store, constraint DDL + loader checks for Postgres, per-shape validation
   queries elsewhere. The shape manifest is the asset; no engine owns it.

4. **Pivot materialization.** Which views are materialized tables versus computed views, refresh
   triggers, and distribution/placement for TigerData-side joins. The view set, grains, keys,
   and as-of semantics are fixed in §10.

## Alternatives Considered

**Two separate masters (`entity_master` + `security_master`).** The previous direction, now
superseded. Set aside in favour of one node store and one id space — while the pivot (§10)
deliberately re-materializes the *shapes* of those masters as regenerable read models.

**A single parent slot on the combined master (`parent_ref` + `parent_relationship_type`).**
Set aside: one slot forces a choice between an issuer link and an underlying link (syrupUSDC
has both), and cannot represent an LP token's two underlyings at all.

**Relationships embedded as `record_type = RELATIONSHIP` rows in the master.** Set aside: node
and relationship rows would share one table, each leaving the other's columns blank, and every
read would lean on `record_type` filters.

**Relationships as a dictionary on the node.** Set aside: one link per target; nowhere clean
for per-edge validity, weight, or provenance; ids in a blob carry no constraints; reverse and
graph reads scan every node.

**Node ids derived from public identifiers or symbols.** Set aside: every public identifier
eventually gets reused, corrected, or reassigned, and symbols collide across deployments; an id
derived from one breaks with it. Identifiers are lookups (§1.3), ids are opaque (§1.1).

**Edge identity as the bare triple (`rel_type`, `src`, `dst`).** Set aside: DM-6 handles
multi-attribute and multi-typing by duplicating the edge, and under a bare-triple identity a
deliberate twin silently supersedes its sibling in the versioned store. `edge_seq` makes twins
first-class.

**Keeping the instrument key as a lookup-only bridge.** Set aside by the model's own promotion
test: the instrument carries attributes (address, decimals, venue, first-seen block) and two
products address it (positions, Time-Series API), so it is a node. The lookup's performance
contract survives as `dim_instrument`.

**Valid time only, system time later.** Set aside: the Auditability PRD makes as-of-system
reads and the restatement/valid-time distinction mandatory (RP-4.1, CR-3.5), and retrofitting a
clock onto loaded data is a silent history rewrite. The store is empty today — the cheapest
moment this will ever have. The same argument starts the hash chain at the first append.

## Consequences

**Positive:**
- The graph carries the full inventory the requirements name — five node kinds now, two staged;
  a governed edge vocabulary in six families; shapes as data — so "complete data model" is a
  property of this document, not of tribal knowledge spread across artifacts.
- Auditable by construction: every append carries who, what, when, why, and with which
  software; corrections chain by reference; both clocks are queryable; hashes run from row one.
- Engine-portable by construction: contracts, vocabulary, and shapes are data; realizations are
  swappable; consumers read only the pivot, which survives an engine change.
- The position stream integrates through three narrow seams (instrument key, holder alias,
  block time) with nothing stamped onto position rows — reclassification never rewrites
  positions.
- Change lands as data: a new relationship type is a vocabulary row; a new required field is a
  shape version; a corporate action is rows.

**Negative / trade-offs (accepted):**
- Cross-row rules (endpoint kinds, single-valued cardinality) are validator- and DQ-enforced,
  not database-enforced; the shape mechanism narrows but does not eliminate the
  loader-discipline surface, and shapes must exist before loading at scale.
- The provenance block is wide (actor, build, lineage, reason code, hash): every loader pays
  that cost on every append.
- Bitemporal reads make current-state views more involved (two-step resolution on two clocks);
  consumers must use the pivot, never base rows.
- The pivot adds a generation step between curation and consumption; its staleness window is a
  new operational property to monitor.
- Two node kinds (ACCOUNT, EVENT) and several edge types ship as drafts: named and typed, but
  unratified until their first consumer, so the vocabulary will see reviewed change.

## Evolvability: how changes land

| Change | How it lands | Cost |
|---|---|---|
| New node attribute | on the node (mechanics per realization); required-ness via a shape version | data, or one small migration |
| New relationship type | a row in the governed vocabulary + endpoint rule | one reviewed change |
| New or changed relationship | insert an edge / close-and-open | data only |
| A recorded value was wrong | restatement append, `supersedes_record_id` + reason code | data only |
| Late-arriving fact | valid-time append, backdated window, honest `recorded_at` | data only |
| Corporate action | status version on the node + a succession edge | data only |
| New concept / category | CONCEPT node + `NARROWER_THAN` placement | data only |
| Tighter validation on a settled type | shape version: severity or tier raised | data + review |
| New node kind | new `record_type` + its shape (ACCOUNT and EVENT staged this way) | data + review |
| Engine or representation change | re-realize the same contracts; consumers keep the pivot | bounded by design |

## Follow-ups / Open Questions

- **Representation and engine decision** (Realization §1–2), informed by the synlang trial and
  the recorded trade table; the conformance hooks are selection criteria.
- **Ratify the staged inventory**: ACCOUNT and EVENT node kinds and the draft edge types, each
  at its first consumer; the vocabulary's governed-list location and migration form.
- **The concept-class list**: which `ref_*` values promote to concept nodes first, and the
  initial shape set (instrument type and subtype lead).
- **Alias register design** (§1.3): schemes, uniqueness rules per scheme, and the holder-address
  resolution path that replaces `holder_entity_resolver`.
- **Tamper-evidence mechanism** (AR-1.2, NFR-5): internal hash chaining versus external
  anchoring — the model requires the hash from the first append; chain anchoring can be added
  without redesign.
- **Reason-code vocabulary** (CR-3.3): the structured `change_reason_code` set, governed like
  the relationship vocabulary.
- **PII store design** (DP-1..DP-4): store, key scheme, and erasure process; the model-level
  rules (pseudonymous surrogate, ciphertext hashing) are decided.
- **Pivot staleness contract** (§10): how fresh each view must be relative to graph writes, per
  consumer.
- **Re-scoped loads**: the entity port, the instrument + security load, position-enrichment
  resolution, and coverage DQ, replacing VEC-418/419/420/524.
- **Retention classes** (AR-1.5): per-data-class retention for the master, alias, and PII
  stores, set with Legal and Compliance; derived pivot rows need only outlive the manifests
  that cite them.
