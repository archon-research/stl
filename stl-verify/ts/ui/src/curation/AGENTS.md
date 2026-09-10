# stl-verify/ts/ui/src/curation

A spike: schema-driven CRUD over the combined master (ADR-0007). Covers VEC-646
UI-1/UI-2 and prototypes UI-5, whose ticket marks it future. Parent:
[../../AGENTS.md](../../AGENTS.md).

**Not shipped.** Vite's default build input is the root `index.html`, so
`curation.html` is served by the dev server and absent from `npm run build`. No
plugin enforces that — it falls out of Vite's own default, which is the cheapest
possible boundary. `vite.curation.config.ts` builds it standalone into
`dist-curation` when a static preview is wanted.

```bash
cd stl-verify/ts
npm run dev -w @stl-verify/ui        # then open /curation.html
npm run dev:curation -w @stl-verify/ui   # same, opens it for you
```

There is no backend and no `VITE_API_MOCKS` switch: msw is the only mode. See
`mocks/handlers.ts`.

## What is here

| Layer | What it owns |
| -- | -- |
| `schema/` | The write contract as zod. Vocabularies, per-kind node attrs, the edge schema, registers, shapes, the resource registry, composite workflows. |
| `form/` | Zod introspection → a render plan; a headless `useSchemaForm`; the field-component manifest; `SchemaForm` with graded overrides. |
| `lib/` | The **proposed** OpenAPI `paths` and the typed client built on it. |
| `mocks/` | An append-only store with the database's own resolution order, plus handlers typed against that contract. |
| `ui/`, `routes/` | Generated list/create/detail views and the route tree. |

## The decisions worth knowing

1. **The DDL is the source of truth, not the worksheets.** Everything in
   `schema/vocabularies.ts` is transcribed from
   `db/migrations/20260904_120000_secstore_node_edge_stores_and_vocabularies.sql`
   (VEC-617, PR #875) and `…_120100_secstore_concept_taxonomy_from_ref.sql`. The
   worksheets disagree in three places and the migration wins each time. When
   wave 2 lands, re-transcribe rather than patch.

2. **Fixtures are the real seed.** `mocks/taxonomy-seed.ts` is 352 concepts and
   149 `NARROWER_THAN` edges extracted from the taxonomy migration. Real
   cardinality is the point: narrowing that looks unnecessary against a dozen
   fixtures is obviously necessary against 90 instrument-type concepts.

3. **CRUD here is Create, Read, Append, Retract.** No update, no delete. A change
   is a new row; a withdrawal is a tombstone append with a zero-length valid
   window. The mock enforces this, which is why it resolves reads rather than
   mutating a collection — `mocks/store.ts` carries the reasoning.

4. **Validation is severity-tiered, and most of it does not block.** Thirteen of
   the fifteen ratified shapes are `EXPECTED`: the row stores, `node_validity`
   flags it, metrics exclude it. A form that treated those as errors would refuse
   most of the partially-curated rows the store exists to hold. `schema/shapes.ts`
   and the worklist view are where this lives.

5. **Overrides are graded.** `ui()` metadata → per-field component override →
   hidden fields → `renderSection` → drop to `useSchemaForm` and lay it out by
   hand. `form/SchemaForm.tsx` lists the ladder. The hook is the contract, not
   the component.

6. **The hook is hand-rolled, and that is reversible.** It is shaped like
   TanStack Form deliberately. Move to it when nested field arrays with their own
   validation arrive; `form/useSchemaForm.ts` records that trigger.

## Open, and worth deciding before this becomes real

- **A re-classification only opens; it does not close.** The classify workflow
  appends a new `BELONGS_TO` without closing the prior window, so two open
  windows for one logical edge coexist and both resolve as current. The database
  accepts this too — cardinality is a DQ check over current state, not a write
  trigger — so it is a genuine gap, not a mock artefact. Close-and-open needs the
  predecessor's window, which means either the client reads it first or the
  endpoint does it. **Decide before any real write path ships.**
- **No transactional endpoint.** The classify workflow issues its appends in
  sequence and stops at the first failure. Survivable (every append is
  independently valid, a half-classified node is just under-curated) but wrong
  long-run.
- **Scoped upper bounds are not client-checkable.** `BELONGS_TO` is
  `1_per_class`, so counting all such edges against a max of 1 flags a correctly
  classified security. `countableMax` in `schema/shapes.ts` skips those; the
  validator owns them.
- **Three findings against the model itself**, all reproducible in the worklist:
  - The `stablecoin` shape expects `HAS_UNDERLYING`, but the worksheet states
    fiat-backed stablecoins have none by design. The shape needs splitting by
    subtype, or the obligation belongs on the crypto-backed branch only.
  - That same shape expects `PEGGED_TO`, which is a **draft** relationship type
    wave 1 does not seed — so it demands an edge that cannot be created.
  - Five of the security worksheet's nine `security_subtype` values
    (`SYNTHETIC`, `COMMODITY_BACKED`, `AAA_TRANCHE`, `GOV_SECURITIES`, `OTHER`)
    have no concept node in the seeded `instrument_subtype` class, so they cannot
    become a `BELONGS_TO` edge at all.
- **`actor`, `run_id` and `source_system` are placeholders.** They belong to
  VEC-647's governed write path.
- **The design system has no combobox.** `ReferencePicker` is built on
  `SearchInput`, which does the job but cannot render a label for an
  already-chosen value — hence the resolved label sitting beneath the control.
  An upstream uikit gap worth filing.

## Conventions

- Two TS programs. `tsconfig.app.json` excludes `src/curation`;
  `tsconfig.curation.json` covers it. This is not tidiness: both apps register a
  TanStack Router into the same global `Register` interface, and one program
  cannot hold two.
- `src/curation/main.tsx` is a knip `entry` — knip does not follow a non-index
  HTML entry's module script, so the module is named directly.
- Hash history, because the entry is a file and not a directory.
  `routes/router.tsx` explains what breaks otherwise.
