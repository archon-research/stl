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

7. **Re-pointing an edge is the server's job, not the client's.** Replacing a
   classification is two appends — the prior edge re-appended with a shorter
   `valid_to`, the new target opened from the same date — and the pair has to be
   atomic. Issued separately it can leave a node unclassified (close lands, open
   fails) or classified twice (open lands, close fails), and the engine catches
   neither: single-valued cardinality is a DQ check over current state, never a
   write trigger, because an open edge always time-overlaps its replacement.
   Hence `POST /v1/secstore/edges/repoint`, which also spares the client a read
   for the predecessor's `valid_from` — the close append has to reuse it to land
   in the same resolution group. `lib/contract.ts` carries why both halves stay
   at `processing_version` 0.

8. **For a file of prices, the unit of validation is a row.** Ingest parses the
   file, checks every row against `schema/timeseries.ts` on its own, and lets
   the curator send the ones that pass. There is no whole-file verdict, no
   window check and no cross-row rule, so three bad lines are three problems
   rather than a rejected file. The checking is client-side against the same
   zod schema the API would use — a 400-row file gives its verdict without a
   round trip — and the one thing the client cannot know, whether an
   observation already exists for that instant and source, stays the server's.
   CSV and JSON share the path: `lib/parse-rows.ts` normalises both to rows,
   and `form/coerce.ts` holds the string→number coercion the forms already did,
   because a CSV cell arrives as a string exactly like an input's value does.

9. **Prices are quoted as strings, and a JSON number is an error with its own
   message.** `numeric(30,18)` does not survive a float round trip:
   `JSON.parse` has already turned `0.999812345678901234` into
   `0.9998123456789012` before any of our code runs, so the loss is not
   recoverable at validation time — only detectable. `schema/primitives.ts`
   gives `exactDecimal` a custom error saying so, since the default "expected
   string, received number" sends the reader hunting for a type bug rather than
   a quoting one.

## Open, and worth deciding before this becomes real

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
