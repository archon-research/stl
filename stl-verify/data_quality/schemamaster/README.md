# schema_master

`schema_master.json` is the canonical column register (INTENT), read by the CI conformance check.

It is **not** stored in the database: conformance is verified against `information_schema` at check time. A column that already conforms (canonical name + canonical type) appears **nowhere** in the register. The register records only what a column should be and which deviations are sanctioned.

This file holds the prose that used to live as comments in the YAML. The register itself is JSON (`encoding/json`, standard library) so it carries no inline comments.

## Sections

### `ignore_tables`
Tables checked but skipped (not governed by this register yet):
- `migrations` — the migrator's own bookkeeping table (never governed).
- `*_ref` — the reference layer (#515); column-level cataloguing is a follow-up.
- `curve_*` — the Curve DEX layer, added after this snapshot; cataloguing is a follow-up. The conformance check flagged these as unregistered, exactly as designed.
- `uniswap_v3_*` — the Uniswap V3 DEX layer (VEC-261), same situation as `curve_*`; column-level cataloguing is a follow-up alongside it.
- the enrichment master and resolver layer — curated append-only SCD2 masters and instrument/code resolvers, not part of the raw-to-canonical transform layer this register governs. Their columns (natural keys `processing_version`, `valid_from`, and the classification/attribute columns) are introduced by the enrichment layer, not the transform vocabulary, and are catalogued by each table's own migration `COMMENT`s. The masters carry no surrogate key: identity is the natural key `(id, processing_version)`, resolved via each `*_current` view. `security_master` (this PR) and `security_instrument_bridge` (VEC-412) are the entries in `ignore_tables`. The remaining enrichment and position-side tables — `entity_master` / `entity_ref_codes` (VEC-410 / VEC-414) and `position_classification` / `position_entity_link` (VEC-401 / VEC-415) — sit outside the transform vocabulary for the same reason but don't exist here yet; each is added to `ignore_tables` by its own PR, not this one (until then the conformance check simply never sees them, since it only scans live `BASE TABLE`s).

### `canonical`
The rulebook: one entry per canonical concept, keyed by column name. `type` is the invariant type the column must have wherever it appears. `class` and `semantics` are defaults; `not_null` marks a column that must be declared NOT NULL (with sanctioned exceptions in `nullable_exempt`).

### `tables`
Governed tables, with optional per-table governance (`type`, `owner`, `transform_defer`). A live table not listed here is flagged `unregistered_table`. `type` (`raw_pipeline` / `config` / `dimension`) drives which required-key rules apply, and is also the transform-target signal: a governed `raw_pipeline` table is a transform target (the transformed layer canonicalises on-chain / observation data); `config` / `dimension` / infrastructure tables are governed but never transformed.

`transform_defer` (optional, on a `raw_pipeline` table) is a reason string marking a transform target that is intentionally not built yet (a later bucket), e.g. `"VEC-494: bucket 3 (no natural PK)"`. `CheckTransformCoverage` enforces that every `raw_pipeline` table is either **built** (has a `transformed._sources` row) or **deferred** (`transform_defer` set), never neither and never both; and that every `_sources` row maps back to a governed `raw_pipeline` table; it also flags a `transform_defer` left on a non-`raw_pipeline` table (so a defer reason cannot linger after a table's `type` changes). So a new `raw_pipeline` table cannot be added and left silently un-transformed. The integration test reads the live `_sources` rows.

A few `maple_*` entries (`maple_ftl_loan`, `maple_ftl_loan_state`, `maple_loan_meta`, `maple_pool_meta`, `maple_sky_strategy_meta`) are intentionally left untyped for now, so the required-key pass (gated on `type`) skips them; they still get the per-column, table-coverage, and nullability checks. Typing them is pending per-table classification.

### `transforms`
Columns the transformation layer rewrites: `rename` / `cast` / `fill`. A `rename` must already be the canonical type; a `cast` declares its source type in `from`. `guard_min` / `guard_max` are plausibility bounds for an epoch (`int8` → `timestamptz`) cast — values outside the range are NULLed rather than cast. Those bounds are policy read by the transform materializer and the runtime cast check; the conformance check does not use them.

### `overrides`
Sanctioned TYPE exemptions the conformance check honours (`accepted_type`): a column deliberately kept at `accepted_type` rather than its canonical type (e.g. an infra surrogate key). Semantics/class overrides and derived-column formulas are deferred to the DQ3 (semantic/enrichment) checks that consume them; the conformance check reads only `canonical` + `tables` + `transforms` + these.

### `fills`
How a governed table obtains a canonical key it lacks natively (the transform layer derives it). The conformance check treats a required key as satisfied if it is a native column, produced by a transform, OR produced by one of these fills.
- `parent`/`key`/`ref` — single-hop FK join to the config parent.
- `then_parent`/`then_key`/`then_ref` — a second hop (e.g. sky strategy → pool → protocol).
- `const` — a literal.
- `block_time` — the `(chain_id, block_number, block_version)` block-time dimension.

Every join is verified 0-unresolved on the live schema.

### `required_keys`
A `raw_pipeline` (on-chain observation) table must resolve each requirement, as a native column, via a transform, or via a fill. `any_of` allows the canonical time to be `block_timestamp` OR `snapshot_time` (API poll) OR `event_time` (CEX). `exempt` lists tables where the key does not apply or is a pending modeling decision:
- off-chain CEX prices/orderbooks and Anchorage custody have no chain, so no `chain_id`.
- not protocol-scoped (prices, token supply, custody) or pending a modeling decision (`prime_debt` = Sky constant, `allocation_position`, `maple_syrup_global_state`, `psm3_reserves`), so no `protocol_id`.
- Anchorage operations carry only processing time (`created_at`), no event/observation time.

### `nullable_exempt`
Columns kept NULL-able despite a `not_null` canonical, sanctioned.
- `build_id` was retrofitted onto pre-existing hypertables (ADR-0002, 2026-04-14). TigerData blocks the validating full-table scan that `SET NOT NULL` needs on tiered chunks, so NOT NULL is enforced implicitly via the PK/UNIQUE key + `DEFAULT 0` + backfill; residual nullability is a harmless leftover. Tables created after the convention (`psm3_reserves`, fluid/maple state, `token_total_supply`) declare `build_id NOT NULL` inline and need no exemption.
- `processing_version` (the audit sibling of `build_id`) is `not_null` too; the same four pre-convention retrofit tables (anchorage ×2, `prime_debt`, `sparklend_reserve_data`) carry a residual nullable `processing_version` for the identical reason and are exempted alongside `build_id`.

### `transform_config`
Upsert key for tables the transform generator can't key from a raw primary key (no usable PK, or a PK that doesn't survive canonicalisation). Each entry gives the column list the generator upserts on. Read by the generator, not by the conformance check. Each key was verified unique against live data (counts omitted as they drift):
- `prime_debt`: `(prime_id, ilk_name, block_number, block_version, processing_version)`.
- `anchorage_operation`: `(prime_id, operation_id, processing_version)`.
- `anchorage_package_snapshot`: `(prime_id, package_id, snapshot_time, processing_version)`.
- `cex_orderbook_snapshots`: `(exchange, symbol, event_time, persisted_at)`. `event_time` alone repeats (multiple rows per exchange/symbol/event_time across re-polls) and `ingested_at` doesn't fully disambiguate; `persisted_at` (write time) does. No `processing_version` on this table.
