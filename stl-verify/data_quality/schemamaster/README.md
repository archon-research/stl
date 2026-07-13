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

### `canonical`
The rulebook: one entry per canonical concept, keyed by column name. `type` is the invariant type the column must have wherever it appears. `class` and `semantics` are defaults; `not_null` marks a column that must be declared NOT NULL (with sanctioned exceptions in `nullable_exempt`).

### `tables`
Governed tables, with optional per-table governance (`type`, `owner`). A live table not listed here is flagged `unregistered_table`. `type` (`raw_pipeline` / `config` / `dimension`) drives which required-key rules apply.

### `transforms`
Columns the transformation layer rewrites: `rename` / `cast` / `fill`. A `rename` must already be the canonical type; a `cast` declares its source type in `from`. `guard_min` / `guard_max` are plausibility bounds for an epoch (`int8` → `timestamptz`) cast — values outside the range are NULLed rather than cast. Those bounds are policy read by the transform materializer and the runtime cast check; the conformance check does not use them.

### `overrides`
Sanctioned TYPE exemptions the conformance check honours (`accepted_type`): a column deliberately kept at `accepted_type` rather than its canonical type (e.g. an infra surrogate key). Semantics/class overrides and derived-column formulas are deferred to the semantic-layer checks that consume them; the conformance check reads only `canonical` + `tables` + `transforms` + these.

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
