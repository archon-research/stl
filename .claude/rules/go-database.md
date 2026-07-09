---
description: Database schema, migration, and snapshot-read rules for stl-verify
paths:
  - stl-verify/db/migrations/**
  - stl-verify/internal/adapters/outbound/**/*repository*.go
  - "**/*repository*.go"
---

# Database & migrations

- Always think hard and carefully about how the wrong data could be written to the database.
- Always think hard and carefully about schema design.
- For timeseries tables, use Tigerdata primitives, and make sure they support distributed tables.
- Reading latest snapshot rows: state/snapshot tables carry `build_id` (audit-only: which deployment wrote the row) and `processing_version` (correction version: 0=original, N=Nth reprocess). To select the current/latest row per entity, order by the table's snapshot-time key — `block_number, block_version` for on-chain tables, `synced_at`/`timestamp`/`snapshot_time` for API-sourced — `DESC`, then `processing_version DESC`. NEVER use `build_id` to pick latest: it appears in no unique constraint and one `build_id` spans many sync cycles, so ordering by it picks an arbitrary cycle and mixes values across cycles (manufactures fake anomalies).
- Interpreting numeric columns: a column's name and magnitude don't determine its unit or scale — verify against the column `COMMENT` (psql `\d+ <table>`) or the domain entity doc before computing, aggregating, or flagging an anomaly. Conventions vary per column: raw native-decimal ints (scale by `token.decimals`), column-specific fixed-point (`maple_loan_collateral.asset_value_usd` = per-unit USD price ×1e8, not a total; `maple_loan_state.acm_ratio` = ratio ×1e6), already-normalized decimals (`onchain_token_price.price_usd`), or values that aren't what the name implies (`allocation_position.scaled_balance` = interest-free reading, not the balance). A value repeated across rows is usually correct (one per-unit price per asset per snapshot), not corruption.
- Document every new table and column with `COMMENT ON` in the same migration that creates it — these are the catalogue's source of truth and what the "Interpreting numeric columns" rule above reads. A `--` inline comment is not enough; it is invisible to `\d+` and the metadata catalogue. Match the established style from `20260609_120000_add_schema_comments.sql`: a `[Type]` tag (`Dimension` | `Configuration` | `Operational` | `Hypertable`) on the table; per-column `Roles` (`PK` | `FK→table.col` | `Derived` | `Partition` | `Audit`); and, for any numeric column, its exact unit/scale (raw native-decimal int vs fixed-point ×1eN vs normalized). A column whose unit/scale is not self-evident from its type MUST state it.
- Read-then-write races: when an insert decision depends on a prior read of the same key (read-latest-then-insert, MAX(version)+1, append-on-change), serialize concurrent writers with `pg_advisory_xact_lock` on the natural key — `ON CONFLICT` alone cannot guard a decision made before the insert (ADR-0002 §3); acquire locks in sorted key order to stay deadlock-free.
- NEVER modify an existing migration file in `stl-verify/db/migrations/`. Migrations are immutable once applied — the migrator tracks checksums and will reject modified files. Always create a new migration file for fixes or additions.
- Filename format: `YYYYMMDD_HHMMSS_description.sql`; plain SQL, applied automatically in order.
- Every migration must self-register: end it with `INSERT INTO migrations (filename) VALUES ('<filename>') ON CONFLICT (filename) DO NOTHING;` or it re-runs on every invocation. Don't mask that with an idempotency guard.
- Every time-series table must get a hypertable + compression policy + S3 tiering policy in the **same** migration that creates it.
- Role admin vs object grants: role-level ops (`CREATE ROLE`, `ALTER ROLE … SET`, role-to-role membership grants) require superuser and belong in the infra repo's `bootstrap-db.sh`. Migrations run as `stl_migrator` (CREATEROLE only) and hold object-level grants only (`GRANT … ON <object> TO <role>`, `ALTER DEFAULT PRIVILEGES`). Rule: a role named on the left of ALTER/GRANT/DROP = bootstrap; object on left, role on right = migration.
