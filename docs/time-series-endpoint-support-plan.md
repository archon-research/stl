# Time-Series Endpoint Support Plan (Scoped)

**Date:** 2026-06-15
**Status:** Draft plan
**Scope:** Isolated plan for adding reusable time-series support to relevant API endpoints only.

## Goal

Enable consistent, future-proof time-series query support across relevant endpoints without introducing chart UI work.

This plan is intentionally limited to:
- DB index changes for time-windowed endpoint access patterns.
- Python API query-param standardization through a shared controller (Pydantic-backed) for time-series params.
- Endpoint sweep to migrate any overlapping implementations to the shared controller.

## Out of Scope

- New chart components or frontend visualizations.
- New analytics service/datamart.
- Materialized rollups, pre-aggregated BI tables, or dashboard customization.

## Relevant Endpoints (Current vs Target)

## 1) Allocation activity (already partially time-windowed)
- Current route: `GET /v1/allocations/activity`
- Current params include: `from_timestamp`, `to_timestamp`, `limit`.
- Current state: time bounds are implemented ad hoc at endpoint/repository level.

Target:
- Migrate time-window parameters to shared controller dependency.
- Keep response contract stable.
- Optionally add `resolution` as accepted-but-optional for forward compatibility (unused in v1 query path unless aggregation is explicitly requested).

## 2) Protocol events (time-ordered but no explicit window)
- Current route: `GET /v1/protocol-events`
- Current params: `tx_hash`, `protocol_name`, `limit`.
- Current state: ordered by `created_at DESC`, but no explicit `from/to`.

Target:
- Add shared time-series params (`from`, `to`, optional `resolution`) via centralized controller.
- Wire repository/service filters for time window.

## 3) Prime debt snapshots (time-ordered snapshot history)
- Current route: `GET /v1/primes/{prime_id}/debt`
- Current params: `limit` only.
- Current state: ordered by `synced_at DESC`, suitable for time-window slicing but lacks common API contract.

Target:
- Add shared time-series params via centralized controller.
- Apply time-window filters against `synced_at`.

## Shared Python API Controller Plan

Create one shared module to normalize all time-series parameters for current and future endpoints.

## Proposed new module
- `stl-verify/python/app/api/time_series.py`

## Proposed core model
- Pydantic model (v2 style), e.g. `TimeSeriesQueryParams` with:
  - `from_timestamp: datetime | None`
  - `to_timestamp: datetime | None`
  - `resolution: TimeSeriesResolution | None` (typed enum, ISO-8601 duration values)
  - `limit: int | None` (only when endpoint chooses to source limit from this model)

## Resolution enum and ISO duration standard
- Use an explicit enum backed by ISO-8601 duration strings (for example `PT1M`, `PT5M`, `PT15M`, `PT1H`, `PT6H`, `P1D`).
- Keep enum values constrained to an allow-list tuned for operational safety; reject free-form durations.
- This keeps request contracts type-safe while preserving a standard serialization format commonly used across observability tooling.

## Naming alignment (Grafana-inspired)
- Grafana query APIs commonly expose `from`/`to` plus interval controls (`intervalMs`).
- For STL v1, keep explicit endpoint param names (`from_timestamp`, `to_timestamp`, `resolution`) for clarity and backward compatibility.
- Do not introduce aliases at this stage.

## Validation rules (centralized)
- Reject inverted ranges: `from > to`.
- Default time window to last 24 hours when either bound is omitted.
- Enforce max range window per endpoint class (configurable constants).
- Enforce allowed resolution enum values.
- Enforce dynamic minimum resolution based on selected window size to prevent expensive scans.
- Compute default resolution from selected window size when caller omits it.
- Normalize timezone handling to UTC semantics.
- Keep unknown/unused params out of repositories.

## Window-to-resolution policy (initial)
- `window <= 6h`: min/default `PT1M`
- `6h < window <= 24h`: min/default `PT5M`
- `24h < window <= 7d`: min/default `PT15M`
- `7d < window <= 30d`: min/default `PT1H`
- `window > 30d`: min/default `PT6H`

## FastAPI integration pattern
- Expose a dependency function such as `parse_time_series_params(...) -> TimeSeriesQueryParams`.
- Endpoints depend on this parser instead of declaring local `from/to/resolution` params.
- Endpoints map normalized params into service-layer filter objects.

## Service/Repository contract consolidation
- Add small filter DTO(s) in service layer (or API-layer typed object passed through) so repositories receive one coherent time filter object.
- Avoid duplicating independent method args (`from_timestamp`, `to_timestamp`, `resolution`) across many signatures over time.

## DB Index Change Plan

Design index changes against actual query shapes only. Do not alter existing migration files; add new migration(s).

## A) allocation_position (for /allocations/activity)
Current query characteristics:
- Filters: `chain_id`, optional `prime/proxy`, optional `action_type`, optional token/protocol text filters.
- Time filter: `created_at >= from` and/or `created_at <= to`.
- Sort: `created_at DESC, block_number DESC, block_version DESC, log_index DESC`.

Planned index additions:
- Composite index focused on high-selectivity path with time ordering:
  - `(prime_id, created_at DESC, block_number DESC, block_version DESC, log_index DESC)`
- Optional chain-scoped variant if EXPLAIN indicates benefit:
  - `(chain_id, created_at DESC, block_number DESC, block_version DESC, log_index DESC)`

Note:
- `protocol_name` and `token_symbol` are substring filters today; keep btree strategy for temporal pruning and evaluate trigram separately only if needed.

## B) protocol_event (for /protocol-events)
Current query characteristics:
- Filters: optional `tx_hash`, optional `protocol_name`.
- Time support to add: `created_at` bounds.
- Sort: `created_at DESC, block_number DESC, log_index DESC`.

Planned index additions:
- Composite index for protocol/time scans:
  - `(protocol_id, created_at DESC, block_number DESC, log_index DESC)`
- Evaluate whether tx_hash path already sufficiently served by existing keys; add dedicated tx hash + time index only if needed.

## C) prime_debt (for /primes/{id}/debt)
Current query characteristics:
- Filters: `prime_id` and soon `synced_at` range.
- Sort: `synced_at DESC, block_number DESC, block_version DESC`.

Current indexes already include:
- `(prime_id, synced_at DESC)`
- `(synced_at DESC)`
- `(prime_id, block_number DESC, block_version DESC)`

Plan:
- Verify via EXPLAIN whether current `(prime_id, synced_at DESC)` remains sufficient with range predicate + order.
- Add `(prime_id, synced_at DESC, block_number DESC, block_version DESC)` only if planner still performs avoidable sort/heap work.

## Migration and Verification Steps

## 1) Add new immutable migration file
- Create a new timestamped migration under `stl-verify/db/migrations/` containing only index DDL.
- Keep it idempotent (`IF NOT EXISTS` when valid for target object).

## 2) Validate with EXPLAIN ANALYZE
For each endpoint query shape:
- Baseline plan before index.
- Plan after index.
- Confirm improved index scan usage and reduced sort cost/rows scanned.

### DBA-ready EXPLAIN checklist (copy/paste)

Run with:

```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
...
```

Capture and compare for each query shape:
- Plan node type changes (Seq Scan -> Index Scan / Bitmap Index Scan).
- Sort behavior changes (explicit Sort removed or reduced cost).
- Rows scanned vs rows returned (`rows`, `Rows Removed by Filter`).
- Buffer profile (`shared hit/read`) and total execution time.

#### A) allocation activity shape (`allocation_position`)

```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    ap.chain_id,
    ap.prime_id,
    ap.token_id,
    ap.created_at,
    ap.block_number,
    ap.block_version,
    ap.log_index
FROM allocation_position ap
WHERE ap.prime_id = :prime_id
  AND ap.created_at >= :from_ts
  AND ap.created_at <= :to_ts
ORDER BY ap.created_at DESC, ap.block_number DESC, ap.block_version DESC, ap.log_index DESC
LIMIT :limit;
```

Optional chain-scoped variant:

```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    ap.chain_id,
    ap.prime_id,
    ap.token_id,
    ap.created_at,
    ap.block_number,
    ap.block_version,
    ap.log_index
FROM allocation_position ap
WHERE ap.chain_id = :chain_id
  AND ap.created_at >= :from_ts
  AND ap.created_at <= :to_ts
ORDER BY ap.created_at DESC, ap.block_number DESC, ap.block_version DESC, ap.log_index DESC
LIMIT :limit;
```

#### B) protocol events shape (`protocol_event`)

```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    pe.protocol_id,
    pe.created_at,
    pe.block_number,
    pe.log_index
FROM protocol_event pe
WHERE pe.protocol_id = :protocol_id
  AND pe.created_at >= :from_ts
  AND pe.created_at <= :to_ts
ORDER BY pe.created_at DESC, pe.block_number DESC, pe.log_index DESC
LIMIT :limit;
```

#### C) prime debt shape (`prime_debt`)

```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    pd.prime_id,
    pd.synced_at,
    pd.block_number,
    pd.block_version
FROM prime_debt pd
WHERE pd.prime_id = :prime_id
  AND pd.synced_at >= :from_ts
  AND pd.synced_at <= :to_ts
ORDER BY pd.synced_at DESC, pd.block_number DESC, pd.block_version DESC
LIMIT :limit;
```

### Suggested local verification flow
1. Ensure migration with new indexes is not applied yet (or temporarily drop new indexes).
2. Run the three EXPLAIN baselines and save output.
3. Apply index migration.
4. Re-run the same EXPLAIN statements.
5. Attach before/after snippets to rollout notes.

### Local run snapshot (kind cluster, synthetic data)

Environment used:
- Local kind cluster (`kind-vector`) resumed via `make dev-resume`.
- TimescaleDB in namespace `vector`.
- Synthetic benchmark load: 50,000 rows each in `allocation_position`, `protocol_event`, `prime_debt`.

Observed before/after (12h window, limit 5000):

1. `allocation_position`
- Before:
  - Plan included `Incremental Sort` over `created_at, block_number, block_version, log_index`.
  - Access path used chunk `created_at` index.
  - Execution time: ~9.401 ms.
- After adding `idx_alloc_pos_prime_created_at_sort`:
  - `Incremental Sort` removed.
  - Access path switched to chunk index derived from `idx_alloc_pos_prime_created_at_sort`.
  - Execution time: ~4.607 ms.

2. `protocol_event`
- Before:
  - Plan included `Incremental Sort` over `created_at, block_number, log_index`.
  - Access path used chunk `created_at` index.
  - Execution time: ~7.440 ms.
- After adding `idx_protocol_event_protocol_created_at_sort`:
  - `Incremental Sort` removed.
  - Access path switched to `Index Only Scan` on chunk index derived from `idx_protocol_event_protocol_created_at_sort`.
  - Execution time: ~4.262 ms.

3. `prime_debt`
- Before:
  - Plan included `Incremental Sort` over `synced_at, block_number, block_version`.
  - Access path used chunk `synced_at` index.
  - Execution time: ~5.810 ms.
- After adding `idx_prime_debt_prime_synced_block_sort`:
  - `Incremental Sort` removed.
  - Access path switched to `Index Only Scan` on chunk index derived from `idx_prime_debt_prime_synced_block_sort`.
  - Execution time: ~4.188 ms.

Notes:
- These measurements were gathered on synthetic local data; absolute timings will vary by workload and hardware.
- Relative plan shape improvements (sort removal and more selective index usage) are the primary validation signal.

## 3) API implementation changes
- Introduce shared `time_series.py` controller and model.
- Refactor endpoint handlers to consume the shared dependency.
- Propagate normalized params through service/repository layers.

## 4) Endpoint sweep for duplicates
Sweep and refactor any endpoint that:
- Accepts or should accept windowing params.
- Implements local `from/to` parsing or validation.
- Introduces `resolution` independently.

Initial sweep targets:
- `app/api/v1/allocations.py`
- `app/api/v1/protocol_events.py`
- `app/api/v1/prime_debts.py`
- Supporting services/repos/ports that carry duplicated time filter args.

## 5) Test updates
- Unit tests for shared controller validation matrix:
  - valid ranges, invalid ranges, timezone normalization, resolution validation.
- Endpoint integration tests:
  - verify uniform behavior and error payloads for invalid time-series params.
  - verify same semantics across all migrated endpoints.

## API Contract Decisions (to lock before implementation)

- Canonical params for v1: `from_timestamp`, `to_timestamp`, `resolution` (no aliases for now).
- `resolution` type: ISO-8601 duration enum only.
- Default time window: last 24 hours.
- Dynamic min/default resolution: derived from selected window per policy table.
- `resolution` in raw-event endpoints: validated and accepted now for contract consistency; aggregation semantics can be added later.

## Rollout Strategy

1. Introduce shared controller + tests first (no behavior change).
2. Migrate allocations activity to shared controller (parity mode).
3. Add protocol events and prime debt time-window support using same controller.
4. Ship index migration with query-plan verification artifacts.
5. Monitor API latencies and DB scan/sort metrics post-deploy.

## Acceptance Criteria

- One shared Pydantic-backed time-series controller is used by all relevant endpoints.
- No endpoint has bespoke duplicated `from/to/resolution` validation logic.
- Endpoint behavior for invalid time params is consistent.
- Time-windowed queries use expected indexes in query plans.
- Existing response payloads remain backward compatible unless explicitly versioned.
