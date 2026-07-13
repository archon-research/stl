# stl-verify — Go service

Block watcher, backfill, backup worker. Ports and Adapters (Hexagonal) architecture.
Root repo map and cross-cutting rules: [../AGENTS.md](../AGENTS.md).
Python and TS sub-services have their own files: [python/AGENTS.md](python/AGENTS.md), [ts/AGENTS.md](ts/AGENTS.md).

## Architecture

```text
stl-verify/
├── cmd/                    # Entry points, grouped by lifecycle (see below)
├── internal/
│   ├── domain/entity/      # Core business entities (no external dependencies)
│   ├── ports/
│   │   ├── inbound/        # Use case interfaces
│   │   └── outbound/       # Infrastructure interfaces
│   ├── adapters/
│   │   ├── inbound/        # HTTP handlers
│   │   └── outbound/       # Implementations: alchemy, postgres, redis, sns, sqs, s3, telemetry
│   └── services/           # Use case implementations (live_data, backfill_gaps, raw_data_backup)
└── db/migrations/          # SQL migrations (auto-applied)
```

**Interface Segregation**: Define ports as small, focused interfaces. Prefer multiple small interfaces over one large one.

**Dependency Injection**: All dependencies are injected via constructors. Never import adapters directly in application code.

Follow [Effective Go](https://go.dev/doc/effective_go).

### `cmd/` tree, grouped by lifecycle

- `cmd/base/watcher` — source of block events: WebSocket subscribe, reorg handling, Redis cache write, SNS publish.
- `cmd/workers/` — long-running SQS FIFO consumers, one message per block (sparklend, morpho, curve, oracle-price, psm3, prime-*, raw-data-backup, ...).
- `cmd/cronjobs/` — **Temporal**-scheduled (not k8s CronJobs): anchorage, maple-graphql, offchain-price, watcher-data-validator. Schedules live in Temporal state; changing an interval env var requires deleting the schedule in Temporal and restarting. Ticks must be idempotent (Temporal retries).
- `cmd/backfillers/` — one-shot historical gap fillers (sparklend, morpho-vault, oracle-pricing, aave-like-user-snapshot, raw-block-bulk-downloader).
- `cmd/util/` — `migrate`, `generate-er`, `null-payload-refill`, `stress-test`.

Every binary extracts a `run(ctx, args) error` from `main()` and runs under `lifecycle.Run` (workers) or `temporal.RunCronjob` (cronjobs) for graceful SIGINT/SIGTERM shutdown (~25s).

### Data flow

```text
Alchemy WebSocket → watcher → PostgreSQL (TimescaleDB) + Redis (cache) + SNS FIFO → SQS workers
```

Chains: Ethereum plus Avalanche / Arbitrum / Base / Optimism / Unichain (per-chain `run-*-avax` etc. targets). Only the **Ethereum** watcher fetches execution traces; every other chain has no `trace_block`, so its watcher runs `--enable-traces=false`. Workers read the block payload from **Redis, not Alchemy**, via the cache key below; SNS/SQS messages carry only a block pointer.

### Cache Key Convention

```text
stl:{chainId}:{blockNumber}:{version}:{dataType}
```
- version increments on chain reorgs
- dataType: block, receipts, traces, blobs

### Environment

- Go 1.26+
- Docker for local development (PostgreSQL, Redis, Jaeger, LocalStack)
- AWS for production (EKS on Graviton arm64 — migrating from ECS Fargate. RDS Aurora (TimescaleDB via TigerData), ElastiCache Redis, SNS/SQS, S3)
- Alchemy API key required for Ethereum mainnet access

## Building & Running

All commands run from `stl-verify/`:

```bash
# Development
make dev-up              # Start kind cluster with full pipeline (mock blockchain server by default)
make dev-suspend         # Suspend local kind nodes (local dev only; do not use in CI/prod)
make dev-resume          # Resume suspended local kind nodes (local dev only; do not use in CI/prod)
make dev-down            # Delete local kind cluster (dev-wipe also nukes volumes)
make dev-env             # Generate .env files for all services (fetches secrets from AWS)
make run-watcher         # Run one service on the host against the cluster
make run-<worker>        # grep '^run-' in the Makefile for the full list (incl. per-chain *-avax)
make kind-use-alchemy    # Switch watcher from the mock chain to real Alchemy (key in .env.secrets)

# Testing
make test               # Unit tests only
make test-race          # Unit tests with race detector (CI default)
make test-integration   # Integration tests (requires Docker, 5m timeout)
make e2e                # End-to-end tests with testcontainers
make cover              # Generate coverage report
go test -race -run 'TestName' ./internal/services/<pkg>/   # single test

# CI (runs all checks)
make ci                 # test-race, vet, fmt-check, tidy-check, staticcheck, vulncheck, golangci-lint

# Formatting & linting (all languages, run from stl-verify/)
make install-hooks      # Install lefthook git pre-commit hooks (auto-runs on dev-up)
make format             # Auto-format all code locally (Go, Python, TS)
make lint               # Run linters locally (delegates to language pipelines)

# Docker (ARM64 for Fargate Graviton)
make docker-release ENV=sentinelstaging          # Build and push watcher image
make docker-release-backup ENV=sentinelstaging   # Build and push backup worker image

# Erigon node management (requires ERIGON_USER, ERIGON_IP)
make erigon-status ERIGON_USER=<user> ERIGON_IP=<ip>
make deploy-bulk-download ERIGON_USER=<user> ERIGON_IP=<ip>
```

See [Makefile](Makefile) for the complete list of targets.

### Go linting

- Pre-commit hooks: gofmt, goimports (staged files only)
- Pre-push hooks: go vet (full module)
- CI (`go-ci.yml` → `make ci-checks && make test-race`): vet, staticcheck, golangci-lint, vulncheck, tidy — **source of truth**
- Install tools with `make tools`. Don't bypass hooks.

## Code Conventions

These apply to every language in the service (Go, Python, TS). Go-specific rules are in the [Go conventions](#go-conventions) section below.

- **Keep an eye out for deduplicate possibilities**: Try to consolidate lots of duplicated code. Create shareable libraries instead of duplicating code everywhere.
- **Files**: snake_case
- **Testing**:
    - Mock outbound ports for unit tests.
    - One scenario per test, named for the single behavior it covers. Never chain independent scenarios in one function — a failure must point at one thing. A parametrized/table-driven test varies *inputs* of the *same* behavior (one case per row); distinct behaviors get distinct functions. Tempted to join with "and" in a test name → write two tests.
    - Parametrize, don't copy-paste. When two tests differ only in inputs and expected outputs, fold them into one parametrized test (a case per row) rather than near-duplicate functions. The split rule above wins on conflict: a distinct *behavior* stays its own function even if its body looks similar.
    - Share setup, don't repeat it. Spot a setup pattern recurring across tests — especially in the same file — and hoist it into a common fixture/helper.
    - Use fixture factories for varying data. When setups build the same shape of data but differ in a few values, write a fixture factory (a constructor taking the varying values, sensible defaults for the rest) instead of one helper per variant.
    - Services should have 100% coverage. Think very hard about edge cases, it is mission-critical that code is correct and robust.
    - In services, ONLY test the public api. Don't test internals if you can avoid it.
    - For services, create both unit and integration tests.
    - Integration tests are only allowed to mock our data sources that we cannot control, e.g. Alchemy
    - **No test-order dependencies in integration tests sharing a schema**: never rely on migration-seeded rows or on rows another test created — sibling tests TRUNCATE/DELETE shared tables (e.g. `TRUNCATE protocol CASCADE`), so seed everything your test needs yourself via idempotent upserts. Verify by running the whole test file/package, not just your tests filtered with `-run` (a filtered run hides the wipe that breaks you).
- **Function composition** (read code like a book):
    - A function body should read like prose: a short, linear sequence of named steps. Each step is a call to a well-named helper whose name says *what* it does, so the reader understands the flow without reading the helper's internals.
    - Compose large functions from smaller ones. Treat these as signals to extract: a body longer than roughly one screen; comment-delimited "sections" inside a function (each section becomes a named helper, and the helper name replaces the comment); a `for`/`if` block more than a few lines deep; or any step you would describe with "and then".
    - Name helpers for the outcome, not the mechanics (`decodeSwaps`, `snapshotTouchedPools`, `persistBlock`), not (`processLoop`, `handleStuff`).
    - This is strongest for orchestration functions (block/event handlers, coordinators, `main` flows, batch builders): the top-level function must be a readable outline, with detail pushed down into helpers. A single sprawling handler that inlines decode + snapshot + persist is a defect, not a style preference.
    - Enforced in the Review phase: the code-quality reviewer rejects any new or modified function that violates this. Audit EVERY changed function, not a named subset (scoping the review to specific files creates blind spots, which is how a 254-line function once slipped through). Pre-existing functions the PR does not touch are out of scope: refactor them in a separate follow-up PR, not the feature PR that happened to sit next to them.
- **Comments**: Explain *why*, not *what*; default to none.
    - Never restate the code or the language: no comments on signatures, field names, or standard-library behavior the reader already knows.
    - No doc comments on self-evident `Params`/`Config`/`Options` structs or their fields. If such a struct exists for a non-obvious reason (e.g. named fields to block a same-typed arg swap), state it once in the consuming constructor, not on the struct.
    - DO comment the non-recoverable why: a non-obvious invariant, a workaround and the bug it dodges, a deliberate convention break, a safety/ordering/locking constraint, or units/scale the type can't express.
    - State each rationale once, at the canonical site (the type, column, or merge it governs). At call sites that depend on it, keep the comment to a short pointer or omit it; don't paste the same "why" at every caller.
    - When unsure, leave it out: a stale or redundant comment is worse than none.
    - No history in comments: don't duplicate what git tracks. Describe current code, not what it replaced or why something was removed.
- **Libraries**:
    - Use the standard library as much as possible.
    - Instead of duplicating code, create a function containing the shared functionality, and re-use it.
- **System-wide registries** (`chain`, `token`, `user`, `protocol`, `prime`, `oracle` + mapping tables): FK these instead of duplicating address/symbol/decimals/name columns.
    - FK by natural key only (`token`/`user`/`protocol`: `(chain_id, address)`; `oracle`/`prime`: `name`). Never resolve FKs by display label (e.g. token symbol) — labels are not unique or authoritative.
    - Assets with no on-chain address (custodied BTC/SOL, off-chain API symbols) get no `token` row: store raw symbol or curated nullable `token_id` (see `offchain_price_asset`).
    - Seed fixed on-chain sets statically: for a known, finite set (specific vaults/tokens), hardcode the verified addresses in the migration and resolve FK ids by natural key, to ensure fresh-DB determinism.
    - Never invent addresses: every on-chain address (token, contract, vault, oracle) must come from a verified authoritative source (live API/explorer/contract), never guessed or assumed.
- **External API adapters**:
    - Verify response shapes against the live API during development, not just against fixtures — a temporary live smoke test caught three schema drifts in the Maple GraphQL API (null `acmRatio` on active loans, `loanMeta` with null `type`, JSON-number fields among string-encoded integers) that fixture-only tests would have shipped broken.
    - Encoding can vary *across rows of the same field*. The Maple FTL `interestRate` is 18-decimal on V1-era loans (`fundingPoolV1` set, `fundingPool` null) but 6-decimal on live PoolV2 loans; a live smoke test surfaced this. When a field's scale depends on a row's lineage, scope the query to the lineage you index (here: live, non-terminal states, which are all PoolV2), re-check the discriminator in the parser (state + non-null pool), and store raw — never assume one global scale from one sample.

## Go conventions

Go-only rules for the stl-verify service. Language-agnostic conventions (testing philosophy, function composition, comments, dedup, registries) are in the Code Conventions section above.

- **Interfaces**: Behavior interfaces use the `-er` suffix (Reader, Publisher, BlockSubscriber). Ports follow the established noun patterns instead: persistence ports are `XxxRepository`, external-system ports are `XxxClient`/`XxxCache`/`XxxProvider`. Do not rename Repository/Client ports to `-er` forms.
- **Constructors**: Use `New` prefix
- **Amounts**: Wei / token amounts are `big.Int`, never `float64`.
- **Errors**:
    - Wrap with context: `fmt.Errorf("doing X: %w", err)`.
    - Never ignore errors.
    - Lean towards returning errors instead of continuing, unless there is an extremely good reason to continue instead.
    - **Fail hard and early on unexpected errors.**
    - **Never swallow a failure into partial success.** A sub-result that fails (a multicall sub-call, a batch row, one item in a loop) must propagate and stop the whole unit of work; do not default it to nil/zero/empty and keep going. Silent partial data is the worst outcome: it looks healthy, and repairing the holes later forces a backfiller rerun.
    - **A partial failure stops the whole event/block.** Do not ack, commit, or persist a partially-processed event. Stopping and retrying is correct; continuing with a hole is not.
    - **Poison pills get fixed or explicitly discarded, never silently skipped.** When an event persistently fails, the only acceptable responses are to make the code handle it, or to make a deliberate, explicit decision to discard that specific event. Silently dropping or defaulting it is forbidden.
    - **"Best effort" / `AllowFailure` reads still bubble up.** A call you issue is expected to succeed, so treat a failed result as an error and propagate it. If a value is genuinely optional for some inputs (e.g. a getter that does not exist on a particular contract/pool variant), do not issue the call for those inputs; gate it structurally. A NULL or absent value must be a documented structural fact, never the residue of a swallowed failure.
    - Panic only in `main`/`cmd` entry points. Everywhere else (`internal/`, adapters, services, libraries) return an error and let the caller deal with it, bubbling it up until it reaches `main`.
- **Testing**:
    - Prefer table-driven tests (each case under `t.Run`).
    - `main.go` entry points should also have 100% coverage. Move the `main.go` body into a `run(ctx, args) error` function and call only that from `main()` so you can test it.
    - For `main.go` files, only create integration tests.
- **Comments**:
    - The "standard-library behavior" the reader already knows includes Go zero values, nil-map reads, `json.Unmarshal` of null, `defer` order, etc. — don't comment them.
    - Keep package and exported-API doc comments, but make each say something the signature doesn't.
- **Function composition**: a function-length / complexity linter (golangci-lint `funlen`/`gocognit`) is the planned deterministic backstop so an over-long function fails CI automatically rather than relying on a reviewer noticing.
- **Binaries/Building**: When building binaries using `go build`, output to `stl-verify/dist`
- **Code structure**: In main.go files, keep main() at the top of the file.

## Database & migrations

Applies when you touch `db/migrations/**` or repository adapters.

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

## Adding New Features

### New Use Case
1. Add method to inbound port interface in `internal/ports/inbound/`
2. Implement the method in `internal/services/`
3. Add HTTP handler in `internal/adapters/inbound/`

### New External Dependency
1. Define interface in `internal/ports/outbound/`
2. Implement adapter in `internal/adapters/outbound/<name>/`
3. Inject via constructor in `cmd/<cmd>/main.go`

### New Entity
1. Create entity in `internal/domain/entity/`
2. Add repository methods to outbound port
3. Implement in relevant adapters

## Do NOT

- Add business logic to adapters
- Use global state or singletons
