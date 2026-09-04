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
- `cmd/cronjobs/` — **Temporal**-scheduled (not k8s CronJobs): anchorage, maple-graphql, offchain-price, watcher-data-validator. Schedules live in Temporal state; workers reconcile a changed interval env var into the existing schedule at startup (Go `reconcileScheduleSpec`, Python `ensure_schedule`), so a redeploy is enough. Ticks must be idempotent (Temporal retries). `morpho-v2-bootstrap` sits here by neighbourhood only: it carries no schedule and is started by hand like a backfiller.
- `cmd/backfillers/` — historical gap fillers. Mostly one-shot CLI binaries (sparklend,
  oracle-pricing, aave-like-user-snapshot, raw-block-bulk-downloader), plus
  `offchain-price-backfill` and `morpho-vault-backfill`, which are long-running **on-demand
  Temporal workers**: they poll a task queue and idle until a run is started by hand from the
  Temporal UI, with the range as workflow input. Grouped here by purpose (backfilling), not by
  lifecycle.
- `cmd/util/` — `migrate`, `generate-er`, `null-payload-refill`, `stress-test`.

Every binary extracts a `run(ctx, args) error` from `main()` and runs under one of three
entry points for graceful SIGINT/SIGTERM shutdown, all inside the pods' 60s
`terminationGracePeriodSeconds`: `lifecycle.Run` (workers — bounded by
`lifecycle.ShutdownTimeout`, 40s, plus a 15s `lifecycle.ShutdownTailBudget` for the
deferred archive drain and OTEL flush), `temporal.RunCronjob` (scheduled cronjobs), or
`temporal.RunWorker` (on-demand Temporal jobs — no schedule; parameters, where the job
takes any, supplied at start time; see `docs/temporal_guide.md`). The two Temporal entry
points hand shutdown to the Temporal SDK and read neither `lifecycle` constant.

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
- `stl` is the default of `REDIS_KEY_PREFIX`; only the tests that drive a worker binary set it, production leaves it unset

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
make dev-up-new          # A whole cluster of your own next to the running ones: free port offset, derived name
KIND_CLUSTER=<name> KIND_PORT_OFFSET=<n> make dev-up   # Isolated second cluster: own name, host ports +n, image tags, data dir
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
make ci                 # test-race, fmt/imports/tidy checks, golangci-lint (vet+staticcheck+modernize), vulncheck

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

See [Makefile](Makefile) for the complete list of targets. Every `run-*`, `dev-*` and
`kind-*` target honours `KIND_CLUSTER` / `KIND_PORT_OFFSET`, so two agents can each run
a full cluster on one machine without colliding on ports, image tags or data dirs. Once a
cluster exists, `export KIND_CLUSTER=<name>` is enough — the offset is derived from the
host port its control plane publishes.

### Go linting

- Pre-commit hooks: gofmt, goimports (staged files only)
- CI (`go-ci.yml`): fmt/imports/tidy checks + golangci-lint v2 (covers go vet, staticcheck, and go fix's modernizers — config in `.golangci.yml`) + vulncheck — **source of truth**
- Install tools with `make tools`; golangci-lint is version-pinned because the config schema is version-coupled, so rerun it when a stale local binary rejects `.golangci.yml`. Don't bypass hooks.

## Code Conventions

These apply to every language in the service (Go, Python, TS). Go-specific rules are in the [Go conventions](#go-conventions) section below.

- **Keep an eye out for deduplicate possibilities**: Try to consolidate lots of duplicated code. Create shareable libraries instead of duplicating code everywhere.
- **Files**: snake_case
- **Testing**:
    - Mock outbound ports for unit tests.
    - **Every unit-test file pairs with the source file it tests.** In Go, `foo_test.go` must sit next to a `foo.go` in the same package, and unit tests for code living in `bar.go` belong in `bar_test.go`. A unit-test file with no matching source file is a smell with exactly two resolutions: the source split is missing (extract the code into the matching file so the pair exists) or the tests are filed wrong (move them into the existing source file's `_test.go`). Two deliberate exceptions: shared fixtures/helpers for a package's tests live in a clearly-named helpers file (`testhelpers_test.go`), and build-tagged integration files that exercise a cross-cutting scenario rather than one source file are named for the scenario (`*_integration_test.go`). Python mirrors the same pairing through the `tests/` tree: `tests/…/test_foo.py` corresponds to `app/…/foo.py`.
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
    - **Two lines max**, constraint first, no preamble. Longer whys go in the doc comment of the thing they govern, an ADR, or the PR description.
    - **Never restate** the code: a signature, a field name, standard-library behavior (in Go: zero values, nil-map reads, `json.Unmarshal` of null, `defer` order), or a self-evident `Params`/`Config`/`Options` struct — one that exists for a non-obvious reason (named fields blocking a same-typed arg swap) is explained in the consuming constructor, not on the struct.
    - **Keep package and exported-API doc comments**, but each must say something the signature doesn't.
    - **State each why once**, at its canonical site (the type, column, or helper it governs). Check the callee first: if its doc carries the why, the call site needs nothing.
    - **DO comment** the non-recoverable why: non-obvious invariant, workaround plus the bug it dodges, deliberate convention break, safety/ordering/locking constraint, units/scale the type can't express.
    - **Tests get no exemption** — don't narrate setup. Banner and numbered-step comments (`// 1. …`) are extraction signals, not comments; see Function composition.
    - **No history** — git tracks it, and no ticket archaeology. Describe current code, not what it replaced.
    - When unsure, leave it out. Enforced in the Review phase: deleting is the reviewer's default for a comment that restates code or repeats a rationale.
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
    - Panic only in `main`/`cmd` entry points. Everywhere else (`internal/`, adapters, services, libraries) return an error and let the caller deal with it, bubbling it up until it reaches `main`. A test binary's `TestMain`/`init` is its entry point for this purpose, so a `testutil` helper written for that position (`SetupDBForMain` and its `*ForMain` siblings) may `log.Fatal` rather than hand 20 call sites the same error check.
- **Testing**:
    - Prefer table-driven tests (each case under `t.Run`).
    - `main.go` entry points should also have 100% coverage. Move the `main.go` body into a `run(ctx, args) error` function and call only that from `main()` so you can test it.
    - For `main.go` files, only create integration tests.
    - **One service set per CI shard, never per test** — service startup and migrations, not the tests, dominate integration-test CI time. A package declares what it needs in `TestMain` via `testutil.RunShared`, which owns service lifecycle, teardown order and the goroutine leak check — never hand-roll those in a package. Each handle it publishes lands in a package var the tests read (`sharedDSN`, `sharedRedisAddr`, `sharedLocalStackCfg`). In CI it takes the shard's `services:` containers (`STL_TEST_POSTGRES_DSN`, `STL_TEST_REDIS_ADDR`, `STL_TEST_LOCALSTACK_ENDPOINT`); locally it starts testcontainers. The Postgres server those variables name must be disposable and reached as a superuser — the suite creates and drops databases, flips template flags and evicts sessions it does not own — so never point them at a dev database you care about. `make shared-container-check` (part of `ci-checks`) fails any container started in a test.
    - **Isolate each test inside those services**: `testutil.SetupTestDB(t, sharedDSN)` for Postgres, a `testutil.SanitizeTestName(t.Name())` prefix for Redis keys and SQS/SNS names, `testutil.S3TestBucketName(t, prefix)` for buckets, `testutil.SQSTestFifoQueueName(t, prefix)` for FIFO queues. Anything a test counts (rows, objects, messages) needs its own database/bucket/queue. A test that drives a binary cannot namespace the names the binary builds for itself, so it hands the binary a namespace to build them from: `REDIS_KEY_PREFIX` for the cache key, and an `S3_BUCKET` from `testutil.S3TestBucketName(t, "stl-sentinel{env}-{chain}-raw-")` — `chainutil.ValidateS3BucketForChain` checks that prefix, not the whole name. Reach for `testutil.EnsureBucket` only where one package's own tests share a bucket, such as an archive bucket named for the worker. `make ci-service-check` holds the workflow's service images and LocalStack `SERVICES` to what the helpers ask for.
    - **`SetupTestDB` clones a migrated template database**, so a new test costs a file copy and migration time stays flat as tests are added. Never migrate per test; use `testutil.SetupDBForMain(baseDSN, name)` for a database shared by one test file. The template name carries a digest of the migration set plus `templateFormat` — bump that constant whenever `buildTemplate` changes, or a stale template outlives the change. Either edit leaves stale templates behind on a long-lived server: `make test-templates-clean` drops them, by hand because dropping from inside the suite would race a sibling process mid-clone. `db/migrator` is the deliberate exception — applying migrations from scratch is what it tests.
- **Function composition**: a function-length / complexity linter (golangci-lint `funlen`/`gocognit`) is the planned deterministic backstop so an over-long function fails CI automatically rather than relying on a reviewer noticing.
- **Binaries/Building**: When building binaries using `go build`, output to `stl-verify/dist`
- **Code structure**: In main.go files, keep main() at the top of the file.

## Database & migrations

Schema, migration, and snapshot-read rules live in [db/migrations/AGENTS.md](db/migrations/AGENTS.md). In Codex, they are in the automatic instruction chain only when a session starts under `db/migrations/`, but they also apply to the PostgreSQL adapters.
Before modifying anything under `internal/adapters/outbound/postgres/`, read and apply that file explicitly.

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
- Use global state or singletons in service code. Test binaries are the exception: a
  `TestMain`-scoped service handle in a package var is the pattern above, not a violation.
