# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project & Architecture

### Repository Structure

This repository contains the application code:
- **stl-verify/** - Main Go service (block watcher, backfill, backup worker)
- **k8s/** - Kubernetes manifests (Kustomize) for all environments
  - `k8s/base/` — one subdirectory per service: `Deployment`, `ServiceAccount` (reused by every overlay)
  - `k8s/overlays/prod/` — prod-specific patches (namespace, images/image tags)
  - `k8s/overlays/staging/` — staging-specific patches (namespace, images/image tags)
  - `k8s/overlays/dev/` — local kind overlay (localhost/*:local images, shared stl-config/stl-secrets via a runtime Component); `workers/` sub-overlay for Alchemy-key workers
  - `k8s/dev-infra/` — local-only artifacts with no EKS equivalent: infra (timescaledb, redis, localstack, temporal, jaeger, mock-blockchain-server), `jobs/`, and `kind.yaml` (the kind cluster definition); applied imperatively by `stl-verify/Makefile`
- **experiments/** - Exploration projects
- **docs/** - Architecture diagrams and entity relations

Infrastructure code (Terraform/OpenTofu) lives in a separate repository for security reasons.

### Hexagonal (Ports and Adapters)

```text
stl-verify/
├── cmd/                    # Entry points (watcher, bulk-download, raw_data_backup, event-persister, migrate)
├── internal/
│   ├── domain/entity/      # Core business entities (no external dependencies)
│   ├── ports/
│   │   ├── inbound/        # Use case interfaces
│   │   └── outbound/       # Infrastructure interfaces
│   ├── adapters/
│   │   ├── inbound/        # HTTP, gRPC, CLI handlers
│   │   └── outbound/       # Implementations: alchemy, postgres, redis, sns, sqs, s3, telemetry
│   └── services/           # Use case implementations (live_data, backfill_gaps, raw_data_backup)
└── db/migrations/          # SQL migrations (auto-applied)
```

**Dependency Rule**: Dependencies flow inward only. Domain has no dependencies; adapters depend on ports; ports depend on domain.

### Core Services

1. **Live Data Service** - WebSocket subscription to Alchemy for new blocks, handles chain reorgs, publishes to SNS FIFO
2. **Backfill Service** - Fills gaps in block data via HTTP polling
3. **Raw Data Backup** - Backs up block data to S3

### Data Flow

```text
Alchemy WebSocket → Live Data Service → PostgreSQL (TimescaleDB) + Redis (cache) + SNS FIFO → SQS consumers
```

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

### Common Commands

All commands run from `stl-verify/`:

```bash
# Development
make dev-up              # Start kind cluster with full pipeline (mock blockchain server by default)
make dev-suspend         # Suspend local kind nodes (local dev only; do not use in CI/prod)
make dev-resume          # Resume suspended local kind nodes (local dev only; do not use in CI/prod)
make dev-down            # Delete local kind cluster
make dev-env             # Generate .env files for all services (fetches secrets from AWS)
make run-watcher         # Run watcher (loads .env from cmd/watcher/)

# Testing
make test               # Unit tests only
make test-race          # Unit tests with race detector (CI default)
make test-integration   # Integration tests (requires Docker, 5m timeout)
make e2e                # End-to-end tests with testcontainers
make cover              # Generate coverage report

# CI (runs all checks)
make ci                 # test-race, vet, fmt-check, tidy-check, staticcheck, vulncheck, golangci-lint

# Formatting & Linting (Go/Python/TypeScript)
make install-hooks      # Install lefthook git pre-commit hooks (auto-runs on dev-up)
make format             # Auto-format all code locally (Go, Python, TS)
make lint               # Run linters locally (delegates to language pipelines)

# Note: CI workflows run per-language in .github/workflows/:
# - go-ci.yml:      `make ci-checks && make test-race`
# - python-ci.yml:  `make lint` + `make test-unit` + `make test-integration` (in python/)
# - ts-ci.yml:      `npm run lint` + `npm run format:check` + `npm run build` (in ts/)

# Docker (ARM64 for Fargate Graviton)
make docker-release ENV=sentinelstaging    # Build and push watcher image
make docker-release-backup ENV=sentinelstaging  # Build and push backup worker image

# Erigon node management (requires ERIGON_USER, ERIGON_IP)
make erigon-status ERIGON_USER=<user> ERIGON_IP=<ip>
make deploy-bulk-download ERIGON_USER=<user> ERIGON_IP=<ip>
```

See [stl-verify/Makefile](stl-verify/Makefile) for the complete list of targets.

### Linting & Code Quality

#### Architecture

Quality is enforced at three levels:

1. **Git Hooks (Lefthook)** — Runs on `git commit` and `git push`
   - Automatically fixes formatting issues (`stage_fixed: true`)
   - Prevents broken code from being pushed
   - Pre-commit: runs on staged files only (fast)
   - Pre-push: runs full-module checks (go vet, etc.)
   - Configured per-language: `stl-verify/lefthook.yml`, `stl-verify/python/lefthook.yml`, `stl-verify/ts/lefthook.yml`

2. **Local Development** — Convenience Makefile targets
   - `make install-hooks` — Install git hooks
   - `make format` — Auto-format code across all languages
   - `make lint` — Run linters locally
   - Delegates to language-specific tooling (Go, Python, TS)

3. **CI Workflows** — Post-push checks (`.github/workflows/`)
   - `go-ci.yml` — Go linting, vet, staticcheck, tests, vulncheck
   - `python-ci.yml` — Ruff lint/format, unit & integration tests
   - `ts-ci.yml` — oxlint, oxfmt, typecheck, build
   - Triggered on changed files (via `changed-files.yml`)
   - **These are the source of truth** — CI definitions are authoritative

#### Linting by Language

**Go:**
- Pre-commit hooks: gofmt, goimports (staged files only)
- Pre-push hooks: go vet (full module)
- CI: `make ci-checks` (vet, staticcheck, golangci-lint, vulncheck, tidy)
- Tools: Install with `make tools`

**Python:**
- Hooks: ruff lint, ruff format
- CI: `make lint` (from `stl-verify/python/Makefile`)
- Tools: `uv sync --all-extras` (includes ruff, pytest, etc.)

**TypeScript:**
- Hooks: oxlint, oxfmt
- CI: `npm run lint`, `npm run format:check`, `npm run build`
- Tools: `npm ci` (includes oxlint, oxfmt, etc.)

#### Running Manually

```bash
# Run git hooks manually without committing
lefthook run pre-commit   # Run all pre-commit hooks

# Format and lint locally before committing
make format
make lint

# Or let git hooks do it automatically on commit
git commit  # Hooks auto-fix, may stage changes
```

#### Key Points

- **Don't bypass hooks**: They catch issues early and cheaply
- **Hooks are fast**: Only check staged files, not entire codebase
- **CI is strict**: Fails if any checks don't pass (source of truth)
- **Language pipelines are independent**: Changes to one language don't trigger linting for others

## Code Conventions

- **Keep an eye out for deduplicate possibilities**: Try to consolidate lots of duplicated code. Create shareable libraries instead of duplicating code everywhere.
- **Interfaces**: Behavior interfaces use the `-er` suffix (Reader, Publisher, BlockSubscriber). Ports follow the established noun patterns instead: persistence ports are `XxxRepository`, external-system ports are `XxxClient`/`XxxCache`/`XxxProvider`. Do not rename Repository/Client ports to `-er` forms.
- **Constructors**: Use `New` prefix
- **Files**: snake_case
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
    - Table-driven tests, mock outbound ports for unit tests.
    - One scenario per test, named for the single behavior it covers. Never chain independent scenarios in one function — a failure must point at one thing. Table-driven varies *inputs* of the *same* behavior (each case under `t.Run`); distinct behaviors get distinct functions. Tempted to join with "and" in a test name → write two tests.
    - Parametrize, don't copy-paste. When two tests differ only in inputs and expected outputs, fold them into one table-driven test (a row per case) rather than near-duplicate functions. The split rule above wins on conflict: a distinct *behavior* stays its own function even if its body looks similar.
    - Share setup, don't repeat it. Spot a setup pattern recurring across tests — especially in the same file — and hoist it into a common fixture/helper.
    - Use fixture factories for varying data. When setups build the same shape of data but differ in a few values, write a fixture factory (a constructor taking the varying values, sensible defaults for the rest) instead of one helper per variant.
    - Services and main.go files should have 100% coverage. Think very hard about edge cases, it is mission-critical that code is correct and robust.
    - In services, ONLY test the public api. Don't test internals if you can avoid it.
    - You can move the main.go code into a function and only call that from main() so that you can test it properly.
    - For main.go files, only create integration tests.
    - For services, create both unit and integration tests.
    - Integration tests are only allowed to mock our data sources that we cannot control, e.g. Alchemy
    - **No test-order dependencies in integration tests sharing a schema**: never rely on migration-seeded rows or on rows another test created — sibling tests TRUNCATE/DELETE shared tables (e.g. `TRUNCATE protocol CASCADE`), so seed everything your test needs yourself via idempotent upserts. Verify by running the whole test file/package, not just your tests filtered with `-run` (a filtered run hides the wipe that breaks you).
- **Binaries/Building**: When building binaries using `go build`, output to `stl/dist`
- **Code structure**: In main.go files, keep main() at the top of the file.
- **Function composition** (read code like a book):
    - A function body should read like prose: a short, linear sequence of named steps. Each step is a call to a well-named helper whose name says *what* it does, so the reader understands the flow without reading the helper's internals.
    - Compose large functions from smaller ones. Treat these as signals to extract: a body longer than roughly one screen; comment-delimited "sections" inside a function (each section becomes a named helper, and the helper name replaces the comment); a `for`/`if` block more than a few lines deep; or any step you would describe with "and then".
    - Name helpers for the outcome, not the mechanics (`decodeSwaps`, `snapshotTouchedPools`, `persistBlock`), not (`processLoop`, `handleStuff`).
    - This is strongest for orchestration functions (block/event handlers, coordinators, `main` flows, batch builders): the top-level function must be a readable outline, with detail pushed down into helpers. A single sprawling handler that inlines decode + snapshot + persist is a defect, not a style preference.
    - Enforced in the Review phase: the code-quality reviewer rejects any new or modified function that violates this. Audit EVERY changed function, not a named subset (scoping the review to specific files creates blind spots, which is how a 254-line function once slipped through). Pre-existing functions the PR does not touch are out of scope: refactor them in a separate follow-up PR, not the feature PR that happened to sit next to them. A function-length / complexity linter (golangci-lint `funlen`/`gocognit`) is the planned deterministic backstop so an over-long function fails CI automatically rather than relying on a reviewer noticing.
- **Comments**: Explain *why*, not *what*; default to none.
    - Never restate the code or the language: no comments on signatures, field names, or standard Go behavior (zero values, nil-map reads, `json.Unmarshal` of null, `defer` order, etc.). The reader knows Go.
    - No doc comments on self-evident `Params`/`Config`/`Options` structs or their fields. If such a struct exists for a non-obvious reason (e.g. named fields to block a same-typed arg swap), state it once in the consuming constructor, not on the struct.
    - DO comment the non-recoverable why: a non-obvious invariant, a workaround and the bug it dodges, a deliberate convention break, a safety/ordering/locking constraint, or units/scale the type can't express.
    - Keep package and exported-API doc comments, but make each say something the signature doesn't.
    - State each rationale once, at the canonical site (the type, column, or merge it governs). At call sites that depend on it, keep the comment to a short pointer or omit it; don't paste the same "why" at every caller.
    - When unsure, leave it out: a stale or redundant comment is worse than none.
    - No history in comments: don't duplicate what git tracks. Describe current code, not what it replaced or why something was removed.
- **Libraries**:
    - Use the standard library as much as possible.
    - Instead of duplicating code, create a function containing the shared functionality, and re-use it.
- **Database**:
    - Always think hard and carefully about how the wrong data could be written to the database.
    - Always think hard and carefully about schema design.
    - For timeseries tables, use Tigerdata primitives, and make sure they support distributed tables.
    - Reading latest snapshot rows: state/snapshot tables carry `build_id` (audit-only: which deployment wrote the row) and `processing_version` (correction version: 0=original, N=Nth reprocess). To select the current/latest row per entity, order by the table's snapshot-time key — `block_number, block_version` for on-chain tables, `synced_at`/`timestamp`/`snapshot_time` for API-sourced — `DESC`, then `processing_version DESC`. NEVER use `build_id` to pick latest: it appears in no unique constraint and one `build_id` spans many sync cycles, so ordering by it picks an arbitrary cycle and mixes values across cycles (manufactures fake anomalies).
    - Interpreting numeric columns: a column's name and magnitude don't determine its unit or scale — verify against the column `COMMENT` (psql `\d+ <table>`) or the domain entity doc before computing, aggregating, or flagging an anomaly. Conventions vary per column: raw native-decimal ints (scale by `token.decimals`), column-specific fixed-point (`maple_loan_collateral.asset_value_usd` = per-unit USD price ×1e8, not a total; `maple_loan_state.acm_ratio` = ratio ×1e6), already-normalized decimals (`onchain_token_price.price_usd`), or values that aren't what the name implies (`allocation_position.scaled_balance` = interest-free reading, not the balance). A value repeated across rows is usually correct (one per-unit price per asset per snapshot), not corruption.
    - Document every new table and column with `COMMENT ON` in the same migration that creates it — these are the catalogue's source of truth and what the "Interpreting numeric columns" rule above reads. A `--` inline comment is not enough; it is invisible to `\d+` and the metadata catalogue. Match the established style from `20260609_120000_add_schema_comments.sql`: a `[Type]` tag (`Dimension` | `Configuration` | `Operational` | `Hypertable`) on the table; per-column `Roles` (`PK` | `FK→table.col` | `Derived` | `Partition` | `Audit`); and, for any numeric column, its exact unit/scale (raw native-decimal int vs fixed-point ×1eN vs normalized). A column whose unit/scale is not self-evident from its type MUST state it.
    - Read-then-write races: when an insert decision depends on a prior read of the same key (read-latest-then-insert, MAX(version)+1, append-on-change), serialize concurrent writers with `pg_advisory_xact_lock` on the natural key — `ON CONFLICT` alone cannot guard a decision made before the insert (ADR-0002 §3); acquire locks in sorted key order to stay deadlock-free.
    - NEVER modify an existing migration file in `stl-verify/db/migrations/`. Migrations are immutable once applied — the migrator tracks checksums and will reject modified files. Always create a new migration file for fixes or additions.
    - Role admin vs object grants: role-level ops (`CREATE ROLE`, `ALTER ROLE … SET`, role-to-role membership grants) require superuser and belong in the infra repo's `bootstrap-db.sh`. Migrations run as `stl_migrator` (CREATEROLE only) and hold object-level grants only (`GRANT … ON <object> TO <role>`, `ALTER DEFAULT PRIVILEGES`). Rule: a role named on the left of ALTER/GRANT/DROP = bootstrap; object on left, role on right = migration.
- **System-wide registries** (`chain`, `token`, `user`, `protocol`, `prime`, `oracle` + mapping tables): FK these instead of duplicating address/symbol/decimals/name columns.
    - FK by natural key only (`token`/`user`/`protocol`: `(chain_id, address)`; `oracle`/`prime`: `name`). Never resolve FKs by display label (e.g. token symbol) — labels are not unique or authoritative.
    - Assets with no on-chain address (custodied BTC/SOL, off-chain API symbols) get no `token` row: store raw symbol or curated nullable `token_id` (see `offchain_price_asset`). Never invent addresses.
- **External API adapters**:
    - Verify response shapes against the live API during development, not just against fixtures — a temporary live smoke test caught three schema drifts in the Maple GraphQL API (null `acmRatio` on active loans, `loanMeta` with null `type`, JSON-number fields among string-encoded integers) that fixture-only tests would have shipped broken.
    - Encoding can vary *across rows of the same field*. The Maple FTL `interestRate` is 18-decimal on V1-era loans (`fundingPoolV1` set, `fundingPool` null) but 6-decimal on live PoolV2 loans; a live smoke test surfaced this. When a field's scale depends on a row's lineage, scope the query to the lineage you index (here: live, non-terminal states, which are all PoolV2), re-check the discriminator in the parser (state + non-null pool), and store raw — never assume one global scale from one sample.

## Observability — alerts & runbooks (required for new indexers)

A new indexer / data service that emits metrics ships its alert rules **and**
runbook sections in the same PR — same definition-of-done as tests.

- Rules → a group in `alerts/vector-<service>.yaml`; runbooks → matching
  `## AlertName` sections in `docs/runbooks/vector-<service>.md`. Copy an
  existing pair (`vector-indexers.yaml` + `.md`) — their header comments carry
  the label, severity→routing, and window conventions; follow them.
- Cover at minimum: liveness/stall, error rate, silent-empty / data-quality
  holes the error path won't catch, and latency.
- Any counter an alert reads with an absence shape (`increase()`/`rate()`
  `== 0`) MUST be seeded to 0 at construction via `telemetry.SeedStatusCounter`
  (status-labelled) or `telemetry.SeedCounter` (fixed labels). An unseeded OTel
  counter series first appears at 1, so `increase()` misses the 0->1 after every
  pod rollover (false page on error+success pairs, see PR #529) and a
  dead-from-birth worker emits no series at all (the stalled alert can never
  fire). Open-ended label sets (per-operation error counters) cannot be seeded —
  pair those alerts with a kube-state `Down` companion instead.
- `critical` must have a `runbook_url` + runbook section; `warning`/`info` must
  have a runbook section.

### Do NOT

- Import adapters in domain or application layer
- Add business logic to adapters
- Use global state or singletons
- Skip error handling
- Commit generated files or binaries

## Review phase

After completing a substantive code change (new feature, refactor, bug fix touching multiple files), and before declaring the work done, spawn the following reviewer subagents **in parallel** — a single message containing multiple `Agent` tool calls. Sequential review wastes wall-clock time and starves later reviewers of attention.

Always spawn:

1. **`pr-review-toolkit:code-reviewer`** — adherence to project guidelines, style, conventions.
2. **`pr-review-toolkit:silent-failure-hunter`** — error swallowing, ignored errors, inadequate fallback, NotFound-treated-as-success.
3. **General-purpose with architecture brief** — hexagonal layering, dependency direction, port/adapter boundary, single responsibility, separation of concerns, coupling.
4. **General-purpose with code-quality/patterns brief** — function size, naming, idiomatic Go, DRY, premature abstraction, test design, SOLID-ish concerns.

Spawn additionally when applicable:

5. **`pr-review-toolkit:pr-test-analyzer`** — when new tests are added or coverage is at risk.
6. **`pr-review-toolkit:type-design-analyzer`** — when new types or interfaces are introduced.
7. **`pr-review-toolkit:comment-analyzer`** — when substantive new docstrings or comments are added.

Each reviewer prompt must include:

- The plan file path (if a plan exists) and a precise list of files in scope.
- A specific audit checklist tailored to that reviewer's lens — don't ask reviewers to "review the diff"; tell them what to look for.
- The expected output format: **Blocking** / **Should-fix** / **Nice-to-have** / **Verified correct**, with file:line citations.

Apply blocking and should-fix items before declaring the work done. Nice-to-have items are surfaced to the user for an explicit decision.
