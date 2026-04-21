# Contributing to stl

Welcome!

This document is aimed at someone who has never seen the repo before. It
explains the layout, how to run things locally, how data acquisition works
(both per-block and on a cron schedule), and the conventions you are
expected to follow when opening a pull request.

> If anything here is wrong or unclear, fix it in the same PR as the work
> that surfaced the problem. Docs rot fast — keep them honest.

---

## 1. Prerequisites

| Tool | Why | Install |
|---|---|---|
| Go 1.26+ | Every service is Go | <https://go.dev/dl/> |
| Docker | Local infra (Postgres, Redis, Temporal, LocalStack) | Docker Desktop / Colima |
| [`kind`](https://kind.sigs.k8s.io/) | Runs a Kubernetes cluster inside Docker — mirrors prod | `brew install kind` |
| `kubectl` | Talks to the kind cluster | `brew install kubectl` |
| `kustomize` (optional) | Only needed for manually previewing a deploy (`kustomize build … \| kubectl diff -f -`). `make dev-up` uses `kubectl`'s built-in kustomize. | `brew install kustomize` |
| AWS CLI (optional) | Only needed if you want to fetch real Alchemy keys via `make dev-env` | `brew install awscli` |
| An Alchemy API key | Mainnet access | ask a team member or sign up at alchemy.com |

You do **not** need AWS credentials to develop locally — the kind cluster
runs LocalStack to emulate SNS/SQS/S3.

---

## 2. Five-minute quick start

```bash
git clone git@github.com:archon-research/stl.git
cd stl/stl-verify

# One-time: install lint/test tools.
make tools

# Spin up a full local pipeline (kind cluster, Postgres, Redis,
# LocalStack, Temporal, watcher, workers, cronjobs, python-api).
make dev-up
```

The first run builds every Docker image and takes several minutes. Warm
re-runs skip already-built images; pass `COLD=1 make dev-up` to force a
rebuild.

When it finishes:

```bash
kubectl --context=kind-vector get pods -n vector
```

Everything should be `Running`. Teardown with `make dev-down`; nuke
persistent volumes too with `make dev-wipe`.

> **⚠️ You need an Alchemy key for anything to actually work.** By
> default `make dev-up` points the watcher at a **mock blockchain
> server** that ships with the repo. The mock is enough to boot the
> cluster and exercise plumbing, but it is **not a fully implemented
> chain** — most RPC methods are stubs, responses are synthetic, and
> any worker that expects realistic block / receipt / trace data will
> produce garbage or fall over.
>
> To run against the real chain, put your key in `.env.secrets` at the
> repo root (the file is created by `make dev-preflight` on first run)
> and switch the cluster over:
>
> ```bash
> make kind-secrets        # propagate .env.secrets into the cluster
> make kind-use-alchemy    # point the watcher at Alchemy instead of the mock
> ```
>
> To go back to the mock (e.g. offline dev): `make kind-use-mock`.

To iterate on a single service without rebuilding the whole cluster, use
the `run-*` targets, which run the binary on your host against the kind
cluster's Postgres/Redis:

```bash
make run-watcher                      # Ethereum live watcher
make run-oracle-price-worker          # Oracle-price per-block worker
make run-sparklend-position-tracker   # SparkLend per-block worker
# ... grep `^run-` in stl-verify/Makefile for the full list
```

If a `run-*` target needs secrets (e.g. `ALCHEMY_API_KEY`), run
`make dev-env` first — it pulls secrets from AWS Secrets Manager and
writes per-service `.env` files.

---

## 3. Repo layout

```
stl/
├── stl-verify/           # All Go code lives here (single Go module)
│   ├── cmd/              # One subdirectory per binary (see §6)
│   ├── internal/
│   │   ├── domain/       # Business entities — zero dependencies
│   │   ├── ports/        # Interfaces (inbound = use cases, outbound = infra)
│   │   ├── services/     # Use-case implementations
│   │   ├── adapters/
│   │   │   ├── inbound/  # HTTP / gRPC / CLI handlers
│   │   │   └── outbound/ # alchemy, postgres, redis, sns, sqs, s3, temporal…
│   │   └── pkg/          # Cross-cutting helpers (env, telemetry, lifecycle…)
│   ├── db/migrations/    # SQL migrations, applied automatically
│   ├── python/           # Python API (served via k8s)
│   ├── ts/               # TypeScript helpers
│   └── Makefile          # The canonical entry point for every workflow
├── k8s/
│   ├── base/<svc>/       # env-agnostic Kustomize manifests per service
│   └── overlays/{staging,prod}/  # image tags + namespace per environment
├── docs/                 # Protocol specs, ADRs, entity diagrams
├── experiments/          # Scratch — not shipped
└── CLAUDE.md / AGENTS.md # Conventions for AI agents (worth reading as a human too)
```

**Infrastructure (Terraform/OpenTofu) lives in a separate private repo**
for security reasons. If your change needs new AWS resources (a new SQS
queue, an SNS subscription, an IAM policy, a secret, etc.), the change to
that repo must land **before** the code here can deploy cleanly.

---

## 4. Language policy — what to write new code in

Pick the language **before** you start, and when in doubt, ask first.

- **APIs are Python.** Anything user- or client-facing — HTTP/JSON
  endpoints, GraphQL, anything that serves data out to another system —
  belongs in `stl-verify/python/`. The existing `python-api` service is
  the reference; extend it rather than starting a new HTTP server.
- **Workers, cronjobs, and backfillers can be any language you prefer,
  but we strongly prefer Go or Python.** Most of the pipeline is Go
  today (the watcher, all SQS workers, all Temporal cronjobs), which
  means Go is the path of least resistance: shared helpers, the
  `lifecycle.Run`/`temporal.RunCronjob` harnesses, the hexagonal
  scaffolding, the Makefile wiring, and CI are all built around it.
  Python is a fully supported second option.
- **Anything outside Go or Python needs prior discussion.** Open an
  issue or start a thread with
  `@archon-research/vector-engineers` **before** writing code. PRs
  introducing a new runtime (Rust, TypeScript services, Java, …) without
  a prior design conversation **may be rejected** regardless of code
  quality — every new language adds build infrastructure, observability
  integration, deployment, dependency-management, and on-call load that
  the team has to absorb forever. That cost is only worth paying when
  we've agreed it is.
- The existing `stl-verify/ts/` directory is a small set of helpers, not
  a license to add new TypeScript services.

---

## 5. Architecture in one picture

The repo follows a **hexagonal (ports and adapters)** architecture.
Dependencies point inward: `domain ← ports ← services ← adapters`, and
`cmd/*` wires concrete adapters into services.

```
   Alchemy WS ──► watcher ──► Redis (block cache, 2d TTL)
                     │                │
                     │                └─► SNS FIFO (one topic per chain)
                     ▼                         │
              Postgres (block_state)           ▼
                                        SQS FIFO queues
                                               │
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                       oracle-price-    sparklend-       morpho-indexer
                         worker          tracker          (and others)
                              │                │                │
                              └────────────────┴────────────────┘
                                               ▼
                                     Postgres / TimescaleDB
                                     (time-series, append-only)

   Temporal schedules ──► cronjob pods ──► external APIs ──► Postgres
                          (anchorage,       (CoinGecko,
                           offchain-prices,   Anchorage,
                           data-validator)    …)
```

The cache key convention is:

```
stl:{chainId}:{blockNumber}:{version}:{dataType}
```

where `version` is bumped when a reorg invalidates a block and `dataType`
is one of `block`, `receipts`, `traces`, `blobs`.

---

## 6. The `cmd/` tree — where binaries live

Each subdirectory under `cmd/` is one `main.go`. They are grouped by
lifecycle:

| Group | Purpose | Example |
|---|---|---|
| `cmd/base/` | The chain watcher — the **source of block events** | `watcher` |
| `cmd/workers/` | Long-running SQS consumers — **one message per block** | `oracle-price-indexer`, `morpho-indexer`, `sparklend-indexer`, `raw-data-backup` |
| `cmd/cronjobs/` | Long-running Temporal workers triggered on a **schedule** | `offchain-price-indexer`, `anchorage-indexer`, `watcher-data-validator` |
| `cmd/backfillers/` | **One-shot** jobs that fill historical gaps | `oracle-pricing-backfill`, `sparklend-backfill`, `raw-block-bulk-downloader` |
| `cmd/util/` | Dev tooling (`migrate`, `generate-er`, `cronjob-manifest`, stress-test helpers) | — |

If you're adding data acquisition, **pick the category first** — it
determines the plumbing, deployment shape, and tests you'll write.

---

## 7. Data acquisition — per block

This is the hot path: extract something from every new Ethereum (or
Avalanche / Arbitrum / Base / Optimism / Unichain) block.

### How it works

1. **`cmd/base/watcher`** subscribes to `newHeads` over an Alchemy
   WebSocket, fetches the full block (header, receipts, traces, optional
   blobs) via HTTP, writes the raw block into the Redis cache, and
   publishes a `BlockEvent` to an SNS FIFO topic. It also handles reorgs
   by bumping the cache `version` and re-emitting affected blocks.
2. **SNS → SQS FIFO fan-out** (defined in the Infrastructure repo) gives
   each worker its own queue, partitioned by block number for ordered
   processing.
3. **A worker in `cmd/workers/<name>`** pulls a message, reads the cached
   block data from Redis, does its protocol-specific work (e.g. call
   oracle contracts at that block, diff SparkLend positions, etc.), and
   writes rows to Postgres/TimescaleDB.

Every worker follows the same skeleton (`cmd/workers/oracle-price-indexer/main.go`
is the reference):

```go
func run(ctx context.Context, args []string) error {
    cfg, err := parseConfig(args)          // flags + env, fail fast on missing config
    ...
    consumer, err := sqsadapter.NewConsumer(awsCfg, sqsadapter.Config{...}, logger)
    pool, err   := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
    repo, err   := postgres.NewOnchainPriceRepository(pool, logger, buildID, 0)
    service, err := oracle_price_worker.NewService(shared.SQSConsumerConfig{...}, consumer, repo, ...)

    return lifecycle.Run(ctx, logger, service) // handles SIGINT/SIGTERM + graceful stop
}
```

### Adding a new per-block worker

1. **Create `cmd/workers/<my-worker>/main.go`.** Copy an existing worker
   as a template. Keep `main()` small — it parses flags, wires adapters,
   and calls `lifecycle.Run`.
2. **Create a service in `internal/services/<my_worker>/`.** The service
   owns the business logic, depends only on ports, and exposes a public
   API tested in isolation (mock the repo + consumer + any contract
   caller). Services are expected to have ~100% unit-test coverage.
3. **Add outbound ports/adapters if needed.** A new protocol reader goes
   in `internal/adapters/outbound/<vendor>/`; expose it behind a small
   interface in `internal/ports/outbound/`.
4. **Add migrations** (see §9) for any new tables.
5. **Add k8s manifests** under `k8s/base/<my-worker>/`:
   `deployment.yaml`, `serviceaccount.yaml`, `kustomization.yaml`. Copy
   `k8s/base/oracle-price-worker/` as the template. Wire the new service
   into `k8s/overlays/{staging,prod}/kustomization.yaml`.
6. **Add build/deploy targets to the Makefile** (`docker-build-<name>`,
   `docker-release-<name>`, and register the worker in the `run-*` /
   `kind-load-workers` / `kind-deploy-workers` groupings). Grep for an
   existing worker name in the Makefile to see every site you need to
   touch.
7. **Coordinate with infra.** Open a PR in the Infrastructure repo for
   the SQS queue, SNS subscription, IAM policy, and any secrets — your
   code PR depends on those resources existing.

### If you're writing the worker in Python

**The flow is identical to the Go version** — same SNS FIFO fan-out,
same SQS FIFO queue, same Redis cache lookup by
`stl:{chainId}:{blockNumber}:{version}:{dataType}`, same Postgres
writes. Only the language changes.

Put everything under `stl-verify/python/`, mirroring the Go layout
one-to-one:

- `stl-verify/python/cmd/workers/<my_worker>/main.py` — the **entry
  point**, directly analogous to Go's `cmd/workers/<name>/main.go`.
  Keep it small: parse config + env, wire adapters, start the SQS
  consume loop, hand off to the graceful-shutdown harness. No business
  logic here.
- `stl-verify/python/app/services/<my_worker>/` — business logic for
  one block event. Full unit tests; mock the SQS consumer, the Redis
  reader, the repo, and any contract caller.
- `stl-verify/python/app/domain/entities/` — pure entities.
- `stl-verify/python/app/ports/` — interfaces.
- `stl-verify/python/app/adapters/{sqs,redis,postgres,onchain,…}/` —
  concrete implementations, one subpackage per vendor.

Use `boto3` (or `aioboto3` if the service is async) for SQS and
`redis-py` for the cache. Follow the same operational contract as the
Go workers:

- The worker is a long-running process that polls SQS with long-poll
  receive, processes one message at a time in FIFO order, deletes the
  message on success, and lets it redrive on failure.
- Handle `SIGINT` / `SIGTERM` — finish the in-flight message, close the
  DB pool, exit within ~25s (the equivalent of Go's `lifecycle.Run`).
- Read block data from Redis using the exact cache-key convention above;
  do not refetch from Alchemy unless the cache miss rate indicates a
  real bug.
- Ship as its own Deployment under `k8s/base/<my-worker>/` with the
  same shape as `k8s/base/oracle-price-worker/`.
- Open the same Infrastructure-repo PR (SQS queue, SNS subscription,
  IAM) — the plumbing outside the worker is language-agnostic.

If this is the first Python SQS worker in the repo, factor the
boilerplate (SQS consume loop, signal handling, graceful drain) into a
shared harness under `app/adapters/sqs/` so the second one is
copy-paste — don't inline it into your worker's entrypoint.

---

## 8. Data acquisition — on a cron schedule

Use this when the data source is **not keyed on block height** — typically
an external REST API (CoinGecko, Anchorage) or a periodic consistency
check of our own data.

### How it works

We use **Temporal** for scheduling, not Kubernetes CronJobs. Each cronjob
is a plain Deployment running a Temporal worker that registers a schedule
and an activity on startup:

- The shared runner is `internal/adapters/outbound/temporal.RunCronjob`.
- Schedules are stored in Temporal, not in k8s. On startup the worker
  calls `ScheduleClient().Create` — if it already exists, Temporal
  returns `AlreadyExists` and we carry on.
- Changing the interval env var **does not take effect automatically**:
  you must delete the schedule in Temporal (UI or CLI) and restart the
  worker. This is intentional — schedules are Temporal state, not k8s
  state.

A minimal cronjob (`cmd/cronjobs/offchain-price-indexer/main.go`):

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    err := temporal.RunCronjob(ctx, temporal.BuildMeta{...}, temporal.CronjobConfig{
        Name:            "offchain-price-indexer",  // → task queue, schedule ID
        IntervalEnv:     "PRICE_FETCH_INTERVAL",    // overrideable at runtime
        IntervalDefault: "5m",
        OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", ...))),
        Setup:           setupRunner,               // returns temporal.Runner
    })
    if err != nil { slog.Error("fatal", "error", err); os.Exit(1) }
}

func setupRunner(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
    // Build your service here; return a Runner whose Run(ctx) is the body
    // of one tick. Temporal wraps it in an activity + workflow for you.
}
```

### Adding a new cronjob

1. **Create `cmd/cronjobs/<my-cronjob>/main.go`** using the skeleton
   above. Pick a sensible default interval — err towards longer, and
   always make it overrideable via an env var.
2. **Put the business logic in `internal/services/<my_cronjob>/`** with
   full unit tests (mock external HTTP clients).
3. **Add k8s manifests** under `k8s/base/<my-cronjob>/`:
   `deployment.yaml`, `serviceaccount.yaml`, `kustomization.yaml`. Copy
   `k8s/base/offchain-price-indexer/` as the template — cronjob
   Deployments are small (50m/64Mi requests) because the work happens
   inside Temporal activities. Register the service in
   `k8s/overlays/{staging,prod}/kustomization.yaml`.
   A skeleton generator is available via
   `go run ./cmd/util/cronjob-manifest` if you'd rather start from
   generated YAML.
4. **Add a `docker-build-cronjob-<name>` target** to the Makefile (follow
   the pattern — most of the wiring is automatic thanks to the
   `CRONJOBS := ...` glob in `stl-verify/Makefile`).
5. **Infra PR** for any new secrets/IAM + a Temporal namespace entry if
   needed.

Cronjobs are **idempotent by design** — a tick may be retried by
Temporal. Your service must tolerate running twice on the same window
without producing duplicates.

### If you're writing the cronjob in Python

**The flow is identical to the Go version** — Temporal is still the
scheduler, the worker still registers a schedule on startup, and each
tick still runs one activity. Only the language changes.

Put everything under `stl-verify/python/`, mirroring the Go layout
one-to-one:

- `stl-verify/python/cmd/cronjobs/<my_cronjob>/main.py` — the **entry
  point**, directly analogous to Go's `cmd/cronjobs/<name>/main.go`.
  Keep it small: parse config, wire adapters, start the Temporal worker,
  register the schedule. No business logic here.
- `stl-verify/python/app/services/<my_cronjob>/` — business logic
  (the body of one tick). Full unit tests, mock external HTTP clients.
- `stl-verify/python/app/domain/entities/` — pure entities, no
  dependencies.
- `stl-verify/python/app/ports/` — interfaces.
- `stl-verify/python/app/adapters/{postgres,onchain,…}/` — concrete
  implementations, one subpackage per vendor.

For the Temporal worker itself, use the
[`temporalio`](https://pypi.org/project/temporalio/) SDK and follow the
same contract as `internal/adapters/outbound/temporal.RunCronjob`:

- Task queue, schedule ID, and workflow ID all derive from the cronjob
  name.
- Interval is read from `<NAME>_INTERVAL` env var with a sensible default.
- Create the schedule on startup; swallow `AlreadyExists`; changing the
  interval still requires deleting the schedule in Temporal and
  restarting the worker.
- Ship as its own Deployment under `k8s/base/<my-cronjob>/` with the
  same small resource profile as a Go cronjob.

If this is the first Python Temporal worker in the repo, factor the
boilerplate into a shared harness (e.g. `app/adapters/temporal/`) so the
second one is a copy-paste — don't inline it into your cronjob's main.

Idempotency and migration rules above still apply.

---

## 9. Database migrations

Migrations live in `stl-verify/db/migrations/` and are applied
automatically — by the migrator Job on `make dev-up`, and by the
ArgoCD PreSync hook in staging/prod.

**Rules (non-negotiable):**

1. **Filename:** `YYYYMMDD_HHMMSS_description.sql` (use `date +"%Y%m%d_%H%M%S"`).
2. **Plain SQL only**, ending with a self-tracking insert:
   ```sql
   INSERT INTO migrations (filename) VALUES ('20260420_120000_my_change.sql')
   ON CONFLICT (filename) DO NOTHING;
   ```
3. **Never modify an applied migration.** The migrator checksums every
   file; a changed checksum fails the deploy. To fix a mistake, write a
   new migration.
4. **Every timeseries table is a hypertable, tiered to S3, and
   compressed.** Without exception. All three are set up in the same
   migration that creates the table — don't ship a naked table and
   "add the policies later". Specifically:
   - **Hypertable** via `SELECT create_hypertable(...)` (or the
     distributed-hypertable equivalent). Pick a chunk interval that
     matches the ingest rate — too small and planning cost dominates,
     too large and compression and S3 tiering can't evict anything.
   - **Compression policy** via `ALTER TABLE ... SET (timescaledb.compress, ...)`
     plus `SELECT add_compression_policy(...)`. Choose
     `segmentby`/`orderby` columns that reflect how the table is
     queried — getting this wrong costs 10–100× on reads.
   - **Tiered-storage (S3) policy** via `SELECT add_tiering_policy(...)`.
     This is what keeps the hot Postgres volume small; skipping it is
     how we run out of disk in prod.
   Also: primitives must be compatible with **distributed** hypertables.
   When in doubt, read `docs/data_entities.md` and ADR-0002, or copy
   the most recent timeseries migration as a template.
5. Use `CREATE INDEX CONCURRENTLY` on big tables. Test on staging first.

---

## 10. Testing

| Command | What it runs | When you need it |
|---|---|---|
| `make test` | Unit tests | Everyday dev |
| `make test-race` | Unit tests with `-race` | What CI runs — run before pushing |
| `make test-integration` | Tests tagged `integration` (requires Docker — uses testcontainers) | Anything touching Postgres, Redis, SQS, S3 |
| `make e2e` | End-to-end watcher tests | Changes to the live/backfill pipeline |
| `make cover` / `make cover-all` | Coverage report | When working on a service with a coverage goal |

**Philosophy:**

- **Services are the unit under test.** Test only the public API; mock
  outbound ports with small handwritten fakes. Use table-driven tests.
  Services and `main.go` files are expected to approach 100% coverage.
- **Integration tests may only mock things we do not control.** Alchemy
  and other third-party APIs: mock. Our Postgres, our Redis, our SQS:
  use the real thing (via testcontainers) — mocking them has bitten us
  in production.
- For `main.go`, extract a `run(ctx, args) error` function and call it
  from `main()` — then the integration test calls `run` directly.

---

## 11. Code conventions

Most of these are also spelled out in [CLAUDE.md](./CLAUDE.md) and
[stl-verify/AGENTS.md](./stl-verify/AGENTS.md), which apply to humans too.

- **Hexagonal boundaries.** Domain has zero imports outside the standard
  library. `services/` depends on `ports/`, never on `adapters/`.
  Adapters are dumb — no business logic.
- **Interfaces** use the `-er` suffix (`Reader`, `Publisher`, `Multicaller`).
- **Constructors** are `NewFoo(...)`.
- **Files** are `snake_case.go`.
- **Errors:** wrap with context — `fmt.Errorf("fetching block %d: %w", n, err)`.
  Never ignore an error. Prefer returning the error over continuing.
- **Prefer the standard library.** Pull in a dependency only when the
  stdlib equivalent is materially worse. De-duplicate by extracting a
  helper, not by copy-paste.
- **Keep `main()` at the top of the file** and small — it should read
  like prose. Extract helpers aggressively.
- **No global state, no singletons.** Inject everything through
  constructors.
- **Binaries** built with `go build` go into `stl/dist/` (gitignored).

---

## 12. Pull request workflow

1. **Branch off `main`.** Name the branch after the Linear ticket if
   there is one (`VEC-123-short-slug`).
2. **Open a PR early** — drafts are fine. The `CODEOWNERS` file
   auto-requests review from `@archon-research/vector-engineers`.
3. **Before you push**, run:
   ```bash
   cd stl-verify
   make ci                  # lint + static + vuln + unit + race
   make test-integration    # if you touched anything data-adjacent
   ```
   CI runs the same commands. A CI run that modifies files (e.g. a
   stray `go mod tidy` diff) is a hard fail — commit the result locally.
4. **PR title format: `TICKET-1234: <good description>`** — e.g.
   `VEC-123: Add SparkLend position tracker`. The ticket prefix is how
   we link PRs to Linear and find things later; the description should
   say what the PR *does*, not what it *is* (prefer
   `VEC-123: Backfill oracle prices for new Aave markets` over
   `VEC-123: Oracle changes`). Commit messages can be whatever you like
   — **do not bother squashing WIP commits locally**, GitHub is
   configured for squash-and-merge, so the PR title becomes the commit
   message on `main` and your intermediate commits are discarded
   automatically.
5. **Merge to `main`** — CI then triggers `.github/workflows/deploy.yaml`,
   which bumps image tags in `k8s/overlays/staging/kustomization.yaml`
   and ArgoCD rolls the change into the `vector` namespace on the
   staging EKS cluster. Prod is a separate manual promotion (bump the
   tag in `k8s/overlays/prod/kustomization.yaml`).

---

## 13. Things that will save you time

- **Read the Makefile directly** — `stl-verify/Makefile` is the source
  of truth for every workflow and has many more targets than are
  documented here (Erigon management, bulk downloads, stress tests,
  bastion tunnels, …). Grep the top of the file or search for the
  target name you're after; there is no `make help`.
- **Look at existing examples before inventing a pattern.** Every
  category (`workers/`, `cronjobs/`, `backfillers/`) has a reference
  implementation that wires the adapters in the approved way. Copy it.
- **Secrets never go in git.** Local secrets live in `.env.secrets` at
  the repo root (gitignored, created by `make dev-preflight`). Cloud
  secrets live in AWS Secrets Manager and are pulled by `make dev-env`.
- **If you're stuck on infra**, contact the vector team.

---

## 14. Getting help

- **Code questions / design review:** @archon-research/vector-engineers
  (review is required anyway — ask early).
- **Protocol specs:** see `docs/` — `aave_v3_spec.md`, `morpho_spec.md`,
  `sparklend_spec.md`, etc.
- **Architecture decisions:** `docs/adr/` holds the short record of why
  we chose kind over Minikube, why every row is versioned, and so on.

Thanks for contributing — ship data-correct code and we'll all sleep
better.
