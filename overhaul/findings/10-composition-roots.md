Status: FINAL

# 10 — Composition roots and configuration

Area: every `main.go` under `stl-verify/cmd/`, the shared startup packages
(`internal/pkg/{env,lifecycle,telemetry,awsconfig,buildinfo}`), `cmd/workers/internal/dexbootstrap`,
`Dockerfile.common` / `Dockerfile.migrate`, and the `main_*_test.go` files beside each root.
k8s is touched only far enough to know what is deployed (agent 13 owns it).

---

## 1. Area map

32 Go `main` packages (not 34 — see F10.11), grouped by lifecycle. There are **three different
composition-root shapes** in the tree, and only one of them is hand-rolled:

```
                        ┌─ shape A: "kit owns startup" ────────────────────┐
cmd/cronjobs/*  (7) ────┤ temporal.RunCronjob(ctx, BuildMeta, CronjobConfig{ … Setup })
cmd/backfillers/        │ temporal.RunWorker (ondemand.go)                 │
  offchain-price-backfill│  → newBootstrap(): logger, InitOTEL, pool,      │
  reference-capital-     │    temporal client, unwind-on-error, shutdown   │
  morpho-vault-backfill  │  main.go = 79–295 LOC, run() = 8–20 LOC         │
                        └──────────────────────────────────────────────────┘
                        ┌─ shape B: "kit owns startup", cmd-local ─────────┐
cmd/workers/dex-indexer ┤ dexbootstrap.ParseConfig + dexbootstrap.Bootstrap│
                        │  main.go = 118 LOC; a Factory registry picks the │
                        │  DEX; 3 former binaries collapsed into 1         │
                        └──────────────────────────────────────────────────┘
                        ┌─ shape C: hand-rolled ───────────────────────────┐
cmd/workers/ 8 others ──┤ parseConfig() 76–101 LOC + run() 111–217 LOC     │
cmd/base/watcher        │  flags→env→cliConfig, InitOTEL, awsconfig.Load,  │
cmd/backfillers/ 5      │  sqs.NewConsumer, redis.NewBlockCache,           │
cmd/util/ 6             │  cache.NewReaderWithFallback, rpchttp.Dial,      │
                        │  OpenPool, buildregistry.New, multicall+tel,     │
                        │  archivingwire.Bootstrap, N repos, NewService,   │
                        │  lifecycle.RunWithTimeoutGuard                   │
                        └──────────────────────────────────────────────────┘
```

Wiring to the rest: roots import `internal/adapters/outbound/{postgres,redis,sqs,sns,s3,cache}`,
`internal/pkg/{blockchain,multicall,archivingwire,rpchttp,telemetry,lifecycle,awsconfig,env,buildinfo}`
and one `internal/services/<name>` package. Config arrives only as env vars (55 distinct) plus flags;
k8s supplies all of it through `envFrom` (106 `configMapRef` + 56 `secretRef` across 56 bases, zero
inline env). Deployment is one image per binary (`Dockerfile.common` parameterised by
`CMD_PATH`/`BIN`), 54 Deployments, chain selected by `CHAIN_ID` in a ConfigMap.

**The finding in one line: the kit that deletes shape C already exists, twice, and the team has
written down that it deliberately did not adopt it** (`58f9c196`, "No consolidation here — that is
deliberately separate, since each adoption step needs its own deploy and soak").

---

## 2. Metrics

| Metric | Value |
|---|---|
| Go `main` packages under `cmd/` | 32 (`go list`) |
| `main.go` total lines | 8,677 |
| `main_integration_test.go` files / lines | 22 / 8,804 |
| `main_test.go` files / lines | 19 / 3,825 |
| Test lines per production line in `cmd/**/main*.go` | **1.46 : 1** |
| Distinct env vars read (non-test) | **55**, across **180** call sites |
| Env vars read by ≥7 binaries | 12 (`DATABASE_URL` 27 sites, `ALCHEMY_HTTP_URL` 12, `CHAIN_ID` 11, `ALCHEMY_API_KEY` 11, `AWS_SQS_ENDPOINT` 10, `S3_BUCKET`/`AWS_SQS_QUEUE_URL`/`REDIS_ADDR` 8, `SQS_WAIT_TIME`/`SQS_VISIBILITY_TIMEOUT`/`DEPLOY_ENV`/`REDIS_PASSWORD` 7) |
| Binaries with a typed config struct | 13 (`cliConfig`/`workerConfig`/`config`/`Config`); 19 do scattered inline reads |
| Binaries that call `telemetry.InitOTEL` (directly or via a kit) | **21 of 32** |
| Binaries that hand-roll `config.LoadDefaultConfig` instead of `awsconfig.Load` | 4 |
| Binaries with no `main.go` test at all | **7** |
| k8s Deployments / base dirs / distinct images in roster | 54 / 56 / 24 |
| `docker-release-*` Makefile targets | 35 public + 21 `_internal` (Makefile is 2,851 lines) |
| Functions >120 lines in `cmd/` (non-test) | **15 of 360**; 10 of them are a `run()` |
| Length/complexity linter enabled | **none** (`.golangci.yml` enables only `gocritic` + `modernize`) |

### Largest functions in `cmd/` (non-test)

| Lines | Function | Location |
|---|---|---|
| 268 | `run` | `stl-verify/cmd/backfillers/aave-like-user-snapshot-indexer/main.go:185` |
| 232 | `scanJSONStringField` | `stl-verify/cmd/backfillers/raw-block-bulk-downloader/plan.go:228` |
| 217 | `run` | `stl-verify/cmd/workers/prime-allocation-indexer/main.go:169` |
| 209 | `run` | `stl-verify/cmd/workers/psm3-indexer/main.go:66` |
| 180 | `run` | `stl-verify/cmd/workers/morpho-indexer/main.go:161` |
| 177 | `run` | `stl-verify/cmd/workers/sparklend-indexer/main.go:161` |
| 172 | `run` | `stl-verify/cmd/backfillers/sparklend-backfill/main.go:103` |
| 158 | `Bootstrap` | `stl-verify/cmd/workers/internal/dexbootstrap/bootstrap.go:108` |
| 158 | `run` | `stl-verify/cmd/workers/fluid-vault-indexer/main.go:177` |
| 151 | `ParseConfig` | `stl-verify/cmd/workers/internal/dexbootstrap/parseconfig.go:61` |
| 148 | `fetchHypertableInfo` | `stl-verify/cmd/util/generate-er/main.go:308` |
| 146 | `Run` | `stl-verify/cmd/util/null-payload-refill/main.go:197` |
| 145 | `run` / `generateMermaid` | `oracle-price-indexer/main.go:133`, `generate-er/main.go:664` |
| 130 | `run` | `stl-verify/cmd/workers/prime-debt-indexer/main.go:156` |
| 111 | `run` | `stl-verify/cmd/workers/raw-data-backup/main.go:209` |

Largest files: `raw-block-bulk-downloader/main.go` 893, `generate-er/main.go` 807, `base/watcher/main.go` 561,
`null-payload-refill/main.go` 523, `aave-like-user-snapshot-indexer/main.go` 461,
`prime-allocation-indexer/main.go` 385.

### Skeleton duplication across 7 SQS workers (measured)

Normalised (trim, drop comments/blank/pure-punctuation) over `sparklend`, `morpho`, `fluid-vault`,
`prime-allocation`, `prime-debt`, `psm3`, `oracle-price`:

| | lines |
|---|---|
| Substantive lines written across the 7 files | **1,480** |
| Distinct substantive lines in their union | **489** |
| **Duplicate lines (written again in a sibling)** | **991 — 67%** |
| Lines present in ≥6 of 7 files | 77 |
| Lines present in all 7 | 47 |
| Highest pair overlap | `morpho` ↔ `fluid-vault`: **172 identical lines** (of 229 / 229) |
| `sparklend` ↔ `morpho` | 170 identical lines |

### Churn classification (probe 2)

70 (file, commit) pairs across the 5 most-churned roots since 2026-03-01:

| Bucket | Count | % |
|---|---|---|
| **E — signature/wiring ripple from a change elsewhere** | **33** | **47%** |
| F — lifecycle / telemetry / shutdown plumbing | 10 | 14% |
| G — other (5 gocritic lint, 5 pure rename) | 10 | 14% |
| A — new env var / config plumbing | 8 | 11% |
| B — new dependency wired | 7 | 10% |
| C — new chain support | 1 | 1.4% |
| D — new registry entry | 1 | 1.4% |

"Shotgun" commits (≥6 `main.go` files at once), 9 in six months:

| sha | main.go files | subject |
|---|---|---|
| `91773d5e` | **26** | `chore: enable gocritic and fix the 49 findings it reported (#754)` — a one-hunk `defer cancel()` → `cancel()` fix |
| `aba593a4` | 24 | `VEC-N/A: Standardize cron (#160)` (pure directory renames) |
| `5c8566bd` | 14 | `VEC-475: SQS shutdown hardening (#781)` |
| `ac662cd3` | 11 | `VEC-80: Data audibility (#191)` — 96 files total |
| `17b08499` / `454b74a7` | 9 each | SQS ack semantics; raw SC call archiving |
| `1e8b4e8c` | 8 | null-data cmd (incidentally extracted `awsconfig.Load` in 4 mains) |
| `58f9c196` | 8 | `VEC-NA: fix worker bootstrap drift across the 7 hand-rolled SQS worker mains (#674)` |
| `0dd8b1c7` | 7 | fleet-wide DB query-error counter |

45 of the 70 pairs (64%) come from those 9 all-five-at-once commits.

---

## 3. Findings

### F10.1 — Eight hand-rolled SQS worker roots repeat a ~170-line startup skeleton; the kit that deletes it already exists twice in-tree

**Strength**: Strong
**Size**: XL (epic; lands as ~6 S/M PRs, one worker per PR)

**Files**
- `stl-verify/cmd/workers/sparklend-indexer/main.go:73-159` (`parseConfig`), `:161-337` (`run`)
- `stl-verify/cmd/workers/morpho-indexer/main.go:69-159`, `:161-340`
- `stl-verify/cmd/workers/fluid-vault-indexer/main.go:75-175`, `:177-334`
- `stl-verify/cmd/workers/prime-allocation-indexer/main.go:69-167`, `:169-385`
- `stl-verify/cmd/workers/prime-debt-indexer/main.go:77-152`, `:156-285`
- `stl-verify/cmd/workers/psm3-indexer/main.go:66-274` (config inline in `run`)
- `stl-verify/cmd/workers/oracle-price-indexer/main.go:70-131`, `:133-277`
- `stl-verify/cmd/workers/raw-data-backup/main.go:99-193`, `:209-319`
- The two kits: `stl-verify/cmd/workers/internal/dexbootstrap/bootstrap.go:108-265` and
  `stl-verify/internal/adapters/outbound/temporal/temporal.go:116-180`, `ondemand.go:122-152`,
  `temporal.go:182-242` (`newBootstrap`)

**Problem**

67% of the substantive lines in these roots are duplicates (metrics above). The repeated skeleton
is nine fixed steps in a fixed order:

1. `slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: env.ParseLogLevel(slog.LevelInfo)}))` + `slog.SetDefault`
2. `telemetry.InitOTEL(ctx, telemetry.OTELConfig{ServiceName: "<name>", ServiceVersion: buildinfo.GitHash(), BuildTime: BuildTime, Logger: logger})` + `defer shutdownOTEL(context.Background())`
3. `awsconfig.Load(ctx, awsconfig.Options{StaticCredentialsFromEnv: true})`
4. `sqsAdapter.NewConsumer(awsCfg, sqsAdapter.Config{QueueURL, WaitTimeSeconds, VisibilityTimeout, BaseEndpoint: env.Get("AWS_SQS_ENDPOINT", "")}, logger)` + `defer Close`
5. `redisAdapter.NewBlockCache(...)` + `defer Close` + `Ping`
6. `s3adapter.NewReaderFromEnv` → `cache.NewReaderWithFallback(blockCache, s3Reader, chainID, deployEnv, s3Bucket, logger)`
7. `rpchttp.DialEthereum` + `defer Close`
8. `postgres.OpenPool(ctx, postgres.WorkerDBConfig(dbURL))` + `defer Close` + `buildregistry.New(ctx, pool)`
9. `entity.ChainName` → `multicall.NewTelemetry` → `multicall.NewClient` → `archivingwire.Bootstrap` → `mc = archiveWrap(mc)` → repos → `NewService` → `lifecycle.RunWithTimeoutGuard`

`dexbootstrap.Bootstrap` (bootstrap.go:108-265) performs steps 1–9 minus the service, and
`dexbootstrap.ParseConfig` (parseconfig.go:61-211) performs the whole flag/env resolution. It is
used by exactly one binary. Its own package doc says it exists because "each worker's main.go
duplicates ~300 LOC of identical setup" — and it was created by folding three DEX binaries into
one `dex-indexer` with a `Factory` registry (`cmd/workers/dex-indexer/main.go:44-51`,
`factories.go`). That collapse is the exact refactoring this finding proposes, already executed
once and shipped.

The cost of not adopting it is measurable in the churn: **47% of edits to the five most-churned
roots are bucket E — a mechanical edit forced by a signature or wiring change in another package.**
Only 2 of 70 edits were "new chain" or "new registry entry". Six months of examples:

| One idea | sha | Repeated |
|---|---|---|
| `postgres.DefaultDBConfig` → `WorkerDBConfig` | `b44e0b47` | identical 1-line edit ×5 |
| `ethclient.Dial*` → `rpchttp.DialEthereum` | `17b08499` | identical 2-line edit ×5 (9 mains) |
| `archivingwire.Bootstrap` 3→4 returns; `lifecycle.Run` → `RunWithTimeoutGuard`; `run` signature | `5c8566bd` | identical 4-hunk edit ×5 (14 mains, 92 files) |
| move `InitOTEL` above `awsconfig.Load`; `buildReg.GitHash()` → `buildinfo.GitHash()` | `0dd8b1c7` | byte-identical 11+/12− edit ×5, only the `ServiceName` literal differs |
| `defer cancel()` → explicit `cancel()` | `91773d5e` | identical 3+/2− edit ×**26** |
| `buildReg.BuildID()` into 7 repo constructors, forcing `InitOTEL` below the pool | `ac662cd3` | same idea, 14–33 lines ×5, 96-file commit |
| `archivingwire.Bootstrap(ctx, logger, chainID, buildID, "<source>")` + `defer archiveDrain()` + `mc = archiveWrap(mc)` | `454b74a7` | same 6-line block ×5 — **only the source string differs** (`"morpho"`, `"sparklend"`, `"prime-debt"`, `"prime-allocation"`, `"oracle-price"`) |

And the drift the duplication caused had to be repaired by hand in `58f9c196` ("fix worker
bootstrap drift across the 7 hand-rolled SQS worker mains"), whose body enumerates four real
production bugs that existed only because the copies diverged: explicit flags losing to env vars in
3 workers; `sparklend`'s visibility timeout hardcoded to 300 with no knob, so it alone could not be
retuned from a ConfigMap during a backpressure incident; `sparklend`'s S3 reader ignoring
`AWS_S3_ENDPOINT`; `prime-debt` composing a URL ending in `/` on an empty `ALCHEMY_API_KEY`. That
commit explicitly deferred the fix: *"No consolidation here — that is deliberately separate, since
each adoption step needs its own deploy and soak."*

Divergences that remain today, all pure accident:

| | signal handling | Redis config | key prefix | CHAIN_ID default | `ServiceVersion` |
|---|---|---|---|---|---|
| sparklend | `signal.NotifyContext` | `ConfigDefaults()` (TTL 24h) | `env.Get("REDIS_KEY_PREFIX","stl")` | `"1"` | `buildinfo.GitHash()` |
| morpho | `signal.NotifyContext` | literal, **TTL 2×24h** | `env.Get(…)` | `"1"` | `buildinfo.GitHash()` |
| fluid-vault | `signal.NotifyContext` | literal, TTL 2×24h | **hardcoded `"stl"`** | `"1"` | `buildinfo.GitHash()` |
| prime-allocation | `signal.NotifyContext` | `ConfigDefaults()` | `env.Get(…)` | `"1"` | `buildinfo.GitHash()` |
| oracle-price | `signal.NotifyContext` | `ConfigDefaults()` | `env.Get(…)` | `"1"` | `buildinfo.GitHash()` |
| prime-debt | **hand-rolled `sigChan` goroutine** (`main.go:49-56`) | — | — | `"1"` | `buildinfo.GitHash()` |
| psm3 | **hand-rolled `sigChan` goroutine** (`main.go:47-54`) | — | — | **`""` (required)** | `buildinfo.GitHash()` |
| dex (dexbootstrap) | `signal.NotifyContext` | literal, TTL 2×24h | hardcoded `"stl"` | **`""` (required)** | `buildinfo.GitHash()` |
| watcher | **`lifecycle.SignalContext`** | literal, **TTL 2h** | hardcoded `"stl"` | required via `env.Require` | **`GitCommit`** |
| cex-orderbook | `signal.NotifyContext` | — | — | — | **`GitCommit`** |
| raw-data-backup | `signal.NotifyContext` | literal | — | required (`os.Getenv`) | `buildinfo.GitHash()` |

`lifecycle.SignalContext` (`internal/pkg/lifecycle/lifecycle.go:120-139`) exists precisely to log
*which* signal arrived ("'SIGTERM from the kubelet' versus 'SIGINT from an operator' is the first
thing worth knowing when a pod restarts unexpectedly") and is used by **one** binary. Ten roots
re-implement a worse version. `redisAdapter.ConfigDefaults()` exists and is used by 3 of 6 Redis
consumers; the other 3 write a literal with a different TTL. `fluid-vault-indexer` hardcoding
`KeyPrefix: "stl"` (`main.go:222`) means its integration test cannot namespace Redis keys the way
`sparklend`/`morpho`/`oracle-price`/`prime-allocation`'s tests do (`REDIS_KEY_PREFIX` is set in
exactly 4 test files) — an infra-sharing hazard the other workers were explicitly fixed for.

**Proposed change**

Promote `dexbootstrap` to `internal/workerkit` (see F10.7) and give it the shape the two existing
kits already have — config in, `Deps` out, business logic supplied as a function. Sketch:

```go
package workerkit

// Config is what every block-event worker needs. Parsed once, validated once.
type Config struct {
    QueueURL, RedisAddr, DBURL, AlchemyURL, S3Bucket, DeployEnv string
    ChainID                                        int64
    MaxMessages, WaitTime, VisibilityTimeout       int
    SweepBlocks                                    int64
    SweepBlocksSet                                 bool
}

// ParseConfig is dexbootstrap.ParseConfig with the -dex flag lifted out and
// `extra` for worker-specific knobs (VAT_ADDRESS, TARGET_DEBT_TOKEN, …).
func ParseConfig(name string, args []string, extra ...Knob) (Config, error)

// Deps is dexbootstrap.Deps plus the fields the eight workers additionally need.
type Deps struct {
    Logger  *slog.Logger
    Config  Config
    ChainName string

    SQSConsumer outbound.SQSConsumer
    CacheReader outbound.BlockCacheReader
    EthClient   *rpchttp.Client
    Multicaller outbound.Multicaller   // already archiving-wrapped
    Pool        *pgxpool.Pool
    Build       *buildregistry.Registry
    Metrics     *telemetry.Metrics

    TxManager    outbound.TxManager
    ProtocolRepo outbound.ProtocolRepository
    TokenRepo    outbound.TokenRepository
    UserRepo     outbound.UserRepository
    EventRepo    outbound.EventRepository
}

// Worker is what a protocol contributes: everything else is the kit's job.
type Worker interface {
    Name() string          // "sparklend-indexer" — OTEL service.name + metric prefix
    ArchiveSource() string // "sparklend"
    Build(context.Context, *Deps) (lifecycle.Service, error)
}

// Run is the whole of main(): parse, bootstrap, build, serve, bound the tail.
func Run(ctx context.Context, w Worker, args []string, onShutdownTimeout func()) error
```

Then each root becomes what the cronjobs already are:

```go
func main() {
    ctx, stop := lifecycle.SignalContext(context.Background())
    err := workerkit.Run(ctx, sparklend.Worker{}, os.Args[1:],
        lifecycle.ForceExitAfter(lifecycle.ShutdownTailBudget))
    stop()
    if err != nil { slog.Error("fatal", "error", err); os.Exit(1) }
}
```

`Build` moves next to the service it constructs (`internal/services/aavelike_position_tracker/worker.go`,
etc.) — which is where the knowledge of *which eight repositories that service needs* actually
belongs, and where a signature change and its call site land in the same file.

**Benefits**

- **Locality**: the nine steps and their order live in one file. `0dd8b1c7`'s "InitOTEL must run
  before the pool" constraint, currently asserted at runtime by
  `telemetry.assertNoInstrumentsPredateTelemetry` (`otel.go:47-56`) *because* per-main source order
  says nothing, becomes structurally true.
- **Leverage**: the six ×5 and ×26 edits above become 1-line edits. Applying the table above, the
  five roots' six-month churn would have been ~8 commits instead of 24.
- **Tests**: `parseConfig` gets one table-driven test instead of 8 near-copies (`dexbootstrap`
  already has 374 lines of them in `parseconfig_test.go`); `Build` is unit-testable against fake
  ports with no Postgres/Redis/LocalStack (see F10.6).
- The drift table above collapses: one Redis config, one signal handler, one `CHAIN_ID` policy.

**Risk / migration**

Incremental and already rehearsed. Move `dexbootstrap` → `internal/workerkit` first (F10.7, no
behaviour change), then adopt one worker per PR, cheapest first: `morpho` and `fluid-vault` (172
identical lines already), then `sparklend`, `prime-allocation`, `oracle-price`, `psm3`, `prime-debt`.
Each PR is one binary and one deploy, matching the soak cadence `58f9c196` asked for. Adoption
*changes behaviour where the copies drifted* — `CHAIN_ID` stops defaulting to mainnet (F10.3),
`chainutil.ValidateS3BucketForChain` starts running, `MaxMessages`/`VisibilityTimeout` gain range
checks — so each PR must state the newly-enforced invariants and confirm the overlay already sets
`CHAIN_ID` (it does; every base is per-chain).

**Depends on / enables**: needs F10.7 first; enables F10.2, F10.3, F10.4, F10.6, F10.8.

---

### F10.2 — Positional constructor arguments make every new dependency a fan-out; the ratchet is visible in the churn

**Strength**: Strong
**Size**: M (per service; L across the eight)

**Files**
- `stl-verify/internal/services/allocation_tracker/handler_prime_positions.go:35-44` — 8 positional params
- `stl-verify/cmd/workers/sparklend-indexer/main.go:311-329` — `NewService` with 13 positional args
- `stl-verify/cmd/backfillers/sparklend-backfill/main.go:228-245` — the **same** 13 args, with `nil, nil` in slots 2–3
- `stl-verify/cmd/workers/morpho-indexer/main.go:320-332` — 11 positional args
- `stl-verify/cmd/workers/fluid-vault-indexer/main.go:308-326` — 9 positional args

**Problem**

`aavelike_position_tracker.NewService` takes a config plus 12 positional ports. It has two call
sites, in `sparklend-indexer/main.go:311` and `sparklend-backfill/main.go:228`, and the backfiller's
copy passes `nil, nil` for the consumer and cache reader. `sparklend-backfill/main.go:177-244` is a
verbatim transcription of `sparklend-indexer/main.go:252-329`: the same eight repositories,
constructed in the same order, with the same error strings. Adding one port to that service is a
two-file edit, plus the `nil` slot count has to stay right.

`NewPrimePositionHandler` shows the ratchet directly: 5 params → 7 (`4dd59ed8`, adding `supplyRepo`
and `txm`) → 8 (`52623743`, adding `atTel`). Each widening was a
`prime-allocation-indexer/main.go` edit in a 19–46-file commit. Same shape for
`aavelike_position_tracker.NewService` (`+multicaller` in `454b74a7`, `+debtTokenRepo` in
`4dc7d353`), `oracle_price_worker.NewService` (`+referenceEffectiveAt` in `9f92e344`), and
`NewBalanceOfSource` (`+atokenReadABI` in `4dd59ed8`).

`ae8dd745` is the purest case: adding a `chain` metric label changed
`morpho_indexer.NewTelemetry()` → `NewTelemetry(chain string)`. Neither root knew a chain *name*,
only a chain *ID*, so **each main grew an `entity.ChainName` call to feed a constructor**
(`oracle-price-indexer/main.go:215-218`, `morpho-indexer/main.go:143-146`,
`prime-debt-indexer/main.go:226-229`, `psm3-indexer/main.go:114-117`,
`prime-allocation-indexer/main.go:178-181`, `sparklend-indexer/main.go:291-294`,
`fluid-vault-indexer/main.go:149-152`, `dexbootstrap/bootstrap.go:230-234`). `f1e74a67` then
repeated the identical move for `multicall.NewTelemetry` in four more roots. Eight copies of
"resolve chainName so a constructor can take it".

Similarly `archivingwire.Bootstrap` returns four values and **9 of its 10 call sites discard the
middle one with `_`** (`grep 'archivingwire.Bootstrap' cmd/` — only
`morpho-vault-backfill/main.go:172` binds `archiveWait`). A 4-tuple return where 90% of callers want
3 is a shallow interface that cost a 92-file commit to widen.

**Proposed change**

Two moves, both narrow:

1. Replace the positional port lists with the `Deps` struct F10.1 introduces. `NewService(cfg,
   deps)` where `deps` is `*workerkit.Deps` (or a service-local struct embedding it) makes a new
   port a one-field addition, and the compiler still catches an unmapped field —
   `dexbootstrap.Deps.CommonDeps()` (`bootstrap.go:88-98`) already documents exactly this rationale:
   "a port added to Deps surfaces as a build error here, not a silently unmapped field at three
   call sites."
2. Resolve `chainName` and the archiving wrap once, in the kit, and hand both to the service. Then
   `Bootstrap`'s 4-tuple can shrink to `(Wrap, Drain, error)` with the wait exposed as a method on
   the one caller that needs it.

For `aavelike_position_tracker`, replace the `nil, nil` backfill mode with a second, explicit
constructor (`NewBackfillService(cfg, deps)`) so the two call sites stop sharing a positional list
that neither fully uses.

**Benefits**

Locality: a service's dependency list lives in one struct next to the service, not spread across two
`main.go` files. Leverage: the ×5 edits in the churn table become ×1. Tests: a `Deps` literal with
three fields set is a far cheaper fixture than a 13-arg call with ten fakes.

**Risk / migration**

Mechanical and compiler-checked; do it service-by-service after F10.1's `Deps` exists, so the struct
is not invented twice. `nil`-mode removal in `aavelike_position_tracker` needs its integration test
(`cmd/backfillers/sparklend-backfill/main_integration_test.go`, 816 lines) to keep passing.

**Depends on**: F10.1.

---

### F10.3 — The same env var means different things in different binaries; three of the disagreements are safety-relevant

**Strength**: Strong
**Size**: M

**Files**
- `CHAIN_ID`: `morpho-indexer/main.go:137`, `sparklend-indexer/main.go:141`, `prime-debt-indexer/main.go:129`, `fluid-vault-indexer/main.go:143`, `oracle-price-indexer/main.go:123`, `prime-allocation-indexer/main.go:149` (all `env.Get("CHAIN_ID", "1")`) vs `psm3-indexer/main.go:104-107` and `dexbootstrap/parseconfig.go:175-178` (required) vs `base/watcher/main.go:277` (`env.Require`) vs `chainutil.RequireChainID()` (`internal/pkg/chainutil/chainutil.go:98`, used by 6 cronjobs/backfillers)
- `DATABASE_URL`: 27 call sites, 3 policies — `env.Require` (`transform-worker/main.go:30`, `morpho-v2-bootstrap/main.go:123`, `morpho-vault-backfill/main.go:98`, `transform-bootstrap/main.go:239`, `migrate/main.go:23`, `generate-er/main.go:30`); silently default to `postgres://postgres:postgres@localhost:5432/…` (`base/watcher/main.go:301`, `psm3-indexer/main.go:68`, `prime-debt-indexer/main.go:79`, all 5 cronjob `PoolOpener` lines, `gen-transformed/main.go:40`, `reference-capital-backfill/main.go:68`, `offchain-price-backfill/main.go:71`); error with a bespoke message (8 workers/backfillers)
- LocalStack endpoint: `AWS_SQS_ENDPOINT` (9 binaries), `AWS_S3_ENDPOINT` (`s3adapter.NewReaderFromEnv`), `AWS_SNS_ENDPOINT` (watcher), `AWS_ENDPOINT_URL` (`sparklend-backfill/main.go:144,156`)
- Queue URL: `AWS_SQS_QUEUE_URL` (8 binaries) vs `SQS_QUEUE_URL` (`raw-data-backup/main.go:102`)
- `stl-verify/internal/pkg/chainutil/chainutil.go:26-42` (`ValidateS3BucketForChain`) — called by only 3 of the 8 binaries that read `S3_BUCKET` directly

**Problem**

`env.Get("CHAIN_ID", "1")` in six workers means a Deployment that forgets `CHAIN_ID` silently
indexes **Ethereum mainnet** and writes mainnet rows tagged as whatever chain the operator meant.
`dexbootstrap` fixed this and wrote down why: `"CHAIN_ID environment variable is required (no silent
default to mainnet)"` (parseconfig.go:177). The other six still carry the trap.

`dexbootstrap` also cross-checks the bucket against the chain and env
(`chainutil.ValidateS3BucketForChain`, parseconfig.go:199-201) — "Catches a staging-bucket /
prod-deploy mixup at boot; pre-fix, this would only surface as missing/stale data hours later" — and
refuses the mainnet-only Alchemy default for a non-mainnet chain (parseconfig.go:206-208), and
range-checks `VisibilityTimeout` ∈ [0,43200] and `MaxMessages` ∈ [1,10] (parseconfig.go:166-173).
**None of those four guards exists in the eight hand-rolled roots.** They read the same env vars and
build the same clients. The mainnet-default trap is the same class of bug as the bucket mixup
`chainutil` was written for.

The `DATABASE_URL` split is the same problem in the other direction: `transform-worker/main.go:27-29`
carries the reasoning ("a deployed worker that silently connected to a local (empty) database would
report healthy while materializing nothing") and four other roots repeat that comment nearly
verbatim, while 13 roots still default to localhost.

Four spellings of "the LocalStack endpoint" is a test-only cost today, but
`sparklend-backfill/main.go:141-162` bypasses `awsconfig.Load` entirely to honour `AWS_ENDPOINT_URL`,
which is why it also lacks the AKID-without-secret guard (F10.5).

**Proposed change**

Make each env var have exactly one reader with one documented policy, in `internal/pkg/env` or
`internal/pkg/chainutil` — the packages that already hold `RequireChainID`,
`ValidateS3BucketForChain`, `ValidateSNSTopicForChain`, `EnvironmentFromBucket`, `ParseLogLevel`,
`ReferenceEffectiveAt`. Concretely: `chainutil.RequireChainID64()` returning `(int64, string, error)`
(id + name, since every caller wants both — see F10.2); `env.RequireDatabaseURL()`;
`awsconfig.Options{Endpoint: …}` reading one `AWS_ENDPOINT_URL` with the per-service overrides kept
only as deprecated aliases. Then have F10.1's `workerkit.ParseConfig` be the single reader of the
twelve fleet-wide vars, so the policy is enforced by the type rather than by 8 copies of the same
`if == "" { return err }`.

Also worth a one-page `docs/configuration.md` table (55 vars × required/default/who reads it) —
today the only way to answer "what does this pod need?" is to read a `main.go`.

**Benefits**

Removes a whole class of silent-misconfiguration incident (mainnet default, wrong bucket, localhost
DB). Locality: 27 `DATABASE_URL` sites → 1. Leverage: adding a fleet-wide var is one edit.

**Risk / migration**

Tightening `CHAIN_ID` is a behaviour change: verify every ConfigMap sets it before flipping (all 54
Deployments use `envFrom` with a per-workload ConfigMap, and every base is per-chain, so this looks
safe — but confirm with agent 13). `DATABASE_URL` tightening breaks any local workflow relying on
the localhost default: land it with the `make run-*` targets updated in the same PR. Ship the
tightening *before* F10.1's adoption PRs, or ship it as part of each one, so the diff attributes
cleanly.

**Depends on / enables**: independent of F10.1 but cheaper if landed inside it.

---

### F10.4 — `run()` is the largest function in `cmd/`, in direct conflict with the repo's own strongest rule, and nothing enforces it

**Strength**: Strong
**Size**: S (enable the linter) + M (the extractions it forces, mostly subsumed by F10.1)

**Files**
- `stl-verify/cmd/backfillers/aave-like-user-snapshot-indexer/main.go:185-452` (268 lines)
- `stl-verify/cmd/workers/prime-allocation-indexer/main.go:169-385` (217)
- `stl-verify/cmd/workers/psm3-indexer/main.go:66-274` (209 — config parsing inlined in `run`)
- `stl-verify/cmd/workers/morpho-indexer/main.go:161-340` (180), `sparklend-indexer/main.go:161-337` (177)
- `stl-verify/cmd/backfillers/sparklend-backfill/main.go:103-274` (172)
- `stl-verify/.golangci.yml:9-12` — enabled linters are `gocritic` and `modernize` only
- Counter-example: `stl-verify/cmd/base/watcher/main.go:142-229` — an 88-line `run()` decomposed into
  `startTrace`, `startPprofServer`, `loadWatcherConfig`, `openRedisCache`, `openEventSink`,
  `openDependencies`, `newServices`, `serveUntilShutdown`

**Problem**

`stl-verify/AGENTS.md` (Function composition) names orchestration functions and `main` flows as the
place the rule matters most — "A single sprawling handler that inlines decode + snapshot + persist
is a defect, not a style preference" — and lists "comment-delimited sections inside a function" as
an extraction signal. `prime-allocation-indexer/main.go:169-385` has eleven such banner comments
(`// SQS`, `// Redis (block cache)`, `// S3 + cache reader with fallback`, `// Ethereum`,
`// Database`, …), each of which is the helper name the rule asks for. The same AGENTS.md admits the
backstop is missing: "a function-length / complexity linter (golangci-lint `funlen`/`gocognit`) is
the **planned** deterministic backstop". `.golangci.yml` confirms it is not enabled. 15 of 360
non-test functions in `cmd/` exceed 120 lines; 10 are a `run()`.

The watcher shows the target shape exists and is achievable in this codebase.

**Proposed change**

Enable `funlen` (say 80 lines / 50 statements) and `gocognit` in `.golangci.yml` with a
`//nolint` allowlist for the files F10.1 is about to delete, so the ratchet is one-way. Then the
F10.1 adoption PRs land under an enforced ceiling rather than by reviewer vigilance. For the roots
F10.1 does not cover (`aave-like-user-snapshot-indexer`, `sparklend-backfill`,
`null-payload-refill`), decompose along the existing banner comments into
`openInfrastructure`/`buildRepositories`/`buildService` helpers, as the watcher already does.

**Benefits**

Deterministic instead of "a 254-line function once slipped through" (AGENTS.md's own note).
Locality: a reader of `run()` sees the outline, not the mechanics.

**Risk / migration**

Enabling the linter fails CI on ~15 pre-existing functions. Land it with a per-file exclusion list
and delete entries as F10.1 proceeds; that list is also a live progress tracker for the epic.

**Depends on / enables**: pairs with F10.1; independent of the rest.

---

### F10.5 — 11 of 32 binaries never initialise telemetry, and 4 hand-roll AWS config, so a whole class of binary runs blind and unguarded

**Strength**: Strong
**Size**: M

**Files**
- No `InitOTEL` and no Temporal kit: `cmd/util/null-payload-refill`, `cmd/util/gen-transformed`,
  `cmd/util/migrate`, `cmd/util/generate-er`, `cmd/util/stress-test/data-export`,
  `cmd/util/stress-test/mock-blockchain-server`, `cmd/backfillers/raw-block-bulk-downloader`,
  `cmd/backfillers/sparklend-backfill`, `cmd/backfillers/aave-like-user-snapshot-indexer`,
  `cmd/backfillers/oracle-pricing-backfill`, `cmd/backfillers/transform-bootstrap`
- Hand-rolled `config.LoadDefaultConfig`: `cmd/backfillers/sparklend-backfill/main.go:141-151`,
  `cmd/backfillers/raw-block-bulk-downloader/main.go:307`,
  `cmd/util/stress-test/data-export/main.go:104,150,165`,
  `cmd/util/stress-test/mock-blockchain-server/main.go:151`
- The helper they bypass: `stl-verify/internal/pkg/awsconfig/awsconfig.go:41-75`
- The comment that names this exact anti-pattern: `cmd/workers/internal/dexbootstrap/bootstrap.go:267-274`

**Problem**

`sparklend-backfill` constructs a multicall client with no telemetry
(`main.go:216`, `multicall.NewClient(ethClient, blockchain.Multicall3)` — no `WithTelemetry`) and
never calls `InitOTEL`, so its DB query errors, multicall batch sizes and duration histograms go
nowhere. `telemetry.assertNoInstrumentsPredateTelemetry` (`otel.go:47-56`) guards *ordering* but
cannot catch a binary that never initialises at all: its rationale ("a database refusing every
connection can crash-loop a service while `db_query_errors_total` stays flat") applies with more
force to a backfiller that runs for hours writing millions of rows.

`awsconfig.Load` exists specifically "so `cmd/*` entry points don't each reimplement the
LocalStack-friendly static-creds fallback" (awsconfig.go:1-4) and carries two behaviours the
hand-rolled copies lack: the `eu-west-1` default (vs `us-east-1` in the SDK chain / `cfg.awsRegion`
flag defaults) and the `AWS_ACCESS_KEY_ID`-without-`AWS_SECRET_ACCESS_KEY` error
(awsconfig.go:59-67). `dexbootstrap`'s `loadAWSConfig` comment states this was already litigated:
"Pre-N8-1/N8-2 this function inlined its own version with a us-east-1 default and no guard — that
divergence is exactly what the dedup pass was supposed to eliminate." Four binaries still carry it.

**Proposed change**

(a) Route the four hand-rolled loads through `awsconfig.Load`, extending `Options` with the
`UsePathStyle`/`AWS_ENDPOINT_URL` handling `sparklend-backfill` needs (which also fixes F10.3's
endpoint-name sprawl in one place). (b) Give the backfillers a `backfillkit.Run` mirroring
`temporal.RunWorker`'s `newBootstrap` (`temporal.go:182-242`) — logger + `InitOTEL` + pool +
unwind-on-error — so a one-shot CLI gets the same observability as a worker for ~10 lines.
`reference-capital-backfill/main.go` (82 lines total, 8-line `run()`) and
`offchain-price-backfill/main.go` are the proof that shape fits a backfiller.
(c) Add a CI check (or a `cmd`-tree test) asserting every `main` package reaches exactly one of
`InitOTEL` / `temporal.Run*` / `workerkit.Run` — `dex-indexer/main_test.go:134` shows the team
already writes this style of structural assertion.

**Benefits**

A backfill's failures become visible in the same dashboards as a worker's; the AKID guard and region
default stop being per-binary luck. `migrate` and `generate-er` legitimately need no telemetry — the
CI check should allowlist them explicitly rather than leave the gap ambiguous.

**Risk / migration**

Low. Adding `InitOTEL` to a one-shot CLI adds a deferred flush bounded by
`telemetry.ShutdownFlushTimeout` (10s), already inside `lifecycle.ShutdownTailBudget`.

**Depends on / enables**: independent; overlaps F10.3 on the endpoint naming.

---

### F10.6 — The `main.go` integration tests are service end-to-end tests wearing a composition-root hat: 8,804 lines, four pieces of infrastructure, 59 `t.Setenv` calls in one file

**Strength**: Worth exploring
**Size**: L

**Files**
- `stl-verify/cmd/base/watcher/main_integration_test.go` (1,503 lines)
- `stl-verify/cmd/backfillers/morpho-vault-backfill/main_integration_test.go` (827)
- `stl-verify/cmd/backfillers/sparklend-backfill/main_integration_test.go` (816)
- `stl-verify/cmd/workers/prime-debt-indexer/main_integration_test.go` (726, **59** `t.Setenv` calls)
- `stl-verify/cmd/workers/morpho-indexer/main_integration_test.go` (520), `psm3-indexer` (519), `prime-allocation-indexer` (456)
- `stl-verify/cmd/workers/sparklend-indexer/main_integration_test.go:40-47` (`TestMain` → `testutil.RunShared`), `:54-117` (the setup preamble)

**Problem**

AGENTS.md states two rules: "`main.go` entry points should also have 100% coverage… Move the
`main.go` body into a `run(ctx, args) error`" and "For `main.go` files, only create integration
tests." Neither holds. **7 of 32 binaries have no `main.go` test at all**
(`reference-capital-backfill`, `anchorage-indexer`, `offchain-price-indexer`,
`reference-capital-indexer`, `gen-transformed`, `generate-er`, `migrate`), and 19 have `main_test.go`
unit tests — which are the *good* tests (13 of ~14 test functions across the workers are
`TestParseConfig*`: fast, no infra, exactly the seam that matters).

What the integration tests actually cost, per `sparklend-indexer/main_integration_test.go`: a shared
Postgres via `testutil.RunShared` + `SetupTestDB`, a shared Redis, LocalStack S3, a mock SQS HTTP
server, a mock RPC HTTP server, 14 `t.Setenv` calls, a seeded Borrow receipt, and then assertions
about *borrower rows* — i.e. the bulk of the file exercises
`aavelike_position_tracker`'s behaviour through the composition root. `prime-debt-indexer`'s needs 59
`t.Setenv` calls to do it. The composition-root part being tested is: does env→config resolve, and
do the nine steps run in an order that doesn't crash.

Consequence: `cmd/**/main*.go` carries **1.46 test lines per production line** (12,629 : 8,677), and
adding a worker means writing another 200–700-line infra-heavy file. Two of the ten workers
(`psm3-indexer`, `cex-orderbook-indexer`) have 175–210 lines of config logic and **no**
`main_test.go`, because the cheap seam is not the one the rule points at.

**Proposed change**

Make the seam match the rule's intent: `run()` becomes a thin shell over `Wire()`.

- `ParseConfig(name, args) (Config, error)` — pure. One table-driven test per binary covering
  required/default/precedence/range. `dexbootstrap/parseconfig_test.go` (374 lines, one file) already
  does this for all the shared vars; after F10.1 it covers all eight workers at once.
- `Wire(ctx, cfg, deps) (lifecycle.Service, error)` — the nine steps, with the ports injected.
  Unit-testable against fakes: asserts *ordering* and *error paths* (a failing Redis Ping must not
  leave a pool open) with no containers. This is the assertion
  `assertNoInstrumentsPredateTelemetry` currently makes at runtime because there is no such seam.
- Keep **one** end-to-end `main_integration_test.go` per *shape*, not per binary — one for the SQS
  worker path, one for the Temporal cronjob path, one for the Temporal on-demand path, plus the
  watcher (which is genuinely its own shape). Move the protocol-behaviour assertions down into the
  service packages' own integration tests, where they belong and where they don't need a mock SQS
  server.

**Benefits**

CI time and flake surface drop with the container count. Coverage of the composition root goes *up*
while infrastructure goes down — the 7 untested binaries become cheap to cover, because covering
them no longer means standing up LocalStack. AGENTS.md's rule can then be restated as something
achievable and be true.

**Risk / migration**

The real risk is losing an end-to-end assertion that currently catches a wiring bug. Mitigate by
keeping the per-shape E2E test genuinely end-to-end (real SQS→Redis→Postgres for one worker) and
migrating protocol assertions down one service at a time, deleting from the `cmd/` test only what
has landed elsewhere. Do this *after* F10.1, when there is one wiring path to test instead of eight.

**Depends on**: F10.1 (the `Wire` seam is `workerkit.Run`'s internals).

---

### F10.7 — `dexbootstrap` is the fleet's worker kit, imprisoned under `cmd/workers/internal/` where two-thirds of the fleet cannot import it

**Strength**: Strong
**Size**: S

**Files**
- `stl-verify/cmd/workers/internal/dexbootstrap/{bootstrap.go,parseconfig.go}` (274 + 211 lines, 785 lines of tests)
- Importers: `cmd/workers/dex-indexer/{main.go,factories.go}` — and a doc-comment reference from
  `stl-verify/internal/pkg/telemetry/otel.go:42`

**Problem**

Answering probe 6: `dexbootstrap` is the *only* extracted composition-root kit for SQS workers. It
was created when three DEX binaries (curve, uniswap-v3, balancer) were folded into one `dex-indexer`
selected by a `-dex` flag / `DEX` env var, and its package doc says so: "Without this helper, each
worker's main.go duplicates ~300 LOC of identical setup; the reviews for VEC-79 (N7-3 + S3) flagged
the duplication". It is the highest-quality startup code in the tree — it is the only place that
validates the S3 bucket against the chain, refuses a silent mainnet default, range-checks the SQS
knobs, unwinds partially-initialised resources on error, and projects its ports through a
compiler-checked mapping (`CommonDeps`, bootstrap.go:88-98).

It lives under `cmd/workers/internal/`. Go's `internal` rule means **only packages under
`cmd/workers/` can import it**. `cmd/base/watcher`, all seven `cmd/cronjobs/*`, all eight
`cmd/backfillers/*` and all six `cmd/util/*` are locked out by the directory choice — 22 of 32
binaries. Nothing about its content is DEX-specific except the `Dex` config field and
`DexTelemetry`.

The imprisonment already leaks the other way:
`stl-verify/internal/pkg/telemetry/otel.go:42` — a shared infrastructure package — explains its
runtime assertion by pointing at `cmd/workers/internal/dexbootstrap`: "half the affected binaries
open their pool inside a shared helper (`cmd/workers/internal/dexbootstrap`), where per-main source
order says nothing." A domain-adjacent package documenting itself in terms of a `cmd/` internal
package is a sign the dependency is pointing the wrong way.

**Proposed change**

Move `cmd/workers/internal/dexbootstrap` → `stl-verify/internal/workerkit`, splitting the two
DEX-specific fields (`Config.Dex`, `Deps.DexTelemetry`, and `CommonDeps`' `dexconsumer` projection)
into a thin `internal/workerkit/dexkit` that wraps it. Pure move plus two field relocations; no
behaviour change. Rename `Bootstrap` → `workerkit.Open` and `ParseConfig` → `workerkit.ParseConfig`.
Then delete the `otel.go:42` reference in favour of naming the kit.

**Benefits**

The kit becomes importable by the 22 binaries that need it, which is the precondition for F10.1.
Its 785 lines of tests start protecting the whole fleet rather than one binary.

**Risk / migration**

Near-zero: one directory move, two importers, no behaviour change. `go build ./...` and
`go vet ./...` are the whole verification (both currently clean).

**Enables**: F10.1, F10.2, F10.5, F10.6.

---

### F10.8 — One image per binary costs 21 Makefile target groups, 24 roster lines and 54 Deployments; the stated blocker does not actually block consolidation

**Strength**: Worth exploring
**Size**: L

**Files**
- `stl-verify/Dockerfile.common:6-13` (the `CMD_PATH`/`BIN` contract), `:76-92` (`BIN`, `APP_BIN`, entrypoint)
- `stl-verify/Dockerfile.common:8-10` — the stated reason `BIN` is per-service
- `stl-verify/Makefile` — 35 `docker-release-*` + 21 `_docker-release-*-internal` targets in 2,851 lines
- `k8s/image-roster.txt:36-61` — 24 roster lines, 13 `service` + 11 `cronjob`
- `k8s/base/` — 56 base dirs, 54 Deployments, 2 Jobs
- `k8s/base/sparklend-position-tracker/deployment.yaml:40-45` — `exec: ["/bin/sh","-c","pgrep -f sparklend"]`

**Problem**

`Dockerfile.common` is already one shared build, parameterised: "Pick the target with build args:
`CMD_PATH` … `BIN`". So the *Dockerfile* is consolidated but the *images* are not — each binary gets
its own ECR repo (`service`) or its own tag prefix in a shared repo (`cronjob`). The roster comments
show the fleet has already learnt the better pattern in three places:
`watcher … # one binary, chain via env`; `dex-indexer … # curve-indexer + uniswap-v3-indexer bases;
DEX env selects the factory`; `cex-orderbook-indexer … # one image for every per-exchange order book
pod; EXCHANGE env selects`. Per-chain fan-out is 13 `*watcher*` bases, 7 `*allocation-tracker*`,
6 `*backup-worker*`, 6 `*watcher-data-validator*`, 4 `*psm3-indexer*`, 3 `*oracle-price-worker*`,
3 `cex-orderbook-indexer-*`, 2 `*sparklend*`, 2 DEX — all one image each, differentiated only by
ConfigMap. **The chain axis is already collapsed; the protocol axis is not.**

The Dockerfile states why `BIN` stays per-service: "the k8s liveness probes match the process with
`pgrep -f <BIN>`, so the running process name must stay the service-specific binary name." That
reason does not hold: `pgrep -f` matches the **full command line**, arguments included. The probes
are already substring matches — `pgrep -f sparklend`, `pgrep -f morpho`, `pgrep -f allocation`,
`pgrep -f watcher` — so `stl-worker sparklend-indexer` matches `pgrep -f sparklend` exactly as
today's binary does. The `BIN` names are also already inconsistent (`oracle_price_worker` and
`sparklend_position_tracker` with underscores, everything else with dashes), and the `cronjob` kind
already ships **11 different binaries in one repo** under one `BIN=cronjob`
(`grep 'BIN=' Makefile` → `BIN=cronjob`), which is the consolidation this finding proposes, already
running in production for the cronjobs.

**Proposed change**

After F10.1 exists, add `cmd/stl-worker` — a `main()` whose only job is
`registry[os.Args[1]]` → `workerkit.Run(ctx, worker, os.Args[2:], …)`, with the registry being a
literal slice of `workerkit.Worker` implementations (the shape `dex-indexer/main.go:44-51` uses:
"builds the DEX → Factory map explicitly (no `init()` registration / package-level singletons), so
the set of supported DEXes is visible at the single call site"). Deployments gain
`args: ["sparklend-indexer"]`; the roster collapses ~9 `service` lines into one; the Makefile loses
~9 target groups (~45 targets).

Do **not** consolidate `watcher` (WebSocket + SNS + reorg, a different shape), the Temporal
binaries (already consolidated as `cronjob`), or the util CLIs.

**Benefits**

One build, one push, one tag for the whole worker fleet: faster CI, one image to scan and promote,
and the `go build` cache in `Dockerfile.common:55` stops being re-warmed per service. The registry
slice becomes the answer to "what workers exist?", replacing "grep the Makefile". Deleting ~45
Makefile targets removes a per-binary edit from the cost of adding a worker.

**Risk / migration**

Real risks, all manageable: image size grows (one binary linking every service — measure before
committing; if it matters, the consolidation is still worth it for the roster/Makefile alone with a
separate build per group); a bad `args` value must fail loudly at boot, not run the wrong worker
(the `dex-indexer` unknown-DEX error at `main.go:64-72` lists valid keys — copy that); the probes
need re-verification per workload; and `ORB-362`'s generated `images:` blocks plus
`verify-ecr-images.sh` need updating in the same PR as the roster. Land it one worker at a time, each
keeping its own Deployment, so a rollback is one manifest.

**Depends on**: F10.1, F10.7. Coordinate with agent 13 (k8s) and whoever owns the Makefile.

---

### F10.9 — Two `cmd/` binaries are whole programs hiding in `main.go`: 893 and 807 lines, one with no tests at all

**Strength**: Strong
**Size**: M each

**Files**
- `stl-verify/cmd/backfillers/raw-block-bulk-downloader/main.go` — 893 lines, 40 top-level
  declarations, including a `pipeline` type with 6 methods (`:414-503`) and a `blockArchiver` type
  with 9 methods (`:505-671`), plus `traceCollector`/`traceWorker`/`uploadWorker`/`reportProgress`
  (`:673-881`)
- `stl-verify/cmd/util/generate-er/main.go` — 807 lines, 29 functions, **the only file in its
  directory** (no `_test.go`), with `fetchHypertableInfo` at 148 lines (`:308-458`) and
  `generateMermaid` at 145 (`:664-808`)

**Problem**

Neither is a composition root. `raw-block-bulk-downloader/main.go` is a concurrent
fetch→plan→archive→upload pipeline with two domain types and a channel topology, living in
`package main` — so nothing outside the binary can construct a `pipeline` or a `blockArchiver`, and
its unit tests can only reach it because they are in the same package. Its siblings show the
extraction is already half-done (`plan.go` 458, `dispatch.go` 180, `finality.go` 37, all with paired
tests); `main.go` is the residue that never moved. `scanJSONStringField` at 232 lines
(`plan.go:228`) is a hand-rolled JSON field scanner — worth a separate look by whoever owns
`internal/pkg`.

`generate-er/main.go` is a schema-introspection tool (6 `fetch*` SQL functions), a Postgres→Mermaid
renderer, a type mapper and a file writer, in one 807-line file with **zero tests** — despite
generating `docs/entity_relation.md`, a document the brief names as a domain-vocabulary source. A
silent regression in `generateMermaid` or `mapDataType` would ship undetected.

**Proposed change**

`raw-block-bulk-downloader`: move `pipeline` and `blockArchiver` into
`internal/services/raw_block_download/`, leaving `main.go` as `parseFlags` + `validateConfig` +
`run` wiring (and pick up `InitOTEL` per F10.5). `generate-er`: split into
`internal/services/erdiagram/` — `introspect.go` (the six `fetch*`), `model.go` (`Table`/`Column`/
`Relationship`/`buildTables`/`mapDataType`), `mermaid.go` (`generateMermaid`/`renderColumn`/
`hypertableComment`) — each with a paired `_test.go`. The Mermaid renderer is pure and needs only a
golden-file test; only the `fetch*` half needs Postgres.

**Benefits**

The pipeline becomes reusable (the watcher's backfill path and the null-payload refiller solve
adjacent problems) and testable without building the binary. The ER generator gains its first tests.
Both bring the two largest files in `cmd/` under the F10.4 ceiling.

**Risk / migration**

Mechanical moves; the existing `plan_test.go`/`dispatch_test.go` (671 lines) protect the downloader.
`generate-er` has no tests, so add golden-file tests for `generateMermaid` against today's
`docs/entity_relation.md` *before* moving anything.

---

### F10.10 — `cmd/base/watcher` diverges from the fleet on four startup details, each of which has a named shared alternative

**Strength**: Strong
**Size**: S

**Files**
- `stl-verify/cmd/base/watcher/main.go:156-158` — `Level: slog.LevelDebug` hardcoded
- `stl-verify/cmd/base/watcher/main.go:97` — `const cleanupTimeout = 15 * time.Second`
- `stl-verify/cmd/base/watcher/main.go:176` — `ServiceVersion: GitCommit`
- `stl-verify/cmd/base/watcher/main.go:492-496` — `lifecycle.Run` + hand-rolled `errors.Is(err, ErrShutdownTimedOut)` check
- `stl-verify/cmd/base/watcher/main.go:189` — `postgres.DefaultDBConfig` (workers use `WorkerDBConfig`)
- `stl-verify/cmd/workers/raw-data-backup/main.go:58-60` — the same hardcoded `slog.LevelDebug`

**Problem**

Four independent drifts, each with a shared alternative in-tree:

1. **`LOG_LEVEL` is ignored.** `env.ParseLogLevel(slog.LevelInfo)` (`internal/pkg/env/loglevel.go:11`)
   is used by 21 binaries; the watcher and `raw-data-backup` hardcode `slog.LevelDebug`. The two
   highest-throughput binaries in the fleet log at debug in production and cannot be turned down
   without a redeploy.
2. **A magic number shadows a named constant.** `cleanupTimeout = 15 * time.Second` (`:97`) equals
   `lifecycle.ShutdownTailBudget` (`internal/pkg/lifecycle/shutdown_budget.go:13`), which every
   other root passes to `ForceExitAfter`. `lifecycle/shutdown_budget_test.go:61-77` derives the
   budget chain from the named constants — so the watcher's copy is **outside the invariant the test
   enforces**. Raising `ShutdownTailBudget` silently leaves the watcher at 15s.
3. **`ServiceVersion: GitCommit`** instead of `buildinfo.GitHash()`, whose doc
   (`buildinfo.go:38-45`) explains it also falls back to `BUILD_GIT_HASH` "which is how `make run-*`
   stamps a `go run` build, where Go embeds no VCS info". A host-run watcher reports an empty
   `service.version`. `cex-orderbook-indexer/main.go:145` has the same bug.
4. **`serveUntilShutdown` re-implements `RunWithTimeoutGuard`.** `lifecycle.go:41-43` is exactly
   `Run` + "call `onShutdownTimeout` on a shutdown timeout, before the error reaches the caller",
   with a doc comment explaining why that ordering matters. The watcher does it by hand.

**Proposed change**

Four one-to-three-line edits: `env.ParseLogLevel(slog.LevelInfo)` in both roots; delete
`cleanupTimeout` and pass `lifecycle.ShutdownTailBudget`; `buildinfo.GitHash()` in both roots;
replace `serveUntilShutdown`'s body with `lifecycle.RunWithTimeoutGuard`. Confirm with agent 13
whether the watcher's pool genuinely wants `DefaultDBConfig` rather than `WorkerDBConfig` — if not,
that is a fifth one-liner; if so, the difference deserves a comment.

**Benefits**

`LOG_LEVEL` starts working on the two loudest binaries. The watcher's shutdown tail comes under the
one test that governs the budget chain. `service.version` stops being empty in local runs.

**Risk / migration**

Item 1 changes production log volume (downward) — flag it in the PR. Items 2–4 are behaviour-neutral.

---

### F10.11 — Two `cmd/` directories contain nothing but a `.env` for binaries that do not exist, and are untracked by git

**Strength**: Strong
**Size**: S

**Files**
- `stl-verify/cmd/workers/orderbook-indexer/.env` (8 lines, references `AWS_SQS_CEX_FEED_QUEUE_URL`, `DB_FLUSH_INTERVAL`)
- `stl-verify/cmd/base/cex-feed-watcher/.env` (6 lines, references `CEX_NAME`, `AWS_SNS_CEX_FEED_TOPIC_ARN`)

**Problem**

`go list ./cmd/...` finds **32** `main` packages, not the 34 the tree suggests, because these two
directories hold only a `.env`. `git log -- <both paths>` returns nothing: they are untracked local
residue. Their contents describe an architecture that no longer exists — a `cex-feed-watcher`
publishing to an SNS `stl-cex-feed` topic consumed by an `orderbook-indexer` — which the live
`cmd/workers/cex-orderbook-indexer` replaced with a direct WebSocket→Postgres daemon
(`main.go:1-5`, "One pod per exchange, selected by the `EXCHANGE` env var"). Four env vars named
here (`CEX_NAME`, `AWS_SNS_CEX_FEED_TOPIC_ARN`, `AWS_SQS_CEX_FEED_QUEUE_URL`, `DB_FLUSH_INTERVAL`)
are read nowhere in the codebase.

They cost real time: anyone counting binaries, generating a per-binary Makefile target, or auditing
the config surface has to work out that two of the directories are ghosts.

**Proposed change**

Delete both directories. Add `cmd/**/.env` to `.gitignore` if per-binary local env files are the
intended workflow (the 15 `run-*` Makefile targets suggest they are), so the residue cannot
accumulate again.

**Benefits**

`ls cmd/*/*/ | wc -l` becomes the binary count. Removes 4 phantom env vars from the config surface.

**Risk / migration**

None; untracked files with no readers.

---

## 4. Cross-area observations

- `internal/pkg/telemetry/otel.go:42` — a shared infrastructure package documents its runtime
  assertion by pointing at `cmd/workers/internal/dexbootstrap`. Dependency direction inverted; see F10.7.
- `stl-verify/internal/services/aavelike_position_tracker.NewService` takes 12 positional ports and
  is called with `nil, nil` in slots 2–3 from `sparklend-backfill/main.go:228` to mean "backfill
  mode". A nil-as-mode-switch inside a service, not a `cmd/` problem.
- `stl-verify/internal/services/shared.SQSConsumerConfig` (services own their poll loop, 8 workers)
  and `stl-verify/internal/common/sqsutil.Config` + `RunLoop` (2,249 lines across 11 files, 953 of
  them one test file, **1 caller**) are two complete SQS consume-loop designs. `sqsutil` is the newer and better one
  (`HandlerTimeout`, visibility-vs-timeout validation, drain/settle/release budgets) and only
  `dex-indexer` uses it. That also forces a duplicate budget test:
  `cmd/workers/dex-indexer/main_test.go:134-146` re-derives the shutdown chain for the `RunLoop`
  path because `lifecycle/shutdown_budget_test.go` only covers the `lifecycle.Run` path.
- `stl-verify/internal/common/` and `stl-verify/internal/pkg/` both hold cross-cutting helpers with
  no visible rule separating them (`common/sqsutil` vs `pkg/{chainutil,rawsckey,s3key,retry,…}`).
- `stl-verify/cmd/backfillers/raw-block-bulk-downloader/plan.go:228` — `scanJSONStringField`, a
  232-line hand-rolled JSON field scanner. Worth checking against `internal/pkg/gziputil`/`hexutil`
  and the standard library.
- `stl-verify/Makefile` is 2,851 lines with 35 `docker-release-*` + 21 `_docker-release-*-internal`
  targets in near-identical five-target groups. Adding a binary means editing the Makefile, the
  roster, and a k8s base — three files before any code.
- `k8s/base/robinhood-watcher/` and `k8s/base/robinhood-allocation-tracker/` exist as bases but
  `robinhood` appears in no roster alias line (`k8s/image-roster.txt:36,40`). For agent 13.
- `entity.ChainName` is called from 8 different composition roots purely to feed a telemetry
  constructor. The `chainID → chainName` resolution wants to happen once, at the config boundary.

## 5. Open questions

- Does every one of the 54 Deployments' ConfigMaps actually set `CHAIN_ID`? F10.3 tightens the
  default away; the manifests use `envFrom` so I could not read the values from `k8s/base` alone
  (the ConfigMaps may be generated or live in overlays). Agent 13 can confirm.
- How large does a single consolidated `stl-worker` binary get? F10.8's cost/benefit turns on it, and
  I did not build one (read-only).
- `raw-data-backup` reads `SQS_QUEUE_URL` while eight siblings read `AWS_SQS_QUEUE_URL`. Deliberate
  (different infra module owns that queue) or drift? Nothing in the code says.
- `cmd/base/watcher` uses `postgres.DefaultDBConfig` where workers use `WorkerDBConfig`. Is the
  watcher's connection profile intentionally different, or did `b44e0b47` simply not reach it?
- Are `migrate`, `generate-er`, `gen-transformed` and the two `stress-test` binaries intended to be
  operator tools exempt from telemetry (F10.5), or is the gap accidental? An explicit allowlist
  either way would settle it.
- `cmd/cronjobs/morpho-v2-bootstrap` sits under `cronjobs/` "by neighbourhood only" per AGENTS.md
  (no schedule, started by hand). Would moving it to `backfillers/` alongside the other on-demand
  Temporal workers be welcome, or does the roster/deploy tooling depend on the path?
