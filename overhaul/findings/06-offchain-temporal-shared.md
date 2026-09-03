Status: FINAL

# 06 — Off-chain indexers, Temporal cronjob framework, shared HTTP/retry/telemetry plumbing

All paths relative to `/Users/tore/workspace/stl/stl-verify/` unless stated.
Verified against `main` @ `c4e0a8f2`. `go vet` clean on every package touched.

## 1. Area map

Two off-chain indexers (Anchorage REST, Maple GraphQL) run as Temporal cronjobs; a
third group of external-API adapters (CoinGecko, Etherscan, Sky, Sky-data, orderbook
venues) is consumed by other cronjobs and workers. Everything sits on three shared
layers: `adapters/outbound/temporal` (lifecycle), `pkg/httpclient` + `pkg/retry`
(transport), `pkg/telemetry` (OTel primitives).

```
cmd/cronjobs/<job>/main.go ──> temporal.RunCronjob(BuildMeta, CronjobConfig{Setup})
cmd/backfillers/<job>/main.go ─> temporal.RunWorker(BuildMeta, WorkerConfig{Register})
        │                              │
        │                              ├─ newBootstrap: slog → InitOTEL → pool → client
        │                              ├─ ensureSchedule / reconcileScheduleSpec
        │                              └─ cronjobWorkflow ──> cronjobActivities.Execute
        │                                                      └─ Runner.Run(ctx)
        ▼ (Setup wires)
services/anchorage_tracker ─────> its OWN Client (net/http, no retry) ─> Anchorage
services/maple_graphql_indexer ─> adapters/outbound/maple.Client ──────> Maple GraphQL
                                    │                    └─> domain/entity/maple (12 files)
                                    └─ borrows httpclient.NonRetryableError, re-implements
                                       DoRequest because httpclient is GET-only

pkg/httpclient (GET only) ──> pkg/retry ──> used by coingecko, etherscan, sky, skydata
pkg/proxytls ──> ONLY alchemy/subscriber.go + pkg/wsclient (no HTTP client uses it)
pkg/telemetry (Init/buckets/StatusAttr/NoopSpan) ──> 11 per-package Telemetry structs
services/shared (11 files) ──> imported by 36 non-test files for 4 unrelated reasons
```

`services/shared` is the only package in the area that is *not* a layer: it is a
grab bag (see F06.1). The Temporal framework, by contrast, is the healthiest module
here — 6 `RunCronjob` callers and 4 `RunWorker` callers, a real seam either way.

## 2. Metrics

| Package | src files | src lines | test files | test lines |
|---|---|---|---|---|
| `internal/services/maple_graphql_indexer` | 2 | 1110 | 4 | 2912 |
| `internal/adapters/outbound/maple` | 1 | 1242 | 1 | 1546 |
| `internal/adapters/outbound/temporal` | 6 | 1099 | 6 | 1614 |
| `internal/domain/entity/maple` | 12 | 812 | 11 | 1421 |
| `internal/services/shared` | 7 | 627 | 4 | 778 |
| `internal/pkg/telemetry` | 5 | 592 | 6 | 443 |
| `internal/services/anchorage_tracker` | 3 | 464 | 2 | 609 |
| `internal/adapters/outbound/sky` | 1 | 404 | 1 | 408 |
| `internal/adapters/outbound/etherscan` | 2 | 380 | 1 | 505 |
| `internal/adapters/outbound/coingecko` | 2 | 343 | 1 | 316 |
| `internal/adapters/outbound/skydata` | 1 | 343 | 1 | 423 |
| `internal/pkg/httpclient` | 1 | 202 | 1 | **27** |
| `internal/pkg/dextelemetry` | 1 | 177 | 1 | 438 |
| `internal/pkg/retry` | 1 | 145 | 1 | 280 |
| `internal/pkg/env` | 3 | 121 | 2 | 134 |
| `internal/pkg/awsconfig` | 1 | 75 | 1 | 152 |
| `internal/pkg/buildinfo` | 1 | 61 | **0** | **0** |
| `internal/pkg/proxytls` | 1 | 46 | 1 | 103 |

Largest functions in the area:

| Lines | Function | File |
|---|---|---|
| 115 | `parseFixedTermLoan` | `adapters/outbound/maple/client.go:714` |
| 98 | `(*Service).syncPools` | `services/maple_graphql_indexer/service.go:219` |
| 91 | `(*Service).syncLoans` | `services/maple_graphql_indexer/service.go:390` |
| 87 | `InitTracer` | `pkg/telemetry/tracer.go` |
| 77 | `(*Service).syncFixedTermLoans` | `services/maple_graphql_indexer/service.go:607` |
| 76 | `(*Service).syncSkyStrategies` | `services/maple_graphql_indexer/service.go:820` |
| 74 | `NewServiceTelemetryWithProvider` | `services/shared/telemetry.go:49` |
| 71 | `(*Client).doSingleRequest` | `adapters/outbound/maple/client.go:1093` |
| 68 | `newBootstrap` | `adapters/outbound/temporal/temporal.go` |
| 56 | `(*Service).runPhases` | `services/maple_graphql_indexer/service.go:140` |
| 53 | `(*Client).doSingleRequest` | `pkg/httpclient/client.go:116` |

Other counts:

| Thing | Count | Note |
|---|---|---|
| Per-package `Telemetry`/`Metrics` structs with their own OTel registration | 11 | 4 different constructor signatures |
| Outbound HTTP-based adapters | 8 | 4 use `pkg/httpclient`, 4 hand-roll |
| Hand-rolled test doubles in area | 7 | token/user repos reuse `internal/testutil` |
| `temporal.RunCronjob` callers / `RunWorker` callers | 6 / 4 | real seams |
| Cronjob `main.go` with **zero** tests | 3 of 7 | anchorage, offchain-price, reference-capital |
| Cronjob `main.go` with a `run(ctx) error` | 1 of 7 | contradicts `stl-verify/AGENTS.md:43` |
| Copies of the literal `postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable` | 11 | non-test |
| `main.go` repeating the `GitCommit/GitBranch/BuildTime` + `PopulateFromVCS` block | 21 of 30 | |
| Dead exported functions found | 1 | `shared.FormatAmount` |

### HTTP plumbing per outbound adapter (probe 4)

| Adapter | Transport | Retry | Rate limit | Error taxonomy | Body-close |
|---|---|---|---|---|---|
| `coingecko/client.go` | `pkg/httpclient` | `pkg/retry` (via httpclient) | `rate` (per-min→per-sec, burst) | `outbound.ErrRequestRejected` on 4xx | httpclient warn-logs |
| `etherscan/client.go` | `pkg/httpclient` | `pkg/retry` (via httpclient) | `rate` (per-sec, burst 1) | `outbound.ErrCanonicalSourceUnavailable` + `httpclient.NonRetryableError` + **private dead `nonRetryableError`** | httpclient warn-logs |
| `sky/risk_capital_client.go` | `pkg/httpclient` | `httpclient.DefaultConfig()` + 5 `if >0` overrides | `DefaultConfig()` (5/s) | none — plain `fmt.Errorf` | httpclient warn-logs |
| `skydata/balance_sheet_client.go` | `pkg/httpclient` | `DefaultConfig()`, **no retry knobs exposed** | `DefaultConfig()` (5/s) | none — plain `fmt.Errorf` | httpclient warn-logs |
| `maple/client.go` | own `http.Client` (POST) | `pkg/retry` directly, **own copy of `DoRequest`** | own `rate.NewLimiter` | `httpclient.NonRetryableError` only | own warn-log |
| `services/anchorage_tracker/client.go` | own `http.Client{Timeout:30s}` | **none** | **none** | none — plain `fmt.Errorf` | `defer resp.Body.Close()`, error dropped |
| `orderbook/instruments.go` | **package-global** `var instrumentsClient` | **none** | **none** | none | `_ = resp.Body.Close()` |
| `alchemy/client.go` | own `http.Client` (+ `pkg/rpchttp` retry transport) | own | own | own | own |

None of the eight wires `pkg/proxytls`.

## 3. Findings

---

### F06.1 — `services/shared` is four unrelated modules in one package; the deletion test splits it cleanly

**Strength**: Strong
**Files**: `internal/services/shared/{abidecode.go:20, abilog.go:24-192, snapshotread.go:22-68, telemetry.go:28-192, config.go:10-46, types.go:4-33, utils.go:14-56}`

**Problem**: 36 non-test files import `internal/services/shared`, but per-symbol caller
analysis shows four disjoint audiences. Applying the deletion test file by file:

| File | Exported symbols → caller packages | Deletion test |
|---|---|---|
| `abidecode.go` | `UnpackUint` → curveindexer, uniswapv3indexer | complexity reappears in **2 DEX** pkgs |
| `abilog.go` | `DecodeLog` → curve/uniswapv3/dexconsumer; `LogBelongsTo`, `GetAddrField`, `GetBigIntField`, `ParseHexUint` → 2 DEX pkgs; `OptionalUintResult` (19 refs), `UnpackSingleUint`, `UnpackUintArray`, `GetBigIntSliceField` → **curveindexer only** | DEX-only; four symbols have a single caller |
| `snapshotread.go` | `SnapshotRead` (119 refs), `RunSnapshotReads` → curveindexer, uniswapv3indexer | DEX-only, genuinely deep |
| `telemetry.go` (192 lines) | `NewServiceTelemetry` → **`cmd/base/watcher` only** | watcher-only |
| `config.go` | `SQSConsumerConfig` → 10 pkgs (4 services embed, 6 mains build) | **earns its keep** |
| `types.go` | `TransactionReceipt` (31 refs / 8 pkgs), `Log` (31 refs / 7 pkgs) | earns its keep, but see F06.9 |
| `utils.go` | `ParseBlockHeader` → 2 pkgs (a one-line `json.Unmarshal` wrapper); `CacheKey` → 1 caller and a divergent copy of `redis.BlockCache.key`; `FormatAmount` → **0 callers, 0 tests** | pass-through + dead |

The package doc contradicts itself twice: `telemetry.go:1` says "shared utilities and
instrumentation for services", `utils.go:1` says "shared utilities for application
services", and `abidecode.go:12`/`abilog.go:17` say "shared by the per-DEX worker
packages". The name buys nothing: a change to the DEX ABI helpers recompiles the
watcher, and `grep shared\.` gives no hint which of four subsystems a call belongs to.

Concrete rot enabled by the grab bag:
- `shared.CacheKey` (`utils.go:21-23`) hardcodes the `stl:` prefix; the real key
  builder `redis/blockcache.go:287` uses the configurable `c.keyPrefix`. Its sole
  caller (`raw_data_backup/service.go:605`) puts the result in an error message, so
  under a non-default `REDIS_KEY_PREFIX` the diagnostic names a key that was never
  looked up.
- `shared.FormatAmount` (`utils.go:27-56`, 30 lines) has no caller and no test.

**Proposed change**: dissolve the package into three homed ones plus a deletion.
1. `abidecode.go` + `abilog.go` + `snapshotread.go` → `internal/pkg/dexabi/` (or
   fold into the existing DEX-scoped `internal/pkg/dextelemetry` neighbourhood as
   `internal/pkg/dex/{abi,snapshot}`). The four curve-only symbols move into
   `services/curveindexer` as unexported helpers.
2. `telemetry.go` → `internal/services/live_data/telemetry.go` (or wherever the
   watcher's service lives), matching every other service's layout.
3. `config.go` + `types.go` → keep, renamed for what they are:
   `internal/pkg/sqsconsumer` (config) and `internal/pkg/jsonrpc` (wire types).
4. Delete `FormatAmount`; delete `ParseBlockHeader` and inline `json.Unmarshal` at
   its 2 call sites; make `CacheKey` take the prefix or delete it and have
   `raw_data_backup` ask the cache adapter for the key it missed.

**Benefits**: locality — a DEX ABI change touches one DEX package instead of a
package the watcher and backup worker import. Leverage — the four one-caller symbols
stop being public API that must keep working. Tests get better: `shared`'s 778 test
lines split to sit beside the code they cover, and the DEX helpers become testable
without pulling `outbound` into a "shared services" package.

**Risk / migration**: mechanical, import-path-only. Land as 4 PRs, one per cluster;
each is `go build ./...`-verifiable. No behaviour change except the `CacheKey`
message fix. Risk is merge conflicts with concurrent work in the 36 importers.

**Size**: L (4 PRs) · **Enables**: F06.5

---

### F06.2 — `pkg/httpclient` is GET-only, so Maple re-implemented it; and the shared client has 27 lines of tests

**Strength**: Strong
**Files**: `internal/pkg/httpclient/client.go:93-202`, `internal/adapters/outbound/maple/client.go:1066-1163`, `internal/pkg/httpclient/client_test.go` (whole file)

**Problem**: `httpclient.Client.DoRequest` hardcodes `http.MethodGet` and takes no
body (`client.go:117`). Maple needs POST for GraphQL, so `maple/client.go` keeps a
near-verbatim copy of the retry/rate-limit/classification wrapper while still
importing `httpclient` for `NonRetryableError`/`WrapNonRetryable`:

`httpclient/client.go:93-114` vs `maple/client.go:1066-1091` — same `isRetryable`
closure, same `onRetry` warn with the identical four keys (`attempt`, `maxRetries`,
`backoff`, `error`), same `retry.DoVoid(...)` body with `limiter.Wait(ctx)` and
`WrapNonRetryable(fmt.Errorf("rate limiter: %w", err))`. Then
`httpclient/client.go:116-168` vs `maple/client.go:1093-1163` repeat the same
429 → retryable / ≥500 → retryable / ≥400 → non-retryable ladder.

The two copies have already drifted:
- `httpclient.isNonRetryable` (`client.go:189-202`) hand-rolls the unwrap loop and
  only follows `Unwrap() error`; maple's copy uses `errors.As` (`client.go:1073-1074`),
  which also traverses `Unwrap() []error`. A `NonRetryableError` inside an
  `errors.Join` is therefore **retried by httpclient and not retried by maple**.
- `isNonRetryable(err, &nonRetryable)` writes an out-param the only caller discards
  (`client.go:96`) — the parameter exists to be ignored.
- Maple's 429/5xx errors carry `readBodySnippet(resp.Body)`; httpclient's do not, so
  a retry-exhausted CoinGecko/Etherscan 5xx loses the upstream diagnostic that Maple
  keeps.

Meanwhile the module four adapters route every request through has one test, covering
a 9-line private mapping function. `DoRequest`, `doSingleRequest`, `isNonRetryable`
and the `NonRetryableError` contract are untested.

**Proposed change**: deepen `httpclient` to own the full request, not just GET.
```go
type RequestConfig struct {
    Method  string            // "" → GET
    URL     string
    Headers map[string]string
    Body    []byte            // nil → no body
}
// plus, for the 429/5xx path:
type Config struct { ...; IncludeErrorBodySnippet bool }  // or always on
```
`isNonRetryable` collapses to `errors.As`. Maple's `execute` becomes
`c.http.DoRequest(ctx, RequestConfig{Method: POST, Body: body, Headers: …}, &envelope)`
plus its GraphQL-envelope handling as an `ErrorParser` — deleting ~55 lines of copied
transport from `maple/client.go` and its share of the 1546-line client test.

**Benefits**: one place enforces "jitter is always on, 429 and 5xx retry, 4xx does
not" — today that invariant is asserted twice and differs. Tests improve twice over:
the shared client finally gets the table-driven httptest coverage that maple's copy
already has, and maple's client test shrinks to parsing.

**Risk / migration**: two steps. (1) Extend `RequestConfig`/`Config` with defaults
that keep GET behaviour byte-identical, add the missing httpclient tests. (2) Port
maple onto it; the GraphQL `errors[]` tolerance logic
(`tolerableUnpriceableCollateral`, `pathThroughCollateral`) must move into the
`ErrorParser` unchanged — it is the piece with real production history, so port it
with its tests first.

**Size**: M · **Enables**: F06.3, F06.4

---

### F06.3 — Six external-API clients spell "apply config defaults" six different ways

**Strength**: Strong
**Files**: `etherscan/client.go:68-115` + `:118-149`, `coingecko/client.go:62-113` + `:116-…`, `sky/risk_capital_client.go:73-108`, `skydata/balance_sheet_client.go:71-107`, `maple/client.go:95-119`, `services/anchorage_tracker/client.go:25-39`

**Problem**: `etherscan.applyDefaults` and `coingecko.applyDefaults` are the same
function. Diffing them yields only three real differences (etherscan has `ChainID`
and a negative-`MaxRetries` clamp; the rate field is named `RateLimitPerSec` vs
`RateLimitPerMin`) across 32 and 26 lines. The other four siblings each chose a
different shape:

- etherscan / coingecko: `ClientConfigDefaults() ClientConfig` + package-level
  `applyDefaults(*ClientConfig, ClientConfig)`.
- sky: no defaults struct; `httpclient.DefaultConfig()` then five `if cfg.X > 0`
  overrides written out longhand.
- skydata: same as sky but exposes **only** `Timeout` — `MaxRetries`,
  `InitialBackoff`, `MaxBackoff`, `BackoffFactor` are not configurable at all.
- maple: a `func (c *Config) applyDefaults()` method on the config itself.
- anchorage: no config struct — `NewClient(baseURL, apiKey string)`.

Two of them additionally declare a config field that is never read:
`etherscan/client.go:64-65` and `coingecko/client.go:58-59` both carry
`// HTTPClient is an optional custom HTTP client` / `HTTPClient *http.Client`. Neither
is referenced anywhere (`grep HTTPClient` across both packages returns only the
declarations). The seam is advertised and does not exist, so every test must reach
for `BaseURL` + `httptest` instead.

**Proposed change**: one embedded config in `pkg/httpclient`:
```go
// httpclient.Options is the caller-facing knob set; zero fields take DefaultConfig().
type Options struct {
    Timeout, InitialBackoff, MaxBackoff time.Duration
    MaxRetries int
    BackoffFactor float64
    Rate rate.Limit; Burst int
    Logger *slog.Logger
}
func (o Options) config() Config   // one applyDefaults, tested once
```
Each adapter's `ClientConfig` embeds `httpclient.Options` and keeps only its own
fields (`APIKey`, `BaseURL`, `ChainID`, `Now`). Delete both `applyDefaults` copies,
the two dead `HTTPClient` fields, and maple's method.

**Benefits**: locality — the "0 means default, negative means disabled" rule is
decided once instead of four times with three answers. Leverage — skydata gets retry
knobs for free; adding a knob (e.g. `MaxIdleConns`) stops being a six-file change.

**Risk / migration**: per-adapter, one PR each, behaviour-preserving except that
skydata gains knobs it previously ignored. Etherscan's negative-`MaxRetries` clamp is
a documented behaviour and must survive into `Options.config()`.

**Size**: M · **Depends on**: F06.2

---

### F06.4 — `anchorage_tracker` keeps a whole outbound HTTP adapter inside a service package, with no retry, no telemetry and swallowed URL errors

**Strength**: Strong
**Files**: `internal/services/anchorage_tracker/{client.go, types.go, service.go:14-17}`

**Problem**: four distinct violations of the area's own conventions, all in one
place, and all invisible unless you compare the package with its five siblings:

1. **Adapter in the service layer.** `client.go` (156 lines) is an HTTP adapter and
   `types.go` (82 lines) is a set of API wire DTOs, both in
   `internal/services/anchorage_tracker`. Its port `AnchorageClient` is declared in
   `service.go:14-17` — inside the service, not in `internal/ports/outbound/`
   (where `AnchorageSnapshotRepository`/`AnchorageOperationRepository` correctly
   live, `ports/outbound/anchorage_repository.go:11,17`). `stl-verify/AGENTS.md:212-215`
   is explicit: interface in `ports/outbound`, adapter in `adapters/outbound/<name>`.
   Every sibling (coingecko, etherscan, maple, sky, skydata) follows it.
2. **No retry, no rate limit.** `client.go:35-37` is a bare
   `&http.Client{Timeout: 30 * time.Second}`. A single upstream 500 fails the tick.
   Every sibling routes through `pkg/httpclient`.
3. **Ignored errors.** `client.go:26` `parsed, _ := url.Parse(baseURL)` and
   `client.go:149` `base, _ := url.Parse(c.baseURL)`. A malformed base URL yields
   `baseHost == ""`, which makes the pagination-host guard at `client.go:143` reject
   every absolute next-URL — it fails closed, but with an error that names the wrong
   cause. `AGENTS.md:181` ("Never ignore errors") and `:183` ("Fail hard and early").
4. **No telemetry.** The package has no `telemetry.go`; the only signal is
   `cronjob.runs.total` from the framework. Maple, reference-capital and
   transform-worker all have one. `filterActivePackages` (`service.go:143-157`) drops
   packages with a per-package `Warn` and no counter, so "every package went inactive"
   is unalertable.

Two smaller smells: `poll` and `syncOperations` both return `(int, error)` and `Run`
discards both counts (`service.go:50,53`); `BackfillOperations` (`service.go:62-70`)
is exported, wraps `syncOperations` in two log lines, and has no caller outside its
own test.

**Proposed change**: move `client.go` + `types.go` to
`internal/adapters/outbound/anchorage/`, put `AnchorageClient` in
`internal/ports/outbound/anchorage_client.go` alongside the two repositories, and
route requests through `pkg/httpclient` (a paginating GET client is exactly its
shape). Add `anchorage_tracker/telemetry.go` with the same
cycles/rows-written/skipped-packages instruments its siblings have. Propagate the two
`url.Parse` errors out of `NewClient`. Drop the unused `int` returns and either wire
`BackfillOperations` to a binary or delete it.

**Benefits**: locality — Anchorage's wire shapes stop being part of the service's
public surface, so a service test no longer sees `Package`/`Operation`. Leverage — it
inherits jittered retry and the 429/5xx ladder for free. Tests get better: the client
test can move to the adapter package and the service test mocks only the port.

**Risk / migration**: the move is mechanical (one importer:
`cmd/cronjobs/anchorage-indexer/main.go:18`). Adding retry changes timing behaviour
under upstream failure — it turns 1 request into up to 4, which the Anchorage rate
limit must tolerate; pick a conservative `RateLimit` and verify against the live API
per `AGENTS.md:169`.

**Size**: M · **Depends on**: F06.2, F06.3

---

### F06.5 — Eleven per-package `Telemetry` structs repeat one registration skeleton; the codebase already solved this once and stopped

**Strength**: Strong
**Files**: `pkg/dextelemetry/telemetry.go:1-8` (the existing solution), plus
`services/{maple_graphql_indexer,morpho_indexer,oracle_price_worker,psm3,reference_capital_indexer,transform_worker,allocation_tracker,shared}/telemetry.go`,
`adapters/outbound/{alchemy,orderbook}/telemetry.go`,
`pkg/blockchain/multicall/telemetry.go`, `adapters/outbound/postgres/query_telemetry.go`

**Problem**: eleven files, ~2,200 source lines, all shaped identically:
`const instrumentationName = "<this package's own import path>"` → a struct of
`metric.*` fields → `NewTelemetry()` delegating to a `WithProvider(s)` variant → a run
of `t.x, err = meter.Int64Counter(name, WithDescription(…)); if err != nil { return
nil, fmt.Errorf("creating x counter: %w", err) }` → `Record*` methods each opening
`if t == nil { return }`. Instrument counts: alchemy 9, morpho 11, oracle 8,
transform 8, reference-capital 6, dextelemetry 5, orderbook 5, maple 4+1, psm3 3,
allocation 1, multicall 1.

The same seam has four names and four signatures:

| Constructor | Files |
|---|---|
| `NewTelemetryWithProvider(mp)` | reference_capital_indexer, transform_worker |
| `NewTelemetryWithProvider(mp, chain)` | psm3, allocation_tracker |
| `NewTelemetryWithProviders(tp, mp)` | maple_graphql_indexer |
| `NewTelemetryWithProviders(tp, mp, chain)` | morpho_indexer, oracle_price_worker |
| `NewServiceTelemetryWithProvider(mp)` | services/shared |
| `NewTelemetry(prefix, chainID)` | pkg/dextelemetry |

The `chain` attribute — the label every Vector alert groups by — is produced three
ways: `entity.ChainName(chainID)` with a hard failure on unknown IDs
(`dextelemetry/telemetry.go:55-58`), a caller-supplied `chain string` (morpho, oracle,
psm3, allocation), and a hardcoded `attribute.String("chain", "ethereum")`
(`maple_graphql_indexer/telemetry.go:49`). Only the first validates.

The decisive evidence that this is duplication and not coincidence is
`pkg/dextelemetry/telemetry.go:1-8`, which says so: *"The structure mirrors the
per-package telemetry in services/morpho_indexer and services/oracle_price_worker
but accepts a prefix so the three workers can share one implementation instead of
duplicating it."* The consolidation was designed, proven on three workers, and never
applied to the other eight.

**Proposed change**: promote the dextelemetry idea into `pkg/telemetry` as a
declarative registry:
```go
// pkg/telemetry
type Spec struct {
    Counters   map[string]string // name → description
    Histograms map[string]string // seconds-unit; buckets from SecondsDurationBuckets
    Gauges     map[string]string
}
type Set struct{ … }             // nil-safe Add/Record by name; one error per bad spec
func NewSet(instrumentation string, chainID int64, s Spec) (*Set, error)
```
`instrumentationName` is derived from the caller's package via `reflect`/`runtime`
or passed once. Each service keeps a thin typed façade — `func (t *Telemetry)
RecordPhase(ctx, phase, dur, err)` — over a `*Set`, so call sites are unchanged and
the 40-line registration run becomes a 6-line `Spec` literal. `chainID` goes through
`entity.ChainName` once, so maple's hardcoded `"ethereum"` becomes validated.

**Benefits**: leverage — a new instrument is one map entry, not a field plus 6 lines
plus a nil guard. Locality — the seconds-bucket rule, the `status` attribute
convention and the `chain` label all get exactly one enforcement point (today
`SecondsDurationBuckets` is passed explicitly at 8 sites and backstopped by a view).
Tests: `pkg/telemetry` gets one table-driven test for the registration contract in
place of eight near-identical `telemetry_test.go` files (currently 443 + 438 + 374 +
… lines).

**Risk / migration**: metric names and attributes must not change — they are wired
into `alerts/*.yaml`. Migrate one package per PR, starting with the two smallest
(`allocation_tracker` 1 instrument, `multicall` 1), and diff the exported series in
a local OTLP capture before and after. `dextelemetry` folds in last.

**Size**: L (one PR per package, ~8) · **Depends on**: F06.1 (moves `shared/telemetry.go` out first)

---

### F06.6 — The retry-stable snapshot timestamp is a real seam honoured by 1 of 6 cronjobs, enforced only by a comment

**Strength**: Worth exploring
**Files**: `adapters/outbound/temporal/workflow.go:24-42,242`, `cmd/cronjobs/maple-graphql-indexer/main.go:110-120`, `services/anchorage_tracker/service.go:121`, `services/reference_capital_indexer/service.go:139`, `db/migrations/20260319_120000_create_anchorage_tables.sql:34`

**Problem**: `cronjobWorkflow` records `workflow.Now(ctx).UTC()` once in the workflow
history (`workflow.go:242`) and threads it to the activity, so every server-side
retry of a run sees the same instant; `ScheduledAtFromContext` is the accessor
(`workflow.go:32-35`). `AGENTS.md:34` states the invariant plainly: "Ticks must be
idempotent (Temporal retries)."

Exactly one cronjob uses it — Maple, in its composition root
(`maple-graphql-indexer/main.go:113-119`), with a `logger.Warn` fallback for
in-flight old workflows. The two other snapshot-writing cronjobs read the wall clock
inside the service instead:

- `anchorage_tracker/service.go:121` `now := time.Now().UTC()`, stamped into every
  row's `SnapshotTime`. The table's dedup key is
  `UNIQUE (prime_id, package_id, asset_type, custody_type, snapshot_time)`
  (migration line 34), so with the framework default of 5 attempts, a run that fails
  after `SaveSnapshots` succeeded writes a second full snapshot set at a new
  `snapshot_time` that the constraint cannot collapse.
- `reference_capital_indexer/service.go:139` `syncedAt := s.now().UTC()`. Here the
  behaviour is *deliberate* and documented (`service.go:178,235`: "every table is
  append-only and a retry stamps a fresh synced_at") — but nothing distinguishes the
  deliberate case from the Anchorage one at the seam.

So the invariant lives in a `RunnerFunc` closure in one `main.go`. A new cronjob
copies the template in `docs/temporal_guide.md:169-183`, which does **not** include
the `ScheduledAtFromContext` branch, and silently gets wall-clock behaviour.

**Proposed change**: make the seam impossible to miss by moving the choice into
`CronjobConfig`. Either widen `Runner` to a second, time-taking interface
```go
type ScheduledRunner interface { RunAt(ctx context.Context, scheduledAt time.Time) error }
```
which `RunCronjob` prefers when the runner implements it, or add an explicit
`CronjobConfig.SnapshotTime` enum (`ScheduleStable` | `WallClock`) that
`RunCronjob` validates, so "wall clock" is a recorded decision rather than a
default. Then audit Anchorage: either take `scheduledAt` (dedup works) or state in
the migration why duplicate snapshot sets are acceptable.

**Benefits**: the retry-idempotency invariant moves from a comment in one
composition root to the framework's interface — new cronjobs cannot get it wrong by
copying the recipe. Anchorage's snapshot table starts deduping the retries its
unique constraint was clearly written for.

**Risk / migration**: changing Anchorage's `snapshot_time` source changes what a
retry writes; verify against the constraint on a restored snapshot before landing.
The `ScheduledRunner` variant is additive and cannot break the five existing runners.
Update `docs/temporal_guide.md`'s recipe in the same PR.

**Size**: S–M · **Depends on**: F06.4 (Anchorage half)

---

### F06.7 — Maple's four `Get*` collection methods repeat one 35-line paginate/nil-check/parse skeleton

**Strength**: Strong
**Files**: `adapters/outbound/maple/client.go:387-424, 429-466, 473-505, 509-538`

**Problem**: `GetPools`, `GetActiveLoans`, `GetActiveFixedTermLoans` and
`GetSkyStrategies` are the same function four times, ~145 lines total. Each:

```go
wires, err := fetchAll(c.logger, "<label>", <batchSize>, func(first, skip int) ([]<W>, error) {
    var resp struct{ Data struct{ <Field> *[]<W> `json:"<field>"` } `json:"data"` }
    if err := c.execute(ctx, <query>, pageVariables(first, skip), &resp); err != nil {
        return nil, fmt.Errorf("querying <label> (skip=%d): %w", skip, err)
    }
    if resp.Data.<Field> == nil {
        return nil, fmt.Errorf("querying <label> (skip=%d): API returned null <field> collection", skip)
    }
    return *resp.Data.<Field>, nil
})
if err != nil { return nil, err }
out := make([]<D>, 0, len(wires))
for _, w := range wires {
    d, err := parse<X>(w); if err != nil { return nil, err }
    out = append(out, d)
}
return out, nil
```

Everything that varies is data: query constant, JSON field name, wire type, DTO type,
parse function, batch size, log label. Three of the four even carry a comment
pointing at `GetPools` as the original ("Pointer decode: … (see GetPools)"), which is
the duplication admitting itself. The only genuine per-method behaviour is the
warn-logging in `GetPools` (`:414-420`) and `GetActiveLoans` (`:454-462`) — and
`GetActiveFixedTermLoans` documents at `:492-494` that it deliberately has none.

Adding a sixth collection today means copying the block a fifth time; changing the
null-collection rule means editing it in four places.

**Proposed change**: one generic collection fetcher parameterised by a spec.
```go
type collection[W, D any] struct {
    Label     string
    Field     string        // GraphQL response key
    Query     string
    BatchSize int
    Parse     func(W) (D, error)
    Inspect   func(*Client, context.Context, W, D)  // optional null-warn hook
}
func fetchCollection[W, D any](ctx context.Context, c *Client, spec collection[W, D]) ([]D, error)
```
Decode into `struct{ Data map[string]json.RawMessage }`, look up `spec.Field`, and
treat a missing key or a JSON `null` as the same hard error the four copies raise
today. Each `Get*` becomes ~8 lines declaring its spec. `GetSyrupGlobals`
(`:542-555`) stays as-is — it is a singleton, not a collection.

**Benefits**: ~145 lines → ~60. `client.go` drops below 1,150 and the pointer-decode
invariant ("a null collection must fail hard, not look like an empty result") is
enforced once at the seam instead of asserted in four comments. `client_test.go`
(1546 lines) can test the fetcher's pagination/null contract once and each
collection's parsing separately.

**Risk / migration**: low; error message text should be preserved verbatim because
the existing tests assert on it. Land after F06.2 so the transport move and this
refactor do not collide in the same file.

**Size**: S · **Depends on**: F06.2 (same file)

---

### F06.8 — Maple's four sync phases repeat a 6-step skeleton, and `runPhases` hand-maintains a growing `errors.Join` ladder

**Strength**: Strong
**Files**: `services/maple_graphql_indexer/service.go:140-196` (`runPhases`), `:219-317` (`syncPools`), `:390-481` (`syncLoans`), `:607-684` (`syncFixedTermLoans`), `:820-892` (`syncSkyStrategies`)

**Problem**: two nested duplications in the largest service in the area.

**(a) The phase bodies.** Four functions of 98 / 91 / 77 / 76 lines, each inlining
the same six steps: fetch from the client → check for an empty collection → check ID
uniqueness (`requireUniqueIDs`) → walk the rows emitting `RecordNullDowngrade` per
nullable field → one `txManager.WithTransaction` closure that resolves FK ids, builds
registry entities, records them, builds state entities from the returned id map, and
saves the states → `RecordRowsWritten` + `logger.Info`. Each body carries
comment-delimited sections and a nested build-loop, which
`stl-verify/AGENTS.md:147-149` names as extraction signals and calls "a defect, not
a style preference". `syncPools` at 98 lines is the largest function in the service.

Comparing the four side by side also surfaces an inconsistency only visible by
reading all of them: `syncPools` treats zero rows as a **hard error**
(`:227-229`, "refusing to treat an empty pool set as a valid snapshot") while
`syncLoans` (`:395-403`), `syncSkyStrategies` (`:825-831`) and
`syncFixedTermLoans` all treat zero as `Warn` + explicit-zero metric + `return nil`.
Both rules are defensible; nothing at the seam records which a phase chose.

**(b) `runPhases` (56 lines).** Four repetitions of:
```go
xErr = s.runPhase(ctx, "<name>", func(ctx context.Context) error { return s.syncX(…) })
if ctxErr := ctx.Err(); ctxErr != nil {
    return errors.Join(<every prior err, positionally>, fmt.Errorf("aborting sync cycle after <name> phase: %w", ctxErr))
}
```
The `errors.Join` argument list grows by one at each rung — a hand-maintained
positional accumulator across four sites. Adding a phase means editing four
`errors.Join` calls, and forgetting one silently drops an error from the joined
result. This is precisely the failure mode `shared/snapshotread.go:28-37` was written
to remove for multicall reads ("no caller maintains an index cursor across reads");
the same service does not apply the idea to its own phases. The pool-failure branch
(`:172-181`) then re-states the skip errors and their `RecordPhase` calls three times.

**Proposed change**: (a) split each phase into the named steps its comments already
delimit — `fetchPools` / `recordPoolNullDowngrades` / `persistPools(tx)` — so each
`syncX` reads as a 6-line outline; the shared shape may then justify a small
`snapshotPhase[W, R, S]` helper, but even without one the length problem is solved.
(b) replace the ladder with a declarative phase list:
```go
type phase struct {
    Name      string
    Run       func(ctx context.Context) error
    DependsOn string // "" = independent; "pools" = skipped with a recorded error if pools failed
}
func (s *Service) runPipeline(ctx context.Context, phases []phase) error
```
`runPipeline` owns the abort-on-cancel check, the accumulate-and-join, and the
skip-with-`RecordPhase` bookkeeping once.

**Benefits**: locality — adding a Maple phase is one list entry instead of edits in
four `errors.Join` calls plus three skip-error lines. Leverage — the abort/join/skip
policy is testable in isolation from Maple's five phases. `service.go` drops well
under 800 lines and the four sync functions come inside one screen each.

**Risk / migration**: the joined-error text and per-phase metric attributes are
asserted by the 1850-line `service_test.go` and by the Vector alerts; keep phase
names and error wording byte-identical. Land (b) first — it is self-contained and
its behaviour is fully covered — then (a) one phase per PR.

**Size**: M–L (2–3 PRs)

---

### F06.9 — Every cronjob `main.go` repeats the same 25-line prologue, including an 11-times-duplicated production DSN default; the guide enshrines it

**Strength**: Strong
**Files**: `cmd/cronjobs/{anchorage-indexer,maple-graphql-indexer,offchain-price-indexer,reference-capital-indexer,transform-worker,watcher-data-validator}/main.go:1-56`, `cmd/backfillers/{offchain-price,reference-capital,morpho-vault}-backfill/main.go`, `cmd/cronjobs/morpho-v2-bootstrap/main.go`, `docs/temporal_guide.md:123-183`, `Dockerfile.common:58`

**Problem**: `temporal.RunCronjob` *is* a deep module — this finding is not about the
framework, it is about the ~25 lines each caller still writes to reach it. Across the
six cronjob mains, `main()` plus the build-metadata block is identical except for
three `CronjobConfig` string fields. Duplicated verbatim:

- `var (GitCommit; GitBranch; BuildTime string)` + `init() {
  buildinfo.PopulateFromVCS(&GitCommit, &BuildTime) }` — 9 lines, in **21 of 30**
  `main.go` files. `Dockerfile.common:58` already stamps both `main.*` **and**
  `buildinfo.GitCommit`/`buildinfo.BuildTime`, and `buildinfo.Resolve()`
  (`buildinfo.go:46-55`) already implements the full ldflags → VCS → `BUILD_GIT_HASH`
  fallback. Only `GitBranch` has no `buildinfo` home. So all 21 blocks exist to
  re-derive what `buildinfo` already knows.
- `signal.NotifyContext(…SIGINT, SIGTERM)` → `RunCronjob` → `cancel()` →
  `slog.Error("fatal", …)` → `os.Exit(1)` — 15 lines, 6 times.
- `postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL",
  "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")))` — the
  110-character literal appears **11 times** in non-test code.

That last one is not merely repetition. `transform-worker/main.go:27-35` deliberately
does the opposite, with the reason written out: *"Require DATABASE_URL rather than
default to localhost: a deployed worker that silently connected to a local (empty)
database would report healthy while materializing nothing."* That reasoning applies
verbatim to the other five cronjobs, which all keep the localhost default. One
binary was fixed; ten copies of the hazard remain, and
`docs/temporal_guide.md:169` hands the hazardous line to the next author as the
recommended template.

Two more divergences the side-by-side exposes:
- `AGENTS.md:43-44` requires every binary to extract `run(ctx, args) error` from
  `main()` and `:191` requires 100% `main.go` coverage. **1 of 7** cronjobs has a
  `run()` (morpho-v2-bootstrap); **3 of 7** have no test file at all
  (anchorage-indexer, offchain-price-indexer, reference-capital-indexer), and
  `reference-capital-backfill` has a `run()` but no tests.
- `Name:` is `env.Get("SERVICE_NAME", …)` in transform-worker and
  watcher-data-validator (which is what lets them deploy per chain, with a comment
  explaining it) and a hardcoded literal in the other four — so per-chain deployment
  of, say, the Maple indexer needs a code change rather than a manifest change.
- `maple-graphql-indexer/main.go:54-57` nil-guards `deps.Logger` (`if logger == nil
  { logger = slog.Default() }`); no sibling does, and `newBootstrap`
  (`temporal.go:175-178`) never passes nil.
- `maple-graphql-indexer/main.go:70` reads `os.Getenv("MAPLE_GRAPHQL_ENDPOINT")`
  where every sibling uses `env.Get`.

`docs/temporal_guide.md` is also stale: its "Current cronjobs" table
(`:76-84`) lists 4 jobs; there are 6 scheduled ones (`transform-worker` and
`reference-capital-indexer` are missing).

**Proposed change**: give the framework the prologue.
```go
// pkg/buildinfo — add GitBranch (stamped alongside the two existing vars)
func Meta() (commit, branch, buildTime string)

// adapters/outbound/temporal
func Meta() BuildMeta                 // from buildinfo.Meta()
func MainCronjob(cfg CronjobConfig)   // NotifyContext + RunCronjob + cancel + Exit(1)
func MainWorker(cfg WorkerConfig)     // ditto for the on-demand path
// postgres
func DatabaseURLOpener() (func(context.Context) (*pgxpool.Pool, error), error)  // Require, no localhost default
```
Each cronjob `main.go` collapses to a package doc, a `setupRunner`, and:
```go
func main() { temporal.MainCronjob(temporal.CronjobConfig{
    Name: env.Get("SERVICE_NAME", "anchorage-indexer"),
    IntervalEnv: "ANCHORAGE_INDEX_INTERVAL", IntervalDefault: "15m",
    OpenDatabase: opener, Setup: setupRunner,
}) }
```
Add `-X …buildinfo.GitBranch=${GIT_BRANCH}` to `Dockerfile.common:58` and delete the
21 var/init blocks. Rewrite the guide's recipe and refresh its cronjob table.

**Benefits**: locality — the "fail on a missing DATABASE_URL" decision, the shutdown
signal set, and the exit-code convention each get one home instead of 6–11. Leverage
— the `SERVICE_NAME` per-chain capability becomes the default for every cronjob.
Tests: with the prologue behind `MainCronjob`, each `main.go` is a `setupRunner` plus
a config literal, which is exactly what the three untested cronjobs need to reach the
mandated coverage; the framework's own `temporal_test.go` covers the rest.

**Risk / migration**: switching `DATABASE_URL` from defaulted to required changes
local-run ergonomics — `make dev-env` already writes `.env` files, so verify each
`run-*` target still starts. Do the `buildinfo.GitBranch` ldflags change and the
var-block deletion as one mechanical PR (21 files, all deletions); do `MainCronjob`
and the DSN change as a second, per-binary.

**Size**: M · **Enables**: F06.6 (the guide's recipe is where the missing
`ScheduledAtFromContext` branch belongs)

---

### F06.10 — Five sibling adapters use four different error taxonomies; CoinGecko swallows malformed rows into partial success

**Strength**: Strong (the swallow); Worth exploring (the taxonomy)
**Files**: `coingecko/client.go:239-269`, `:289-303`; `etherscan/client.go:262-285`, `:322-333`; `sky/risk_capital_client.go:166,192,272`; `maple/client.go:1145-1162`; `services/anchorage_tracker/client.go:111-117`; `ports/outbound/{price_provider.go:16, block_verifier.go:12}`

**Problem (the swallow)**: `coingecko.GetHistoricalData` walks three response arrays
and, for each, drops any element that fails a length check with a `logger.Warn` and
keeps going:

```go
for _, p := range response.Prices {
    if len(p) >= 2 { result.Prices = append(…) } else {
        c.logger.Warn("malformed price data point from CoinGecko API", "assetID", assetID, "dataPoint", p)
    }
}
```
— repeated at `:239-247` (prices), `:249-258` (volumes), `:260-269` (market caps).
The caller receives a `*outbound.HistoricalData` that looks complete and is silently
short. `AGENTS.md:184` is unambiguous: "Never swallow a failure into partial success…
Silent partial data is the worst outcome: it looks healthy, and repairing the holes
later forces a backfiller rerun." This feeds `offchain-price-backfill`, so the holes
land in the price history the risk models read. Contrast `maple/client.go:558-560`,
which states the opposite rule for the same class of problem ("Every malformed value
fails the whole call with the owning entity's ID in the error — rows are never
silently skipped") and implements it.

**Problem (the taxonomy)**: the five adapters classify failures five ways, so each
caller re-derives what a returned error means:

| Adapter | Permanent | Transient-exhausted | Read by |
|---|---|---|---|
| coingecko | `outbound.ErrRequestRejected` (4xx) | untagged | `offchain_price_fetcher/service.go:358` |
| etherscan | `httpclient.NonRetryableError` pass-through | `outbound.ErrCanonicalSourceUnavailable` | `data_validator/service.go:646` |
| maple | `httpclient.NonRetryableError` | untagged | nobody inspects |
| sky / skydata | untagged | untagged | nobody inspects |
| anchorage | untagged (no retry at all) | n/a | nobody inspects |

Each sentinel has exactly one producer and one consumer — two hypothetical seams
dressed as a taxonomy. Worse, `etherscan/client.go:322-333` defines a **private**
`nonRetryableError` type, constructs it at seven sites (`:210,215,223,233,238,248,253`)
and never inspects it anywhere; the only type actually checked is
`httpclient.NonRetryableError` (`:281`). Twelve lines plus seven allocation sites
assert a classification nothing reads.

Body-close handling diverges four ways across the same five adapters: httpclient
warn-logs the close error (`client.go:131-135`), maple warn-logs it
(`client.go:1104-1108`), anchorage drops it (`client.go:109`), orderbook explicitly
discards it (`instruments.go:49`).

**Proposed change**: (a) make CoinGecko's three loops fail the call, returning an
error naming the asset and the offending index — the Maple rule, applied to its
sibling. If a short array is genuinely expected for some assets, gate it
structurally per `AGENTS.md:187` and document the shape. (b) Define the outcome
taxonomy once in `pkg/httpclient`, since that is where the 429/5xx/4xx decision
already lives: `ErrRejected` (permanent, 4xx / parse) and `ErrUpstreamUnavailable`
(retry-exhausted transient). The two `ports/outbound` sentinels become thin aliases
or are deleted in favour of the shared pair. Delete `etherscan.nonRetryableError` and
return `httpclient.WrapNonRetryable` (or the new `ErrRejected`) at its seven sites.
(c) Fold body-close handling into `httpclient`, which is the only place that should
be touching `resp.Body` once F06.2 lands.

**Benefits**: leverage — a service can ask "is this my fault or theirs?" the same way
regardless of which upstream it called, which is what `data_validator`'s
inconclusive-check logic and `offchain_price_fetcher`'s rejection handling each
hand-rolled. Locality — the CoinGecko fix removes a class of hole that can only be
found by re-running a backfiller. Tests: a swallowed row is currently unobservable in
a unit test; a returned error is asserted directly.

**Risk / migration**: (a) is a behaviour change — a malformed CoinGecko page that
today yields a short series will start failing the backfill run. That is the intended
outcome, but check the warn logs in staging first to learn whether the branch ever
fires in practice. (b) is additive; migrate one adapter at a time. (c) after F06.2.

**Size**: S (the swallow) + M (the taxonomy) · **Depends on**: F06.2 for (b)/(c)

---

### F06.11 — `RunCronjob` and `RunWorker` record the same metric through two different mechanisms

**Strength**: Worth exploring
**Files**: `adapters/outbound/temporal/temporal.go:135-141`, `ondemand.go:40-44,93-108,133-138`, `interceptor.go:10-56`, `workflow.go:66-71,95`

**Problem**: `cronjob.runs.total` / `cronjob.run.duration_seconds` reach the exporter
two ways. `RunCronjob` builds `worker.New(boot.client, taskQueue, worker.Options{})`
— no interceptor — and threads `metrics` into `newCronjobActivities`, where
`Execute` calls `a.metrics.RecordRun` (`workflow.go:95`). `RunWorker` builds
`worker.New(cfg.Name, workerOptions(metrics))` with `runMetricsInterceptor` and
passes `nil` metrics to the activity (`ondemand.go:102`). The asymmetry is load-bearing
enough that `RegisterRunner` carries a comment warning that both would double-count:
*"The activity gets no metrics recorder on purpose: RunWorker's interceptor already
records one cronjob.runs.total per activity execution, so a second recorder here
would double every count."*

So `cronjobActivities` has a `metrics` field, `newCronjobActivities` a `metrics`
parameter, and `RunCronjob` a `newCronjobMetrics()` call, all of which exist only
because the scheduled path did not adopt the interceptor the on-demand path added
later. The two also measure slightly different spans: the interceptor wraps the whole
activity invocation, the in-activity recorder wraps only `runner.Run`.

This is the churn probe's answer, incidentally. `temporal.go` changed 7 times since
March, and every commit is a *policy* change arriving at the one place that owns it —
`aba593a4` standardise cron, `38960bd1` Maple's arrival, `71ccbe63` alert on every
failure (ORB-284), `9ed58c31` data-validator throttle noise, `c58ab106` seed
`cronjob_runs_total` to 0, `accea5d9` canceled-vs-error status, `f39aeaf8` on-demand
jobs (VEC-218). That is a deep module absorbing changes on behalf of 10 binaries, not
a module thrashing — none of those commits touched a cronjob `main.go`'s logic. The
one seam that did *not* absorb its change cleanly is this metrics duplication, which
VEC-218 added rather than unified.

**Proposed change**: give `RunCronjob` the same `workerOptions(metrics)` the
on-demand path uses, pass `nil` metrics to `newCronjobActivities`, then delete the
`metrics` field and parameter and the `RecordRun` call from `workflow.go`. Both paths
then measure the same span, and `newCronjobMetrics` has exactly one caller shape.

**Benefits**: locality — one answer to "where does a cronjob run get counted".
Leverage — a future run-level attribute (task queue, attempt number) is added in one
interceptor rather than two recorders. Tests: `metrics_test.go` and
`workflow_test.go` currently both exercise recording; the workflow test can drop it.

**Risk / migration**: the measured duration shifts slightly (activity invocation vs
runner body) — check the `cronjob.run.duration_seconds` p99 alert thresholds in
`alerts/vector-cronjobs.yaml` before landing. `seedStatusSeries` must still run once
per worker, which it will (it is in `newCronjobMetrics`). Verify the `canceled` status
classification still works from the interceptor's ctx — `runStatusAttr` reads
`ctx.Err()`, and the interceptor's ctx is the activity ctx, so it should.

**Size**: S

---

### F06.12 — `entity/maple`'s 23-file split earns its keep; the residual duplication is the identity/timestamp validation

**Strength**: Speculative
**Files**: `internal/domain/entity/maple/*.go` (12 source, 11 test, 812 + 1421 lines)

**Problem**: the probe asked whether the file-per-type split pays. It does. The 23
files are 10 entity types (`Pool`, `PoolState`, `Loan`, `LoanState`,
`LoanCollateral`, `FTLLoan`, `FTLLoanState`, `SkyStrategy`, `SkyStrategyState`,
`SyrupGlobalState`) each with a struct, a `New*` constructor, a `Validate`, and a
paired `_test.go` — which is exactly what `AGENTS.md:135` mandates. Files run 43–148
lines; nothing is a god file. `validation.go` already hoists the three big.Int
predicates (`requireNonNegBigInt` used 11×, `requireNonNegBigIntIfSet` 13×,
`requireNonNegInt64`) and `NormalizeSyncedAt` (7 call sites), and its comment
explains why exact `synced_at` equality is load-bearing. This is the best-factored
package in the area.

What remains is a thin repeated identity check: `chainID must be positive` in 5
files, `protocolID must be positive` in 3, `must be 20 bytes` in 4,
`syncedAt must not be zero` in 6. Plus the `New*` shape — assign, `Validate()`, wrap
`fmt.Errorf("New<X>: %w", err)` — 10 times, and a `<X>Params` struct in 4 of the 10
(the ones with enough fields to make positional arguments unsafe) but not the others,
with no rule stating the threshold.

**Proposed change**: only worth doing if touched for another reason. Two small
value types would absorb the residue:
`type Identity struct { ChainID, ProtocolID int64; Address []byte }` with one
`Validate`, embedded in the four registry entities; and
`type SnapshotAt time.Time` (or a `requireSyncedAt(t time.Time) error` helper next to
`NormalizeSyncedAt`) for the six state entities. Separately, pick a rule for
`*Params` — e.g. "4+ fields or two same-typed adjacent fields" — and apply it to all
10 constructors so the shape stops being per-author.

**Benefits**: modest. Locality for the 20-byte-address rule, which is the one likely
to change (a non-EVM chain). Mostly this finding exists to record that the package
does **not** need restructuring, so the overhaul does not spend effort here.

**Risk / migration**: error message text is asserted by 1421 lines of tests; an
`Identity` embed changes field access paths across the service. Low value, non-trivial
churn — defer.

**Size**: S · **Depends on**: nothing

---

### F06.13 — `pkg/proxytls` advertises HTTPS support that no HTTP client wires

**Strength**: Worth exploring
**Files**: `internal/pkg/proxytls/proxytls.go:1-12`, `alchemy/subscriber.go:519`, `pkg/wsclient/wsclient.go:113`

**Problem**: the package doc says it "builds a TLS config that trusts an additional
CA certificate for outbound connections (**HTTPS and WebSocket**) made from behind a
TLS-intercepting proxy." Its only two callers are WebSocket dialers
(`alchemy/subscriber.go:519`, `wsclient.go:113`). No HTTP client uses it: not
`pkg/httpclient` (which builds `&http.Client{Timeout: …}` on the default transport),
not maple, not anchorage, not orderbook, not alchemy's own HTTP client.

The likely explanation is benign — Go's Unix `x509.SystemCertPool` already honours
`SSL_CERT_FILE`, so the default `http.Transport` picks the CA up on Linux and only a
dialer that sets its own `TLSClientConfig` needs help. But on darwin the system trust
store is used and `SSL_CERT_FILE` is not consulted, so the doc's claim and the code's
reach differ on exactly the platform where developers run `make run-*`. As written,
the doc invites the next author to assume HTTPS is covered.

**Proposed change**: either narrow the doc to "WebSocket dialers, which set their own
`TLSClientConfig`" and explain the `SystemCertPool` interaction that makes HTTP
clients fine without it, or — if darwin HTTPS behind a proxy is a real workflow —
have `httpclient.NewClient` build its `http.Client` with
`&http.Transport{TLSClientConfig: proxytls.Config()}`, which fixes all four adapters
that use it at once. Verify empirically before choosing; do not guess.

**Benefits**: removes a documented-but-absent capability. If the transport wiring is
the right answer, it is a 2-line change that covers coingecko, etherscan, sky and
skydata together.

**Risk / migration**: `proxytls.Config()` returns nil when `SSL_CERT_FILE` is unset,
and `&http.Transport{TLSClientConfig: nil}` is the default — but a hand-built
`Transport` loses `http.DefaultTransport`'s connection-pool and proxy settings unless
cloned. Use `http.DefaultTransport.(*http.Transport).Clone()`.

**Size**: S

---

## 4. Cross-area observations

- **21 of 30 `main.go` files** repeat the same 9-line `GitCommit`/`GitBranch`/`BuildTime`
  + `buildinfo.PopulateFromVCS` block, although `Dockerfile.common:58` already stamps
  `buildinfo.GitCommit`/`BuildTime` and `buildinfo.Resolve()` implements the whole
  fallback chain. Only `GitBranch` lacks a `buildinfo` home. (agent 10)
- **11 copies** of the literal
  `postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable` in non-test
  code, spanning cronjobs, workers, backfillers and `cmd/util/gen-transformed`. One
  binary (`transform-worker`) deliberately requires the var instead and documents why;
  the other ten keep the hazard. (agent 10)
- `internal/services/curveindexer` is the sole caller of four `services/shared`
  symbols (`OptionalUintResult` 19 refs, `UnpackSingleUint`, `UnpackUintArray`,
  `GetBigIntSliceField`) — worth checking against the DEX area's own view of what
  should be shared between curve and uniswapv3. (DEX agent)
- `pkg/dextelemetry`'s package doc names `services/morpho_indexer` and
  `services/oracle_price_worker` as the templates it deduplicated; those two files
  (346 and 333 lines, 11 and 8 instruments) are the biggest remaining copies of the
  skeleton in F06.5. (whoever owns the SQS workers)
- `internal/pkg/buildinfo` has **zero** test files despite `Resolve()`'s three-level
  fallback being what every binary's `service.version` depends on.
- `pkg/telemetry/meter.go:75-163` keeps process-global mutable state
  (`startupSeeds`) to work around OTel's own global provider install ordering. It is
  well-documented and probably unavoidable, but it is global state in a `pkg/`, and
  `newBootstrap`'s comment (`temporal.go:161-167`) says the ordering is load-bearing —
  worth one look from whoever audits startup ordering.
- `internal/adapters/outbound/orderbook/instruments.go:16` holds a package-level
  `var instrumentsClient = &http.Client{…}`, which `AGENTS.md:225` forbids for
  service code and which makes the venue-instruments call untestable without a
  package-var swap. (CEX/orderbook agent)
- `docs/temporal_guide.md:76-84`'s cronjob table lists 4 jobs; there are 6 scheduled
  ones. Its "add a cronjob" recipe (`:123-183`) is the source of the boilerplate in
  F06.9 and omits the retry-idempotency branch from F06.6.

## 5. Open questions

- **Anchorage retry semantics.** Does the Anchorage API rate-limit, and what does it
  return on throttle? F06.4 proposes routing it through `pkg/httpclient`, which needs
  a `RateLimit` value; `AGENTS.md:169` requires verifying against the live API rather
  than fixtures. Cannot determine from code.
- **Is Anchorage's duplicate-snapshot exposure (F06.6) real in production?** The
  unique constraint plus wall-clock `snapshot_time` makes retried runs write a second
  set, but whether `SaveSnapshots` has ever partially failed mid-retry is a data
  question. A `SELECT prime_id, package_id, count(DISTINCT snapshot_time)` grouped
  per 15m bucket would settle it.
- **Does CoinGecko's malformed-datapoint branch (F06.10) ever fire?** The
  `logger.Warn` calls are the only evidence trail. If it never fires, the fix is
  free; if it fires regularly, the loops are load-bearing tolerance and the right fix
  is a structural gate plus a documented reason, not a hard error.
- **`chain` label for a non-mainnet Maple.** `maple_graphql_indexer/telemetry.go:49`
  hardcodes `"ethereum"` on the stated ground that the Maple API is mainnet-scoped,
  while the service takes a `ChainID` config field. If Maple ever indexes a second
  chain, which wins? Not answerable from the code.
- **Whether `reference_capital_indexer`'s fresh-`synced_at`-per-retry
  (`service.go:178,235`) is a considered trade-off or inherited.** The comments assert
  it is fine because the tables are append-only; whether the downstream readers
  tolerate two cycles seconds apart is a modelling question.
