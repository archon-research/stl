Status: FINAL
# 11 — Test infrastructure, test doubles, lint/CI/build tooling, dependency hygiene

## 1. Area map

Test infrastructure has one hub and several satellites.

`internal/testutil` (51 files, 8,645 lines, ~65 exported symbols) is imported by **60 packages** —
by far the most-depended-on package in the repo. It carries six unrelated concerns in one flat
namespace:

1. **Shared-service lifecycle** — `RunShared`/`Shared` (`runshared.go`), `StartTimescaleDBForMain`,
   `StartRedisForMain`, `StartLocalStackForMain`, `templatedb.go`, `images.go`, `sharedservices.go`.
2. **Per-test isolation** — `SetupTestDB`, `SanitizeTestName`, `S3TestBucketName`,
   `SQSTestFifoQueueName` (`names.go`).
3. **Domain seeding** — 11 `Seed*`/`Set*` helpers over the registry tables (`seeds.go`).
4. **ABI packing/decoding** — 8 `Pack*`, 4 `Extract*`, `HandleMulticall3` (`abi.go`, `multicall3.go`).
5. **Hand-rolled port mocks** — 13 `Mock*` types in 13 `mock_*.go` files.
6. **Fake HTTP/JSON-RPC servers** — `ethrpc.go`, `sparklend_mock_rpc.go`, `mocksqs.go`.

Two subtrees under `internal/testutil` are **not test infrastructure at all**: `mockchain` (1,776
non-test lines) is the library behind the shipped `cmd/util/stress-test/mock-blockchain-server`
binary (`Dockerfile.mock-blockchain-server`, `make kind-deploy-mock-blockchain-server`), and
`dataexport` backs `cmd/util/stress-test/data-export`. Each has exactly one external importer, and
it is a `cmd/` binary, not a test. `leaktest` has zero external importers by design (it is a
meta-test proving `GOEXPERIMENT=goroutineleakprofile` fires).

```text
                        ┌──────────────────────────────┐
   60 packages ───────► │ internal/testutil (flat, 65   │
   (services, adapters, │ exported symbols, 6 concerns) │
    cmd/*, db/migrator) └───────┬──────────────────────┘
                                │ imports
                    internal/ports/outbound (61 interfaces)
                                ▲
        148 hand-rolled doubles │ (Mock*/fake*/stub*) in 76 files,
        scattered across 60+ packages; 11 ports have ≥3 doubles

   internal/pkg/testutils  (17 lines, 1 func, 1 importer)   ← name collides with testutil
   internal/pkg/metrictest (62 lines, 2 funcs, 4 importers)
   internal/testutil/mockchain, /dataexport  ← libraries of cmd/ binaries, misfiled
```

Tooling: `stl-verify/Makefile` (2,851 lines, 239 targets) is the workflow source of truth;
`lint.mk` holds hooks/format/lint; the root `Makefile` (13 lines) only includes `skills.mk`.
`.github/workflows/ci.yml` orchestrates change detection and calls `go-ci.yml`, which runs three
jobs: lint, unit-tests (`make test-race`), and a 2-way integration matrix with `services:`
containers for TimescaleDB/Redis/LocalStack. `stl-verify/ci/` holds three bash guards
(`check-integration-shards.sh`, `check-ci-services.sh`, `clean-test-templates.sh`) plus the
hand-written shard manifests.

## 2. Metrics

| Metric | Value |
|---|---|
| Go packages | 123 |
| Go files | 874 (457 non-test, 417 test) |
| Go lines | 259,409 (non-test 90,721; **test 168,688**) |
| Test:source line ratio | **1.86 : 1** |
| Test files: unit / integration-tagged | 303 / **114** (27% of files, 28% of test lines) |
| Build tags in use | `integration` (113), `integration \|\| livevalidation` (1), `livevalidation` (1), `leaktest` (1), `benchmark` (1). No `e2e`-tagged file exists although `make e2e` runs `-tags=e2e` |
| Packages with test files | 112 (67 unit-only, 5 integration-only) |
| Port interfaces in `internal/ports` | 61 |
| **Hand-rolled test doubles** | **148** in 76 files |
| Ports with ≥3 independent doubles | 11 |
| Mocking libraries | none generated. `testify/mock` in **3** files (Temporal's own mock testsuite); `gomock` in 0 files (indirect via `go.temporal.io/sdk`); no `//go:generate` for moq/mockery/mockgen |
| `//nolint` directives | **4** (2 revive, 1 gosec, 1 errcheck) in 4 files |
| `go vet ./...` | **clean** (exit 0, no output) |
| `go vet -tags=integration ./...` | **clean** (exit 0) |
| golangci-lint enabled beyond defaults | `gocritic`, `modernize`. **No** funlen / gocognit / gocyclo / depguard / import restrictions |
| Longest production function | 295 lines (`orderbook/kraken.go:141 krakenHandler.handle`) |
| Longest test function | **1,854 lines** (`oracle_price_worker/service_test.go:718 TestStartAndProcessMessages`) |
| Functions > 60 / 100 / 200 lines | production 127 / 40 / 6; test **528 / 165 / 26** |
| `stl-verify/Makefile` | 2,851 lines, 239 unique targets; `docker*`/`_docker*` = **951 lines (33%)** across 82 targets |
| `go.mod` | 28 direct + 90 indirect = 118 modules |
| Churn since 2026-03-01 | `stl-verify/Makefile` **71 commits** (most-changed build file in the repo); `testutil/db.go` 12, `localstack.go` 9, `redis.go` 6, `.golangci.yml` 3 |

**Largest test files** (all god-file candidates): `raw_data_backup/service_test.go` 4,082;
`live_data/live_data_service_test.go` 3,942; `postgres/morpho_repository_integration_test.go` 3,183;
`backfill_gaps/backfill_gaps_service_test.go` 2,950; `postgres/blockstate_repository_integration_test.go`
2,823; `oracle_backfill/service_test.go` 2,720; `oracle_price_worker/service_test.go` 2,569.

**Doubles per port** (≥3, from a method-set match of each double against the 61 port interfaces):

| Port | Doubles | Where |
|---|---|---|
| `outbound.SQSConsumer` | **8** | `testutil.MockSQSConsumer`; `prime_debt.fakeSQSConsumer`; `psm3.fakeSQSConsumer`; `sqsutil.mockConsumer`; `oracle_price_worker.mockConsumer`; `raw_data_backup.mockSQSConsumer`; `dexconsumer.stubSQS`; `dexbootstrap.stubSQSConsumer` |
| `outbound.S3Reader` | 6 | `raw-block-bulk-downloader.fakeListReader`; `morpho-vault-backfill.fakeReplayS3Reader`; `null-payload-refill.fakeS3Reader`; `mockchain.mockS3Lister`; `cache.mockS3Reader`; `sparklend_backfill.mockS3Reader` |
| `outbound.Multicaller` | 6 | `testutil.MockMulticaller`; `curveindexer.fakeMulticaller`; `blockchain.mockHashMulticaller`; `archiving.stubInner`; `dexbootstrap.stubMulticaller`; `dexconsumer.stubMulticaller` |
| `outbound.TxManager` | 6 | `testutil.MockTxManager`; `curveindexer.fakeTxManager`; `reference_capital_indexer.fakeTxManager`; `dexbootstrap.stubTxManager`; `dexconsumer.stubTxManager`; `fluid_vault_indexer.stubTxManager` |
| `outbound.EventRepository` | 5 | `testutil.MockEventRepository`; `curveindexer.fakeEventRepo`; `dexconsumer.fakeEventRepo`; `uniswapv3indexer.fakeEventRepo`; `dexbootstrap.stubEventRepo` |
| `outbound.BlockchainClient` | 4 | `testutil.MockBlockchainClient`; `null-payload-refill.fakeRPCClient`; `raw_data_backup.mockBlockchainClient`; `raw_data_backup.stubBlockchainClient` |
| `outbound.DeadLetterPublisher` | 4 | `raw_data_backup.mockDeadLetterPublisher`; `live_data.mockFailingEventSink`; `live_data.mockOrderTrackingEventSink`; `sns.mockSNSClient` |
| `outbound.BlockCacheReader` | 3 | `oracle_price_worker.mockBlockCacheReader`; `cache.mockCacheReader`; `dexbootstrap.stubCacheReader` |
| `outbound.TokenRepository` | 3 | `testutil.MockTokenRepository`; `dexbootstrap.stubTokenRepo`; `dexconsumer.stubTokenRepo` |
| `outbound.ProtocolRepository` | 3 | `testutil.MockProtocolRepository`; `dexbootstrap.stubProtocolRepo`; `dexconsumer.stubProtocolRepo` |
| `outbound.OnchainPriceRepository` | 3 | `oracle_backfill.mockRepo`; `oracle_price_worker.mockRepo`; `oracle_pricing.mockRepo` |

A further 22 ports have 1–2 doubles; 46 of the 148 double a package-local narrow interface rather
than a port (legitimate, and the better pattern — see F11.1).

**Highest-double packages**: `reference_capital_indexer/service_test.go` 10;
`live_data/live_data_service_test.go` 8; `fluid_vault_indexer/testhelpers_test.go` 8;
`dexbootstrap/bootstrap_test.go` 7; `raw_data_backup/service_test.go` 6; `dexconsumer/deps_test.go` 5.

**Unit-test coverage** (`go test ./internal/... -count=1 -cover`, ran clean in ~11 min wall,
654 s summed; unit tests only — the 114 integration files are excluded):

| Services below 90% | | Services at/above 90% |
|---|---|---|
| `aavelike_position_tracker` **54.7%** | `oracle_backfill` 88.3% | `morpho_v2_bootstrap` 90.0% |
| `shared` **61.1%** | `allocation_tracker` 88.5% | `oracle_price_worker` 92.4%, `oracle_pricing` 92.7% |
| `backfill_gaps` 79.7% | `transform_worker` 89.1% | `data_validator` 93.2%, `reference_capital_indexer` 93.3% |
| `curveindexer` 80.0%, `anchorage_tracker` 81.6% | | `prime_debt`/`psm3` 94.7%, `dexconsumer` 95.1% |
| `morpho_indexer` 84.1%, `sparklend_backfill` 85.1% | | `cex_orderbook_indexer` 96.7%, `reference_capital_backfill` 98.0% |
| `live_data` 85.3%, `uniswapv3indexer` 86.3%, `fluid_vault_indexer` 88.0% | | `maple_graphql_indexer` 98.7%, `offchain_price_fetcher` 99.5%, `raw_data_backup` 99.6% |

`internal/services/sparklend` has **no test files at all**. Notable non-service unit coverage:
`postgres` 7.6% (its tests are integration-tagged), `redis` 15.3%, `httpclient` 1.8%,
`postgres/buildregistry` 0.0%.

**What the brief's dependency hypotheses turned out to be** — mostly false, worth recording:
no `lib/pq` (pgx only), no decimal library at all (`big.Int` per convention), no third-party HTTP
client (stdlib + `otelhttp`), no AWS SDK v1, one AWS SDK v2 major. Real duplicate-major pairs are
all *indirect*: `cenkalti/backoff` v4+v5, `shirou/gopsutil` v3+v4, `containerd/errdefs`,
`moby/moby`, `moby/sys`. `golang/mock v1.6.0` is indirect via `go.temporal.io/sdk/internal`.
`stretchr/testify` is the one genuinely marginal *direct* dep: 3 files, `testify/mock` only.

## 3. Findings

---

### F11.1 — 148 hand-rolled port doubles; 11 ports have ≥3 independent copies, and the repo already writes them in the exact shape `moq` generates

**Strength**: Strong
**Size**: L (2–4 PRs)

**Files**
- `stl-verify/internal/testutil/mock_token_repository.go:12-58`, `mock_tx_manager.go:10-21`,
  `mock_event_repository.go:12-31` (the 13-file `Mock*` family)
- `stl-verify/internal/services/dexconsumer/deps_test.go:10-14`
- `stl-verify/cmd/workers/internal/dexbootstrap/bootstrap_test.go:88-96`
- `stl-verify/internal/services/fluid_vault_indexer/testhelpers_test.go:21-205` (8 doubles)
- `stl-verify/internal/services/reference_capital_indexer/service_test.go:27-245` (10 doubles)
- Full 148-row inventory and the doubles-per-port table in §2

**Problem**

Every `Mock*` in `internal/testutil` follows one convention exactly — an exported
`XxxFn func(...)` field per method, and a method that calls the field when non-nil and returns a
zero-ish default otherwise:

```go
type MockTokenRepository struct {
	GetOrCreateTokenFn func(ctx context.Context, tx pgx.Tx, chainID int64, ...) (int64, error)
	...
}
func (m *MockTokenRepository) GetOrCreateToken(...) (int64, error) {
	if m.GetOrCreateTokenFn != nil { return m.GetOrCreateTokenFn(...) }
	return 1, nil
}
```

That is precisely `moq`'s output shape. So the codebase has *already converged on the generated
form* — it just types it out by hand, 148 times, under three different name prefixes
(`mock`/`Mock` 77, `fake` 48, `stub`/`Stub` 23), in 76 files, with no shared home. Consequences visible in the code:

- **Port change ⇒ N-file fan-out.** Adding a method to `outbound.SQSConsumer` breaks 8 doubles in 8
  packages. `TxManager`, `Multicaller` and `S3Reader` are 6 each.
- **Siblings diverge for no reason.** `internal/services/dexconsumer/deps_test.go:10-14` and
  `cmd/workers/internal/dexbootstrap/bootstrap_test.go:90-96` are the *same* dex feature area and
  declare the *same* embedded-port stubs (`stubMulticaller`, `stubTxManager`, `stubTokenRepo`,
  `stubProtocolRepo`), independently, for the same job (validating a `CommonDeps`/`Deps` struct).
  The only difference: dexbootstrap adds `stubCacheReader`/`stubEventRepo`, dexconsumer instead
  points at hand-written `&fakeCache{}`/`&fakeEventRepo{}` in two other files.
- **`internal/testutil`'s own mocks are losing.** `MockTxManager` exists and 5 packages still wrote
  their own; `MockTokenRepository` exists and 2 more did. So the shared version is not discoverable
  or not flexible enough, and nothing tells an author it is there.
- **Only 10 doubles in the whole repo carry a `var _ outbound.X = (*double)(nil)` assertion**
  (`raw_data_backup` has 5 of them). The other 138 are compile-checked only where they happen to be
  passed to a constructor, so a double can silently drift from the port it claims to double.

The 46 doubles that match no port are largely *good*: they implement a narrow, package-local
interface (`fakeBlockQuerier`, `fakeChainReader`, `fakeHeaderFetcher`, `fakeV3Pool`). Those should
stay hand-written; the problem is confined to the 102 that double a shared port.

**Proposed change**

Two moves, in order:

1. **Generate the port doubles.** Add `//go:generate moq -out mock_x.go . XxxRepository` (or a
   single `mockery` config) over `internal/ports/outbound`, emitting into one
   `internal/testutil/fakes` package. The generated type keeps today's field-function shape, so
   call sites change only in name:
   ```go
   fakes.SQSConsumerMock{ ReceiveMessagesFunc: func(...) {...} }
   ```
   Add a `make generate-check` (in the `ci-checks` chain, next to `integration-shard-check`) that
   regenerates and fails on a diff, so a port edit cannot land without its doubles.
2. **Keep a small hand-written *stub* set for the embedded-port case.** `dexconsumer` and
   `dexbootstrap` both want "a distinct nil-behaviour type per port so a transposed field compares
   unequal". That is one file, `internal/testutil/fakes/stubs.go`, holding
   `type SQSConsumer struct{ outbound.SQSConsumer }` etc. — 12 lines replacing 12 declarations in
   2 packages, and available to the next worker that needs it.

`internal/testutil/mock_*.go` then deletes entirely (13 files, ~800 lines).

**Benefits**

*Locality*: a port and its double move together, enforced by a CI diff check instead of by 8 authors
remembering. *Leverage*: a new worker gets every port double for free — today it writes 5–8 by hand
(`fluid_vault_indexer` wrote 8, `reference_capital_indexer` 10). *Tests get better*: the generated
form records calls, so the "did it get called with X" assertions that today need a bespoke counter
field come for free; and every double is compile-asserted against its port.

**Risk / migration**

Landable port-by-port, highest-fanout first (`SQSConsumer` → `TxManager` → `Multicaller` →
`S3Reader`), one PR per two or three ports. Risk is churn in large test files (`raw_data_backup`
4,082 lines, `live_data` 3,942) — but the edits are mechanical renames. Generated code must be
committed (not git-ignored) so `go test` works without a generate step; that is consistent with the
repo's "never commit generated files" rule only if called out explicitly, so **check with the
maintainer**: the rule's stated exception list (`k8s` image blocks) would need a second entry.

**Depends on / enables**: enables F11.8 (splitting `testutil`).

---

### F11.2 — 951 of 2,846 Makefile lines are 82 near-identical docker targets, and the parameterised helper that would collapse them already exists and is used by only one of the three families

**Strength**: Strong
**Size**: M (one PR, < 1000 lines — almost all deletions)

**Files**
- `stl-verify/Makefile` — `docker-build-*` 28 targets/312 lines; `docker-push-*` 24/108;
  `docker-release-*` 34/286; `_docker-release-*` 20/170
- `stl-verify/Makefile` `_docker-release-go-service` (the parameterised helper)
- `stl-verify/Dockerfile.common:1-92`

**Problem**

19 services each get up to 6 hand-written targets: `docker-build-X`, `docker-build-X-staging`,
`docker-push-X`, `docker-push-X-staging`, `docker-release-X`, `docker-release-X-staging`. All 24
build invocations pass `-f Dockerfile.common`; they differ only in `CMD_PATH`, `BIN`, `ECR_REPO_X`
and a human-readable name. A representative `docker-build-*` body is 24 lines of which 4 vary:

```make
docker-build-oracle-price-worker:
ifdef LOCAL
	docker build ... --build-arg CMD_PATH=cmd/workers/oracle-price-indexer --build-arg BIN=oracle_price_worker -t stl-oracle-price-worker:$(LOCAL_TAG) -f Dockerfile.common .
else
	@echo "==> Building Oracle Price Worker Docker image for ARM64..."
	... 14 more lines ...
	docker buildx build --platform linux/arm64 ... -f Dockerfile.common -t $(ECR_REPO_ORACLE_PRICE_WORKER):$(IMAGE_TAG) ... --load .
endif
```

The decisive evidence that this is unnecessary: the **release** family was already deduplicated.
`_docker-release-oracle-price-worker-internal` is a single line:

```make
	@$(MAKE) _docker-release-go-service GO_NAME="Oracle Price Worker" GO_REPO="$(ECR_REPO_ORACLE_PRICE_WORKER)" GO_CMD_PATH=cmd/workers/oracle-price-indexer GO_BIN=oracle_price_worker GO_CACHE_KEY=oracle-price-worker
```

…which delegates to `_docker-release-go-service` → `_docker-release-image`. So the pattern is
designed, proven and in production — it was simply never applied to `docker-build-*` or
`docker-push-*`. The Makefile is the repo's most-churned build file (**71 commits since March**),
and adding one worker today means touching **6 targets in 1 file plus a `_internal` delegate**.

Two of the three Dockerfiles are the same story: `Dockerfile.migrate` (44 lines) and
`Dockerfile.mock-blockchain-server` (55 lines) replicate `Dockerfile.common`'s build/runtime stages
with a hardcoded `CMD_PATH` and no ldflags. `Dockerfile.common` already takes `CMD_PATH`/`BIN` as
required args; migrate needs one extra `COPY db/migrations/`, mock needs an `EXPOSE`.

**Proposed change**

- Add `_docker-build-go-service` and `_docker-push-go-service` alongside the existing
  `_docker-release-go-service`, taking the same `GO_NAME`/`GO_REPO`/`GO_CMD_PATH`/`GO_BIN` vars.
- Move the 19 services' parameters into a single table near the top of the Makefile — one line per
  service — and generate the six public targets per service with a `$(foreach ...)`/`$(eval ...)`
  or a static-pattern rule. A new worker then adds **one line**.
- Fold `Dockerfile.migrate` and `Dockerfile.mock-blockchain-server` into `Dockerfile.common` behind
  an optional `EXTRA_COPY`/`EXPOSE_PORTS` arg, or keep migrate separate only if the
  `COPY db/migrations/` layer must stay outside the shared cache.

**Benefits**

*Locality*: "how a service is built and pushed" lives in one 40-line block and one table, not
scattered over 951 lines. *Leverage*: adding a worker becomes a one-line diff — directly attacking
the p90-31-files PR problem the brief names. *Tests get better* indirectly: `make ci` gets faster to
reason about, and `check-overlay-images` has one roster to compare against.

**Risk / migration**

Makefile `$(eval)` loops are harder to read than 82 flat targets, and `make <tab>` completion
degrades. Mitigate by keeping the public target *names* byte-identical (generated targets are still
real targets) and landing in two steps: first `_docker-build-go-service`/`_docker-push-go-service`
with the 19 bodies rewritten as one-line delegates (mechanical, reviewable), then the table/foreach
collapse. CI and the deploy workflows call the public names, so they stay untouched. Verify with
`make -n docker-release-all` diffed before and after.

---

### F11.3 — No function-length or complexity linter, so a 1,854-line test function and 6 production functions over 200 lines pass CI

**Strength**: Strong
**Size**: S for the config, L for the backlog it exposes

**Files**
- `stl-verify/.golangci.yml:9-12` (`linters.enable:` is `gocritic`, `modernize` — nothing else)
- `stl-verify/AGENTS.md:196` — "a function-length / complexity linter (golangci-lint
  `funlen`/`gocognit`) is the **planned** deterministic backstop"
- `stl-verify/internal/services/oracle_price_worker/service_test.go:718` —
  `TestStartAndProcessMessages`, **1,854 lines**, 17 `t.Run` subtests in one function
- `stl-verify/internal/services/oracle_backfill/service_test.go:588` — `TestRun`, 1,057 lines
- `stl-verify/internal/adapters/outbound/orderbook/kraken.go:141` — `krakenHandler.handle`, 295 lines
- `stl-verify/cmd/backfillers/aave-like-user-snapshot-indexer/main.go:185` — `run`, 268 lines
- `stl-verify/cmd/workers/prime-allocation-indexer/main.go:169` — `run`, 217 lines
- `stl-verify/cmd/workers/psm3-indexer/main.go:66` — `run`, 209 lines

**Problem**

`AGENTS.md` states the function-composition rule twice, with teeth ("Enforced in the Review phase:
the code-quality reviewer rejects any new or modified function that violates this… which is how a
254-line function once slipped through") and names the linter as "planned". It is still planned.
The measured distribution:

| | > 60 lines | > 100 | > 200 |
|---|---|---|---|
| production functions | 127 | 40 | 6 |
| test functions | **528** | **165** | **26** |

The worst case is instructive. `TestStartAndProcessMessages` is 1,854 lines containing 17
independent `t.Run` scenarios — a direct violation of the stated "One scenario per test… Never chain
independent scenarios in one function — a failure must point at one thing". Nothing in CI objects.
Five `cmd/*/main.go` `run()` functions are 158–268 lines, and `AGENTS.md` singles out exactly this
shape ("main flows… the top-level function must be a readable outline"). The convention is written
down, restated, and unenforced; the only backstop is a reviewer's attention, which the AGENTS.md
comment itself records as having failed.

**Proposed change**

Enable, with a grandfathering baseline rather than a big-bang refactor:

```yaml
linters:
  enable: [gocritic, modernize, funlen, gocognit]
  settings:
    funlen:   { lines: 80, statements: 50 }
    gocognit: { min-complexity: 30 }
  exclusions:
    rules:
      - path: <the ~40 files currently over budget>   # shrink this list, never grow it
        linters: [funlen, gocognit]
```

Add a `nolintlint`-style rule or a CI check that the exclusion list only ever shrinks. `funlen` has
a `ignore-comments` option and can be scoped away from `_test.go` initially — but given 528 test
functions over 60 lines and the explicit one-scenario-per-test rule, tests are where the rule buys
the most, so prefer a *higher* test threshold (say 150) over exempting them.

**Benefits**

*Locality*: the composition rule stops being tribal knowledge re-litigated per PR. *Leverage*: the
reviewer panel's `stl-review-phase` pass gets shorter — the mechanical half is automated.
*Tests get better*: splitting a 1,854-line function into 17 named tests makes a failure name the
behaviour that broke, which is the stated goal of the one-scenario rule.

**Risk / migration**

The grandfathering list is the whole risk: too coarse and it exempts new code in old files. Prefer
golangci-lint's `new-from-rev` for the first few months (only new/changed code is checked), then
convert to an explicit shrinking path-exclusion list. Splitting the god test functions is a separate
L-size effort per package, ordered by size.

**Depends on / enables**: F11.12.

---

### F11.4 — The 114 integration-tagged files (13% of the Go file set) sit outside the default lint gate

**Strength**: Worth exploring
**Size**: S

**Files**
- `stl-verify/.golangci.yml` — no `run:` section at all, so no `build-tags`
- `.github/workflows/go-ci.yml:41-46` — `golangci/golangci-lint-action` with `version:` only, no `args:`
- 114 files matching `^//go:build .*integration`

**Problem**

Measured with `go list`: 757 Go files are visible with no build tags, 871 with
`-tags=integration` — **114 files, 13% of the tagged set, are invisible to the default
`golangci-lint run` and to `go vet ./...`**. Concentration:
`internal/adapters/outbound/postgres` +33, `db/migrator` +15, `oracle_price_worker` +4,
`backfill_gaps` +3, `allocation_tracker` +3. So the two packages with the most database-adjacent
code — where the append-only and snapshot-read invariants live — have almost all of their test code
outside the lint gate.

To be fair to the current state: `go vet ./...` **and** `go vet -tags=integration ./...` both exit 0
with no output, so there is no accumulated debt hiding there today. I could not run the v2 linter
locally to check `gocritic`/`staticcheck`/`modernize` under the tag — the machine has
`golangci-lint v1.64.8` on `PATH` while CI pins `v2.12.2`, and v1 refuses the v2 config schema. So
this is a **future-drift** finding, not a "there are N latent bugs" finding.

**Proposed change**

Add to `.golangci.yml`:
```yaml
run:
  build-tags: [integration]
```
`golangci-lint` type-checks per build configuration, so this widens the file set without a second
run. Then confirm the tagged run is clean and, if it is not, fix or exclude explicitly. Optionally
add `livevalidation`/`benchmark` too (1 file each).

While there: `make tools` installs the pinned `golangci-lint@v2.12.2`, but a stale v1 on `PATH`
silently shadows it and fails with a schema error rather than a version error. A one-line version
assertion in the `golangci-lint` make target (`$(GOBIN)/golangci-lint --version | grep -q 2\.12\.2`)
turns a confusing failure into a clear one.

**Benefits**

*Locality*: one lint configuration covers all the Go in the repo, so a reviewer does not need to know
which files the gate can see. *Leverage*: `staticcheck`'s SA checks over 47,615 lines of integration
test code that currently get none.

**Risk / migration**

Low. Worst case is a batch of first-time findings in `postgres` and `db/migrator`; land the config
change and the fixes in one PR, or gate with `new-from-rev` first.

---

### F11.5 — CI shards integration tests through two hand-written manifests that must together list all 123 packages, so every new package is a required edit in a second file

**Strength**: Strong
**Size**: S

**Files**
- `stl-verify/ci/integration-shards/1.txt` (56 lines), `2.txt` (67 lines) — 123 total
- `stl-verify/ci/check-integration-shards.sh:41-53`
- `stl-verify/Makefile` `test-integration-shard`
- `.github/workflows/go-ci.yml:79-84` (`matrix: shard: [1, 2]`)

**Problem**

The guard requires the two manifests to be an exact partition of `go list -tags=integration ./...`:

```bash
actual="$(go list -tags=integration ./... | sed 's#^github.com/archon-research/stl/stl-verify#.#' | sort)"
if ! diff -u <(printf '%s\n' "$actual") <(printf '%s\n' "$configured"); then
  echo "ERROR: integration shard manifests must contain every package exactly once" >&2
```

So `1.txt` + `2.txt` = **every package in the module**, 123 of them, hand-maintained — including
dozens with no integration test at all (`internal/pkg/hexutil`, `internal/pkg/partition`,
`internal/pkg/buildinfo`, `internal/ports/inbound`). Consequences:

- **Adding any package**, test-bearing or not, reds CI until someone appends it to a manifest.
  That is the definition of a "small change fans out across files".
- **The split is arbitrary and cannot self-balance.** 56 vs 67 entries, assigned by hand. Shard 1
  carries `postgres` (33 integration files) *and* `db/migrator` (15) — the two heaviest — so the
  matrix is lopsided by construction and only rebalances when a human notices. Both jobs have a
  20-minute timeout; nothing measures or reports the actual split.
- The manifests have churned 7 times since March purely as bookkeeping.

**Proposed change**

Replace the manifests with a deterministic computed partition, e.g. in `test-integration-shard`:

```make
packages=$$(go list -tags=integration ./... | awk -v s=$(INTEGRATION_SHARD) -v n=$(INTEGRATION_SHARDS) \
  '{h=0; for(i=1;i<=length($$0);i++) h=(h*31+index("...",substr($$0,i,1)))%1000003; if (h%n==s-1) print}')
```

…or simpler and better: `go list` the packages that actually *have* integration files
(`{{if .TestGoFiles}}`), then round-robin them by index. Delete `check-integration-shards.sh`
(41 lines of guard for a problem that stops existing) and make `INTEGRATION_SHARDS` a variable so
the matrix can widen from 2 to 4 by editing one number in `go-ci.yml`.

For balance, prefer time-based sharding: cache each package's previous duration
(`go test -json` durations as a CI artifact) and greedily bin-pack. That is a follow-up, not the
first PR.

**Benefits**

*Locality*: shard assignment is derived, not declared, so adding a package touches nothing.
*Leverage*: the shard count becomes a tuning knob rather than a refactor. Deletes a 41-line guard
script and 123 lines of manifest.

**Risk / migration**

A hash-based split reassigns packages, so the first run after the change has cold caches — a
one-time slowdown. Land it with the manifests still present but unchecked for one cycle so a
regression is a `git revert` of one target.

---

### F11.6 — Three independent fake Ethereum JSON-RPC servers, six copies of the aggregate3 result struct, and two sources of truth for the Multicall3 ABI — most of it inside the single `testutil` package

**Strength**: Strong
**Size**: M

**Files**
- `stl-verify/internal/testutil/ethrpc.go:26-135` (198 lines) — `StartMockEthRPC`, packs
  `Methods["aggregate3"].Outputs` by hand at lines 69 and 83, declares its own
  `type Result struct` at line 53
- `stl-verify/internal/testutil/sparklend_mock_rpc.go:31-308` — `BuildSparkLendBorrowMockRPC`,
  packs aggregate3 at lines 77 and 129, declares `type mcResult struct` at line 71
- `stl-verify/internal/testutil/multicall3.go:13-25,44-87` — `HandleMulticall3` plus a
  **hardcoded `multicall3ABIJSON`** literal
- `stl-verify/internal/pkg/blockchain/abis/multicall3_abi.go:5` — `GetMulticall3ABI()`
- `stl-verify/internal/testutil/mockchain/` — a third, full mock chain (HTTP RPC + WebSocket +
  admin + replayer, 1,776 non-test lines)
- Hand-rolled `mcResult`: `cmd/backfillers/sparklend-backfill/main_integration_test.go:582`,
  `cmd/workers/morpho-indexer/main_integration_test.go:431`,
  `cmd/workers/fluid-vault-indexer/main_integration_test.go:156`,
  `cmd/workers/prime-allocation-indexer/main_integration_test.go:382`

**Problem**

Three separate implementations answer `eth_call` in tests, and the ABI plumbing is duplicated
*within one package*:

- `internal/testutil/multicall3.go` embeds a hand-written `multicall3ABIJSON` string literal for
  `aggregate3`, while its own package siblings `ethrpc.go:29`, `sparklend_mock_rpc.go:44,228,279`
  and `abi.go:119` all call `abis.GetMulticall3ABI()`. Two spellings of the same ABI, one package.
- `HandleMulticall3(calldata, dispatch SubcallDispatcher)` is exactly the right seam — decode
  `aggregate3`, dispatch each sub-call, re-pack — and it has three consumers
  (`prime-debt-indexer`, `psm3-indexer`, `blockchain/multicall`). But **its two closest neighbours in
  the same package do not use it**: `ethrpc.go` and `sparklend_mock_rpc.go` each re-do the pack by
  hand.
- The identical 2-field result struct `{Success bool; ReturnData []byte}` is declared **six** times
  (`ethrpc.go:53` as `Result`, `sparklend_mock_rpc.go:71` and four `cmd/*/main_integration_test.go`
  files as `mcResult`) — and `outbound.Result` in `internal/ports/outbound/multicaller.go:32` is the
  same shape, and `HandleMulticall3` has a seventh copy as an unexported `abiResult`.
- Meanwhile six hand-rolled `Multicaller` doubles exist elsewhere (F11.1), so a test wanting
  multicall behaviour has three fake servers, one shared decoder and six doubles to choose from,
  with no guidance on which.

**Proposed change**

1. Delete `multicall3ABIJSON`; have `multicall3.go` use `abis.GetMulticall3ABI()` like its siblings.
2. Export the result type once — reuse `outbound.Result` or add
   `testutil.Multicall3Result{Success, ReturnData}` — and delete the six local copies.
3. Add `testutil.PackAggregate3([]Multicall3Result) (string, error)` (the pack half of
   `HandleMulticall3`) and rewrite `ethrpc.go`, `sparklend_mock_rpc.go` and the four
   `cmd/*/main_integration_test.go` sites to call it.
4. Fold `StartMockEthRPC` and `BuildSparkLendBorrowMockRPC` into one
   `testutil.StartMockRPC(t, handlers...)` whose per-method handlers are supplied by the caller —
   the two differ only in which methods they answer and with what fixture. Keep `mockchain`
   separate; it is a different product (F11.8), not a third variant of the same helper.

**Benefits**

*Locality*: one place knows the aggregate3 wire format. *Leverage*: a new protocol's integration
test declares fixtures, not JSON-RPC plumbing — today it copies ~60 lines from a sibling
`main_integration_test.go`. *Tests get better*: the `AllowFailure` semantics
(`HandleMulticall3` correctly reverts the whole batch when `!success && !AllowFailure`, matching the
real Multicall3 and the AGENTS.md "best effort reads still bubble up" rule) get applied everywhere
instead of only where `HandleMulticall3` is used.

**Risk / migration**

Low and incremental: steps 1–2 are pure deletions, step 3 is mechanical, step 4 is one PR per fake
server. The behaviour is test-only, so a mistake fails loudly in CI rather than in production.

**Depends on / enables**: overlaps F11.1 (the `Multicaller` doubles).

---

### F11.7 — `internal/pkg/testutils` is a 17-line, one-function package whose name differs from `internal/testutil` by one letter

**Strength**: Strong
**Size**: S

**Files**
- `stl-verify/internal/pkg/testutils/testutils.go` (17 lines, one function
  `BigFromStr(t testing.TB, s string) *big.Int`)
- `stl-verify/internal/pkg/testutils/testutils_test.go` (31 lines)
- Sole external importer: `internal/services/morpho_indexer`

**Problem**

The deletion test settles this one. `internal/pkg/testutils` holds exactly one 8-line helper, has
exactly **one** external importer, and its 31-line test file is longer than the code. Deleting the
package moves one function into `internal/testutil` (imported by 60 packages) and nothing else
happens — no complexity reappears anywhere. Meanwhile the near-identical names guarantee confusion:
`internal/pkg/testutils` vs `internal/testutil`, one letter apart, both test helpers, and a reader
grepping for "testutil" hits both.

`internal/pkg/metrictest` (62 lines, `ChainValue` + `RequireChain`, 4 external importers) is a
milder case of the same thing, but it has a coherent single purpose and four consumers, so it earns
its keep as a package — it just belongs under `internal/testutil/` with its siblings rather than in
`internal/pkg/` next to production libraries.

**Proposed change**

Move `BigFromStr` into `internal/testutil` (it fits beside `E18`, which is already there and does
the same class of job) and delete `internal/pkg/testutils` outright. Optionally move
`internal/pkg/metrictest` to `internal/testutil/metrictest` so every test-only package lives under
one prefix and `internal/pkg/` means "production library".

**Benefits**

*Locality*: one place to look for test helpers, and no name that is a typo of another.
*Leverage*: `BigFromStr` becomes visible to the 60 packages that already import `testutil`; today 59
of them cannot find it.

**Risk / migration**

Trivial: 2 import-line edits, 1 package deletion. Land alone.

---

### F11.8 — `internal/testutil` is a 65-symbol flat grab bag, and two of its subpackages are actually the libraries behind shipped binaries

**Strength**: Worth exploring
**Size**: M

**Files**
- `stl-verify/internal/testutil/` — 51 files, 8,645 lines, ~65 exported symbols, 6 concerns (§1)
- `stl-verify/internal/testutil/mockchain/` — 1,776 non-test + 2,601 test lines; sole external
  importer `cmd/util/stress-test/mock-blockchain-server`; shipped via
  `Dockerfile.mock-blockchain-server` and `make kind-deploy-mock-blockchain-server`
- `stl-verify/internal/testutil/dataexport/s3.go` (156 lines); sole external importer
  `cmd/util/stress-test/data-export`
- `stl-verify/internal/testutil/db.go:200-231` — `IsUniqueViolation` and `IsDeadlock` are the same
  20-line body with a different SQLSTATE constant

**Problem**

Two distinct issues under one roof.

*(a) The grab bag.* One package name covers service lifecycle, per-test isolation, registry seeding,
ABI packing, port mocks, and fake HTTP servers. An author looking for "how do I seed a token" and an
author looking for "how do I fake a multicall" import the same package and get 65 symbols with no
grouping. The churn pattern is consistent with this: `db.go` changed **12 times since March**,
`localstack.go` 9, `redis.go` 6 — but reading them shows *why*, and it is not disorder. `RunShared`
(`runshared.go`) is a genuinely deep module: `TestMain` is one statement in **41 of 46** packages —

```go
func TestMain(m *testing.M) { os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN})) }
```

— it owns start order, reverse teardown, the leak check ordering, and validates the
LocalStack/SERVICES pair; and it is tested through an injected `serviceStarters` seam so the
ordering invariants have real tests (`runshared_test.go`, 207 lines). **No package re-does the
RunShared dance**; the answer to probe 4 is that this duplication was already eliminated, and the
churn is the consolidation itself, not thrash. The 5 non-`RunShared` `TestMain`s
(`morpho-vault-backfill`, `morpho-v2-bootstrap` ×2, `morpho_indexer`, `templatedb`) are worth a look
but are a small tail.

*(b) The misfiling.* `mockchain` and `dataexport` are not test infrastructure. Each has exactly one
external importer and it is a `cmd/` binary; `mockchain` is a deployed service with its own
Dockerfile and kind deployment target. Yet they sit under `internal/testutil`, which means: 2,601
lines of their tests inflate the "test infrastructure" figure; a reader auditing test helpers wades
through a full JSON-RPC/WebSocket chain simulator; and the hexagonal layering says nothing about
where a mock *product* lives.

Minor, same file: `IsUniqueViolation` and `IsDeadlock` (`db.go:200-231`) are byte-identical apart
from the constant — one `func isSQLState(err error, code string) bool` and two one-line wrappers.

**Proposed change**

- Split `internal/testutil` by concern, keeping the hub small:
  `testutil` (RunShared, Shared, Start*ForMain, templatedb, images — the lifecycle),
  `testutil/isolate` (SetupTestDB, names), `testutil/seed` (the 11 `Seed*`),
  `testutil/abipack` (Pack*/Extract*/HandleMulticall3), `testutil/fakes` (F11.1),
  `testutil/rpcserver` (the merged fake RPC server from F11.6).
- Move `internal/testutil/mockchain` → `cmd/util/stress-test/mock-blockchain-server/internal/chain`
  (or `internal/mockchain`, outside `testutil`), and `internal/testutil/dataexport` → beside its one
  consumer. Neither is imported by a test.
- Collapse `IsUniqueViolation`/`IsDeadlock` onto a shared `isSQLState`.

**Benefits**

*Locality*: "test infrastructure" stops meaning "also a deployed mock blockchain". The metric
"8,645 lines of testutil" becomes ~4,300, which is the number a maintainer should actually reason
about. *Leverage*: the sub-package names tell an author which helper family to reach for, which is
what 65 flat symbols cannot.

**Risk / migration**

Import churn across 60 packages — large diff, zero behaviour change, and `goimports` does most of
it. Land the `mockchain`/`dataexport` moves first (they touch 2 files each and are pure wins), then
the split one sub-package at a time, keeping thin aliases in `testutil` for one release if the diff
size is a problem.

**Depends on / enables**: F11.1 needs a home for `fakes`; F11.6 needs one for `rpcserver`.

---

### F11.9 — `make cover` reports coverage from unit tests only, so the headline number omits 28% of the test code, and nothing gates it

**Strength**: Worth exploring
**Size**: S

**Files**
- `stl-verify/Makefile` `cover` / `cover-all`
- `stl-verify/AGENTS.md:140` — "Services should have 100% coverage"; `:142` — "For services, create
  both unit and integration tests"; `:191` — "`main.go` entry points should also have 100% coverage"
- `.github/workflows/go-ci.yml` — no coverage step in any job

**Problem**

`cover` runs `go test -coverprofile=coverage.out ./...` with no `-tags=integration`, so the profile
excludes 114 files and 47,615 lines — 28% of all test code. For the packages where integration tests
*are* the tests, the reported number is meaningless: `internal/adapters/outbound/postgres` measures
**7.6%** while carrying 33 integration test files and 20,596 integration test lines; `db/migrator`
has 15 integration files and 5,394 lines and appears only via its 222 unit lines.

Measured unit coverage against the stated 100% target: median service ~89%, and four services below
80% — `aavelike_position_tracker` **54.7%**, `shared` **61.1%**, `backfill_gaps` 79.7%,
`curveindexer` 80.0%. `internal/services/sparklend` has **no test files at all**.
`internal/adapters/outbound/postgres/buildregistry` reports 0.0% with "no tests to run".
No CI job computes coverage, so none of this is visible on a PR.

**Proposed change**

- Split the target honestly: `cover-unit` (today's behaviour) and `cover-all` running
  `-tags=integration -coverpkg=./...` so integration tests credit the code they exercise. Merge the
  two profiles for the report.
- Add a coverage step to the integration job that uploads the merged profile as an artifact and
  prints a per-package table; then, once the real numbers are known, add a **per-package floor**
  for `internal/services/*` that can only rise. A repo-wide single threshold would be gamed; a
  ratchet per service matches the "services should have 100% coverage" intent.
- Give `internal/services/sparklend` tests or explain in its package doc why it has none.

**Benefits**

*Locality*: one number that means what it says. *Leverage*: the four sub-80% services become visible
work items instead of folklore. *Tests get better*: `-coverpkg` reveals which production code the
integration suite actually reaches, which is the question "should this be a unit or integration test"
needs answered.

**Risk / migration**

`-coverpkg=./...` slows the integration run measurably (instrumenting every package); measure before
enabling on every shard, and consider running the coverage build only on `main` pushes.

---

### F11.10 — Container image tags are declared twice and kept in sync by a 100-line grep-the-source shell script

**Strength**: Worth exploring
**Size**: M

**Files**
- `stl-verify/internal/testutil/images.go:8-12` — `ImageTimescaleDB`, `ImageRedis`, `ImageLocalStack`
- `.github/workflows/go-ci.yml:97,111,121` — the same three tags, each with
  `# keep in sync with internal/testutil/images.go`
- `stl-verify/ci/check-ci-services.sh` (100 lines) — greps `images.go` for `Image[A-Za-z0-9_]+ = "…"`
  and `awk`s the workflow's `integration-tests` job block to compare

**Problem**

The same three image tags and the LocalStack `SERVICES` list live in two places — Go source and
workflow YAML — because a local run starts containers from test code and CI runs them from
`services:`. Nothing structural forces agreement, so a 100-line bash script parses both sides:

```bash
declared_images="$(grep -oE 'Image[A-Za-z0-9_]+[[:space:]]*=[[:space:]]*"[^"]+"' "$images_file" ...)"
job_block="$(awk -v job="  $job:" 'index($0, job) == 1 { injob = 1; next } ...' "$workflow")"
```

The script is careful and well-commented (it distinguishes grep exit 1 from exit 2, and explains
why), which is the tell: it is *good code solving a problem that should not exist*. It is coupled to
YAML indentation (`"  $job:"`), to the job's name, and to a Go declaration's spelling — the
`Shared` struct's own doc comment has to warn "Keep it a string literal:
`ci/check-ci-services.sh` greps the declarations to hold the workflow's SERVICES to their union, and
cannot see through a const or a var." An invariant that constrains how you may write Go so a shell
script can read it is an invariant in the wrong place.

The `LocalStackServices` union has the same shape: the script greps every `LocalStackServices: "…"`
and `StartLocalStackForMain("…")` across all `*_test.go` and compares to the workflow's `SERVICES`.

**Proposed change**

Make one side derived. Cheapest version: move the tags and the LocalStack services union into a
single data file (`stl-verify/ci/test-images.env` or a small YAML), have `images.go` read it via
`go:embed`, and have the workflow read it in a step that sets the `services:` images through
`env`-substituted values or a composite action. Alternative: keep `images.go` canonical and generate
the workflow's `services:` block, checked by `git diff --exit-code` — the same mechanism the repo
already uses for `k8s/overlays/*/kustomization.yaml` images blocks.

Either way `check-ci-services.sh` shrinks to a generated-file diff check, and the "keep it a string
literal" constraint on `Shared` disappears.

**Benefits**

*Locality*: one declaration of what services the suite needs. *Leverage*: bumping TimescaleDB is a
one-line edit instead of a two-place edit plus hoping the grep still matches.

**Risk / migration**

GitHub Actions cannot use `${{ }}` expressions freely inside `services:`, which is likely why it was
done this way — verify what the runner accepts before committing to the embed approach; the generate-
and-diff approach sidesteps the limitation entirely and is the safer first PR.

---

### F11.11 — 32 test files break the stated `foo_test.go` ↔ `foo.go` pairing rule outside the two sanctioned exceptions

**Strength**: Worth exploring
**Size**: M

**Files** (a selection of the 32)
- `stl-verify/internal/services/allocation_tracker/` — `entries_chain_test.go`, `guardrails_test.go`,
  `loaders_test.go`, `routing_guardrail_test.go`, `served_chains_guardrail_test.go` (5 in one package)
- `stl-verify/internal/services/oracle_price_worker/` — `service_curve_lp_ng_test.go`,
  `service_erc4626_test.go`, `service_freshness_test.go`, `telemetry_chain_test.go`
- `stl-verify/internal/adapters/outbound/postgres/` — `blockstate_test.go`, `db_config_test.go`,
  `db_pool_test.go`, `repository_validation_test.go`, `repository_benchmark_test.go`
- `stl-verify/internal/adapters/outbound/alchemy/` — `client_null_result_test.go`, `proxy_tls_test.go`,
  `telemetry_chain_test.go`, `subscriber_benchmark_test.go`

**Problem**

`AGENTS.md:135` states the rule and its two exceptions precisely: shared fixtures in
`testhelpers_test.go`, and build-tagged cross-cutting scenarios named
`*_integration_test.go`. Measured: 417 test files, 267 paired, 150 unpaired — of which 112 are
`*_integration_test.go` and 6 are `testhelpers_test.go`, leaving **32 unsanctioned**. AGENTS.md also
names the two valid resolutions ("the source split is missing" or "the tests are filed wrong"), so
each of the 32 is a decision, not a judgement call.

The pattern in the list is informative and suggests the *first* resolution usually applies:
`telemetry_chain_test.go` recurs in 4 different packages (alchemy, telemetry, morpho_indexer,
oracle_price_worker) with no `telemetry_chain.go` anywhere — i.e. a cross-cutting "every metric
carries a chain label" guardrail that has no source file to pair with because it tests a convention,
not a function. Same for `allocation_tracker`'s three `*_guardrail_test.go` files. `_benchmark_test.go`
(3 files) is a third de-facto category the rule does not mention.

**Proposed change**

Resolve as three groups rather than 32 individual decisions:
1. **Guardrail/convention tests** (`*_guardrail_test.go`, `telemetry_chain_test.go` ×4) — recognise
   them as a third sanctioned exception in AGENTS.md, or better, consolidate the four
   `telemetry_chain_test.go` copies into one repo-wide guardrail test that walks the metric registry.
2. **Benchmarks** (`*_benchmark_test.go`) — add to the sanctioned list; they are already build-tagged
   `benchmark` in one case.
3. **The genuine remainder** (`db_config_test.go`, `db_pool_test.go`, `blockstate_test.go`,
   `client_null_result_test.go`, `service_erc4626_test.go`, …) — extract the matching source file,
   which is the resolution AGENTS.md prefers and which would also break up
   `postgres/repository.go`-scale files.
4. Add a `make pairing-check` to `ci-checks` once the categories are settled, so the count cannot
   grow silently. It is ~15 lines of shell and mirrors the existing `shared-container-check`.

**Benefits**

*Locality*: a reader looking for the tests of `foo.go` finds them at `foo_test.go`, always.
*Leverage*: the four duplicated `telemetry_chain_test.go` guardrails become one.

**Risk / migration**

Mostly file moves. The risk is disagreeing with the author's intent on the ~15 in group 3; do those
package-by-package with the owning team, and land the AGENTS.md clarification plus the check first.

---

### F11.12 — God test files: seven test files over 2,500 lines, the largest holding all of a service's unit tests in one file

**Strength**: Worth exploring
**Size**: L

**Files**
- `stl-verify/internal/services/raw_data_backup/service_test.go` — **4,082 lines**, 6 doubles
- `stl-verify/internal/services/live_data/live_data_service_test.go` — **3,942 lines**, 8 doubles
- `stl-verify/internal/adapters/outbound/postgres/morpho_repository_integration_test.go` — 3,183
- `stl-verify/internal/services/backfill_gaps/backfill_gaps_service_test.go` — 2,950
- `stl-verify/internal/adapters/outbound/postgres/blockstate_repository_integration_test.go` — 2,823
- `stl-verify/internal/services/oracle_backfill/service_test.go` — 2,720
- `stl-verify/internal/services/oracle_price_worker/service_test.go` — 2,569

**Problem**

Test code is 168,688 lines against 90,721 lines of production code — a **1.86:1 ratio** — and it is
concentrated: `raw_data_backup` has 4,082 unit-test lines in **one file**, `live_data` 3,942 in
**one file**. Both are also the packages with the most hand-rolled doubles (6 and 8), because a
single file that covers a whole service needs a double for every port that service touches, and the
doubles then sit thousands of lines away from the tests using them (`live_data`'s `mockMetrics` is at
line 1,946, `mockClientWithHashTracking` at 2,493, `mockOrderTrackingCache` at 2,906).

This interacts badly with F11.11: because the file is `service_test.go` and the source is split
across many files, the pairing rule is satisfied only nominally — one `service_test.go` "pairs" with
`service.go` while actually testing a dozen source files.

**Proposed change**

Split each god test file to match the source split it is testing, one package per PR, ordered by
size. `raw_data_backup` and `live_data` first. In each: move the doubles into the package's
`testhelpers_test.go` (the sanctioned exception) or, after F11.1, delete them in favour of
`testutil/fakes`; then move each group of tests into the `_test.go` matching the source file it
exercises. This is the cheapest way to satisfy F11.11 and F11.3 at once, because the giant test
functions (`TestStartAndProcessMessages` 1,854 lines, `TestRun` 1,057) live in exactly these files.

**Benefits**

*Locality*: a change to one source file has one test file to read. *Leverage*: the per-package double
count drops from 6–8 to 0 once `fakes` exists. *Tests get better*: 17 `t.Run` scenarios become 17
named top-level tests whose failure names the behaviour.

**Risk / migration**

Pure motion, no behaviour change, but very large diffs that conflict with any concurrent work in
those packages — coordinate timing. Verify with `go test -run . -count=1` before and after and
compare the test-name list, not just the pass/fail.

**Depends on / enables**: easier after F11.1; enables F11.3 and F11.11.

---

### F11.13 — Small tooling debris

**Strength**: Strong (each item verified) · **Size**: S (one PR for all)

- **Dead make targets.** `make staticcheck` runs the standalone `$(GOBIN)/staticcheck ./...` and
  `make vet` runs `go vet ./...`, but `.golangci.yml`'s own header says golangci-lint "covers what
  used to be four separate CI passes: govet (go vet), staticcheck…". Neither target is in
  `ci-checks` or any workflow, and `make tools` still installs
  `honnef.co/go/tools/cmd/staticcheck@latest`. Two targets and one tool install to delete.
- **`stretchr/testify` is a direct dependency for 3 files.** `go.mod:19`; the only import anywhere is
  `testify/mock`, in `cmd/cronjobs/morpho-v2-bootstrap/main_integration_test.go:10`,
  `cmd/backfillers/morpho-vault-backfill/main_integration_test.go:24`, and
  `internal/adapters/outbound/temporal/temporal_test.go:12` — all three because Temporal's
  `testsuite` requires it. Legitimate, but worth a one-line comment in `go.mod` so nobody
  "standardises on testify" for assertions, given the repo otherwise uses plain `t.Fatalf` throughout
  (0 files import `testify/require` or `testify/assert`).
- **`make e2e` runs `-tags=e2e` and no file carries that tag.** `stl-verify/Makefile` `e2e` runs
  `go test -tags=e2e ./cmd/base/watcher/...`; zero files match `^//go:build .*e2e`. AGENTS.md
  advertises `make e2e` as "End-to-end tests with testcontainers". The target is a no-op today.
- **Inconsistent script permissions.** `ci/check-integration-shards.sh` is `-rw-r--r--` while its two
  siblings are `-rwxr-xr-x`. Harmless (all three are invoked as `bash ci/…`) but it will trip anyone
  who runs it directly.
- **Positive, worth not breaking**: only **4** `//nolint` directives in the whole repo (2 revive,
  1 gosec, 1 errcheck) — essentially zero lint-suppression debt. `go vet ./...` and
  `go vet -tags=integration ./...` are both clean.

## 4. Cross-area observations

- `internal/services/sparklend` has **no test files**, and `internal/services/shared` is at 61.1%
  unit coverage — both are service-area concerns (agents 04/05).
- Five `cmd/*/main.go` `run()` functions are 158–268 lines
  (`aave-like-user-snapshot-indexer` 268, `prime-allocation-indexer` 217, `psm3-indexer` 209,
  `morpho-indexer` 180, `sparklend-indexer` 177) — composition-root sprawl for whoever owns `cmd/`.
- `internal/adapters/outbound/orderbook/kraken.go:141 krakenHandler.handle` is the single longest
  production function at 295 lines.
- 4 `cmd/*/main_integration_test.go` files hand-roll `TestParseConfig` at 255–472 lines
  (`prime-allocation-indexer` 472, `morpho-indexer` 388, `oracle-price-indexer` 277,
  `fluid-vault-indexer` 255) — near-certainly the same table-driven config test copied four times;
  belongs to whoever owns worker config/wiring.
- `internal/pkg/httpclient` is at 1.8% unit coverage and `internal/adapters/outbound/redis` at 15.3%
  — the redis figure is explained by integration tags, the httpclient one probably is not.
- `internal/adapters/outbound/postgres` holds 33 integration test files and 20,596 integration test
  lines in one package — the largest single test surface in the repo, for the database agent.
- `.github/workflows/deploy.yaml` (61 commits since March) and `ci.yml` (26) are the second and third
  most-churned CI files after `stl-verify/Makefile` (71) — deploy pipeline complexity is its own area.

## 5. Open questions

- **Is committing generated mocks acceptable?** F11.1 needs it (tests must run without a generate
  step), but the root `AGENTS.md` rule is "Never commit generated files or binaries" with one
  deliberate exception. This needs a maintainer decision, not an inference.
- **Actual CI wall-clock per job.** The workflows declare `timeout-minutes: 10` (lint), `10`
  (unit-tests) and `20` (integration, per shard), but nothing in the config records real durations,
  and I cannot read Actions history. Whether the 2-way shard split is the bottleneck — and whether
  shard 1 (which carries both `postgres` and `db/migrator`) is near its 20-minute ceiling — is
  unanswerable from the repo alone. That determines the priority of F11.5.
- **Why `funlen` never landed** despite being written into AGENTS.md as "planned": was it tried and
  reverted (too many findings), or simply never started? Changes whether F11.3 needs a grandfathering
  list or `new-from-rev`.
- **Is `mockchain` intended to become a product?** It has a Dockerfile, a kind deploy target and an
  admin API, which is a lot for a test fixture. If it is meant to stay a shipped mock chain, F11.8's
  move is clearly right; if it is meant to fold back into tests, the answer differs.
- **Does `golangci-lint v2.12.2` stay clean under `-tags=integration`?** I could not verify: the
  local binary is v1.64.8 and rejects the v2 config schema. `go vet` is clean both ways, but that
  covers only one of the four analyser families. Someone with the pinned binary should run
  `golangci-lint run --build-tags=integration ./...` before F11.4 is sized.
- **Why do 5 `TestMain`s bypass `RunShared`** (`morpho-vault-backfill`, `morpho-v2-bootstrap` ×2,
  `morpho_indexer`, `templatedb`)? `templatedb` is plausibly self-referential, but the four Morpho
  ones may share a requirement `Shared` cannot express.
