# Roadmap

Status: v1 — sequencing of the candidates in `CANDIDATES.md`. History baselines come from
`findings/12-history-metrics.md` (619 PR units on `main`, 2026-01-01 → 2026-09-03, deploy-bot
commits excluded); the rest are measured in the area reports.

## Principles

- **One slice, one PR, one deploy.** Every slice below is S or M on its own. Epics (C1, C2, C5,
  C6) land one worker, one indexer, one repository or one bounded context per PR, each with its
  own soak, exactly as commit `58f9c196` asked for when it deferred the worker consolidation.
- **Finish adoptions before inventing.** Nine of the deep modules the repo needs already exist
  (`dexbootstrap`, `sqsutil.RunLoop`, `dexconsumer`, `shared.RunSnapshotReads`,
  `temporal.RunCronjob`, `testutil.MockMulticaller`, `RunShared`, `AppendOnChange`,
  `dextelemetry`) and one more sits on an unmerged branch (`StateReader`). Phases 0–1 are mostly
  adoption.
- **Ratchet, don't wall.** Lint rules land with `--new-from-rev` so the existing backlog is a
  metric that goes down, not a CI wall.
- **Behaviour changes are named.** Adopting a shared module changes behaviour where the copies
  had drifted (`CHAIN_ID` becomes required, bucket validation runs, the pin becomes a type). The
  PR description lists every newly enforced invariant.
- **This folder is updated in the PR.** A slice that moves a metric or closes a finding edits
  `PROGRESS.md` in the same PR.

## Phase 0 — Stop the bleeding and set the ratchet (≈ 2 weeks, all S, fully parallel)

| Slice | Candidate | What lands |
|---|---|---|
| 0.1 | C1 | Rebase `toreluntang/vec-na/blockpin-statereader-seam` onto `main` (600 commits behind, no textual conflicts in a trial merge), re-run its tests, open the PR. Fixes the live Fluid reorg bug (F03.2). Decision D1 does **not** block this. |
| 0.2 | C10 | Tickets for every row of the C10 table. Land the S ones now: strict `blockevent.Decode` (F05.1), CoinGecko malformed rows (F06.10), `isNonRetryable` and `errors.Join` (F06.2), case-sensitive hash compare (F05.13), `VatCaller` in-band failures (F08.13), ignored `Pack` errors, the S3 endpoint variable (F02.15), the alert allowlist (F13.6), star normalisation (F02.5). |
| 0.3 | C10 | `oracle_backfill` exits non-zero when any block failed (F04.3). `CHAIN_ID` required in the six workers that default to mainnet (F10.3). |
| 0.4 | C7 | Delete the inbound hexagon, `adapters/outbound/memory`, `generate-er`, `services/sparklend`, `pkg/testutils`, the two empty `cmd/` dirs, the root scratch file; move `mockchain` under `cmd/util`. Rewrite the "Adding New Features" recipes in `stl-verify/AGENTS.md` from the code. |
| 0.5 | C2 | Move `cmd/workers/internal/dexbootstrap` → `internal/workerkit`, no behaviour change (F10.7). |
| 0.6 | C12 | Enable `funlen`, `gocognit`, `depguard` (services must not import adapters; ports must not import `pgx`) with `--new-from-rev`; lint the integration-tagged files; generate the shard manifests (F11.3, F11.4, F11.5). |
| 0.7 | — | Record the metric baselines below in `PROGRESS.md`; add the two scripts that compute the code-shape metrics under `overhaul/metrics/`. |

## Phase 1 — The two spines (≈ 6–8 weeks, two parallel tracks that meet at one signature)

The handler signature both tracks converge on, fixed before either starts:
`Handle(ctx context.Context, r outbound.StateReader, ev blockevent.Event) error`.

**Track A — C1, block identity and the pinned reader.** One indexer per PR, innermost first,
deleting tuple parameters as they become unused.

| Slice | What lands |
|---|---|
| A.1 | D1 resolved: `entity.BlockRef` (identity) with `Pin()` deriving the July `BlockPin`; `BlockRef.CacheKey`. |
| A.2 | `shared.RunSnapshotReads` becomes `StateReader.ReadNamed`; Curve and Uniswap switch (they already have the shape). |
| A.3 | psm3, then `prime_debt`/`VatCaller`. |
| A.4 | `oracle_price_worker` and `oracle_backfill` together (sets up C8). |
| A.5 | `pkg/aavelike` + `aavelike_position_tracker` + `allocation_tracker` (the 10-hop, 11-parameter chain). |
| A.6 | `morpho_indexer` (30 functions, 18 skeletons) and `morpho_v2_bootstrap`. |
| A.7 | `BlockCacheReader.GetBlock(ctx, ref)` — one change covers 23 of the 125 sites; entity constructors take `BlockRef`. |
| A.8 | Remove `Multicaller` from service constructors; delete `blockchain.ExecutePinned` and the archiving `context.Value` helpers; the three hand-rolled "was it pinned?" test assertions become dead. |

**Track B — C2 and C9, one worker runtime.** One worker per PR; telemetry rides along.

| Slice | What lands |
|---|---|
| B.1 | `workerkit` gains the shape both existing kits share: `ParseConfig → Deps → Worker.Build → Run`; `dexconsumer.BlockProcessor` + `sqsutil.RunLoop` are its only runner; `dextelemetry` generalised to the kit's instrument set (C9). |
| B.2 | morpho-indexer and fluid-vault-indexer adopt (172 identical lines already); Fluid and psm3 pick up the `DueSet` reorg re-snapshot rule (F03.3). |
| B.3 | sparklend-indexer, prime-allocation-indexer. |
| B.4 | oracle-price-indexer, psm3-indexer, prime-debt-indexer. |
| B.5 | raw-data-backup onto `RunLoop` (F05.2); cex-orderbook and the watcher onto the same lifecycle/signal/config prologue (F10.10). |
| B.6 | Retire `shared.SQSConsumerConfig`, the 7 bespoke `SQSConsumer` doubles, the 4 `TestParseConfig` copies; `Build` gets unit tests against fakes, `main` tests shrink to smoke (F10.6). |
| B.7 | Cronjob prologue into `temporal.RunCronjob` (build metadata, DSN default removed, F06.9); backfillers onto `workerkit.ParseConfig`. |

**Alongside, small and independent:** C11 HTTP plumbing (`httpclient` gains methods, one config
idiom, one error taxonomy, alchemy subscriber onto `wsclient`); C15 `PositionSource` owns
interpretation.

## Phase 2 — Dialects and persistence (≈ 8–10 weeks)

| Slice | Candidate | What lands |
|---|---|---|
| 2.1 | C3 | `abis` registry parsed once at init, getters return `*abi.ABI` (F08.4). |
| 2.2 | C3 | One `erc20meta.Reader`, one `TokenMetadata` (F08.3, F02.3, F03.13). |
| 2.3 | C3 | `SnapshotRead` combinator library; Curve's two handlers become one handler with two ABI tables (F03.1); `liquidity_decode` becomes a table (F03.10). |
| 2.4 | C3 | The remaining skeleton copies (morpho 18, pricing 8, allocation ~10) onto `ReadNamed`; the three in-service chain adapters move behind ports (D6). |
| 2.5 | C4 | Morpho event registry (F01.1, F01.5, F01.6); `OraclePricer` registry (F04.1); one chain registry shared with Python (F13.1). |
| 2.6 | C5 | `pgcore`: `WithTx`, `ScanAll[T]`, `BatchExec`, one close convention (F07.3); `BatchUpsert[T]` replaces the four `VALUES` loops. |
| 2.7 | C5 | `AppendOnChange[T]` batched; uniswap and morpho drop their hand-built forms (F07.2). Registry resolvers with the drift guard always on (F07.4). |
| 2.8 | C5 | D4: database default flipped to REVOKE, single allowlist asserted against `schema_master.json`; `block_states` lifecycle flags as versioned rows (F07.1). Migration timestamp collisions fixed (F07.10). |
| 2.9 | C8 | Oracle worker and backfiller share one `Pricer` core over two drivers (F04.2); reference-capital pair (F02.5); Morpho backfiller pools and bisect (F01.10); `cacheAndPublishBlockData` once (F05.3). |

## Phase 3 — Reshape the boundaries (≈ 10–12 weeks)

| Slice | Candidate | What lands |
|---|---|---|
| 3.1 | C6 | Opaque `outbound.Tx`; `TxManager` is the only thing that knows pgx (F09.1). |
| 3.2 | C6 | Block-domain types (`BlockEvent`, `BlockState`, `ReorgEvent`) move to `entity`; the port inside `entity` moves out (F09.5, F09.13). |
| 3.3 | C6 | Ports grouped by bounded context, one PR each: registry, lending, dex, prime, price, feeds, chainio, infra (F09.2, F09.8, F09.12); each merge deletes its dead methods and replaces hand-rolled doubles with `moq` output (C12, F11.1). |
| 3.4 | C6 | `BlockStateRepository` → block, backfill and reorg ledgers (F05.5); `internal/common` merges into `internal/pkg` (F09.11). |
| 3.5 | C14 | `backfill_gaps` split by concern (F05.6); `live_data` renamed and rehomed as the watcher's reorg module (F04.14); `services/shared` and `pkg/blockchain` split (F06.1, F08.11); `aavelike_position_tracker/service.go` (F02.6). |
| 3.6 | C13 | Kustomize components per chain replace the 34 copied `k8s/base` dirs (F13.5); Makefile docker families onto the existing helper (F11.2); then D3, one image with a worker registry (F10.8). |

## Decisions needed from the maintainer

| # | Decision | Recommendation |
|---|---|---|
| D1 | Lean `BlockPin` (July: number, version, hash, mode) vs full identity (September evidence: chain id and timestamp travel with it in 125 functions, the cache key needs chain id, 7 `time.Unix` sites) | Keep `BlockPin` as the read pin exactly as built. Add `entity.BlockRef` as identity with `Pin()`. Handlers receive `BlockRef`; readers are bound to it. Land 0.1 first. |
| D2 | Does `workerkit` own only bootstrap, or also the loop and the handler runner? | Own all three. A worker contributes `Build` and a `Handle`; nothing else is protocol-specific. |
| D3 | One image with `stl-worker <name>` vs one image per binary | Defer to Phase 3; it falls out of C2 and the roster/Makefile cost is measured (F10.8). |
| D4 | Flip the database default to append-only with an allowlist | Yes, in 2.8, coordinated with the Python service that shares the schema. |
| D5 | Two silent-by-default data paths: the alchemy subscriber dropping a header when the channel is full; a payload absent from both Redis and S3 stalling a FIFO head | Decide explicitly under the poison-pill rule; both become tickets in 0.2. |
| D6 | Where the three in-service chain adapters (morpho, aavelike, fluid `blockchain_service.go`) live | `adapters/outbound/<protocol>chain`, behind a port, like the DEX indexers' `Factory`. |

## Metrics

Code-shape metrics are recomputed per phase by scripts under `overhaul/metrics/` (slice 0.7);
history metrics come from `findings/12`. Note what the history says about where the pain is: the
overall median has been flat-to-falling all year while volume grew five-fold; the growth is in
the tail, and the tail is made of PRs that touch the shared layers. The roadmap therefore attacks
the amplifiers (ports, composition roots, block identity) rather than the median.

| Metric | Baseline | Source | Target |
|---|---|---|---|
| Files per PR: median / p90 / max | 5 / 29 / 96 | F12 | 4 / 15 / — |
| PRs of ≥40 files per month | 15 in August alone | F12 | ≤ 2 |
| Median files of a PR that touches `ports/`, `domain/`, `pkg/` or `testutil/` | 21–22 (vs 5 overall) | F12 | ≤ 10 |
| `ports/`-touching PRs that also touch ≥3 service packages | 33.8% | F12 | < 10% |
| Service-package PRs that also touch that package's `main.go` | 20.3% | F12 | < 5% |
| `main.go` edits that are ripple from another package | 47% | F10.1 | ~0 |
| Files touched to add a worker (boilerplate floor) | 19–31 | F12 | `cmd/` 1 file + service + k8s component |
| Multicall state reads pinned by number outside a `StaticProber` | 23 of 51 | F08.1 | 0 |
| Non-test functions with ≥3 block-identity params | 125 | F09.3 | 0 |
| Copies of the multicall pack/execute/check/unpack skeleton | ~35 | F08.2 | 0 |
| `TokenMetadata` types / ERC20 readers | 3 / 4 | F08.3 | 1 / 1 |
| SQS worker root lines (7 roots, substantive / distinct) | 1,480 / 489 | F10.1 | ~70 / ~70 |
| Complete SQS consume-loop designs | 2 | F10 | 1 |
| Per-package `Telemetry` structs | 11 | F06.5 | 1 |
| Binaries that never initialise telemetry | 11 of 32 | F10.5 | 0 |
| Hand-rolled test doubles | 148 | F11.1 | < 10 |
| Port interfaces / real seams | 61 / 2 | F09 | ~16 / all |
| Port files importing `pgx` | 18 of 45 | F09.1 | 0 |
| `postgres.New*` call sites in `cmd/` | 86 | F09.2 | ~15 |
| Hand-written `rows.Next` scan loops / registry get-or-create idioms | 48 / 9 | F07.3, F07.4 | 0 / 1 |
| Tables writable in place | 128 (default) | F07.1 | the allowlist |
| Files touched to add a Morpho event / an oracle type | 6 / 21–25 | F01.1, F04.1 | 1 / ~3 |
| Makefile docker-target lines | 951 | F11.2 | ~100 |
| `k8s/base` dirs that are per-chain clones | 34 of 56 | F13.5 | 0 |
| Production functions over 60 lines | 127 | F11.3 | ratchet to 0 |
