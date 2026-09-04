# System map

Snapshot of the repo as of 2026-09-03 (main at `c4e0a8f2`). Numbers come from `wc`, `find`,
`grep` and `git log`; they are inputs to the findings in `findings/`, not findings themselves.

## Shape

| Part | What | Size |
|---|---|---|
| `stl-verify/` Go module | block watcher, 32 binaries, hexagonal layout | 874 files, 259k lines, Go 1.26, 117 direct deps |
| `stl-verify/python/` | APIs, risk models, some cronjobs | 316 files, 52k lines (excl. venv) |
| `stl-verify/ts/` | frontend only | 116 files |
| `stl-verify/db/migrations/` | SQL, auto-applied | 128 files |
| `k8s/` | Kustomize base + overlays | — |
| `alerts/`, `docs/runbooks/` | Prometheus rules + runbooks | — |

## Go layers by weight

| Layer | Files | Lines | Notes |
|---|---|---|---|
| `internal/services` | 217 | 108.6k | 27 packages; use-case code. **42% of all Go** |
| `internal/adapters` | 178 | 60.9k | postgres alone is 82 files / 31.5k |
| `internal/pkg` | 136 | 23.1k | 31 packages; chain access, telemetry, utilities |
| `internal/domain` | 99 | 12.1k | `entity/` (76 files) + `entity/maple/` (23) |
| `internal/testutil` | 51 | 8.6k | shared test infra + `mockchain` (4.4k) |
| `internal/ports` | 47 | 2.4k | 59 interfaces, 46 files, 1 test |
| `internal/common` | 11 | 2.2k | `sqsutil` only |
| `cmd/` | — | — | 34 `main.go` binaries |

### Services by size

| Package | Files | Lines | Package | Files | Lines |
|---|---|---|---|---|---|
| morpho_indexer | 28 | 19.8k | oracle_backfill | 5 | 4.7k |
| allocation_tracker | 42 | 11.1k | aavelike_position_tracker | 4 | 4.1k |
| curveindexer | 13 | 8.0k | maple_graphql_indexer | 6 | 4.0k |
| oracle_price_worker | 12 | 7.3k | fluid_vault_indexer | 10 | 2.9k |
| backfill_gaps | 6 | 7.2k | offchain_price_fetcher | 4 | 2.8k |
| raw_data_backup | 4 | 6.8k | morpho_v2_bootstrap | 7 | 2.8k |
| uniswapv3indexer | 15 | 6.2k | data_validator | 6 | 2.2k |
| live_data | 2 | 5.1k | (13 more, each < 2k) | | |

### Adapters by size

postgres 31.5k · alchemy 6.4k · orderbook 4.5k · maple 2.8k · temporal 2.7k · redis 1.7k ·
s3 1.6k · memory 1.6k · blockchain 1.5k · sqs 1.2k · sns 1.1k · etherscan 0.9k · sky 0.8k ·
skydata 0.8k · coingecko 0.7k · cache 0.5k · http (inbound) 0.4k · blockverifier 0.1k

## Binaries (32 Go programs; 34 `cmd/` leaf directories, two of which hold only an untracked `.env` — F03.14, F10.11)

| Group | Binaries |
|---|---|
| `cmd/base` (2) | watcher, cex-feed-watcher |
| `cmd/workers` (12) | cex-orderbook-indexer, dex-indexer, fluid-vault-indexer, morpho-indexer, oracle-price-indexer, orderbook-indexer, prime-allocation-indexer, prime-debt-indexer, psm3-indexer, raw-data-backup, sparklend-indexer (+ `internal/dexbootstrap`) |
| `cmd/cronjobs` (7, Temporal) | anchorage-indexer, maple-graphql-indexer, morpho-v2-bootstrap, offchain-price-indexer, reference-capital-indexer, transform-worker, watcher-data-validator |
| `cmd/backfillers` (8) | aave-like-user-snapshot-indexer, morpho-vault-backfill, offchain-price-backfill, oracle-pricing-backfill, raw-block-bulk-downloader, reference-capital-backfill, sparklend-backfill, transform-bootstrap |
| `cmd/util` (5) | gen-transformed, generate-er, migrate, null-payload-refill, stress-test |

Data flow (intent, per `stl-verify/AGENTS.md`):
`Alchemy WS → watcher → Postgres (Timescale) + Redis cache + SNS FIFO → SQS → workers (read payload from Redis by cache key)`.

## Largest non-test files

| Lines | File |
|---|---|
| 1408 | services/morpho_indexer/blockchain_service.go |
| 1371 | services/backfill_gaps/backfill_gaps_service.go |
| 1242 | adapters/outbound/maple/client.go |
| 1218 | adapters/outbound/postgres/blockstate_repository.go |
| 1163 | services/live_data/live_data_service.go |
| 1161 | services/aavelike_position_tracker/service.go |
| 1158 | services/curveindexer/stableswap_handler.go |
| 1137 | services/morpho_indexer/event_extractor.go |
| 1124 | pkg/aavelike/blockchain_service.go |
| 1024 | services/curveindexer/cryptoswap_handler.go |
| 933 | services/maple_graphql_indexer/service.go |
| 926 | services/morpho_indexer/service.go |
| 893 | cmd/backfillers/raw-block-bulk-downloader/main.go |
| 864 | adapters/outbound/postgres/curve_repository.go |
| 852 | services/raw_data_backup/service.go |
| 807 | cmd/util/generate-er/main.go |
| 806 | adapters/outbound/alchemy/client.go |

## Change dynamics (2026-03-01 → 2026-09-03, non-merge commits touching stl-verify)

| Metric | Value |
|---|---|
| Commits | 426 |
| Files per commit: median / p90 / max | 7 / 31 / 96 |

Most-churned non-test Go files (commits since March):

| Commits | File |
|---|---|
| 18 | services/morpho_indexer/service.go |
| 18 | cmd/workers/prime-allocation-indexer/main.go |
| 15 | cmd/workers/oracle-price-indexer/main.go |
| 14 | services/oracle_price_worker/service.go |
| 14 | services/allocation_tracker/service.go |
| 13 | cmd/workers/morpho-indexer/main.go |
| 12 | testutil/db.go |
| 12 | services/allocation_tracker/handler_prime_positions.go |
| 12 | cmd/workers/sparklend-indexer/main.go |
| 12 | cmd/workers/prime-debt-indexer/main.go |
| 11 | services/aavelike_position_tracker/service.go |
| 11 | adapters/outbound/postgres/allocation_repository.go |
| 10 | cmd/base/watcher/main.go |

Read: composition roots (`main.go`) and the big service files absorb most change. Five of the
thirteen hottest files are `main.go`. See `findings/10-composition-roots.md` and
`findings/12-history-metrics.md` for the breakdown.

## Other raw counts

- Hand-rolled test doubles (`type (mock|Mock|fake|Fake|stub|Stub)X struct`): **148**
- Port interfaces: **59** in 46 files (`ports/outbound`), 1 inbound file
- ADRs: 0001 kind, 0002 auditability/processing versioning, 0003 lefthook, 0004 sentinel-verify compat, 0006 append-only (0005 absent)
- Lint: golangci-lint v2 in CI; `funlen`/`gocognit` "planned" per AGENTS.md; no architecture-boundary linter found in the toolchain
