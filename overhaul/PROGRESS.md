# Progress log

Working branch: `toreluntang/vec-na/architecture-overhaul-findings` (pushed after every logical
commit; no PR). Started 2026-09-03 from `main` at `c4e0a8f2`.

## How to resume in a fresh session

1. `git checkout toreluntang/vec-na/architecture-overhaul-findings && git pull`.
2. Read `BRIEF.md` (method), `SYSTEM-MAP.md` (shape of the repo), then this file.
3. Check the table below against `ls findings/`. A report whose first line says
   `Status: DRAFT` was checkpointed mid-investigation; re-dispatch that area with the scope in the
   table and the brief, telling the agent to read the draft first and finish it.
4. When all 13 reports are `FINAL`, do the synthesis steps under "Next".

## Investigation status

| # | Report | Scope (packages) | Status |
|---|---|---|---|
| 01 | findings/01-morpho.md | services/morpho_indexer, morpho_v2_bootstrap, cmd morpho-*, postgres morpho repos | FINAL (15 findings) |
| 02 | findings/02-allocation-prime-aave.md | services/allocation_tracker, aavelike_position_tracker, pkg/aavelike, prime_debt, sparklend*, reference_capital_*, their cmd/ and repos | running |
| 03 | findings/03-dex-amm.md | services/curveindexer, uniswapv3indexer, pkg/uniswapv3, fluid_vault_indexer, psm3, dexconsumer, cex_orderbook_indexer, adapters/orderbook, pkg/wsclient, dextelemetry, cmd dex-*/cex-* | running |
| 04 | findings/04-pricing.md | services/oracle_price_worker, oracle_backfill, oracle_pricing, offchain_price_fetcher, live_data, adapters coingecko/sky/skydata, inbound http, cmd oracle-*/offchain-* | FINAL (16 findings) |
| 05 | findings/05-block-pipeline.md | cmd/base/watcher, services/backfill_gaps, raw_data_backup, data_validator, transform_worker, adapters s3/sns/sqs/redis/cache/memory/blockverifier, common/sqsutil, pkg/lifecycle, postgres blockstate repo | running |
| 06 | findings/06-offchain-temporal-shared.md | services/anchorage_tracker, maple_graphql_indexer, adapters/maple, entity/maple, adapters/etherscan, adapters/temporal, services/shared, pkg httpclient/retry/proxytls/telemetry/env | FINAL (13 findings) |
| 07 | findings/07-postgres-schema.md | adapters/outbound/postgres, db/migrations, db/migrator, testutil/db.go, cmd/util/migrate + generate-er | FINAL (12 findings) |
| 08 | findings/08-chain-access.md | adapters alchemy/blockchain/blockverifier, pkg/blockchain/* (multicall, archiving, abis, rpcerr), pkg rpchttp/retry/chainutil, testutil/mockchain; block-pinned reader hypothesis | running |
| 09 | findings/09-ports-domain.md | ports/inbound, ports/outbound (59 interfaces), domain/entity, common; per-port adapter/consumer table | running |
| 10 | findings/10-composition-roots.md | all 34 cmd/*/main.go, dexbootstrap, pkg/env, lifecycle, telemetry init; main.go churn classification | FINAL (11 findings) |
| 11 | findings/11-testing-tooling.md | testutil, 148 hand-rolled test doubles, .golangci.yml, Makefiles, lefthook, CI workflows, go.mod hygiene | running |
| 12 | findings/12-history-metrics.md | git history 2026-01 → now: PR size, ripple metrics, co-change clusters, cost of a new indexer | running (resumed once after a stall) |
| 13 | findings/13-python-ts-k8s-alerts.md | python/, ts/, k8s/, alerts/, docs/runbooks, cross-language contracts, repo hygiene | running |

## Next (synthesis, after all reports are FINAL)

1. Read the "Findings" and "Cross-area observations" sections of every report.
2. Write `CANDIDATES.md`: the deduplicated, repo-wide list of refactoring candidates. Each
   candidate names the finding ids that feed it, its strength, size, dependencies, and the
   deletion-test verdict. Expect the same theme to surface from several areas (block identity /
   pinned reads, the block-event runner, multicall decode skeleton, composition-root skeleton,
   registry-vs-hand-maintained-lists, test-double sprawl, port granularity).
3. Write `ROADMAP.md`: candidates sequenced into phases of PR-sized slices, ordered by
   dependency and value, with the metric each phase should move (median PR size, files touched
   per new indexer, hand-rolled doubles, main.go churn).
4. Commit and push. Then execution starts, one slice per PR, with this file as the log.

## Early signals worth carrying forward

- A shared block-event runner already exists (`services/dexconsumer.BlockProcessor`, two
  adapters) but Morpho and seven other block handlers hand-roll their own (F01.14).
- Block identity travels as five loose positional params; one invariant change (VEC-471, block
  hash pinning) rewrote 59 signatures in 74 files (F01.3). `fluid_vault_indexer` still has zero
  block-hash references and pins state reads by number (F01 cross-area).
- Five of the thirteen most-churned files in the repo are `main.go` composition roots.
- `services/shared.RunSnapshotReads` exists and replaces the multicall skeleton, but only Curve
  and Uniswap use it; Morpho alone has 18 hand-written copies (F01.2).
