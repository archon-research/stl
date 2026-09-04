# Progress log

Working branch: `toreluntang/vec-na/architecture-overhaul-findings` (pushed after every logical
commit; no PR). Started 2026-09-03 from `main` at `c4e0a8f2`. Investigation complete; synthesis written
2026-09-04.

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
| 02 | findings/02-allocation-prime-aave.md | services/allocation_tracker, aavelike_position_tracker, pkg/aavelike, prime_debt, sparklend*, reference_capital_*, their cmd/ and repos | FINAL (16 findings) |
| 03 | findings/03-dex-amm.md | services/curveindexer, uniswapv3indexer, pkg/uniswapv3, fluid_vault_indexer, psm3, dexconsumer, cex_orderbook_indexer, adapters/orderbook, pkg/wsclient, dextelemetry, cmd dex-*/cex-* | FINAL (17 findings) |
| 04 | findings/04-pricing.md | services/oracle_price_worker, oracle_backfill, oracle_pricing, offchain_price_fetcher, live_data, adapters coingecko/sky/skydata, inbound http, cmd oracle-*/offchain-* | FINAL (16 findings) |
| 05 | findings/05-block-pipeline.md | cmd/base/watcher, services/backfill_gaps, raw_data_backup, data_validator, transform_worker, adapters s3/sns/sqs/redis/cache/memory/blockverifier, common/sqsutil, pkg/lifecycle, postgres blockstate repo | FINAL |
| 06 | findings/06-offchain-temporal-shared.md | services/anchorage_tracker, maple_graphql_indexer, adapters/maple, entity/maple, adapters/etherscan, adapters/temporal, services/shared, pkg httpclient/retry/proxytls/telemetry/env | FINAL (13 findings) |
| 07 | findings/07-postgres-schema.md | adapters/outbound/postgres, db/migrations, db/migrator, testutil/db.go, cmd/util/migrate + generate-er | FINAL (12 findings) |
| 08 | findings/08-chain-access.md | adapters alchemy/blockchain/blockverifier, pkg/blockchain/* (multicall, archiving, abis, rpcerr), pkg rpchttp/retry/chainutil, testutil/mockchain; block-pinned reader hypothesis | FINAL |
| 09 | findings/09-ports-domain.md | ports/inbound, ports/outbound (59 interfaces), domain/entity, common; per-port adapter/consumer table | FINAL |
| 10 | findings/10-composition-roots.md | all 34 cmd/*/main.go, dexbootstrap, pkg/env, lifecycle, telemetry init; main.go churn classification | FINAL (11 findings) |
| 11 | findings/11-testing-tooling.md | testutil, 148 hand-rolled test doubles, .golangci.yml, Makefiles, lefthook, CI workflows, go.mod hygiene | FINAL |
| 12 | findings/12-history-metrics.md | git history 2026-01 → now: PR size, ripple metrics, co-change clusters, cost of a new indexer | FINAL |
| 13 | findings/13-python-ts-k8s-alerts.md | python/, ts/, k8s/, alerts/, docs/runbooks, cross-language contracts, repo hygiene | FINAL |

## Synthesis status

- `CANDIDATES.md` v2 written: 15 candidates from findings 01–11 and 13 (2026-09-04).
- `ROADMAP.md` v1 written: four phases, six maintainer decisions, a metrics table with history
  baselines from `findings/12`.
- `README.md` indexes the folder.

## Discoveries that change the plan

- **Phase 1 of candidate C1 already exists** on `origin/toreluntang/vec-na/blockpin-statereader-seam`
  (12 commits, 16 files, +1,023/−263, tip 2026-07-10, no PR ever opened). `main` is 600 commits
  ahead; a trial merge shows no textual conflicts. Its plan is the untracked
  `docs/superpowers/plans/2026-07-09-blockpin-statereader-seam.md`. Roadmap slice 0.1 is to rebase
  and land it; decision D1 (lean pin vs full identity) does not block that.
- PR #551 (consolidating `Multicaller` doubles onto `testutil.MockMulticaller`) merged 2026-07-10,
  which is why the chain-access report found the shared double widely adopted.

## Next

1. **Paused** here on 2026-09-04 at the maintainer's request, with all 13 reports final and the
   synthesis committed.
2. On resume: the maintainer reads `CANDIDATES.md` and `ROADMAP.md`, takes decisions D1–D6, and
   picks Phase 0 slices. Execution is one slice per PR from a branch named per repo convention;
   each PR updates this file.

## Early signals worth carrying forward

- A shared block-event runner already exists (`services/dexconsumer.BlockProcessor`, two
  adapters) but Morpho and seven other block handlers hand-roll their own (F01.14).
- Block identity travels as five loose positional params; one invariant change (VEC-471, block
  hash pinning) rewrote 59 signatures in 74 files (F01.3). `fluid_vault_indexer` still has zero
  block-hash references and pins state reads by number (F01 cross-area).
- Five of the thirteen most-churned files in the repo are `main.go` composition roots.
- `services/shared.RunSnapshotReads` exists and replaces the multicall skeleton, but only Curve
  and Uniswap use it; Morpho alone has 18 hand-written copies (F01.2).
