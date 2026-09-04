Status: FINAL

# 12 — History metrics: why PRs are big and getting bigger

Area: git-history mining across the whole repo (2026-01-01 to 2026-09-03), read-only,
cross-referenced against the current `stl-verify/` tree where a metric warranted looking
at actual source (allowed: reading files; no state-changing git commands were run).

## 1. Area map / methodology (the "area" here is the git log itself)

Pipeline: `git log --first-parent --numstat` on `main`, dumped **once** to a scratch
file, then every metric below computed from that one file with Python (no per-commit
`git` calls except ~30 bounded `git diff --numstat` calls for real-merge PRs, see below).

- **Unit of analysis**: a "PR unit" is one first-parent step on `main`. A plain
  (non-merge) first-parent commit uses its own `--numstat`. A **merge** commit (28
  found, all dated 2026-07-31..2026-08-17 — a short-lived non-squash workflow window)
  uses `git diff --numstat merge^1 merge`, so a multi-commit branch is counted once at
  its true total size, not as N fake small PRs (naively using `git log --no-merges`
  would have both double-counted 93 sub-commits *and* dropped the 28 merges' true
  combined diffs entirely).
- **Excluded**: subjects starting with `deploy(` (477 commits, 100% `archon-deploy-bot[bot]`)
  and commits touching only `k8s/overlays/*/kustomization.yaml` (4 more, manual
  image-tag/CPU-limit tweaks). Result: **619 PR units**.
- **File identity**: for co-change/churn (sections 3–4) file paths are canonicalized
  through a union-find over every detected git rename, so the 2026-03-31 `cmd/<name>/`
  → `cmd/{workers,backfillers,base,cronjobs,util}/<name>/` reorg and chains like
  `borrow_processor` → `sparklend_position_tracker` → `aavelike_position_tracker` /
  `pkg/aavelike` collapse to one identity instead of creating phantom duplicate files.
  Area-tagging (sections 1–2, 7) intentionally does **not** canonicalize: a commit is
  tagged by the path it touched *at that time*, which is the historically correct read.
- Scripts and full intermediate JSON/CSV are in the scratchpad
  (`/private/tmp/claude-501/-Users-tore-workspace-stl/042f3c1b-fa0d-45cd-8200-0d91f621b849/scratchpad`),
  not the repo. Key scripts: `parse_commits.py`, `build_canon_map.py`,
  `build_pr_units.py`, `build_pkg_map.py`, `item1`..`item7_8.py`.
- Note on the brief's own headline number ("median 7 files, p90 31, max 96 since March
  2026"): restricting my dataset to `>=2026-03-01` gives median **4**, p90 **26**, max
  **96** — same ballpark, not identical, because that figure likely didn't exclude the
  manual image-tag commits or collapse the 28 real-merge PRs the way this pipeline does.
  Max agrees exactly, which is a useful cross-check that both are looking at the same PR.

## 2. Metrics

### 2.1 PR size distribution (files changed / total lines ins+del)

| Scope | n | files med | files p75 | files p90 | files max | lines med | lines p90 | lines max |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| **All (Jan1–Sep3)** | 619 | 5 | 16 | 29 | 96 | 231 | 2960 | 20421 |
| Jan–Apr | 216 | 6.5 | 18 | 28.5 | 96 | 279 | 3325 | 18835 |
| May–Sep | 403 | 5 | 14.5 | 29 | 92 | 221 | 2949 | 20421 |

By area touched (a commit can count under several areas; overall window):

| Area | n | files med | files p75 | files p90 | files max | lines med | lines p90 |
|---|--:|--:|--:|--:|--:|--:|--:|
| `cmd/` | 135 | 19 | 29 | 58 | 96 | 1230 | 5097 |
| `internal/services/` | 154 | 17 | 25.8 | 45.7 | 96 | 1039 | 5133 |
| `internal/adapters/` | 147 | 17 | 24.5 | 53.2 | 96 | 1230 | 4752 |
| `internal/ports/` | 74 | 21 | 31.8 | 66.5 | 96 | 2216 | 6894 |
| `internal/domain/` | 50 | 21 | 29.8 | 68 | 96 | 2002 | 10743 |
| `internal/pkg/` | 78 | 21 | 29 | 58 | 92 | 1864 | 5960 |
| `db/migrations/` | 118 | 11 | 23 | 47.2 | 96 | 712 | 4841 |
| `internal/testutil/` | 48 | 22 | 39.8 | 69.8 | 92 | 1433 | 7837 |
| `docs/` | 103 | 10 | 22.5 | 49.4 | 96 | 1005 | 5114 |
| `k8s/` | 98 | 7 | 19.8 | 35.9 | 87 | 192 | 2566 |
| `alerts/` | 52 | 9 | 23 | 49.5 | 92 | 691 | 4015 |
| `ts/` | 82 | 18.5 | 29.8 | 43.8 | 91 | 1167 | 5703 |
| `python/` | 115 | 15 | 28 | 44.6 | 96 | 781 | 4039 |

**Headline**: PRs touching `ports/`, `domain/`, `pkg/`, or `testutil/` run **~3–4x**
bigger by median (21–22 files) than the overall median (5) or a `k8s/`-only PR (7).
Those four areas are the ripple amplifiers named in section 4 below.

**Period trend is not what "getting bigger" suggests at the median** — monthly detail:

| Month | n | files med | files p90 | files max | PRs ≥20 files | PRs ≥40 files |
|---|--:|--:|--:|--:|--:|--:|
| 2026-01 | 28 | 11.5 | 27.3 | 63 | 7 (25%) | 2 (7%) |
| 2026-02 | 60 | 7.0 | 30.4 | 84 | 17 (28%) | 5 (8%) |
| 2026-03 | 60 | 7.5 | 26.2 | 77 | 13 (22%) | 2 (3%) |
| 2026-04 | 68 | 3.5 | 27.9 | 96 | 12 (18%) | 5 (7%) |
| 2026-05 | 58 | 4.5 | 31.0 | 36 | 13 (22%) | 0 (0%) |
| 2026-06 | 82 | 5.0 | 28.6 | 68 | 18 (22%) | 3 (4%) |
| 2026-07 | 115 | 3.0 | 19.0 | 75 | 11 (10%) | 3 (3%) |
| 2026-08 | 138 | 6.0 | 41.3 | 91 | 31 (23%) | **15 (11%)** |
| 2026-09 (partial) | 10 | 17.5 | 61.4 | 92 | 4 (40%) | 2 (20%) |

Monthly **median** files is flat-to-declining (11.5 → ~5) while monthly **volume**
grew ~5x (28 → 138). The tail did not shrink to match: p90/max hold steady all year,
and August alone had 15 PRs ≥40 files — more than double any other month, and by count
alone more large PRs than most whole quarters. Seven of the "12 largest PRs" (2.4) land
in August. So "PRs are big and getting bigger" is true of the **tail**, and of the
**absolute count** of big PRs per month, not of the typical (median) PR.

### 2.2 Ripple metrics

| Metric | n / denominator | % |
|---|---|---|
| `internal/ports/`-touching commits that also touch ≥3 distinct `services/<pkg>` | 25 / 74 | **33.8%** |
| (commit, `services/<pkg>`) pairs that also touch that pkg's `cmd/.../main.go` | 63 / 310 | **20.3%** |
| same, but "touch *any* file in that binary's cmd dir" (main.go+factories.go+config.go) | 103 / 310 | **33.2%** |
| commits touching `db/migrations/` **and** `adapters/outbound/postgres/` **and** some `services/` (full-stack vertical) | 45 / 619 | **7.3%** |
| commits touching `db/migrations/` that also touch the postgres adapter | 55 / 118 | 46.6% |
| commits touching `internal/testutil/` | 48 / 619 | **7.8%** |

Distribution of #distinct service packages touched by a ports-touching commit:
`{0:6, 1:30, 2:13, 3:15, 4:3, 5:3, 6:1, 7:1, 8:1, 9:1}` — a third of ports changes
already spill into 3+ packages, and the tail runs to 9.

Per-package main.go/cmd-dir touch rates (packages with n≥5 commits touching them; full
41-package table in `scratchpad/item2_output.txt`):

| services/pkg | n | also touch main.go | also touch cmd dir | main.go(s) |
|---|--:|--:|--:|---|
| allocation_tracker | 41 | 24.4% | 31.7% | prime-allocation-indexer |
| oracle_price_worker | 33 | 24.2% | 39.4% | oracle-price-indexer |
| backfill_gaps | 28 | 7.1% | 21.4% | base/watcher |
| morpho_indexer | 27 | 25.9% | 33.3% | 3 binaries (morpho-indexer, morpho-vault-backfill, morpho-v2-bootstrap) |
| raw_data_backup | 21 | 9.5% | 19.0% | raw-data-backup |
| live_data | 18 | 11.1% | 11.1% | base/watcher |
| oracle_backfill | 17 | 17.6% | 35.3% | oracle-pricing-backfill |
| aavelike_position_tracker | 12 | 41.7% | 41.7% | 3 binaries |
| data_validator | 12 | 25.0% | 50.0% | watcher-data-validator |
| offchain_price_fetcher | 10 | 20.0% | 50.0% | 2 binaries |
| prime_debt | 6 | 66.7% | 83.3% | prime-debt-indexer |
| psm3 | 5 | 60.0% | 80.0% | psm3-indexer |

The 20.3%-vs-33.2% gap (main.go alone vs. anywhere in the binary's cmd dir) says wiring
is inconsistently split between `main.go` and a sibling `factories.go`/`config.go`
per binary — see F12.3. `oracle_pricing`, `sparklend`, `curveindexer`, `uniswapv3indexer`
have **no** direct `main.go` importer at all (they're one hop away, wired through another
services package or a `factories.go`) — 0% "touch main.go" for those is a mapping
artifact, not a claim they're unwired.

### 2.3 Co-change clusters (non-test `.go` files, canonicalized identity)

313 file pairs co-change in ≥4 commits. Top 40:

| n | Layers | Pair |
|--:|---|---|
| 14 | cmd | `cmd/workers/morpho-indexer/main.go` ↔ `cmd/workers/oracle-price-indexer/main.go` |
| 14 | cmd | `cmd/workers/morpho-indexer/main.go` ↔ `cmd/workers/sparklend-indexer/main.go` |
| 13 | cmd | `cmd/workers/morpho-indexer/main.go` ↔ `cmd/workers/prime-allocation-indexer/main.go` |
| 13 | cmd | `cmd/workers/oracle-price-indexer/main.go` ↔ `cmd/workers/sparklend-indexer/main.go` |
| 13 | cmd | `cmd/workers/prime-allocation-indexer/main.go` ↔ `cmd/workers/sparklend-indexer/main.go` |
| 13 | cmd↔services | `cmd/workers/sparklend-indexer/main.go` ↔ `services/aavelike_position_tracker/service.go` |
| 12 | cmd | `cmd/workers/prime-allocation-indexer/main.go` ↔ `cmd/workers/prime-debt-indexer/main.go` |
| 11 | cmd | `cmd/workers/morpho-indexer/main.go` ↔ `cmd/workers/prime-debt-indexer/main.go` |
| 11 | cmd | `cmd/workers/oracle-price-indexer/main.go` ↔ `cmd/workers/prime-allocation-indexer/main.go` |
| 11 | cmd↔services | `cmd/workers/oracle-price-indexer/main.go` ↔ `services/oracle_price_worker/service.go` |
| 11 | cmd↔services | `cmd/workers/prime-allocation-indexer/main.go` ↔ `services/allocation_tracker/service.go` |
| 10 | cmd | `cmd/backfillers/sparklend-backfill/main.go` ↔ `cmd/workers/sparklend-indexer/main.go` |
| 10 | cmd | `cmd/workers/oracle-price-indexer/main.go` ↔ `cmd/workers/prime-debt-indexer/main.go` |
| 10 | cmd | `cmd/workers/prime-debt-indexer/main.go` ↔ `cmd/workers/sparklend-indexer/main.go` |
| 10 | services | `services/aavelike_position_tracker/service.go` ↔ `services/morpho_indexer/service.go` |
| 10 | pkg↔services | `pkg/aavelike/blockchain_service.go` ↔ `services/aavelike_position_tracker/service.go` |
| 10 | cmd↔adapters | `cmd/base/watcher/main.go` ↔ `adapters/outbound/postgres/blockstate_repository.go` |
| 10 | services | `services/backfill_gaps/backfill_gaps_service.go` ↔ `services/live_data/live_data_service.go` |
| 9 | adapters↔ports | `adapters/outbound/memory/blockstate.go` ↔ `ports/outbound/blockstate.go` |
| 9 | adapters↔ports | `adapters/outbound/postgres/blockstate_repository.go` ↔ `ports/outbound/blockstate.go` |
| 9 | adapters↔services | `adapters/outbound/postgres/blockstate_repository.go` ↔ `services/backfill_gaps/backfill_gaps_service.go` |
| 9 | cmd | `cmd/backfillers/oracle-pricing-backfill/main.go` ↔ `cmd/workers/oracle-price-indexer/main.go` |
| 9 | cmd↔services | `cmd/backfillers/morpho-vault-backfill/discovery.go` ↔ `services/morpho_indexer/service.go` |
| 9 | services | `services/allocation_tracker/handler_prime_positions.go` ↔ `services/allocation_tracker/service.go` |
| 9 | services | `services/allocation_tracker/handler_prime_positions.go` ↔ `services/allocation_tracker/types.go` |
| 9 | services | `services/morpho_indexer/blockchain_service.go` ↔ `services/morpho_indexer/service.go` |
| 9 | cmd↔services | `cmd/base/watcher/main.go` ↔ `services/live_data/live_data_service.go` |
| 9 | adapters↔services | `adapters/outbound/postgres/blockstate_repository.go` ↔ `services/live_data/live_data_service.go` |
| 8 | adapters | `adapters/outbound/memory/blockstate.go` ↔ `adapters/outbound/postgres/blockstate_repository.go` |
| 8 | adapters↔services | `adapters/outbound/memory/blockstate.go` ↔ `services/backfill_gaps/backfill_gaps_service.go` |
| 8 | ports↔services | `ports/outbound/blockstate.go` ↔ `services/backfill_gaps/backfill_gaps_service.go` |
| 8 | cmd | `cmd/backfillers/aave-like-user-snapshot-indexer/main.go` ↔ `cmd/backfillers/sparklend-backfill/main.go` |
| 8 | cmd | `cmd/backfillers/aave-like-user-snapshot-indexer/main.go` ↔ `cmd/workers/sparklend-indexer/main.go` |
| 8 | cmd | `cmd/backfillers/sparklend-backfill/main.go` ↔ `cmd/workers/morpho-indexer/main.go` |
| 8 | cmd↔services | `cmd/backfillers/sparklend-backfill/main.go` ↔ `services/aavelike_position_tracker/service.go` |
| 8 | cmd↔services | `cmd/workers/morpho-indexer/main.go` ↔ `services/morpho_indexer/service.go` |
| 8 | cmd↔services | `cmd/workers/sparklend-indexer/main.go` ↔ `services/morpho_indexer/service.go` |
| 8 | services | `services/allocation_tracker/service.go` ↔ `services/allocation_tracker/types.go` |
| 8 | cmd↔services | `cmd/base/watcher/main.go` ↔ `services/backfill_gaps/backfill_gaps_service.go` |
| 8 | adapters | `adapters/outbound/postgres/position_repository.go` ↔ `adapters/outbound/postgres/protocol_repository.go` |

(`stl-verify/internal/` prefix dropped from the table for width.)

Raising the threshold to isolate real clusters rather than the raw ≥4 ask (at ≥4,
transitivity through long-lived hub files merges almost everything into one 84-node
component — a graph-connectivity artifact of stable glue files, not evidence that 84
files mutually ripple):

- **Threshold ≥9, 16 files, spans cmd ↔ pkg ↔ services**: all six same-shape indexer
  `main.go`s (morpho, oracle-price, sparklend, prime-allocation, prime-debt,
  sparklend-backfill/oracle-pricing-backfill) plus their `service.go`/
  `blockchain_service.go`.
- **Threshold ≥9, 6 files, spans cmd ↔ adapters ↔ ports ↔ services**: the
  block-state/watcher core — `cmd/base/watcher/main.go`,
  `adapters/outbound/{memory,postgres}` blockstate, `ports/outbound/blockstate.go`,
  `services/{backfill_gaps,live_data}`.

### 2.4 Churn × size hotspots (top 30 non-test `.go` files, commits-since-Jan × current LOC)

| Score | Commits | LOC | File |
|--:|--:|--:|---|
| 32508 | 28 | 1161 | `internal/services/aavelike_position_tracker/service.go` |
| 21924 | 18 | 1218 | `internal/adapters/outbound/postgres/blockstate_repository.go` |
| 20565 | 15 | 1371 | `internal/services/backfill_gaps/backfill_gaps_service.go` |
| 18520 | 20 | 926 | `internal/services/morpho_indexer/service.go` |
| 16282 | 14 | 1163 | `internal/services/live_data/live_data_service.go` |
| 15736 | 14 | 1124 | `internal/pkg/aavelike/blockchain_service.go` |
| 14421 | 19 | 759 | `internal/services/oracle_price_worker/service.go` |
| 14080 | 10 | 1408 | `internal/services/morpho_indexer/blockchain_service.go` |
| 13464 | 24 | 561 | `cmd/base/watcher/main.go` |
| 9737 | 13 | 749 | `cmd/backfillers/morpho-vault-backfill/discovery.go` |
| 9099 | 27 | 337 | `cmd/workers/sparklend-indexer/main.go` |
| 8855 | 23 | 385 | `cmd/workers/prime-allocation-indexer/main.go` |
| 7592 | 13 | 584 | `internal/services/allocation_tracker/handler_prime_positions.go` |
| 7254 | 9 | 806 | `internal/adapters/outbound/alchemy/client.go` |
| 7117 | 11 | 647 | `internal/adapters/outbound/memory/blockstate.go` |
| 7020 | 10 | 702 | `internal/services/oracle_backfill/service.go` |
| 6816 | 8 | 852 | `internal/services/raw_data_backup/service.go` |
| 6495 | 15 | 433 | `internal/services/allocation_tracker/service.go` |
| 6210 | 5 | 1242 | `internal/adapters/outbound/maple/client.go` |
| 5817 | 21 | 277 | `cmd/workers/oracle-price-indexer/main.go` |
| 5780 | 17 | 340 | `cmd/workers/morpho-indexer/main.go` |
| 5598 | 6 | 933 | `internal/services/maple_graphql_indexer/service.go` |
| 5358 | 6 | 893 | `cmd/backfillers/raw-block-bulk-downloader/main.go` |
| 4680 | 9 | 520 | `internal/adapters/outbound/alchemy/subscriber.go` |
| 4610 | 10 | 461 | `cmd/backfillers/aave-like-user-snapshot-indexer/main.go` |
| 4550 | 10 | 455 | `internal/adapters/outbound/postgres/onchain_price_repository.go` |
| 4548 | 4 | 1137 | `internal/services/morpho_indexer/event_extractor.go` |
| 4146 | 6 | 691 | `internal/adapters/outbound/postgres/morpho_repository.go` |
| 3780 | 12 | 315 | `internal/adapters/outbound/postgres/allocation_repository.go` |
| 3705 | 13 | 285 | `cmd/workers/prime-debt-indexer/main.go` |

(prefix `stl-verify/` dropped.) The top 8 are the same files that dominate the
co-change clusters above — churn and coupling point at the same handful of files.

### 2.5 The 12 largest PR units (by files changed)

| Date | Files | Lines | Ticket | Subject | Judgement |
|---|--:|--:|---|---|---|
| 2026-04-14 | 96 | 4020 | VEC-80 | Data audibility | Genuine breadth — an audit trail is cross-cutting by nature (25 domain, 21 adapters, 12 services) |
| 2026-09-02 | 92 | 6632 | VEC-475 | SQS shutdown hardening — never strand an in-flight message on rollout | **Ripple** — one lifecycle/shutdown pattern hand-applied to 27 worker `cmd/` + 21 services files |
| 2026-08-20 | 91 | 6000 | VEC-272 | CORE model integration (1/3) | Genuine, but localized — 85/91 files in `python/`, not a Go-side ripple |
| 2026-08-20 | 87 | 2214 | VEC-565 | share one container set per CI run, template-DB cloning | **Ripple** — one test-infra convention touching every package's integration-test setup (23 adapters, 16 services, 12 testutil) |
| 2026-08-20 | 87 | 6988 | VEC-NA | reference mode serving Sky's published figures | Genuine full-stack feature (python+ts+adapters+k8s+ports+domain) |
| 2026-08-24 | 84 | 10964 | VEC-218 | on-demand Temporal history jobs — V2 bootstrap + S3 vault backfill | Genuine — new capability, vertical by nature |
| 2026-02-26 | 84 | 14174 | SEN-155 | Index Morpho | Genuine — introducing a brand-new indexer |
| 2026-08-31 | 80 | 3488 | VEC-NA | give each view its own route component, lift shared code out | **Ripple** — mechanical per-route split, 80/80 files in `ts/` |
| 2026-03-31 | 77 | 4013 | (none) | Standardize cron | **Ripple** — the cmd/ directory reorg commit itself (see F12.6); renames dominate the diff |
| 2026-07-13 | 75 | 17355 | VEC-261 | Uniswap V3 indexer | Genuine — new indexer (dex-indexer) |
| 2026-07-09 | 74 | 2686 | VEC-471 | pin all indexer state multicalls to block hash | **Ripple** — one invariant, no seam, hand-applied across 38 services + 18 pkg files (see F12.1) |
| 2026-08-24 | 70 | 5768 | VEC-NA | composite results from indexed + reference data | Genuine, localized — 42 ts + 27 python |

**7 of 12 are genuine feature breadth; 5 of 12 are ripple from a single conceptual
change with no seam to contain it** — even among the very largest PRs, "big" doesn't
mean "complex feature" almost half the time.

### 2.6 Cost of a new indexer (clean single-binary introductions, one row = one logical indexer)

| Indexer | Files | Lines | Notes |
|---|--:|--:|---|
| VEC-320 maple-graphql-indexer | 68 | 10648 | includes a hand-written GraphQL client — above the floor |
| VEC-346 psm3-indexer | 19 | 3152 | near the boilerplate floor |
| VEC-374 cex-orderbook-indexer (Coinbase) | 19 | 1980 | near the boilerplate floor |
| VEC-260 curve-indexer | 50 | 15782 | Stableswap+Cryptoswap AMM math — above the floor |
| VEC-437+438 fluid-vault-indexer | 31 | 4354 | **split across 2 consecutive PRs**: #494 service/adapter logic, #495 cmd+k8s+alerts |
| VEC-261 dex-indexer (Uniswap V3) | 75 | 17355 | tick-math heavy — above the floor |
| VEC-540 offchain-price-backfill (merge PR #660) | 29 | 3146 | near the boilerplate floor |

Average per-layer cost across these 7 (full breakdown in `scratchpad/item6_output.txt`):

| Layer | avg files | avg lines |
|---|--:|--:|
| cmd | 4.7 | 675 |
| services | 12.7 | 3960 |
| adapters/postgres | 3.3 | 1293 |
| adapters/other | 2.1 | 511 |
| ports | 1.3 | 59 |
| domain | 4.0 | 531 |
| pkg | 1.9 | 198 |
| migrations | 1.1 | 445 |
| testutil | 0.4 | 18 |
| k8s | 6.3 | 95 |
| alerts/runbooks | 1.4 | 179 |
| Makefile | 0.6 | 25 |

Total: median 31 files / 4354 lines, mean 42 files / 8060 lines (n=7). The
"simple" indexers (psm3, cex-orderbook, fluid-vault, offchain-backfill: 19–31 files)
share a consistent **~19–31-file floor** that is pure scaffolding — a new `main.go`
(+ sometimes a `factories.go`), 6–10 k8s manifests, 1–2 alerts/runbook files, a
Makefile line, a domain entity, a ports interface, a postgres repo, a service
skeleton — regardless of the indexer's actual business complexity. VEC-437/438
shows a team already splitting that fixed cost into two PRs by hand (service logic,
then wiring) when it got too big for one.

### 2.7 Ticket-prefix mix

| Prefix | n | files med | files p90 | lines med | lines p90 | span |
|---|--:|--:|--:|--:|--:|---|
| VEC | 298 | 7 | 35.3 | 470 | 3445 | 2026-03-13 .. 2026-09-03 |
| (none — conventional-commit `chore/fix/feat/docs`, or bare) | 183 | 2 | 14 | 24 | 735 | 2026-01-05 .. 2026-09-02 |
| SEN | 99 | 13 | 35.8 | 1033 | 4612 | 2026-01-09 .. 2026-05-07 |
| ORB | 16 | 2.5 | 8 | 199 | 814 | 2026-05-04 .. 2026-08-31 |
| ARCT | 14 | 11 | 23.7 | 343 | 3101 | 2026-08-21 .. 2026-09-03 |
| other (TEN, ADR, H, UV, GO, NON, SENPRI, VEV) | 9 | — | — | — | — | scattered |

The tracker prefix itself changed over the window: **SEN** (Jan–May, biggest median at
13 files) gave way to **VEC** (Mar onward, the bulk of the work, smaller median at 7),
while **ORB** (small, ops-flavored, median 2.5 files) and **ARCT** (appears only in the
last 2 weeks of the window) look like separate concern-specific trackers rather than
a size trend. 183/619 (30%) of PRs carry no ticket reference at all — mostly
`chore:`/`fix:`/dependency-bump style commits — so ticket-based velocity tracking
undercounts real change volume by close to a third.

### 2.8 Authors

**16 distinct commit authors** in the 619 PR units (15 human + `dependabot[bot]`).
1 author has exactly 1 PR in-window; 10 authors have ≥10 PRs each.

## 3. Findings

### F12.1 — The reorg-safety invariant ("pin reads to a block hash, not a number") is enforced by convention at each call site, not by a seam, and is confirmably not applied everywhere today
**Strength**: Strong
**Files**:
- `stl-verify/internal/ports/outbound/multicaller.go:10,21` — the port exposes both
  `Execute(ctx, calls, blockNumber *big.Int)` (line 10) and `ExecuteAtHash(ctx, calls,
  blockHash)` (line 21). The doc comment on `ExecuteAtHash` (lines 11-20) *explicitly*
  spells out the danger: "after a reorg an archive node answers eth_call-by-number with
  the new canonical state, which can silently disagree with the reorged (older-version)
  data being processed" — i.e. the port itself documents the exact bug and still leaves
  choosing the safe method up to each caller.
- `stl-verify/internal/services/fluid_vault_indexer/blockchain_service.go:145,187,266` —
  **all three** call sites (including `readVaultEntireDataChunk`, which snapshots vault
  state while processing a specific block's logs — line 349-403 of `service.go` traces
  `blockNumber` back to `scanLogs`/`discoverDeployedVault`, i.e. real per-block event
  processing, not a "latest tip" read) use the number-pinned `Execute`, never
  `ExecuteAtHash`. Zero hash-pinned calls anywhere in that package's non-test code.
- Compare the sibling that does the same job correctly:
  `stl-verify/internal/services/curveindexer/service.go:291-296` (`snapshotPools`) and
  `cryptoswap_handler.go:164-181` explicitly call `ExecuteAtHash`/`shared.RunSnapshotReads`
  with a comment: *"pinned to blockHash ... so the read cannot silently answer from a
  post-reorg fork."*
- Also number-pinned (partial/mixed within the same package): `morpho_indexer/vault_probe.go:186,360`
  and `adapter_probe.go:92` (defensible if these are genuinely "does this address look
  like a vault" existence probes, not state extraction — needs a maintainer call);
  `internal/adapters/outbound/blockchain/vat_caller.go:78`;
  `allocation_tracker/handler_prime_positions.go:488` (this file *also* appears in the
  hash-pinned list — mixed within one file).
**Problem**: `git show --stat` on `c92be237` (VEC-471, PR #520, 2026-07-09, "pin all
indexer state multicalls to block hash", 74 files / 2686 lines) shows a repo-wide sweep
that touched 9 services packages — but only **test** files in `curveindexer` and
`fluid_vault_indexer` (mock signature updates, not production logic), and could not
possibly reach `dex-indexer`/`uniswapv3indexer`, which didn't exist until
2026-07-13 (VEC-261), 4 days later. A convention-based fix has no way to bind future
code. `fluid_vault_indexer`'s core snapshot path (`readVaultEntireDataChunk`, doing
exactly what `curveindexer.snapshotPools` does) still reads at a block number today.
This is a live, reorg-race-condition-shaped correctness gap, not just an aesthetic one.
**Proposed change**: Give the `Multicaller` port one hash-pinned path for any read tied
to a specific historical block, and make the number-pinned variant explicitly opt-in
for "latest" reads (rename to something like `ExecuteAtNumberUnsafe`, or fold it behind
a `Latest()` helper) so a new indexer's author has to actively choose the unsafe path
rather than land on it by copying the wrong example. Longer term, this is the same gap
already flagged as "missing block-pinned reader seam" in a prior blast-radius diagnosis
(PR #520) — a shared `BlockPinnedReader`/`shared.RunSnapshotReads`-style helper that
every indexer's blockchain-service is built from would make pinning structural instead
of remembered.
**Benefits**: Locality (the invariant lives in one seam, not ~10 call sites across 6+
packages); leverage (new indexers get correct reorg handling for free — shrinks the
"cost of a new indexer" floor in 2.6 by removing a class of manual follow-up work);
tests (one seam-level reorg-simulation test replaces N ad hoc per-indexer ones).
**Risk / migration**: Low. Each `Execute(ctx, calls, big.NewInt(n))` call site already
has the block hash available (from the event/BlockState being processed) one frame up
— swap to `ExecuteAtHash`. `fluid_vault_indexer` (3 call sites) is a clean, small first
PR. The probe-path call sites need a maintainer decision on intentional "latest" reads
before flipping them.
**Size**: S (fluid_vault_indexer alone) to M (bundled with the port rename/lint guard
and the probe/vat_caller call sites).
**Depends on / enables**: Directly relevant to whatever the ports/domain area (09) finds
about the `Multicaller` port; enables shrinking the fixed cost in F12.2/2.6.

### F12.2 — No shared scaffold for "new worker/indexer/backfiller binary": the same ~19-31-file skeleton is hand-copied every time
**Strength**: Strong
**Files**: co-change table 2.3 (the ≥9-threshold 16-file cluster: all six indexer
`main.go`s plus their `service.go`/`blockchain_service.go`); cost table 2.6.
**Problem**: Every new indexer PR re-produces the same shape: a `main.go` (+ often a
`factories.go`) wiring flags/env/DB pool/lifecycle, 6-10 near-identical k8s manifests,
1-2 alerts/runbook files, a Makefile line, a domain entity, a ports interface, a
postgres repository, a service skeleton. The co-change data shows this isn't
hypothetical: the six indexer `main.go` files changed together 8-14 times each pair,
and `cmd/base/watcher/main.go` alone shows up in the top-30 churn×size table despite
being only 561 lines. Section 2.6 shows the "simple" indexers cluster tightly at
19-31 files even when their actual business logic (curve AMM math, GraphQL client) is
trivial — that floor is boilerplate, not complexity.
**Proposed change**: A generator/template (even a simple `cmd/util/gen-transformed`-style
code generator, or a documented copy-and-fill checklist backed by a lint/CI check that
diffs a new indexer's `main.go` against the template) for the parts that are genuinely
mechanical: lifecycle/flag/env wiring in `main.go`, the k8s manifest set, and the
Makefile entry. This doesn't touch the genuinely-variable parts (service logic, domain
entity, postgres schema).
**Benefits**: Leverage (new-indexer cost drops from a ~19-31-file floor to whatever's
genuinely new); locality (a taxonomy change, like the 2026-03-31 reorg in F12.6, becomes
a template edit instead of 30+ hand edits); consistency (the F12.1 pinning gap and the
"20.3% vs 33.2%" main.go/factories.go split in F12.3 are both symptoms of hand-copying
without a canonical template to copy from).
**Risk / migration**: Retrofitting existing binaries is optional and can be done
opportunistically; only new indexers need the generator. Start with k8s manifests
(most mechanical, lowest risk) before touching `main.go` wiring.
**Size**: L (templating cmd wiring + k8s + Makefile is several PRs; a full generator is
an epic).
**Depends on / enables**: F12.1, F12.3, F12.6.

### F12.3 — Composition-root wiring is inconsistently split between `main.go` and a sibling `factories.go`/`config.go`
**Strength**: Worth exploring
**Files**: per-package table in 2.2 (e.g. `allocation_tracker` 24.4% main.go vs 31.7%
cmd-dir; `dexconsumer` 33.3% vs 100%); `stl-verify/cmd/workers/dex-indexer/factories.go`
(imports `curveindexer`/`uniswapv3indexer`, not `main.go`).
**Problem**: Aggregated across all (commit, services/pkg) pairs, only 20.3% also touch
`main.go` directly, but 33.2% touch *some* file in the binary's cmd directory — meaning
roughly a third more wiring changes land in a `factories.go`/`config.go` sibling than in
`main.go` itself, and which file owns wiring for a given binary isn't predictable from
outside that directory. This is exactly the "configuration and wiring sprawl in
composition roots" pattern the overhaul brief calls out.
**Proposed change**: Pick one canonical shape (either `main.go` always delegates to a
same-named `wire.go`/`factories.go`, or wiring stays in `main.go` and only truly
reusable multi-binary factories live elsewhere) and apply it uniformly; likely overlaps
with whatever area 10 (composition roots) already found in more depth.
**Benefits**: Locality — one place to look for "how is this binary wired" across all 32
binaries.
**Risk / migration**: Pure move/rename in most cases; low risk, mechanical.
**Size**: M.
**Depends on / enables**: Feeds F12.2's template. Cross-check against area 10's findings.

### F12.4 — `internal/ports/` is the single biggest size amplifier, and a third of its changes already ripple into 3+ service packages
**Strength**: Worth exploring
**Files**: table 2.1 (`ports/` area: median 21 files / 2216 lines vs. overall median 5 /
231 — the largest multiple of any area); table 2.2 (33.8% of ports-touching commits
touch ≥3 distinct services packages; distribution tail runs to 9 packages in one commit).
**Problem**: A port signature or contract change is supposed to be a narrow, one-seam
edit; instead it's the single strongest predictor of a large, multi-package PR in this
dataset. That's consistent with ports being **shallow** (interfaces that mirror
implementation details closely enough that every implementer and every caller needs a
matching edit) rather than deep seams that absorb change.
**Proposed change**: Not diagnosable from history alone which specific ports are
shallow — needs the actual port/adapter code (area 09's job). Flagging the quantitative
signal here: whichever ports show up with many adapters *and* many calling services are
the ones to prioritize for depth review.
**Benefits**: N/A until root-caused; see area 09.
**Risk / migration**: N/A.
**Size**: Unknown pending 09's read of the actual interfaces.
**Depends on / enables**: Depends on area 09 (ports/domain) for the actual port-level diagnosis.

### F12.5 — A handful of "god service" files are simultaneously the top churn×size hotspots and the central co-change hubs
**Strength**: Worth exploring
**Files**: `internal/services/aavelike_position_tracker/service.go` (1161 lines, touched
in 28 of 619 PRs — 4.5% of *all* PRs in 8 months), `internal/adapters/outbound/postgres/blockstate_repository.go`
(1218 lines, 18 touches), `internal/services/backfill_gaps/backfill_gaps_service.go`
(1371 lines, 15 touches), `internal/services/morpho_indexer/service.go` (926 lines, 20
touches), `internal/services/live_data/live_data_service.go` (1163 lines, 14 touches).
**Problem**: The same files top both the churn×size table (2.4) and the co-change
cluster analysis (2.3) — they're not just big, they're the files every related change
has to go through. That's consistent with each acting as a shallow, do-everything
module for its indexer (deletion test: if `aavelike_position_tracker/service.go`'s
responsibilities were split, would the 28 touches concentrate in fewer, more targeted
files, or would the same total complexity just reappear split up?).
**Proposed change**: Have the services-area agents (Morpho, Aave-like, block-state/watcher)
apply the deletion test to these five files specifically and decide whether the
complexity is intrinsic (indexer genuinely does a lot) or a shallow-module symptom
(should split into event-handling vs. persistence vs. reconciliation).
**Benefits**: Would need the split to evaluate.
**Risk / migration**: N/A pending that read.
**Size**: Unknown.
**Depends on / enables**: Cross-check against whichever numbered area covers Morpho,
Aave-like tracking, and the block-state/watcher core.

### F12.6 — The cmd/ directory taxonomy already needed one mechanical repo-wide reorg, and will need another as binary count grows
**Strength**: Speculative
**Files**: `aba593a4` (2026-03-31, "Standardize cron", 77 files / 4013 lines) — renamed
every flat `cmd/<name>/` into `cmd/{workers,backfillers,base,cronjobs,util}/<name>/` in
one PR (confirmed via rename-edge extraction: e.g. `cmd/watcher` → `cmd/base/watcher`,
`cmd/oracle-price-worker` → `cmd/workers/oracle-price-indexer`, 15+ such renames same
commit), while also adding 4 new cronjob binaries as part of the same "standardize cron"
push.
**Problem**: This was a one-time tax paid because there was no scaffold (F12.2) —
adding the grouping convention meant touching every existing binary by hand. At 32
binaries today (vs. presumably fewer in March) and growing, the next taxonomy change
(if binaries outgrow the current 5 groups) repeats this cost, worse.
**Proposed change**: No separate action beyond F12.2 — a scaffold/generator would make
a future reorg a template-and-regenerate operation instead of 77 files by hand.
**Benefits**: See F12.2.
**Risk / migration**: N/A — historical, not actionable on its own.
**Size**: S (informational).
**Depends on / enables**: Motivates F12.2.

## 4. Interpretation — what would actually shrink the median PR

The median PR (5 files) is already small and, if anything, shrinking — the growth
story is in the **tail**: p90/max haven't come down all year, and August 2026 had more
≥40-file PRs (15) than most other months combined. Full-stack-vertical commits
(migrations + postgres + services together) are rare (7.3%), so most day-to-day change
does *not* ripple the whole stack — the big PRs cluster into three recognizable
categories: (1) genuine new-capability work (new indexers, new cross-stack features —
7 of the 12 largest, and all of 2.6's "cost of a new indexer" samples), (2) ripple from
one small conceptual change with no seam to contain it (5 of the 12 largest: shutdown
lifecycle, block-hash pinning, CI container sharing, a cmd/ reorg, a ts route split),
and (3) big-but-localized single-language features (python/CORE-model, ts/frontend)
that aren't a Go-service ripple at all.

To shrink the *tail*, the fixes are the ones in section 3: (a) collapse the block-hash
pinning choice into one safe path (F12.1) so the next "apply this invariant everywhere"
PR doesn't need to exist; (b) give new indexers a template so their ~19-31-file floor
stops being hand-copied (F12.2); (c) settle the main.go-vs-factories.go split (F12.3)
so wiring changes stay in one predictable place; (d) whatever area 09 finds makes
`internal/ports/` deeper will directly cut the biggest area-level size multiplier in
2.1's table.

## 5. Cross-area observations

- `internal/testutil/` has 13 hand-rolled `mock_*.go` files (1031 lines total:
  `mock_receipt_token_repository.go`, `mock_user_repository.go`,
  `mock_debt_token_repository.go`, `mock_protocol_repository.go`,
  `mock_morpho_repository.go`, `mock_event_repository.go`, `mock_block_cache.go`,
  `mock_multicaller.go`, `mock_tx_manager.go`, `mock_sqs_consumer.go`,
  `mock_subscriber.go`, `mock_blockchain_client.go`, `mock_token_repository.go`) — one
  per repository-shaped port, by hand. Worth the ports/testing-focused area checking
  whether these could be generated from the port interfaces instead.
- `internal/services/oracle_pricing` and `internal/services/sparklend` (base package,
  just `types.go`) have no direct binary or caller found via import-grep — possible dead
  or purely-shared code; worth a services-area agent confirming.
- `cmd/base/cex-feed-watcher/` contains only a `.env` file, no `main.go` — either a stub
  for planned work or leftover scaffolding.
- ts/ PR volume grew from 6 (Jan-Apr) to 76 (May-Sep) commits touching `ts/`, and
  `alerts/` didn't exist at all before May (n=0 Jan-Apr, 52 commits May-Sep) — both
  probably fine, just noting for whichever area tracks frontend or alerting maturity.
- `allocation_tracker/handler_prime_positions.go` and `psm3_caller.go` each call *both*
  the number-pinned and hash-pinned `Multicaller` methods in the same file — worth a
  second look alongside F12.1.

## 6. Open questions

- Whether `morpho_indexer`'s probe-path number-pinned calls (`vault_probe.go`,
  `adapter_probe.go`) are an intentional "latest tip is fine for existence-checking"
  design choice or an oversight — needs a maintainer/domain-expert call, not visible
  from history or the code alone.
- Whether the 2026-03-31 cmd/ reorg's 13 touched `adapters/` files carried real logic
  changes alongside the renames, or were import-path-only; I did not diff its content
  in detail beyond confirming the rename edges.
- Whether `oracle_pricing` and `sparklend` (services) are genuinely dead code or wired
  through a mechanism my import-grep missed (e.g. reflection, code generation, a build
  tag).
- The exact cause of the August 2026 spike in large PRs (15 PRs ≥40 files) is
  inferred from co-occurring ticket subjects (CORE model, ts route split, SQS
  hardening, CI container sharing, Sky reference mode) rather than confirmed as a
  single coordinated cause — could be coincidental clustering of unrelated large
  efforts, or a deliberate push before some deadline.
