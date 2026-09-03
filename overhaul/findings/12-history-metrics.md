Status: DRAFT — investigation in progress

# 12 — History metrics: why PRs are big and getting bigger

Area: git-history mining across the whole repo (2026-01-01 to 2026-09-03), focused on
`stl-verify/` Go service layout. Read-only git analysis; see methodology note below.

## Methodology

- Dataset built from `git log --first-parent` on `main`, treating each first-parent
  commit as one "PR unit": a plain (non-merge) first-parent commit uses its own
  `--numstat`; a merge commit (28 found, all within 2026-07-31..2026-08-17, apparently a
  short-lived non-squash workflow) uses `git diff --numstat merge^1 merge` so a
  multi-commit branch is counted once, at its true total size, not as N fake small PRs.
- Excluded: subjects starting with `deploy(` (477 commits, all `archon-deploy-bot[bot]`),
  and commits touching only `k8s/overlays/*/kustomization.yaml` (4 more, incl. manual
  image-tag/CPU-limit tweaks).
- Result: **619 PR units** in the window.
- File identity for co-change/churn (items 3–4 only) is canonicalized through a
  union-find over all detected git renames, so e.g. the 2026-03-31 `cmd/<name>/` →
  `cmd/{workers,backfillers,base,util,cronjobs}/<name>/` reorg and the
  `borrow_processor` → `sparklend_position_tracker` → `aavelike_position_tracker`
  service rename chain don't create phantom duplicate files. Area-tagging (items 1–2)
  intentionally does NOT canonicalize — a commit is tagged by the path it touched *at
  that time*.
- Scripts and intermediate JSON live in the scratchpad
  (`/private/tmp/claude-501/-Users-tore-workspace-stl/042f3c1b-fa0d-45cd-8200-0d91f621b849/scratchpad`),
  not committed to the repo.

## 1. PR size distribution

Files changed / total lines (ins+del) per PR unit, n / median / p75 / p90 / max:

| Scope | n | files median | files p75 | files p90 | files max | lines median | lines p90 | lines max |
|---|---|---|---|---|---|---|---|---|
| **All (Jan1–Sep3)** | 619 | 5 | 16 | 29 | 96 | 231 | 2960 | 20421 |
| Jan–Apr | 216 | 6.5 | 18 | 28.5 | 96 | 279 | 3325 | 18835 |
| May–Sep | 403 | 5 | 14.5 | 29 | 92 | 221 | 2949 | 20421 |

By area touched (overall, files median/p90; a commit can count in several areas):

| Area | n | files median | files p90 | lines median |
|---|---|---|---|---|
| cmd/ | 135 | 19 | 58 | 1230 |
| services/ | 154 | 17 | 45.7 | 1039 |
| adapters/ | 147 | 17 | 53.2 | 1230 |
| ports/ | 74 | 21 | 66.5 | 2216 |
| domain/ | 50 | 21 | 68 | 2002 |
| pkg/ | 78 | 21 | 58 | 1864 |
| db/migrations/ | 118 | 11 | 47.2 | 712 |
| testutil/ | 48 | 22 | 69.8 | 1433 |
| docs/ | 103 | 10 | 49.4 | 1005 |
| k8s/ | 98 | 7 | 35.9 | 192 |

Headline: PRs touching `ports/`, `domain/`, `pkg/`, or `testutil/` run ~3-4x bigger
(median 21-22 files) than the overall median (5 files) or `k8s/`-only PRs (7 files).
Full period-split and per-area Jan-Apr-vs-May-Sep tables saved in
`scratchpad/item1_output.txt`.

## 2. Ripple metrics

- Commits touching `internal/ports/` (n=74): **33.8%** (25/74) also touch ≥3 distinct
  `services/<pkg>` in the same commit. Distribution of #distinct service pkgs touched
  by a ports-touching commit: {0:6, 1:30, 2:13, 3:15, 4:3, 5:3, 6:1, 7:1, 8:1, 9:1}.
- Of (commit, services/<pkg>) pairs (n=310 aggregate across all packages): only
  **20.3%** also touch that package's `cmd/.../main.go` directly; **33.2%** touch
  *any* file in that binary's cmd directory (main.go + factories.go + config.go...).
  This gap (20% vs 33%) itself shows wiring is often split between `main.go` and a
  same-directory `factories.go`, not concentrated in one file. Per-package table (41
  packages) in `scratchpad/item2_output.txt`; e.g. `prime_debt` 66.7% touch main.go,
  `backfill_gaps` only 7.1%.
- **Full-stack vertical** (same commit touches `db/migrations/` AND
  `adapters/outbound/postgres/` AND some `services/`): **45/619 = 7.3%**. Of the 118
  commits touching migrations, 46.6% (55) also touch the postgres adapter layer.
- Commits touching `internal/testutil/`: **48/619 = 7.8%**.

## 3. Co-change clusters

313 non-test `.go` file pairs co-change in ≥4 commits (post-canonicalization). Top of
list (full top-40 in `scratchpad/item3_output.txt`):

| Count | Pair |
|---|---|
| 14 | `cmd/workers/morpho-indexer/main.go` ↔ `cmd/workers/oracle-price-indexer/main.go` |
| 14 | `cmd/workers/morpho-indexer/main.go` ↔ `cmd/workers/sparklend-indexer/main.go` |
| 13 | `cmd/workers/sparklend-indexer/main.go` ↔ `services/aavelike_position_tracker/service.go` |
| 11 | `cmd/workers/oracle-price-indexer/main.go` ↔ `services/oracle_price_worker/service.go` |
| 10 | `pkg/aavelike/blockchain_service.go` ↔ `services/aavelike_position_tracker/service.go` |
| 10 | `cmd/base/watcher/main.go` ↔ `adapters/outbound/postgres/blockstate_repository.go` |

Raising the co-change threshold to isolate real clusters (not just the raw ≥4 ask)
finds two clean, cross-layer clusters spanning cmd ↔ services ↔ pkg (and one
cmd ↔ adapters ↔ ports ↔ services):
- Threshold ≥9: a 16-file cluster spanning **cmd, pkg, services** — all six
  same-shape indexer `main.go`s plus their `service.go`/`blockchain_service.go`.
- Threshold ≥9: a 6-file cluster spanning **cmd, adapters, ports, services** around
  block-state/watcher: `cmd/base/watcher/main.go`, `adapters/outbound/{memory,postgres}`
  blockstate, `ports/outbound/blockstate.go`, `services/{backfill_gaps,live_data}`.

(Note: at the literal ≥4 threshold, transitivity through long-lived hub files merges
almost everything into one 84-node component; this is a graph-connectivity artifact of
stable glue files, not 84 files that mutually ripple. The ≥9/≥10 cut isolates the
genuine tight clusters — see scratchpad for the full threshold sweep.)

## Still to do (removed once complete)

- Item 4: churn × size hotspots (top 30 files by commits × current LOC).
- Item 5: 12 largest PRs by files, with `git show --stat` judgement.
- Item 6: cost of a new indexer (new cmd/*/main.go commits, lines per layer).
- Item 7: ticket-prefix mix (VEC-/ARCT-/ORB-/SEN- etc.) vs PR size.
- Item 8: author count.
- Findings section (ranked, with strength/size/risk) not yet written.
- Cross-area observations and open questions not yet written.

This file will be overwritten with `Status: FINAL` and the complete report.
