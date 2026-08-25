# Reference-snapshots investigation

Session summary: reviving ingestion of the "Star Agents Risk Capital & Requirements
Monitor" data source (`app/services/data_provenance_service.py`), on branch
`rohit/prime-capital-snapshots`.

## Starting question

"Are we still ingesting the data from 'Star Agents Risk Capital & Requirements
Monitor'?" — No. It's registered in `DataProvenanceService` purely as a
documentation/provenance entry (`caveat="Kept for parity checks; no longer the
source of the dashboard's risk-capital figures."`). There is, and never was, a
storage-backed ingestion pipeline for it on `main`.

## What actually shipped vs. what was abandoned

- **PR #275** ("Add logos for protocols, chains, tokens") also merged a **live
  proxy endpoint**, `GET /v1/capital-metrics`
  (`app/api/v1/allocations.py::list_capital_metrics`,
  `_fetch_star_risk_capital_payload`). It fetches the upstream list endpoint on
  every request and returns derived metrics. Nothing is persisted — no history,
  no backfill possible from it, by construction.
- **`rohit/prime-capital-snapshots`** (two commits, dated 2026-05-07, never
  merged) builds on top of that proxy: a `prime_capital_stack` Postgres table +
  Python read path (`get_latest_capital_stack`), and a Go Temporal cronjob
  (`cmd/cronjobs/reference-capital-indexer`) that polls the upstream list endpoint
  every 15 minutes and upserts snapshots. This is the actual ingestion pipeline
  that was scaffolded but abandoned mid-flight.
- This branch has now been **rebased cleanly onto current `main`** (commits
  `108241cf`, `60b08377`). Conflicts were mostly obsolete-code deletions (the
  old `CapitalMetricsService`/`capital_metrics.py`, superseded by
  `PrimeRiskCapitalService`/`prime_risk_capital.py` since this branch was cut)
  and one Makefile `dev-env` merge. Build, `go vet`, `ruff`, `ty`, and the full
  unit test suite all pass post-rebase.

## Requested feature: a `reference` query param

Ask: add a `reference: bool` param to `/v1/primes/{id}/risk-capital` (and
plausibly `/total-capital`, `/exposure`) that switches between STL's
self-computed figures and the upstream reference feed. Design direction agreed
so far: **keep response shapes identical** between the two modes — reference
mode should *add* data, not replace or null out self-computed fields (e.g.
`per_allocation`, which only STL's own on-chain model can produce). Exact field
list is not yet finalized — this was mid-design when the investigation below
took over, because the "one reference source" assumption turned out to be
wrong.

**Not yet implemented in code** — this doc captures the endpoint catalogue so
implementation can resume with the full picture instead of the single `/primes/`
list endpoint originally assumed.

## Upstream endpoint catalogue

Two distinct hosts, discovered by direct probing (`curl`) during this session.
Every request below was a live, read-only GET against a public URL.

### Host A — `info-sky.blockanalitica.com`

**Confirmed by exhaustive probing** (this session): Host A has exactly six live
routes — `/star-monitoring/risk-capital/primes/`, `.../primes/{star}/`,
`.../primes/{star}/allocations/`, `.../risk-capital/aggregates/`, `/overall/`,
and `/overall/historic/`. There is **no API schema** (`/openapi.json`,
`/api/schema/`, `/docs` all 404) and no route index. Note
`/star-monitoring/risk-capital/primes/historic/` returns **500, not 404** — this
is *not* a broken history endpoint, it is `historic` being captured by the
`{star}` path segment and failing the star lookup (`/primes/zzznotastar/`
returns the same 500). **There is no per-prime history on Host A.**

Every `/star-monitoring/...` path here is current-snapshot only: `?date=`,
`?from=`/`?to=`, `?as_of=`, **`?days_ago=`** and **`?order=`** are silently
ignored, and `/star-monitoring/.../history/` or `/historic/` paths all 404.
Verified by checksum, not by eye: `/primes/`, `/primes/?order=-exposure`,
`/primes/?days_ago=1`, `/primes/?days_ago=30` and `/primes/?date=2026-07-20`
all return **byte-identical** payloads (same md5, 1075 bytes). Only `?limit=`
changes the response, and only in the byte where `pagination.limit` echoes it
back. `?order=` being accepted-but-ignored is the dangerous one — it returns
`200` and looks sorted, so never rely on upstream ordering; sort our side.
**Correction from the previous version of this doc**: that's not true of the
whole host — `/overall/historic/` (different top-level path, not under
`/star-monitoring/`) is a genuine daily historic endpoint. See below.

| Endpoint | Shape | Notes |
|---|---|---|
| `GET /star-monitoring/risk-capital/primes/` | `{data:{results:[{star, exposure, total_rc, financial_rrc, exposure_share, risk_tolerance_ratio}], pagination:{page, limit, total, pages, next, previous}}, status, success}` | Current only. **Already integrated** (`allocations.py`'s `_fetch_star_risk_capital_payload`). One call, all primes. **Covers only 4 primes today** (grove/spark/obex/osero) while Host B's `/internal/primes/historic/` carries 11 (keel, ozone, …) — so reference mode is unavailable for any prime the risk-capital monitor does not track, and that must read as "no reference data", never as zero. Minor drift: this endpoint's `exposure` for spark (`2098120696.91`) and the detail endpoint's `total_exposure` (`2098041068.29`) disagree slightly, so pick one source per field rather than mixing the two. |
| `GET /star-monitoring/risk-capital/primes/{name}/` | `{data:{total_exposure, total_rrc, total_exposure_share, total_jrc, total_src, total_rc, encumbrance_ratio, internal_jrc, external_jrc, tokenized_jrc, internal_src, external_src, epi_utilization, spj_utilization}, status, success}` | Current only. **Not yet integrated.** Real junior/senior capital split (`total_jrc`/`total_src`) instead of our derived `max(total_rc - financial_rrc, 0)` approximation, plus a precomputed `encumbrance_ratio`. One call per prime. |
| `GET /star-monitoring/risk-capital/primes/{name}/allocations/?limit=&page=&order=` | `{data:{results:[{protocol, network, star, token_address, symbol, name, loan_token_address, loan_token_symbol, exposure, rrc, crr}], pagination:{page, limit, total, pages, next, previous}}, status, success}` | Current only. **Not yet integrated, and not in the previous version of this doc.** Upstream's own *per-allocation* RRC breakdown — maps almost 1:1 onto our `PrimeRiskCapitalResponse.per_allocation` (`symbol`, `protocol`, `exposure`, `rrc`, `crr`), so `per_allocation` **can** be reference-sourced after all. Two conversion traps: `crr` is a **fraction** (max observed `0.5`) where ours is `crr_pct` on a 0-100 scale, and `token_address`+`network` are the join key back to our `receipt_token_id`, not `symbol`. 12 rows for spark across `sparklend`/`morpho`/`uniswap`/`arkis`/`anchorage` on `ethereum`/`base`. **Silent-empty trap**: an unknown star returns `200` with `results: []` (not 404) — `/primes/zzznotastar/allocations/` confirmed — so an empty list must never be read as "this prime holds nothing". |
| `GET /star-monitoring/risk-capital/aggregates/` | `{data:{total_exposure, total_rrc, total_rc, total_exposure_share, total_rtr}, status, success}` | Current only. Cross-prime totals. |
| `GET /overall/historic/?days_ago=N` | `{data:[{date, datetime, total_dai, total_usds, surplus_buffer, total}]}` | **Real daily history, protocol-wide (not per-prime)** — `total_dai`/`total_usds`/`surplus_buffer`/`total` describe the whole Sky protocol, not any one star. Confirmed `days_ago=1` (2 rows) and `days_ago=30` (31 rows) and `days_ago=90` (91 rows) return real distinct daily data. **Flaky**: `days_ago=5`, `10`, `60`, `200`, `400` all deterministically 500 (reproduced on retry, not a rate-limit — some values just error). Don't assume an arbitrary `days_ago` works; validate the specific value before depending on it, and handle 500 as "endpoint unstable," not "no data." Doesn't help per-prime backfill since it's protocol-wide only. |

### Host B — `sky.data.blockanalitica.com/internal/...` (has real history)

Everything here sits under an `/internal/` path prefix — unlike Host A this was
never documented as an "approved" source in the old planning notes on the
abandoned branch, so depending on it in production probably wants an explicit
call-out/sign-off in the PR per the repo's off-chain-feed rule
(`AGENTS.md`: "Off-chain feeds need maintainer approval, justified in the PR
description").

| Endpoint | Shape | History depth | Notes |
|---|---|---|---|
| `GET /internal/primes/` | `{data:[{star, name, group, ilk, ilks (JSON string: per-chain ALM proxy addresses), assets, assets_change, allocated_assets, idle_assets, debt, liabilities, treasury_balance, treasury_balance_change, backstop_capital, backstop_capital_change, in_transit_assets, nav, aum, ...}]}` | current only | Looks like the **live source of truth our hardcoded axis-synome contract mirrors** — `ilks[].alm_proxy_addresses` gives per-chain proxy addresses per prime directly. Out of scope for this task, but worth flagging: could reduce/replace the maintained-by-hand contract if we ever want live proxy discovery instead of a static seed. |
| `GET /internal/primes/historic/?days_ago=N` | `{data:[{date, star, name, assets, allocated_assets, idle_assets, debt, liabilities, nav, in_transit_assets, treasury_balance, aum, apy, estimated_profit, backstop_capital}]}` | **Confirmed real daily history**, `days_ago=365` → 366 distinct dates × 11 primes = 2749 rows | `treasury_balance` is exactly our on-chain "Total Risk Capital" concept and matches `total_capital.py`'s docstring claim ("matches the upstream Star `total_capital`"). **This is the one endpoint that can genuinely backfill a time series** (`/v1/primes/{id}/total-capital`'s reference mode). No RRC/JRC/SRC fields here — balance-sheet only. |
| `GET /internal/primes/risk-capital/historic/` | `{data:[], status:200, success:true}` | **route exists, always empty** | Tried `days_ago=1/30/365`, `star=spark`, no params — every combination returns an empty array. There is **no historical RRC/JRC/SRC data anywhere** on either host; only Host A's current snapshot exists for risk-capital figures. Any risk-capital "backfill" can only mean accumulating forward from now via our own poller (`reference-capital-indexer`), never reconstructing the past. |
| `GET /internal/primes/{star}/balance-sheet/aggregates/?group_by=month` | `{data:[{date (YYYY-MM), snapshot_date, uid, balance, what (assets\|liabilities), category, category_name, name, symbol, protocol, network}]}` | **Confirmed ~2 years of monthly history**: `2024-10` through `2026-08` for `spark` (923 rows) | Per-position, per-protocol, per-network line items, categorized (`stablecoins`, `savings_vaults`, `onchain_crypto_lending`, `otc_crypto_lending`, `basis_trade`, `basis_trade_cme`, `short_duration_treasury_bills`). This is a genuine historical decomposition of exposure by position — much richer than a single aggregate number, and a plausible cross-check/backfill source for `/v1/primes/{id}/exposure`'s per-bucket time series, not just a single latest point. |
| `GET /internal/backing/items/?prime=spark` | `{data:[{collateral_symbol, collateral_address, underlying_symbol, underlying_address, token_address, wallet_address, borrow_amount, network, protocol, category, category_name, star, name, symbol, updated_at, backed, backed_lt, backed_total, lt}]}` | current only (has `updated_at` per row) | Per-collateral/per-underlying loan positions with a liquidation threshold (`lt`) and backed/backed_lt/backed_total figures — this is upstream's own version of the `BackedBreakdown` concept our `gap_sweep` model computes independently on-chain. Potential external validation source for our per-allocation backed-breakdown math, not just aggregate RRC. 65 rows for spark alone. |

## Which Host B figures are the same measurement as Host A

Verified by back-to-back fetch on 2026-08-19, for the two primes the
axis-synome contract tracks (grove, spark — the `prime` table also carries a
stale `obex` row, which is why the contract, not the table, is the tracked set):

| Host A | Host B | Verdict |
|---|---|---|
| `total_rc` | `treasury_balance` | **Identical.** spark 48,142,491.09 and grove 26,124,170.36 on both feeds. This is what lets `/total-capital` splice Host B history onto Host A snapshots with no step at the join. |
| `total_exposure` | `allocated_assets` | **Different measurements.** spark +32.07%, grove +0.54% at the same instant — the gap is prime-dependent, so it is not an offset that can be corrected for. Never serve `allocated_assets` as exposure. |
| `total_exposure` | `assets` | Different again (spark +56.53%). |

So `/exposure` has no reference history and cannot acquire one: Host A has no
per-prime history, and Host B's nearest field is a different quantity. Its
reference series necessarily starts when the syncer first ran.

Host B holds a clean year for both tracked primes — 366 daily rows each,
2025-08-19 to 2026-08-19, no duplicate dates, with `treasury_balance`,
`assets`, `allocated_assets`, `idle_assets`, `debt` and `backstop_capital`
populated on every row. `nav`, `aum`, `apy`, `liabilities`, `in_transit_assets`
and `estimated_profit` are null on all 732 rows, so no column exists for them.
Coverage is not uniform across the feed, though: `obex` has a null
`allocated_assets` on 2025-11-17, which the backfiller would reject rather than
zero-fill if that prime ever became tracked.

## Why exposure has no reference history (and it is not units)

The obvious hypothesis — Host B encodes exposure at a different scale — is
wrong. Two independent checks, both from back-to-back fetches:

- **The ratio is not constant.** `allocated_assets / total_exposure` is 1.3207
  for spark and 1.0054 for grove. A unit or scale mismatch is a single
  multiplier and would be identical for both.
- **The control matches exactly.** `treasury_balance` equals `total_rc` to the
  last decimal on both primes, from the same pair of feeds. Same encoding, same
  scale — so the exposure gap is a real difference in what is being counted.

Each side satisfies its own identity, and they are different identities:

| feed | identity | verified |
|---|---|---|
| Host B | `assets − idle_assets == allocated_assets` | exact (2,683,481,657.46) |
| Host A | `Σ per-allocation exposure == total_exposure` | to ~1e-6 (2,020,123,506 vs 2,020,124,395) |

So Host A answers *"what has the risk model priced"* and Host B answers *"what
assets are deployed"*. Host B's own category breakdown
(`/internal/primes/{star}/balance-sheet/aggregates/`) shows where spark's ~663M
gap sits — two categories tie straight to Host A rows (OTC Crypto Lending is
the 210,000,001 `ANCHORAGE` position, Basis Trade is the 20,279,877 `arkis`
one), while **Savings Vaults has no counterpart in the risk breakdown at all**
and the large Stablecoins bucket is mostly plain holdings rather than risk
positions. Grove's near-match is a quirk of its portfolio, not agreement
between the definitions.

The consequence: exposure cannot be backfilled from this feed by any
correction. It would need a category→risk-position mapping, and even with one
the idle/unpriced residual has no risk-side counterpart *by construction* — it
is not missing data, it is a quantity the risk monitor deliberately does not
count. Splicing it would have put a ~663M step in spark's series at the join,
looking like a real capital event.

Note the category totals are a monthly aggregate while the others are daily
snapshots, so they characterise the shape of the gap rather than reconciling to
the cent.

## Exposure going forward: already indexed, only the history is absent

Not backfilling exposure does **not** mean it is unavailable. The syncer writes
`exposure_usd` from Host A's `total_exposure` on every cycle, so
`/v1/primes/{id}/exposure?reference=true` returns a growing series from the day
the syncer first ran — confirmed in the local cluster across six consecutive
15-minute cycles. Only the year *before* that first run is missing, and by the
argument above it always will be.

## Reading the reference allocations grid

Two effects make the UI look sparser under `reference=true` than it is. Both are
measurable; neither is a defect in the reference path.

**The risk-capital column is mostly `n/a` because the token-registry join
misses.** The grid keys risk capital by `receipt_token_id`, which is resolved
from upstream's `token_address` + `network`. Against a local dev cluster whose
`receipt_token` table holds 9 rows, exactly 2 of spark's 11 upstream positions
join — `spUSDT` and `sparkUSDCbc` — and those are precisely the two rows that
show a figure. The causes are not the same, and only one is permanent:

- **2 rows can never resolve.** The Uniswap V4 positions carry a 66-character
  pool id where an address is expected. No registry will ever match them.
- **7 rows fail only on this cluster.** Their addresses are absent from the
  local `receipt_token` seed. A populated registry resolves them.

Worth flagging one that is neither: the local registry holds a `spDAI` at
`0x73e65dbd…` while the monitor reports `spDAI` at `0x4dedf261…`. Same symbol,
different address — a reminder that symbols are not keys, and that the join
must stay on `(chain_id, address)`.

**Row counts move in the opposite direction to what it looks like.** Measured
against the same cluster:

| address | self | reference |
|---|---|---|
| `0x1601…347e` (spark ALM proxy) | 2 | **11** |
| `0x691a…16ba` (spark vault) | 1 | **404** |

Reference returns *more* rows for an ALM proxy, not fewer; the self-mode
baseline is near-empty because this cluster has no real allocation data. The
404 on the vault address is correct: it is not an ALM proxy, so the axis-synome
contract resolves no star for it and there is nothing to ask the monitor. Note
`/v1/primes` lists both, so a UI that offers the vault as a selectable prime
will hit that 404 in reference mode.

Upstream's own position count is not fixed either — spark reported 12
allocations when this document was first written and 11 later. Treat any count
here as a sample, not a constant.

## Implications for the `reference` param design

- **`/v1/primes/{id}/risk-capital`**: reference mode can only ever be a live
  "latest" comparison (no history exists upstream for RRC/JRC/SRC). Open
  question still unresolved: keep using Host A's coarse `/primes/` list (one
  call, already integrated), or switch to the richer per-prime detail endpoint
  (real JRC/SRC split, one call per prime, new upstream contract to validate).
- **`/v1/primes/{id}/total-capital`**: reference mode *can* be a real time
  series — `/internal/primes/historic/` genuinely backfills up to a year of
  `treasury_balance` per prime, matching the endpoint's existing bucketed
  shape.
- **`/v1/primes/{id}/exposure`**: `/internal/primes/{star}/balance-sheet/aggregates/?group_by=month`
  could genuinely backfill ~2 years of monthly exposure history, though at
  monthly (not the endpoint's finer bucket) granularity and requiring a
  per-position rollup to match our aggregate `exposure_usd` shape.
- The `reference-capital-indexer` Go cronjob (rebased, not yet modified) currently
  polls Host A's coarse list endpoint (`SKY_RISK_CAPITAL_URL` env var). If we
  want the JRC/SRC split it would need to switch to the per-prime detail
  endpoint or fetch both.

## Open decisions (none made yet)

1. Risk-capital reference source: coarse list vs. per-prime detail endpoint.
2. Whether to pursue real backfill for total-capital (`/internal/primes/historic/`)
   and/or exposure (`/internal/primes/{star}/balance-sheet/aggregates/`) given
   they sit on the unofficial `/internal/` host — needs sign-off before
   depending on it in production.
3. Whether `/internal/backing/items/` is worth wiring in as a per-allocation
   cross-check against our own `BackedBreakdown` computation, independent of
   the `reference` param work.
4. Whether `/internal/primes/` is worth exploring later as a live replacement
   for the hardcoded axis-synome ALM-proxy contract (separate, larger change —
   not part of this task).
