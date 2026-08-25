# Upstream data catalogue — what Sky actually publishes

Probed 2026-08-20 by driving both front-ends in Playwright, capturing their network
calls, then GETting every discovered endpoint directly and recording shapes, row
counts and reconciliation. All 27 endpoints answered 200. Working document — not
committed.

Companion to `SOURCE-PARAM-SCOPE.md`.

---

## 0. Headlines

1. **There are two upstream hosts, not one, and they are not interchangeable.**
   `info.skyeco.com` is backed by `info-sky.blockanalitica.com/star-monitoring/` —
   the host STL reads today. `skyeco-finacial.vercel.app` is backed by
   `sky.data.blockanalitica.com/internal/` — the Host B feed pending sign-off in
   #729. The second is **substantially richer**.
2. **A proxy-scoped allocations endpoint exists** — `/internal/allocations/?prime=`
   — 59 rows for spark against the Star monitor's 12, carrying `wallet_address`
   (the ALM proxy). This dissolves the scope mismatch that made `both` awkward:
   it can be joined to STL's rows on `(chain, token address, proxy)` directly.
3. **The prime-level scalars decompose per allocation and reconcile to ~0%.**
   Exposure and RRC to −0.0002%; assets / allocated / idle to **0.0000%**. So D4 is
   yes, for four of six figures.
4. **Grove's reference data exists upstream and STL's 502 is our bug** — grove holds
   positions on `plume` and `robinhood`, and one unmappable network currently fails
   the entire list. See §6.
5. **Daily history exists** for the balance sheet (`/primes/{star}/historic/`, 31
   days) and for ASC (`/asc/history/`), but still **not** for risk capital — which
   matches what #729 already documents.
6. **The two hosts disagree slightly on the same figures** (spark exposure 2.1476bn
   vs 2.1238bn, ~1.1%; encumbrance 0.4042 vs 0.4010). Separately computed live
   snapshots, so a merged view must not present them as one number.

---

## 1. Host map

| Front-end | API host | Base path | STL status |
|---|---|---|---|
| `info.skyeco.com` | `info-sky.blockanalitica.com` | `/star-monitoring/` | **In use** — `SKY_RISK_CAPITAL_URL` |
| `skyeco-finacial.vercel.app` | `sky.data.blockanalitica.com` | `/internal/` | **Host B**, sign-off pending (#729) |

Both are unauthenticated, CORS-open, and answer JSON as `{success, status, data}`
(the ASC endpoints return bare arrays instead — noted per row below).
Paginated collections nest under `data.results`; the rest are `data` directly.

---

## 2. `sky.data.blockanalitica.com/internal/` — the richer host

### 2.1 Allocations — the important one

**`GET /internal/allocations/?prime={star}&limit=1000`** → 59 rows (spark), 41 (grove)

23 fields: `address`, `wallet_address`, `network`, `star`, `protocol`, `type`,
`allocation_type`, `token_symbol`, `token_name`, `assets`, `allocated_assets`,
`idle_assets`, `apy`, `shares`, `price`, `index`, `estimated_profit`, `extra_data`,
plus a `*_change` for each of the six numeric fields.

Why this matters:

- **`wallet_address` is the ALM proxy** — so rows are per-proxy, the same grain as
  STL's `allocation_position`. Spark's 59 rows split across 7 proxies exactly matching
  `alm_proxy_addresses`: ethereum 35, base 7, arbitrum 6, unichain 4, optimism 4,
  avalanche 2, robinhood 1.
- **`address` is the position token** and is present on 59/59 (42-char on 57; the two
  exceptions are Uniswap V4 pool ids).
- **`(network, address, wallet_address)` is unique — 59 distinct tuples for 59 rows.**
  A ready-made dedup key.
- **`allocation_type` maps onto STL's `category`** almost exactly: upstream
  `{allocation: 35, asset: 10, pol: 10, psm3: 4}` vs STL's
  `{allocation, asset, pol, psm3, custody}`.
- `type` is a finer position taxonomy STL has no equivalent for: `erc20`, `atoken`,
  `erc4626`, `psm3`, `proxy`, `curve`, `uni_v4`, `morphov2`, `superstate`, `buidl`,
  `centrifuge`, `anchorage`.

**`GET /internal/allocations/{token_address}/?wallet_address=&network=`** → one row,
same 23 fields. A per-position detail lookup.

**`GET /internal/allocations/?limit=1000`** → 110 rows, every prime at once.

**`GET /internal/allocations/savings-vaults/`** → 8 rows: `vault_address`,
`underlying_address`, `symbol`, `name`, `protocol`, `agent`, `network`, `date`,
`price`, `total_balance`, `idle_balance`.

### 2.2 Prime balance sheet

**`GET /internal/primes/`** → 11 primes. **`GET /internal/primes/{star}/`** → 32 fields.

Notable beyond the six #729 already ingests: **`alm_proxy_addresses`** (a JSON map of
network → proxy; spark lists 8 including `xlayer` and `robinhood`), `treasury_address`,
`ilk` / `ilks`, `networks`, `group`, plus `nav`, `aum`, `liabilities`,
`in_transit_assets`, `apy`, `estimated_profit` — all currently `null` for spark.

**`GET /internal/primes/{star}/historic/?days_ago=30`** → 31 daily rows, 15 fields:
`date`, `star`, `name`, `assets`, `allocated_assets`, `idle_assets`, `debt`,
`treasury_balance`, `backstop_capital`, and the five null-for-now fields above.
**This is the feed #729 uses.** `days_ago` is honoured.

**`GET /internal/primes/historic/?days_ago=30`** → 341 rows, all primes ✕ 31 days.
One call for every prime's history.

**`GET /internal/primes/{star}/balance-sheet/aggregates/?group_by=month`** → 923 rows:
`date`, `snapshot_date`, `uid`, `what` (e.g. `liabilities`), `balance`, `category`,
`category_name`, `name`, `symbol`, `protocol`, `network`. A categorised balance sheet
over time.

**`GET /internal/primes/{star}/ilks/`** → `ilk`, `slug`, `urn`, `debt`,
`alm_proxy_addresses`, `rate_limit_addresses`.

### 2.3 Events

**`GET /internal/primes/{star}/events/?page=&limit=&exclude_operations=`** — 19 fields
including `block_number`, `tx_hash`, `datetime`, `operation`, `event`, `amount`,
`assets_delta`, `sender`, `receiver`, `address`, `wallet_address`, `token_symbol`,
`underlying_symbol`, `network`, `order_index`. Paginated.

**`GET /internal/primes/{star}/urn-events/`** — vat-level: `ilk`, `urn`, `ink`, `art`,
`dink`, `dart`, `rate`, `debt`, `operation` (`Borrow`), `event` (`frob`).

These overlap STL's own activity feed, which is built from chain events — so they are
a cross-check, not a source (the on-chain rule in `AGENTS.md` applies).

### 2.4 Backing — no STL equivalent

**`GET /internal/backing/items/?prime={star}`** → 67 rows (also filterable by
`token_address`, `wallet_address`, `network` → 8 rows for spUSDS).

19 fields: `collateral_symbol`, `collateral_address`, `underlying_symbol`,
`underlying_address`, `token_address`, `wallet_address`, `borrow_amount`, `lt`,
`backed`, `backed_lt`, `backed_total`, `category` (`onchain_crypto_lending`),
`category_name`, `protocol`, `network`, `star`, `symbol`, `name`, `updated_at`.

What actually collateralises each position, with liquidation thresholds. Nothing in
STL models this.

### 2.5 Required risk capital (BA's copy)

**`GET /internal/rrc/primes/?limit=100`** → 4 stars: `star`, `exposure`, `total_rc`,
`financial_rrc`, `exposure_share`, `risk_tolerance_ratio`.

**`GET /internal/rrc/primes/{star}/`** → 5 fields: `total_exposure`, `total_rrc`,
`total_exposure_share`, `total_rc`, `encumbrance_ratio`.

**`GET /internal/rrc/primes/{star}/allocations/?limit=100`** → 13 rows (spark), 12
fields: `protocol`, `network`, `star`, `token_address`, **`wallet_address`**, `symbol`,
`name`, `loan_token_address`, `loan_token_symbol`, `exposure`, `rrc`, `crr`.

**This is a strict superset of the Star monitor's allocations endpoint** — same fields
plus `wallet_address`, and one extra row.

### 2.6 Supply

**`GET /internal/supply/non-circulating/?agent={star}`** → 13 rows: `network`,
`protocol`, `token_address`, `wallet_address`, `underlying_address`,
`underlying_symbol`, `idle`, `agent`, `name`.

---

## 3. `info-sky.blockanalitica.com/star-monitoring/` — the host STL reads

### 3.1 Risk capital

**`GET /risk-capital/primes/?order=-exposure`** → 4 stars, same six fields as BA's
`rrc/primes`.

**`GET /risk-capital/aggregates/`** → ecosystem totals: `total_exposure`, `total_rrc`,
`total_rc`, `total_exposure_share`, **`total_rtr`** (risk tolerance ratio, 0.4999).
Not currently read by STL, and the only place the ecosystem-wide RTR appears.

**`GET /risk-capital/primes/{star}/`** → 14 fields. **Richer than BA's 5** — adds the
junior/senior splits (`total_jrc`, `total_src`, `internal_jrc`, `external_jrc`,
`tokenized_jrc`, `internal_src`, `external_src`) and `epi_utilization` /
`spj_utilization`. This is what STL's reference risk-capital snapshot serves.

**`GET /risk-capital/primes/{star}/allocations/?limit=100`** → 12 rows (spark), 15
(grove). 11 fields — BA's 12 minus `wallet_address`.

### 3.2 Actively Stabilizing Collateral — entirely new to STL

**`GET /asc/`** → `total_asc`, `total_resting_asc`, `total_latent_asc`, `asc_share`,
`max_latent_asc`, `utilization_latent_asc`, plus `*_change`. Returns fields at the top
level, not under `data`.

**`GET /asc/history/?days_ago=30`** → bare array, 31 rows: `date`,
`total_resting_asc`, `total_latent_asc`. **History does exist here** — unlike risk
capital.

**`GET /asc/stars/?order=-total_asc`** → bare array, 7 rows: `star`, `resting_asc`,
`latent_asc`, `total_asc`, `asc_share`, `star_resting_share`, `star_latent_share`, `*_change`.

**`GET /asc/stars/{star}/`** → the same shape for one star.

**`GET /asc/stars/{star}/positions/?order=-total_asc`** → bare array: `asc_type`
(`resting` / `latent`), `network`, `source` (e.g. `LITE-PSM-USDC-A`), `asset_symbol`,
`underlying_address`, `underlying_symbol`, `total_asc`, `asc_change`.

---

## 4. Do the parts sum to the whole?

Measured for spark, 2026-08-20. This is what makes a per-allocation breakdown
trustworthy.

| Figure | Per-allocation source | Σ rows | Prime total | Δ |
|---|---|---|---|---|
| Exposure | `rrc/primes/spark/allocations` (13) | 2,147,562,385.89 | 2,147,563,280.49 | **−0.0000%** |
| Exposure | `risk-capital/primes/spark/allocations` (12) | 2,123,762,754.71 | 2,123,763,643.16 | **−0.0000%** |
| Required RC | `rrc/…/allocations` (13) | 19,460,345.24 | 19,460,375.95 | **−0.0002%** |
| Required RC | `risk-capital/…/allocations` (12) | 19,325,302.74 | 19,325,327.29 | **−0.0001%** |
| Assets | `allocations/?prime=spark` (59) | 3,292,566,032.55 | 3,292,566,032.55 | **0.0000%** |
| Allocated assets | same | 2,781,993,051.20 | 2,781,993,051.20 | **0.0000%** |
| Idle assets | same | 510,572,981.36 | 510,572,981.36 | **0.0000%** |

**Not decomposable:** `total_rc` — it is the treasury balance
(`total_rc == prime.treasury_balance == 48,142,491.09` exactly) and no per-allocation
`rc` column exists. `encumbrance_ratio` is `total_rrc / total_rc` (verified:
19,460,375.95 / 48,142,491.09 = 0.40422 = the published ratio), so its **numerator**
decomposes and its denominator does not — each position can be attributed a share of
encumbrance as `rrc_i / total_rc`, which is a meaningful breakdown even though the
ratio itself is prime-level.

---

## 5. How well would `both` actually join?

Spark, upstream `/internal/allocations/` vs STL staging indexed rows, keyed on
`(chain_id, token address)`:

| | rows |
|---|---|
| upstream total | 59 |
| upstream on chains STL indexes (1, 8453, 43114) | 44 |
| upstream elsewhere (arbitrum 6, unichain 4, optimism 4, robinhood 1) | 15 |
| STL indexed | 27 |
| **matched** | **25** |
| upstream-only (on indexed chains) | 19 |
| STL-only | 1 (`spWETH`) |
| union after dedup | 45 |

Against the Star monitor's prime-scoped list the same join matched only 6 of 12. So
**switching the reference side to the `/internal/allocations/` endpoint roughly
quadruples the match rate** and reduces STL-only rows to one.

The 19 upstream-only rows are genuinely unindexed position types, not key failures:
Superstate (`USCC`, `USTB`), Maple `syrupUSDT`, Arkis, two Uniswap V4 LPs, BlackRock
`BUIDL-I`, Centrifuge `JTRSY`, two Morpho v2 vaults, Anchorage, `PSM3` on base, and
plain token holdings (`DAI`, `USDT`, `USDC`, `USDS`, `sUSDS`).

Two traps worth pinning in tests:

- **`sparkUSDTbc` appears twice** on ethereum with different addresses
  (`0xb0c42411…`, `0xc7cdcfde…`) — two distinct Morpho v2 vaults sharing a symbol. Any
  symbol-keyed dedup merges them wrongly.
- **Anchorage** is `network=ethereum` with a token address upstream, but `chain_id=0`
  with a null address in STL. It needs a named special case either way.

---

## 6. Bug found: grove's reference data is dropped by STL, not missing upstream

STL returns **502 on all three grove proxies** for `?reference=true`. Upstream is fine:

| endpoint | grove |
|---|---|
| `risk-capital/primes/grove/` | 200 |
| `risk-capital/primes/grove/allocations/` | 200, **15 rows** |
| `rrc/primes/grove/allocations/` | 200, 16 rows |
| `allocations/?prime=grove` | 200, 41 rows |
| `primes/grove/historic/` | 200, 31 days |

Cause: grove holds positions on `plume` (1) and `robinhood` (1). STL's
`CHAIN_ID_TO_NAME` knows six chains and neither of those, so `_reference_allocation_row`
(`app/api/v1/allocations.py:765`) raises `ReferenceDataUnavailableError` for the
unmapped network, and the handler turns it into a 502 for **the whole list** — losing
the 13 rows on chains STL does know.

Spark is unaffected only because it happens to sit on ethereum and base.

This also contradicts the convention its own chain vocabulary states:
`app/domain/chain_names.py` says an unrecognised chain "must surface as a null field on
the response rather than a failed request".

**Recommendation:** serve the mappable rows and report the dropped ones — an
`unmapped_networks` field on the envelope, or rows with a null `chain_id` plus the
upstream network name. Either is compatible with the "no silent partial data" rule
because the omission is stated. Worth doing regardless of the `source` work; it is a
small fix that makes reference mode work for half the primes it currently fails.

---

## 7. What this unlocks beyond the current scope

Not proposals, just what is now known to be available:

- **Per-allocation exposure/RRC/CRR breakdown** with reconciliation (§4) — D4.
- **Collateral backing per position** with liquidation thresholds (§2.4).
- **ASC**, including 31 days of history (§3.2).
- **Upstream's own ALM proxy map** (§2.2) — a cross-check against the axis-synome
  contract and `allocation_position`, and the reason STL sees 3 of spark's 7 networks.
- **Categorised balance sheet over time** (§2.2).
- **Ecosystem risk-tolerance ratio** `total_rtr` (§3.1).
- **Savings-vault idle vs total balances** (§2.1).

Everything here is off-chain third-party data, so each would need the `AGENTS.md`
maintainer approval that #729's Host B note already covers for this host.

---

## 8. Reproducing this

Scripts in the session scratchpad:

- `probe_upstream.py` — GETs all 27 endpoints, writes `upstream_catalogue.json`
  (fields + a sample row each)
- `verify_decomposition.py` — the §4 reconciliation and §2.1 scoping
- `join_ba_allocations.py` — the §5 join against STL staging
- `grove_check.py` — the §6 per-star availability matrix

Nothing writes; all GETs. STL comparisons went through the staging Vite proxy on
`localhost:5475`.
