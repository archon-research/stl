# CORE model: parquet inputs vs what the database has

Tracks every gap between the static parquet snapshots and the live data in our
own tables — each entry says what is missing and what brings it back. Started
as a full inventory on **14 Aug 2026**; per-section UPDATE notes carry later
dates. As of **9 Sep 2026**, 8 of the 9 enabled markets run fully live
(SparkLend ×4, Morpho ×2, Syrup ×2); Anchorage stays on parquet. Re-verify any
quoted staging number before acting on it.

The model needs three inputs per market: borrower positions, daily price
history, and sell-side order book depth. Galaxy is excluded throughout — it is
explicitly disabled in `inputs/market_configs.json` (see `_galaxy_disabled`).

Collateral set required by the 9 enabled markets:
`BTC, CBBTC, EZETH, HYPE, LBTC, RETH, RSETH, TBTC, WBTC, WEETH, WETH, WSTETH, XRP`

---

## 1. Prices — RESOLVED for CORE by switching to `onchain_token_price` (17 Aug 2026)

**The CORE price reader now reads on-chain oracle prices, not the offchain
feed.** Checked against staging: all 10 SparkLend collaterals have 355 days of
gap-free daily closes in `onchain_token_price` **under the `sparklend` oracle**
(block-driven worker + the Erigon `oracle-pricing-backfill`; that pipeline
never had the outages below). The series is pinned to that one oracle
(`PostgresPriceReader(oracle_name=...)`): competing oracles hold separate rows
per token, and "newest block of the day" would hop between feeds from day to
day. Chainlink is indexed since 3 Feb 2026 only (186 days on 26 Aug 2026, just
above `TRAIN_SIZE`), so it is not a usable default yet.
BA's original used Yahoo Finance purely for convenience — the model's own
liquidation mechanics run on oracle prices, so calibrating on the oracle
series is more self-consistent, and it is the repo's preferred lineage.

Remaining price limits for CORE:
- History depth is ~1 year vs BA's 2014+ Yahoo series. TRAIN_SIZE (180) is
  satisfied; the volatility floor sees less history than BA's, making it
  somewhat less conservative. Deepening it = extending the Erigon backfill.
- BTC, HYPE, XRP, SOL, JITOSOL have no on-chain oracle on our chains. Solved
  per symbol (8–9 Sep 2026): HYPE and XRP read the CoinGecko series in the
  token-less `asset_price` table (UPDATE below), and native BTC/ETH ride the
  WBTC/WETH oracle series as proxies in the price reader. SOL/JITOSOL remain
  open — only disabled Galaxy would need them (§4).

### The offchain feed's own gaps (still real, no longer blocking CORE)

`offchain_token_price` keeps the holes found on 14 Aug — they matter to any
*other* consumer of that table, and to future CORE markets needing off-chain
assets. Prod is byte-identical to staging.

| Symbol | History starts | Days in last 180 | Status |
|---|---|---|---|
| WETH, WBTC | 2025-08-14 | 159 | gaps (below) |
| CBBTC, EZETH, LBTC, RETH, RSETH, TBTC, WEETH, WSTETH | 2026-03-17 | 129 | too short + same gaps |
| HYPE, XRP | 2026-01-01 | 180 | token-less rows in `asset_price`; backfilled in staging 8 Sep and prod 9 Sep |
| BTC | — | — | no `offchain_price_asset` row at all |

**Gaps** (shared by all assets — indexer downtime, not per-asset):
- 2026-04-18 .. 2026-04-19 (2 days)
- 2026-06-18 .. 2026-07-07 (20 days)

**What brings it back:**
- Gaps + short history → run `cmd/backfillers/offchain-price-backfill`.
  This is exactly what [VEC-540](https://linear.app/archontech/issue/VEC-540)
  built for WETH/WBTC (PRs #660/#664; operator guide:
  `docs/backfilling-offchain-prices.md`) — WETH/WBTC's longer history *is*
  that backfill; the other eight assets were simply never run.

  Ready-to-run inputs (Temporal UI, namespace `vector`, task queue
  `offchain-price-backfill`, type `OffchainPriceBackfill`; assets are
  CoinGecko ids; slice long ranges per the guide's perf warning):

  1. The eight never-backfilled assets, missing history:
     `{"assets":["coinbase-wrapped-btc","renzo-restaked-eth","lombard-staked-btc","rocket-pool-eth","kelp-dao-restaked-eth","tbtc","wrapped-eeth","wrapped-steth"],"from":"2025-08-01T00:00:00Z","to":"2026-03-18T00:00:00Z"}`
  2. All ten SparkLend collaterals, gap window 1:
     add `weth`,`wrapped-bitcoin` to the list, `"from":"2026-04-17…","to":"2026-04-20…"`
  3. All ten, gap window 2 (the June outage):
     same assets, `"from":"2026-06-17…","to":"2026-07-08…"`

  Repeat per environment (staging, then prod — identical gaps). Verification:
  flip `PRICE_SOURCE` on one market; the adapter's window validation is the
  check.
- BTC, HYPE, XRP → add `offchain_price_asset` rows. These have no mainnet
  ERC-20, so per the registry rules they get symbol-keyed rows with
  `token_id` NULL. CoinGecko ids: `bitcoin`, `hyperliquid`, `ripple`.
  Then backfill 180+ days.

  **UPDATE (8–9 Sep 2026):** DONE for HYPE and XRP in **both environments**:
  PR #858 created `asset_price` + the `ripple`/`hyperliquid` catalog rows, and
  workflow `backfill-xrp-hype-2026` backfilled hourly history from 2026-01-01
  (staging 8 Sep: 6,001 points each; prod 9 Sep after the #858 rollout: 6,025
  points each — `coveredFrom` = requested `from`, zero missing days, well
  above TRAIN_SIZE). The 5-minute sweep keeps both series current. BTC
  (`bitcoin`) remains unregistered — only Anchorage needs it, and it could
  instead reuse the BTC→WBTC proxy path the CORE price reader now has (see §3).

---

## 2. Order books — `cex_orderbook_snapshots`: RESOLVED for all 9 markets (9 Sep 2026)

Requirement per collateral: an aggregated sell-side book. Routing (confirmed
against BA's parquet — the LST books are the raw ETH book duplicated,
unscaled): ETH-group (WETH, WSTETH, WEETH, RETH, RSETH, EZETH) → ETH book;
BTC-group (BTC, WBTC, LBTC, TBTC, CBBTC) → BTC book; everything else direct.

| Book needed | Covers | Status |
|---|---|---|
| ETH | WETH + 5 LSTs | live in staging AND prod (Coinbase, OKX, Kraken) |
| BTC | BTC + 4 wrappers | live in staging AND prod (Coinbase, OKX, Kraken) |
| XRP | syrup_usdc, syrup_usdt | live in staging (25 Aug, ARCT-316/319/321) AND prod (9 Sep, #927) |
| HYPE | syrup_usdc | live in staging AND prod, same rollouts — the venues list HYPE spot now, contrary to the 14 Aug note |

The venue configs carry six products per venue (Coinbase ARCT-316 #750, OKX
ARCT-319 #767, Kraken ARCT-321 #769 — staging 25 Aug; prod mirrored verbatim
in #927, 9 Sep), each symbol verified against the live venue API first; an
entry a venue stops listing is skipped silently (ARCT-240), so drift thins a
book rather than crashing. SOL and JITOSOL flow too, ahead of any consumer
(future Galaxy). VEC-455 (deploy the indexer fleet to prod) closed 24 Aug;
new-symbol prod rollouts ride each venue task.

**Remaining:**
- Venue depth: we aggregate 3 venues vs BA's 11 → thinner books → higher
  modelled slippage → conservative CRR bias. Acceptable to start; revisit if
  CRR reconciliation against BA's dashboards shows material divergence. The
  live books also hold only the top 100 levels per venue side — README Known
  Issue #12 / [VEC-740](https://linear.app/archontech/issue/VEC-740).

---

## 3. Positions — per-protocol tables

**SparkLend (4 markets): done** behind `CORE_MODEL_POSITION_SOURCE=postgres`.
The reader builds the wide users frame from `borrower` /
`borrower_collateral` / `sparklend_reserve_data` / `token_price_current`
(positions via the trigger-fed `borrower_current` / `borrower_collateral_current`
caches, because the histories tier year-old chunks to S3 that a plain session
cannot see; Morpho has no such cache yet and enables tiered reads instead),
validated against staging: the per-user borrow sum matches the reserve-level
total debt within 0.6% (interest accrual since each user's last event), and a
full CRR computed end to end on the live frame.

**Valuation oracle (25 Aug 2026):** positions are valued with the protocol's
own oracle, resolved through `protocol_oracle` and read from
`token_price_current` by token id — SparkLend with `sparklend` (the Aave-style
oracle that triggers its liquidations), Morpho with `chainlink`, because Blue's
per-market oracle contracts are not indexed (a small deviation: Chainlink USD
feeds instead of each market's own oracle). Before this the reader took the
newest row across *every* oracle, so the price source of a run was arbitrary
(staging: WBTC via Chainlink at 79037 while SparkLend's oracle said 79012).
Freshness is checked per feed, not per token: the oracle worker writes a row
only when a price changes, so SparkLend's fixed $1 stables last wrote at the
worker's restart 42 days earlier while the feed as a whole ticks every block.
A per-token age bound would fail every SparkLend run on those tokens.

Known deviations from BA's snapshot semantics (all conservative or negligible;
also documented in the reader module):

- **e-mode is not indexed**: reserve-level LT/bonus are used for every user,
  `emode_category` is always 0. E-mode users' HF is understated → the model
  over-liquidates them → CRR biased up, not down.
- **Interest accrual**: a user's debt is as of their last on-chain event, so
  long-idle debts are slightly understated (the 0.6% above).
- **Zero-collateral borrowers are excluded** (logged with the dropped USD
  total). They are existing bad debt, not simulatable future liquidations,
  and they NaN-poison the CRR if kept ($34 total when measured).

**Morpho (2 markets): done** behind the same per-market flags (18 Aug 2026).
`morpho_market_position` holds exactly what the model wants — all borrowers of
a collateral/loan pair — so the receipt-token n:m mismatch does not apply to
the model input. All LLTV tranches of the pair are included; the LIF is Morpho
Blue's closed formula (pinned against BA's own parquet value). Validated the
same way as SparkLend: per-user borrow sum vs the contract-level market total
= 1.3% apart (interest accrual), fully-live CRR computed for both markets.

**Scope change, deliberate:** live Morpho is **Ethereum mainnet** (342 cbBTC
borrowers, $275M). BA's parquet snapshots were built from **Base**'s markets
(19,804 borrowers, $1.0B — their README routes cbBTC liquidity via a Base
pool). Live CRRs will not reconcile with parquet-era CRRs for these keys:
different borrower universe, not a data bug. Modelling Base needs Base Morpho
market indexing (we index Base Morpho *vault receipts*, not market positions).

Measured (25 Aug 2026, N_MC=100, SEED=0, live vs parquet on identical code; predates the
live order-book side and sort fix in #891, so later live runs are not comparable to these
figures, and live books are top-100 levels only, see README Known Issue #12 / VEC-740):
cbBTC/USDC 0.398% → 0.010%, WETH/USDC 3.548% → 0.362%. The live CRRs are
*lower* despite Base being the bigger market, and that is expected: CRR is
expected loss **per borrowed dollar**, so market size alone does not raise it.
Two drivers, both visible in the run's LTV-bucket printout: (1) the Ethereum
borrowers run far lower LTVs — cbBTC/USDC has 89.6% of live borrow below 50%
LTV and ~0% above 70%, where Base's parquet book has ~39% above 60% LTV — so
the same price shock liquidates a much smaller share; (2) slippage is
non-linear in liquidated size, so selling slices of a $1.0B book walks deeper
into the order books than slices of a $275M one, raising the *percentage*
loss too. Same-code parquet reruns reproduce the stored parquet CRRs, so the
gap is entirely the borrower universe, not the code.

**Syrup (2 markets): done (8–9 Sep 2026)**, behind the same per-market flags —
flipped, see the go-live note below. One row per external Active
loan of the pool's current sync cycle (the pool-cycle anchor and the
same-`(synced_at, processing_version)` collateral join are lifted from the
Maple backed-breakdown repository; the indexer emits no tombstones, so a
repaid loan's last Active state lingers in older cycles forever). LT is the
inverse of Maple's margin-call coverage trigger
(`maple_loan_collateral.liquidation_level`, ×1e6): level 1204800 → LT
0.830013, the parquet's own 0.83001. Collateral is valued with Maple's
attested per-unit prices (`asset_value_usd`, ×1e8) — the protocol's own
valuation, and exactly what BA's parquet used (HYPE 16,305,615 / 400,000.058
= 40.764 = its market-frame price). `liquidation_incentive` is BA's flat
1.02. Unit pinning: a synthetic replay of the parquet's 0x198aec… row
reproduces lltv/ltv/HF to 1e-5 (unit test); against staging, per-loan
computed coverage matches Maple's own `acm_ratio` to rounding (505 ×
78,276.425 / 25M = 1.58118 vs acm 1.581184), with a >1% disagreement logged.
Validated live against staging (8 Sep): 27 USDC + 8 USDT loans, all modeled
collaterals priced (BTC, WBTC, ETH, HYPE, XRP), every HF > 1.

Known deviations (also in the reader's module docstring):

- **Stable-on-stable loans are excluded** (trigger at/above par coverage,
  `liquidation_level` ≤ 1e6 → LT ≥ 1): the protocol margin-calls them at/above
  full coverage, so collateral price is not what protects them — no
  simulatable price-liquidation mechanism, and LT ≥ 1 would break the
  liquidator's `-1 + LT×(1+bonus) < 0` guard. BA's frames contain no such
  rows (they post-date the snapshot). Their debt would add exposure with
  ~zero simulated loss, so exclusion biases CRR up. Staging, 8 Sep: $17.5M
  (3 loans) in syrup_usdc, $52.4M (5 loans) in syrup_usdt.
- **No pool-level debt reconciliation**: `maple_pool_state.principal_out`
  includes the internal (amm/strategy) loans the model excludes (~700M of the
  USDC pool's 958M), so the external-loan sum cannot be reconciled against
  it. The per-loan `acm_ratio` cross-check above is the reconciliation.
- BA's `interest_rate` / `loan_token_symbol` / `collateral_token_symbol`
  parquet columns are skipped — nothing in the model reads them.

**Syrup go-live (9 Sep 2026):** all three `*_SOURCE` flags flipped to
`postgres` in `market_configs.json`. The price reader gained the two missing
paths: token-less symbols (XRP, HYPE) read the CoinGecko series in
`asset_price` (backfilled in staging, see §1), and native BTC/ETH proxy the
WBTC/WETH oracle series (approved in the 17 Aug #at_stl thread; a market
holding both BTC and WBTC gets two identical columns, which the copula's
eigenvalue flooring absorbs). The order-book reader gained the XRP and HYPE
venue books. Fully-live CRRs computed end to end against staging (9 Sep,
N_MC=100, SEED=0): syrup_usdc crr_el 1.75%, syrup_usdt 2.77% (parquet
same-code baselines: 6.04% / 9.77% — not expected to reconcile, the live
borrower book and price regime differ from BA's June snapshot, same as the
Morpho scope note above).

**Prod data caught up on 9 Sep 2026**, so no environment pin ships: the
XRP/HYPE backfill ran in prod (`backfill-xrp-hype-2026`, 6,025 hourly points
each from 2026-01-01, zero holes) and the prod order-book configmaps gained
the staging symbol set (PR #927, indexers restarted 09:06 UTC). Both
environments go live at their first tick carrying this config. Should an
environment ever need to lag again, the per-market env override exists for
exactly that: `CORE_MODEL_<MARKET>_<KEY>=parquet` in that overlay's
`core-model-runner` configmap (most specific wins; `-` in a market key maps
to `_`).

Still parquet:

| Market group | Live source | Notes |
|---|---|---|
| Anchorage | anchorage-indexer tables | indexed; reader unwritten, and it needs a BTC price series — the price reader's BTC→WBTC proxy path now covers that |

---

## 4. Galaxy (parked separately)

Disabled in `market_configs.json`. Checked against staging on 14 Aug 2026:
**nothing is available** — no `%galaxy%` table exists and there is no Galaxy
`protocol` row. [VEC-79](https://linear.app/archontech/issue/VEC-79) (Track
Galaxy position data, In Review since 27 May) only landed its part-1, the
DEX-indexing preparation ([PR #345](https://github.com/archon-research/stl/pull/345));
the position ingestion itself was never built, and the ticket's data-source
investigation is still open.

To re-enable, Galaxy needs all of:
- **Positions**: an ingestion pipeline (VEC-79 proper). Off-chain CLO data,
  so it needs the maintainer-approval step from CONTRIBUTING §5.
- **Order books**: covered — BTC/ETH/XRP/SOL/JITOSOL flow from all three
  venues in staging AND prod (see §2). BA never shipped the ETH/SOL/JITOSOL
  parquet books, so before this there was no parquet fallback either.
- **Prices**: SOL/JITOSOL token-less rows in `offchain_price_asset` plus an
  `asset_price` backfill — the exact path XRP/HYPE took (§1); XRP is already
  done.

---

## SparkLend go-live: DONE (17 Aug 2026)

All four SparkLend markets are flipped to live sources in
`market_configs.json` (positions from the borrower tables, prices from
`onchain_token_price`, books from `cex_orderbook_snapshots`). No backfill was
needed — the price switch to on-chain oracles removed that dependency.
Verified end to end against staging: `sparklend_usdt` fully live computed
CRR 0.89% at N_MC=50.

Local kind has no indexed data, so its `core-model-runner` Deployment carries
the global env overrides (`CORE_MODEL_*_SOURCE=parquet`) in the dev overlay —
local runs stay on the parquet snapshots. Non-SparkLend markets keep parquet
everywhere; the daily "all" tick keeps succeeding.

## Order of re-enablement (cheapest first)

Written when everything was parquet; kept as the ledger of how the plan
resolved. 8 of the 9 enabled markets are live as of 9 Sep 2026.

1. ~~Offchain price gap backfills~~ — superseded for CORE by the switch to
   `onchain_token_price` (§1); the `offchain_token_price` holes remain for
   other consumers of that table.
2. ~~HYPE/XRP price asset rows + backfill~~ — **done 8–9 Sep** (§1, via the
   new `asset_price` table). BTC never got a row; Anchorage can use the
   BTC→WBTC proxy path instead.
3. ~~Orderbook adapter switch~~ — **done**; `params.ORDERBOOK_SOURCE` on every
   result row says which books produced it.
4. ~~XRP/HYPE/SOL/JITOSOL orderbook symbols~~ — **done in staging 25 Aug and
   prod 9 Sep** (§2).
5. ~~Positions adapters per protocol~~ — **done** for SparkLend, Morpho and
   Syrup (§3). Anchorage stays parquet (its reader is unwritten; smallest
   remaining piece once the BTC price question is settled).
6. Galaxy inputs (new sources — separate decision, §4). Still open.
