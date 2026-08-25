# CORE model: parquet inputs vs what the database has

Tracks every gap between the static parquet snapshots the model currently
reads and the live data available in our own tables. Each entry says what is
missing and what brings it back. Re-verify the staging numbers before acting;
they are a snapshot from **14 Aug 2026**.

The model needs three inputs per market: borrower positions, daily price
history, and sell-side order book depth. Galaxy is excluded throughout — it is
explicitly disabled in `inputs/market_configs.json` (see `_galaxy_disabled`).

Collateral set required by the 9 enabled markets:
`BTC, CBBTC, EZETH, HYPE, LBTC, RETH, RSETH, TBTC, WBTC, WEETH, WETH, WSTETH, XRP`

---

## 1. Prices — RESOLVED for CORE by switching to `onchain_token_price` (17 Aug 2026)

**The CORE price reader now reads on-chain oracle prices, not the offchain
feed.** Checked against staging: all 10 SparkLend collaterals have 355 days of
gap-free daily closes in `onchain_token_price` (block-driven worker + the
Erigon `oracle-pricing-backfill`; that pipeline never had the outages below).
BA's original used Yahoo Finance purely for convenience — the model's own
liquidation mechanics run on oracle prices, so calibrating on the oracle
series is more self-consistent, and it is the repo's preferred lineage.

Remaining price limits for CORE:
- History depth is ~1 year vs BA's 2014+ Yahoo series. TRAIN_SIZE (180) is
  satisfied; the volatility floor sees less history than BA's, making it
  somewhat less conservative. Deepening it = extending the Erigon backfill.
- BTC, HYPE, XRP, SOL, JITOSOL have no on-chain oracle on our chains — the
  non-SparkLend markets that need them still need the offchain feed (below).

### The offchain feed's own gaps (still real, no longer blocking CORE)

`offchain_token_price` keeps the holes found on 14 Aug — they matter to any
*other* consumer of that table, and to future CORE markets needing off-chain
assets. Prod is byte-identical to staging.

| Symbol | History starts | Days in last 180 | Status |
|---|---|---|---|
| WETH, WBTC | 2025-08-14 | 159 | gaps (below) |
| CBBTC, EZETH, LBTC, RETH, RSETH, TBTC, WEETH, WSTETH | 2026-03-17 | 129 | too short + same gaps |
| BTC, HYPE, XRP | — | — | no `offchain_price_asset` row at all |

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

---

## 2. Order books — `cex_orderbook_snapshots` (staging)

Requirement per collateral: an aggregated sell-side book. Routing (confirmed
against BA's parquet — the LST books are the raw ETH book duplicated,
unscaled): ETH-group (WETH, WSTETH, WEETH, RETH, RSETH, EZETH) → ETH book;
BTC-group (BTC, WBTC, LBTC, TBTC, CBBTC) → BTC book; everything else direct.

| Book needed | Covers | Status in staging |
|---|---|---|
| ETH | WETH + 5 LSTs | live in staging AND prod (Coinbase, OKX, Kraken) |
| BTC | BTC + 4 wrappers | live in staging AND prod (Coinbase, OKX, Kraken) |
| XRP | syrup_usdc, syrup_usdt | **live in staging** since ARCT-316/319/321 (25 Aug 2026); prod still BTC/ETH only |
| HYPE | syrup_usdc | **live in staging** — the venues list HYPE spot now, contrary to the 14 Aug note; prod still BTC/ETH only |

**UPDATE (25 Aug 2026):** the staging venue configs were expanded after the
14 Aug snapshot — Coinbase to six products (ARCT-316, #750), OKX to six
instruments (ARCT-319, #767), Kraken to seven pairs (ARCT-321, #769), each
symbol verified against the live venue API first (unknown symbols are skipped
silently; ARCT-240). Verified against staging on 25 Aug: XRP, HYPE, SOL and
JITOSOL snapshots flowing from all three venues, latest under a minute old.
**Prod configmaps still index only BTC/ETH** — mirror the staging expansion
before flipping any of these markets live in prod.

So order books now cover **all 9 enabled markets in staging**. The 2 Syrup
markets remain on parquet only because of positions + prices (see §1 and §3),
no longer because of books.

Note: prod has all six venue books flowing (verified on the replica,
14 Aug 2026) even though [VEC-455](https://linear.app/archontech/issue/VEC-455)
(deploy to prod) is still marked Backlog — the ticket lags reality.

**Remaining:**
- Prod symbol expansion (see the update above).
- Venue depth: we aggregate 3 venues vs BA's 11 → thinner books → higher
  modelled slippage → conservative CRR bias. Acceptable to start; revisit if
  CRR reconciliation against BA's dashboards shows material divergence.

---

## 3. Positions — per-protocol tables

**SparkLend (4 markets): done** behind `CORE_MODEL_POSITION_SOURCE=postgres`.
The reader builds the wide users frame from `borrower` /
`borrower_collateral` / `sparklend_reserve_data` / `onchain_token_price`,
validated against staging: the per-user borrow sum matches the reserve-level
total debt within 0.6% (interest accrual since each user's last event), and a
full CRR computed end to end on the live frame.

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

Still parquet:

| Market group | Live source | Notes |
|---|---|---|
| Syrup (2) | maple-graphql-indexer tables | indexed, reader not written; also blocked on XRP/HYPE offchain prices (books solved in staging, 25 Aug 2026 — see §2) |
| Anchorage | anchorage-indexer tables | indexed; blocked on native-BTC price (no on-chain oracle) |

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
- **Order books**: covered in staging as of 25 Aug 2026 — BTC/ETH were
  already live, and XRP, SOL and JITOSOL now flow from all three venues
  (see the §2 update; prod still needs the symbol expansion). BA never
  shipped the ETH/SOL/JITOSOL parquet books, so before this there was no
  parquet fallback either.
- **Prices**: SOL/JITOSOL/XRP rows in `offchain_price_asset` plus backfill.

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

1. Backfill the two price gaps + extend the 129-day assets (backfiller run, no
   code). The price adapter is already merged behind
   `CORE_MODEL_PRICE_SOURCE=postgres`; it validates the window per symbol and
   fails with the exact missing days until the backfill lands, so flipping the
   flag is also the check that the backfill worked.
2. Add BTC/HYPE/XRP price asset rows (small migration) + backfill.
3. Orderbook adapter switch for the 7 covered markets — **done**: set
   `CORE_MODEL_ORDERBOOK_SOURCE=postgres` (default stays `parquet`). The
   stored `params.ORDERBOOK_SOURCE` says which books produced each row.
4. XRP/HYPE/SOL/JITOSOL orderbook symbols — **done in staging** (25 Aug 2026,
   ARCT-316/319/321); prod configmaps still need the same expansion.
5. Positions adapters per protocol (code, largest piece). SparkLend and
   Morpho are done; Syrup (the `maple_*` tables — indexed, reader not
   written) is the remaining one.
6. Galaxy inputs (new sources — separate decision).
