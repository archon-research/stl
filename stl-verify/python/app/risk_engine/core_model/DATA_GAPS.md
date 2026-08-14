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

## 1. Prices — `offchain_token_price` (staging, checked 14 Aug 2026)

Requirement: ≥180 daily closes per collateral (TRAIN_SIZE). More is better:
the volatility floor uses the full history, and BA's parquet went back to 2014.

| Symbol | History starts | Days in last 180 | Status |
|---|---|---|---|
| WETH, WBTC | 2025-08-14 | 159 | gaps (below) |
| CBBTC, EZETH, LBTC, RETH, RSETH, TBTC, WEETH, WSTETH | 2026-03-17 | 129 | too short + same gaps |
| BTC, HYPE, XRP | — | — | no `offchain_price_asset` row at all |

**Gaps** (shared by all assets — indexer downtime, not per-asset):
- 2026-04-18 .. 2026-04-19 (2 days)
- 2026-06-18 .. 2026-07-07 (20 days)

**What brings it back:**
- Gaps + short history → run `cmd/backfillers/offchain-price-backfill`
  (on-demand Temporal worker; trigger from the Temporal UI with the date
  range as workflow input). One run per asset/range.
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
| ETH | WETH + 5 LSTs | live (Coinbase, OKX, Kraken) |
| BTC | BTC + 4 wrappers | live (Coinbase, OKX, Kraken) |
| XRP | syrup_usdc, syrup_usdt | **not tracked** — deliberately deferred (Pablo, 14 Aug 2026: "no xrp for now") |
| HYPE | syrup_usdc | **not trackable on our venues** — BA sourced it from HyperLiquid's native book |

So order books fully cover 7 of 9 enabled markets (4 SparkLend, 2 Morpho,
Anchorage). The 2 Syrup markets stay on parquet books until XRP/HYPE exist.

**What brings it back:**
- XRP → add the pair to the three venue configmaps
  (`k8s/overlays/{staging,prod}/configmaps.yaml`, `SYMBOLS`). Tracked as
  [VEC-458](https://linear.app/archontech/issue/VEC-458). Coinbase/Kraken/OKX
  all carry XRP spot.
- HYPE → needs a HyperLiquid source (new feed adapter), or accept a thinner
  proxy. Separate decision.
- Venue depth: we aggregate 3 venues vs BA's 11 → thinner books → higher
  modelled slippage → conservative CRR bias. Acceptable to start; revisit if
  CRR reconciliation against BA's dashboards shows material divergence.

---

## 3. Positions — per-protocol tables

Not yet swapped; all markets read `users_*.parquet` / `market_*.parquet`.
Status of the underlying live data:

| Market group | Live source | Notes |
|---|---|---|
| SparkLend (4) | sparklend indexer tables | indexed; adapter not written |
| Morpho (2) | morpho indexer tables | indexed; note the model wants *all borrowers of a Blue market*, not vault positions |
| Syrup (2) | maple-graphql-indexer tables | indexed |
| Anchorage | anchorage-indexer tables | indexed |

**What brings it back:** a Postgres `get_protocol_data` per protocol,
replacing the parquet branch in the reader, one market group at a time.

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
- **Order books**: BTC and ETH are already covered by our live books; XRP is
  deferred (see §2); SOL needs a `SYMBOLS` addition on our venues; JITOSOL
  needs a DEX source. BA never shipped the ETH/SOL/JITOSOL parquet books, so
  there is no parquet fallback either — the market cannot run at all today.
- **Prices**: SOL/JITOSOL/XRP rows in `offchain_price_asset` plus backfill.

---

## Order of re-enablement (cheapest first)

1. Backfill the two price gaps + extend the 129-day assets (backfiller run, no code).
2. Add BTC/HYPE/XRP price asset rows (small migration) + backfill.
3. Orderbook adapter switch for the 7 covered markets — **done**: set
   `CORE_MODEL_ORDERBOOK_SOURCE=postgres` (default stays `parquet`). The
   stored `params.ORDERBOOK_SOURCE` says which books produced each row.
4. XRP orderbook symbol (config PR — deferred by decision, see above).
5. Positions adapters per protocol (code, largest piece).
6. HYPE book, Galaxy inputs (new sources — separate decisions).
