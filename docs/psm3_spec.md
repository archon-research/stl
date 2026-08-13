# Spark PSM3 Specification

**Version:** 1.0
**Last Updated:** June 2026
**Purpose:** Condensed technical reference for reading reserve composition, valuation, and the prime's position from Spark's PSM3 on L2s.

---

## Protocol Overview

PSM3 (Peg Stability Module v3) is the L2 leg of the **Spark Liquidity Layer**. One contract per chain holds three assets — **USDS, sUSDS, USDC** — and lets anyone swap between them with **no fees**. It is also an LP pool: depositors mint internal shares. In practice the only meaningful LP is Sky's **ALM proxy** (the Spark prime), which holds ~100% of shares on every chain (the deploy script seeds 1 USDC to `address(0)`, locking the first ~1e18 share-units forever — see [PSM3Deploy.sol](https://github.com/sparkdotfi/spark-psm/blob/master/deploy/PSM3Deploy.sol)).

**Not on mainnet.** Deployed on Base, Optimism, Arbitrum, Unichain only (mainnet uses LitePSM instead). Addresses differ per chain.

| Chain (id) | PSM3 | ALM proxy (prime) |
|---|---|---|
| base (8453) | `0x1601843c5E9bC251A3272907010AFa41Fa18347E` | `0x2917956eFF0B5eaF030abDB4EF4296DF775009cA` |
| optimism (10) | `0xe0F9978b907853F354d79188A3dEfbD41978af62` | `0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB` |
| arbitrum (42161) | `0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266` | `0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709` |
| unichain (130) | `0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f` | `0x345E368fcCd62266B3f5F37C9a131FD1c39f5869` |

Token addresses per chain: [`sparkdotfi/spark-address-registry`](https://github.com/sparkdotfi/spark-address-registry/tree/master/src) (`Base.sol`/`Optimism.sol`/`Arbitrum.sol`/`Unichain.sol`) or our axis-synome entries (`protocol = psm3`).

---

## Core Concepts & Mechanics

- **Three reserves, no fees.** USDS and USDC are valued **1:1**. sUSDS is valued via an on-chain **conversion rate** (the cross-chain Sky Savings Rate oracle): `1 sUSDS = rate USDS`, `rate` in 1e27 (RAY), monotonic, ~1.05–1.10 and rising. (Source: [ChainSecurity 2024-10-22 audit](https://github.com/sparkdotfi/spark-audits/blob/master/md/spark-psm/20241022-chainsecurity-audit.md) §2.2; [SSR oracle audit](https://github.com/sparkdotfi/spark-audits/blob/master/md/xchain-ssr-oracle/) — `chi` is the conversion rate in ray.)
- **Not ERC-4626 / ERC-7540.** Shares live in an internal `shares(address)` mapping — there is no share token and no standard vault surface. Generic vault readers do not apply. (Audit §2.2: "shares are purely for internal accounting and non-transferable"; [IPSM3.sol](https://github.com/sparkdotfi/spark-psm/blob/master/src/interfaces/IPSM3.sol) exposes `shares(address)` only, no transfer/approve.)
- **The USDC "pocket".** USDC may be held at a governance-settable `pocket()` address (emits `PocketSet`). Today `pocket == PSM3` on all four chains (default per [Cantina 2024-10-23 audit](https://github.com/sparkdotfi/spark-audits/blob/master/md/spark-psm/20241023-cantina-audit.md) §3.1.2: "default is set to the PSM address (`address(this)`)"), so `USDC.balanceOf(PSM3)` happens to work — but it breaks **silently** if a pocket is ever set. Always read `USDC.balanceOf(pocket())`, resolved fresh (never cached). `rateProvider`/`usds`/`susds`/`usdc` are `immutable` in [PSM3.sol](https://github.com/sparkdotfi/spark-psm/blob/master/src/PSM3.sol); `pocket` is plain storage with `setPocket(newPocket)` (owner-only).
- **`totalAssets()` is PAR valuation:** `usds + usdc + susds × rate` (no market prices). Do not expect it to equal a market-priced sum of reserves; never recompute it from USD columns. Verbatim from [PSM3.sol](https://github.com/sparkdotfi/spark-psm/blob/master/src/PSM3.sol):
  ```solidity
  return _getUsdcValue(usdc.balanceOf(pocket))
       + _getUsdsValue(usds.balanceOf(address(this)))
       + _getSUsdsValue(susds.balanceOf(address(this)), false);
  ```
- **Events are not a complete delta stream.** `Deposit`/`Withdraw`/`Swap` cover protocol actions, but direct ERC-20 transfers to the contract change balances with no event. For exact balance history, index the three tokens' ERC-20 `Transfer` logs filtered on the PSM3/pocket address.

**Decimals:** USDS 1e18, sUSDS 1e18, USDC 1e6, `totalAssets` 1e18, conversion rate 1e27.

---

## Retrieving Key Data

All reads should be **pinned to one block** (multicall) so the snapshot is internally consistent.

**Per-block reserve snapshot (raw integers):**
```
pocket        = PSM3.pocket()
usds_balance  = USDS.balanceOf(PSM3)          # 1e18
susds_balance = sUSDS.balanceOf(PSM3)         # 1e18
usdc_balance  = USDC.balanceOf(pocket)        # 1e6  — note: at pocket, not PSM3
total_assets  = PSM3.totalAssets()            # 1e18, par valuation
rate          = PSM3.rateProvider().getConversionRate()   # 1e27 (SSR chi)
```

**Prime position (share of pool):**
```
prime_shares = PSM3.shares(ALM_PROXY)
total_shares = PSM3.totalShares()
prime_fraction = prime_shares / total_shares   # ~1.0 on every chain
```

**USD valuation (do this off-chain, e.g. in the Python API — Go stores raw only):**
```
usds_price  = <market price, fallback 1.0>     # e.g. Chainlink/Coingecko
usdc_price  = <market price, fallback 1.0>
susds_price = (rate / 1e27) × usds_price        # exact; never fetched directly
*_balance_usd = (balance / 10^dec) × *_price
total_assets_usd = total_assets / 1e18          # serve par value; do NOT sum the *_usd columns
```

**TVL** = `total_assets` (par) or, if you want market-priced TVL, `Σ (balance × market_price)` — the two differ during a depeg.

---

## Smart Contract Methods

View methods (all on the PSM3 contract unless noted):

| Method | Returns | Use |
|---|---|---|
| `usds()` / `susds()` / `usdc()` | `address` | reserve token addresses (immutable; cross-check vs config) |
| `pocket()` | `address` | where USDC is held (settable) |
| `rateProvider()` | `address` | SSR oracle (immutable) |
| `<rateProvider>.getConversionRate()` | `uint256` (1e27) | USDS per sUSDS |
| `totalAssets()` | `uint256` (1e18) | par valuation `usds + usdc + susds×rate` |
| `totalShares()` | `uint256` | total LP shares |
| `shares(address)` | `uint256` | LP shares held by an account (e.g. ALM proxy) |
| `convertToAssetValue(uint256)` | `uint256` | shares → USDS-denominated par value |
| `convertToShares` / `convertToAssets` | `uint256` | share/asset conversions |
| `previewSwapExactIn` / `previewSwapExactOut` | `uint256` | quote a swap |
| `ERC20.balanceOf(holder)` | `uint256` | raw reserve balances (USDC: holder = `pocket()`) |

Events (verbatim from [IPSM3.sol](https://github.com/sparkdotfi/spark-psm/blob/master/src/interfaces/IPSM3.sol) — note `asset` is the **first** indexed topic on Deposit/Withdraw, not last):

```solidity
event Deposit (address indexed asset,   address indexed user,    address indexed receiver, uint256 assetsDeposited, uint256 sharesMinted);
event Withdraw(address indexed asset,   address indexed user,    address indexed receiver, uint256 assetsWithdrawn, uint256 sharesBurned);
event Swap    (address indexed assetIn, address indexed assetOut, address sender, address indexed receiver, uint256 amountIn, uint256 amountOut, uint256 referralCode);
event PocketSet(address indexed oldPocket, address indexed newPocket, uint256 amountTransferred);
```

---

## Notes for This Repo

- Ingested by the **psm3-indexer** (`cmd/workers/psm3-indexer`, service `internal/services/psm3`): per-chain SQS block events → sweep every N blocks → append-only `psm3_reserves` rows (raw integers + `conversion_rate`). USD math lives in the Python API.
- USD pricing reuses the existing coingecko table (`offchain_token_price`) — `usds`, `susds`, `usd-coin` are already seeded; see [prices_oracles_reference.md](prices_oracles_reference.md).
- Append-only and pinned-per-block by design (see [data_entities.md](data_entities.md)); an external reference API samples the same fields but is ~10 min stale and not block-pinned.

---

## References

- **Spark PSM source:** [`sparkdotfi/spark-psm`](https://github.com/sparkdotfi/spark-psm) — [PSM3.sol](https://github.com/sparkdotfi/spark-psm/blob/master/src/PSM3.sol), [IPSM3.sol](https://github.com/sparkdotfi/spark-psm/blob/master/src/interfaces/IPSM3.sol), [PSM3Deploy.sol](https://github.com/sparkdotfi/spark-psm/blob/master/deploy/PSM3Deploy.sol)
- **Spark address registry:** [`sparkdotfi/spark-address-registry`](https://github.com/sparkdotfi/spark-address-registry) — per-chain files in [`src/`](https://github.com/sparkdotfi/spark-address-registry/tree/master/src)
- **PSM3 audits** (`sparkdotfi/spark-audits` → `md/spark-psm/`):
  - [ChainSecurity 2024-10-22](https://github.com/sparkdotfi/spark-audits/blob/master/md/spark-psm/20241022-chainsecurity-audit.md) — pocket integration, totalAssets formula
  - [Cantina 2024-10-23](https://github.com/sparkdotfi/spark-audits/blob/master/md/spark-psm/20241023-cantina-audit.md) — pocket share-price manipulation risk
  - [Cantina 2024-09-09](https://github.com/sparkdotfi/spark-audits/blob/master/md/spark-psm/20240909-cantina-audit.md) — deposit/withdraw share-price invariants (§3.2.1), asset0/asset1 depeg risk (§3.3.4)
- **XChain SSR Oracle audits:** [`sparkdotfi/spark-audits/md/xchain-ssr-oracle/`](https://github.com/sparkdotfi/spark-audits/tree/master/md/xchain-ssr-oracle) — defines `getConversionRate()` and the chi/ray semantics
- **Atlas governance scope:** Sky Atlas `A.3.3.2.2.1.1` and `A.6.1.1.1.2.6.1` (Spark Liquidity Layer) — confirms L2-only deployment list
- **Sky Savings Rate / sUSDS:** https://docs.spark.fi/
- **Spark Liquidity Layer dashboard:** https://data.spark.fi/spark-liquidity-layer
