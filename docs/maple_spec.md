# Maple Finance Protocol Specification

**Version:** 1.2
**Last Updated:** June 2026
**Purpose:** Technical reference for understanding Maple Finance protocol mechanics and data retrieval

> **Note:** GraphQL introspection on the Maple API is disabled (Apollo `INTROSPECTION_DISABLED`), but the full schema (SDL, ~19,400 lines) is published to [Apollo Studio](https://studio.apollographql.com/public/maple-api/variant/mainnet/schema/reference) and fetchable unauthenticated via the Apollo platform API. Two caveats keep live verification relevant: the public SDL strips `@auth` directive applications, so field-level auth gating is only discoverable by execution; and schema-vs-runtime encoding drift exists (the schema says `String`, but some fields arrive as JSON numbers). Re-verify queries against the live API before relying on this document.

---

## Table of Contents

1. [Protocol Overview](#protocol-overview)
2. [Core Concepts & Mechanics](#core-concepts--mechanics)
3. [Retrieving Key Data](#retrieving-key-data)
4. [Smart Contracts & Addresses](#smart-contracts--addresses)
5. [References](#references)

---

## Protocol Overview

### What is Maple Finance?

Maple Finance is a **digital asset lending platform** that provides institutional lending on the blockchain. Founded in 2019, Maple combines traditional credit underwriting with transparent on-chain execution.

**Key Features:**
- **Overcollateralized loans**: Fixed-rate, short-duration loans backed by crypto collateral
- **Retail access via Syrup**: ERC-4626 vaults that aggregate capital from depositors

### Architecture

**Two-Layer Model:**

#### Layer 1: Institutional Lending
- **Open Term Loans (OTL)**: Evergreen loans, callable by lender with notice
- **Fixed Term Loans (FTL)**: Traditional loans with specific maturity dates
- **Native Loans**: Off-chain custody loans collateralized by native assets (BTC, SOL, ETH, weETH observed) — records in Maple's operational database (Mongo ObjectId IDs), not smart contracts

> **Note:** `openTermLoans` is the active book, but not the only loan query with live data:
> - **FTLs** are exposed via the generic `loans` root query (there is no `fixedTermLoans` query; the `Loan` type *is* the fixed-term entity — `maturityDate`, `termDays`, `paymentsRemaining`). The FTL book is **dormant, not retired**: zero loans in any live state as of 2026-06-12, but originations ran through 2025-02-28 (overlapping the open-term era by ~15 months), and the Fixed Term Loan Manager remains part of Maple's documented pool architecture. New FTLs could appear without warning.
> - **Native loans** are exposed via `nativeLoans` / `nativeLoanById` / `nativeLoansSnapshot(timestamp)`. As of 2026-06-11: 12 records, 1 with outstanding principal (~20k USDT against BTC), no new origination since 2025-08-14, but the book is still administered. `nativeLoans` takes no arguments (no pagination or filters), and there is no state field — nonzero `principalOwed` is the only liveness signal.
> - Two further families are auth-gated and unreachable without credentials: `collateralizedLoans` (root-level `UNAUTHORIZED`) and OTC loans (no public root query; only `otcCollateralTxs(loanId)`).

#### Layer 2: Syrup Vaults (ERC-4626)
- **SyrupUSDC**: USDC-denominated vault
- **SyrupUSDT**: USDT-denominated vault
- **SyrupUSDG**: USDG-denominated vault (returned by the GraphQL API as `syrupUSDG`)

**Flow:**
```
Users → Deposit to Syrup Vault → Vault lends to pools → Pools fund institutional loans
                ↓
         Share value increases as interest accrues
                ↓
         Users redeem shares at higher price (profit)
```

### Multi-Chain Deployments

**SyrupUSDC:**
- Ethereum: `0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b`
- Arbitrum: `0x41CA7586cC1311807B4605fBB748a3B8862b42b5`
- Base: `0x660975730059246A68521a3e2FBD4740173100f5`
- Solana: `AvZZF1YaZDziPY2RCK4oJrRVrbN3mTD9NL24hPeaZeUj`

**SyrupUSDT:**
- Ethereum: `0x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d`
- Plasma: `0xC4374775489CB9C56003BF2C9b12495fC64F0771`

**SyrupUSDG:**
- Ethereum: `0x87b65c4aaffa76881f9e96f3e7ed945ddfc3cd7a` (GraphQL `poolV2` ID; deployments on other chains, if any, are TBD)

Each chain deployment is independent but unified through CCIP cross-chain bridging.

---

## Core Concepts & Mechanics

### ERC-4626 Tokenized Vaults

Syrup vaults implement the **ERC-4626 Tokenized Vault Standard**, which defines a standardized interface for yield-bearing vaults.

**Assets vs Shares:**

**Assets:**
- Underlying tokens (USDC or USDT)
- What users deposit and withdraw

**Shares:**
- ERC-20 tokens representing vault ownership
- Minted on deposit, burned on withdrawal
- Appreciate in value as yield accrues

**Core Functions:**

```solidity
// Deposit assets, receive shares
function deposit(uint256 assets, address receiver) returns (uint256 shares)

// Withdraw assets, burn shares
function redeem(uint256 shares, address receiver, address owner) returns (uint256 assets)

// Vault state
function totalAssets() returns (uint256)  // Total assets including accrued yield
function totalSupply() returns (uint256)  // Total shares outstanding

// Conversions
function convertToShares(uint256 assets) returns (uint256 shares)
function convertToAssets(uint256 shares) returns (uint256 assets)
```

**How Yield Works:**

```solidity
// On deposit:
shares = assets * totalSupply / totalAssets

// As yield accrues:
totalAssets increases (from loan interest)
totalSupply stays constant (unless new deposits/withdrawals)
// → Each share becomes worth more assets

// On withdrawal:
assets = shares * totalAssets / totalSupply
// User receives more assets than deposited (profit)
```

**Share Price:**

The share price represents how many assets each share is worth. ERC-4626 provides the `convertToAssets` function for accurate conversion:

```
// Get share price (assets per 1 share)
sharePrice = convertToAssets(1e18)  // For 18-decimal shares

// Or for vaults with different decimal precision:
sharePrice = convertToAssets(10^shareDecimals)
```

**Note:** Direct calculation using `totalAssets / totalSupply` may be inaccurate due to rounding or vault-specific mechanics. Always use `convertToAssets` for accurate share price.

Share price starts at ~1.0 and grows as interest accrues.

**Example:**

```
Initial: 1,000,000 USDC, 1,000,000 shares
  Share Price = 1.00

Alice deposits 10,000 USDC:
  Receives: 10,000 shares
  New total: 1,010,000 USDC, 1,010,000 shares

30 days later, interest accrues (+8,417 USDC):
  Total: 1,018,417 USDC, 1,010,000 shares
  Share Price = 1.008333

Alice redeems 10,000 shares:
  Receives: 10,083.33 USDC
  Profit: 83.33 USDC
```

### Yield Sources

**Institutional Loan Interest:**

Syrup vault yield comes from:
1. Users deposit USDC/USDT to vault
2. Vault allocates to PoolV2 contracts
3. Pools lend to institutional borrowers
4. Borrowers pay interest
5. Interest flows back, increasing vault's `totalAssets()`
6. Share price increases

**Collateral Types:**

Borrowers post collateral:
- Crypto assets: ETH, WBTC, wstETH, cbETH
- Stablecoins: USDC, USDT, DAI (over-collateralization)
- Native assets: BTC, SOL, XRP (custody arrangements; these flow through `openTermLoans` as `collateral.asset` symbols with `custodian`/`loanMeta.walletType` hints — distinct from the `nativeLoans` entity)

**Collateral Ratio (ACM):**

```
ACM Ratio = Collateral Value / Principal Owed

Example: 1.656007 = 165.6% collateralization
```

### Commitment System (Optional)

Users can commit shares for enhanced yield:

**Lockup Tiers:**
- **0-day**: Base APY, fully liquid
- **90-day**: Base APY + bonus + DRIPS rewards
- **180-day**: Base APY + higher bonus + more DRIPS

**DRIPS**: Maple's yield boost tokens earned during lockup periods.

---

## Retrieving Key Data

### Position Value

**On-Chain (Real-time):**

```typescript
// Get user's shares
const shares = await vaultContract.balanceOf(userAddress);

// Convert to asset value (recommended method)
const assets = await vaultContract.convertToAssets(shares);

// USD value (assuming USDC/USDT = $1.00):
const usdValue = assets / 1e6;  // USDC/USDT have 6 decimals
```

**Note:** Always use `convertToAssets()` for accurate share-to-asset conversion. Manual calculation using `(shares * totalAssets) / totalSupply` may be inaccurate due to ERC-4626 implementation details.

**GraphQL API:**

```typescript
// Note: Construct the composite ID on the client side (addresses must be lowercased)
const positionId = `${userAddress.toLowerCase()}-${poolAddress.toLowerCase()}`;

const query = `
  query UserPosition($positionId: ID!) {
    poolV2Position(id: $positionId) {
      account { id }
      availableBalance    # Value of uncommitted shares (in USDC/USDT)
      availableShares     # Uncommitted shares
      lendingBalance      # Total value including committed shares
      commitments {
        amount            # USDC/USDT committed
        shares            # Shares locked
        days              # Lockup period (0, 90, 180)
        dripsEarned       # DRIPS rewards earned
      }
    }
  }
`;

const result = await graphqlQuery(query, { positionId });
```

### APY (Annual Percentage Yield)

**Method 1: Calculate from Share Price Changes (On-Chain)**

```typescript
// Get share price at two points in time using convertToAssets
const sharePrice1 = await vaultContract.convertToAssets(1e18, { blockTag: block1 });
const sharePrice2 = await vaultContract.convertToAssets(1e18, { blockTag: block2 });
const timestamp1 = (await provider.getBlock(block1)).timestamp;
const timestamp2 = (await provider.getBlock(block2)).timestamp;

// Calculate APY with compounding
const timeDelta = timestamp2 - timestamp1;
const periodsPerYear = SECONDS_PER_YEAR / timeDelta;
const returnRate = sharePrice2 / sharePrice1;  // e.g., 1.01 for 1% return

// APY = (1 + periodReturn)^periodsPerYear - 1
const apy = Math.pow(returnRate, periodsPerYear) - 1;
const apyPercent = apy * 100;
```

**Method 2: GraphQL API**

```graphql
query PoolAPY($poolAddress: ID!) {
  poolV2(id: $poolAddress) {
    monthlyApy         # 30-day historical average APY (30 decimals, includes yield on collateral)
    spotApy            # Current pool APY (30 decimals)
    assets             # Pool cash
    collateralValue    # Sum in USD
    principalOut       # Outstanding value of active loans
    tvl                # Total value locked
  }
}
```

**Global Syrup Statistics:**

```graphql
query GlobalSyrupStats {
  syrupGlobals {
    apy                # Overall Syrup APY (30 decimals)
    collateralApy      # APY from collateral (30 decimals)
    poolApy            # APY from pool lending (30 decimals)
    dripsYieldBoost    # Additional yield from DRIPS
    tvl                # Total value locked
  }
}
```

### Position Backing (Collateral Composition)

There are two ways to query collateral data:

1. **Pool Collateral** - Aggregate collateral data at the pool level
2. **Individual Loans** - Detailed collateral data for each active loan (see [Individual Loans section](#individual-loans))

---

#### Pool Collateral

**⚠️ LIMITATION:**

The `poolCollaterals` query returns **aggregate collateral data for external loans only**. This means:

- ✅ It accurately shows collateral backing external borrower loans
- ❌ It **does not include** internal loans made to Maple (e.g., for strategies, AMM positions)
- ❌ It **does not accurately represent** the total backing for Syrup assets like syrupUSDC

**Why this matters:** Pools have excess liquidity that may be deployed internally to Maple strategies. This internal deployment is not captured in `poolCollaterals`, making it insufficient for calculating the true backing composition of Syrup pool tokens.

**Recommendation:** Use the **Individual Loans** section below to query all active loans (both external and internal) for a complete view of pool backing.

---

#### Individual Loans

**⚠️ IMPORTANT: External vs. Internal Loans**

Individual loans include both:
1. **External loans** - Loans made to external parties with traditional collateral backing
2. **Internal loans** - Loans made internally to Maple for strategies and positions (identified by `loanMeta.type` = `"amm"` or `"strategy"`)

On Maple's frontend, they explicitly report collateral backing for **external parties' collateral only**. If following this methodology, internal Maple loan collateral should be filtered out.

**Note:** Internal Maple positions back Syrup pools through **two distinct channels** (verified live 2026-06-15):
1. **AMM/strategy loans** — `openTermLoans` with `loanMeta.type` in `["amm", "strategy"]`, funded by the Syrup pools. **~$100M outstanding across Syrup USDC + USDT as of 2026-06-15.** These are counted in `poolV2.principalOut` (they carry `principalOwed`), so the principal is not missing from pool metrics — but their `collateral` field is a placeholder and misrepresents the real backing (see warning below).
2. **Sky Strategies** (`skyStrategies`) — pool cash deployed into Sky/Maker DeFi yield. **Dormant: `currentlyDeployed = 0` for every strategy as of 2026-06-15**, but the Syrup USDC strategy has cycled ~9.46B USDC cumulatively (`depositedAssets`), so it is live infrastructure that can become nonzero without notice. Only Syrup USDC has a Sky Strategy entity; Syrup USDT and USDG have none. See [Sky Strategies section](#sky-strategies) below.

---

For more granular collateral data, you can query individual active loans and their collateral backing.

**GraphQL Query:**

```graphql
query GetAllActiveLoans($block: Block_height!, $first: Int!, $skip: Int!) {
  openTermLoans(block: $block, first: $first, skip: $skip, where: { state: Active }) {
    id
    borrower { id }
    state
    principalOwed
    acmRatio
    collateral {
      asset
      assetAmount
      assetValueUsd
      decimals
      state
      custodian
      liquidationLevel
    }
    loanMeta {
      type
      assetSymbol
      dexName
      location
      walletAddress
      walletType
    }
    fundingPool {
      id
      name
      asset { symbol decimals }
    }
  }
}
```

**Field Descriptions:**

- `id`: Loan contract address
- `borrower.id`: Borrower's address
- `principalOwed`: Outstanding principal (integer string, 6 decimals for USDC/USDT)
- `acmRatio`: Asset Coverage Margin ratio (6 decimals, e.g., `1445731` = 144.57%). **Nullable**: active uncollateralized loans return `acmRatio: null` (and `collateral: null`). Consumers must not assume a value on active loans.
- `collateral.assetValueUsd`: **Asset price per unit in USD** (integer, 8 decimals) - multiply by `assetAmount` to get total value
- `loanMeta`: Loan metadata (see warning below). Present on most loans, internal and external alike; its presence does **not** indicate an internal position.

**Usage:**

This query supports pagination (required for >1000 loans) and returns all active loans across all pools. You can:
1. Sum `collateral.assetValueUsd` by asset type to get per-asset collateral totals
2. Group loans by `fundingPool.id` to analyze collateral backing for a specific pool
3. Calculate collateralization ratios using `principalOwed` and `collateral.assetValueUsd`

---

**⚠️ IMPORTANT: `loanMeta` and Internal Maple Positions**

The **only** reliable signal that a loan is an internal Maple position is `loanMeta.type` being `"amm"` or `"strategy"`. Do **not** treat the mere presence of `loanMeta` as an internal-loan indicator: most active loans (internal and external) carry a non-null `loanMeta`, and every field inside it (including `type`) is nullable. External loans commonly have `loanMeta` present with `type: null`.

Observed `loanMeta.type` values on active loans include `null`, `"amm"`, `"strategy"`, `"tBills"`, and `"intercompany"`. The schema defines a `LoanType` enum (`amm`, `intercompany`, `mapleTrading`, `strategy`), but treat it as approximate: `"tBills"` (observed live) is absent from the enum, and `"mapleTrading"` has not been seen live. The semantics of `"tBills"` and `"intercompany"` are undocumented and need confirmation from Maple; new values may appear over time.

When `loanMeta.type` is `"amm"` or `"strategy"`, the loan represents an **internal Maple position** (e.g., DeFi strategy, LP position).

**For these loans:**
- ❌ The `collateral` field **may not accurately represent** the actual backing
- ⚠️ The real asset is a DeFi position described by `loanMeta`, not the collateral asset shown
- ⚠️ The underlying asset details may be **incomplete or unavailable**

**Example - Incomplete Asset Information:**

```json
{
  "id": "0x5d8839ef73532e035f7f9ad3049be5d4ff170ca9",
  "acmRatio": "1000000",
  "collateral": {
    "asset": "USDC",
    "assetAmount": "20000000000000",
    "assetValueUsd": "100000000",
    "decimals": 6
  },
  "principalOwed": "20000000000000",
  "loanMeta": {
    "type": "amm",
    "assetSymbol": null,
    "dexName": "Aerodrome",
    "location": null,
    "walletAddress": "0x2570fAF7C8A0da87d3F123B35cC722EC3fCC3e08",
    "walletType": "BASE"
  }
}
```

In this case:
- ✅ We know it's an AMM position on **Base blockchain** (Aerodrome DEX)
- ✅ We know the **wallet address** holding the position
- ❌ We **don't know** the underlying LP token contract address
- ❌ We **don't know** which trading pair it represents
- ⚠️ The `collateral.asset` ("USDC") may not reflect the actual position

**Recommendation:** When aggregating collateral data, flag loans with `loanMeta.type` in `["amm", "strategy"]` as having potentially incomplete asset information.

---

#### Sky Strategies

In addition to individual loans, Maple has internal **Sky Strategies** that deploy pool assets into DeFi positions. These may also contribute to pool backing and should be investigated if comprehensive collateral tracking is needed.

**GraphQL Query:**

```graphql
query GetSkyStrategies($poolId: ID!, $first: Int!, $skip: Int!) {
  skyStrategies(first: $first, skip: $skip, where: { pool: $poolId }) {
    id
    pool { id name }
    state
    currentlyDeployed
    depositedAssets
    withdrawnAssets
    strategyFeeRate
    totalFeesCollected
    version
  }
}
```

**Field Descriptions:**

- `id`: Strategy identifier
- `pool`: The pool this strategy belongs to
- `state`: Current state of the strategy
- `currentlyDeployed`: Amount currently deployed in the strategy (integer string)
- `depositedAssets`: Total assets deposited into the strategy (integer string)
- `withdrawnAssets`: Total assets withdrawn from the strategy (integer string)
- `strategyFeeRate`: Fee rate for the strategy (integer, likely 6 decimals)
- `totalFeesCollected`: Total fees collected by the strategy (integer string)

**Live findings (2026-06-15):** querying `skyStrategies(first: 100)` returns 4 strategies total, 1 tied to a Syrup pool:

| Strategy `id` | `pool.id` (= vault addr) | pool name | state | `currentlyDeployed` | `depositedAssets` (cumulative) |
|---|---|---|---|---|---|
| `0x859c…b038c` | `0x80ac24aa…` | Syrup USDC | Active | `0` | `9464548714891221` (~9.46B) |
| `0x34e7…3b00` | `0xc9c9bab5…` | Maple Lend + Long USDC2 | Active | `0` | `0` |
| `0xb390…5807` | `0x37154b07…` | Maple Lend+Long USDC1 | Active | `0` | `0` |
| `0xe3ee…55cc` | `0xc39a5a61…` | High Yield Secured Lending USDC1 | Active | `0` | `419563235236556` |

Resolved:
1. **Do Sky Strategies back Syrup pools?** Yes in principle — only Syrup USDC has a strategy, and it has cycled ~9.46B cumulatively. **But `currentlyDeployed = 0` everywhere right now**, so they contribute **zero** to current backing. Treat as a live source that must be re-polled, not a one-time decision.
2. **`pool.id` on a strategy equals the pool's VAULT address** (`0x80ac24aa…` for Syrup USDC), which is the same key as `poolV2.id` — they join cleanly. (See the [vault-address keying note](#graphql-api) — `poolV2` is keyed by vault address, not by the "Pool Address" in the contract tables.)
3. **Double-count / valuation:** undeterminable while `currentlyDeployed = 0`. When a strategy goes nonzero, confirm whether its deployed amount is already reflected in `poolV2.assets`/`principalOut` before adding it to a backing total, and verify the underlying asset + decimals/encoding live (`version` arrives as a JSON number, e.g. `100`; `currentlyDeployed`/`depositedAssets`/`totalFeesCollected` arrive as integer strings; `strategyFeeRate` observed `100000`).

### TVL (Total Value Locked)

**Single Vault:**

```typescript
const totalAssets = await vaultContract.totalAssets();
const tvlUsd = totalAssets / 1e6;  // Assuming USDC/USDT = $1.00
```

**Protocol-Wide TVL:**

```typescript
// Note: Vault supplies are only natively minted on Ethereum
// All other chains hold bridged tokens that are already counted in Ethereum totals

const syrupUSDC = "0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b";
const syrupUSDT = "0x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d";

// Query Ethereum mainnet only
const usdcAssets = await vaultContract.at(syrupUSDC).totalAssets();
const usdtAssets = await vaultContract.at(syrupUSDT).totalAssets();

const protocolTvl = (usdcAssets / 1e6) + (usdtAssets / 1e6);
```

**GraphQL API (Pool-Level Data):**

```graphql
# ⚠️ `poolV2` is keyed by the VAULT address, NOT the "Pool Address" in the contract tables below.
#   poolV2(id: "0x80ac24aa…")  [Syrup USDC vault]  → returns the pool ✅
#   poolV2(id: "0x20b79d39…")  [Syrup USDC pool addr] → null ❌  (verified 2026-06-15)
# `skyStrategies.pool.id` also uses the vault address, so the two join cleanly.
query PoolData($poolAddress: ID!) {
  poolV2(id: $poolAddress) {
    tvl                  # NOT a clean sum of assets + collateralValue + principalOut, and NOT vault totalAssets.
                         # Verified 2026-06-15: USDC tvl=2835.13M but assets+collateral+principalOut=3172.36M;
                         # USDT tvl=838.36M vs sum 895.47M. Treat `tvl` as an opaque API-provided figure.
    assets               # Liquid pool cash (6 decimals)
    collateralValue      # USD value of loan collateral (6 decimals)
    principalOut         # Outstanding loan principal (6 decimals)
  }
}

query GlobalSyrupStats {
  syrupGlobals {
    tvl                  # Total across all Syrup pools (includes loans + liquidity + collateral, not just vault totalAssets)
  }
}
```

**Important Note:** The GraphQL `tvl` and `assets` fields represent pool-level lending metrics (liquid cash, outstanding loans, and collateral), **not** the ERC-4626 vault's `totalAssets()` value. The pool's `assets` field (liquid cash) may be lower than the vault's `totalAssets()` because `totalAssets()` includes deployed capital in loans.

**For accurate vault TVL matching on-chain values**, use the on-chain method shown above. The GraphQL API is more suitable for tracking pool performance, loan composition, and collateral backing rather than exact vault TVL.


### Transaction History

**⚠️ Important:** All addresses sent to the Maple GraphQL API **must be lowercase**, otherwise the API will return null/empty results.

```graphql
query UserTransactions($userAddress: String!) {
  txes(
    where: {
      poolV2_: { syrupRouter_not: null },
      account: $userAddress     # MUST be lowercase (e.g., "0x123abc" not "0x123ABC")
    }
    first: 100
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    amount            # Asset amount
    value             # USD value
    symbol            # USDC/USDT
    type              # DEPOSIT, WITHDRAW, etc.
    timestamp
    transaction { id }
    shares
  }
}
```


### Price Data

**SyrupUSDC/USDT Price Formula:**

```
Syrup Token Price = Underlying Asset Price × Share Price
```

**1. Underlying Asset Price (USDC/USDT):**
- Hardcode to `$1.00`, or
- Use Chainlink or other oracle's USDC/USD or USDT/USD feeds (8 decimals)

**2. Share Price (Syrup/Underlying ratio):**
- **Method A:** `convertToAssets(1e18)` on the ERC-4626 vault (most accurate)
- **Method B:** Maple's on-chain oracles:

| Chain | Oracle Address |
|-------|---------------|
| Arbitrum | `0xF8722c901675C4F2F7824E256B8A6477b2c105FB` |
| Base | `0x311D3A3faA1d5939c681E33C2CDAc041FF388EB2` |
| Solana | `CpNyiFt84q66665Kx64bobxZuMgZ2EecrhAJs1HikS2T` |

**Example:**

```typescript
// Calculate syrupUSDC price
const underlyingPrice = 1.0;  // or from Chainlink
const sharePrice = await syrupVault.convertToAssets(1e18) / 1e18;
const syrupUsdcPrice = underlyingPrice * sharePrice;
```

---

## Smart Contracts & Addresses

### Syrup Vault Contracts

**SyrupUSDC:**

| Chain | Vault Address | CCIP Router | Pool Address |
|-------|--------------|-------------|--------------|
| **Ethereum** | `0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b` | `0x80226fc0Ee2b096224EeAc085Bb9a8cba1146f7D` | `0x20B79D39Bd44dEee4F89B1e9d0e3b945fde06491` |
| **Arbitrum** | `0x41CA7586cC1311807B4605fBB748a3B8862b42b5` | `0x141fa059441E0ca23ce184B6A78bafD2A517DdE8` | `0x660975730059246A68521a3e2FBD4740173100f5` |
| **Base** | `0x660975730059246A68521a3e2FBD4740173100f5` | `0x881e3A65B4d4a04dD529061dd0071cf975F58bCD` | `0xA36955b2Bc12Aee77FF7519482D16C7B86DBe42a` |
| **Solana** | `AvZZF1YaZDziPY2RCK4oJrRVrbN3mTD9NL24hPeaZeUj` | `Ccip842gzYHhvdDkSyi2YVCoAWPbYJoApMFzSxQroE9C` | `HrTBpF3LqSxXnjnYdR4htnBLyMHNZ6eNaDZGPundvHbm` |

**SyrupUSDT:**

| Chain | Vault Address | CCIP Router | Pool Address |
|-------|--------------|-------------|--------------|
| **Ethereum** | `0x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d` | `0x80226fc0Ee2b096224EeAc085Bb9a8cba1146f7D` | TBD |
| **Plasma** | `0xC4374775489CB9C56003BF2C9b12495fC64F0771` | `0xcdca5D374e46A6DDDab50bD2D9acB8c796eC35C3` | `0x1d952d2f6eE86Ef4940Fa648aA7477c8fF175F09` |

### Core ERC-4626 Functions

All Syrup vaults implement standard ERC-4626 interface:

```solidity
// Query functions
function totalAssets() external view returns (uint256)
function totalSupply() external view returns (uint256)
function balanceOf(address user) external view returns (uint256)
function convertToAssets(uint256 shares) external view returns (uint256)
function convertToShares(uint256 assets) external view returns (uint256)

// User functions
function deposit(uint256 assets, address receiver) external returns (uint256 shares)
function redeem(uint256 shares, address receiver, address owner) external returns (uint256 assets)
function asset() external view returns (address)  // Returns USDC or USDT address
```

### GraphQL API

**Endpoint:** `https://api.maple.finance/v2/graphql`

**Authentication:** Most resources are public and need no authentication. Some resources are internal-use only and return `UNAUTHORIZED` — root-level (e.g. `collateralizedLoans`) or field-level (e.g. `NativeLoan.collateralAccountType`, `NativeLoan.marginCallActive`). Maple provides **no third-party authentication mechanism at all**; access to gated resources requires a direct arrangement with Maple (partnerships@maple.finance).

**Gated-field behavior:** a gated *nullable* field produces per-row `UNAUTHORIZED` partial errors alongside usable `data`; a gated **non-nullable** field cannot be nulled per-row, so the error propagates and nulls the entire result (e.g. selecting `NativeLoan.marginCallActive` anonymously nulls the whole `nativeLoans` array). Omit gated fields rather than tolerating partial errors.

**Key Conventions:**
- All addresses must be **lowercased** (e.g., `0x123abc` not `0x123ABC`)
- Fractional values use integer strings with specific decimal places
- APY values: 30 decimals
- Collateral ratios: 6 decimals
- Interest rates: 6 decimals
- **Exceptions (JSON numbers, not strings):** `collateral.liquidationLevel` (e.g. `900000`) and `skyStrategy.version` (e.g. `100`) arrive as JSON numbers. Decoders that strictly expect string-encoded integers will fail on these two fields.
- **Encoding is not uniform across entity families.** The conventions above hold for subgraph entities (`OpenTermLoan`, `PoolV2`, `Loan`). `NativeLoan` uses strings for amounts but JSON `Int`s for `liquidationLevel`/`initialLevel`/thresholds, and epoch-millis strings for timestamps (e.g. `"1716566130811"`). `NativeLoanSnapshot` uses plain `Float`s for all monetary values.
- Introspection is disabled (`INTROSPECTION_DISABLED`), but the schema is published to Apollo Studio (see [References](#references)). Auth-gating directives are stripped from the public SDL, so field-level access must still be verified by execution

**Pagination:**

```graphql
query {
  poolV2S(
    first: 100,              # Limit
    skip: 0                  # Offset
  ) { ... }
}
```

**Note:** `orderBy: tvl` is rejected (`Value "tvl" does not exist in "PoolV2_orderBy" enum`); plain `first`/`skip` works. Skip-based pagination has no stable order, so callers needing a complete set should fetch all pages within one cycle and treat duplicates across pages as an error.

### Cross-Chain Considerations

**CCIP (Chainlink Cross-Chain Interoperability Protocol):**

- SyrupUSDC/USDT use **lock/release model** for cross-chain transfers via `LockReleaseTokenPool`
- Tokens are **locked on Ethereum** when bridged to other chains (not burned)
- Other chains receive bridged representations of the locked Ethereum tokens
- Each chain has independent vault contract with same ERC-4626 interface
- Share price remains consistent across all deployments via CCIP oracles

---

## References

### Official Documentation

- **Maple Finance:** https://www.maple.finance/
- **Documentation:** https://docs.maple.finance/
- **Syrup Docs:** https://docs.maple.finance/syrup/
- **Integration Guide:** https://docs.maple.finance/integrate/

### GraphQL API

- **Endpoint:** https://api.maple.finance/v2/graphql
- **Graph Registry:** maple-api@mainnet (single variant `mainnet` — the endpoint is global and Ethereum-mainnet-scoped; `mainnet` is the Apollo Studio variant name, not a URL path segment)
- **Schema:** Introspection is disabled. Browsable schema reference: https://studio.apollographql.com/public/maple-api/variant/mainnet/schema/reference (full SDL also fetchable unauthenticated via the Apollo platform API)

### Technical Standards

- **ERC-4626:** https://eips.ethereum.org/EIPS/eip-4626
- **Chainlink CCIP:** https://docs.chain.link/ccip
- **CCIP Token (SyrupUSDC):** https://docs.chain.link/ccip/directory/mainnet/token/syrupUSDC
- **CCIP Token (SyrupUSDT):** https://docs.chain.link/ccip/directory/mainnet/token/syrupUSDT

### Analytics & Data

- **Dune Dashboard:** https://dune.com/maple-finance/maple-finance
- **Token Terminal:** https://tokenterminal.com/terminal/projects/maple-finance
- **DeFi Llama:** https://defillama.com/protocol/maple-finance

### Community & Support

- **Telegram:** https://t.me/maplefinance
- **Twitter:** https://twitter.com/maplefinance

---

**Contributors:** Technical specification based on Maple Finance protocol documentation, GraphQL API schema, and smart contract interfaces.
