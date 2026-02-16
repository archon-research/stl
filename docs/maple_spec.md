# Maple Finance Protocol Specification

**Version:** 1.0  
**Last Updated:** January 2026  
**Purpose:** Technical reference for understanding Maple Finance protocol mechanics and data retrieval

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
- **Native Loans**: Non-Ethereum assets (SOL, BTC) held in custody

#### Layer 2: Syrup Vaults (ERC-4626)
- **SyrupUSDC**: USDC-denominated vault
- **SyrupUSDT**: USDT-denominated vault

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
- Native assets: BTC, SOL (custody arrangements)

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

**GraphQL Query:**

```graphql
query PoolCollateralData($poolAddress: ID!) {
  poolV2(id: $poolAddress) {
    poolMeta {
      poolName
      poolCollaterals {
        asset              # e.g., "BTC", "ETH", "weETH", "HYPE"
        assetAmount        # Total collateral amount (integer string)
        assetDecimals      # Decimals for assetAmount
        assetValueUsd      # USD value (integer, 6 decimals)
        addresses          # Collateral holder addresses
      }
    }
    assets               # Pool liquid assets
    collateralValue      # Total collateral USD value
    principalOut         # Outstanding loan principal
    tvl                  # Total value locked
  }
}
```

**Example Response:**

```json
{
  "poolV2": {
    "poolMeta": {
      "poolCollaterals": [
        {
          "asset": "BTC",
          "assetValueUsd": "600000000000"
        },
        {
          "asset": "ETH",
          "assetValueUsd": "300000000000"
        },
        {
          "asset": "HYPE",
          "assetValueUsd": "100000000000"
        }
      ]
    },
    "collateralValue": "1000000000000",
    "principalOut": "800000000000"
  }
}
```

**Calculate Backing Composition:**

```typescript
// User holds $1,000 worth of SyrupUSDC
const userPositionUsd = 1000;

// Pool collateral (from GraphQL, 6 decimals):
const btcValueUsd = 600000000000 / 1e6;   // $600,000
const ethValueUsd = 300000000000 / 1e6;   // $300,000
const hypeValueUsd = 100000000000 / 1e6;  // $100,000
const totalCollateralUsd = 1000000;       // $1,000,000 total

// Calculate backing percentages:
const btcPercent = (btcValueUsd / totalCollateralUsd) * 100;   // 60%
const ethPercent = (ethValueUsd / totalCollateralUsd) * 100;   // 30%
const hypePercent = (hypeValueUsd / totalCollateralUsd) * 100; // 10%

// User's $1,000 position is backed by:
// - $600 worth of BTC (60% of $1,000 = $600)
// - $300 worth of ETH (30% of $1,000 = $300)
// - $100 worth of HYPE (10% of $1,000 = $100)
```

**Key Point:** All SyrupUSDC holders have the **same backing composition** - it's determined by the pool's total collateral, not individual user positions.

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
query PoolData($poolAddress: ID!) {
  poolV2(id: $poolAddress) {
    tvl                  # Sum of assets + collateralValue + principalOut (not the same as vault totalAssets)
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

**Authentication:** Public, no authentication required

**Key Conventions:**
- All addresses must be **lowercased** (e.g., `0x123abc` not `0x123ABC`)
- Fractional values use integer strings with specific decimal places
- APY values: 30 decimals
- Collateral ratios: 6 decimals
- Interest rates: 6 decimals

**Pagination:**

```graphql
query {
  poolV2S(
    first: 100,              # Limit
    skip: 0,                 # Offset
    orderBy: tvl,
    orderDirection: desc
  ) { ... }
}
```

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
- **Graph Registry:** maple-api@mainnet
- **Schema:** Available via GraphQL introspection

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

**Document Version:** 1.0  
**Last Updated:** January 2026

**Contributors:** Technical specification based on Maple Finance protocol documentation, GraphQL API schema, and smart contract interfaces.
