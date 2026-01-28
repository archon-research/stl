# Curve Stableswap-NG Plain Pools Specification

**Version:** 1.0  
**Last Updated:** January 2026  
**Purpose:** Technical reference for retrieving composition, TVL, price, and yield data from Curve Stableswap-NG Plain Pools

---

## Table of Contents

1. [Protocol Overview](#protocol-overview)
2. [Core Concepts & Mechanics](#core-concepts--mechanics)
3. [Retrieving Key Data](#retrieving-key-data)
4. [Smart Contract Methods](#smart-contract-methods)
5. [References](#references)

---

## Protocol Overview

### What is Curve Finance?

Curve Finance is a **decentralized exchange (DEX)** optimized for trading stablecoins and similarly-pegged assets with minimal slippage. Launched in 2020, Curve uses specialized automated market maker (AMM) algorithms designed for low-slippage swaps between assets of similar value.

**Key Features:**
- **Low slippage**: Optimized for swaps between similarly-priced assets (stablecoins, wrapped assets)
- **Capital efficiency**: Superior pricing compared to constant product AMMs (e.g., Uniswap) for stable pairs
- **Yield opportunities**: LPs earn trading fees + liquidity mining rewards (CRV tokens)

### Stableswap-NG

**Stableswap-NG** is the latest iteration of Curve's Stableswap implementation, offering:
- Permissionless pool deployment
- Enhanced gas efficiency
- Improved oracle integration
- Support for up to 8 coins per pool

**Plain Pools vs Metapools:**
- **Plain Pools**: Contains 2-8 individual tokens (e.g., USDC/USDT/DAI)
- **Metapools**: Pairs a token against another Curve LP token

This specification focuses exclusively on **Stableswap-NG Plain Pools**.

### Architecture

**Stableswap AMM Model:**

```
Users ↔ Swap tokens in pool ↔ Pay swap fee
         ↓
    Fees distributed to:
    - Liquidity Providers (LPs)
    - veCRV holders (protocol fee)
         ↓
    LPs receive:
    - Trading fees (via increased LP token value)
    - CRV emissions (via Liquidity Gauge)
```

---

## Core Concepts & Mechanics

### Stableswap Invariant

Unlike constant product AMMs (`x * y = k`), Stableswap uses a hybrid invariant that combines constant sum and constant product behavior:

```
A * n^n * sum(x_i) + D = A * D * n^n + D^(n+1) / (n^n * prod(x_i))
```

Where:
- `A` = Amplification coefficient (controls curve shape)
- `n` = Number of coins in pool
- `x_i` = Balance of coin i
- `D` = Total balance ("invariant")

**Behavior:**
- **When balanced**: Acts like constant sum (low slippage, efficient)
- **When imbalanced**: Acts like constant product (prevents pool drain)

### Key Parameters

#### Amplification Coefficient (A)

The **A factor** determines how "flat" the bonding curve is:

- **Higher A** (e.g., 1000-5000): Very flat curve, minimal slippage, assumes assets stay tightly pegged
- **Lower A** (e.g., 10-100): More curve, higher slippage tolerance, safer for loosely-pegged assets

**Example:**
- USDC/USDT/DAI: High A (~2000) - very stable pegs
- wBTC/tBTC: Lower A (~100) - wrapped assets may depeg slightly

The A parameter can be adjusted by pool admin over time via ramping mechanisms.

#### Virtual Price

**Virtual price** represents the growth in LP token value due to accumulated fees:

```
virtual_price = get_virtual_price()  // Returns value in 18 decimals
```

**How it works:**
- Starts at 1.0 (1e18)
- Increases as trading fees accumulate
- Grows monotonically (never decreases)
- Reflects LP profit from fees

**Example:**
```
Initial: virtual_price = 1.000000000000000000
After fees accrue: virtual_price = 1.002345678901234567
LP profit: 0.23% from trading fees
```

**Note:** Virtual price does **not** include CRV emissions from gauges - those are separate rewards.

### LP Tokens

When users provide liquidity, they receive **LP tokens** representing their pool share:

**On Deposit:**
```solidity
// Add liquidity, receive LP tokens
function add_liquidity(
    uint256[N_COINS] amounts,  // Amount of each coin to deposit
    uint256 min_mint_amount    // Minimum LP tokens to receive (slippage protection)
) returns (uint256)
```

**On Withdrawal:**
```solidity
// Burn LP tokens, receive coins
function remove_liquidity(
    uint256 amount,              // LP tokens to burn
    uint256[N_COINS] min_amounts // Minimum of each coin to receive
) returns (uint256[N_COINS])
```

**LP Token Value:**
```
LP Token Value = (Total Pool TVL * virtual_price) / totalSupply
```

### Fee Structure

**Swap Fees:**
- Base fee (e.g., 0.04% = 4 bps)
- Dynamic fee adjustment based on pool imbalance
- Fees increase when pool is imbalanced to discourage further imbalance

**Fee Distribution:**
- 50% to LPs (increases virtual price)
- 50% to veCRV holders (via FeeDistributor)

---

## Retrieving Key Data

### Pool Composition & Balances

**Get all coin addresses:**

```solidity
// Iterate through coins (0-indexed)
address coin0 = pool.coins(0);  // First coin
address coin1 = pool.coins(1);  // Second coin
// ... up to coins(7) for 8-coin pools

uint256 N_COINS = pool.N_COINS();  // Total number of coins
```

**Get pool balances:**

```solidity
// Get balances of all coins in pool
uint256[] memory balances = pool.get_balances();

// Or get individual balances
uint256 balance0 = pool.balances(0);
uint256 balance1 = pool.balances(1);
```

**Example - USDC/DAI pool (different decimals):**

```typescript
const pool = await ethers.getContractAt("CurveStableSwapNG", poolAddress);

// Get coin addresses
const nCoins = await pool.N_COINS();
const coins = [];
for (let i = 0; i < nCoins; i++) {
  coins.push(await pool.coins(i));
}
// coins = [USDC_ADDRESS, DAI_ADDRESS]

// Get decimals for each coin
const decimals = [];
for (let i = 0; i < nCoins; i++) {
  const token = await ethers.getContractAt("ERC20", coins[i]);
  decimals.push(await token.decimals());
}
// decimals = [6, 18]  // USDC=6, DAI=18

// Get raw balances
const balances = await pool.get_balances();
// balances = [43871860921301n, 56128810531382497082801215n]
// Note: Different decimals mean raw values aren't directly comparable!

// Normalize balances to 18 decimals for accurate comparison
const normalizedBalances = balances.map((bal, i) => 
  bal * (10n ** (18n - BigInt(decimals[i])))
);
// normalizedBalances = [43871860921301000000000000n, 56128810531382497082801215n]

// Calculate composition percentages
const total = normalizedBalances.reduce((a, b) => a + b, 0n);
const composition = normalizedBalances.map(bal => 
  Number((bal * 10000n) / total) / 100  // Percentage
);
// composition = [43.87, 56.13]  // 43.87% USDC, 56.13% DAI
```

### TVL (Total Value Locked)

**Calculate pool TVL:**

```typescript
const pool = await ethers.getContractAt("CurveStableSwapNG", poolAddress);

// Method 1: Sum normalized balances (requires token prices)
const nCoins = await pool.N_COINS();
let tvl = 0;

for (let i = 0; i < nCoins; i++) {
  const balance = await pool.balances(i);
  const coinAddress = await pool.coins(i);
  const decimals = await ERC20(coinAddress).decimals();
  const price = await getPrice(coinAddress);  // From oracle or hardcode for stables
  
  const normalizedBalance = balance / (10 ** decimals);
  tvl += normalizedBalance * price;
}

// Method 2: For stablecoin pools, approximate by summing balances
// Assumes all stables ≈ $1.00
const balances = await pool.get_balances();
const tvlApprox = balances.reduce((sum, bal) => sum + bal / 1e18, 0);  // Adjust decimals
```

**For stablecoin pools (USDC/USDT/DAI):**

```typescript
// Quick approximation (assuming $1.00 per stablecoin)
const balances = await pool.get_balances();

// Normalize each balance to 18 decimals
const usdc = balances[0] * 1e12;  // USDC is 6 decimals, multiply by 1e12
const usdt = balances[1] * 1e12;  // USDT is 6 decimals
const dai = balances[2];           // DAI is 18 decimals

const tvl = (usdc + usdt + dai) / 1e18;  // Total in USD
```

### Relative Prices & Exchange Rates

**Get exchange rate between two coins:**

```solidity
// Get amount of coin j received for 1 unit of coin i (including fees)
uint256 rate = pool.get_dy(
    int128 i,       // Index of input coin
    int128 j,       // Index of output coin  
    uint256 dx      // Amount of input coin (use 10^decimals for "1 unit")
);
```

**Example - Price of USDC in terms of USDT:**

```typescript
const pool = await ethers.getContractAt("CurveStableSwapNG", poolAddress);

// Assume: coin(0) = USDC (6 decimals), coin(1) = USDT (6 decimals)

// How much USDT for 1 USDC?
const usdcAmount = 1e6;  // 1 USDC
const usdtOut = await pool.get_dy(0, 1, usdcAmount);

const price = usdtOut / 1e6;  // e.g., 0.9998 means 1 USDC = 0.9998 USDT
console.log(`1 USDC = ${price} USDT`);

// Get price with fees included (actual swap amount)
const usdtOutWithFee = await pool.get_dy(0, 1, usdcAmount);
const priceAfterFee = usdtOutWithFee / 1e6;
```

**Calculate all relative prices:**

```typescript
async function getPriceMatrix(pool) {
  const nCoins = await pool.N_COINS();
  const decimals = [];
  
  // Get decimals for each coin
  for (let i = 0; i < nCoins; i++) {
    const coin = await pool.coins(i);
    decimals[i] = await ERC20(coin).decimals();
  }
  
  // Build price matrix
  const prices = [];
  for (let i = 0; i < nCoins; i++) {
    prices[i] = [];
    for (let j = 0; j < nCoins; j++) {
      if (i === j) {
        prices[i][j] = 1.0;
      } else {
        const amount = 10 ** decimals[i];  // 1 unit of coin i
        const received = await pool.get_dy(i, j, amount);
        prices[i][j] = Number(received) / (10 ** decimals[j]);
      }
    }
  }
  
  return prices;
}

// prices[0][1] = price of coin 0 in terms of coin 1
// prices[1][0] = price of coin 1 in terms of coin 0
```

### LP Token Value & Share Price

**Get virtual price (includes accrued fees):**

```typescript
const virtualPrice = await pool.get_virtual_price();
// Returns value in 18 decimals (1e18 = 1.0)

const virtualPriceDecimal = Number(virtualPrice) / 1e18;
console.log(`Virtual Price: ${virtualPriceDecimal}`);
// e.g., 1.002345 means LPs earned 0.23% from fees
```

**Calculate LP token value in USD:**

```typescript
const pool = await ethers.getContractAt("CurveStableSwapNG", poolAddress);
const lpToken = await ethers.getContractAt("ERC20", await pool.token());

// Get pool TVL (sum of all coin balances in USD)
const tvl = await calculateTVL(pool);  // From previous section

// Get total LP supply
const totalSupply = await lpToken.totalSupply();

// LP token value
const lpTokenValue = tvl / (Number(totalSupply) / 1e18);
console.log(`1 LP token = $${lpTokenValue}`);
```

**Get user's liquidity position:**

```typescript
const userLpBalance = await lpToken.balanceOf(userAddress);
const userLpBalanceDecimal = Number(userLpBalance) / 1e18;

// User's share of pool
const totalSupply = await lpToken.totalSupply();
const sharePercent = (userLpBalance * 10000n) / totalSupply / 100;  // Percentage

// User's position value
const tvl = await calculateTVL(pool);
const userPositionValue = tvl * Number(userLpBalance) / Number(totalSupply);

console.log(`User owns ${sharePercent}% of pool = $${userPositionValue}`);
```

### APY & Yield

Pool yields come from up to three sources, though not all pools have all components:

#### 1. Trading Fee APY (Always Present)

Trading fees increase the virtual price over time. Calculate fee APY on-chain:

```typescript
// Snapshot virtual price at two points in time
const virtualPrice1 = await pool.get_virtual_price({ blockTag: block1 });
const virtualPrice2 = await pool.get_virtual_price({ blockTag: block2 });
const timestamp1 = (await provider.getBlock(block1)).timestamp;
const timestamp2 = (await provider.getBlock(block2)).timestamp;

// Calculate APY from fee accumulation
const timeDelta = timestamp2 - timestamp1;
const periodsPerYear = (365 * 24 * 60 * 60) / timeDelta;
const returnRate = Number(virtualPrice2) / Number(virtualPrice1);

// APY = (1 + periodReturn)^periodsPerYear - 1
const feeApy = Math.pow(returnRate, periodsPerYear) - 1;
const feeApyPercent = feeApy * 100;
```

**Or use Curve API:**

```bash
GET https://api.curve.finance/v1/getBaseApys/{blockchainId}
# e.g., ethereum, arbitrum, optimism, etc.
```

**Example Response:**
```json
{
  "success": true,
  "data": {
    "baseApys": [
      {
        "address": "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
        "latestDailyApyPcent": 0.42,      // Daily fee APY (%)
        "latestWeeklyApyPcent": 0.42,     // Weekly fee APY (%)
        "additionalApyPcentFromLsts": null
      }
    ]
  }
}
```

#### 2. CRV Emissions APY (If Pool Has Gauge)

Pools with active liquidity gauges receive **CRV token emissions**. The APY varies based on user's **veCRV boost**:

- **No boost (1x)**: Base CRV APY
- **Max boost (2.5x)**: 2.5× the base CRV APY

Users lock CRV for veCRV to increase their boost multiplier, earning more CRV rewards on the same LP position.

**Get CRV APY from Curve API:**

```bash
GET https://api.curve.finance/v1/getAllGauges
```

**Example Response (excerpt):**
```json
{
  "frxUSD+sUSDS": {
    "blockchainId": "ethereum",
    "gauge": "0x52618c40ddba3cbbb69f3aaa4cb26ae649844b17",
    "gauge_data": {
      "inflation_rate": "3663926723928765860",
      "working_supply": "4423560386106747416297149"
    },
    "gaugeCrvApy": [1.29, 3.22],  // [minApy, maxApy] based on boost
    "gaugeFutureCrvApy": [1.29, 3.22],
    "lpTokenPrice": 1.0257,
    "is_killed": false,
    "hasNoCrv": false,
    "swap": "0x81a2612f6dea269a6dd1f6deab45c5424ee2c4b7"
  }
}
```

**Key Fields:**
- **`gaugeCrvApy`**: `[minApy, maxApy]` - CRV APY range
  - `minApy`: APY with no veCRV boost (1.0×)
  - `maxApy`: APY with maximum veCRV boost (2.5×)
- **`hasNoCrv`**: `true` if gauge exists but has no CRV emissions
- **`is_killed`**: `true` if gauge is deprecated (no more rewards)

**Note:** Not all pools have gauges. Pools without a gauge will not appear in this endpoint or will have `hasNoCrv: true`.

#### 3. Additional Rewards (Pool-Specific)

Some pools have external incentives from protocols (e.g., SDT, FXS, CVX). These vary by pool and would need to be queried from:
- Gauge reward token contracts
- Third-party incentive platforms

**Total APY:**

```
Total APY = Trading Fee APY + CRV Emission APY + Additional Rewards APY
```

**Important:**
- **Trading Fee APY**: Present in all active pools
- **CRV Emission APY**: Only for pools with active, non-killed gauges
- **Additional Rewards**: Only for specific pools with external incentives

Use Curve API endpoints above for the easiest data retrieval.

---

## Smart Contract Methods

### Core View Methods

#### `coins(uint256 i) → address`

Returns the address of coin at index `i` (0-indexed).

```typescript
const usdc = await pool.coins(0);
const usdt = await pool.coins(1);
```

#### `balances(uint256 i) → uint256`

Returns the balance of coin at index `i` in the pool (excluding admin fees).

```typescript
const usdcBalance = await pool.balances(0);  // Returns amount in token's decimals
```

#### `get_balances() → uint256[]`

Returns array of all coin balances in the pool.

```typescript
const balances = await pool.get_balances();
// [balance0, balance1, balance2, ...]
```

**Note:** Balances do not include admin fees and are the actual tradeable amounts in the pool.

#### `get_dy(int128 i, int128 j, uint256 dx) → uint256`

Calculates the amount of coin `j` received for swapping `dx` amount of coin `i` (including fees).

```typescript
// How much USDT for 1000 USDC?
const usdtOut = await pool.get_dy(
  0,        // USDC index (int128)
  1,        // USDT index (int128)
  1000e6    // 1000 USDC (6 decimals)
);
// Returns amount in USDT decimals
```

**Use cases:**
- Price discovery
- Slippage calculation
- Quote for swaps

#### `get_virtual_price() → uint256`

Returns the current virtual price of the pool's LP token (18 decimals).

```typescript
const virtualPrice = await pool.get_virtual_price();
// 1000000000000000000 = 1.0
// 1002345678901234567 = 1.002345... (0.23% increase from fees)
```

Virtual price increases monotonically as trading fees accumulate.

#### `N_COINS() → uint256`

Returns the total number of coins in the pool.

```typescript
const nCoins = await pool.N_COINS();  // e.g., 3 for USDC/USDT/DAI
```

#### `totalSupply() → uint256`

Returns the total supply of LP tokens (18 decimals).

```typescript
const lpToken = await ethers.getContractAt("ERC20", await pool.token());
const totalSupply = await lpToken.totalSupply();
```

#### `A() → uint256`

Returns the current amplification coefficient.

```typescript
const A = await pool.A();
// Returns A * n^(n-1) where n = number of coins
// For precise A, divide by n^(n-1)
```

#### `fee() → uint256`

Returns the current swap fee (10 decimals = 100%).

```typescript
const fee = await pool.fee();
// 4000000 = 0.04% = 4 bps
const feePercent = Number(fee) / 1e10 * 100;
```

### State-Changing Methods (For Reference)

These methods modify pool state but are useful to understand pool mechanics:

#### `exchange(int128 i, int128 j, uint256 dx, uint256 min_dy) → uint256`

Swap coin `i` for coin `j`.

#### `add_liquidity(uint256[] amounts, uint256 min_mint_amount) → uint256`

Add liquidity and receive LP tokens.

#### `remove_liquidity(uint256 amount, uint256[] min_amounts) → uint256[]`

Remove liquidity proportionally across all coins.

#### `remove_liquidity_one_coin(uint256 amount, int128 i, uint256 min_amount) → uint256`

Remove liquidity in a single coin.

### Oracle Methods

#### `last_price(uint256 i) → uint256`

Returns the last price of coin `i+1` relative to coin `0` (exponential moving average).

```typescript
const lastPrice = await pool.last_price(0);  // Price of coin(1) in terms of coin(0)
// Returns 18 decimal value
```

**Note:** Stableswap-NG includes built-in price oracles using exponential moving averages (EMA).

#### `price_oracle(uint256 i) → uint256`

Returns the oracle price for coin `i+1` relative to coin `0`.

```typescript
const oraclePrice = await pool.price_oracle(0);  // EMA price, less manipulation-prone
```

**Use for:** More reliable price data that's resistant to manipulation compared to spot prices.

---

## References

### Official Documentation

- **Curve Finance:** https://curve.fi/
- **Curve Docs:** https://docs.curve.fi/
- **Stableswap-NG Overview:** https://docs.curve.finance/stableswap-exchange/stableswap-ng/overview/
- **Plain Pools Documentation:** https://docs.curve.finance/stableswap-exchange/stableswap-ng/pools/plainpool/
- **Stableswap Whitepaper:** https://curve.fi/files/stableswap-paper.pdf

### Technical Resources

- **GitHub Repository:** https://github.com/curvefi/stableswap-ng
- **Source Code (Main Pool):** https://github.com/curvefi/stableswap-ng/blob/main/contracts/main/CurveStableSwapNG.vy
- **Pool Factory:** https://github.com/curvefi/stableswap-ng/blob/main/contracts/main/CurveStableSwapFactoryNG.vy
- **ERC-4626 Standard:** https://eips.ethereum.org/EIPS/eip-4626

### Data & Analytics

- **Curve API:** https://api.curve.fi/
- **DefiLlama (Curve):** https://defillama.com/protocol/curve-dex
- **Dune Analytics:** https://dune.com/queries?category=canonical&namespace=curve

### Integration Resources

- **Curve Registry:** Provides pool discovery and metadata
  - MetaRegistry API: https://docs.curve.finance/registry/overview/
- **Address Provider:** Central registry for Curve contract addresses
  - Docs: https://docs.curve.finance/integration/address-provider/

### Community & Support

- **Discord:** https://discord.gg/rgrfS7W
- **Telegram:** https://t.me/curvefi
- **Forum:** https://gov.curve.fi/
- **Twitter:** https://twitter.com/CurveFinance

---

**Document Version:** 1.0  
**Last Updated:** January 2026

**Contributors:** Technical specification based on Curve Finance Stableswap-NG documentation and smart contract interfaces.

**Notes:**
- APY calculation section requires further research for CRV emissions tracking
- Pool discovery via factory/registry not covered (focused on data retrieval from known pools)
- Gauge integration and boost mechanics not fully documented (affects CRV APY calculation)

