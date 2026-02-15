# Uniswap V3 Protocol Specification

**Version:** 1.0  
**Last Updated:** February 2026  
**Purpose:** Technical reference for retrieving composition, TVL, price, and yield data from Uniswap V3 pools

---

## Table of Contents

1. [Protocol Overview](#protocol-overview)
2. [Core Concepts & Mechanics](#core-concepts--mechanics)
3. [Retrieving Key Data](#retrieving-key-data)
4. [Smart Contract Methods](#smart-contract-methods)
5. [References](#references)

---

## Protocol Overview

### What is Uniswap V3?

Uniswap V3 is a **non-custodial, automated market maker (AMM)** protocol that enables decentralized trading of ERC-20 tokens. Launched in May 2021, Uniswap V3 introduces **concentrated liquidity**, allowing liquidity providers to allocate capital within custom price ranges for improved capital efficiency.

**Key Features:**
- **Concentrated liquidity**: LPs can specify price ranges for their liquidity
- **Multiple fee tiers**: 0.01%, 0.05%, 0.30%, and 1.00% fee options per pool
- **NFT positions**: Each liquidity position is represented as a unique NFT
- **Flexible range orders**: Use concentrated positions as limit orders
- **Built-in oracle**: Time-weighted average price (TWAP) oracle in every pool

**Documentation:** https://docs.uniswap.org/

### Architecture

**Core Contracts:**
```
Users ↔ Trade in pools ↔ Pay swap fee
         ↓
    Fees distributed to:
    - Active Liquidity Providers (in their price range)
         ↓
    LPs earn:
    - Trading fees (proportional to in-range liquidity)
    - Position is represented as NFT (NonfungiblePositionManager)
```

**Contract Structure:**
- **UniswapV3Factory**: Deploys new pool contracts
- **UniswapV3Pool**: Individual AMM pool for a token pair and fee tier
- **NonfungiblePositionManager**: Manages LP positions as NFTs
- **SwapRouter**: Executes token swaps (single and multi-hop)

### Deployments

Uniswap V3 is deployed across multiple chains:
- **Ethereum Mainnet** (v1.0.0+)
- **Polygon**, **Arbitrum**, **Optimism**, **Base**, **Unichain**, **Avalanche**, and more

Each deployment has independent factory and pool contracts.

**Contract Addresses:**
- Factory, Router, and NonfungiblePositionManager addresses: https://docs.uniswap.org/contracts/v3/reference/deployments

---

## Core Concepts & Mechanics

### Concentrated Liquidity

Unlike Uniswap V2 (which distributes liquidity from 0 to ∞), **Uniswap V3 allows LPs to concentrate liquidity within specific price ranges**.

**Example:**
- **V2**: LP provides liquidity for ETH/USDC across entire price curve (0 to ∞)
- **V3**: LP provides liquidity for ETH/USDC only between $1,800 and $2,200

**Benefits:**
- **Higher capital efficiency**: Same liquidity depth with less capital
- **Higher fees**: LPs earn more fees per dollar of capital (when price is in range)
- **Customizable exposure**: Choose risk/reward profile via range width

**Trade-off:**
- **Impermanent loss risk**: Positions can go "out of range" if price moves outside the specified range
- **Active management**: May need to adjust ranges as price moves

### Ticks & Price Ranges

Uniswap V3 represents prices using **ticks**, which are discrete price points on a logarithmic scale.

**Tick Math:**
```
price = 1.0001^tick

Example:
- tick = 0     → price = 1.0
- tick = 10000 → price ≈ 2.7183 (e)
- tick = -10000 → price ≈ 0.3679 (1/e)
```

**Price Range:**
- Each position has `tickLower` and `tickUpper`
- Liquidity is active only when current tick is between these bounds
- Tick spacing varies by fee tier (e.g., 1% fee tier = 200 tick spacing)

**Current Price:**
```solidity
// Pool stores current price as sqrtPriceX96
// To get actual price (token1/token0):
price = (sqrtPriceX96 / 2^96)^2
```

### Fee Tiers

Each pool is unique per token pair AND fee tier:

| Fee Tier | Tick Spacing | Typical Use Case |
|----------|--------------|------------------|
| 0.01% | 1 | Stablecoin pairs (USDC/USDT) |
| 0.05% | 10 | Correlated pairs (ETH/stETH) |
| 0.30% | 60 | Most pairs (standard) |
| 1.00% | 200 | Exotic pairs (high volatility) |

**Example:** WETH/USDC can have 4 separate pools (one per fee tier), each with independent liquidity and prices.

### Liquidity & Position Value

**Liquidity (`L`):**
- Represents the amount of virtual `sqrt(xy)` provided by a position
- Calculated from token amounts and price range
- Pool tracks total liquidity at each tick

**Position Value:**

When price is within range, a position holds both token0 and token1:

```
amount0 = L × (sqrt(priceUpper) - sqrt(priceCurrent)) / (sqrt(priceCurrent) × sqrt(priceUpper))
amount1 = L × (sqrt(priceCurrent) - sqrt(priceLower))

Position Value USD = amount0 × price0 + amount1 × price1
```

**When out of range:**
- If price > upper: Position is 100% token0
- If price < lower: Position is 100% token1

### NFT Positions

Unlike V2's fungible LP tokens, **V3 positions are non-fungible** because each has unique parameters:
- Token pair
- Fee tier
- Price range (tickLower, tickUpper)
- Liquidity amount

Each position is represented as an **ERC-721 NFT** via the `NonfungiblePositionManager` contract.

---

## Retrieving Key Data

### Finding Pool Addresses

**Method 1: Using Factory Contract**

```typescript
const factory = new ethers.Contract(FACTORY_ADDRESS, [
  "function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool)"
], provider);

// Get pool address
const poolAddress = await factory.getPool(
  WETH_ADDRESS,   // token0 or token1
  USDC_ADDRESS,   // token1 or token0
  3000            // fee tier (3000 = 0.30%)
);

console.log(`Pool address: ${poolAddress}`);
```

**Fee tier values:**
- `100` = 0.01%
- `500` = 0.05%
- `3000` = 0.30%
- `10000` = 1.00%

**Method 2: Using Uniswap SDK**

```typescript
import { Pool, FeeAmount } from '@uniswap/v3-sdk';
import { Token, ChainId } from '@uniswap/sdk-core';

const USDC = new Token(ChainId.MAINNET, USDC_ADDRESS, 6, 'USDC');
const WETH = new Token(ChainId.MAINNET, WETH_ADDRESS, 18, 'WETH');

// Pool address is deterministic
const poolAddress = Pool.getAddress(USDC, WETH, FeeAmount.MEDIUM);
```

**Factory Addresses:**
- Ethereum: `0x1F98431c8aD98523631AE4a59f267346ea31F984`
- See all chains: https://docs.uniswap.org/contracts/v3/reference/deployments

### Pool Composition & State

**Get pool state:**

```typescript
const pool = new ethers.Contract(poolAddress, [
  "function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, ...)",
  "function liquidity() external view returns (uint128)"
], provider);

const slot0 = await pool.slot0();
const currentLiquidity = await pool.liquidity();

console.log(`Current tick: ${slot0.tick}`);
console.log(`Active liquidity: ${currentLiquidity}`);
```

**Pool Internal Price (token1/token0):**

The pool stores its internal exchange rate as `sqrtPriceX96`. This is the price within this specific pool pair:

```typescript
// Calculate pool's internal price (token1/token0)
const price = (Number(slot0.sqrtPriceX96) / (2 ** 96)) ** 2;
```

**Note:** For TVL calculations, use external price oracles (e.g., Chainlink) rather than pool-internal prices, which can be subject to manipulation or deviation.

### Pool TVL (Total Value Locked)

Pool TVL requires determining the total token holdings in the pool. There are two approaches:

**Method 1: Token Balance**

Check pool contract's token balances (includes unclaimed fees):

```typescript
const token0Contract = new ethers.Contract(token0Address, [
  "function balanceOf(address) view returns (uint256)",
  "function decimals() view returns (uint8)"
], provider);

const token1Contract = new ethers.Contract(token1Address, [
  "function balanceOf(address) view returns (uint256)",
  "function decimals() view returns (uint8)"
], provider);

const balance0 = await token0Contract.balanceOf(poolAddress);
const balance1 = await token1Contract.balanceOf(poolAddress);
const decimals0 = await token0Contract.decimals();
const decimals1 = await token1Contract.decimals();

// Get token prices (from oracle or external source)
const price0USD = await getTokenPrice(token0Address);  // e.g., from oracle
const price1USD = await getTokenPrice(token1Address);

// Calculate Pool TVL in USD
const tvl0 = (Number(balance0) / (10 ** decimals0)) * price0USD;
const tvl1 = (Number(balance1) / (10 ** decimals1)) * price1USD;
const tvlTotal = tvl0 + tvl1;

console.log(`Pool TVL: $${tvlTotal.toFixed(2)}`);
```


**Method 2: Using Uniswap Subgraph**

Query the Uniswap V3 subgraph for pre-indexed TVL data:

```graphql
{
  pool(id: "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8") {
    token0 {
      symbol
      decimals
    }
    token1 {
      symbol
      decimals
    }
    totalValueLockedToken0
    totalValueLockedToken1
    totalValueLockedUSD
    feeTier
    liquidity
    sqrtPrice
    tick
  }
}
```

**Subgraph endpoints (requires API key):**

To use subgraphs, you need to:
1. Create an API key at [The Graph Studio](https://thegraph.com/studio/)
2. Use the gateway endpoint with your key

- **Ethereum Mainnet**: `https://gateway.thegraph.com/api/<YOUR_API_KEY>/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV`
- **Other chains**: See [Uniswap Subgraph Documentation](https://docs.uniswap.org/api/subgraph/overview)

### Position Value & User Liquidity

**Understanding Position NFTs:**

Each liquidity position in Uniswap V3 is represented as an ERC-721 NFT with a unique `tokenId`. To query a position, you first need to discover the tokenId.

**Method 1: Get tokenIds owned by an address**

```typescript
const positionManager = new ethers.Contract(NFT_POSITION_MANAGER_ADDRESS, [
  "function balanceOf(address owner) external view returns (uint256)",
  "function tokenOfOwnerByIndex(address owner, uint256 index) external view returns (uint256)"
], provider);

const userAddress = "0x...";
const balance = await positionManager.balanceOf(userAddress);

// Get all tokenIds owned by user
const tokenIds = [];
for (let i = 0; i < balance; i++) {
  const tokenId = await positionManager.tokenOfOwnerByIndex(userAddress, i);
  tokenIds.push(tokenId);
}

console.log(`User owns ${tokenIds.length} positions:`, tokenIds);
```

**Method 2: Listen to IncreaseLiquidity/DecreaseLiquidity events**

Position NFTs are minted when liquidity is first added. You can discover tokenIds by indexing `IncreaseLiquidity` events from the NonfungiblePositionManager contract.

**Get position data:**

Once you have a tokenId, query the position details:

```typescript
const positionManager = new ethers.Contract(NFT_POSITION_MANAGER_ADDRESS, [
  "function positions(uint256 tokenId) external view returns (uint96 nonce, address operator, address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128, uint128 tokensOwed0, uint128 tokensOwed1)"
], provider);

const tokenId = tokenIds[0];  // Use actual tokenId from discovery above
const position = await positionManager.positions(tokenId);

console.log('Position:', {
  token0: position.token0,
  token1: position.token1,
  fee: position.fee,
  tickLower: position.tickLower,
  tickUpper: position.tickUpper,
  liquidity: position.liquidity,
  tokensOwed0: position.tokensOwed0,  // Unclaimed fees
  tokensOwed1: position.tokensOwed1   // Unclaimed fees
});
```

**Calculate user's token amounts in position:**

The Uniswap V3 SDK provides helpers to calculate how many tokens a specific user has provided to the pool in their position:

```typescript
import { Pool, Position, JSBI } from '@uniswap/v3-sdk';
import { Token } from '@uniswap/sdk-core';

// Create Token objects
const token0 = new Token(chainId, position.token0, decimals0);
const token1 = new Token(chainId, position.token1, decimals1);

// Get pool address from factory
const factory = new ethers.Contract(FACTORY_ADDRESS, [
  "function getPool(address, address, uint24) external view returns (address)"
], provider);
const poolAddress = await factory.getPool(position.token0, position.token1, position.fee);

// Get pool state from contract
const poolContract = new ethers.Contract(poolAddress, [
  "function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, ...)",
  "function liquidity() external view returns (uint128)"
], provider);

const slot0 = await poolContract.slot0();
const liquidity = await poolContract.liquidity();

// Create Pool object
const poolSDK = new Pool(
  token0,
  token1,
  position.fee,
  slot0.sqrtPriceX96.toString(),
  liquidity.toString(),
  slot0.tick
);

// Create Position object
const positionSDK = new Position({
  pool: poolSDK,
  liquidity: position.liquidity.toString(),
  tickLower: position.tickLower,
  tickUpper: position.tickUpper
});

// Get user's token amounts in this position
const amount0 = positionSDK.amount0.toFixed();
const amount1 = positionSDK.amount1.toFixed();

console.log(`User's position contains: ${amount0} ${token0.symbol} + ${amount1} ${token1.symbol}`);
```

**Calculate user's position value in USD:**

```typescript
// Get current token amounts (from SDK above)
const amount0Decimal = Number(amount0) / (10 ** decimals0);
const amount1Decimal = Number(amount1) / (10 ** decimals1);

// Get unclaimed fees
const fees0Decimal = Number(position.tokensOwed0) / (10 ** decimals0);
const fees1Decimal = Number(position.tokensOwed1) / (10 ** decimals1);

// Get token prices
const price0USD = await getTokenPrice(token0Address);
const price1USD = await getTokenPrice(token1Address);

// Calculate USD value for each token including fees
const token0ValueUSD = (amount0Decimal + fees0Decimal) * price0USD;
const token1ValueUSD = (amount1Decimal + fees1Decimal) * price1USD;
const totalValueUSD = token0ValueUSD + token1ValueUSD;

console.log(`${token0.symbol} value (including fees): $${token0ValueUSD.toFixed(2)}`);
console.log(`${token1.symbol} value (including fees): $${token1ValueUSD.toFixed(2)}`);
console.log(`Total position value: $${totalValueUSD.toFixed(2)}`);
```

**Position Status:**

```typescript
// Check if position is in range
const poolContract = new ethers.Contract(poolAddress, [
  "function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, ...)"
], provider);

const slot0 = await poolContract.slot0();
const currentTick = slot0.tick;

const inRange = currentTick >= position.tickLower && currentTick < position.tickUpper;
console.log(`Position is ${inRange ? 'IN RANGE' : 'OUT OF RANGE'}`);

if (!inRange) {
  if (currentTick < position.tickLower) {
    console.log('Position is 100% token1');
  } else {
    console.log('Position is 100% token0');
  }
}
```

### Price Data & Oracles

Uniswap V3 pools have **built-in time-weighted average price (TWAP) oracles**.

**Get current price from slot0:**

```typescript
const slot0 = await pool.slot0();
const price = (Number(slot0.sqrtPriceX96) / (2 ** 96)) ** 2;  // token1/token0
```

**Get TWAP via observe:**

```typescript
const result = await pool.observe([1800, 0]);  // Last 30 min
const avgTick = (result.tickCumulatives[1] - result.tickCumulatives[0]) / 1800;
const twapPrice = 1.0001 ** avgTick;
```

**Note:** Pools store limited oracle history by default. Use `increaseObservationCardinalityNext()` to extend historical data availability.


### APY & Fee Earnings

**Approximate fee APR from historical volume:**

Fee earnings depend on:
1. Trading volume in the pool
2. Your share of in-range liquidity
3. Time your position is in range

**Note:** The calculations below provide an **approximation** based on historical data.

**Using Subgraph for Historical Data:**

```graphql
{
  pool(id: "0x...") {
    volumeUSD
    feeTier
    totalValueLockedUSD
  }
  
  # Get position-specific fees
  position(id: "tokenId") {
    collectedFeesToken0
    collectedFeesToken1
  }
}
```

**Formula:**
```
Daily Volume = pool.volumeUSD / days
Daily Fees = Daily Volume × (feeTier / 1000000)
Daily Fee APR = (Daily Fees / pool.TVL) × 365 × 100
```

**Example:**
- Pool TVL: $10M
- Daily Volume: $5M
- Fee: 0.30%
- Daily Fees: $5M × 0.003 = $15,000
- Fee APR approximation: ($15,000 / $10M) × 365 × 100 = 54.75%

**Important Considerations:**
- **Impermanent Loss:** Must be accounted for in net returns
- **Range Efficiency:** Out-of-range positions earn no fees
- **Fee APR is variable:** Based on real-time trading volume

---

## Smart Contract Methods

### UniswapV3Factory

**Contract Address:** See [Deployments](https://docs.uniswap.org/contracts/v3/reference/deployments)

#### `getPool(address tokenA, address tokenB, uint24 fee) → address`

Returns the pool address for a given token pair and fee tier.

```typescript
const poolAddress = await factory.getPool(tokenA, tokenB, 3000);
// Returns zero address if pool doesn't exist
```

### UniswapV3Pool

**Contract Address:** Obtained from Factory's `getPool()`

#### `token0() → address`

Returns the address of token0.

```typescript
const token0 = await pool.token0();
```

#### `token1() → address`

Returns the address of token1.

```typescript
const token1 = await pool.token1();
```

#### `fee() → uint24`

Returns the pool's fee in hundredths of a bip (0.30% = 3000).

```typescript
const fee = await pool.fee();  // 3000 = 0.30%
```

#### `slot0() → (uint160 sqrtPriceX96, int24 tick, ...)`

Returns the current pool state including price and tick.

```typescript
const slot0 = await pool.slot0();
const sqrtPriceX96 = slot0.sqrtPriceX96;
const currentTick = slot0.tick;
const observationCardinality = slot0.observationCardinality;
```

**Return values:**
- `sqrtPriceX96`: Current sqrt(price) in Q64.96 format
- `tick`: Current tick
- `observationIndex`: Index of last written observation
- `observationCardinality`: Current max observations stored
- `observationCardinalityNext`: Next max observations to store
- `feeProtocol`: Protocol fee as % of swap fee (0-10)
- `unlocked`: Whether pool is unlocked

#### `liquidity() → uint128`

Returns the currently active liquidity in the pool.

```typescript
const activeLiquidity = await pool.liquidity();
```

**Note:** This is the liquidity available at the current tick. Total pool liquidity may be higher.

#### `observe(uint32[] secondsAgos) → (int56[] tickCumulatives, uint160[] secondsPerLiquidityCumulativeX128s)`

Returns cumulative tick and liquidity values for given times in the past.

```typescript
// TWAP over last 10 minutes
const result = await pool.observe([600, 0]);  // [600s ago, now]
const tickCumulatives = result.tickCumulatives;
const avgTick = (tickCumulatives[1] - tickCumulatives[0]) / 600;
```

**Use for:** Time-weighted average price (TWAP) calculations.

#### `increaseObservationCardinalityNext(uint16 observationCardinalityNext)`

Increases the maximum number of price observations stored.

```typescript
await pool.increaseObservationCardinalityNext(200);  // Store up to 200 observations
```

**Note:** Anyone can call this (paying gas). Higher cardinality = more historical TWAP data available.

### NonfungiblePositionManager

**Contract Address:** See [Deployments](https://docs.uniswap.org/contracts/v3/reference/deployments)

#### `positions(uint256 tokenId) → (uint96 nonce, address operator, address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128, uint128 tokensOwed0, uint128 tokensOwed1)`

Returns position details for a given NFT token ID.

```typescript
const position = await positionManager.positions(tokenId);
console.log({
  liquidity: position.liquidity,
  tickLower: position.tickLower,
  tickUpper: position.tickUpper,
  unclaimedFees0: position.tokensOwed0,
  unclaimedFees1: position.tokensOwed1
});
```

---

## References

### Official Documentation

- **Uniswap Docs:** https://docs.uniswap.org/
- **Uniswap V3 Overview:** https://docs.uniswap.org/contracts/v3/overview
- **Concentrated Liquidity:** https://docs.uniswap.org/concepts/protocol/concentrated-liquidity
- **Uniswap V3 Whitepaper:** https://uniswap.org/whitepaper-v3.pdf

### GitHub Repositories

- **v3-core:** https://github.com/Uniswap/v3-core
- **v3-periphery:** https://github.com/Uniswap/v3-periphery
- **v3-sdk:** https://github.com/Uniswap/v3-sdk
- **Interface (Frontend):** https://github.com/Uniswap/interface

### Technical References

- **Contract Deployments:** https://docs.uniswap.org/contracts/v3/reference/deployments
- **Core Reference:** https://docs.uniswap.org/contracts/v3/reference/core/UniswapV3Pool
- **NonfungiblePositionManager:** https://docs.uniswap.org/contracts/v3/reference/periphery/NonfungiblePositionManager
- **Oracle Documentation:** https://docs.uniswap.org/concepts/protocol/oracle
- **Tick Math:** https://github.com/Uniswap/v3-core/blob/main/contracts/libraries/TickMath.sol

### Data & Analytics

- **Uniswap Subgraph:** https://docs.uniswap.org/api/subgraph/overview (requires API key)
- **Uniswap Info (Analytics):** https://info.uniswap.org/
- **DefiLlama (Uniswap V3):** https://defillama.com/protocol/uniswap-v3
- **Dune Analytics:** https://dune.com/browse/dashboards?q=uniswap

### Integration Resources

- **SDK Documentation:** https://docs.uniswap.org/sdk/v3/overview
- **Swap Widget:** https://docs.uniswap.org/sdk/widgets/swap-widget
- **Smart Contract Integration:** https://docs.uniswap.org/contracts/v3/guides/swaps/single-swaps

### Community & Support

- **Discord:** https://discord.gg/uniswap
- **Forum:** https://gov.uniswap.org/
- **Twitter:** https://twitter.com/Uniswap

---

**Document Version:** 1.0  
**Last Updated:** February 2026

**Contributors:** Technical specification based on Uniswap V3 documentation, smart contract interfaces, and official SDK.

