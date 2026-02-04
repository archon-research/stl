# Morpho Protocol Specification

**Version:** 1.0  
**Last Updated:** January 2026  
**Purpose:** Technical reference for understanding Morpho protocol mechanics and indexing implementation

---

## Table of Contents

1. [Protocol Overview](#protocol-overview)
2. [Core Concepts & Mechanics](#core-concepts--mechanics)
3. [Smart Contracts](#smart-contracts)
4. [Data Availability & Indexing Constraints](#data-availability--indexing-constraints)
5. [Indexing Methodology](#indexing-methodology)
6. [Implementation Best Practices](#implementation-best-practices)
7. [References](#references)

---

## Protocol Overview

### What is Morpho?

Morpho is a **modular lending protocol** consisting of two core components:

1. **Morpho Blue**: A minimal, immutable, and efficient lending primitive with isolated markets
2. **MetaMorpho**: Vault infrastructure that aggregates and allocates capital across Morpho Blue markets

The protocol operates through smart contracts on Ethereum and other EVM-compatible chains, with no intermediaries holding user funds.

### Key Features

- **Isolated Markets (Morpho Blue)**: Each market is independent with its own risk parameters
- **Vault Aggregation (MetaMorpho)**: Curators manage capital allocation across multiple markets
- **Share-Based Accounting**: Interest accrues automatically through share appreciation
- **Permissionless Market Creation**: Anyone can create a new Morpho Blue market
- **Flash Loans**: Uncollateralized loans repayable within the same transaction
- **No Governance Dependencies**: Morpho Blue is immutable and governance-free

### Protocol Architecture

**Two-Layer Design:**

Morpho has a unique two-layer architecture that separates risk management from lending primitives:

#### Layer 1: Morpho Blue (Lending Primitive)

- **Isolated markets** with independent risk parameters
- **Market Definition**: Each market is uniquely identified by 5 immutable parameters:
  - `loanToken`: Asset that can be borrowed
  - `collateralToken`: Asset accepted as collateral
  - `oracle`: Price oracle contract
  - `irm`: Interest Rate Model contract
  - `lltv`: Liquidation Loan-to-Value (max borrowable %)
- Permissionless market creation
- No pooled liquidity - each market operates independently
- Market ID: `keccak256(loanToken, collateralToken, oracle, irm, lltv)`

#### Layer 2: MetaMorpho (Vault Infrastructure)

- **Vaults** that aggregate capital and allocate across multiple Morpho Blue markets
- Curated risk management by vault operators
- ERC-4626 compliant (standard tokenized vault interface)
- Two major versions with different architectures:
  - **MetaMorpho V1/V1.1**: Direct allocation to Morpho Blue markets only
  - **MetaMorpho V2**: Adapter-based architecture supporting multiple yield sources

**Multi-Chain Deployments:**

Morpho is deployed across multiple blockchain networks, including:
- Ethereum Mainnet
- Base
- And others (deployment varies by component)

Each chain hosts independent deployments of both Morpho Blue and MetaMorpho vaults.

**Key Architectural Difference from Aave:**

Unlike Aave's **pooled liquidity model** where all suppliers share risk across multiple assets, Morpho Blue uses **isolated markets** where each lending market is completely independent. MetaMorpho vaults then provide the aggregation layer, similar to how Aave pools work, but with explicit curator-controlled allocation.

```
Aave V3:          Multi-collateral Pool → Shared Risk
Morpho:           Isolated Markets → Vault Aggregation → Curated Risk
```

### Protocol Versions

**Morpho Blue:**
- Single immutable version (deployed December 2023)
- No upgrades or governance
- Consistent interface across all deployments

**MetaMorpho Vaults:**

- **V1 (Early 2024)**: Initial vault implementation
  - Direct allocation to Morpho Blue markets
  - Single performance fee
  - Supply/withdraw queues for capital flow management

- **V1.1 (Mid 2024)**: Enhanced V1 with optimizations
  - Same core architecture as V1
  - Event signature differences (simplified `AccrueInterest`)
  - Most production vaults use V1.1

- **V2 (Late 2024)**: Complete redesign
  - **Adapter-based architecture** supporting multiple yield sources
  - Can allocate to Morpho Blue markets AND MetaMorpho V1 vaults
  - Separate performance and management fees
  - Enhanced role separation (Owner, Curator, Allocator, Sentinel)
  - Granular ID & Cap system for risk management
  - Automatic bad debt tracking

**Version Detection in Indexing:**

When indexing, distinguish vault versions by:
- **V1.1**: `AccrueInterest(uint256 newTotalAssets, uint256 feeShares)`
- **V2**: `AccrueInterest(uint256 previousTotalAssets, uint256 newTotalAssets, uint256 performanceFeeShares, uint256 managementFeeShares)`

**Unlisted Vaults:**

Some MetaMorpho vaults are **unlisted** and do not appear on the official Morpho UI. These vaults are fully functional but may be:
- Experimental or testing vaults
- Private/institutional vaults
- Deprecated or migrated vaults

**References:**
- Morpho Blue Repository: https://github.com/morpho-org/morpho-blue
- MetaMorpho V1 Repository: https://github.com/morpho-org/metamorpho
- MetaMorpho V2 Repository: https://github.com/morpho-org/vault-v2

---

## Core Concepts & Mechanics

### Markets vs Vaults: Terminology

Understanding Morpho requires clear distinction between **Markets** and **Vaults**:

**Market (Morpho Blue):**
- A single isolated lending market in the Morpho Blue protocol
- Uniquely identified by its 5 immutable parameters (see Protocol Architecture)
- Users can supply, borrow, and supply collateral directly
- Example: "WETH/USDC market with 86% LLTV using Chainlink oracle"

**Vault (MetaMorpho):**
- An ERC-4626 vault that aggregates capital from depositors
- Allocates deposited assets across multiple Morpho Blue markets
- Managed by a curator who sets allocation strategies
- Example: "Gauntlet Core USDC Vault" that allocates USDC across multiple USDC lending markets

```
Users → Deposit to MetaMorpho Vault → Vault allocates to → Multiple Morpho Blue Markets
```

### Isolated Markets (Morpho Blue)

**What are Isolated Markets?**

Morpho Blue operates on a fundamentally different model than Aave:

**Morpho Blue Isolated Markets:**
- Each market is **completely independent** with no shared liquidity
- No cross-market contagion risk
- Each market tracks its own supply/borrow totals
- Borrowers can only use one collateral type per market

**Aave V3 Pooled Markets:**
- Single pool with multiple reserves
- Users can supply multiple assets as collateral simultaneously
- Borrowing power and health factors calculated across entire portfolio

**Market Creation:**

- **Permissionless**: Anyone can create a market with any parameter combination
- **Immutable**: Once created, parameters cannot be changed
- **Risk Isolation**: Bad parameters in one market don't affect others

### Share-Based Accounting (ERC-4626)

**ERC-4626 Standard:**

MetaMorpho vaults (V1 and V2) implement the **ERC-4626 Tokenized Vault Standard**, which defines a standardized interface for yield-bearing vaults. Morpho Blue markets use the same share-based accounting principles internally.

> **Note:** For complete ERC-4626 specification details, refer to the dedicated ERC-4626 specification document. This section covers the core mechanics relevant to Morpho indexing.

**The Assets vs Shares Model:**

ERC-4626 defines a **dual accounting system**:

**Assets:**
- The actual underlying token amounts (e.g., USDC, WETH)
- What users deposit and withdraw
- Example: "1000 USDC deposited"

**Shares:**
- ERC-20 tokens representing proportional vault ownership
- Increase/decrease only on deposits/withdrawals (via mint/burn)
- Appreciate in value as yield accrues to the vault

**Indexing Advantage:** Unlike Aave's continuously accruing balances that require "scaled balance" tracking optimizations, ERC-4626's share-based accounting is natively efficient for indexing. User shares only change on transactions (deposits, withdrawals, transfers), making event-based tracking straightforward without additional optimizations.

**ERC-4626 Core Functions:**

```solidity
// Deposit assets, receive shares
function deposit(uint256 assets, address receiver) returns (uint256 shares)
function mint(uint256 shares, address receiver) returns (uint256 assets)

// Withdraw assets, burn shares  
function withdraw(uint256 assets, address receiver, address owner) returns (uint256 shares)
function redeem(uint256 shares, address receiver, address owner) returns (uint256 assets)

// Conversion helpers
function convertToShares(uint256 assets) returns (uint256 shares)
function convertToAssets(uint256 shares) returns (uint256 assets)
```

**How It Works:**

```solidity
// On deposit:
shares = assets * totalShares / totalAssets
// User receives shares representing their proportional stake

// As yield accrues:
totalAssets increases (from interest/yield)
totalShares stays constant (unless new deposits/withdrawals)
// → Each share becomes worth more assets

// On withdrawal:
assets = shares * totalAssets / totalShares
// User receives more assets than they deposited (profit!)
```

**Example:**

```
Initial State:
  totalAssets = 1000 USDC
  totalShares = 1000

Alice deposits 100 USDC:
  shares = 100 * 1000 / 1000 = 100 shares
  Alice gets: 100 shares

Interest accrues (+10 USDC):
  totalAssets = 1110 USDC
  totalShares = 1100 (unchanged)
  
Alice withdraws:
  assets = 100 * 1110 / 1100 = 100.909 USDC
  Alice receives: 100.909 USDC (0.909 USDC profit)
```

**Share Price:**

The share price represents the exchange rate:

```
sharePrice = totalAssets / totalShares

WAD-precision (18 decimals):
sharePrice = (totalAssets * 10^18) / totalSupply
```

Share price starts at 1.0 and grows as interest accrues.

**ERC-4626 Security Considerations:**

**Inflation Attack Protection:**
All production MetaMorpho vaults include a "dead deposit" to prevent share price manipulation attacks. The initial shares are minted to address `0x000000000000000000000000000000000000dEaD`, making inflation attacks economically unfeasible.

**Indexing Note:** When indexing vaults, you may observe:
- Initial supply to dead address (typically 1e9+ shares)
- These shares are permanently locked and excluded from circulating supply
- This is expected behavior for secure ERC-4626 vaults

**Slippage Considerations:**
ERC-4626 functions don't include built-in slippage protection. Share price can shift between transaction submission and execution due to:
- Interest accrual (`AccrueInterest` events)
- Other user deposits/withdrawals
- Vault reallocation activities

For complete ERC-4626 mechanics, security patterns, and implementation details, see the dedicated ERC-4626 specification.

### Interest Rate Models

**Adaptive Curve IRM:**

Morpho Blue markets typically use adaptive interest rate models that adjust rates based on:

1. **Utilization Rate:**
   ```
   utilization = totalBorrowAssets / totalSupplyAssets
   ```

2. **Target Utilization:**
   - Models aim for an optimal utilization (e.g., 90%)
   - Rates increase exponentially when utilization exceeds target

3. **Dynamic Adjustment:**
   - Interest rates can adjust rapidly based on market conditions
   - No fixed slopes like Aave's two-slope model

**Interest Accrual:**

Interest accrues continuously in Morpho Blue:

```
Every transaction on a market:
1. Calculate time since last update
2. Apply interest rate for that duration
3. Update totalBorrowAssets (adds accrued interest)
4. Update totalSupplyAssets (adds accrued interest minus fees)
5. Mint fee shares to protocol
```

**Borrow vs Supply Rates:**

```
Supply Rate = Borrow Rate × Utilization × (1 - Fee)

Example with 5% borrow rate, 80% utilization, 10% fee:
Supply Rate = 5% × 0.80 × 0.90 = 3.6%
```

**Rate Updates:**

Unlike Aave where rates are stored in events, Morpho Blue calculates rates on-demand:
- Call `market()` to get market state at any block (requires archive node for historical queries)
- Calculate borrow rate using IRM contract with market parameters
- No `ReserveDataUpdated` equivalent (rates calculated on-demand, not stored in events)
- For efficient historical rate indexing, use Morpho API or implement periodic snapshots
- See [Data Availability & Indexing Constraints](#data-availability--indexing-constraints) for indexing strategies

### Liquidation Mechanics (Morpho Blue)

**When Liquidations Occur:**

Liquidations trigger when a borrower's collateral value falls below the LLTV threshold:

```
liquidatable if: (collateralValue × LLTV) < debtValue

LLTV (Liquidation Loan-to-Value) is the maximum allowed:
- 86% LLTV = Can borrow up to 86% of collateral value
- If debt reaches 86% of collateral, position is liquidatable
```

**Liquidation Process:**

1. **Liquidator calls** `liquidate(marketParams, borrower, seizedAssets, repaidShares, data)`

2. **Protocol validates:**
   - Position is liquidatable (debt ≥ collateral × LLTV)
   - Liquidator has assets to repay
   - Market has sufficient collateral to seize

3. **Protocol executes:**
   - Repays specified debt shares
   - Transfers collateral to liquidator (with bonus)
   - Calculates bad debt if collateral insufficient
   - Emits `Liquidate` event

**Liquidation Incentive:**

The liquidation bonus is built into the LLTV:

```
LLTV = 86% means:
- Liquidation Threshold: 86%
- Liquidation Bonus: ~14% buffer

If collateralValue = $1000, debtValue = $860:
- Position is liquidatable
- Liquidator repays $860
- Liquidator receives: collateral worth more than $860
```

**Bad Debt:**

Unlike Aave, Morpho Blue explicitly tracks bad debt:

```solidity
event Liquidate(
    bytes32 indexed id,
    address indexed caller,
    address indexed borrower,
    uint256 repaidAssets,
    uint256 repaidShares,
    uint256 seizedAssets,
    uint256 badDebtAssets,   // Non-zero if collateral insufficient
    uint256 badDebtShares
);
```

When collateral is insufficient to cover debt:
- `badDebtAssets` and `badDebtShares` are non-zero
- Suppliers absorb the loss proportionally
- Market continues operating normally

### MetaMorpho Vault Mechanics

**Vault Structure:**

MetaMorpho vaults are ERC-4626 compliant vaults that:
- Accept deposits of a single asset (e.g., USDC)
- Allocate capital across multiple Morpho Blue markets
- Distribute yield to depositors via share appreciation
- Charge performance and/or management fees

**Vault Roles:**

| Role | V1.1 | V2 | Responsibilities |
|------|------|-----|------------------|
| **Owner** | ✅ | ✅ | Ultimate authority, can change all parameters |
| **Curator** | ✅ | ✅ | Sets supply/withdraw queues, manages caps |
| **Guardian** | ✅ | ❌ | Emergency pause (V1.1 only) |
| **Allocator** | ✅ | ✅ | Executes capital reallocation between markets |
| **Sentinel** | ❌ | ✅ | Emergency force deallocate (V2 only) |

**Allocation Strategies:**

**V1/V1.1: Direct Market Allocation**

```
Vault → directly supply to → Morpho Blue Markets
```

- Vault holds Morpho Blue supply shares directly
- `ReallocateSupply` event when capital moves to a market
- `ReallocateWithdraw` event when capital moves from a market
- Supply/withdraw queues control allocation order

**V2: Adapter-Based Allocation**

```
Vault → allocate through Adapters → Multiple yield sources
       ↓
       - MorphoMarketV1AdapterV2 → Morpho Blue Markets
       - MorphoVaultV1Adapter → MetaMorpho V1 Vaults
       - Future adapters → Other protocols
```

- Vault allocates capital to **Adapters**, not directly to markets
- Each adapter implements `realAssets()` to report current value
- `Allocate` event when capital allocated to adapter
- `Deallocate` event when capital withdrawn from adapter
- Much more flexible, can compose strategies

**Position Backing in MetaMorpho:**

MetaMorpho provides **direct, transparent position backing** unlike pooled lending protocols:

**For Vault Suppliers:**
- Your position backing = Vault's market allocation composition
- If vault allocates: 40% Market A, 35% Market B, 25% Market C
- Your supply is backed by those same proportions of each market's borrower collateral

**Example:**
```
Alice supplies 10,000 USDC to "Gauntlet Core USDC Vault"

Vault Allocation:
- 60% to WETH/USDC market (borrowers using WETH as collateral)
- 30% to wstETH/USDC market (borrowers using wstETH as collateral)
- 10% to WBTC/USDC market (borrowers using WBTC as collateral)

Alice's Position Backing:
- 60% backed by WETH collateral (from WETH/USDC borrowers)
- 30% backed by wstETH collateral (from wstETH/USDC borrowers)
- 10% backed by WBTC collateral (from WBTC/USDC borrowers)
```

This is dramatically simpler than Aave/Sparklend where:
- Multiple borrowers can use different collateral combinations
- Must analyze each borrower's collateral mix individually
- Must weight by borrow amounts and multiple borrow types
- Requires complex aggregation across all borrowers

**For Direct Morpho Blue Suppliers:**
Even simpler - supply directly to an isolated market:
- Supply to "WETH/USDC 86% LLTV" market
- Backed 100% by WETH collateral from that market's borrowers

**Supply and Withdraw Queues (V1/V1.1):**

V1/V1.1 vaults use **ordered queues** to manage capital flow:

**Supply Queue:**
```
[marketId1, marketId2, marketId3, ...]
```
- When users deposit, vault allocates following this order
- Capital flows to marketId1 first (up to its cap)
- Then marketId2, then marketId3, etc.
- Idle liquidity left in vault if all markets at cap

**Withdraw Queue:**
```
[marketId3, marketId2, marketId1, ...]
```
- When users withdraw, vault withdraws following this order
- Typically reverse of supply queue (LIFO strategy)
- Curator sets these queues via timelock

**Supply Caps:**

Each market in a vault has a **supply cap** limiting exposure:

```
supplyPosition ≤ supplyCap
```

- Prevents over-concentration in a single market
- Curator can update caps via timelock
- Critical risk management tool

### Fee Structure

**V1/V1.1 Vaults:**

- Single **performance fee** (0-50%)
- Applied to interest earned (not deposits)
- Fee shares minted to `feeRecipient`

```
Interest Earned: 100 USDC
Performance Fee (10%): 10 USDC worth of shares minted
Depositors receive: 90 USDC worth of accrued value
```

**V2 Vaults:**

Two separate fee types:

1. **Performance Fee** (0-50%)
   - Applied to interest/profit earned
   - Minted as shares to `performanceFeeRecipient`

2. **Management Fee** (0-5% annually)
   - Time-based fee on total assets
   - Accrues continuously (not just on profits)
   - Minted as shares to `managementFeeRecipient`

```
Total Interest Earned: 100 USDC
Performance Fee (10%): 10 USDC worth of shares
Management Fee (2% annual, prorated): 0.5 USDC worth of shares
Depositors receive: 89.5 USDC worth of accrued value
```

**AccrueInterest Event:**

Fees are charged during the `AccrueInterest` event which occurs on most vault interactions:

```solidity
// V1.1
event AccrueInterest(uint256 newTotalAssets, uint256 feeShares);

// V2
event AccrueInterest(
    uint256 previousTotalAssets,
    uint256 newTotalAssets,
    uint256 performanceFeeShares,
    uint256 managementFeeShares
);
```

## Smart Contracts

### Morpho Blue Contract

**Purpose:** Core immutable lending primitive. Single contract for all markets.

**Core Functions:**

```solidity
// Market creation
function createMarket(MarketParams memory params) returns (bytes32 marketId)

// Supply to market
function supply(
    MarketParams memory params,
    uint256 assets,
    uint256 shares,
    address onBehalf,
    bytes memory data
) returns (uint256, uint256)

// Withdraw from market
function withdraw(
    MarketParams memory params,
    uint256 assets,
    uint256 shares,
    address onBehalf,
    address receiver
) returns (uint256, uint256)

// Supply collateral
function supplyCollateral(
    MarketParams memory params,
    uint256 assets,
    address onBehalf,
    bytes memory data
)

// Borrow against collateral
function borrow(
    MarketParams memory params,
    uint256 assets,
    uint256 shares,
    address onBehalf,
    address receiver
) returns (uint256, uint256)

// Repay borrow
function repay(
    MarketParams memory params,
    uint256 assets,
    uint256 shares,
    address onBehalf,
    bytes memory data
) returns (uint256, uint256)

// Liquidate position
function liquidate(
    MarketParams memory params,
    address borrower,
    uint256 seizedAssets,
    uint256 repaidShares,
    bytes memory data
) returns (uint256, uint256)

// Flash loan
function flashLoan(
    address token,
    uint256 assets,
    bytes memory data
)

// Read market state
function market(bytes32 id) returns (Market memory)

// Read user position
function position(bytes32 id, address user) returns (Position memory)
```

**Key Structs:**

```solidity
struct MarketParams {
    address loanToken;
    address collateralToken;
    address oracle;
    address irm;
    uint256 lltv;
}

struct Market {
    uint128 totalSupplyAssets;
    uint128 totalSupplyShares;
    uint128 totalBorrowAssets;
    uint128 totalBorrowShares;
    uint128 lastUpdate;        // timestamp
    uint128 fee;               // annual fee (in basis points)
}

struct Position {
    uint256 supplyShares;
    uint128 borrowShares;
    uint128 collateral;        // collateral assets
}
```

**Events:** See "Event-Based Indexing" section below for complete list.

### MetaMorpho Vault Contracts (V1/V1.1)

**Purpose:** ERC-4626 vault that allocates capital across Morpho Blue markets.

**Core Functions:**

```solidity
// ERC-4626 Standard
function deposit(uint256 assets, address receiver) returns (uint256 shares)
function mint(uint256 shares, address receiver) returns (uint256 assets)
function withdraw(uint256 assets, address receiver, address owner) returns (uint256 shares)
function redeem(uint256 shares, address receiver, address owner) returns (uint256 assets)

// Vault state
function totalAssets() returns (uint256)
function totalSupply() returns (uint256)

// Market allocation (Curator/Allocator)
function reallocate(MarketAllocation[] calldata allocations)

// Configuration (Curator via timelock)
function setSupplyQueue(bytes32[] memory newSupplyQueue)
function setWithdrawQueue(bytes32[] memory newWithdrawQueue)
function setCap(MarketParams memory params, uint256 newSupplyCap)

// Fee management (Owner)
function setFee(uint256 newFee)
function setFeeRecipient(address newFeeRecipient)

// Read market allocations
function config(bytes32 id) returns (uint256 cap, bool enabled, uint64 removableAt)
function supplyQueue(uint256 index) returns (bytes32 marketId)
function withdrawQueue(uint256 index) returns (bytes32 marketId)
```

**Events:** See "Event-Based Indexing" section below.

### MetaMorpho Vault Contracts (V2)

**Purpose:** Next-generation vault with adapter-based architecture.

**Core Functions:**

```solidity
// ERC-4626 Standard (same as V1.1)
function deposit(uint256 assets, address onBehalf) returns (uint256 shares)
function mint(uint256 shares, address onBehalf) returns (uint256 assets)
function withdraw(uint256 assets, address receiver, address onBehalf) returns (uint256 shares)
function redeem(uint256 shares, address receiver, address onBehalf) returns (uint256 assets)

// Adapter allocation
function allocate(address adapter, uint256 assets, bytes32[] memory ids) returns (int256 change)
function deallocate(address adapter, uint256 assets, bytes32[] memory ids) returns (int256 change)
function forceDeallocate(address adapter, uint256 assets, address onBehalf, bytes32[] memory ids) 
    returns (uint256 penaltyAssets)

// Adapter management (Owner)
function addAdapter(address adapter)
function removeAdapter(address adapter)

// Configuration (Curator via timelock)
function increaseAbsoluteCap(bytes32 id, bytes memory idData, uint256 newCap)
function decreaseAbsoluteCap(address sender, bytes32 id, bytes memory idData, uint256 newCap)
function increaseRelativeCap(bytes32 id, bytes memory idData, uint256 newCap)
function decreaseRelativeCap(address sender, bytes32 id, bytes memory idData, uint256 newCap)

// Fee management (Owner)
function setPerformanceFee(uint256 newFee)
function setManagementFee(uint256 newFee)
function setPerformanceFeeRecipient(address newRecipient)
function setManagementFeeRecipient(address newRecipient)

// Read adapter state
function adapters() returns (address[] memory)
function adapterTotalAssets(address adapter) returns (uint256)
```

**Key V2 Concepts:**

**Adapters:**
```solidity
interface IAdapter {
    function realAssets() external view returns (uint256);
    function asset() external view returns (address);
}
```

Each adapter reports its current value via `realAssets()`, allowing the vault to aggregate total assets from diverse sources.

**ID & Cap System:**

V2 uses a more granular allocation system:
- `id` (bytes32): Identifier for allocation (e.g., market ID or sub-vault ID)
- `absoluteCap`: Fixed maximum allocation to this ID
- `relativeCap`: Percentage-based cap relative to total assets

**Events:** See "Event-Based Indexing" section below.

### Oracle Contracts

**Purpose:** Provide price feeds for collateral valuation.

Morpho Blue markets can use **any oracle contract**, commonly:
- Chainlink price feeds
- Morpho's own oracles
- Custom oracle implementations

**Key Function:**

```solidity
function price() external view returns (uint256);
```

Returns the price with 36 - loan decimals - collateral decimals precision.

### Interest Rate Model (IRM) Contracts

**Purpose:** Calculate borrow rates based on utilization.

**Key Function:**

```solidity
function borrowRate(MarketParams memory params, Market memory market) 
    external view returns (uint256);
```

Returns the current borrow rate for the market (annual rate in WAD precision).

---

## Data Availability & Indexing Constraints

Before implementing an indexer, it's critical to understand **what data sources are available** and **which indexing approach is most practical** for different types of historical data.

### Three Approaches to Historical Data

**1. Event-Based Indexing (Recommended for Most Data):**
- Index event logs emitted by contracts
- Efficient and cost-effective
- Provides complete transaction history
- **Limitation:** Some data (like current rates, total states) not always emitted in events

**2. Archive Node RPC Calls (Possible but Expensive):**
- Call view functions at specific historical block numbers
- Works for ANY contract state at ANY historical block
- **Limitation:** Expensive and slow for bulk historical indexing (thousands of blocks)
- **Use case:** Snapshots at specific blocks, or supplementing event data

**3. Morpho API (Pre-Indexed Historical Data):**
- GraphQL API at `https://api.morpho.org/graphql`
- Provides comprehensive historical data already indexed by Morpho
- **Limitation:** May not expose all granular data points, especially for V2 vaults
- **Use case:** APY calculations, market metrics, USD valuations

### What Can Be Indexed Historically

**Via Events (Efficient & Recommended):**

| Data Type | Source Events | Fully Indexable? |
|-----------|--------------|------------------|
| User vault deposits/withdrawals | `Deposit`, `Withdraw`, `Transfer` | ✅ Yes |
| Vault share balances | `Deposit`, `Withdraw`, `Transfer` | ✅ Yes |
| Market supply/borrow events | `Supply`, `Borrow`, `Repay`, `Withdraw` | ✅ Yes |
| Liquidations | `Liquidate` | ✅ Yes |
| Flash loans | `FlashLoan` | ✅ Yes |
| Vault configuration changes | `SetFee`, `SetCap`, etc. | ✅ Yes |
| Allocation changes (V1) | `ReallocateSupply`, `ReallocateWithdraw` | ✅ Yes |
| Allocation changes (V2) | `Allocate`, `Deallocate` | ✅ Yes |

**Via Archive Node RPC (Possible but Requires Snapshots):**

| Function | Contract | Historical Access | Practical Approach |
|----------|----------|-------------------|-------------------|
| `market(marketId)` | Morpho Blue | ✅ Yes (via archive node + block number) | Use events or Morpho API for bulk historical data |
| `position(marketId, user)` | Morpho Blue | ✅ Yes (via archive node + block number) | Use events to track position changes |
| `totalAssets()` | MetaMorpho | ✅ Yes (via archive node + block number) | Derive from `AccrueInterest` events |
| `adapters()` / `realAssets()` | MetaMorpho V2 | ✅ Yes (via archive node + block number) | Store periodic snapshots if needed |
| `borrowRate()` | IRM Contract | ✅ Yes (via archive node + block number) | Use Morpho API or IRM event data |

### APY & Rate Indexing

**MetaMorpho Vault APY:**

| Method | V1/V1.1 | V2 | Practical Approach |
|--------|---------|----|----|
| Historical native APY from events | ✅ Yes | ⚠️ Partial* | Calculate from `AccrueInterest` share price changes |
| Historical native APY via archive node | ✅ Yes | ✅ Yes | Query `totalAssets()`/`totalSupply()` at intervals, compute from changes |
| Morpho API (native + rewards) | ✅ Yes | ✅ Yes** | Pre-indexed, includes rewards APR (only source for rewards) |

\* **V2 Event-Based APY Limitation:** The `AccrueInterest` event provides `previousTotalAssets` and `newTotalAssets`, enabling native APY calculation from share price changes. However, this captures yield from the vault as a whole without breaking down which adapters contributed what yield. For adapter-specific yield attribution, alternative approaches are needed.

\*\* **V2 Morpho API Limitation:** V2 vaults do not support `historicalState` queries (unlike V1 vaults). The API provides current APY (`avgApy`, `avgNetApy`) but not historical APY time series for V2.

**Recommendations:**
1. **V1/V1.1 vaults**: Event-based native APY calculation works perfectly
2. **V2 vaults**: 
   - For **current native APY**: Use Morpho API or calculate from latest `AccrueInterest`
   - For **historical native APY**: Derive from `AccrueInterest` events (vault-level yield only) OR take periodic snapshots
3. **Rewards APR (all vault versions)**: Never available onchain - always requires Morpho API

**Morpho Blue Market APY:**

| Method | Available | Practical Approach |
|--------|-----------|-------|
| Historical from events | ⚠️ Partial | `AccrueInterest` includes `prevBorrowRate` but not supply rate |
| Historical via archive node | ✅ Yes | Query `market()` + call `IRM.borrowRate()` at intervals |
| Morpho API historical | ✅ Yes | Pre-indexed via `historicalState.borrowApy()` / `supplyApy()` |

**Market APY Considerations:**

The `AccrueInterest` event provides:
```solidity
event AccrueInterest(bytes32 indexed id, uint256 prevBorrowRate, uint256 interest, uint256 feeShares);
```

This gives you borrow rate but not supply rate. To calculate supply APY historically:
- **Option A**: Use `prevBorrowRate` from events + reconstruct utilization from tracked supply/borrow amounts
- **Option B**: Query market state via archive node at intervals and call IRM contract
- **Option C**: Use Morpho API (recommended for simplicity)

The Morpho API is recommended because it handles IRM curve calculations and provides both borrow and supply APY series.

### Rewards: Understanding Availability and Calculation

**Critical Distinction: Native APY vs Rewards APR**

Morpho protocol yields come from two separate sources:

1. **Native APY**: Interest earned from lending activities (onchain, event-derivable)
2. **Rewards APR**: Additional incentives distributed via external programs (NEVER onchain)

**Key Principle**: Rewards data is **NEVER stored onchain or emitted in events**. All rewards information must be fetched from external APIs.

**Quick Answer - Which API Do I Need?**

| What You Want | API to Use |
|---------------|------------|
| Display reward APR on a vault/market | **Morpho API** (`state.rewards`) |
| Show what a user can claim | **Merkl API** + **Morpho Rewards API** |
| Historical reward rates | **Morpho API** (`historicalState.rewards()`) |

---

#### Rewards Program Types & Data Sources

There are **three distinct APIs** for rewards, each serving a different purpose:

| API | Purpose | Data Provided |
|-----|---------|---------------|
| **Morpho API** (`api.morpho.org`) | Reward rates (APRs) | Current/historical reward APRs for vaults & markets |
| **Merkl API** (`api.merkl.xyz`) | User claimable rewards | Current Merkl campaigns - what users can claim |
| **Morpho Rewards API** (`rewards.morpho.org`) | User claimable rewards | Legacy URD programs - what users can claim |

**Important Distinction:**
- **Reward APRs** (how much rewards are being distributed) → **Morpho API** (`state.rewards`)
- **User claimable amounts** (what a specific user has earned) → **Merkl API + Morpho Rewards API**

**Distribution Systems:**

| System | Status | Claiming API |
|--------|--------|--------------|
| **Merkl** | Current | Merkl API - active campaigns |
| **URD (Universal Rewards Distributor)** | Legacy | Morpho Rewards API - historical programs |

Both distribution systems can be active simultaneously. Users may have claimable rewards in both.

---

#### Reward Sources by Vault Type

**MetaMorpho V1/V1.1:**
- **Vault-level rewards**: Direct campaigns targeting the vault
- **Market-level rewards**: Forwarded from Morpho Blue markets where vault allocates

**MetaMorpho V2:**
- **Vault-level rewards**: Direct campaigns targeting the vault
- **Market-level rewards**: Forwarded from Morpho Blue markets (via market adapters)
- **Nested vault rewards**: Forwarded from MetaMorpho V1 vaults (via vault adapters)

**Morpho Blue Markets:**
- **Supply rewards**: Incentives for suppliers
- **Borrow rewards**: Incentives for borrowers (often negative APR = earning rewards while borrowing)

---

#### Data Availability: Reward Rates (APRs)

**Use Case: Displaying vault/market APYs with rewards**

**Source: Morpho API only** (`api.morpho.org/graphql` - query `state.rewards`)

|| V1/V1.1 | V2 | Markets | Notes |
||--------|-----|---------|-------|
|| **Current reward APRs** | ✅ Yes | ✅ Yes | ✅ Yes | `state.rewards[]` for vault/market |
|| **Historical reward APRs** | ✅ Yes | ❌ No | ✅ Yes | V1: `historicalState.rewards()`, V2: not supported |

**Query Structure:**

```graphql
query VaultRewards($address: String!, $chainId: Int!) {
  vaultByAddress(address: $address, chainId: $chainId) {
    state {
      # Native APY (before fees)
      apy
      
      # Complete APY (native + all rewards, after fees)
      netApy
      
      # Vault-level rewards (direct campaigns)
      rewards {
        supplyApr
        yearlySupplyTokens
        asset {
          address
          symbol
          priceUsd
          chain { id }
        }
      }
      
      # Market-level rewards (forwarded from allocations)
      allocation {
        supplyAssetsUsd  # Required for weighted averaging
        market {
          state {
            rewards {
              supplyApr
              asset { address symbol priceUsd }
            }
          }
        }
      }
    }
  }
}
```

**Critical: Manual Aggregation Required**

Morpho API provides rewards in **two separate locations** that must be aggregated manually:

```typescript
// 1. Sum vault-level rewards
const vaultRewardsApr = vault.state.rewards.reduce(
  (sum, r) => sum + parseFloat(r.supplyApr),
  0
);

// 2. Calculate weighted average of market-level rewards
const totalAllocatedUsd = vault.state.allocation.reduce(
  (sum, alloc) => sum + parseFloat(alloc.supplyAssetsUsd),
  0
);

const marketRewardsApr = vault.state.allocation.reduce((sum, alloc) => {
  const marketRewards = alloc.market.state.rewards.reduce(
    (marketSum, r) => marketSum + parseFloat(r.supplyApr),
    0
  );
  const weight = parseFloat(alloc.supplyAssetsUsd) / totalAllocatedUsd;
  return sum + (marketRewards * weight);
}, 0);

// 3. Total rewards APR
const totalRewardsApr = vaultRewardsApr + marketRewardsApr;
const completeApy = parseFloat(vault.state.apy) + totalRewardsApr;
```

**Convenience Field (Simpler Alternative):**

```typescript
// netApy = native APY + all rewards (vault + market) - fees
const completeApy = parseFloat(vault.state.netApy);
```

Use `netApy` for simple total display without breakdown. For detailed reward attribution, manual aggregation is required.

---

#### Data Availability: User Claimable Rewards

**Use Case: What can a specific user claim right now?**

**Source: Merkl API + Morpho Rewards API** (NOT Morpho API)

User claimable rewards are **ONLY available via external APIs**, never onchain.

**Via Merkl API (Current Programs):**

```typescript
// Fetch user's claimable Merkl rewards
const response = await fetch(
  `https://api.merkl.xyz/v4/userRewards?user=${userAddress}`
);
const data = await response.json();

// Structure: { chainId: { campaignAddress: { claimable: { tokenAddress: {...} } } } }
```

**Via Morpho Rewards API (Legacy URD):**

```typescript
// Fetch user's claimable URD rewards
const response = await fetch(
  `https://rewards.morpho.org/v1/users/${userAddress}/rewards`
);
const rewards = await response.json();

// Structure: [{ asset: {...}, amount: { claimable_now, claimed, ... }, ... }]
```

**Combined User Rewards Example:**

```typescript
async function fetchAllUserRewards(userAddress: string) {
  const [merklRewards, urdRewards] = await Promise.all([
    fetch(`https://api.merkl.xyz/v4/userRewards?user=${userAddress}`).then(r => r.json()),
    fetch(`https://rewards.morpho.org/v1/users/${userAddress}/rewards`).then(r => r.json())
  ]);
  
  // Aggregate by token address
  const combinedByToken = new Map<string, { merkl: bigint; urd: bigint; total: bigint }>();
  
  // Process Merkl rewards...
  // Process URD rewards...
  // Combine both sources
  
  return combinedByToken;
}
```

---

#### Historical Rewards Data

| Data Type | V1/V1.1 | V2 | Markets | Source |
|-----------|---------|-----|---------|--------|
| **Historical reward APRs** | ✅ Yes | ❌ No | ✅ Yes | Morpho API `historicalState` |
| **Historical user claims** | ✅ Yes | ✅ Yes | ✅ Yes | Morpho Rewards API (URD only) |
| **Historical Merkl data** | ❌ No | ❌ No | ❌ No | Not available |

**For V1 vaults:**
```graphql
vaultByAddress(address: "0x...", chainId: 1) {
  historicalState {
    rewards(options: { startTimestamp, endTimestamp, interval: "DAY" }) {
      x  # timestamp
      y  # reward APR
    }
  }
}
```

**For V2 vaults:** No `historicalState` available. Only current rewards accessible.

**For historical user claims:** Use Morpho Rewards API `/v1/users/{address}/rewards` - provides full URD distribution history including claimed amounts.

---

#### APY Components Breakdown (Complete Formula)

For a complete understanding of vault yields:

```
Complete APY = Native APY + Underlying Token Yield + Rewards APR - Fees

Where:
- Native APY: Base yield from lending (onchain, event-derivable)
- Underlying Token Yield: Yield-bearing assets like sDAI (API: asset.yield.apr)
- Rewards APR: Sum of all reward programs (ONLY via API)
- Fees: Performance fee (applied to native APY only) + Management fee (time-based)

Net APY = Native APY × (1 - Performance Fee) + Underlying Token Yield + ∑Rewards APR - Management Fee
```

**API Fields:**
- `avgApy`: Native APY (6h average, before fees, excluding rewards)
- `avgNetApy`: Complete APY (6h average, after fees, including all rewards)

---

#### Indexing Recommendations for Rewards

**What You CAN Index from Events:**
- ❌ **Reward APRs** - NEVER onchain
- ❌ **User claimable balances** - NEVER onchain
- ❌ **Reward token distributions** - NEVER onchain

**What You MUST Use APIs For:**

| Data Needed | Correct API | Query |
|-------------|-------------|-------|
| **Reward APRs** (display on vault/market) | **Morpho API** | `state.rewards[]` |
| **User claimable** (current Merkl campaigns) | **Merkl API** | `/v4/userRewards?user={address}` |
| **User claimable** (legacy URD) | **Morpho Rewards API** | `/v1/users/{address}/rewards` |
| **Historical reward APRs** | **Morpho API** | `historicalState.rewards()` (V1/markets only) |

**Practical Indexing Strategy:**

1. **For displaying vault/market APYs (reward rates):**
   - Index native APY from events (V1/V1.1) or API (V2)
   - **Fetch reward APRs from Morpho API** (`state.rewards[]`)
   - Cache Morpho API responses (5-15 minutes)
   - Display combined: `Total APY = Native APY + Rewards APR`

2. **For displaying user claimable rewards (what user can claim):**
   - **Query Merkl API** for current Merkl rewards (`/v4/userRewards?user={address}`)
   - **Query Morpho Rewards API** for URD rewards (`/v1/users/{address}/rewards`)
   - Combine both sources by token address
   - Cache per-user (5-10 minutes)

3. **For historical analysis:**
   - V1 vaults/markets: **Use Morpho API** `historicalState.rewards()` for historical reward APRs
   - V2 vaults: No historical rewards available (current only)
   - Implement periodic snapshots if historical V2 data needed

**Reference:** 
- [Morpho Rewards Integration Guide](https://docs.morpho.org/morpho/rewards/integration/overview)
- [Merkl API Documentation](https://docs.merkl.xyz)
- [Morpho Rewards API](https://rewards.morpho.org/docs)

### Vault Composition & Allocation Tracking

**Why Vault Composition Matters - Position Backing:**

Unlike Aave/Sparklend where multiple borrowers can use the same collateral asset (requiring complex backing analysis), Morpho's architecture provides **direct position backing** through vault composition:

- **For MetaMorpho suppliers**: Your position backing is simply the vault's market allocation
  - Vault allocates 40% to Market A, 30% to Market B, 30% to Market C
  - Your supply is backed by the same 40%/30%/30% split
  - No need to analyze individual borrower collateral compositions

- **For Morpho Blue suppliers**: Your position backing is the single market's borrower collateral
  - Supply to isolated Market X
  - Backed by all collateral in Market X proportionally

This makes position backing analysis **dramatically simpler** than in pooled lending protocols. Tracking vault composition is the primary method for understanding supply risk in MetaMorpho.

---

**MetaMorpho V1/V1.1:**

| Data | Source | Historical Access | Practical Approach |
|------|--------|-------------------|-------------------|
| Which markets vault allocates to | `ReallocateSupply` events | ✅ Yes (events) | Track market IDs in allocation events |
| Allocation amounts per market | `ReallocateSupply/Withdraw` events | ✅ Yes (events) | Sum supply/withdraw amounts per market |
| Supply/withdraw queues | `SetSupplyQueue/SetWithdrawQueue` events | ✅ Yes (events) | Track queue changes over time |
| Supply caps per market | `SetCap` events | ✅ Yes (events) | Track cap changes over time |

**MetaMorpho V2:**

| Data | Source | Historical Access | Practical Approach |
|------|--------|-------------------|-------------------|
| Which adapters vault uses | `AdapterAdded/Removed` events | ✅ Yes (events) | Track adapter lifecycle |
| Allocation amounts per adapter | `Allocate/Deallocate` events | ✅ Yes (events) | Track total allocated per adapter from events |
| **Adapter composition** (markets within adapter) | Adapter contract calls | ✅ Yes (archive node) | Query at intervals OR reconstruct from adapter events |
| Current adapter composition | Morpho API `vaultV2ByAddress` | ✅ Yes (current only) | Use `adapters { assets, assetsUsd, type }` |
| Historical adapter composition | Morpho API | ❌ No | V2 vaults don't have `historicalState` in API |
| Caps per allocation ID | `IncreaseAbsoluteCap/DecreaseAbsoluteCap` | ✅ Yes (events) | Track cap changes |

**V2 Adapter Composition - Detailed Strategies:**

The `Allocate` and `Deallocate` events provide:
- Adapter address
- Total amount allocated/deallocated  
- Array of `ids[]` affected
- Total `change` amount

**What the events DON'T provide:**
- Per-ID allocation breakdown within each adapter (e.g., how much to each market in a MorphoMarketV1AdapterV2)

**Three Approaches for V2 Historical Composition:**

**Approach 1: Adapter-Level Tracking Only (Events)**
```typescript
// Track total per adapter from events, don't break down to markets
VaultAdapterAllocation {
  adapter: address,
  totalAllocated: bigint, // Sum from Allocate/Deallocate events
  // No per-market breakdown
}
```
- ✅ Efficient, event-based only
- ❌ Less granular (adapter-level, not market-level)

**Approach 2: Periodic Archive Node Snapshots**
```typescript
// Query adapter state at regular intervals (e.g., daily)
const markets = await adapter.marketParamsList(); // Market IDs
const assets = await adapter.realAssets(); // Total value
// Store snapshot
```
- ✅ Full granularity (per-market within adapters)
- ⚠️ Requires archive node access and periodic querying
- ❌ Not real-time, snapshot-based

**Approach 3: Current State via Morpho API**
```graphql
vaultV2ByAddress(address: "0x...", chainId: 1) {
  adapters {
    address
    type
    assets
    assetsUsd
  }
}
```
- ✅ Easy to use, pre-indexed
- ❌ Current state only (no historical time series)
- ❌ V2 vaults don't support `historicalState` queries

**Recommendation:** For most use cases, **Approach 1 (adapter-level tracking)** from events is sufficient. If you need per-market granularity historically, implement **Approach 2** with daily/weekly snapshots.

**Comparison Summary:**

```
V1/V1.1 Allocation Tracking:
  Events → Market IDs + exact amounts → ✅ Complete historical tracking

V2 Allocation Tracking:
  Events → Adapter addresses + total amounts → ✅ Complete adapter-level tracking
  Per-market within adapters → Requires archive node snapshots or Morpho API (current only)
```

### Morpho API Data Availability

The **Morpho API** (`api.morpho.org/graphql`) provides comprehensive pre-indexed data for both current and historical protocol states. The API explicitly provides **"Real-time & Historical Data"** according to the official documentation.

**What the Morpho API Provides:**

| Data Type | V1 Vaults | V2 Vaults | Markets | Notes |
|-----------|-----------|-----------|---------|-------|
| **Current native APY** | ✅ Yes | ✅ Yes | ✅ Yes | `avgApy` (before fees, no rewards) |
| **Current complete APY** | ✅ Yes | ✅ Yes | ✅ Yes | `avgNetApy` (after fees, includes rewards) |
| **Historical native APY** | ✅ Yes | ❌ No | ✅ Yes | V1: `historicalState.apy()`, V2: not supported |
| **Current rewards APR** | ✅ Yes | ✅ Yes | ✅ Yes | `state.rewards[]` - ONLY via API, never onchain |
| **Historical rewards APR** | ✅ Yes | ❌ No | ✅ Yes | V1: `historicalState.rewards()`, V2: not supported |
| **Current allocations** | ✅ Yes | ✅ Yes | N/A | Market lists, adapter composition |
| **Historical allocations** | ✅ Yes | ❌ No | N/A | V1: `historicalState.allocation`, V2: not supported |
| **USD valuations** | ✅ Yes | ✅ Yes | ✅ Yes | `totalAssetsUsd`, `borrowAssetsUsd`, etc. |
| **Asset prices** | ✅ Yes | ✅ Yes | ✅ Yes | `assetByAddress { historicalPriceUsd() }` |
| **Risk warnings** | ✅ Yes | ✅ Yes | ✅ Yes | `warnings { type, level }` |
| **User positions** | ✅ Yes | ✅ Yes | ✅ Yes | `userByAddress { vaults, markets }` |

**API Capabilities by Version:**

**V1/V1.1 Vaults:**
- Full `historicalState` support
- Historical APY time series
- Historical allocation breakdowns
- Complete historical data

**V2 Vaults:**
- Current state only (no `historicalState`)
- Current APY (`avgApy`, `avgNetApy`)
- Current adapter composition
- Rewards APR (current)

**Morpho Blue Markets:**
- Full `historicalState` support
- Historical APY (borrow and supply)
- Historical supply/borrow amounts
- Utilization over time

**API Technical Details:**
- **Endpoint:** `https://api.morpho.org/graphql`
- **Default results:** First 100 items (use pagination for more)
- **Default chain:** Ethereum mainnet (specify `chainId` for others)
- **Supported chains:** 1 (Ethereum), 8453 (Base)

**When to Use the API:**
1. **Current native APY for all vaults** - Simpler than calculating from events
2. **Historical native APY for V1 vaults and markets** - Pre-calculated time series
3. **All rewards data** - ONLY available via API (never onchain/in events) - see [Rewards section](#rewards-understanding-availability-and-calculation)
4. **USD valuations** - Pre-calculated with price feeds
5. **V2 vault current state** - Simplest way to get adapter composition

**When Events/Archive Nodes Are Better:**
1. **Real-time event tracking** - Lower latency than API
2. **Custom calculations** - Full control over logic
3. **Data not in API** - V2 historical allocations, granular adapter data
4. **Specific block precision** - Archive nodes give exact block states

**Reference:** See [Morpho API Documentation](https://docs.morpho.org/tools/offchain/api/get-started/) for complete query examples.

### Indexing Strategy Summary

**What's Efficient to Index from Events:**
- ✅ User positions (vault shares, market positions)
- ✅ Transaction history (deposits, withdrawals, borrows, repays)
- ✅ Configuration changes (fees, caps, queues, roles)
- ✅ V1/V1.1 vault allocations (complete market breakdown)
- ✅ V2 adapter allocations (adapter-level totals)
- ✅ V1/V1.1 vault native APY (from share price changes)
- ⚠️ V2 vault native APY (vault-level only, not per-adapter)

**What Requires Archive Node Snapshots:**
- Market state at specific blocks (supply, borrow, utilization)
- User positions at specific blocks (for health factor calculations)
- V2 adapter composition breakdown (per-market within adapters)
- Historical rates at specific moments

**What Morpho API Provides (Easier Than DIY):**
- ✅ Current native APY for all vaults and markets
- ✅ Historical native APY for V1 vaults and markets (V2 vaults: current only)
- ✅ USD valuations and price feeds
- ✅ Risk warnings and metadata

**What's NEVER Onchain (API-Only):**
- ❌ **Rewards APR** - All reward rates (see [Rewards section](#rewards-understanding-availability-and-calculation))
- ❌ **User claimable rewards** - Merkl API + Morpho Rewards API required
- ❌ **Reward token distributions** - No onchain record

**Recommended Indexing Approach:**

**For V1/V1.1 Vaults:**
- **Primary**: Event-based indexing (sufficient for native APY and positions)
- **Required for rewards**: See [Rewards section](#rewards-understanding-availability-and-calculation) - all rewards via external APIs
- **Optional**: Morpho API for USD values

**For V2 Vaults:**
- **Primary**: Event-based indexing for positions, transactions, adapter-level allocations
- **Required for rewards**: See [Rewards section](#rewards-understanding-availability-and-calculation) - all rewards via external APIs
- **Secondary**: Morpho API for current native APY and USD values
- **Optional**: Archive node snapshots if you need per-market adapter composition history

**For Morpho Blue Markets:**
- **Primary**: Event-based indexing for positions and transactions
- **Required for rewards**: See [Rewards section](#rewards-understanding-availability-and-calculation) - all rewards via external APIs
- **Secondary**: Morpho API for historical APY and rates
- **Optional**: Archive node + IRM calls for real-time rate calculations

**Key Insight:** Almost everything CAN be indexed historically (via events or archive nodes), but the Morpho API provides pre-calculated metrics that save significant development effort for native APY, rates, and USD valuations. **For complete guidance on rewards (APRs, user claimables, historical data), see the dedicated [Rewards section](#rewards-understanding-availability-and-calculation)**.

---

## Indexing Methodology

This section describes how to comprehensively index Morpho to track protocol state, user positions, vault allocations, and historical data.

### Event-Based Indexing

**Purpose:** Capture all state-changing transactions by listening to contract events.

#### Morpho Blue Events

**Market Creation:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `CreateMarket` | New market created | id, marketParams (5-tuple of market identifiers) |

**Supply & Withdraw:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `Supply` | Assets supplied to market | id (marketId), caller, onBehalf, assets, shares |
| `Withdraw` | Assets withdrawn from market | id, caller, onBehalf, receiver, assets, shares |
| `SupplyCollateral` | Collateral supplied | id, caller, onBehalf, assets |
| `WithdrawCollateral` | Collateral withdrawn | id, caller, onBehalf, receiver, assets |

**Borrow & Repay:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `Borrow` | Assets borrowed | id, caller, onBehalf, receiver, assets, shares |
| `Repay` | Debt repaid | id, caller, onBehalf, assets, shares |

**Liquidations:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `Liquidate` | Position liquidated | id, caller, borrower, repaidAssets, repaidShares, seizedAssets, badDebtAssets, badDebtShares |

**Interest Accrual:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `AccrueInterest` | Interest accrues on market | id, prevBorrowRate, interest, feeShares |

**Flash Loans:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `FlashLoan` | Flash loan executed | caller, token, assets |

**Configuration:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `SetOwner` | Owner changed | newOwner |
| `SetFee` | Market fee changed | id, newFee |
| `SetFeeRecipient` | Fee recipient changed | newFeeRecipient |
| `EnableIrm` | IRM enabled | irm |
| `EnableLltv` | LLTV enabled | lltv |

#### MetaMorpho V1/V1.1 Events

**Vault Operations:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `Deposit` | User deposits assets | sender, owner, assets, shares |
| `Withdraw` | User withdraws assets | sender, receiver, owner, assets, shares |
| `Transfer` | Shares transferred | from, to, shares |

**Interest & Fees:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `AccrueInterest` | Interest accrues (V1.1) | newTotalAssets, feeShares |
| `UpdateLastTotalAssets` | Last total assets updated | updatedTotalAssets |

**Market Allocation:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `ReallocateSupply` | Capital allocated to market | caller, id, suppliedAssets, suppliedShares |
| `ReallocateWithdraw` | Capital withdrawn from market | caller, id, withdrawnAssets, withdrawnShares |

**Configuration:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `SetSupplyQueue` | Supply queue updated | caller, newSupplyQueue |
| `SetWithdrawQueue` | Withdraw queue updated | caller, newWithdrawQueue |
| `SetCap` | Market cap changed | caller, id, cap |
| `SetFee` | Performance fee changed | caller, newFee |
| `SetFeeRecipient` | Fee recipient changed | feeRecipient |
| `SetOwner` | Owner changed | newOwner |
| `SetCurator` | Curator changed | newCurator |
| `SetGuardian` | Guardian changed | guardian |
| `SetIsAllocator` | Allocator status changed | allocator, isAllocator |

**Timelock:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `SubmitTimelock` | Timelock submitted | newTimelock |
| `AcceptTimelock` | Timelock accepted | newTimelock |
| `SubmitCap` | Cap change submitted | id, cap |
| `SubmitGuardian` | Guardian change submitted | guardian |

#### MetaMorpho V2 Events

**Vault Operations:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `Deposit` | User deposits assets | sender, onBehalf, assets, shares |
| `Withdraw` | User withdraws assets | sender, receiver, onBehalf, assets, shares |
| `Transfer` | Shares transferred | from, to, shares |

**Interest & Fees:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `AccrueInterest` | Interest accrues (V2) | previousTotalAssets, newTotalAssets, performanceFeeShares, managementFeeShares |

**Adapter Allocation:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `Allocate` | Capital allocated to adapter | sender, adapter, assets, ids[], change |
| `Deallocate` | Capital withdrawn from adapter | sender, adapter, assets, ids[], change |
| `ForceDeallocate` | Forced deallocation with penalty | sender, adapter, assets, onBehalf, ids[], penaltyAssets |

**Adapter Management:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `AdapterAdded` | Adapter added to vault | adapter |
| `AdapterRemoved` | Adapter removed from vault | adapter |

**Configuration (Cap Events):**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `IncreaseAbsoluteCap` | Absolute cap increased | id, idData, newCap |
| `DecreaseAbsoluteCap` | Absolute cap decreased | sender, id, idData, newCap |
| `IncreaseRelativeCap` | Relative cap increased | id, idData, newCap |
| `DecreaseRelativeCap` | Relative cap decreased | sender, id, idData, newCap |

**Fee Events:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `SetPerformanceFee` | Performance fee changed | newFee |
| `SetManagementFee` | Management fee changed | newFee |
| `SetPerformanceFeeRecipient` | Performance fee recipient changed | newRecipient |
| `SetManagementFeeRecipient` | Management fee recipient changed | newRecipient |

**Role Events:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `SetOwner` | Owner changed | newOwner |
| `SetCurator` | Curator changed | newCurator |
| `SetIsAllocator` | Allocator status changed | allocator, newIsAllocator |
| `SetIsSentinel` | Sentinel status changed | sentinel, newIsSentinel |

**Timelock Events:**

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `Submit` | Timelocked action submitted | selector, data, executableAt |
| `Accept` | Timelocked action executed | selector, data |
| `Revoke` | Timelocked action revoked | sender, selector, data |

**Implementation:**

For each event, the indexer should:
- Store event data in the database
- Update relevant entity state (vaults, markets, users)
- Track user activity for position discovery
- Calculate derived metrics where appropriate

**Important:** See [Data Availability & Indexing Constraints](#data-availability--indexing-constraints) for understanding what can be derived from events vs. what requires alternative approaches (API, snapshots).

### Vault Share Transfer Tracking

**Critical Issue:** Like Aave's aTokens, MetaMorpho vault shares are **transferable ERC-20 tokens**.

**Problem:**

Users can receive vault shares via `transfer()` without calling `deposit()`:

```
1. Alice deposits 100 USDC → Receives 100 vault shares
2. Alice transfers 50 shares to Bob
3. Bob now has a vault position but never emitted a Deposit event
4. Without transfer tracking, Bob is invisible to the indexer
```

**Solution:** Index `Transfer` events on all MetaMorpho vault contracts.

**Implementation:**

1. **Track Transfer events:**
   ```solidity
   event Transfer(address indexed from, address indexed to, uint256 shares);
   ```

2. **Filter transfers:**
   - Ignore zero address (mint/burn, covered by Deposit/Withdraw)
   - Ignore vault contract address (internal accounting)
   - Process all other transfers

3. **Update user tracking:**
   - Add both `from` and `to` addresses to active user list
   - Update position data for both users
   - Enables complete user discovery for snapshots

### Position Snapshot Indexing

**Purpose:** Periodically capture complete protocol state at specific blocks.

#### 1. Morpho Blue Market Snapshots

**Source:** Direct contract calls to `Morpho.market(marketId)`

**Data to Store:**

| Field | Type | Description | Precision |
|-------|------|-------------|-----------|
| `id` | string | `{protocol}-{marketId}-{block}` | - |
| `protocolId` | string | Protocol identifier | - |
| `marketId` | hex | Market ID (bytes32) | - |
| `blockNumber` | bigint | Block number | - |
| `timestamp` | bigint | Block timestamp | seconds |
| `totalSupplyAssets` | bigint | Total supplied to market | Token decimals |
| `totalSupplyShares` | bigint | Total supply shares | Token decimals |
| `totalBorrowAssets` | bigint | Total borrowed from market | Token decimals |
| `totalBorrowShares` | bigint | Total borrow shares | Token decimals |
| `supplyAPY` | integer | Current supply APY | Basis points |
| `borrowAPY` | integer | Current borrow APY | Basis points |
| `utilization` | integer | Utilization rate | Basis points (10000 = 100%) |
| `totalCollateral` | bigint | Sum of all user collateral | Token decimals |

**Use Cases:**
- Track market growth over time
- Calculate historical utilization rates
- Monitor supply/borrow balances
- Identify liquidity trends

#### 2. Morpho Blue User Position Snapshots

**Source:** Direct contract calls to `Morpho.position(marketId, user)`

**Data to Store:**

| Field | Type | Description | Precision |
|-------|------|-------------|-----------|
| `id` | string | `{protocol}-{marketId}-{user}-{block}` | - |
| `protocolId` | string | Protocol identifier | - |
| `marketId` | hex | Market ID | - |
| `user` | hex | User address | - |
| `blockNumber` | bigint | Block number | - |
| `timestamp` | bigint | Block timestamp | seconds |
| `supplyShares` | bigint | User supply shares | Token decimals |
| `supplyAssets` | bigint | User supply assets (calculated) | Token decimals |
| `borrowShares` | bigint | User borrow shares | Token decimals |
| `borrowAssets` | bigint | User borrow assets (calculated) | Token decimals |
| `collateral` | bigint | User collateral amount | Token decimals |
| `healthFactor` | bigint | Position health factor | 18 decimals (1e18 = 1.0) |

**Share to Asset Conversion:**

```typescript
// Supply assets from shares
supplyAssets = (supplyShares * market.totalSupplyAssets) / market.totalSupplyShares

// Borrow assets from shares
borrowAssets = (borrowShares * market.totalBorrowAssets) / market.totalBorrowShares
```

**Health Factor Calculation:**

```typescript
// Get price from oracle (36 - loan decimals - collateral decimals precision)
const price = await oracle.price();

// Calculate collateral value in loan token terms
const collateralValue = (collateral * price) / (10 ** pricePrecision);

// Health factor = collateral value / (borrow value / LLTV)
// Or equivalently: (collateral value * LLTV) / borrow value
const healthFactor = (collateralValue * lltv * 1e18) / (borrowAssets * 10000);

// healthFactor < 1e18 means liquidatable
```

#### 3. MetaMorpho Vault Snapshots

**Source:** Direct contract calls to vault contracts

**Data to Store:**

| Field | Type | Description | Precision |
|-------|------|-------------|-----------|
| `id` | string | `{protocol}-{vaultAddress}-{block}` | - |
| `protocolId` | string | Protocol identifier | - |
| `vaultAddress` | hex | Vault contract address | - |
| `blockNumber` | bigint | Block number | - |
| `timestamp` | bigint | Block timestamp | seconds |
| `totalAssets` | bigint | Total assets under management | Token decimals |
| `totalSupply` | bigint | Total shares in circulation | Token decimals |
| `sharePrice` | bigint | Assets per share (see Share-Based Accounting) | WAD (18 decimals) |
| `idleAssets` | bigint | Unallocated assets in vault | Token decimals |
| `allocatedAssets` | bigint | Assets allocated to markets/adapters | Token decimals |
| `currentAPY` | integer | Instantaneous APY | Basis points |

#### 4. MetaMorpho Vault Allocation Snapshots

**Source:** Query vault state and adapter/market positions

**Data to Store:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | `{vaultAddress}-{allocationId}-{block}` |
| `vaultAddress` | hex | Vault address |
| `blockNumber` | bigint | Block number |
| `timestamp` | bigint | Block timestamp |
| `allocationType` | string | "market" (V1.1) or "adapter" (V2) |
| `targetAddress` | hex | Market ID (V1.1) or Adapter address (V2) |
| `targetName` | string | Human-readable name |
| `allocatedAssets` | bigint | Assets allocated |
| `allocatedShares` | bigint | Shares allocated |
| `supplyCap` | bigint | Maximum allowed allocation |
| `utilizationRate` | integer | % of cap used (basis points) |

**Use Cases:**
- **Position Backing Analysis (Primary)**: Determine what markets and collateral back suppliers' vault positions
- Track vault composition over time
- Monitor risk concentration
- Calculate weighted average yields
- Identify allocation strategy changes

#### 5. MetaMorpho User Vault Position Snapshots

**Source:** Query vault share balances via `balanceOf(user)`

**Data to Store:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | `{vaultAddress}-{user}-{block}` |
| `vaultAddress` | hex | Vault address |
| `user` | hex | User address |
| `blockNumber` | bigint | Block number |
| `timestamp` | bigint | Block timestamp |
| `shares` | bigint | User vault shares |
| `assets` | bigint | User assets (shares × sharePrice) |
| `assetsUSD` | bigint | USD value (if price available) |

**Asset Calculation:**

```typescript
userAssets = (userShares * vaultTotalAssets) / vaultTotalSupply
```

### APY Calculation from Events

**MetaMorpho Vault APY:**

Unlike Aave which stores rates in events, Morpho vault APY must be **calculated from share price changes** over time.

**Method: Historical Share Price Analysis**

```typescript
// Step 1: Calculate share price at each AccrueInterest event (using formula from Share-Based Accounting section)
const sharePrice = (totalAssets * WAD) / totalSupply;

// Step 2: Get time delta since last AccrueInterest
const timeDelta = currentTimestamp - previousTimestamp;

// Step 3: Calculate annualized return
const priceIncrease = newSharePrice - oldSharePrice;
const returnRate = priceIncrease / oldSharePrice;
const annualizedReturn = returnRate * (SECONDS_PER_YEAR / timeDelta);

// Step 4: Convert to APY in basis points
const apyBps = Math.floor(annualizedReturn * 10000);
```

**Example:**

```
Previous AccrueInterest:
  sharePrice = 1.000000 (WAD: 1000000000000000000)
  timestamp = 1704067200 (Jan 1, 2024)

Current AccrueInterest:
  sharePrice = 1.001000 (WAD: 1001000000000000000)
  timestamp = 1704153600 (Jan 2, 2024)
  timeDelta = 86400 seconds (1 day)

Calculation:
  priceIncrease = 0.001000
  returnRate = 0.001000 / 1.000000 = 0.001 (0.1%)
  annualizedReturn = 0.001 * (31536000 / 86400) = 0.365 (36.5%)
  apyBps = 3650 (36.50%)
```

**Storage:**

Store APY snapshots linked to each `AccrueInterest` event:

```typescript
{
  id: `${vaultAddress}-${blockNumber}-${logIndex}`,
  vault: vaultAddress,
  blockNumber: blockNumber,
  timestamp: timestamp,
  totalAssets: totalAssets,
  totalSupply: totalSupply,
  sharePrice: sharePrice.toString(),
  apyBps: apyBps,
}
```

**Limitations:**

See [Data Availability & Indexing Constraints](#data-availability--indexing-constraints) for complete details on V2 APY limitations and alternative approaches.

**Quick Reference - Morpho API for APY:**

```graphql
# V2 Vault APY (recommended for production)
query {
  vaultV2ByAddress(address: "0x...", chainId: 1) {
    avgApy          # Native APY (before fees)
    avgNetApy       # Net APY (after fees, including rewards)
    performanceFee
    managementFee
    rewards {
      asset { address }
      supplyApr
    }
  }
}

# Market APY (historical)
query {
  marketByUniqueKey(uniqueKey: "0x...", chainId: 1) {
    historicalState {
      borrowApy(options: { startTimestamp, endTimestamp, interval: "HOUR" }) {
        x
        y
      }
      supplyApy(options: { startTimestamp, endTimestamp, interval: "HOUR" }) {
        x
        y
      }
    }
  }
}
```

### Vault Allocation Tracking

**Purpose:** Track how vault assets are allocated across markets/adapters. This is **essential for position backing analysis** - understanding what underlying markets and collateral back a supplier's vault position.

For MetaMorpho suppliers, vault allocation directly determines position backing:
- If a vault allocates 60% to Market A and 40% to Market B
- A supplier's position is backed by the same 60%/40% split of those markets' borrower collateral

This is much simpler than pooled lending protocols (Aave/Sparklend) where backing analysis requires examining all individual borrower collateral compositions.

**V1/V1.1 Allocation Discovery:**

Track `ReallocateSupply` and `ReallocateWithdraw` events:

```typescript
VaultMarketAllocation {
  id: `${vault}-${marketId}`,
  vault: hex,
  marketId: hex,
  currentAssets: bigint,           // Current allocation
  currentShares: bigint,
  supplyCap: bigint,               // Maximum allowed
  totalSupplied: bigint,           // Lifetime supplied
  totalWithdrawn: bigint,          // Lifetime withdrawn
  lastUpdateBlock: bigint,
  lastUpdateTimestamp: bigint,
}
```

**Update Logic:**

```typescript
// On ReallocateSupply
allocation.currentAssets += event.suppliedAssets
allocation.currentShares += event.suppliedShares
allocation.totalSupplied += event.suppliedAssets

// On ReallocateWithdraw
allocation.currentAssets -= event.withdrawnAssets
allocation.currentShares -= event.withdrawnShares
allocation.totalWithdrawn += event.withdrawnAssets
```

**V2 Adapter Allocation Discovery:**

Track `Allocate` and `Deallocate` events:

```typescript
VaultAdapterAllocation {
  id: `${vault}-${adapter}`,
  vault: hex,
  adapter: hex,
  adapterType: string,             // "MorphoMarketV1AdapterV2", "MorphoVaultV1Adapter", etc.
  currentAssets: bigint,
  // For adapter with multiple IDs (markets/sub-vaults)
  allocations: {
    [id: string]: {
      assets: bigint,
      cap: bigint,
    }
  },
  lastUpdateBlock: bigint,
}
```

**Adapter Type Detection:**

Read adapter contract to determine type:

```typescript
// Check if it implements specific interfaces
const supportsV1Adapter = await checkInterface(adapter, "MORPHO_VAULT_V1_ADAPTER");
const supportsMarketAdapter = await checkInterface(adapter, "MORPHO_MARKET_V1_ADAPTER_V2");
```

**Reading Adapter Allocations:**

For detailed composition, call adapter contracts:

```typescript
// V2 adapters report current value
const realAssets = await adapter.realAssets();

// Market adapters expose underlying positions
const positions = await adapter.positions(); // market IDs and amounts
```

### Market Discovery and Metadata

**Market Identification:**

```typescript
MarketMetadata {
  id: hex,                          // Market ID (keccak256 hash)
  loanToken: hex,
  loanTokenSymbol: string,
  loanTokenDecimals: integer,
  collateralToken: hex,
  collateralTokenSymbol: string,
  collateralTokenDecimals: integer,
  oracle: hex,
  irm: hex,
  lltv: bigint,                     // Basis points
  marketName: string,               // "WETH/USDC 86%"
  createdAt: bigint,
  createdAtBlock: bigint,
}
```

**Market Name Generation:**

```typescript
function generateMarketName(
  collateralSymbol: string,
  loanSymbol: string,
  lltv: bigint
): string {
  const lltvPercent = Number(lltv) / 100; // Convert from basis points
  return `${collateralSymbol} / ${loanSymbol} ${lltvPercent}%`;
}

// Example: "WETH / USDC 86%"
```

**Token Metadata Resolution:**

Cache well-known tokens to avoid RPC calls:

```typescript
const KNOWN_TOKENS = {
  "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": { symbol: "WETH", decimals: 18 },
  "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": { symbol: "USDC", decimals: 6 },
  "0xdAC17F958D2ee523a2206206994597C13D831ec7": { symbol: "USDT", decimals: 6 },
  "0x6B175474E89094C44Da98b954EedeAC495271d0F": { symbol: "DAI", decimals: 18 },
  // ... add more
};
```

For unknown tokens, query on-chain:

```typescript
const symbol = await token.symbol();
const decimals = await token.decimals();
```

### Timelock Action Tracking

**Purpose:** MetaMorpho vaults use timelocks for sensitive operations.

**Timelock Flow:**

```
1. Submit: Curator/Owner proposes action → executableAt timestamp set
2. Wait: Timelock period elapses (e.g., 24-48 hours)
3. Accept: Anyone can execute after executableAt
   OR
   Revoke: Curator/Owner cancels before execution
```

**Tracking Schema:**

```typescript
VaultTimelockAction {
  id: `${vault}-${selector}-${blockNumber}`,
  vault: hex,
  action: string,                   // "submit", "accept", "revoke"
  selector: hex,                    // Function selector (bytes4)
  functionName: string,             // Human-readable (e.g., "setCap")
  data: string,                     // Encoded call data
  executableAt: bigint?,            // Timestamp when action can be executed
  status: string,                   // "pending", "executed", "revoked"
  submittedAt: bigint,
  executedAt: bigint?,
  revokedAt: bigint?,
}
```

**Function Selector Mapping:**

```typescript
const SELECTOR_NAMES = {
  "0x7f675a29": "setCap",
  "0x9ff11d0c": "setSupplyQueue",
  "0x94ca3e46": "setWithdrawQueue",
  "0x69fe0e2d": "setFee",
  // ... add more
};
```

### Derived Metrics

**Per-Vault Metrics:**

1. **TVL (Total Value Locked):**
   ```
   TVL = totalAssets × assetPrice
   ```

2. **Idle Liquidity:**
   ```
   idleLiquidity = totalAssets - Σ(allocatedAssets across all markets/adapters)
   ```

3. **Average Weighted APY:**
   ```
   weightedAPY = Σ(marketAPY × allocation) / totalAllocated
   ```

4. **Utilization:**
   ```
   utilization = allocatedAssets / totalAssets
   ```

**Per-Market Metrics (Morpho Blue):**

1. **Utilization Rate:**
   ```
   utilization = totalBorrowAssets / totalSupplyAssets
   ```

2. **Available Liquidity:**
   ```
   available = totalSupplyAssets - totalBorrowAssets
   ```

3. **Total Value Locked:**
   ```
   TVL = (totalSupplyAssets + totalCollateral) × assetPrice
   ```

**Protocol-Wide Metrics:**

1. **Total TVL:**
   ```
   totalTVL = Σ(vault TVL) + Σ(direct market supplies)
   ```

2. **Total Borrowed:**
   ```
   totalBorrowed = Σ(market.totalBorrowAssets × price)
   ```

3. **Active Users:**
   - Count users with non-zero vault shares OR non-zero market positions

---

## Implementation Best Practices

### Tracking Active Users

**Purpose:** Maintain accurate list of users with active positions.

**User Discovery Sources:**

1. **MetaMorpho Vault Positions:**
   - `Deposit` events (onBehalf address)
   - `Transfer` events (from and to addresses)
   - Must track transfers to find users who never called deposit()

2. **Morpho Blue Market Positions:**
   - `Supply` events (onBehalf address)
   - `Borrow` events (onBehalf address)
   - `SupplyCollateral` events (onBehalf address)

**Active User Table:**

```typescript
ActiveUser {
  id: hex,                          // User address
  firstSeenBlock: bigint,           // First interaction block
  firstSeenTimestamp: bigint,       // First interaction timestamp
  lastActivityBlock: bigint,        // Most recent activity
  lastActivityTimestamp: bigint,    // Most recent activity timestamp
  hasVaultPosition: boolean,        // Has any vault shares > 0
  hasMarketPosition: boolean,       // Has any market supply/borrow/collateral > 0
}
```

**Position Determination:**

```typescript
// Update after each transaction:
hasVaultPosition = Σ(user vault shares) > 0

hasMarketPosition = 
  Σ(user supply shares) > 0 ||
  Σ(user borrow shares) > 0 ||
  Σ(user collateral) > 0
```

**Snapshot Optimization:**

Query only active users during snapshots:

```sql
SELECT user 
FROM ActiveUser 
WHERE hasVaultPosition = true OR hasMarketPosition = true
```

This dramatically reduces RPC calls by skipping users who have fully exited.

### Handling Multi-Version Vaults

**Unified Schema Approach:**

Use a single table supporting both versions with nullable fields:

```typescript
MorphoVault {
  id: hex,                          // Vault address
  version: string,                  // "v1.1" or "v2"
  // Common fields
  owner: hex,
  asset: hex,
  name: string,
  symbol: string,
  decimals: integer,
  totalAssets: bigint,
  totalSupply: bigint,
  // Version-specific fields (nullable)
  performanceFee: bigint?,          // V1.1: single fee, V2: performance fee
  managementFee: bigint?,           // V2 only
  performanceFeeRecipient: hex?,
  managementFeeRecipient: hex?,     // V2 only
  curator: hex?,
  // Timestamps
  createdAt: bigint,
  createdAtBlock: bigint,
}
```

**Event Handler Normalization:**

```typescript
// Normalize V1.1 AccrueInterest to V2 format
function normalizeAccrueInterest(event, version) {
  if (version === "v1.1") {
    return {
      previousTotalAssets: 0n,
      newTotalAssets: event.args.newTotalAssets,
      performanceFeeShares: event.args.feeShares,
      managementFeeShares: 0n,
    };
  } else {
    return {
      previousTotalAssets: event.args.previousTotalAssets,
      newTotalAssets: event.args.newTotalAssets,
      performanceFeeShares: event.args.performanceFeeShares,
      managementFeeShares: event.args.managementFeeShares,
    };
  }
}
```

### Price Oracle Integration

**Optional Enhancement:**

For USD valuations and health factor calculations, integrate oracle prices:

```typescript
AssetPrice {
  id: `${asset}-${blockNumber}`,
  asset: hex,
  blockNumber: bigint,
  timestamp: bigint,
  priceUSD: bigint,                 // 8 decimals (standard)
  source: string,                   // "chainlink", "morpho-oracle", etc.
}
```

**Price Snapshot Strategy:**

1. **On-demand**: Query oracle when calculating metrics
2. **Periodic**: Store prices at fixed intervals (e.g., hourly)
3. **Event-based**: Store prices on AccrueInterest or user transactions

**Oracle Contract Calls:**

```typescript
// Morpho oracle (36 - loan decimals - collateral decimals precision)
const price = await oracle.price();

// Convert to standard 8 decimal USD price
const priceUSD = convertToUSD(price, loanDecimals, collateralDecimals);
```

---

## References

### Official Documentation

- Morpho Protocol Documentation: https://docs.morpho.org/
- Morpho Blue Documentation: https://docs.morpho.org/morpho-blue/overview
- MetaMorpho Vault Documentation: https://docs.morpho.org/metamorpho/overview
- Get Data Tutorial: https://docs.morpho.org/build/earn/tutorials/get-data

### GitHub Repositories

- **Morpho Blue:** https://github.com/morpho-org/morpho-blue
- **MetaMorpho (V1):** https://github.com/morpho-org/metamorpho
- **MetaMorpho Vault V2:** https://github.com/morpho-org/vault-v2
- **Ponder Indexer Template:** https://github.com/morpho-org/ponder-template

### Technical Resources

- **Market Mechanics:** https://docs.morpho.org/build/borrow/concepts/market-mechanics
- **Vault Mechanics:** https://docs.morpho.org/build/earn/concepts/vault-mechanics
- **Vault V2 Concepts:** https://docs.morpho.org/learn/concepts/vault-v2
- **Yield Generation:** https://docs.morpho.org/curate/concepts/yield/

### API Reference

- **Morpho API Endpoint:** https://api.morpho.org/graphql
- **GraphQL Playground:** Available at the API endpoint
- **API Documentation:** https://docs.morpho.org/build/earn/tutorials/get-data

**API Notes:**
- Returns first 100 results by default
- Targets Ethereum mainnet unless specified
- Supports chainIds: 1 (Ethereum), 8453 (Base)
- V2 APY calculations include adapter-aware yield aggregation

### Deployed Contracts

**Finding Contract Addresses:**

- **Morpho Blue (Ethereum):** `0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb`
- **Individual Vault Addresses:** Discoverable via factory contracts or Morpho API
- **Market IDs:** Generated via `keccak256(abi.encode(marketParams))`

Use blockchain explorers (Etherscan, Basescan) to verify contracts and find deployments on other chains.

---

**Document Version:** 1.0  
**Last Updated:** January 2026

**Contributors:** Technical specification based on Morpho protocol documentation, smart contract ABIs, and indexing implementation analysis.

