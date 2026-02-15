# Sparklend Protocol Specification

**Version:** 1.0  
**Last Updated:** January 2026  
**Purpose:** Technical reference for understanding Sparklend protocol mechanics and indexing implementation

---

## Table of Contents

1. [Protocol Overview](#protocol-overview)
2. [ABI Differences from Aave V3](#abi-differences-from-aave-v3)
3. [Core Concepts & Mechanics](#core-concepts--mechanics)
4. [Smart Contracts](#smart-contracts)
5. [Indexing Methodology](#indexing-methodology)
6. [Architecture Considerations](#architecture-considerations)
7. [References](#references)

---

## Protocol Overview

### What is Sparklend?

Sparklend is a **non-custodial, open-source liquidity protocol** developed by Spark that enables users to:
- **Supply assets** to earn interest
- **Borrow assets** against collateral
- **Participate as liquidators** to maintain protocol health

Sparklend is a **fork of Aave V3**, maintaining full protocol functionality while being governed by Sky. It operates through smart contracts on Ethereum Mainnet and Gnosis Chain, with no intermediaries holding user funds.

**Documentation:** https://docs.spark.fi/

### Key Features

- **Multi-collateral positions**: Users can supply multiple assets and borrow against their entire portfolio
- **Over-collateralization**: All loans must be over-collateralized to protect lenders
- **Dynamic interest rates**: Rates adjust algorithmically based on supply and demand
- **Instant liquidity**: No lock-up periods; withdraw anytime (if not used as collateral for active borrows)
- **Flash loans**: Uncollateralized loans that must be repaid within the same transaction
- **Efficiency Mode (E-Mode)**: Higher capital efficiency for correlated assets

### Protocol Architecture

**Single Market Per Chain:**

Sparklend uses a **single market per chain** architecture, simpler than Aave V3's multi-market model:
- One Pool per blockchain
- All assets on that chain share the same Pool
- No market isolation within a chain

**Current Deployments:**

Sparklend is deployed on:
- **Ethereum Mainnet**
- **Gnosis Chain**

Each Pool operates **independently** with isolated liquidity, reserves, and risk parameters.

### Relationship to Aave V3

Sparklend is a **fork of Aave V3** with the following characteristics:

- **Shared Core Protocol**: Same lending/borrowing mechanics
- **Simplified ABI**: Fewer events and simpler constructor (see next section)
- **Independent Governance**: Governed by Sky (MakerDAO)
- **Custom Risk Parameters**: Different LTV, liquidation thresholds, and caps per Pool
- **Same Architecture**: Uses identical Pool contract, spToken (aToken), and data provider contracts

**Key Takeaway:** Sparklend shares the same core mechanics as Aave V3. The main differences are in governance, risk parameters, and minor ABI variations.

---

## ABI Differences from Aave V3

This section highlights the key differences between Sparklend's smart contract ABIs and Aave V3 ABIs. Understanding these differences is crucial for building a unified indexer that handles both protocols.

### Summary

Sparklend is a **simplified fork** of Aave V3 with:
- ✅ **Identical core functionality** (supply, borrow, repay, withdraw, liquidationCall)
- ✅ **Same event signatures** for main operations
- ❌ **Fewer specialized events** (no deficit tracking, no position manager)
- ⚠️ **Simpler constructor** (single parameter vs. two)

### Event Differences

#### Events Present in Some Aave V3 Pools but NOT in Sparklend

| Event | Purpose | Parameters | Impact on Indexing |
|-------|---------|------------|-------------------|
| **DeficitCreated** | Tracks deficit creation in Aave's risk management system | `reserve`, `user`, `amountCovered` | Not applicable to Sparklend |
| **DeficitCovered** | Tracks deficit coverage | `reserve`, `caller`, `amountCovered` | Not applicable to Sparklend |
| **PositionManagerApproved** | Delegation feature allowing approved addresses to manage positions | `user`, `positionManager` | Not applicable to Sparklend (no delegation) |
| **PositionManagerRevoked** | Revokes position manager approval | `user`, `positionManager` | Not applicable to Sparklend |

**Implication:** These are **Aave-specific features** not present in Sparklend. These events do not need to be indexed for Sparklend.

#### Events Present in Both Sparklend and Aave V3

All core protocol events are **identical** between Sparklend and Aave V3:

- ✅ Supply, Withdraw, Borrow, Repay
- ✅ LiquidationCall
- ✅ FlashLoan
- ✅ ReserveDataUpdated
- ✅ ReserveUsedAsCollateralEnabled / Disabled
- ✅ UserEModeSet
- ✅ SwapBorrowRateMode
- ✅ IsolationModeTotalDebtUpdated
- ✅ MintUnbacked, BackUnbacked
- ✅ MintedToTreasury

**Implication:** These events have the same signature and meaning in both protocols.

### Constructor Differences

**Sparklend Pool Constructor:**

```solidity
constructor(IPoolAddressesProvider provider)
```

- **Parameters:** Single parameter (addresses provider)
- **Simpler initialization**

**Aave V3 Core Pool Constructor:**

```solidity
constructor(
    IPoolAddressesProvider provider,
    IReserveInterestRateStrategy interestRateStrategy_
)
```

- **Parameters:** Two parameters (addresses provider + interest rate strategy)
- **Separate strategy contract reference**

**Implication:** This difference only matters for deployment/initialization, not for indexing. Both pools expose the same public interface.

### Function Signatures

**Core functions are identical** between Sparklend and Aave V3:

```solidity
// These function signatures are the same in both protocols
function supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
function withdraw(address asset, uint256 amount, address to) returns (uint256)
function borrow(address asset, uint256 amount, uint256 interestRateMode, uint16 referralCode, address onBehalfOf)
function repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf) returns (uint256)
function liquidationCall(address collateral, address debt, address user, uint256 debtToCover, bool receiveAToken)
function getUserAccountData(address user) returns (...)
function getReserveData(address asset) returns (...)
```

### Indexer Implementation Strategy

**Event Handler Approach:**

When building an indexer, the core events can be handled with the same logic:

1. **Core Transaction Events** (Supply, Borrow, Repay, Withdraw, LiquidationCall, FlashLoan)
   - Same event signatures and parameters
   - Same processing logic
   - Only differs in protocol/pool identifier for data storage

2. **Reserve & Market Events** (ReserveDataUpdated, ReserveUsedAsCollateral*, etc.)
   - Identical structure and meaning
   - Track the same state changes

3. **User Configuration Events** (UserEModeSet, SwapBorrowRateMode)
   - Same functionality across both protocols

**Protocol Differences to Handle:**

- **Deficit Events**: Only in some Aave deployments, not in Sparklend (skip these for Sparklend)
- **Position Manager Events**: Only in some Aave deployments (skip these for Sparklend)
- **Constructor Parameters**: Different initialization, but doesn't affect event processing

### ABI Comparison Summary

**Sparklend Pool ABI:**
- Based on standard Aave V3 Pool
- Excludes deficit and position manager events
- All core lending/borrowing functionality present

**Some Aave V3 Pool ABIs:**
- Include all core events plus Pool-specific additions:
  - DeficitCreated / DeficitCovered (in some Pools)
  - PositionManagerApproved / PositionManagerRevoked (in some Pools)

### Data Provider Compatibility

**Important:** Both Sparklend and Aave V3 use the **same UiPoolDataProviderV3 interface**:

```solidity
interface IUiPoolDataProviderV3 {
    function getReservesData(address provider) returns (AggregatedReserveData[], BaseCurrencyInfo)
    function getUserReservesData(address provider, address user) returns (UserReserveData[], uint8)
}
```

**Implication:** Snapshot fetching logic can be **100% shared** between protocols. Only the contract addresses differ.

### Conclusion

**Key Takeaways for Indexing:**

1. **Core events are identical** - Supply, Borrow, Repay, Withdraw, LiquidationCall, FlashLoan all have the same structure
2. **Skip Aave-specific events** - DeficitCreated, DeficitCovered, PositionManager* are not in Sparklend
3. **Same snapshot logic** - UiPoolDataProviderV3 interface is identical
4. **Same data structures** - UserAccountData, UserReserveData, ReserveData are the same
5. **Same risk parameters** - LTV, liquidation threshold, reserve factor, etc. use same format

**Functional Equivalence:** Despite minor ABI differences, Sparklend and Aave V3 share identical core lending/borrowing mechanics. The differences are in specialized features (deficit management, position delegation) that Sparklend doesn't implement.

---

## Core Concepts & Mechanics

### Markets & Chain Deployments

**What is a Market in Sparklend?**

Sparklend uses a **single market per chain** approach. Each market is deployed as a Pool on a specific blockchain with:
- Pool contract
- Set of supported assets (reserves)
- Liquidity pools
- Risk parameters
- User positions

**Single Market Per Chain Architecture:**

Sparklend uses a **simpler architecture** than Aave V3:
- **One Pool per chain**
- All assets within that chain share the same Pool
- No market isolation within a chain

**Pool Deployments:**

Sparklend Pools are currently deployed on:
1. **Ethereum Mainnet** - Supporting major assets (e.g., WETH, wstETH, sDAI, USDC)
2. **Gnosis Chain** - Supporting various assets including stablecoins and wrapped tokens

**Cross-Chain Independence:**

- Positions on different Pools are completely separate
- Liquidations occur per Pool, not across Pools
- Each Pool has its own health factor calculations
- Oracle prices may differ between Pools

### Multi-Collateral Positions

**How Users Supply Multiple Assets:**

1. User supplies Asset A (e.g., 10 ETH)
   - Receives spTokenA (e.g., 10 spWETH - Sparklend's aToken)
   - Can enable as collateral

2. User supplies Asset B (e.g., 5,000 USDC)
   - Receives spTokenB (e.g., 5,000 spUSDC)
   - Can enable as collateral

3. User's borrowing power = Sum of (Asset Value × LTV) for all collateral-enabled assets

**Borrowing Against Portfolio:**

- Each asset has its own **Loan-to-Value (LTV)** ratio
- User's total borrowing power is calculated as:
  ```
  Max Borrow = Σ (Collateral Asset Value × Asset LTV)
  ```
- Example with ETH (80% LTV) and USDC (85% LTV):
  ```
  10 ETH × $2,000 × 0.80 = $16,000
  5,000 USDC × $1 × 0.85 = $4,250
  Total Max Borrow = $20,250
  ```

**Position Aggregation:**

The protocol tracks positions at multiple levels:
- **Per-asset positions**: Individual supply/borrow amounts per reserve
- **Aggregate account data**: Total collateral, total debt, health factor across all assets
- **Collateral enablement**: Users can toggle which assets to use as collateral

### aTokens (Interest-Bearing Tokens)

**What are Sparklend's aTokens?**

In Sparklend, interest-bearing tokens are called **spTokens** (e.g., spWETH, spUSDC) but they function identically to Aave's aTokens. They are **ERC-20 tokens** that:
- Are minted 1:1 when users supply assets
- Accrue interest in real-time
- Can be transferred between addresses
- Can be used in other DeFi protocols (composability)

**Example:** Supplying 100 USDC → Receive 100 spUSDC

**Naming Convention:**
- Sparklend: spWETH, spUSDC, spsDAI (prefix: "sp")
- Aave: aWETH, aUSDC, aDAI (prefix: "a")
- **Functionally identical**, just different names

**Interest Accrual Mechanism:**

Sparklend uses the same **scaled balance** system as Aave V3 for gas-efficient interest accrual:

1. **Scaled Balance**: The "principal" amount stored on-chain
   ```
   scaledBalance = balance / liquidityIndex
   ```

2. **Liquidity Index**: Grows continuously based on interest rates
   - Starts at 1.0 (with 27 decimal precision - "ray")
   - Increases every block based on liquidityRate
   - Formula: `newIndex = oldIndex × (1 + liquidityRate × timeDelta / secondsPerYear)`

3. **Current Balance**: Calculated on-the-fly
   ```
   currentBalance = scaledBalance × currentLiquidityIndex
   ```

**Why Scaled Balances?**
- Avoid updating every user's balance on every block (expensive)
- Store one index per reserve instead of N balances
- Users' real balances grow automatically as the index grows

**Transfer Mechanics:**

- spTokens (aTokens) are **fully transferable** ERC-20 tokens
- Transfers move supply positions between addresses
- **Important for indexing**: Users can receive spTokens without calling `supply()`
- Interest continues accruing for whoever holds the spTokens

**Discovery:**
spToken addresses can be discovered by monitoring `ReserveInitialized` events emitted by the Pool contract during reserve setup. Each event contains the spToken (aToken) address for that reserve.

### Interest Rate Model

**Utilization-Based Rates:**

Interest rates in Sparklend adjust automatically based on **utilization rate**:

```
Utilization Rate (U) = Total Borrowed / Total Supply
```

Where `Total Supply = Total Borrowed + Available Liquidity`

**Two-Slope Rate Model:**

Sparklend uses the same piecewise linear model as Aave V3:

```
if U < U_optimal:
    borrowRate = baseRate + (U / U_optimal) × slope1
else:
    borrowRate = baseRate + slope1 + ((U - U_optimal) / (1 - U_optimal)) × slope2
```

**Parameters:**
- **Base Rate**: Minimum borrow rate when utilization is 0%
- **Optimal Utilization (U_optimal)**: Target utilization (e.g., 80%)
- **Slope 1**: Rate increase from 0% to optimal utilization
- **Slope 2**: Steep increase beyond optimal (incentivizes repayment)

**Supply vs Borrow Rates:**

```
Supply APY = Borrow APY × Utilization Rate × (1 - Reserve Factor)
```

- **Borrow APY**: Rate borrowers pay
- **Supply APY**: Rate suppliers earn (always < Borrow APY)
- **Reserve Factor**: Percentage taken by protocol treasury

**Rate Updates:**

- Rates update on every state-changing transaction (supply, borrow, repay, withdraw)
- `ReserveDataUpdated` event emitted with new rates
- Each reserve has its own interest rate strategy contract

**Example Rate Curve (USDC):**
- Base Rate: 0%
- Optimal Utilization: 90%
- Slope 1: 4%
- Slope 2: 60%

If utilization is 95%:
```
borrowRate = 0% + 4% + ((0.95 - 0.90) / (1 - 0.90)) × 60%
          = 4% + (0.05 / 0.10) × 60%
          = 4% + 30% = 34% APY
```

### Health Factor & Risk Management

**Health Factor Formula:**

```
Health Factor = (Total Collateral in Base Currency × Weighted Avg Liquidation Threshold) / Total Debt in Base Currency
```

**Components:**

1. **Total Collateral in Base Currency:**
   - Sum of all collateral-enabled assets valued in base currency (USD)
   - `Collateral Value = Σ (spToken Balance × Asset Price)`

2. **Weighted Average Liquidation Threshold:**
   - Each asset has its own liquidation threshold (e.g., 85% for ETH, 90% for USDC)
   - Weighted by collateral value:
     ```
     Weighted LT = Σ (Asset Collateral Value × Asset LT) / Total Collateral Value
     ```

3. **Total Debt in Base Currency:**
   - Sum of all borrowed assets valued in base currency
   - `Total Debt = Σ (Debt Balance × Asset Price)`

**Example Calculation:**

User has:
- 10 ETH as collateral ($2,000/ETH, 85% LT) = $20,000 collateral × 0.85
- 5,000 USDC as collateral ($1/USDC, 90% LT) = $5,000 collateral × 0.90
- Borrowed 8,000 DAI ($1/DAI) = $8,000 debt

```
Weighted LT = ($20,000 × 0.85 + $5,000 × 0.90) / $25,000
            = ($17,000 + $4,500) / $25,000
            = 0.86 (86%)

Health Factor = ($25,000 × 0.86) / $8,000
              = $21,500 / $8,000
              = 2.69
```

**Interpretation:**

- **HF > 1.0**: Position is healthy (safe)
- **HF = 1.0**: Position is at liquidation threshold
- **HF < 1.0**: Position is **liquidatable**

**Loan-to-Value (LTV) Ratio:**

- LTV determines maximum initial borrowing power
- Always **lower** than liquidation threshold (safety buffer)
- Example: 80% LTV, 85% liquidation threshold → 5% safety buffer

**Risk Tiers:**

- **HF > 2.0**: Very safe
- **HF 1.5 - 2.0**: Moderate risk
- **HF 1.1 - 1.5**: Elevated risk
- **HF 1.0 - 1.1**: High risk (near liquidation)
- **HF < 1.0**: Liquidatable

### Liquidation Mechanics

**When Liquidations Occur:**

Liquidations are triggered when a user's Health Factor falls below 1.0, which can happen due to:
- Borrowed asset price increase
- Collateral asset price decrease
- Interest accrual increasing debt

**Liquidation Process:**

1. **Liquidator calls** `liquidationCall(collateralAsset, debtAsset, user, debtToCover, receiveAToken)`
   
2. **Protocol validates:**
   - User's HF < 1.0 (liquidatable)
   - Liquidator has sufficient debtAsset to repay
   - DebtToCover does not exceed close factor

3. **Protocol executes:**
   - Burns liquidator's debtAsset (repaying user's debt)
   - Transfers user's collateralAsset to liquidator (with bonus)
   - Updates user's position
   - Emits `LiquidationCall` event

**Close Factor:**

The maximum percentage of debt that can be liquidated in a single transaction:

- **If HF >= 0.95**: Max 50% of debt can be liquidated (partial liquidation)
- **If HF < 0.95**: Max 100% of debt can be liquidated (full liquidation)

This prevents over-liquidation and gives users a chance to restore health.

**Source:** These values are inherited from Aave V3 and defined in the liquidation logic: https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/logic/LiquidationLogic.sol

**Liquidation Bonus:**

Liquidators receive a bonus (typically 5-10%) as incentive:

```
Collateral Received = (Debt Repaid × Debt Price / Collateral Price) × (1 + Liquidation Bonus)
```

**Example:**
- User has $10,000 USDC debt, $12,000 ETH collateral (HF = 0.95)
- Liquidation bonus: 5%
- Liquidator can repay up to $5,000 USDC (50% close factor)
- Liquidator receives: $5,000 × 1.05 = $5,250 worth of ETH

**receiveAToken Parameter:**

- `true`: Liquidator receives spTokens (continues earning interest)
- `false`: Liquidator receives underlying asset directly

**Liquidation Events:**

```solidity
event LiquidationCall(
    address indexed collateralAsset,
    address indexed debtAsset,
    address indexed user,
    uint256 debtToCover,
    uint256 liquidatedCollateralAmount,
    address liquidator,
    bool receiveAToken
);
```

### Efficiency Mode (E-Mode)

**What is E-Mode?**

Efficiency Mode allows **higher capital efficiency** (higher LTV/liquidation thresholds) when borrowing assets within the same **category** of correlated assets.

**Use Cases:**

1. **Stablecoin Category**: Borrow USDC against DAI collateral with 97% LTV (instead of 75%)
2. **ETH Derivatives**: Borrow ETH against stETH with 90% LTV (instead of 80%)

**How E-Mode Works:**

1. **E-Mode Categories** are defined per deployment (e.g., Category 1 = Stablecoins, Category 2 = ETH Correlated)

2. **Users opt-in** to a category by calling `setUserEMode(categoryId)`

3. **When in E-Mode:**
   - If borrowing AND collateral are both in the same category → Use E-Mode parameters
   - If borrowing OR collateral are outside category → Use standard parameters
   - Category 0 = E-Mode disabled (default)

**E-Mode Parameters:**

Each category defines:
- **LTV**: Maximum borrow amount (e.g., 97% for stablecoins)
- **Liquidation Threshold**: Health factor trigger (e.g., 98%)
- **Liquidation Bonus**: Liquidator incentive (e.g., 2%)
- **Price Source**: Optional custom oracle for category

**Example:**

Standard Mode:
- Supply 10,000 DAI (75% LTV) → Borrow max 7,500 USDC

E-Mode (Stablecoin category, 97% LTV):
- Supply 10,000 DAI → Borrow max 9,700 USDC

**Risk Trade-Off:**

- Higher capital efficiency → Higher liquidation risk
- Small price deviations between category assets can cause liquidations
- Designed for highly correlated assets only

**Health Factor in E-Mode:**

When in E-Mode, the health factor calculation uses **category-specific parameters** for assets within the category, and standard parameters for assets outside:

```
HF = (In-Category Collateral × Category LT + Out-Category Collateral × Standard LT) / Total Debt
```

**Events:**

```solidity
event UserEModeSet(
    address indexed user,
    uint8 categoryId
);
```

### Reserves & Parameters

**What is a Reserve?**

A **reserve** represents a specific asset market within a Sparklend pool. Each reserve is a configured lending market for one asset (e.g., WETH reserve, USDC reserve).

**Reserve Configuration:**

Each reserve has associated contracts:
- **Underlying Asset**: The actual token (e.g., WETH, USDC)
- **spToken**: Interest-bearing token for suppliers (e.g., spWETH, spUSDC)
- **Variable Debt Token**: Tracks variable rate borrows
- **Stable Debt Token**: Tracks stable rate borrows (deprecated in V3)
- **Interest Rate Strategy**: Contract defining rate curves

**Key Reserve Parameters:**

#### Core Risk Parameters

| Parameter | Description | Example Value | Importance |
|-----------|-------------|---------------|------------|
| **LTV (Loan-to-Value)** | Maximum borrowing power when used as collateral. Expressed as percentage (basis points). | 80% (8000 bps) = Borrow $80 per $100 supplied | Determines initial borrowing capacity. Lower = safer but less capital efficient. |
| **Liquidation Threshold** | Health factor trigger point. When debt reaches this % of collateral value, position becomes liquidatable. | 85% (8500 bps) = Liquidatable if debt reaches 85% of collateral | Critical for liquidation risk. Always > LTV to provide safety buffer. |
| **Liquidation Bonus** | Extra collateral liquidators receive as incentive. | 5% (10500 bps represents 105%) = Liquidator receives $105 collateral per $100 debt | Higher bonus = stronger liquidation incentive but higher cost to borrowers. |
| **Reserve Factor** | Percentage of interest revenue allocated to protocol treasury. | 10% (1000 bps) = 10% of interest goes to treasury | Balances protocol revenue with competitive supply APY. |

#### Supply & Borrow Limits

| Parameter | Description | Example |
|-----------|-------------|---------|
| **Supply Cap** | Maximum total supply allowed for the reserve. Prevents over-concentration. | 1,000,000 USDC |
| **Borrow Cap** | Maximum total borrows allowed for the reserve. Risk management tool. | 800,000 USDC |
| **Debt Ceiling** | Maximum debt allowed when reserve is used as isolated collateral (isolation mode). | 10,000,000 USD |

#### Interest Rate Strategy

| Parameter | Description |
|-----------|-------------|
| **Interest Rate Strategy Address** | Address of contract defining rate curves (base rate, optimal utilization, slopes). |
| **Base Variable Borrow Rate** | Minimum borrow rate at 0% utilization. |
| **Optimal Utilization** | Target utilization rate (inflection point in rate curve). |
| **Variable Rate Slope 1** | Rate increase per utilization % before optimal. |
| **Variable Rate Slope 2** | Steep rate increase beyond optimal utilization. |

#### Operational Flags

| Flag | Description |
|------|-------------|
| **Usage as Collateral Enabled** | Whether asset can be enabled as collateral by users. |
| **Borrowing Enabled** | Whether asset can be borrowed from the reserve. |
| **Stable Rate Enabled** | Whether stable rate borrowing is allowed (deprecated in V3). |
| **Is Active** | Reserve is operational and accepting transactions. |
| **Is Frozen** | Reserve only allows repay/withdraw, no new supply/borrow. |
| **Is Paused** | Reserve is completely paused (emergency only). |

#### E-Mode & Isolation

| Parameter | Description |
|-----------|-------------|
| **E-Mode Category** | Which efficiency mode category this reserve belongs to (0 = none). |
| **Isolation Mode Total Debt** | Current total debt when reserve is used as isolated collateral. |

**Detailed Parameter Explanations:**

**1. LTV vs Liquidation Threshold:**
- **LTV**: Guards initial borrowing (prevents over-leveraging at start)
- **Liquidation Threshold**: Guards against liquidation (provides safety buffer)
- **Gap**: Typically 5-10% buffer between them
- Example: 75% LTV, 80% liquidation threshold → 5% buffer before liquidation risk

**2. Reserve Factor Impact:**
```
Supplier receives: Borrow Rate × Utilization × (1 - Reserve Factor)
Protocol receives: Borrow Rate × Utilization × Reserve Factor

Example with 10% reserve factor, 80% utilization, 5% borrow rate:
- Suppliers earn: 5% × 0.80 × 0.90 = 3.6% APY
- Protocol earns: 5% × 0.80 × 0.10 = 0.4% APY
```

**3. Supply/Borrow Caps:**
- Protect against governance attacks or oracle failures
- Limit potential loss in worst-case scenarios
- Can be updated via governance
- 0 = no cap

**4. Isolation Mode:**
- Restricts borrowing when isolated asset is used as collateral
- Only allows borrowing stablecoins up to debt ceiling
- Prevents systemic risk from new/volatile assets
- Indicated by non-zero debt ceiling

**Reserve Discovery:**

Reserves are initialized via the `ReserveInitialized` event:

```solidity
event ReserveInitialized(
    address indexed asset,
    address indexed aToken,
    address stableDebtToken,
    address variableDebtToken,
    address interestRateStrategyAddress
);
```

---

## Smart Contracts

### Pool Contract

**Purpose:** Main entry point for all user interactions with the protocol. Each Pool has its own contract address.

**Core Functions:**

```solidity
// Supply asset to earn interest
function supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)

// Withdraw supplied asset
function withdraw(address asset, uint256 amount, address to) returns (uint256)

// Borrow asset against collateral
function borrow(address asset, uint256 amount, uint256 interestRateMode, uint16 referralCode, address onBehalfOf)

// Repay borrowed asset
function repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf) returns (uint256)

// Liquidate undercollateralized position
function liquidationCall(address collateral, address debt, address user, uint256 debtToCover, bool receiveAToken)

// Execute flash loan
function flashLoan(address receiverAddress, address[] assets, uint256[] amounts, uint256[] modes, address onBehalfOf, bytes params, uint16 referralCode)

// Enable/disable asset as collateral
function setUserUseReserveAsCollateral(address asset, bool useAsCollateral)

// Set user's E-Mode category
function setUserEMode(uint8 categoryId)

// Get user's aggregate account data
function getUserAccountData(address user) returns (
    uint256 totalCollateralBase,
    uint256 totalDebtBase,
    uint256 availableBorrowsBase,
    uint256 currentLiquidationThreshold,
    uint256 ltv,
    uint256 healthFactor
)

// Get reserve data
function getReserveData(address asset) returns (ReserveData)
```

**Events:** See the "Event-Based Indexing" section below for a complete list of all Pool events and their parameters.

### UiPoolDataProviderV3

**Purpose:** Read-only helper contract for efficient batch querying of protocol state. Essential for indexing and frontend applications. Each Pool has an associated UiPoolDataProviderV3 contract.

**Key Functions:**

```solidity
// Get all reserves data with prices and liquidity
function getReservesData(address provider) returns (
    AggregatedReserveData[] reservesData,
    BaseCurrencyInfo baseCurrencyInfo
)

// Get user's positions across all reserves
function getUserReservesData(address provider, address user) returns (
    UserReserveData[] userReserves,
    uint8 userEModeCategory
)
```

**AggregatedReserveData Structure:**

```solidity
struct AggregatedReserveData {
    address underlyingAsset;
    string name;                        // Token name
    string symbol;                      // Token symbol
    uint256 decimals;                   // Token decimals
    // Risk parameters (basis points - 4 decimals)
    uint256 baseLTVasCollateral;        // Max LTV (e.g., 8000 = 80%)
    uint256 reserveLiquidationThreshold; // Liquidation trigger (e.g., 8500 = 85%)
    uint256 reserveLiquidationBonus;     // Liquidator bonus (e.g., 10500 = 105% = 5% bonus)
    uint256 reserveFactor;              // Protocol fee (e.g., 1000 = 10%)
    // Operational flags
    bool usageAsCollateralEnabled;      // Can be used as collateral
    bool borrowingEnabled;              // Borrowing is enabled
    bool stableBorrowRateEnabled;       // Stable rate borrowing enabled (deprecated but present)
    bool isActive;                      // Reserve is active
    bool isFrozen;                      // Reserve is frozen
    // Indexes (ray precision - 27 decimals but stored as uint128)
    uint128 liquidityIndex;             // Cumulative liquidity index
    uint128 variableBorrowIndex;        // Cumulative variable borrow index
    // Interest rates (ray precision - 27 decimals but stored as uint128)
    uint128 liquidityRate;              // Supply APY
    uint128 variableBorrowRate;         // Variable borrow APY
    uint128 stableBorrowRate;           // Stable borrow APY (deprecated but present)
    uint40 lastUpdateTimestamp;         // Last index/rate update
    // Contract addresses
    address aTokenAddress;              // spToken contract
    address stableDebtTokenAddress;     // Stable debt token contract (deprecated but present)
    address variableDebtTokenAddress;   // Variable debt token contract
    address interestRateStrategyAddress; // Interest rate strategy contract
    // Market state - KEY FIELDS FOR TOTAL SUPPLY/BORROW CALCULATION
    uint256 availableLiquidity;         // Total available to borrow
    uint256 totalPrincipalStableDebt;   // Total stable debt (deprecated, typically 0)
    uint256 averageStableRate;          // Average stable rate (deprecated)
    uint256 stableDebtLastUpdateTimestamp; // Stable debt update time (deprecated)
    uint256 totalScaledVariableDebt;    // Total variable debt (SCALED - needs calculation)
    uint256 priceInMarketReferenceCurrency; // Asset price (8 decimals in USD)
    address priceOracle;                // Oracle address
    // Interest rate model parameters - Variable rates
    uint256 variableRateSlope1;         // Variable rate slope 1
    uint256 variableRateSlope2;         // Variable rate slope 2
    uint256 baseVariableBorrowRate;     // Base variable rate
    // Interest rate model parameters - Stable rates (deprecated but present)
    uint256 stableRateSlope1;           // Stable rate slope 1
    uint256 stableRateSlope2;           // Stable rate slope 2
    uint256 baseStableBorrowRate;       // Base stable rate
    uint256 optimalUsageRatio;          // Optimal utilization
    // Additional operational flags
    bool isPaused;                      // Reserve is paused
    bool isSiloedBorrowing;             // Siloed borrowing mode
    uint128 accruedToTreasury;          // Accrued to treasury
    uint128 unbacked;                   // Unbacked amount
    uint128 isolationModeTotalDebt;     // Isolation mode debt
    bool flashLoanEnabled;              // Flash loans enabled
    // Caps and limits
    uint256 debtCeiling;                // Isolation mode debt ceiling
    uint256 debtCeilingDecimals;        // Debt ceiling decimals
    uint8 eModeCategoryId;              // E-Mode category for this reserve
    uint256 borrowCap;                  // Max total borrows
    uint256 supplyCap;                  // Max total supply
    // E-Mode parameters for this category
    uint16 eModeLtv;                    // E-Mode LTV
    uint16 eModeLiquidationThreshold;   // E-Mode liquidation threshold
    uint16 eModeLiquidationBonus;       // E-Mode liquidation bonus
    address eModePriceSource;           // E-Mode price source
    string eModeLabel;                  // E-Mode label
    bool borrowableInIsolation;         // Can be borrowed in isolation
}
```

**Important Differences from Aave V3:**

Sparklend's `AggregatedReserveData` is based on an earlier Aave V3 version and includes:
- ✅ **Stable rate fields** (deprecated but present): `stableBorrowRateEnabled`, `stableBorrowRate`, `stableDebtTokenAddress`, `totalPrincipalStableDebt`, stable rate model parameters
- ✅ **E-Mode fields directly in reserve data**: `eModeCategoryId`, `eModeLtv`, `eModeLiquidationThreshold`, `eModeLiquidationBonus`, `eModePriceSource`, `eModeLabel`
- ✅ **`unbacked` field** instead of newer Aave's `virtualUnderlyingBalance` and `deficit`

**Important: Calculating Total Supply and Total Borrow**

The struct provides `totalScaledVariableDebt` (scaled value) and `availableLiquidity` (direct value). To get actual totals, you MUST calculate:

```
Total Borrowed = totalScaledVariableDebt × variableBorrowIndex / 1e27
Total Supply = Total Borrowed + availableLiquidity
```

**Note on Stable Debt:** While stable rate fields are present, they are deprecated in Sparklend (inherited from Aave V3) and typically have zero values. Focus on variable debt for calculations.

There are NO pre-calculated `totalDebt` or `totalSupply` fields - you must derive them from the scaled values.

**UserReserveData Structure:**

```solidity
struct UserReserveData {
    address underlyingAsset;
    uint256 scaledATokenBalance;        // Scaled supply balance
    bool usageAsCollateralEnabledOnUser; // Is this asset enabled as collateral?
    uint256 stableBorrowRate;           // User's stable borrow rate (deprecated)
    uint256 scaledVariableDebt;         // Scaled variable debt
    uint256 principalStableDebt;        // Stable debt principal (deprecated)
    uint256 stableBorrowLastUpdateTimestamp; // Last stable rate update (deprecated)
}
```

**Note:** Unlike Aave V3's newer versions, Sparklend's `UserReserveData` does NOT include pre-calculated `currentATokenBalance` or `currentVariableDebt` fields. You must calculate current balances by multiplying scaled balances by the current indexes from `AggregatedReserveData`:

```
currentATokenBalance = scaledATokenBalance × liquidityIndex / 1e27
currentVariableDebt = scaledVariableDebt × variableBorrowIndex / 1e27
```

**Why UiPoolDataProviderV3 is Critical for Indexing:**

1. **Batch Queries**: Get all reserves or all user positions in a single call
2. **Scaled Balances**: Returns scaled balances that you can use with indexes to calculate current balances
3. **Complete Market Data**: Includes rates, indexes, prices, and all reserve parameters in one call
4. **Efficient RPC Usage**: Reduces number of calls dramatically vs querying Pool directly

**Note:** Unlike some newer Aave V3 versions, Sparklend's `UserReserveData` returns only scaled balances. You must calculate current balances using the indexes from `AggregatedReserveData`.

**Usage in Indexer:**

- `getReservesData()`: Called periodically to capture market snapshots (rates, prices, liquidity)
- `getUserReservesData()`: Called per user to capture position snapshots (balances, debt, collateral status)
- Combined with `Pool.getUserAccountData()` for complete user health data

### PoolAddressesProvider

**Purpose:** Central registry for all core protocol contract addresses. Used for contract discovery. Each Pool has its own PoolAddressesProvider.

**Key Functions:**

```solidity
// Get Pool contract address
function getPool() returns (address)

// Get price oracle address
function getPriceOracle() returns (address)

// Get ACL (Access Control List) manager
function getACLManager() returns (address)

// Get pool configurator
function getPoolConfigurator() returns (address)
```

**Usage in Indexer:**
- Pass to `UiPoolDataProviderV3.getUserReservesData()` to specify which pool to query
- Discover oracle address for price feeds

### AaveOracle

**Purpose:** Price feed aggregator providing asset prices in base currency (USD with 8 decimals).

**Key Functions:**

```solidity
// Get asset price in base currency
function getAssetPrice(address asset) returns (uint256)

// Get multiple asset prices
function getAssetsPrices(address[] assets) returns (uint256[])
```

**Price Precision:**
- 8 decimals in USD (e.g., 2000_00000000 = $2,000.00)

**Note:** Prices are also included in `UiPoolDataProviderV3.getReservesData()` as `priceInMarketReferenceCurrency`, which is often more convenient for indexing.

### spToken Contracts (aTokens)

**Purpose:** Interest-bearing ERC-20 tokens representing supply positions (Sparklend's version of aTokens). Each reserve in each Pool has its own spToken contract (e.g., spWETH, spUSDC, spsDAI).

**Key Characteristics:**

1. **ERC-20 Compliant**: Fully transferable tokens
2. **1:1 Minting**: Minted on supply, burned on withdraw
3. **Interest Bearing**: Balance grows automatically via scaled balance mechanism
4. **Composable**: Can be used in other DeFi protocols

**Key Functions:**

```solidity
// Standard ERC-20
function balanceOf(address user) returns (uint256) // Returns current balance with interest
function transfer(address to, uint256 amount) returns (bool)
function approve(address spender, uint256 amount) returns (bool)

// Aave-specific
function scaledBalanceOf(address user) returns (uint256) // Returns scaled balance
function getScaledUserBalanceAndSupply(address user) returns (uint256, uint256)
```

**Events:**

```solidity
event Transfer(address indexed from, address indexed to, uint256 value)
event Approval(address indexed owner, address indexed spender, uint256 value)
```

**Why Transfer Events Matter for Indexing:**

- Users can receive spTokens via direct transfer (not just via supply())
- Must track Transfer events to discover all users with positions
- Critical for complete user discovery (see Architecture Considerations section below)

**Important for Indexing:**
- Track `Transfer` events on ALL spToken contracts
- Track both `from` and `to` addresses
- Filter out zero address (minting/burning) and Sparklend helper contract addresses

---

## Indexing Methodology

This section describes how to comprehensively index Sparklend to track and derive all protocol state, user positions, and historical data.

### Event-Based Indexing

**Purpose:** Capture all state-changing transactions in real-time by listening to Pool contract events.

**Events to Index:**

#### Core Transaction Events

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `Supply` | User supplies asset | reserve, user, onBehalfOf, amount, referralCode |
| `Withdraw` | User withdraws asset | reserve, user, to, amount |
| `Borrow` | User borrows asset | reserve, user, onBehalfOf, amount, interestRateMode, borrowRate, referralCode |
| `Repay` | User repays debt | reserve, user, repayer, amount, useATokens |
| `LiquidationCall` | Position liquidated | collateralAsset, debtAsset, user, debtToCover, liquidatedCollateralAmount, liquidator, receiveAToken |

#### Reserve & Market Events

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `ReserveDataUpdated` | Interest rates/indexes update | reserve, liquidityRate, stableBorrowRate, variableBorrowRate, liquidityIndex, variableBorrowIndex |
| `ReserveUsedAsCollateralEnabled` | User enables collateral | reserve, user |
| `ReserveUsedAsCollateralDisabled` | User disables collateral | reserve, user |
| `IsolationModeTotalDebtUpdated` | Isolation debt changes | asset, totalDebt |
| `MintedToTreasury` | Interest sent to treasury | reserve, amountMinted |

#### User Configuration Events

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `UserEModeSet` | User changes E-Mode | user, categoryId |
| `SwapBorrowRateMode` | User swaps rate mode | reserve, user, interestRateMode |

#### Special Events

| Event | Emitted When | Key Data |
|-------|--------------|----------|
| `BackUnbacked` | Unbacked backed | reserve, backer, amount, fee |
| `MintUnbacked` | Unbacked minted | reserve, user, onBehalfOf, amount, referralCode |

#### spToken (aToken) Events

**Critical for Complete User Discovery:** Track `Transfer` events on all spToken contracts to discover users who receive tokens via direct transfers (not through Pool interactions).

| Event | Contract | Emitted When | Key Data |
|-------|----------|--------------|----------|
| `Transfer` | spToken | spTokens transferred between addresses | from, to, value |
| `Mint` | spToken | spTokens minted (supply) | from (0x0), to, value, index |
| `Burn` | spToken | spTokens burned (withdraw) | from, to (0x0), value, index |

**Why These Matter:**
- **Transfer**: Users can receive supply positions without calling `supply()` - critical for complete user discovery
- **Mint/Burn**: Alternative to tracking Pool `Supply`/`Withdraw` events (see note below)

**Filter addresses to ignore:**
- Zero address (`0x000...000`) for Mint/Burn
- Gateway contracts (e.g., WrappedTokenGatewayV3)

**Alternative Approach: Track spToken Events Instead of Pool Events**

Instead of (or in addition to) tracking Pool events for supply/withdraw operations, you can track spToken Mint/Burn/Transfer events:

**Benefits:**
- **Single source of truth**: spToken balance changes are the ground truth for supply positions
- **Simpler logic**: No need to reconcile Pool events with spToken state

**Trade-offs:**
- **Missing context**: Mint/Burn events don't include `onBehalfOf` or `referralCode` parameters
- **Multiple contracts**: Must listen to events from all spToken contracts (discovered via `ReserveInitialized`)
- **Index parameter**: Mint/Burn include the `index` at time of operation, useful for scaled balance tracking

**Recommended Approach:**
Track both Pool events (for complete transaction context) AND spToken Transfer events (for user discovery). This provides complete coverage with full context.

**Implementation:**

For each event, the indexer should:
- Store the event data in the database
- Update the user's position state
- Track the user as active
- Update scaled balance snapshots (for offchain accounting optimization)

**Event indexing captures:**
- Complete transaction history
- User activity timeline
- Real-time position updates
- Liquidation events
- Reserve parameter changes
- All supply position transfers between users

### spToken Transfer Tracking

**See Also:** The complete list of spToken events (Transfer, Mint, Burn) is documented in the [Event-Based Indexing](#event-based-indexing) section above.

**Critical Issue:** Event-based indexing of Pool events alone misses users who receive spTokens without calling `supply()`.

**Problem Scenario:**

1. Alice supplies 100 USDC → Receives 100 spUSDC (emits Pool `Supply` + spToken `Mint`)
2. Alice transfers 50 spUSDC to Bob (direct ERC-20 transfer, emits spToken `Transfer`)
3. Bob now has a position but never emitted a Pool `Supply` event
4. **Without transfer tracking, Bob is invisible to the indexer**

**Solution:** Index spToken `Transfer`, `Mint`, and `Burn` events on all spToken contracts.

**Events to Track:**

| Event | Purpose | Key Addresses |
|-------|---------|---------------|
| `Transfer` | Track position transfers between users | `from`, `to` (both non-zero) |
| `Mint` | Alternative to Pool `Supply` event | `from` = 0x0, `to` = receiver |
| `Burn` | Alternative to Pool `Withdraw` event | `from` = burner, `to` = 0x0 |

**Implementation Approach:**

For each spToken event:

1. **Filter addresses to ignore:**
   - Zero address (`0x000...000`) - only in `from` for Mint, only in `to` for Burn
   - Pool contract address - internal accounting
   - Gateway contracts - intermediate contracts, not end users

2. **Process valid events:**
   - **Transfer** (non-zero from/to): Add both `from` and `to` addresses to active user list
   - **Mint** (from=0x0): Add `to` address to active user list
   - **Burn** (to=0x0): Update `from` address position
   - Update position tracking for affected users
   - Map spToken address to its underlying reserve
   - Store event for historical records (optional)

3. **Key data to capture:**
   - Amount (value)
   - Sender and recipient addresses (from, to)
   - Block number and timestamp
   - Transaction hash and log index
   - Index parameter (for Mint/Burn events - useful for scaled balance verification)

**Using Mint/Burn vs. Pool Supply/Withdraw Events:**

You can choose to index either:
- **Option A**: Pool events (`Supply`, `Withdraw`) + spToken `Transfer` - provides transaction context
- **Option B**: spToken events only (`Mint`, `Burn`, `Transfer`) - single source of truth, simpler
- **Option C**: Both (recommended) - complete coverage with context and verification

**Discovery:**
spToken addresses can be discovered from `ReserveInitialized` events emitted by the Pool contract.

### Snapshot-Based Indexing

**Purpose:** Periodically query smart contracts directly to capture complete protocol state at specific blocks.

**Why Snapshots?**

1. **Ground Truth**: On-chain state is always correct (events can be missed or misprocessed)
2. **Interest Accrual**: Balances grow between transactions; snapshots capture this
3. **Health Factors**: Dynamically calculated based on current prices and indexes
4. **Efficiency**: Batch query multiple users via multicall
5. **Historical Analysis**: Time-series data for positions and markets

**Snapshot Frequency:**

- Choose granularity based on requirements (e.g., per block, hourly, daily)
- Can trigger on-demand: After liquidations or major market events
- Trade-off: More frequent snapshots = more accurate but higher storage/compute costs

**Implementation:**

At each snapshot interval:

1. **Get active users** from tracking table (including spToken transfer recipients)
2. **Batch query smart contracts** (100-200 users per batch):
   - `Pool.getUserAccountData(user)` → UserAccountSnapshot (aggregate health)
   - `UiPoolDataProviderV3.getUserReservesData(provider, user)` → UserReserveSnapshot (per-asset positions)
   - `UiPoolDataProviderV3.getReservesData(provider)` → ReserveMarketSnapshot (market-wide data, once per batch)
3. **Store** all snapshot data to database

#### 1. UserAccountSnapshot

**Source:** `Pool.getUserAccountData(user)`

**Purpose:** Capture aggregate account health across ALL user positions.

**Data to Store:**

**Fields:**

| Field | Type | Description | Precision |
|-------|------|-------------|-----------|
| `id` | string | `{protocol}-{user}-{block}` | - |
| `protocolId` | string | Protocol identifier | - |
| `userId` | string | User identifier | - |
| `user` | hex | User address | - |
| `blockNumber` | bigint | Block number | - |
| `timestamp` | bigint | Block timestamp | seconds |
| `totalCollateralBase` | bigint | Total collateral value | 8 decimals (USD) |
| `totalDebtBase` | bigint | Total debt value | 8 decimals |
| `availableBorrowsBase` | bigint | Remaining borrow capacity | 8 decimals |
| `currentLiquidationThreshold` | bigint | Weighted avg liquidation threshold | 4 decimals (basis points) |
| `ltv` | bigint | Weighted avg LTV | 4 decimals (basis points) |
| `healthFactor` | bigint | Health factor | 18 decimals (1e18 = 1.0) |
| `eModeCategory` | integer | E-Mode category ID (0 = disabled) | - |

**Use Cases:**

- Check if user is at risk of liquidation (HF < 1.2)
- Track health factor over time
- Calculate total protocol collateral and debt (sum across users)
- Identify users in E-Mode

#### 2. UserReserveSnapshot

**Source:** `UiPoolDataProviderV3.getUserReservesData(provider, user)`

**Purpose:** Capture per-asset positions for each user.

**Data to Store:**

**Fields:**

| Field | Type | Description | Precision |
|-------|------|-------------|-----------|
| `id` | string | `{protocol}-{user}-{block}-{reserve}` | - |
| `protocolId` | string | Protocol identifier | - |
| `userId` | string | User identifier | - |
| `reserveId` | string | Reserve identifier | - |
| `user` | hex | User address | - |
| `underlyingAsset` | hex | Reserve asset address | - |
| `blockNumber` | bigint | Block number | - |
| `timestamp` | bigint | Block timestamp | seconds |
| `scaledATokenBalance` | bigint | Scaled supply balance FROM API | Token decimals |
| `usageAsCollateralEnabled` | boolean | Is this asset enabled as collateral? FROM API | - |
| `scaledVariableDebt` | bigint | Scaled variable debt FROM API | Token decimals |
| `principalStableDebt` | bigint | Stable debt principal FROM API (deprecated, typically 0) | Token decimals |
| `stableBorrowRate` | bigint | User's stable borrow rate FROM API (deprecated) | 27 decimals (ray) |
| `stableBorrowLastUpdateTimestamp` | bigint | Last stable rate update FROM API (deprecated) | seconds |
| `currentATokenBalance` | bigint | **CALCULATED**: `scaledATokenBalance × liquidityIndex / 1e27` | Token decimals |
| `currentVariableDebt` | bigint | **CALCULATED**: `scaledVariableDebt × variableBorrowIndex / 1e27` | Token decimals |

**Important for Sparklend:** The `getUserReservesData()` API returns only scaled balances. You must calculate `currentATokenBalance` and `currentVariableDebt` using the indexes from the corresponding `AggregatedReserveData` (obtained via `getReservesData()`).

#### 3. ReserveMarketSnapshot

**Source:** `UiPoolDataProviderV3.getReservesData(provider)`

**Purpose:** Capture market-wide reserve state (rates, prices, liquidity).

**Data to Store:**

Sparklend uses the same `AggregatedReserveData` structure as Aave V3 (documented in detail in the UiPoolDataProviderV3 section above). 

**Core Fields to Store:** (minimum recommended)

- `underlyingAsset`, `blockNumber`, `timestamp`
- `liquidityIndex`, `variableBorrowIndex`, `liquidityRate`, `variableBorrowRate`
- `baseLTVasCollateral`, `reserveLiquidationThreshold`, `reserveLiquidationBonus`
- **`availableLiquidity`** - Required for total supply calculation
- **`totalScaledVariableDebt`** - Required for total borrow calculation
- **`priceInMarketReferenceCurrency`** - Asset price (8 decimals USD)
- `isActive`, `isFrozen`, `isPaused`, `borrowingEnabled`

**Additional Fields:** The complete struct includes `name`, `symbol`, `decimals`, `reserveFactor`, `aTokenAddress`, rate model parameters (`variableRateSlope1/2`, `optimalUsageRatio`), caps (`borrowCap`, `supplyCap`), and more. Store additional fields based on your requirements.

### Derived Metrics

From the indexed events and snapshots, we can calculate comprehensive protocol metrics.

**Per-Reserve Metrics:**

1. **Total Supply:**
   ```
   Total Supply = (totalScaledVariableDebt × variableBorrowIndex / 1e27) + availableLiquidity
   ```
   Or equivalently: `Total Supply = Total Borrowed + Available Liquidity`
   
   This represents the sum of all user supply positions in the reserve.

2. **Total Borrowed:**
   ```
   Total Borrowed = totalScaledVariableDebt × variableBorrowIndex / 1e27
   ```
   
   This represents the sum of all user borrow positions in the reserve.

3. **Utilization Rate:**
   ```
   Utilization = Total Borrowed / Total Supply
   ```

4. **Supply APY & Borrow APY:**
   ```
   Supply APY = (liquidityRate / 1e27) × 100%
   Borrow APY = (variableBorrowRate / 1e27) × 100%
   ```

5. **TVL per Reserve:**
   ```
   TVL = Total Supply × Price in USD
   ```

**Protocol-Wide Metrics:**

- Total TVL across all reserves
- Total Debt across all reserves
- Active user count
- Users at risk count (1.0 < HF < 1.2)

**User-Specific Metrics:**

- Position value in USD
- Interest earned/paid
- Effective APY over time

### Position Backing Analysis

**Purpose:** Understand what collateral assets "back" a lender's supply position by analyzing the borrower side of the market.

**Use Case:** A lender wants to know the risk composition of their supply position - what types of collateral would be liquidated to repay their supplied assets in case of borrower defaults.

**Methodology:**

For a lender who has supplied Asset A:

1. **Identify all borrowers** of Asset A
2. **For each borrower**:
   - Calculate what portion of their collateral backs Asset A specifically
   - Allocation ratio = (Borrower's Asset A debt) / (Borrower's total debt)
   - Asset A-allocated collateral = Allocation ratio × Borrower's total collateral
3. **Sum up** all Asset A-allocated collateral across all borrowers to get total backing
4. **Calculate percentages** of each collateral type from the total backing (will sum to 100%)

**Basic Example (Single Borrow Type):**

```
Market State:
- Total USDC Supplied: 10,000 USDC
- Total USDC Borrowed: 8,000 USDC
- Alice is a lender with 10,000 USDC supplied (100% of supply)

Borrowers:
- Bob borrows 4,000 USDC (50% of total borrowed)
  - Collateral: $6,000 ETH + $2,000 WBTC = $8,000 total
  - Collateral composition: 75% ETH, 25% WBTC

- Carol borrows 4,000 USDC (50% of total borrowed)
  - Collateral: $8,000 sDAI = $8,000 total
  - Collateral composition: 100% sDAI

Alice's Position Backing Calculation:
- Bob's contribution: 50% × (75% ETH, 25% WBTC) = 37.5% ETH, 12.5% WBTC
- Carol's contribution: 50% × (100% sDAI) = 50% sDAI

Alice's USDC position is backed by:
- 37.5% ETH
- 12.5% WBTC
- 50.0% sDAI
```

**Advanced Example (Multiple Borrow Types):**

When borrowers have multiple borrow positions, their collateral must be allocated proportionally across all their debts:

```
Market State (USDC lending):
- Total USDC Borrowed: 8,000 USDC
- Alice supplies 10,000 USDC

Borrowers:
- Bob borrows 4,000 USDC
  - Also borrows 2,000 sDAI
  - Total borrows: $6,000 ($4,000 USDC + $2,000 sDAI)
  - Collateral: $6,000 ETH + $3,000 wstETH = $9,000 total

- Carol borrows 4,000 USDC
  - Also borrows 4,000 sDAI
  - Total borrows: $8,000 ($4,000 USDC + $4,000 sDAI)
  - Collateral: $10,000 wstETH = $10,000 total

Step 1: Calculate USDC-allocated collateral per borrower
(What portion of each borrower's collateral backs their USDC borrow)

Bob's USDC-allocated collateral:
- Allocation ratio: $4,000 USDC / $6,000 total borrows = 66.67%
- ETH backing USDC: 66.67% × $6,000 ETH = $4,000 ETH
- wstETH backing USDC: 66.67% × $3,000 wstETH = $2,000 wstETH

Carol's USDC-allocated collateral:
- Allocation ratio: $4,000 USDC / $8,000 total borrows = 50%
- wstETH backing USDC: 50% × $10,000 wstETH = $5,000 wstETH

Step 2: Sum total collateral backing USDC market
Total USDC backing:
- $4,000 ETH (from Bob)
- $2,000 wstETH (from Bob)
- $5,000 wstETH (from Carol)
- Total = $11,000 of collateral backing $8,000 borrowed

Step 3: Calculate backing composition as percentages
Alice's USDC position is backed by:
- ETH: $4,000 / $11,000 = 36.36%
- wstETH: ($2,000 + $5,000) / $11,000 = 63.64%
- Total = 100% ✓
```

**Implementation:**

```typescript
async function calculatePositionBacking(
  suppliedAsset: string,
  blockNumber: bigint
) {
  // 1. Get all borrowers of this asset at this block
  const borrowers = await getBorrowersOfAsset(suppliedAsset, blockNumber);
  
  // Map to accumulate total collateral backing this asset
  const totalBackingByAsset: Map<string, bigint> = new Map();
  let totalBackingUSD = 0n;
  
  // 2. For each borrower
  for (const borrower of borrowers) {
    // Get their full position data
    const positions = await getUserReservesData(borrower.address, blockNumber);
    
    // Calculate their total borrows value (across all assets)
    let totalBorrowsUSD = 0n;
    for (const position of positions) {
      if (position.currentVariableDebt > 0) {
        const borrowValueUSD = 
          (position.currentVariableDebt * position.priceUSD) / 10n ** position.decimals;
        totalBorrowsUSD += borrowValueUSD;
      }
    }
    
    // Get their borrow of the specific asset we're analyzing
    const borrowOfTargetAsset = positions.find(p => p.asset === suppliedAsset)?.currentVariableDebt || 0n;
    const borrowOfTargetAssetUSD = (borrowOfTargetAsset * assetPrice) / 10n ** assetDecimals;
    
    // Calculate allocation ratio: what % of their collateral backs this asset
    const allocationRatio = (borrowOfTargetAssetUSD * PRECISION) / totalBorrowsUSD;
    
    // Allocate their collateral proportionally
    for (const position of positions) {
      if (position.usageAsCollateralEnabled && position.currentATokenBalance > 0) {
        const collateralValueUSD = 
          (position.currentATokenBalance * position.priceUSD) / 10n ** position.decimals;
        
        // This much of their collateral backs the target asset
        const allocatedCollateralUSD = (collateralValueUSD * allocationRatio) / PRECISION;
        
        // Add to total backing
        const current = totalBackingByAsset.get(position.asset) || 0n;
        totalBackingByAsset.set(position.asset, current + allocatedCollateralUSD);
        totalBackingUSD += allocatedCollateralUSD;
      }
    }
  }
  
  // 3. Convert to percentages (basis points for precision)
  const backingCompositionBps: Map<string, number> = new Map();
  
  for (const [collateralAsset, collateralUSD] of totalBackingByAsset) {
    const bps = Number((collateralUSD * 10000n) / totalBackingUSD);
    backingCompositionBps.set(collateralAsset, bps);
  }
  
  return {
    backingByAsset: totalBackingByAsset,        // Absolute USD amounts
    backingCompositionBps: backingCompositionBps, // Percentages (basis points)
    totalBackingUSD: totalBackingUSD            // Total collateral backing
  };
}
```

**Important Limitations:**

**1. Methodology Choice:**
This is one approach to position backing analysis. Other methodologies might:
- Weight by liquidation risk rather than borrow amounts
- Consider time-to-liquidation based on health factors
- Account for correlation between supplied asset and collateral assets

**2. Inaccurate Picture with Mixed Collateral Types:**

The methodology treats all collateral equally, but borrowers often have heterogeneous collateral with vastly different liquidation probabilities:

```
Example - Misleading Backing with Mixed Collateral:
- Bob borrows 10,000 USDC
- Collateral: $15,000 sDAI (stablecoin) + $15,000 wstETH (volatile)
- Total collateral: $30,000
- Health Factor: 2.55 (very healthy)

Position Backing Calculation says:
- 50% backed by sDAI
- 50% backed by wstETH

Issue: This is misleading because of correlation effects:
- sDAI and USDC are highly correlated (both ~$1)
- Even if wstETH crashes 50% (to $7,500), Bob won't be liquidated
  - Remaining collateral: $15,000 sDAI + $7,500 wstETH = $22,500
  - Health Factor: ~1.91 (still healthy)
- wstETH would need to drop ~85% before liquidation becomes possible
- The sDAI acts as a "buffer" preventing liquidation

Reality: The wstETH collateral is unlikely to ever flow to USDC suppliers 
because the correlated sDAI prevents liquidation under most scenarios.

In contrast:
- Alice borrows 10,000 USDC
- Collateral: $15,000 wstETH (no stablecoin buffer)
- Health Factor: 1.28

This position has real liquidation risk. If wstETH drops ~20%, Alice gets 
liquidated and the wstETH collateral flows to USDC suppliers.

The position backing calculation treats both as "backed by wstETH",
but Bob's wstETH backing is illusory while Alice's is real.
```

---

## Architecture Considerations

### Tracking Active Users via spToken Transfers

**Problem Statement:**

Users cannot be completely tracked by only monitoring Pool supply events. spTokens (aTokens) are **transferable ERC-20 tokens**, which means:

1. **Direct Transfers:** Users can receive spTokens via `transfer()` without ever calling `supply()`
2. **"Silent" Positions:** These users have active positions but aren't captured in event-based indexing
3. **Incomplete Snapshots:** Snapshot handlers won't know to query these users

**Example Scenario:**

```
Block 100: Alice calls supply(1000 USDC)
  → Pool emits Supply event
  → Alice receives 1000 spUSDC
  → Indexer tracks Alice ✅

Block 200: Alice calls spUSDC.transfer(Bob, 500)
  → spUSDC emits Transfer event
  → Bob receives 500 spUSDC
  → Bob now has a position
  → Without Transfer tracking: Bob is invisible ❌

Block 300: Snapshot runs
  → Queries Alice's position ✅
  → Doesn't query Bob (unknown user) ❌
  → Bob's 500 spUSDC is untracked
```

**Solution: Index spToken Events (Transfer, Mint, Burn)**

**See Also:** Complete details in the [spToken Transfer Tracking](#sptoken-transfer-tracking) and [Event-Based Indexing](#event-based-indexing) sections.

**Step 1: Discover spToken Contracts**

Identify all spToken addresses for the Pool by:
- Monitoring `ReserveInitialized` events from the Pool contract
- Each event contains the spToken (aToken) address for that reserve
- Track all spToken addresses to index their events

**Step 2: Choose Indexing Approach**

**Option A: Pool Events + spToken Transfers (Recommended)**
- Index Pool `Supply`/`Withdraw` events for transaction context
- Index spToken `Transfer` events for user discovery
- Provides complete coverage with full context

**Option B: spToken Events Only (Alternative)**
- Index spToken `Mint`/`Burn`/`Transfer` events exclusively
- Single source of truth, simpler reconciliation
- Missing some transaction context (onBehalfOf, referralCode)

**Step 3: Handle spToken Events**

For each spToken event:

1. **Filter addresses to ignore:**
   - Zero address - for `from` in Mint, for `to` in Burn
   - Pool contract address - internal accounting
   - Gateway contracts - intermediate contracts, not end users

2. **Process valid events:**
   - **Transfer** (non-zero from/to): Mark both sender and recipient as active users
   - **Mint** (from=0x0): Mark `to` address as active user
   - **Burn** (to=0x0): Update `from` address position
   - Update supply positions and scaled balance snapshots for all affected users

**Benefits:**

- **Complete user coverage** (no silent positions)
- **Accurate TVL** (all supply positions tracked)
- **Comprehensive snapshots** (all users queried)
- **Correct liquidation monitoring** (health factors for all users)

**Step 4: Use Active User List in Snapshots**

Query active users (including transfer recipients) when running snapshots to ensure complete coverage. See "Tracking Active Users" section below for implementation details.

### Tracking Active Users

**Purpose:** Maintain an accurate list of all users who currently have active positions (non-zero supply or debt) to optimize snapshot operations.

**Active User Table Schema:**

```typescript
ActiveUser {
  id: hex,                      // User address (primary key)
  firstSeenBlock: bigint,       // Block when user first interacted
  firstSeenTimestamp: bigint,   // Timestamp of first interaction
  lastActivityBlock: bigint,    // Block of most recent activity
  lastActivityTimestamp: bigint,// Timestamp of most recent activity
  hasActivePosition: boolean,   // TRUE if user has any supply or debt > 0
}
```

**Tracking Methodology:**

1. **User Discovery:**
   - Add user to `ActiveUser` table on first interaction (Supply, Borrow, spToken Transfer, etc.)
   - Set `hasActivePosition = true` initially
   - Record `firstSeenBlock` and `lastActivityBlock`

2. **Activity Updates:**
   - Update `lastActivityBlock` and `lastActivityTimestamp` on every user transaction
   - Update `hasActivePosition` flag based on actual position state

3. **Position Status Determination:**
   
   A user has an active position if:
   ```
   (Total Supply Balance > 0) OR (Total Debt Balance > 0)
   ```
   
   **After each transaction:**
   - Calculate user's total supply across all reserves
   - Calculate user's total debt across all reserves
   - Update `hasActivePosition = (totalSupply > 0 || totalDebt > 0)`

4. **Snapshot Optimization:**
   
   When running periodic snapshots:
   ```sql
   SELECT user FROM ActiveUser WHERE hasActivePosition = true
   ```
   
   This query returns only users with non-zero positions, dramatically reducing:
   - RPC calls to smart contracts
   - Database write operations
   - Snapshot computation time

**Benefits:**

- **Performance:** Skip users with zero balances during snapshots
- **Accuracy:** Always reflects current position status
- **Scalability:** As protocol grows, avoid querying inactive users
- **Cost:** Reduces RPC costs by eliminating unnecessary queries

**Implementation Note:** The `hasActivePosition` flag should be updated after every position-changing event (Supply, Withdraw, Borrow, Repay, Liquidation, spToken Transfer).

### Offchain Scaled Token Accounting Optimization

**Problem Statement:**

Calculating historical health factors for many users across many blocks requires knowing each user's exact balance and debt at each point in time. Naively, this would require:
- Making RPC calls to `Pool.getUserAccountData()` for every user at every snapshot block
- For example: 1,000 users × 365 snapshots = 365,000 RPC calls
- Expensive, slow, and hits rate limits on RPC providers

**Solution: Scaled Balance Tracking**

Instead of querying smart contracts for historical state, track **scaled balances** event-by-event and calculate current balances using Sparklend's interest accrual math offchain (same as Aave V3).

**Core Concept:**

Sparklend uses the same **scaled balance system** as Aave V3 where:
```
scaledBalance = currentBalance / liquidityIndex
currentBalance = scaledBalance × liquidityIndex
```

The `liquidityIndex` and `variableBorrowIndex` grow continuously based on interest rates. By tracking **scaled balances** (which only change on transactions) and **indexes** (which update on every transaction via `ReserveDataUpdated` events), we can calculate any user's balance at any historical block **without RPC calls**.

---

#### Scaled Balance Schema

**1. Supply Position Snapshots:**

```typescript
UserScaledSupplyPosition {
  id: text,                      // `${user}-${asset}-${blockNumber}`
  user: hex,
  asset: hex,
  blockNumber: bigint,           // Block of this snapshot
  timestamp: bigint,
  scaledBalance: bigint,         // Scaled spToken balance
  isCollateral: boolean,         // Whether asset is enabled as collateral
  lastLiquidityIndex: bigint,    // liquidityIndex at this block (27 decimals)
}
```

**2. Borrow Position Snapshots:**

```typescript
UserScaledBorrowPosition {
  id: text,                      // `${user}-${asset}-${blockNumber}`
  user: hex,
  asset: hex,
  blockNumber: bigint,
  timestamp: bigint,
  scaledVariableDebt: bigint,    // Scaled variable debt
  lastVariableBorrowIndex: bigint, // variableBorrowIndex at this block (27 decimals)
}
```

**3. E-Mode Tracking:**

```typescript
UserEModeCategory {
  id: text,                      // `${user}-${blockNumber}`
  user: hex,
  blockNumber: bigint,
  timestamp: bigint,
  categoryId: integer,           // E-Mode category (0 = disabled)
}
```

---

#### Event-by-Event Tracking

**Why Event-by-Event Tracking?**

Scaled balances only change when users perform transactions (Supply, Withdraw, Borrow, Repay, Liquidation, etc.). By capturing the scaled balance at each transaction block and storing the corresponding index, we can later reconstruct the exact balance at any historical block using only the index growth formula—**without needing to query the blockchain for historical state**.

This is the key optimization: instead of making thousands of historical RPC calls, we track deltas event-by-event and calculate historical balances offchain.

**⚠️ Critical: Handling Multiple Events in the Same Block**

When multiple position-changing events occur in the same block for the same user and asset, handlers must **not overwrite** previous updates from the same block. Since the snapshot ID is `${user}-${asset}-${blockNumber}`, multiple events in the same block share the same ID.

**Two Implementation Approaches:**

**Option 1: Check and Update Existing Snapshot (Recommended)**
```typescript
// Check if a snapshot already exists for this block
const existingSnapshot = getSnapshot(user, asset, blockNumber);

if (existingSnapshot) {
  // Multiple events in same block - update existing snapshot
  const newScaledBalance = existingSnapshot.scaledBalance + scaledAmount;
  updateUserScaledSupplyPosition({
    id: `${user}-${asset}-${blockNumber}`,
    scaledBalance: newScaledBalance,
    lastLiquidityIndex: currentIndex,
  });
} else {
  // First event in this block - create new snapshot based on previous block
  const prevSnapshot = getPreviousSnapshot(user, asset, blockNumber - 1);
  const newScaledBalance = (prevSnapshot?.scaledBalance || 0n) + scaledAmount;
  insertUserScaledSupplyPosition({...});
}
```

**Option 2: Use Log Index in ID**
```typescript
// Include transaction log index in ID to store all intermediate states
const id = `${user}-${asset}-${blockNumber}-${logIndex}`;

// Always get most recent snapshot up to and including current block
const prevSnapshot = getMostRecentSnapshot(user, asset, blockNumber); // <= not <
const newScaledBalance = (prevSnapshot?.scaledBalance || 0n) + scaledAmount;

insertUserScaledSupplyPosition({
  id,
  scaledBalance: newScaledBalance,
  ...
});
```

**Track scaled balances on every position-changing event:**

**Supply Event:**
```typescript
async function trackScaledSupply(user, asset, amount, blockNumber, timestamp) {
  // 1. Get current liquidityIndex from ReserveDataUpdated event
  const reserveData = getLatestReserveDataAtBlock(asset, blockNumber);
  const currentIndex = reserveData.liquidityIndex;
  
  // 2. Convert supplied amount to scaled balance
  const scaledAmount = (amount × RAY) / currentIndex;
  
  // 3. Check if snapshot already exists for this block (multiple events)
  const existingSnapshot = getSnapshot(user, asset, blockNumber);
  
  if (existingSnapshot) {
    // Multiple events in same block - update existing
    const newScaledBalance = existingSnapshot.scaledBalance + scaledAmount;
    updateUserScaledSupplyPosition({
      id: `${user}-${asset}-${blockNumber}`,
      scaledBalance: newScaledBalance,
      lastLiquidityIndex: currentIndex,
    });
  } else {
    // First event in this block - get from previous block
    const prevSnapshot = getPreviousSnapshot(user, asset, blockNumber - 1);
    const prevScaledBalance = prevSnapshot?.scaledBalance || 0n;
    const newScaledBalance = prevScaledBalance + scaledAmount;
    
    // Store new snapshot
    insertUserScaledSupplyPosition({
      id: `${user}-${asset}-${blockNumber}`,
      user,
      asset,
      blockNumber,
      timestamp,
      scaledBalance: newScaledBalance,
      isCollateral: prevSnapshot?.isCollateral ?? true,
      lastLiquidityIndex: currentIndex,
    });
  }
}
```

**Withdraw Event:**
```typescript
async function trackScaledWithdraw(user, asset, amount, blockNumber, timestamp) {
  // Calculate scaled amount to subtract
  const reserveData = getLatestReserveDataAtBlock(asset, blockNumber);
  const scaledAmount = (amount × RAY) / reserveData.liquidityIndex;
  
  // Check if snapshot already exists for this block
  const existingSnapshot = getSnapshot(user, asset, blockNumber);
  
  if (existingSnapshot) {
    // Multiple events in same block - update existing
    const newScaledBalance = existingSnapshot.scaledBalance - scaledAmount;
    updateUserScaledSupplyPosition({
      id: `${user}-${asset}-${blockNumber}`,
      scaledBalance: newScaledBalance >= 0n ? newScaledBalance : 0n,
      lastLiquidityIndex: reserveData.liquidityIndex,
    });
  } else {
    // First event in this block - get from previous block
    const prevSnapshot = getPreviousSnapshot(user, asset, blockNumber - 1);
    const newScaledBalance = prevSnapshot.scaledBalance - scaledAmount;
    
    // Store updated snapshot (even if balance becomes 0)
    insertUserScaledSupplyPosition({
      id: `${user}-${asset}-${blockNumber}`,
      user,
      asset,
      blockNumber,
      timestamp,
      scaledBalance: newScaledBalance >= 0n ? newScaledBalance : 0n,
      isCollateral: prevSnapshot?.isCollateral ?? true,
      lastLiquidityIndex: reserveData.liquidityIndex,
    });
  }
}
```

**Borrow/Repay Events:** Similar logic using `variableBorrowIndex` instead of `liquidityIndex`. Same rules apply for handling multiple events in the same block.

**Collateral Toggle Events:**
```typescript
async function trackCollateralEnabled(user, asset, blockNumber, timestamp) {
  // Check if snapshot already exists for this block
  const existingSnapshot = getSnapshot(user, asset, blockNumber);
  
  if (existingSnapshot) {
    // Update existing snapshot's collateral flag
    updateUserScaledSupplyPosition({
      id: `${user}-${asset}-${blockNumber}`,
      isCollateral: true,
    });
  } else {
    // Get previous block's snapshot and create new one with updated flag
    const prevSnapshot = getPreviousSnapshot(user, asset, blockNumber - 1);
    
    insertUserScaledSupplyPosition({
      id: `${user}-${asset}-${blockNumber}`,
      user,
      asset,
      blockNumber,
      timestamp,
      scaledBalance: prevSnapshot?.scaledBalance || 0n,
      isCollateral: true,  // Enable collateral
      lastLiquidityIndex: prevSnapshot?.lastLiquidityIndex || 0n,
    });
  }
}
```

**E-Mode Events:**
```typescript
async function trackEModeSet(user, categoryId, blockNumber, timestamp) {
  // E-Mode is per-user (not per-asset), so check if already set in this block
  const existingEMode = getEModeSnapshot(user, blockNumber);
  
  if (existingEMode) {
    // Update existing E-Mode setting for this block
    updateUserEModeCategory({
      id: `${user}-${blockNumber}`,
      categoryId,
    });
  } else {
    // Create new E-Mode snapshot
    insertUserEModeCategory({
      id: `${user}-${blockNumber}`,
      user,
      blockNumber,
      timestamp,
      categoryId,
    });
  }
}
```

---

#### Health Factor Calculation Using Scaled Balances

**Step-by-Step Process:**

**1. Get User's Latest Scaled Positions:**

For a given user at block N:
```sql
-- Get latest supply snapshot for each asset before or at block N
SELECT DISTINCT ON (asset)
  asset, scaledBalance, isCollateral, lastLiquidityIndex
FROM UserScaledSupplyPosition
WHERE user = $user AND blockNumber <= $blockN
ORDER BY asset, blockNumber DESC

-- Get latest borrow snapshot for each asset before or at block N
SELECT DISTINCT ON (asset)
  asset, scaledVariableDebt, lastVariableBorrowIndex
FROM UserScaledBorrowPosition
WHERE user = $user AND blockNumber <= $blockN
ORDER BY asset, blockNumber DESC

-- Get latest E-Mode setting before or at block N
SELECT categoryId
FROM UserEModeCategory
WHERE user = $user AND blockNumber <= $blockN
ORDER BY blockNumber DESC
LIMIT 1
```

**2. Calculate Current Indexes:**

For each reserve, get the latest `ReserveDataUpdated` event at or before block N:
```sql
SELECT liquidityRate, liquidityIndex, variableBorrowRate, variableBorrowIndex, timestamp
FROM ReserveDataUpdated
WHERE reserve = $asset AND blockNumber <= $blockN
ORDER BY blockNumber DESC
LIMIT 1
```

Then calculate current indexes using Sparklend's formula (identical to Aave V3):
```typescript
function calculateCurrentIndex(
  lastIndex: bigint,
  lastRate: bigint,
  lastTimestamp: bigint,
  currentTimestamp: bigint
): bigint {
  const timeDelta = currentTimestamp - lastTimestamp;
  
  // Linear interest approximation: linearInterest = rate × timeDelta / secondsPerYear
  const linearInterest = (lastRate × timeDelta) / SECONDS_PER_YEAR;
  
  // newIndex = oldIndex × (1 + linearInterest)
  const newIndex = (lastIndex × (RAY + linearInterest)) / RAY;
  
  return newIndex;
}
```

**3. Calculate Current Balances:**

```typescript
// Supply balance with accrued interest
const currentBalance = (scaledBalance × currentLiquidityIndex) / RAY;

// Debt with accrued interest
const currentDebt = (scaledVariableDebt × currentVariableBorrowIndex) / RAY;
```

**4. Get Asset Prices:**

Asset prices must be fetched for each snapshot block to calculate USD values. Two approaches:

**Option A: Use Previously Stored Reserve Snapshots**
If you've already created `ReserveMarketSnapshot` records at this block:
```sql
SELECT priceInMarketReferenceCurrency
FROM ReserveMarketSnapshot
WHERE asset = $asset AND blockNumber = $blockN
```

**Option B: Query Directly from Smart Contract**
If snapshots don't exist yet, make an RPC call to get reserve data:
- Call `UiPoolDataProviderV3.getReservesData(provider)` at block N
- Extract `priceInMarketReferenceCurrency` for each reserve (8 decimals in USD)

**Important Note:** While this requires RPC calls, it's still a massive optimization:
- **Without optimization:** 1 RPC call per user per snapshot block
- **With optimization:** 1 RPC call per snapshot block (shared across all users)
- For 1,000 users: **99.9% reduction** in RPC calls (1 call vs. 1,000 calls per snapshot)

**5. Calculate Health Factor:**

```typescript
async function calculateHealthFactor(user, blockNumber, timestamp) {
  const supplyPositions = getLatestScaledSupplyPositions(user, blockNumber);
  const borrowPositions = getLatestScaledBorrowPositions(user, blockNumber);
  const eModeCategory = getLatestEModeCategory(user, blockNumber);
  
  let totalCollateralBase = 0n;
  let totalCollateralTimesLT = 0n;
  let totalDebtBase = 0n;
  
  // Calculate collateral
  for (const position of supplyPositions) {
    if (!position.isCollateral) continue;
    
    // Get current index and calculate current balance
    const reserveData = getLatestReserveData(position.asset, blockNumber);
    const currentIndex = calculateCurrentIndex(
      reserveData.liquidityIndex,
      reserveData.liquidityRate,
      reserveData.timestamp,
      timestamp
    );
    const currentBalance = (position.scaledBalance × currentIndex) / RAY;
    
    // Get price and reserve parameters
    const price = getAssetPrice(position.asset, blockNumber);
    const reserveConfig = getReserveConfiguration(position.asset);
    
    // Determine liquidation threshold (E-Mode or standard)
    let liquidationThreshold = reserveConfig.liquidationThreshold;
    if (eModeCategory > 0 && reserveConfig.eModeCategory === eModeCategory) {
      liquidationThreshold = getEModeLiquidationThreshold(eModeCategory);
    }
    
    // Calculate value in base currency (8 decimals)
    const valueBase = (currentBalance × price) / (10n ** assetDecimals);
    
    totalCollateralBase += valueBase;
    totalCollateralTimesLT += (valueBase × BigInt(liquidationThreshold)) / 10000n;
  }
  
  // Calculate debt
  for (const position of borrowPositions) {
    const reserveData = getLatestReserveData(position.asset, blockNumber);
    const currentIndex = calculateCurrentIndex(
      reserveData.variableBorrowIndex,
      reserveData.variableBorrowRate,
      reserveData.timestamp,
      timestamp
    );
    const currentDebt = (position.scaledVariableDebt × currentIndex) / RAY;
    
    const price = getAssetPrice(position.asset, blockNumber);
    const valueBase = (currentDebt × price) / (10n ** assetDecimals);
    
    totalDebtBase += valueBase;
  }
  
  // Calculate health factor
  if (totalDebtBase === 0n) {
    return { healthFactor: MAX_UINT256, totalCollateralBase, totalDebtBase };
  }
  
  // HF = (totalCollateral × avgLT) / totalDebt
  // avgLT = totalCollateralTimesLT / totalCollateralBase
  const healthFactor = (totalCollateralTimesLT × RAY) / (totalDebtBase × 10000n);
  
  return { healthFactor, totalCollateralBase, totalDebtBase };
}
```

---

#### Reserve Data Requirements

**For accurate offchain calculations, index these events/data per reserve:**

1. **ReserveDataUpdated Events** (critical):
   - `liquidityIndex` - For calculating supply balances
   - `variableBorrowIndex` - For calculating borrow balances
   - `liquidityRate` - For projecting index growth
   - `variableBorrowRate` - For projecting index growth
   - `timestamp` - For time-based calculations

2. **Reserve Configuration** (from `PoolConfigurator` or `UiPoolDataProviderV3`):
   - `decimals` - Token decimals
   - `liquidationThreshold` - Standard LT (basis points)
   - `ltv` - Loan-to-value ratio
   - `eModeCategory` - Which E-Mode category this reserve belongs to

3. **E-Mode Configuration** (per category):
   - `liquidationThreshold` - E-Mode LT (higher than standard)
   - `ltv` - E-Mode LTV (higher than standard)
   - `liquidationBonus` - E-Mode liquidation bonus

4. **Asset Prices**:
   - Store periodic price snapshots from oracle
   - Use for calculating USD values

---

#### E-Mode Accounting

**E-Mode affects liquidation thresholds and LTV:**

**When user has E-Mode enabled (categoryId > 0):**

1. **For each collateral asset:**
   ```typescript
   if (asset.eModeCategory === user.eModeCategory) {
     // Use E-Mode parameters
     liquidationThreshold = eModeConfig[categoryId].liquidationThreshold;
     ltv = eModeConfig[categoryId].ltv;
   } else {
     // Use standard parameters
     liquidationThreshold = reserveConfig.liquidationThreshold;
     ltv = reserveConfig.ltv;
   }
   ```

2. **Health factor calculation includes both:**
   ```
   HF = (In-Category Collateral × E-Mode LT + Out-Category Collateral × Standard LT) / Total Debt
   ```

3. **Track E-Mode changes:**
   - Store `UserEModeCategory` snapshot on every `UserEModeSet` event
   - Query latest E-Mode setting at calculation time

---

#### Accuracy Considerations

**Offchain Math Limitations:**

The offchain approach uses **linear interest approximation**:
```
newIndex ≈ oldIndex × (1 + rate × timeDelta / secondsPerYear)
```

Sparklend's actual on-chain math (inherited from Aave V3) uses **compound interest** with per-second compounding. The approximation is:
- ✅ **Very accurate** for short time periods (< 1 day)
- ⚠️ **Slightly less accurate** for longer periods without updates
- ❌ **Can drift** if no `ReserveDataUpdated` events occur for extended periods

**Recommended: Periodic Smart Contract Refresh**

To maintain accuracy, implement a **periodic refresh job** that:

1. **Queries actual on-chain state** for all reserves:
   - Get all active users from tracking table
   - For each user, call `Pool.getUserAccountData()` and `UiPoolDataProviderV3.getUserReservesData()`
   - Update scaled balance snapshots in database with ground truth from chain
   - Store both scaled balances (`scaledATokenBalance`, `scaledVariableDebt`) and current balances

2. **Benefits:**
   - Corrects any drift from linear approximation
   - Catches any missed events
   - Ensures database stays synchronized with chain state
   - Dramatically reduces RPC usage compared to querying historical state

3. **Frequency Considerations:**
   - More frequent = more accurate but higher RPC costs
   - Less frequent = lower costs but potential drift
   - Suggested: Once per day, or based on specific accuracy requirements
   - Can adjust frequency dynamically based on market volatility

---

#### Complete Snapshot Workflow

**Historical Snapshot Calculation Process:**

High-level workflow for calculating snapshots across a block range:

1. **Get all users with positions** from active user tracking

2. **Determine snapshot blocks** based on desired granularity
   - Example: Every N blocks for periodic snapshots
   - Or: Specific blocks of interest (liquidations, major events)

3. **For each snapshot block:**
   - Get block timestamp
   - **Make 1 RPC call** to fetch reserve data (prices, indexes, rates) for all reserves
     - Call `UiPoolDataProviderV3.getReservesData(provider)` at this block
     - This returns data for all reserves in a single call
   - Cache this reserve data for all user calculations at this block
   
4. **For each user at this block:**
   - Get user's latest scaled positions from database (no RPC needed)
   - Calculate current balances using cached indexes from step 3 (no RPC needed)
   - Calculate health factor using cached prices from step 3 (no RPC needed)
   - Store `UserHealthFactorHistory` record
   - Store detailed `UserPositionBreakdown` for collateral and debt

5. **Result:** Complete historical snapshots with minimal RPC usage
   - **Only 1 RPC call per snapshot block** (regardless of number of users)
   - For 1,000 users across 100 blocks: 100 RPC calls instead of 100,000
   - **99.9% reduction** in RPC usage vs. querying each user individually

---

#### Performance Comparison

**Without Scaled Balance Optimization:**
- Example: 1,000 users × 365 snapshots = 365,000 RPC calls
- ~10-30 seconds per RPC call = Days of indexing time
- High cost from RPC provider
- Rate limiting issues

**With Scaled Balance Optimization:**
- 0 RPC calls for historical data (uses event-derived scaled balances)
- Optional: Periodic refresh calls (frequency based on accuracy requirements)
- Indexing completes in minutes instead of days
- 99%+ reduction in RPC usage

**Trade-off:** Slightly less accurate due to linear approximation, but accuracy maintained via periodic refreshes.

---

## References

### Official Documentation

- **Sparklend Documentation:** https://docs.spark.fi/
- **Sky (MakerDAO) Website:** https://sky.money/

### GitHub Repositories

- **Aave V3 Core** (parent protocol): https://github.com/aave/aave-v3-core
- **Aave V3 Periphery** (UiPoolDataProviderV3): https://github.com/aave/aave-v3-periphery

### Technical Resources

- **Aave V3 Technical Paper** (architecture basis): https://github.com/aave/aave-v3-core/blob/master/techpaper/Aave_V3_Technical_Paper.pdf

### Finding Contract Addresses

Contract addresses for Sparklend Pools can be found through:
- **Sparklend Documentation:** https://docs.spark.fi/
- **PoolAddressesProvider:** Query this contract to discover other protocol contracts for a Pool
- **Chain Explorers:** Verify contracts on Etherscan (Mainnet), Gnosisscan (Gnosis), etc.

### Additional Resources

- **Aave V3 Core (parent protocol):** https://github.com/aave/aave-v3-core - Reference implementation

---

**Document Version:** 1.0  
**Last Updated:** January 2026  



