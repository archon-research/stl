# Real-World Asset (RWA) Tokenization Platforms Specification

**Version:** 1.0  
**Last Updated:** January 2026  
**Purpose:** Technical reference for understanding RWA tokenization platforms and data retrieval methods

---

## Table of Contents

1. [Introduction to Blockchain-Based RWAs](#introduction-to-blockchain-based-rwas)
2. [ERC-4626 Tokenized Vaults Review](#erc-4626-tokenized-vaults-review)
3. [Arkis](#arkis)
4. [Anchorage](#anchorage)
5. [Centrifuge](#centrifuge)
6. [Securitize](#securitize)
7. [Galaxy/Arch](#galaxyarch)
8. [References](#references)

---

## Introduction to Blockchain-Based RWAs

### What are Real-World Assets (RWAs)?

Real-World Assets (RWAs) represent the tokenization of off-chain, tangible or intangible assets on blockchain networks. These can include:

- **Credit instruments**: Loans, receivables, structured credit
- **Real estate**: Commercial or residential properties
- **Treasury instruments**: Government bonds, T-bills
- **Commodities**: Gold, carbon credits, agricultural products
- **Securities**: Stocks, bonds, fund shares

RWA tokenization brings transparency, programmability, and composability to traditionally illiquid or difficult-to-access assets.

### Common Architectural Models

RWA platforms typically follow one of several architectural patterns:

#### ERC-4626 Vault Model

**Structure:**
- Vault smart contract implements ERC-4626 standard
- Users deposit stablecoins (typically USDC/USDT) and receive tokenized shares
- Vault administrators deploy capital off-chain to underlying assets
- Share price increases as yield accrues from underlying assets
- On-chain redemption mechanism for converting shares back to underlying

**Key Components:**
```solidity
interface IERC4626 {
    function totalAssets() external view returns (uint256);
    function convertToAssets(uint256 shares) external view returns (uint256);
    function convertToShares(uint256 assets) external view returns (uint256);
    function deposit(uint256 assets, address receiver) external returns (uint256 shares);
    function redeem(uint256 shares, address receiver, address owner) external returns (uint256 assets);
}
```

**Pricing:**
- Share price = `totalAssets() / totalSupply()`
- Use `convertToAssets()` for accurate per-share valuation
- NAV (Net Asset Value) updates reflect off-chain asset performance

#### ERC-4626-Derived Variants

Some platforms implement modified versions of ERC-4626 with additional features:

- **Asynchronous redemptions (ERC-7540)**: Request-based deposit/redeem flows
- **Multi-asset vaults (ERC-7575)**: Accept multiple currencies for the same share token
- **Tranched structures**: Senior/junior share classes with different risk profiles

#### Simple ERC-20 Model

**Structure:**
- Custom ERC-20 token represents ownership or claim on underlying asset
- Off-chain API or oracle provides pricing data
- Custom smart contract infrastructure for minting/burning
- May or may not have automatic on-chain redemption

**Pricing:**
- Typically relies on off-chain price feed or API endpoint
- Manual or periodic NAV updates pushed on-chain
- Price oracle contract may be used for integration with DeFi

### Token Restrictions & Compliance

Many RWA tokens include restrictions uncommon in standard ERC-20 tokens:

**Transfer Restrictions:**
```solidity
// KYC/AML gating via transfer hooks
function _beforeTokenTransfer(address from, address to, uint256 amount) internal {
    require(isWhitelisted(from) && isWhitelisted(to), "Not whitelisted");
    require(!isFrozen(from) && !isFrozen(to), "Account frozen");
}
```

**Common Restrictions:**
- **Whitelisting**: Only KYC-verified addresses can hold tokens
- **Geographic restrictions**: Certain jurisdictions excluded
- **Investor accreditation**: Minimum net worth or income requirements
- **Lock-up periods**: Time-based restrictions on transfers or redemptions
- **Freezing mechanisms**: Admin ability to freeze accounts

**DeFi Composability Implications:**

These restrictions significantly limit DeFi integration:
- **No permissionless pools**: Cannot be used in standard AMMs (Uniswap, Curve)
- **Lending limitations**: Most lending protocols cannot accept restricted tokens as collateral
- **Limited composability**: Cannot be freely used in yield aggregators or strategies
- **Specialized integrations required**: Only whitelisted smart contracts can interact

### Data Retrieval Patterns

**On-Chain Data:**
- Token balances: `balanceOf(address)`
- Total supply: `totalSupply()`
- Share price (ERC-4626): `convertToAssets(1e18)`
- Vault TVL (ERC-4626): `totalAssets()`

**Off-Chain Data:**
- Underlying asset composition and valuation
- NAV calculation methodologies
- Historical performance metrics
- Legal structure and documentation
- Audit reports and compliance data

**Hybrid Approaches:**
- On-chain NAV oracles updated periodically
- API endpoints for real-time pricing
- Subgraph indexing for historical data
- Proprietary SDKs for data access

---

## ERC-4626 Tokenized Vaults Review

### Overview

ERC-4626 is the Tokenized Vault Standard, defining a standardized interface for yield-bearing vaults. This standard is widely used in RWA tokenization.

### Assets vs Shares Model

**Assets:**
- Underlying tokens (typically USDC, USDT, DAI)
- What users deposit and withdraw
- Represent actual value in the vault

**Shares:**
- ERC-20 tokens representing vault ownership
- Minted on deposit, burned on withdrawal
- Appreciate in value as yield accrues

### Core Functions

```solidity
// Deposit assets, receive shares
function deposit(uint256 assets, address receiver) external returns (uint256 shares);

// Withdraw assets, burn shares  
function redeem(uint256 shares, address receiver, address owner) external returns (uint256 assets);

// Vault state
function totalAssets() external view returns (uint256);  // Total assets including accrued yield
function totalSupply() external view returns (uint256);  // Total shares outstanding

// Conversions
function convertToShares(uint256 assets) external view returns (uint256 shares);
function convertToAssets(uint256 shares) external view returns (uint256 assets);
```

### Share Price Mechanics

**Calculation:**
```typescript
// Get share price (assets per 1 share)
const sharePrice = await vault.convertToAssets(1e18);  // For 18-decimal shares

// Or manually (less accurate):
const totalAssets = await vault.totalAssets();
const totalSupply = await vault.totalSupply();
const sharePriceManual = totalAssets / totalSupply;
```

**Note:** Always use `convertToAssets()` for accurate conversion. Manual calculation may be inaccurate due to vault-specific mechanics.

**Share Price Appreciation:**
```
Initial: 1,000,000 USDC, 1,000,000 shares → Share Price = 1.00

After 30 days, yield accrues (+10,000 USDC):
  Total: 1,010,000 USDC, 1,000,000 shares → Share Price = 1.01
  
User holding 10,000 shares:
  Initial value: 10,000 USDC
  Current value: 10,100 USDC
  Profit: 100 USDC (1% return)
```

### Position Valuation

**Get User Position Value:**
```typescript
// Get user's shares
const shares = await vaultContract.balanceOf(userAddress);

// Convert to asset value (recommended method)
const assets = await vaultContract.convertToAssets(shares);

// USD value - assuming underlying asset price = $1.00
const price = 1.0;  // Hardcoded to $1 for stablecoins like USDC/USDT
const decimals = await vaultContract.decimals();  // Typically 18
const usdValue = (assets / (10 ** decimals)) * price;
```

### APY Calculation

**Method: Share Price Changes Over Time**

```typescript
// Get share price at two points in time
const sharePrice1 = await vaultContract.convertToAssets(1e18, { blockTag: block1 });
const sharePrice2 = await vaultContract.convertToAssets(1e18, { blockTag: block2 });
const timestamp1 = (await provider.getBlock(block1)).timestamp;
const timestamp2 = (await provider.getBlock(block2)).timestamp;

// Calculate APY with compounding
const SECONDS_PER_YEAR = 365 * 24 * 60 * 60;
const timeDelta = timestamp2 - timestamp1;
const periodsPerYear = SECONDS_PER_YEAR / timeDelta;
const returnRate = sharePrice2 / sharePrice1;  // e.g., 1.01 for 1% return

// APY = (1 + periodReturn)^periodsPerYear - 1
const apy = Math.pow(returnRate, periodsPerYear) - 1;
const apyPercent = apy * 100;
```

### TVL Calculation

**Single Vault:**
```typescript
const totalAssets = await vaultContract.totalAssets();
const decimals = await vaultContract.decimals();
const tvlUsd = totalAssets / (10 ** decimals);  // Assuming underlying = $1.00
```

**Multi-Chain Protocol:**
```typescript
// If vaults are deployed across multiple chains, query each chain separately
const chains = [1, 8453, 42161];  // Ethereum, Base, Arbitrum
let totalTvl = 0;

for (const chainId of chains) {
    const provider = getProviderForChain(chainId);
    const vault = new ethers.Contract(vaultAddress, abi, provider);
    const totalAssets = await vault.totalAssets();
    const decimals = 6;  // USDC/USDT typically use 6 decimals
    totalTvl += totalAssets / (10 ** decimals);
}
```

---

## Arkis

### Protocol Overview

[Arkis](https://www.arkis.xyz/) is a **digital asset prime brokerage credit protocol** for institutional borrowers and lenders. The protocol enables:

- **For Lenders**: Decentralized credit provision with whitelisting, guaranteed repayment flow, and overcollateralization benefits
- **For Borrowers**: Capital-efficient leverage (up to 5x) with exotic collateral and portfolio margining options

Arkis addresses DeFi fragmentation across blockchains, CeFi/DeFi venues, and protocols through margin trading smart contracts and a unified portfolio margining system that considers a trader's entire portfolio rather than isolated positions.

**Documentation**: [docs.arkis.xyz](https://docs.arkis.xyz/home)  
**GitHub**: [github.com/ArkisXYZ](https://github.com/ArkisXYZ) *(Note: Repository contains minimal code; primarily whitepaper and audit reports)*

### Vault Structure

Arkis vaults are **modified ERC-4626** contracts with additional features:

- **Performance Fees**: Fees accrue to a curator based on vault performance
- **Investor Whitelisting**: Private vaults restrict deposits to approved addresses
- **Market Allocation**: Curator can deploy vault assets to whitelisted markets (other yield strategies)
- **Checkpointing**: Captures performance fees and updates exchange rate

### Retrieving Key Data

**Position Value:**

```typescript
// Standard ERC-4626 methods work
const shares = await vault.balanceOf(userAddress);
const assets = await vault.convertToAssets(shares);

// Assuming stablecoin underlying (e.g., USDC with 6 decimals)
const decimals = await ERC20(await vault.asset()).decimals();
const usdValue = Number(assets) / (10 ** decimals);
```

**Exchange Rate (Share Price):**

Arkis vaults provide an `exchangeRate()` function for precise performance tracking:

```typescript
// Exchange rate with 24 decimal precision
const exchangeRate = await vault.exchangeRate();
// exchangeRate = totalAssets * 1e24 / totalSupply

// Convert to human-readable (assets per share)
const sharePrice = Number(exchangeRate) / 1e24;

// Example: exchangeRate = 1050000000000000000000000 means 1.05 assets per share
```

**Why use `exchangeRate()`?**
- 24-decimal precision (vs 18 for standard shares)
- Consistent scaling across different asset decimals
- Easier performance comparison over time

**APY Calculation:**

```typescript
// Snapshot exchange rate at two points in time
const rate1 = await vault.exchangeRate({ blockTag: block1 });
const rate2 = await vault.exchangeRate({ blockTag: block2 });
const timestamp1 = (await provider.getBlock(block1)).timestamp;
const timestamp2 = (await provider.getBlock(block2)).timestamp;

// Calculate APY
const timeDelta = timestamp2 - timestamp1;
const periodsPerYear = (365 * 24 * 60 * 60) / timeDelta;
const returnRate = Number(rate2) / Number(rate1);
const apy = Math.pow(returnRate, periodsPerYear) - 1;
```

**Pending Performance Fees:**

```typescript
const [feeAssets, feeShares] = await vault.pendingFee();
// feeAssets: Assets to be taken as performance fee (in asset decimals)
// feeShares: Shares to be minted to curator upon next checkpoint
```

**Vault Metadata:**

```typescript
const info = await vault.info();
// Returns:
// - asset: Underlying asset address (e.g., USDC)
// - totalAssetsThreshold: Max TVL cap (0 = unlimited)
// - markets: Whitelisted markets for asset allocation
// - investors: Whitelisted investors (if private vault)
// - curator: Address that manages allocations and receives fees
// - performanceFee: Fee in basis points (e.g., 2000 = 20%)
// - isPrivate: Whether vault restricts deposits
```

**TVL:**

```typescript
const totalAssets = await vault.totalAssets();
const decimals = await ERC20(await vault.asset()).decimals();
const tvl = Number(totalAssets) / (10 ** decimals);
```

### Key Contract Methods

```solidity
// Standard ERC-4626
function asset() view returns (address)
function totalAssets() view returns (uint256)
function convertToAssets(uint256 shares) view returns (uint256)
function balanceOf(address) view returns (uint256)

// Arkis-specific
function exchangeRate() view returns (uint256 rate)  // 24 decimals
function pendingFee() view returns (uint256 feeAssets, uint256 feeShares)
function info() view returns (Metadata memory)
function curator() view returns (address)
function suspended() view returns (bool)
function closed() view returns (bool)
```

**Note**: Arkis vaults may be paused (`suspended()`) or permanently closed (`closed()`), which affects deposit/withdrawal availability. Always check these states before attempting transactions.

### Asset Backing Information

For detailed information about the **real-world assets** or **digital assets** backing an Arkis vault (e.g., collateral composition, borrower positions, loan terms), refer to the [Arkis platform documentation](https://docs.arkis.xyz/home) and vault-specific disclosure materials.

Key information typically includes:
- Borrower identity and creditworthiness
- Collateral types and ratios
- Loan terms and maturity dates
- Market allocations and strategies
- Historical performance and default rates

This information is generally provided off-chain through:
- Vault disclosure documents
- Arkis platform interface
- Curator communications
- Periodic performance reports

The on-chain data focuses on vault mechanics (TVL, share price, fees) rather than granular asset composition.

---

## Anchorage

### Protocol Overview

[Anchorage Digital](https://www.anchorage.com/) is a federally chartered digital asset bank providing institutional custody, staking, and tokenization services. Anchorage offers tokenized real-world assets through regulated structures.

### Current Implementation

At this time, Anchorage does not have publicly accessible developer documentation or a GitHub repository for RWA tokenization. RWA tokens are not currently issued on-chain. Instead, RWA purchases are conducted by sending USDC to an Anchorage-controlled EOA (Externally Owned Account), which is then offramped via FalconX Centralized Exchange.

### Asset Backing Information

For detailed information about the **real-world assets** backing Anchorage tokenized products (e.g., asset composition, custody arrangements, regulatory compliance), refer to the [Anchorage platform](https://www.anchorage.com/) and product-specific disclosure documents.

Key information typically includes:
- Underlying asset composition and custody details
- Regulatory framework and compliance structure
- Issuer identity and legal structure
- Redemption mechanisms and terms
- Audit and attestation reports

This information is provided off-chain through:
- Anchorage platform documentation
- Product disclosure materials
- Regulatory filings
- Client-specific communications
- Third-party audits and attestations

The on-chain data focuses on token transfers, balances, and vault mechanics rather than granular asset composition.

---

## Centrifuge

### Protocol Overview

Centrifuge is an open-source, decentralized protocol for tokenizing and distributing financial products across multiple blockchain networks. The protocol enables issuers to create asset-backed tokens with configurable compliance controls and multi-chain distribution.

**Key Features:**
- Multi-chain deployment (Ethereum, Base, Arbitrum, Avalanche, Plume, BSC)
- ERC-4626 and ERC-7540 vault implementations
- Hub-and-spoke architecture for cross-chain coordination
- On-chain accounting and NAV calculation
- Permissioned and permissionless vault options

### Multi-Chain Deployment

Centrifuge uses a hub-and-spoke model where:

**Hub Chain:**
- Central accounting and control layer
- NAV calculations and pricing updates
- Pool management and asset allocation
- Consolidated double-entry bookkeeping

**Spoke Chains:**
- Tokenization and distribution layer
- ERC-20 share token deployments
- ERC-4626/ERC-7540 vaults for user deposits/redemptions
- Transfer hooks for compliance logic

**Currently Deployed:**
- Ethereum (Chain ID: 1)
- Base (Chain ID: 8453)
- Arbitrum (Chain ID: 42161)
- Avalanche (Chain ID: 43114)
- Plume (Chain ID: 98866)
- BNB Smart Chain (Chain ID: 56)

### Vault Types

#### Synchronous Vaults (ERC-4626)

Real-time minting and redemption for liquid strategies:

```typescript
// Standard ERC-4626 deposit
const tx = await vault.deposit(
    ethers.utils.parseUnits("1000", 6),  // 1000 USDC
    userAddress
);

// Returns shares immediately
const shares = await vault.balanceOf(userAddress);
```

**Use cases:**
- Highly liquid on-chain strategies
- DeFi-native integrations
- Instant liquidity provision

#### Asynchronous Vaults (ERC-7540)

Request-based flow for RWA strategies with off-chain components:

```typescript
// Submit deposit request
await vault.requestDeposit(ethers.utils.parseUnits("1000", 6), userAddress);

// Wait for request processing (hours to days)
// ...

// Check claimable amount
const claimable = await vault.claimableDepositRequest(userAddress);

// Claim shares after approval
if (claimable.shares > 0) {
    await vault.claimDeposit(userAddress);
}
```

**Use cases:**
- RWA strategies with off-chain asset deployment
- Compliance review requirements
- Subscription period/epoch-based issuance

### Retrieving Key Data

**Centrifuge Architecture Quick Reference:**
- **PoolManager** (`0x9c8454A506263549f07c80698E276e3622077098` on Ethereum): `getVault(poolId, trancheId, assetAddress)` returns vault
- **ShareToken**: Restricted ERC-20 representing pool shares
- **Vault**: ERC-7540 `AsyncVault` (extends ERC-4626) - use standard methods: `convertToAssets()`, `totalAssets()`

---

#### Position Value

**Prerequisites - Finding poolId and trancheId:**

If you have a Share Token address, find its `DeployTranche` event from the PoolManager contract:

```typescript
// PoolManager emits DeployTranche event when ShareToken is deployed
const poolManager = new ethers.Contract(
  "0x9c8454A506263549f07c80698E276e3622077098",
  ["event DeployTranche(uint64 indexed poolId, bytes16 indexed trancheId, address indexed tranche)"],
  provider
);

// Query for the event where the ShareToken was deployed
const filter = poolManager.filters.DeployTranche(null, null, shareTokenAddress);
const events = await poolManager.queryFilter(filter, 0, 'latest');
const poolId = Number(events[0].args.poolId);  // Convert BigInt to number
const trancheId = events[0].args.trancheId;  // bytes16
```

**Centrifuge GitHub**: Smart contract source code and ABIs can be found at [github.com/centrifuge/liquidity-pools](https://github.com/centrifuge/liquidity-pools).

**Alternative - GraphQL API for Pool/Token Discovery:**

```graphql
query GetPoolTokens {
  pools(where: { isActive: true }, limit: 20) {
    items {
      id
      name
      centrifugeId
      tokens {
        items {
          id
          name
          symbol
          tokenPrice
          totalIssuance
        }
      }
    }
  }
}
```

To get share token addresses on specific chains, query `TokenInstance` (use token `id` from previous query):

```graphql
query GetTokenDeployments($tokenId: String!) {
  tokenInstances(where: { tokenId: $tokenId }) {
    items {
      address
      blockchain {
        chainId
        name
      }
    }
  }
}
```

Endpoint: `https://api.centrifuge.io/`

This returns the ShareToken contract addresses deployed on each blockchain for that token.

**On-Chain Method:**

```typescript
// Get vault address
const poolManager = new ethers.Contract("0x9c8454A506263549f07c80698E276e3622077098", [
    "function getVault(uint64,bytes16,address) view returns (address)"
], provider);
const vaultAddress = await poolManager.getVault(poolId, trancheId, assetAddress);

// Query user position
const shares = await shareToken.balanceOf(userAddress);
const assets = await vault.convertToAssets(shares);

// Calculate USD value (assuming stablecoin = $1)
const decimals = 6;  // USDC/USDT typically have 6 decimals
const usdValue = Number(assets) / (10 ** decimals);
```

**SDK Method:**

```typescript
const centrifuge = new Centrifuge({ environment: "mainnet" });
const pool = await centrifuge.pool(new PoolId(4139607887));
const vault = await pool.vault(chainId, shareClassId, assetId);
const investment = await vault.investment(userAddress);
console.log(`Position value: ${investment.assetBalance}`);
```

#### APY Calculation

Centrifuge vaults use ERC-7540 (extends ERC-4626), so APY is calculated from share price changes:

```typescript
// Get share price at two different blocks
const sharePrice1 = await vault.convertToAssets(1e18, { blockTag: pastBlock });
const sharePrice2 = await vault.convertToAssets(1e18, { blockTag: currentBlock });

// Calculate APY with compounding
const timeDelta = timestamp2 - timestamp1;
const periodsPerYear = (365 * 24 * 60 * 60) / timeDelta;
const returnRate = Number(sharePrice2) / Number(sharePrice1);
const apy = Math.pow(returnRate, periodsPerYear) - 1;
```

#### Asset Backing Information

For detailed information about the **real-world assets** backing a Centrifuge pool (e.g., loan portfolios, collateral composition), refer to the [Centrifuge App](https://app.centrifuge.io/) and pool-specific documentation. This information is typically provided off-chain through pool reports, disclosure documents, and the Centrifuge web interface.

The Centrifuge GraphQL API does not provide granular details about underlying asset composition or loan-level data. It focuses on on-chain protocol data like token prices, issuance, and investor positions.

#### TVL Calculation

```typescript
// Single vault TVL
const totalAssets = await vault.totalAssets();
const tvlUsd = Number(totalAssets) / 1e6;  // Assuming 6 decimal stablecoin

// Multi-chain: Query each chain separately and sum
```

### Finding Asset Documentation

For detailed information about a specific Centrifuge pool's underlying assets:

**1. Pool Metadata (SDK/API):**

```typescript
const metadata = await pool.metadata();

console.log(`Pool: ${metadata.name}`);
console.log(`Issuer: ${metadata.issuerName}`);
console.log(`Description: ${metadata.description}`);
console.log(`Asset Class: ${metadata.assetClass}`);

// Links to documentation
if (metadata.details) {
    console.log(`Website: ${metadata.details.website}`);
    console.log(`Documentation: ${metadata.details.documents}`);
}
```

**2. Pool Details Page:**

Visit the Centrifuge app: `https://app.centrifuge.io/pools/{poolId}`

This includes:
- Executive summary
- Asset details and composition
- Financial reports
- Legal structure
- Audit reports

**3. GraphQL Query for Pool Metadata:**

```graphql
query PoolMetadata($poolId: String!) {
  pool(id: $poolId) {
    id
    name
    isActive
    asset {
      symbol
      decimals
    }
    tokens {
      items {
        name
        symbol
        tokenPrice
        totalIssuance
      }
    }
  }
}
```

### Smart Contract Addresses

All Centrifuge contracts are deployed at the same address across all supported chains.

**Key Contracts:**

| Contract | Address | Networks |
|----------|---------|----------|
| Root | `0x7Ed48C31f2fdC40d37407cBaBf0870B2b688368f` | All |
| Hub | `0x9c8454A506263549f07c80698E276e3622077098` | All |
| Spoke | `0xd30Da1d7F964E5f6C2D9fE2AAA97517F6B23FA2B` | All |
| Vault Router | `0xdbCcee499563D4AC2D3788DeD3acb14FB92B175D` | All |

**Example Tokens:**

See the [Centrifuge deployments page](https://docs.centrifuge.io/developer/protocol/deployments/) for a complete list of deployed pools and their token addresses across all chains.

### Additional Resources

- **Documentation:** https://docs.centrifuge.io/
- **SDK Documentation:** https://docs.centrifuge.io/developer/centrifuge-sdk/overview/
- **GraphQL API:** https://api.centrifuge.io/
- **App:** https://app.centrifuge.io/
- **GitHub:** https://github.com/centrifuge

---

## Securitize

### Protocol Overview

[Securitize](https://securitize.io/) is a leading digital asset securities platform enabling compliant issuance, management, and trading of tokenized securities. Securitize provides end-to-end infrastructure for security token offerings (STOs) and tokenized funds, with built-in compliance, investor management, and secondary market support.

**Key Features:**
- Compliant security token issuance (Reg D, Reg S, Reg A+, Reg CF)
- Integrated investor onboarding and KYC/AML
- Transfer restrictions and compliance automation
- Secondary trading infrastructure
- Corporate actions (dividends, voting)

**Notable Assets:**
- BlackRock USD Institutional Digital Liquidity Fund (BUIDL) - [securitize.io/blackrock/buidl](https://securitize.io/blackrock/buidl)

**Documentation**: [docs.securitize.io](https://docs.securitize.io/)  
**GitHub**: [github.com/securitize-io](https://github.com/securitize-io)

### Token Structure

Securitize uses the **DS Protocol** (Digital Securities Protocol) with **DSToken** as the core token implementation.

**Contract Type**: Modified ERC-20 with DS Protocol extensions

**Key Features:**
- **Role-Based Access Control**: Master, Issuer, Transfer Agent, Exchange roles
- **Compliance Integration**: External compliance service validates all transfers
- **Token Locking**: Time-based and conditional locks on issuance
- **Multi-Chain Support**: Native deployments across EVM chains and Solana
- **Investor Tracking**: Balance aggregation by investor ID across wallets
- **Omnibus Wallet Support**: Asset tracking for custodial/omnibus structures
- **Token Cap**: Maximum supply enforcement
- **Corporate Actions**: Minting, burning, seizing, pausing

**DS Services Architecture:**
```
DSToken relies on external service contracts:
- Compliance Service: Transfer validation and restrictions
- Registry Service: Investor/wallet registration and KYC status
- Lock Manager: Token timelock management
- Omnibus TBE Controller: Trade-by-Exception omnibus tracking
```

```solidity
// Simplified ABI for key functions
[
  "function balanceOf(address) view returns (uint256)",
  "function totalSupply() view returns (uint256)",
  "function totalIssued() view returns (uint256)",
  "function decimals() view returns (uint8)",
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function cap() view returns (uint256)",
  "function isPaused() view returns (bool)",
  "function balanceOfInvestor(string memory investorId) view returns (uint256)",
  "function walletCount() view returns (uint256)",
  "function getWalletAt(uint256 index) view returns (address)",
  "function preTransferCheck(address from, address to, uint256 value) view returns (uint256 code, string memory reason)"
]
```

**Important**: DSToken contracts do not have built-in price or NAV functions. Token prices are available on-chain via **RedStone oracles** (Securitize's official oracle provider), which use the Chainlink-compatible interface and reflect prices from Securitize's API.

### Retrieving Key Data

#### Share Price / NAV (via RedStone Oracle)

DSToken contracts do not have built-in price functions, but **RedStone provides on-chain price oracles** for Securitize tokens using the Chainlink-compatible interface.

**RedStone Oracle**: [redstone.finance](https://www.redstone.finance/)

**Example - STAC_FUNDAMENTAL Oracle** (Ethereum):
- Oracle Address: `0xEdC6287D3D41b322AF600317628D7E226DD3add4`
- Interface: Chainlink-compatible
- Decimals: 8

```typescript
import { ethers } from 'ethers';

const provider = new ethers.JsonRpcProvider(RPC_URL);

// RedStone oracle ABI (Chainlink-compatible)
const oracleAbi = [
  "function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)",
  "function latestAnswer() view returns (int256)",
  "function decimals() pure returns (uint8)",
  "function description() view returns (string)"
];

// Example: STAC_FUNDAMENTAL oracle
const oracleAddress = "0xEdC6287D3D41b322AF600317628D7E226DD3add4";
const oracle = new ethers.Contract(oracleAddress, oracleAbi, provider);

// Get latest price
const [roundId, answer, startedAt, updatedAt, answeredInRound] = await oracle.latestRoundData();
const decimals = await oracle.decimals(); // Returns 8
const description = await oracle.description();

// Convert to human-readable price
const price = Number(answer) / (10 ** decimals);

console.log(`Oracle: ${description}`);
console.log(`Price: $${price.toFixed(8)}`);
console.log(`Last Updated: ${new Date(Number(updatedAt) * 1000).toISOString()}`);
```

**Finding Oracle Addresses:**

Oracle addresses for Securitize tokens can be found in the RedStone relayer configuration, e.g.:
- **GitHub Reference**: [RedStone Ethereum Multi-Feed Config](https://github.com/redstone-finance/redstone-oracles-monorepo/blob/9fa4242a9ba29a46e33fd477ced55b7463ceea4a/packages/relayer-remote-config/main/relayer-manifests-multi-feed/ethereumMultiFeed.json)

Example tokens with RedStone oracles:
- `STAC_FUNDAMENTAL`: 0xEdC6287D3D41b322AF600317628D7E226DD3add4
- `iBENJI_ETHEREUM_FUNDAMENTAL`: 0x009119Cd7eB8912863c30362CfdCe0B2F8a52D6C
- `SIERRA_FUNDAMENTAL`: 0x9269127F104C040AB526575573c23F3e67401aD9


#### Position Value

```typescript
// Get user's token balance
const balance = await token.balanceOf(userAddress);
const decimals = await token.decimals();
const balanceFormatted = Number(balance) / (10 ** decimals);

// Get NAV per token from RedStone oracle
const oracleAddress = "0x..."; // RedStone oracle for this token
const oracle = new ethers.Contract(oracleAddress, oracleAbi, provider);
const answer = await oracle.latestAnswer();
const oracleDecimals = await oracle.decimals();
const navPerToken = Number(answer) / (10 ** oracleDecimals);

const usdValue = balanceFormatted * navPerToken;
```

**Multi-Wallet Positions:**

DSToken tracks investor balances across multiple wallets:

```typescript
// If you have an investor ID from Securitize
const investorBalance = await token.balanceOfInvestor(investorId);
// This aggregates balances across all wallets associated with this investor
```

#### TVL Calculation

**Challenge**: Securitize tokens are natively deployed on **multiple chains including Solana** (non-EVM).

**Recommended Approach**: 
- For EVM chains: Query `totalSupply()` on each chain and use RedStone oracles for pricing
- For Solana and multi-chain aggregation: Contact Securitize for comprehensive TVL data

**Single-Chain TVL** (if needed):

```typescript
const totalSupply = await token.totalSupply();
const decimals = await token.decimals();
const totalTokens = Number(totalSupply) / (10 ** decimals);

// NAV per token from RedStone oracle
const oracle = new ethers.Contract(oracleAddress, oracleAbi, provider);
const answer = await oracle.latestAnswer();
const oracleDecimals = await oracle.decimals();
const navPerToken = Number(answer) / (10 ** oracleDecimals);

const chainTvl = totalTokens * navPerToken;
```

**Note**: `totalSupply()` returns the circulating supply on this specific chain. For protocol-wide TVL across all chains (including Solana), aggregate supply from each chain manually or contact Securitize.

#### APY Calculation

Securitize tokens handle yield in **two different ways** depending on the asset:

**Type 1: Price Appreciation Tokens**

Some tokens increase in price/NAV over time (similar to ERC-4626 vaults). APY is calculated from NAV changes via RedStone oracle:

```typescript
// Query RedStone oracle at two different blocks
const currentBlock = await provider.getBlockNumber();
const pastBlock = currentBlock - 216000; // ~30 days ago (12s blocks)

const oracle = new ethers.Contract(oracleAddress, oracleAbi, provider);
const oracleDecimals = await oracle.decimals();

// Get NAV at both points in time
const answer1 = await oracle.latestAnswer({ blockTag: pastBlock });
const answer2 = await oracle.latestAnswer({ blockTag: currentBlock });

const nav1 = Number(answer1) / (10 ** oracleDecimals);
const nav2 = Number(answer2) / (10 ** oracleDecimals);

// Get timestamps
const timestamp1 = (await provider.getBlock(pastBlock)).timestamp;
const timestamp2 = (await provider.getBlock(currentBlock)).timestamp;

// Calculate APY
const timeDelta = timestamp2 - timestamp1;
const periodsPerYear = (365 * 24 * 60 * 60) / timeDelta;
const returnRate = nav2 / nav1;
const apy = Math.pow(returnRate, periodsPerYear) - 1;

console.log(`APY: ${(apy * 100).toFixed(2)}%`);
```

**Type 2: Yield-Bearing Stablecoins**

Other tokens (like BUIDL) maintain a stable $1.00 NAV and distribute yield as **additional tokens**:

- Snapshots are taken periodically
- Yield tokens are minted and distributed by Securitize to holders
- APY may **vary by chain** and token variant
- Distribution is manual, not automatic

**Tracking Yield Distributions:**

```typescript
// Monitor Transfer events from zero address (minting)
const filter = token.filters.Transfer(ethers.ZeroAddress, userAddress);
const events = await token.queryFilter(filter, fromBlock, toBlock);

// Calculate received yield
let totalYield = 0;
for (const event of events) {
  totalYield += Number(event.args.value);
}

const yieldFormatted = totalYield / (10 ** decimals);
```

**For accurate APY**: Consult Securitize's published APY rates in their private API, which may differ by:
- Chain (Ethereum, Arbitrum, Avalanche, Polygon, Solana, etc.)
- Token variant (BUIDL_I, BUIDL)

#### Compliance Checks

DSToken includes compliance validation:

```typescript
// Check if a transfer would be allowed
const [code, reason] = await token.preTransferCheck(from, to, amount);

if (code === 0) {
  console.log("Transfer allowed");
} else {
  console.log(`Transfer blocked: ${reason} (code: ${code})`);
}

// Check if token is paused
const paused = await token.isPaused();
```

### Asset Backing Information

For detailed information about the **real-world assets** backing Securitize tokens (e.g., treasury bills, money market funds, real estate, private equity), refer to the [Securitize platform](https://securitize.io/) and token-specific offering documents.

**Example - BUIDL**: BlackRock USD Institutional Digital Liquidity Fund invests primarily in cash, U.S. Treasury bills, and repurchase agreements. Details available at [securitize.io/blackrock/buidl](https://securitize.io/blackrock/buidl).

Asset information is typically provided through:
- Securitize investor portal (requires KYC/accreditation)
- Issuer disclosure documents and prospectuses
- Regulatory filings (Form C, Form 1-A, Form D, etc.)
- Periodic fund reports and financial statements

The on-chain data focuses on token ownership, transfer restrictions, and distribution events rather than underlying asset composition. Access to detailed asset information requires investor accreditation and verification through Securitize's platform.

---

## Galaxy/Arch

### Protocol Overview

This section covers tokenized assets from multiple institutional-grade RWA platforms:

**[Galaxy](https://www.galaxy.com/)**: Digital asset financial services firm offering institutional-grade products including tokenized funds and treasury management solutions.

**[Arch](https://archlending.com/)**: Crypto-backed lending platform offering Bitcoin, Ethereum, and Solana-backed loans with custody through Anchorage Digital. Arch does not issue RWA tokens but partners with Galaxy and other issuers for tokenized asset distribution.


### Token Structure

A typical token of interest is **GalaxyToken1** (Galaxy Arch CLO Token - GACLO-1), which represents tokenized collateralized loan obligations (CLOs).

**Contract Type**: Modified ERC-20 with ERC-1404 transfer restrictions

**Key Features**:
- **Access Control**: Role-based permissions (Admin, Minter, Burner, Revoker, Pauser, Whitelister, Timelocker)
- **Whitelisting**: Transfers restricted to approved addresses only
- **Token Timelocking**: Ability to lock tokens until a future release time
- **Pausability**: Emergency pause for all transfers
- **Minting/Burning**: Controlled token supply management
- **Revocation**: Admin ability to reclaim tokens from holders
- **Transfer Restrictions**: ERC-1404 standard with external validation contract
- **Decimals**: 6 (similar to USDC/USDT)

```solidity
// Simplified ABI for key functions
[
  "function balanceOf(address) view returns (uint256)",
  "function totalSupply() view returns (uint256)",
  "function decimals() pure returns (uint8)",  // Returns 6
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function paused() view returns (bool)",
  "function getWhitelistStatus(address) view returns (bool)",
  "function getLockUpInfo(address) view returns (uint256 releaseTime, uint256 amount)",
  "function checkTimelock(address, uint256) view returns (bool)",
  "function detectTransferRestriction(address from, address to, uint256 amount) view returns (uint8)",
  "function messageForTransferRestriction(uint8 restrictionCode) view returns (string)"
]
```

**Important**: GalaxyToken1 is a **compliance-heavy security token**, not a yield-bearing vault. It does not automatically accrue value or provide on-chain APY calculations. Value changes are reflected through periodic NAV updates provided by the issuer.

### Retrieving Key Data

**Note**: GalaxyToken1 does not appear to have a publicly known on-chain price oracle. An oracle contract **may** exist but has not been identified.

#### Share Price / NAV

Unlike ERC-4626 vaults, GalaxyToken1 does not have built-in `convertToAssets()` or share price functions. NAV (Net Asset Value) must be obtained directly from Galaxy (they may have a non-public API).

#### Position Value

```typescript
// Get user's token balance
const balance = await token.balanceOf(userAddress);
const decimals = await token.decimals(); // Returns 6
const balanceFormatted = Number(balance) / (10 ** decimals);

// Calculate USD value using off-chain NAV
const navPerToken = 1.00; // Must be obtained from Galaxy
const usdValue = balanceFormatted * navPerToken;
```


#### TVL Calculation

```typescript
// Total supply in circulation
const totalSupply = await token.totalSupply();
const decimals = await token.decimals();
const totalTokens = Number(totalSupply) / (10 ** decimals);

// TVL in USD requires off-chain NAV per token
const navPerToken = 1.00; // Must be obtained from Galaxy
const tvlUsd = totalTokens * navPerToken;

console.log(`Total Supply: ${totalTokens} tokens`);
console.log(`TVL: $${tvlUsd.toFixed(2)} (assuming NAV = $${navPerToken})`);
```

#### Compliance Checks

```typescript
const complianceAbi = [
  "function paused() view returns (bool)",
  "function getWhitelistStatus(address) view returns (bool)",
  "function getLockUpInfo(address) view returns (uint256, uint256)",
  "function checkTimelock(address, uint256) view returns (bool)"
];

const token = new ethers.Contract(tokenAddress, complianceAbi, provider);

// Check if transfers are paused
const isPaused = await token.paused();

// Check if address is whitelisted
const isWhitelisted = await token.getWhitelistStatus(userAddress);

// Check lockup details
const [releaseTime, lockedAmount] = await token.getLockUpInfo(userAddress);
const isLocked = releaseTime > Math.floor(Date.now() / 1000);

console.log(`Paused: ${isPaused}`);
console.log(`Whitelisted: ${isWhitelisted}`);
console.log(`Locked Amount: ${Number(lockedAmount) / 1e6}`);
console.log(`Release Time: ${new Date(Number(releaseTime) * 1000).toISOString()}`);
console.log(`Currently Locked: ${isLocked}`);
```

### Asset Backing Information

For detailed information about the **real-world assets** backing tokens from Galaxy and Arch (e.g., treasury bills, corporate bonds, fund holdings, derivatives), refer to the respective platform documentation and product-specific disclosure materials:

**Galaxy**:
- Platform: [galaxy.com](https://www.galaxy.com/)
- Asset composition and custody arrangements
- Fund structure and investment strategy
- Performance reports and NAV calculations

**Arch**:
- Platform: [archlending.com](https://archlending.com/)
- Note: Arch is a crypto lending platform; RWA tokens like GalaxyToken1 are issued through partnerships with Galaxy


The on-chain data focuses on token mechanics and ownership rather than detailed asset composition. For asset backing details and NAV information, contact Galaxy directly.

---

## References

### Standards & Specifications

- **ERC-20:** https://eips.ethereum.org/EIPS/eip-20
- **ERC-1404:** https://github.com/simple-restricted-token/simple-restricted-token (Transfer Restrictions)
- **ERC-4626:** https://eips.ethereum.org/EIPS/eip-4626
- **ERC-7540:** https://eips.ethereum.org/EIPS/eip-7540
- **ERC-7575:** https://eips.ethereum.org/EIPS/eip-7575

### Maple

- **Main Documentation:** https://docs.maple.finance/
- **Integration Guide:** https://docs.maple.finance/integrate/ethereum-mainnet/backend-integrations
- **App:** https://app.maple.finance/
- **GitHub:** https://github.com/maple-labs
- **Discord:** https://discord.gg/maple

### Arkis

- **Documentation:** https://docs.arkis.xyz/home
- **Website:** https://www.arkis.xyz/
- **GitHub:** https://github.com/ArkisXYZ (whitepaper and audits)

### Anchorage

- **Website:** https://www.anchorage.com/
- **Note:** No public developer documentation or GitHub for RWA tokenization available

### Centrifuge

- **Main Documentation:** https://docs.centrifuge.io/
- **User Guide:** https://docs.centrifuge.io/user/overview/
- **Developer Docs:** https://docs.centrifuge.io/developer/protocol/overview/
- **SDK:** https://docs.centrifuge.io/developer/centrifuge-sdk/overview/
- **GraphQL API:** https://api.centrifuge.io/
- **Deployments:** https://docs.centrifuge.io/developer/protocol/deployments/
- **App:** https://app.centrifuge.io/
- **GitHub:** https://github.com/centrifuge
- **Discord:** https://discord.gg/centrifuge

### Securitize

- **Documentation:** https://docs.securitize.io/
- **Website:** https://securitize.io/
- **Platform:** https://id.securitize.io/
- **GitHub:** https://github.com/securitize-io
  - **DSToken:** https://github.com/securitize-io/dstoken (DS Protocol implementation)
  - **DSTokenInterfaces:** https://github.com/securitize-io/DSTokenInterfaces (Protocol interfaces)
- **Notable Products:**
  - **BlackRock BUIDL:** https://securitize.io/blackrock/buidl

### RedStone (Securitize Oracle Provider)

- **Website:** https://www.redstone.finance/
- **Oracle Addresses:** https://github.com/redstone-finance/redstone-oracles-monorepo/blob/main/packages/relayer-remote-config/main/relayer-manifests-multi-feed/ethereumMultiFeed.json
- **Note:** Official oracle provider for Securitize tokens; Chainlink-compatible interface

### Galaxy

- **Website:** https://www.galaxy.com/

### Arch

- **Website:** https://archlending.com/
- **Note:** Crypto lending platform; does not issue RWA tokens directly

### Industry Resources

- **RWA.xyz:** https://www.rwa.xyz/ - RWA market data and analytics
- **DeFi Llama (RWA):** https://defillama.com/protocols/RWA - TVL tracking for RWA protocols
- **Messari RWA Reports:** https://messari.io/report-category/real-world-assets

---

**Document Version:** 1.0  
**Last Updated:** January 2026

**Contributors:** Technical specification based on Centrifuge protocol documentation, ERC-4626 standard, and RWA industry best practices.

