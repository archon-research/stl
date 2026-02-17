# Maple Spark Syrup Collateral Breakdown Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan.

**Goal:** Produce a reproducible, block-specific breakdown of the collateral assets backing Spark's Syrup USDC/USDT allocations on Maple, matching the Maple dashboard methodology as closely as possible.

**Architecture:** Use Maple subgraph GraphQL as the source of truth at a target `block.number`. Compute (1) Spark allocations per Syrup pool via `poolV2Positions`, (2) per-pool collateral mix via the best available source at that block (prefer `NativeLoan`/`OpenTermLoan` USD fields; fall back to `poolMeta.poolCollaterals`), then (3) attribute collateral pro-rata to Spark using `SparkBalanceUsd / PoolTvlUsd`.

**Tech Stack:** GraphQL (Maple subgraph SDL), Go (optional implementation in `stl-verify`), JSON output.

## Context / Current Evidence

- At `block.number = 24446926`, `poolV2Totals(where:{pool:$poolId})` and `poolV2 { collateralByAsset }` are empty for Syrup USDC.
- `openTermLoans(where:{fundingPool:$poolId})` returns many results, but `collateral.assetValueUsd` is mostly null.
- `poolV2 { poolMeta { poolCollaterals } }` returns non-zero for: BTC, LBTC, USTB, weETH, XRP, HYPE (but this does not fully match the Maple dashboard list).

This suggests the dashboard's collateral breakdown may be sourced from `NativeLoan` collateral USD values and/or additional offchain-native entities, not from `PoolV2Total` or `OpenTermLoan.collateral.assetValueUsd`.

## Plan

### Task 1: Establish the pool IDs and Spark positions (historical)

**Files:**
- None (query-only)

**Step 1: Run Spark allocations query**

Run this query at the chosen block:

```graphql
query SparkSyrupPositions($block: Block_height!, $sparkAccount: String!) {
  poolV2Positions(
    block: $block
    first: 1000
    where: { account: $sparkAccount }
    orderBy: lendingBalanceUsd
    orderDirection: desc
  ) {
    id
    account { id }
    pool { id name symbol tvlUsd }
    lendingBalanceUsd
  }
}
```

**Step 2: Confirm the two Syrup pools and capture**
- `pool.id` for Syrup USDC and Syrup USDT
- `lendingBalanceUsd` for Spark for each
- `pool.tvlUsd` for each

Expected: Two large positions (USDC and USDT) and possibly smaller ones.

### Task 2: Determine the dashboard's collateral source (root cause investigation)

**Files:**
- None (query-only)

**Step 1: Query NativeLoan for collateral breakdown by pool**

Run:

```graphql
query NativeLoansForPool($poolMetaId: String!) {
  nativeLoans {
    id
    fundingPool { _id poolName contractAddress }
    principalOwed
    collateralAsset
    collateralAssetValueUsd
    collateralAssetAmount
    currentCollateralAssetAmount
    collateralDecimals
    collateralBlockchain
    term
  }
}
```

Notes:
- `nativeLoans` does not accept `block` in the SDL; treat it as “latest”.
- Filter client-side by `fundingPool._id` matching the Syrup pool metadata id.

**Step 2: Map PoolV2 -> PoolMetadata ids**

If needed, query `poolsMeta` and locate entries where `contractAddress` matches Syrup pool addresses.

```graphql
query PoolsMeta {
  poolsMeta {
    _id
    poolName
    contractAddress
    poolCollaterals { asset assetValueUsd assetAmount assetDecimals }
  }
}
```

**Step 3: Compare three candidate sources**
- A: Sum `nativeLoans` by `collateralAsset` using `collateralAssetValueUsd` (prefer).
- B: Sum `openTermLoans` by `collateral.asset` using `collateral.assetValueUsd` (likely sparse at historical blocks).
- C: Use `poolMeta.poolCollaterals` (static-ish) as a fallback.

Success criteria:
- Identify which source yields the dashboard's set of assets (e.g. includes PYUSD or others the dashboard shows).

### Task 3: Decide and document the attribution formula

**Files:**
- Modify: `docs/plans/2026-02-13-maple-spark-syrup-collateral-breakdown.md`

**Step 1: Write down exact formula**
- Pool weight = `spark_lending_balance_usd / pool_tvl_usd`
- Spark collateral per asset = `pool_collateral_usd_by_asset * pool weight`

**Step 2: Define rounding and “Others”**
- Round to 2 decimals USD.
- Group assets contributing < 1% into `Others` (unless user wants raw list).

### Task 4: (Optional) Implement a Go helper to compute the breakdown

**Files:**
- Create: `stl-verify/internal/adapters/outbound/maple/collateral_breakdown.go`
- Create: `stl-verify/internal/adapters/outbound/maple/collateral_breakdown_test.go`
- Modify (if needed): existing Maple GraphQL client files under `stl-verify/internal/adapters/outbound/maple/graphql/`

**Step 1: TDD - write failing unit test**

Test case uses fixture JSON representing:
- Spark positions in two pools with known tvlUsd and lendingBalanceUsd
- Per-pool collateral totals by asset

Expected output: summed per-asset Spark-attributed USD.

**Step 2: Implement minimal aggregator**
- Parse numeric strings safely (decimal) and handle null/empty as 0.
- Compute weights and per-asset attribution.

**Step 3: Add integration hook (optional)**
- A small CLI under `stl-verify/cmd/` to run the queries and print the breakdown.

### Task 5: Verification

**Files:**
- None (commands)

Run:
- `make test` (if Go code is added)

Success criteria:
- Unit tests green.
- Output breakdown matches dashboard within acceptable tolerance (or documented differences).

## Execution Handoff

Plan complete and saved to `docs/plans/2026-02-13-maple-spark-syrup-collateral-breakdown.md`.

Two execution options:
1. Subagent-Driven (this session) - dispatch per task and iterate quickly.
2. Parallel Session (separate) - implement via `superpowers:executing-plans` with checkpoints.
