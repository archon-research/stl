# MakerDAO Debt Calculation Reference

**Version:** 1.0  
**Last Updated:** March 2026  
**Purpose:** Reference for computing total and per-vault debt for Star allocator ilks (Spark, Grove) from the MakerDAO Vat contract

---

## Table of Contents

1. [Key Addresses & Constants](#key-addresses--constants)
2. [Core Concepts](#core-concepts)
3. [Total Ilk Debt](#total-ilk-debt)
4. [Individual Vault Debt](#individual-vault-debt)
5. [Contract Interface](#contract-interface)

---

## Key Addresses & Constants

### MCD Vat

| Name | Address |
|---|---|
| **MCD Vat** (Ethereum) | `0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B` |

### Star Ilk Identifiers

| Star | Human-readable name | `bytes32` ilk |
|---|---|---|
| **Spark** | `ALLOCATOR-SPARK-A` | `0x414c4c4f4341544f522d535041524b2d41000000000000000000000000000000` |
| **Grove** | `ALLOCATOR-BLOOM-A` | `0x414c4c4f4341544f522d424c4f4f4d2d41000000000000000000000000000000` |

> **Note:** Grove's on-chain ilk name is `ALLOCATOR-BLOOM-A`. The `bytes32` value is simply the UTF-8 encoded name right-padded with zeros to 32 bytes.

### Allocator Urn Addresses

Each Star's allocator vault contract acts as the urn for its ilk. Pass these as the `usr` argument to `vat.urns(ilk, usr)` for per-vault debt queries.

| Star | Allocator Urn Address |
|---|---|
| **Spark** | `0x691a6c29e9e96dd897718305427Ad5D534db16BA` |
| **Grove** | `0x26512A41C8406800f21094a7a7A0f980f6e25d43` |

### Precision Constants

```typescript
const WAD = 10n ** 18n;  // 1e18 — used for token amounts and normalized debt (Art / art)
const RAY = 10n ** 27n;  // 1e27 — used for the accumulated rate
```

---

## Core Concepts

The Vat is MakerDAO's central accounting contract. It tracks all collateral and debt across every collateral type (ilk).

### Ilk Struct (`vat.ilks(ilk)`)

| Field | Type | Description |
|---|---|---|
| `Art` | uint256 (wad) | Total normalized debt across all vaults for this ilk |
| `rate` | uint256 (ray) | Accumulated stability fee multiplier; starts at RAY (1.0), grows over time |
| `spot` | uint256 (ray) | Collateral price with safety margin (used for liquidation checks) |
| `line` | uint256 (rad) | Debt ceiling for this ilk |
| `dust` | uint256 (rad) | Minimum vault debt (dust floor) |

### Urn Struct (`vat.urns(ilk, usr)`)

| Field | Type | Description |
|---|---|---|
| `ink` | uint256 (wad) | Collateral locked in this specific vault |
| `art` | uint256 (wad) | Normalized debt for this specific vault |

### Debt Formula

Normalized debt (`art` or `Art`) must be multiplied by `rate` to get actual DAI debt including accrued stability fees:

```
actual_debt = art * rate / RAY
```

Because `rate` is in RAY precision (27 decimals) and `art` is in WAD precision (18 decimals), dividing by RAY returns a WAD-precision DAI amount (18 decimals).

---

## Total Ilk Debt

To get the total DAI debt outstanding across **all vaults** for an ilk, use `Art * rate`:

```typescript
const VAT = "0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B";

const VAT_ABI = parseAbi([
  "function ilks(bytes32) view returns (uint256 Art, uint256 rate, uint256 spot, uint256 line, uint256 dust)",
]);

const ILKS = {
  spark: "0x414c4c4f4341544f522d535041524b2d41000000000000000000000000000000",
  grove:  "0x414c4c4f4341544f522d424c4f4f4d2d41000000000000000000000000000000",
} as const;

const RAY = 10n ** 27n;

async function getTotalIlkDebt(ilk: `0x${string}`): Promise<bigint> {
  const { Art, rate } = await client.readContract({
    address: VAT,
    abi: VAT_ABI,
    functionName: "ilks",
    args: [ilk],
  });

  // Art (wad) * rate (ray) / RAY → debt in DAI (wad, 18 decimals)
  return (Art * rate) / RAY;
}

const sparkDebt = await getTotalIlkDebt(ILKS.spark);
const groveDebt  = await getTotalIlkDebt(ILKS.grove);

console.log(`Spark total debt: ${formatUnits(sparkDebt, 18)} DAI`);
console.log(`Grove total debt:  ${formatUnits(groveDebt,  18)} DAI`);
```

---

## Individual Vault Debt

To get the debt for a **specific allocator vault** (urn), use `art * rate`:

```typescript
const VAT_ABI_FULL = parseAbi([
  "function ilks(bytes32) view returns (uint256 Art, uint256 rate, uint256 spot, uint256 line, uint256 dust)",
  "function urns(bytes32, address) view returns (uint256 ink, uint256 art)",
]);

async function getVaultDebt(ilk: `0x${string}`, urnAddress: Address): Promise<bigint> {
  const [{ rate }, { art }] = await client.multicall({
    contracts: [
      { address: VAT, abi: VAT_ABI_FULL, functionName: "ilks", args: [ilk] },
      { address: VAT, abi: VAT_ABI_FULL, functionName: "urns", args: [ilk, urnAddress] },
    ],
    allowFailure: false,
  });

  // art (wad) * rate (ray) / RAY → vault debt in DAI (wad, 18 decimals)
  return (art * rate) / RAY;
}

const sparkVaultDebt = await getVaultDebt(ILKS.spark, "0x691a6c29e9e96dd897718305427Ad5D534db16BA");
const groveVaultDebt  = await getVaultDebt(ILKS.grove,  "0x26512A41C8406800f21094a7a7A0f980f6e25d43");
```

For allocator ilks there is typically a single urn per ilk (the allocator vault contract itself), so `getVaultDebt` should return the same value as `getTotalIlkDebt`. Comparing the two is a useful sanity check.

---

## Contract Interface

### Vat ABI (relevant functions)

```typescript
const VAT_ABI = parseAbi([
  // Total normalized debt (Art) and rate for an ilk
  "function ilks(bytes32 ilk) view returns (uint256 Art, uint256 rate, uint256 spot, uint256 line, uint256 dust)",

  // Per-vault collateral (ink) and normalized debt (art)
  "function urns(bytes32 ilk, address usr) view returns (uint256 ink, uint256 art)",
]);
```

### Precision Summary

| Value | Unit | Decimals | Notes |
|---|---|---|---|
| `Art` / `art` | wad | 18 | Normalized debt — multiply by `rate` to get actual DAI |
| `rate` | ray | 27 | Accumulated stability fee multiplier; divide result by RAY |
| `debt` (derived) | wad | 18 | `art * rate / RAY` — actual DAI owed including fees |
| `line` / `dust` | rad | 45 | wad × ray; divide by RAY to compare with wad amounts |

