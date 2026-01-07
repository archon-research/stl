# CLI Job Scripts

Manual job runners for historical data processing and snapshot calculations.

## Overview

While the Ponder indexer tracks events in **real-time**, these CLI jobs compute **historical snapshots** of user positions and asset prices. This separation allows:

- **Real-time indexing** to focus on capturing events as they happen
- **Snapshot calculations** to run independently for historical analysis
- **On-demand execution** for specific block ranges

## Available Jobs

### 1. Price Capture Job

Captures asset prices at specified block intervals for historical USD valuations.

**What it does:**
- Fetches oracle prices for all reserves at specific blocks
- Stores snapshots in `AssetPriceSnapshot` tables
- Used by snapshot calculations for position valuations

**Usage:**

```bash
# Mainnet - capture prices every 7200 blocks (~1 day)
pnpm job price-capture mainnet 16776401 18000000 7200

# Gnosis - capture prices every 17280 blocks (~1 day)  
pnpm job price-capture gnosis 29817457 30000000 17280
```

**Output:**
```
🚀 Starting price-capture job for mainnet
   Blocks: 16776401 → 18000000
   Interval: 7200

📊 Processing block 16776401
   ✓ Captured 15 asset prices
📊 Processing block 16783601
   ✓ Captured 15 asset prices
...
✅ Job completed successfully!
```

### 2. Snapshot Calculation Job

Calculates comprehensive user position snapshots including health factors and detailed breakdowns.

**What it calculates:**
- **Health Factor**: `Σ(Collateral Value × Liquidation Threshold) / Total Debt`
- **Position Breakdown**: Each collateral and debt position with USD values
- **Active Users**: Tracks users with open positions

**Usage:**

```bash
# Mainnet - daily snapshots (7200 blocks ≈ 1 day)
pnpm job snapshot-calculation mainnet 16776401 18000000 7200

# Gnosis - daily snapshots (17280 blocks ≈ 1 day)
pnpm job snapshot-calculation gnosis 29817457 30000000 17280
```

**Output:**
```
🚀 Starting snapshot-calculation job for mainnet
   Blocks: 16776401 → 18000000
   Interval: 7200

📸 Snapshotting block 16776401 (timestamp: 1234567890)
   ✓ Captured 245 users
   ✓ Health factors calculated
📸 Snapshotting block 16783601
   ✓ Captured 248 users
...
✅ Job completed successfully!
```

## Command Structure

```bash
pnpm job <job-type> <chain> <startBlock> <endBlock> [interval]
```

**Parameters:**

| Parameter | Description | Required |
|-----------|-------------|----------|
| `job-type` | Job to run: `price-capture` or `snapshot-calculation` | Yes |
| `chain` | Blockchain: `mainnet` or `gnosis` | Yes |
| `startBlock` | Starting block number | Yes |
| `endBlock` | Ending block number | Yes |
| `interval` | Blocks between snapshots (default: 7200 for mainnet) | No |

## Block Intervals by Chain

Approximate blocks for common time periods:

### Ethereum Mainnet (~12 sec/block)

| Period | Blocks |
|--------|--------|
| 1 hour | 300 |
| 6 hours | 1,800 |
| 1 day | 7,200 |
| 1 week | 50,400 |

### Gnosis Chain (~5 sec/block)

| Period | Blocks |
|--------|--------|
| 1 hour | 720 |
| 6 hours | 4,320 |
| 1 day | 17,280 |
| 1 week | 120,960 |

## Environment Variables

Ensure these are set in your `.env` or environment:

```env
# Required RPC URLs
PONDER_RPC_URL_1=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
PONDER_RPC_URL_100=https://rpc.gnosischain.com

# Required database connection
DATABASE_URL=postgresql://user:pass@localhost:5432/ponder
```

**Note**: Jobs connect directly to the database and RPC, bypassing Ponder's internal indexing.

## Common Use Cases

### Historical Backfill

Fill in snapshots for historical data:

```bash
# Backfill entire Sparklend history on Mainnet
pnpm job snapshot-calculation mainnet 16776401 18500000 7200
```

### Recent Period Analysis

Capture snapshots for a recent time period:

```bash
# Last 30 days of snapshots (approximately)
pnpm job snapshot-calculation mainnet 18250000 18466000 7200
```

### High-Frequency Snapshots

Capture more frequent snapshots for detailed analysis:

```bash
# Hourly snapshots for 1 week
pnpm job snapshot-calculation mainnet 18450000 18500400 300
```

## Troubleshooting

### "Missing RPC URL for chain"

**Error**: `Missing RPC URL for mainnet. Set PONDER_RPC_URL_1`

**Solution**: Ensure environment variables are set:
```bash
export PONDER_RPC_URL_1="https://..."
export PONDER_RPC_URL_100="https://..."
```

### "DATABASE_URL environment variable is required"

**Solution**: Set database connection:
```bash
export DATABASE_URL="postgresql://user:pass@host:5432/db"
```

### Job Hangs or Times Out

**Possible causes**:
- RPC rate limiting → Use premium RPC provider
- Database connection issues → Check `DATABASE_URL`
- Network problems → Verify connectivity

**Solution**: Reduce interval or block range, try again

### "Could not find a declaration file for module 'pg'"

**Solution**: Install TypeScript types:
```bash
pnpm install
```

## Related Files

- **Job Runner**: `src/scripts/run-job.ts` - Main job execution script
- **Price Capture**: `src/protocols/sparklend/jobs/price-capture.ts`
- **Snapshot Calc**: `src/protocols/sparklend/jobs/snapshot-calculation.ts`
- **Health Factor Utils**: `src/protocols/sparklend/utils/health-factor-snapshot.ts`
