# block_time backfill (VEC-491)

`block_time (chain_id, block_number, block_timestamp)` is the durable on-chain block-time
dimension. The table is created by `20260722_140000_create_block_time_dimension.sql`; population and
coverage measurement run **out of band on staging** (a 19M+ row `INSERT...SELECT` does not belong in
the migrator transaction). Run these as a non-superuser role against staging, not the pooler
(no 2-minute statement timeout).

## Step 1 -- populate from block_states (correct, complete for the retention window)

`block_states.created_at` is the true on-chain block timestamp (verified: 0s delta vs the
natively-timestamped indexers; `received_at` is the +1-2s node receipt time). Every canonical block
in the window is present. Idempotent.

```sql
INSERT INTO block_time (chain_id, block_number, block_timestamp)
SELECT chain_id, number, created_at
FROM block_states
WHERE NOT is_orphaned
ON CONFLICT (chain_id, block_number) DO NOTHING;
```

## Step 2 -- measure the gap (the deciding step)

How many blocks the bucket-2 tables reference that `block_time` cannot yet resolve. Only
`protocol_event` and `allocation_position` carry `chain_id`; `borrower`, `borrower_collateral` and
`sparklend_reserve_data` do **not** and mix chains (ETH ~16.7-25.6M and Avalanche ~88-91M in one
table), so their chain must first be resolved by the transform's chain-fill step (VEC-500, 6c)
before their coverage can be measured or joined.

```sql
-- chained tables: distinct (chain, block) not resolvable via block_time
SELECT 'protocol_event' AS tbl, count(*) AS missing
FROM (SELECT DISTINCT chain_id, block_number FROM protocol_event
      EXCEPT SELECT chain_id, block_number FROM block_time) x
UNION ALL
SELECT 'allocation_position', count(*)
FROM (SELECT DISTINCT chain_id, block_number FROM allocation_position
      EXCEPT SELECT chain_id, block_number FROM block_time) x;
```

Expected from prod range analysis: `block_states` alone leaves the pre-window history uncovered
(~59% of `protocol_event` sits below the window floor). `allocation_position` (blocks 24.6M+) should
be almost fully covered once Step 3 adds the deeper sources.

## Step 3 -- close the gap, cheapest first (only if Step 2 is material)

1. **Deeper in-DB sources.** The native-timestamp tables push coverage back to ETH block ~22.97M
   (`onchain_token_price`), which is where ~100% of the row mass lives (only 726 `protocol_event`
   rows fall below it). Add them to `block_time`, attributing `chain_id` where the table lacks it
   (morpho/uniswap/curve are ETH-only; `onchain_token_price` mixes ETH+Avalanche -- split by the
   non-overlapping block ranges). Re-run Step 2.
2. **Node/Alchemy fetch** for whatever genuinely remains (the deep tail + any in-range blocks with no
   coincident event). This is the only part needing external access (Erigon for ETH, Alchemy for the
   L2s/Avalanche) -- scope it to the measured residual, not a blanket re-fetch.

## Going-forward maintenance (decision for the watcher owners)

`block_states.created_at` stays authoritative for new blocks, so `block_time` needs a top-up. Options,
recommendation first:

- **Transform-worker / cron top-up (recommended):** each cycle, `INSERT INTO block_time SELECT
  chain_id, number, created_at FROM block_states WHERE NOT is_orphaned AND (chain_id, number) NOT IN
  (...) ON CONFLICT DO NOTHING`. No write on the ingestion hot path.
- **AFTER INSERT trigger on block_states:** simplest to reason about, but adds a write to core
  ingestion for every block on every chain -- needs watcher-owner sign-off before going on the hot path.
