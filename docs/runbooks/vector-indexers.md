# Vector — indexers runbook

Owner: vector team · Source rules: [alerts/vector-indexers.yaml](../../alerts/vector-indexers.yaml)

The Vector indexers are the last hop in the pipeline before customer-facing
data lands in TimescaleDB. They consume blocks from the watcher and derive
product state (positions, prices, DEX state). A stall here directly
translates to stale downstream reads.

Indexers covered by this runbook:

- `stl-morpho-indexer` (Morpho positions)
- `stl-oracle-indexer` (Chainlink price snapshots)
- `curve-dex-worker` (Curve Stableswap pool / gauge state)
- `uniswap-v3-dex-worker` (UV3 pool + NFPM position state)
- `balancer-dex-worker` (Balancer V2 pool state)

All three DEX workers share the wiring in
`cmd/workers/internal/dexbootstrap` and emit metrics via
`internal/pkg/dextelemetry` — common-cause checks (Postgres, Redis, Alchemy,
SQS) therefore apply uniformly to all three.

---

## VectorMorphoIndexerStalled / VectorOracleIndexerStalled

**Severity:** critical · **For:** 15m

### What it means

The labelled indexer on the labelled `chain` has processed zero blocks for
15 minutes. Downstream TimescaleDB state for Morpho positions / oracle
prices is going stale.

### First checks (≤5 min)

1. **Pod status** — run the one matching the firing alert:
   - Morpho: `kubectl -n vector get pods -l app=stl-morpho-indexer`
   - Oracle: `kubectl -n vector get pods -l app=stl-oracle-indexer`
2. **Recent logs** — look for decode panics, DB connection errors, or
   `context deadline exceeded` against the watcher's archive RPC.
3. **Upstream lag** — confirm the watcher is producing for this chain (if
   not, fix that first — that's `VectorWatcherNoBlocks`).
4. **TimescaleDB health** — connection pool exhaustion or replication lag
   can stall writes; check the Postgres dashboard.

### Common causes

- Indexer stuck on a malformed event after a contract upgrade → add the new
  ABI / decoder and redeploy.
- DB connection pool saturated → restart the pod; longer-term raise the
  pool limit.
- Watcher archive RPC slow → see `VectorMorphoIndexerRPCLatencyHigh`.

### Verify recovery

`rate({morpho,oracle}_blocks_processed_total) > 0` for the affected chain.

---

## VectorMorphoIndexerErrorsHigh / VectorOracleIndexerErrorsHigh

**Severity:** warning · **For:** 15m

### What it means

The labelled indexer is logging more than 0.1 errors/sec sustained for
15 minutes. Investigate before it escalates to a full stall.

### First checks

- Inspect recent logs for the dominant error class.
- Correlate with recent deploys (`kubectl rollout history`).
- Check for chain reorgs in the watcher logs — indexers may need to roll
  back state.

---

## VectorMorphoIndexerRPCLatencyHigh / VectorOracleIndexerRPCLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 RPC request latency from the indexer (to the watcher's archive RPC) is
over 3 seconds sustained for 15 minutes. The indexer is degraded; expect
downstream lag.

### First checks

- Watcher RPC pod health and CPU usage.
- Network latency between the indexer node group and the watcher service.
- Recent changes in batch size / pagination defaults.

---

---

## VectorCurveDexWorkerStalled

**Severity:** critical · **For:** 15m · **Metric:** `curve_blocks_processed_total`

### What it means

`curve-dex-worker` has processed zero SQS block events for 15 minutes.
Curve pool / gauge / LP-position state in TimescaleDB is going stale.

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=curve-dex-worker`. A
   crash-loop or `Pending` pod is the most common cause.
2. **Recent logs** — `kubectl -n vector logs -l app=curve-dex-worker --tail=200`.
   Look for: decode panics on a new pool kind, MetaRegistry RPC errors during
   startup bootstrap (review-6 B5 made these non-fatal but a flood points to
   an upstream Alchemy problem), `context deadline exceeded` against the
   multicall RPC, or repeated `ON CONFLICT DO NOTHING` skips (cross-build
   re-processing, expected per ADR-0002).
3. **Upstream queue depth** — confirm the watcher is still publishing to
   the SNS topic and the worker's SQS queue is draining:
   `aws sqs get-queue-attributes --queue-url $CURVE_QUEUE_URL --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible`.
4. **DB / cache health** — `psql ... -c "SELECT count(*) FROM curve_pool_state WHERE timestamp > now() - interval '15 min'"`
   should be non-zero on a healthy chain.

### Common causes

- Alchemy WebSocket gap upstream → watcher stops producing, downstream
  workers stall in sympathy. Fix watcher first (`VectorWatcherNoBlocks`).
- Postgres connection pool saturated → restart the worker pod; longer-term
  raise the pool size.
- New Curve Stableswap-NG pool variant whose multicall layout we can't
  decode → see ABI mismatch errors in logs; needs a code fix.

### Verify recovery

`sum(rate(curve_blocks_processed_total{k8s_namespace_name="vector"}[5m])) > 0`.

---

## VectorCurveDexWorkerErrorsHigh

**Severity:** warning · **For:** 15m · **Metric:** `curve_errors_total`

### What it means

curve-dex-worker is logging more than 0.1 errors/sec sustained for 15
minutes. Investigate before it turns into a stall.

### First checks

- Group errors by operation label — currently the worker only emits the
  `processBlockEvent` operation, so the log message is the discriminator.
- Correlate with recent Curve deploys / factory deploys; a new pool kind
  the worker doesn't recognise generates a steady error stream until ABIs
  are updated.
- A persistent burst on `MetaRegistry.get_gauge` calls suggests Alchemy
  rate-limits — back off the worker's startup poll or contact upstream.

---

## VectorCurveDexWorkerLatencyHigh

**Severity:** warning · **For:** 15m · **Metric:** `curve_block_duration_seconds_bucket`

### What it means

p99 wall-clock duration of one `processBlockEvent` is over 5 seconds for
15 minutes. The worker is keeping up but slowly; SQS visibility-timeout
margin is shrinking.

### First checks

- Multicall RPC latency: check Alchemy dashboard or the equivalent
  `eth_call` latency from the worker's logs.
- Postgres write path: `pg_stat_activity` for long-running queries against
  `curve_pool_state` or `curve_user_lp_position` hypertables; the columnstore
  chunk policy can stall an UPSERT path if chunks are mid-compression.
- Receipt size: a block with thousands of Curve events in one tx will
  legitimately take longer; correlate with `block_number` in the logs.

---

## VectorUniswapV3DexWorkerStalled

**Severity:** critical · **For:** 15m · **Metric:** `uniswap_v3_blocks_processed_total`

### What it means

`uniswap-v3-dex-worker` has processed zero SQS block events for 15
minutes. Uniswap V3 pool state and NFPM position state are going stale.

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=uniswap-v3-dex-worker`.
2. **Recent logs** — `kubectl -n vector logs -l app=uniswap-v3-dex-worker --tail=200`.
   UV3-specific patterns: `balanceOf` revert errors (review-6 B2 made these
   fatal — a burst means a misbehaving ERC-20 is poisoning a block), slot0
   decode errors (B1 — comma-ok type assertions, error names the offending
   field), NFPM position read failures.
3. **NFPM_ADDRESS sanity** — `kubectl -n vector describe pod ... | grep NFPM_ADDRESS`.
   Should be `0xC36442b4a4522E871399CD717aBDD847Ab11FE88` on mainnet
   (uniswap_v3_dex.DefaultNFPMAddress).
4. **SQS / DB / cache** — same as Curve worker checks above.

### Common causes

- A poison-pill ERC-20 in an indexed pool returning malformed data → B2
  fail-fast triggers, block re-enters queue, eventually DLQ. Mitigation:
  identify the token via logs and pause that pool until upstream fixes.
- Slot0 / position ABI drift on a new pool kind → code fix needed.

### Verify recovery

`sum(rate(uniswap_v3_blocks_processed_total{k8s_namespace_name="vector"}[5m])) > 0`.

---

## VectorUniswapV3DexWorkerErrorsHigh

**Severity:** warning · **For:** 15m · **Metric:** `uniswap_v3_errors_total`

Same shape as Curve's ErrorsHigh. UV3-specific: a sustained error stream
that does NOT also trigger Stalled typically means individual blocks are
DLQ'd while others process; find the poison-pill block (logs include
`block` and `approximateReceiveCount`) and decide whether to skip it.

---

## VectorUniswapV3DexWorkerLatencyHigh

**Severity:** warning · **For:** 15m · **Metric:** `uniswap_v3_block_duration_seconds_bucket`

Same shape as Curve's LatencyHigh. UV3-specific cost driver: NFPM position
reads — every Increase/Decrease/Collect on a known pool fans out a
multicall to NFPM. A block with many position-touches takes proportionally
longer.

---

## VectorBalancerDexWorkerStalled

**Severity:** critical · **For:** 15m · **Metric:** `balancer_blocks_processed_total`

### What it means

`balancer-dex-worker` has processed zero SQS block events for 15 minutes.
Balancer V2 pool state in TimescaleDB is going stale.

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=balancer-dex-worker`.
2. **Recent logs** — Balancer-specific patterns:
   - "decoding getActualSupply / totalSupply / getScalingFactors /
     getSwapFeePercentage for pool 0x..." — review-7 S1 makes these
     mandatory-field decode failures fatal. Records the offending pool
     address; signals a Balancer pool variant we don't decode yet.
   - "BalancerV2 worker only supports chain_id=1" — misconfigured deploy.
3. **CHAIN_ID** — Balancer worker hard-rejects non-mainnet via the
   `chain_id != 1` guard at construction. If the deploy ever moves to L2,
   that guard needs adjusting too.

### Common causes

- A new ComposableStable / Linear pool variant whose multicall layout
  diverges from our ABI → code fix.
- Phantom-BPT-slot handling drift after a new pool factory → check
  recently-added pools.

### Verify recovery

`sum(rate(balancer_blocks_processed_total{k8s_namespace_name="vector"}[5m])) > 0`.

---

## VectorBalancerDexWorkerErrorsHigh

**Severity:** warning · **For:** 15m · **Metric:** `balancer_errors_total`

Same shape as Curve's ErrorsHigh. Balancer-specific: mandatory-field decode
errors are now fatal (review-7 S1), so a sustained error stream against
the same pool address indicates an ABI mismatch on that pool — disable it
in the registry until the decoder is updated.

---

## VectorBalancerDexWorkerLatencyHigh

**Severity:** warning · **For:** 15m · **Metric:** `balancer_block_duration_seconds_bucket`

Same shape as Curve's LatencyHigh. Balancer-specific cost driver: the
per-pool multicall fans out 8 fixed slots + per-token getTokenRate, so
high-token-count pools amplify the multicall round-trip cost.

---

## See also

- Watcher runbook: [vector-watcher.md](vector-watcher.md)
- Backup worker runbook: [vector-backup-worker.md](vector-backup-worker.md)
