# Vector — curve-indexer runbook

Owner: vector team · Source rules: [alerts/vector-curve-indexer.yaml](../../alerts/vector-curve-indexer.yaml)

The `curve-indexer` is an SQS worker (VEC-260) that consumes blocks from the
watcher pipeline and writes Curve DEX state into TimescaleDB: per-block pool
snapshots (stableswap and cryptoswap), swap events, and liquidity events. A
stall or silent-empty snapshot loop means downstream Curve pool state goes
stale.

---

## VectorCurveIndexerStalled

**Severity:** critical · **For:** 15m

### What it means

`curve_blocks_processed_total` has not incremented for 15 minutes on the
labelled `chain`. The Curve pool state in TimescaleDB is going stale; no swap
or liquidity events are being recorded.

### First checks (<=5 min)

1. **Pod status**
   `kubectl -n vector get pods -l app=curve-indexer`
2. **Recent logs** — look for decode panics, DB connection errors,
   `context deadline exceeded`, or SQS poll failures:
   `kubectl -n vector logs -l app=curve-indexer --tail=100`
3. **Upstream lag** — confirm the watcher is producing blocks for this chain
   (if not, the root cause is upstream — see `VectorWatcherNoBlocks`).
4. **SQS queue depth** — check the curve-indexer SQS queue. A depth of 0 with
   no processing means the consumer lost its connection or the queue is empty.
5. **TimescaleDB health** — connection pool exhaustion or replication lag can
   stall writes; check the Postgres dashboard.

### Common causes

- Indexer stuck on a malformed event after a contract upgrade -> add the new
  ABI / decoder and redeploy.
- DB connection pool saturated -> restart the pod; longer-term raise the pool
  limit.
- SQS consumer lost connection -> pod restart reconnects.
- Block latency high enough that the worker is processing but not completing
  within the 5m rate window (see `VectorCurveIndexerBlockLatencyHigh`).

### Verify recovery

`rate(curve_blocks_processed_total[5m]) > 0` for the affected chain.

---

## VectorCurveIndexerErrorsHigh

**Severity:** warning · **For:** 15m

### What it means

`curve_errors_total` is above 0.1 errors/sec sustained for 15 minutes. Errors
are counted per operation (attribute `operation`); the indexer continues
processing but errors at this rate often precede a full stall.

### First checks

1. **Dominant error class** — `sum by (operation)(rate(curve_errors_total[10m]))`
   to see which operation is failing most.
2. **Pod logs** — `kubectl -n vector logs -l app=curve-indexer | grep "ERROR"`
3. **Recent deploys** — `kubectl rollout history deploy/curve-indexer -n vector`.
   A failed ABI decode after a contract change is a common trigger.
4. **Chain reorgs** — check watcher logs; a reorg delivers blocks the indexer
   may reject until the version advances.

### Common causes

- ABI decode failure after a Curve contract upgrade -> update the ABI/decoder.
- DB write error (FK constraint, duplicate key) -> inspect the failing pool and
  block number.
- Transient RPC timeout -> usually self-clears; investigate if sustained.

### Verify recovery

`rate(curve_errors_total[10m]) == 0` for the affected chain.

---

## VectorCurveIndexerBlockLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 block processing duration (`curve_block_duration_seconds`) exceeds 3
seconds sustained for 15 minutes. The indexer is degraded; expect downstream
lag in Curve pool state.

### First checks

1. **Multicall/RPC latency** — the Coordinator's `SnapshotState` calls issue
   batched multicalls to the archive RPC. High latency there dominates block
   duration. Check the archive RPC pod health and CPU.
2. **Pool count** — a large `snapshotSet` (many touched pools or heartbeat
   firing on all pools) multiplies multicall round-trips. Check
   `HeartbeatBlocks` config and pool count.
3. **DB write latency** — confirm TimescaleDB is not under I/O pressure.
4. **Pod CPU/memory** — `kubectl top pod -n vector -l app=curve-indexer`.

### Common causes

- Archive RPC node degraded -> coordinate with infra; consider circuit-
  breaker or fallback RPC.
- Heartbeat interval too short for pool count -> raise `HEARTBEAT_BLOCKS`.
- TimescaleDB I/O contention -> investigate concurrent write patterns.

### Verify recovery

`histogram_quantile(0.99, sum by (le)(rate(curve_block_duration_seconds_bucket[10m]))) < 3`
for the affected chain.

---

## VectorCurveIndexerNoStateWritten

**Severity:** warning · **For:** 30m

### What it means

Blocks are advancing (`curve_blocks_processed_total{status="success"}` is
non-zero) but no pool-state snapshot rows have been written
(`curve_state_rows_written_total` is zero) for 30 minutes. The error path will
NOT catch this: a quietly-empty snapshot loop (e.g. `buildSnapshotSet` always
returns empty, heartbeat disabled and no touched pools, or all pools skipped)
produces no errors, just no state rows.

This is the data-quality / silent-empty check that `VectorCurveIndexerStalled`
cannot see.

### First checks

1. **HeartbeatBlocks config** — if `HEARTBEAT_BLOCKS=0` (disabled) and no
   blocks contain Curve events, `buildSnapshotSet` legitimately returns empty.
   Confirm there is genuine pool activity on-chain or re-enable heartbeat.
2. **Touched pools** — check whether any registered pool addresses match logs
   in the processed blocks. `kubectl logs -l app=curve-indexer` should show
   pool-touch debug entries (or absence thereof).
3. **Pool registry** — if the pool list is empty (LoadPools returned 0 rows),
   `buildSnapshotSet` can never produce entries. Confirm the DB has rows in
   `curve_pool`.
4. **snapshotSet size metric** is not separately emitted; use
   `curve_state_rows_written_total` as the proxy. A sudden drop to zero after
   previously non-zero is more urgent than a fresh deploy with no history.

### Common causes

- `HEARTBEAT_BLOCKS=0` and no Curve events on this chain in 30m (legitimate
  low-activity window) -> confirm on-chain before escalating.
- Pool registry empty (migration not applied, wrong chain ID) -> verify
  `SELECT count(*) FROM curve_pool WHERE chain_id = <id>`.
- Contract address mismatch (new pool deployed at different address) -> update
  the pool registry.

### Verify recovery

`rate(curve_state_rows_written_total[30m]) > 0` for the affected chain, or
confirm on-chain that no Curve activity occurred (legitimate quiet window).

---

## See also

- Watcher runbook: [vector-watcher.md](vector-watcher.md)
- Indexers runbook: [vector-indexers.md](vector-indexers.md)
