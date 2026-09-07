# Vector — psm3-indexer runbook

Owner: vector team · Source rules: [alerts/vector-psm3.yaml](../../alerts/vector-psm3.yaml)

`psm3-indexer` is a per-chain, SQS-driven worker. It consumes block events and
every N blocks reads Spark PSM3 reserve state (USDS/sUSDS/USDC balances,
`totalAssets`, sUSDS conversion rate) pinned to the event's block via two
multicall rounds, then appends one `psm3_reserves` row. One Deployment per chain
(`base`, `optimism`, `unichain`, `arbitrum` — **not** mainnet). It is the last
hop before downstream readers see PSM3 reserves; a stall makes those views stale.

Steady state is ~1 sweep / ~10m/chain (one per N-block interval). A failed sweep
is logged and ACKed (not retried): the worker skips that interval and sweeps
again at the next one, so persistent failure surfaces here as a stall or a high
error ratio, not in the DLQ.

Metrics (per chain): `psm3_sweeps_total{status}`, `psm3_sweep_duration_seconds`,
`psm3_last_snapshot_block`, `psm3_total_assets`, `psm3_conversion_rate`, plus
`multicall_batch_size` from the instrumented multicaller.

---

## VectorPSM3IndexerStalled

**Severity:** critical · **For:** 15m

### What it means

`psm3-indexer` on the labelled `chain` has written **no snapshot** for ~45m
(`rate[30m] == 0` held for 15m). Keyed on `psm3_sweeps_total{status="success"}`
going flat, so it fires in two
cases: no sweep ran at all (SQS-starved, wedged loop, or crash-loop), or every
sweep is erroring (`ReadState`/`SaveReserves` failing). Either way no fresh
`psm3_reserves` row lands. This is the silent alive-but-behind case (pod
`Running`, 0 restarts, hours behind) that the process-liveness probe cannot
catch. Check `VectorPSM3IndexerErrorRateHigh` to tell the two apart: if it is
also firing, sweeps are running but all failing — go straight to the logs.

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=psm3-indexer-{{chain}}`
   (one Deployment per chain). Note `Running` vs `CrashLoopBackOff` and restarts.
2. **Recent logs** — `kubectl -n vector logs -l app=psm3-indexer-{{chain}} --tail=100`.
   Healthy logs `psm3 sweep complete` every ~10m. Look for repeated
   `psm3 sweep failed`, RPC `context deadline exceeded`, or DB connection errors.
3. **SQS upstream** — confirm the watcher is publishing block events for this
   chain. No messages on the queue ⇒ the worker is correctly idle and the
   problem is upstream (fix that first).
4. **Last row written** —
   `SELECT max(block_number), max(block_timestamp) FROM psm3_reserves WHERE chain_id = <id>;`
   confirms how far behind the table is.

### Common causes

- SQS queue empty (watcher not producing for this chain) → fix the watcher.
- Wedged loop / lost RPC or DB connection with no crash → restart the pod.
- Crash-loop on startup (bad config, unreachable RPC/DB) → read logs, fix the
  dependency.

### Verify recovery

`rate(psm3_sweeps_total{status="success"}[30m]) > 0` for the affected chain, and
a fresh row lands (`max(block_timestamp)` advances within ~10m).

---

## VectorPSM3IndexerErrorRateHigh

**Severity:** warning · **For:** 15m

### What it means

More than 30% of sweeps on the labelled `chain` are erroring over 10m. Keyed on
the error ratio, not error presence: a single transient RPC failure or a
shutdown-time `context canceled` sweep is one point in a ~5/min stream and stays
under the threshold. While some sweeps still succeed, data is partial (some
intervals skipped), not absent — warn, don't page. A **total** failure (no
successful sweeps) additionally trips the critical `VectorPSM3IndexerStalled`,
which is the one that pages.

### First checks

1. **Dominant error class** —
   `kubectl -n vector logs -l app=psm3-indexer-{{chain}} | grep "psm3 sweep failed"`.
   Common: multicall RPC failures (`ReadState`), DB write failures
   (`SaveReserves`), or a structural state defect (`Validate`).
2. **Correlate with deploys** — `kubectl -n vector rollout history deploy/psm3-indexer-{{chain}}`.
3. **RPC / DB health** — check the upstream RPC and the TimescaleDB dashboard.

### Common causes

- Flaky / rate-limited RPC → usually transient; confirm it clears.
- TimescaleDB write pressure (connection pool, replication lag) → check Postgres.
- A genuine contract/decoder issue after an upgrade → fix the caller.

---

## VectorPSM3IndexerSweepLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 sweep duration on the labelled `chain` exceeded 10s over 15m. A sweep is two
multicall rounds plus a DB write; sustained high latency almost always means the
upstream RPC is degraded. Expect lag in PSM3 reserve state before this becomes a
stall.

### First checks

- `histogram_quantile(0.99, sum by (chain, le) (rate(psm3_sweep_duration_seconds_bucket[10m])))`
  — confirm which chain and how high.
- Upstream RPC node health / latency.
- `multicall_batch_size` and pod CPU — a degraded multicall endpoint or a
  saturated pod both inflate sweep time.

---

## See also

- Indexers runbook: [vector-indexers.md](vector-indexers.md)
- Watcher runbook: [vector-watcher.md](vector-watcher.md)
