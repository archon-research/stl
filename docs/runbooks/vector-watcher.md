# Vector — watcher runbook

Owner: vector team · Source rules: [alerts/vector-watcher.yaml](../../alerts/vector-watcher.yaml)

The chain watcher (`stl-watcher`) polls Alchemy for new blocks per chain and
feeds the downstream Vector pipeline (backup worker, indexers). It is the
head of the pipeline — if it stalls, everything downstream goes idle.

---

## VectorWatcherNoBlocks

**Severity:** critical · **For:** 1m

### What it means

`stl-watcher` on the labelled `chain` has not issued a single
`eth_getBlockByNumber` call to Alchemy in the last 1 minute (rate over a
1m lookback + `for: 1m`, so effective time-to-fire is ~2m). A healthy
watcher polls every block (~12s on L1, ~2s on L2s) so a 1-minute gap is
already abnormal.

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=stl-watcher` (filter
   to the chain via the `chain` env var or the pod label your overlay uses).
   Look for `CrashLoopBackOff`, `OOMKilled`, or `0/1 Ready`.
2. **Recent logs** — `kubectl -n vector logs <pod> --tail=200`. Look for
   panics, `context deadline exceeded`, or auth/quota errors from Alchemy.
3. **Alchemy status page** — https://status.alchemy.com/ — confirm the
   provider is up for that chain.

### Common causes

- Watcher pod evicted / crashed → restart by deleting the pod; ArgoCD/Deployment
  will recreate it.
- Alchemy outage for that chain → wait for upstream recovery; nothing to do
  on our side. If extended, fail over to the fallback subscriber (see service
  README).
- `ALCHEMY_API_KEY` rotated but not redeployed → check the secret value
  matches what Alchemy expects.

### Verify recovery

The alert auto-resolves once `rate(alchemy_client_requests_total{rpc_method="eth_getBlockByNumber"}) > 0`
for the chain. Cross-check downstream lag in the Vector dashboard.

---

## VectorWatcherAlchemyErrorsHigh

**Severity:** critical · **For:** 10m

### What it means

Over 50% of Alchemy RPC calls from `stl-watcher` on the labelled `chain` are
failing, sustained for 10 minutes. Below 50% the SDK retry path should
absorb the failures; above that, the watcher will start lagging.

### First checks (≤5 min)

1. **Alchemy status page** — most likely cause.
2. **Recent logs** — look for the specific HTTP status / RPC error code
   (`429` = rate limit, `401/403` = auth, `5xx` = upstream outage).
3. **API key health** — check the Alchemy dashboard for quota / billing
   issues.

### Common causes

- Alchemy outage → wait for recovery.
- Quota exhausted → bump the plan or rotate to a backup key.
- Misconfigured endpoint after a deploy → roll back to the previous tag.

### Verify recovery

`error_ratio` drops below 0.5 sustained for the same window length.

---

## VectorWatcherAlchemyLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 Alchemy RPC latency from the labelled watcher `service_name` is over 5s
sustained for 15m. Healthy p99 is sub-second. The poll loop is degraded but
not broken — a leading indicator before `VectorWatcherNoBlocks` fires.

### First checks (≤5 min)

1. **Alchemy status page** — https://status.alchemy.com/ for that chain.
2. **Correlate** with `VectorWatcherAlchemyErrorsHigh` /
   `VectorWatcherAlchemyRetriesHigh` — latency usually rises alongside a
   429/error storm.
3. **Watcher logs** for `context deadline exceeded` and slow-request entries.

### Common causes

- Alchemy degraded for that chain → wait for upstream recovery.
- Quota throttling under load → check the Alchemy dashboard; bump plan or
  rotate to a backup key.
- Network path between the watcher node group and Alchemy degraded.

### Verify recovery

`histogram_quantile(0.99, rate(alchemy_client_request_duration_seconds_bucket[10m]))`
returns sub-second for the chain.

---

## VectorWatcherAlchemyRetriesHigh

**Severity:** warning · **For:** 15m

### What it means

Over 20% of Alchemy calls from the labelled watcher `service_name` are being
retried, sustained for 15m. Alchemy is intermittently failing and the SDK is
masking it via retries. Below 20% is normal noise. Left unchecked this escalates
to `VectorWatcherAlchemyErrorsHigh` (retries exhausted).

### First checks (≤5 min)

1. **Alchemy status page** — intermittent upstream failures are the usual cause.
2. **Recent logs** — identify the retried error class (`429` rate-limit is most
   common; `5xx` = upstream instability).
3. **Shared-key contention** — check for a concurrent bulk refill / backfill tool
   hitting the same Alchemy key (429 storms are the known trigger; see the
   2026-06-02 arbitrum incident).

### Common causes

- Alchemy rate-limiting (429) → reduce concurrent load or rotate to a backup key.
- Transient upstream 5xx → usually self-clears; confirm the ratio drops.
- Quota near exhaustion → bump the plan.

### Verify recovery

The retry ratio (`alchemy_client_retries_total / alchemy_client_requests_total`)
returns below 0.2 sustained.

---

## VectorWatcherSilentBackfillNoCanonical

**Severity:** critical · **For:** 10m

### What it means

`backfill_gap_fill_no_canonical_total` is non-zero for the labelled
`service_name`. A per-block gap-fill cycle completed without producing a
non-orphaned canonical row in `block_states`. This is the exact silent-failure
shape behind the 2026-06-02 arbitrum backfill incident, where the gap finder
re-found the same blocks every cycle and the backfill refetched them as no-ops.

### First checks (≤5 min)

1. **Pull the matching ERROR log** (`backfill completed but no canonical row
   produced`) for the block number and hash.
2. **Inspect the row** for that number on the chain's DB:
   `SELECT number, hash, is_orphaned, version FROM block_states WHERE chain_id = <id> AND number = <N> ORDER BY version;`
   The failure mode is an orphaned row with no non-orphaned row at the number.
3. **Check for a concurrent bulk refill / backfill tool** sharing the Alchemy
   key (429 storms are the known trigger).

### Recovery

Follow [docs/incidents/2026-06-02-arbitrum-backfill-loop.md](../incidents/2026-06-02-arbitrum-backfill-loop.md).
With the VEC-277 fix deployed the backfill loop self-heals within one poll
interval; confirm the counter returns to zero and `totalMissing` drains.

### Verify recovery

`rate(backfill_gap_fill_no_canonical_total[10m])` returns to zero sustained.

---

## VectorWatcherBackfillWatermarkLagHigh

**Severity:** critical · **For:** 30m

### What it means

`backfill_watermark_lag` (highest known block minus the backfill watermark) on
the labelled `service_name` has been over 1000 for 30 minutes. The backfill is
not draining toward head. In steady state this gauge sits near zero; normal
post-restart catch-up drains within minutes.

### First checks (≤5 min)

1. **Is a gap stuck?** Query the chain DB for an orphaned row with no canonical
   row at the same number (the VEC-277 shape) — see the silent-backfill runbook
   above.
2. **Upstream RPC** — check the Alchemy 429 / error rate; degraded RPC beyond
   the catch-up rate also grows lag.
3. **Watcher logs** for repeated gap-fill of the same numbers.

### Recovery

If it is the VEC-277 orphan-only shape, the self-heal drains it automatically
once the fix is deployed; otherwise follow
[docs/incidents/2026-06-02-arbitrum-backfill-loop.md](../incidents/2026-06-02-arbitrum-backfill-loop.md).

### Verify recovery

`backfill_watermark_lag` returns toward zero.

---

## VectorWatcherOutOfOrderBlocksHigh

**Severity:** warning · **For:** 15m

### What it means

`live_block_out_of_order_total` (blocks delivered with number ≤ head) on the
labelled `service_name` is sustained above 0.1/s over 15m. Upstream (Alchemy) is
delivering headers out of order — the VEC-277 trigger. The `outcome` label
splits benign late-arrival fills from reorg-classified blocks.

### First checks (≤5 min)

1. **Correlate with the Alchemy 429 / error rate** — out-of-order delivery
   clusters under upstream rate-limit storms.
2. **Watch the reorg rate** (`chain.reorgs.total`) and
   `backfill_watermark_lag`; if they rise, the trigger is turning into churn.

### Notes

This is a leading indicator, not itself damage: the fix classifies clean
late arrivals as gap fills and the backfill self-heals any over-orphaning. Use
it to catch upstream degradation early.

### Verify recovery

`rate(live_block_out_of_order_total[10m])` returns toward zero.

---

## VectorWatcherOutOfOrderReorgClassification

**Severity:** warning · **For:** 1m

### What it means

`live_block_out_of_order_total{outcome="reorg"}` on the labelled `service_name`
is nonzero over the last 10m: a header arrived at or below the canonical head
and did NOT link cleanly onto our chain, so it was routed to reorg handling
rather than classified as a clean gap fill. The rule is scoped to an explicit
allow-list of the single-sequencer rollup watchers (arbitrum, optimism, base,
unichain, avalanche), where a real reorg is essentially impossible, so this is
the over-orphaning trigger from the 2026-06-02 incident. Ethereum (the bare
`watcher`) reorgs normally (depth 1, a few times a day) and is deliberately not
listed. New single-sequencer chains must be added to the allow-list when
onboarded. Scoping by service name is interim; the proper fix is a chain-behavior
label so alerts stop hardcoding chain names (see VEC-295). Unlike
VectorWatcherOutOfOrderBlocksHigh (a sustained-rate warning across all outcomes),
this fires on a single occurrence of the dangerous subset.

### First checks (≤5 min)

1. Correlate with the Alchemy 429 / error rate: this path clusters under
   upstream rate-limit storms.
2. Check `backfill_watermark_lag` and `backfill_gap_fill_no_canonical_total`
   for orphan churn following the reorg classification.
3. Pull the watcher logs for the block number to see whether the reorg was
   committed or dropped by the RPC canonical-hash check.

### Notes

A reorg-classified out-of-order block is routed through RPC canonical-hash
verification before any state mutation, and the backfill self-heals any
over-orphaning, so a single occurrence is a signal rather than confirmed damage.
Sustained occurrences mean upstream is degraded and the watcher is doing
defensive work it should not need to.

### Verify recovery

`increase(live_block_out_of_order_total{outcome="reorg"}[10m])` returns to zero.

---

## See also

- Pipeline overview: [docs/live_data_architecture.png](../live_data_architecture.png)
- Backup worker runbook: [vector-backup-worker.md](vector-backup-worker.md)
- Indexers runbook: [vector-indexers.md](vector-indexers.md)
