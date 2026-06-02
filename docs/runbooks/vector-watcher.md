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

## See also

- Pipeline overview: [docs/live_data_architecture.png](../live_data_architecture.png)
- Backup worker runbook: [vector-backup-worker.md](vector-backup-worker.md)
- Indexers runbook: [vector-indexers.md](vector-indexers.md)
