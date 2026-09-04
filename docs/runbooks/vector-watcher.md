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

## VectorWatcherNoHeadersReceived

**Severity:** critical · **For:** 5m (a 5m rate window plus `for: 5m`, so up to
~10 minutes from onset to page)

### What it means

`alchemy_subscriber_blocks_received_total` on the labelled `service_name` has been
flat for over 5 minutes while the process is still issuing RPC calls: no
`newHeads` are reaching the live path. Live ingestion for that chain has stopped.
Blocks will still land eventually, but only through the backfill loop, minutes to
hours late and without the live SNS fan-out that the indexers consume.

**Two different faults produce this signal**, and step 1 tells them apart. The
counter is incremented only on a *successful* hand-off into the subscriber's
buffered channel, so a wedged live consumer — socket perfectly healthy, buffer
full, every header discarded — also drives it to zero. In that case
`alchemy_subscriber_blocks_dropped_total` is climbing and the fault is
downstream of the socket, not in it.

This is the counterpart to `VectorWatcherNoBlocks`, and the two are not
interchangeable. `VectorWatcherNoBlocks` watches
`alchemy_client_requests_total{rpc_method="eth_getBlockByNumber"}`, which the live
path and the backfill loop both feed through one shared HTTP client. Backfill
re-verifies its boundary blocks on every poll (30s by default; 5s on the
avalanche and arbitrum watchers), so that counter stays busy even when the socket
has gone completely silent — a dead subscriber does not fire it. This rule reads
the subscriber itself, and pairs it with the shared request counter so that a
subscriber which never delivered a single header — and therefore has no series to
go flat — still alerts.

### First checks (≤5 min)

1. **Is this a dead socket or a wedged consumer?** Check
   `increase(alchemy_subscriber_blocks_dropped_total[10m])` for the same
   `service_name` first. **Nonzero means headers are arriving and being thrown
   away**: the socket is fine, the live consumer is stalled, and
   `VectorWatcherBlocksDropped` will be firing alongside this one — triage from
   that runbook section instead and ignore the rest of this one. Zero means the
   subscriber really is delivering nothing; continue below.
2. **Is the socket flapping or just silent?** Compare
   `rate(alchemy_subscriber_reconnections_total[10m])` and
   `increase(alchemy_subscriber_stalls_total[10m])` for the same
   `service_name`. Stalls climbing means the data-freshness watchdog is firing
   and forcing reconnects that do not recover; both flat means the connection
   looks healthy to us but is delivering nothing.
3. **Pod logs** — `kubectl -n vector logs <watcher pod> --tail=200`. Look for
   `no newHeads within HealthTimeout`, repeated
   `WebSocket connection lost, reconnecting...`, or `failed to connect`.
4. **Alchemy status page** — https://status.alchemy.com/ — confirm WebSocket
   availability for that chain specifically; the HTTP endpoint can be healthy
   while the subscription endpoint is not.
5. **Did the chain itself halt?** Check an explorer for the chain's head. A
   genuinely halted chain produces this alert legitimately, and there is nothing
   to fix on our side.

### Common causes

- Alchemy WebSocket subscription silently dropped without closing the socket —
  the watchdog should force a reconnect, so if this persists the reconnect is
  failing too.
- Alchemy WebSocket outage or subscription quota exhaustion for that chain.
- Network path to the WebSocket endpoint blocked while HTTP still works.

### Recovery

Only once step 1 has ruled out a wedged consumer: restart the watcher pod
(`kubectl -n vector delete pod <pod>`); the Deployment recreates it and the
subscription is re-established from scratch. If the alert returns after a
restart, the problem is upstream — check the Alchemy status page and the API
key's WebSocket quota. Backfill will close the gap once headers flow
again; track it with `backfill_watermark_lag`.

### Verify recovery

`rate(alchemy_subscriber_blocks_received_total[5m])` returns to roughly the
chain's block rate, and `backfill_watermark_lag` drains back toward zero.

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

## VectorWatcherBlocksDropped

**Severity:** warning · **For:** 1m

### What it means

`alchemy_subscriber_blocks_dropped_total` on the labelled `service_name` is
nonzero over the last 10m. The Alchemy WebSocket subscriber forwards each
`newHeads` header into a 100-slot buffered channel with a non-blocking send, so a
header is only discarded once that buffer is full — i.e. the live consumer fell
~100 blocks behind the socket. How long that took depends on the chain: ~20
minutes on Ethereum (~12s blocks), ~3 minutes on an L2 at ~2s, and seconds on a
chain producing several blocks a second.

A dropped header never reaches the live path, so the block lands in Postgres only
when backfill picks the gap up.

This counter is also the cheap way to answer the ARCT-374 question when a gap
appears: **drops here mean our channel dropped it; silence here means Alchemy
never delivered it.**

### First checks (≤5 min)

1. **Watcher logs** — the drop path logs `channel full, dropping block` with the
   block number. `kubectl -n vector logs <watcher pod> | grep 'dropping block'`
   gives the exact heights lost.
2. **What stalled the consumer** — the live path fetches the block body, receipts
   (and traces on Ethereum) from Alchemy, writes Redis, then publishes to SNS.
   Check `VectorWatcherAlchemyLatencyHigh` / `VectorWatcherAlchemyRetriesHigh`
   for a slow RPC leg, then Redis and SNS publish errors in the same logs.
3. **Confirm the gap is being healed** — check `backfill_watermark_lag` and the
   backfill logs for the heights from step 1.

### Common causes

- Alchemy RPC latency spike or a 429 storm slowing the per-block fetch below the
  chain's block rate.
- Redis or SNS backpressure / errors blocking the persist step.
- Watcher pod CPU-throttled or a noisy-neighbour node, so the consumer goroutine
  cannot keep up.

### Recovery

Fix the consumer stall identified in step 2 — that is the action this alert asks
for. Then confirm the heights from the log line exist as canonical rows. Backfill
normally heals them on its own (every staging and prod watcher runs
`ENABLE_BACKFILL=true`; the dev overlay does not), so manual repair is rarely
needed. If backfill is not catching them, follow
`VectorWatcherBackfillWatermarkLagHigh`.

### Verify recovery

`increase(alchemy_subscriber_blocks_dropped_total[10m])` returns to zero while
`rate(alchemy_subscriber_blocks_received_total[5m])` recovers to the chain's block
rate. If dropped goes to zero but received stays at zero, the consumer stall was
not the whole story and the socket is down too — continue from
`VectorWatcherNoHeadersReceived`.

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

That self-heal only reaches heights whose `block_states` rows still exist:
retention drops them after 30 days, and `retryBlockPublish` only ever looks at
`NOT is_orphaned` rows. An orphan-only height older than that is repaired by the
**`block-republisher`** on-demand worker instead, which re-publishes the
canonical block under a new `block_version` so every indexer appends the
correction — see
[vector-cronjobs.md](vector-cronjobs.md#special-case-block-republisher-on-demand-no-schedule).
Confirm the shape first with the query above; the rows may already be gone, in
which case the S3 archive holding only `<number>_0_*` objects is the evidence.

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
2. **Which shape is it?** Compare the orphaned row's hash against the chain:
   `cast block <N> --field hash --rpc-url <chain rpc>`. A hash the chain **does**
   know is the VEC-277 over-orphan, healed by un-orphaning the row. A hash the
   chain does **not** know is the ARCT-379 shape — a losing fork the watcher
   kept after the canonical broadcast for that height was dropped as
   `stale_fork`. The gap filler refuses to un-orphan it (doing so would make the
   losing fork canonical and wedge every height above it), so the watermark
   stays pinned below N and the lag grows.
3. **Read `backfill_watermark_advance_skipped_total` the right way round.** A
   non-zero rate on the same `service_name` means a reorg commit landed
   between a pass's cursor read and its compare-and-set write: the pass's
   conclusion was stale and it declined to advance, and the next pass re-runs
   the scan against the new cursor. That is contention, and it self-heals — it
   is *not* the wedge signature. Every wedge shape returns before the
   compare-and-set is ever attempted, so the counter stays at **zero** while
   the cursor is stuck: the ARCT-379 shape in step 2 (the target never rises
   above the pinned watermark), a target capped at an unpublished block, and a
   persistent chain-integrity violation over the range being retired. **A
   wedge is this counter at zero with the lag gauge climbing**; a busy chain is
   this counter ticking with the lag still draining.
4. **Upstream RPC** — check the Alchemy 429 / error rate; degraded RPC beyond
   the catch-up rate also grows lag.
5. **Watcher logs** for repeated gap-fill of the same numbers.

### Recovery

If it is the VEC-277 orphan-only shape, the self-heal drains it automatically
once the fix is deployed. If it is the ARCT-379 shape, use the repair in
[vector-cronjobs.md → `watcher-data-validator` "Orphan-only heights"](vector-cronjobs.md#special-case-watcher-data-validator-orphan-only-heights);
otherwise follow
[docs/incidents/2026-06-02-arbitrum-backfill-loop.md](../incidents/2026-06-02-arbitrum-backfill-loop.md).
A height whose `block_states` rows have aged out of the 30-day retention is past
the self-heal and needs the `block-republisher` (see
`VectorWatcherSilentBackfillNoCanonical` above).

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
