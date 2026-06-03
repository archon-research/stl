# VEC-277 root-cause findings (for independent verification)

Status: root cause re-verified across code, Loki, and read-only DB. Framing
corrected: trigger = Alchemy out-of-order delivery under a self-induced 429
storm; root cause = our reorg-as-blanket-orphan logic + unrecoverable orphan state.
No fix is committed; fix code from this session is parked/uncommitted. This
document is written so another agent can independently re-verify each claim.

Data sources used:
- Staging TimescaleDB, read-only, via SSH tunnel at `localhost:63725`
  (user `stl_read_only`, db `tsdb`). The watcher subscribes to Arbitrum
  **mainnet**; staging `block_states` / `reorg_events` reflect the staging
  watcher processing mainnet headers.
- Grafana Loki (staging), datasource uid `grafanacloud-logs`,
  `{service_name="arbitrum-watcher"}`.
- A live read-only `newHeads` observer run against `wss://arb-mainnet.g.alchemy.com/v2/<key>`.
- Source at branch `bugfix/watcher-stuck-arbitrum` (currently = PR #373 head `1c579cf3`).

---

## 1. The problem

The Arbitrum watcher's backfill loop got stuck on a fixed set of ~584 (prod) /
~829 (staging) blocks from ~2026-05-21 onward. Each backfill cycle re-found the
same blocks, refetched them from Alchemy, and changed nothing, wasting ~16
req/s/chain of Alchemy compute and pinning the backfill watermark. No data was
served incorrectly; the cost was wasted CU and a permanently lagging watermark.
It was invisible to our own monitoring and surfaced only via an Alchemy
compute-unit alert.

## 2. Confirmed facts (each with how to verify)

**F1. Canonical blocks were orphaned and never recovered (the stuck set).**
Staging: orphaned rows exist whose hash is referenced as `parent_hash` by the
canonical child block (so they are on the real chain, i.e. wrongly orphaned),
with no non-orphaned row at that number. Verify:
```sql
WITH b AS (SELECT number FROM block_states
           WHERE chain_id=42161 AND NOT is_orphaned AND number >= 460322232),
     g AS (SELECT number, lag(number) OVER (ORDER BY number) AS prev FROM b),
     gaps AS (SELECT prev+1 AS s, number-1 AS e FROM g WHERE number-prev>1)
SELECT count(DISTINCT bs.number) AS missing_numbers_with_orphaned_row
FROM block_states bs JOIN gaps ON bs.number BETWEEN gaps.s AND gaps.e
WHERE bs.chain_id=42161 AND bs.is_orphaned;
-- observed: 829
```

**F2. The watcher recorded reorgs on a chain that does not really reorg.**
Arbitrum One runs a single Nitro sequencer; real multi-block reorgs are
essentially nonexistent. Yet:
```sql
SELECT depth, count(*) FROM reorg_events WHERE chain_id=42161 GROUP BY depth ORDER BY depth;
-- observed: 183 events, depths 1..18, none with empty old_hash
SELECT count(*) FILTER (WHERE ob.is_orphaned) AS old_still_orphaned,
       count(*) FILTER (WHERE ob.hash IS NOT NULL AND NOT ob.is_orphaned) AS old_now_canonical
FROM reorg_events re LEFT JOIN block_states ob
  ON ob.chain_id=42161 AND ob.hash=re.old_hash WHERE re.chain_id=42161;
-- observed: old_still_orphaned=160, old_now_canonical=0
```

**F3. The live service processed blocks OUT OF ORDER during the 429 storm.**
Reorg events, ordered by detection time, show non-monotonic block numbers:
```sql
SELECT to_timestamp(extract(epoch from detected_at))::timestamp(0) AS detected,
       block_number, depth, left(old_hash,12) AS old_h, left(new_hash,12) AS new_h
FROM reorg_events WHERE chain_id=42161 ORDER BY detected_at DESC LIMIT 12;
```
Observed (ascending time):
```
05:10:11  466757632  depth=7  new=0x51fdd04423
05:10:12  466757631  depth=2  old=0x51fdd04423   <- LOWER number, 1s later; old_hash == prior new_hash
05:10:14  466757640  depth=5  new=0xbce96652d5
05:10:15  466757637  depth=3  old=0xbce96652d5   <- LOWER again; old_hash == prior new_hash
05:10:17  466757647  depth=1
```
`reorg_events.block_number` is the incoming block; `detected_at` is set when the
live service processed it. The sequence 632 -> 631 -> 640 -> 637 is the live
service processing lower-numbered blocks after higher ones. The old_hash ==
prior new_hash chaining shows the head thrashing (each out-of-order block
reorged out the previous one's block).

**F4. A 429 storm was active at that moment.** Loki, window 2026-05-26
05:09-05:11, is dominated by `RPC error 429: Your app has exceeded its compute
units per second capacity` on `component=backfill-service` fetches. There is
also a live line `reorg detection failed ... HTTP 429` at 05:10:13 (the reorg
walk itself hitting 429). Verify:
```
{service_name="arbitrum-watcher"} |~ "429|reorg detection failed"  [2026-05-26 05:09..05:11]
```

**F5. Out-of-order arrivals cluster in the incident window.** Caveat: this
metric is partly contaminated by backfill (see "ruled weak" below), but the
time-clustering is still informative. Adjacent canonical pairs where the higher
number has an earlier `received_at`, by day: background days 1-17, incident days
(2026-05-20..26) 149,78,92,65,190,166,193. The #327 deploy was 2026-05-20.

**F6. Reorg events are written ONLY by the live path.**
`grep -rn "HandleReorgAtomic(" internal/ cmd/ --include="*.go" | grep -v _test`
-> only `internal/services/live_data/live_data_service.go:574` (in
`persistBlockState`). Backfill orphans via `MarkBlockOrphaned`, which does not
write `reorg_events`. So F3's records are the live path's processing order.

**F7. The live path processes strictly sequentially.**
`processHeaders` (live_data_service.go) is a single loop calling
`processBlockWithPrefetch` synchronously; no per-block goroutine. So live
processing order == subscriber delivery order. Backfill `processBatch` is also a
sequential `for` loop over the batch.

**F8. The subscriber preserves wire order within a connection and cannot
reorder/replay/duplicate.** `internal/adapters/outbound/alchemy/subscriber.go`:
a single reader goroutine (line ~325) does `ReadJSON` sequentially -> `blockChan`
-> main loop -> `s.headers`, all single-producer/single-consumer FIFO.
Backpressure is DROP (line ~404), not reorder. Reconnect (line ~247) issues a
fresh `eth_subscribe newHeads` (no historical replay); missed blocks become a
forward gap. `blockChan` is recreated per `readLoop`.

**F10. No websocket reconnect occurred during the incident window.** Loki
`{service_name="arbitrum-watcher"} |~ "connection lost|connected to Alchemy"`
over 2026-05-26 04:30-05:30 returned ZERO matches (602,774 lines scanned),
while the out-of-order reorg cascade (F3) was at 05:10. So the reordering
happened WITHIN a single stable connection, not across a reconnect. (A reconnect
WAS seen later at ~05:52, unrelated to the 05:10 cascade.) Implication: the
confirmed mechanism is within-connection out-of-order delivery from Alchemy
under 429 load.

Reconnect as a SEPARATE theoretical path (not implicated here): on reconnect the
subscriber dials fresh and resends `eth_subscribe newHeads`, resuming forward
from the new backend node's head; it tracks no last-seen block number, so if the
new node lags the previous one it would forward a lower header unguarded into
the `block.Number <= head` reorg branch. Note `SetOnReconnect` is NEVER wired in
the watcher (no caller in cmd/ or internal/services/), so reconnect does not
trigger any backfill ramp.

**F9. In a calm window, delivery is perfectly in order.** A read-only observer
reusing `alchemy.NewSubscriber`/`Subscribe` ran 4 minutes against live Arbitrum
mainnet: 961 headers, blocks 469,529,580 -> 469,530,540, strictly ascending,
zero non-monotonic, zero duplicates, zero parent-link breaks, no reconnects.

## 3. Root cause and trigger

The earlier framing ("received out of order from Alchemy") named the correct
*mechanism* but the wrong *owner*. The two must be separated.

### Root cause (ours)

The live watcher treats any incoming header whose number is at or below our
stored canonical tip as a reorg and, via `HandleReorgAtomic`, unconditionally
blanket-orphans every canonical block above the common ancestor:

```sql
UPDATE block_states SET is_orphaned = TRUE WHERE chain_id = $1 AND number > $2 AND NOT is_orphaned
```

It cannot tell a competing fork apart from a late / out-of-order re-delivery of a
block that is itself still canonical. Three properties together convert a
transient, self-healing upstream event into permanent corruption:

1. No late-arrival discrimination in `detectReorg` (the `block.Number <=
   latestBlock.Number` branch, live_data_service.go). Pre-VEC-277 any such header
   went straight to `handleReorg`.
2. The VEC-202 verify gate does not catch it. `verifyIncomingIsCanonical` (PR
   #269, ordered before #327 so live during this incident) only rejects headers
   that are NOT canonical (stale forks). A late header IS canonical, so it passes
   the RPC hash check and the reorg commits. The guard has a hole precisely for
   the canonical-but-late case.
3. Orphaned rows are terminal. `FindGaps` filters `NOT is_orphaned`
   (blockstate_repository.go:737), so a wrongly-orphaned canonical block reads as
   a permanent gap; backfill refetches it, finds the orphan-only row by hash, logs
   "block already exists, skipping", and (pre-VEC-277) never clears the flag. No
   flow resurrects it.

JSON-RPC providers do not guarantee `newHeads` ordering or completeness and
degrade this way under throttling on fast chains, so an out-of-order or regressed
head is a foreseeable upstream condition a watcher must tolerate. The latent
fragility above, not the upstream behavior, is why a hiccup became 829 lost
canonical blocks (staging; ~584 prod). This is the SECOND instance of the same
design weakness: VEC-202 was over-orphaning from stale forks (patched with the
gate that left the hole), VEC-277 is the same over-orphaning through the
canonical-but-late hole.

### Trigger (Alchemy-side, under self-induced load)

The 2026-05-20 deploy of #327 (VEC-241 null-data cleanup) ran a bulk refill that
shared the Alchemy API key, exhausting compute-units/sec and producing sustained
429 storms (F4). Under that throttling, Alchemy delivered `newHeads` for the
fast-moving Arbitrum chain OUT OF ORDER on a single, stable WebSocket connection.

### Direct observation (this is no longer only a deduction)

The incident logs capture the out-of-order delivery directly. On one pod
(`arbitrum-watcher-65798c4f5c-6st8g`), with zero reconnects in the window (F10
re-verified: no "connected to Alchemy" / "connection lost" / "failed to connect"
between 04:45 and 05:25), the processed block-number sequence (= dequeue order =
wire-delivery order) at 05:10 was:

```
629, 635, 650, 632, 631, 637, 640, 637, 645, 646, 647
```

`650` then `632`, `631` is an 18-block backward collapse, and block `637` is
delivered twice. Verify in Loki:
```
{service_name="arbitrum-watcher"} |~ "reorg detected|failed to process live block"
  [2026-05-26 05:08..05:12]
  # read block= in time order; confirm a single k8s_pod_name and no reconnect lines
```
The consumer is a single sequential loop (F7) draining a
single-producer/single-consumer FIFO channel whose only backpressure action is
DROP, never reorder (F8), on a single pod (verified below) with no reconnect
(F10). A FIFO with drops can lag the stream but cannot invert it, so a decreasing
pair in the processed output can only come from a decreasing pair on the wire.
There were 100+ "channel full, dropping block" lines in the window, consistent
with the slow 429-throttled consumer, but drops do not explain the inversion.
Therefore Alchemy placed `650` on the wire before `632`.

Single-pod is verified, not assumed: across 2026-05-20..27 (54.9M log lines),
`count(sum by (k8s_pod_name)(count_over_time({service_name="arbitrum-watcher"}[2m])) > 0)`
is constant 1, with one transient 2 at the 2026-05-20 deploy rollout.

A one-hour calm read-only observer (2026-06-03) saw zero out-of-order events.
That is consistent, not contradictory: the fault is load-induced, and calm
operation has no 429 storm to trigger it (matches the F9 calm observer).

### Mechanism end to end

A lower-but-canonical header (e.g. 632 arriving after 650 was already saved)
enters the `block.Number <= head` branch; `handleReorg` walks back to a common
ancestor; `HandleReorgAtomic` orphans the real canonical blocks above it and
inserts the late block. Repeated head thrashing under the storm (each
out-of-order header reorging out the block the previous one installed) orphans a
band of genuine blocks that have no canonical replacement, so they stick.

### DB tie-out (read-only, staging tsdb)

- The 05:10 band is still orphaned with no canonical row at each number,
  interleaved exactly as the thrash predicts: 632, 634-636, 640-644, 650-652
  orphaned while 631, 633, 637-639, 645-649 are canonical. Canonical neighbors on
  both sides prove the orphaned ones are real on-chain blocks left permanently
  gapped.
- `reorg_events` for 466757631..650 match the logs to the sub-second, with
  `old_hash[n] == new_hash[n-1]` chaining (0x51fdd04423, 0xbce96652d5): head
  thrash recorded by our own code.
- F1 still returns 829 (stuck set intact, so the VEC-277 self-heal is not yet
  deployed to staging). At least 251 orphaned rows are provably on-chain (the hash
  is the `parent_hash` of a canonical child); the fuller measure is the 829.
- Reorg depths actually run 1..35 with 183 events total (F2's "1..18" understated
  the max) on a single-sequencer chain that effectively never reorgs: a depth-35
  "reorg" is impossible as a real chain event. All are over-orphaning artifacts.

### Answer

"Received out of order from Alchemy vs processed out of order by our code":
**received out of order from Alchemy**, now directly observed (single connection,
single pod, no reconnect). But the permanent damage is **our** reorg-as-blanket-
orphan logic, which had zero tolerance for an allowed upstream condition and no
recovery path. Trigger = Alchemy under a self-induced 429 storm. Root cause = the
reorg-as-blanket-orphan design plus unrecoverable orphan state.

## 4. What was ruled out

- **Subscriber reorders within a connection:** ruled out by F8 (code) + F9
  (calm observation).
- **Our code processes out of order (live pipeline or backfill concurrency):**
  ruled out by F6 + F7 (live is the only reorg writer and is sequential;
  backfill is sequential and does not write reorg events).
- **Real chain reorgs:** ruled out by F2 (single-sequencer chain; orphaned
  blocks are chain-linked / actually canonical).

## 5. Confidence, evidence tiering, and residual gaps

Not all evidence here is equally strong. Tiering:

- LOAD-BEARING (the conclusion rests on these), and solid:
  - F3 `reorg_events` processing-order non-monotonicity. Solid because it is a
    direct record written by the code, not an inferred metric. Verified that
    `detected_at` = `receivedAt` = `time.Now()` stamped at channel dequeue in
    `processHeaders` (live_data_service.go:191), built into the event at
    lines 926/937, stored at the INSERT (blockstate_repository.go:565), and that
    the reorg write is on the synchronous `runStateOps` path, not the prefetch
    goroutine. So detected_at order == delivery order.
  - F6 (reorg events live-only), F7 (live sequential), F8 (subscriber FIFO):
    statically verifiable from source.
  The deduction: sequential processing (F7) + FIFO/order-preserving delivery
  (F8) means non-monotonic processing order (F3) can only arise from
  non-monotonic delivery, which (F8) can only originate at Alchemy.

- CORROBORATING ONLY (do NOT rely on as proof):
  - F5 `received_at` inversion counts. `received_at` is also written by backfill
    at fill-time, and during the incident backfill filled recent gaps within
    seconds, so even the "< 600s" filter does not cleanly exclude backfill.
    Use only for time-clustering, never as a measure of live out-of-order rate.

Residual caveats even on the load-bearing evidence:
1. `detected_at` is wall-clock `time.Now()`; a backward NTP step could flip
   second-level ordering. Would need repeated >1s backward jumps to fake the
   systematic non-monotonicity. Low risk, not zero.
2. Single watcher pod in the window: VERIFIED, not assumed. Across 2026-05-20..27
   the count of concurrently-active arbitrum-watcher pods is constant 1 (one
   transient 2 at the 2026-05-20 deploy rollout), and every line of the 05:10
   cascade is one pod UID (arbitrum-watcher-65798c4f5c-6st8g).
3. The out-of-order delivery is now a DIRECT observation, not only a deduction:
   the 05:10 processed sequence (650 -> 632 -> 631 -> 637 -> 640 -> 637 -> 647) on
   a single pod with no reconnect (F10) shows the inversion in our own logs, and
   the DB band + reorg_events tie it to the stuck set. The only thing still not
   captured is a literal packet capture off the wire, which is immaterial: a
   single sequential consumer (F7) on a FIFO drop-not-reorder channel (F8) with no
   reconnect (F10) cannot invert order itself, so processed inversion == wire
   inversion.

Overall confidence: HIGH, now multi-source (code + Loki + DB), for both claims:
(a) received out of order from Alchemy, not reordered by our code (F3+F6+F7+F8
plus the direct 05:10 log sequence), and (b) the permanent damage is our
reorg-as-blanket-orphan logic plus unrecoverable orphan state (section 3), not
the upstream delivery.

## 6. Independent checks a verifier should run

1. Re-run the F1, F2, F3 SQL against staging (tunnel up) and confirm the numbers.
2. Re-run the F4 Loki query and confirm the 429 storm at 05:09-05:10 2026-05-26.
3. Re-read subscriber.go (F8) and `processHeaders` (F7) and confirm single
   sequential reader + sequential live loop.
4. Confirm `HandleReorgAtomic` has exactly one non-test caller (F6).
5. Optional, for literal 100%: run a read-only `newHeads` observer during a
   degraded/429 period (or for many hours) and look for a header with
   `number <= runningMax`. A single occurrence is direct proof.

## 7. Fix status (not applied/committed)

Candidate minimal fix discussed but PARKED pending this verification:
- F: `detectReorg` classifies a low block that links cleanly onto the canonical
  chain (no competing canonical row at its number, parent is the canonical
  block at number-1) as a late arrival -> save, not reorg. Targets the
  `block.Number <= head` branch identified as the trigger.
- B: backfill self-heal (`ClearBlockOrphaned`) drains any orphan-only row
  regardless of how it was created, and drains the existing stuck set on deploy.

Note: PR #373's `preserveChain` machinery and several review-driven hardenings
were also written this session; whether to keep them is a separate decision
(see pr-373-review.md). They are not required to validate the root cause above.
