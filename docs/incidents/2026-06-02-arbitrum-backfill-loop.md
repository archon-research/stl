# Incident: Arbitrum backfill loop stuck on orphan-only rows

- **Date discovered:** 2026-06-02 (VEC-277)
- **Window:** continuous from ~2026-05-21 until the fix deployed
- **Chains affected:** arbitrum (chain_id 42161); same latent bug on all chains,
  only arbitrum triggered it
- **Impact:** no data loss or incorrect data served. ~584 arbitrum blocks
  permanently reported missing by the gap finder; the backfill loop refetched
  them every poll cycle as no-ops, wasting ~16 req/s/chain of Alchemy compute
  and pinning the backfill watermark. Surfaced only via an Alchemy
  compute-unit (CUPS) alert, not by our own monitoring.

## Symptom

`totalMissing` for arbitrum sat locked at ~584 and never drained. Each backfill
cycle logged `starting gap backfill` / `gap backfill complete` for the same
block numbers, fetched them from Alchemy, and changed nothing. Other chains were
healthy. Watcher RPC traffic on arbitrum was ~10x baseline and flat.

## Root cause (two bugs compounding)

**1. Over-orphaning in the reorg handler.** `HandleReorgAtomic` orphaned *every*
row above the common ancestor:

```sql
UPDATE block_states SET is_orphaned = TRUE WHERE chain_id = $1 AND number > $2;
```

When a block arrives whose number is at or below our stored head, `detectReorg`
classifies it as a reorg. On a sub-second-block-time chain like arbitrum,
**out-of-order delivery** of consecutive blocks is routine and is
indistinguishable, at that check, from a real reorg. The handler then
blanket-orphaned canonical successors that were *still on the canonical chain* —
their hashes never changed.

**2. No way back once orphaned.** `processBlockData`'s idempotency check used
`GetBlockByHash` (which returns orphaned rows) and skipped on any hit. So when
the backfill refetched one of those wrongly-orphaned blocks, it saw the
orphan-only row, logged "block already exists", and returned — never
re-canonicalising it. Meanwhile `FindGaps` filters out orphaned rows, so it kept
reporting the same numbers as gaps. A closed loop: gap finder finds it → backfill
fetches it → idempotency check skips it → still a gap next cycle, forever.

### Why only arbitrum

Out-of-order / late delivery requires two blocks in flight close together.
Arbitrum's ~0.25s spacing makes that routine; ethereum (12s) and the other L2s
(1–2s) have enough slack that it effectively never happens. Over 30 days:
arbitrum saw the false-reorg path constantly; ethereum's reorgs were all *real*
(new hashes), which the system handles correctly.

### What triggered it (timeline)

The latent reorg-handler bug had existed for a while but produced no stuck set
on its own — transient orphans normally got overwritten. The **2026-05-20 deploy
of #327 (VEC-241 null-data cleanup)** ran a bulk refill that shared the Alchemy
key, producing 429 rate-limit storms (peak ~29k events/6h on 2026-05-26). Those
storms stalled in-flight blocks behind retries while successors piled up,
manufacturing out-of-order delivery in bulk. Each burst created a fresh batch of
wrongly-orphaned blocks that, per bug #2, could never heal — so the stuck set
ratcheted up and stayed.

## Why it was invisible for ~13 days

A gap-fill cycle could "succeed" (return no error) without ever producing a
canonical row, and nothing alerted on that. The only outward signal was Alchemy
compute consumption. There was no metric for backfill backlog or for the
no-canonical-row invariant.

## The fix (VEC-277 / PR #373)

- **Hash-aware reorg orphaning.** `HandleReorgAtomic` takes `preserveChain`
  (`(number, hash)` pairs of the new canonical chain — the incoming block, the
  reorg-walk's discovered parents, and rows that chain forward from the incoming
  block) and excludes them from the orphan UPDATE. Late arrivals no longer orphan
  canonical successors.
- **Self-heal on idempotent refetch.** `processBlockData` clears `is_orphaned`
  on an orphan-only row when the refetched block's parent hash and linkage match
  (`ClearBlockOrphaned`, guarded by the per-`(chain_id, number)` advisory lock and
  a conflicting-canonical check). This drains the existing stuck set via the
  normal backfill loop on deploy — no migration.
- **Observability.** New counter `backfill.gap_fill.no_canonical.total` + ERROR
  log when a cycle completes without a canonical row, plus alerts
  `VectorWatcherSilentBackfillNoCanonical` and `VectorWatcherBackfillBacklogGrowing`.

## Recovery playbook (if this recurs)

1. Confirm the shape: pick a stuck block number `N` from the ERROR log and run,
   against the chain's DB:
   ```sql
   SELECT number, hash, is_orphaned, version, block_published
   FROM block_states WHERE chain_id = <id> AND number = <N> ORDER BY version;
   ```
   The failure mode is: an orphaned row at `N` with **no** non-orphaned row at `N`.
2. Verify it is wrongly orphaned: the canonical child at `N+1` should reference
   the orphaned row's hash as its `parent_hash`. If so, the orphan is on the real
   chain and must be resurrected.
3. With the fix deployed, the backfill loop heals it automatically within one
   poll interval. Confirm `totalMissing` drops to ~0 and
   `backfill_gap_fill_no_canonical_total` stays at zero.
4. If a bulk refill / backfill tool is the trigger, run it with a **separate
   Alchemy key** or strict concurrency so it cannot 429-starve the live watcher.
5. Check downstream: blocks orphaned during the incident emitted reorg events at
   that time; spot-check the S3 backups / indexers for the affected numbers, since
   the heal intentionally does **not** republish.

## Prevention / follow-ups

- Distinguish late-arrival from real reorg in `detectReorg` itself, rather than
  relying on `HandleReorgAtomic` being correct after the fact.
- Export a direct backfill-backlog / watermark-lag gauge so the symptom is
  alertable without the no-canonical proxy.
